use crate::{auth::Krb5Ctx, utils::BytesWriter};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use failure::Error;
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{mem, ops::Drop, result::Result};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task,
};

const BUF: usize = 4096;
const LEN_MASK: u32 = 0x7FFFFFFF;
const ENC_MASK: u32 = 0x80000000;

pub(crate) struct Msg<'a, C: Krb5Ctx + Clone> {
    head: BytesMut,
    body: BytesMut,
    con: &'a mut WriteChannel<C>,
    finished: bool,
}

impl<'a, C: Krb5Ctx + Clone> Drop for Msg<'a, C> {
    fn drop(&mut self) {
        if !self.finished {
            self.head.clear();
            self.body.clear();
            self.head.unsplit(self.body.split());
            self.con.buf.unsplit(self.head.split());
        }
    }
}

impl<'a, C: Krb5Ctx + Clone> Msg<'a, C> {
    pub(crate) fn pack_ut(&mut self, msg: &Bytes) {
        self.body.extend_from_slice(&**msg);
    }

    pub(crate) fn pack<T: Serialize>(
        &mut self,
        msg: &T,
    ) -> Result<(), rmp_serde::encode::Error> {
        rmp_serde::encode::write_named(&mut BytesWriter(&mut self.body), msg)
    }

    pub(crate) fn finish(mut self) -> Result<(), Error> {
        if self.body.len() > LEN_MASK as usize {
            bail!("message too large {} > {}", self.body.len(), LEN_MASK)
        } else {
            match self.con.ctx {
                None => {
                    self.head.put_u32(self.body.len() as u32);
                    self.head.unsplit(self.body.split());
                    self.con.buf.unsplit(self.head.split());
                }
                Some(ref ctx) => {
                    let buf = ctx.wrap(true, &*self.body)?;
                    if buf.len() > LEN_MASK as usize {
                        bail!("message too large {} > {}", buf.len(), LEN_MASK)
                    }
                    self.head.put_u32(buf.len() as u32 | ENC_MASK);
                    self.body.clear();
                    self.head.unsplit(self.body.split());
                    self.head.extend_from_slice(&*buf);
                    self.con.buf.unsplit(self.head.split());
                }
            }
            self.finished = true;
            Ok(())
        }
    }
}

fn flush_task(
    mut soc: WriteHalf<TcpStream>,
) -> UnboundedSender<(Bytes, oneshot::Sender<Result<(), Error>>)> {
    type T = (Bytes, oneshot::Sender<Result<(), Error>>);
    let (tx, mut rx): (UnboundedSender<T>, UnboundedReceiver<T>) =
        mpsc::unbounded_channel();
    task::spawn(async move {
        while let Some((batch, fin)) = rx.next().await {
            let _ = fin.send(soc.write_all(&*batch).await.map_err(Error::from));
        }
    });
    tx
}

pub(crate) struct WriteChannel<C> {
    to_flush: UnboundedSender<(Bytes, oneshot::Sender<Result<(), Error>>)>,
    buf: BytesMut,
    ctx: Option<C>,
}

impl<C: Krb5Ctx + Clone> WriteChannel<C> {
    pub(crate) fn new(socket: WriteHalf<TcpStream>) -> WriteChannel<C> {
        WriteChannel {
            to_flush: flush_task(socket),
            buf: BytesMut::with_capacity(BUF),
            ctx: None,
        }
    }

    pub(crate) fn set_ctx(&mut self, ctx: Option<C>) {
        self.ctx = ctx;
    }

    /// Queue outgoing messages. This ONLY queues the messages, use
    /// flush to initiate sending. It will fail if the message is
    /// larger then `u32::max_value()`.
    pub(crate) fn queue_send_ut(&mut self, msgs: &[Bytes]) -> Result<(), Error> {
        let mut msgbuf = self.begin_msg();
        for msg in msgs {
            msgbuf.pack_ut(msg);
        }
        Ok(msgbuf.finish()?)
    }

    /// Same as queue_send_ut, but encodes the message using msgpack
    pub(crate) fn queue_send<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        let mut msgbuf = self.begin_msg();
        match msgbuf.pack(msg) {
            Ok(()) => msgbuf.finish(),
            Err(e) => bail!("can't encode message {}", e),
        }
    }

    /// Get an interface that you can use to pack multiple different
    /// things in one message with one length header.
    pub(crate) fn begin_msg<'a>(&'a mut self) -> Msg<'a, C> {
        if self.buf.remaining_mut() < mem::size_of::<u32>() {
            self.buf.reserve(self.buf.capacity());
        }
        let mut body = self.buf.split_off(self.buf.len());
        body.put_u32(0);
        let mut head = body.split_to(body.len());
        head.clear();
        Msg {
            head,
            body,
            con: self,
            finished: false,
        }
    }

    pub(crate) async fn send_one<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        self.queue_send(msg)?;
        Ok(self.flush().await??)
    }

    pub(crate) fn bytes_queued(&self) -> usize {
        self.buf.len()
    }

    /// Initiate sending all outgoing messages. The actual send will
    /// be done on a background task, which will signal completion via
    /// the returned channel.
    pub(crate) fn flush(&mut self) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.to_flush.send((self.buf.split().freeze(), tx));
        rx
    }
}

pub(crate) struct ReadChannel<C> {
    socket: ReadHalf<TcpStream>,
    buf: BytesMut,
    decrypted: BytesMut,
    ctx: Option<C>,
}

impl<C: Krb5Ctx + Clone> ReadChannel<C> {
    pub(crate) fn new(socket: ReadHalf<TcpStream>) -> ReadChannel<C> {
        ReadChannel {
            socket,
            buf: BytesMut::with_capacity(BUF),
            decrypted: BytesMut::with_capacity(BUF),
            ctx: None,
        }
    }

    pub(crate) fn set_ctx(&mut self, ctx: Option<C>) {
        self.ctx = ctx;
    }

    /// Read a load of bytes from the socket into the read buffer
    pub(crate) async fn fill_buffer(&mut self) -> Result<(), Error> {
        if self.buf.remaining_mut() < BUF {
            self.buf.reserve(self.buf.capacity());
        }
        Ok(if self.socket.read_buf(&mut self.buf).await? == 0 {
            bail!("eof while reading");
        })
    }

    fn decode_from_buffer(&mut self) -> Result<Option<Bytes>, Error> {
        if self.buf.remaining() < mem::size_of::<u32>() {
            Ok(None)
        } else {
            let (encrypted, len) = {
                let header = BigEndian::read_u32(&*self.buf);
                if header > LEN_MASK {
                    (true, (header & LEN_MASK) as usize)
                } else {
                    (false, header as usize)
                }
            };
            if self.buf.remaining() - mem::size_of::<u32>() < len {
                Ok(None)
            } else {
                if encrypted {
                    match self.ctx {
                        None => bail!("a decryption context is required"),
                        Some(ref ctx) => {
                            self.buf.advance(mem::size_of::<u32>());
                            let buf = ctx.unwrap(&*self.buf.split_to(len))?;
                            self.decrypted.extend_from_slice(&*buf);
                            Ok(Some(self.decrypted.split().freeze()))
                        }
                    }
                } else {
                    self.buf.advance(mem::size_of::<u32>());
                    Ok(Some(self.buf.split_to(len).freeze()))
                }
            }
        }
    }

    /// Direct access to the read buffer
    pub(crate) fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Receive one message, potentially waiting for one to arrive if
    /// none are presently in the buffer.
    pub(crate) async fn receive_ut(&mut self) -> Result<Bytes, Error> {
        loop {
            match self.decode_from_buffer()? {
                Some(msg) => break Ok(msg),
                None => {
                    self.fill_buffer().await?;
                }
            }
        }
    }

    pub(crate) async fn receive<T: DeserializeOwned>(&mut self) -> Result<T, Error> {
        Ok(rmp_serde::decode::from_read(&*self.receive_ut().await?)?)
    }

    /// Receive one or more messages.
    pub(crate) async fn receive_batch_ut(
        &mut self,
        batch: &mut Vec<Bytes>,
    ) -> Result<(), Error> {
        batch.push(self.receive_ut().await?);
        while let Some(b) = self.decode_from_buffer()? {
            batch.push(b);
        }
        Ok(())
    }

    /// Receive and decode one or more messages. If any messages fails
    /// to decode, processing will stop and error will be returned,
    /// however some messages may have already been put in the batch.
    pub(crate) async fn receive_batch<T: DeserializeOwned>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<(), Error> {
        batch.push(self.receive().await?);
        while let Some(b) = self.decode_from_buffer()? {
            batch.push(rmp_serde::decode::from_read(&*b)?);
        }
        Ok(())
    }
}

pub(crate) struct Channel<C> {
    read: ReadChannel<C>,
    write: WriteChannel<C>,
}

impl<C: Krb5Ctx + Clone> Channel<C> {
    pub(crate) fn new(socket: TcpStream) -> Channel<C> {
        let (rh, wh) = io::split(socket);
        Channel {
            read: ReadChannel::new(rh),
            write: WriteChannel::new(wh),
        }
    }

    pub(crate) fn set_ctx(&mut self, ctx: Option<C>) {
        self.read.set_ctx(ctx.clone());
        self.write.set_ctx(ctx);
    }

    pub(crate) fn split(self) -> (ReadChannel<C>, WriteChannel<C>) {
        (self.read, self.write)
    }

    pub(crate) fn queue_send_ut(&mut self, msg: &[Bytes]) -> Result<(), Error> {
        self.write.queue_send_ut(msg)
    }

    pub(crate) fn queue_send<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.queue_send(msg)
    }

    pub(crate) async fn send_one<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.send_one(msg).await
    }

    pub(crate) fn begin_msg<'a>(&'a mut self) -> Msg<'a, C> {
        self.write.begin_msg()
    }

    #[allow(dead_code)]
    pub(crate) fn bytes_queued(&self) -> usize {
        self.write.bytes_queued()
    }

    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        Ok(self.write.flush().await??)
    }

    #[allow(dead_code)]
    pub(crate) async fn receive_ut(&mut self) -> Result<Bytes, Error> {
        self.read.receive_ut().await
    }

    pub(crate) async fn receive<T: DeserializeOwned>(&mut self) -> Result<T, Error> {
        self.read.receive().await
    }

    #[allow(dead_code)]
    pub(crate) async fn receive_batch_ut(
        &mut self,
        batch: &mut Vec<Bytes>,
    ) -> Result<(), Error> {
        self.read.receive_batch_ut(batch).await
    }

    #[allow(dead_code)]
    pub(crate) async fn receive_batch<T: DeserializeOwned>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<(), Error> {
        self.read.receive_batch(batch).await
    }
}
