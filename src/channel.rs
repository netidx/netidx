use crate::utils::BytesWriter;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    io::{Error, ErrorKind},
    mem,
    result::Result,
    ops::Drop,
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{
        mpsc::{self, UnboundedSender, UnboundedReceiver},
        oneshot,
    },
    task,
};
use futures::prelude::*;

const BUF: usize = 4096;

pub(crate) struct Msg<'a> {
    head: BytesMut,
    body: BytesMut,
    con: &'a mut WriteChannel,
    finished: bool,
}

impl<'a> Drop for Msg<'a> {
    fn drop(&mut self) {
        if !self.finished {
            self.head.clear();
            self.body.clear();
            self.head.unsplit(self.body.split());
            self.con.buf.unsplit(self.head.split());
        }
    }
}

impl<'a> Msg<'a> {
    pub(crate) fn pack_ut(&mut self, msg: &Bytes) {
        self.body.extend_from_slice(&**msg);
    }

    pub(crate) fn pack<T: Serialize>(
        &mut self,
        msg: &T
    ) -> Result<(), rmp_serde::encode::Error> {
        rmp_serde::encode::write_named(&mut BytesWriter(&mut self.body), msg)
    }

    pub(crate) fn finish(mut self) -> Result<(), Error> {
        if self.body.len() > u32::max_value() as usize {
            Err(Error::new(
                ErrorKind::InvalidData,
                format!("message too large {} > {}", self.body.len(), u32::max_value()),
            ))
        } else {
            self.head.put_u32(self.body.len() as u32);
            self.head.unsplit(self.body.split());
            self.con.buf.unsplit(self.head.split());
            self.finished = true;
            Ok(())
        }
    }
}

pub(crate) struct WriteChannel {
    to_flush: UnboundedSender<(Bytes, oneshot::Sender<Result<(), Error>>)>,
    buf: BytesMut,
}

impl WriteChannel {
    pub(crate) fn new(socket: WriteHalf<TcpStream>) -> WriteChannel {
        WriteChannel {
            to_flush: WriteChannel::flush_task(socket),
            buf: BytesMut::with_capacity(BUF),
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
                let _ = fin.send(soc.write_all(&*batch).await);
            }
        });
        tx
    }

    /// Queue outgoing messages. This ONLY queues the messages, use
    /// flush to initiate sending. It will fail if the message is
    /// larger then `u32::max_value()`.
    pub(crate) fn queue_send_ut(&mut self, msgs: &[Bytes]) -> Result<(), Error> {
        let len = msgs.into_iter().fold(0, |s, m| s + m.len());
        if len > u32::max_value() as usize {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("message too large {} > {}", len, u32::max_value()),
            ));
        }
        if self.buf.remaining_mut() < mem::size_of::<u32>() {
            self.buf.reserve(self.buf.capacity());
        }
        self.buf.put_u32(len as u32);
        Ok(for m in msgs { self.buf.extend_from_slice(&**m); })
    }

    /// Same as queue_send_ut, but encodes the message using msgpack
    pub(crate) fn queue_send<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        let mut msgbuf = self.begin_msg();
        match msgbuf.pack(msg) {
            Ok(()) => msgbuf.finish(),
            Err(e) => Err(Error::new(
                ErrorKind::InvalidData,
                format!("can't encode message {}", e)
            ))
        }
    }

    /// Get an interface that you can use to pack multiple different
    /// things in one message with one length header.
    pub(crate) fn begin_msg<'a>(&'a mut self) -> Msg<'a> {
        if self.buf.remaining_mut() < mem::size_of::<u32>() {
            self.buf.reserve(self.buf.capacity());
        }
        let mut body = self.buf.split_off(self.buf.len());
        body.put_u32(0);
        let mut head = body.split_to(body.len());
        head.clear();
        Msg { head, body, con: self, finished: false }
    }

    pub(crate) async fn send_one<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        self.queue_send(msg)?;
        match self.flush().await {
            Ok(r) => r,
            Err(_) => Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("flush process died while writing"),
            ))
        }
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

pub(crate) struct ReadChannel {
    socket: ReadHalf<TcpStream>,
    buf: BytesMut,
}
    
impl ReadChannel {
    pub(crate) fn new(socket: ReadHalf<TcpStream>) -> ReadChannel {
        ReadChannel {
            socket,
            buf: BytesMut::with_capacity(BUF),
        }
    }
    
    /// Read a load of bytes from the socket into the read buffer
    pub(crate) async fn fill_buffer(&mut self) -> Result<(), Error> {
        if self.buf.remaining_mut() < BUF {
            self.buf.reserve(self.buf.capacity());
        }
        match self.socket.read_buf(&mut self.buf).await {
            Err(e) => Err(e),
            Ok(0) => Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("eof while reading"),
            )),
            Ok(_) => Ok(()),
        }
    }

    fn decode_from_buffer(&mut self) -> Option<Bytes> {
        if self.buf.remaining() < mem::size_of::<u32>() {
            None
        } else {
            let len = BigEndian::read_u32(&*self.buf) as usize;
            if self.buf.remaining() - mem::size_of::<u32>() < len {
                None
            } else {
                self.buf.advance(mem::size_of::<u32>());
                Some(self.buf.split_to(len).freeze())
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
            match self.decode_from_buffer() {
                Some(msg) => break Ok(msg),
                None => { self.fill_buffer().await?; }
            }
        }
    }

    pub(crate) async fn receive<T: DeserializeOwned>(&mut self) -> Result<T, Error> {
        rmp_serde::decode::from_read(&*self.receive_ut().await?)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))
    }

    /// Receive one or more messages.
    pub(crate) async fn receive_batch_ut(
        &mut self,
        batch: &mut Vec<Bytes>,
    ) -> Result<(), Error> {
        batch.push(self.receive_ut().await?);
        while let Some(b) = self.decode_from_buffer() {
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
        while let Some(b) = self.decode_from_buffer() {
            batch.push(
                rmp_serde::decode::from_read(&*b)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            );
        }
        Ok(())
    }
}

pub(crate) struct Channel {
    read: ReadChannel,
    write: WriteChannel,
}

impl Channel {
    pub(crate) fn new(socket: TcpStream) -> Channel {
        let (rh, wh) = io::split(socket);
        Channel {
            read: ReadChannel::new(rh),
            write: WriteChannel::new(wh),
        }
    }

    pub(crate) fn split(self) -> (ReadChannel, WriteChannel) {
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

    pub(crate) fn begin_msg<'a>(&'a mut self) -> Msg<'a> {
        self.write.begin_msg()
    }

    #[allow(dead_code)]
    pub(crate) fn bytes_queued(&self) -> usize {
        self.write.bytes_queued()
    }

    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        match self.write.flush().await {
            Ok(r) => r,
            Err(_) => Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("flush process died while writing"),
            ))
        }
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
