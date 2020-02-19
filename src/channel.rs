use crate::utils::BytesWriter;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    io::{Error, ErrorKind},
    mem,
    result::Result,
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

    /// Queue an outgoing message. This ONLY queues the message, use
    /// flush to initiate sending. It will fail if the message is
    /// larger then `u32::max_value()`.
    pub(crate) fn queue_send_ut(&mut self, msg: Bytes) -> Result<(), Error> {
        if msg.len() > u32::max_value() as usize {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("message too large {} > {}", msg.len(), u32::max_value()),
            ));
        }
        if self.buf.remaining_mut() < mem::size_of::<u32>() {
            self.buf.reserve(self.buf.capacity());
        }
        self.buf.put_u32(msg.len() as u32);
        Ok(self.buf.extend_from_slice(&*msg))
    }

    /// Same as queue_send_ut, but encodes the message using msgpack
    pub(crate) fn queue_send<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        if self.buf.remaining_mut() < mem::size_of::<u32>() {
            self.buf.reserve(self.buf.capacity());
        }
        let mut msgbuf = self.buf.split_off(self.buf.len());
        msgbuf.put_u32(0);
        let mut header = msgbuf.split_to(msgbuf.len());
        header.clear();
        let r = rmp_serde::encode::write_named(&mut BytesWriter(&mut msgbuf), msg)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e));
        match r {
            Ok(()) => {
                header.put_u32(msgbuf.len() as u32);
                header.unsplit(msgbuf);
                Ok(self.buf.unsplit(header))
            }
            Err(e) => {
                msgbuf.clear();
                header.unsplit(msgbuf);
                self.buf.unsplit(header);
                Err(e)
            }
        }
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
    
    async fn fill_buffer(&mut self) -> Result<(), Error> {
        if self.buf.remaining_mut() < BUF {
            self.buf.reserve(self.buf.capacity());
        }
        self.socket.read_buf(&mut self.buf).await?;
        Ok(())
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

    pub(crate) fn queue_send_ut(&mut self, msg: Bytes) -> Result<(), Error> {
        self.write.queue_send_ut(msg)
    }

    pub(crate) fn queue_send<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.queue_send(msg)
    }

    pub(crate) async fn send_one<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.send_one(msg).await
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
