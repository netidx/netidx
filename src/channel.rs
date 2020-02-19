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
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task,
};

const BUF: usize = 4096;

fn flush_task(
    mut soc: WriteHalf,
) -> UnboundedSender<(Bytes, oneshot::Sender<Result<(), Error>>)> {
    let (tx, rx) = mpsc::unbounded_channel();
    task::spawn(async move {
        while let Some((batch, fin)) = rx.next().await {
            let _ = fin.send(soc.write_all(&*batch).await);
        }
    });
    tx
}

fn decode_from_buffer(incoming: &mut BytesMut) -> Option<Bytes> {
    if incoming.remaining() < mem::size_of::<u32>() {
        None
    } else {
        let len = BigEndian::read_u32(&*self.incoming) as usize;
        if incoming.remaining() - mem::size_of::<u32>() < len {
            None
        } else {
            incoming.advance(mem::size_of::<u32>());
            Some(incoming.split_to(len).freeze())
        }
    }
}

fn read_task(
    mut soc: ReadHalf,
) -> UnboundedSender<(Vec<Bytes>, oneshot::Sender<(Vec<Bytes>, Result<(), Error>)>)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let mut incoming = BytesMut::with_capacity(BUF);
    task::spawn(async move {
        while let Some((mut batch, fin)) = rx.next().await {
            if incoming.remaining_mut() < BUF {
                incoming.reserve(incoming.capacity());
            }
            let r = match soc.read_buf(&mut incoming).await {
                Err(e) => (batch, Err(e)),
                Ok(0) => (batch, Err(Error::new(ErrorKind::UnexpectedEof, ""))),
                Ok(_) => {
                    while let Some(v) = decode_from_buffer(&mut incoming) {
                        batch.push(v);
                    }
                    (batch, Ok(()))
                }
            };
            let _ = fin.send(r);
        }
    });
}

/// RawChannel sends and receives u32 length prefixed messages, which
/// are otherwise just raw bytes.
pub(crate) struct Channel {
    to_flush: UnboundedSender<(Bytes, oneshot::Sender<Result<(), Error>>)>,
    to_read:
        UnboundedSender<(Vec<Bytes>, oneshot::Sender<(Vec<Bytes>, Result<(), Error>)>)>,
    outgoing: BytesMut,
    incoming: Vec<Bytes>,
}

impl Channel {
    pub(crate) fn new(socket: TcpStream) -> Channel {
        Channel {
            socket,
            outgoing: BytesMut::with_capacity(BUF),
            incoming: BytesMut::with_capacity(BUF),
        }
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
        if self.outgoing.remaining_mut() < mem::size_of::<u32>() {
            self.outgoing.reserve(self.outgoing.capacity());
        }
        self.outgoing.put_u32(msg.len() as u32);
        Ok(self.outgoing.extend_from_slice(&*msg))
    }

    /// Same as queue_send_raw, but encodes the message using msgpack
    pub(crate) fn queue_send<T: Serialize>(&mut self, msg: &T) -> Result<(), Error> {
        if self.outgoing.remaining_mut() < mem::size_of::<u32>() {
            self.outgoing.reserve(self.outgoing.capacity());
        }
        let mut msgbuf = self.outgoing.split_off(self.outgoing.len());
        msgbuf.put_u32(0);
        let mut header = msgbuf.split_to(msgbuf.len());
        header.clear();
        let r = rmp_serde::encode::write_named(&mut BytesWriter(&mut msgbuf), msg)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e));
        match r {
            Ok(()) => {
                header.put_u32(msgbuf.len() as u32);
                header.unsplit(msgbuf);
                Ok(self.outgoing.unsplit(header))
            }
            Err(e) => {
                msgbuf.clear();
                header.unsplit(msgbuf);
                self.outgoing.unsplit(header);
                Err(e)
            }
        }
    }

    pub(crate) fn outgoing(&self) -> usize {
        self.outgoing.len()
    }

    /// Initiate sending all outgoing messages. The actual send will
    /// be done on a background task, which will signal completion via
    /// the returned channel.
    pub(crate) fn flush(&mut self) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.to_flush.send((self.outgoing.split().freeze(), tx));
        rx
    }

    pub(crate) fn fill_buffer(&mut self) -> Option<oneshot::Receiver<Result<(), Error>>> {
        if self.incoming.len() > 0 {
            None
        } else {
            let (tx, rx) = oneshot::channel();
            let batch = mem::replace(&mut self.incoming, Vec::new());
            self.Some(rx)
        }
    }

    /// Receive one message, potentially waiting for one to arrive if
    /// none are presently in the buffer.
    pub(crate) async fn receive_raw(&mut self) -> Result<Bytes, Error> {
        loop {
            match self.decode_from_buffer() {
                Some(msg) => break Ok(msg),
                None => {
                    self.fill_buffer().await?;
                }
            }
        }
    }

    pub(crate) async fn receive<T: DeserializeOwned>(&mut self) -> Result<T, Error> {
        rmp_serde::decode::from_read(&*self.receive_raw().await?)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))
    }

    /// Receive one or more messages.
    pub(crate) async fn receive_batch_raw(
        &mut self,
        batch: &mut Vec<Bytes>,
    ) -> Result<(), Error> {
        batch.push(self.receive_raw().await?);
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
