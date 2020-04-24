use crate::{auth::Krb5Ctx, utils::BytesWriter};
use anyhow::{anyhow, Error, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{buf::BufExt, Buf, BufMut, Bytes, BytesMut};
use futures::{prelude::*, select_biased};
use log::info;
use protobuf::{
    error::WireError, CodedInputStream, CodedOutputStream, Message, ProtobufError,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{cmp::min, mem, ops::Drop, result::Result};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{
        mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task,
};

const BUF: usize = 4096;
const LEN_MASK: u32 = 0x7FFFFFFF;
const MAX_BATCH: u32 = 0x3FFFFFFF;
const ENC_MASK: u32 = 0x80000000;

enum ToFlush<C> {
    Flush(BytesMut),
    SetCtx(Option<C>),
}

async fn flush_buf<B: Buf>(
    soc: &mut WriteHalf<TcpStream>,
    buf: B,
    encrypted: bool,
) -> Result<()> {
    let len = if encrypted {
        buf.remaining() as u32 | ENC_MASK
    } else {
        buf.remaining() as u32
    };
    let lenb = len.to_be_bytes();
    let mut buf = BufExt::chain(&lenb[..], buf);
    while buf.has_remaining() {
        soc.write_buf(&mut buf).await?;
    }
    Ok(())
}

fn flush_task<C: Krb5Ctx + Send + Sync + 'static>(
    mut soc: WriteHalf<TcpStream>,
) -> Sender<ToFlush<C>> {
    let (tx, mut rx): (Sender<ToFlush<C>>, Receiver<ToFlush<C>>) = mpsc::channel(10);
    task::spawn(async move {
        let mut ctx: Option<C> = None;
        let mut header = BytesMut::new();
        let mut padding = BytesMut::new();
        let mut trailer = BytesMut::new();
        let res = loop {
            match rx.next().await {
                None => break Ok(()),
                Some(m) => match m {
                    ToFlush::SetCtx(c) => {
                        ctx = c;
                    }
                    ToFlush::Flush(mut data) => match ctx {
                        None => try_cf!(flush_buf(&mut soc, data, false)).await,
                        Some(ref ctx) => {
                            try_cf!(ctx.wrap_iov(
                                true,
                                &mut header,
                                &mut data,
                                &mut padding,
                                &mut trailer
                            ));
                            let msg = header.split().chain(
                                data.chain(padding.split().chain(trailer.split())),
                            );
                            try_cf!(flush_buf(&mut soc, msg, true).await);
                        }
                    },
                },
            }
        };
        info!("flush task shutting down {}", res);
    });
    tx
}

pub(crate) struct WriteChannel<C> {
    to_flush: Sender<ToFlush<C>>,
    buf: BytesMut,
}

impl<C: Krb5Ctx + Clone + Send + Sync + 'static> WriteChannel<C> {
    pub(crate) fn new(socket: WriteHalf<TcpStream>) -> WriteChannel<C> {
        WriteChannel {
            to_flush: flush_task(socket),
            buf: BytesMut::with_capacity(BUF),
        }
    }

    pub(crate) fn set_ctx(&mut self, ctx: Option<C>) -> Result<()> {
        Ok(self.to_flush.send(ctx)?)
    }

    /// Queue a message for sending. This only encodes the message and
    /// writes it to the buffer, you must call flush actually send it.
    pub(crate) fn queue_send<T: Message>(&mut self, msg: &T) -> Result<()> {
        let len = msg.compute_size();
        if self.buf.remaining_mut() < len {
            self.buf.reserve(max(BUF, len));
        }
        let mut out = CodedOutputStream::bytes(&mut *self.buf);
        Ok(msg.write_to_writer(&mut out)?)
    }

    /// Queue and flush one message.
    pub(crate) async fn send_one<T: Message>(&mut self, msg: &T) -> Result<()> {
        self.queue_send(msg)?;
        Ok(self.flush().await?)
    }

    /// Return the number of bytes queued for sending.
    pub(crate) fn bytes_queued(&self) -> usize {
        self.buf.len()
    }

    /// Initiate sending all outgoing messages. The actual send will
    /// be done on a background task. If there is sufficient room in
    /// the buffer flush will complete immediately.
    pub(crate) async fn flush(&mut self) -> Result<()> {
        while self.buf.has_remaining() {
            let chunk = self.buf.split_to(min(MAX_BATCH, self.buf.len()));
            self.to_flush.send(ToFlush::Flush(chunk.freeze())).await?
        }
        Ok(())
    }
}

fn read_task<C: Krb5Ctx + Send + Sync + 'static>(
    soc: ReadHalf<TcpStream>,
    set_ctx: oneshot::Receiver<C>,
) -> Receiver<Bytes> {
    let (tx, rx) = mpsc::channel(10);
    task::spawn(async move {
        let mut ctx: Option<C> = None;
        let mut buf = BytesMut::with_capacity(BUF);
        let res = 'main: loop {
            while buf.len() >= mem::size_of::<u32>() {
                let (encrypted, len) = {
                    let hdr = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                    if hdr > LEN_MASK {
                        (true, (hdr & LEN_MASK) as usize)
                    } else {
                        (false, hdr as usize)
                    }
                };
                if buf.len() - mem::size_of::<u32>() < len {
                    break; // read more
                } else if !encrypted {
                    buf.advance(mem::size_of::<u32>());
                    try_cf!(break, 'main, tx.send(buf.split_to(len).freeze()).await)
                } else {
                    let ctx = match ctx {
                        Some(ref ctx) => ctx,
                        None => {
                            ctx = Some(try_cf!(break, 'main, set_ctx.await));
                            ctx.as_ref().unwrap()
                        }
                    };
                    buf.advance(mem::size_of::<u32>());
                    let decrypted = try_cf!(break, 'main, ctx.unwrap(len, &mut buf));
                    try_cf!(break, 'main, tx.send(decrypted.freeze()).await);
                }
            }
            if buf.remaining_mut() < mem::size_of::<u32>() {
                buf.reserve(buf.capacity());
            }
            if try_cf!(soc.read_buf(&mut buf).await) == 0 {
                break Err(anyhow!("EOF"));
            }
        };
        log::info!("read task shutting down {}", res);
    });
    rx
}

pub(crate) struct ReadChannel<C> {
    buf: Bytes,
    set_ctx: Option<oneshot::Sender<C>>,
    incoming: Receiver<Box<dyn Buf>>,
}

impl<C: Krb5Ctx + Clone> ReadChannel<C> {
    pub(crate) fn new(socket: ReadHalf<TcpStream>) -> ReadChannel<C> {
        let (set_ctx, read_ctx) = oneshot::channel();
        ReadChannel {
            buf: Bytes,
            set_ctx: Some(set_ctx),
            incoming: read_task(socket, set_ctx),
        }
    }

    /// Read context may only be set once. This method will panic if
    /// you try to set it twice.
    pub(crate) fn set_ctx(&mut self, ctx: C) {
        let _ = mem::replace(&mut self.set_ctx, None).unwrap().send(ctx);
    }

    /// Read a load of bytes from the socket into the read buffer
    pub(crate) async fn fill_buffer(&mut self) -> Result<()> {
        if let Some(chunk) = self.incoming.next().await {
            if !self.buf.has_remaining() {
                self.buf = chunk;
            } else {
                // this should be very rare because a batch must
                // exceed 1 GB before it can contain a partial
                // message.
                buf.extend_from_slice(&*chunk);
            }
        } else {
            Err(anyhow!("EOF"))
        }
    }

    pub(crate) async fn receive<T: Message>(&mut self) -> Result<T> {
        if !self.buf.has_remaining() {
            self.fill_buffer().await?;
        }
        loop {
            let mut m = T::new();
            let mut stream = CodedInputStream::from_carllerche_bytes(&self.buf);
            match m.merge_from(&mut stream) {
                Ok(()) => {
                    self.buf.advance(stream.pos());
                    Ok(m)
                }
                Err(ProtobufError::WireError(WireError::UnexpectedEof)) => {
                    self.fill_buffer().await?
                }
                Err(e) => return Err(Error::from(e)),
            }
        }
    }

    pub(crate) async fn receive_batch<T: Message>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<()> {
        batch.push(self.receive()?);
        let mut stream = CodedInputStream::from_carllerche_bytes(&self.buf);
        let res = loop {
            let mut m = T::new();
            match m.merge_from(&mut stream) {
                Ok(()) => batch.push(m),
                Err(ProtobufError::WireError(WireError::UnexpectedEof)) => break Ok(()),
                Err(e) => break Err(Error::from(e)),
            }
        };
        self.buf.advance(stream.pos());
        Ok(())
    }
}

pub(crate) struct Channel<C> {
    read: ReadChannel<C>,
    write: WriteChannel<C>,
}

impl<C: Krb5Ctx + Send + Sync + 'static> Channel<C> {
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

    pub(crate) fn queue_send<T: Message>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.queue_send(msg)
    }

    pub(crate) async fn send_one<T: Message>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.send_one(msg).await
    }

    #[allow(dead_code)]
    pub(crate) fn bytes_queued(&self) -> usize {
        self.write.bytes_queued()
    }

    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        Ok(self.write.flush().await??)
    }

    pub(crate) async fn receive<T: Message>(&mut self) -> Result<T, Error> {
        self.read.receive().await
    }

    pub(crate) async fn receive_batch<T: Message>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<(), Error> {
        self.read.receive_batch(batch).await
    }
}
