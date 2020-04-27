use crate::{auth::Krb5Ctx, utils::Pack};
use anyhow::{anyhow, Error, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{buf::BufExt, Buf, BufMut, BytesMut};
use futures::prelude::*;
use log::info;
use std::{
    cmp::{max, min},
    fmt::Debug,
    mem, result,
    time::Duration,
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{
        mpsc::{
            self,
            error::SendTimeoutError,
            Receiver, Sender,
        },
        oneshot,
    },
    task,
};

const BUF: usize = 4096;
const LEN_MASK: u32 = 0x7FFFFFFF;
const MAX_BATCH: usize = 0x3FFFFFFF;
const ENC_MASK: u32 = 0x80000000;

#[derive(Debug)]
enum ToFlush<C> {
    Flush(BytesMut),
    SetCtx(C),
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

fn flush_task<C: Krb5Ctx + Debug + Send + Sync + 'static>(
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
                        ctx = Some(c);
                    }
                    ToFlush::Flush(mut data) => match ctx {
                        None => try_cf!(flush_buf(&mut soc, data, false).await),
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
        info!("flush task shutting down {:?}", res)
    });
    tx
}

pub(crate) struct WriteChannel<C> {
    to_flush: Sender<ToFlush<C>>,
    buf: BytesMut,
}

impl<C: Krb5Ctx + Debug + Clone + Send + Sync + 'static> WriteChannel<C> {
    pub(crate) fn new(socket: WriteHalf<TcpStream>) -> WriteChannel<C> {
        WriteChannel { to_flush: flush_task(socket), buf: BytesMut::with_capacity(BUF) }
    }

    pub(crate) async fn set_ctx(&mut self, ctx: C) -> Result<()> {
        Ok(self.to_flush.send(ToFlush::SetCtx(ctx)).await?)
    }

    /// Queue a message for sending. This only encodes the message and
    /// writes it to the buffer, you must call flush actually send it.
    pub(crate) fn queue_send<T: Pack>(&mut self, msg: &T) -> Result<()> {
        let len = msg.len();
        if len > u32::MAX as usize {
            return Err(anyhow!("message length {} exceeds max size u32::MAX", len));
        }
        if self.buf.remaining_mut() < len {
            self.buf.reserve(max(BUF, len));
        }
        Ok(msg.encode(&mut self.buf)?)
    }

    /// Queue and flush one message.
    pub(crate) async fn send_one<T: Pack>(&mut self, msg: &T) -> Result<()> {
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
            self.to_flush.send(ToFlush::Flush(chunk)).await?;
        }
        Ok(())
    }

    pub(crate) async fn flush_timeout(
        &mut self,
        timeout: Duration,
    ) -> result::Result<(), SendTimeoutError<()>> {
        while self.buf.has_remaining() {
            let chunk = self.buf.split_to(min(MAX_BATCH, self.buf.len()));
            match self.to_flush.send_timeout(ToFlush::Flush(chunk), timeout).await {
                Ok(()) => (),
                Err(SendTimeoutError::Timeout(ToFlush::Flush(mut chunk))) => {
                    chunk.unsplit(self.buf.split());
                    self.buf = chunk;
                    return Err(SendTimeoutError::Timeout(()));
                }
                Err(SendTimeoutError::Timeout(_)) => {
                    return Err(SendTimeoutError::Timeout(()))
                }
                Err(SendTimeoutError::Closed(_)) => {
                    return Err(SendTimeoutError::Closed(()))
                }
            }
        }
        Ok(())
    }
}

fn read_task<C: Krb5Ctx + Clone + Debug + Send + Sync + 'static>(
    mut soc: ReadHalf<TcpStream>,
    mut set_ctx: oneshot::Receiver<C>,
) -> Receiver<BytesMut> {
    let (mut tx, rx) = mpsc::channel(10);
    task::spawn(async move {
        let mut ctx: Option<C> = None;
        let mut buf = BytesMut::with_capacity(BUF);
        let res: Result<()> = 'main: loop {
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
                    try_cf!(break, 'main, tx.send(buf.split_to(len)).await);
                } else {
                    let ctx = match ctx {
                        Some(ref ctx) => ctx,
                        None => {
                            ctx = Some(try_cf!(break, 'main, set_ctx.await));
                            set_ctx = oneshot::channel().1;
                            ctx.as_ref().unwrap()
                        }
                    };
                    buf.advance(mem::size_of::<u32>());
                    let decrypted = try_cf!(break, 'main, ctx.unwrap_iov(len, &mut buf));
                    try_cf!(break, 'main, tx.send(decrypted).await);
                }
            }
            if buf.remaining_mut() < mem::size_of::<u32>() {
                buf.reserve(buf.capacity());
            }
            if try_cf!(soc.read_buf(&mut buf).await) == 0 {
                break Err(anyhow!("EOF"));
            }
        };
        log::info!("read task shutting down {:?}", res);
    });
    rx
}

pub(crate) struct ReadChannel<C> {
    buf: BytesMut,
    set_ctx: Option<oneshot::Sender<C>>,
    incoming: Receiver<BytesMut>,
}

impl<C: Krb5Ctx + Debug + Clone + Send + Sync + 'static> ReadChannel<C> {
    pub(crate) fn new(socket: ReadHalf<TcpStream>) -> ReadChannel<C> {
        let (set_ctx, read_ctx) = oneshot::channel();
        ReadChannel {
            buf: BytesMut::new(),
            set_ctx: Some(set_ctx),
            incoming: read_task(socket, read_ctx),
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
                let len = self.buf.remaining() + chunk.remaining();
                let mut tmp = BytesMut::with_capacity(len);
                tmp.extend_from_slice(&*self.buf);
                tmp.extend_from_slice(&*chunk);
                self.buf = tmp;
            }
            Ok(())
        } else {
            Err(anyhow!("EOF"))
        }
    }

    pub(crate) async fn receive<T: Pack>(&mut self) -> Result<T> {
        loop {
            if self.buf.remaining() < mem::size_of::<u32>() {
                self.fill_buffer().await?;
            } else {
                let len = BigEndian::read_u32(&*self.buf) as usize;
                if self.buf.remaining() < len + mem::size_of::<u32>() {
                    self.fill_buffer().await?;
                } else {
                    break;
                }
            }
        }
        self.buf.advance(mem::size_of::<u32>());
        Ok(T::decode(&mut self.buf)?)
    }

    pub(crate) async fn receive_batch<T: Pack>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<()> {
        batch.push(self.receive().await?);
        loop {
            if self.buf.remaining() < mem::size_of::<u32>() {
                break;
            }
            let len = BigEndian::read_u32(&*self.buf) as usize;
            if self.buf.remaining() < len + mem::size_of::<u32>() {
                break;
            }
            self.buf.advance(mem::size_of::<u32>());
            batch.push(T::decode(&mut self.buf)?);
        }
        Ok(())
    }
}

pub(crate) struct Channel<C> {
    read: ReadChannel<C>,
    write: WriteChannel<C>,
}

impl<C: Krb5Ctx + Debug + Clone + Send + Sync + 'static> Channel<C> {
    pub(crate) fn new(socket: TcpStream) -> Channel<C> {
        let (rh, wh) = io::split(socket);
        Channel { read: ReadChannel::new(rh), write: WriteChannel::new(wh) }
    }

    pub(crate) async fn set_ctx(&mut self, ctx: C) {
        self.read.set_ctx(ctx.clone());
        let _ = self.write.set_ctx(ctx).await;
    }

    #[allow(dead_code)]
    pub(crate) fn split(self) -> (ReadChannel<C>, WriteChannel<C>) {
        (self.read, self.write)
    }

    pub(crate) fn queue_send<T: Pack>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.queue_send(msg)
    }

    pub(crate) async fn send_one<T: Pack>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.send_one(msg).await
    }

    #[allow(dead_code)]
    pub(crate) fn bytes_queued(&self) -> usize {
        self.write.bytes_queued()
    }

    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        Ok(self.write.flush().await?)
    }

    pub(crate) async fn flush_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<()>> {
        Ok(self.write.flush_timeout(timeout).await?)
    }

    pub(crate) async fn receive<T: Pack>(&mut self) -> Result<T, Error> {
        self.read.receive().await
    }

    pub(crate) async fn receive_batch<T: Pack>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<(), Error> {
        self.read.receive_batch(batch).await
    }
}
