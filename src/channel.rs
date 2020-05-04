use crate::{auth::Krb5Ctx, utils::Pack};
use anyhow::{anyhow, Error, Result};
use bytes::{buf::BufExt, Buf, BufMut, BytesMut};
use byteorder::{BigEndian, ByteOrder};
use futures::prelude::*;
use log::info;
use std::{
    cmp::min,
    fmt::Debug,
    mem, result,
    time::Duration,
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    sync::{
        mpsc::{self, error::SendTimeoutError, Receiver, Sender},
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
    boundries: Vec<usize>,
}

impl<C: Krb5Ctx + Debug + Clone + Send + Sync + 'static> WriteChannel<C> {
    pub(crate) fn new(socket: WriteHalf<TcpStream>) -> WriteChannel<C> {
        WriteChannel {
            to_flush: flush_task(socket),
            buf: BytesMut::with_capacity(BUF),
            boundries: Vec::new(),
        }
    }

    pub(crate) async fn set_ctx(&mut self, ctx: C) -> Result<()> {
        Ok(self.to_flush.send(ToFlush::SetCtx(ctx)).await?)
    }

    /// Queue a message for sending. This only encodes the message and
    /// writes it to the buffer, you must call flush actually send it.
    pub(crate) fn queue_send<T: Pack>(&mut self, msg: &T) -> Result<()> {
        let len = msg.len();
        if len > MAX_BATCH as usize {
            return Err(anyhow!("message length {} exceeds max size {}", len, MAX_BATCH));
        }
        if self.buf.remaining_mut() < len {
            self.buf.reserve(self.buf.capacity());
        }
        let buf_len = self.buf.remaining();
        if (buf_len - self.boundries.last().copied().unwrap_or(0)) + len > MAX_BATCH {
            let prev_len: usize = self.boundries.iter().sum();
            self.boundries.push(buf_len - prev_len);
        }
        match msg.encode(&mut self.buf) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.buf.resize(buf_len, 0x0);
                self.boundries.pop();
                Err(Error::from(e))
            }
        }
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
        for b in self.boundries.drain(..) {
            self.to_flush.send(ToFlush::Flush(self.buf.split_to(b))).await?;
        }
        self.to_flush.send(ToFlush::Flush(self.buf.split())).await?;
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
            while buf.remaining() >= mem::size_of::<u32>() {
                let (encrypted, len) = {
                    let hdr = BigEndian::read_u32(&*buf);
                    if hdr > LEN_MASK {
                        (true, (hdr & LEN_MASK) as usize)
                    } else {
                        (false, hdr as usize)
                    }
                };
                if buf.remaining() - mem::size_of::<u32>() < len {
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
            self.buf = chunk;
            Ok(())
        } else {
            Err(anyhow!("EOF"))
        }
    }

    pub(crate) async fn receive<T: Pack + Debug>(&mut self) -> Result<T> {
        if !self.buf.has_remaining() {
            self.fill_buffer().await?;
        }
        Ok(T::decode(&mut self.buf)?)
    }

    pub(crate) async fn receive_batch<T: Pack + Debug>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<()> {
        batch.push(self.receive().await?);
        while self.buf.has_remaining() {
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

    #[allow(dead_code)]
    pub(crate) async fn flush_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError<()>> {
        Ok(self.write.flush_timeout(timeout).await?)
    }

    pub(crate) async fn receive<T: Pack + Debug>(&mut self) -> Result<T, Error> {
        self.read.receive().await
    }

    pub(crate) async fn receive_batch<T: Pack + Debug>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<(), Error> {
        self.read.receive_batch(batch).await
    }
}
