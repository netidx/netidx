use crate::pack::Pack;
use anyhow::{anyhow, Error, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, BytesMut};
use cross_krb5::K5Ctx;
use futures::{
    channel::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    future,
    prelude::*,
    select_biased, stream,
};
use log::info;
use parking_lot::Mutex;
use std::{clone::Clone, fmt::Debug, mem, ops::Deref, sync::Arc, time::Duration};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    task, time,
};

const BUF: usize = 4096;
const LEN_MASK: u32 = 0x7FFFFFFF;
const MAX_BATCH: usize = 0x3FFFFFFF;
const ENC_MASK: u32 = 0x80000000;

#[derive(Debug)]
pub struct K5CtxWrap<C: K5Ctx + Debug + Send + Sync + 'static>(Arc<Mutex<C>>);

impl<C: K5Ctx + Debug + Send + Sync + 'static> K5CtxWrap<C> {
    pub fn new(ctx: C) -> Self {
        K5CtxWrap(Arc::new(Mutex::new(ctx)))
    }
}

impl<C: K5Ctx + Debug + Send + Sync + 'static> Clone for K5CtxWrap<C> {
    fn clone(&self) -> Self {
        K5CtxWrap(Arc::clone(&self.0))
    }
}

impl<C: K5Ctx + Debug + Send + Sync + 'static> Deref for K5CtxWrap<C> {
    type Target = Mutex<C>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(Debug)]
enum ToFlush<C: K5Ctx + Debug + Send + Sync + 'static> {
    Flush(BytesMut),
    SetCtx(K5CtxWrap<C>),
}

async fn flush_buf<B: Buf, S: AsyncWrite + Send + 'static>(
    soc: &mut WriteHalf<S>,
    buf: B,
    encrypted: bool,
) -> Result<()> {
    let len = if encrypted {
        buf.remaining() as u32 | ENC_MASK
    } else {
        buf.remaining() as u32
    };
    let lenb = len.to_be_bytes();
    let mut buf = Buf::chain(&lenb[..], buf);
    while buf.has_remaining() {
        soc.write_buf(&mut buf).await?;
    }
    Ok(())
}

fn flush_task<C: K5Ctx + Debug + Send + Sync + 'static, S: AsyncWrite + Send + 'static>(
    mut soc: WriteHalf<S>,
) -> Sender<ToFlush<C>> {
    let (tx, mut rx): (Sender<ToFlush<C>>, Receiver<ToFlush<C>>) = mpsc::channel(3);
    task::spawn(async move {
        let mut ctx: Option<K5CtxWrap<C>> = None;
        let res = loop {
            match rx.next().await {
                None => break Ok(()),
                Some(m) => match m {
                    ToFlush::SetCtx(c) => {
                        ctx = Some(c);
                    }
                    ToFlush::Flush(data) => match ctx {
                        None => try_cf!(flush_buf(&mut soc, data, false).await),
                        Some(ref ctx) => {
                            let msg = try_cf!(task::block_in_place(|| ctx
                                .lock()
                                .wrap_iov(true, data)));
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

pub(crate) struct WriteChannel<C: K5Ctx + Debug + Send + Sync + 'static> {
    to_flush: Sender<ToFlush<C>>,
    buf: BytesMut,
    boundries: Vec<usize>,
}

impl<C: K5Ctx + Debug + Send + Sync + 'static> WriteChannel<C> {
    pub(crate) fn new<S: AsyncWrite + Send + 'static>(socket: WriteHalf<S>) -> WriteChannel<C> {
        WriteChannel {
            to_flush: flush_task(socket),
            buf: BytesMut::with_capacity(BUF),
            boundries: Vec::new(),
        }
    }

    pub(crate) async fn set_ctx(&mut self, ctx: K5CtxWrap<C>) -> Result<()> {
        Ok(self.to_flush.send(ToFlush::SetCtx(ctx)).await?)
    }

    /// Queue a message for sending. This only encodes the message and
    /// writes it to the buffer, you must call flush actually send it.
    pub(crate) fn queue_send<T: Pack>(&mut self, msg: &T) -> Result<()> {
        let len = msg.encoded_len();
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

    /// Clear unflused queued messages
    pub(crate) fn clear(&mut self) {
        self.boundries.clear();
        self.buf.clear();
    }

    /// Queue and flush one message.
    pub(crate) async fn send_one<T: Pack>(&mut self, msg: &T) -> Result<()> {
        self.queue_send(msg)?;
        Ok(self.flush().await?)
    }

    /// Return the number of bytes queued for sending.
    pub(crate) fn bytes_queued(&self) -> usize {
        self.buf.remaining()
    }

    /// Initiate sending all outgoing messages. The actual send will
    /// be done on a background task. If there is sufficient room in
    /// the buffer flush will complete immediately.
    pub(crate) async fn flush(&mut self) -> Result<()> {
        loop {
            if self.try_flush()? {
                break Ok(());
            } else {
                future::poll_fn(|cx| self.to_flush.poll_ready(cx)).await?
            }
        }
    }

    /// Flush as much data as possible now, but don't wait if the
    /// channel is full. Return true if all data was flushed,
    /// otherwise false.
    pub(crate) fn try_flush(&mut self) -> Result<bool> {
        while self.buf.has_remaining() {
            let boundry = self.boundries.first().copied().unwrap_or(self.buf.len());
            let chunk = self.buf.split_to(boundry);
            match self.to_flush.try_send(ToFlush::Flush(chunk)) {
                Ok(()) => {
                    if self.boundries.len() > 0 {
                        self.boundries.remove(0);
                    }
                }
                Err(e) if e.is_full() => match e.into_inner() {
                    ToFlush::Flush(mut chunk) => {
                        chunk.unsplit(self.buf.split());
                        self.buf = chunk;
                        return Ok(false);
                    }
                    ToFlush::SetCtx(_) => unreachable!(),
                },
                Err(_) => bail!("can't flush to closed connection"),
            }
        }
        Ok(true)
    }

    /// Initiate sending all outgoing messages and wait `timeout` for
    /// the operation to complete. If `timeout` expires some data may
    /// have been sent.
    pub(crate) async fn flush_timeout(&mut self, timeout: Duration) -> Result<()> {
        Ok(time::timeout(timeout, self.flush()).await??)
    }
}

fn read_task<C: K5Ctx + Debug + Send + Sync + 'static, S: AsyncRead + Send + 'static>(
    stop: oneshot::Receiver<()>,
    mut soc: ReadHalf<S>,
    mut set_ctx: oneshot::Receiver<K5CtxWrap<C>>,
) -> Receiver<BytesMut> {
    let (mut tx, rx) = mpsc::channel(3);
    task::spawn(async move {
        let mut stop = stop.fuse();
        let mut ctx: Option<K5CtxWrap<C>> = None;
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
                    if ctx.is_some() {
                        break 'main Err(anyhow!("encryption is required"));
                    }
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
                    let decrypted = try_cf!(break, 'main, task::block_in_place(|| {
                        ctx.lock().unwrap_iov(len, &mut buf)
                    }));
                    try_cf!(break, 'main, tx.send(decrypted).await);
                }
            }
            if buf.remaining_mut() < mem::size_of::<u32>() {
                buf.reserve(buf.capacity());
            }
            select_biased! {
                _ = stop => break Ok(()),
                i = soc.read_buf(&mut buf).fuse() => {
                    if try_cf!(i) == 0 {
                        break Err(anyhow!("EOF"));
                    }
                }
            }
        };
        log::info!("read task shutting down {:?}", res);
    });
    rx
}

pub(crate) struct ReadChannel<C: K5Ctx + Debug + Send + Sync + 'static> {
    buf: BytesMut,
    _stop: oneshot::Sender<()>,
    set_ctx: Option<oneshot::Sender<K5CtxWrap<C>>>,
    incoming: stream::Fuse<Receiver<BytesMut>>,
}

impl<C: K5Ctx + Debug + Send + Sync + 'static> ReadChannel<C> {
    pub(crate) fn new<S: AsyncRead + Send + 'static>(socket: ReadHalf<S>) -> ReadChannel<C> {
        let (set_ctx, read_ctx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        ReadChannel {
            buf: BytesMut::new(),
            _stop: stop_tx,
            set_ctx: Some(set_ctx),
            incoming: read_task(stop_rx, socket, read_ctx).fuse(),
        }
    }

    /// Read context may only be set once. This method will panic if
    /// you try to set it twice.
    pub(crate) fn set_ctx(&mut self, ctx: K5CtxWrap<C>) {
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

    pub(crate) async fn receive_batch_fn<T, F>(&mut self, mut f: F) -> Result<()>
    where
        T: Pack + Debug,
        F: FnMut(T),
    {
        f(self.receive().await?);
        while self.buf.has_remaining() {
            f(T::decode(&mut self.buf)?);
        }
        Ok(())
    }
}

pub(crate) struct Channel<C: K5Ctx + Debug + Send + Sync + 'static> {
    read: ReadChannel<C>,
    write: WriteChannel<C>,
}

impl<C: K5Ctx + Debug + Send + Sync + 'static> Channel<C> {
    pub(crate) fn new<S: AsyncRead + AsyncWrite + Send + 'static>(socket: S) -> Channel<C> {
        let (rh, wh) = io::split(socket);
        Channel { read: ReadChannel::new(rh), write: WriteChannel::new(wh) }
    }

    pub(crate) async fn set_ctx(&mut self, ctx: K5CtxWrap<C>) {
        self.read.set_ctx(ctx.clone());
        let _ = self.write.set_ctx(ctx).await;
    }

    pub(crate) fn split(self) -> (ReadChannel<C>, WriteChannel<C>) {
        (self.read, self.write)
    }

    pub(crate) fn queue_send<T: Pack>(&mut self, msg: &T) -> Result<(), Error> {
        self.write.queue_send(msg)
    }

    pub(crate) fn clear(&mut self) {
        self.write.clear();
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
    pub(crate) fn try_flush(&mut self) -> Result<bool> {
        self.write.try_flush()
    }

    pub(crate) async fn flush_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        self.write.flush_timeout(timeout).await
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

    pub(crate) async fn receive_batch_fn<T, F>(&mut self, f: F) -> Result<()>
    where
        T: Pack + Debug,
        F: FnMut(T),
    {
        self.read.receive_batch_fn(f).await
    }
}
