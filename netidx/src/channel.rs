use crate::{pack::Pack, utils};
use anyhow::{anyhow, Error, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{buf::UninitSlice, Buf, BufMut, BytesMut};
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
use log::{info, trace};
use poolshark::{Pool, Pooled};

use parking_lot::Mutex;
use std::{
    clone::Clone,
    fmt::Debug,
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut},
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf},
    task, time,
};

const BUF: usize = 8388608;
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

/// Send a single unencrypted message directly to the specified
/// socket. This is intended to be used to do some initialization
/// before the proper channel can be created.
pub(crate) async fn write_raw<T: Pack, S: AsyncWrite + Unpin>(
    socket: &mut S,
    msg: &T,
) -> Result<()> {
    let len = msg.encoded_len();
    if len > MAX_BATCH as usize {
        bail!("message length {} exceeds max size {}", len, MAX_BATCH)
    }
    let buf = utils::pack(msg)?;
    let len = buf.remaining() as u32;
    let lenb = len.to_be_bytes();
    let mut buf = Buf::chain(&lenb[..], buf);
    while buf.has_remaining() {
        socket.write_buf(&mut buf).await?;
    }
    socket.flush().await?;
    Ok(())
}

/// Read a single, small, unencrypted message from the specified
/// socket. This is intended to be used to do some initialization
/// before the proper channel can be created.
pub(crate) async fn read_raw<T: Pack, S: AsyncRead + Unpin, const MAX: usize>(
    socket: &mut S,
) -> Result<T> {
    let mut buf = [0u8; MAX];
    socket.read_exact(&mut buf[0..4]).await?;
    let len = BigEndian::read_u32(&buf[0..4]);
    if len > LEN_MASK {
        bail!("message is encrypted")
    }
    let len = len as usize;
    if len > MAX {
        bail!("message is too large")
    }
    socket.read_exact(&mut buf[0..len]).await?;
    let mut buf = &buf[0..len];
    let res = T::decode(&mut buf)?;
    if buf.has_remaining() {
        bail!("batch contained more than one message")
    }
    Ok(res)
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
        let i = buf.remaining();
        soc.write_buf(&mut buf).await?;
        trace!("flush_buf wrote {}", i - buf.remaining());
    }
    soc.flush().await?;
    Ok(())
}

fn flush_task<
    C: K5Ctx + Debug + Send + Sync + 'static,
    S: AsyncWrite + Send + 'static,
>(
    ctx: Option<K5CtxWrap<C>>,
    mut soc: WriteHalf<S>,
) -> Sender<BytesMut> {
    let (tx, mut rx): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(3);
    task::spawn(async move {
        let res = loop {
            match rx.next().await {
                None => break Ok(()),
                Some(data) => match ctx {
                    None => try_cf!(flush_buf(&mut soc, data, false).await),
                    Some(ref ctx) => {
                        let msg = try_cf!(ctx.lock().wrap_iov(true, data));
                        try_cf!(flush_buf(&mut soc, msg, true).await);
                    }
                },
            }
        };
        info!("flush task shutting down {:?}", res)
    });
    tx
}

pub(crate) struct WriteChannel {
    to_flush: Sender<BytesMut>,
    buf: BytesMut,
    boundries: Vec<usize>,
}

impl WriteChannel {
    pub(crate) fn new<
        C: K5Ctx + Debug + Send + Sync + 'static,
        S: AsyncWrite + Send + 'static,
    >(
        ctx: Option<K5CtxWrap<C>>,
        socket: WriteHalf<S>,
    ) -> WriteChannel {
        WriteChannel {
            to_flush: flush_task(ctx, socket),
            buf: BytesMut::with_capacity(BUF),
            boundries: Vec::new(),
        }
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
        let mut pushed = false;
        if (buf_len - self.boundries.last().copied().unwrap_or(0)) + len > MAX_BATCH {
            let prev_len: usize = self.boundries.iter().sum();
            self.boundries.push(buf_len - prev_len);
            pushed = true;
        }
        match msg.encode(&mut self.buf) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.buf.resize(buf_len, 0x0);
                if pushed {
                    self.boundries.pop();
                }
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
            match self.to_flush.try_send(chunk) {
                Ok(()) => {
                    if self.boundries.len() > 0 {
                        self.boundries.remove(0);
                    }
                }
                Err(e) if e.is_full() => {
                    let mut chunk = e.into_inner();
                    chunk.unsplit(self.buf.split());
                    self.buf = chunk;
                    return Ok(false);
                }
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

struct PBuf {
    data: Pooled<Vec<u8>>,
    pos: usize,
}

impl PBuf {
    // reserve additional mutable capacity
    fn reserve_mut(&mut self, n: usize) {
        let rem = self.remaining_mut();
        self.data.reserve(rem + n)
    }

    fn extend_from_slice(&mut self, d: &[u8]) {
        if self.remaining_mut() < d.len() {
            self.reserve_mut(d.len());
        }
        self.chunk_mut()[..d.len()].copy_from_slice(d);
        unsafe { self.advance_mut(d.len()) }
    }

    fn capacity(&self) -> usize {
        self.data.capacity()
    }

    fn resize(&mut self, n: usize, t: u8) {
        self.data.resize(self.pos + n, t)
    }

    // return 0..len self becomes len..remaining
    fn split_to(&mut self, len: usize) -> Self {
        if self.remaining() < len {
            panic!("not enough data in the buffer to split to {}", len)
        }
        if self.remaining() == len {
            mem::take(self)
        } else if self.remaining() - len > len {
            let mut new = Self::default();
            new.extend_from_slice(&self[..len]);
            self.advance(len);
            let pos = self.pos;
            let rem = self.remaining();
            if pos > rem << 1 {
                self.data.copy_within(pos..pos + rem, 0);
                self.pos = 0;
                self.resize(rem, 0);
            }
            new
        } else {
            let mut new = Self::default();
            new.extend_from_slice(&self[len..]);
            self.resize(len, 0);
            mem::replace(self, new)
        }
    }
}

impl Default for PBuf {
    fn default() -> Self {
        static POOL: LazyLock<Pool<Vec<u8>>> = LazyLock::new(|| Pool::new(10, BUF << 2));

        Self { data: POOL.take(), pos: 0 }
    }
}

impl Buf for PBuf {
    fn advance(&mut self, cnt: usize) {
        self.pos += cnt
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.pos..]
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }
}

unsafe impl BufMut for PBuf {
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let new = self.data.len() + cnt;
        assert!(new <= self.data.capacity());
        unsafe { self.data.set_len(new) }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        let cap = self.data.spare_capacity_mut();
        let len = cap.len();
        let ptr = cap.as_mut_ptr() as *mut u8;
        unsafe { UninitSlice::from_raw_parts_mut(ptr, len) }
    }

    fn remaining_mut(&self) -> usize {
        self.data.capacity() - self.data.len()
    }
}

impl Deref for PBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data[self.pos..]
    }
}

impl DerefMut for PBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let pos = self.pos;
        &mut self.data[pos..]
    }
}

fn read_task<C: K5Ctx + Debug + Send + Sync + 'static, S: AsyncRead + Send + 'static>(
    stop: oneshot::Receiver<()>,
    mut soc: ReadHalf<S>,
    ctx: Option<K5CtxWrap<C>>,
) -> Receiver<PBuf> {
    trace!("starting read task");
    let (mut tx, rx) = mpsc::channel(3);
    task::spawn(async move {
        let mut stop = stop.fuse();
        let mut buf = PBuf::default();
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
                    trace!(
                        "read_task: {} is less than batch len {}, reading more",
                        buf.remaining() - mem::size_of::<u32>(),
                        len
                    );
                    break;
                } else if !encrypted {
                    if ctx.is_some() {
                        break 'main Err(anyhow!("encryption is required"));
                    }
                    buf.advance(mem::size_of::<u32>());
                    try_cf!(break, 'main, tx.send(buf.split_to(len)).await);
                } else {
                    let ctx = match ctx {
                        Some(ref ctx) => ctx,
                        None => break 'main Err(anyhow!("encryption is not supported")),
                    };
                    buf.advance(mem::size_of::<u32>());
                    let decrypted = try_cf!(break, 'main, ctx.lock().unwrap(&buf[..len]));
                    buf.advance(len);
                    buf.extend_from_slice(&*decrypted);
                    try_cf!(break, 'main, tx.send(mem::take(&mut buf)).await);
                }
            }
            if buf.remaining_mut() < BUF {
                buf.reserve_mut(std::cmp::max(buf.capacity(), BUF));
            }
            let cap = unsafe {
                mem::transmute::<&mut [MaybeUninit<u8>], &mut [u8]>(
                    buf.chunk_mut().as_uninit_slice_mut(),
                )
            };
            trace!("reading more from socket");
            #[rustfmt::skip]
            select_biased! {
                _ = stop => break Ok(()),
                i = soc.read(cap).fuse() => {
		    trace!("read {:?} from the socket", i);
                    if try_cf!(i) == 0 {
                        break Err(anyhow!("EOF"));
                    }
		    unsafe { buf.advance_mut(i.unwrap()); }
                }
            }
        };
        log::info!("read task shutting down {:?}", res);
    });
    rx
}

pub(crate) struct ReadChannel {
    buf: PBuf,
    _stop: oneshot::Sender<()>,
    incoming: stream::Fuse<Receiver<PBuf>>,
}

impl ReadChannel {
    pub(crate) fn new<
        C: K5Ctx + Debug + Send + Sync + 'static,
        S: AsyncRead + Send + 'static,
    >(
        k5ctx: Option<K5CtxWrap<C>>,
        socket: ReadHalf<S>,
    ) -> ReadChannel {
        let (stop_tx, stop_rx) = oneshot::channel();
        ReadChannel {
            buf: PBuf::default(),
            _stop: stop_tx,
            incoming: read_task(stop_rx, socket, k5ctx).fuse(),
        }
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
        let res = T::decode(&mut self.buf);
        trace!("receive decoded {:?}", res);
        Ok(res?)
    }

    pub(crate) async fn receive_batch<T: Pack + Debug>(
        &mut self,
        batch: &mut Vec<T>,
    ) -> Result<()> {
        batch.push(self.receive().await?);
        let mut n = self.buf.remaining();
        while self.buf.has_remaining() {
            let t = T::decode(&mut self.buf);
            trace!("receive_batch remains {} decoded {:?}", self.buf.remaining(), t);
            batch.push(t?);
            if n - self.buf.remaining() > 8 * 1024 * 1024 {
                n = self.buf.remaining();
                task::yield_now().await
            }
        }
        Ok::<_, anyhow::Error>(())
    }

    pub(crate) async fn receive_batch_fn<T, F>(&mut self, mut f: F) -> Result<()>
    where
        T: Pack + Debug,
        F: FnMut(T),
    {
        f(self.receive().await?);
        let mut n = self.buf.remaining();
        while self.buf.has_remaining() {
            let t = T::decode(&mut self.buf);
            trace!("receive_batch_fn remains {} decoded {:?}", self.buf.remaining(), t);
            f(t?);
            if n - self.buf.remaining() > 8 * 1024 * 1024 {
                n = self.buf.remaining();
                task::yield_now().await
            }
        }
        Ok::<_, anyhow::Error>(())
    }
}

pub(crate) struct Channel {
    read: ReadChannel,
    write: WriteChannel,
}

impl Channel {
    pub(crate) fn new<
        C: K5Ctx + Debug + Send + Sync + 'static,
        S: AsyncRead + AsyncWrite + Send + 'static,
    >(
        k5ctx: Option<K5CtxWrap<C>>,
        socket: S,
    ) -> Channel {
        let (rh, wh) = io::split(socket);
        Channel {
            read: ReadChannel::new(k5ctx.clone(), rh),
            write: WriteChannel::new(k5ctx, wh),
        }
    }

    pub(crate) fn split(self) -> (ReadChannel, WriteChannel) {
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
