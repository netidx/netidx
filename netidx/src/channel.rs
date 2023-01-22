use crate::{
    pack::Pack,
    protocol::publisher::{Id, ZeroCopyUpdate, ZeroCopyWrite, ZeroCopyWriteResult},
    utils,
};
use anyhow::{anyhow, Error, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{buf::Chain, Buf, BufMut, Bytes, BytesMut};
use cross_krb5::K5Ctx;
use futures::{
    channel::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    prelude::*,
    select_biased, stream,
};
use log::info;
use parking_lot::Mutex;
use std::{
    clone::Clone, collections::VecDeque, fmt::Debug, mem, ops::Deref, sync::Arc,
    time::Duration,
};
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
    Ok(())
}

/// Read a single, small, unencrypted message from the specified
/// socket. This is intended to be used to do some initialization
/// before the proper channel can be created.
pub(crate) async fn read_raw<T: Pack, S: AsyncRead + Unpin>(socket: &mut S) -> Result<T> {
    const MAX: usize = 1024;
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
        soc.write_buf(&mut buf).await?;
    }
    Ok(())
}

enum ToFlush {
    Normal(BytesMut),
    ZeroCopy(Chain<Bytes, Bytes>),
}

fn flush_task<
    C: K5Ctx + Debug + Send + Sync + 'static,
    S: AsyncWrite + Send + 'static,
>(
    ctx: Option<K5CtxWrap<C>>,
    mut soc: WriteHalf<S>,
) -> Sender<ToFlush> {
    let (tx, mut rx): (Sender<ToFlush>, Receiver<ToFlush>) = mpsc::channel(3);
    task::spawn(async move {
        let mut copybuf = BytesMut::new();
        let res = loop {
            match rx.next().await {
                None => break Ok(()),
                Some(data) => match ctx {
                    None => match data {
                        ToFlush::Normal(b) => {
                            try_cf!(flush_buf(&mut soc, b, false).await)
                        }
                        ToFlush::ZeroCopy(b) => {
                            try_cf!(flush_buf(&mut soc, b, false).await)
                        }
                    },
                    Some(ref ctx) => {
                        let data = match data {
                            ToFlush::Normal(b) => b,
                            ToFlush::ZeroCopy(b) => {
                                // *sigh* kerberos
                                copybuf.extend(b);
                                copybuf.split()
                            }
                        };
                        let msg = try_cf!(task::block_in_place(|| ctx
                            .lock()
                            .wrap_iov(true, data)));
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
    to_flush: Sender<ToFlush>,
    buf: BytesMut,
    chunks: VecDeque<ToFlush>,
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
            chunks: VecDeque::new(),
        }
    }

    /// Queue a message for sending. This only encodes the message and
    /// writes it to the buffer, you must call flush actually send it.
    pub(crate) fn queue_send<T: Pack>(&mut self, msg: &T) -> Result<()> {
        let len = msg.encoded_len();
        if len > MAX_BATCH as usize {
            return Err(anyhow!("message length {} exceeds max size {}", len, MAX_BATCH));
        }
        if self.buf.remaining() + len > MAX_BATCH {
            self.chunks.push_back(ToFlush::Normal(self.buf.split()));
        }
        if self.buf.remaining_mut() < len {
            self.buf.reserve(self.buf.capacity());
        }
        let buf_len = self.buf.remaining();
        match msg.encode(&mut self.buf) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.buf.resize(buf_len, 0x0);
                Err(Error::from(e))
            }
        }
    }

    fn queue_send_zero_copy<T: Pack, F: FnOnce(T) -> Bytes>(
        &mut self,
        hdr: T,
        get: F,
    ) -> Result<()> {
        if self.buf.remaining() > 0 {
            self.chunks.push_back(ToFlush::Normal(self.buf.split()));
        }
        if let Err(e) = hdr.encode(&mut self.buf) {
            self.buf.clear();
            bail!(e)
        }
        let update = get(hdr);
        let hdr = self.buf.split().freeze();
        if update.remaining() + hdr.remaining() > MAX_BATCH {
            bail!("message is too large")
        }
        self.chunks.push_back(ToFlush::ZeroCopy(hdr.chain(update)));
        unimplemented!()
    }

    pub(crate) fn queue_send_zero_copy_update(
        &mut self,
        id: Id,
        update: Bytes,
    ) -> Result<()> {
        self.queue_send_zero_copy(ZeroCopyUpdate { id, update }, |h| h.update)
    }

    pub(crate) fn queue_send_zero_copy_write(
        &mut self,
        id: Id,
        reply: bool,
        update: Bytes,
    ) -> Result<()> {
        self.queue_send_zero_copy(ZeroCopyWrite { id, reply, update }, |h| h.update)
    }

    pub(crate) fn queue_send_zero_copy_write_result(
        &mut self,
        id: Id,
        update: Bytes,
    ) -> Result<()> {
        self.queue_send_zero_copy(ZeroCopyWriteResult { id, update }, |h| h.update)
    }

    /// Clear unflused queued messages
    pub(crate) fn clear(&mut self) {
        self.chunks.clear();
        self.buf.clear();
    }

    /// Queue and flush one message.
    pub(crate) async fn send_one<T: Pack>(&mut self, msg: &T) -> Result<()> {
        self.queue_send(msg)?;
        Ok(self.flush().await?)
    }

    /// Return the number of bytes queued for sending.
    pub(crate) fn has_queued(&self) -> bool {
        self.buf.has_remaining() || !self.chunks.is_empty()
    }

    /// Initiate sending all outgoing messages. The actual send will
    /// be done on a background task. If there is sufficient room in
    /// the buffer flush will complete immediately.
    pub(crate) async fn flush(&mut self) -> Result<()> {
        if self.buf.remaining() > 0 {
            self.chunks.push_back(ToFlush::Normal(self.buf.split()));
        }
        while let Some(chunk) = self.chunks.pop_front() {
            if let Err(_) = self.to_flush.send(chunk).await {
                bail!("can't flush to closed connection")
            }
        }
        Ok(())
    }

    /// Flush as much data as possible now, but don't wait if the
    /// channel is full. Return true if all data was flushed,
    /// otherwise false.
    pub(crate) fn try_flush(&mut self) -> Result<bool> {
        if self.buf.remaining() > 0 {
            self.chunks.push_back(ToFlush::Normal(self.buf.split()));
        }
        while let Some(chunk) = self.chunks.pop_front() {
            match self.to_flush.try_send(chunk) {
                Ok(()) => (),
                Err(e) if e.is_full() => {
                    self.chunks.push_front(e.into_inner());
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

fn read_task<C: K5Ctx + Debug + Send + Sync + 'static, S: AsyncRead + Send + 'static>(
    stop: oneshot::Receiver<()>,
    mut soc: ReadHalf<S>,
    ctx: Option<K5CtxWrap<C>>,
) -> Receiver<BytesMut> {
    let (mut tx, rx) = mpsc::channel(3);
    task::spawn(async move {
        let mut stop = stop.fuse();
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
                        None => break 'main Err(anyhow!("encryption is not supported")),
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

pub(crate) struct ReadChannel {
    buf: BytesMut,
    _stop: oneshot::Sender<()>,
    incoming: stream::Fuse<Receiver<BytesMut>>,
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
            buf: BytesMut::new(),
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
