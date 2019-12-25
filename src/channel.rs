use bytes::{BytesMut, Bytes, Buf, BufMut};
use async_std::{
    prelude::*,
    net::TcpStream,
};
use std::{
    mem,
    cmp::min,
    result::Result,
    io::{IoSlice, Error, ErrorKind},
    iter::FromIterator,
};
use smallvec::SmallVec;


const MSGS: usize = 64;
const READ_BUF: usize = 4096;

fn advance(bufs: &mut SmallVec<[Bytes; MSGS * 2]>, mut len: usize) {
    let mut i = 0;
    while len > 0 && i < bufs.len() {
        let b = &mut bufs[i];
        let n = min(b.remaining(), len);
        b.advance(n);
        if b.remaining() == 0 { i += 1; }
        len -= n;
    }
    bufs.retain(|b| b.remaining() > 0);
}

/// RawChannel sends and receives u32 length prefixed messages, which
/// are otherwise just raw bytes.
pub(crate) struct RawChannel {
    socket: TcpStream,
    outgoing: SmallVec<[Bytes; MSGS * 2]>,
    headers: BytesMut,
    incoming: BytesMut,
}

impl RawChannel {
    pub(crate) fn new(socket: TcpStream) -> RawChannel {
        RawChannel {
            socket,
            outgoing: SmallVec::new(),
            headers: BytesMut::with_capacity(mem::size_of::<u32>() * MSGS),
            incoming: BytesMut::with_capacity(READ_BUF),
        }
    }

    /// Queue an outgoing message. This ONLY queues the message, use
    /// flush to initiate sending. It will fail if the message is
    /// larger then `u32::max_value()`.
    pub(crate) fn queue_send(&mut self, msg: Bytes) -> Result<(), Error> {
        if msg.len() > u32::max_value() as usize {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("message too large {} > {}", msg.len(), u32::max_value())
            ));
        }
        if self.headers.remaining_mut() < mem::size_of::<u32>() {
            self.headers.reserve(self.headers.capacity());
        }
        self.headers.put_u32(msg.len() as u32);
        self.outgoing.push(self.headers.split().freeze());
        Ok(self.outgoing.push(msg))
    }

    /// Initiate sending all outgoing messages and wait for the
    /// process to finish.
    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        match self.outgoing.len() {
            0 => Ok(()),
            1 => {
                let v = self.outgoing.pop().unwrap();
                self.socket.write_all(&*v).await
            },
            _ => loop {
                let n = {
                    let bufs = self.outgoing.iter().map(|b| IoSlice::new(b.as_ref()));
                    let iovecs = SmallVec::<[IoSlice; MSGS * 2]>::from_iter(bufs);
                    self.socket.write_vectored(iovecs.as_slice()).await?
                };
                advance(&mut self.outgoing, n);
                if self.outgoing.len() == 0 { break Ok(()); }
            }
        }
    }

    async fn fill_buffer(&mut self) -> Result<(), Error> {
        if self.incoming.remaining_mut() < READ_BUF {
            self.incoming.reserve(self.incoming.capacity());
        }
        let n = {
            // This is safe because MaybeUninit has #repr(transparent)
            // and I am not going to read the uninitialized bytes, I
            // am only going pass the buffer to the operating system,
            // which will write into it. Thus when `read` has
            // completed the bytes will be safely initialized. I will
            // then advance the length by the number of bytes `read`
            // initialized.
            let buf = unsafe {
                mem::transmute::<&mut [mem::MaybeUninit<u8>], &mut [u8]>(
                    self.incoming.bytes_mut()
                )
            };
            self.socket.read(buf).await?
        };
        // This is safe because we are advancing by the number of
        // bytes the OS read into the buffer. Those bytes are now
        // properly initialized, and can be safely read. This is
        // *slightly* silly, because u8s are just numbers, and are
        // thus always "properly initialized". The other side of the
        // connection could send us absolutely anything and it would
        // still be valid u8s, but we're certifying that these bytes
        // came from the socket and not from random junk sitting in
        // memory.
        unsafe { self.incoming.advance_mut(n); }
        Ok(())
    }

    /// Receive one message, potentially waiting for one to arrive if
    /// none are presently in the buffer.
    pub(crate) async fn receive(&mut self) -> Result<Bytes, Error> {
        loop {
            if self.incoming.remaining() >= mem::size_of::<u32>() {
                let len = self.incoming.get_u32() as usize;
                break loop {
                    if self.incoming.remaining() >= len {
                        break Ok(self.incoming.split_to(len).freeze());
                    } else {
                        self.fill_buffer().await?;
                    }
                };
            } else {
                self.fill_buffer().await?;
            }
        }
    }
}
