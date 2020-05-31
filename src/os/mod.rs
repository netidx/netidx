use anyhow::Result;
use bytes::BytesMut;
use std::{ops::Deref, time::Duration};

pub(crate) trait Krb5Ctx {
    type Buf: Deref<Target = [u8]> + Send + Sync;

    fn step(&self, token: Option<&[u8]>) -> Result<Option<Self::Buf>>;
    fn wrap(&self, encrypt: bool, msg: &[u8]) -> Result<Self::Buf>;
    fn wrap_iov(
        &self,
        header: &mut BytesMut,
        data: &mut BytesMut,
        padding: &mut BytesMut,
        trailer: &mut BytesMut,
    ) -> Result<()>;
    fn unwrap(&self, msg: &[u8]) -> Result<Self::Buf>;
    fn unwrap_iov(&self, len: usize, msg: &mut BytesMut) -> Result<BytesMut>;
    fn ttl(&self) -> Result<Duration>;
}

pub(crate) trait Krb5ServerCtx: Krb5Ctx {
    fn client(&self) -> Result<String>;
}

#[cfg(unix)]
pub(crate) mod unix;

#[cfg(unix)]
pub(crate) use unix::*;

#[cfg(windows)]
pub(crate) mod windows;

#[cfg(windows)]
pub(crate) use windows::*;
