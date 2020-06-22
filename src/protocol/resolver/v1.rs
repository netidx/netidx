use crate::{
    chars::Chars,
    pack::{Pack, PackError, Z64},
    path::Path,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fxhash::FxBuildHasher;
use std::{collections::HashMap, net::SocketAddr, result};

type Error = PackError;
pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CtxId(u64);

impl CtxId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        CtxId(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    #[cfg(test)]
    pub(crate) fn mk(i: u64) -> CtxId {
        CtxId(i)
    }
}

impl Pack for CtxId {
    fn len(&self) -> usize {
        <u64 as Pack>::len(&self.0)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        <u64 as Pack>::encode(&self.0, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        Ok(CtxId(<u64 as Pack>::decode(buf)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientAuthRead {
    Anonymous,
    Reuse(CtxId),
    Initiate(Bytes),
}

impl Pack for ClientAuthRead {
    fn len(&self) -> usize {
        1 + match self {
            ClientAuthRead::Anonymous => 0,
            ClientAuthRead::Reuse(ref i) => Pack::len(i),
            ClientAuthRead::Initiate(ref b) => Pack::len(b),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            ClientAuthRead::Anonymous => Ok(buf.put_u8(0)),
            ClientAuthRead::Reuse(ref id) => {
                buf.put_u8(1);
                Ok(<CtxId as Pack>::encode(id, buf)?)
            }
            ClientAuthRead::Initiate(ref tok) => {
                buf.put_u8(2);
                Ok(<Bytes as Pack>::encode(tok, buf)?)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ClientAuthRead::Anonymous),
            1 => Ok(ClientAuthRead::Reuse(<CtxId as Pack>::decode(buf)?)),
            2 => Ok(ClientAuthRead::Initiate(<Bytes as Pack>::decode(buf)?)),
            _ => return Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientAuthWrite {
    Anonymous,
    Reuse,
    Initiate { spn: Option<Chars>, token: Bytes },
}

impl Pack for ClientAuthWrite {
    fn len(&self) -> usize {
        1 + match self {
            ClientAuthWrite::Anonymous => 0,
            ClientAuthWrite::Reuse => 0,
            ClientAuthWrite::Initiate { spn, token } => {
                <Option<Chars> as Pack>::len(spn) + <Bytes as Pack>::len(token)
            }
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            ClientAuthWrite::Anonymous => Ok(buf.put_u8(0)),
            ClientAuthWrite::Reuse => Ok(buf.put_u8(1)),
            ClientAuthWrite::Initiate { spn, token } => {
                buf.put_u8(2);
                <Option<Chars> as Pack>::encode(spn, buf)?;
                <Bytes as Pack>::encode(token, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ClientAuthWrite::Anonymous),
            1 => Ok(ClientAuthWrite::Reuse),
            2 => {
                let spn = <Option<Chars> as Pack>::decode(buf)?;
                let token = <Bytes as Pack>::decode(buf)?;
                Ok(ClientAuthWrite::Initiate { spn, token })
            }
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientHelloWrite {
    pub write_addr: SocketAddr,
    pub auth: ClientAuthWrite,
}

impl Pack for ClientHelloWrite {
    fn len(&self) -> usize {
        <SocketAddr as Pack>::len(&self.write_addr) + ClientAuthWrite::len(&self.auth)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        <SocketAddr as Pack>::encode(&self.write_addr, buf)?;
        ClientAuthWrite::encode(&self.auth, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        let write_addr = <SocketAddr as Pack>::decode(buf)?;
        let auth = ClientAuthWrite::decode(buf)?;
        Ok(ClientHelloWrite { write_addr, auth })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientHello {
    /// Instruct the resolver server that this connection will not
    /// publish paths.
    ReadOnly(ClientAuthRead),
    /// Instruct the resolver server that this connection will
    /// only publish paths. All published paths will use the
    /// specified address `write_addr`, and the publisher must
    /// send a heartbeat at least every `ttl` seconds or the
    /// resolver server will purge all paths published by
    /// `write_addr`.
    WriteOnly(ClientHelloWrite),
}

impl Pack for ClientHello {
    fn len(&self) -> usize {
        1 + match self {
            ClientHello::ReadOnly(r) => ClientAuthRead::len(r),
            ClientHello::WriteOnly(r) => ClientHelloWrite::len(r),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            ClientHello::ReadOnly(r) => {
                buf.put_u8(0);
                <ClientAuthRead as Pack>::encode(r, buf)
            }
            ClientHello::WriteOnly(r) => {
                buf.put_u8(1);
                <ClientHelloWrite as Pack>::encode(r, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ClientHello::ReadOnly(ClientAuthRead::decode(buf)?)),
            1 => Ok(ClientHello::WriteOnly(ClientHelloWrite::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerHelloRead {
    Anonymous,
    Reused,
    Accepted(Bytes, CtxId),
}

impl Pack for ServerHelloRead {
    fn len(&self) -> usize {
        1 + match self {
            ServerHelloRead::Anonymous => 0,
            ServerHelloRead::Reused => 0,
            ServerHelloRead::Accepted(tok, id) => {
                <Bytes as Pack>::len(tok) + CtxId::len(id)
            }
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            ServerHelloRead::Anonymous => Ok(buf.put_u8(0)),
            ServerHelloRead::Reused => Ok(buf.put_u8(1)),
            ServerHelloRead::Accepted(tok, id) => {
                buf.put_u8(2);
                <Bytes as Pack>::encode(tok, buf)?;
                CtxId::encode(id, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ServerHelloRead::Anonymous),
            1 => Ok(ServerHelloRead::Reused),
            2 => {
                let tok = <Bytes as Pack>::decode(buf)?;
                let id = CtxId::decode(buf)?;
                Ok(ServerHelloRead::Accepted(tok, id))
            }
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerAuthWrite {
    Anonymous,
    Reused,
    Accepted(Bytes),
}

impl Pack for ServerAuthWrite {
    fn len(&self) -> usize {
        1 + match self {
            ServerAuthWrite::Anonymous => 0,
            ServerAuthWrite::Reused => 0,
            ServerAuthWrite::Accepted(b) => <Bytes as Pack>::len(b),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            ServerAuthWrite::Anonymous => Ok(buf.put_u8(0)),
            ServerAuthWrite::Reused => Ok(buf.put_u8(1)),
            ServerAuthWrite::Accepted(b) => {
                buf.put_u8(2);
                <Bytes as Pack>::encode(b, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ServerAuthWrite::Anonymous),
            1 => Ok(ServerAuthWrite::Reused),
            2 => {
                let tok = <Bytes as Pack>::decode(buf)?;
                Ok(ServerAuthWrite::Accepted(tok))
            }
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerHelloWrite {
    pub ttl: u64,
    pub ttl_expired: bool,
    pub auth: ServerAuthWrite,
    pub resolver_id: SocketAddr,
}

impl Pack for ServerHelloWrite {
    fn len(&self) -> usize {
        <u64 as Pack>::len(&self.ttl)
            + <bool as Pack>::len(&self.ttl_expired)
            + ServerAuthWrite::len(&self.auth)
            + <SocketAddr as Pack>::len(&self.resolver_id)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        <u64 as Pack>::encode(&self.ttl, buf)?;
        <bool as Pack>::encode(&self.ttl_expired, buf)?;
        ServerAuthWrite::encode(&self.auth, buf)?;
        <SocketAddr as Pack>::encode(&self.resolver_id, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        let ttl = <u64 as Pack>::decode(buf)?;
        let ttl_expired = <bool as Pack>::decode(buf)?;
        let auth = ServerAuthWrite::decode(buf)?;
        let resolver_id = <SocketAddr as Pack>::decode(buf)?;
        Ok(ServerHelloWrite { ttl, ttl_expired, auth, resolver_id })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Secret(pub u128);

impl Pack for Secret {
    fn len(&self) -> usize {
        <u128 as Pack>::len(&self.0)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        <u128 as Pack>::encode(&self.0, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        Ok(Secret(<u128 as Pack>::decode(buf)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyForOwnershipCheck;

impl Pack for ReadyForOwnershipCheck {
    fn len(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        Ok(buf.put_u8(0))
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ReadyForOwnershipCheck),
            _ => Err(PackError::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToRead {
    /// Resolve path to addresses/ports
    Resolve(Path),
    /// List the paths published under the specified root path
    List(Path),
    /// Describe the table rooted at the specified path
    Table(Path),
}

impl Pack for ToRead {
    fn len(&self) -> usize {
        1 + match self {
            ToRead::Resolve(path) | ToRead::List(path) | ToRead::Table(path) => {
                <Path as Pack>::len(path)
            }
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            ToRead::Resolve(path) => {
                buf.put_u8(0);
                <Path as Pack>::encode(path, buf)
            }
            ToRead::List(path) => {
                buf.put_u8(1);
                <Path as Pack>::encode(path, buf)
            }
            ToRead::Table(path) => {
                buf.put_u8(2);
                <Path as Pack>::encode(path, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => {
                let path = <Path as Pack>::decode(buf)?;
                Ok(ToRead::Resolve(path))
            }
            1 => {
                let path = <Path as Pack>::decode(buf)?;
                Ok(ToRead::List(path))
            }
            2 => {
                let path = <Path as Pack>::decode(buf)?;
                Ok(ToRead::Table(path))
            }
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub krb5_spns: HashMap<SocketAddr, Chars, FxBuildHasher>,
    pub resolver: SocketAddr,
    pub addrs: Vec<(SocketAddr, Bytes)>,
    pub timestamp: u64,
    pub permissions: u32,
}

impl Pack for Resolved {
    fn len(&self) -> usize {
        <HashMap<SocketAddr, Chars, FxBuildHasher> as Pack>::len(&self.krb5_spns)
            + <SocketAddr as Pack>::len(&self.resolver)
            + <Vec<(SocketAddr, Bytes)> as Pack>::len(&self.addrs)
            + <u64 as Pack>::len(&self.timestamp)
            + <u32 as Pack>::len(&self.permissions)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        <HashMap<SocketAddr, Chars, FxBuildHasher> as Pack>::encode(
            &self.krb5_spns,
            buf,
        )?;
        <SocketAddr as Pack>::encode(&self.resolver, buf)?;
        <Vec<(SocketAddr, Bytes)> as Pack>::encode(&self.addrs, buf)?;
        <u64 as Pack>::encode(&self.timestamp, buf)?;
        <u32 as Pack>::encode(&self.permissions, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        let krb5_spns = <HashMap<SocketAddr, Chars, FxBuildHasher> as Pack>::decode(buf)?;
        let resolver = <SocketAddr as Pack>::decode(buf)?;
        let addrs = <Vec<(SocketAddr, Bytes)> as Pack>::decode(buf)?;
        let timestamp = <u64 as Pack>::decode(buf)?;
        let permissions = <u32 as Pack>::decode(buf)?;
        Ok(Resolved { krb5_spns, resolver, addrs, timestamp, permissions })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Referral {
    pub path: Path,
    pub ttl: u64,
    pub addrs: Vec<SocketAddr>,
    pub krb5_spns: HashMap<SocketAddr, Chars, FxBuildHasher>,
}

impl Pack for Referral {
    fn len(&self) -> usize {
        <Path as Pack>::len(&self.path)
            + <u64 as Pack>::len(&self.ttl)
            + <Vec<SocketAddr> as Pack>::len(&self.addrs)
            + <HashMap<SocketAddr, Chars, FxBuildHasher> as Pack>::len(&self.krb5_spns)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        <Path as Pack>::encode(&self.path, buf)?;
        <u64 as Pack>::encode(&self.ttl, buf)?;
        <Vec<SocketAddr> as Pack>::encode(&self.addrs, buf)?;
        <HashMap<SocketAddr, Chars, FxBuildHasher> as Pack>::encode(&self.krb5_spns, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        let path = <Path as Pack>::decode(buf)?;
        let ttl = <u64 as Pack>::decode(buf)?;
        let addrs = <Vec<SocketAddr> as Pack>::decode(buf)?;
        let krb5_spns = <HashMap<SocketAddr, Chars, FxBuildHasher> as Pack>::decode(buf)?;
        Ok(Referral { path, ttl, addrs, krb5_spns })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromRead {
    Resolved(Resolved),
    List(Vec<Path>),
    Table { rows: Vec<Path>, cols: HashMap<Path, Z64> },
    Referral(Referral),
    Denied,
    Error(Chars),
}

impl Pack for FromRead {
    fn len(&self) -> usize {
        1 + match self {
            FromRead::Resolved(a) => Resolved::len(a),
            FromRead::List(l) => <Vec<Path> as Pack>::len(l),
            FromRead::Table { rows, cols } => {
                <Vec<Path> as Pack>::len(rows) + <HashMap<Path, Z64> as Pack>::len(cols)
            }
            FromRead::Referral(r) => <Referral as Pack>::len(r),
            FromRead::Denied => 0,
            FromRead::Error(e) => <Chars as Pack>::len(e),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            FromRead::Resolved(a) => {
                buf.put_u8(0);
                Resolved::encode(a, buf)
            }
            FromRead::List(l) => {
                buf.put_u8(1);
                <Vec<Path> as Pack>::encode(l, buf)
            }
            FromRead::Table {rows, cols} => {
                buf.put_u8(2);
                <Vec<Path> as Pack>::encode(rows, buf)?;
                <HashMap<Path, Z64>>::encode(cols, buf)
            }
            FromRead::Referral(r) => {
                buf.put_u8(3);
                <Referral as Pack>::encode(r, buf)
            }
            FromRead::Denied => Ok(buf.put_u8(4)),
            FromRead::Error(e) => {
                buf.put_u8(5);
                <Chars as Pack>::encode(e, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(FromRead::Resolved(Resolved::decode(buf)?)),
            1 => Ok(FromRead::List(<Vec<Path> as Pack>::decode(buf)?)),
            2 => {
                let rows = <Vec<Path> as Pack>::decode(buf)?;
                let cols = <HashMap<Path, Z64>>::decode(buf)?;
                Ok(FromRead::Table {rows, cols})
            }
            3 => Ok(FromRead::Referral(<Referral as Pack>::decode(buf)?)),
            4 => Ok(FromRead::Denied),
            5 => Ok(FromRead::Error(<Chars as Pack>::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToWrite {
    /// Publish the path
    Publish(Path),
    /// Add a default publisher to path
    PublishDefault(Path),
    /// Stop publishing the path
    Unpublish(Path),
    /// Clear all values you've published
    Clear,
    /// Tell the resolver that we are still alive
    Heartbeat,
}

impl Pack for ToWrite {
    fn len(&self) -> usize {
        1 + match self {
            ToWrite::Publish(p) => <Path as Pack>::len(p),
            ToWrite::PublishDefault(p) => <Path as Pack>::len(p),
            ToWrite::Unpublish(p) => <Path as Pack>::len(p),
            ToWrite::Clear => 0,
            ToWrite::Heartbeat => 0,
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            ToWrite::Publish(p) => {
                buf.put_u8(0);
                <Path as Pack>::encode(p, buf)
            }
            ToWrite::PublishDefault(p) => {
                buf.put_u8(1);
                <Path as Pack>::encode(p, buf)
            }
            ToWrite::Unpublish(p) => {
                buf.put_u8(2);
                <Path as Pack>::encode(p, buf)
            }
            ToWrite::Clear => Ok(buf.put_u8(3)),
            ToWrite::Heartbeat => Ok(buf.put_u8(4)),
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ToWrite::Publish(<Path as Pack>::decode(buf)?)),
            1 => Ok(ToWrite::PublishDefault(<Path as Pack>::decode(buf)?)),
            2 => Ok(ToWrite::Unpublish(<Path as Pack>::decode(buf)?)),
            3 => Ok(ToWrite::Clear),
            4 => Ok(ToWrite::Heartbeat),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromWrite {
    Published,
    Unpublished,
    Referral(Referral),
    Denied,
    Error(Chars),
}

impl Pack for FromWrite {
    fn len(&self) -> usize {
        1 + match self {
            FromWrite::Published => 0,
            FromWrite::Unpublished => 0,
            FromWrite::Referral(r) => <Referral as Pack>::len(r),
            FromWrite::Denied => 0,
            FromWrite::Error(c) => <Chars as Pack>::len(c),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            FromWrite::Published => Ok(buf.put_u8(0)),
            FromWrite::Unpublished => Ok(buf.put_u8(1)),
            FromWrite::Referral(r) => {
                buf.put_u8(2);
                <Referral as Pack>::encode(r, buf)
            }
            FromWrite::Denied => Ok(buf.put_u8(3)),
            FromWrite::Error(c) => {
                buf.put_u8(4);
                <Chars as Pack>::encode(c, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(FromWrite::Published),
            1 => Ok(FromWrite::Unpublished),
            2 => Ok(FromWrite::Referral(<Referral as Pack>::decode(buf)?)),
            3 => Ok(FromWrite::Denied),
            4 => Ok(FromWrite::Error(<Chars as Pack>::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}
