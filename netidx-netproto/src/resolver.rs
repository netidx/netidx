use crate::glob::GlobSet;
use bytes::{Buf, BufMut, Bytes};
use fxhash::FxBuildHasher;
use netidx_core::{
    chars::Chars,
    pack::{Pack, PackError, Z64},
    path::Path,
    pool::Pooled,
};
use std::{
    cmp::{Eq, PartialEq},
    collections::HashMap,
    hash::{Hash, Hasher},
    net::SocketAddr,
    result,
};

type Error = PackError;
pub type Result<T> = result::Result<T, Error>;

atomic_id!(CtxId);

impl Pack for CtxId {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.0)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.0, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(CtxId(Pack::decode(buf)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientAuthRead {
    Anonymous,
    Reuse(CtxId),
    Initiate(Bytes),
    Local(Bytes),
}

impl Pack for ClientAuthRead {
    fn encoded_len(&self) -> usize {
        1 + match self {
            ClientAuthRead::Anonymous => 0,
            ClientAuthRead::Reuse(ref i) => Pack::encoded_len(i),
            ClientAuthRead::Initiate(ref b) | ClientAuthRead::Local(ref b) => {
                Pack::encoded_len(b)
            }
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ClientAuthRead::Anonymous => Ok(buf.put_u8(0)),
            ClientAuthRead::Reuse(ref id) => {
                buf.put_u8(1);
                Ok(Pack::encode(id, buf)?)
            }
            ClientAuthRead::Initiate(ref tok) => {
                buf.put_u8(2);
                Ok(Pack::encode(tok, buf)?)
            }
            ClientAuthRead::Local(ref tok) => {
                buf.put_u8(3);
                Ok(Pack::encode(tok, buf)?)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ClientAuthRead::Anonymous),
            1 => Ok(ClientAuthRead::Reuse(Pack::decode(buf)?)),
            2 => Ok(ClientAuthRead::Initiate(Pack::decode(buf)?)),
            3 => Ok(ClientAuthRead::Local(Pack::decode(buf)?)),
            _ => return Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientAuthWrite {
    Anonymous,
    Reuse,
    Initiate { spn: Option<Chars>, token: Bytes },
    Local(Bytes),
}

impl Pack for ClientAuthWrite {
    fn encoded_len(&self) -> usize {
        1 + match self {
            ClientAuthWrite::Anonymous => 0,
            ClientAuthWrite::Reuse => 0,
            ClientAuthWrite::Initiate { spn, token } => {
                Pack::encoded_len(spn) + Pack::encoded_len(token)
            }
            ClientAuthWrite::Local(b) => Pack::encoded_len(b),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ClientAuthWrite::Anonymous => Ok(buf.put_u8(0)),
            ClientAuthWrite::Reuse => Ok(buf.put_u8(1)),
            ClientAuthWrite::Initiate { spn, token } => {
                buf.put_u8(2);
                Pack::encode(spn, buf)?;
                Pack::encode(token, buf)
            }
            ClientAuthWrite::Local(b) => {
                buf.put_u8(3);
                Pack::encode(b, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ClientAuthWrite::Anonymous),
            1 => Ok(ClientAuthWrite::Reuse),
            2 => {
                let spn = Pack::decode(buf)?;
                let token = Pack::decode(buf)?;
                Ok(ClientAuthWrite::Initiate { spn, token })
            }
            3 => Ok(ClientAuthWrite::Local(Pack::decode(buf)?)),
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
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.write_addr) + Pack::encoded_len(&self.auth)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.write_addr, buf)?;
        Pack::encode(&self.auth, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let write_addr = Pack::decode(buf)?;
        let auth = Pack::decode(buf)?;
        Ok(ClientHelloWrite { write_addr, auth })
    }

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<()> {
        self.write_addr = Pack::decode(buf)?;
        self.auth = Pack::decode(buf)?;
        Ok(())
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ClientHello::ReadOnly(r) => Pack::encoded_len(r),
            ClientHello::WriteOnly(r) => Pack::encoded_len(r),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ClientHello::ReadOnly(r) => {
                buf.put_u8(0);
                Pack::encode(r, buf)
            }
            ClientHello::WriteOnly(r) => {
                buf.put_u8(1);
                Pack::encode(r, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ClientHello::ReadOnly(Pack::decode(buf)?)),
            1 => Ok(ClientHello::WriteOnly(Pack::decode(buf)?)),
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ServerHelloRead::Anonymous => 0,
            ServerHelloRead::Reused => 0,
            ServerHelloRead::Accepted(tok, id) => {
                Pack::encoded_len(tok) + Pack::encoded_len(id)
            }
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ServerHelloRead::Anonymous => Ok(buf.put_u8(0)),
            ServerHelloRead::Reused => Ok(buf.put_u8(1)),
            ServerHelloRead::Accepted(tok, id) => {
                buf.put_u8(2);
                Pack::encode(tok, buf)?;
                Pack::encode(id, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ServerHelloRead::Anonymous),
            1 => Ok(ServerHelloRead::Reused),
            2 => {
                let tok = Pack::decode(buf)?;
                let id = Pack::decode(buf)?;
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ServerAuthWrite::Anonymous => 0,
            ServerAuthWrite::Reused => 0,
            ServerAuthWrite::Accepted(b) => Pack::encoded_len(b),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ServerAuthWrite::Anonymous => Ok(buf.put_u8(0)),
            ServerAuthWrite::Reused => Ok(buf.put_u8(1)),
            ServerAuthWrite::Accepted(b) => {
                buf.put_u8(2);
                Pack::encode(b, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ServerAuthWrite::Anonymous),
            1 => Ok(ServerAuthWrite::Reused),
            2 => Ok(ServerAuthWrite::Accepted(Pack::decode(buf)?)),
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
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.ttl)
            + Pack::encoded_len(&self.ttl_expired)
            + Pack::encoded_len(&self.auth)
            + Pack::encoded_len(&self.resolver_id)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.ttl, buf)?;
        Pack::encode(&self.ttl_expired, buf)?;
        Pack::encode(&self.auth, buf)?;
        Pack::encode(&self.resolver_id, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let ttl = Pack::decode(buf)?;
        let ttl_expired = Pack::decode(buf)?;
        let auth = Pack::decode(buf)?;
        let resolver_id = Pack::decode(buf)?;
        Ok(ServerHelloWrite { ttl, ttl_expired, auth, resolver_id })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Secret(pub u128);

impl Pack for Secret {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.0)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.0, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(Secret(Pack::decode(buf)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyForOwnershipCheck;

impl Pack for ReadyForOwnershipCheck {
    fn encoded_len(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Ok(buf.put_u8(0))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    /// List paths matching the specified glob set.
    ListMatching(GlobSet),
    /// Get the change nr for the specified path
    GetChangeNr(Path),
}

impl Pack for ToRead {
    fn encoded_len(&self) -> usize {
        1 + match self {
            ToRead::Resolve(path)
            | ToRead::List(path)
            | ToRead::Table(path)
            | ToRead::GetChangeNr(path) => Pack::encoded_len(path),
            ToRead::ListMatching(g) => Pack::encoded_len(g),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ToRead::Resolve(path) => {
                buf.put_u8(0);
                Pack::encode(path, buf)
            }
            ToRead::List(path) => {
                buf.put_u8(1);
                Pack::encode(path, buf)
            }
            ToRead::Table(path) => {
                buf.put_u8(2);
                Pack::encode(path, buf)
            }
            ToRead::ListMatching(globs) => {
                buf.put_u8(3);
                Pack::encode(globs, buf)
            }
            ToRead::GetChangeNr(path) => {
                buf.put_u8(4);
                Pack::encode(path, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ToRead::Resolve(Pack::decode(buf)?)),
            1 => Ok(ToRead::List(Pack::decode(buf)?)),
            2 => Ok(ToRead::Table(Pack::decode(buf)?)),
            3 => Ok(ToRead::ListMatching(Pack::decode(buf)?)),
            4 => Ok(ToRead::GetChangeNr(Pack::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub krb5_spns: Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>>,
    pub resolver: SocketAddr,
    pub addrs: Pooled<Vec<(SocketAddr, Bytes)>>,
    pub timestamp: u64,
    pub flags: u16,
    pub permissions: u16,
}

impl Pack for Resolved {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.krb5_spns)
            + Pack::encoded_len(&self.resolver)
            + Pack::encoded_len(&self.addrs)
            + Pack::encoded_len(&self.timestamp)
            + Pack::encoded_len(&self.flags)
            + Pack::encoded_len(&self.permissions)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.krb5_spns, buf)?;
        Pack::encode(&self.resolver, buf)?;
        Pack::encode(&self.addrs, buf)?;
        Pack::encode(&self.timestamp, buf)?;
        Pack::encode(&self.flags, buf)?;
        Pack::encode(&self.permissions, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let krb5_spns = Pack::decode(buf)?;
        let resolver = Pack::decode(buf)?;
        let addrs = Pack::decode(buf)?;
        let timestamp = Pack::decode(buf)?;
        let flags = Pack::decode(buf)?;
        let permissions = Pack::decode(buf)?;
        Ok(Resolved { krb5_spns, resolver, addrs, timestamp, permissions, flags })
    }
}

#[derive(Clone, Debug)]
pub struct Referral {
    pub path: Path,
    pub ttl: u64,
    pub addrs: Pooled<Vec<SocketAddr>>,
    pub krb5_spns: Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>>,
}
// CR estokes: These are probably not needed anymore
impl Hash for Referral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.addrs, state)
    }
}

impl PartialEq for Referral {
    fn eq(&self, other: &Referral) -> bool {
        self.addrs == other.addrs
    }
}

impl Eq for Referral {}

impl Pack for Referral {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.path)
            + Pack::encoded_len(&self.ttl)
            + Pack::encoded_len(&self.addrs)
            + Pack::encoded_len(&self.krb5_spns)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.path, buf)?;
        Pack::encode(&self.ttl, buf)?;
        Pack::encode(&self.addrs, buf)?;
        Pack::encode(&self.krb5_spns, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let path = Pack::decode(buf)?;
        let ttl = Pack::decode(buf)?;
        let addrs = Pack::decode(buf)?;
        let krb5_spns = Pack::decode(buf)?;
        Ok(Referral { path, ttl, addrs, krb5_spns })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub rows: Pooled<Vec<Path>>,
    pub cols: Pooled<Vec<(Path, Z64)>>,
}

impl Pack for Table {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.rows) + Pack::encoded_len(&self.cols)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.rows, buf)?;
        Pack::encode(&self.cols, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let rows = Pack::decode(buf)?;
        let cols = Pack::decode(buf)?;
        Ok(Table { rows, cols })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListMatching {
    pub matched: Pooled<Vec<Pooled<Vec<Path>>>>,
    pub referrals: Pooled<Vec<Referral>>,
}

impl Pack for ListMatching {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.matched) + Pack::encoded_len(&self.referrals)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.matched, buf)?;
        Pack::encode(&self.referrals, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let matched = Pack::decode(buf)?;
        let referrals = Pack::decode(buf)?;
        Ok(ListMatching { matched, referrals })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetChangeNr {
    pub change_number: Z64,
    pub resolver: SocketAddr,
    pub referrals: Pooled<Vec<Referral>>,
}

impl Pack for GetChangeNr {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.change_number)
            + Pack::encoded_len(&self.resolver)
            + Pack::encoded_len(&self.referrals)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.change_number, buf)?;
        Pack::encode(&self.resolver, buf)?;
        Pack::encode(&self.referrals, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let change_number = Pack::decode(buf)?;
        let resolver = Pack::decode(buf)?;
        let referrals = Pack::decode(buf)?;
        Ok(GetChangeNr { change_number, resolver, referrals })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FromRead {
    Resolved(Resolved),
    List(Pooled<Vec<Path>>),
    ListMatching(ListMatching),
    GetChangeNr(GetChangeNr),
    Table(Table),
    Referral(Referral),
    Denied,
    Error(Chars),
}

impl Pack for FromRead {
    fn encoded_len(&self) -> usize {
        1 + match self {
            FromRead::Resolved(a) => Pack::encoded_len(a),
            FromRead::List(l) => Pack::encoded_len(l),
            FromRead::Table(t) => Pack::encoded_len(t),
            FromRead::Referral(r) => Pack::encoded_len(r),
            FromRead::ListMatching(m) => Pack::encoded_len(m),
            FromRead::GetChangeNr(m) => Pack::encoded_len(m),
            FromRead::Denied => 0,
            FromRead::Error(e) => Pack::encoded_len(e),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            FromRead::Resolved(a) => {
                buf.put_u8(0);
                Pack::encode(a, buf)
            }
            FromRead::List(l) => {
                buf.put_u8(1);
                Pack::encode(l, buf)
            }
            FromRead::Table(t) => {
                buf.put_u8(2);
                Pack::encode(t, buf)
            }
            FromRead::Referral(r) => {
                buf.put_u8(3);
                Pack::encode(r, buf)
            }
            FromRead::Denied => Ok(buf.put_u8(4)),
            FromRead::Error(e) => {
                buf.put_u8(5);
                Pack::encode(e, buf)
            }
            FromRead::ListMatching(l) => {
                buf.put_u8(6);
                Pack::encode(l, buf)
            }
            FromRead::GetChangeNr(l) => {
                buf.put_u8(7);
                Pack::encode(l, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(FromRead::Resolved(Pack::decode(buf)?)),
            1 => Ok(FromRead::List(Pack::decode(buf)?)),
            2 => Ok(FromRead::Table(Pack::decode(buf)?)),
            3 => Ok(FromRead::Referral(Pack::decode(buf)?)),
            4 => Ok(FromRead::Denied),
            5 => Ok(FromRead::Error(Pack::decode(buf)?)),
            6 => Ok(FromRead::ListMatching(Pack::decode(buf)?)),
            7 => Ok(FromRead::GetChangeNr(Pack::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
    /// Publish the path and set associated flags
    PublishWithFlags(Path, u16),
    /// Add a default publisher to path and set associated flags
    PublishDefaultWithFlags(Path, u16),
    /// Unpublish a default publisher
    UnpublishDefault(Path),
}

impl Pack for ToWrite {
    fn encoded_len(&self) -> usize {
        1 + match self {
            ToWrite::Publish(p) => Pack::encoded_len(p),
            ToWrite::PublishDefault(p) => Pack::encoded_len(p),
            ToWrite::Unpublish(p) => Pack::encoded_len(p),
            ToWrite::Clear => 0,
            ToWrite::Heartbeat => 0,
            ToWrite::PublishWithFlags(p, f) => {
                Pack::encoded_len(p) + Pack::encoded_len(f)
            }
            ToWrite::PublishDefaultWithFlags(p, f) => {
                Pack::encoded_len(p) + Pack::encoded_len(f)
            }
            ToWrite::UnpublishDefault(p) => Pack::encoded_len(p),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ToWrite::Publish(p) => {
                buf.put_u8(0);
                Pack::encode(p, buf)
            }
            ToWrite::PublishDefault(p) => {
                buf.put_u8(1);
                Pack::encode(p, buf)
            }
            ToWrite::Unpublish(p) => {
                buf.put_u8(2);
                Pack::encode(p, buf)
            }
            ToWrite::Clear => Ok(buf.put_u8(3)),
            ToWrite::Heartbeat => Ok(buf.put_u8(4)),
            ToWrite::PublishWithFlags(p, f) => {
                buf.put_u8(5);
                Pack::encode(p, buf)?;
                Pack::encode(f, buf)
            }
            ToWrite::PublishDefaultWithFlags(p, f) => {
                buf.put_u8(6);
                Pack::encode(p, buf)?;
                Pack::encode(f, buf)
            }
            ToWrite::UnpublishDefault(p) => {
                buf.put_u8(7);
                <Path as Pack>::encode(p, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ToWrite::Publish(<Path as Pack>::decode(buf)?)),
            1 => Ok(ToWrite::PublishDefault(<Path as Pack>::decode(buf)?)),
            2 => Ok(ToWrite::Unpublish(<Path as Pack>::decode(buf)?)),
            3 => Ok(ToWrite::Clear),
            4 => Ok(ToWrite::Heartbeat),
            5 => {
                let p = <Path as Pack>::decode(buf)?;
                let f = <u16 as Pack>::decode(buf)?;
                Ok(ToWrite::PublishWithFlags(p, f))
            }
            6 => {
                let p = <Path as Pack>::decode(buf)?;
                let f = <u16 as Pack>::decode(buf)?;
                Ok(ToWrite::PublishDefaultWithFlags(p, f))
            }
            7 => Ok(ToWrite::UnpublishDefault(<Path as Pack>::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum FromWrite {
    Published,
    Unpublished,
    Referral(Referral),
    Denied,
    Error(Chars),
}

impl Pack for FromWrite {
    fn encoded_len(&self) -> usize {
        1 + match self {
            FromWrite::Published => 0,
            FromWrite::Unpublished => 0,
            FromWrite::Referral(r) => <Referral as Pack>::encoded_len(r),
            FromWrite::Denied => 0,
            FromWrite::Error(c) => <Chars as Pack>::encoded_len(c),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
