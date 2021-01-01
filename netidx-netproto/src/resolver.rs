use crate::glob::GlobSet;
use netidx_core::{
    chars::Chars,
    pack::{Pack, PackError, Z64},
    path::Path,
    pool::Pooled,
};
use bytes::{Buf, BufMut, Bytes};
use fxhash::FxBuildHasher;
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
        <u64 as Pack>::encoded_len(&self.0)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <u64 as Pack>::encode(&self.0, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ClientAuthRead::Anonymous => 0,
            ClientAuthRead::Reuse(ref i) => Pack::encoded_len(i),
            ClientAuthRead::Initiate(ref b) => Pack::encoded_len(b),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ClientAuthWrite::Anonymous => 0,
            ClientAuthWrite::Reuse => 0,
            ClientAuthWrite::Initiate { spn, token } => {
                <Option<Chars> as Pack>::encoded_len(spn)
                    + <Bytes as Pack>::encoded_len(token)
            }
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    fn encoded_len(&self) -> usize {
        <SocketAddr as Pack>::encoded_len(&self.write_addr)
            + ClientAuthWrite::encoded_len(&self.auth)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <SocketAddr as Pack>::encode(&self.write_addr, buf)?;
        ClientAuthWrite::encode(&self.auth, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let write_addr = <SocketAddr as Pack>::decode(buf)?;
        let auth = ClientAuthWrite::decode(buf)?;
        Ok(ClientHelloWrite { write_addr, auth })
    }

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<()> {
        self.write_addr = <SocketAddr as Pack>::decode(buf)?;
        self.auth = <ClientAuthWrite as Pack>::decode(buf)?;
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
            ClientHello::ReadOnly(r) => ClientAuthRead::encoded_len(r),
            ClientHello::WriteOnly(r) => ClientHelloWrite::encoded_len(r),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ServerHelloRead::Anonymous => 0,
            ServerHelloRead::Reused => 0,
            ServerHelloRead::Accepted(tok, id) => {
                <Bytes as Pack>::encoded_len(tok) + CtxId::encoded_len(id)
            }
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ServerAuthWrite::Anonymous => 0,
            ServerAuthWrite::Reused => 0,
            ServerAuthWrite::Accepted(b) => <Bytes as Pack>::encoded_len(b),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            ServerAuthWrite::Anonymous => Ok(buf.put_u8(0)),
            ServerAuthWrite::Reused => Ok(buf.put_u8(1)),
            ServerAuthWrite::Accepted(b) => {
                buf.put_u8(2);
                <Bytes as Pack>::encode(b, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    fn encoded_len(&self) -> usize {
        <u64 as Pack>::encoded_len(&self.ttl)
            + <bool as Pack>::encoded_len(&self.ttl_expired)
            + ServerAuthWrite::encoded_len(&self.auth)
            + <SocketAddr as Pack>::encoded_len(&self.resolver_id)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <u64 as Pack>::encode(&self.ttl, buf)?;
        <bool as Pack>::encode(&self.ttl_expired, buf)?;
        ServerAuthWrite::encode(&self.auth, buf)?;
        <SocketAddr as Pack>::encode(&self.resolver_id, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
    fn encoded_len(&self) -> usize {
        <u128 as Pack>::encoded_len(&self.0)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <u128 as Pack>::encode(&self.0, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(Secret(<u128 as Pack>::decode(buf)?))
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
            | ToRead::GetChangeNr(path) => <Path as Pack>::encoded_len(path),
            ToRead::ListMatching(g) => <GlobSet as Pack>::encoded_len(g),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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
            ToRead::ListMatching(globs) => {
                buf.put_u8(3);
                <GlobSet as Pack>::encode(globs, buf)
            }
            ToRead::GetChangeNr(path) => {
                buf.put_u8(4);
                <Path as Pack>::encode(path, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(ToRead::Resolve(<Path as Pack>::decode(buf)?)),
            1 => Ok(ToRead::List(<Path as Pack>::decode(buf)?)),
            2 => Ok(ToRead::Table(<Path as Pack>::decode(buf)?)),
            3 => Ok(ToRead::ListMatching(<GlobSet as Pack>::decode(buf)?)),
            4 => Ok(ToRead::GetChangeNr(<Path as Pack>::decode(buf)?)),
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
    pub permissions: u32,
}

impl Pack for Resolved {
    fn encoded_len(&self) -> usize {
        <Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>> as Pack>::encoded_len(
            &self.krb5_spns,
        ) + <SocketAddr as Pack>::encoded_len(&self.resolver)
            + <Pooled<Vec<(SocketAddr, Bytes)>> as Pack>::encoded_len(&self.addrs)
            + <u64 as Pack>::encoded_len(&self.timestamp)
            + <u32 as Pack>::encoded_len(&self.permissions)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>> as Pack>::encode(
            &self.krb5_spns,
            buf,
        )?;
        <SocketAddr as Pack>::encode(&self.resolver, buf)?;
        <Pooled<Vec<(SocketAddr, Bytes)>> as Pack>::encode(&self.addrs, buf)?;
        <u64 as Pack>::encode(&self.timestamp, buf)?;
        <u32 as Pack>::encode(&self.permissions, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let krb5_spns =
            <Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>> as Pack>::decode(buf)?;
        let resolver = <SocketAddr as Pack>::decode(buf)?;
        let addrs = <Pooled<Vec<(SocketAddr, Bytes)>> as Pack>::decode(buf)?;
        let timestamp = <u64 as Pack>::decode(buf)?;
        let permissions = <u32 as Pack>::decode(buf)?;
        Ok(Resolved { krb5_spns, resolver, addrs, timestamp, permissions })
    }
}

#[derive(Clone, Debug)]
pub struct Referral {
    pub path: Path,
    pub ttl: u64,
    pub addrs: Pooled<Vec<SocketAddr>>,
    pub krb5_spns: Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>>,
}

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
        <Path as Pack>::encoded_len(&self.path)
            + <u64 as Pack>::encoded_len(&self.ttl)
            + <Pooled<Vec<SocketAddr>> as Pack>::encoded_len(&self.addrs)
            + <Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>> as Pack>::encoded_len(
                &self.krb5_spns,
            )
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <Path as Pack>::encode(&self.path, buf)?;
        <u64 as Pack>::encode(&self.ttl, buf)?;
        <Pooled<Vec<SocketAddr>> as Pack>::encode(&self.addrs, buf)?;
        <Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>> as Pack>::encode(
            &self.krb5_spns,
            buf,
        )
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let path = <Path as Pack>::decode(buf)?;
        let ttl = <u64 as Pack>::decode(buf)?;
        let addrs = <Pooled<Vec<SocketAddr>> as Pack>::decode(buf)?;
        let krb5_spns =
            <Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>> as Pack>::decode(buf)?;
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
        <Pooled<Vec<Path>>>::encoded_len(&self.rows)
            + <Pooled<Vec<(Path, Z64)>>>::encoded_len(&self.cols)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <Pooled<Vec<Path>>>::encode(&self.rows, buf)?;
        <Pooled<Vec<(Path, Z64)>>>::encode(&self.cols, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let rows = <Pooled<Vec<Path>>>::decode(buf)?;
        let cols = <Pooled<Vec<(Path, Z64)>>>::decode(buf)?;
        Ok(Table { rows, cols })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListMatching {
    pub matched: Pooled<Vec<Path>>,
    pub referrals: Pooled<Vec<Referral>>,
}

impl Pack for ListMatching {
    fn encoded_len(&self) -> usize {
        <Pooled<Vec<Path>> as Pack>::encoded_len(&self.matched)
            + <Pooled<Vec<Referral>> as Pack>::encoded_len(&self.referrals)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <Pooled<Vec<Path>> as Pack>::encode(&self.matched, buf)?;
        <Pooled<Vec<Referral>> as Pack>::encode(&self.referrals, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let matched = <Pooled<Vec<Path>> as Pack>::decode(buf)?;
        let referrals = <Pooled<Vec<Referral>> as Pack>::decode(buf)?;
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
        <Z64 as Pack>::encoded_len(&self.change_number)
            + <SocketAddr as Pack>::encoded_len(&self.resolver)
            + <Pooled<Vec<Referral>> as Pack>::encoded_len(&self.referrals)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        <Z64 as Pack>::encode(&self.change_number, buf)?;
        <SocketAddr as Pack>::encode(&self.resolver, buf)?;
        <Pooled<Vec<Referral>> as Pack>::encode(&self.referrals, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let change_number = <Z64 as Pack>::decode(buf)?;
        let resolver = <SocketAddr as Pack>::decode(buf)?;
        Ok(GetChangeNr {
            change_number,
            resolver,
            referrals: <Pooled<Vec<Referral>> as Pack>::decode(buf)?,
        })
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
            FromRead::Resolved(a) => Resolved::encoded_len(a),
            FromRead::List(l) => <Pooled<Vec<Path>> as Pack>::encoded_len(l),
            FromRead::Table(t) => <Table as Pack>::encoded_len(t),
            FromRead::Referral(r) => <Referral as Pack>::encoded_len(r),
            FromRead::ListMatching(m) => <ListMatching as Pack>::encoded_len(m),
            FromRead::GetChangeNr(m) => <GetChangeNr as Pack>::encoded_len(m),
            FromRead::Denied => 0,
            FromRead::Error(e) => <Chars as Pack>::encoded_len(e),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            FromRead::Resolved(a) => {
                buf.put_u8(0);
                Resolved::encode(a, buf)
            }
            FromRead::List(l) => {
                buf.put_u8(1);
                <Pooled<Vec<Path>> as Pack>::encode(l, buf)
            }
            FromRead::Table(t) => {
                buf.put_u8(2);
                <Table as Pack>::encode(t, buf)
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
            FromRead::ListMatching(l) => {
                buf.put_u8(6);
                <ListMatching as Pack>::encode(l, buf)
            }
            FromRead::GetChangeNr(l) => {
                buf.put_u8(7);
                <GetChangeNr as Pack>::encode(l, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(FromRead::Resolved(Resolved::decode(buf)?)),
            1 => Ok(FromRead::List(<Pooled<Vec<Path>> as Pack>::decode(buf)?)),
            2 => Ok(FromRead::Table(<Table as Pack>::decode(buf)?)),
            3 => Ok(FromRead::Referral(<Referral as Pack>::decode(buf)?)),
            4 => Ok(FromRead::Denied),
            5 => Ok(FromRead::Error(<Chars as Pack>::decode(buf)?)),
            6 => Ok(FromRead::ListMatching(<ListMatching as Pack>::decode(buf)?)),
            7 => Ok(FromRead::GetChangeNr(<GetChangeNr as Pack>::decode(buf)?)),
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
    fn encoded_len(&self) -> usize {
        1 + match self {
            ToWrite::Publish(p) => <Path as Pack>::encoded_len(p),
            ToWrite::PublishDefault(p) => <Path as Pack>::encoded_len(p),
            ToWrite::Unpublish(p) => <Path as Pack>::encoded_len(p),
            ToWrite::Clear => 0,
            ToWrite::Heartbeat => 0,
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
