use crate::glob::GlobSet;
use bytes::{Buf, BufMut, Bytes};
use netidx_core::{
    chars::Chars,
    pack::{self, Pack, PackError, Z64},
    path::Path,
    pool::Pooled,
};
use std::{
    cmp::{Eq, PartialEq},
    hash::{Hash, Hasher},
    net::SocketAddr,
    result,
};

type Error = PackError;
pub type Result<T> = result::Result<T, Error>;

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum HashMethod {
    Sha3_512,
}

impl Pack for HashMethod {
    fn encoded_len(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Self::Sha3_512 => Ok(buf.put_u8(0)),
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Self::Sha3_512),
            _ => Err(PackError::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct AuthChallenge {
    pub hash_method: HashMethod,
    pub challenge: u128,
}

impl Pack for AuthChallenge {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.hash_method) + Pack::encoded_len(&self.challenge)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.hash_method, buf)?;
        Pack::encode(&self.challenge, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let hash_method = Pack::decode(buf)?;
        let challenge = Pack::decode(buf)?;
        Ok(AuthChallenge { hash_method, challenge })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthRead {
    Anonymous,
    Krb5,
    Local,
    Tls,
}

impl Pack for AuthRead {
    fn encoded_len(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Self::Anonymous => Ok(buf.put_u8(0)),
            Self::Krb5 => Ok(buf.put_u8(1)),
            Self::Local => Ok(buf.put_u8(2)),
            Self::Tls => Ok(buf.put_u8(3)),
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Self::Anonymous),
            1 => Ok(Self::Krb5),
            2 => Ok(Self::Local),
            3 => Ok(Self::Tls),
            _ => return Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthWrite {
    Anonymous,
    Reuse,
    Krb5 { spn: Chars },
    Local,
    Tls,
}

impl Pack for AuthWrite {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Self::Anonymous | Self::Reuse | Self::Local | Self::Tls => 0,
            Self::Krb5 { spn } => Pack::encoded_len(spn),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Self::Anonymous => Ok(buf.put_u8(0)),
            Self::Reuse => Ok(buf.put_u8(1)),
            Self::Krb5 { spn } => {
                buf.put_u8(2);
                Pack::encode(spn, buf)
            }
            Self::Local => Ok(buf.put_u8(3)),
            Self::Tls => Ok(buf.put_u8(4)),
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Self::Anonymous),
            1 => Ok(Self::Reuse),
            2 => Ok(Self::Krb5 { spn: Pack::decode(buf)? }),
            3 => Ok(Self::Local),
            4 => Ok(Self::Tls),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientHelloWrite {
    pub write_addr: SocketAddr,
    pub auth: AuthWrite,
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
    ReadOnly(AuthRead),
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
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(ClientHello::ReadOnly(Pack::decode(buf)?)),
            1 => Ok(ClientHello::WriteOnly(Pack::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerHelloWrite {
    pub ttl: u64,
    pub ttl_expired: bool,
    pub auth: AuthWrite,
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
        match <u8 as Pack>::decode(buf)? {
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
        match <u8 as Pack>::decode(buf)? {
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
pub enum Auth {
    Anonymous,
    Local { path: Chars },
    Krb5 { spn: Chars },
    Tls,
}

impl Pack for Auth {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Self::Anonymous | Self::Tls => 0,
            Self::Local { path } => Pack::encoded_len(path),
            Self::Krb5 { spn } => Pack::encoded_len(spn),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Self::Anonymous => Ok(buf.put_u8(0)),
            Self::Local { path } => {
                buf.put_u8(1);
                Pack::encode(path, buf)
            }
            Self::Krb5 { spn } => {
                buf.put_u8(2);
                Pack::encode(spn, buf)
            }
            Self::Tls => Ok(buf.put_u8(3)),
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Self::Anonymous),
            1 => Ok(Self::Local { path: Pack::decode(buf)? }),
            2 => Ok(Self::Krb5 { spn: Pack::decode(buf)? }),
            3 => Ok(Self::Tls),
            _ => Err(Error::UnknownTag),
        }
    }
}

atomic_id!(PublisherId);

impl Pack for PublisherId {
    fn encoded_len(&self) -> usize {
        pack::varint_len(self.0)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Ok(pack::encode_varint(self.0, buf))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(Self(pack::decode_varint(buf)?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TargetAuth {
    Anonymous,
    Local,
    Krb5 { spn: Chars },
    Tls,
}

impl TargetAuth {
    pub fn is_anonymous(&self) -> bool {
        match self {
            Self::Anonymous => true,
            Self::Krb5 { spn: _ } | Self::Local | Self::Tls => false,
        }
    }
}

impl TryFrom<AuthWrite> for TargetAuth {
    type Error = anyhow::Error;

    fn try_from(v: AuthWrite) -> result::Result<Self, Self::Error> {
        match v {
            AuthWrite::Anonymous => Ok(Self::Anonymous),
            AuthWrite::Local => Ok(Self::Local),
            AuthWrite::Krb5 { spn } => Ok(Self::Krb5 { spn }),
            AuthWrite::Reuse => bail!("no session to reuse"),
            AuthWrite::Tls => Ok(Self::Tls),
        }
    }
}

impl Pack for TargetAuth {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Self::Anonymous | Self::Local | Self::Tls => 0,
            Self::Krb5 { spn } => Pack::encoded_len(spn),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Self::Anonymous => Ok(buf.put_u8(0)),
            Self::Local => Ok(buf.put_u8(1)),
            Self::Krb5 { spn } => {
                buf.put_u8(2);
                Pack::encode(spn, buf)
            }
            Self::Tls => Ok(buf.put_u8(3)),
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Self::Anonymous),
            1 => Ok(Self::Local),
            2 => Ok(Self::Krb5 { spn: Pack::decode(buf)? }),
            3 => Ok(Self::Tls),
            _ => Err(PackError::UnknownTag),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publisher {
    pub resolver: SocketAddr,
    pub id: PublisherId,
    pub addr: SocketAddr,
    pub hash_method: HashMethod,
    pub target_auth: TargetAuth,
}

impl Pack for Publisher {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.resolver)
            + Pack::encoded_len(&self.id)
            + Pack::encoded_len(&self.addr)
            + Pack::encoded_len(&self.hash_method)
            + Pack::encoded_len(&self.target_auth)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.resolver, buf)?;
        Pack::encode(&self.id, buf)?;
        Pack::encode(&self.addr, buf)?;
        Pack::encode(&self.hash_method, buf)?;
        Pack::encode(&self.target_auth, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let resolver = Pack::decode(buf)?;
        let id = Pack::decode(buf)?;
        let addr = Pack::decode(buf)?;
        let hash_method = Pack::decode(buf)?;
        let target_auth = Pack::decode(buf)?;
        Ok(Publisher { resolver, id, addr, hash_method, target_auth })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublisherRef {
    pub id: PublisherId,
    pub token: Bytes,
}

impl Pack for PublisherRef {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.id) + Pack::encoded_len(&self.token)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.id, buf)?;
        Pack::encode(&self.token, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let id = Pack::decode(buf)?;
        let token = Pack::decode(buf)?;
        Ok(PublisherRef { id, token })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub resolver: SocketAddr,
    pub publishers: Pooled<Vec<PublisherRef>>,
    pub timestamp: u64,
    pub flags: u32,
    pub permissions: u32,
}

impl Pack for Resolved {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.resolver)
            + Pack::encoded_len(&self.publishers)
            + Pack::encoded_len(&self.timestamp)
            + Pack::encoded_len(&self.flags)
            + Pack::encoded_len(&self.permissions)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.resolver, buf)?;
        Pack::encode(&self.publishers, buf)?;
        Pack::encode(&self.timestamp, buf)?;
        Pack::encode(&self.flags, buf)?;
        Pack::encode(&self.permissions, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let resolver = Pack::decode(buf)?;
        let publishers = Pack::decode(buf)?;
        let timestamp = Pack::decode(buf)?;
        let flags = Pack::decode(buf)?;
        let permissions = Pack::decode(buf)?;
        Ok(Resolved { resolver, publishers, timestamp, permissions, flags })
    }
}

#[derive(Clone, Debug)]
pub struct Referral {
    pub path: Path,
    pub ttl: Option<u16>,
    pub addrs: Pooled<Vec<(SocketAddr, Auth)>>,
}

impl Hash for Referral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (addr, _) in &*self.addrs {
            Hash::hash(&addr, state)
        }
    }
}

impl PartialEq for Referral {
    fn eq(&self, other: &Referral) -> bool {
        self.addrs.iter().zip(other.addrs.iter()).all(|(l, r)| l == r)
    }
}

impl Eq for Referral {}

impl Pack for Referral {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&self.path)
            + Pack::encoded_len(&self.ttl)
            + Pack::encoded_len(&self.addrs)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Pack::encode(&self.path, buf)?;
        Pack::encode(&self.ttl, buf)?;
        Pack::encode(&self.addrs, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let path = Pack::decode(buf)?;
        let ttl = Pack::decode(buf)?;
        let addrs = Pack::decode(buf)?;
        Ok(Referral { path, ttl, addrs })
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
    Publisher(Publisher),
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
            FromRead::Publisher(p) => Pack::encoded_len(p),
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
            FromRead::Publisher(p) => {
                buf.put_u8(0);
                Pack::encode(p, buf)
            }
            FromRead::Resolved(a) => {
                buf.put_u8(1);
                Pack::encode(a, buf)
            }
            FromRead::List(l) => {
                buf.put_u8(2);
                Pack::encode(l, buf)
            }
            FromRead::Table(t) => {
                buf.put_u8(3);
                Pack::encode(t, buf)
            }
            FromRead::Referral(r) => {
                buf.put_u8(4);
                Pack::encode(r, buf)
            }
            FromRead::Denied => Ok(buf.put_u8(5)),
            FromRead::Error(e) => {
                buf.put_u8(6);
                Pack::encode(e, buf)
            }
            FromRead::ListMatching(l) => {
                buf.put_u8(7);
                Pack::encode(l, buf)
            }
            FromRead::GetChangeNr(l) => {
                buf.put_u8(8);
                Pack::encode(l, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(FromRead::Publisher(Pack::decode(buf)?)),
            1 => Ok(FromRead::Resolved(Pack::decode(buf)?)),
            2 => Ok(FromRead::List(Pack::decode(buf)?)),
            3 => Ok(FromRead::Table(Pack::decode(buf)?)),
            4 => Ok(FromRead::Referral(Pack::decode(buf)?)),
            5 => Ok(FromRead::Denied),
            6 => Ok(FromRead::Error(Pack::decode(buf)?)),
            7 => Ok(FromRead::ListMatching(Pack::decode(buf)?)),
            8 => Ok(FromRead::GetChangeNr(Pack::decode(buf)?)),
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
    PublishWithFlags(Path, u32),
    /// Add a default publisher to path and set associated flags
    PublishDefaultWithFlags(Path, u32),
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
                Pack::encode(p, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(ToWrite::Publish(Pack::decode(buf)?)),
            1 => Ok(ToWrite::PublishDefault(Pack::decode(buf)?)),
            2 => Ok(ToWrite::Unpublish(Pack::decode(buf)?)),
            3 => Ok(ToWrite::Clear),
            4 => Ok(ToWrite::Heartbeat),
            5 => {
                let p = Pack::decode(buf)?;
                let f = Pack::decode(buf)?;
                Ok(ToWrite::PublishWithFlags(p, f))
            }
            6 => {
                let p = Pack::decode(buf)?;
                let f = Pack::decode(buf)?;
                Ok(ToWrite::PublishDefaultWithFlags(p, f))
            }
            7 => Ok(ToWrite::UnpublishDefault(Pack::decode(buf)?)),
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
            FromWrite::Referral(r) => Pack::encoded_len(r),
            FromWrite::Denied => 0,
            FromWrite::Error(c) => Pack::encoded_len(c),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            FromWrite::Published => Ok(buf.put_u8(0)),
            FromWrite::Unpublished => Ok(buf.put_u8(1)),
            FromWrite::Referral(r) => {
                buf.put_u8(2);
                Pack::encode(r, buf)
            }
            FromWrite::Denied => Ok(buf.put_u8(3)),
            FromWrite::Error(c) => {
                buf.put_u8(4);
                Pack::encode(c, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(FromWrite::Published),
            1 => Ok(FromWrite::Unpublished),
            2 => Ok(FromWrite::Referral(Pack::decode(buf)?)),
            3 => Ok(FromWrite::Denied),
            4 => Ok(FromWrite::Error(Pack::decode(buf)?)),
            _ => Err(Error::UnknownTag),
        }
    }
}
