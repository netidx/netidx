use crate::glob::GlobSet;
use bytes::{Buf, BufMut, Bytes};
use netidx_core::{
    chars::Chars,
    pack::{
        self, len_wrapped_decode, len_wrapped_encode, len_wrapped_len, Pack, PackError,
        Z64,
    },
    path::Path,
    pool::Pooled,
};
use netidx_derive::Pack;
use std::{
    cmp::{Eq, PartialEq},
    hash::{Hash, Hasher},
    net::SocketAddr,
    result,
};

type Error = PackError;
pub type Result<T> = result::Result<T, Error>;

#[derive(Clone, Debug, Copy, PartialEq, Eq, Pack)]
pub enum HashMethod {
    Sha3_512,
}

#[derive(Clone, Debug, Copy, PartialEq, Pack)]
pub struct AuthChallenge {
    pub hash_method: HashMethod,
    pub challenge: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub enum AuthRead {
    Anonymous,
    Krb5,
    Local,
    Tls,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub enum AuthWrite {
    Anonymous,
    Reuse,
    Krb5 { spn: Chars },
    Local,
    Tls { name: Chars },
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct ClientHelloWrite {
    pub write_addr: SocketAddr,
    pub auth: AuthWrite,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
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

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct ServerHelloWrite {
    pub ttl: u64,
    pub ttl_expired: bool,
    pub auth: AuthWrite,
    pub resolver_id: SocketAddr,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct Secret(pub u128);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyForOwnershipCheck;

impl Pack for ReadyForOwnershipCheck {
    fn encoded_len(&self) -> usize {
        len_wrapped_len(1)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        len_wrapped_encode(buf, self, |buf| Ok(buf.put_u8(0)))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        len_wrapped_decode(buf, |buf| match <u8 as Pack>::decode(buf)? {
            0 => Ok(ReadyForOwnershipCheck),
            _ => Err(PackError::UnknownTag),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
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

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub enum Auth {
    Anonymous,
    Local { path: Chars },
    Krb5 { spn: Chars },
    Tls { name: Chars },
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

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub enum TargetAuth {
    Anonymous,
    Local,
    Krb5 { spn: Chars },
    Tls { name: Chars },
}

impl TargetAuth {
    pub fn is_anonymous(&self) -> bool {
        match self {
            Self::Anonymous => true,
            Self::Krb5 { .. } | Self::Local | Self::Tls { .. } => false,
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
            AuthWrite::Tls { name } => Ok(Self::Tls { name }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct Publisher {
    pub resolver: SocketAddr,
    pub id: PublisherId,
    pub addr: SocketAddr,
    pub hash_method: HashMethod,
    pub target_auth: TargetAuth,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct PublisherRef {
    pub id: PublisherId,
    pub token: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct Resolved {
    pub resolver: SocketAddr,
    pub publishers: Pooled<Vec<PublisherRef>>,
    pub timestamp: u64,
    pub flags: u32,
    pub permissions: u32,
}

#[derive(Clone, Debug, Pack)]
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

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct Table {
    pub rows: Pooled<Vec<Path>>,
    pub cols: Pooled<Vec<(Path, Z64)>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct ListMatching {
    pub matched: Pooled<Vec<Pooled<Vec<Path>>>>,
    pub referrals: Pooled<Vec<Referral>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct GetChangeNr {
    pub change_number: Z64,
    pub resolver: SocketAddr,
    pub referrals: Pooled<Vec<Referral>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub enum FromRead {
    Publisher(Publisher),
    Resolved(Resolved),
    List(Pooled<Vec<Path>>),
    Table(Table),
    Referral(Referral),
    Denied,
    Error(Chars),
    ListMatching(ListMatching),
    GetChangeNr(GetChangeNr),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Pack)]
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Pack)]
pub enum FromWrite {
    Published,
    Unpublished,
    Referral(Referral),
    Denied,
    Error(Chars),
}
