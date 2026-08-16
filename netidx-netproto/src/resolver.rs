use crate::glob::GlobSet;
use arcstr::ArcStr;
use bytes::{Buf, BufMut, Bytes};
use netidx_core::{
    pack::{
        Pack, PackError, Z64, len_wrapped_decode, len_wrapped_encode, len_wrapped_len,
    },
    path::Path,
};
use netidx_derive::Pack;
use poolshark::global::GPooled;
use smallvec::SmallVec;
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
    Krb5 { spn: ArcStr },
    Local,
    Tls { name: ArcStr },
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct ClientHelloWrite {
    pub write_addr: SocketAddr,
    pub auth: AuthWrite,
    #[pack(default)]
    pub priority: PublisherPriority,
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

/// Why the resolver will not accept this publisher, sent instead of dropping
/// the socket and leaving it to guess.
///
/// One value, not a set: a single server refuses for a single reason. A
/// client that doesn't recognise one just knows it was refused.
#[derive(Clone, Debug, Copy, PartialEq, Eq, Pack)]
#[repr(u8)]
pub enum WriteRefusal {
    LinkLocalAddr,
    BroadcastAddr,
    PrivateAddr,
    UnspecifiedAddr,
    MulticastAddr,
    LoopbackAddr,
    /// The handshake succeeded, but the resolver could not decide what the
    /// publisher is allowed to do — the name it presented did not map to a
    /// user, or the map that would have said so did not answer. Which of
    /// those is in the resolver's log; the publisher can act on neither, and
    /// needs only to know that retrying will not help.
    Unauthorized,
}

impl From<netidx_core::utils::AddrError> for WriteRefusal {
    fn from(e: netidx_core::utils::AddrError) -> Self {
        use netidx_core::utils::AddrError as A;
        match e {
            A::LinkLocal => Self::LinkLocalAddr,
            A::Broadcast => Self::BroadcastAddr,
            A::Private => Self::PrivateAddr,
            A::Unspecified => Self::UnspecifiedAddr,
            A::Multicast => Self::MulticastAddr,
            A::Loopback => Self::LoopbackAddr,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct ServerHelloWrite {
    pub ttl: u64,
    pub ttl_expired: bool,
    pub auth: AuthWrite,
    pub resolver_id: SocketAddr,
    /// If set, everything above is moot: the connection is about to close.
    ///
    /// A trailing `#[pack(default)]` field, so a 0.32 publisher decodes the
    /// message, skips it, and then sees the connection close — which is what
    /// it sees today. No protocol version bump.
    #[pack(default)]
    pub refused: Option<WriteRefusal>,
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
    Local { path: ArcStr },
    Krb5 { spn: ArcStr },
    Tls { name: ArcStr },
}

atomic_id!(PublisherId);

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub enum TargetAuth {
    Anonymous,
    Local,
    Krb5 { spn: ArcStr },
    Tls { name: ArcStr },
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Pack)]
pub struct UserInfo {
    pub name: ArcStr,
    pub primary_group: ArcStr,
    pub groups: SmallVec<[ArcStr; 16]>,
    pub resolver: SocketAddr,
    pub token: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pack)]
pub enum PublisherPriority {
    High,
    Normal,
    Low,
}

impl Default for PublisherPriority {
    fn default() -> Self {
        PublisherPriority::Normal
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct Publisher {
    pub resolver: SocketAddr,
    pub id: PublisherId,
    pub addr: SocketAddr,
    pub hash_method: HashMethod,
    pub target_auth: TargetAuth,
    #[pack(default)]
    pub user_info: Option<UserInfo>,
    #[pack(default)]
    pub priority: PublisherPriority,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct PublisherRef {
    pub id: PublisherId,
    pub token: Bytes,
}

/// The result of resolving a path
#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct Resolved {
    pub resolver: SocketAddr,
    pub publishers: GPooled<Vec<PublisherRef>>,
    pub timestamp: u64,
    pub flags: u32,
    pub permissions: u32,
}

#[derive(Clone, Debug, Pack)]
pub struct Referral {
    pub path: Path,
    pub ttl: Option<u16>,
    pub addrs: GPooled<Vec<(SocketAddr, Auth)>>,
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
        *self.addrs == *other.addrs
    }
}

impl Eq for Referral {}

/// An inferred table
#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct Table {
    pub rows: GPooled<Vec<Path>>,
    pub cols: GPooled<Vec<(Path, Z64)>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct ListMatching {
    pub matched: GPooled<Vec<GPooled<Vec<Path>>>>,
    pub referrals: GPooled<Vec<Referral>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub struct GetChangeNr {
    pub change_number: Z64,
    pub resolver: SocketAddr,
    pub referrals: GPooled<Vec<Referral>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Pack)]
pub enum FromRead {
    Publisher(Publisher),
    Resolved(Resolved),
    List(GPooled<Vec<Path>>),
    Table(Table),
    Referral(Referral),
    Denied,
    Error(ArcStr),
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
    Error(ArcStr),
}
