//! Resolver server configuration
//!
//! See the file module for documentation of the on disk format.

use self::file::IdMapType;
use crate::{
    path::Path,
    protocol::resolver::{self, Referral},
    tls, utils,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx_derive::Pack;
use serde_json::from_str;
use std::{
    collections::{
        BTreeMap, Bound,
        Bound::{Excluded, Unbounded},
        HashMap,
    },
    convert::AsRef,
    convert::Into,
    default::Default,
    net::{IpAddr, SocketAddr},
    path::Path as FsPath,
    time::Duration,
};

type Permissions = ArcStr;
type Entity = ArcStr;

/// Whether a member server refuses read clients.
///
/// A resolver holds only what publishers have told it, so a replica that has
/// just joined a cluster knows nothing until every publisher has found it and
/// republished. Subscribers pointed at it in the meantime get correct-looking
/// empty answers. Gating reads keeps them away while letting publishers fill
/// it — writes are never gated, which is the whole point.
///
/// It is equally the way to take a decommissioned member out of service
/// without stopping it: `Yes` makes it stop answering subscribers while its
/// records age out, and it survives a restart of that host because it lives
/// in the config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub enum ReadGate {
    /// Serve reads.
    No,
    /// Refuse reads until an administrator opens the gate.
    Yes,
    /// Refuse reads until this time. Set when a member joins a cluster that
    /// is already publishing, because that is the one case where how long to
    /// wait can actually be computed.
    Until(chrono::DateTime<chrono::Utc>),
}

impl Default for ReadGate {
    fn default() -> Self {
        ReadGate::No
    }
}

impl ReadGate {
    /// When reads become allowed, in milliseconds since the epoch.
    ///
    /// A gate says "refuse reads until", so the two constant answers are the
    /// extremes of the same scale: `No` opened at the beginning of time and
    /// `Yes` opens at the end of it. Collapsing the enum this way is what lets
    /// a running server hold its gate in one atomic. Neither sentinel can
    /// collide with a real deadline; chrono's range is an order of magnitude
    /// short of `i64` milliseconds.
    pub(crate) fn opens_at(self) -> i64 {
        match self {
            ReadGate::No => i64::MIN,
            ReadGate::Yes => i64::MAX,
            ReadGate::Until(t) => t.timestamp_millis(),
        }
    }

    pub(crate) fn open_at(millis: i64) -> bool {
        match millis {
            i64::MIN => true,
            i64::MAX => false,
            t => chrono::Utc::now().timestamp_millis() >= t,
        }
    }

    pub fn is_open(&self) -> bool {
        ReadGate::open_at(self.opens_at())
    }

    /// The gate that refuses reads for longer. A host whose member blocks
    /// disagree is described by its strictest one, since that is the one a
    /// subscriber can be turned away by.
    pub fn strictest(self, other: ReadGate) -> ReadGate {
        match (self, other) {
            (ReadGate::Yes, _) | (_, ReadGate::Yes) => ReadGate::Yes,
            (ReadGate::Until(a), ReadGate::Until(b)) => ReadGate::Until(a.max(b)),
            (ReadGate::Until(t), ReadGate::No) | (ReadGate::No, ReadGate::Until(t)) => {
                ReadGate::Until(t)
            }
            (ReadGate::No, ReadGate::No) => ReadGate::No,
        }
    }
}

/// The type of authentication to use
#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Local { path: ArcStr },
    Krb5 { spn: ArcStr },
    Tls { name: ArcStr, trusted: ArcStr, certificate: ArcStr, private_key: ArcStr },
}

impl Into<resolver::Auth> for Auth {
    fn into(self) -> resolver::Auth {
        match self {
            Self::Anonymous => resolver::Auth::Anonymous,
            Self::Local { path } => resolver::Auth::Local { path },
            Self::Krb5 { spn } => resolver::Auth::Krb5 { spn },
            Self::Tls { name, .. } => resolver::Auth::Tls { name },
        }
    }
}

impl From<file::Auth> for Auth {
    fn from(f: file::Auth) -> Self {
        match f {
            file::Auth::Anonymous => Self::Anonymous,
            file::Auth::Krb5(spn) => Self::Krb5 { spn },
            file::Auth::Local(path) => Self::Local { path },
            file::Auth::Tls { name, trusted, certificate, private_key } => {
                Self::Tls { name, trusted, certificate, private_key }
            }
        }
    }
}

pub(crate) fn check_addrs<T: Clone + Into<resolver::Auth>>(
    a: &Vec<(SocketAddr, T)>,
) -> Result<()> {
    if a.is_empty() {
        bail!("empty addrs")
    }
    for (addr, auth) in a {
        utils::check_addr::<()>(addr.ip(), &[])?;
        match auth.clone().into() {
            resolver::Auth::Anonymous => (),
            resolver::Auth::Local { .. } if !addr.ip().is_loopback() => {
                bail!("local auth is not allowed for a network server")
            }
            resolver::Auth::Local { .. } => (),
            resolver::Auth::Krb5 { spn } => {
                if spn.is_empty() {
                    bail!("spn is required in krb5 mode")
                }
            }
            resolver::Auth::Tls { name } => {
                if name.is_empty() {
                    bail!("name is required in tls mode")
                }
            }
        }
    }
    if !a.iter().all(|(a, _)| a.ip().is_loopback())
        && !a.iter().all(|(a, _)| !a.ip().is_loopback())
    {
        bail!("can't mix loopback addrs with non loopback addrs")
    }
    Ok(())
}

/// The permissions format.
///
/// JSON on disk, because an operator reads and hand-edits it; Pack on the
/// wire, because the admin plane moves it between hosts and nothing there is a
/// document. The bits stay opaque strings here — the resolver compiles them
/// when it builds its runtime map, and `netidx-admin` checks them before it
/// writes — so this type carries the file's shape, not its meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub struct PMap(pub HashMap<ArcStr, HashMap<Entity, Permissions>>);

impl Default for PMap {
    fn default() -> Self {
        PMap(HashMap::new())
    }
}

/// Merge `from` into `into` with entity-level "later wins" semantics:
/// for each `(path, entity)` pair in `from`, replace the value in
/// `into`. Paths only present in `from` are added; paths only in
/// `into` are untouched.
pub fn merge_pmap(into: &mut PMap, from: PMap) {
    for (path, table) in from.0 {
        let entry = into.0.entry(path).or_insert_with(HashMap::new);
        for (entity, perms) in table {
            entry.insert(entity, perms);
        }
    }
}

/// Apply the merge contract: walk `cfg.include_permissions` in order
/// (later wins), then merge the inline `cfg.perms` last so it
/// overrides anything from included files. Returns the merged file-
/// level `PMap`.
///
/// This is the perms-only slice of `Config::from_file`, exposed for
/// the SIGHUP reload path: that path needs the merged perms but
/// **not** the rest of `from_file`'s validation (which opens TLS
/// cert files from disk and re-validates structural fields the
/// running server won't apply live anyway). Splitting the merge
/// out keeps the reload's I/O footprint small and prevents spurious
/// failures from e.g. mid-rotation cert files.
pub fn merge_perms_only(cfg: &file::Config) -> Result<PMap> {
    check_perms_sources(cfg)?;
    let mut merged = load_included_pmap(&cfg.include_permissions)?;
    merge_pmap(&mut merged, cfg.perms.clone());
    Ok(merged)
}

/// Refuse a config that carries both an inline `perms` block and
/// `include_permissions`.
///
/// Inline perms merge last, so they override every included file. An admin
/// server reads and writes only `include_permissions[0]`, which means a
/// resolver configured both ways enforces permissions the admin plane cannot
/// see and cannot change — `netidx admin perms show` would display one thing
/// while the resolver applies another. That is a silent authorization
/// divergence, so it is an error rather than a documented precedence rule.
///
/// Either source alone is fine: inline-only is a hand-managed standalone
/// resolver, includes-only is what the installers produce.
fn check_perms_sources(cfg: &file::Config) -> Result<()> {
    if !cfg.perms.0.is_empty() && !cfg.include_permissions.is_empty() {
        bail!(
            "this resolver config has both an inline `perms` block and \
             `include_permissions` ({:?}). Inline perms override every included \
             file, so the resolver would enforce permissions the admin server \
             cannot see or manage — it reads and writes only the first included \
             file. Move the inline entries into that file (`netidx admin perms \
             edit`) and delete the inline `perms` block; or, for a resolver you \
             manage by hand, remove `include_permissions`.",
            cfg.include_permissions
        )
    }
    Ok(())
}

/// Read and parse a single permissions file in the on-disk `PMap`
/// JSON format. The one place that knows how to load a perms file:
/// `load_included_pmap` and the `netidx-admin` config tooling both go
/// through here rather than re-implementing the read/parse.
pub fn load_perms<P: AsRef<FsPath>>(path: P) -> Result<PMap> {
    let path = path.as_ref();
    let bytes =
        std::fs::read(path).with_context(|| format!("reading perms file {path:?}"))?;
    let pm: PMap = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing perms file {path:?}"))?;
    Ok(pm)
}

/// Load each path in `paths` as a `PMap` and merge them in list order
/// (later wins). Returns the accumulator. Empty list ⇒ empty PMap.
pub(crate) fn load_included_pmap(paths: &[ArcStr]) -> Result<PMap> {
    let mut merged = PMap::default();
    for p in paths {
        let pm = load_perms(p.as_str())
            .with_context(|| format!("loading include_permissions file {p:?}"))?;
        merge_pmap(&mut merged, pm);
    }
    Ok(merged)
}

/// Rewrite relative `include_permissions` entries in `cfg` to absolute
/// paths, joining each against the canonicalized parent directory of
/// `config_path` (the location the config is, or will be, stored at).
/// Absolute entries pass through unchanged. A no-op when there are no
/// relative entries — so it won't fail when `config_path`'s parent
/// doesn't exist yet (e.g. validating a config before its
/// fresh-install directory is created).
///
/// This is the single implementation shared by `Config::load_raw`
/// (runtime load) and the `netidx-admin` editor's pre-save validation,
/// so the two can't drift.
///
/// **Path-traversal posture.** Relative entries are joined with the
/// canonicalized parent directory; the engine does **not** sandbox
/// include paths to remain under the config dir. The config file is
/// operator-trusted, and preventing traversal would break legitimate
/// layouts (e.g. a `/etc/netidx/resolver.json` pointing at
/// `/var/lib/netidx/perms/foo.json`).
pub fn resolve_relative_includes(
    cfg: &mut file::Config,
    config_path: &FsPath,
) -> Result<()> {
    let has_relative =
        cfg.include_permissions.iter().any(|e| FsPath::new(e.as_str()).is_relative());
    if !has_relative {
        return Ok(());
    }
    // Canonicalize the parent so a config given with a relative path
    // like `./resolver.json` still produces absolute include paths
    // (otherwise the joined `./perms.d/main.json` would break after
    // daemonize chdir's the process to `/`).
    let parent = match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => Some(p.to_path_buf()),
        _ => None,
    };
    if let Some(parent) = parent {
        let parent = parent
            .canonicalize()
            .with_context(|| format!("canonicalizing parent dir {parent:?}"))?;
        for entry in cfg.include_permissions.iter_mut() {
            let p = FsPath::new(entry.as_str());
            if p.is_relative() {
                *entry = ArcStr::from(parent.join(p).to_string_lossy().as_ref());
            }
        }
    }
    Ok(())
}

/// The on disk format, encoded as JSON
pub mod file {
    use super::{super::config::check_addrs, PMap, resolver};
    use crate::path::Path;
    use anyhow::Result;
    use arcstr::ArcStr;
    use derive_builder::Builder;
    use netidx_derive::Pack;
    use poolshark::global::GPooled;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
    };

    /// Type of authentication to use.
    ///
    /// `Pack` as well as serde: the admin plane carries a member block from a
    /// host to the CA at enrollment, so the CA can be authoritative for that
    /// host's resolver config afterwards. The TLS variant names *paths*, not
    /// key material.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
    #[serde(deny_unknown_fields)]
    pub enum Auth {
        #[pack(tag(0))]
        Anonymous,
        #[pack(tag(1))]
        Krb5(ArcStr),
        #[pack(tag(2))]
        Local(ArcStr),
        #[pack(tag(3))]
        Tls { name: ArcStr, trusted: ArcStr, certificate: ArcStr, private_key: ArcStr },
    }

    impl Into<resolver::Auth> for Auth {
        fn into(self) -> resolver::Auth {
            match self {
                Self::Anonymous => resolver::Auth::Anonymous,
                Self::Krb5(spn) => resolver::Auth::Krb5 { spn },
                Self::Local(path) => resolver::Auth::Local { path },
                Self::Tls { name, .. } => resolver::Auth::Tls { name },
            }
        }
    }

    /// Type of authentication used by this `Referral`
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
    #[serde(deny_unknown_fields)]
    pub enum RefAuth {
        #[pack(tag(0))]
        Anonymous,
        #[pack(tag(1))]
        Krb5(ArcStr),
        #[pack(tag(2))]
        Local(ArcStr),
        #[pack(tag(3))]
        Tls(ArcStr),
    }

    impl Into<resolver::Auth> for RefAuth {
        fn into(self) -> resolver::Auth {
            match self {
                Self::Anonymous => resolver::Auth::Anonymous,
                Self::Krb5(spn) => resolver::Auth::Krb5 { spn },
                Self::Local(path) => resolver::Auth::Local { path },
                Self::Tls(name) => resolver::Auth::Tls { name },
            }
        }
    }

    /// A referral to another resolver server
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Builder, Pack)]
    #[serde(deny_unknown_fields)]
    pub struct Referral {
        /// The path where the referred cluster attaches to the tree
        #[builder(setter(into))]
        pub path: ArcStr,
        /// The time to live in seconds, default forever
        #[serde(default)]
        #[builder(setter(strip_option), default)]
        pub ttl: Option<u16>,
        /// The addresses of the cluster in this referral
        pub addrs: Vec<(SocketAddr, RefAuth)>,
    }

    impl Referral {
        /// `us` is this cluster's own member addresses; a referral may not
        /// point back at one of them. Only the addresses matter, so the
        /// caller need not carry the auth along with them.
        pub(super) fn check(self, us: Option<&[SocketAddr]>) -> Result<super::Referral> {
            let path = Path::from(self.path);
            if !Path::is_absolute(&path) {
                bail!("absolute server path is required")
            }
            check_addrs(&self.addrs)?;
            if let Some(ttl) = self.ttl {
                if ttl == 0 {
                    bail!("ttl must be non zero");
                }
            }
            if let Some(us) = us {
                for a in us {
                    if self.addrs.iter().any(|(s, _)| s == a) {
                        bail!("server may not be it's own parent");
                    }
                }
            }
            Ok(super::Referral {
                path,
                ttl: self.ttl,
                addrs: GPooled::orphan(
                    self.addrs.into_iter().map(|(s, a)| (s, a.into())).collect(),
                ),
            })
        }
    }

    fn default_bind_addr() -> IpAddr {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }

    /// The type of user id mapping to perform
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
    #[serde(deny_unknown_fields)]
    pub enum IdMapType {
        /// Don't map user ids at all
        DoNotMap,
        /// Run an external command to map ids (such as /bin/id in unix)
        Command,
        /// Send a message to a local socket to map ids
        Socket,
    }

    fn default_id_map_type() -> IdMapType {
        IdMapType::Command
    }

    /// How long a group-membership answer stays good, when the config does
    /// not say.
    ///
    /// An hour, because a lookup is not free and most identities' groups never
    /// change. This is a bound on how long a stale answer can survive
    /// unnoticed, not the means of noticing: the id-map daemon publishes
    /// invalidations and the resolver flushes on them, so a change made
    /// through the admin plane is enforced in seconds regardless of this. It
    /// is what remains for a daemon too old to publish them.
    pub(crate) fn default_id_map_timeout() -> u64 {
        3600
    }

    fn default_hello_timeout() -> u64 {
        10
    }

    fn default_max_connections() -> usize {
        768
    }

    fn default_pid_file() -> PathBuf {
        "".into()
    }

    fn default_reader_ttl() -> u64 {
        60
    }

    fn default_writer_ttl() -> u64 {
        120
    }

    /// Describes a member of the local resolver cluster.
    ///
    /// `Pack` as well as serde: a host hands its own block to the CA at
    /// enrollment, which is what lets the CA render that host's whole resolver
    /// config afterwards rather than patching the topology into a file it does
    /// not otherwise understand.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Builder, Pack)]
    #[serde(deny_unknown_fields)]
    pub struct MemberServer {
        /// The advertised external address and port of this
        /// server. Usually this is the same as bind_addr, but if you
        /// have funky networking arrangements (looking at you aws) it
        /// might not be.
        pub addr: SocketAddr,
        /// The actual address the server will bind to. Usually the
        /// same as the IpAddr in the listen address, but could be
        /// different if you have funky networking going on.
        #[serde(default = "default_bind_addr")]
        pub bind_addr: IpAddr,
        /// The auth mechanism this member server will use. This *can*
        /// be different from other member servers in the cluster,
        /// however that would be a pretty strange choice.
        pub auth: Auth,
        /// How long, in seconds, to wait for a client hello to finish
        /// before closing the connection (default 10 seconds).
        #[serde(default = "default_hello_timeout")]
        #[builder(default = "default_hello_timeout()")]
        pub hello_timeout: u64,
        /// How many simultaneous connections to allow (default 768).
        #[serde(default = "default_max_connections")]
        #[builder(default = "default_max_connections()")]
        pub max_connections: usize,
        /// The name to append to the pid file (default ""). If you
        /// are running more that one server on the same host as the
        /// same user you may need to set this.
        #[serde(default = "default_pid_file")]
        #[builder(setter(into), default = "default_pid_file()")]
        pub pid_file: PathBuf,
        /// How long, in seconds, to keep an idle reader client before
        /// disconnecting (default 60)
        #[serde(default = "default_reader_ttl")]
        #[builder(default = "default_reader_ttl()")]
        pub reader_ttl: u64,
        /// How long, in seconds, to keep an idle writer client before
        /// disconnecting (default 120).
        #[serde(default = "default_writer_ttl")]
        #[builder(default = "default_writer_ttl()")]
        pub writer_ttl: u64,
        /// The command to run to map netidx names to platform
        /// names. The command will be passed the netidx name and must
        /// output the same format as /bin/id on posix platforms. If
        /// not specified a platform default will be chosen.
        #[serde(default)]
        #[builder(setter(into, strip_option), default)]
        pub id_map_command: Option<ArcStr>,
        /// The type of id mapping to perform. Id mapping maps netidx
        /// names to platform names for the purposes of determining
        /// group membership. The default type is to call /bin/id with
        /// the netidx name as an argument and parse it's output. This
        /// will only work on posix platforms, on other platforms a
        /// different mapping type should be chosen.
        #[serde(default = "default_id_map_type")]
        #[builder(default = "default_id_map_type()")]
        pub id_map_type: IdMapType,
        /// How long, in seconds, an identity's group membership stays cached
        /// before it is looked up again.
        ///
        /// Unset takes a default from [`id_map_type`](Self::id_map_type),
        /// because the two sources cost very different amounts to ask and go
        /// stale in very different ways. `Command` forks `/bin/id`, which may
        /// go out to SSSD or AD, and reflects a directory nobody here
        /// administers — an hour. `Socket` is a round trip to a local daemon
        /// holding the map in memory, and that map is edited through the admin
        /// plane, so a long cache means a revocation an operator has watched
        /// converge everywhere is still not being enforced — a minute.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[builder(default)]
        pub id_map_timeout: Option<u64>,
        /// Whether this member refuses read clients (default `No`). Applied
        /// live on reload, so it can be opened or shut without a restart.
        /// See [`super::ReadGate`].
        #[serde(default)]
        #[builder(default)]
        pub read_gated: super::ReadGate,
    }

    /// The toplevel config object
    ///
    /// The config file is expected to contain exactly one of these
    /// encoded as json.
    ///
    /// `Pack` as well as serde: the CA is authoritative for this document and
    /// hands it to the host it belongs to. JSON stays the on-disk form, which
    /// is what an operator reads.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Builder, Pack)]
    #[serde(deny_unknown_fields)]
    pub struct Config {
        /// A list of referrals to child clusters, if any
        #[serde(default)]
        #[builder(default)]
        pub children: Vec<Referral>,
        /// A referral to the parent cluster, if any
        #[serde(default)]
        #[builder(setter(strip_option), default)]
        pub parent: Option<Referral>,
        /// The member servers in this cluster. At least one is required.
        pub member_servers: Vec<MemberServer>,
        /// The permissions on this server (default empty). If this is
        /// not specified the result will depend on the auth type. For
        /// all strong auth types all operations on the server will be
        /// denied. For Anonymous all operations on the server are
        /// always allowed.
        #[serde(default)]
        #[builder(default)]
        pub perms: PMap,
        /// Optional list of additional perms files to merge into
        /// `perms`. Each entry is a filesystem path pointing at a
        /// JSON `PMap`. The resolver loads them in order, merging
        /// later files over earlier ones (entity-level "later wins"
        /// semantics). The inline `perms` field is treated as the
        /// final file in the merge chain, so anything specified
        /// inline always overrides values from included files.
        ///
        /// Empty by default — old configs that only set inline `perms`
        /// are unaffected.
        #[serde(default)]
        #[builder(default)]
        pub include_permissions: Vec<ArcStr>,
    }
}

/// The type of user id mapping to perform
#[derive(Debug, Clone)]
pub enum IdMap {
    DoNotMap,
    PlatformDefault,
    Command(ArcStr),
    Socket(ArcStr),
}

/// Describes a member of the local resolver cluster
#[derive(Debug, Clone)]
pub struct MemberServer {
    pub(super) addr: SocketAddr,
    pub(super) bind_addr: IpAddr,
    pub(super) auth: Auth,
    pub(super) read_gated: ReadGate,
    pub(super) hello_timeout: Duration,
    pub(super) max_connections: usize,
    pub(super) reader_ttl: Duration,
    pub(super) writer_ttl: Duration,
    #[allow(dead_code)]
    pub(crate) id_map: IdMap,
    pub(crate) id_map_timeout: chrono::Duration,
}

/// The toplevel config object
#[derive(Debug, Clone)]
pub struct Config {
    pub(super) parent: Option<Referral>,
    pub(super) children: BTreeMap<Path, Referral>,
    pub(super) perms: PMap,
    pub member_servers: Vec<MemberServer>,
}

/// Validate a config's referrals against the member addresses `us`, returning
/// the parent and the children keyed by where they attach.
///
/// Shared by `Config::from_file` and by the reload path in the running
/// server, so an edit is held to exactly the rules a fresh start would apply.
pub(super) fn check_referrals(
    parent: Option<file::Referral>,
    children: Vec<file::Referral>,
    us: &[SocketAddr],
) -> Result<(Option<Referral>, BTreeMap<Path, Referral>)> {
    let parent = parent.map(|r| r.check(Some(us))).transpose()?;
    let root = parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/");
    let children = children
        .into_iter()
        .map(|r| {
            let r = r.check(Some(us))?;
            Ok((r.path.clone(), r))
        })
        .collect::<Result<BTreeMap<Path, Referral>>>()?;
    for (p, r) in children.iter() {
        // Component-aware containment (matches the overlap check below):
        // `/european` is not under `/eu`, so a byte-prefix test would wrongly
        // accept it as a child of a `/eu`-rooted resolver.
        if !Path::is_parent(root, p) {
            bail!("child paths much be under the root path {}", p)
        }
        if Path::levels(&*p) <= Path::levels(&*root) {
            bail!("child paths must be deeper than the root {}", p);
        }
        let mut res = children.range::<str, (Bound<&str>, Bound<&str>)>((
            Excluded(r.path.as_ref()),
            Unbounded,
        ));
        match res.next() {
            None => (),
            Some((p, _)) => {
                // The immediate successor is the lexicographically smallest
                // path greater than r.path; if r.path is an ancestor of
                // anything, its descendants sort first, so this catches every
                // nested (overlapping) child. Component-aware: `/european` is
                // not below `/eu`.
                if Path::is_parent(&r.path, p) {
                    bail!("can't put a referral {} below {}", p, r.path);
                }
            }
        }
    }
    Ok((parent, children))
}

impl Config {
    /// Translate a file::Config into a validated netidx cluster Config
    pub fn from_file(cfg: file::Config) -> Result<Config> {
        let perms = merge_perms_only(&cfg)?;
        let addrs = cfg
            .member_servers
            .iter()
            .map(|m| (m.addr, m.auth.clone()))
            .collect::<Vec<_>>();
        check_addrs(&addrs)?;
        let member_addrs = addrs.iter().map(|(a, _)| *a).collect::<Vec<_>>();
        let (parent, children) =
            check_referrals(cfg.parent, cfg.children, &member_addrs)?;
        fn check_member_server_auth(m: &file::MemberServer) -> Result<()> {
            match &m.auth {
                file::Auth::Anonymous
                | file::Auth::Krb5 { .. }
                | file::Auth::Local { .. } => Ok(()),
                file::Auth::Tls { name, trusted, certificate, private_key } => {
                    if let Err(e) = tls::load_certs(&trusted) {
                        bail!("failed to load trusted certificates {}", e)
                    }
                    if let Err(e) = tls::load_private_key(None, private_key) {
                        bail!("failed to load the private key {}", e)
                    }
                    match tls::load_certs(&certificate) {
                        Err(e) => bail!("failed to load server certificate {}", e),
                        Ok(cert) => {
                            if cert.len() == 0 || cert.len() > 1 {
                                bail!("certificate should contain exactly 1 cert")
                            }
                            match tls::get_names(&*cert[0])? {
                                None => {
                                    bail!("server certificate has no subjectAltName name")
                                }
                                Some(names) if &names.alt_name != name => {
                                    bail!("name must match the subjectAltName name")
                                }
                                Some(_) => Ok(()),
                            }
                        }
                    }
                }
            }
        }
        fn member_server_from_file(m: file::MemberServer) -> Result<MemberServer> {
            if m.max_connections == 0 {
                bail!("max_connections must be positive")
            }
            if m.reader_ttl == 0 {
                bail!("reader_ttl must be positive")
            }
            if m.writer_ttl == 0 {
                bail!("writer_ttl must be positive")
            }
            if m.hello_timeout == 0 {
                bail!("hello_timeout must be positive")
            }
            check_member_server_auth(&m)?;
            let id_map_timeout = chrono::Duration::seconds(
                m.id_map_timeout.unwrap_or_else(file::default_id_map_timeout) as i64,
            );
            let id_map = match &m.id_map_type {
                IdMapType::DoNotMap => IdMap::DoNotMap,
                IdMapType::Socket => match m.id_map_command {
                    None => bail!("you must specify the socket path as id_map_command"),
                    Some(path) => IdMap::Socket(path),
                },
                IdMapType::Command => match m.id_map_command {
                    None => IdMap::PlatformDefault,
                    Some(cmd) => {
                        if let Err(e) = std::fs::File::open(&*cmd) {
                            bail!("id_map_command error: {}", e)
                        }
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::MetadataExt;
                            if std::fs::metadata(&*cmd)?.mode() & 0o001 == 0 {
                                bail!("id_map_command must be executable")
                            }
                        }
                        // lets pretend the resolver server will someday run on windows
                        #[cfg(windows)]
                        {
                            if !cmd.ends_with(".exe") {
                                bail!("id_map_command must be executable")
                            }
                        }
                        IdMap::Command(cmd)
                    }
                },
            };
            Ok(MemberServer {
                addr: m.addr,
                bind_addr: m.bind_addr,
                auth: m.auth.into(),
                read_gated: m.read_gated,
                hello_timeout: Duration::from_secs(m.hello_timeout),
                max_connections: m.max_connections,
                reader_ttl: Duration::from_secs(m.reader_ttl),
                writer_ttl: Duration::from_secs(m.writer_ttl),
                id_map,
                id_map_timeout,
            })
        }
        let member_servers = cfg
            .member_servers
            .into_iter()
            .map(|m| member_server_from_file(m))
            .collect::<Result<Vec<_>>>()?;
        Ok(Config { parent, children, perms, member_servers })
    }

    /// Parse a file::Config and translate it into a validated netidx cluster Config
    pub fn parse(s: &str) -> Result<Config> {
        Self::from_file(from_str(s)?)
    }

    /// Read and parse a resolver config file in it's raw form.
    ///
    /// This will load the raw resolver config, the only difference between
    /// this function reading the file with serde is that this function will
    /// canonicalize the paths of any included permissions files (see
    /// [`resolve_relative_includes`]).
    /// Also returns the modification time of the descriptor the config was
    /// read from — not of the path, which is a different thing and wrong in
    /// the dangerous direction. See [`crate::config_file`].
    pub fn load_raw<P: AsRef<FsPath>>(
        file: P,
    ) -> Result<(file::Config, Option<std::time::SystemTime>)> {
        let file_path = file.as_ref();
        let (contents, mtime) = crate::config_file::read(file_path)?;
        let mut parsed: file::Config = from_str(&contents)?;
        resolve_relative_includes(&mut parsed, file_path)?;
        Ok((parsed, mtime))
    }

    /// Load the cluster config from the specified file.
    ///
    /// Relative paths in `include_permissions` are resolved against
    /// the config file's parent directory (via [`Config::load_file`]),
    /// so an operator can write
    /// `"include_permissions": ["perms.d/main.json"]` and have it
    /// keep working after `daemonize` chdirs the process to `/`.
    /// Absolute include paths pass through unchanged.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Self::from_file(Self::load_raw(file)?.0)
    }

    pub(super) fn root(&self) -> &str {
        self.parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/")
    }

    /// Borrow the merged file-level permission map (after
    /// `include_permissions` + inline `perms` have been folded
    /// together by `from_file`). Exposed primarily for testing the
    /// merge semantics, including from `netidx-admin`'s cross-crate
    /// template tests.
    pub fn perms(&self) -> &PMap {
        &self.perms
    }
}

#[cfg(test)]
mod perms_merge_tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    fn pmap(pairs: &[(&str, &[(&str, &str)])]) -> PMap {
        let mut top: HashMap<ArcStr, HashMap<ArcStr, ArcStr>> = HashMap::new();
        for (path, entries) in pairs {
            let mut tbl: HashMap<ArcStr, ArcStr> = HashMap::new();
            for (ent, bits) in *entries {
                tbl.insert(ArcStr::from(*ent), ArcStr::from(*bits));
            }
            top.insert(ArcStr::from(*path), tbl);
        }
        PMap(top)
    }

    #[test]
    fn merge_pmap_later_wins() {
        let mut a = pmap(&[("/foo", &[("alice", "swlpd"), ("bob", "sl")])]);
        let b = pmap(&[
            ("/foo", &[("alice", "sl")]), // override alice
            ("/bar", &[("alice", "p")]),  // new path
        ]);
        merge_pmap(&mut a, b);
        assert_eq!(a.0.get("/foo").unwrap().get("alice").unwrap().as_str(), "sl");
        assert_eq!(a.0.get("/foo").unwrap().get("bob").unwrap().as_str(), "sl");
        assert_eq!(a.0.get("/bar").unwrap().get("alice").unwrap().as_str(), "p");
    }

    #[test]
    fn load_included_pmap_in_order() {
        use std::io::Write;
        let a = NamedTempFile::new().unwrap();
        let b = NamedTempFile::new().unwrap();
        writeln!(
            &mut a.as_file(),
            "{}",
            serde_json::to_string(&pmap(&[("/foo", &[("alice", "swlpd")])])).unwrap()
        )
        .unwrap();
        writeln!(
            &mut b.as_file(),
            "{}",
            serde_json::to_string(&pmap(&[("/foo", &[("alice", "sl")])])).unwrap()
        )
        .unwrap();
        let merged = load_included_pmap(&[
            ArcStr::from(a.path().to_string_lossy().as_ref()),
            ArcStr::from(b.path().to_string_lossy().as_ref()),
        ])
        .unwrap();
        // b is later → wins.
        assert_eq!(merged.0.get("/foo").unwrap().get("alice").unwrap().as_str(), "sl");
    }

    /// A config carrying both sources at once. Inline perms would win, and
    /// the admin server manages only the included file, so the two together
    /// are refused rather than silently resolved.
    fn both_sources_cfg(included: &NamedTempFile) -> String {
        use std::io::Write;
        writeln!(
            &mut included.as_file(),
            "{}",
            serde_json::to_string(&pmap(&[("/foo", &[("alice", "swlpd")])])).unwrap()
        )
        .unwrap();
        format!(
            r#"{{
              "member_servers": [
                {{
                  "addr": "127.0.0.1:5001",
                  "bind_addr": "127.0.0.1",
                  "auth": "Anonymous"
                }}
              ],
              "perms": {{ "/foo": {{ "alice": "sl" }} }},
              "include_permissions": [{:?}]
            }}"#,
            included.path().to_string_lossy()
        )
    }

    #[test]
    fn inline_perms_alongside_includes_is_refused() {
        let included = NamedTempFile::new().unwrap();
        let err = Config::parse(&both_sources_cfg(&included)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("inline `perms`"), "unexpected error: {msg}");
        assert!(msg.contains("include_permissions"), "unexpected error: {msg}");
    }

    /// The reload path must refuse it too, or a running server could be
    /// SIGHUP'd into enforcing perms the admin plane cannot see.
    #[test]
    fn inline_perms_alongside_includes_is_refused_on_reload() {
        let included = NamedTempFile::new().unwrap();
        let cfg: file::Config =
            from_str(&both_sources_cfg(&included)).expect("parses as a file::Config");
        let err = merge_perms_only(&cfg).unwrap_err();
        assert!(
            format!("{err:#}").contains("inline `perms`"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn either_perms_source_alone_is_fine() {
        use std::io::Write;
        let included = NamedTempFile::new().unwrap();
        writeln!(
            &mut included.as_file(),
            "{}",
            serde_json::to_string(&pmap(&[("/foo", &[("alice", "swlpd")])])).unwrap()
        )
        .unwrap();
        let includes_only = format!(
            r#"{{
              "member_servers": [
                {{
                  "addr": "127.0.0.1:5001",
                  "bind_addr": "127.0.0.1",
                  "auth": "Anonymous"
                }}
              ],
              "include_permissions": [{:?}]
            }}"#,
            included.path().to_string_lossy()
        );
        let cfg = Config::parse(&includes_only).unwrap();
        assert_eq!(
            cfg.perms.0.get("/foo").unwrap().get("alice").unwrap().as_str(),
            "swlpd",
        );
        // An empty inline `perms` is not "both sources" — it is what serde
        // fills in for a config that never mentioned the field, and what the
        // old starter templates wrote.
        let empty_inline = includes_only
            .replace(r#""include_permissions""#, r#""perms": {}, "include_permissions""#);
        assert!(Config::parse(&empty_inline).is_ok());
    }

    #[test]
    fn missing_include_file_errors_clearly() {
        let raw_cfg = r#"{
          "member_servers": [
            {
              "addr": "127.0.0.1:5001",
              "bind_addr": "127.0.0.1",
              "auth": "Anonymous"
            }
          ],
          "include_permissions": ["/does/not/exist/perms.json"]
        }"#;
        let err = Config::parse(raw_cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("include_permissions"));
    }

    #[test]
    fn relative_include_paths_resolve_against_config_dir() {
        use std::io::Write;
        // A perms file in a subdirectory of the config dir, referenced
        // by a relative path in the main config — like a typical
        // `/etc/netidx/perms.d/main.json` layout.
        let cfg_dir = tempfile::tempdir().unwrap();
        let perms_dir = cfg_dir.path().join("perms.d");
        std::fs::create_dir(&perms_dir).unwrap();
        let perms_file = perms_dir.join("main.json");
        std::fs::write(
            &perms_file,
            serde_json::to_string(&pmap(&[("/foo", &[("alice", "swlpd")])])).unwrap(),
        )
        .unwrap();

        let cfg_file = cfg_dir.path().join("resolver.json");
        let mut f = std::fs::File::create(&cfg_file).unwrap();
        writeln!(
            f,
            r#"{{
              "member_servers": [
                {{
                  "addr": "127.0.0.1:5001",
                  "bind_addr": "127.0.0.1",
                  "auth": "Anonymous"
                }}
              ],
              "include_permissions": ["perms.d/main.json"]
            }}"#,
        )
        .unwrap();
        drop(f);

        let cfg = Config::load(&cfg_file).unwrap();
        assert_eq!(
            cfg.perms.0.get("/foo").unwrap().get("alice").unwrap().as_str(),
            "swlpd",
        );
    }

    #[test]
    fn backwards_compat_no_include_field() {
        let raw_cfg = r#"{
          "member_servers": [
            {
              "addr": "127.0.0.1:5001",
              "bind_addr": "127.0.0.1",
              "auth": "Anonymous"
            }
          ],
          "perms": { "/foo": { "alice": "swlpd" } }
        }"#;
        let cfg = Config::parse(raw_cfg).unwrap();
        assert_eq!(
            cfg.perms.0.get("/foo").unwrap().get("alice").unwrap().as_str(),
            "swlpd",
        );
    }
}

#[cfg(test)]
mod children_overlap_tests {
    use super::*;
    use std::net::SocketAddr;

    fn try_build(children: &[(&str, &str)]) -> anyhow::Result<Config> {
        let member = file::MemberServerBuilder::default()
            .addr("127.0.0.1:4564".parse::<SocketAddr>().unwrap())
            .bind_addr("127.0.0.1".parse::<std::net::IpAddr>().unwrap())
            .auth(file::Auth::Anonymous)
            .build()
            .unwrap();
        let refs = children
            .iter()
            .map(|(p, ip)| file::Referral {
                path: ArcStr::from(*p),
                ttl: None,
                addrs: vec![(
                    format!("{ip}:4564").parse::<SocketAddr>().unwrap(),
                    file::RefAuth::Anonymous,
                )],
            })
            .collect::<Vec<_>>();
        let cfg = file::ConfigBuilder::default()
            .member_servers(vec![member])
            .children(refs)
            .build()
            .unwrap();
        Config::from_file(cfg)
    }

    #[test]
    fn nested_children_are_rejected() {
        // A child nested under another is an ambiguous mount table.
        assert!(
            try_build(&[("/eu", "203.0.113.9"), ("/eu/sub", "203.0.113.10")]).is_err()
        );
        // Insertion order is irrelevant — validation sorts internally.
        assert!(
            try_build(&[("/eu/sub", "203.0.113.10"), ("/eu", "203.0.113.9")]).is_err()
        );
        // Deeper nesting is caught too.
        assert!(
            try_build(&[("/eu", "203.0.113.9"), ("/eu/a/b/c", "203.0.113.11")]).is_err()
        );
    }

    #[test]
    fn disjoint_children_are_accepted() {
        // Distinct sibling subtrees.
        assert!(try_build(&[("/eu", "203.0.113.9"), ("/asia", "203.0.113.10")]).is_ok());
        // A lexical prefix that is NOT a path-component prefix: `/european`
        // is not under `/eu`, so both may be delegated independently.
        assert!(
            try_build(&[("/eu", "203.0.113.9"), ("/european", "203.0.113.10")]).is_ok()
        );
    }

    #[test]
    fn child_must_be_within_parent_subtree() {
        // A mid-tier resolver that is itself delegated /eu (it carries a
        // parent referral rooted at /eu). Its own children must stay
        // within /eu — checked component-aware, not by byte prefix.
        let build = |child_path: &str| -> anyhow::Result<Config> {
            let member = file::MemberServerBuilder::default()
                .addr("127.0.0.1:4564".parse::<SocketAddr>().unwrap())
                .bind_addr("127.0.0.1".parse::<std::net::IpAddr>().unwrap())
                .auth(file::Auth::Anonymous)
                .build()
                .unwrap();
            let referral = |path: &str, ip: &str| file::Referral {
                path: ArcStr::from(path),
                ttl: None,
                addrs: vec![(
                    format!("{ip}:4564").parse::<SocketAddr>().unwrap(),
                    file::RefAuth::Anonymous,
                )],
            };
            let cfg = file::ConfigBuilder::default()
                .member_servers(vec![member])
                .parent(referral("/eu", "203.0.113.99"))
                .children(vec![referral(child_path, "203.0.113.10")])
                .build()
                .unwrap();
            Config::from_file(cfg)
        };
        // Genuinely under /eu — fine.
        assert!(build("/eu/west").is_ok());
        // Component-aware: /eu2/... is NOT under /eu and must be rejected.
        assert!(build("/eu2/x").is_err());
        // A subtree entirely outside the delegated root is rejected.
        assert!(build("/asia/x").is_err());
    }
}

#[cfg(test)]
mod id_map_timeout_tests {
    use super::*;
    use std::net::SocketAddr;

    fn member_with(id_map_type: &file::IdMapType, timeout: Option<u64>) -> Config {
        let mut b = file::MemberServerBuilder::default();
        b.addr("127.0.0.1:4564".parse::<SocketAddr>().unwrap())
            .bind_addr("127.0.0.1".parse::<std::net::IpAddr>().unwrap())
            .auth(file::Auth::Anonymous)
            .id_map_type(id_map_type.clone())
            .id_map_timeout(timeout);
        if matches!(id_map_type, file::IdMapType::Socket) {
            b.id_map_command(arcstr::literal!("/tmp/id-map.sock"));
        }
        Config::from_file(file::Config {
            parent: None,
            children: Vec::new(),
            member_servers: vec![b.build().unwrap()],
            perms: crate::resolver_server::config::PMap::default(),
            include_permissions: Vec::new(),
        })
        .unwrap()
    }

    /// The cache timeout is a backstop, not the means of noticing a change:
    /// the id-map daemon publishes invalidations and the resolver flushes on
    /// them. So it is one number for every source, and a long one.
    #[test]
    fn the_default_timeout_does_not_depend_on_the_source() {
        for source in [file::IdMapType::Socket, file::IdMapType::Command] {
            let c = member_with(&source, None);
            assert_eq!(
                c.member_servers[0].id_map_timeout,
                chrono::Duration::seconds(3600),
                "{source:?}"
            );
        }
    }

    /// An operator who names a timeout gets exactly it.
    #[test]
    fn an_explicit_timeout_is_honoured() {
        let c = member_with(&file::IdMapType::Socket, Some(120));
        assert_eq!(c.member_servers[0].id_map_timeout, chrono::Duration::seconds(120));
    }

    /// An unset timeout is left out of the file rather than written as null,
    /// so a config the CA renders keeps taking whatever the default becomes
    /// instead of pinning today's value into every host's document.
    #[test]
    fn an_unset_timeout_is_not_serialized() {
        let m = file::MemberServerBuilder::default()
            .addr("127.0.0.1:4564".parse::<SocketAddr>().unwrap())
            .bind_addr("127.0.0.1".parse::<std::net::IpAddr>().unwrap())
            .auth(file::Auth::Anonymous)
            .build()
            .unwrap();
        let s = serde_json::to_string(&m).unwrap();
        assert!(!s.contains("id_map_timeout"), "{s}");
    }
}
