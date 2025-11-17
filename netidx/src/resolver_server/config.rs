//! Resolver server configuration
//!
//! See the file module for documentation of the on disk format.

use self::file::IdMapType;
use crate::{
    path::Path,
    protocol::resolver::{self, Referral},
    tls, utils,
};
use anyhow::Result;
use arcstr::ArcStr;
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
    fs::read_to_string,
    net::{IpAddr, SocketAddr},
    path::Path as FsPath,
    time::Duration,
};

type Permissions = ArcStr;
type Entity = ArcStr;

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
            // CR estokes: verify the certificates
            resolver::Auth::Tls { .. } => (),
        }
    }
    if !a.iter().all(|(a, _)| a.ip().is_loopback())
        && !a.iter().all(|(a, _)| !a.ip().is_loopback())
    {
        bail!("can't mix loopback addrs with non loopback addrs")
    }
    Ok(())
}

/// The permissions format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PMap(pub HashMap<ArcStr, HashMap<Entity, Permissions>>);

impl Default for PMap {
    fn default() -> Self {
        PMap(HashMap::new())
    }
}

/// The on disk format, encoded as JSON
pub mod file {
    use super::{super::config::check_addrs, resolver, PMap};
    use crate::path::Path;
    use anyhow::Result;
    use arcstr::ArcStr;
    use derive_builder::Builder;
    use poolshark::global::GPooled;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
    };

    /// Type of authentication to use
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum Auth {
        Anonymous,
        Krb5(ArcStr),
        Local(ArcStr),
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
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum RefAuth {
        Anonymous,
        Krb5(ArcStr),
        Local(ArcStr),
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
    #[derive(Debug, Clone, Serialize, Deserialize, Builder)]
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
        pub(super) fn check(
            self,
            us: Option<&Vec<(SocketAddr, Auth)>>,
        ) -> Result<super::Referral> {
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
                for (a, _) in us {
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
    #[derive(Debug, Clone, Serialize, Deserialize)]
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

    fn default_id_map_timeout() -> u64 {
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

    /// Describes a member of the local resolver cluster
    #[derive(Debug, Clone, Serialize, Deserialize, Builder)]
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
        /// How long, in seconds, to wait for the id map command or socket to
        /// return an answer for a given user. If the timeout expires
        /// the request will be denied. (default 3600)
        #[serde(default = "default_id_map_timeout")]
        #[builder(default = "default_id_map_timeout()")]
        pub id_map_timeout: u64,
    }

    /// The toplevel config object
    ///
    /// The config file is expected to contain exactly one of these
    /// encoded as json.
    #[derive(Debug, Clone, Serialize, Deserialize, Builder)]
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

impl Config {
    /// Translate a file::Config into a validated netidx cluster Config
    pub fn from_file(cfg: file::Config) -> Result<Config> {
        let addrs = cfg
            .member_servers
            .iter()
            .map(|m| (m.addr, m.auth.clone()))
            .collect::<Vec<_>>();
        check_addrs(&addrs)?;
        let parent = cfg.parent.map(|r| r.check(Some(&addrs))).transpose()?;
        let children = {
            let root = parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/");
            let children = cfg
                .children
                .into_iter()
                .map(|r| {
                    let r = r.check(Some(&addrs))?;
                    Ok((r.path.clone(), r))
                })
                .collect::<Result<BTreeMap<Path, Referral>>>()?;
            for (p, r) in children.iter() {
                if !p.starts_with(&*root) {
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
                        if r.path.starts_with(p.as_ref()) {
                            bail!("can't put a referral {} below {}", p, r.path);
                        }
                    }
                }
            }
            children
        };
        let member_servers = cfg
            .member_servers
            .into_iter()
            .map(|m| {
		let id_map = match &m.id_map_type {
		    IdMapType::DoNotMap => IdMap::DoNotMap,
		    IdMapType::Socket => match m.id_map_command {
			None => bail!("you must specify the socket path as id_map_command"),
			Some(path) => IdMap::Socket(path),
		    }
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
		    }
		};
                match &m.auth {
                    file::Auth::Anonymous
                    | file::Auth::Krb5 { .. }
                    | file::Auth::Local { .. } => (),
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
                                    Some(_) => (),
                                }
                            }
                        }
                    }
                }
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
                Ok(MemberServer {
                    addr: m.addr,
                    bind_addr: m.bind_addr,
                    auth: m.auth.into(),
                    hello_timeout: Duration::from_secs(m.hello_timeout),
                    max_connections: m.max_connections,
                    reader_ttl: Duration::from_secs(m.reader_ttl),
                    writer_ttl: Duration::from_secs(m.writer_ttl),
                    id_map,
		    id_map_timeout: chrono::Duration::seconds(m.id_map_timeout as i64),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Config { parent, children, perms: cfg.perms, member_servers })
    }

    /// Parse a file::Config and translate it into a validated netidx cluster Config
    pub fn parse(s: &str) -> Result<Config> {
        Self::from_file(from_str(s)?)
    }

    /// Load the cluster config from the specified file.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    pub(super) fn root(&self) -> &str {
        self.parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/")
    }
}
