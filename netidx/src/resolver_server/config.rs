use crate::{
    chars::Chars,
    path::Path,
    protocol::resolver::{self, Referral},
    tls, utils,
};
use anyhow::Result;
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
use self::file::IdMapType;

type Permissions = String;
type Entity = String;

#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Local { path: Chars },
    Krb5 { spn: Chars },
    Tls { name: Chars, trusted: Chars, certificate: Chars, private_key: Chars },
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
            file::Auth::Krb5(spn) => Self::Krb5 { spn: Chars::from(spn) },
            file::Auth::Local(path) => Self::Local { path: Chars::from(path) },
            file::Auth::Tls { name, trusted, certificate, private_key } => Self::Tls {
                name: Chars::from(name),
                trusted: Chars::from(trusted),
                certificate: Chars::from(certificate),
                private_key: Chars::from(private_key),
            },
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
pub(super) struct PMap(pub HashMap<String, HashMap<Entity, Permissions>>);

impl Default for PMap {
    fn default() -> Self {
        PMap(HashMap::new())
    }
}

/// The on disk format, encoded as JSON
pub mod file {
    use super::{super::config::check_addrs, resolver, Chars, PMap};
    use crate::{path::Path, pool::Pooled};
    use anyhow::Result;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) enum Auth {
        Anonymous,
        Krb5(String),
        Local(String),
        Tls { name: String, trusted: String, certificate: String, private_key: String },
    }

    impl Into<resolver::Auth> for Auth {
        fn into(self) -> resolver::Auth {
            match self {
                Self::Anonymous => resolver::Auth::Anonymous,
                Self::Krb5(spn) => resolver::Auth::Krb5 { spn: Chars::from(spn) },
                Self::Local(path) => resolver::Auth::Local { path: Chars::from(path) },
                Self::Tls { name, .. } => resolver::Auth::Tls { name: Chars::from(name) },
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) enum RefAuth {
        Anonymous,
        Krb5(String),
        Local(String),
        Tls(String),
    }

    impl Into<resolver::Auth> for RefAuth {
        fn into(self) -> resolver::Auth {
            match self {
                Self::Anonymous => resolver::Auth::Anonymous,
                Self::Krb5(spn) => resolver::Auth::Krb5 { spn: Chars::from(spn) },
                Self::Local(path) => resolver::Auth::Local { path: Chars::from(path) },
                Self::Tls(name) => resolver::Auth::Tls { name: Chars::from(name) },
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct Referral {
        path: String,
        #[serde(default)]
        ttl: Option<u16>,
        addrs: Vec<(SocketAddr, RefAuth)>,
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
                addrs: Pooled::orphan(
                    self.addrs.into_iter().map(|(s, a)| (s, a.into())).collect(),
                ),
            })
        }
    }

    fn default_bind_addr() -> IpAddr {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) enum IdMapType {
        DoNotMap,
        Command,
    }

    fn default_id_map_type() -> IdMapType {
        IdMapType::Command
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct MemberServer {
        pub(super) addr: SocketAddr,
        #[serde(default = "default_bind_addr")]
        pub(super) bind_addr: IpAddr,
        pub(super) auth: Auth,
        pub(super) hello_timeout: u64,
        pub(super) max_connections: usize,
        pub(super) pid_file: String,
        pub(super) reader_ttl: u64,
        pub(super) writer_ttl: u64,
        #[serde(default)]
        pub(super) id_map_command: Option<String>,
        #[serde(default = "default_id_map_type")]
        pub(super) id_map_type: IdMapType,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct Config {
        pub(super) children: Vec<Referral>,
        pub(super) parent: Option<Referral>,
        pub(super) member_servers: Vec<MemberServer>,
        pub(super) perms: PMap,
    }
}

#[derive(Debug, Clone)]
pub enum IdMap {
    DoNotMap,
    PlatformDefault,
    Command(String),
}

#[derive(Debug, Clone)]
pub struct MemberServer {
    pub(super) addr: SocketAddr,
    pub(super) bind_addr: IpAddr,
    pub(super) auth: Auth,
    pub(super) hello_timeout: Duration,
    pub(super) max_connections: usize,
    pub pid_file: String,
    pub(super) reader_ttl: Duration,
    pub(super) writer_ttl: Duration,
    #[allow(dead_code)]
    pub(crate) id_map: IdMap,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub(super) parent: Option<Referral>,
    pub(super) children: BTreeMap<Path, Referral>,
    pub(super) perms: PMap,
    pub member_servers: Vec<MemberServer>,
}

impl Config {
    pub fn parse(s: &str) -> Result<Config> {
        let cfg: file::Config = from_str(s)?;
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
		    IdMapType::Command => match m.id_map_command {
			None => IdMap::PlatformDefault,
			Some(cmd) => {
			    if let Err(e) = std::fs::File::open(&cmd) {
				bail!("id_map_command error: {}", e)
			    }
			    #[cfg(unix)]
			    {
				use std::os::unix::fs::MetadataExt;
				if std::fs::metadata(&cmd)?.mode() & 0o001 == 0 {
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
                                match tls::get_names(&cert[0].0)? {
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
                    pid_file: m.pid_file,
                    addr: m.addr,
                    bind_addr: m.bind_addr,
                    auth: m.auth.into(),
                    hello_timeout: Duration::from_secs(m.hello_timeout),
                    max_connections: m.max_connections,
                    reader_ttl: Duration::from_secs(m.reader_ttl),
                    writer_ttl: Duration::from_secs(m.writer_ttl),
                    id_map,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Config { parent, children, perms: cfg.perms, member_servers })
    }

    /// Load the cluster config from the specified file.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    pub(super) fn root(&self) -> &str {
        self.parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/")
    }
}
