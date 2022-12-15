use crate::{
    chars::Chars,
    path::Path,
    protocol::resolver::{self, Referral},
    utils,
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
    net::SocketAddr,
    path::Path as FsPath,
    time::Duration,
};

type Permissions = String;
type Entity = String;

#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Local { path: Chars },
    Krb5 { spn: Chars },
    Tls { root_certificates: Chars, certificate: Chars, private_key: Chars },
}

impl Into<resolver::Auth> for Auth {
    fn into(self) -> resolver::Auth {
        match self {
            Self::Anonymous => resolver::Auth::Anonymous,
            Self::Local { path } => resolver::Auth::Local { path },
            Self::Krb5 { spn } => resolver::Auth::Krb5 { spn },
            Self::Tls { .. } => resolver::Auth::Tls,
        }
    }
}

impl From<file::Auth> for Auth {
    fn from(f: file::Auth) -> Self {
        match f {
            file::Auth::Anonymous => Self::Anonymous,
            file::Auth::Krb5(spn) => Self::Krb5 { spn: Chars::from(spn) },
            file::Auth::Local(path) => Self::Local { path: Chars::from(path) },
            file::Auth::Tls { root_certificates, certificate, private_key } => {
                Self::Tls {
                    root_certificates: Chars::from(root_certificates),
                    certificate: Chars::from(certificate),
                    private_key: Chars::from(private_key),
                }
            }
        }
    }
}

pub(crate) fn check_addrs(a: &Vec<(SocketAddr, file::Auth)>) -> Result<()> {
    use file::Auth;
    if a.is_empty() {
        bail!("empty addrs")
    }
    for (addr, auth) in a {
        utils::check_addr::<()>(addr.ip(), &[])?;
        match auth {
            Auth::Anonymous => (),
            Auth::Local(_) if !addr.ip().is_loopback() => {
                bail!("local auth is not allowed for a network server")
            }
            Auth::Local(_) => (),
            Auth::Krb5(spn) => {
                if spn.is_empty() {
                    bail!("spn is required in krb5 mode")
                }
            }
            // CR estokes: verify the certificates
            Auth::Tls { root_certificates: _, certificate: _, private_key: _ } => (),
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
pub(crate) mod file {
    use super::super::config::check_addrs;
    use super::PMap;
    use crate::{path::Path, pool::Pooled};
    use anyhow::Result;
    use std::net::SocketAddr;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) enum Auth {
        Anonymous,
        Krb5(String),
        Local(String),
        Tls { root_certificates: String, certificate: String, private_key: String },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Referral {
        path: String,
        ttl: Option<u16>,
        addrs: Vec<(SocketAddr, Auth)>,
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
                    self.addrs
                        .into_iter()
                        .map(|(s, a)| (s, super::Auth::from(a).into()))
                        .collect(),
                ),
            })
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct MemberServer {
        pub(super) addr: SocketAddr,
        pub(super) auth: Auth,
        pub(super) hello_timeout: u64,
        pub(super) max_connections: usize,
        pub(super) pid_file: String,
        pub(super) reader_ttl: u64,
        pub(super) writer_ttl: u64,
        pub(super) id_map: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Config {
        pub(super) children: Vec<Referral>,
        pub(super) parent: Option<Referral>,
        pub(super) member_servers: Vec<MemberServer>,
        pub(super) perms: PMap,
    }
}

#[derive(Debug, Clone)]
pub struct MemberServer {
    pub(super) addr: SocketAddr,
    pub(super) auth: Auth,
    pub(super) hello_timeout: Duration,
    pub(super) max_connections: usize,
    pub pid_file: String,
    pub(super) reader_ttl: Duration,
    pub(super) writer_ttl: Duration,
    pub(super) id_map: String,
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
                    auth: m.auth.into_iter().map(|a| a.into()).collect(),
                    hello_timeout: Duration::from_secs(m.hello_timeout),
                    max_connections: m.max_connections,
                    reader_ttl: Duration::from_secs(m.reader_ttl),
                    writer_ttl: Duration::from_secs(m.writer_ttl),
                    id_map: m.id_map,
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
