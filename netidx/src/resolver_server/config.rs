use crate::{
    path::Path,
    protocol::resolver::{Auth, Referral},
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

pub(crate) fn check_addrs(a: &Vec<(SocketAddr, file::Auth)>) -> Result<()> {
    use file::Auth;
    if a.is_empty() {
        bail!("empty addrs")
    }
    for (addr, auth) in a {
        utils::check_addr(addr.ip(), &[])?;
        if cfg!(not(test)) && addr.port() == 0 {
            bail!("non zero port required {:?}", addr);
        }
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
        }
    }
    if !a.iter().all(|(a, _)| a.ip().is_loopback())
        && !a.iter().all(|(a, _)| !a.ip().is_loopback())
    {
        bail!("can't mix loopback addrs with non loopback addrs")
    }
    Ok(())
}

/// The permissions file format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PMap(pub HashMap<String, HashMap<Entity, Permissions>>);

impl Default for PMap {
    fn default() -> Self {
        PMap(HashMap::new())
    }
}

impl PMap {
    pub(super) fn parse(s: &str) -> Result<PMap> {
        let pmap: PMap = from_str(s)?;
        for p in pmap.0.keys() {
            if !Path::is_absolute(p) {
                bail!("permission paths must be absolute {}", p)
            }
        }
        Ok(pmap)
    }

    pub(super) fn load(file: &str) -> Result<PMap> {
        PMap::parse(&read_to_string(file)?)
    }
}

/// The on disk format, encoded as JSON
pub(crate) mod file {
    use super::super::config::check_addrs;
    use super::PMap;
    use crate::{chars::Chars, path::Path, pool::Pooled};
    use anyhow::Result;
    use std::net::SocketAddr;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(crate) enum Auth {
        Anonymous,
        Krb5(String),
        Local(String),
    }

    impl Into<super::Auth> for Auth {
        fn into(self) -> super::Auth {
            match self {
                Self::Anonymous => super::Auth::Anonymous,
                Self::Local(path) => super::Auth::Local { path: Chars::from(path) },
                Self::Krb5(spn) => super::Auth::Krb5 { spn: Chars::from(spn) },
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Referral {
        path: String,
        ttl: u64,
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
            if self.ttl == 0 {
                bail!("ttl must be non zero");
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

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Config {
        pub(super) parent: Option<Referral>,
        pub(super) children: Vec<Referral>,
        pub(super) pid_file: String,
        pub(super) max_connections: usize,
        pub(super) reader_ttl: u64,
        pub(super) writer_ttl: u64,
        pub(super) hello_timeout: u64,
        pub(super) addr: SocketAddr,
        pub(super) auth: Auth,
        pub(super) perms: PMap,
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub parent: Option<Referral>,
    pub children: BTreeMap<Path, Referral>,
    pub pid_file: String,
    pub max_connections: usize,
    pub reader_ttl: Duration,
    pub writer_ttl: Duration,
    pub hello_timeout: Duration,
    pub addr: SocketAddr,
    pub auth: Auth,
    pub perms: PMap,
}

impl Config {
    pub fn parse(s: &str) -> Result<Config> {
        let cfg: file::Config = from_str(s)?;
        let addrs = vec![(cfg.addr, cfg.auth.clone())];
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
        Ok(Config {
            parent,
            children,
            pid_file: cfg.pid_file,
            max_connections: cfg.max_connections,
            reader_ttl: Duration::from_secs(cfg.reader_ttl),
            writer_ttl: Duration::from_secs(cfg.writer_ttl),
            hello_timeout: Duration::from_secs(cfg.hello_timeout),
            addr: cfg.addr,
            auth: cfg.auth.into(),
            perms: cfg.perms,
        })
    }

    /// Load the cluster config from the specified file.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    pub(super) fn root(&self) -> &str {
        self.parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/")
    }
}
