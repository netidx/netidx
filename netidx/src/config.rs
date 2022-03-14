use crate::{chars::Chars, path::Path, pool::Pooled, utils};
use anyhow::Result;
use fxhash::FxBuildHasher;
use log::debug;
use serde_json::from_str;
use std::{
    cmp::{Eq, PartialEq},
    collections::{
        BTreeMap, Bound,
        Bound::{Excluded, Unbounded},
        HashMap,
    },
    convert::AsRef,
    convert::Into,
    default::Default,
    env,
    fs::read_to_string,
    hash::{Hash, Hasher},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path as FsPath,
    time::Duration,
};

/// The type of authentication used by a resolver server or publisher
#[derive(Debug, Clone)]
pub enum ServerAuth {
    Anonymous,
    Krb5(Pooled<HashMap<SocketAddr, Chars, FxBuildHasher>>),
    Local(String),
}

/// A description of a resolver server
#[derive(Debug, Clone)]
pub struct Server {
    pub path: Path,
    pub ttl: u64,
    pub addrs: Pooled<Vec<SocketAddr>>,
    pub auth: ServerAuth,
}

impl Hash for Server {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.addrs, state)
    }
}

impl PartialEq for Server {
    fn eq(&self, other: &Server) -> bool {
        self.addrs == other.addrs
    }
}

impl Eq for Server {}

impl From<crate::protocol::resolver::Referral> for Server {
    fn from(mut r: crate::protocol::resolver::Referral) -> Server {
        Server {
            path: r.path,
            ttl: r.ttl,
            addrs: r.addrs,
            auth: {
                if r.krb5_spns.is_empty() {
                    ServerAuth::Anonymous
                } else {
                    ServerAuth::Krb5(r.krb5_spns)
                }
            },
        }
    }
}

impl From<client::Config> for Server {
    fn from(c: client::Config) -> Server {
        Server {
            path: c.base,
            ttl: u32::MAX as u64,
            addrs: Pooled::orphan(self.addrs),
            auth: self.auth.into(),
        }
    }
}

fn check_addrs(a: &Vec<SocketAddr>) {
    if self.addrs.is_empty() {
        bail!("empty addrs")
    }
    for addr in &self.addrs {
        utils::check_addr(addr.ip(), &[])?;
        if cfg!(not(test)) && addr.port() == 0 {
            bail!("non zero port required {:?}", addr);
        }
    }
    if !self.addrs.iter().all(|a| a.ip().is_loopback())
        && !self.addrs.iter().all(|a| !a.ip().is_loopback())
    {
        bail!("can't mix loopback addrs with non loopback addrs")
    }
}

/// This is the configuration of a resolver server, either the local
/// machine server, or a network server.
pub mod server {
    use super::*;

    type Permissions = String;
    type Entity = String;

    /// The permissions file format
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PMap(pub HashMap<String, HashMap<Entity, Permissions>>);

    impl Default for PMap {
        fn default() -> Self {
            PMap(HashMap::new())
        }
    }

    impl PMap {
        pub fn parse(s: &str) -> Result<PMap> {
            let pmap: PMap = from_str(s)?;
            for p in pmap.0.keys() {
                if !Path::is_absolute(p) {
                    bail!("permission paths must be absolute {}", p)
                }
            }
            Ok(pmap)
        }

        pub fn load(file: &str) -> Result<PMap> {
            PMap::parse(&read_to_string(file)?)
        }
    }

    /// The on disk format, encoded as JSON
    pub(crate) mod file {
        use crate::{chars::Chars, path::Path, pool::Pooled, utils};
        use anyhow::Result;
        use std::{collections::HashMap, net::SocketAddr};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(crate) enum ServerAuth {
            Anonymous,
            Krb5(HashMap<SocketAddr, String>),
            Local(String),
        }

        impl Into<super::ServerAuth> for ServerAuth {
            fn into(self) -> super::ServerAuth {
                match self {
                    ServerAuth::Anonymous => super::ServerAuth::Anonymous,
                    ServerAuth::Local(s) => super::ServerAuth::Local(s),
                    ServerAuth::Krb5(spns) => super::ServerAuth::Krb5(Pooled::orphan(
                        spns.into_iter().map(|(a, s)| (a, Chars::from(s))).collect(),
                    )),
                }
            }
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Server {
            path: String,
            ttl: u64,
            addrs: Vec<SocketAddr>,
            auth: ServerAuth,
        }

        impl Server {
            pub(super) fn check(
                self,
                us: Option<&Vec<SocketAddr>>,
            ) -> Result<super::Server> {
                let path = Path::from(self.path);
                if !Path::is_absolute(&path) {
                    bail!("absolute server path is required")
                }
                check_addrs(&self.addrs);
                match &self.auth {
                    ServerAuth::Anonymous => (),
                    ServerAuth::Local(_) if !self.addrs[0].ip().is_loopback() => {
                        bail!("local auth is not allowed for a network server")
                    }
                    ServerAuth::Local(_) => (),
                    ServerAuth::Krb5(spns) => {
                        if spns.is_empty() {
                            bail!("at least one SPN is required in krb5 mode")
                        }
                        for a in &self.addrs {
                            if !spns.contains_key(a) {
                                bail!("spn for server {:?} is required", a)
                            }
                        }
                        if spns.len() > self.addrs.len() {
                            bail!("there should be exactly 1 spn for each server address")
                        }
                    }
                }
                if self.ttl == 0 {
                    bail!("ttl must be non zero");
                }
                if let Some(us) = us {
                    for a in us {
                        if self.addrs.contains(a) {
                            bail!("server may not be it's own parent");
                        }
                    }
                }
                Ok(super::Server {
                    path,
                    ttl: self.ttl,
                    addrs: Pooled::orphan(self.addrs),
                    auth: self.auth.into(),
                })
            }
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Config {
            pub(super) parent: Option<Server>,
            pub(super) children: Vec<Server>,
            pub(super) pid_file: String,
            pub(super) max_connections: usize,
            pub(super) reader_ttl: u64,
            pub(super) writer_ttl: u64,
            pub(super) hello_timeout: u64,
            pub(super) addrs: Vec<SocketAddr>,
            pub(super) auth: ServerAuth,
        }
    }

    #[derive(Debug, Clone)]
    pub struct Config {
        pub parent: Option<Server>,
        pub children: BTreeMap<Path, Server>,
        pub pid_file: String,
        pub max_connections: usize,
        pub reader_ttl: Duration,
        pub writer_ttl: Duration,
        pub hello_timeout: Duration,
        pub addrs: Vec<SocketAddr>,
        pub auth: ServerAuth,
    }

    impl Config {
        pub fn parse(s: &str) -> Result<Config> {
            let cfg: file::Config = from_str(s)?;
            check_addrs(&cfg.addrs);
            let addrs = cfg.addrs;
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
                    .collect::<Result<BTreeMap<Path, Server>>>()?;
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
                pid_file: remote.pid_file,
                addrs,
                max_connections: remote.max_connections,
                reader_ttl: Duration::from_secs(remote.reader_ttl),
                writer_ttl: Duration::from_secs(remote.writer_ttl),
                hello_timeout: Duration::from_secs(remote.hello_timeout),
                auth: remote.auth.into(),
            })
        }

        /// Load the cluster config from the specified file.
        pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
            Config::parse(&read_to_string(file)?)
        }
    }
}

/// This is the resolver client configuration, used by subscribers and
/// publishers.
pub mod client {
    use super::*;

    /// The on disk format, encoded as JSON
    mod file {
        use super::*;
        use server::file::ServerAuth;
        use std::net::SocketAddr;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Config {
            pub(super) base: String,
            pub(super) addrs: Vec<SocketAddr>,
            pub(super) auth: ServerAuth,
        }
    }

    #[derive(Debug, Clone)]
    pub struct Config {
        pub base: Path,
        pub addrs: Vec<SocketAddr>,
        pub auth: ServerAuth,
    }

    impl Config {
        pub fn parse(s: &str) -> Result<Config> {
            let cfg: file::Config = from_str(s)?;
            if cfg.addrs.is_empty() {
                bail!("you must specify at least one address");
            }
            for addr in &cfg.addrs {
                utils::check_addr(addr.ip(), &[])?;
            }
            if !cfg.addrs.iter().all(|a| a.ip().is_loopback())
                && !cfg.addrs.iter().all(|a| !a.ip().is_loopback())
            {
                bail!("can't mix loopback addrs with non loopback addrs")
            }
            match &cfg.auth {
                server::file::ServerAuth::Anonymous
                | server::file::ServerAuth::Krb5(_) => (),
                server::file::ServerAuth::Local(_) => {
                    if !cfg.addrs[0].ip().is_loopback() {
                        bail!("local auth is not allowed for remote servers")
                    }
                }
            }
            Ok(Config {
                base: Path::from(cfg.base),
                addrs: cfg.addrs,
                auth: cfg.auth.into(),
            })
        }

        /// Load the cluster config from the specified file.
        pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
            Config::parse(&read_to_string(file)?)
        }

        /// This will try in order,
        ///
        /// * $NETIDX_CFG
        /// * ${dirs::config_dir}/netidx.json
        /// * ${dirs::home_dir}/.netidx.json
        ///
        /// It will load the first file that exists, if that file fails to
        /// load then Err will be returned.
        pub fn load_default() -> Result<Config> {
            if let Some(cfg) = env::var_os("NETIDX_CFG") {
                debug!("loading {}", cfg.to_string_lossy());
                return Config::load(cfg);
            }
            if let Some(mut cfg) = dirs::config_dir() {
                cfg.push("netidx.json");
                debug!("loading {}", cfg.to_string_lossy());
                if cfg.is_file() {
                    return Config::load(cfg);
                }
            }
            if let Some(mut home) = dirs::home_dir() {
                home.push(".netidx.json");
                debug!("loading {}", home.to_string_lossy());
                if home.is_file() {
                    return Config::load(home);
                }
            }
            bail!("no default config file was found")
        }
    }
}
