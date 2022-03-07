use crate::{
    chars::Chars, path::Path, pool::Pooled, protocol::resolver::Referral, utils,
};
use anyhow::Result;
use fxhash::FxBuildHasher;
use log::debug;
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
    env,
    fs::read_to_string,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path as FsPath,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Auth {
    Anonymous,
    Krb5(HashMap<SocketAddr, String>),
    Local(String),
}

pub mod server {
    use crossbeam::epoch::Pointable;

    use super::*;

    type Permissions = String;
    type Entity = String;

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

    pub(crate) mod file {
        use super::Auth;
        use crate::{
            chars::Chars, path::Path, pool::Pooled, protocol::resolver::Referral as Pref,
            utils,
        };
        use anyhow::Result;
        use std::{collections::HashMap, net::SocketAddr};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Referral {
            path: String,
            ttl: u64,
            addrs: Vec<SocketAddr>,
            krb5_spns: HashMap<SocketAddr, String>,
        }

        impl Referral {
            pub(super) fn check(self, us: Option<&Vec<SocketAddr>>) -> Result<Pref> {
                let path = Path::from(self.path);
                if !Path::is_absolute(&path) {
                    bail!("absolute referral path is required")
                }
                if self.addrs.is_empty() {
                    bail!("empty referral addrs")
                }
                for addr in &self.addrs {
                    utils::check_addr(addr.ip(), &[])?;
                    if cfg!(not(test)) && addr.port() == 0 {
                        bail!("non zero port required {:?}", addr);
                    }
                }
                if !self.krb5_spns.is_empty() {
                    for a in &self.addrs {
                        if !self.krb5_spns.contains_key(a) {
                            bail!("spn for server {:?} is required", a)
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
                Ok(Pref {
                    path,
                    ttl: self.ttl,
                    addrs: Pooled::orphan(self.addrs),
                    krb5_spns: Pooled::orphan(
                        self.krb5_spns
                            .into_iter()
                            .map(|(a, s)| (a, Chars::from(s)))
                            .collect(),
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
            pub(super) addrs: Vec<SocketAddr>,
            pub(super) auth: Auth,
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
        pub addrs: Vec<SocketAddr>,
        pub auth: Auth,
    }

    impl Config {
        pub fn root(&self) -> &str {
            self.parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/")
        }

        pub fn parse(s: &str) -> Result<Config> {
            let cfg: file::Config = from_str(s)?;
            if cfg.addrs.len() < 1 {
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
            if cfg.addrs.iter().any(|a| a.ip().is_loopback()) {
                if !cfg.children.is_empty() || cfg.parent.is_some() {
                    bail!("local servers may not have parents or children")
                }
            }
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
                addrs,
                max_connections: cfg.max_connections,
                reader_ttl: Duration::from_secs(cfg.reader_ttl),
                writer_ttl: Duration::from_secs(cfg.writer_ttl),
                hello_timeout: Duration::from_secs(cfg.hello_timeout),
                auth: cfg.auth,
            })
        }

        /// Load the cluster config from the specified file.
        pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
            Config::parse(&read_to_string(file)?)
        }
    }
}

pub mod client {
    use super::*;

    mod file {
        use super::Auth;
        use std::net::SocketAddr;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Local {
            pub(super) port: u16,
            pub(super) auth: Auth,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Config {
            pub(super) local: Option<Local>,
            pub(super) addrs: Vec<SocketAddr>,
            pub(super) auth: Auth,
        }
    }

    pub struct Local {
        port: u16,
        auth: Auth,
    }

    pub struct Config {
        pub local: Option<Local>,
        pub addrs: Vec<SocketAddr>,
        pub auth: Auth,
    }

    impl Config {
        pub fn parse(s: &str) -> Result<Config> {
            let cfg: file::Config = from_str(s)?;
            if cfg.addrs.len() < 1 || cfg.local.is_some() {
                bail!("you must specify at least one address, or local machine server");
            }
            for addr in &cfg.addrs {
                utils::check_addr(addr.ip(), &[])?;
            }
            if !cfg.addrs.iter().all(|a| a.ip().is_loopback())
                && !cfg.addrs.iter().all(|a| !a.ip().is_loopback())
            {
                bail!("can't mix loopback addrs with non loopback addrs")
            }
            Ok(Config {
                local: cfg
                    .local
                    .take()
                    .map(|local| Local { port: local.port, auth: local.auth }),
                addrs: cfg.addrs,
                auth: cfg.auth,
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

        /// return a referral to the local machine resolver, if it exists
        pub fn to_local(self) -> Option<Referral> {
            self.local.take().map(|local| {
                let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
                let port = self.local.unwrap().port;
                let addrs = Vec::from([SocketAddr::new(ip, port)]);
                Referral {
                    path: Path::from("/"),
                    ttl: u32::MAX as u64,
                    addrs: Pooled::orphan(addrs),
                    krb5_spns: match local.auth {
                        Auth::Local(_) | Auth::Anonymous => {
                            Pooled::orphan(HashMap::with_hasher(FxBuildHasher::default()))
                        }
                        Auth::Krb5(mut spns) => {
                            let mut h = Pooled::orphan(HashMap::with_hasher(
                                FxBuildHasher::default(),
                            ));
                            h.extend(spns.drain().map(|(k, v)| (k, Chars::from(v))));
                            h
                        }
                    },
                }
            })
        }

        /// return a referral to the remote resolver, if it exists
        pub fn to_remote(self) -> Option<Referral> {
            if self.addrs.is_empty() {
                None
            } else {
                Some(Referral {
                    path: Path::from("/"),
                    ttl: u32::MAX as u64,
                    addrs: Pooled::orphan(self.addrs),
                    krb5_spns: match self.auth {
                        Auth::Local(_) | Auth::Anonymous => {
                            Pooled::orphan(HashMap::with_hasher(FxBuildHasher::default()))
                        }
                        Auth::Krb5(mut spns) => {
                            let mut h = Pooled::orphan(HashMap::with_hasher(
                                FxBuildHasher::default(),
                            ));
                            h.extend(spns.drain().map(|(k, v)| (k, Chars::from(v))));
                            h
                        }
                    },
                })
            }
        }
    }

    impl From<Referral> for Config {
        fn from(mut r: Referral) -> Config {
            Config {
                local: None,
                addrs: r.addrs.detach(),
                auth: {
                    if r.krb5_spns.is_empty() {
                        Auth::Anonymous
                    } else {
                        Auth::Krb5(
                            r.krb5_spns.drain().map(|(k, v)| (k, v.into())).collect(),
                        )
                    }
                },
            }
        }
    }
}
