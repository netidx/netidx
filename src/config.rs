mod common_file {
    use crate::{
        chars::Chars, path::Path, protocol::resolver::v1::Referral as Pref, utils,
    };
    use anyhow::Result;
    use std::{collections::HashMap, net::SocketAddr};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Referral {
        pub(super) path: String,
        pub(super) ttl: u64,
        pub(super) addrs: Vec<SocketAddr>,
        pub(super) krb5_spns: HashMap<SocketAddr, String>,
    }

    impl Referral {
        pub(super) fn check(self, us: Option<SocketAddr>) -> Result<Pref> {
            let path = Path::from(self.path);
            if !Path::is_absolute(&path) {
                bail!("absolute referral path is required")
            }
            if self.addrs.is_empty() {
                bail!("empty referral addrs")
            }
            for addr in &self.addrs {
                utils::check_addr(addr.ip(), &[])?;
                if addr.port() == 0 && cfg!(not(test)) {
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
            if let Some(ref us) = us {
                if self.addrs.contains(&us) {
                    bail!("server may not be it's own parent");
                }
            }
            Ok(Pref {
                path,
                ttl: self.ttl,
                addrs: self.addrs,
                krb5_spns: self
                    .krb5_spns
                    .into_iter()
                    .map(|(a, s)| (a, Chars::from(s)))
                    .collect(),
            })
        }
    }
}

pub mod resolver_server {
    use crate::{path::Path, protocol::resolver::v1::Referral, utils};
    use anyhow::Result;
    use serde_json::from_str;
    use std::{
        collections::{
            BTreeMap, Bound,
            Bound::{Excluded, Unbounded},
            HashMap,
        },
        convert::AsRef,
        fs::read_to_string,
        net::SocketAddr,
        path::Path as FsPath,
        time::Duration,
    };

    mod file {
        use super::super::common_file::Referral;
        use std::net::SocketAddr;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) enum Auth {
            Anonymous,
            Krb5 { spn: String, permissions: String },
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Config {
            pub(super) parent: Option<Referral>,
            pub(super) children: Vec<Referral>,
            pub(super) pid_file: String,
            pub(super) addr: SocketAddr,
            pub(super) max_connections: usize,
            pub(super) reader_ttl: u64,
            pub(super) writer_ttl: u64,
            pub(super) hello_timeout: u64,
            pub(super) auth: Auth,
        }
    }

    type Permissions = String;
    type Entity = String;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PMap(pub HashMap<String, HashMap<Entity, Permissions>>);

    #[derive(Debug, Clone)]
    pub enum Auth {
        Anonymous,
        Krb5 { spn: String, permissions: PMap },
    }

    #[derive(Debug, Clone)]
    pub struct Config {
        pub parent: Option<Referral>,
        pub children: BTreeMap<Path, Referral>,
        pub pid_file: String,
        pub addr: SocketAddr,
        pub max_connections: usize,
        pub reader_ttl: Duration,
        pub writer_ttl: Duration,
        pub hello_timeout: Duration,
        pub auth: Auth,
    }

    impl Config {
        pub fn root(&self) -> &str {
            self.parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/")
        }

        pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
            let cfg: file::Config = from_str(&read_to_string(file)?)?;
            utils::check_addr(cfg.addr.ip(), &[])?;
            let addr = cfg.addr;
            if addr.port() == 0 {
                bail!("You must specify a non zero port {:?}", addr);
            }
            let parent = cfg.parent.map(|r| r.check(Some(addr))).transpose()?;
            let children = {
                let root = parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/");
                let children = cfg
                    .children
                    .into_iter()
                    .map(|r| {
                        let r = r.check(Some(addr))?;
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
            let auth = match cfg.auth {
                file::Auth::Anonymous => Auth::Anonymous,
                file::Auth::Krb5 { spn, permissions } => {
                    let permissions: PMap = from_str(&read_to_string(&permissions)?)?;
                    Auth::Krb5 { spn, permissions }
                }
            };
            Ok(Config {
                parent,
                children,
                pid_file: cfg.pid_file,
                addr,
                max_connections: cfg.max_connections,
                reader_ttl: Duration::from_secs(cfg.reader_ttl),
                writer_ttl: Duration::from_secs(cfg.writer_ttl),
                hello_timeout: Duration::from_secs(cfg.hello_timeout),
                auth,
            })
        }
    }
}

pub mod resolver {
    use crate::{protocol::resolver::v1::Referral, utils};
    use anyhow::Result;
    use fxhash::FxBuildHasher;
    use serde_json::from_str;
    use std::{
        collections::HashMap, convert::AsRef, convert::From, fs, net::SocketAddr,
        path::Path,
    };
    use tokio::fs::read_to_string;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Config {
        pub addrs: Vec<SocketAddr>,
        pub krb5_spns: HashMap<SocketAddr, String, FxBuildHasher>,
    }

    impl From<Referral> for Config {
        fn from(r: Referral) -> Self {
            Config {
                addrs: r.addrs,
                krb5_spns: r.krb5_spns.into_iter().map(|(k, v)| (k, v.into())).collect(),
            }
        }
    }

    impl Config {
        fn check(&self) -> Result<()> {
            for addr in &self.addrs {
                utils::check_addr(addr.ip(), &[])?;
                if addr.port() == 0 && cfg!(not(test)) {
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
            Ok(())
        }

        pub async fn load_async<P: AsRef<Path>>(file: P) -> Result<Config> {
            let cfg: Config = from_str(&read_to_string(file).await?)?;
            cfg.check()?;
            Ok(cfg)
        }

        pub fn load<P: AsRef<Path>>(file: P) -> Result<Config> {
            let cfg: Config = from_str(&fs::read_to_string(file)?)?;
            cfg.check()?;
            Ok(cfg)
        }
    }
}
