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
    default::Default,
    fs::read_to_string,
    net::SocketAddr,
    path::Path as FsPath,
    time::Duration,
};

pub(crate) mod file {
    use super::Auth;
    use crate::{
        chars::Chars, path::Path, protocol::resolver::v1::Referral as Pref, utils,
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
                addrs: self.addrs,
                krb5_spns: self
                    .krb5_spns
                    .into_iter()
                    .map(|(a, s)| (a, Chars::from(s)))
                    .collect(),
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

fn lookup_txt(name: &str) -> Result<String> {
    use trust_dns_resolver::Resolver;
    let resolver = Resolver::from_system_conf()?;
    let mut s = String::new();
    if let Some(txt) = resolver.txt_lookup(name)?.iter().next() {
        for data in txt.txt_data() {
            s.push_str(&*String::from_utf8_lossy(&*data))
        }
    }
    Ok(s)
}

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

    pub fn load_from_file(file: &str) -> Result<PMap> {
        PMap::parse(&read_to_string(file)?)
    }

    /// If you wish you can store your netindex permission map in a
    /// DNS TXT record. The default name is "netindex-permissions"
    /// where <your-fqdn> is replaced with the fully qualified domain
    /// name of the machine this function is run on. Or you can
    /// specify a custom name.
    pub fn load_from_dns(name: Option<&str>) -> Result<PMap> {
        PMap::parse(&lookup_txt(name.unwrap_or("netindex-permissions"))?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Auth {
    Anonymous,
    Krb5(HashMap<SocketAddr, String>),
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

impl From<Referral> for Config {
    fn from(r: Referral) -> Config {
        // not for use with a server
        Config {
            parent: None,
            children: BTreeMap::new(),
            pid_file: String::new(),
            max_connections: 768,
            reader_ttl: Duration::from_secs(120),
            writer_ttl: Duration::from_secs(600),
            hello_timeout: Duration::from_secs(10),
            addrs: r.addrs,
            auth: {
                if r.krb5_spns.is_empty() {
                    Auth::Anonymous
                } else {
                    Auth::Krb5(
                        r.krb5_spns.into_iter().map(|(k, v)| (k, v.into())).collect(),
                    )
                }
            },
        }
    }
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
            if cfg!(not(test)) && addr.port() == 0 {
                bail!("You must specify a non zero port {:?}", addr);
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

    pub fn load_from_file<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    /// If you wish to have a network wide netindex cluster
    /// configuration without the need to sync config files between
    /// machines then you may store the configuration in a DNS TXT
    /// record. If you specify a name, then this function will try to
    /// find the config at that name, you may need this for a complex
    /// configuration, for example if you have multiple clusters in a
    /// hierarchy but only one DNS domain. If no name is specified
    /// then the default name is, "netindex".
    pub fn load_from_dns(name: Option<&str>) -> Result<Config> {
        Config::parse(&lookup_txt(name.unwrap_or("netindex"))?)
    }
}
