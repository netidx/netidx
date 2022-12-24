use crate::{
    path::Path,
    pool::Pooled,
    protocol::resolver::{Auth, Referral},
    subscriber::DesiredAuth,
    utils,
};
use anyhow::Result;
use arcstr::ArcStr;
use log::debug;
use serde_json::from_str;
use std::{
    cmp::min,
    collections::BTreeMap,
    convert::AsRef,
    convert::Into,
    env,
    fs::read_to_string,
    mem,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    str,
};

/// The on disk format, encoded as JSON
mod file {
    use crate::chars::Chars;
    use std::net::SocketAddr;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) enum Auth {
        Anonymous,
        Krb5(String),
        Local(String),
        Tls(String),
    }

    impl Into<crate::protocol::resolver::Auth> for Auth {
        fn into(self) -> crate::protocol::resolver::Auth {
            use crate::protocol::resolver::Auth as A;
            match self {
                Self::Anonymous => A::Anonymous,
                Self::Krb5(spn) => A::Krb5 { spn: Chars::from(spn) },
                Self::Local(path) => A::Local { path: Chars::from(path) },
                Self::Tls(name) => A::Tls { name: Chars::from(name) },
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Config {
        pub(super) base: String,
        pub(super) addrs: Vec<(SocketAddr, Auth)>,
        #[serde(default)]
        pub(super) tls: Option<super::Tls>,
        #[serde(default)]
        pub(super) default_auth: super::DefaultAuthMech,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsIdentity {
    pub certificate: String,
    pub private_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tls {
    pub ca_certs: String,
    pub identities: BTreeMap<String, TlsIdentity>,
    pub agent: Option<String>,
}

impl Tls {
    pub(crate) fn reverse_domain_name(name: &mut String) -> usize {
        const MAX: usize = 1024;
        let mut tmp = [0u8; MAX + 1];
        let mut i = 0;
        let mut parts = 0;
        for part in name[0..min(name.len(), MAX)].split('.').rev() {
            dbg!(part);
            tmp[i..i + part.len()].copy_from_slice(part.as_bytes());
            tmp[i + part.len()] = '.' as u8;
            i += part.len() + 1;
            parts += 1;
        }
        name.clear();
        name.push_str(str::from_utf8(&mut tmp[0..i]).unwrap());
        parts
    }

    // Identities are domain names (e.g. the common name in the certificate).
    // We allow the user to enter them in forward order, but we then reverse them
    // so we can use the closest match to find the right identity for a publisher
    // or resolver server
    fn reverse_domain_names(&mut self) {
        let identities = mem::replace(&mut self.identities, BTreeMap::new());
        self.identities.extend(identities.into_iter().map(|(mut domain, v)| {
            Tls::reverse_domain_name(&mut domain);
            (domain, v)
        }))
    }

    fn check(&self) -> Result<()> {
        use std::fs;
        if let Err(e) = fs::File::open(&self.ca_certs) {
            bail!("ca certs cannot be read {}", e)
        }
        if self.identities.len() == 0 {
            bail!("at least one identity is required for tls authentication")
        }
        for (name, id) in &self.identities {
            if let Err(e) = fs::File::open(&id.certificate) {
                bail!("{} certificate can't be read {}", name, e)
            }
            match &id.private_key {
                None => {
                    if self.agent.is_none() {
                        bail!("if not using the agent private keys must be specified")
                    }
                }
                Some(pkey) => {
                    if let Err(e) = fs::File::open(pkey) {
                        bail!("{} private_key can't be read {}", name, e)
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DefaultAuthMech {
    Anonymous,
    Local,
    Krb5,
    Tls,
}

impl Default for DefaultAuthMech {
    fn default() -> Self {
        DefaultAuthMech::Krb5
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base: Path,
    pub addrs: Vec<(SocketAddr, Auth)>,
    pub tls: Option<Tls>,
    pub default_auth: DefaultAuthMech,
}

impl Config {
    pub fn parse(s: &str) -> Result<Config> {
        let mut cfg: file::Config = from_str(s)?;
        if cfg.addrs.is_empty() {
            bail!("you must specify at least one address");
        }
        match cfg.default_auth {
            DefaultAuthMech::Anonymous
            | DefaultAuthMech::Local
            | DefaultAuthMech::Krb5 => (),
            DefaultAuthMech::Tls => {
                if cfg.tls.is_none() {
                    bail!("tls identities require for tls auth")
                }
            }
        }
        if let Some(tls) = &cfg.tls {
            tls.check()?
        }
        for (addr, auth) in &cfg.addrs {
            use file::Auth as FAuth;
            utils::check_addr::<()>(addr.ip(), &[])?;
            match auth {
                FAuth::Anonymous | FAuth::Krb5(_) => (),
                FAuth::Tls(name) => match &cfg.tls {
                    None => bail!("tls auth requires tls_ca_certs path to be set"),
                    Some(tls) => {
                        if !tls.identities.contains_key(name) {
                            bail!(
                                "required identity {} not found in tls identities",
                                name
                            )
                        }
                    }
                },
                FAuth::Local(_) => {
                    if !addr.ip().is_loopback() {
                        bail!("local auth is not allowed for remote servers")
                    }
                }
            }
        }
        if !cfg.addrs.iter().all(|(a, _)| a.ip().is_loopback())
            && !cfg.addrs.iter().all(|(a, _)| !a.ip().is_loopback())
        {
            bail!("can't mix loopback addrs with non loopback addrs")
        }
        Ok(Config {
            base: Path::from(cfg.base),
            addrs: cfg.addrs.into_iter().map(|(s, a)| (s, a.into())).collect(),
            tls: cfg.tls,
            default_auth: cfg.default_auth,
        })
    }

    pub fn default_auth(&self) -> DesiredAuth {
        match self.default_auth {
            DefaultAuthMech::Anonymous => DesiredAuth::Anonymous,
            DefaultAuthMech::Local => DesiredAuth::Local,
            DefaultAuthMech::Krb5 => DesiredAuth::Krb5 { upn: None, spn: None },
            DefaultAuthMech::Tls => DesiredAuth::Tls {
                name: None,
                identity: None,
                tls: self.tls.as_ref().unwrap().clone(),
            },
        }
    }

    /// Load the cluster config from the specified file.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    pub fn to_referral(self) -> Referral {
        Referral { path: self.base, ttl: None, addrs: Pooled::orphan(self.addrs) }
    }

    /// This will try in order,
    ///
    /// * $NETIDX_CFG
    /// * ${dirs::config_dir}/netidx/client.json
    /// * ${dirs::home_dir}/.config/netidx/client.json
    /// * C:\netidx\client.json on windows
    /// * /etc/netidx/client.json on unix
    ///
    /// It will load the first file that exists, if that file fails to
    /// load then Err will be returned.
    pub fn load_default() -> Result<Config> {
        if let Some(cfg) = env::var_os("NETIDX_CFG") {
            let cfg = PathBuf::from(cfg);
            if cfg.is_file() {
                debug!("loading {}", cfg.to_string_lossy());
                return Config::load(cfg);
            }
        }
        if let Some(mut cfg) = dirs::config_dir() {
            cfg.push("netidx");
            cfg.push("client.json");
            if cfg.is_file() {
                debug!("loading {}", cfg.to_string_lossy());
                return Config::load(cfg);
            }
        }
        if let Some(mut home) = dirs::home_dir() {
            home.push(".config");
            home.push("netidx");
            home.push("client.json");
            if home.is_file() {
                debug!("loading {}", home.to_string_lossy());
                return Config::load(home);
            }
        }
        let dir = if cfg!(windows) {
            PathBuf::from("C:\\netidx\\client.json")
        } else {
            PathBuf::from("/etc/netidx/client.json")
        };
        if dir.is_file() {
            debug!("loading {}", dir.to_string_lossy());
            return Config::load(dir);
        }
        bail!("no default config file was found")
    }
}
