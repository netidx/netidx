use crate::{
    path::Path,
    pool::Pooled,
    protocol::resolver::{Auth, Referral},
    publisher,
    subscriber::DesiredAuth,
    tls, utils,
};
use anyhow::Result;
use serde_json::from_str;
use std::{
    cmp::min, collections::BTreeMap, convert::AsRef, convert::Into, fs::read_to_string,
    net::SocketAddr, path::Path as FsPath, str,
};

/// The on disk format, encoded as JSON
pub mod file {
    use derive_builder::Builder;
    use crate::chars::Chars;
    use anyhow::Result;
    use std::{
        collections::BTreeMap,
        env,
        net::SocketAddr,
        path::{Path, PathBuf},
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub enum Auth {
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

    #[derive(Debug, Clone, Serialize, Deserialize, Builder)]
    #[serde(deny_unknown_fields)]
    pub struct TlsIdentity {
        /// The path to the pem file containing the the set of trusted certificates
        pub trusted: String,
        /// The path to the certificate for this identity
        pub certificate: String,
        /// The path to the private key corresponding to the
        /// certificate. If the private_key is encrypted then an askpass
        /// program must be specified in the Tls struct
        pub private_key: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Builder)]
    #[serde(deny_unknown_fields)]
    pub struct Tls {
        /// The name of the default identity, which must appear as a key of identities
        #[serde(default)]
        #[builder(setter(strip_option), default)]
        pub default_identity: Option<String>,
        /// The set of identities. The key should be the domain name
        /// the identity is to be used for. When selecting an identity
        /// to use, netidx will choose the closest domain name match.
        pub identities: BTreeMap<String, TlsIdentity>,
        /// The askpass program to run in order to get the key to
        /// decrypt private keys. The askpass program will be passed
        /// the path to the private key and is expected to write the
        /// password to stdout. \r and \n will be trimmed from the end
        /// of the password. Passwords will be cached, so askpass
        /// should only need to run once per private key per process.
        #[serde(default)]
        #[builder(setter(strip_option), default)]
        pub askpass: Option<String>,
    }

    const DEFAULT_BASE: &str = "/";

    #[derive(Debug, Clone, Serialize, Deserialize, Builder)]
    #[serde(deny_unknown_fields)]
    pub struct Config {
        /// The base path of the local resolver server cluster
        #[builder(setter(into), default = "DEFAULT_BASE.into()")]
        pub base: String,
        /// The addresses of the local resolver server cluster
        pub addrs: Vec<(SocketAddr, Auth)>,
        #[serde(default)]
        /// The optional tls config. 
        #[builder(setter(strip_option), default)]
        pub tls: Option<Tls>,
        /// The default authentication mechanism. If this is Tls then
        /// you must specify a tls config. Note that this should not
        /// be Local unless you only want to talk on the Local
        /// machine. Local machine servers will automatically choose
        /// Local auth even if the default is Kerberos or Tls.
        ///
        /// The default if not specified is Krb5
        #[serde(default)]
        #[builder(default)]
        pub default_auth: super::DefaultAuthMech,
        /// The default publisher bind config on this host
        #[serde(default)]
        #[builder(setter(into, strip_option), default)]
        pub default_bind_config: Option<String>,
    }

    impl Config {
	/// This will return the platform default user config file. It
	/// will not verify that the file exists, and it will not
	/// check the system default.
	pub fn user_platform_default_path() -> Result<PathBuf> {
            if let Some(mut cfg) = dirs::config_dir() {
                cfg.push("netidx");
                cfg.push("client.json");
                return Ok(cfg);
            }
	    bail!("default netidx config path could not be determined")
	}

	/// This will search the user default, and system default
	/// paths for a netidx config file and return the first one it
	/// finds. User default paths take priority over system defaults.
        pub fn default_path() -> Result<PathBuf> {
            if let Some(cfg) = env::var_os("NETIDX_CFG") {
                let cfg = PathBuf::from(cfg);
                if cfg.is_file() {
                    return Ok(cfg);
                }
            }
            if let Some(mut cfg) = dirs::config_dir() {
                cfg.push("netidx");
                cfg.push("client.json");
                if cfg.is_file() {
                    return Ok(cfg);
                }
            }
            if let Some(mut home) = dirs::home_dir() {
                home.push(".config");
                home.push("netidx");
                home.push("client.json");
                if home.is_file() {
                    return Ok(home);
                }
            }
            let dir = if cfg!(windows) {
                PathBuf::from("C:\\netidx\\client.json")
            } else {
                PathBuf::from("/etc/netidx/client.json")
            };
            if dir.is_file() {
                return Ok(dir);
            }
            bail!("no default config file was found")
        }

        pub fn load<P: AsRef<Path>>(file: P) -> Result<Config> {
            Ok(serde_json::from_reader(std::fs::File::open(file)?)?)
        }

        pub fn load_default() -> Result<Config> {
            Self::load(Self::default_path()?)
        }
    }
}

#[derive(Debug, Clone)]
pub struct TlsIdentity {
    pub trusted: String,
    pub name: String,
    pub certificate: String,
    pub private_key: String,
}

#[derive(Debug, Clone)]
pub struct Tls {
    pub default_identity: String,
    pub identities: BTreeMap<String, TlsIdentity>,
    pub askpass: Option<String>,
}

impl Tls {
    // e.g. marketdata.architect.com => com.architect.marketdata.
    pub fn reverse_domain_name(name: &mut String) {
        const MAX: usize = 1024;
        let mut tmp = [0u8; MAX + 1];
        let mut i = 0;
        for part in name[0..min(name.len(), MAX)].split('.').rev() {
            tmp[i..i + part.len()].copy_from_slice(part.as_bytes());
            tmp[i + part.len()] = '.' as u8;
            i += part.len() + 1;
        }
        name.clear();
        name.push_str(str::from_utf8(&mut tmp[0..i]).unwrap());
    }

    pub fn default_identity(&self) -> &TlsIdentity {
        &self.identities[&self.default_identity]
    }

    fn load(t: file::Tls) -> Result<Self> {
        use std::fs;
        if t.identities.len() == 0 {
            bail!("at least one identity is required for tls authentication")
        }
        if let Some(askpass) = &t.askpass {
            if let Err(e) = fs::File::open(askpass) {
                bail!("{} askpass can't be read {}", askpass, e)
            }
        }
        let mut default_identity = match t.default_identity {
            Some(id) => {
                if !t.identities.contains_key(&id) {
                    bail!("the default identity must exist")
                }
                id
            }
            None => {
                if t.identities.len() > 1 {
                    bail!("default_identity must be specified")
                } else {
                    t.identities.keys().next().unwrap().clone()
                }
            }
        };
        Self::reverse_domain_name(&mut default_identity);
        let mut identities = BTreeMap::new();
        for (mut name, id) in t.identities {
            if let Err(e) = tls::load_certs(&id.trusted) {
                bail!("trusted certs {} cannot be read {}", id.trusted, e)
            }
            let dns = match tls::load_certs(&id.certificate) {
                Err(e) => bail!("certificate can't be read {}", e),
                Ok(certs) => {
                    if certs.len() == 0 || certs.len() > 1 {
                        bail!("certificate file should contain 1 cert")
                    }
                    match tls::get_names(&certs[0].0)? {
                        Some(name) => name.alt_name,
                        None => bail!("certificate has no common name"),
                    }
                }
            };
            if let Err(e) = fs::File::open(&id.private_key) {
                bail!("{} private_key can't be read {}", name, e)
            }
            Self::reverse_domain_name(&mut name);
            identities.insert(
                name,
                TlsIdentity {
                    trusted: id.trusted,
                    name: dns,
                    certificate: id.certificate,
                    private_key: id.private_key,
                },
            );
        }
        Ok(Tls { askpass: t.askpass, default_identity, identities })
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
    pub default_bind_config: publisher::BindCfg,
}

impl Config {
    /// Transform a file::Config into a validated netidx config. Use
    /// this if you built a file::Config with the file::ConfigBuilder.
    pub fn from_file(cfg: file::Config) -> Result<Config> {
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
        let tls = match cfg.tls {
            Some(tls) => Some(Tls::load(tls)?),
            None => None,
        };
        for (addr, auth) in &cfg.addrs {
            use file::Auth as FAuth;
            utils::check_addr::<()>(addr.ip(), &[])?;
            match auth {
                FAuth::Anonymous | FAuth::Krb5(_) => (),
                FAuth::Tls(name) => match &tls {
                    None => bail!("tls auth requires a valid tls configuration"),
                    Some(tls) => {
                        let mut rev_name = name.clone();
                        Tls::reverse_domain_name(&mut rev_name);
                        if tls::get_match(&tls.identities, &rev_name).is_none() {
                            bail!(
                                "required identity for {} not found in tls identities",
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
            tls,
            default_auth: cfg.default_auth,
            default_bind_config: match cfg.default_bind_config {
                None => publisher::BindCfg::default(),
                Some(s) => s.parse()?,
            },
        })
    }

    /// Parse and transform a file::Config into a validated netidx Config
    pub fn parse(s: &str) -> Result<Config> {
        Self::from_file(from_str(s)?)
    }

    /// Return the default DesiredAuth as specified by the config.
    pub fn default_auth(&self) -> DesiredAuth {
        match self.default_auth {
            DefaultAuthMech::Anonymous => DesiredAuth::Anonymous,
            DefaultAuthMech::Local => DesiredAuth::Local,
            DefaultAuthMech::Krb5 => DesiredAuth::Krb5 { upn: None, spn: None },
            DefaultAuthMech::Tls => DesiredAuth::Tls { identity: None },
        }
    }

    /// Load the config from the specified file.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    /// Transform the config into a resolver Referral with a ttl that
    /// will never expire
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
        Self::load(file::Config::default_path()?)
    }
}
