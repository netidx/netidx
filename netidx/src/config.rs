use crate::{
    path::Path,
    pool::Pooled,
    protocol::resolver::{Auth, Referral},
    resolver_server::config::file::Auth as FAuth,
    utils,
};
use anyhow::Result;
use log::debug;
use serde_json::from_str;
use std::{
    convert::AsRef,
    convert::Into,
    env,
    fs::read_to_string,
    net::SocketAddr,
    path::Path as FsPath,
};

/// The on disk format, encoded as JSON
mod file {
    use super::*;
    use std::net::SocketAddr;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Config {
        pub(super) base: String,
        pub(super) addrs: Vec<(SocketAddr, FAuth)>,
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base: Path,
    pub addrs: Vec<(SocketAddr, Auth)>,
}

impl Config {
    pub fn parse(s: &str) -> Result<Config> {
        let cfg: file::Config = from_str(s)?;
        if cfg.addrs.is_empty() {
            bail!("you must specify at least one address");
        }
        for (addr, auth) in &cfg.addrs {
            utils::check_addr(addr.ip(), &[])?;
            match auth {
                FAuth::Anonymous | FAuth::Krb5(_) => (),
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
        })
    }

    /// Load the cluster config from the specified file.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    pub fn to_referral(self) -> Referral {
        Referral {
            path: self.base,
            ttl: u32::MAX as u64,
            addrs: Pooled::orphan(self.addrs),
        }
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
