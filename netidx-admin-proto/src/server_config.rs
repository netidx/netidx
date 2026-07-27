use crate::{
    AdminServerId, fingerprint::Fingerprint, identity::admin_cert_identity_from_der,
};
use anyhow::{Context, Result, bail};
use serde_derive::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaRole {
    pub dir: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autorenew: Option<PathBuf>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde::option"
    )]
    pub session_absolute_lifetime: Option<Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde::option"
    )]
    pub session_idle_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolverRole {
    pub config: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IdMapRole {
    pub map: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Roles {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca: Option<CaRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolver: Option<ResolverRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_map: Option<IdMapRole>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdminServerConfig {
    pub domain: String,
    pub server_id: AdminServerId,
    pub home_ca_fingerprint: String,
    pub listen: SocketAddr,
    pub serving_cert: PathBuf,
    pub serving_key: PathBuf,
    pub trusted: PathBuf,
    pub roles: Roles,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_addr: Option<SocketAddr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<SocketAddr>,
    #[serde(default = "default_true")]
    pub mdns: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_units_dir: Option<PathBuf>,
}

fn default_true() -> bool {
    true
}

impl AdminServerConfig {
    pub fn validate_with_serving_cert(&self, pem: &[u8]) -> Result<()> {
        self.validate_structure()?;
        let configured_fp = Fingerprint::parse_text(&self.home_ca_fingerprint)
            .context("invalid home_ca_fingerprint")?;
        let mut cursor = std::io::Cursor::new(pem);
        let mut certs = rustls_pemfile::certs(&mut cursor);
        let leaf = certs
            .next()
            .context("serving certificate chain is empty")?
            .context("parsing serving certificate")?;
        let rest: Vec<_> = certs
            .collect::<std::result::Result<_, _>>()
            .context("parsing serving certificate chain")?;
        let home = rest
            .last()
            .context("serving certificate must include its home CA after the leaf")?;
        let actual_fp = Fingerprint::of_cert_der(home.as_ref())?;
        if actual_fp != configured_fp {
            bail!("home_ca_fingerprint does not match the CA in the serving chain");
        }
        let cert_id = admin_cert_identity_from_der(leaf.as_ref())?;
        if cert_id.server_id != self.server_id {
            bail!(
                "configured server_id {} does not match serving certificate identity {}",
                self.server_id,
                cert_id.server_id
            );
        }
        if cert_id.controller != self.roles.ca.is_some() {
            bail!(
                "serving certificate controller marker ({}) does not match CA role ({})",
                cert_id.controller,
                self.roles.ca.is_some()
            );
        }
        Ok(())
    }

    pub fn validate_structure(&self) -> Result<()> {
        if self.roles.ca.is_none() && self.ca_addr.is_none() {
            bail!(
                "a non-CA admin server must set `ca_addr` — the CA it registers \
                 with and fetches the admin domain map from. Only the CA host (with \
                 a `ca` role) may omit it."
            );
        }
        Ok(())
    }
}
