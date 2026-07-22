//! On-disk config for the admin-server daemon (`admin-server.json`).
//!
//! Written by the install flows, read by `netidx admin component server run`.
//! Roles are explicit: the file states exactly what this host does
//! (holds the CA, runs a resolver, runs an id-map) and where each
//! role's backing files live — the daemon never guesses from what it
//! finds lying around. `peers` is the complex-network fallback for
//! hosts mDNS can't see; on a flat LAN discovery makes it redundant.

use crate::{
    admin_proto::AdminServerId, atomic, config_lock::ConfigDirLock,
    fingerprint::Fingerprint,
};
use anyhow::{Context, Result, bail};
use serde_derive::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// This host holds the CA vault and answers Sign/Enroll.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaRole {
    /// The CA directory (vault, certificate.pem, audit log).
    pub dir: PathBuf,
    /// Path to the auto-renewal slot's keytab. `Some` ⇒ the daemon
    /// approves verified renewals in-process as the dedicated empty-scope
    /// `autorenew` admin whose password the keytab holds (TPM-sealed where
    /// the host can); `None` ⇒ renewals wait for a human. A standalone
    /// file — kept out of the CA dir and excluded from backups — so
    /// rotating the credential never has to rewrite this config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autorenew: Option<PathBuf>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde::option"
    )]
    pub session_absolute_lifetime: Option<std::time::Duration>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "humantime_serde::option"
    )]
    pub session_idle_timeout: Option<std::time::Duration>,
}

/// A resolver server runs on this host; GetInfo reports its address
/// and data-plane auth, read fresh from the resolver config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResolverRole {
    /// Path to the resolver-server config (`resolver.json`).
    pub config: PathBuf,
}

/// An id-map daemon runs on this host; AddIdentity registrations are
/// written to its map (the daemon live-reloads on the atomic write).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IdMapRole {
    /// Path to the id-map JSON (`id-map.json`).
    pub map: PathBuf,
}

/// What this host does. Every field optional — a admin server with no
/// roles still answers GetInfo (domain + peers), which is enough to be
/// a stepping stone in a peer walk.
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
    /// The TLS domain this network is rooted at (e.g. `ryu-oh.org`).
    pub domain: String,
    pub server_id: AdminServerId,
    /// SPKI fingerprint of the one home CA that owns this admin plane.
    pub home_ca_fingerprint: String,
    pub listen: SocketAddr,
    /// The serving certificate **chain** PEM, `[leaf, ca]` — clients
    /// read the CA cert from the end of the chain.
    pub serving_cert: PathBuf,
    /// The serving leaf's private key (PKCS#8 PEM).
    pub serving_key: PathBuf,
    /// The CA bundle. Roots for optional client-cert verification (peer
    /// pushes) and for verifying peers' serving certs on outbound
    /// server-to-server connections.
    pub trusted: PathBuf,
    pub roles: Roles,
    /// The CA's admin server: where this host registers its facts and
    /// fetches the network map (and where Sign/Enroll go). **Required for
    /// a non-CA admin server** — without it the host can't register and is
    /// silently absent from the map. Only the CA host (which owns the map)
    /// may omit it. Enforced by [`AdminServerConfig::validate`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_addr: Option<SocketAddr>,
    /// Other admin servers this one knows of. Served verbatim in GetInfo
    /// for the client-side peer walk, and used by the ca role to find
    /// id-map hosts to push registrations to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<SocketAddr>,
    /// Advertise over mDNS. Default true; complex networks that filter
    /// multicast turn it off and rely on `peers`.
    #[serde(default = "default_true")]
    pub mdns: bool,
    /// The activation supervisor's unit directory on this host, when it isn't
    /// the default — used to find the supervisor's control socket for remote
    /// service control. `None` ⇒ the default search location.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_units_dir: Option<PathBuf>,
}

fn default_true() -> bool {
    true
}

impl AdminServerConfig {
    /// Parse a config without validating its serving files. Disaster recovery
    /// needs this narrow entry point because those files may be absent or
    /// sealed to the dead machine; the recovery operation validates every
    /// identity field against the restored CA and authoritative map before it
    /// replaces them.
    pub fn load_for_recovery(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading admin-server config {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))
    }

    pub fn load(path: &Path) -> Result<Self> {
        let cfg = Self::load_for_recovery(path)?;
        cfg.validate()
            .with_context(|| format!("invalid admin-server config {}", path.display()))?;
        Ok(cfg)
    }

    pub async fn load_for_recovery_async(path: &Path) -> Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading admin-server config {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))
    }

    pub async fn load_async(path: &Path) -> Result<Self> {
        let cfg = Self::load_for_recovery_async(path).await?;
        cfg.validate_async()
            .await
            .with_context(|| format!("invalid admin-server config {}", path.display()))?;
        Ok(cfg)
    }

    pub async fn validate_async(&self) -> Result<()> {
        self.validate_structure()?;
        let pem = tokio::fs::read(&self.serving_cert).await.with_context(|| {
            format!("reading serving certificate {}", self.serving_cert.display())
        })?;
        self.validate_with_serving_cert(&pem)
    }

    /// A non-CA admin server must know its CA: without `ca_addr` it can
    /// neither register its facts nor version-check + pull the network
    /// map, so it would be silently invisible to the map. Only the CA host
    /// (`roles.ca` set), which *is* the map's owner, may omit it.
    pub fn validate(&self) -> Result<()> {
        self.validate_structure()?;
        let pem = std::fs::read(&self.serving_cert).with_context(|| {
            format!("reading serving certificate {}", self.serving_cert.display())
        })?;
        self.validate_with_serving_cert(&pem)
    }

    fn validate_with_serving_cert(&self, pem: &[u8]) -> Result<()> {
        self.validate_structure()?;
        let configured_fp = Fingerprint::parse_text(&self.home_ca_fingerprint)
            .context("invalid home_ca_fingerprint")?;
        let mut cursor = std::io::Cursor::new(&pem);
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
        let cert_id = crate::tls::admin_cert_identity_from_der(leaf.as_ref())?;
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

    fn validate_structure(&self) -> Result<()> {
        if self.roles.ca.is_none() && self.ca_addr.is_none() {
            bail!(
                "a non-CA admin server must set `ca_addr` — the CA it registers \
                 with and fetches the network map from. Only the CA host (with \
                 a `ca` role) may omit it."
            );
        }
        Ok(())
    }

    pub fn save(&self, config_lock: &ConfigDirLock, path: &Path) -> Result<()> {
        let path = config_lock.require_contained(path)?;
        let bytes =
            serde_json::to_vec_pretty(self).context("serializing admin-server config")?;
        atomic::write_atomic(&path, &bytes, 0o644)
    }

    pub async fn save_async(
        &self,
        config_lock: &ConfigDirLock,
        path: &Path,
    ) -> Result<()> {
        let path = config_lock.require_contained(path)?;
        let bytes =
            serde_json::to_vec_pretty(self).context("serializing admin-server config")?;
        atomic::write_atomic_async(&path, &bytes, 0o644).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AdminServerConfig {
        AdminServerConfig {
            domain: "ryu-oh.org".to_string(),
            server_id: AdminServerId::new(),
            home_ca_fingerprint: Fingerprint::of_der(b"sample-ca").text(),
            listen: "192.168.0.5:4565".parse().unwrap(),
            serving_cert: PathBuf::from("/etc/netidx/ca/server/cert.pem"),
            serving_key: PathBuf::from("/etc/netidx/ca/server/key.pem"),
            trusted: PathBuf::from("/etc/netidx/tls/trusted.pem"),
            roles: Roles {
                ca: Some(CaRole {
                    dir: PathBuf::from("/etc/netidx/ca"),
                    autorenew: Some(PathBuf::from("/etc/netidx/autorenew.keytab")),
                    session_absolute_lifetime: None,
                    session_idle_timeout: None,
                }),
                resolver: Some(ResolverRole {
                    config: PathBuf::from("/etc/netidx/resolver.json"),
                }),
                id_map: Some(IdMapRole { map: PathBuf::from("/etc/netidx/id-map.json") }),
            },
            ca_addr: None,
            peers: vec!["192.168.0.6:4565".parse().unwrap()],
            mdns: true,
            activation_units_dir: None,
        }
    }

    #[test]
    fn round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("admin-server.json");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        let cfg = sample();
        cfg.save(&lock, &p).unwrap();
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(serde_json::from_slice::<AdminServerConfig>(&bytes).unwrap(), cfg);
    }

    #[test]
    fn minimal_config_defaults() {
        // A roleless stepping-stone server: no roles, no peers; mdns
        // defaults on, omitted optionals default cleanly.
        let json = r#"{
            "domain": "ryu-oh.org",
            "server_id": "00000000-0000-0000-0000-000000000001",
            "home_ca_fingerprint": "A4VYW 7QKRO XW3QY B4AZD M5VZT XFBWI MNA3F GHCZV RB3KG 26JSA",
            "listen": "192.168.0.7:4565",
            "serving_cert": "/a/cert.pem",
            "serving_key": "/a/key.pem",
            "trusted": "/a/trusted.pem",
            "roles": {}
        }"#;
        let cfg: AdminServerConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.mdns);
        assert!(cfg.peers.is_empty());
        assert!(cfg.ca_addr.is_none());
        assert_eq!(cfg.roles, Roles::default());
    }

    #[test]
    fn non_ca_requires_ca_addr() {
        let mut cfg = sample();
        cfg.roles.ca = None;
        let error = cfg.validate().unwrap_err().to_string();
        assert!(error.contains("ca_addr"), "non-CA + no ca_addr must be rejected");
        // Supplying it passes the structural gate (validation then reaches the
        // deliberately nonexistent certificate path in this fixture).
        cfg.ca_addr = Some("10.0.0.1:4565".parse().unwrap());
        assert!(!cfg.validate().unwrap_err().to_string().contains("must set `ca_addr`"));
        // load() enforces it: an on-disk non-CA config without ca_addr is
        // rejected at load, not silently accepted (it would never register).
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("admin-server.json");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        let mut bad = sample();
        bad.roles.ca = None;
        bad.ca_addr = None;
        bad.save(&lock, &p).unwrap(); // save does not validate
        assert!(AdminServerConfig::load(&p).is_err(), "load rejects non-CA + no ca_addr");
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let json = r#"{
            "domain": "ryu-oh.org",
            "server_id": "00000000-0000-0000-0000-000000000001",
            "home_ca_fingerprint": "A4VYW 7QKRO XW3QY B4AZD M5VZT XFBWI MNA3F GHCZV RB3KG 26JSA",
            "listen": "192.168.0.7:4565",
            "serving_cert": "/a/cert.pem",
            "serving_key": "/a/key.pem",
            "trusted": "/a/trusted.pem",
            "roles": {},
            "tpyo": true
        }"#;
        assert!(serde_json::from_str::<AdminServerConfig>(json).is_err());
    }
}
