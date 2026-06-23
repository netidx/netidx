//! On-disk config for the conf-server daemon (`conf-server.json`).
//!
//! Written by the install flows, read by `netidx conf component server run`.
//! Roles are explicit: the file states exactly what this host does
//! (holds the CA, runs a resolver, runs an id-map) and where each
//! role's backing files live — the daemon never guesses from what it
//! finds lying around. `peers` is the complex-network fallback for
//! hosts mDNS can't see; on a flat LAN discovery makes it redundant.

use crate::atomic;
use anyhow::{bail, Context, Result};
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

/// What this host does. Every field optional — a conf server with no
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
pub struct ConfServerConfig {
    /// The TLS domain this network is rooted at (e.g. `ryu-oh.org`).
    pub domain: String,
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
    /// The CA's conf server: where this host registers its facts and
    /// fetches the network map (and where Sign/Enroll go). **Required for
    /// a non-CA conf server** — without it the host can't register and is
    /// silently absent from the map. Only the CA host (which owns the map)
    /// may omit it. Enforced by [`ConfServerConfig::validate`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_addr: Option<SocketAddr>,
    /// Other conf servers this one knows of. Served verbatim in GetInfo
    /// for the client-side peer walk, and used by the ca role to find
    /// id-map hosts to push registrations to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<SocketAddr>,
    /// Advertise over mDNS. Default true; complex networks that filter
    /// multicast turn it off and rely on `peers`.
    #[serde(default = "default_true")]
    pub mdns: bool,
}

fn default_true() -> bool {
    true
}

impl ConfServerConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading conf-server config {}", path.display()))?;
        let cfg: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()
            .with_context(|| format!("invalid conf-server config {}", path.display()))?;
        Ok(cfg)
    }

    /// A non-CA conf server must know its CA: without `ca_addr` it can
    /// neither register its facts nor version-check + pull the network
    /// map, so it would be silently invisible to the map. Only the CA host
    /// (`roles.ca` set), which *is* the map's owner, may omit it.
    pub fn validate(&self) -> Result<()> {
        if self.roles.ca.is_none() && self.ca_addr.is_none() {
            bail!(
                "a non-CA conf server must set `ca_addr` — the CA it registers \
                 with and fetches the network map from. Only the CA host (with \
                 a `ca` role) may omit it."
            );
        }
        Ok(())
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let bytes =
            serde_json::to_vec_pretty(self).context("serializing conf-server config")?;
        atomic::write_atomic(path, &bytes, 0o644)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ConfServerConfig {
        ConfServerConfig {
            domain: "ryu-oh.org".to_string(),
            listen: "192.168.0.5:4565".parse().unwrap(),
            serving_cert: PathBuf::from("/etc/netidx/ca/server/cert.pem"),
            serving_key: PathBuf::from("/etc/netidx/ca/server/key.pem"),
            trusted: PathBuf::from("/etc/netidx/tls/trusted.pem"),
            roles: Roles {
                ca: Some(CaRole {
                    dir: PathBuf::from("/etc/netidx/ca"),
                    autorenew: Some(PathBuf::from("/etc/netidx/autorenew.keytab")),
                }),
                resolver: Some(ResolverRole {
                    config: PathBuf::from("/etc/netidx/resolver.json"),
                }),
                id_map: Some(IdMapRole {
                    map: PathBuf::from("/etc/netidx/id-map.json"),
                }),
            },
            ca_addr: None,
            peers: vec!["192.168.0.6:4565".parse().unwrap()],
            mdns: true,
        }
    }

    #[test]
    fn round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("conf-server.json");
        let cfg = sample();
        cfg.save(&p).unwrap();
        assert_eq!(ConfServerConfig::load(&p).unwrap(), cfg);
    }

    #[test]
    fn minimal_config_defaults() {
        // A roleless stepping-stone server: no roles, no peers; mdns
        // defaults on, omitted optionals default cleanly.
        let json = r#"{
            "domain": "ryu-oh.org",
            "listen": "192.168.0.7:4565",
            "serving_cert": "/a/cert.pem",
            "serving_key": "/a/key.pem",
            "trusted": "/a/trusted.pem",
            "roles": {}
        }"#;
        let cfg: ConfServerConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.mdns);
        assert!(cfg.peers.is_empty());
        assert!(cfg.ca_addr.is_none());
        assert_eq!(cfg.roles, Roles::default());
    }

    #[test]
    fn non_ca_requires_ca_addr() {
        // The CA host (sample has a `ca` role) may omit ca_addr — it owns
        // the map.
        let mut cfg = sample();
        assert!(cfg.ca_addr.is_none());
        assert!(cfg.validate().is_ok());
        // Drop the CA role: now ca_addr is mandatory (it must know where to
        // register / fetch the map).
        cfg.roles.ca = None;
        assert!(cfg.validate().is_err(), "non-CA + no ca_addr must be rejected");
        // Supplying it makes the config valid again.
        cfg.ca_addr = Some("10.0.0.1:4565".parse().unwrap());
        assert!(cfg.validate().is_ok());
        // load() enforces it: an on-disk non-CA config without ca_addr is
        // rejected at load, not silently accepted (it would never register).
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("conf-server.json");
        let mut bad = sample();
        bad.roles.ca = None;
        bad.ca_addr = None;
        bad.save(&p).unwrap(); // save does not validate
        assert!(ConfServerConfig::load(&p).is_err(), "load rejects non-CA + no ca_addr");
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let json = r#"{
            "domain": "ryu-oh.org",
            "listen": "192.168.0.7:4565",
            "serving_cert": "/a/cert.pem",
            "serving_key": "/a/key.pem",
            "trusted": "/a/trusted.pem",
            "roles": {},
            "tpyo": true
        }"#;
        assert!(serde_json::from_str::<ConfServerConfig>(json).is_err());
    }
}
