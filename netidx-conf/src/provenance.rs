//! Install provenance: a small record (`install.json`) written next to a
//! host's config at install/join time, recording **what role** was
//! installed and **which network** it joined.
//!
//! The network half is load-bearing for the lifecycle ops (`status`,
//! `update`, `join`): they trust a conf server's picture of the network
//! ("here are the resolvers, add the ones you're missing"), so they must
//! first re-pin to the **same** CA identity the operator glyph-confirmed
//! at install. Storing that identity here is what makes an unattended
//! `update` safe rather than an MITM foothold — a rogue conf server with
//! a different CA fingerprint is refused before anything is changed.

use crate::{atomic, fingerprint::Fingerprint, paths};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

/// Which system-role template produced this install. Distinct from
/// [`conf_proto::Role`](crate::conf_proto::Role), which enumerates the
/// services a *conf server* offers (ca / resolver / id-map).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstallRole {
    Workstation,
    Resolver,
    Publisher,
}

impl InstallRole {
    pub fn as_str(self) -> &'static str {
        match self {
            InstallRole::Workstation => "workstation",
            InstallRole::Resolver => "resolver",
            InstallRole::Publisher => "publisher",
        }
    }
}

/// The network a host joined: the domain and the CA fingerprint the
/// operator glyph-confirmed. The fingerprint is stored in the
/// grouped-base32 text form ([`Fingerprint::text`]) so the record reads
/// the same as the glyph shown at install; compare via [`Self::matches`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkIdentity {
    pub domain: String,
    pub ca_fingerprint: String,
}

impl NetworkIdentity {
    /// Build from a confirmed [`Fingerprint`] and domain.
    pub fn new(domain: impl Into<String>, ca: &Fingerprint) -> Self {
        NetworkIdentity { domain: domain.into(), ca_fingerprint: ca.text() }
    }

    /// Does `presented` match the pinned identity? Parses the stored
    /// text form and compares the raw digests. Fails closed: a malformed
    /// stored fingerprint is an error (refuse to trust), not a silent
    /// mismatch.
    pub fn matches(&self, presented: &Fingerprint) -> Result<bool> {
        let pinned = Fingerprint::parse_text(&self.ca_fingerprint).with_context(|| {
            format!("parsing pinned CA fingerprint {:?}", self.ca_fingerprint)
        })?;
        Ok(&pinned == presented)
    }
}

/// The provenance record at `${config}/install.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallRecord {
    /// The role template that produced this install.
    pub role: InstallRole,
    /// The role's base path (workstation `/local`; resolver/publisher `/`).
    pub base: String,
    /// The data-plane auth chosen at install (`anonymous`/`local`/`krb5`/`tls`).
    pub auth: String,
    /// The network this host joined, when it joined one — carrying the
    /// pinned CA identity. `None` for a standalone/local-only install (a
    /// workstation with no parent, or a fresh first resolver).
    #[serde(default)]
    pub network: Option<NetworkIdentity>,
    /// A conf-server address known at install time, if any — a starting
    /// point for lifecycle ops (which also fall back to mDNS discovery).
    #[serde(default)]
    pub conf_server: Option<SocketAddr>,
    /// Unix seconds the record was written.
    pub created_unix: u64,
}

impl InstallRecord {
    /// Construct a record stamped with the current time.
    pub fn new(
        role: InstallRole,
        base: impl Into<String>,
        auth: impl Into<String>,
        network: Option<NetworkIdentity>,
        conf_server: Option<SocketAddr>,
    ) -> Self {
        let created_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        InstallRecord {
            role,
            base: base.into(),
            auth: auth.into(),
            network,
            conf_server,
            created_unix,
        }
    }

    /// Read a record from `path`.
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading install record {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing install record {}", path.display()))
    }

    /// Read the record at the user-default path, or `None` if there is
    /// none (e.g. a hand-rolled config, or an install predating the
    /// record). Errors only on a present-but-unreadable file.
    pub fn load_default() -> Result<Option<Self>> {
        let path = paths::user_install_record()?;
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(Self::load(&path)?))
    }

    /// Atomically write the record (mode 0644) to `path`.
    pub fn save(&self, path: &Path) -> Result<()> {
        atomic::write_atomic_pretty_json(path, self)
    }

    /// Write the record to the user-default path.
    pub fn save_default(&self) -> Result<()> {
        self.save(&paths::user_install_record()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("install.json");
        let fp = Fingerprint::of_der(b"a fake spki for the test");
        let rec = InstallRecord::new(
            InstallRole::Workstation,
            "/local",
            "tls",
            Some(NetworkIdentity::new("ryu-oh.org", &fp)),
            Some("192.168.50.11:4564".parse().unwrap()),
        );
        rec.save(&path).unwrap();
        let back = InstallRecord::load(&path).unwrap();
        assert_eq!(rec, back);
        // The pinned identity round-trips through the stored text form
        // and matches the original fingerprint; a different key doesn't.
        let net = back.network.as_ref().unwrap();
        assert!(net.matches(&fp).unwrap());
        let other = Fingerprint::of_der(b"a different key");
        assert!(!net.matches(&other).unwrap());
    }

    #[test]
    fn local_only_has_no_network() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("install.json");
        let rec =
            InstallRecord::new(InstallRole::Workstation, "/local", "local", None, None);
        rec.save(&path).unwrap();
        let back = InstallRecord::load(&path).unwrap();
        assert!(back.network.is_none());
        assert!(back.conf_server.is_none());
    }
}
