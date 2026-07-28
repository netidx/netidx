//! Install provenance: a small record (`install.json`) written next to a
//! host's config at install/join time, recording **what role** was
//! installed and **which admin domain** it joined.
//!
//! The admin domain half is load-bearing for the lifecycle ops (`status`,
//! `update`, `join`): they trust an admin server's picture of the admin domain
//! ("here are the resolvers, add the ones you're missing"), so they must
//! first re-pin to the **same** CA identity the operator glyph-confirmed
//! at install. Storing that identity here is what makes an unattended
//! `update` safe rather than an MITM foothold — a rogue admin server with
//! a different CA fingerprint is refused before anything is changed.

use crate::{atomic, config_lock::ConfigDirLock, fingerprint::Fingerprint, paths};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Which system-role template produced this install. Distinct from
/// [`admin_proto::Role`](netidx_admin_proto::Role), which enumerates the
/// services a *admin server* offers (CA / resolver / id-map).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstallRole {
    Ca,
    Workstation,
    Resolver,
    Publisher,
}

impl InstallRole {
    pub fn as_str(self) -> &'static str {
        match self {
            InstallRole::Ca => "CA",
            InstallRole::Workstation => "workstation",
            InstallRole::Resolver => "resolver",
            InstallRole::Publisher => "publisher",
        }
    }
}

/// The admin domain a host joined: the domain and the CA fingerprint the
/// operator glyph-confirmed. The fingerprint is stored in the
/// grouped-base32 text form ([`Fingerprint::text`]) so the record reads
/// the same as the glyph shown at install; compare via [`Self::matches`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDomainIdentity {
    pub domain: String,
    pub ca_fingerprint: String,
}

impl AdminDomainIdentity {
    /// Build from a confirmed [`Fingerprint`] and domain.
    pub fn new(domain: impl Into<String>, ca: &Fingerprint) -> Self {
        AdminDomainIdentity { domain: domain.into(), ca_fingerprint: ca.text() }
    }

    /// Does `presented` match the pinned identity? Parses the stored
    /// text form and compares the raw digests. Fails closed: a malformed
    /// stored fingerprint is an error (refuse to trust), not a silent
    /// mismatch.
    pub fn matches(&self, presented: &Fingerprint) -> Result<bool> {
        let pinned =
            Fingerprint::parse_text(&self.ca_fingerprint).with_context(|| {
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
    /// The admin domain this host belongs to — the admin domain it founded or the
    /// one it joined — carrying that admin domain's CA identity (domain + glyph).
    /// `None` for a standalone/local-only install (a workstation with no
    /// parent, or a resolver with no admin server).
    #[serde(default)]
    pub admin_domain: Option<AdminDomainIdentity>,
    /// Every admin server this host knows of: seeded with the one it
    /// enrolled through and refreshed from the CA-authoritative map by
    /// `update`, so a host keeps working when one of them moves. The
    /// starting point for reaching the admin domain — see
    /// [`crate::discovery::admin_servers`], which falls back to mDNS.
    #[serde(default)]
    pub admin_servers: Vec<SocketAddr>,
    /// Files and directories produced by the role template. Backup uses this
    /// inventory to refuse a falsely "complete" portable bundle when an
    /// operator deliberately installed managed state outside the config root.
    #[serde(default)]
    pub managed_paths: Vec<PathBuf>,
    /// Unix seconds the record was written.
    pub created_unix: u64,
}

impl InstallRecord {
    /// Construct a record stamped with the current time.
    pub fn new(
        role: InstallRole,
        base: impl Into<String>,
        auth: impl Into<String>,
        admin_domain: Option<AdminDomainIdentity>,
        admin_server: Option<SocketAddr>,
    ) -> Self {
        let created_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        InstallRecord {
            role,
            base: base.into(),
            auth: auth.into(),
            admin_domain,
            admin_servers: admin_server.into_iter().collect(),
            managed_paths: Vec::new(),
            created_unix,
        }
    }

    pub fn set_managed_paths(&mut self, paths: Vec<PathBuf>) {
        self.managed_paths = paths;
        self.managed_paths.sort();
        self.managed_paths.dedup();
    }

    /// Point the record at a relocated admin server: `addr` becomes the
    /// address tried first, replacing the one this host used to reach the
    /// admin domain through — which, having moved, is gone. Any other
    /// known server is kept. Returns whether anything changed.
    pub fn relocate_admin_server(&mut self, addr: SocketAddr) -> bool {
        if self.admin_servers.first() == Some(&addr) {
            return false;
        }
        if !self.admin_servers.is_empty() {
            self.admin_servers.remove(0);
        }
        self.admin_servers.retain(|a| *a != addr);
        self.admin_servers.insert(0, addr);
        true
    }

    /// Replace the known admin servers with the CA-authoritative set,
    /// keeping the address we already reach the admin domain through at the
    /// front. Returns whether anything changed, so a caller can skip
    /// rewriting the record. Only the map is authoritative about
    /// membership — a server dropped from it is dropped here.
    pub fn set_admin_servers(
        &mut self,
        addrs: impl IntoIterator<Item = SocketAddr>,
    ) -> bool {
        let mut next: Vec<SocketAddr> = Vec::new();
        for addr in addrs {
            if !next.contains(&addr) {
                next.push(addr);
            }
        }
        if let Some(first) = self.admin_servers.first()
            && let Some(i) = next.iter().position(|a| a == first)
        {
            next.swap(0, i);
        }
        let changed = next != self.admin_servers;
        self.admin_servers = next;
        changed
    }

    /// Read a record from `path`.
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading install record {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing install record {}", path.display()))
    }

    pub async fn load_async(path: &Path) -> Result<Self> {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading install record {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing install record {}", path.display()))
    }

    /// Read this host's record — the user-scope one, else the system-scope
    /// one a daemon running as root has — or `None` if there is none (e.g.
    /// a hand-rolled config, or an install predating the record). Errors
    /// only on a present-but-unreadable file.
    ///
    /// Callers that mean one specific scope (the TUI enumerating both)
    /// must use [`Self::load`] with the path they mean.
    pub fn load_default() -> Result<Option<Self>> {
        match paths::discover_install_record() {
            Err(_) => Ok(None),
            Ok(path) => Ok(Some(Self::load(&path)?)),
        }
    }

    pub async fn load_default_async() -> Result<Option<Self>> {
        match paths::discover_install_record_async().await {
            Err(_) => Ok(None),
            Ok(path) => Ok(Some(Self::load_async(&path).await?)),
        }
    }

    /// Atomically write the record (mode 0644) to `path`.
    pub fn save(&self, config_lock: &ConfigDirLock, path: &Path) -> Result<()> {
        let path = config_lock.require_contained(path)?;
        atomic::write_atomic_pretty_json(&path, self)
    }

    pub async fn save_async(
        &self,
        config_lock: &ConfigDirLock,
        path: &Path,
    ) -> Result<()> {
        let path = config_lock.require_contained(path)?;
        atomic::write_atomic_pretty_json_async(&path, self).await
    }

    /// Write the record to the user-default path.
    pub fn save_default(&self, config_lock: &ConfigDirLock) -> Result<()> {
        self.save(config_lock, &paths::user_install_record()?)
    }

    pub async fn save_default_async(&self, config_lock: &ConfigDirLock) -> Result<()> {
        self.save_async(config_lock, &paths::user_install_record()?).await
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
            Some(AdminDomainIdentity::new("ryu-oh.org", &fp)),
            Some("192.168.50.11:4564".parse().unwrap()),
        );
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        rec.save(&lock, &path).unwrap();
        let back = InstallRecord::load(&path).unwrap();
        assert_eq!(rec, back);
        // The pinned identity round-trips through the stored text form
        // and matches the original fingerprint; a different key doesn't.
        let net = back.admin_domain.as_ref().unwrap();
        assert!(net.matches(&fp).unwrap());
        let other = Fingerprint::of_der(b"a different key");
        assert!(!net.matches(&other).unwrap());
    }

    #[test]
    fn local_only_has_no_admin_domain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("install.json");
        let rec =
            InstallRecord::new(InstallRole::Workstation, "/local", "local", None, None);
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        rec.save(&lock, &path).unwrap();
        let back = InstallRecord::load(&path).unwrap();
        assert!(back.admin_domain.is_none());
        assert!(back.admin_servers.is_empty());
    }

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, n], 4565))
    }

    fn rec_with(addrs: &[SocketAddr]) -> InstallRecord {
        let mut rec = InstallRecord::new(InstallRole::Publisher, "/", "tls", None, None);
        rec.admin_servers = addrs.to_vec();
        rec
    }

    /// The map is authoritative about membership, but the address this host
    /// actually reaches the admin domain through has to stay first — that's
    /// the one already known to work from here.
    #[test]
    fn refreshing_from_the_map_keeps_the_working_address_first() {
        let mut rec = rec_with(&[addr(2), addr(1)]);
        assert!(rec.set_admin_servers([addr(1), addr(2), addr(3)]));
        assert_eq!(rec.admin_servers, vec![addr(2), addr(1), addr(3)]);
        // Idempotent: the same map again is not a change to write out.
        assert!(!rec.set_admin_servers([addr(1), addr(2), addr(3)]));
        // A server dropped from the map is dropped here, even the first one.
        assert!(rec.set_admin_servers([addr(1), addr(3)]));
        assert_eq!(rec.admin_servers, vec![addr(1), addr(3)]);
    }

    /// A relocated admin server displaces the one it replaces — that address
    /// is gone — while any other known server survives.
    #[test]
    fn relocating_replaces_only_the_head() {
        let mut rec = rec_with(&[addr(1), addr(2)]);
        assert!(rec.relocate_admin_server(addr(9)));
        assert_eq!(rec.admin_servers, vec![addr(9), addr(2)]);
        assert!(!rec.relocate_admin_server(addr(9)));
        // Promoting a server already in the list must not duplicate it.
        assert!(rec.relocate_admin_server(addr(2)));
        assert_eq!(rec.admin_servers, vec![addr(2)]);
        // An empty record just gains the address.
        let mut rec = rec_with(&[]);
        assert!(rec.relocate_admin_server(addr(9)));
        assert_eq!(rec.admin_servers, vec![addr(9)]);
    }

    #[test]
    fn ca_role_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("install.json");
        let rec = InstallRecord::new(InstallRole::Ca, "/", "admin-tls", None, None);
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        rec.save(&lock, &path).unwrap();
        assert_eq!(InstallRecord::load(&path).unwrap().role, InstallRole::Ca);
        assert_eq!(InstallRole::Ca.as_str(), "CA");
    }
}
