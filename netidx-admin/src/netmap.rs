//! The CA-authoritative network map: durable load/save + the pure
//! mutation helpers the register / deregister / remove-server handlers use.
//!
//! The CA owns this file (`<ca-dir>/netmap.json`) and is its only writer.
//! It is built purely from admin-server pushes — never a walk — so it must
//! survive a restart on its own; load/save here are that persistence. Every
//! other admin server holds an in-memory cache of it (filled by the refresh
//! loop) and never writes here.

use crate::{
    atomic,
    admin_proto::{NetworkMap, ServerEntry},
};
use anyhow::{Context, Result};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// The map file, beside the CA's other state.
pub fn path(ca_dir: &Path) -> PathBuf {
    ca_dir.join("netmap.json")
}

/// Load the map, or a fresh empty one if it doesn't exist yet.
pub fn load(ca_dir: &Path) -> Result<NetworkMap> {
    let p = path(ca_dir);
    match std::fs::read(&p) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing network map {p:?}")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(NetworkMap::default()),
        Err(e) => Err(e).with_context(|| format!("reading network map {p:?}")),
    }
}

/// Atomically persist the map.
pub fn save(ca_dir: &Path, map: &NetworkMap) -> Result<()> {
    atomic::write_atomic_pretty_json(&path(ca_dir), map)
}

/// Upsert a admin server's entry (keyed by `addr`), bumping `version` only
/// when something actually changed — an idempotent re-register of identical
/// facts is a no-op, so it doesn't churn every cache's version. Returns
/// whether the map changed.
pub fn upsert(map: &mut NetworkMap, entry: ServerEntry) -> bool {
    let changed = match map.servers.iter_mut().find(|s| s.addr == entry.addr) {
        Some(s) if *s == entry => false,
        Some(s) => {
            *s = entry;
            true
        }
        None => {
            map.servers.push(entry);
            true
        }
    };
    if changed {
        map.servers.sort_by_key(|s| s.addr);
        map.version += 1;
    }
    changed
}

/// Remove the entry for `addr`, bumping `version` if it was present.
/// Returns whether anything was removed. The dropped entry carries that
/// server's own resolver-cluster facts, so removing it drops its resolver
/// servers from the map too (the dead-machine cascade).
pub fn remove(map: &mut NetworkMap, addr: SocketAddr) -> bool {
    let before = map.servers.len();
    map.servers.retain(|s| s.addr != addr);
    let removed = map.servers.len() != before;
    if removed {
        map.version += 1;
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_proto::Role;

    fn entry(addr: &str, roles: Vec<Role>) -> ServerEntry {
        ServerEntry { addr: addr.parse().unwrap(), roles, cluster: None }
    }

    #[test]
    fn upsert_inserts_updates_and_is_idempotent() {
        let mut m = NetworkMap::default();
        assert_eq!(m.version, 0);
        assert!(upsert(&mut m, entry("10.0.0.1:4565", vec![Role::Ca])));
        assert_eq!(m.version, 1);
        assert_eq!(m.servers.len(), 1);
        // identical re-register: no change, no version churn
        assert!(!upsert(&mut m, entry("10.0.0.1:4565", vec![Role::Ca])));
        assert_eq!(m.version, 1);
        // a real change (roles): bumps
        assert!(upsert(&mut m, entry("10.0.0.1:4565", vec![Role::Ca, Role::Resolver])));
        assert_eq!(m.version, 2);
        assert_eq!(m.servers.len(), 1);
        // a second server, kept sorted by addr
        assert!(upsert(&mut m, entry("10.0.0.2:4565", vec![Role::Resolver])));
        assert_eq!(m.version, 3);
        assert_eq!(m.servers.len(), 2);
        assert_eq!(m.servers[0].addr, "10.0.0.1:4565".parse().unwrap());
    }

    #[test]
    fn remove_drops_and_bumps_only_when_present() {
        let mut m = NetworkMap::default();
        upsert(&mut m, entry("10.0.0.1:4565", vec![Role::Ca]));
        upsert(&mut m, entry("10.0.0.2:4565", vec![Role::Resolver]));
        let v = m.version;
        assert!(!remove(&mut m, "10.0.0.9:4565".parse().unwrap()));
        assert_eq!(m.version, v);
        assert!(remove(&mut m, "10.0.0.2:4565".parse().unwrap()));
        assert_eq!(m.version, v + 1);
        assert_eq!(m.servers.len(), 1);
    }

    #[test]
    fn load_missing_is_empty_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load(dir.path()).unwrap(), NetworkMap::default());
        let mut m = NetworkMap {
            ca_addr: Some("10.0.0.1:4565".parse().unwrap()),
            ..Default::default()
        };
        upsert(&mut m, entry("10.0.0.1:4565", vec![Role::Ca]));
        save(dir.path(), &m).unwrap();
        assert_eq!(load(dir.path()).unwrap(), m);
    }
}
