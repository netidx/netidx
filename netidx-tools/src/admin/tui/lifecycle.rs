//! Async entry points to `netidx_admin::sync` for the Local tab.
//!
//! The CLI `lifecycle.rs` builds its own `tokio::runtime::Runtime` per call
//! and prints, so it can't be called from inside the TUI's runtime (a nested
//! runtime panics). Everything below is the same library call with a plan
//! returned instead of printed.

use anyhow::{Context, Result, bail};
use netidx_admin::{
    provenance::{InstallRecord, InstallRole},
    sync::{self, SyncPlan},
};
use netidx_admin_proto::{AdminDomainMap, NodeKind};
use std::path::Path;

/// Build this host's sync plan, checked against the admin domain (pinned
/// to the CA identity recorded at install). The caller renders
/// `plan.edits.describe()` and applies it. Errors if this host is local-only
/// or the wrong role.
///
/// The record is loaded by explicit path rather than discovered: the Local
/// tab enumerates both scopes, and a plan must write back to the one it
/// was built from.
pub(super) async fn sync_plan(config_root: &Path, role: InstallRole) -> Result<SyncPlan> {
    let path = config_root.join("install.json");
    let rec = InstallRecord::load(&path)?;
    if rec.role != role {
        bail!("this host is a {} install, not a {}", rec.role.as_str(), role.as_str());
    }
    if role == InstallRole::Ca {
        bail!("the CA has no resolver configuration to reconcile")
    }
    sync::plan(rec, path).await
}

/// Fetch the admin domain map as this host, pinned to the CA identity recorded at
/// install — for map-driven UI (the parent picker). Errors if this host isn't
/// part of an admin domain or no admin server answers with the pinned identity.
pub(super) async fn fetch_local_map(config_root: &Path) -> Result<AdminDomainMap> {
    let rec = InstallRecord::load(&config_root.join("install.json"))?;
    let net_id = rec
        .admin_domain
        .as_ref()
        .context("this host is not part of an admin domain (local-only)")?;
    sync::fetch_map(net_id, NodeKind::Resolver).await
}
