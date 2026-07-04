//! Async lifecycle helpers for the Local tab's post-install actions.
//!
//! The tools `lifecycle.rs` builds its own `tokio::runtime::Runtime` per call and
//! prints results, so it can't be reused from inside the TUI's runtime (a nested
//! runtime panics). These are the same operations rewritten as plain `async fn`s
//! over the library's `admin_client` / `reconcile` / `discovery` pieces (none of
//! which are cfg-gated), returning the plan for the UI to render + apply.

use anyhow::{Context, Result, bail};
use netidx_admin::{
    admin_client::{self, NetworkInfo},
    admin_proto::{NetworkMap, NodeKind},
    discovery, paths,
    provenance::{InstallRecord, InstallRole, NetworkIdentity},
    reconcile::{self, EditPlan},
    resolver::ResolverConfig,
};
use std::{net::SocketAddr, time::Duration};

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);

/// Build the config-reconciliation plan for this host's role, checking it
/// against the network (pinned to the CA identity recorded at install). The
/// caller renders `plan.describe()` and applies it. Errors if this host is
/// local-only (nothing to update) or the wrong role.
pub(super) async fn update_plan(role: InstallRole) -> Result<EditPlan> {
    let rec = InstallRecord::load_default()?
        .context("no install record (install.json) found on this host")?;
    if rec.role != role {
        bail!("this host is a {} install, not a {}", rec.role.as_str(), role.as_str());
    }
    let net_id = rec
        .network
        .as_ref()
        .context("this host is local-only — nothing to update")?;
    match role {
        InstallRole::Workstation => {
            let rpath = paths::discover_resolver_config()?;
            let info = fetch_network_pinned(net_id, rec.admin_server, NodeKind::Client).await?;
            reconcile::reconcile_resolver_peers(&rpath, &info)
        }
        InstallRole::Resolver => {
            let map = fetch_map_pinned(net_id, rec.admin_server, NodeKind::Resolver).await?;
            let mut plan = match paths::discover_client_config() {
                Ok(cpath) => reconcile::reconcile_client_peers(&cpath, &map)?,
                Err(_) => EditPlan::default(),
            };
            let rpath = paths::discover_resolver_config()?;
            if ResolverConfig::load(&rpath)?.as_file().parent.is_some() {
                plan = plan.merge(reconcile::reconcile_parent_peers(&rpath, &map)?);
            }
            Ok(plan)
        }
        InstallRole::Publisher => {
            let map = fetch_map_pinned(net_id, rec.admin_server, NodeKind::Publisher).await?;
            let cpath = paths::discover_client_config()?;
            reconcile::reconcile_client_peers(&cpath, &map)
        }
    }
}

/// A one-line "now restart X" hint for a role after an update applies.
pub(super) fn restart_hint(role: InstallRole) -> &'static str {
    match role {
        InstallRole::Workstation => "Restart the local resolver to serve the new peers.",
        InstallRole::Resolver => "Restart the resolver / re-run clients to use the new peers.",
        InstallRole::Publisher => "Re-run publishers to use the new resolvers.",
    }
}

/// Candidate admin-server addresses: the recorded one plus anything mDNS finds.
async fn candidates(admin_server: Option<SocketAddr>) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    if let Some(a) = admin_server {
        out.push(a);
    }
    for d in discovery::browse(DISCOVERY_TIMEOUT).await.unwrap_or_default() {
        out.extend(d.socket_addrs());
    }
    out.dedup();
    out
}

/// Walk the network (GetInfo aggregate) via the first candidate whose CA matches
/// the pinned identity. Fail-closed: a reachable server with a different CA is
/// refused, never silently trusted.
async fn fetch_network_pinned(
    net_id: &NetworkIdentity,
    admin_server: Option<SocketAddr>,
    kind: NodeKind,
) -> Result<NetworkInfo> {
    let mut saw_mismatch = false;
    for addr in candidates(admin_server).await {
        let id = match admin_client::fetch_identity(addr, kind).await {
            Ok(id) => id,
            Err(_) => continue,
        };
        if net_id.matches(&id.fingerprint)? {
            return admin_client::aggregate(&[addr], kind, &id)
                .await
                .context("mapping the network (GetInfo)");
        }
        saw_mismatch = true;
    }
    fail(saw_mismatch)
}

/// One-shot pinned network-map fetch, same fail-closed logic as
/// [`fetch_network_pinned`].
async fn fetch_map_pinned(
    net_id: &NetworkIdentity,
    admin_server: Option<SocketAddr>,
    kind: NodeKind,
) -> Result<NetworkMap> {
    let mut saw_mismatch = false;
    for addr in candidates(admin_server).await {
        let id = match admin_client::fetch_identity(addr, kind).await {
            Ok(id) => id,
            Err(_) => continue,
        };
        if net_id.matches(&id.fingerprint)? {
            return admin_client::get_map_pinned(addr, kind, &id)
                .await
                .context("fetching the network map");
        }
        saw_mismatch = true;
    }
    fail(saw_mismatch)
}

fn fail<T>(saw_mismatch: bool) -> Result<T> {
    if saw_mismatch {
        bail!(
            "reached an admin server, but its CA fingerprint did not match this \
             install's pinned network identity — refusing to trust it; re-join if \
             the CA legitimately changed"
        )
    }
    bail!("could not reach any admin server (recorded address and mDNS both failed)")
}
