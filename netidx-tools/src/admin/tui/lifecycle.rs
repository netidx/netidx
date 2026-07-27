//! Async lifecycle helpers for the Local tab's post-install actions.
//!
//! The tools `lifecycle.rs` builds its own `tokio::runtime::Runtime` per call and
//! prints results, so it can't be reused from inside the TUI's runtime (a nested
//! runtime panics). These are the same operations rewritten as plain `async fn`s
//! over the library's `transport` / `reconcile` / `discovery` pieces (none of
//! which are cfg-gated), returning the plan for the UI to render + apply.

use anyhow::{Context, Result, bail};
use netidx_admin_client::{
    discovery, paths,
    provenance::{InstallRecord, InstallRole, TrustDomainIdentity},
    reconcile::{self, EditPlan},
    resolver::ResolverConfig,
    transport::{self, TrustDomainInfo},
};
use netidx_admin_proto::{NodeKind, TrustDomainMap};
use std::{net::SocketAddr, path::Path, time::Duration};

pub(super) const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);

/// Build the config-reconciliation plan for this host's role, checking it
/// against the network (pinned to the CA identity recorded at install). The
/// caller renders `plan.describe()` and applies it. Errors if this host is
/// local-only (nothing to update) or the wrong role.
pub(super) async fn update_plan(
    config_root: &Path,
    role: InstallRole,
) -> Result<EditPlan> {
    let rec = InstallRecord::load(&config_root.join("install.json"))?;
    if rec.role != role {
        bail!("this host is a {} install, not a {}", rec.role.as_str(), role.as_str());
    }
    let net_id =
        rec.network.as_ref().context("this host is local-only — nothing to update")?;
    match role {
        InstallRole::Controller => {
            bail!("the controller has no resolver configuration to reconcile")
        }
        InstallRole::Workstation => {
            let rpath = paths::discover_resolver_config()?;
            let info =
                fetch_trust_domain_pinned(net_id, rec.admin_server, NodeKind::Client)
                    .await?;
            reconcile::reconcile_resolver_peers(&rpath, &info)
        }
        InstallRole::Resolver => {
            let map =
                fetch_map_pinned(net_id, rec.admin_server, NodeKind::Resolver).await?;
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
            let map =
                fetch_map_pinned(net_id, rec.admin_server, NodeKind::Publisher).await?;
            let cpath = paths::discover_client_config()?;
            reconcile::reconcile_client_peers(&cpath, &map)
        }
    }
}

/// Fetch the network map as this host, pinned to the CA identity recorded at
/// install — for map-driven UI (the parent picker). Errors if this host isn't
/// part of a cluster or no admin server answers with the pinned identity.
pub(super) async fn fetch_local_map(config_root: &Path) -> Result<TrustDomainMap> {
    let rec = InstallRecord::load(&config_root.join("install.json"))?;
    let net_id = rec
        .network
        .as_ref()
        .context("this host is not part of a cluster (local-only)")?;
    fetch_map_pinned(net_id, rec.admin_server, NodeKind::Resolver).await
}

/// An activation hint after an update applies. Resolver updates can touch only
/// client configuration, only the resolver's parent referral, or both, so the
/// hint must follow the actual edit plan rather than only the host role.
pub(super) fn restart_hint_for_plan(role: InstallRole, plan: &EditPlan) -> &'static str {
    match role {
        InstallRole::Controller => "The controller requires no resolver restart.",
        InstallRole::Workstation => {
            "No service was restarted. Restart the local resolver to serve the new peers."
        }
        InstallRole::Resolver if plan.changes_resolver_config() => {
            "No service was restarted. Restart this resolver manually at its place in the \
             cluster's rolling sequence; re-run client processes if their resolver addresses \
             changed."
        }
        InstallRole::Resolver => {
            "Re-run client processes to use the new resolver addresses; no resolver service \
             restart is needed."
        }
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
async fn fetch_trust_domain_pinned(
    net_id: &TrustDomainIdentity,
    admin_server: Option<SocketAddr>,
    kind: NodeKind,
) -> Result<TrustDomainInfo> {
    let mut saw_mismatch = false;
    for addr in candidates(admin_server).await {
        let id = match transport::fetch_identity(addr, kind).await {
            Ok(id) => id,
            Err(_) => continue,
        };
        if net_id.matches(&id.fingerprint)? {
            return transport::aggregate(&[addr], kind, &id)
                .await
                .context("mapping the network (GetInfo)");
        }
        saw_mismatch = true;
    }
    fail(saw_mismatch)
}

/// One-shot pinned network-map fetch, same fail-closed logic as
/// [`fetch_trust_domain_pinned`].
async fn fetch_map_pinned(
    net_id: &TrustDomainIdentity,
    admin_server: Option<SocketAddr>,
    kind: NodeKind,
) -> Result<TrustDomainMap> {
    let mut saw_mismatch = false;
    for addr in candidates(admin_server).await {
        let id = match transport::fetch_identity(addr, kind).await {
            Ok(id) => id,
            Err(_) => continue,
        };
        if net_id.matches(&id.fingerprint)? {
            return transport::get_map_pinned(addr, kind, &id)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_only_resolver_update_does_not_request_a_resolver_restart() {
        let hint = restart_hint_for_plan(InstallRole::Resolver, &EditPlan::default());
        assert!(hint.contains("no resolver service restart is needed"));
        assert!(!hint.contains("Restart this resolver"));
    }
}
