//! Map-routed permissions administration as query + action.
//!
//! An admin contacts *a* admin server, glyph-confirms its CA (the one human
//! trust decision), and the network map locates the resolver cluster mounted at
//! a path. `show_perms` reads that cluster's perms; `edit_perms` authenticates
//! to the CA, which validates the file and propagates it to every cluster
//! member. The `$EDITOR` loop between read and write is a frontend concern and
//! stays in the CLI — the library exposes the read and the authenticated write.
//!
//! **The map can route us at an impostor.** Every connection the map sends us
//! to is re-pinned against the CA the operator already confirmed
//! ([`same_ca_identity`]): a target presenting a different CA fingerprint is
//! refused, so a poisoned map can't redirect a perms read or edit to a
//! look-alike server.

use super::{open_admin_session, resolve_identity};
use crate::{
    admin_client::{self, CaIdentity},
    admin_local,
    admin_proto::{NetworkMap, NodeKind, PeerResult, Secret},
    answer::Answerer,
};
use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// A glyph-confirmed admin server plus the network map fetched from it.
struct MapBootstrap {
    /// The bootstrap admin server (whose CA the operator confirmed).
    addr: SocketAddr,
    /// The confirmed CA identity — every later connection re-pins to it.
    id: CaIdentity,
    /// The network map fetched (pinned) from the bootstrap server.
    map: NetworkMap,
}

/// Reach an admin server (explicit, else this host's own), confirm its CA glyph
/// (auto-verified against the local cert, or through the answerer), and pull the
/// map — the shared preamble for both perms ops.
async fn bootstrap(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<&Path>,
) -> Result<MapBootstrap> {
    let (addr, id) = resolve_identity(ans, server, ca_dir).await?;
    let map = admin_client::get_map_pinned(addr, NodeKind::Client, &id)
        .await
        .context("fetching the network map")?;
    Ok(MapBootstrap { addr, id, map })
}

/// The admin server of the cluster mounted exactly at `at` (the same exact-base
/// match the CA uses to route the edit).
fn route(map: &NetworkMap, at: &str) -> Result<SocketAddr> {
    let mut bases = BTreeSet::new();
    for c in map.clusters.iter().filter(|c| {
        c.state == crate::admin_proto::ClusterState::Active
    }) {
        if c.base == at {
            if let Some(server) = map.servers.iter().find(|s| {
                s.cluster == Some(c.id)
                    && s.state == crate::admin_proto::ServerState::Registered
            }) {
                return Ok(server.addr);
            }
        }
        bases.insert(c.base.as_str());
    }
    bail!(
        "no resolver cluster is mounted at {at:?} in the network map. Known cluster \
         bases: {}",
        bases.into_iter().collect::<Vec<_>>().join(", ")
    )
}

/// The CA identity to use for `target`, pinned to the SAME CA the operator
/// confirmed at bootstrap. Reuses the bootstrap identity when `target` is the
/// bootstrap host; otherwise fetches `target`'s identity and refuses it if its
/// CA fingerprint differs (the map could route us at an impostor).
async fn same_ca_identity(bs: &MapBootstrap, target: SocketAddr) -> Result<CaIdentity> {
    if target == bs.addr {
        return Ok(bs.id.clone());
    }
    let tid = admin_client::fetch_identity(target, NodeKind::Client)
        .await
        .with_context(|| format!("contacting admin server {target}"))?;
    if tid.fingerprint != bs.id.fingerprint {
        bail!(
            "the admin server at {target} presents a DIFFERENT CA than the one you \
             confirmed — refusing to trust where the map routed us."
        );
    }
    Ok(tid)
}

/// The `perms show --at <path>` query: read the raw perms JSON of the cluster
/// mounted at `at` (glyph-confirmed, re-pinned; perms are readable within the
/// trust domain, so no admin password). The CLI pretty-prints the result.
pub async fn show_perms(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    at: &str,
) -> Result<String> {
    let bs = bootstrap(ans, server, ca_dir.as_deref()).await?;
    let target = route(&bs.map, at)?;
    let id = same_ca_identity(&bs, target).await?;
    admin_client::get_perms(target, NodeKind::Client, &id).await
}

/// List every level (resolver-cluster base) in the network map — the exact
/// `--at` targets a perms read/edit can route to. Deduped and sorted. The
/// cluster-scope perms UI offers these instead of a free-text path.
pub async fn list_levels(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
) -> Result<Vec<String>> {
    let bs = bootstrap(ans, server, ca_dir.as_deref()).await?;
    let bases: BTreeSet<String> = bs
        .map
        .clusters
        .iter()
        .filter(|c| c.state == crate::admin_proto::ClusterState::Active)
        .map(|c| c.base.clone())
        .collect();
    Ok(bases.into_iter().collect())
}

/// The `perms edit --at <path>` action: hand the already-edited, already-
/// validated `edited` perms JSON to the CA (authenticated), which re-validates
/// and propagates it to every cluster member. Returns the per-peer results so a
/// partial failure surfaces. The editor loop + validation live in the CLI; this
/// is the authenticated write only.
pub async fn edit_perms(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    at: &str,
    edited: &str,
) -> Result<Vec<PeerResult>> {
    let bs = bootstrap(ans, server, ca_dir.as_deref()).await?;
    // `open_admin_session` resolves and exactly verifies the authoritative
    // controller before it collects or sends credentials.
    let session = open_admin_session(ans, Some(bs.addr), ca_dir, admin, password).await?;
    admin_client::edit_perms(
        session.server,
        NodeKind::Client,
        &session.identity,
        session.credential,
        at,
        edited,
    )
    .await
}

/// The Local-tab `perms edit` action: hand the already-edited, already-
/// validated `edited` perms JSON to *this host's own* CA over its local
/// control socket — no glyph, no admin password (the `SO_PEERCRED` superuser
/// gate is the authorization). The daemon re-validates, routes by the network
/// map, and propagates the edit to every member of the cluster mounted at
/// `target_path`, so a local edit is as cluster-consistent as a remote one.
///
/// The *read* side has no Local variant: on the CA host [`show_perms`] with no
/// `--server` already auto-verifies against the local CA cert (glyph-free) and
/// reads perms within the trust domain (password-free), so it serves the Local
/// tab unchanged.
pub async fn edit_perms_local(
    cfg_path: &Path,
    target_path: &str,
    edited: &str,
) -> Result<Vec<PeerResult>> {
    admin_local::edit_perms(cfg_path, target_path, edited).await
}
