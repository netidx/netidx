//! Map-routed permissions administration as query + action.
//!
//! An admin contacts *an* admin server, glyph-confirms its CA (the one human
//! trust decision), and resolves the authoritative CA before sending
//! credentials. Both reads and edits authenticate there. The CA
//! authorizes the requested path and uses its CA-owned map plus exact server-ID
//! pinning to reach resolver cluster members. The `$EDITOR` loop between read and write
//! is a frontend concern and stays in the CLI.

use super::{AdminSession, open_admin_session, resolve_identity};
#[cfg(unix)]
use crate::local;
use crate::{
    admin_proto::{AdminDomainMap, NodeKind, PeerResult, Secret},
    answer::Answerer,
    transport,
};
use anyhow::{Context, Result};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// Reach an admin server (explicit, else this host's own), confirm its CA glyph
/// (auto-verified against the local cert, or through the answerer), and pull the
/// map for the resolver cluster picker. The map is a discovery hint only; authenticated
/// reads and edits separately resolve and verify the CA.
async fn bootstrap(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<&Path>,
) -> Result<AdminDomainMap> {
    let (addr, id) = resolve_identity(ans, server, ca_dir).await?;
    transport::get_map_pinned(addr, NodeKind::Client, &id)
        .await
        .context("fetching the admin domain map")
}

/// The `perms show --at <path>` query: read the raw perms JSON of the resolver cluster
/// mounted at `at`. The authoritative CA is verified before collecting
/// credentials, then authenticates and authorizes the read. The CLI
/// pretty-prints the result.
pub async fn show_perms(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    at: &str,
) -> Result<String> {
    let (_session, perms_json) =
        open_perms_session(ans, server, ca_dir, admin, password, at).await?;
    Ok(perms_json)
}

/// Open one verified remote-admin session and read `at`. Editor frontends keep
/// the returned session across human think-time and use it for the write, so a
/// one-shot password is collected only once.
pub async fn open_perms_session(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    at: &str,
) -> Result<(AdminSession, String)> {
    let session = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let perms_json = transport::read_perms(
        session.server,
        NodeKind::Client,
        &session.identity,
        session.credential.clone(),
        at,
    )
    .await?;
    Ok((session, perms_json))
}

/// List every resolver cluster base in the admin domain map — the exact
/// `--at` targets a perms read/edit can route to. Deduped and sorted. The
/// resolver cluster-scope perms UI offers these instead of a free-text path.
pub async fn list_resolver_clusters(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
) -> Result<Vec<String>> {
    let map = bootstrap(ans, server, ca_dir.as_deref()).await?;
    let bases: BTreeSet<String> = map
        .resolver_clusters
        .iter()
        .filter(|c| c.state == netidx_admin_proto::ResolverClusterState::Active)
        .map(|c| c.base.clone())
        .collect();
    Ok(bases.into_iter().collect())
}

/// The `perms edit --at <path>` action: hand the already-edited, already-
/// validated `edited` perms JSON to the CA (authenticated), which re-validates
/// and propagates it to every resolver cluster member. Returns the per-peer results so a
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
    // `open_admin_session` resolves and exactly verifies the authoritative
    // CA before it collects or sends credentials.
    let session = open_admin_session(ans, server, ca_dir, admin, password).await?;
    edit_perms_with_session(&session, at, edited).await
}

/// Apply an edit using a session already used for the editor's initial read.
pub async fn edit_perms_with_session(
    session: &AdminSession,
    at: &str,
    edited: &str,
) -> Result<Vec<PeerResult>> {
    transport::edit_perms(
        session.server,
        NodeKind::Client,
        &session.identity,
        session.credential.clone(),
        at,
        edited,
    )
    .await
}

/// The Local-tab `perms edit` action: hand the already-edited, already-
/// validated `edited` perms JSON to *this host's own* CA over its local
/// control socket — no glyph, no admin password (the `SO_PEERCRED` superuser
/// gate is the authorization). The daemon re-validates, routes by the admin domain
/// map, and propagates the edit to every member of the resolver cluster mounted at
/// `target_path`, so a local edit is as resolver cluster-consistent as a remote one.
///
#[cfg(unix)]
pub async fn edit_perms_local(
    cfg_path: &Path,
    target_path: &str,
    edited: &str,
) -> Result<Vec<PeerResult>> {
    local::edit_perms(cfg_path, target_path, edited).await
}

/// Read this resolver's own permissions over its protected local socket. The
/// daemon confines the request to the host's configured resolver cluster base.
#[cfg(unix)]
pub async fn show_perms_local(cfg_path: &Path, target_path: &str) -> Result<String> {
    local::read_perms(cfg_path, target_path).await
}
