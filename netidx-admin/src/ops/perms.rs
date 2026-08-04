//! Map-routed permissions administration as query + action.
//!
//! An admin contacts *an* admin server, glyph-confirms its CA (the one human
//! trust decision), and resolves the authoritative CA before sending
//! credentials. Both reads and edits authenticate there. The CA
//! authorizes the requested path and uses its CA-owned map plus exact server-ID
//! pinning to reach resolver cluster members. The `$EDITOR` loop between read and write
//! is a frontend concern and stays there.

use super::{AdminTarget, AppliedEdit};
#[cfg(unix)]
use crate::local;
use crate::{
    admin_proto::{NodeKind, PeerResult},
    transport,
};
use anyhow::{Context, Result};
use std::collections::BTreeSet;

/// Read the perms of the resolver cluster mounted at `at`.
///
/// One shape for both targets: a pinned remote session authenticates a named
/// admin, and this host's own daemon trusts the `SO_PEERCRED` superuser over
/// its control socket. Which one is in hand is [`AdminTarget`]'s business, not
/// a frontend's.
pub async fn show_perms(target: &AdminTarget, at: &str) -> Result<String> {
    match target {
        AdminTarget::Remote { session } => {
            transport::read_perms(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                at,
            )
            .await
        }
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => local::read_perms(cfg_path, at).await,
    }
}

/// Hand already-edited, already-validated perms JSON to the CA, which
/// re-validates and propagates it to every member of the resolver cluster
/// mounted at `at`. Returns the per-peer results, so a partial failure
/// surfaces rather than reading as success.
///
/// The `$EDITOR` loop stays in the frontend — suspending a terminal and
/// offering a text area are different gestures. [`crate::perms::validate`] is
/// the rule it validates against.
pub async fn edit_perms(
    target: &AdminTarget,
    at: &str,
    edited: &str,
) -> Result<Vec<PeerResult>> {
    match target {
        AdminTarget::Remote { session } => {
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
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => local::edit_perms(cfg_path, at, edited).await,
    }
}

/// Grant `entity` `bits` at `path`, inserting or replacing that one entry in
/// the perms of the resolver cluster mounted at `at`.
///
/// Read-modify-write against the whole document, because that is the only
/// shape the admin plane has: [`show_perms`], mutate, [`edit_perms`]. Doing it
/// here rather than in a frontend keeps one implementation for all three, and
/// keeps the JSON surgery away from code whose job is to print.
pub async fn set_entry(
    target: &AdminTarget,
    at: &str,
    path: &str,
    entity: &str,
    bits: &str,
) -> Result<AppliedEdit> {
    crate::perms::validate_bits(bits)?;
    let mut pmap = crate::perms::validate(&show_perms(target, at).await?)
        .context("the resolver cluster's current perms are not valid")?;
    let changed =
        crate::perms::lookup(&pmap, path, entity).map(|b| b.as_str()) != Some(bits);
    crate::perms::add_entry(&mut pmap, path, entity, bits)?;
    Ok(AppliedEdit { changed, peers: propagate(target, at, &pmap).await? })
}

/// Remove `entity`'s entry at `path` from the perms of the resolver cluster
/// mounted at `at`.
///
/// An entry that was already absent is reported (`changed: false`), not
/// refused. Refusing would break the one property every perms command has:
/// that re-running it converges a resolver cluster left inconsistent by a
/// partial propagation failure. The second run reads the member the removal
/// already succeeded on, would find nothing to remove, and would error out
/// with the other members still holding the entry.
pub async fn remove_entry(
    target: &AdminTarget,
    at: &str,
    path: &str,
    entity: &str,
) -> Result<AppliedEdit> {
    let mut pmap = crate::perms::validate(&show_perms(target, at).await?)
        .context("the resolver cluster's current perms are not valid")?;
    let changed = crate::perms::remove_entry(&mut pmap, path, entity);
    Ok(AppliedEdit { changed, peers: propagate(target, at, &pmap).await? })
}

async fn propagate(
    target: &AdminTarget,
    at: &str,
    pmap: &crate::perms::PMap,
) -> Result<Vec<PeerResult>> {
    let edited = serde_json::to_string(pmap).context("serializing perms")?;
    edit_perms(target, at, &edited).await
}

/// The exact `--at` targets a perms read or edit can route to.
///
/// A remote admin may reach every active resolver cluster in the admin domain.
/// A local one is confined by its own daemon to this host's resolver cluster,
/// so that base is the only answer — and if this host's resolver config cannot
/// be read there is no answer at all. Guessing `/` there would tell the
/// operator they were editing the root of the namespace.
pub async fn list_resolver_clusters(target: &AdminTarget) -> Result<Vec<String>> {
    match target {
        AdminTarget::Remote { session } => {
            let map = transport::get_map_pinned(
                session.server,
                NodeKind::Client,
                &session.identity,
            )
            .await
            .context("fetching the admin domain map")?;
            let bases: BTreeSet<String> = map
                .resolver_clusters
                .iter()
                .filter(|c| c.state == netidx_admin_proto::ResolverClusterState::Active)
                .map(|c| c.base.clone())
                .collect();
            Ok(bases.into_iter().collect())
        }
        #[cfg(unix)]
        AdminTarget::Local { .. } => {
            let cfg = crate::resolver::ResolverConfig::load_default().context(
                "reading this host's resolver config to find the resolver cluster a \
                 local perms edit is confined to",
            )?;
            Ok(vec![cfg.base_path()])
        }
    }
}
