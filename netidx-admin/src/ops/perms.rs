//! Map-routed permissions administration as query + action.
//!
//! An admin contacts *an* admin server, glyph-confirms its CA (the one human
//! trust decision), and resolves the authoritative CA before sending
//! credentials. Both reads and edits authenticate there. The CA
//! authorizes the requested path and uses its CA-owned map plus exact server-ID
//! pinning to reach resolver cluster members. The `$EDITOR` loop between read and write
//! is a frontend concern and stays there.

use super::AdminTarget;
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
