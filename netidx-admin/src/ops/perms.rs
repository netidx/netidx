//! Map-routed permissions administration as query + action.
//!
//! An admin contacts *an* admin server, glyph-confirms its CA (the one human
//! trust decision), and resolves the authoritative CA before sending
//! credentials. Both reads and edits authenticate there. The CA
//! authorizes the requested path and uses its CA-owned map plus exact server-ID
//! pinning to reach resolver cluster members. The `$EDITOR` loop between read and write
//! is a frontend concern and stays there.

use super::{AdminTarget, EditResult, RecordedEdit};
#[cfg(unix)]
use crate::local;
use crate::{
    admin_proto::{EditOutcome, NodeKind},
    perms::PMap,
    transport,
};
use anyhow::{Context, Result, bail};
use std::collections::BTreeSet;

/// Read the perms of the resolver cluster mounted at `at`.
///
/// One shape for both targets: a pinned remote session authenticates a named
/// admin, and this host's own daemon trusts the `SO_PEERCRED` superuser over
/// its control socket. Which one is in hand is [`AdminTarget`]'s business, not
/// a frontend's.
pub async fn show_perms(target: &AdminTarget, at: &str) -> Result<PMap> {
    Ok(read_perms_versioned(target, at).await?.1)
}

/// The document and the version it is at, which is what an edit built on it
/// must hand back so the CA can tell whether it is still current.
///
/// `None` is the CA holding no model for this cluster — a real state, and one
/// an edit can be based on, not an absence of information.
pub async fn read_perms_versioned(
    target: &AdminTarget,
    at: &str,
) -> Result<(Option<u64>, PMap)> {
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

/// Hand an edited document to the CA, which re-checks it and records it as
/// what the resolver cluster mounted at `at` is supposed to hold. Returns the
/// version it recorded; members converge on it at their next register.
///
/// The `$EDITOR` loop stays in the frontend — suspending a terminal and
/// offering a text area are different gestures — and so does the rendering it
/// needs: [`crate::perms::render`] out, [`crate::perms::parse`] back.
pub async fn edit_perms(
    target: &AdminTarget,
    at: &str,
    edited: &PMap,
    base_version: Option<u64>,
) -> Result<EditResult> {
    Ok(match submit(target, at, edited, base_version).await? {
        EditOutcome::Recorded(ok) => EditResult::Recorded(RecordedEdit {
            version: ok.version,
            changed: ok.changed,
        }),
        EditOutcome::Stale { current_version, current } => {
            EditResult::Stale { current_version, current }
        }
    })
}

/// The wire round trip on its own, so [`edit_one`] can rebase without going
/// back through the frontend-facing vocabulary each time.
async fn submit(
    target: &AdminTarget,
    at: &str,
    edited: &PMap,
    base_version: Option<u64>,
) -> Result<EditOutcome> {
    match target {
        AdminTarget::Remote { session } => {
            transport::edit_perms(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                at,
                edited,
                base_version,
            )
            .await
        }
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => {
            local::edit_perms(cfg_path, at, edited, base_version).await
        }
    }
}

/// The cluster's document and its version, checked before an edit is built on
/// top of it. Bits that don't parse are something to tell the operator about,
/// not something to carry forward into the next edit.
async fn basis(target: &AdminTarget, at: &str) -> Result<(Option<u64>, PMap)> {
    let (version, pmap) = read_perms_versioned(target, at).await?;
    crate::perms::check(&pmap)
        .context("the resolver cluster's current perms are not valid")?;
    Ok((version, pmap))
}

/// How many times a single-entry edit will rebase onto a document that changed
/// under it before giving up.
///
/// A small number, because each round trip only loses to another admin editing
/// the same cluster in the same instant. Bounded rather than unbounded so two
/// scripts fighting over one cluster fail loudly instead of spinning.
const REBASE_ATTEMPTS: usize = 4;

/// Apply one entry-level change to the cluster's document, rebasing if it
/// moved underneath.
///
/// The admin plane has no per-entry write — the only shape is submit the whole
/// document — so `perms set` and `perms remove` are read-modify-writes and can
/// lose a concurrent edit. The CA refuses a submission whose base is no longer
/// current and hands back what is, and an intent as narrow as "grant these bits
/// to this entity at this path" is still exactly as meaningful against the new
/// document, so it is reapplied there rather than made the operator's problem.
///
/// A whole-document `$EDITOR` edit is not reapplied this way, and must not be:
/// what the operator submitted *is* the intent, so a conflict is theirs to
/// resolve.
async fn edit_one(
    target: &AdminTarget,
    at: &str,
    mut apply: impl FnMut(&mut PMap) -> Result<bool>,
) -> Result<RecordedEdit> {
    let (mut base, mut pmap) = basis(target, at).await?;
    for _ in 0..REBASE_ATTEMPTS {
        let mut next = pmap.clone();
        let changed = apply(&mut next)?;
        match submit(target, at, &next, base).await? {
            EditOutcome::Recorded(ok) => {
                return Ok(RecordedEdit { version: ok.version, changed });
            }
            EditOutcome::Stale { current_version, current } => {
                crate::perms::check(&current).context(
                    "the resolver cluster's perms changed under this edit, and what \
                     replaced them is not valid",
                )?;
                base = current_version;
                pmap = current;
            }
        }
    }
    bail!(
        "the perms of the resolver cluster at {at:?} changed under this edit \
         {REBASE_ATTEMPTS} times running — someone else is editing the same cluster"
    )
}

/// Grant `entity` `bits` at `path`, inserting or replacing that one entry in
/// the perms of the resolver cluster mounted at `at`.
///
/// Read-modify-write against the whole document, because that is the only
/// shape the admin plane has: [`show_perms`], mutate, [`edit_perms`]. Doing it
/// here rather than in a frontend keeps one implementation for all three.
pub async fn set_entry(
    target: &AdminTarget,
    at: &str,
    path: &str,
    entity: &str,
    bits: &str,
) -> Result<RecordedEdit> {
    crate::perms::validate_bits(bits)?;
    edit_one(target, at, |pmap| {
        let changed =
            crate::perms::lookup(pmap, path, entity).map(|b| b.as_str()) != Some(bits);
        crate::perms::add_entry(pmap, path, entity, bits)?;
        Ok(changed)
    })
    .await
}

/// Remove `entity`'s entry at `path` from the perms of the resolver cluster
/// mounted at `at`.
///
/// An entry that was already absent is reported (`changed: false`), not
/// refused — the operator asked for something already true, which is not an
/// error.
pub async fn remove_entry(
    target: &AdminTarget,
    at: &str,
    path: &str,
    entity: &str,
) -> Result<RecordedEdit> {
    edit_one(target, at, |pmap| Ok(crate::perms::remove_entry(pmap, path, entity))).await
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
