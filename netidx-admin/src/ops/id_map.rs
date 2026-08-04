//! Map-routed id-map administration as query + actions.
//!
//! The same two targets as every other op group: a pinned remote session
//! authenticating a named admin, or this host's own daemon over its
//! `SO_PEERCRED` control socket. The CA authorizes the caller and propagates
//! to every id-map host.
//!
//! Unlike [`super::perms`], these are not read-modify-write. Each op names an
//! operation ([`IdMapEdit`]) that every host applies to its own map, because
//! uids are allocated per host — see the wire docs on `IdMapEdit`. Two
//! consequences worth knowing at the call site: there is no lost-update
//! window between concurrent admins, and re-running an op after a partial
//! propagation failure converges rather than erroring.

use super::{AdminTarget, AppliedEdit};
#[cfg(unix)]
use crate::local;
use crate::{
    admin_proto::{IdMapEdit, NodeKind},
    transport,
};
use anyhow::Result;

/// Read one host's id-map, as JSON.
///
/// One host's, not a merged view: two hosts holding the same identity under
/// different uids are both correct, so there is nothing to merge. Names and
/// group membership are what must agree, and those are what an edit
/// propagates.
pub async fn show_id_map(target: &AdminTarget) -> Result<String> {
    match target {
        AdminTarget::Remote { session } => {
            transport::get_id_map(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
            )
            .await
        }
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => local::get_id_map(cfg_path).await,
    }
}

async fn apply(target: &AdminTarget, edit: IdMapEdit) -> Result<AppliedEdit> {
    match target {
        AdminTarget::Remote { session } => {
            transport::edit_id_map(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                &edit,
            )
            .await
        }
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => local::edit_id_map(cfg_path, &edit).await,
    }
}

/// Create `name` if it isn't there. The gid is chosen by each host — the
/// resolver reads group *names*, never gids.
pub async fn add_group(target: &AdminTarget, name: &str) -> Result<AppliedEdit> {
    apply(target, IdMapEdit::AddGroup { name: name.to_string() }).await
}

/// Remove `name`. Refused while any identity still holds it — that would
/// leave the membership dangling.
pub async fn remove_group(target: &AdminTarget, name: &str) -> Result<AppliedEdit> {
    apply(target, IdMapEdit::RemoveGroup { name: name.to_string() }).await
}

/// Register `san` with `primary_group` and any secondary `groups`, creating
/// groups that don't exist. An identity already registered this way keeps its
/// uid — re-running is not a renumbering.
pub async fn add_user(
    target: &AdminTarget,
    san: &str,
    primary_group: &str,
    groups: &[String],
) -> Result<AppliedEdit> {
    apply(
        target,
        IdMapEdit::AddIdentity {
            san: san.to_string(),
            primary_group: primary_group.to_string(),
            groups: groups.to_vec(),
        },
    )
    .await
}

/// Drop `san` from every id-map host.
///
/// Note what this is not: revoking a certificate. A revoked identity's map
/// entry is inert because its cert no longer authenticates, so removing it is
/// hygiene rather than a security boundary.
pub async fn remove_user(target: &AdminTarget, san: &str) -> Result<AppliedEdit> {
    apply(target, IdMapEdit::RemoveIdentity { san: san.to_string() }).await
}

/// Add `san` to `group`.
pub async fn add_member(
    target: &AdminTarget,
    san: &str,
    group: &str,
) -> Result<AppliedEdit> {
    apply(target, IdMapEdit::AddMember { san: san.to_string(), group: group.to_string() })
        .await
}

/// Remove `san` from `group`. Refused for a primary group — an identity
/// without one is not representable; use [`remove_user`] or re-register it.
pub async fn remove_member(
    target: &AdminTarget,
    san: &str,
    group: &str,
) -> Result<AppliedEdit> {
    apply(
        target,
        IdMapEdit::RemoveMember { san: san.to_string(), group: group.to_string() },
    )
    .await
}
