//! The CA admin roster as query + actions.
//!
//! Unlike the queue/delegation/revoke groups, the roster is addressed by admin
//! **name**, not a code — an admin name is already a stable, human-chosen id.
//! `list_admins` is the query; `add_role_admin` / `set_admin_policy` /
//! `remove_admin` are the actions. Each runs against an [`AdminTarget`] — this
//! host's own admin server over its local control socket (superuser, no
//! password), or a pinned remote session (glyph + admin password). The CA
//! enforces no-escalation on the remote path (a granted policy must be a subset
//! of the managing admin's), so exposing every policy knob here is safe.
//!
//! The system-managed signing slots (`recovery` / `autorenew`) are never role
//! admins: they can't be added, rescoped, or removed through this group (that
//! would orphan the CA key or the box credential). Rotate them with
//! `ca recovery rotate` / `ca auto-approve --rotate` instead.

use super::AdminTarget;
#[cfg(unix)]
use crate::local;
use crate::{
    admin_proto::{NodeKind, Secret},
    transport,
};
use anyhow::{Result, bail};
use netidx_admin_proto::policy::{AdminInfo, Policy, is_reserved_admin};

/// Refuse to touch a reserved signing slot as if it were a role admin.
fn guard_not_reserved(name: &str) -> Result<()> {
    if is_reserved_admin(name) {
        bail!(
            "{name:?} is a system-managed signing slot (recovery / autorenew), not a \
             role admin — it cannot be added, rescoped, or removed here (that would \
             orphan the CA key or the box credential). Rotate it with \
             `netidx admin ca recovery rotate` or `netidx admin ca auto-approve --rotate`."
        );
    }
    Ok(())
}

/// The `ca admin list` query: the roster (tier + policy per admin).
pub async fn list_admins(target: &AdminTarget) -> Result<Vec<AdminInfo>> {
    match target {
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => local::list_admins(cfg_path).await,
        AdminTarget::Remote { session } => {
            transport::list_admins(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
            )
            .await
        }
    }
}

/// The `ca admin add-role <name>` action: mint a new role admin with `policy`
/// and an initial `new_password`.
pub async fn add_role_admin(
    target: &AdminTarget,
    name: &str,
    new_password: &Secret,
    policy: Policy,
) -> Result<()> {
    guard_not_reserved(name)?;
    match target {
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => {
            local::add_role_admin(cfg_path, name, new_password.as_str(), policy).await
        }
        AdminTarget::Remote { session } => {
            transport::add_role_admin(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                name,
                new_password.as_str(),
                policy,
            )
            .await
        }
    }
}

/// The `ca admin set-policy <name>` action: replace an admin's policy wholesale.
pub async fn set_admin_policy(
    target: &AdminTarget,
    name: &str,
    policy: Policy,
) -> Result<()> {
    guard_not_reserved(name)?;
    match target {
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => {
            local::set_admin_policy(cfg_path, name, policy).await
        }
        AdminTarget::Remote { session } => {
            transport::set_admin_policy(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                name,
                policy,
            )
            .await
        }
    }
}

/// The `ca admin remove <name>` action: revoke a role admin. The daemon's
/// last-manager and reserved-slot guards still apply (so a `force` knob would
/// be meaningless — an orphaned CA key is never allowed).
pub async fn remove_admin(target: &AdminTarget, name: &str) -> Result<()> {
    guard_not_reserved(name)?;
    match target {
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => local::remove_admin(cfg_path, name).await,
        AdminTarget::Remote { session } => {
            transport::remove_admin(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                name,
            )
            .await
        }
    }
}
