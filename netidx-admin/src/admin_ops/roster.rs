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
use crate::{
    admin_client, admin_local,
    admin_proto::{NodeKind, Secret},
    ca_policy::{AdminInfo, Policy},
    ca_vault,
};
use anyhow::{Result, bail};

/// Refuse to touch a reserved signing slot as if it were a role admin.
fn guard_not_reserved(name: &str) -> Result<()> {
    if ca_vault::is_reserved_admin(name) {
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
        AdminTarget::Local { cfg_path } => admin_local::list_admins(cfg_path).await,
        AdminTarget::Remote { session } => {
            admin_client::list_admins(
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
        AdminTarget::Local { cfg_path } => {
            admin_local::add_role_admin(cfg_path, name, new_password.as_str(), policy)
                .await
        }
        AdminTarget::Remote { session } => {
            admin_client::add_role_admin(
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
        AdminTarget::Local { cfg_path } => {
            admin_local::set_admin_policy(cfg_path, name, policy).await
        }
        AdminTarget::Remote { session } => {
            admin_client::set_admin_policy(
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
        AdminTarget::Local { cfg_path } => {
            admin_local::remove_admin(cfg_path, name).await
        }
        AdminTarget::Remote { session } => {
            admin_client::remove_admin(
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
