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
    answer::{Answerer, Field},
    ca_policy::{AdminInfo, Policy},
    ca_vault,
    plan::enroll::parse_id_map_answer,
};
use anyhow::{Result, bail};
use compact_str::format_compact;
use std::time::Duration;

/// The raw policy knobs an action collects before building a [`Policy`]. The
/// booleans route through the answerer (so strict mode requires them
/// explicitly); the scope lists are typed inputs taken straight from flags.
pub struct PolicyInputs<'a> {
    /// SAN globs this admin may issue (empty ⇒ answerer suggests `*.<domain>`).
    pub allow_san: &'a [String],
    /// Max validity this admin may issue.
    pub max_validity: Duration,
    /// id-map groups this admin may assign (empty ⇒ answerer default `users`).
    pub id_map_groups: &'a [String],
    /// Whether this admin may enroll new admin servers.
    pub may_enroll_servers: Option<bool>,
    /// Whether this admin may manage the roster (add/rescope/remove admins).
    pub may_manage_admins: Option<bool>,
    /// Netidx paths this admin may edit perms under.
    pub perms_scope: &'a [String],
    /// Netidx paths this admin may control services under.
    pub service_scope: &'a [String],
}

/// The `*.<domain>` SAN suggestion, from an explicit domain or by stripping the
/// CA CN's leftmost label; `*` when neither yields a domain.
fn san_suggestion(cn: &str, domain: Option<&str>) -> String {
    match domain {
        Some(d) if !d.is_empty() => format_compact!("*.{d}").into_string(),
        _ => match cn.split_once('.') {
            Some((_, d)) if !d.is_empty() => format_compact!("*.{d}").into_string(),
            _ => "*".to_string(),
        },
    }
}

/// Build a [`Policy`] from the raw inputs, asking the answerer for any knob not
/// supplied by a flag (replaces the old `prompt_policy`). `cn`/`domain` seed the
/// `*.<domain>` SAN suggestion.
pub async fn gather_policy(
    ans: &mut dyn Answerer,
    inputs: PolicyInputs<'_>,
    cn: &str,
    domain: Option<&str>,
) -> Result<Policy> {
    let allowed_san = if !inputs.allow_san.is_empty() {
        inputs.allow_san.to_vec()
    } else {
        let suggestion = san_suggestion(cn, domain);
        let entry = ans
            .text(Field::AllowSan, None, Some(&suggestion), false)
            .await?
            .unwrap_or(suggestion);
        vec![entry]
    };
    let id_map_groups = if !inputs.id_map_groups.is_empty() {
        // An explicit `--id-map-group ''` is "none" — drop blanks so the policy
        // is empty (registration disabled) rather than carrying an empty name.
        inputs
            .id_map_groups
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        let entry = ans
            .text(Field::IdMapGroups, None, Some("users"), false)
            .await?
            .unwrap_or_else(|| "users".to_string());
        parse_id_map_answer(&entry)
    };
    let may_enroll_servers =
        ans.confirm(Field::MayEnrollServers, inputs.may_enroll_servers, false).await?;
    let may_manage_admins =
        ans.confirm(Field::MayManageAdmins, inputs.may_manage_admins, false).await?;
    let trim = |scopes: &[String]| -> Vec<String> {
        scopes.iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
    };
    Ok(Policy {
        allowed_san,
        max_validity: inputs.max_validity,
        id_map_groups,
        may_enroll_servers,
        perms_edit_scopes: trim(inputs.perms_scope),
        may_manage_admins,
        service_control_scopes: trim(inputs.service_scope),
    })
}

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
                &session.admin,
                session.password.as_str(),
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
            admin_local::add_role_admin(cfg_path, name, new_password.as_str(), policy).await
        }
        AdminTarget::Remote { session } => {
            admin_client::add_role_admin(
                session.server,
                NodeKind::Client,
                &session.identity,
                &session.admin,
                session.password.as_str(),
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
                &session.admin,
                session.password.as_str(),
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
        AdminTarget::Local { cfg_path } => admin_local::remove_admin(cfg_path, name).await,
        AdminTarget::Remote { session } => {
            admin_client::remove_admin(
                session.server,
                NodeKind::Client,
                &session.identity,
                &session.admin,
                session.password.as_str(),
                name,
            )
            .await
        }
    }
}
