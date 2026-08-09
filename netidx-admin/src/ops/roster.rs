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
//!
//! **Passwords.** No admin here ever chooses a password for another admin.
//! `add_role_admin` and `reset_password` both mint a one-time key that
//! authorizes nothing but its own replacement, hand it back to be read out
//! once, and leave the holder to set a real one with `change_password`. That
//! removes the `Welcome1`-for-everyone path outright rather than warning
//! against it, and it means the window in which a manager knows a working
//! credential for someone else is empty rather than indefinite.

use super::AdminTarget;
#[cfg(unix)]
use crate::local;
use crate::{
    admin_proto::{AdminCredential, NodeKind, Secret},
    answer::{Answerer, Field},
    transport,
};
use anyhow::{Result, bail};
use netidx_admin_proto::policy::{AdminInfo, Policy, is_reserved_admin};
use zeroize::Zeroizing;

/// The shortest password an admin may choose for itself. Low on purpose: the
/// real defence is the per-source throttle on the CA (see
/// `admin_server::password_limiter`), not a complexity rule that pushes
/// operators towards a sticky note. It exists to catch the empty string and
/// the accidental single keystroke.
pub const MIN_PASSWORD_LEN: usize = 8;

/// A one-time key: 160 bits of Crockford base32, the same generator the CA
/// recovery password uses. Rendered in quads for whoever has to read it out.
fn one_time_password() -> Zeroizing<String> {
    crate::password::gen_crockford_password()
}

/// Check a password an admin chose for itself. Lives here, not in a frontend,
/// so the TUI and the strict CLI cannot disagree about what is acceptable.
pub fn check_new_password(password: &str) -> Result<()> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        bail!("a password must be at least {MIN_PASSWORD_LEN} characters");
    }
    Ok(())
}

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
/// and a one-time password, returned for the caller to show exactly once.
///
/// The new admin cannot use it for anything but `change_password`, so the
/// manager running this never holds a working credential for the admin it
/// just created — only a key that lets its holder pick one.
pub async fn add_role_admin(
    target: &AdminTarget,
    name: &str,
    policy: Policy,
) -> Result<Zeroizing<String>> {
    guard_not_reserved(name)?;
    let new_password = one_time_password();
    match target {
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => {
            local::add_role_admin(cfg_path, name, &new_password, policy).await?
        }
        AdminTarget::Remote { session } => {
            transport::add_role_admin(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                name,
                &new_password,
                policy,
            )
            .await?
        }
    }
    Ok(new_password)
}

/// The `ca admin reset-password <name>` action: replace `name`'s password
/// with a one-time key, returned for the caller to show exactly once.
///
/// Every session `name` currently holds stops working — the CA bumps the
/// slot's credential revision — so this is also how you evict an admin whose
/// credential you believe is compromised.
pub async fn reset_password(
    target: &AdminTarget,
    name: &str,
) -> Result<Zeroizing<String>> {
    guard_not_reserved(name)?;
    let new_password = one_time_password();
    match target {
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => {
            local::reset_password(cfg_path, name, &new_password).await?
        }
        AdminTarget::Remote { session } => {
            transport::reset_password(
                session.server,
                NodeKind::Client,
                &session.identity,
                session.credential.clone(),
                name,
                &new_password,
            )
            .await?
        }
    }
    Ok(new_password)
}

/// The `ca admin change-password` action: replace the **caller's own**
/// password, proving the current one and prompting for the new one (twice,
/// until they match).
///
/// Remote-only. The local control socket authenticates by unix credentials,
/// so there is no keyslot behind it to rekey; a caller that reaches here with
/// a local target has confused "the admin on this box" with "an admin".
pub async fn change_password(
    ans: &mut dyn Answerer,
    target: &AdminTarget,
    new_password: Option<Secret>,
) -> Result<()> {
    let session = match target {
        AdminTarget::Remote { session } => session,
        #[cfg(unix)]
        AdminTarget::Local { .. } => bail!(
            "the local control socket authenticates by unix credentials, not a \
             password — there is nothing to change here. To reset a role admin's \
             password use `netidx admin ca admin reset-password <name>`."
        ),
    };
    let old_password = match &session.credential {
        AdminCredential::Password { password, .. } => password.clone(),
        // A token says a password was typed when it was minted, hours ago —
        // not that whoever holds it now knows one. This op mints a credential
        // that outlives every session and ends them all, so it is proved now.
        AdminCredential::Session { .. } => ans.secret(Field::AdminPassword, None).await?,
    };
    let new_password = confirm_new_password(ans, new_password).await?;
    transport::change_password(
        session.server,
        NodeKind::Client,
        &session.identity,
        &session.admin,
        old_password.as_str(),
        new_password.as_str(),
    )
    .await
}

/// Ask for a new password and its confirmation, re-prompting an interactive
/// frontend until the two match and the result is acceptable. A
/// non-interactive one supplies the value once (`--new-password-file`), so it
/// is validated and returned rather than re-asked — there is nobody to ask.
async fn confirm_new_password(
    ans: &mut dyn Answerer,
    provided: Option<Secret>,
) -> Result<Secret> {
    if !ans.interactive() || provided.is_some() {
        let secret = ans.secret(Field::NewAdminPassword, provided).await?;
        check_new_password(&secret.0)?;
        return Ok(secret);
    }
    loop {
        let first = ans.secret(Field::NewAdminPassword, None).await?;
        if let Err(e) = check_new_password(&first.0) {
            ans.warn(&format!("{e:#}"));
            continue;
        }
        let again = ans.secret(Field::AdminPasswordConfirm, None).await?;
        if first.0 == again.0 {
            return Ok(first);
        }
        ans.warn("the passwords did not match — try again");
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
