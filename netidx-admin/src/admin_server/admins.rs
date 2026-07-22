use super::{
    MutableState, Server, audit,
    auth::{authenticate, local_superuser, prepared_authentication, scope_covers},
};
use crate::{
    admin_proto::{
        self, AddRoleAdminRequest, AdminListResponse, AdminMgmtResponse,
        ListAdminsRequest, RemoveAdminRequest, SetAdminPolicyRequest,
    },
    ca_store, ca_vault,
};
use anyhow::Result;
use std::time::Duration;

pub(super) async fn handle_login(
    state: &Server,
    req: &admin_proto::LoginRequest,
) -> admin_proto::LoginResponse {
    if !state.has_ca().await {
        return admin_proto::LoginResponse::Err {
            reason: "login must be sent to the CA controller".to_string(),
        };
    }
    state
        .write(move |state| {
            let ca = state.ca.as_mut().expect("CA role held");
            match &req.credential {
                admin_proto::AdminCredential::Password { .. } => {
                    match prepared_authentication(ca) {
                        Some(Ok(authenticated)) => {
                            ca.sessions.login_authenticated(authenticated)
                        }
                        Some(Err(_)) | None => admin_proto::LoginResponse::Err {
                            reason: "authentication failed".into(),
                        },
                    }
                }
                admin_proto::AdminCredential::Session { .. } => {
                    admin_proto::LoginResponse::Err {
                        reason: "login requires an administrator password".into(),
                    }
                }
            }
        })
        .await
}

pub(super) async fn handle_logout(
    state: &Server,
    req: &admin_proto::LogoutRequest,
) -> admin_proto::LogoutResponse {
    state
        .write(move |state| match state.ca.as_mut() {
            None => admin_proto::LogoutResponse::Err {
                reason: "logout must be sent to the CA controller".to_string(),
            },
            Some(ca) => ca.sessions.logout(&req.credential),
        })
        .await
}

/// Authenticate the caller and require authority to manage administrators.
fn authorize_admin_mgmt(
    ca: &mut ca_store::CaDir,
    credential: &admin_proto::AdminCredential,
    local: bool,
) -> std::result::Result<ca_vault::Authenticated, String> {
    if local {
        return Ok(local_superuser());
    }
    let authd = authenticate(ca, credential)?;
    if matches!(authd.kind, ca_vault::SlotKind::Signing) || authd.policy.may_manage_admins
    {
        Ok(authd)
    } else {
        Err(format!(
            "admin {:?} is not authorized to manage admins (needs may_manage_admins)",
            authd.admin
        ))
    }
}

/// The synthetic identity for a request over the local control socket: a
/// signing-tier superuser. The signing tier is what every admin-management
/// gate ([`authorize_admin_mgmt`], the `kind == Signing` no-escalation
/// bypass) checks, so this authorizes exactly the way a real recovery /
/// autorenew signing slot does — without a password. Reaching the socket is
/// the authorization (`0600` + `SO_PEERCRED`, root / the daemon's own uid).
fn glob_covers(caller: &str, granted: &str) -> bool {
    if caller == granted {
        return true; // identical pattern
    }
    if caller == "*" {
        return true; // matches every (slash-free) name
    }
    if let Some(suffix) = caller.strip_prefix("*.") {
        // `*.<suffix>` matches exactly the names ending in `.<suffix>`. The
        // granted pattern is covered iff its tail is the literal `.<suffix>`:
        // then every name it matches ends in `.<suffix>`, whatever globbing
        // precedes that tail.
        return granted.ends_with(&format!(".{suffix}"));
    }
    false // a literal (or a pattern we don't reason about) only covers itself
}

/// The no-escalation rule: a managing role admin may grant a target only
/// capabilities that are a subset of its own. Returns the first violated
/// field as a safe reason, or `Ok(())`. The founding signing slots bypass
/// this entirely (they hold the key — the caller checks `kind` first).
fn policy_within(
    caller: &ca_vault::Policy,
    granted: &ca_vault::Policy,
) -> std::result::Result<(), String> {
    for g in &granted.allowed_san {
        if !caller.allowed_san.iter().any(|c| glob_covers(c, g)) {
            return Err(format!(
                "cannot grant issuance scope {g:?}: it is not within your own scope {:?}",
                caller.allowed_san
            ));
        }
    }
    if granted.max_validity > caller.max_validity {
        return Err(format!(
            "cannot grant max_validity {} — yours is {}",
            humantime::format_duration(granted.max_validity),
            humantime::format_duration(caller.max_validity)
        ));
    }
    for g in &granted.id_map_groups {
        if !caller.id_map_groups.contains(g) {
            return Err(format!(
                "cannot grant id-map group {g:?}: it is not in your own set {:?}",
                caller.id_map_groups
            ));
        }
    }
    for scope in &granted.server_enroll_scopes {
        if !scope_covers(&caller.server_enroll_scopes, scope) {
            return Err(format!(
                "cannot grant server enrollment scope {scope:?} — it is outside your scopes"
            ));
        }
    }
    if !caller.server_enroll_roles.contains(granted.server_enroll_roles) {
        return Err(format!(
            "cannot grant server enrollment roles {:?} — yours are {:?}",
            granted.server_enroll_roles, caller.server_enroll_roles
        ));
    }
    if granted.may_manage_admins && !caller.may_manage_admins {
        return Err("cannot grant may_manage_admins — you do not have it".to_string());
    }
    for s in &granted.perms_edit_scopes {
        if !scope_covers(&caller.perms_edit_scopes, s) {
            return Err(format!(
                "cannot grant perms scope {s:?}: it is not within your own scopes {:?}",
                caller.perms_edit_scopes
            ));
        }
    }
    for s in &granted.service_control_scopes {
        if !scope_covers(&caller.service_control_scopes, s) {
            return Err(format!(
                "cannot grant service-control scope {s:?}: it is not within your own \
                 scopes {:?}",
                caller.service_control_scopes
            ));
        }
    }
    Ok(())
}

/// Whether `target` is the only ROLE admin carrying `may_manage_admins`.
/// Removing or demoting it would strand remote admin management — only the
/// off-box recovery password (a signing slot) could restore it. We refuse
/// that footgun by default; the recovery credential remains the backstop.
fn last_role_manager(vault: &ca_vault::CAVault, target: &str) -> Result<bool> {
    let admins = vault.list_admins()?;
    let is_role_manager = |a: &ca_vault::AdminInfo| {
        a.kind == ca_vault::SlotKind::Role && a.policy.may_manage_admins
    };
    let managers = admins.iter().filter(|a| is_role_manager(a)).count();
    let target_is_manager =
        admins.iter().any(|a| a.admin == target && is_role_manager(a));
    Ok(target_is_manager && managers <= 1)
}

pub(super) async fn handle_add_role_admin_prepared(
    state: &Server,
    req: &AddRoleAdminRequest,
    local: bool,
    prepared: std::result::Result<ca_vault::PreparedRoleSlot, String>,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_add_role_admin_inner(state, &req, local, prepared).await
        })
        .await
}

async fn handle_add_role_admin_inner(
    state: &mut MutableState,
    req: &AddRoleAdminRequest,
    local: bool,
    prepared: std::result::Result<ca_vault::PreparedRoleSlot, String>,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    // The reserved signing-slot names are off-limits (add_role_slot also
    // refuses them, but a clear message beats a generic one).
    if ca_vault::is_reserved_admin(&req.name) {
        return err(format!(
            "{:?} is a reserved signing-slot name and cannot be a role admin",
            req.name
        ));
    }
    // No escalation — the founding signing credentials are exempt.
    if !matches!(authd.kind, ca_vault::SlotKind::Signing)
        && let Err(reason) = policy_within(&authd.policy, &req.policy)
    {
        return err(reason);
    }
    // The KDF completed before this write transaction; revalidate and commit the
    // prepared slot while the lock has exclusive access.
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
    let added = match prepared {
        Ok(prepared) => vault.add_prepared_role_slot(prepared).await,
        Err(reason) => return err(reason),
    };
    match added {
        Ok(()) => {
            audit(&dir, &authd.admin, "add-role-admin", &req.name, Duration::ZERO).await;
            AdminMgmtResponse::Ok(())
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `SetAdminPolicy`: rescope an existing role admin (CA-only).
pub(super) async fn handle_set_admin_policy(
    state: &Server,
    req: &SetAdminPolicyRequest,
    local: bool,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_set_admin_policy_inner(state, &req, local).await
        })
        .await
}

async fn handle_set_admin_policy_inner(
    state: &mut MutableState,
    req: &SetAdminPolicyRequest,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if ca_vault::is_reserved_admin(&req.target) {
        return err(format!("{:?} is a system-managed signing slot", req.target));
    }
    if !matches!(authd.kind, ca_vault::SlotKind::Signing)
        && let Err(reason) = policy_within(&authd.policy, &req.policy)
    {
        return err(reason);
    }
    // The state write lock keeps the look-up, last-manager guard, and write one
    // operation, closing the TOCTOU.
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
    // The target must exist and be a role slot — remote ops never touch the
    // master-key-holding signing slots.
    let (kind, current) = match vault.slot_policy(&req.target) {
        Ok(kp) => kp,
        Err(_) => return err(format!("no admin named {:?}", req.target)),
    };
    if kind != ca_vault::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    // Don't let a rescope strand admin management by demoting the last role
    // manager.
    if current.may_manage_admins && !req.policy.may_manage_admins {
        match last_role_manager(vault, &req.target) {
            Ok(true) => {
                return err(format!(
                    "refusing to drop may_manage_admins from {:?}: it is the last role \
                     admin that can manage admins (restoring it would need the recovery \
                     password)",
                    req.target
                ));
            }
            Ok(false) => (),
            Err(e) => return err(format!("checking admin roster: {e:#}")),
        }
    }
    match vault.set_policy(&req.target, req.policy.clone()).await {
        Ok(()) => {
            audit(&dir, &authd.admin, "set-admin-policy", &req.target, Duration::ZERO)
                .await;
            AdminMgmtResponse::Ok(())
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `RemoveAdmin`: remove a role admin (CA-only).
pub(super) async fn handle_remove_admin(
    state: &Server,
    req: &RemoveAdminRequest,
    local: bool,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_remove_admin_inner(state, &req, local).await
        })
        .await
}

async fn handle_remove_admin_inner(
    state: &mut MutableState,
    req: &RemoveAdminRequest,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if ca_vault::is_reserved_admin(&req.target) {
        return err(format!(
            "{:?} is a system-managed signing slot — rotate it with `recovery rotate` \
             / `auto-approve --rotate`, it cannot be removed",
            req.target
        ));
    }
    // The state write lock keeps the look-up, last-manager guard, and removal one
    // operation, so concurrent removals cannot strand management.
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
    let (kind, current) = match vault.slot_policy(&req.target) {
        Ok(kp) => kp,
        Err(_) => return err(format!("no admin named {:?}", req.target)),
    };
    if kind != ca_vault::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    if current.may_manage_admins {
        match last_role_manager(vault, &req.target) {
            Ok(true) => {
                return err(format!(
                    "refusing to remove {:?}: it is the last role admin that can manage \
                     admins (restoring it would need the recovery password)",
                    req.target
                ));
            }
            Ok(false) => (),
            Err(e) => return err(format!("checking admin roster: {e:#}")),
        }
    }
    match vault.remove_slot(&req.target, false).await {
        Ok(()) => {
            audit(&dir, &authd.admin, "remove-admin", &req.target, Duration::ZERO).await;
            AdminMgmtResponse::Ok(())
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `ListAdmins`: the admin roster (CA-only; gated on management authority so
/// a lower-tier role can't read everyone's capabilities).
pub(super) async fn handle_list_admins(
    state: &Server,
    req: &ListAdminsRequest,
    local: bool,
) -> AdminListResponse {
    let req = req.clone();
    state.write(move |state| handle_list_admins_inner(state, &req, local)).await
}

fn handle_list_admins_inner(
    state: &mut MutableState,
    req: &ListAdminsRequest,
    local: bool,
) -> AdminListResponse {
    let err = |reason: String| AdminListResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    if let Err(reason) = authorize_admin_mgmt(ca, &req.credential, local) {
        return err(reason);
    }
    match ca.vault.list_admins() {
        Ok(admins) => AdminListResponse::Ok(admins),
        Err(e) => err(format!("listing admins: {e:#}")),
    }
}
