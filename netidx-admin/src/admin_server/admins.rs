use super::{
    MutableState, Server, audit,
    auth::{
        PreparedAdminAuthentication, authenticate, authenticate_for_password_change,
        broad_admin, local_superuser, scope_covers, signing_slot,
    },
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
    prepared: &PreparedAdminAuthentication,
) -> admin_proto::LoginResponse {
    if !state.has_ca().await {
        return admin_proto::LoginResponse::Err {
            reason: "login must be sent to the CA".to_string(),
        };
    }
    state
        .write(move |state| {
            let ca = state.ca.as_mut().expect("CA role held");
            match &req.credential {
                admin_proto::AdminCredential::Password { .. } => {
                    // Deliberately the ungated resolution: a one-time password
                    // has to get past *authentication* to be told it must be
                    // changed. Answering that before the password verifies
                    // would name which accounts are mid-reset to anyone who
                    // can guess a login. No session is minted either way.
                    match authenticate_for_password_change(ca, prepared) {
                        Ok(authenticated) if authenticated.must_change => {
                            admin_proto::LoginResponse::PasswordChangeRequired {
                                admin: authenticated.admin,
                            }
                        }
                        Ok(authenticated) => {
                            ca.sessions.login_authenticated(authenticated)
                        }
                        Err(_) => admin_proto::LoginResponse::Err {
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
                reason: "logout must be sent to the CA".to_string(),
            },
            Some(ca) => ca.sessions.logout(&req.credential),
        })
        .await
}

/// Authenticate the caller and require authority to manage administrators.
fn authorize_admin_mgmt(
    ca: &mut ca_store::CaDir,
    credential: &admin_proto::AdminCredential,
    prepared: &PreparedAdminAuthentication,
    local: bool,
) -> std::result::Result<ca_vault::Authenticated, String> {
    if local {
        return Ok(local_superuser());
    }
    let authd = authenticate(ca, credential, prepared)?;
    if broad_admin(&authd) {
        Ok(authd)
    } else {
        Err(format!(
            "admin {:?} is not authorized to manage admins (needs may_manage_admins)",
            authd.admin
        ))
    }
}

/// Whether the caller's issuance glob `caller` covers the granted glob
/// `granted` — i.e. every name `granted` could match is also matched by
/// `caller`. Decidable and **sound** for the realistic DNS patterns (`*`,
/// `*.<suffix>`, and literal names): it never reports coverage that does not
/// hold, so it can't permit an escalation. Patterns it can't prove
/// containment for (a mid-string `*`, a `?`) are conservatively *not* covered
/// — grant those on-box with `ca admin add-role`, which carries no subset
/// check.
fn glob_covers(caller: &str, granted: &str) -> bool {
    if caller == granted {
        return true; // identical pattern
    }
    if caller == "*" {
        // globset's `literal_separator` is off by default (see
        // `glob_star_matches_every_name`), so `*` matches EVERY name, `/`
        // included. This is only sound while that holds.
        return true;
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
/// Both policies are destructured, with no `..`, so that adding a capability
/// to [`Policy`] fails to compile here instead of silently becoming grantable
/// without a subset check. This is the privilege boundary; the compiler, not
/// code review, is what keeps it complete.
fn policy_within(
    caller: &netidx_admin_proto::policy::Policy,
    granted: &netidx_admin_proto::policy::Policy,
) -> std::result::Result<(), String> {
    use netidx_admin_proto::policy::Policy;
    let Policy {
        allowed_san: caller_allowed_san,
        max_validity: caller_max_validity,
        id_map_groups: caller_id_map_groups,
        server_enroll_scopes: caller_server_enroll_scopes,
        server_enroll_roles: caller_server_enroll_roles,
        perms_edit_scopes: caller_perms_edit_scopes,
        may_manage_admins: caller_may_manage_admins,
        service_control_scopes: caller_service_control_scopes,
    } = caller;
    let Policy {
        allowed_san,
        max_validity,
        id_map_groups,
        server_enroll_scopes,
        server_enroll_roles,
        perms_edit_scopes,
        may_manage_admins,
        service_control_scopes,
    } = granted;
    for g in allowed_san {
        if !caller_allowed_san.iter().any(|c| glob_covers(c, g)) {
            return Err(format!(
                "cannot grant issuance scope {g:?}: it is not within your own scope \
                 {caller_allowed_san:?}"
            ));
        }
    }
    if max_validity > caller_max_validity {
        return Err(format!(
            "cannot grant max_validity {} — yours is {}",
            humantime::format_duration(*max_validity),
            humantime::format_duration(*caller_max_validity)
        ));
    }
    for g in id_map_groups {
        if !caller_id_map_groups.iter().any(|c| glob_covers(c, g)) {
            return Err(format!(
                "cannot grant id-map group {g:?}: it is not within your own set \
                 {caller_id_map_groups:?}"
            ));
        }
    }
    for scope in server_enroll_scopes {
        if !scope_covers(caller_server_enroll_scopes, scope) {
            return Err(format!(
                "cannot grant server enrollment scope {scope:?} — it is outside your scopes"
            ));
        }
    }
    if !caller_server_enroll_roles.contains(*server_enroll_roles) {
        return Err(format!(
            "cannot grant server enrollment roles {server_enroll_roles:?} — yours are \
             {caller_server_enroll_roles:?}"
        ));
    }
    if *may_manage_admins && !caller_may_manage_admins {
        return Err("cannot grant may_manage_admins — you do not have it".to_string());
    }
    for s in perms_edit_scopes {
        if !scope_covers(caller_perms_edit_scopes, s) {
            return Err(format!(
                "cannot grant perms scope {s:?}: it is not within your own scopes \
                 {caller_perms_edit_scopes:?}"
            ));
        }
    }
    for s in service_control_scopes {
        if !scope_covers(caller_service_control_scopes, s) {
            return Err(format!(
                "cannot grant service-control scope {s:?}: it is not within your own \
                 scopes {caller_service_control_scopes:?}"
            ));
        }
    }
    Ok(())
}

/// The manage-scope rule: a managing role admin may act on a target only if
/// the target's **current** authority is within its own.
///
/// [`policy_within`] answers a different question — what may be *granted* —
/// and on its own it leaves the target's existing authority unexamined. That
/// gap is what makes reset-password dangerous: a manager with a narrow scope
/// could reset the superuser, read the one-time key off its own terminal, and
/// log in as it. The same check covers rescope and removal, where the damage
/// is destructive rather than acquisitive (demoting or deleting an admin
/// above you) but the rule that forbids it is the same one.
///
/// Signing slots bypass, as everywhere else: they hold the key, so there is
/// nothing above them to protect.
fn manageable(
    caller: &ca_vault::Authenticated,
    target: &str,
    target_policy: &netidx_admin_proto::policy::Policy,
) -> std::result::Result<(), String> {
    if signing_slot(caller) {
        return Ok(());
    }
    policy_within(&caller.policy, target_policy).map_err(|reason| {
        format!("{target:?} holds authority you do not: {reason}. You can only manage admins within your own scope.")
    })
}

/// Whether `target` is the only ROLE admin carrying `may_manage_admins`.
/// Removing or demoting it would strand remote admin management — only the
/// off-box recovery password (a signing slot) could restore it. We refuse
/// that footgun by default; the recovery credential remains the backstop.
fn last_role_manager(vault: &ca_vault::CAVault, target: &str) -> Result<bool> {
    let admins = vault.list_admins()?;
    let is_role_manager = |a: &netidx_admin_proto::policy::AdminInfo| {
        a.kind == netidx_admin_proto::policy::SlotKind::Role && a.policy.may_manage_admins
    };
    let managers = admins.iter().filter(|a| is_role_manager(a)).count();
    let target_is_manager =
        admins.iter().any(|a| a.admin == target && is_role_manager(a));
    Ok(target_is_manager && managers <= 1)
}

pub(super) async fn handle_add_role_admin_prepared(
    state: &Server,
    req: &AddRoleAdminRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
    prepared: std::result::Result<ca_vault::PreparedRoleSlot, String>,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_add_role_admin_inner(state, &req, authentication, local, prepared)
                .await
        })
        .await
}

async fn handle_add_role_admin_inner(
    state: &mut MutableState,
    req: &AddRoleAdminRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
    prepared: std::result::Result<ca_vault::PreparedRoleSlot, String>,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, authentication, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    // The reserved signing-slot names are off-limits (add_role_slot also
    // refuses them, but a clear message beats a generic one).
    if netidx_admin_proto::policy::is_reserved_admin(&req.name) {
        return err(format!(
            "{:?} is a reserved signing-slot name and cannot be a role admin",
            req.name
        ));
    }
    // No escalation — the founding signing credentials are exempt.
    if !signing_slot(&authd)
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
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_set_admin_policy_inner(state, &req, authentication, local).await
        })
        .await
}

async fn handle_set_admin_policy_inner(
    state: &mut MutableState,
    req: &SetAdminPolicyRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, authentication, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if netidx_admin_proto::policy::is_reserved_admin(&req.target) {
        return err(format!("{:?} is a system-managed signing slot", req.target));
    }
    if !signing_slot(&authd)
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
    if kind != netidx_admin_proto::policy::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    if let Err(reason) = manageable(&authd, &req.target, &current) {
        return err(reason);
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
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_remove_admin_inner(state, &req, authentication, local).await
        })
        .await
}

async fn handle_remove_admin_inner(
    state: &mut MutableState,
    req: &RemoveAdminRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, authentication, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if netidx_admin_proto::policy::is_reserved_admin(&req.target) {
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
    if kind != netidx_admin_proto::policy::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    if let Err(reason) = manageable(&authd, &req.target, &current) {
        return err(reason);
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

/// `ChangePassword`: replace the **caller's own** password (CA-only).
///
/// The one op a one-time password may drive, so it authenticates through
/// [`authenticate_for_password_change`] rather than the gated funnel. Three
/// things keep that exemption narrow: what it authenticates is the current
/// password, never a session; the slot rekeyed is `authd.slot_id` — the
/// request carries no target, so no name can redirect it — and a signing slot
/// is refused outright.
///
/// Nothing here reads the request: the password it carries was spent
/// authenticating, and the replacement was derived from it before the write
/// lock. What survives is `authentication` and `prepared`.
pub(super) async fn handle_change_password(
    state: &Server,
    authentication: &PreparedAdminAuthentication,
    local: bool,
    prepared: PreparedRekey,
) -> AdminMgmtResponse {
    state
        .write_async(async move |state| {
            handle_change_password_inner(state, authentication, local, prepared).await
        })
        .await
}

/// A rekey whose KDF has run, or the reason it could not. Both new-password
/// ops prepare off the write lock and commit under it.
pub(super) type PreparedRekey = std::result::Result<ca_vault::PreparedRoleRekey, String>;

async fn handle_change_password_inner(
    state: &mut MutableState,
    authentication: &PreparedAdminAuthentication,
    local: bool,
    prepared: PreparedRekey,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    // Not the local superuser: `SO_PEERCRED` root has no keyslot, so there is
    // no password of its own to change. Saying so beats rekeying nothing.
    if local {
        return err("the local control socket authenticates by unix credentials, not a \
             password — there is nothing to change. To reset a role admin's password \
             use `netidx admin ca admin reset-password <name>`."
            .to_string());
    }
    let authd = match authenticate_for_password_change(ca, authentication) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if signing_slot(&authd) {
        return err(format!(
            "{:?} is a system-managed signing slot; rotate it with `netidx admin ca \
             recovery rotate` or `netidx admin ca auto-approve --rotate`",
            authd.admin
        ));
    }
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(reason) => return err(reason),
    };
    // The prepared rekey names the slot by the id + revision it was derived
    // against; install refuses if either moved. Cross-check that it is *this*
    // caller's slot, so a prepare built for someone else can never install
    // here even if the two raced.
    if !prepared.targets(authd.slot_id, authd.credential_revision) {
        return err(
            "your keyslot changed while the new password was prepared; try again"
                .to_string(),
        );
    }
    match vault.install_role_rekey(prepared).await {
        Ok(()) => {
            audit(&dir, &authd.admin, "change-password", &authd.admin, Duration::ZERO)
                .await;
            AdminMgmtResponse::Ok(())
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `ResetPassword`: replace a role admin's password with the caller's
/// one-time key and lock the slot to `ChangePassword` (CA-only).
pub(super) async fn handle_reset_password(
    state: &Server,
    req: &admin_proto::ResetPasswordRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
    prepared: PreparedRekey,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_reset_password_inner(state, &req, authentication, local, prepared)
                .await
        })
        .await
}

async fn handle_reset_password_inner(
    state: &mut MutableState,
    req: &admin_proto::ResetPasswordRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
    prepared: PreparedRekey,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, authentication, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if netidx_admin_proto::policy::is_reserved_admin(&req.target) {
        return err(format!(
            "{:?} is a system-managed signing slot — rotate it with `recovery rotate` \
             / `auto-approve --rotate`, its password cannot be reset here",
            req.target
        ));
    }
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
    let (kind, current) = match vault.slot_policy(&req.target) {
        Ok(kp) => kp,
        Err(_) => return err(format!("no admin named {:?}", req.target)),
    };
    if kind != netidx_admin_proto::policy::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    // The check that makes reset safe. Unlike a rescope or a removal, this
    // hands the caller a working credential for the target — so a manager may
    // only reset an admin it could not gain anything by becoming.
    if let Err(reason) = manageable(&authd, &req.target, &current) {
        return err(reason);
    }
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(reason) => return err(reason),
    };
    match vault.install_role_rekey(prepared).await {
        Ok(()) => {
            audit(&dir, &authd.admin, "reset-password", &req.target, Duration::ZERO)
                .await;
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
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> AdminListResponse {
    let req = req.clone();
    state
        .write(move |state| handle_list_admins_inner(state, &req, authentication, local))
        .await
}

fn handle_list_admins_inner(
    state: &mut MutableState,
    req: &ListAdminsRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> AdminListResponse {
    let err = |reason: String| AdminListResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    if let Err(reason) = authorize_admin_mgmt(ca, &req.credential, authentication, local)
    {
        return err(reason);
    }
    match ca.vault.list_admins() {
        Ok(admins) => AdminListResponse::Ok(admins),
        Err(e) => err(format!("listing admins: {e:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_server::auth::name_permitted;
    use globset::Glob;
    use netidx_admin_proto::policy::Policy;

    /// `glob_covers` returns true for a caller pattern of `*` against ANY
    /// granted pattern, which is only sound because globset leaves
    /// `literal_separator` off — `*` really does match every name, `/`
    /// included. Pin that: if a future globset default (or a switch to
    /// `GlobBuilder`) turned it on, `*` would stop matching `a/b` and the
    /// coverage claim would silently start permitting escalations.
    #[test]
    fn glob_star_matches_every_name() {
        let star = Glob::new("*").unwrap().compile_matcher();
        assert!(star.is_match("host.example.com"));
        assert!(star.is_match("a/b"));
        assert!(star.is_match(""));
        // The `*.<suffix>` branch relies on the same property.
        let suffix = Glob::new("*.eu.example").unwrap().compile_matcher();
        assert!(suffix.is_match("a/b.eu.example"));
    }

    fn pol() -> Policy {
        Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(86400),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: Default::default(),
            perms_edit_scopes: vec![],
            may_manage_admins: false,
            service_control_scopes: vec![],
        }
    }

    /// The local control-socket superuser holds every capability, id-map
    /// groups included. This was the one field `superuser_policy` left empty,
    /// which denied the on-box admin every group operation while the doc
    /// comment claimed full authority.
    #[test]
    fn the_superuser_holds_id_map_authority_like_every_other_capability() {
        let su = netidx_admin_proto::policy::superuser_policy();
        let mut granted = pol();
        granted.id_map_groups = vec!["users".into(), "wheel".into()];
        assert!(policy_within(&su, &granted).is_ok());
        // And a concrete group is permitted by it — the check issuance and
        // the id-map edits both make.
        assert!(name_permitted("users", &su.id_map_groups).unwrap());
    }

    /// The no-escalation rule, one capability at a time. Every field of
    /// `Policy` is exercised, and `policy_within` destructures both sides
    /// with no `..` so a new field can't be added without landing here.
    #[test]
    fn a_role_admin_can_only_grant_a_subset_of_itself() {
        let scoped = |f: fn(&mut Policy)| {
            let mut p = pol();
            f(&mut p);
            p
        };
        // allowed_san
        let caller = scoped(|p| p.allowed_san = vec!["*.eu.example".into()]);
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.allowed_san = vec!["a.eu.example".into()])
            )
            .is_ok()
        );
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.allowed_san = vec!["a.ap.example".into()])
            )
            .is_err()
        );
        // max_validity
        let caller = scoped(|p| p.max_validity = Duration::from_secs(3600));
        assert!(
            policy_within(&caller, &scoped(|p| p.max_validity = Duration::from_secs(60)))
                .is_ok()
        );
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.max_validity = Duration::from_secs(7200))
            )
            .is_err()
        );
        // id_map_groups
        let caller = scoped(|p| p.id_map_groups = vec!["users".into()]);
        assert!(
            policy_within(&caller, &scoped(|p| p.id_map_groups = vec!["users".into()]))
                .is_ok()
        );
        assert!(
            policy_within(&caller, &scoped(|p| p.id_map_groups = vec!["wheel".into()]))
                .is_err()
        );
        // server_enroll_scopes
        let caller = scoped(|p| p.server_enroll_scopes = vec!["/eu".into()]);
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.server_enroll_scopes = vec!["/eu/x".into()])
            )
            .is_ok()
        );
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.server_enroll_scopes = vec!["/".into()])
            )
            .is_err()
        );
        // server_enroll_roles
        let caller =
            scoped(|p| p.server_enroll_roles = admin_proto::Role::Resolver.into());
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.server_enroll_roles = admin_proto::Role::Resolver.into())
            )
            .is_ok()
        );
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.server_enroll_roles = admin_proto::Role::Ca.into())
            )
            .is_err()
        );
        // may_manage_admins
        assert!(policy_within(&pol(), &scoped(|p| p.may_manage_admins = true)).is_err());
        let caller = scoped(|p| p.may_manage_admins = true);
        assert!(policy_within(&caller, &scoped(|p| p.may_manage_admins = true)).is_ok());
        // perms_edit_scopes
        let caller = scoped(|p| p.perms_edit_scopes = vec!["/eu".into()]);
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.perms_edit_scopes = vec!["/eu/x".into()])
            )
            .is_ok()
        );
        assert!(
            policy_within(&caller, &scoped(|p| p.perms_edit_scopes = vec!["/".into()]))
                .is_err()
        );
        // service_control_scopes
        let caller = scoped(|p| p.service_control_scopes = vec!["/eu".into()]);
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.service_control_scopes = vec!["/eu/x".into()])
            )
            .is_ok()
        );
        assert!(
            policy_within(
                &caller,
                &scoped(|p| p.service_control_scopes = vec!["/".into()])
            )
            .is_err()
        );
    }

    fn role_admin(name: &str, policy: Policy) -> ca_vault::Authenticated {
        ca_vault::Authenticated {
            slot_id: uuid::Uuid::new_v4(),
            credential_revision: 0,
            admin: name.to_string(),
            policy,
            kind: netidx_admin_proto::policy::SlotKind::Role,
            must_change: false,
        }
    }

    /// The manage-scope rule, one capability at a time — the mirror of
    /// `a_role_admin_can_only_grant_a_subset_of_itself` for the target's
    /// *existing* authority. `manageable` delegates to `policy_within`, which
    /// destructures `Policy` with no `..`, so a new capability lands in both.
    ///
    /// Without this, reset-password is an escalation primitive: a manager
    /// scoped to `/eu` resets the superuser, reads the one-time key off its
    /// own terminal, and sets a password for an account that can do anything.
    #[test]
    fn a_role_admin_can_only_manage_admins_within_its_own_scope() {
        let scoped = |f: fn(&mut Policy)| {
            let mut p = pol();
            f(&mut p);
            p
        };
        let manager = |f: fn(&mut Policy)| {
            let mut p = pol();
            p.may_manage_admins = true;
            f(&mut p);
            role_admin("eu-ops", p)
        };
        for (name, caller, within, beyond) in [
            (
                "allowed_san",
                manager(|p| p.allowed_san = vec!["*.eu.example".into()]),
                scoped(|p| p.allowed_san = vec!["a.eu.example".into()]),
                scoped(|p| p.allowed_san = vec!["a.ap.example".into()]),
            ),
            (
                "max_validity",
                manager(|p| p.max_validity = Duration::from_secs(3600)),
                scoped(|p| p.max_validity = Duration::from_secs(60)),
                scoped(|p| p.max_validity = Duration::from_secs(7200)),
            ),
            (
                "id_map_groups",
                manager(|p| p.id_map_groups = vec!["users".into()]),
                scoped(|p| p.id_map_groups = vec!["users".into()]),
                scoped(|p| p.id_map_groups = vec!["wheel".into()]),
            ),
            (
                "server_enroll_scopes",
                manager(|p| p.server_enroll_scopes = vec!["/eu".into()]),
                scoped(|p| p.server_enroll_scopes = vec!["/eu/x".into()]),
                scoped(|p| p.server_enroll_scopes = vec!["/".into()]),
            ),
            (
                "server_enroll_roles",
                manager(|p| p.server_enroll_roles = admin_proto::Role::Resolver.into()),
                scoped(|p| p.server_enroll_roles = admin_proto::Role::Resolver.into()),
                scoped(|p| p.server_enroll_roles = admin_proto::Role::Ca.into()),
            ),
            (
                "perms_edit_scopes",
                manager(|p| p.perms_edit_scopes = vec!["/eu".into()]),
                scoped(|p| p.perms_edit_scopes = vec!["/eu/x".into()]),
                scoped(|p| p.perms_edit_scopes = vec!["/".into()]),
            ),
            (
                "service_control_scopes",
                manager(|p| p.service_control_scopes = vec!["/eu".into()]),
                scoped(|p| p.service_control_scopes = vec!["/eu/x".into()]),
                scoped(|p| p.service_control_scopes = vec!["/".into()]),
            ),
        ] {
            assert!(
                manageable(&caller, "target", &within).is_ok(),
                "{name}: a target within scope must be manageable"
            );
            assert!(
                manageable(&caller, "target", &beyond).is_err(),
                "{name}: a target holding MORE than the caller must not be"
            );
        }
        // Peers still manage each other: the rule bounds authority, it does
        // not make every manager an island. Two admins with the same scope
        // can each reset the other — which is what keeps a locked-out admin
        // recoverable without reaching for the off-box recovery password.
        let peer = manager(|p| p.perms_edit_scopes = vec!["/eu".into()]);
        assert!(
            manageable(
                &peer,
                "peer",
                &scoped(|p| {
                    p.may_manage_admins = true;
                    p.perms_edit_scopes = vec!["/eu".into()];
                })
            )
            .is_ok()
        );
    }

    /// The founding signing credentials bypass the manage-scope rule, exactly
    /// as they bypass no-escalation. They hold the master key, so there is no
    /// authority above them the rule could be protecting.
    #[test]
    fn a_signing_slot_may_manage_any_admin() {
        let mut recovery = role_admin("recovery", pol());
        recovery.kind = netidx_admin_proto::policy::SlotKind::Signing;
        let superuser = netidx_admin_proto::policy::superuser_policy();
        assert!(manageable(&recovery, "superuser", &superuser).is_ok());
        // …and the local control-socket superuser, which authorizes as one.
        assert!(manageable(&local_superuser(), "superuser", &superuser).is_ok());
        // The same target refuses a role admin that does not hold it.
        let narrow = role_admin("eu-ops", {
            let mut p = pol();
            p.may_manage_admins = true;
            p.perms_edit_scopes = vec!["/eu".into()];
            p
        });
        assert!(manageable(&narrow, "superuser", &superuser).is_err());
    }
}
