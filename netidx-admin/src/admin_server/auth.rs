use super::{AUTORENEW_ADMIN, Server};
use crate::{
    admin_proto::{self, AddRoleAdminRequest, SERVING_SAN, SignResponse},
    ca_store, ca_vault,
};
use anyhow::{Context, Result};
use globset::Glob;
use log::{info, warn};
use std::{
    net::IpAddr,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, Ordering},
    },
};
use tokio::sync::Semaphore;
use triomphe::Arc as TArc;

pub(super) fn scope_covers(scopes: &[String], target: &str) -> bool {
    scopes.iter().any(|s| netidx::path::Path::is_parent(s, target))
}

pub(super) fn local_superuser() -> ca_vault::Authenticated {
    ca_vault::Authenticated {
        slot_id: uuid::Uuid::nil(),
        credential_revision: 0,
        admin: "local".to_string(),
        policy: netidx_admin_proto::policy::superuser_policy(),
        kind: netidx_admin_proto::policy::SlotKind::Signing,
        // Synthetic — there is no slot behind it and so no password to
        // change. Root on the box is already root on the box.
        must_change: false,
    }
}

/// The refusal a one-time credential gets from everything but
/// [`Request::ChangePassword`](admin_proto::Request::ChangePassword). One
/// string so the CLI's advice and the TUI's routing agree on what happened.
pub(super) const MUST_CHANGE_PASSWORD: &str = concat!(
    "your password was reset and must be changed before you can do anything ",
    "else — run `netidx admin ca admin change-password`"
);

/// Resolve a caller's credential to its live vault slot, and refuse a slot
/// still holding a one-time password.
///
/// The `must_change` check lives here, at the single funnel every handler
/// reaches authentication through, rather than in each op: a handler added
/// later inherits the refusal without anyone having to remember it. What must
/// work anyway asks for [`authenticate_for_password_change`] by name — an
/// exemption spelled out at the sites that need it, not a gap in the default.
///
/// Both credential kinds are validated during the prepare phase (see
/// [`prepare_admin_authentication`]); this revalidates the prepared result
/// against the live vault, so a slot removed or rotated in between is refused.
pub(super) fn authenticate(
    ca: &mut ca_store::CaDir,
    credential: &admin_proto::AdminCredential,
    prepared: &PreparedAdminAuthentication,
) -> std::result::Result<ca_vault::Authenticated, String> {
    let authd = match (credential, prepared) {
        (
            admin_proto::AdminCredential::Password { .. },
            PreparedAdminAuthentication::Password(result),
        ) => revalidate(ca, result, "authentication failed"),
        (
            admin_proto::AdminCredential::Session { .. },
            PreparedAdminAuthentication::Session(result),
        ) => revalidate(ca, result, "login required: session is no longer valid"),
        (
            admin_proto::AdminCredential::Password { .. },
            PreparedAdminAuthentication::Session(_),
        )
        | (
            admin_proto::AdminCredential::Session { .. },
            PreparedAdminAuthentication::Password(_),
        ) => Err("authentication failed".to_string()),
    }?;
    if authd.must_change {
        return Err(MUST_CHANGE_PASSWORD.to_string());
    }
    Ok(authd)
}

/// [`authenticate`] for a **password only**, and without the one-time-password
/// refusal. Its two callers are the two points a one-time password has to get
/// through: the login that answers "change it first", and the change itself.
/// Never call it for anything else — it is the exemption, not a shortcut.
///
/// A session is refused rather than resolved, and by then it is the second
/// refusal: [`admin_proto::ChangePasswordRequest`] has no field a token could
/// arrive in, so a prepared session here means a caller reached this function
/// from somewhere it does not belong.
pub(super) fn authenticate_for_password_change(
    ca: &mut ca_store::CaDir,
    prepared: &PreparedAdminAuthentication,
) -> std::result::Result<ca_vault::Authenticated, String> {
    match prepared {
        PreparedAdminAuthentication::Password(result) => {
            revalidate(ca, result, "authentication failed")
        }
        PreparedAdminAuthentication::Session(_) => {
            Err("authentication failed".to_string())
        }
    }
}

/// Re-resolve a prepared authentication against the live vault, so a slot
/// removed or rekeyed while the (deliberately slow) KDF ran is refused.
fn revalidate(
    ca: &ca_store::CaDir,
    prepared: &std::result::Result<ca_vault::Authenticated, String>,
    stale: &str,
) -> std::result::Result<ca_vault::Authenticated, String> {
    let authenticated = prepared.as_ref().map_err(Clone::clone)?;
    ca.vault
        .resolve_session_slot(authenticated.slot_id, authenticated.credential_revision)
        .map_err(|_| stale.to_string())
}

pub(super) fn safe_auth_failure(
    credential: &admin_proto::AdminCredential,
    reason: String,
) -> String {
    match credential {
        // Session failures carry no password-verification oracle and tell the
        // client to discard its now-useless sealed cache.
        admin_proto::AdminCredential::Session { .. } => reason,
        admin_proto::AdminCredential::Password { .. } => {
            "authentication failed".to_string()
        }
    }
}

/// The server's OWN signing key: unlock the vault with the box-held
/// `autorenew` credential the issuer holds. This is how the server signs on
/// an authenticated, authorized admin's behalf — no admin password ever
/// reaches the key. The opportunistic CRL re-sign rides here (it needs the
/// key, and every signing op passes through). `Err` (a safe wire reason)
/// when the CA holds no autorenew credential (read-only CA). The caller
/// prepares the key from a vault snapshot on the bounded blocking pool; this
/// function revalidates the slot revision against the live vault.
pub(super) async fn server_unlock(
    ca: &mut ca_store::CaDir,
    prepared: &PreparedServerUnlock,
) -> std::result::Result<TArc<ca_vault::Unlocked>, String> {
    let unlocked = prepared.0.as_ref().map_err(Clone::clone)?.clone();
    let current = ca
        .vault
        .resolve_session_slot(unlocked.slot_id, unlocked.credential_revision)
        .map_err(|_| {
            "the CA's autorenew credential changed while its key was unlocking"
                .to_string()
        })?;
    if current.admin != AUTORENEW_ADMIN
        || current.kind != netidx_admin_proto::policy::SlotKind::Signing
    {
        return Err("the prepared CA key did not come from the autorenew slot".into());
    }
    match ca.store.refresh_crl_if_stale(&unlocked.ca_key_pem).await {
        Ok(true) => info!("admin-server: created or refreshed the CRL"),
        Ok(false) => (),
        Err(e) => warn!("admin-server: opportunistic CRL refresh failed: {e:#}"),
    }
    Ok(unlocked)
}

pub(super) async fn run_signing<T, F>(signs: &Arc<Semaphore>, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit =
        signs.clone().acquire_owned().await.expect("sign semaphore is never closed");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await
    .context("CA signing task panicked")
}

/// What a request offers as proof of who is sending it. Most carry an
/// [`admin_proto::AdminCredential`] and so accept either kind; change-password
/// carries a bare password, which is how "a session cannot authorize it"
/// becomes a property of the message rather than a check someone must
/// remember to write.
#[derive(Debug, Clone, Copy)]
pub(super) enum AdminAuth<'a> {
    Credential(&'a admin_proto::AdminCredential),
    Password { admin: &'a str, password: &'a admin_proto::Secret },
}

impl AdminAuth<'_> {
    /// Whether resolving this costs a KDF, and so must be rate limited.
    pub(super) fn is_password(self) -> bool {
        match self {
            Self::Password { .. }
            | Self::Credential(admin_proto::AdminCredential::Password { .. }) => true,
            Self::Credential(admin_proto::AdminCredential::Session { .. }) => false,
        }
    }
}

pub(super) enum PreparedAdminAuthentication {
    Password(std::result::Result<ca_vault::Authenticated, String>),
    Session(std::result::Result<ca_vault::Authenticated, String>),
}

impl PreparedAdminAuthentication {
    /// The credential is already known bad, so no further expensive work
    /// (the server's own Argon2 unlock, a new slot's KDF) may be done on its
    /// behalf. Both credential kinds must answer here: a session token is
    /// validated by a hashmap lookup, so letting an unauthenticated one
    /// through would hand any client a free 64 MiB KDF.
    pub(super) fn credential_failed(&self) -> bool {
        self.credential_failure().is_some()
    }

    pub(super) fn credential_failure(&self) -> Option<String> {
        match self {
            Self::Password(Err(reason)) | Self::Session(Err(reason)) => {
                Some(reason.clone())
            }
            Self::Password(Ok(_)) | Self::Session(Ok(_)) => None,
        }
    }

    /// The admin this credential belongs to, once it has verified. The one
    /// caller is change-password, which needs a slot name to derive the
    /// replacement for *before* it takes the write lock — and must take that
    /// name from the credential rather than the request, so no admin can aim
    /// a password change at someone else's slot.
    pub(super) fn admin_name(&self) -> Option<String> {
        match self {
            Self::Password(Ok(a)) | Self::Session(Ok(a)) => Some(a.admin.clone()),
            Self::Password(Err(_)) | Self::Session(Err(_)) => None,
        }
    }
}

pub(super) struct PreparedServerUnlock(
    std::result::Result<TArc<ca_vault::Unlocked>, String>,
);

impl PreparedServerUnlock {
    pub(super) fn from_result(
        result: std::result::Result<TArc<ca_vault::Unlocked>, String>,
    ) -> Self {
        Self(result)
    }

    pub(super) fn failed(reason: impl Into<String>) -> Self {
        Self(Err(reason.into()))
    }
}

pub(super) async fn prepare_admin_authentication(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    auth: AdminAuth<'_>,
    attempt: Option<PasswordAttempt>,
) -> PreparedAdminAuthentication {
    let (admin, password) = match auth {
        AdminAuth::Password { admin, password } => (admin.to_string(), password.clone()),
        AdminAuth::Credential(admin_proto::AdminCredential::Password {
            admin,
            password,
        }) => (admin.clone(), password.clone()),
        // Validating a session token is a hashmap lookup plus a slot-revision
        // check — cheap, and it must happen here so a bogus token never
        // reaches the KDF-bearing preparation steps below.
        AdminAuth::Credential(admin_proto::AdminCredential::Session { token }) => {
            let token = token.clone();
            return PreparedAdminAuthentication::Session(
                state
                    .write(move |state| match state.ca.as_mut() {
                        Some(ca) => ca.sessions.authenticate_session(&ca.vault, &token),
                        None => Err("this host does not hold the CA".to_string()),
                    })
                    .await,
            );
        }
    };
    let snapshot = match state
        .read(move |state| {
            state.ca.as_ref().context("this host does not hold the CA")?.vault.snapshot()
        })
        .await
    {
        Ok(snapshot) => snapshot,
        Err(e) => {
            return PreparedAdminAuthentication::Password(Err(format!(
                "authentication failed: {e:#}"
            )));
        }
    };
    PreparedAdminAuthentication::Password(
        run_signing(signs, move || {
            let result = snapshot
                .authenticate(&admin, password.as_str())
                .map_err(|_| "authentication failed".to_string());
            if let Some(attempt) = attempt {
                attempt.finish(result.is_ok());
            }
            result
        })
        .await
        .unwrap_or_else(|e| Err(format!("authentication task failed: {e:#}"))),
    )
}

pub(super) async fn prepare_server_unlock(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
) -> PreparedServerUnlock {
    let captured = state
        .read(move |state| {
            let ca = state.ca.as_ref().context("this host does not hold the CA")?;
            let password = ca
                .autorenew_pw
                .clone()
                .context("this CA cannot sign: it holds no autorenew credential")?;
            Ok::<_, anyhow::Error>((ca.vault.snapshot()?, password))
        })
        .await;
    let (snapshot, password) = match captured {
        Ok(captured) => captured,
        Err(e) => return PreparedServerUnlock::failed(format!("{e:#}")),
    };
    PreparedServerUnlock::from_result(
        run_signing(signs, move || {
            snapshot.unlock(&password).map(TArc::new).map_err(|e| {
                format!("the CA's autorenew credential failed to unlock the key: {e:#}")
            })
        })
        .await
        .unwrap_or_else(|e| Err(format!("CA unlock task failed: {e:#}"))),
    )
}

pub(super) async fn prepare_role_slot(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &AddRoleAdminRequest,
) -> std::result::Result<ca_vault::PreparedRoleSlot, String> {
    let snapshot = state
        .read(move |state| {
            state.ca.as_ref().context("this host does not hold the CA")?.vault.snapshot()
        })
        .await
        .map_err(|e| format!("reading the CA vault: {e:#}"))?;
    let name = req.name.clone();
    let password = req.new_password.clone();
    let policy = req.policy.clone();
    let must_change = req.must_change;
    run_signing(signs, move || {
        snapshot
            .prepare_role_slot(&name, password.as_str(), policy, must_change)
            .map_err(|e| format!("{e:#}"))
    })
    .await
    .map_err(|e| format!("preparing the administrator slot: {e:#}"))?
}

/// Derive the replacement slot for a password change or reset. Like
/// [`prepare_role_slot`] this runs the (deliberately slow) Argon2 derivation
/// on the bounded signing pool, off the state write lock; the revision it
/// captures is rechecked at commit.
pub(super) async fn prepare_role_rekey(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    target: &str,
    new_password: &admin_proto::Secret,
    must_change: bool,
) -> std::result::Result<ca_vault::PreparedRoleRekey, String> {
    let snapshot = state
        .read(move |state| {
            state.ca.as_ref().context("this host does not hold the CA")?.vault.snapshot()
        })
        .await
        .map_err(|e| format!("reading the CA vault: {e:#}"))?;
    let target = target.to_string();
    let password = new_password.clone();
    run_signing(signs, move || {
        snapshot
            .prepare_role_rekey(&target, password.as_str(), must_change)
            .map_err(|e| format!("{e:#}"))
    })
    .await
    .map_err(|e| format!("preparing the administrator slot: {e:#}"))?
}

#[derive(Clone)]
pub(super) struct PasswordAttempt(Arc<PasswordAttemptInner>);

struct PasswordAttemptInner {
    state: Weak<Server>,
    source: IpAddr,
    result: AtomicU8,
    runtime: tokio::runtime::Handle,
}

impl PasswordAttemptInner {
    fn result(&self) -> Option<bool> {
        // Matching a u8 used as a tri-state; only this impl ever writes it.
        match self.result.load(Ordering::Acquire) {
            0 => None,
            1 => Some(true),
            2 => Some(false),
            _ => unreachable!(),
        }
    }
}

impl PasswordAttempt {
    pub(super) fn new(state: &Arc<Server>, source: IpAddr) -> Self {
        Self(Arc::new(PasswordAttemptInner {
            state: Arc::downgrade(state),
            source,
            result: AtomicU8::new(0),
            runtime: tokio::runtime::Handle::current(),
        }))
    }

    pub(super) fn finish(&self, success: bool) {
        let result = if success { 1 } else { 2 };
        let _ = self.0.result.compare_exchange(
            0,
            result,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

impl Drop for PasswordAttemptInner {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let source = self.source;
            let result = self.result();
            self.runtime.spawn(async move {
                state
                    .write(move |state| state.password_limiter.complete(source, result))
                    .await;
                state.password_attempt_completed.notify_waiters();
            });
        }
    }
}

/// True if `name` matches any of the administrator's allowed glob patterns.
pub(super) fn name_permitted(name: &str, allowed: &[String]) -> Result<bool> {
    for pat in allowed {
        let glob = Glob::new(pat).with_context(|| format!("bad policy glob {pat:?}"))?;
        if glob.compile_matcher().is_match(name) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether `authd` is authorized to act destructively (revoke / deny) on a
/// certificate or request for `name`. This MUST mirror the issuance gate in
/// [`try_handle`] — otherwise an admin could destroy (revoke/deny) a name it
/// could never have signed, the exact scope-confinement break the role tier
/// exists to prevent.
///
/// - A `Signing` slot (the on-box recovery / autorenew credentials) or a
///   `may_manage_admins` superuser holds authority over everything.
/// - The reserved serving name needs a scoped server-enrollment grant
///   that authorizes minting it.
/// - Any other name must fall within the admin's issuance scope
///   (`allowed_san`).
/// A signing-tier credential: the on-box recovery / autorenew slots, which
/// wrap the master key and so hold the CA key outright. Distinct from
/// [`broad_admin`] on purpose — the no-escalation bypass is signing-only,
/// because a `may_manage_admins` role admin is still bound by the subset rule
/// when granting.
pub(super) fn signing_slot(authd: &ca_vault::Authenticated) -> bool {
    matches!(authd.kind, netidx_admin_proto::policy::SlotKind::Signing)
}

/// An admin whose authority is not scope-bound: a signing slot, or a
/// `may_manage_admins` superuser (it can mint itself any credential, so
/// confining it elsewhere would be theatre).
pub(super) fn broad_admin(authd: &ca_vault::Authenticated) -> bool {
    signing_slot(authd) || authd.policy.may_manage_admins
}

pub(super) fn admin_authority_over(
    authd: &ca_vault::Authenticated,
    name: &str,
) -> Result<bool> {
    if broad_admin(authd) {
        return Ok(true);
    }
    if name.eq_ignore_ascii_case(SERVING_SAN) {
        return Ok(!authd.policy.server_enroll_scopes.is_empty());
    }
    name_permitted(name, &authd.policy.allowed_san)
}

/// Whether `authd` may approve or deny a delegation of the resolver hierarchy subtree
/// at `path`. A delegation restructures the resolver hierarchy under `path`,
/// so — like a perms edit (see [`handle_edit_perms`]) — it needs authority
/// over that subtree: a broad admin (signing slot / `may_manage_admins`) or a
/// `perms_edit_scopes` entry covering the path.
pub(super) fn delegation_authority(authd: &ca_vault::Authenticated, path: &str) -> bool {
    broad_admin(authd) || scope_covers(&authd.policy.perms_edit_scopes, path)
}

pub(super) fn reject(reason: &str) -> SignResponse {
    SignResponse::Err { reason: reason.to_string() }
}

/// The one-live-cert refusal message, shared by the enqueue and sign paths.
/// `live` is the non-empty set of live certs already issued for `name`.
/// Naming the existing cert (serial/dates/glyph) makes clear this is a
/// *previously-issued* certificate, not one this request created — the point
/// of confusion when re-enrolling a node that was enrolled before.
pub(super) fn one_live_refusal(name: &str, live: &[ca_store::IssuedRecord]) -> String {
    let existing = match live {
        [rec] => format!("The existing certificate — {}.", rec.describe()),
        [rec, ..] => {
            format!("{} live certificates exist, e.g. {}.", live.len(), rec.describe())
        }
        [] => "The existing certificate is already live.".to_string(),
    };
    format!(
        "an unexpired certificate already exists for {name:?} — this is a \
         previously-issued certificate, not one this request created. {existing} \
         An admin must revoke it first (`netidx admin ca revoke {name}`), then re-enroll."
    )
}
