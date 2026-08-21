//! netidx administration from Graphix.
//!
//! The Graphix face of `netidx_admin`: the fourth consumer of the
//! query/action + `Answerer` seams (after the strict CLI, the ratatui
//! TUI, and Atlas). Design: `design/graphix-admin.md`. The Rust in this
//! crate is BRIDGING ONLY — types, IO, and the `Answerer` seam; decision
//! and presentation logic belongs in Graphix (the no-workarounds rule).

use anyhow::{Error, Result};
use arcstr::ArcStr;
use compact_str::format_compact;
use graphix_compiler::{
    Apply, BuiltIn, Event, ExecCtx, Node, Rt, Scope, TagValue, UserEvent,
    effects::EffectKind, errf, expr::ExprId, typ::FnType,
};
use graphix_package_core::{
    CachedArgs, CachedArgsAsync, CachedVals, EvalCached, EvalCachedAsync, seam_tick,
};
use netidx_admin::{
    answer::Answerer as _,
    ops::{self as aops, AdminTarget},
};
use netidx_admin_proto::{
    fingerprint::Fingerprint,
    policy::{AdminInfo, SlotKind},
};
use netidx_derive::{FromValue, IntoValue};
use netidx_value::{Abstract, ValArray, Value, abstract_type::AbstractWrapper};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, LazyLock},
};

// ── errors ───────────────────────────────────────────────────────

/// Convert an op failure into the package's error union. Typed routing:
/// [`aops::password_change_required`] becomes its own variant because
/// frontends must route on it, not on message text.
pub(crate) fn admin_err(e: Error) -> Value {
    match aops::password_change_required(&e) {
        Some(p) => errf!("PasswordChangeRequired", "{}", p.admin),
        None => errf!("Admin", "{e:#}"),
    }
}

// ── Target (opaque) ──────────────────────────────────────────────

/// The opaque `Target` value: which admin server an operation talks to,
/// shared by pointer identity like every graphix opaque handle.
#[derive(Clone)]
pub(crate) struct TargetValue(pub(crate) Arc<AdminTarget>);

/// Manual because [`AdminTarget`] holds an [`aops::AdminSession`] whose
/// credential must never reach a debug rendering.
impl std::fmt::Debug for TargetValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &*self.0 {
            #[cfg(unix)]
            AdminTarget::Local { cfg_path } => {
                f.debug_struct("Target").field("local", cfg_path).finish()
            }
            AdminTarget::Remote { session } => f
                .debug_struct("Target")
                .field("server", &session.server)
                .field("admin", &session.admin)
                .finish(),
        }
    }
}

impl PartialEq for TargetValue {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for TargetValue {}

impl PartialOrd for TargetValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TargetValue {
    fn cmp(&self, other: &Self) -> Ordering {
        Arc::as_ptr(&self.0).cmp(&Arc::as_ptr(&other.0))
    }
}

impl Hash for TargetValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state)
    }
}

graphix_package_core::impl_no_pack!(TargetValue);

static TARGET_WRAPPER: LazyLock<AbstractWrapper<TargetValue>> = LazyLock::new(|| {
    let id = uuid::Uuid::from_bytes([
        0x6e, 0x41, 0xd2, 0x0c, 0x8a, 0x5f, 0x4b, 0x11, 0x9d, 0x03, 0x77, 0x2e, 0xa9,
        0x64, 0x1c, 0x55,
    ]);
    Abstract::register::<TargetValue>(id).expect("failed to register TargetValue")
});

pub(crate) fn get_target(cached: &CachedVals, idx: usize) -> Option<TargetValue> {
    match cached.0.get(idx)?.as_ref()? {
        Value::Abstract(a) => a.downcast_ref::<TargetValue>().cloned(),
        _ => None,
    }
}

// ── data model mirrors ───────────────────────────────────────────
//
// Wire mirrors of the ops row types, shaped exactly as the `.gxi`
// declares them. `IntoValue` on a named struct produces the sorted-pair
// graphix struct representation; unit enum variants become tag strings
// and payload variants `(tag, payload)` — the graphix variant forms.

#[derive(Debug, Clone, IntoValue, FromValue)]
pub(crate) struct FingerprintV {
    pub(crate) code: String,
    pub(crate) short: String,
}

impl From<&Fingerprint> for FingerprintV {
    fn from(fp: &Fingerprint) -> Self {
        FingerprintV { code: fp.text(), short: fp.short() }
    }
}

#[derive(Debug, Clone, IntoValue)]
enum AgreementV {
    Current { version: u64 },
    Behind { reported: Option<u64>, want: u64 },
    Unrecorded,
    Ahead { reported: u64, ca: u64 },
}

impl From<aops::drift::Agreement> for AgreementV {
    fn from(a: aops::drift::Agreement) -> Self {
        use aops::drift::Agreement as A;
        match a {
            A::Current { version } => AgreementV::Current { version },
            A::Behind { reported, want } => AgreementV::Behind { reported, want },
            A::Unrecorded => AgreementV::Unrecorded,
            A::Ahead { reported, ca } => AgreementV::Ahead { reported, ca },
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct ServerDriftV {
    server: String,
    addr: String,
    perms: Option<AgreementV>,
    id_map: Option<AgreementV>,
    config: Option<AgreementV>,
    config_drift: bool,
}

impl From<aops::drift::ServerDrift> for ServerDriftV {
    fn from(d: aops::drift::ServerDrift) -> Self {
        ServerDriftV {
            server: d.server.to_string(),
            addr: d.addr.to_string(),
            perms: d.perms.map(AgreementV::from),
            id_map: d.id_map.map(AgreementV::from),
            config: d.config.map(AgreementV::from),
            config_drift: d.config_drift,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
enum SlotKindV {
    Signing,
    Role,
}

#[derive(Debug, Clone, IntoValue)]
struct AdminInfoV {
    slot_id: String,
    admin: String,
    kind: SlotKindV,
    policy: ops::PolicyV,
    must_change: bool,
}

impl From<AdminInfo> for AdminInfoV {
    fn from(a: AdminInfo) -> Self {
        AdminInfoV {
            slot_id: a.slot_id.to_string(),
            admin: a.admin,
            kind: match a.kind {
                SlotKind::Signing => SlotKindV::Signing,
                SlotKind::Role => SlotKindV::Role,
            },
            policy: a.policy.into(),
            must_change: a.must_change,
        }
    }
}

pub(crate) fn rows_value<T, V: Into<Value> + From<T>>(rows: Vec<T>) -> Value {
    Value::Array(ValArray::from_iter_exact(rows.into_iter().map(|r| V::from(r).into())))
}

// ── local (async) ────────────────────────────────────────────────

#[derive(Debug, Default)]
struct LocalEv;

impl EvalCachedAsync for LocalEv {
    type Args = Option<PathBuf>;

    const NAME: &str = "netidx_admin_local";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let cfg_path = match cached.0.first()?.as_ref()? {
            Value::String(s) => Some(PathBuf::from(s.as_str())),
            Value::Null => None,
            _ => return None,
        };
        cached.0.get(1)?.as_ref()?;
        Some(cfg_path)
    }

    #[cfg(unix)]
    fn eval(cfg_path: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            let cfg_path = match cfg_path {
                Some(p) if p.is_file() => p,
                Some(p) => {
                    return errf!("Admin", "no admin server config at {}", p.display());
                }
                None => match netidx_admin::paths::discover_admin_server_config() {
                    Ok(p) => p,
                    Err(e) => return admin_err(e),
                },
            };
            TARGET_WRAPPER.wrap(TargetValue(Arc::new(AdminTarget::Local { cfg_path })))
        }
    }

    #[cfg(not(unix))]
    fn eval(_cfg_path: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            errf!(
                "Unsupported",
                "the local admin server control socket is unix-only; use connect"
            )
        }
    }
}

type Local = CachedArgsAsync<LocalEv>;

// ── drift (async) ────────────────────────────────────────────────

#[derive(Debug, Default)]
struct DriftEv;

impl EvalCachedAsync for DriftEv {
    type Args = TargetValue;

    const NAME: &str = "netidx_admin_drift";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        get_target(cached, 0)
    }

    fn eval(t: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match aops::drift::drift(&t.0).await {
                Ok(rows) => rows_value::<_, ServerDriftV>(rows),
                Err(e) => admin_err(e),
            }
        }
    }
}

type Drift = CachedArgsAsync<DriftEv>;

// ── list_admins (async) ──────────────────────────────────────────

#[derive(Debug, Default)]
struct ListAdminsEv;

impl EvalCachedAsync for ListAdminsEv {
    type Args = TargetValue;

    const NAME: &str = "netidx_admin_list_admins";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        get_target(cached, 0)
    }

    fn eval(t: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match aops::roster::list_admins(&t.0).await {
                Ok(rows) => rows_value::<_, AdminInfoV>(rows),
                Err(e) => admin_err(e),
            }
        }
    }
}

type ListAdmins = CachedArgsAsync<ListAdminsEv>;

// ── parse_fingerprint (sync, pure) ───────────────────────────────

#[derive(Debug, Default)]
struct ParseFingerprintEv;

impl<R: Rt, E: UserEvent> EvalCached<R, E> for ParseFingerprintEv {
    const EFFECT: EffectKind = EffectKind::Sync;
    const STATELESS: bool = true;
    const NAME: &str = "netidx_admin_parse_fingerprint";

    fn eval(&mut self, _ctx: &mut ExecCtx<R, E>, cached: &CachedVals) -> Option<Value> {
        let code = cached.get::<String>(0)?;
        Some(match Fingerprint::parse_text(&code) {
            Ok(fp) => FingerprintV::from(&fp).into(),
            Err(e) => errf!("Admin", "{e:#}"),
        })
    }
}

type ParseFingerprint = CachedArgs<ParseFingerprintEv>;

// ── identicon (sync, pure) ───────────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
struct RgbV {
    r: u8,
    g: u8,
    b: u8,
}

#[derive(Debug, Clone, IntoValue)]
struct IdenticonV {
    cells: Vec<Vec<bool>>,
    color: RgbV,
}

#[derive(Debug, Default)]
struct IdenticonEv;

impl<R: Rt, E: UserEvent> EvalCached<R, E> for IdenticonEv {
    const EFFECT: EffectKind = EffectKind::Sync;
    const STATELESS: bool = true;
    const NAME: &str = "netidx_admin_identicon";

    fn eval(&mut self, _ctx: &mut ExecCtx<R, E>, cached: &CachedVals) -> Option<Value> {
        let fp = cached.get::<FingerprintV>(0)?;
        Some(match Fingerprint::parse_text(&fp.code) {
            Ok(fp) => {
                let (r, g, b) = fp.identicon_color();
                IdenticonV {
                    cells: fp.identicon_cells().iter().map(|row| row.to_vec()).collect(),
                    color: RgbV { r, g, b },
                }
                .into()
            }
            Err(e) => errf!("Admin", "{e:#}"),
        })
    }
}

type Identicon = CachedArgs<IdenticonEv>;

// ── connect (ceremony) ───────────────────────────────────────────

/// Open an authenticated remote-admin session as a ceremony: the
/// identity confirmation, admin name, and password arrive as events
/// unless provided; `Done` carries the `Target`.
#[derive(Debug)]
pub(crate) struct Connect {
    admin: Option<String>,
    password: Option<String>,
    ca_dir: Option<String>,
    out: TagValue,
}

impl<R: Rt, E: UserEvent> BuiltIn<R, E> for Connect {
    const EFFECT: EffectKind = EffectKind::Async;
    const NAME: &str = "netidx_admin_connect";

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        _from: &'c [Node<R, E>],
        _top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(Connect {
            admin: None,
            password: None,
            ca_dir: None,
            out: TagValue::phantom(),
        }))
    }
}

fn opt_str_arg<R: Rt, E: UserEvent>(
    ctx: &mut ExecCtx<R, E>,
    node: &mut Node<R, E>,
    event: &mut Event<E>,
    into: &mut Option<String>,
) {
    if let Some(tv) = seam_tick(node.update(ctx, event)) {
        *into = match tv.value_cloned() {
            Value::String(s) => Some(s.to_string()),
            _ => None,
        };
    }
}

impl<R: Rt, E: UserEvent> Apply<R, E> for Connect {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> &TagValue {
        let (admin_n, rest) = from.split_first_mut().unwrap();
        let (password_n, rest) = rest.split_first_mut().unwrap();
        let (ca_dir_n, rest) = rest.split_first_mut().unwrap();
        let (server_n, _) = rest.split_first_mut().unwrap();
        opt_str_arg(ctx, admin_n, event, &mut self.admin);
        opt_str_arg(ctx, password_n, event, &mut self.password);
        opt_str_arg(ctx, ca_dir_n, event, &mut self.ca_dir);
        let server = match seam_tick(server_n.update(ctx, event)) {
            Some(tv) => match tv.value_cloned() {
                Value::String(s) => s,
                _ => return self.out.ride(),
            },
            None => return self.out.ride(),
        };
        let addr: SocketAddr = match server.parse() {
            Ok(a) => a,
            Err(e) => {
                return self
                    .out
                    .set(TagValue::fired(errf!("Admin", "bad server address: {e}")));
            }
        };
        let admin = self.admin.clone();
        let password = self.password.clone().map(netidx_admin_proto::Secret);
        let ca_dir = self.ca_dir.clone().map(PathBuf::from);
        let op: ceremony::BoxOp = Box::new(move |ans| {
            Box::pin(async move {
                let session =
                    aops::open_admin_session(ans, Some(addr), ca_dir, admin, password)
                        .await?;
                // Exchange the password for a bearer token — which is also
                // where the password is VERIFIED (a password session is a
                // credential holder; the server checks on first use) and
                // where PasswordChangeRequired surfaces. The cached token
                // is what keeps every later op on this target quiet.
                match aops::cache_session(&session, aops::Retention::ProcessLifetime)
                    .await
                {
                    Ok(Some(logged)) => {
                        if let Some(why) = logged.unsealed {
                            ans.note(&format_compact!(
                                "{why}; this login lasts until the process exits"
                            ));
                        }
                    }
                    Ok(None) => (),
                    Err(e) => return Err(e),
                }
                Ok(TARGET_WRAPPER
                    .wrap(TargetValue(Arc::new(AdminTarget::Remote { session }))))
            })
        });
        self.out.set(TagValue::fired(ceremony::start_ceremony(ctx, None, op)))
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {}

    fn reset_replay(&mut self, _ctx: &mut ExecCtx<R, E>) {}
}

// ── package registration ─────────────────────────────────────────

graphix_derive::defpackage! {
    builtins => [
        Local,
        Drift,
        ListAdmins,
        ParseFingerprint,
        Identicon,
        Connect,
        ceremony::Events,
        ceremony::Answer,
        ops::ListQueue,
        ops::ApproveReq,
        ops::ApproveRenewals,
        ops::Deny,
        ops::Issued,
        ops::Revoke,
        ops::ListPendingDelegations,
        ops::ApproveDelegation,
        ops::DenyDelegation,
        ops::ListServers,
        ops::RemoveServer,
        ops::ReconcileCa,
        ops::ChangePassword,
        ops::ListResolverClusters,
        ops::ShowIdMap,
        ops::IdMapAddGroup,
        ops::IdMapRemoveGroup,
        ops::IdMapAddUser,
        ops::IdMapRemoveUser,
        ops::IdMapAddMember,
        ops::IdMapRemoveMember,
        ops::AddRoleAdmin,
        ops::ResetPassword,
        ops::SetAdminPolicy,
        ops::RemoveAdmin,
        ops::Discover,
    ],
}

mod ceremony;
mod ops;

#[cfg(all(test, unix))]
mod e2e;
#[cfg(test)]
mod test;
#[cfg(all(test, unix))]
mod tui_test;
