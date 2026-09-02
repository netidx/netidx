//! The remote-admin query/action surface.
//!
//! Two shapes, mirroring how the ratatui TUI drives the same
//! `netidx_admin::ops` functions:
//!
//! - **Remote ceremonies** ([`RemoteOp`] / [`RemoteCeremony`]): the
//!   answerer-threaded ops (queue, revoke, delegation, servers). Each
//!   takes a `Target` and runs with the session's server + admin name
//!   and a pinned-glyph answerer, so in steady state no questions fire —
//!   but a session-cache expiry honestly surfaces a `Secret` question
//!   instead of failing.
//! - **Plain async builtins**: the `AdminTarget`-based ops (roster,
//!   id-map, clusters) that never converse.

use crate::{
    FingerprintV, TargetValue, admin_err,
    ceremony::{self, BoxOp, Trigger},
    get_target, rows_value,
};
use anyhow::{Result, anyhow, bail};
use arcstr::ArcStr;
use enumflags2::BitFlags;
use graphix_compiler::{
    Apply, BuiltIn, Event, ExecCtx, Node, Rt, Scope, TagValue, UserEvent,
    effects::EffectKind, errf, expr::ExprId, typ::FnType,
};
use graphix_package_core::{CachedArgsAsync, CachedVals, EvalCachedAsync};
use netidx::resolver_server::config::ReadGate;
use netidx_admin::{
    discovery,
    ops::{self, AdminTarget, RecordedEdit},
};
use netidx_admin_proto::{
    AdminServerId, PeerResult, Role, Secret, ServiceUnit, ServiceUnitDef,
    fingerprint::Fingerprint, policy::Policy,
};
use netidx_derive::{FromValue, IntoValue};
use netidx_value::{FromValue, Value};
use std::{fmt::Debug, marker::PhantomData, net::SocketAddr, time::Duration};

// ── shared conversions ───────────────────────────────────────────

fn roles_value(roles: BitFlags<Role>) -> Vec<String> {
    roles.iter().map(|r| format!("{r:?}")).collect()
}

fn parse_roles(names: &[String]) -> Result<BitFlags<Role>> {
    let mut out = BitFlags::empty();
    for n in names {
        out.insert(match n.as_str() {
            "Ca" => Role::Ca,
            "Resolver" => Role::Resolver,
            "IdMap" => Role::IdMap,
            n => bail!("unknown role {n}: expected Ca, Resolver, or IdMap"),
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, IntoValue)]
struct PeerResultV {
    server: String,
    addr: String,
    error: Option<String>,
}

impl From<PeerResult> for PeerResultV {
    fn from(p: PeerResult) -> Self {
        PeerResultV {
            server: p.server.to_string(),
            addr: p.addr.to_string(),
            error: p.error,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct QueueItemV {
    code: Option<FingerprintV>,
    kind: String,
    requested_name: String,
    requested_validity: Duration,
    age_secs: u64,
    peer: String,
    verified_renewal: bool,
    enroll_listen: Option<String>,
    requested_roles: Vec<String>,
    cluster_base: Option<String>,
    replaces_serial: Option<u64>,
}

impl From<ops::queue::QueueItem> for QueueItemV {
    fn from(q: ops::queue::QueueItem) -> Self {
        QueueItemV {
            code: q.code.as_ref().map(FingerprintV::from),
            kind: format!("{:?}", q.kind),
            requested_name: q.requested_name,
            requested_validity: q.requested_validity,
            age_secs: q.age_secs,
            peer: q.peer,
            verified_renewal: q.verified_renewal,
            enroll_listen: q.enroll_listen.map(|a| a.to_string()),
            requested_roles: roles_value(q.requested_roles),
            cluster_base: q.cluster_base,
            replaces_serial: q.replaces_serial,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct ApproveOutcomeV {
    requested_name: String,
    enroll_listen: Option<String>,
    id_map_groups: Vec<String>,
    warnings: Vec<String>,
}

impl From<ops::queue::ApproveOutcome> for ApproveOutcomeV {
    fn from(o: ops::queue::ApproveOutcome) -> Self {
        ApproveOutcomeV {
            requested_name: o.requested_name,
            enroll_listen: o.enroll_listen.map(|a| a.to_string()),
            id_map_groups: o.id_map_groups,
            warnings: o.warnings,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct RenewalResultV {
    requested_name: String,
    error: Option<String>,
}

impl From<ops::queue::RenewalResult> for RenewalResultV {
    fn from(r: ops::queue::RenewalResult) -> Self {
        RenewalResultV { requested_name: r.requested_name, error: r.error }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct IssuedEntryV {
    serial: u64,
    name: String,
    spki_fp: String,
    not_after_unix: u64,
    revoked: bool,
}

impl From<netidx_admin_proto::IssuedEntry> for IssuedEntryV {
    fn from(e: netidx_admin_proto::IssuedEntry) -> Self {
        IssuedEntryV {
            serial: e.serial,
            name: e.name,
            spki_fp: e.spki_fp,
            not_after_unix: e.not_after_unix,
            revoked: e.revoked,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct RevokeOutcomeV {
    revoked: Vec<IssuedEntryV>,
    warnings: Vec<String>,
    operation_id: Option<String>,
    peers: Vec<PeerResultV>,
}

impl From<ops::revoke::RevokeOutcome> for RevokeOutcomeV {
    fn from(o: ops::revoke::RevokeOutcome) -> Self {
        RevokeOutcomeV {
            revoked: o.revoked.into_iter().map(IssuedEntryV::from).collect(),
            warnings: o.warnings,
            operation_id: o.operation_id.map(|o| o.to_string()),
            peers: o.peers.into_iter().map(PeerResultV::from).collect(),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct PendingDelegationV {
    code: FingerprintV,
    proposed_path: String,
    parent_base: String,
    child_base: String,
    parent: Vec<String>,
    child: Vec<String>,
    approved: bool,
    age_secs: u64,
    peer: String,
}

impl From<ops::delegation::PendingDelegation> for PendingDelegationV {
    fn from(d: ops::delegation::PendingDelegation) -> Self {
        PendingDelegationV {
            code: FingerprintV::from(&d.code),
            proposed_path: d.proposed_path,
            parent_base: d.parent_base,
            child_base: d.child_base,
            parent: d.parent.iter().map(|a| a.addr.to_string()).collect(),
            child: d.child.iter().map(|a| a.addr.to_string()).collect(),
            approved: d.approved,
            age_secs: d.age_secs,
            peer: d.peer,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct DelegationDecisionV {
    proposed_path: String,
    peers: Vec<PeerResultV>,
}

impl From<ops::delegation::DelegationDecision> for DelegationDecisionV {
    fn from(d: ops::delegation::DelegationDecision) -> Self {
        DelegationDecisionV {
            proposed_path: d.proposed_path,
            peers: d.peers.into_iter().map(PeerResultV::from).collect(),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct ServerInfoV {
    id: String,
    addr: String,
    roles: Vec<String>,
    state: String,
    resolver: Option<String>,
    cluster_base: Option<String>,
    cluster_state: Option<String>,
    ca: bool,
    read_gate: Option<ReadGateV>,
}

/// A read gate as a server reports it.
#[derive(Debug, Clone, IntoValue)]
enum ReadGateV {
    Open,
    Shut,
    Until(chrono::DateTime<chrono::Utc>),
}

impl From<ReadGate> for ReadGateV {
    fn from(g: ReadGate) -> Self {
        match g {
            ReadGate::No => ReadGateV::Open,
            ReadGate::Yes => ReadGateV::Shut,
            ReadGate::Until(t) => ReadGateV::Until(t),
        }
    }
}

/// A gate to set: the reported forms, or shut for a while from now.
#[derive(Debug, Clone, FromValue)]
enum GateRequestV {
    Open,
    Shut,
    Until(chrono::DateTime<chrono::Utc>),
    ShutFor(Duration),
}

impl TryFrom<GateRequestV> for ReadGate {
    type Error = anyhow::Error;

    fn try_from(g: GateRequestV) -> Result<ReadGate> {
        Ok(match g {
            GateRequestV::Open => ReadGate::No,
            GateRequestV::Shut => ReadGate::Yes,
            GateRequestV::Until(t) => ReadGate::Until(t),
            GateRequestV::ShutFor(d) => ops::servers::read_gate_for(d)?,
        })
    }
}

#[derive(Debug, Clone, IntoValue)]
struct ServiceServerV {
    id: String,
    addr: String,
    base: String,
    roles: Vec<String>,
}

impl From<ops::service::ServiceServer> for ServiceServerV {
    fn from(s: ops::service::ServiceServer) -> Self {
        ServiceServerV {
            id: s.id.to_string(),
            addr: s.addr.to_string(),
            base: s.base,
            roles: roles_value(s.roles),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
enum UnitStateV {
    NotStarted,
    Running(Option<u32>),
    Stopped,
    Died,
}

#[derive(Debug, Clone, IntoValue)]
struct ServiceUnitDefV {
    exe: String,
    args: Vec<String>,
    trigger: String,
    restart: String,
}

#[derive(Debug, Clone, IntoValue)]
struct ServiceUnitV {
    unit: String,
    state: UnitStateV,
    definition: Option<ServiceUnitDefV>,
}

impl From<ServiceUnit> for ServiceUnitV {
    fn from(u: ServiceUnit) -> Self {
        use netidx_activation::control::UnitState as S;
        ServiceUnitV {
            unit: u.unit,
            state: match u.state {
                S::NotStarted => UnitStateV::NotStarted,
                S::Running { pid } => UnitStateV::Running(pid),
                S::Stopped => UnitStateV::Stopped,
                S::Died => UnitStateV::Died,
            },
            definition: u.definition.map(
                |ServiceUnitDef { exe, args, trigger, restart }| ServiceUnitDefV {
                    exe,
                    args,
                    trigger,
                    restart,
                },
            ),
        }
    }
}

#[derive(Debug, Clone, FromValue)]
enum ServiceOpV {
    Start,
    Stop,
    Restart,
    Status,
}

impl From<ServiceOpV> for netidx_activation::control::ControlOp {
    fn from(op: ServiceOpV) -> Self {
        use netidx_activation::control::ControlOp as C;
        match op {
            ServiceOpV::Start => C::Start,
            ServiceOpV::Stop => C::Stop,
            ServiceOpV::Restart => C::Restart,
            ServiceOpV::Status => C::Status,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct PermEntryV {
    path: String,
    entity: String,
    bits: String,
}

fn perm_entries(p: &netidx::resolver_server::config::PMap) -> Value {
    let mut rows: Vec<PermEntryV> = netidx_admin::perms::iter(p)
        .map(|(path, entity, bits)| PermEntryV {
            path: path.to_string(),
            entity: entity.to_string(),
            bits: bits.to_string(),
        })
        .collect();
    rows.sort_by(|a, b| (&a.path, &a.entity).cmp(&(&b.path, &b.entity)));
    rows_value::<_, PermEntryV>(rows)
}

impl From<ops::servers::ServerInfo> for ServerInfoV {
    fn from(s: ops::servers::ServerInfo) -> Self {
        ServerInfoV {
            id: s.id.to_string(),
            addr: s.addr.to_string(),
            roles: roles_value(s.roles),
            state: format!("{:?}", s.state),
            resolver: s.resolver.map(|r| r.addr.to_string()),
            cluster_base: s.cluster_base.clone(),
            cluster_state: s.cluster_state.map(|c| format!("{c:?}")),
            ca: s.ca,
            read_gate: s.read_gate.map(ReadGateV::from),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct RemoveServerOutcomeV {
    version: u64,
    operation_id: Option<String>,
    revoked: u64,
    removed: bool,
    affected_clusters: Vec<String>,
    peers: Vec<PeerResultV>,
    crl_peers: Vec<PeerResultV>,
    gated: Option<PeerResultV>,
}

impl From<netidx_admin::transport::RemoveServerOutcome> for RemoveServerOutcomeV {
    fn from(o: netidx_admin::transport::RemoveServerOutcome) -> Self {
        RemoveServerOutcomeV {
            version: o.version,
            operation_id: o.operation_id.map(|o| o.to_string()),
            revoked: o.revoked,
            removed: o.removed,
            affected_clusters: o.affected_clusters,
            peers: o.peers.into_iter().map(PeerResultV::from).collect(),
            crl_peers: o.crl_peers.into_iter().map(PeerResultV::from).collect(),
            gated: o.gated.map(PeerResultV::from),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct ReconcileOutcomeV {
    operation: String,
    peers: Vec<PeerResultV>,
}

#[derive(Debug, Clone, IntoValue, FromValue)]
pub(crate) struct PolicyV {
    pub(crate) allowed_san: Vec<String>,
    pub(crate) max_validity: Duration,
    pub(crate) id_map_groups: Vec<String>,
    pub(crate) server_enroll_scopes: Vec<String>,
    pub(crate) server_enroll_roles: Vec<String>,
    pub(crate) perms_edit_scopes: Vec<String>,
    pub(crate) may_manage_admins: bool,
    pub(crate) service_control_scopes: Vec<String>,
}

impl From<Policy> for PolicyV {
    fn from(p: Policy) -> Self {
        PolicyV {
            allowed_san: p.allowed_san,
            max_validity: p.max_validity,
            id_map_groups: p.id_map_groups,
            server_enroll_scopes: p.server_enroll_scopes,
            server_enroll_roles: roles_value(p.server_enroll_roles),
            perms_edit_scopes: p.perms_edit_scopes,
            may_manage_admins: p.may_manage_admins,
            service_control_scopes: p.service_control_scopes,
        }
    }
}

impl TryFrom<PolicyV> for Policy {
    type Error = anyhow::Error;

    fn try_from(p: PolicyV) -> Result<Policy> {
        Ok(Policy {
            allowed_san: p.allowed_san,
            max_validity: p.max_validity,
            id_map_groups: p.id_map_groups,
            server_enroll_scopes: p.server_enroll_scopes,
            server_enroll_roles: parse_roles(&p.server_enroll_roles)?,
            perms_edit_scopes: p.perms_edit_scopes,
            may_manage_admins: p.may_manage_admins,
            service_control_scopes: p.service_control_scopes,
        })
    }
}

#[derive(Debug, Clone, IntoValue)]
struct IdentityV {
    name: String,
    primary_group: String,
    groups: Vec<String>,
}

#[derive(Debug, Clone, IntoValue)]
struct IdMapV {
    default_group: Option<String>,
    groups: Vec<String>,
    identities: Vec<IdentityV>,
}

impl From<netidx_id_map::file::IdMap> for IdMapV {
    fn from(m: netidx_id_map::file::IdMap) -> Self {
        IdMapV {
            default_group: m.default_group.map(|g| g.to_string()),
            groups: m.groups.iter().map(|g| g.to_string()).collect(),
            identities: m
                .identities
                .iter()
                .map(|(name, id)| IdentityV {
                    name: name.to_string(),
                    primary_group: id.primary_group.to_string(),
                    groups: id.groups.iter().map(|g| g.to_string()).collect(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct RecordedEditV {
    version: u64,
    changed: bool,
}

impl From<RecordedEdit> for RecordedEditV {
    fn from(e: RecordedEdit) -> Self {
        RecordedEditV { version: e.version, changed: e.changed }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct DiscoveredV {
    addrs: Vec<String>,
    port: u16,
    domain: String,
    roles: Vec<String>,
    fp_short: String,
}

impl From<discovery::Discovered> for DiscoveredV {
    fn from(d: discovery::Discovered) -> Self {
        DiscoveredV {
            addrs: d.addrs.iter().map(|a| a.to_string()).collect(),
            port: d.port,
            domain: d.domain,
            roles: roles_value(d.roles),
            fp_short: d.fp_short,
        }
    }
}

// ── config value helpers ─────────────────────────────────────────

pub(crate) fn opt_string(v: Option<&Value>) -> Result<Option<String>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.to_string())),
        Some(v) => bail!("expected a string or null, got {v}"),
    }
}

pub(crate) fn req_string(v: Option<&Value>, what: &str) -> Result<String> {
    match opt_string(v)? {
        Some(s) => Ok(s),
        None => bail!("{what} is required"),
    }
}

/// A `[Fingerprint, null]` arg: the CA fingerprint already confirmed.
pub(crate) fn opt_glyph(v: Option<&Value>) -> Result<Option<Fingerprint>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            let fp = FingerprintV::from_value(v.clone())?;
            Ok(Some(Fingerprint::parse_text(&fp.code)?))
        }
    }
}

/// An admin server's immutable id, as `ServerInfo.id` shows it.
fn server_id(v: Option<&Value>) -> Result<AdminServerId> {
    let id = req_string(v, "the server id")?;
    Ok(AdminServerId(id.parse().map_err(|e| anyhow!("bad server id: {e}"))?))
}

/// The trailing `Target` arg of a ceremony.
fn target_arg(v: Option<&Value>) -> Option<TargetValue> {
    match v? {
        Value::Abstract(a) => a.downcast_ref::<TargetValue>().cloned(),
        _ => None,
    }
}

fn opt_bool(v: Option<&Value>, default: bool) -> Result<bool> {
    match v {
        None | Some(Value::Null) => Ok(default),
        Some(Value::Bool(b)) => Ok(*b),
        Some(v) => bail!("expected a bool, got {v}"),
    }
}

fn string_list(v: Option<&Value>) -> Result<Vec<String>> {
    match v {
        None | Some(Value::Null) => Ok(vec![]),
        Some(Value::Array(a)) => a
            .iter()
            .map(|v| match v {
                Value::String(s) => Ok(s.to_string()),
                v => bail!("expected a string, got {v}"),
            })
            .collect(),
        Some(v) => bail!("expected an array of strings, got {v}"),
    }
}

// ── the remote-ceremony builtin family ───────────────────────────

/// The connection facts a remote op runs with, taken from the target's
/// authenticated session.
pub(crate) struct ConnInfo {
    pub(crate) server: SocketAddr,
    pub(crate) admin: String,
}

impl TargetValue {
    /// The remote session's facts, or an error for a Local target —
    /// these ops live on the admin plane.
    fn remote(&self) -> Result<(ConnInfo, Fingerprint)> {
        match &*self.0 {
            AdminTarget::Remote { session } => Ok((
                ConnInfo { server: session.server, admin: session.admin.clone() },
                session.identity.fingerprint.clone(),
            )),
            #[cfg(unix)]
            AdminTarget::Local { .. } => Err(anyhow!(
                "this operation needs an authenticated remote session — use connect"
            )),
        }
    }
}

pub(crate) trait RemoteOp: Debug + Send + Sync + 'static {
    const NAME: &'static str;
    /// How many leading config args precede the trailing target.
    const NCFG: usize;
    /// Build the ceremony's op from the latest config values. An error
    /// becomes the mint site's error value.
    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp>;
}

#[derive(Debug)]
pub(crate) struct RemoteCeremony<T: RemoteOp> {
    trigger: Trigger,
    out: TagValue,
    ph: PhantomData<T>,
}

impl<R: Rt, E: UserEvent, T: RemoteOp> BuiltIn<R, E> for RemoteCeremony<T> {
    const EFFECT: EffectKind = EffectKind::Async;
    const NAME: &str = T::NAME;

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        from: &'c [Node<R, E>],
        _top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(RemoteCeremony::<T> {
            trigger: Trigger::new(from),
            out: TagValue::phantom(),
            ph: PhantomData,
        }))
    }
}

impl<R: Rt, E: UserEvent, T: RemoteOp> Apply<R, E> for RemoteCeremony<T> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> &TagValue {
        let Some(args) = self.trigger.tick(ctx, from, event) else {
            return self.out.ride();
        };
        let Some(target) = target_arg(args[T::NCFG].as_ref()) else {
            return self.out.ride();
        };
        let (conn, fp) = match target.remote() {
            Ok(r) => r,
            Err(e) => return self.out.set(TagValue::fired(admin_err(e))),
        };
        match T::op(&args[..T::NCFG], conn) {
            Ok(op) => {
                self.out.set(TagValue::fired(ceremony::start_ceremony(ctx, Some(fp), op)))
            }
            Err(e) => self.out.set(TagValue::fired(errf!("Admin", "{e:#}"))),
        }
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {}

    fn reset_replay(&mut self, _ctx: &mut ExecCtx<R, E>) {}
}

// ── the remote ops ───────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct ListQueueOp;

impl RemoteOp for ListQueueOp {
    const NAME: &'static str = "netidx_admin_list_queue";
    const NCFG: usize = 0;

    fn op(_cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let rows = ops::queue::list_queue(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                )
                .await?;
                Ok(rows_value::<_, QueueItemV>(rows))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ApproveOp;

impl RemoteOp for ApproveOp {
    const NAME: &'static str = "netidx_admin_approve";
    const NCFG: usize = 3;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let groups = string_list(cfg[0].as_ref())?;
        let no_id_map = opt_bool(cfg[1].as_ref(), false)?;
        let code = req_string(cfg[2].as_ref(), "the request code")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let out = ops::queue::approve(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    &code,
                    &groups,
                    no_id_map,
                )
                .await?;
                Ok(ApproveOutcomeV::from(out).into())
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ApproveRenewalsOp;

impl RemoteOp for ApproveRenewalsOp {
    const NAME: &'static str = "netidx_admin_approve_renewals";
    const NCFG: usize = 0;

    fn op(_cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let rows = ops::queue::approve_renewals(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                )
                .await?;
                Ok(rows_value::<_, RenewalResultV>(rows))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct DenyOp;

impl RemoteOp for DenyOp {
    const NAME: &'static str = "netidx_admin_deny";
    const NCFG: usize = 2;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let reason = opt_string(cfg[0].as_ref())?;
        let code = req_string(cfg[1].as_ref(), "the request code")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let denied = ops::queue::deny(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    &code,
                    reason.as_deref(),
                )
                .await?;
                Ok(Value::String(ArcStr::from(denied.as_str())))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct IssuedOp;

impl RemoteOp for IssuedOp {
    const NAME: &'static str = "netidx_admin_issued";
    const NCFG: usize = 2;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let include_revoked = opt_bool(cfg[0].as_ref(), false)?;
        let name_filter = opt_string(cfg[1].as_ref())?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let rows = ops::revoke::issued(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    include_revoked,
                    name_filter.as_deref(),
                )
                .await?;
                Ok(rows_value::<_, IssuedEntryV>(rows))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct RevokeOp;

impl RemoteOp for RevokeOp {
    const NAME: &'static str = "netidx_admin_revoke";
    const NCFG: usize = 3;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let glyph = match opt_string(cfg[0].as_ref())? {
            Some(g) => Some(Fingerprint::parse_text(&g)?),
            None => None,
        };
        let reason = opt_string(cfg[1].as_ref())?;
        let selector =
            ops::revoke::parse_selector(&req_string(cfg[2].as_ref(), "the selector")?);
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let out = ops::revoke::revoke(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    selector,
                    glyph,
                    reason.as_deref(),
                )
                .await?;
                Ok(RevokeOutcomeV::from(out).into())
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ListPendingDelegationsOp;

impl RemoteOp for ListPendingDelegationsOp {
    const NAME: &'static str = "netidx_admin_list_pending_delegations";
    const NCFG: usize = 0;

    fn op(_cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let rows = ops::delegation::list_pending_delegations(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                )
                .await?;
                Ok(rows_value::<_, PendingDelegationV>(rows))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ApproveDelegationOp;

impl RemoteOp for ApproveDelegationOp {
    const NAME: &'static str = "netidx_admin_approve_delegation";
    const NCFG: usize = 1;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let code = req_string(cfg[0].as_ref(), "the request code")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let out = ops::delegation::approve_delegation(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    &code,
                )
                .await?;
                Ok(DelegationDecisionV::from(out).into())
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct DenyDelegationOp;

impl RemoteOp for DenyDelegationOp {
    const NAME: &'static str = "netidx_admin_deny_delegation";
    const NCFG: usize = 2;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let reason = opt_string(cfg[0].as_ref())?;
        let code = req_string(cfg[1].as_ref(), "the request code")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let out = ops::delegation::deny_delegation(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    &code,
                    reason.as_deref(),
                )
                .await?;
                Ok(PendingDelegationV::from(out).into())
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ListServersOp;

impl RemoteOp for ListServersOp {
    const NAME: &'static str = "netidx_admin_list_servers";
    const NCFG: usize = 0;

    fn op(_cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let rows =
                    ops::servers::list_servers(ans, Some(conn.server), None).await?;
                Ok(rows_value::<_, ServerInfoV>(rows))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct RemoveServerOp;

impl RemoteOp for RemoveServerOp {
    const NAME: &'static str = "netidx_admin_remove_server";
    const NCFG: usize = 1;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let id = server_id(cfg[0].as_ref())?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let out = ops::servers::remove_server(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    id,
                )
                .await?;
                Ok(RemoveServerOutcomeV::from(out).into())
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ListServiceServersOp;

impl RemoteOp for ListServiceServersOp {
    const NAME: &'static str = "netidx_admin_list_service_servers";
    const NCFG: usize = 0;

    fn op(_cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let rows =
                    ops::service::list_service_servers(ans, conn.server, None).await?;
                Ok(rows_value::<_, ServiceServerV>(rows))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ServiceControlOp;

impl RemoteOp for ServiceControlOp {
    const NAME: &'static str = "netidx_admin_service_control";
    const NCFG: usize = 3;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let units = string_list(cfg[0].as_ref())?;
        let op = ServiceOpV::from_value(cfg[1].clone().unwrap_or(Value::Null))?;
        let server = server_id(cfg[2].as_ref())?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let units = ops::service::control_remote(
                    ans,
                    conn.server,
                    None,
                    Some(conn.admin),
                    None,
                    server,
                    units,
                    op.into(),
                )
                .await?;
                Ok(rows_value::<_, ServiceUnitV>(units))
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct SetReadGateOp;

impl RemoteOp for SetReadGateOp {
    const NAME: &'static str = "netidx_admin_set_read_gate";
    const NCFG: usize = 2;

    fn op(cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        let server = server_id(cfg[0].as_ref())?;
        let gate = GateRequestV::from_value(cfg[1].clone().unwrap_or(Value::Null))?;
        let gate = ReadGate::try_from(gate)?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                ops::service::set_read_gate(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                    server,
                    gate,
                )
                .await?;
                Ok(Value::Null)
            })
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ReconcileCaOp;

impl RemoteOp for ReconcileCaOp {
    const NAME: &'static str = "netidx_admin_reconcile_ca";
    const NCFG: usize = 0;

    fn op(_cfg: &[Option<Value>], conn: ConnInfo) -> Result<BoxOp> {
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let (op_id, peers) = ops::servers::reconcile_ca(
                    ans,
                    Some(conn.server),
                    None,
                    Some(conn.admin),
                    None,
                )
                .await?;
                Ok(ReconcileOutcomeV {
                    operation: op_id.to_string(),
                    peers: peers.into_iter().map(PeerResultV::from).collect(),
                }
                .into())
            })
        }))
    }
}

pub(crate) type ListServiceServers = RemoteCeremony<ListServiceServersOp>;
pub(crate) type ServiceControl = RemoteCeremony<ServiceControlOp>;
pub(crate) type SetReadGate = RemoteCeremony<SetReadGateOp>;
pub(crate) type ListQueue = RemoteCeremony<ListQueueOp>;
pub(crate) type ApproveReq = RemoteCeremony<ApproveOp>;
pub(crate) type ApproveRenewals = RemoteCeremony<ApproveRenewalsOp>;
pub(crate) type Deny = RemoteCeremony<DenyOp>;
pub(crate) type Issued = RemoteCeremony<IssuedOp>;
pub(crate) type Revoke = RemoteCeremony<RevokeOp>;
pub(crate) type ListPendingDelegations = RemoteCeremony<ListPendingDelegationsOp>;
pub(crate) type ApproveDelegation = RemoteCeremony<ApproveDelegationOp>;
pub(crate) type DenyDelegation = RemoteCeremony<DenyDelegationOp>;
pub(crate) type ListServers = RemoteCeremony<ListServersOp>;
pub(crate) type RemoveServer = RemoteCeremony<RemoveServerOp>;
pub(crate) type ReconcileCa = RemoteCeremony<ReconcileCaOp>;

// ── change_password (target-based ceremony) ──────────────────────

/// Unlike the queue family this op takes the `AdminTarget` itself (it
/// works over the local socket too), so it gets its own builtin rather
/// than a [`RemoteOp`].
#[derive(Debug)]
pub(crate) struct ChangePassword {
    trigger: Trigger,
    out: TagValue,
}

impl<R: Rt, E: UserEvent> BuiltIn<R, E> for ChangePassword {
    const EFFECT: EffectKind = EffectKind::Async;
    const NAME: &str = "netidx_admin_change_password";

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        from: &'c [Node<R, E>],
        _top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(ChangePassword {
            trigger: Trigger::new(from),
            out: TagValue::phantom(),
        }))
    }
}

impl<R: Rt, E: UserEvent> Apply<R, E> for ChangePassword {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> &TagValue {
        let Some(args) = self.trigger.tick(ctx, from, event) else {
            return self.out.ride();
        };
        let Some(target) = target_arg(args[1].as_ref()) else {
            return self.out.ride();
        };
        let new_password = match opt_string(args[0].as_ref()) {
            Ok(p) => p.map(Secret),
            Err(e) => return self.out.set(TagValue::fired(errf!("Admin", "{e:#}"))),
        };
        let glyph = target.remote().ok().map(|(_, fp)| fp);
        let op: BoxOp = Box::new(move |ans| {
            Box::pin(async move {
                ops::roster::change_password(ans, &target.0, new_password).await?;
                Ok(Value::Null)
            })
        });
        self.out.set(TagValue::fired(ceremony::start_ceremony(ctx, glyph, op)))
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {}

    fn reset_replay(&mut self, _ctx: &mut ExecCtx<R, E>) {}
}

// ── plain target-based builtins ──────────────────────────────────

macro_rules! target_op {
    ($ty:ident, $name:literal, $args:ty, $prepare:expr, $run:expr) => {
        #[derive(Debug, Default)]
        pub(crate) struct $ty;

        impl EvalCachedAsync for $ty {
            type Args = (TargetValue, $args);

            const NAME: &str = $name;

            fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
                let t = get_target(cached, cached.0.len() - 1)?;
                let extra = ($prepare)(cached)?;
                Some((t, extra))
            }

            fn eval((t, extra): Self::Args) -> impl Future<Output = Value> + Send {
                async move {
                    match ($run)(t, extra).await {
                        Ok(v) => v,
                        Err(e) => admin_err(e),
                    }
                }
            }
        }
    };
}

target_op!(
    ListResolverClustersEv,
    "netidx_admin_list_resolver_clusters",
    (),
    |_c: &CachedVals| Some(()),
    |t: TargetValue, _: ()| async move {
        let rows = ops::perms::list_resolver_clusters(&t.0).await?;
        Ok::<_, anyhow::Error>(Value::from(rows))
    }
);

target_op!(
    ShowPermsEv,
    "netidx_admin_show_perms",
    String,
    |c: &CachedVals| c.get::<String>(0),
    |t: TargetValue, at: String| async move {
        let p = ops::perms::show_perms(&t.0, &at).await?;
        Ok::<_, anyhow::Error>(perm_entries(&p))
    }
);

target_op!(
    SetPermEv,
    "netidx_admin_set_perm",
    (String, String, String, String),
    |c: &CachedVals| Some((
        c.get::<String>(0)?,
        c.get::<String>(1)?,
        c.get::<String>(2)?,
        c.get::<String>(3)?
    )),
    |t: TargetValue, (at, path, entity, bits): (String, String, String, String)| async move {
        let e = ops::perms::set_entry(&t.0, &at, &path, &entity, &bits).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    RemovePermEv,
    "netidx_admin_remove_perm",
    (String, String, String),
    |c: &CachedVals| Some((
        c.get::<String>(0)?,
        c.get::<String>(1)?,
        c.get::<String>(2)?
    )),
    |t: TargetValue, (at, path, entity): (String, String, String)| async move {
        let e = ops::perms::remove_entry(&t.0, &at, &path, &entity).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    ShowIdMapEv,
    "netidx_admin_show_id_map",
    (),
    |_c: &CachedVals| Some(()),
    |t: TargetValue, _: ()| async move {
        let m = ops::id_map::show_id_map(&t.0).await?;
        Ok::<_, anyhow::Error>(IdMapV::from(m).into())
    }
);

target_op!(
    IdMapAddGroupEv,
    "netidx_admin_id_map_add_group",
    String,
    |c: &CachedVals| c.get::<String>(0),
    |t: TargetValue, name: String| async move {
        let e = ops::id_map::add_group(&t.0, &name).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    IdMapRemoveGroupEv,
    "netidx_admin_id_map_remove_group",
    String,
    |c: &CachedVals| c.get::<String>(0),
    |t: TargetValue, name: String| async move {
        let e = ops::id_map::remove_group(&t.0, &name).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    IdMapAddUserEv,
    "netidx_admin_id_map_add_user",
    (Vec<String>, String, String),
    |c: &CachedVals| {
        let groups = string_list(c.0.first()?.as_ref()).ok()?;
        Some((groups, c.get::<String>(1)?, c.get::<String>(2)?))
    },
    |t: TargetValue, (groups, san, primary): (Vec<String>, String, String)| async move {
        let e = ops::id_map::add_user(&t.0, &san, &primary, &groups).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    IdMapRemoveUserEv,
    "netidx_admin_id_map_remove_user",
    String,
    |c: &CachedVals| c.get::<String>(0),
    |t: TargetValue, san: String| async move {
        let e = ops::id_map::remove_user(&t.0, &san).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    IdMapAddMemberEv,
    "netidx_admin_id_map_add_member",
    (String, String),
    |c: &CachedVals| Some((c.get::<String>(0)?, c.get::<String>(1)?)),
    |t: TargetValue, (san, group): (String, String)| async move {
        let e = ops::id_map::add_member(&t.0, &san, &group).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    IdMapRemoveMemberEv,
    "netidx_admin_id_map_remove_member",
    (String, String),
    |c: &CachedVals| Some((c.get::<String>(0)?, c.get::<String>(1)?)),
    |t: TargetValue, (san, group): (String, String)| async move {
        let e = ops::id_map::remove_member(&t.0, &san, &group).await?;
        Ok::<_, anyhow::Error>(RecordedEditV::from(e).into())
    }
);

target_op!(
    AddRoleAdminEv,
    "netidx_admin_add_role_admin",
    (String, PolicyV),
    |c: &CachedVals| Some((c.get::<String>(0)?, c.get::<PolicyV>(1)?)),
    |t: TargetValue, (name, policy): (String, PolicyV)| async move {
        let policy = Policy::try_from(policy)?;
        let pw = ops::roster::add_role_admin(&t.0, &name, policy).await?;
        Ok::<_, anyhow::Error>(Value::String(ArcStr::from(pw.as_str())))
    }
);

target_op!(
    ResetPasswordEv,
    "netidx_admin_reset_password",
    String,
    |c: &CachedVals| c.get::<String>(0),
    |t: TargetValue, name: String| async move {
        let pw = ops::roster::reset_password(&t.0, &name).await?;
        Ok::<_, anyhow::Error>(Value::String(ArcStr::from(pw.as_str())))
    }
);

target_op!(
    SetAdminPolicyEv,
    "netidx_admin_set_admin_policy",
    (String, PolicyV),
    |c: &CachedVals| Some((c.get::<String>(0)?, c.get::<PolicyV>(1)?)),
    |t: TargetValue, (name, policy): (String, PolicyV)| async move {
        let policy = Policy::try_from(policy)?;
        ops::roster::set_admin_policy(&t.0, &name, policy).await?;
        Ok::<_, anyhow::Error>(Value::Null)
    }
);

target_op!(
    RemoveAdminEv,
    "netidx_admin_remove_admin",
    String,
    |c: &CachedVals| c.get::<String>(0),
    |t: TargetValue, name: String| async move {
        ops::roster::remove_admin(&t.0, &name).await?;
        Ok::<_, anyhow::Error>(Value::Null)
    }
);

pub(crate) type ListResolverClusters = CachedArgsAsync<ListResolverClustersEv>;
pub(crate) type ShowPerms = CachedArgsAsync<ShowPermsEv>;
pub(crate) type SetPerm = CachedArgsAsync<SetPermEv>;
pub(crate) type RemovePerm = CachedArgsAsync<RemovePermEv>;
pub(crate) type ShowIdMap = CachedArgsAsync<ShowIdMapEv>;
pub(crate) type IdMapAddGroup = CachedArgsAsync<IdMapAddGroupEv>;
pub(crate) type IdMapRemoveGroup = CachedArgsAsync<IdMapRemoveGroupEv>;
pub(crate) type IdMapAddUser = CachedArgsAsync<IdMapAddUserEv>;
pub(crate) type IdMapRemoveUser = CachedArgsAsync<IdMapRemoveUserEv>;
pub(crate) type IdMapAddMember = CachedArgsAsync<IdMapAddMemberEv>;
pub(crate) type IdMapRemoveMember = CachedArgsAsync<IdMapRemoveMemberEv>;
pub(crate) type AddRoleAdmin = CachedArgsAsync<AddRoleAdminEv>;
pub(crate) type ResetPassword = CachedArgsAsync<ResetPasswordEv>;
pub(crate) type SetAdminPolicy = CachedArgsAsync<SetAdminPolicyEv>;
pub(crate) type RemoveAdmin = CachedArgsAsync<RemoveAdminEv>;

// ── discover ─────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub(crate) struct DiscoverEv;

impl EvalCachedAsync for DiscoverEv {
    type Args = Duration;

    const NAME: &str = "netidx_admin_discover";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let timeout = match cached.0.first()?.as_ref()? {
            Value::Null => Duration::from_secs(2),
            Value::Duration(d) => **d,
            _ => return None,
        };
        cached.0.get(1)?.as_ref()?;
        Some(timeout)
    }

    fn eval(timeout: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match discovery::browse(timeout).await {
                Ok(rows) => rows_value::<_, DiscoveredV>(rows),
                Err(e) => admin_err(e),
            }
        }
    }
}

pub(crate) type Discover = CachedArgsAsync<DiscoverEv>;
