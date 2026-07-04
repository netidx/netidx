//! Tab 2 — **Remote admin**: connect to a (local or remote) admin server and
//! drive its enrollment queue (and, in later slices, delegations, roster,
//! perms, service control, revocation) over `netidx_admin::admin_ops`.
//!
//! The connection is established once (glyph confirm → admin name → password)
//! and cached in [`RemoteConn`]; every subsequent op reuses it — the cached
//! fingerprint is fed to a [`TuiAnswerer::with_glyph`](super::answer::TuiAnswerer)
//! so `confirm_identity` auto-accepts (still re-pinning per op), and the cached
//! `admin`/`password` are passed as `Some(..)` so those prompts short-circuit.
//!
//! `admin_ops` is unix-only, so the op *bodies* ([`run`]) are `#[cfg(unix)]`;
//! the state, action, and result types hold only cross-platform values (the ops
//! render `admin_ops` rows into plain [`PanelRow`]s), so the UI compiles
//! everywhere and simply reports "unavailable" off unix.

use super::{action::Action, answer::TuiAnswerer, widgets};
use anyhow::Result;
#[cfg(unix)]
use anyhow::Context;
use crossterm::event::KeyCode;
use netidx_admin::{admin_proto::Secret, fingerprint::Fingerprint};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use std::net::SocketAddr;

/// A pinned, authenticated remote-admin session, captured once at connect and
/// reused by every panel op. All fields are cross-platform.
#[derive(Clone)]
pub(super) struct RemoteConn {
    pub(super) server: SocketAddr,
    pub(super) domain: String,
    pub(super) confirmed_fp: Fingerprint,
    pub(super) admin: String,
    pub(super) password: Secret,
}

/// Which panel a set of rows belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Panel {
    Queue,
    Delegations,
    Roster,
    Revocation,
    Perms,
    Service,
}

impl Panel {
    fn title(self) -> &'static str {
        match self {
            Panel::Queue => "Enrollment queue",
            Panel::Delegations => "Delegation requests",
            Panel::Roster => "Admin roster",
            Panel::Revocation => "Issued certificates",
            Panel::Perms => "Permissions",
            Panel::Service => "Services",
        }
    }

    /// Whether this panel first needs a target netidx path (the map routes it to
    /// the cluster owning that path). Such panels enter through a path prompt.
    fn path_scoped(self) -> bool {
        matches!(self, Panel::Perms | Panel::Service)
    }
}

/// A rendered panel row: display text plus the typed key an action on it needs.
/// Cross-platform — the op formats `admin_ops` rows into these so unix-only
/// types never reach the UI state.
#[derive(Clone)]
pub(super) struct PanelRow {
    text: String,
    key: RowKey,
}

/// The action key a panel row carries. Each panel keys its rows differently: an
/// enrollment or delegation by its out-of-band **code**; an admin by **name**; a
/// certificate by **serial** (+ its per-key glyph for the revoke gate). A row
/// with no actionable key (a renewal, an unparseable CSR, a status line) is
/// [`RowKey::None`].
#[derive(Clone)]
pub(super) enum RowKey {
    None,
    /// A full fingerprint code (queue enrollment, delegation request).
    Code(String),
    /// An admin name (roster). Reserved signing slots render as [`RowKey::None`]
    /// so the roster actions never target them.
    Name(String),
    /// A certificate: its serial plus the per-key glyph shown for it (`None`
    /// when the stored glyph is empty/unparseable — a legacy directly-issued
    /// cert, revocable by its unique serial without a glyph assertion).
    Cert { serial: u64, glyph: Option<Fingerprint> },
    /// One activation unit on one cluster member (service control).
    Unit { unit: String, member: u32 },
}

/// A service-control verb chosen in the services panel. A cross-platform mirror
/// of the unix-only `ControlOp` (mapped to it in the op body), so the action
/// type stays cross-platform.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ServiceOp {
    Start,
    Stop,
    Restart,
}

/// A remote-admin op the event loop runs (via [`Action::Remote`]).
pub(super) enum RemoteAction {
    /// Establish the session to `server` (glyph confirm + login).
    Connect { server: SocketAddr },
    /// (Re)list a panel. `path` is the target path for a path-scoped panel
    /// (perms), `None` for the rest.
    Refresh { conn: RemoteConn, panel: Panel, path: Option<String> },
    /// Approve one queued enrollment by its full code.
    Approve { conn: RemoteConn, code: String },
    /// Approve every verified renewal (code-free batch).
    ApproveRenewals { conn: RemoteConn },
    /// Deny one queued enrollment by its full code (reason prompted).
    Deny { conn: RemoteConn, code: String },
    /// Approve one pending delegation by its full code (cluster-wide).
    ApproveDelegation { conn: RemoteConn, code: String },
    /// Deny one pending delegation by its full code (reason prompted).
    DenyDelegation { conn: RemoteConn, code: String },
    /// Revoke one issued certificate by serial (glyph-gated, reason prompted).
    /// Irreversible — gated by a yes/no confirm before it runs.
    Revoke { conn: RemoteConn, serial: u64, glyph: Option<Fingerprint> },
    /// Mint a new role admin (name + initial password + policy via `$EDITOR`).
    AddAdmin { conn: RemoteConn },
    /// Replace an admin's policy (edited as JSON in `$EDITOR`).
    SetPolicy { conn: RemoteConn, name: String },
    /// Remove a role admin. Gated by a yes/no confirm before it runs.
    RemoveAdmin { conn: RemoteConn, name: String },
    /// Edit the permissions of the cluster mounted at `at` (via `$EDITOR`).
    EditPerms { conn: RemoteConn, at: String },
    /// Start/stop/restart one activation unit on one member of the cluster
    /// serving `path`. Stop is gated by a yes/no confirm (it leaves a service
    /// down).
    ServiceControl {
        conn: RemoteConn,
        path: String,
        unit: String,
        member: u32,
        op: ServiceOp,
    },
}

impl RemoteAction {
    pub(super) fn label(&self) -> String {
        match self {
            RemoteAction::Connect { .. } => "Connecting".to_string(),
            RemoteAction::Refresh { .. } => "Loading".to_string(),
            RemoteAction::Approve { .. } => "Approving".to_string(),
            RemoteAction::ApproveRenewals { .. } => "Approving renewals".to_string(),
            RemoteAction::Deny { .. } => "Denying".to_string(),
            RemoteAction::ApproveDelegation { .. } => "Approving delegation".to_string(),
            RemoteAction::DenyDelegation { .. } => "Denying delegation".to_string(),
            RemoteAction::Revoke { .. } => "Revoking certificate".to_string(),
            RemoteAction::AddAdmin { .. } => "Adding an admin".to_string(),
            RemoteAction::SetPolicy { .. } => "Setting policy".to_string(),
            RemoteAction::RemoveAdmin { .. } => "Removing an admin".to_string(),
            RemoteAction::EditPerms { .. } => "Editing permissions".to_string(),
            RemoteAction::ServiceControl { op, .. } => match op {
                ServiceOp::Start => "Starting service".to_string(),
                ServiceOp::Stop => "Stopping service".to_string(),
                ServiceOp::Restart => "Restarting service".to_string(),
            },
        }
    }

    /// A yes/no confirmation to require before running, or `None`. The
    /// irreversible revoke, the destructive admin-removal, and a service stop
    /// (which leaves a unit down) are gated.
    pub(super) fn confirm_message(&self) -> Option<String> {
        match self {
            RemoteAction::Revoke { serial, .. } => Some(format!(
                "Revoke certificate serial {serial}? This is irreversible — the \
                 cluster re-signs its CRL and the holder can no longer authenticate."
            )),
            RemoteAction::RemoveAdmin { name, .. } => Some(format!(
                "Remove admin {name:?}? Their password will no longer authenticate \
                 to this CA."
            )),
            RemoteAction::ServiceControl { op: ServiceOp::Stop, unit, member, .. } => Some(
                format!("Stop unit {unit:?} on member {member}? It will stay down until started."),
            ),
            _ => None,
        }
    }

    /// The pre-confirmed CA fingerprint for a reused session (so the answerer
    /// auto-accepts the identity), or `None` for the first connect.
    pub(super) fn glyph(&self) -> Option<Fingerprint> {
        match self {
            RemoteAction::Connect { .. } => None,
            RemoteAction::Refresh { conn, .. }
            | RemoteAction::Approve { conn, .. }
            | RemoteAction::ApproveRenewals { conn }
            | RemoteAction::Deny { conn, .. }
            | RemoteAction::ApproveDelegation { conn, .. }
            | RemoteAction::DenyDelegation { conn, .. }
            | RemoteAction::Revoke { conn, .. }
            | RemoteAction::AddAdmin { conn }
            | RemoteAction::SetPolicy { conn, .. }
            | RemoteAction::RemoveAdmin { conn, .. }
            | RemoteAction::EditPerms { conn, .. }
            | RemoteAction::ServiceControl { conn, .. } => Some(conn.confirmed_fp),
        }
    }
}

/// A result a completed op applies to [`RemoteState`].
pub(super) enum RemoteUpdate {
    Connected(RemoteConn),
    Rows { panel: Panel, rows: Vec<PanelRow> },
}

// ---- op bodies (unix-only: they call admin_ops) ---------------------------

/// Run a remote-admin action to completion. The `admin_ops` calls are unix-only.
#[cfg(unix)]
pub(super) async fn run(ans: &mut TuiAnswerer, action: RemoteAction) -> Result<super::action::Outcome> {
    match action {
        RemoteAction::Connect { server } => connect(ans, server).await,
        RemoteAction::Refresh { conn, panel, path } => refresh(ans, conn, panel, path).await,
        RemoteAction::Approve { conn, code } => approve(ans, conn, code).await,
        RemoteAction::ApproveRenewals { conn } => approve_renewals(ans, conn).await,
        RemoteAction::Deny { conn, code } => deny(ans, conn, code).await,
        RemoteAction::ApproveDelegation { conn, code } => {
            approve_delegation(ans, conn, code).await
        }
        RemoteAction::DenyDelegation { conn, code } => deny_delegation(ans, conn, code).await,
        RemoteAction::Revoke { conn, serial, glyph } => {
            revoke(ans, conn, serial, glyph).await
        }
        RemoteAction::AddAdmin { conn } => add_admin(ans, conn).await,
        RemoteAction::SetPolicy { conn, name } => set_policy(ans, conn, name).await,
        RemoteAction::RemoveAdmin { conn, name } => remove_admin(ans, conn, name).await,
        RemoteAction::EditPerms { conn, at } => edit_perms(ans, conn, at).await,
        RemoteAction::ServiceControl { conn, path, unit, member, op } => {
            service_control(ans, conn, path, unit, member, op).await
        }
    }
}

#[cfg(not(unix))]
pub(super) async fn run(
    _ans: &mut TuiAnswerer,
    _action: RemoteAction,
) -> Result<super::action::Outcome> {
    anyhow::bail!("remote administration is only available on unix hosts")
}

#[cfg(unix)]
async fn connect(ans: &mut TuiAnswerer, server: SocketAddr) -> Result<super::action::Outcome> {
    use netidx_admin::{
        admin_client::fetch_identity, admin_proto::NodeKind, answer::Answerer,
        plan::enroll::current_username,
    };
    let id = fetch_identity(server, NodeKind::Client).await?;
    if !ans.confirm_identity(&id).await? {
        anyhow::bail!("connection cancelled — identity not confirmed");
    }
    let default_user = current_username();
    let admin = ans
        .text(netidx_admin::answer::Field::AdminName, None, default_user.as_deref(), true)
        .await?
        .unwrap_or_default();
    let password = ans.secret(netidx_admin::answer::Field::AdminPassword, None).await?;
    let conn = RemoteConn {
        server,
        domain: id.domain.clone(),
        confirmed_fp: id.fingerprint,
        admin,
        password,
    };
    Ok(super::action::Outcome::remote_toast(
        "Connected",
        vec![format!("Connected to {} at {} as {}.", id.domain, server, conn.admin)],
        RemoteUpdate::Connected(conn),
    ))
}

#[cfg(unix)]
async fn refresh(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    panel: Panel,
    path: Option<String>,
) -> Result<super::action::Outcome> {
    let rows = match panel {
        Panel::Queue => queue_rows(ans, &conn).await?,
        Panel::Delegations => delegation_rows(ans, &conn).await?,
        Panel::Roster => roster_rows(ans, &conn).await?,
        Panel::Revocation => revocation_rows(ans, &conn).await?,
        Panel::Perms => {
            let at = path.context("a target path is required for the perms panel")?;
            perms_rows(ans, &conn, &at).await?
        }
        Panel::Service => {
            let p = path.context("a target path is required for the services panel")?;
            service_rows(ans, &conn, &p).await?
        }
    };
    Ok(super::action::Outcome::remote_rows(panel, rows))
}

#[cfg(unix)]
async fn queue_rows(ans: &mut TuiAnswerer, conn: &RemoteConn) -> Result<Vec<PanelRow>> {
    use netidx_admin::admin_ops::queue::list_queue;
    let items = list_queue(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
    )
    .await?;
    Ok(items.iter().map(queue_row).collect())
}

/// Format one queue item into a display row + its action code.
#[cfg(unix)]
fn queue_row(item: &netidx_admin::admin_ops::queue::QueueItem) -> PanelRow {
    let name = if let Some(listen) = item.enroll_listen {
        format!("admin-server enrollment @ {listen}")
    } else {
        item.requested_name.clone()
    };
    if item.verified_renewal {
        return PanelRow {
            text: format!("↻ {name}  (renewal, {}, from {})", widgets::fmt_age(item.age_secs), item.peer),
            key: RowKey::None,
        };
    }
    let code = item.code.as_ref().map(|c| c.text());
    let (tail, key) = match code {
        Some(c) => (
            format!("{:?}  ({}, from {})", item.kind, widgets::fmt_age(item.age_secs), item.peer),
            RowKey::Code(c),
        ),
        None => (format!("{:?}  (unparseable CSR — deny only)", item.kind), RowKey::None),
    };
    PanelRow { text: format!("{name}  {tail}"), key }
}

#[cfg(unix)]
async fn approve(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    code: String,
) -> Result<super::action::Outcome> {
    use netidx_admin::admin_ops::queue::approve;
    let out = approve(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        &code,
        &[],
        false,
    )
    .await?;
    let mut lines = vec![format!("Approved {}.", out.requested_name)];
    if !out.id_map_groups.is_empty() {
        lines.push(format!("id-map groups: {}", out.id_map_groups.join(", ")));
    }
    for w in &out.warnings {
        lines.push(format!("warning: {w}"));
    }
    let rows = queue_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after("Approved", lines, Panel::Queue, rows))
}

#[cfg(unix)]
async fn approve_renewals(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
) -> Result<super::action::Outcome> {
    use netidx_admin::admin_ops::queue::approve_renewals;
    let results = approve_renewals(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
    )
    .await?;
    let ok = results.iter().filter(|r| r.error.is_none()).count();
    let mut lines = vec![format!("Approved {ok} of {} renewal(s).", results.len())];
    for r in results.iter().filter(|r| r.error.is_some()) {
        lines.push(format!("  ! {} : {}", r.requested_name, r.error.as_deref().unwrap_or("")));
    }
    let rows = queue_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after("Renewals", lines, Panel::Queue, rows))
}

#[cfg(unix)]
async fn deny(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    code: String,
) -> Result<super::action::Outcome> {
    use netidx_admin::{admin_ops::queue::deny, answer::Answerer};
    let reason = ans
        .text(netidx_admin::answer::Field::RevokeReason, None, Some("denied"), true)
        .await?
        .unwrap_or_else(|| "denied".to_string());
    let name = deny(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        &code,
        &reason,
    )
    .await?;
    let rows = queue_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after(
        "Denied",
        vec![format!("Denied {name}.")],
        Panel::Queue,
        rows,
    ))
}

#[cfg(unix)]
async fn delegation_rows(ans: &mut TuiAnswerer, conn: &RemoteConn) -> Result<Vec<PanelRow>> {
    use netidx_admin::admin_ops::delegation::list_pending_delegations;
    let items = list_pending_delegations(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
    )
    .await?;
    Ok(items.iter().map(delegation_row).collect())
}

/// Format one pending delegation into a display row + its action code.
#[cfg(unix)]
fn delegation_row(
    item: &netidx_admin::admin_ops::delegation::PendingDelegation,
) -> PanelRow {
    let child = item
        .child
        .iter()
        .map(|a| a.addr.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    PanelRow {
        text: format!(
            "{}  ({}, from {}, child {child})",
            item.proposed_path,
            widgets::fmt_age(item.age_secs),
            item.peer,
        ),
        key: RowKey::Code(item.code.text()),
    }
}

#[cfg(unix)]
async fn approve_delegation(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    code: String,
) -> Result<super::action::Outcome> {
    use netidx_admin::admin_ops::delegation::approve_delegation;
    let out = approve_delegation(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        &code,
    )
    .await?;
    let mut lines = vec![format!("Delegated {} to the child cluster.", out.proposed_path)];
    let failed: Vec<_> = out.peers.iter().filter(|p| p.error.is_some()).collect();
    if failed.is_empty() {
        lines.push(format!("Propagated to {} cluster member(s).", out.peers.len()));
    } else {
        for p in &failed {
            lines.push(format!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("?")));
        }
    }
    let rows = delegation_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after("Approved", lines, Panel::Delegations, rows))
}

#[cfg(unix)]
async fn deny_delegation(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    code: String,
) -> Result<super::action::Outcome> {
    use netidx_admin::{admin_ops::delegation::deny_delegation, answer::Answerer};
    let reason = ans
        .text(netidx_admin::answer::Field::RevokeReason, None, Some("denied"), true)
        .await?
        .unwrap_or_else(|| "denied".to_string());
    let item = deny_delegation(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        &code,
        &reason,
    )
    .await?;
    let rows = delegation_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after(
        "Denied",
        vec![format!("Denied delegation of {}.", item.proposed_path)],
        Panel::Delegations,
        rows,
    ))
}

#[cfg(unix)]
async fn revocation_rows(ans: &mut TuiAnswerer, conn: &RemoteConn) -> Result<Vec<PanelRow>> {
    use netidx_admin::admin_ops::revoke::issued;
    let entries = issued(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        false, // live certificates only — the revoke target set
        None,
    )
    .await?;
    Ok(entries.iter().map(revocation_row).collect())
}

/// Format one issued certificate into a display row + its revoke key (serial +
/// its per-key glyph, `None` when the stored glyph is empty/unparseable).
#[cfg(unix)]
fn revocation_row(e: &netidx_admin::admin_proto::IssuedEntry) -> PanelRow {
    let glyph = Fingerprint::parse_text(&e.spki_fp).ok();
    let short = match &glyph {
        Some(g) => g.text().split(' ').take(2).collect::<Vec<_>>().join(" "),
        None => "(no glyph)".to_string(),
    };
    let name = if e.name.is_empty() { "(no DNS SAN)" } else { e.name.as_str() };
    PanelRow {
        text: format!(
            "#{:<6} {name}  exp {}  {short}",
            e.serial,
            widgets::fmt_expiry(e.not_after_unix),
        ),
        key: RowKey::Cert { serial: e.serial, glyph },
    }
}

#[cfg(unix)]
async fn revoke(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    serial: u64,
    glyph: Option<Fingerprint>,
) -> Result<super::action::Outcome> {
    use netidx_admin::{
        admin_ops::revoke::{RevokeSelector, revoke},
        answer::Answerer,
    };
    let reason = ans
        .text(netidx_admin::answer::Field::RevokeReason, None, Some("revoked"), true)
        .await?
        .unwrap_or_else(|| "revoked".to_string());
    let out = revoke(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        RevokeSelector::Serial(serial),
        glyph, // assert the glyph we displayed (skipped for a legacy empty glyph)
        &reason,
    )
    .await?;
    let mut lines: Vec<String> =
        out.revoked.iter().map(|e| format!("Revoked #{} {}.", e.serial, e.name)).collect();
    for w in &out.warnings {
        lines.push(format!("warning: {w}"));
    }
    let rows = revocation_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after("Revoked", lines, Panel::Revocation, rows))
}

#[cfg(unix)]
async fn admin_target(
    ans: &mut TuiAnswerer,
    conn: &RemoteConn,
) -> Result<netidx_admin::admin_ops::AdminTarget> {
    use netidx_admin::admin_ops::resolve_admin_target;
    resolve_admin_target(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
    )
    .await
}

#[cfg(unix)]
async fn roster_rows(ans: &mut TuiAnswerer, conn: &RemoteConn) -> Result<Vec<PanelRow>> {
    use netidx_admin::admin_ops::roster::list_admins;
    let target = admin_target(ans, conn).await?;
    Ok(list_admins(&target).await?.iter().map(roster_row).collect())
}

/// Format one roster entry. Reserved signing slots (recovery / autorenew) are
/// display-only ([`RowKey::None`]) — the roster actions must never target them.
#[cfg(unix)]
fn roster_row(a: &netidx_admin::ca_vault::AdminInfo) -> PanelRow {
    use netidx_admin::ca_vault::{SlotKind, is_reserved_admin};
    let tier = match a.kind {
        SlotKind::Signing => "signing",
        SlotKind::Role => "role",
    };
    let reserved = is_reserved_admin(&a.admin);
    let tag = if reserved { "  (system slot)" } else { "" };
    let key = if reserved { RowKey::None } else { RowKey::Name(a.admin.clone()) };
    PanelRow {
        text: format!("{:<18} [{tier}]{tag}  {}", a.admin, policy_summary(&a.policy)),
        key,
    }
}

/// A one-line summary of an admin's granted authorities for the roster row.
#[cfg(unix)]
fn policy_summary(p: &netidx_admin::ca_vault::Policy) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !p.allowed_san.is_empty() {
        parts.push(format!("san={}", p.allowed_san.join("|")));
    }
    if p.may_enroll_servers {
        parts.push("enroll-servers".to_string());
    }
    if p.may_manage_admins {
        parts.push("manage-admins".to_string());
    }
    if !p.perms_edit_scopes.is_empty() {
        parts.push(format!("perms={}", p.perms_edit_scopes.join("|")));
    }
    if !p.service_control_scopes.is_empty() {
        parts.push(format!("svc={}", p.service_control_scopes.join("|")));
    }
    if parts.is_empty() { "(no grants)".to_string() } else { parts.join(" ") }
}

/// The `$EDITOR` validator for a policy JSON blob: it must parse as a `Policy`;
/// returns the normalized (pretty) JSON to store.
#[cfg(unix)]
fn policy_validator() -> super::answer::EditValidator {
    Box::new(|s: &str| {
        let p: netidx_admin::ca_vault::Policy =
            serde_json::from_str(s).context("not valid policy JSON")?;
        serde_json::to_string_pretty(&p).context("serializing policy")
    })
}

/// A starter policy for a new role admin — every field present (all grants off)
/// so the editor shows exactly what can be granted.
#[cfg(unix)]
fn policy_template() -> netidx_admin::ca_vault::Policy {
    netidx_admin::ca_vault::Policy {
        allowed_san: vec![],
        max_validity: std::time::Duration::from_secs(730 * 86400),
        id_map_groups: vec![],
        may_enroll_servers: false,
        perms_edit_scopes: vec![],
        may_manage_admins: false,
        service_control_scopes: vec![],
    }
}

#[cfg(unix)]
async fn add_admin(ans: &mut TuiAnswerer, conn: RemoteConn) -> Result<super::action::Outcome> {
    use netidx_admin::{
        admin_ops::roster::add_role_admin,
        answer::{Answerer, Field},
    };
    let name = ans
        .text(Field::AdminName, None, None, true)
        .await?
        .context("an admin name is required")?;
    let password = ans.secret(Field::AdminPassword, None).await?;
    let seed = serde_json::to_string_pretty(&policy_template())?;
    let edited = ans.edit(seed, policy_validator()).await?;
    let policy: netidx_admin::ca_vault::Policy = serde_json::from_str(&edited)?;
    let target = admin_target(ans, &conn).await?;
    add_role_admin(&target, &name, &password, policy).await?;
    let rows = roster_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after(
        "Admin added",
        vec![format!("Added role admin {name:?}.")],
        Panel::Roster,
        rows,
    ))
}

#[cfg(unix)]
async fn set_policy(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    name: String,
) -> Result<super::action::Outcome> {
    use netidx_admin::admin_ops::roster::{list_admins, set_admin_policy};
    let target = admin_target(ans, &conn).await?;
    let current = list_admins(&target)
        .await?
        .into_iter()
        .find(|a| a.admin == name)
        .with_context(|| format!("admin {name:?} not found in the roster"))?;
    let seed = serde_json::to_string_pretty(&current.policy)?;
    let edited = ans.edit(seed, policy_validator()).await?;
    let policy: netidx_admin::ca_vault::Policy = serde_json::from_str(&edited)?;
    set_admin_policy(&target, &name, policy).await?;
    let rows = roster_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after(
        "Policy updated",
        vec![format!("Updated the policy of {name:?}.")],
        Panel::Roster,
        rows,
    ))
}

#[cfg(unix)]
async fn remove_admin(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    name: String,
) -> Result<super::action::Outcome> {
    use netidx_admin::admin_ops::roster::remove_admin;
    let target = admin_target(ans, &conn).await?;
    remove_admin(&target, &name).await?;
    let rows = roster_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after(
        "Admin removed",
        vec![format!("Removed admin {name:?}.")],
        Panel::Roster,
        rows,
    ))
}

#[cfg(unix)]
async fn perms_rows(ans: &mut TuiAnswerer, conn: &RemoteConn, at: &str) -> Result<Vec<PanelRow>> {
    use netidx_admin::admin_ops::perms::show_perms;
    let json = show_perms(ans, Some(conn.server), None, at).await?;
    let pretty = super::super::perms_admin::pretty(&json)?;
    let mut rows: Vec<PanelRow> =
        pretty.lines().map(|l| PanelRow { text: l.to_string(), key: RowKey::None }).collect();
    if rows.is_empty() {
        rows.push(PanelRow { text: "(no permissions set)".to_string(), key: RowKey::None });
    }
    Ok(rows)
}

#[cfg(unix)]
async fn edit_perms(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    at: String,
) -> Result<super::action::Outcome> {
    use netidx_admin::admin_ops::perms::{edit_perms, show_perms};
    // Seed the editor with the cluster's current perms, validate locally, then
    // hand the normalized result to the CA (which re-validates + propagates).
    let current = show_perms(ans, Some(conn.server), None, &at).await?;
    let seed = super::super::perms_admin::pretty(&current)?;
    let validate: super::answer::EditValidator =
        Box::new(|s: &str| super::super::perms_admin::validate(s));
    let edited = ans.edit(seed, validate).await?;
    let peers = edit_perms(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        &at,
        &edited,
    )
    .await?;
    let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
    let lines = if failed.is_empty() {
        vec![format!(
            "Updated perms at {at:?} on {} cluster member(s). Restart the resolver \
             server(s) to load them.",
            peers.len()
        )]
    } else {
        let mut v = vec![format!(
            "{} of {} member(s) could NOT be updated — the cluster is INCONSISTENT; \
             re-edit to converge:",
            failed.len(),
            peers.len()
        )];
        for p in &failed {
            v.push(format!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("?")));
        }
        v
    };
    let rows = perms_rows(ans, &conn, &at).await?;
    Ok(super::action::Outcome::remote_after("Perms updated", lines, Panel::Perms, rows))
}

#[cfg(unix)]
async fn service_rows(ans: &mut TuiAnswerer, conn: &RemoteConn, path: &str) -> Result<Vec<PanelRow>> {
    use netidx_activation::control::ControlOp;
    let results = service_status(ans, conn, path, Vec::new(), ControlOp::Status).await?;
    let mut rows: Vec<PanelRow> = Vec::new();
    for r in &results {
        if let Some(e) = &r.error {
            rows.push(PanelRow {
                text: format!("member {} ({}) — ERROR: {e}", r.member, r.addr),
                key: RowKey::None,
            });
            continue;
        }
        if r.units.is_empty() {
            rows.push(PanelRow {
                text: format!("member {} ({}) — no units", r.member, r.addr),
                key: RowKey::None,
            });
        }
        for u in &r.units {
            rows.push(PanelRow {
                text: format!("  m{} {:<18} {}", r.member, u.unit, fmt_unit_state(&u.state)),
                key: RowKey::Unit { unit: u.unit.clone(), member: r.member },
            });
        }
    }
    Ok(rows)
}

/// One-shot service-control RPC (shared by the status query and the control
/// actions), forwarding to `admin_ops::service::control_remote`.
#[cfg(unix)]
async fn service_status(
    ans: &mut TuiAnswerer,
    conn: &RemoteConn,
    path: &str,
    targets: Vec<netidx_admin::admin_proto::UnitTarget>,
    op: netidx_activation::control::ControlOp,
) -> Result<Vec<netidx_admin::admin_proto::ServiceControlResult>> {
    use netidx_admin::admin_ops::service::control_remote;
    control_remote(
        ans,
        conn.server,
        None,
        Some(conn.admin.clone()),
        Some(conn.password.clone()),
        path,
        targets,
        op,
    )
    .await
}

#[cfg(unix)]
fn fmt_unit_state(state: &netidx_activation::control::UnitState) -> String {
    use netidx_activation::control::UnitState;
    match state {
        UnitState::NotStarted => "not started".to_string(),
        UnitState::Running { pid: Some(pid) } => format!("running (pid {pid})"),
        UnitState::Running { pid: None } => "running".to_string(),
        UnitState::Stopped => "stopped".to_string(),
        UnitState::Died => "died".to_string(),
    }
}

#[cfg(unix)]
async fn service_control(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    path: String,
    unit: String,
    member: u32,
    op: ServiceOp,
) -> Result<super::action::Outcome> {
    use netidx_activation::control::ControlOp;
    let (control_op, verb) = match op {
        ServiceOp::Start => (ControlOp::Start, "Started"),
        ServiceOp::Stop => (ControlOp::Stop, "Stopped"),
        ServiceOp::Restart => (ControlOp::Restart, "Restarted"),
    };
    let targets =
        vec![netidx_admin::admin_proto::UnitTarget { unit: unit.clone(), member: Some(member) }];
    let results = service_status(ans, &conn, &path, targets, control_op).await?;
    let mut lines: Vec<String> = Vec::new();
    for r in &results {
        match &r.error {
            Some(e) => lines.push(format!("member {} ({}): {e}", r.member, r.addr)),
            None => lines.push(format!("member {} ({}): ok", r.member, r.addr)),
        }
    }
    if lines.is_empty() {
        lines.push(format!("{verb} {unit} on member {member}."));
    }
    let rows = service_rows(ans, &conn, &path).await?;
    Ok(super::action::Outcome::remote_after(verb, lines, Panel::Service, rows))
}

// ---- UI state (cross-platform) --------------------------------------------

/// Which Tab-2 screen is showing.
enum Screen {
    /// Enter an admin-server address to connect to.
    Connect,
    /// Pick a panel.
    Menu,
    /// Enter the target path for a path-scoped panel (perms) before opening it.
    PathPrompt { panel: Panel, input: String },
    /// A panel's rows.
    Panel(Panel),
}

/// Tab-2 state.
pub(super) struct RemoteState {
    conn: Option<RemoteConn>,
    screen: Screen,
    /// The connect-screen address field.
    addr: String,
    error: Option<String>,
    /// The panel-menu cursor.
    menu: ListState,
    /// The current panel's rows + cursor.
    rows: Vec<PanelRow>,
    list: ListState,
    /// The target path of the current path-scoped panel (perms), for its
    /// actions and title. `None` outside such a panel.
    panel_path: Option<String>,
}

/// The panels offered in the menu (label + which panel).
const PANELS: [Panel; 6] = [
    Panel::Queue,
    Panel::Delegations,
    Panel::Roster,
    Panel::Revocation,
    Panel::Perms,
    Panel::Service,
];

impl RemoteState {
    pub(super) fn new() -> RemoteState {
        let mut menu = ListState::default();
        menu.select(Some(0));
        RemoteState {
            conn: None,
            screen: Screen::Connect,
            addr: default_server(),
            error: None,
            menu,
            rows: Vec::new(),
            list: ListState::default(),
            panel_path: None,
        }
    }

    /// Apply a completed op's result.
    pub(super) fn apply(&mut self, update: RemoteUpdate) {
        match update {
            RemoteUpdate::Connected(conn) => {
                self.conn = Some(conn);
                self.screen = Screen::Menu;
                self.error = None;
            }
            RemoteUpdate::Rows { panel, rows } => {
                self.rows = rows;
                if self.list.selected().is_none() && !self.rows.is_empty() {
                    self.list.select(Some(0));
                }
                let sel = self.list.selected().unwrap_or(0);
                self.list.select(Some(sel.min(self.rows.len().saturating_sub(1))));
                self.screen = Screen::Panel(panel);
            }
        }
    }

    pub(super) fn on_key(&mut self, code: KeyCode) -> Option<Action> {
        if !cfg!(unix) {
            return None;
        }
        match &self.screen {
            Screen::Connect => self.on_key_connect(code),
            Screen::Menu => self.on_key_menu(code),
            Screen::PathPrompt { .. } => self.on_key_path_prompt(code),
            Screen::Panel(panel) => self.on_key_panel(code, *panel),
        }
    }

    fn on_key_connect(&mut self, code: KeyCode) -> Option<Action> {
        match code {
            KeyCode::Char(c) => {
                self.addr.push(c);
                self.error = None;
            }
            KeyCode::Backspace => {
                self.addr.pop();
            }
            KeyCode::Enter => {
                match netidx_admin::plan::resolve_admin_server_addr(self.addr.trim()) {
                    Ok(server) => {
                        return Some(Action::Remote(RemoteAction::Connect { server }));
                    }
                    Err(e) => self.error = Some(format!("{e:#}")),
                }
            }
            _ => {}
        }
        None
    }

    fn on_key_menu(&mut self, code: KeyCode) -> Option<Action> {
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.menu.select_previous(),
            KeyCode::Down | KeyCode::Char('j') => self.menu.select_next(),
            KeyCode::Esc => {
                // Back to the connect screen (disconnect).
                self.conn = None;
                self.screen = Screen::Connect;
            }
            KeyCode::Enter => {
                let panel = PANELS[self.menu.selected().unwrap_or(0).min(PANELS.len() - 1)];
                if panel.path_scoped() {
                    self.error = None;
                    self.screen = Screen::PathPrompt { panel, input: String::new() };
                } else if let Some(conn) = &self.conn {
                    self.panel_path = None;
                    self.list.select(None);
                    self.rows.clear();
                    return Some(Action::Remote(RemoteAction::Refresh {
                        conn: conn.clone(),
                        panel,
                        path: None,
                    }));
                }
            }
            _ => {}
        }
        None
    }

    /// The path-entry screen for a path-scoped panel (perms): collect the target
    /// path, then open the panel against it. Each arm scopes its `self.screen`
    /// borrow tightly so it can also touch the other fields.
    fn on_key_path_prompt(&mut self, code: KeyCode) -> Option<Action> {
        match code {
            KeyCode::Char(c) => {
                if let Screen::PathPrompt { input, .. } = &mut self.screen {
                    input.push(c);
                }
                None
            }
            KeyCode::Backspace => {
                if let Screen::PathPrompt { input, .. } = &mut self.screen {
                    input.pop();
                }
                None
            }
            KeyCode::Esc => {
                self.screen = Screen::Menu;
                None
            }
            KeyCode::Enter => {
                let (panel, path) = match &self.screen {
                    Screen::PathPrompt { panel, input } => (*panel, input.trim().to_string()),
                    _ => return None,
                };
                if path.is_empty() {
                    self.error = Some("a target path is required".to_string());
                    return None;
                }
                let conn = self.conn.clone()?;
                self.error = None;
                self.panel_path = Some(path.clone());
                self.list.select(None);
                self.rows.clear();
                Some(Action::Remote(RemoteAction::Refresh { conn, panel, path: Some(path) }))
            }
            _ => None,
        }
    }

    fn on_key_panel(&mut self, code: KeyCode, panel: Panel) -> Option<Action> {
        let conn = self.conn.clone()?;
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.list.select_previous(),
            KeyCode::Down | KeyCode::Char('j') => self.list.select_next(),
            KeyCode::Esc => {
                self.panel_path = None;
                self.screen = Screen::Menu;
            }
            KeyCode::Char('r') => {
                return Some(Action::Remote(RemoteAction::Refresh {
                    conn,
                    panel,
                    path: self.panel_path.clone(),
                }));
            }
            _ => match panel {
                Panel::Queue => return self.on_key_queue(code, conn),
                Panel::Delegations => return self.on_key_delegations(code, conn),
                Panel::Roster => return self.on_key_roster(code, conn),
                Panel::Revocation => return self.on_key_revocation(code, conn),
                Panel::Perms => return self.on_key_perms(code, conn),
                Panel::Service => return self.on_key_service(code, conn),
            },
        }
        None
    }

    fn on_key_perms(&mut self, code: KeyCode, conn: RemoteConn) -> Option<Action> {
        match code {
            KeyCode::Char('e') => self
                .panel_path
                .clone()
                .map(|at| Action::Remote(RemoteAction::EditPerms { conn, at })),
            _ => None,
        }
    }

    fn on_key_service(&mut self, code: KeyCode, conn: RemoteConn) -> Option<Action> {
        let op = match code {
            KeyCode::Char('s') => ServiceOp::Start,
            KeyCode::Char('t') => ServiceOp::Stop,
            KeyCode::Char('R') => ServiceOp::Restart,
            _ => return None,
        };
        let path = self.panel_path.clone()?;
        let (unit, member) = self.selected_unit()?;
        Some(Action::Remote(RemoteAction::ServiceControl { conn, path, unit, member, op }))
    }

    fn on_key_queue(&mut self, code: KeyCode, conn: RemoteConn) -> Option<Action> {
        match code {
            KeyCode::Char('R') => Some(Action::Remote(RemoteAction::ApproveRenewals { conn })),
            KeyCode::Char('a') => self.selected_code().map(|code| {
                Action::Remote(RemoteAction::Approve { conn, code })
            }),
            KeyCode::Char('d') => self
                .selected_code()
                .map(|code| Action::Remote(RemoteAction::Deny { conn, code })),
            _ => None,
        }
    }

    fn on_key_delegations(&mut self, code: KeyCode, conn: RemoteConn) -> Option<Action> {
        match code {
            KeyCode::Char('a') => self.selected_code().map(|code| {
                Action::Remote(RemoteAction::ApproveDelegation { conn, code })
            }),
            KeyCode::Char('d') => self.selected_code().map(|code| {
                Action::Remote(RemoteAction::DenyDelegation { conn, code })
            }),
            _ => None,
        }
    }

    fn on_key_revocation(&mut self, code: KeyCode, conn: RemoteConn) -> Option<Action> {
        match code {
            KeyCode::Char('x') => self.selected_cert().map(|(serial, glyph)| {
                Action::Remote(RemoteAction::Revoke { conn, serial, glyph })
            }),
            _ => None,
        }
    }

    fn on_key_roster(&mut self, code: KeyCode, conn: RemoteConn) -> Option<Action> {
        match code {
            KeyCode::Char('a') => Some(Action::Remote(RemoteAction::AddAdmin { conn })),
            KeyCode::Char('e') => self
                .selected_name()
                .map(|name| Action::Remote(RemoteAction::SetPolicy { conn, name })),
            KeyCode::Char('d') => self
                .selected_name()
                .map(|name| Action::Remote(RemoteAction::RemoveAdmin { conn, name })),
            _ => None,
        }
    }

    /// The full code of the selected row, if it carries one (not a renewal /
    /// unparseable / non-code row).
    fn selected_code(&self) -> Option<String> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Code(c) => Some(c.clone()),
            RowKey::None | RowKey::Name(_) | RowKey::Cert { .. } | RowKey::Unit { .. } => None,
        }
    }

    /// The selected certificate's serial + glyph, if the row is a cert row.
    fn selected_cert(&self) -> Option<(u64, Option<Fingerprint>)> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Cert { serial, glyph } => Some((*serial, *glyph)),
            RowKey::None | RowKey::Code(_) | RowKey::Name(_) | RowKey::Unit { .. } => None,
        }
    }

    /// The selected admin's name, if the row is an actionable roster row.
    fn selected_name(&self) -> Option<String> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Name(n) => Some(n.clone()),
            RowKey::None | RowKey::Code(_) | RowKey::Cert { .. } | RowKey::Unit { .. } => None,
        }
    }

    /// The selected unit + member, if the row is a service-unit row.
    fn selected_unit(&self) -> Option<(String, u32)> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Unit { unit, member } => Some((unit.clone(), *member)),
            RowKey::None | RowKey::Code(_) | RowKey::Name(_) | RowKey::Cert { .. } => None,
        }
    }

    pub(super) fn render(&mut self, f: &mut Frame, area: Rect) {
        if !cfg!(unix) {
            let msg = Paragraph::new(
                "Remote administration is only available on unix hosts (it drives the \
                 openssl-backed CA admin path).",
            )
            .wrap(Wrap { trim: true })
            .block(Block::default().borders(Borders::ALL).title(" Remote admin "));
            f.render_widget(msg, area);
            return;
        }
        match &self.screen {
            Screen::Connect => self.render_connect(f, area),
            Screen::Menu => self.render_menu(f, area),
            Screen::PathPrompt { panel, input } => self.render_path_prompt(f, area, *panel, input),
            Screen::Panel(panel) => self.render_panel(f, area, *panel),
        }
    }

    fn render_path_prompt(&self, f: &mut Frame, area: Rect, panel: Panel, input: &str) {
        let mut lines = vec![
            Line::from(format!("Enter the netidx path for the {} panel.", panel.title())),
            Line::from("It is routed to the resolver cluster mounted there (e.g. / or /eu).".dim()),
            Line::from(""),
            Line::from(vec![
                Span::raw("path: "),
                Span::styled(input.to_string(), Style::default().add_modifier(Modifier::BOLD)),
            ]),
        ];
        if let Some(e) = &self.error {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(e.clone(), Style::default().fg(Color::Red))));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(" Enter open · Esc back ".dim()));
        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(format!(" {} ", panel.title()))),
            area,
        );
    }

    fn render_connect(&self, f: &mut Frame, area: Rect) {
        let mut lines = vec![
            Line::from("Connect to an admin server to manage its network."),
            Line::from("Leave blank for this host's own admin server.".dim()),
            Line::from(""),
            Line::from(vec![
                Span::raw("server: "),
                Span::styled(
                    self.addr.clone(),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
            ]),
        ];
        if let Some(e) = &self.error {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                e.clone(),
                Style::default().fg(Color::Red),
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(" Enter connect ".dim()));
        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(" Connect ")),
            area,
        );
    }

    fn render_menu(&self, f: &mut Frame, area: Rect) {
        let title = match &self.conn {
            Some(c) => format!(" {} — {} ", c.domain, c.admin),
            None => " Remote admin ".to_string(),
        };
        let items: Vec<ListItem> = PANELS.iter().map(|p| ListItem::new(p.title())).collect();
        let mut st = self.menu.clone();
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_bottom(Line::from(" ↑/↓ · Enter open · Esc disconnect ").dim()),
            )
            .highlight_style(
                Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, area, &mut st);
    }

    fn render_panel(&self, f: &mut Frame, area: Rect, panel: Panel) {
        let items: Vec<ListItem> = if self.rows.is_empty() {
            vec![ListItem::new(Line::from("(empty)".dim()))]
        } else {
            self.rows.iter().map(|r| ListItem::new(r.text.clone())).collect()
        };
        let hint = match panel {
            Panel::Queue => " a approve · d deny · R renewals · r refresh · Esc back ",
            Panel::Delegations => " a approve · d deny · r refresh · Esc back ",
            Panel::Roster => " a add · e edit-policy · d remove · r refresh · Esc back ",
            Panel::Revocation => " x revoke · r refresh · Esc back ",
            Panel::Perms => " e edit · r reload · Esc back ",
            Panel::Service => " s start · t stop · R restart · r reload · Esc back ",
        };
        let title = match &self.panel_path {
            Some(p) => format!(" {} @ {p} ", panel.title()),
            None => format!(" {} ", panel.title()),
        };
        let mut st = self.list.clone();
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_bottom(Line::from(hint).dim()),
            )
            .highlight_style(
                Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, area, &mut st);
    }
}

/// The default connect address: this host's own admin server, if it runs one.
#[cfg(unix)]
fn default_server() -> String {
    netidx_admin::admin_ops::local_admin_server_listen()
        .map(|a| a.to_string())
        .unwrap_or_default()
}

#[cfg(not(unix))]
fn default_server() -> String {
    String::new()
}
