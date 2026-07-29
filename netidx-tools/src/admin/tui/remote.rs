//! Tab 2 — **Admin domain**: connect to a (local or remote) admin server and
//! drive its enrollment queue (and, in later slices, delegations, roster,
//! perms, service control, revocation) over `netidx_admin_client::ops`.
//!
//! The connection is established once (glyph confirm → CA verification
//! → login) and cached in [`RemoteConn`]; every subsequent op reuses it — the cached
//! fingerprint is fed to a [`TuiAnswerer::with_glyph`](super::answer::TuiAnswerer)
//! so `confirm_identity` auto-accepts (still re-pinning per op), while the
//! bearer session is read from the sealed or process-local session cache.
//!
//! `ops` is unix-only, so the op *bodies* ([`run`]) are `#[cfg(unix)]`;
//! the state, action, and result types hold only cross-platform values (the ops
//! render `ops` rows into plain [`PanelRow`]s), so the UI compiles
//! everywhere and simply reports "unavailable" off unix.

use super::{
    action::Action,
    admin_domains::{self, KnownAdminDomain, KnownAdminDomains, PollState},
    answer::TuiAnswerer,
    theme, widgets,
};
use anyhow::Result;
#[cfg(unix)]
use anyhow::{Context, bail};
use crossterm::event::KeyCode;
use netidx::resolver_server::config::ReadGate;
use netidx_admin_proto::AdminServerId;
use netidx_admin_proto::fingerprint::Fingerprint;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{List, ListItem, ListState, Paragraph, Wrap},
};
use std::{net::SocketAddr, path::PathBuf};

/// A pinned, authenticated remote-admin session, captured once at connect and
/// reused by every panel op. All fields are cross-platform.
#[derive(Clone)]
pub(super) struct RemoteConn {
    pub(super) server: SocketAddr,
    pub(super) domain: String,
    pub(super) confirmed_fp: Fingerprint,
    pub(super) admin: String,
}

/// Where an admin-panel op runs: this box's own admin server over its local
/// control socket (no auth, `SO_PEERCRED` superuser), or a pinned authenticated
/// remote session. The same panel surface serves the Local tab (a `Local`
/// target, opened directly) and the Admin domain tab (a `Remote` target, after the
/// connect form).
#[derive(Clone)]
pub(super) enum PanelTarget {
    /// This box's own admin server over its `SO_PEERCRED` control socket.
    Local {
        cfg_path: PathBuf,
    },
    Remote(RemoteConn),
}

impl PanelTarget {
    /// The pre-confirmed CA fingerprint for a remote session (so the answerer
    /// auto-accepts the identity), or `None` for a local target.
    fn glyph(&self) -> Option<Fingerprint> {
        match self {
            PanelTarget::Remote(c) => Some(c.confirmed_fp),
            PanelTarget::Local { .. } => None,
        }
    }

    /// Borrow the remote session, or error for a local target (a remote-only op).
    fn remote(&self) -> Result<&RemoteConn> {
        match self {
            PanelTarget::Remote(c) => Ok(c),
            PanelTarget::Local { .. } => {
                anyhow::bail!("this operation requires connecting to an admin domain")
            }
        }
    }

    /// Consume into the remote session, or error for a local target.
    fn into_remote(self) -> Result<RemoteConn> {
        match self {
            PanelTarget::Remote(c) => Ok(c),
            PanelTarget::Local { .. } => {
                anyhow::bail!("this operation requires connecting to an admin domain")
            }
        }
    }

    /// The panels valid for this target — a local target has no no-auth backend
    /// for the enrollment queue, delegations, revocation, or (yet) perms.
    fn panels(&self) -> &'static [Panel] {
        match self {
            PanelTarget::Local { .. } => &LOCAL_PANELS,
            PanelTarget::Remote(_) => &PANELS,
        }
    }
}

/// Which panel a set of rows belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Panel {
    Queue,
    Delegations,
    Roster,
    Servers,
    Revocation,
    Perms,
    Service,
}

impl Panel {
    fn title(self) -> &'static str {
        match self {
            Panel::Queue => "Enrollment Queue",
            Panel::Delegations => "Delegation Requests",
            Panel::Roster => "Admin Roster",
            Panel::Servers => "Admin Servers",
            Panel::Revocation => "Issued Certificates",
            Panel::Perms => "Permissions",
            Panel::Service => "Services",
        }
    }

    /// A one-line description shown in the menu's detail pane.
    fn desc(self) -> &'static str {
        match self {
            Panel::Queue => {
                "Review and approve certificate-enrollment requests from nodes joining the admin domain."
            }
            Panel::Delegations => {
                "Review and approve requests from resolvers asking to attach under this resolver cluster."
            }
            Panel::Roster => {
                "The admin domain's admins and their scopes — mint, scope, or remove admins."
            }
            Panel::Servers => {
                "Every CA-authoritative server identity, grouped by resolver cluster — permanently remove a dead node."
            }
            Panel::Revocation => {
                "Certificates this CA has issued — revoke one to bar the holder from the admin domain."
            }
            Panel::Perms => "View and edit the permissions on a netidx path.",
            Panel::Service => {
                "Start, stop, or restart the netidx services on an admin domain member."
            }
        }
    }

    /// Whether this panel first needs a target netidx path (the map routes it to
    /// the admin domain owning that path). Only admin domain Perms — a resolver cluster pick from the
    /// map. Services picks an admin server (also from the map), not a path.
    fn path_scoped(self) -> bool {
        matches!(self, Panel::Perms)
    }

    /// The panel's action keys, shown in the App gutter while the panel is open.
    fn keys(self) -> &'static str {
        match self {
            Panel::Queue => "a approve · d deny · R renewals · r refresh · Esc back",
            Panel::Delegations => "a approve · d deny · r refresh · Esc back",
            Panel::Roster => "a add · e edit-policy · d remove · r refresh · Esc back",
            Panel::Servers => {
                "g read gate · c reconcile CA · x force-remove · r refresh · Esc back"
            }
            Panel::Revocation => "x revoke · r refresh · Esc back",
            Panel::Perms => "e edit · r reload · Esc back",
            Panel::Service => "s start · t stop · R restart · r refresh · Esc back",
        }
    }
}

/// A rendered panel row: display text plus the typed key an action on it needs.
/// Cross-platform — the op formats `ops` rows into these so unix-only
/// types never reach the UI state.
#[derive(Clone)]
pub(super) struct PanelRow {
    text: String,
    key: RowKey,
    /// Pre-formatted `(label, value)` lines for a detail pane below the list —
    /// the roster's readable policy breakdown. Empty for panels with no
    /// key/value detail pane (glyph panels derive their detail from `key`).
    detail: Vec<(String, String)>,
}

impl PanelRow {
    /// A row with no key/value detail pane (the common case).
    fn plain(text: String, key: RowKey) -> PanelRow {
        PanelRow { text, key, detail: Vec::new() }
    }
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
    Cert {
        serial: u64,
        glyph: Option<Fingerprint>,
    },
    /// An immutable admin-server identity. The mutable address and admin domain are
    /// carried only to make the destructive confirmation unambiguous.
    Server {
        id: AdminServerId,
        addr: SocketAddr,
        cluster: String,
        ca: bool,
        /// Whether this identity holds a resolver grant at all — a host with
        /// no resolver has nothing to gate.
        resolver: bool,
        /// The gate this host last reported. Carried so the confirmation can
        /// say how much of a timed gate is being cut short.
        gate: Option<ReadGate>,
    },
}

/// A service-control verb chosen in the services panel. A cross-platform mirror
/// of the unix-only `ControlOp` (mapped to it in the op body), so the action
/// type stays cross-platform.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ServiceOp {
    Start,
    Stop,
    Restart,
    /// Read-only: list the server's units + state (the initial list + refresh).
    Status,
}

/// A remote-admin op the event loop runs (via [`Action::Remote`]).
pub(super) enum RemoteAction {
    /// Establish a session to `server`: fetch + confirm the CA glyph (always,
    /// before any credential), then prompt admin name + password. `expected_fp`
    /// is a saved admin domain's fingerprint, flagged if the live identity differs.
    Connect { server: SocketAddr, expected_fp: Option<Fingerprint> },
    /// Revoke the active bearer session when reachable and always clear its
    /// sealed/process-local cache.
    Logout { conn: RemoteConn },
    /// Browse mDNS for admin servers, verify each, and merge them into the saved
    /// admin domain registry.
    Discover,
    /// (Re)list a panel. `path` is the target path for a path-scoped panel
    /// (perms), `None` for the rest.
    Refresh { target: PanelTarget, panel: Panel, path: Option<String> },
    /// Approve one queued enrollment by its full code.
    Approve { target: PanelTarget, code: String },
    /// Approve every verified renewal (code-free batch).
    ApproveRenewals { target: PanelTarget },
    /// Deny one queued enrollment by its full code (reason prompted).
    Deny { target: PanelTarget, code: String },
    /// Approve one pending delegation by its full code (admin domain-wide).
    ApproveDelegation { target: PanelTarget, code: String },
    /// Deny one pending delegation by its full code (reason prompted).
    DenyDelegation { target: PanelTarget, code: String },
    /// Revoke one issued certificate by serial (glyph-gated, reason prompted).
    /// Irreversible — gated by a yes/no confirm before it runs.
    Revoke { target: PanelTarget, serial: u64, glyph: Option<Fingerprint> },
    /// Mint a new role admin (name + initial password + policy via `$EDITOR`).
    AddAdmin { target: PanelTarget },
    /// Replace an admin's policy (edited as JSON in `$EDITOR`).
    SetPolicy { target: PanelTarget, name: String },
    /// Remove a role admin. Gated by a yes/no confirm before it runs.
    RemoveAdmin { target: PanelTarget, name: String },
    /// Permanently revoke and remove a dead admin-server identity. The active
    /// CA is shown in the inventory but never yields this action.
    RemoveServer {
        target: PanelTarget,
        server: AdminServerId,
        addr: SocketAddr,
        cluster: String,
    },
    /// Open or shut one member's read gate. Confirm-gated in both directions:
    /// shutting takes it out of service for subscribers, and opening one that
    /// is still filling exposes a partial namespace.
    SetReadGate {
        target: PanelTarget,
        server: ServiceTarget,
        gate: ReadGate,
        /// What the member reported before this change, for the confirmation.
        current: Option<ReadGate>,
    },
    /// Re-send the CA's current address, map, and CRL to every
    /// registered node. Idempotent manual retry after recovery/relocation.
    ReconcileCa { target: PanelTarget },
    /// List the admin domain's resolver clusters (by base path) from the map, to
    /// pick one to view/edit — replaces free-text path entry for admin domain perms.
    ListResolverClusters { target: PanelTarget },
    /// Edit the permissions of the admin domain mounted at `at` (via `$EDITOR`).
    EditPerms { target: PanelTarget, at: String },
    /// List the admin domain's admin servers from the map, to pick one whose services
    /// to control — replaces free-text path entry for admin domain services.
    ListServiceServers { target: PanelTarget },
    /// Control services on ONE admin server (`server`): `Status` carries no
    /// units and (re)lists; `Start`/`Stop`/`Restart` carry the selected unit.
    /// Per-server by design — never an admin domain-wide fanout. Stop is confirm-gated.
    ServiceControl {
        target: PanelTarget,
        server: ServiceTarget,
        units: Vec<String>,
        op: ServiceOp,
    },
}

impl RemoteAction {
    pub(super) fn label(&self) -> String {
        match self {
            RemoteAction::Connect { .. } => "Connecting".to_string(),
            RemoteAction::Logout { .. } => "Logging out".to_string(),
            RemoteAction::Discover => "Discovering admin domains".to_string(),
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
            RemoteAction::RemoveServer { .. } => "Removing a server".to_string(),
            RemoteAction::SetReadGate { gate, .. } => match gate {
                ReadGate::No => "Opening the read gate".to_string(),
                ReadGate::Yes | ReadGate::Until(_) => {
                    "Shutting the read gate".to_string()
                }
            },
            RemoteAction::ReconcileCa { .. } => "Reconciling CA state".to_string(),
            RemoteAction::ListResolverClusters { .. } => "Loading bases".to_string(),
            RemoteAction::EditPerms { .. } => "Editing permissions".to_string(),
            RemoteAction::ListServiceServers { .. } => "Loading servers".to_string(),
            RemoteAction::ServiceControl { op, .. } => match op {
                ServiceOp::Start => "Starting service".to_string(),
                ServiceOp::Stop => "Stopping service".to_string(),
                ServiceOp::Restart => "Restarting service".to_string(),
                ServiceOp::Status => "Loading services".to_string(),
            },
        }
    }

    /// A yes/no confirmation to require before running, or `None`. The two
    /// approvals show the request's glyph + code (see [`Self::confirm_glyph`]) so
    /// the admin can match them against the screenshot the enrollee/child sent
    /// out of band (the TUI's stand-in for the strict CLI's type-the-full-code
    /// gesture); the irreversible revoke, the destructive admin-removal, and a
    /// service stop are gated for safety.
    pub(super) fn confirm_message(&self) -> Option<String> {
        match self {
            RemoteAction::Approve { code, .. } => Some(format!(
                "Approve this enrollment?\n\nVerify this glyph and code match the \
                 screenshot the enrollee sent you, out of band:\n\n{code}"
            )),
            RemoteAction::ApproveDelegation { code, .. } => Some(format!(
                "Approve or reconcile this delegation?\n\nVerify this glyph and code \
                 match the screenshot the child resolver's operator sent you, out \
                 of band:\n\n{code}"
            )),
            RemoteAction::Revoke { serial, .. } => Some(format!(
                "Revoke certificate serial {serial}? This is irreversible — the \
                 admin domain re-signs its CRL and the holder can no longer authenticate."
            )),
            RemoteAction::RemoveAdmin { name, .. } => Some(format!(
                "Remove admin {name:?}? Their password will no longer authenticate \
                 to this CA."
            )),
            RemoteAction::RemoveServer { server, addr, cluster, .. } => Some(format!(
                "Force-remove dead server {server}?\n\nLast address: {addr}\nResolver cluster: {cluster}\n\nThis permanently revokes every serving certificate for that immutable identity and removes its enrollment grant. The active CA cannot be removed. No service will be restarted."
            )),
            RemoteAction::ServiceControl {
                op: ServiceOp::Stop, units, server, ..
            } => Some(format!(
                "Stop {} on {} at {}? It will stay down until started.",
                units
                    .first()
                    .map(|u| format!("unit {u:?}"))
                    .unwrap_or_else(|| "services".to_string()),
                server.id,
                server.addr,
            )),
            RemoteAction::SetReadGate { server, gate, current, .. } => {
                read_gate_confirm(server, *gate, *current)
            }
            // Everything else runs without a prompt. Adding a destructive
            // action means adding it above, not here.
            RemoteAction::Connect { .. }
            | RemoteAction::Logout { .. }
            | RemoteAction::Discover
            | RemoteAction::Refresh { .. }
            | RemoteAction::ApproveRenewals { .. }
            | RemoteAction::Deny { .. }
            | RemoteAction::DenyDelegation { .. }
            | RemoteAction::AddAdmin { .. }
            | RemoteAction::SetPolicy { .. }
            | RemoteAction::ReconcileCa { .. }
            | RemoteAction::ListResolverClusters { .. }
            | RemoteAction::EditPerms { .. }
            | RemoteAction::ListServiceServers { .. }
            | RemoteAction::ServiceControl { .. } => None,
        }
    }

    /// The request's own glyph to show on the approve confirmation — the enrollee
    /// or child's code parsed back into a fingerprint, so the admin verifies the
    /// identicon against the screenshot. `None` for actions with no request glyph.
    pub(super) fn confirm_glyph(&self) -> Option<Fingerprint> {
        match self {
            RemoteAction::Approve { code, .. }
            | RemoteAction::ApproveDelegation { code, .. } => {
                Fingerprint::parse_text(code).ok()
            }
            RemoteAction::Connect { .. }
            | RemoteAction::Logout { .. }
            | RemoteAction::Discover
            | RemoteAction::Refresh { .. }
            | RemoteAction::ApproveRenewals { .. }
            | RemoteAction::Deny { .. }
            | RemoteAction::DenyDelegation { .. }
            | RemoteAction::Revoke { .. }
            | RemoteAction::AddAdmin { .. }
            | RemoteAction::SetPolicy { .. }
            | RemoteAction::RemoveAdmin { .. }
            | RemoteAction::RemoveServer { .. }
            | RemoteAction::SetReadGate { .. }
            | RemoteAction::ReconcileCa { .. }
            | RemoteAction::ListResolverClusters { .. }
            | RemoteAction::EditPerms { .. }
            | RemoteAction::ListServiceServers { .. }
            | RemoteAction::ServiceControl { .. } => None,
        }
    }

    /// The pre-confirmed CA fingerprint for a reused session (so the answerer
    /// auto-accepts the identity), or `None` for the first connect.
    pub(super) fn glyph(&self) -> Option<Fingerprint> {
        match self {
            RemoteAction::Connect { .. } | RemoteAction::Discover => None,
            RemoteAction::Logout { conn } => Some(conn.confirmed_fp),
            RemoteAction::Refresh { target, .. }
            | RemoteAction::Approve { target, .. }
            | RemoteAction::ApproveRenewals { target }
            | RemoteAction::Deny { target, .. }
            | RemoteAction::ApproveDelegation { target, .. }
            | RemoteAction::DenyDelegation { target, .. }
            | RemoteAction::Revoke { target, .. }
            | RemoteAction::AddAdmin { target }
            | RemoteAction::SetPolicy { target, .. }
            | RemoteAction::RemoveAdmin { target, .. }
            | RemoteAction::RemoveServer { target, .. }
            | RemoteAction::SetReadGate { target, .. }
            | RemoteAction::ReconcileCa { target }
            | RemoteAction::ListResolverClusters { target }
            | RemoteAction::EditPerms { target, .. }
            | RemoteAction::ListServiceServers { target }
            | RemoteAction::ServiceControl { target, .. } => target.glyph(),
        }
    }
}

/// The gate states offered for a member, as label + what it does. Never "yes"
/// and "no": a gate that is "on" and reads that are "on" mean opposite things,
/// and an operator reading a one-word answer has no way to tell which was
/// meant. The CLI avoids it the same way, with `--open` / `--shut`.
const GATE_CHOICES: [(&str, &str); 3] = [
    ("Open", "Answer read clients."),
    ("Shut", "Stop answering read clients until someone opens the gate again."),
    ("Shut until", "Stop answering read clients for a while, then start."),
];

/// The confirmation for a gate change, or `None` when nothing is changing.
///
/// Both directions are worth stopping on, for opposite reasons. Shutting takes
/// a member out of service for subscribers. Opening one that is still filling
/// is the quieter mistake: it answers, but from a namespace that publishers
/// have not finished rebuilding, and a path that is merely missing is
/// indistinguishable from a path that does not exist.
fn read_gate_confirm(
    server: &ServiceTarget,
    gate: ReadGate,
    current: Option<ReadGate>,
) -> Option<String> {
    let (id, addr) = (server.id, server.addr);
    let left = |t: chrono::DateTime<chrono::Utc>| {
        humantime::format_duration(std::time::Duration::from_secs(
            (t - chrono::Utc::now()).num_seconds().max(0) as u64 / 60 * 60,
        ))
        .to_string()
    };
    match (gate, current) {
        (ReadGate::No, Some(ReadGate::Until(t))) if !ReadGate::Until(t).is_open() => {
            Some(format!(
                "Start {id} at {addr} answering read clients {} early?\n\nIt is \
                 waiting for publishers to find it. Anything that has not been \
                 republished yet will look absent to subscribers resolving through \
                 it.",
                left(t)
            ))
        }
        (ReadGate::No, Some(ReadGate::Yes)) => Some(format!(
            "Start {id} at {addr} answering read clients?\n\nIt will answer from \
             whatever it holds now. If it was taken out of service, that may be a \
             stale picture of the namespace."
        )),
        // Already open, or as good as: nothing to warn about.
        (ReadGate::No, _) => None,
        (ReadGate::Yes, _) => Some(format!(
            "Stop {id} at {addr} answering read clients?\n\nSubscribers stop \
             resolving through it until someone opens the gate again. Publishers \
             keep writing to it, so its records stay fresh — it just stops \
             answering."
        )),
        (ReadGate::Until(t), _) => Some(format!(
            "Stop {id} at {addr} answering read clients for {}?\n\nSubscribers stop \
             resolving through it until then. Publishers keep writing to it, so its \
             records stay fresh — it just stops answering.",
            left(t)
        )),
    }
}

/// The immutable identity selected for service control plus the address shown
/// to the operator at selection time. Only `id` is authoritative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ServiceTarget {
    pub(super) id: AdminServerId,
    pub(super) addr: SocketAddr,
}

/// One admin server offered in the service-control server picker.
#[derive(Clone)]
pub(super) struct ServiceServerRow {
    pub(super) target: ServiceTarget,
    pub(super) label: String,
}

/// A result a completed op applies to [`RemoteState`].
pub(super) enum RemoteUpdate {
    Connected(RemoteConn),
    LoggedOut,
    Rows {
        panel: Panel,
        rows: Vec<PanelRow>,
    },
    /// The saved admin domain registry after a discover pass — refreshes the list.
    AdminDomains(Vec<KnownAdminDomain>),
    /// The admin domain's permission bases — opens the resolver cluster picker for a panel.
    ResolverClusters {
        panel: Panel,
        bases: Vec<String>,
    },
    /// The admin domain's admin servers — opens the service-control server picker.
    ServiceServers {
        servers: Vec<ServiceServerRow>,
    },
    /// The selected server's units — the services panel rows (shared type with
    /// the Local Services surface, so the two views render identically).
    ServiceRows {
        rows: Vec<super::services::ServiceRow>,
    },
}

// ---- op bodies (unix-only: they call ops) ---------------------------

/// Run a remote-admin action to completion. The `ops` calls are unix-only.
#[cfg(unix)]
pub(super) async fn run(
    ans: &mut TuiAnswerer,
    action: RemoteAction,
) -> Result<super::action::Outcome> {
    match action {
        RemoteAction::Connect { server, expected_fp } => {
            connect(ans, server, expected_fp).await
        }
        RemoteAction::Logout { conn } => logout(conn).await,
        RemoteAction::Discover => discover(ans).await,
        RemoteAction::Refresh { target, panel, path } => {
            refresh(ans, target, panel, path).await
        }
        RemoteAction::ListResolverClusters { target } => {
            list_resolver_clusters(ans, target).await
        }
        RemoteAction::ListServiceServers { target } => {
            list_service_servers(ans, target).await
        }
        RemoteAction::Approve { target, code } => {
            approve(ans, target.into_remote()?, code).await
        }
        RemoteAction::ApproveRenewals { target } => {
            approve_renewals(ans, target.into_remote()?).await
        }
        RemoteAction::Deny { target, code } => {
            deny(ans, target.into_remote()?, code).await
        }
        RemoteAction::ApproveDelegation { target, code } => {
            approve_delegation(ans, target.into_remote()?, code).await
        }
        RemoteAction::DenyDelegation { target, code } => {
            deny_delegation(ans, target.into_remote()?, code).await
        }
        RemoteAction::Revoke { target, serial, glyph } => {
            revoke(ans, target.into_remote()?, serial, glyph).await
        }
        RemoteAction::AddAdmin { target } => add_admin(ans, target).await,
        RemoteAction::SetPolicy { target, name } => set_policy(ans, target, name).await,
        RemoteAction::RemoveAdmin { target, name } => {
            remove_admin(ans, target, name).await
        }
        RemoteAction::RemoveServer { target, server, .. } => {
            remove_server(ans, target.into_remote()?, server).await
        }
        RemoteAction::SetReadGate { target, server, gate, .. } => {
            set_read_gate(ans, target.into_remote()?, server, gate).await
        }
        RemoteAction::ReconcileCa { target } => {
            reconcile_ca(ans, target.into_remote()?).await
        }
        RemoteAction::EditPerms { target, at } => edit_perms(ans, target, at).await,
        RemoteAction::ServiceControl { target, server, units, op } => {
            service_control(ans, target.into_remote()?, server, units, op).await
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
async fn connect(
    ans: &mut TuiAnswerer,
    server: SocketAddr,
    expected_fp: Option<Fingerprint>,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::{
        answer::Answerer,
        ops,
        session_cache::{self, CachedSession},
        transport::{self, fetch_identity},
    };
    use netidx_admin_proto::{AdminCredential, NodeKind};
    // Glyph first — always, before any credential. Fetch the live identity and,
    // when we saved this admin domain before, flag a fingerprint that has changed
    // since (a CA rotation, or a different admin domain reusing the address) so the
    // operator scrutinises the glyph rather than rubber-stamping it.
    let id = fetch_identity(server, NodeKind::Client).await?;
    if let Some(fp) = expected_fp
        && fp != id.fingerprint
    {
        ans.warn(
            "this admin domain's CA glyph has CHANGED since you last saved it — verify \
             the glyph below out of band before continuing.",
        );
    }
    // `open_admin_session` repeats the identity fetch so its own trust ceremony
    // stays self-contained, resolves and verifies the exact CA, and
    // only then asks for a password (unless a valid cache already exists).
    let session = ops::open_admin_session(ans, Some(server), None, None, None).await?;
    let admin = session.admin.clone();
    if let AdminCredential::Password { admin, password } = session.credential {
        let logged = transport::login(
            session.server,
            &session.identity,
            &admin,
            password.as_str(),
        )
        .await?;
        let cached = CachedSession {
            ca_fingerprint: session.identity.fingerprint.text(),
            bootstrap: session.server,
            admin: logged.admin,
            token: logged.token,
            issued_unix: logged.issued_unix,
            absolute_deadline_unix: logged.absolute_deadline_unix,
            idle_timeout_secs: logged.idle_timeout_secs,
        };
        if let Err(e) = session_cache::store(cached.clone()) {
            session_cache::remember(cached)?;
            ans.note(&format!(
                "{}; this login will be retained only until the TUI exits",
                e
            ));
        }
    }
    let conn = RemoteConn {
        server: session.server,
        domain: session.identity.domain.clone(),
        confirmed_fp: session.identity.fingerprint,
        admin,
    };
    // Remember this admin domain (by its confirmed identity) for next time.
    let mut known = KnownAdminDomains::load();
    if known.upsert(&id.domain, server, id.fingerprint) {
        if let Err(e) = known.save() {
            ans.warn(&format!("could not save the admin domain list: {e:#}"));
        }
    }
    Ok(super::action::Outcome::remote_toast(
        "Connected",
        vec![format!("Connected to {} at {} as {}.", id.domain, server, conn.admin)],
        RemoteUpdate::Connected(conn),
    ))
}

#[cfg(unix)]
async fn logout(conn: RemoteConn) -> Result<super::action::Outcome> {
    use netidx_admin_client::{session_cache, transport};
    use netidx_admin_proto::NodeKind;
    let fingerprint = conn.confirmed_fp.text();
    let mut lines = Vec::new();
    match session_cache::load(&fingerprint) {
        Ok(Some(cached)) => {
            let revoked = async {
                let identity =
                    transport::fetch_identity(conn.server, NodeKind::Client).await?;
                if identity.fingerprint != conn.confirmed_fp || !identity.ca {
                    anyhow::bail!("the cached CA identity changed");
                }
                transport::logout(conn.server, &identity, cached.token.as_str()).await
            }
            .await;
            if let Err(e) = revoked {
                lines.push(format!("Remote revocation was unavailable: {e:#}"));
            }
        }
        Ok(None) => lines.push("The local session was already absent.".to_string()),
        Err(e) => {
            lines.push(format!("The local session cache could not be opened: {e:#}"))
        }
    }
    session_cache::delete(&fingerprint)?;
    lines.push(format!("Logged out {}.", conn.admin));
    Ok(super::action::Outcome::remote_toast("Logged out", lines, RemoteUpdate::LoggedOut))
}

/// Discover admin domains on the local network and refresh the Admin domain tab's
/// list — the same browse + per-admin domain CA-identity fetch the install flow and
/// `netidx admin discover` use (via [`enroll::discover_admin_domains`]), not a private
/// copy. Merges every reachable admin domain into the saved registry, then hands the
/// list back so the landing screen re-polls and shows the verified ones — no
/// toast to dismiss, just the list, like discovery everywhere else.
#[cfg(unix)]
async fn discover(ans: &mut TuiAnswerer) -> Result<super::action::Outcome> {
    use netidx_admin_client::{
        answer::{Answerer, Progress, Stage},
        plan::enroll,
    };
    use netidx_admin_proto::NodeKind;
    let timeout = netidx_admin_client::discovery::DISCOVERY_TIMEOUT;
    ans.progress(Progress::timed(
        Stage::Discovering,
        "browsing for admin domains…",
        timeout,
    ));
    // `None` enumerates every admin domain in the window — the tab may manage several,
    // unlike the install flow's early-exit-on-first-found.
    let reports = enroll::discover_admin_domains(timeout, NodeKind::Client, None).await;
    let mut known = KnownAdminDomains::load();
    let mut verified = 0usize;
    let mut unverified: Vec<String> = Vec::new();
    for r in &reports {
        let addrs =
            r.admin_servers.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ");
        match &r.identity {
            Ok(id) => {
                verified += 1;
                for addr in &r.admin_servers {
                    known.upsert(&r.domain, *addr, id.fingerprint);
                }
                ans.note(&format!("discovered admin domain {:?} at {addrs}", r.domain));
            }
            Err(e) => {
                ans.warn(&format!(
                    "beacon for {:?} at [{addrs}] did not verify: {e}",
                    r.domain
                ));
                unverified.push(format!("{:?} at {addrs}: {e}", r.domain));
            }
        }
    }
    if let Err(e) = known.save() {
        ans.warn(&format!("could not save the admin domain list: {e:#}"));
    }
    // Success is silent — the refreshed list is the result. But if beacons were
    // seen yet none verified (the confusing empty-after-discover case), surface
    // the addresses + reason so an unreachable advertised address is diagnosable
    // instead of looking like "discovery found nothing".
    if verified == 0 && !unverified.is_empty() {
        let mut lines = vec![format!(
            "Found {} advertised admin server(s), but none answered with a CA identity:",
            unverified.len()
        )];
        lines.extend(unverified);
        lines.push(String::new());
        lines.push(
            "The admin server may be advertising an address this host can't reach \
             (check its listen address / firewall)."
                .to_string(),
        );
        return Ok(super::action::Outcome::remote_toast(
            "Discovery",
            lines,
            RemoteUpdate::AdminDomains(known.domains),
        ));
    }
    Ok(super::action::Outcome::remote_clusters(known.domains))
}

#[cfg(unix)]
async fn refresh(
    ans: &mut TuiAnswerer,
    target: PanelTarget,
    panel: Panel,
    path: Option<String>,
) -> Result<super::action::Outcome> {
    let rows = match panel {
        Panel::Queue => queue_rows(ans, target.remote()?).await?,
        Panel::Delegations => delegation_rows(ans, target.remote()?).await?,
        Panel::Roster => roster_rows(ans, &target).await?,
        Panel::Servers => server_rows(ans, target.remote()?).await?,
        Panel::Revocation => revocation_rows(ans, target.remote()?).await?,
        Panel::Perms => {
            let at = path.context("a target path is required for the perms panel")?;
            perms_rows(ans, &target, &at).await?
        }
        // Services are never opened/refreshed via `Refresh`: they go through
        // `ServiceControl` (which carries the one target server), because a
        // service listing is scoped to a single admin server, not a path.
        Panel::Service => {
            bail!("the services panel is driven by ServiceControl, not Refresh")
        }
    };
    Ok(super::action::Outcome::remote_rows(panel, rows))
}

#[cfg(unix)]
async fn queue_rows(ans: &mut TuiAnswerer, conn: &RemoteConn) -> Result<Vec<PanelRow>> {
    use netidx_admin_client::ops::queue::list_queue;
    let items =
        list_queue(ans, Some(conn.server), None, Some(conn.admin.clone()), None).await?;
    Ok(items.iter().map(queue_row).collect())
}

/// Format one queue item into a display row + its action code.
#[cfg(unix)]
fn queue_row(item: &netidx_admin_client::ops::queue::QueueItem) -> PanelRow {
    let name = if let Some(listen) = item.enroll_listen {
        format!("admin-server enrollment @ {listen}")
    } else {
        item.requested_name.clone()
    };
    if item.verified_renewal {
        return PanelRow::plain(
            format!(
                "↻ {name}  (renewal, {}, from {})",
                widgets::fmt_age(item.age_secs),
                item.peer
            ),
            RowKey::None,
        );
    }
    let code = item.code.as_ref().map(|c| c.text());
    let (tail, key) = match code {
        Some(c) => (
            format!(
                "{:?}  ({}, from {})",
                item.kind,
                widgets::fmt_age(item.age_secs),
                item.peer
            ),
            RowKey::Code(c),
        ),
        None => (format!("{:?}  (unparseable CSR — deny only)", item.kind), RowKey::None),
    };
    let mut row = PanelRow::plain(format!("{name}  {tail}"), key);
    if let Some(serial) = item.replaces_serial {
        row.detail.push((
            "Restores".to_string(),
            format!("certificate serial {serial} (will be revoked)"),
        ));
    }
    if let Some(listen) = item.enroll_listen {
        let cluster = match &item.cluster {
            Some(netidx_admin_proto::ResolverClusterPlacement::Create { .. }) => format!(
                "create at {}",
                item.cluster_base.as_deref().unwrap_or("(unknown base)")
            ),
            Some(netidx_admin_proto::ResolverClusterPlacement::Join { cluster }) => {
                format!(
                    "{cluster} at {}",
                    item.cluster_base.as_deref().unwrap_or("(unknown base)")
                )
            }
            None => "(missing)".to_string(),
        };
        let members = item
            .resolver_members
            .iter()
            .map(|m| format!("{} {:?}", m.addr, m.auth))
            .collect::<Vec<_>>()
            .join(", ");
        row.detail.extend([
            ("Listen".to_string(), listen.to_string()),
            ("Roles".to_string(), format!("{:?}", item.requested_roles)),
            ("Resolver cluster".to_string(), cluster),
            ("Resolver members".to_string(), members),
        ]);
        if let Some(old) = item.replaces {
            row.detail.push((
                "Replaces".to_string(),
                format!("{old} (old certificates will be revoked)"),
            ));
        }
    }
    row
}

#[cfg(unix)]
async fn approve(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    code: String,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::queue::approve;
    let out = approve(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
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
    use netidx_admin_client::ops::queue::approve_renewals;
    let results =
        approve_renewals(ans, Some(conn.server), None, Some(conn.admin.clone()), None)
            .await?;
    let ok = results.iter().filter(|r| r.error.is_none()).count();
    let mut lines = vec![format!("Approved {ok} of {} renewal(s).", results.len())];
    for r in results.iter().filter(|r| r.error.is_some()) {
        lines.push(format!(
            "  ! {} : {}",
            r.requested_name,
            r.error.as_deref().unwrap_or("")
        ));
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
    use netidx_admin_client::{answer::Answerer, ops::queue::deny};
    let reason = ans
        .text(
            netidx_admin_client::answer::Field::RevokeReason,
            None,
            Some("denied"),
            true,
        )
        .await?
        .unwrap_or_else(|| "denied".to_string());
    let name = deny(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
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
async fn delegation_rows(
    ans: &mut TuiAnswerer,
    conn: &RemoteConn,
) -> Result<Vec<PanelRow>> {
    use netidx_admin_client::ops::delegation::list_pending_delegations;
    let items = list_pending_delegations(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
    )
    .await?;
    Ok(items.iter().map(delegation_row).collect())
}

/// Format one pending or approved delegation into a display row + its action code.
#[cfg(unix)]
fn delegation_row(
    item: &netidx_admin_client::ops::delegation::PendingDelegation,
) -> PanelRow {
    let parent =
        item.parent.iter().map(|a| a.addr.to_string()).collect::<Vec<_>>().join(", ");
    let child =
        item.child.iter().map(|a| a.addr.to_string()).collect::<Vec<_>>().join(", ");
    PanelRow::plain(
        format!(
            "{}  [{}; {}, from {}; parent {} [{}] {parent}; child {} [{}] {child}]",
            item.proposed_path,
            if item.approved { "approved — a reconciles" } else { "pending" },
            widgets::fmt_age(item.age_secs),
            item.peer,
            item.parent_base,
            item.parent_cluster,
            item.child_base,
            item.child_cluster,
        ),
        RowKey::Code(item.code.text()),
    )
}

#[cfg(unix)]
async fn approve_delegation(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    code: String,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::delegation::approve_delegation;
    let out = approve_delegation(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
        &code,
    )
    .await?;
    let mut lines =
        vec![format!("Delegated {} to the child resolver cluster.", out.proposed_path)];
    let failed: Vec<_> = out.peers.iter().filter(|p| p.error.is_some()).collect();
    if failed.is_empty() {
        lines.push(format!(
            "Wrote topology configuration on {} resolver cluster member(s); no service was restarted.",
            out.peers.len()
        ));
    } else {
        for p in &failed {
            lines.push(format!(
                "  ! server {} at {} : {}",
                p.server,
                p.addr,
                p.error.as_deref().unwrap_or("?")
            ));
        }
    }
    lines.push(
        "Roll each affected resolver cluster manually: restart one member, wait the resolver delay-reads period for publishers to republish, then restart the next member."
            .to_string(),
    );
    let rows = delegation_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after("Approved", lines, Panel::Delegations, rows))
}

#[cfg(unix)]
async fn deny_delegation(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    code: String,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::{answer::Answerer, ops::delegation::deny_delegation};
    let reason = ans
        .text(
            netidx_admin_client::answer::Field::RevokeReason,
            None,
            Some("denied"),
            true,
        )
        .await?
        .unwrap_or_else(|| "denied".to_string());
    let item = deny_delegation(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
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
async fn revocation_rows(
    ans: &mut TuiAnswerer,
    conn: &RemoteConn,
) -> Result<Vec<PanelRow>> {
    use netidx_admin_client::ops::revoke::issued;
    let entries = issued(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
        false, // live certificates only — the revoke target set
        None,
    )
    .await?;
    Ok(entries.iter().map(revocation_row).collect())
}

/// Format one issued certificate into a display row + its revoke key (serial +
/// its per-key glyph, `None` when the stored glyph is empty/unparseable).
#[cfg(unix)]
fn revocation_row(e: &netidx_admin_proto::IssuedEntry) -> PanelRow {
    let glyph = Fingerprint::parse_text(&e.spki_fp).ok();
    let short = match &glyph {
        Some(g) => g.text().split(' ').take(2).collect::<Vec<_>>().join(" "),
        None => "(no glyph)".to_string(),
    };
    let name = if e.name.is_empty() { "(no DNS SAN)" } else { e.name.as_str() };
    PanelRow::plain(
        format!(
            "#{:<6} {name}  exp {}  {short}",
            e.serial,
            widgets::fmt_expiry(e.not_after_unix),
        ),
        RowKey::Cert { serial: e.serial, glyph },
    )
}

#[cfg(unix)]
async fn revoke(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    serial: u64,
    glyph: Option<Fingerprint>,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::{
        answer::Answerer,
        ops::revoke::{RevokeSelector, revoke},
    };
    let reason = ans
        .text(
            netidx_admin_client::answer::Field::RevokeReason,
            None,
            Some("revoked"),
            true,
        )
        .await?
        .unwrap_or_else(|| "revoked".to_string());
    let out = revoke(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
        RevokeSelector::Serial(serial),
        glyph, // assert the glyph we displayed (skipped for a legacy empty glyph)
        &reason,
    )
    .await?;
    let mut lines: Vec<String> = out
        .revoked
        .iter()
        .map(|e| format!("Revoked #{} {}.", e.serial, e.name))
        .collect();
    if let Some(operation_id) = out.operation_id {
        lines.push(format!("CRL distribution operation {operation_id}."));
    }
    for peer in &out.peers {
        match &peer.error {
            None => lines.push(format!("updated {} at {}", peer.server, peer.addr)),
            Some(error) => {
                lines.push(format!("FAILED {} at {}: {error}", peer.server, peer.addr))
            }
        }
    }
    for w in &out.warnings {
        lines.push(format!("warning: {w}"));
    }
    let rows = revocation_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after("Revoked", lines, Panel::Revocation, rows))
}

#[cfg(unix)]
async fn admin_target(
    ans: &mut TuiAnswerer,
    target: &PanelTarget,
) -> Result<netidx_admin_client::ops::AdminTarget> {
    use netidx_admin_client::ops::{AdminTarget, resolve_admin_target};
    match target {
        PanelTarget::Local { cfg_path, .. } => {
            Ok(AdminTarget::Local { cfg_path: cfg_path.clone() })
        }
        PanelTarget::Remote(conn) => {
            resolve_admin_target(
                ans,
                Some(conn.server),
                None,
                Some(conn.admin.clone()),
                None,
            )
            .await
        }
    }
}

#[cfg(unix)]
async fn roster_rows(
    ans: &mut TuiAnswerer,
    target: &PanelTarget,
) -> Result<Vec<PanelRow>> {
    use netidx_admin_client::ops::roster::list_admins;
    let at = admin_target(ans, target).await?;
    Ok(list_admins(&at).await?.iter().map(roster_row).collect())
}

#[cfg(unix)]
async fn server_rows(ans: &mut TuiAnswerer, conn: &RemoteConn) -> Result<Vec<PanelRow>> {
    use netidx_admin_client::ops::servers::list_servers;
    let servers = list_servers(ans, Some(conn.server), None).await?;
    Ok(servers.iter().map(server_row).collect())
}

#[cfg(unix)]
fn server_row(server: &netidx_admin_client::ops::servers::ServerInfo) -> PanelRow {
    let cluster = server.cluster_base.clone().unwrap_or_else(|| "(none)".to_string());
    let roles = server
        .roles
        .iter()
        .map(|role| match role {
            netidx_admin_proto::Role::Ca => "CA",
            netidx_admin_proto::Role::Resolver => "resolver",
            netidx_admin_proto::Role::IdMap => "id-map",
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut tags = format!("{:?}", server.state);
    if server.ca {
        tags.push_str(", CA");
    }
    PanelRow {
        text: format!(
            "{:<12}  {}  {}  {:<14}  [{tags}]",
            cluster,
            server.id,
            server.addr,
            netidx_admin_client::ops::servers::read_gate_label(server.read_gate),
        ),
        key: RowKey::Server {
            id: server.id,
            addr: server.addr,
            cluster: cluster.clone(),
            ca: server.ca,
            resolver: server.roles.contains(netidx_admin_proto::Role::Resolver),
            gate: server.read_gate,
        },
        detail: vec![
            ("Server ID".to_string(), server.id.to_string()),
            ("Admin address".to_string(), server.addr.to_string()),
            ("Resolver cluster".to_string(), cluster),
            (
                "Resolver cluster ID".to_string(),
                server
                    .cluster
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            (
                "Resolver cluster state".to_string(),
                server
                    .cluster_state
                    .map(|state| format!("{state:?}"))
                    .unwrap_or_else(|| "-".to_string()),
            ),
            ("Enrollment state".to_string(), format!("{:?}", server.state)),
            (
                "Read gate".to_string(),
                netidx_admin_client::ops::servers::read_gate_detail(server.read_gate),
            ),
            ("Roles".to_string(), if roles.is_empty() { "-".to_string() } else { roles }),
            (
                "Resolver address".to_string(),
                server
                    .resolver
                    .as_ref()
                    .map(|resolver| resolver.addr.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
            (
                "Removal".to_string(),
                if server.ca {
                    "protected — replace the CA first".to_string()
                } else {
                    "press x only after the machine is permanently dead".to_string()
                },
            ),
        ],
    }
}

#[cfg(unix)]
async fn remove_server(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    server: AdminServerId,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::servers;
    let out = servers::remove_server(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
        server,
    )
    .await?;
    let failed: Vec<_> = out.peers.iter().filter(|peer| peer.error.is_some()).collect();
    let crl_failed: Vec<_> =
        out.crl_peers.iter().filter(|peer| peer.error.is_some()).collect();
    let mut lines = vec![
        if out.removed {
            format!("Permanently removed server {server}.")
        } else {
            format!("Server {server} was already absent; reconciled topology.")
        },
        format!("Revoked {} serving certificate(s).", out.revoked),
        format!(
            "Updated CRL on {} of {} target(s).",
            out.crl_peers.len() - crl_failed.len(),
            out.crl_peers.len()
        ),
        format!(
            "Updated topology on {} of {} target(s); no service was restarted.",
            out.peers.len() - failed.len(),
            out.peers.len()
        ),
    ];
    if let Some(operation_id) = out.operation_id {
        lines.push(format!("Operation {operation_id}; map version {}.", out.version));
    }
    if !out.affected_clusters.is_empty() {
        lines.push(format!(
            "Affected resolver clusters: {}.",
            out.affected_clusters.join(", ")
        ));
    }
    for peer in failed {
        lines.push(format!(
            "  ! server {} at {}: {}",
            peer.server,
            peer.addr,
            peer.error.as_deref().unwrap_or("unknown error")
        ));
    }
    for peer in crl_failed {
        lines.push(format!(
            "  ! CRL server {} at {}: {}",
            peer.server,
            peer.addr,
            peer.error.as_deref().unwrap_or("unknown error")
        ));
    }
    if !out.peers.is_empty() {
        lines.push(
            "If the written topology requires a restart, roll each affected resolver cluster manually: restart one member, wait the resolver delay-reads period for publishers to republish, then restart the next member."
                .to_string(),
        );
    }
    if !out.peers.iter().all(|peer| peer.error.is_none())
        || !out.crl_peers.iter().all(|peer| peer.error.is_none())
    {
        lines.push(
            "Topology is not fully reconciled. When the failed target is reachable, repeat force-remove with the same UUID to converge."
                .to_string(),
        );
    }
    let rows = server_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after(
        "Server removed",
        lines,
        Panel::Servers,
        rows,
    ))
}

/// The panel is re-listed from the map afterwards rather than assuming the
/// change took: the member reports its own gate, so what comes back is what
/// that host says it is doing, not what we just asked for.
#[cfg(unix)]
async fn set_read_gate(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    server: ServiceTarget,
    gate: ReadGate,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::service;
    service::set_read_gate(
        ans,
        conn.server,
        None,
        Some(conn.admin.clone()),
        None,
        server.id,
        gate,
    )
    .await?;
    let (title, line) = match gate {
        ReadGate::No => (
            "Read gate opened",
            format!("{} at {} is answering read clients.", server.id, server.addr),
        ),
        ReadGate::Yes => (
            "Read gate shut",
            format!(
                "{} at {} is no longer answering read clients. Publishers keep \
                 writing to it.",
                server.id, server.addr
            ),
        ),
        ReadGate::Until(t) => (
            "Read gate shut",
            format!(
                "{} at {} will start answering read clients at {}.",
                server.id,
                server.addr,
                t.format("%Y-%m-%d %H:%M:%SZ")
            ),
        ),
    };
    let rows = server_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after(title, vec![line], Panel::Servers, rows))
}

#[cfg(unix)]
async fn reconcile_ca(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::servers;
    let (operation_id, peers) = servers::reconcile_ca(
        ans,
        Some(conn.server),
        None,
        Some(conn.admin.clone()),
        None,
    )
    .await?;
    let failed: Vec<_> = peers.iter().filter(|peer| peer.error.is_some()).collect();
    let mut lines = vec![
        format!("Operation {operation_id}."),
        format!(
            "Updated {} of {} registered server(s).",
            peers.len() - failed.len(),
            peers.len()
        ),
    ];
    for peer in failed {
        lines.push(format!(
            "  ! server {} at {}: {}",
            peer.server,
            peer.addr,
            peer.error.as_deref().unwrap_or("unknown error")
        ));
    }
    if !peers.iter().all(|peer| peer.error.is_none()) {
        lines.push(
            "Bring failed servers back online and press c again; CA reconciliation is idempotent."
                .to_string(),
        );
    }
    let rows = server_rows(ans, &conn).await?;
    Ok(super::action::Outcome::remote_after("CA reconciled", lines, Panel::Servers, rows))
}

/// Format one roster entry. Reserved signing slots (recovery / autorenew) are
/// display-only ([`RowKey::None`]) — the roster actions must never target them.
#[cfg(unix)]
fn roster_row(a: &netidx_admin_proto::policy::AdminInfo) -> PanelRow {
    use netidx_admin_proto::policy::{SlotKind, is_reserved_admin};
    let tier = match a.kind {
        SlotKind::Signing => "signing",
        SlotKind::Role => "role",
    };
    let reserved = is_reserved_admin(&a.admin);
    let tag = if reserved { "  (system slot)" } else { "" };
    let key = if reserved { RowKey::None } else { RowKey::Name(a.admin.clone()) };
    // The list line is just identity; the granted authorities go in the detail
    // pane below, spelled out rather than crammed into a cryptic one-liner.
    PanelRow {
        text: format!("{:<20} [{tier}]{tag}", a.admin),
        key,
        detail: policy_detail(a),
    }
}

/// An admin's granted authorities as readable `(label, value)` lines for the
/// roster detail pane. A signing slot holds the CA master key, so its authority
/// is total and the granular policy doesn't apply; a role admin is the sum of
/// its explicit grants (an empty scope reads as "none", not "any").
#[cfg(unix)]
fn policy_detail(a: &netidx_admin_proto::policy::AdminInfo) -> Vec<(String, String)> {
    use netidx_admin_proto::policy::SlotKind;
    if matches!(a.kind, SlotKind::Signing) {
        return vec![
            (
                "Kind".to_string(),
                "signing slot (recovery / auto-renew credential)".to_string(),
            ),
            (
                "Authority".to_string(),
                "full — holds the CA master key; can issue or revoke any certificate"
                    .to_string(),
            ),
        ];
    }
    // Destructured with no `..`, so a new capability can't be added to Policy
    // without an operator ever being shown it.
    let netidx_admin_proto::policy::Policy {
        allowed_san,
        max_validity,
        id_map_groups,
        server_enroll_scopes,
        server_enroll_roles,
        perms_edit_scopes,
        may_manage_admins,
        service_control_scopes,
    } = &a.policy;
    let scope =
        |v: &[String]| if v.is_empty() { "(none)".to_string() } else { v.join(", ") };
    let yesno = |b: bool| if b { "yes".to_string() } else { "no".to_string() };
    vec![
        ("May issue certs for (SAN)".to_string(), scope(allowed_san)),
        (
            "Max validity it may grant".to_string(),
            format!("{} days", max_validity.as_secs() / 86400),
        ),
        ("Id-map groups it may grant".to_string(), scope(id_map_groups)),
        ("Enroll servers under".to_string(), scope(server_enroll_scopes)),
        ("Enrollment roles".to_string(), format!("{server_enroll_roles:?}")),
        ("Manage other admins".to_string(), yesno(*may_manage_admins)),
        ("Edit permissions under".to_string(), scope(perms_edit_scopes)),
        ("Control services under".to_string(), scope(service_control_scopes)),
    ]
}

/// The `$EDITOR` validator for a policy JSON blob: it must parse as a `Policy`;
/// returns the normalized (pretty) JSON to store.
#[cfg(unix)]
fn policy_validator() -> super::answer::EditValidator {
    Box::new(|s: &str| {
        let p: netidx_admin_proto::policy::Policy =
            serde_json::from_str(s).context("not valid policy JSON")?;
        serde_json::to_string_pretty(&p).context("serializing policy")
    })
}

/// A starter policy for a new role admin — every field present (all grants off)
/// so the editor shows exactly what can be granted.
#[cfg(unix)]
fn policy_template() -> netidx_admin_proto::policy::Policy {
    netidx_admin_proto::policy::Policy {
        allowed_san: vec![],
        max_validity: std::time::Duration::from_secs(730 * 86400),
        id_map_groups: vec![],
        server_enroll_scopes: vec![],
        server_enroll_roles: enumflags2::BitFlags::empty(),
        perms_edit_scopes: vec![],
        may_manage_admins: false,
        service_control_scopes: vec![],
    }
}

#[cfg(unix)]
async fn add_admin(
    ans: &mut TuiAnswerer,
    target: PanelTarget,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::{
        answer::{Answerer, Field},
        ops::roster::add_role_admin,
    };
    let name = ans
        .text(Field::AdminName, None, None, true)
        .await?
        .context("an admin name is required")?;
    let password = ans.secret(Field::AdminPassword, None).await?;
    let seed = serde_json::to_string_pretty(&policy_template())?;
    let edited = ans.edit(seed, policy_validator()).await?;
    let policy: netidx_admin_proto::policy::Policy = serde_json::from_str(&edited)?;
    let at = admin_target(ans, &target).await?;
    add_role_admin(&at, &name, &password, policy).await?;
    let rows = roster_rows(ans, &target).await?;
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
    target: PanelTarget,
    name: String,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::roster::{list_admins, set_admin_policy};
    let at = admin_target(ans, &target).await?;
    let current = list_admins(&at)
        .await?
        .into_iter()
        .find(|a| a.admin == name)
        .with_context(|| format!("admin {name:?} not found in the roster"))?;
    let seed = serde_json::to_string_pretty(&current.policy)?;
    let edited = ans.edit(seed, policy_validator()).await?;
    let policy: netidx_admin_proto::policy::Policy = serde_json::from_str(&edited)?;
    set_admin_policy(&at, &name, policy).await?;
    let rows = roster_rows(ans, &target).await?;
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
    target: PanelTarget,
    name: String,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::roster::remove_admin;
    let at = admin_target(ans, &target).await?;
    remove_admin(&at, &name).await?;
    let rows = roster_rows(ans, &target).await?;
    Ok(super::action::Outcome::remote_after(
        "Admin removed",
        vec![format!("Removed admin {name:?}.")],
        Panel::Roster,
        rows,
    ))
}

/// Read the perms of the admin domain mounted at `at`, for either target. A remote
/// target authenticates to its verified CA; a local target uses the
/// protected control socket and is confined to this host's own resolver cluster.
#[cfg(unix)]
async fn show_perms_for(
    ans: &mut TuiAnswerer,
    target: &PanelTarget,
    at: &str,
) -> Result<String> {
    use netidx_admin_client::ops::perms::{show_perms, show_perms_local};
    match target {
        PanelTarget::Remote(conn) => {
            show_perms(ans, Some(conn.server), None, Some(conn.admin.clone()), None, at)
                .await
        }
        PanelTarget::Local { cfg_path, .. } => show_perms_local(cfg_path, at).await,
    }
}

/// Fetch the admin domain's resolver clusters (by base path) and open the resolver cluster
/// picker for the Perms panel — the admin domain-scope replacement for typing a path.
#[cfg(unix)]
async fn list_resolver_clusters(
    ans: &mut TuiAnswerer,
    target: PanelTarget,
) -> Result<super::action::Outcome> {
    let bases = match &target {
        PanelTarget::Remote(conn) => {
            netidx_admin_client::ops::perms::list_resolver_clusters(
                ans,
                Some(conn.server),
                None,
            )
            .await?
        }
        PanelTarget::Local { .. } => vec![local_own_base()],
    };
    Ok(super::action::Outcome::resolver_clusters(Panel::Perms, bases))
}

#[cfg(unix)]
async fn perms_rows(
    ans: &mut TuiAnswerer,
    target: &PanelTarget,
    at: &str,
) -> Result<Vec<PanelRow>> {
    let json = show_perms_for(ans, target, at).await?;
    let pretty = super::super::perms_admin::pretty(&json)?;
    let mut rows: Vec<PanelRow> =
        pretty.lines().map(|l| PanelRow::plain(l.to_string(), RowKey::None)).collect();
    if rows.is_empty() {
        rows.push(PanelRow::plain("(no permissions set)".to_string(), RowKey::None));
    }
    Ok(rows)
}

#[cfg(unix)]
async fn edit_perms(
    ans: &mut TuiAnswerer,
    target: PanelTarget,
    at: String,
) -> Result<super::action::Outcome> {
    use netidx_admin_client::ops::perms::{
        edit_perms_local, edit_perms_with_session, open_perms_session, show_perms_local,
    };
    // Seed the editor with the admin domain's current perms, validate locally, then
    // hand the normalized result to the CA (which re-validates + propagates).
    let (session, current) = match &target {
        PanelTarget::Remote(conn) => {
            let (session, current) = open_perms_session(
                ans,
                Some(conn.server),
                None,
                Some(conn.admin.clone()),
                None,
                &at,
            )
            .await?;
            (Some(session), current)
        }
        PanelTarget::Local { cfg_path, .. } => {
            (None, show_perms_local(cfg_path, &at).await?)
        }
    };
    let seed = super::super::perms_admin::pretty(&current)?;
    let validate: super::answer::EditValidator =
        Box::new(|s: &str| super::super::perms_admin::validate(s));
    let edited = ans.edit(seed, validate).await?;
    let peers = match &target {
        PanelTarget::Remote(_) => {
            edit_perms_with_session(
                session.as_ref().expect("remote branch opened a session"),
                &at,
                &edited,
            )
            .await?
        }
        PanelTarget::Local { cfg_path, .. } => {
            edit_perms_local(cfg_path, &at, &edited).await?
        }
    };
    let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
    let lines = if failed.is_empty() {
        vec![format!(
            "Updated perms at {at:?} on {} resolver cluster member(s). Restart the resolver \
             server(s) to load them.",
            peers.len()
        )]
    } else {
        let mut v = vec![format!(
            "{} of {} member(s) could NOT be updated — the resolver cluster is INCONSISTENT; \
             re-edit to converge:",
            failed.len(),
            peers.len()
        )];
        for p in &failed {
            v.push(format!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("?")));
        }
        v
    };
    let rows = perms_rows(ans, &target, &at).await?;
    Ok(super::action::Outcome::remote_after("Perms updated", lines, Panel::Perms, rows))
}

/// The admin domain's admin servers, from the map — the service-control server
/// picker (the first pick). Each row preserves the immutable ID that a control op uses;
/// its address is display-only routing context.
#[cfg(unix)]
async fn list_service_servers(
    ans: &mut TuiAnswerer,
    target: PanelTarget,
) -> Result<super::action::Outcome> {
    let conn = target.remote()?;
    let servers =
        netidx_admin_client::ops::service::list_service_servers(ans, conn.server, None)
            .await?;
    let rows: Vec<ServiceServerRow> = servers
        .into_iter()
        .map(|s| ServiceServerRow {
            target: ServiceTarget { id: s.id, addr: s.addr },
            label: format!("{:<22} {:<12} {}", s.addr.to_string(), s.base, s.id),
        })
        .collect();
    Ok(super::action::Outcome::service_servers(rows))
}

/// The selected server's units as shared [`ServiceRow`]s (state + definition),
/// so the remote panel renders identically to the Local Services surface. A
/// status query to that one server (`units` empty ⇒ all).
#[cfg(unix)]
async fn fetch_service_rows(
    ans: &mut TuiAnswerer,
    conn: &RemoteConn,
    server: ServiceTarget,
) -> Result<Vec<super::services::ServiceRow>> {
    use netidx_activation::control::ControlOp;
    let units = service_op(ans, conn, server, Vec::new(), ControlOp::Status).await?;
    Ok(units.iter().map(super::services::ServiceRow::from_service_unit).collect())
}

/// One-shot service-control RPC against a single admin server, forwarding to
/// `ops::service::control_remote`. Returns each unit's state + (from the
/// member) its definition.
#[cfg(unix)]
async fn service_op(
    ans: &mut TuiAnswerer,
    conn: &RemoteConn,
    server: ServiceTarget,
    units: Vec<String>,
    op: netidx_activation::control::ControlOp,
) -> Result<Vec<netidx_admin_proto::ServiceUnit>> {
    use netidx_admin_client::ops::service::control_remote;
    control_remote(
        ans,
        conn.server,
        None,
        Some(conn.admin.clone()),
        None,
        server.id,
        units,
        op,
    )
    .await
}

#[cfg(unix)]
async fn service_control(
    ans: &mut TuiAnswerer,
    conn: RemoteConn,
    server: ServiceTarget,
    units: Vec<String>,
    op: ServiceOp,
) -> Result<super::action::Outcome> {
    use netidx_activation::control::ControlOp;
    let (control_op, verb) = match op {
        ServiceOp::Start => (ControlOp::Start, "Started"),
        ServiceOp::Stop => (ControlOp::Stop, "Stopped"),
        ServiceOp::Restart => (ControlOp::Restart, "Restarted"),
        ServiceOp::Status => (ControlOp::Status, "Services"),
    };
    let result = service_op(ans, &conn, server, units.clone(), control_op).await;
    // Whatever the op did, re-list the server so the panel reflects reality.
    let rows = fetch_service_rows(ans, &conn, server).await?;
    // The initial listing / a refresh opens the panel silently — no toast the
    // operator has to dismiss before seeing the units (Eric's ask). An explicit
    // start/stop/restart reports its outcome.
    if matches!(op, ServiceOp::Status) {
        return Ok(super::action::Outcome::remote_service_rows(rows));
    }
    let lines = match (&result, units.first()) {
        (Ok(_), Some(u)) => {
            vec![format!("{verb} {u} on {} at {}.", server.id, server.addr)]
        }
        (Ok(_), None) => vec![format!("{verb} on {} at {}.", server.id, server.addr)],
        (Err(e), _) => vec![format!("{} at {}: {e:#}", server.id, server.addr)],
    };
    Ok(super::action::Outcome::remote_service_after(verb, lines, rows))
}

// ---- UI state (cross-platform) --------------------------------------------

/// Which Tab-2 screen is showing.
enum Screen {
    /// The known-admin domain list — the Admin domain tab's landing screen.
    AdminDomains,
    /// Manually enter an admin-server host + port to connect to directly.
    Manual { host: String, port: String, focus: ManualFocus },
    /// Pick a panel.
    Menu,
    /// Pick one of the admin domain's resolver clusters (from the map) before opening
    /// the perms panel — the admin domain-scope map-driven pick.
    ResolverClusterPick { panel: Panel, bases: Vec<String>, state: ListState },
    /// Pick one of the admin domain's admin servers (from the map) before opening the
    /// services panel — service control is per-server, so you pick the one to
    /// manage.
    ServerPick { admin_servers: Vec<ServiceServerRow>, state: ListState },
    /// Choose a read gate for the member selected in the Servers panel: pick a
    /// state, then — only for a timed gate — say how long. Kept out of the op
    /// body so the confirmation can name what is actually about to happen.
    Gate {
        server: ServiceTarget,
        current: Option<ReadGate>,
        choice: ListState,
        /// `Some` once "Shut until" is picked: the duration being typed.
        until: Option<String>,
    },
    /// A panel's rows.
    Panel(Panel),
}

/// Which field of the manual-connect form has focus.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ManualFocus {
    Host,
    Port,
}

impl ManualFocus {
    fn toggle(self) -> Self {
        match self {
            ManualFocus::Host => ManualFocus::Port,
            ManualFocus::Port => ManualFocus::Host,
        }
    }
}

pub(super) struct RemoteState {
    /// The panel target once established: a `Remote` session after connecting
    /// (Admin domain tab), or a `Local` control-socket target (Local tab).
    target: Option<PanelTarget>,
    screen: Screen,
    /// The saved admin-domain registry (loaded from disk). The landing list
    /// shows the subset whose CA identity currently verifies (`poll` ==
    /// `Present`).
    domains: Vec<KnownAdminDomain>,
    /// Per-admin-domain poll state, parallel to `domains`.
    poll: Vec<PollState>,
    /// Cursor over the *visible* (Present) admin domains on the landing screen.
    domain_list: ListState,
    error: Option<String>,
    /// The panel-menu cursor.
    menu: ListState,
    /// The current panel's rows + cursor.
    rows: Vec<PanelRow>,
    list: ListState,
    /// The target path of the current path-scoped panel (perms), for its
    /// actions and title. `None` outside such a panel.
    panel_path: Option<String>,
    /// The admin server the open services panel controls (picked from the map).
    /// `None` outside the services panel.
    service_target: Option<ServiceTarget>,
    /// The services panel's units (state + definition), shared type with the
    /// Local Services surface so both render via `services::render_units`. The
    /// generic `rows` above stays empty while the services panel is open.
    service_rows: Vec<super::services::ServiceRow>,
}

/// The panels offered in the menu (label + which panel).
const PANELS: [Panel; 7] = [
    Panel::Queue,
    Panel::Delegations,
    Panel::Roster,
    Panel::Servers,
    Panel::Revocation,
    Panel::Perms,
    Panel::Service,
];

/// The panels a local (no-auth) target can serve: the admin roster over the
/// control socket, and perms — read and written over the control socket (the
/// daemon authorizes the local superuser and confines both to its own resolver cluster).
/// The queue, delegations, and revocation have no no-auth local backend and
/// stay Admin domain-only.
const LOCAL_PANELS: [Panel; 2] = [Panel::Roster, Panel::Perms];

/// This host's own resolver base — the single resolver cluster a local (control-socket)
/// perms edit is allowed to touch. Best-effort from the local resolver config,
/// falling back to the root; the admin server enforces the confinement anyway.
fn local_own_base() -> String {
    netidx_admin_client::resolver::ResolverConfig::load_default()
        .map(|c| c.base_path())
        .unwrap_or_else(|_| "/".to_string())
}

/// Load the saved admin domain registry, ensuring this host's own admin domain (when it
/// runs an admin server) is included so a locally-created admin domain shows up
/// without a manual discover, and persisting that addition.
fn load_seeded_clusters() -> KnownAdminDomains {
    let mut known = KnownAdminDomains::load();
    if admin_domains::seed_local_admin_domain(&mut known) {
        let _ = known.save();
    }
    known
}

impl RemoteState {
    pub(super) fn new() -> RemoteState {
        let mut menu = ListState::default();
        menu.select(Some(0));
        let mut domain_list = ListState::default();
        domain_list.select(Some(0));
        let known = load_seeded_clusters();
        let poll = vec![PollState::Unpolled; known.domains.len()];
        RemoteState {
            target: None,
            screen: Screen::AdminDomains,
            domains: known.domains,
            poll,
            domain_list,
            error: None,
            menu,
            rows: Vec::new(),
            list: ListState::default(),
            panel_path: None,
            service_target: None,
            service_rows: Vec::new(),
        }
    }

    /// A local admin surface opened **directly** onto `panel` (skipping the panel
    /// menu) — the Local tab's split "Admins" / "Permissions" items. For the
    /// path-scoped Perms panel there is no path prompt: local perms are confined
    /// to this host's own resolver base. Returns the initial refresh op to run.
    pub(super) fn local_panel(
        cfg_path: PathBuf,
        panel: Panel,
    ) -> (RemoteState, Option<Action>) {
        let mut s = RemoteState::new();
        let target = PanelTarget::Local { cfg_path };
        s.target = Some(target.clone());
        // Local perms are always this host's own resolver cluster — no prompt, no picking
        // another resolver's permissions.
        let path = panel.path_scoped().then(local_own_base);
        s.panel_path = path.clone();
        s.screen = Screen::Panel(panel);
        let initial = Action::Remote(RemoteAction::Refresh { target, panel, path });
        (s, Some(initial))
    }

    /// Admin domain tab regained focus: on the landing list (not mid-session), reload
    /// the saved registry — an admin domain may have been saved this session — and
    /// re-poll it.
    pub(super) fn on_focus(&mut self) {
        if matches!(self.screen, Screen::AdminDomains) {
            self.reload_clusters();
        }
    }

    /// Reload the saved registry from disk and mark everything unpolled so the
    /// event loop re-verifies it.
    fn reload_clusters(&mut self) {
        let known = load_seeded_clusters();
        self.domains = known.domains;
        self.poll = vec![PollState::Unpolled; self.domains.len()];
    }

    /// The saved admin domains currently verified `Present`, each with the address to
    /// connect to — exactly the rows the landing list shows.
    fn visible(&self) -> Vec<(usize, SocketAddr)> {
        self.domains
            .iter()
            .enumerate()
            .filter_map(|(i, _)| {
                self.poll.get(i).and_then(PollState::present_addr).map(|a| (i, a))
            })
            .collect()
    }

    /// Saved admin domains not yet polled; marks each `Polling` so the event loop
    /// launches exactly one poll pass. Only polls on the landing screen.
    pub(super) fn take_pending_poll(&mut self) -> Vec<(usize, KnownAdminDomain)> {
        if !matches!(self.screen, Screen::AdminDomains) {
            return Vec::new();
        }
        let mut out = Vec::new();
        for i in 0..self.domains.len() {
            if matches!(self.poll[i], PollState::Unpolled) {
                self.poll[i] = PollState::Polling;
                out.push((i, self.domains[i].clone()));
            }
        }
        out
    }

    /// Fold in a finished poll pass, keeping the list cursor in range.
    pub(super) fn apply_poll(&mut self, results: Vec<(usize, PollState)>) {
        for (i, st) in results {
            if let Some(slot) = self.poll.get_mut(i) {
                *slot = st;
            }
        }
        let n = self.visible().len();
        let sel = self.domain_list.selected().unwrap_or(0);
        self.domain_list.select(Some(sel.min(n.saturating_sub(1))));
    }

    /// Apply a completed op's result.
    pub(super) fn apply(&mut self, update: RemoteUpdate) {
        match update {
            RemoteUpdate::Connected(conn) => {
                self.target = Some(PanelTarget::Remote(conn));
                self.screen = Screen::Menu;
                self.error = None;
            }
            RemoteUpdate::LoggedOut => {
                self.target = None;
                self.screen = Screen::AdminDomains;
                self.error = None;
                self.reload_clusters();
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
            RemoteUpdate::AdminDomains(clusters) => {
                self.domains = clusters;
                self.poll = vec![PollState::Unpolled; self.domains.len()];
                self.screen = Screen::AdminDomains;
                self.error = None;
            }
            RemoteUpdate::ResolverClusters { panel, bases } => {
                let mut state = ListState::default();
                state.select((!bases.is_empty()).then_some(0));
                self.screen = Screen::ResolverClusterPick { panel, bases, state };
                self.error = None;
            }
            RemoteUpdate::ServiceServers { servers } => {
                let mut state = ListState::default();
                state.select((!servers.is_empty()).then_some(0));
                self.screen = Screen::ServerPick { admin_servers: servers, state };
                self.error = None;
            }
            RemoteUpdate::ServiceRows { rows } => {
                self.service_rows = rows;
                if self.list.selected().is_none() && !self.service_rows.is_empty() {
                    self.list.select(Some(0));
                }
                let sel = self.list.selected().unwrap_or(0);
                self.list
                    .select(Some(sel.min(self.service_rows.len().saturating_sub(1))));
                self.screen = Screen::Panel(Panel::Service);
                self.error = None;
            }
        }
    }

    /// True when a text field is focused, so the App loop must route keystrokes
    /// here rather than treating 'q'/'l'/Tab as global shortcuts (hostnames and
    /// paths routinely contain those letters).
    pub(super) fn capturing_text(&self) -> bool {
        matches!(self.screen, Screen::Manual { .. })
            || matches!(self.screen, Screen::Gate { until: Some(_), .. })
    }

    /// The tool keys for the App gutter when this surface is drilled in (a
    /// connected admin domain or a sub-form), or `None` at the admin domain-list landing
    /// (where the tab bar + global gutter show instead).
    pub(super) fn gutter(&self) -> Option<String> {
        let keys = match &self.screen {
            Screen::AdminDomains => return None,
            Screen::Manual { .. } => "Enter connect · Esc back",
            // A Local surface (Local tab) closes back to the tab; a Remote one
            // disconnects.
            Screen::Menu => match self.target {
                Some(PanelTarget::Local { .. }) => "↑/↓ · Enter open · Esc back",
                Some(PanelTarget::Remote(_)) | None => {
                    "↑/↓ · Enter open · L logout · Esc disconnect"
                }
            },
            Screen::ResolverClusterPick { .. } => "↑/↓ · Enter open · Esc back",
            Screen::ServerPick { .. } => "↑/↓ · Enter open · Esc back",
            Screen::Gate { until: None, .. } => "↑/↓ · Enter choose · Esc back",
            Screen::Gate { until: Some(_), .. } => "Enter apply · Esc back",
            Screen::Panel(panel) => panel.keys(),
        };
        Some(format!(" {keys} "))
    }

    pub(super) fn on_key(&mut self, code: KeyCode) -> Option<Action> {
        if !cfg!(unix) {
            return None;
        }
        match &self.screen {
            Screen::AdminDomains => self.on_key_clusters(code),
            Screen::Manual { .. } => self.on_key_manual(code),
            Screen::Menu => self.on_key_menu(code),
            Screen::ResolverClusterPick { .. } => self.on_key_resolver_cluster_pick(code),
            Screen::ServerPick { .. } => self.on_key_server_pick(code),
            Screen::Gate { .. } => self.on_key_gate(code),
            Screen::Panel(panel) => self.on_key_panel(code, *panel),
        }
    }

    /// Pick a read gate for the selected member, then — for a timed gate only
    /// — how long. The duration is a duration, not an instant: it is what the
    /// CLI's `--until` takes, it needs no timezone, and `1h` needs no
    /// explaining. The confirmation echoes back what it resolved to.
    fn on_key_gate(&mut self, code: KeyCode) -> Option<Action> {
        let target = self.target.clone()?;
        let Screen::Gate { server, current, choice, until } = &mut self.screen else {
            return None;
        };
        let (server, current) = (*server, *current);
        if let Some(text) = until {
            match code {
                KeyCode::Char(c) => {
                    text.push(c);
                    self.error = None;
                }
                KeyCode::Backspace => {
                    text.pop();
                }
                KeyCode::Esc => {
                    *until = None;
                    self.error = None;
                }
                KeyCode::Enter => match parse_gate_duration(text) {
                    Ok(gate) => {
                        self.error = None;
                        self.screen = Screen::Panel(Panel::Servers);
                        return Some(Action::Remote(RemoteAction::SetReadGate {
                            target,
                            server,
                            gate,
                            current,
                        }));
                    }
                    Err(e) => self.error = Some(e),
                },
                _ => {}
            }
            return None;
        }
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                let i = choice.selected().unwrap_or(0).saturating_sub(1);
                choice.select(Some(i));
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let last = GATE_CHOICES.len() - 1;
                let i = choice.selected().map_or(0, |i| (i + 1).min(last));
                choice.select(Some(i));
            }
            KeyCode::Esc => {
                self.error = None;
                self.screen = Screen::Panel(Panel::Servers);
            }
            KeyCode::Enter => {
                let gate = match choice.selected().unwrap_or(0) {
                    0 => ReadGate::No,
                    1 => ReadGate::Yes,
                    _ => {
                        *until = Some(DEFAULT_GATE_DURATION.to_string());
                        self.error = None;
                        return None;
                    }
                };
                self.error = None;
                self.screen = Screen::Panel(Panel::Servers);
                return Some(Action::Remote(RemoteAction::SetReadGate {
                    target,
                    server,
                    gate,
                    current,
                }));
            }
            _ => {}
        }
        None
    }

    /// Pick a resolver cluster from the map-derived list, then open the
    /// perms panel against it.
    fn on_key_resolver_cluster_pick(&mut self, code: KeyCode) -> Option<Action> {
        let Screen::ResolverClusterPick { panel, bases, state } = &mut self.screen else {
            return None;
        };
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                let i = state.selected().unwrap_or(0).saturating_sub(1);
                state.select(Some(i));
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let last = bases.len().saturating_sub(1);
                let i = state.selected().map_or(0, |i| (i + 1).min(last));
                state.select(Some(i));
                None
            }
            KeyCode::Esc => {
                self.screen = Screen::Menu;
                None
            }
            KeyCode::Enter => {
                let sel = state.selected()?;
                let path = bases.get(sel)?.clone();
                let panel = *panel;
                let target = self.target.clone()?;
                self.panel_path = Some(path.clone());
                self.list.select(None);
                self.rows.clear();
                self.screen = Screen::Panel(panel);
                Some(Action::Remote(RemoteAction::Refresh {
                    target,
                    panel,
                    path: Some(path),
                }))
            }
            _ => None,
        }
    }

    /// Pick an admin domain admin server from the map-derived list, then open the
    /// services panel scoped to that one server.
    fn on_key_server_pick(&mut self, code: KeyCode) -> Option<Action> {
        let Screen::ServerPick { admin_servers: servers, state } = &mut self.screen
        else {
            return None;
        };
        match code {
            KeyCode::Up | KeyCode::Char('k') => {
                let i = state.selected().unwrap_or(0).saturating_sub(1);
                state.select(Some(i));
                None
            }
            KeyCode::Down | KeyCode::Char('j') => {
                let last = servers.len().saturating_sub(1);
                let i = state.selected().map_or(0, |i| (i + 1).min(last));
                state.select(Some(i));
                None
            }
            KeyCode::Esc => {
                self.screen = Screen::Menu;
                None
            }
            KeyCode::Enter => {
                let sel = state.selected()?;
                let server = servers.get(sel)?.target;
                let target = self.target.clone()?;
                self.service_target = Some(server);
                self.list.select(None);
                self.rows.clear();
                self.screen = Screen::Panel(Panel::Service);
                // Initial listing: a read-only status query against the server.
                Some(Action::Remote(RemoteAction::ServiceControl {
                    target,
                    server,
                    units: Vec::new(),
                    op: ServiceOp::Status,
                }))
            }
            _ => None,
        }
    }

    /// The landing screen: navigate the verified-present admin domains, connect to the
    /// selected one (glyph + login handled by the op), discover, or connect
    /// directly by address.
    fn on_key_clusters(&mut self, code: KeyCode) -> Option<Action> {
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.domain_list.select_previous(),
            KeyCode::Down | KeyCode::Char('j') => self.domain_list.select_next(),
            KeyCode::Char('d') => return Some(Action::Remote(RemoteAction::Discover)),
            KeyCode::Char('c') => {
                self.error = None;
                self.screen = Screen::Manual {
                    host: String::new(),
                    port: DEFAULT_ADMIN_PORT.to_string(),
                    focus: ManualFocus::Host,
                };
            }
            KeyCode::Char('r') => self.reload_clusters(),
            KeyCode::Enter => {
                let visible = self.visible();
                if visible.is_empty() {
                    return None;
                }
                let sel = self.domain_list.selected().unwrap_or(0).min(visible.len() - 1);
                let (ci, addr) = visible[sel];
                return Some(Action::Remote(RemoteAction::Connect {
                    server: addr,
                    expected_fp: self.domains[ci].fp(),
                }));
            }
            _ => {}
        }
        None
    }

    /// The manual host/port form — connect directly to an admin server by
    /// address. The op fetches + confirms the glyph before any credential.
    fn on_key_manual(&mut self, code: KeyCode) -> Option<Action> {
        let Screen::Manual { host, port, focus } = &mut self.screen else { return None };
        match code {
            KeyCode::Up | KeyCode::Down | KeyCode::Tab | KeyCode::BackTab => {
                *focus = focus.toggle()
            }
            KeyCode::Char(c) => {
                match focus {
                    ManualFocus::Host => host.push(c),
                    ManualFocus::Port => port.push(c),
                }
                self.error = None;
            }
            KeyCode::Backspace => match focus {
                ManualFocus::Host => {
                    host.pop();
                }
                ManualFocus::Port => {
                    port.pop();
                }
            },
            KeyCode::Esc => {
                self.error = None;
                self.screen = Screen::AdminDomains;
            }
            KeyCode::Enter => {
                let spec = format!("{}:{}", host.trim(), port.trim());
                match netidx_admin_client::plan::resolve_admin_server_addr(&spec) {
                    Ok(server) => {
                        self.screen = Screen::AdminDomains;
                        self.error = None;
                        return Some(Action::Remote(RemoteAction::Connect {
                            server,
                            expected_fp: None,
                        }));
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
                // Back to the admin domain list (disconnect); the cached session is
                // dropped. Reload so the admin domain we just connected to (now saved)
                // appears. (A Local surface is instead closed by its host, which
                // intercepts Esc-at-menu.)
                self.target = None;
                self.screen = Screen::AdminDomains;
                self.reload_clusters();
            }
            KeyCode::Char('L') => {
                if let Some(PanelTarget::Remote(conn)) = self.target.clone() {
                    return Some(Action::Remote(RemoteAction::Logout { conn }));
                }
            }
            KeyCode::Enter => {
                let panels =
                    self.target.as_ref().map(PanelTarget::panels).unwrap_or(&PANELS);
                let panel =
                    panels[self.menu.selected().unwrap_or(0).min(panels.len() - 1)];
                if matches!(panel, Panel::Perms) {
                    // Admin domain perms: pick a resolver cluster from the map, not a typed path.
                    self.error = None;
                    if let Some(target) = &self.target {
                        return Some(Action::Remote(
                            RemoteAction::ListResolverClusters { target: target.clone() },
                        ));
                    }
                } else if matches!(panel, Panel::Service) {
                    // Admin domain services: pick an admin server from the map, then
                    // control that one server (never an admin domain-wide fanout).
                    self.error = None;
                    if let Some(target) = &self.target {
                        return Some(Action::Remote(RemoteAction::ListServiceServers {
                            target: target.clone(),
                        }));
                    }
                } else if let Some(target) = &self.target {
                    self.panel_path = None;
                    self.list.select(None);
                    self.rows.clear();
                    return Some(Action::Remote(RemoteAction::Refresh {
                        target: target.clone(),
                        panel,
                        path: None,
                    }));
                }
            }
            _ => {}
        }
        None
    }

    fn on_key_panel(&mut self, code: KeyCode, panel: Panel) -> Option<Action> {
        let target = self.target.clone()?;
        // The services panel navigates its own `service_rows`, clamped to their
        // bounds like the Local surface; every other panel walks `rows`.
        let svc_len = matches!(panel, Panel::Service).then(|| self.service_rows.len());
        match code {
            KeyCode::Up | KeyCode::Char('k') => match svc_len {
                Some(0) => {}
                Some(_) => {
                    let i = self.list.selected().unwrap_or(0).saturating_sub(1);
                    self.list.select(Some(i));
                }
                None => self.list.select_previous(),
            },
            KeyCode::Down | KeyCode::Char('j') => match svc_len {
                Some(0) => {}
                Some(len) => {
                    let i = self.list.selected().map_or(0, |i| (i + 1).min(len - 1));
                    self.list.select(Some(i));
                }
                None => self.list.select_next(),
            },
            KeyCode::Esc => {
                self.panel_path = None;
                self.service_target = None;
                self.screen = Screen::Menu;
            }
            KeyCode::Char('r') => {
                return match panel {
                    // The services panel re-lists the one server's units with a
                    // silent status query; it has no path, so it can't go through
                    // Refresh (which is path-scoped and toasts).
                    Panel::Service => self.service_target.map(|server| {
                        Action::Remote(RemoteAction::ServiceControl {
                            target,
                            server,
                            units: Vec::new(),
                            op: ServiceOp::Status,
                        })
                    }),
                    _ => Some(Action::Remote(RemoteAction::Refresh {
                        target,
                        panel,
                        path: self.panel_path.clone(),
                    })),
                };
            }
            _ => match panel {
                Panel::Queue => return self.on_key_queue(code, target),
                Panel::Delegations => return self.on_key_delegations(code, target),
                Panel::Roster => return self.on_key_roster(code, target),
                Panel::Servers => return self.on_key_servers(code, target),
                Panel::Revocation => return self.on_key_revocation(code, target),
                Panel::Perms => return self.on_key_perms(code, target),
                Panel::Service => return self.on_key_service(code, target),
            },
        }
        None
    }

    fn on_key_perms(&mut self, code: KeyCode, target: PanelTarget) -> Option<Action> {
        match code {
            KeyCode::Char('e') => self
                .panel_path
                .clone()
                .map(|at| Action::Remote(RemoteAction::EditPerms { target, at })),
            _ => None,
        }
    }

    fn on_key_service(&mut self, code: KeyCode, target: PanelTarget) -> Option<Action> {
        let op = match code {
            KeyCode::Char('s') => ServiceOp::Start,
            KeyCode::Char('t') => ServiceOp::Stop,
            KeyCode::Char('R') => ServiceOp::Restart,
            _ => return None,
        };
        let server = self.service_target?;
        let unit = self.selected_unit()?;
        Some(Action::Remote(RemoteAction::ServiceControl {
            target,
            server,
            units: vec![unit],
            op,
        }))
    }

    fn on_key_queue(&mut self, code: KeyCode, target: PanelTarget) -> Option<Action> {
        match code {
            KeyCode::Char('R') => {
                Some(Action::Remote(RemoteAction::ApproveRenewals { target }))
            }
            KeyCode::Char('a') => self
                .selected_code()
                .map(|code| Action::Remote(RemoteAction::Approve { target, code })),
            KeyCode::Char('d') => self
                .selected_code()
                .map(|code| Action::Remote(RemoteAction::Deny { target, code })),
            _ => None,
        }
    }

    fn on_key_delegations(
        &mut self,
        code: KeyCode,
        target: PanelTarget,
    ) -> Option<Action> {
        match code {
            KeyCode::Char('a') => self.selected_code().map(|code| {
                Action::Remote(RemoteAction::ApproveDelegation { target, code })
            }),
            KeyCode::Char('d') => self.selected_code().map(|code| {
                Action::Remote(RemoteAction::DenyDelegation { target, code })
            }),
            _ => None,
        }
    }

    fn on_key_revocation(
        &mut self,
        code: KeyCode,
        target: PanelTarget,
    ) -> Option<Action> {
        match code {
            KeyCode::Char('x') => self.selected_cert().map(|(serial, glyph)| {
                Action::Remote(RemoteAction::Revoke { target, serial, glyph })
            }),
            _ => None,
        }
    }

    fn on_key_roster(&mut self, code: KeyCode, target: PanelTarget) -> Option<Action> {
        match code {
            KeyCode::Char('a') => Some(Action::Remote(RemoteAction::AddAdmin { target })),
            KeyCode::Char('e') => self
                .selected_name()
                .map(|name| Action::Remote(RemoteAction::SetPolicy { target, name })),
            KeyCode::Char('d') => self
                .selected_name()
                .map(|name| Action::Remote(RemoteAction::RemoveAdmin { target, name })),
            _ => None,
        }
    }

    fn on_key_servers(&mut self, code: KeyCode, target: PanelTarget) -> Option<Action> {
        match code {
            KeyCode::Char('c') => {
                Some(Action::Remote(RemoteAction::ReconcileCa { target }))
            }
            KeyCode::Char('g') => {
                // The CA can be gated like any other member — unlike
                // force-remove, taking it out of service for readers is an
                // ordinary thing to want.
                match &self.rows.get(self.list.selected()?)?.key {
                    RowKey::Server { resolver: false, .. } => {
                        self.error = Some(
                            "that server runs no resolver, so it has no read gate to set"
                                .to_string(),
                        );
                    }
                    RowKey::Server { id, addr, gate, .. } => {
                        let mut choice = ListState::default();
                        choice.select(Some(0));
                        self.error = None;
                        self.screen = Screen::Gate {
                            server: ServiceTarget { id: *id, addr: *addr },
                            current: *gate,
                            choice,
                            until: None,
                        };
                    }
                    RowKey::None
                    | RowKey::Code(_)
                    | RowKey::Name(_)
                    | RowKey::Cert { .. } => {}
                }
                None
            }
            KeyCode::Char('x') => {
                self.selected_server().map(|(server, addr, cluster)| {
                    Action::Remote(RemoteAction::RemoveServer {
                        target,
                        server,
                        addr,
                        cluster,
                    })
                })
            }
            _ => None,
        }
    }

    /// The full code of the selected row, if it carries one (not a renewal /
    /// unparseable / non-code row).
    fn selected_code(&self) -> Option<String> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Code(c) => Some(c.clone()),
            RowKey::None
            | RowKey::Name(_)
            | RowKey::Cert { .. }
            | RowKey::Server { .. } => None,
        }
    }

    /// The selected certificate's serial + glyph, if the row is a cert row.
    fn selected_cert(&self) -> Option<(u64, Option<Fingerprint>)> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Cert { serial, glyph } => Some((*serial, *glyph)),
            RowKey::None | RowKey::Code(_) | RowKey::Name(_) | RowKey::Server { .. } => {
                None
            }
        }
    }

    /// The selected admin's name, if the row is an actionable roster row.
    fn selected_name(&self) -> Option<String> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Name(n) => Some(n.clone()),
            RowKey::None
            | RowKey::Code(_)
            | RowKey::Cert { .. }
            | RowKey::Server { .. } => None,
        }
    }

    /// The selected non-ca server. The CA remains visible in
    /// the inventory but cannot produce a destructive action.
    fn selected_server(&self) -> Option<(AdminServerId, SocketAddr, String)> {
        match &self.rows.get(self.list.selected()?)?.key {
            RowKey::Server { id, addr, cluster, ca: false, .. } => {
                Some((*id, *addr, cluster.clone()))
            }
            RowKey::None
            | RowKey::Code(_)
            | RowKey::Name(_)
            | RowKey::Cert { .. }
            | RowKey::Server { ca: true, .. } => None,
        }
    }

    /// The name of the selected unit in the services panel (clamped to the
    /// visible selection, matching what `render_units` highlights).
    fn selected_unit(&self) -> Option<String> {
        if self.service_rows.is_empty() {
            return None;
        }
        let i = self.list.selected()?.min(self.service_rows.len() - 1);
        self.service_rows.get(i).map(|r| r.name.clone())
    }

    pub(super) fn render(&mut self, f: &mut Frame, area: Rect) {
        if !cfg!(unix) {
            let msg = Paragraph::new(
                "Remote administration is only available on unix hosts (it drives the \
                 openssl-backed CA admin path).",
            )
            .wrap(Wrap { trim: true })
            .style(theme::panel_style())
            .block(
                theme::panel_block()
                    .title(Span::styled(" Admin domain ", theme::title_style())),
            );
            f.render_widget(msg, area);
            return;
        }
        match &self.screen {
            Screen::AdminDomains => self.render_clusters(f, area),
            Screen::Manual { host, port, focus } => {
                self.render_manual(f, area, host, port, *focus)
            }
            Screen::Menu => self.render_menu(f, area),
            Screen::ResolverClusterPick { panel, bases, state } => self
                .render_resolver_cluster_pick(f, area, *panel, bases, &mut state.clone()),
            Screen::ServerPick { admin_servers: servers, state } => {
                self.render_server_pick(f, area, servers, &mut state.clone())
            }
            Screen::Gate { server, current, choice, until } => {
                self.render_gate(f, area, server, *current, &mut choice.clone(), until)
            }
            Screen::Panel(panel) => self.render_panel(f, area, *panel),
        }
    }

    /// The gate chooser: the three states on the left with what each does on
    /// the right, then the duration form once a timed gate is picked.
    fn render_gate(
        &self,
        f: &mut Frame,
        area: Rect,
        server: &ServiceTarget,
        current: Option<ReadGate>,
        choice: &mut ListState,
        until: &Option<String>,
    ) {
        use netidx_admin_client::ops::servers::read_gate_detail;
        if let Some(text) = until {
            render_form(
                f,
                area,
                "Shut the read gate until",
                &[
                    "How long should it stop answering read clients?",
                    "A duration, such as 30m, 90m, or 2h.",
                ],
                text,
                self.error.as_deref(),
                "Publishers keep writing to it the whole time.",
            );
            return;
        }
        let cols =
            Layout::horizontal([Constraint::Length(24), Constraint::Min(0)]).split(area);
        let items: Vec<ListItem> =
            GATE_CHOICES.iter().map(|(label, _)| ListItem::new(*label)).collect();
        f.render_stateful_widget(
            List::new(items)
                .style(theme::panel_style())
                .block(
                    theme::panel_block()
                        .title(Span::styled(" Read gate ", theme::title_style())),
                )
                .highlight_style(theme::selected_style())
                .highlight_symbol("▸ "),
            cols[0],
            choice,
        );
        let sel = choice.selected().unwrap_or(0).min(GATE_CHOICES.len() - 1);
        let mut lines = vec![
            Line::from(vec![
                Span::styled("server:  ", theme::hint_style()),
                Span::styled(server.id.to_string(), theme::panel_style()),
            ]),
            Line::from(vec![
                Span::styled("address: ", theme::hint_style()),
                Span::styled(server.addr.to_string(), theme::panel_style()),
            ]),
            Line::from(vec![
                Span::styled("now:     ", theme::hint_style()),
                Span::styled(read_gate_detail(current), theme::panel_style()),
            ]),
            Line::from(""),
            Line::from(Span::styled(GATE_CHOICES[sel].1, theme::panel_style())),
        ];
        if let Some(e) = &self.error {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                e.clone(),
                Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT),
            )));
        }
        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: true })
                .style(theme::panel_style())
                .block(
                    theme::panel_block()
                        .title(Span::styled(" Member ", theme::title_style())),
                ),
            cols[1],
        );
    }

    /// The admin domain resolver cluster picker: a list of the map's resolver bases.
    fn render_resolver_cluster_pick(
        &self,
        f: &mut Frame,
        area: Rect,
        panel: Panel,
        bases: &[String],
        state: &mut ListState,
    ) {
        let items: Vec<ListItem> = if bases.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "(no bases found in the admin domain map)",
                theme::hint_style(),
            )))]
        } else {
            bases.iter().map(|l| ListItem::new(l.clone())).collect()
        };
        let list = List::new(items)
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(
                format!(" {} — pick a resolver cluster ", panel.title()),
                theme::title_style(),
            )))
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, area, state);
    }

    /// The service-control server picker: the admin domain's admin servers, each
    /// labelled with its resolver cluster base. Pick one to control its services.
    fn render_server_pick(
        &self,
        f: &mut Frame,
        area: Rect,
        servers: &[ServiceServerRow],
        state: &mut ListState,
    ) {
        let items: Vec<ListItem> = if servers.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "(no admin servers found in the admin domain map)",
                theme::hint_style(),
            )))]
        } else {
            servers.iter().map(|s| ListItem::new(s.label.clone())).collect()
        };
        let list = List::new(items)
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(
                " Services — pick an admin server ",
                theme::title_style(),
            )))
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, area, state);
    }

    /// The landing screen: the verified-present admin domains on the left, the
    /// selected admin domain's CA glyph on the right (same split as the Local tab's
    /// install card).
    fn render_clusters(&self, f: &mut Frame, area: Rect) {
        let visible = self.visible();
        let hint = " Enter connect · d discover · c connect direct · r refresh ";
        let block = theme::panel_block()
            .title(Span::styled(" Admin Domains ", theme::title_style()))
            .title_bottom(Line::from(Span::styled(hint, theme::hint_style())));
        let inner = block.inner(area);
        f.render_widget(block, area);
        if visible.is_empty() {
            let checking = self
                .poll
                .iter()
                .any(|p| matches!(p, PollState::Unpolled | PollState::Polling));
            let msg = if self.domains.is_empty() {
                "No saved admin domains yet. Press d to discover admin domains on the local \
                 admin domain, or c to connect to one by address."
            } else if checking {
                "Checking saved admin domains…"
            } else {
                "No saved admin domain is reachable here right now (an admin domain only shows \
                 when its CA glyph verifies). Press d to discover, c to connect, or r \
                 to re-check."
            };
            f.render_widget(
                Paragraph::new(msg).style(theme::hint_style()).wrap(Wrap { trim: true }),
                inner,
            );
            return;
        }
        let cols = Layout::horizontal([
            Constraint::Min(0),
            Constraint::Length(widgets::IDENTICON_WIDTH),
        ])
        .split(inner);
        let items: Vec<ListItem> = visible
            .iter()
            .map(|(ci, addr)| {
                let c = &self.domains[*ci];
                ListItem::new(Line::from(vec![
                    Span::styled(format!("{} ", c.domain), theme::panel_style()),
                    Span::styled(format!("({addr})"), theme::hint_style()),
                ]))
            })
            .collect();
        let mut st = self.domain_list;
        let list = List::new(items)
            .style(theme::panel_style())
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, cols[0], &mut st);
        // The selected admin domain's glyph.
        let sel = st.selected().unwrap_or(0).min(visible.len() - 1);
        if let Some(fp) = self.domains[visible[sel].0].fp() {
            let mut lines =
                vec![Line::from(Span::styled("CA glyph", theme::hint_style()))];
            lines.extend(widgets::identicon_lines(&fp));
            f.render_widget(Paragraph::new(lines).style(theme::panel_style()), cols[1]);
        }
    }

    /// The manual host/port connect form.
    fn render_manual(
        &self,
        f: &mut Frame,
        area: Rect,
        host: &str,
        port: &str,
        focus: ManualFocus,
    ) {
        let block = theme::panel_block()
            .title(Span::styled(" Connect direct ", theme::title_style()));
        let inner = block.inner(area);
        f.render_widget(block, area);
        let rows = Layout::vertical([
            Constraint::Length(2), // help
            Constraint::Length(1), // spacer
            Constraint::Length(1), // host
            Constraint::Length(1), // port
            Constraint::Length(1), // error
            Constraint::Length(1), // spacer
            Constraint::Length(1), // hint
            Constraint::Min(0),
        ])
        .split(inner);
        f.render_widget(
            Paragraph::new(
                "Enter an admin server's host and port. You'll confirm its CA glyph \
                 before entering any credentials.",
            )
            .style(theme::hint_style())
            .wrap(Wrap { trim: true }),
            rows[0],
        );
        let cursor =
            labeled_field(f, rows[2], "Host", host, false, focus == ManualFocus::Host)
                .or(labeled_field(
                    f,
                    rows[3],
                    "Port",
                    port,
                    false,
                    focus == ManualFocus::Port,
                ));
        if let Some(e) = &self.error {
            let err = Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT);
            f.render_widget(Paragraph::new(e.clone()).style(err), rows[4]);
        }
        f.render_widget(
            Paragraph::new(" Tab field · Enter connect · Esc back ")
                .style(theme::hint_style()),
            rows[6],
        );
        if let Some(pos) = cursor {
            f.set_cursor_position(pos);
        }
    }

    fn render_menu(&self, f: &mut Frame, area: Rect) {
        let title = match &self.target {
            Some(PanelTarget::Remote(c)) => format!(" {} — {} ", c.domain, c.admin),
            Some(PanelTarget::Local { .. }) => " Local admin server ".to_string(),
            None => " Admin domain ".to_string(),
        };
        let cols =
            Layout::horizontal([Constraint::Min(0), Constraint::Length(42)]).split(area);
        let panels = self.target.as_ref().map(PanelTarget::panels).unwrap_or(&PANELS);
        let items: Vec<ListItem> =
            panels.iter().map(|p| ListItem::new(p.title())).collect();
        let mut st = self.menu;
        let list = List::new(items)
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(title, theme::title_style())))
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, cols[0], &mut st);
        let sel = self.menu.selected().unwrap_or(0).min(panels.len().saturating_sub(1));
        let desc = panels.get(sel).map_or("", |p| p.desc());
        let blurb = Paragraph::new(desc)
            .wrap(Wrap { trim: true })
            .style(theme::panel_style())
            .block(
                theme::panel_block()
                    .title(Span::styled(" Description ", theme::title_style())),
            );
        f.render_widget(blurb, cols[1]);
    }

    fn render_panel(&self, f: &mut Frame, area: Rect, panel: Panel) {
        // The glyph-bearing panels — the enrollment queue and delegations (match
        // the screenshot before approving) and issued certificates (the glyph the
        // admin may have saved) — split into a list over a detail pane showing the
        // selected row's glyph + hash.
        if matches!(panel, Panel::Service) {
            // The services panel shares the Local Services surface's renderer —
            // unit list + Status + Definition — so the remote and local views
            // look and behave identically (remote just omits create/edit/delete).
            // A full-width header names the target server (the narrow list column
            // can't hold an address); the shared view renders below it.
            let header = match self.service_target {
                Some(server) => format!("Services @ {} ({})", server.addr, server.id),
                None => "Services".to_string(),
            };
            let split =
                Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).split(area);
            f.render_widget(
                Paragraph::new(Line::from(Span::styled(header, theme::title_style())))
                    .style(theme::panel_style()),
                split[0],
            );
            super::services::render_units(
                f,
                split[1],
                &self.service_rows,
                &self.list,
                " Services ",
            );
        } else if matches!(panel, Panel::Queue | Panel::Delegations | Panel::Revocation) {
            let split = Layout::vertical([
                Constraint::Min(0),
                Constraint::Length(widgets::IDENTICON_HEIGHT + 3),
            ])
            .split(area);
            self.render_panel_list(f, split[0], panel);
            self.render_panel_detail(f, split[1], panel);
        } else if matches!(panel, Panel::Roster | Panel::Servers) {
            // Admins and servers both have a compact identity list plus a
            // readable key/value detail pane for authority or topology facts.
            let split = Layout::vertical([
                Constraint::Percentage(45),
                Constraint::Percentage(55),
            ])
            .split(area);
            self.render_panel_list(f, split[0], panel);
            self.render_key_value_detail(f, split[1], panel);
        } else {
            self.render_panel_list(f, area, panel);
        }
    }

    /// The detail pane under the admin roster: the selected admin's name and its
    /// granted authorities as readable label/value lines (from the row's
    /// pre-formatted `detail`).
    fn render_key_value_detail(&self, f: &mut Frame, area: Rect, panel: Panel) {
        let title = if matches!(panel, Panel::Servers) {
            " Server identity "
        } else {
            " Authority "
        };
        let block = theme::panel_block().title(Span::styled(title, theme::title_style()));
        let inner = block.inner(area);
        f.render_widget(block, area);
        let row = self.list.selected().and_then(|i| self.rows.get(i));
        let lines: Vec<Line> = match row {
            None => {
                vec![Line::from(Span::styled("(no admin selected)", theme::hint_style()))]
            }
            Some(r) => {
                let mut ls = vec![
                    Line::from(Span::styled(r.text.clone(), theme::title_style())),
                    Line::from(""),
                ];
                for (label, value) in &r.detail {
                    ls.push(Line::from(vec![
                        Span::styled(format!("{label}: "), theme::hint_style()),
                        Span::styled(value.clone(), theme::panel_style()),
                    ]));
                }
                ls
            }
        };
        f.render_widget(
            Paragraph::new(lines).wrap(Wrap { trim: true }).style(theme::panel_style()),
            inner,
        );
    }

    fn render_panel_list(&self, f: &mut Frame, area: Rect, panel: Panel) {
        let items: Vec<ListItem> = if self.rows.is_empty() {
            vec![ListItem::new(Line::from(Span::styled("(empty)", theme::hint_style())))]
        } else {
            self.rows.iter().map(|r| ListItem::new(r.text.clone())).collect()
        };
        // A path-scoped panel (perms) shows its resolver cluster base; otherwise just the name.
        // (The services panel renders elsewhere, via the shared units view.)
        let title = match &self.panel_path {
            Some(p) => format!(" {} @ {p} ", panel.title()),
            None => format!(" {} ", panel.title()),
        };
        let mut st = self.list;
        let list = List::new(items)
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(title, theme::title_style())))
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, area, &mut st);
    }

    /// The detail pane under a glyph-bearing list: the selected row's glyph
    /// identicon (left) beside its summary + grouped hash (right) — the request
    /// code (queue / delegations) or the certificate's per-key glyph (issued
    /// certs). Laid out horizontally so the identicon never clips the hash.
    fn render_panel_detail(&self, f: &mut Frame, area: Rect, panel: Panel) {
        let title = if matches!(panel, Panel::Revocation) {
            " Certificate "
        } else {
            " Request "
        };
        let block = theme::panel_block().title(Span::styled(title, theme::title_style()));
        let inner = block.inner(area);
        f.render_widget(block, area);
        let row = self.list.selected().and_then(|i| self.rows.get(i));
        let glyph = row.and_then(|r| match &r.key {
            RowKey::Code(code) => Fingerprint::parse_text(code).ok(),
            RowKey::Cert { glyph, .. } => glyph.clone(),
            RowKey::None | RowKey::Name(_) | RowKey::Server { .. } => None,
        });
        let cols = Layout::horizontal([
            Constraint::Length(widgets::IDENTICON_WIDTH),
            Constraint::Min(20),
        ])
        .split(inner);
        // Left: the identicon (trim:false — its leading "off" cells are spaces).
        if let Some(fp) = &glyph {
            f.render_widget(
                Paragraph::new(widgets::identicon_lines(fp))
                    .wrap(Wrap { trim: false })
                    .style(theme::panel_style()),
                cols[0],
            );
        }
        // Right: the request summary, then its grouped hash (or why there's none).
        let mut right: Vec<Line> = match row {
            None => vec![Line::from(Span::styled(
                "(no request selected)",
                theme::hint_style(),
            ))],
            Some(row) => {
                vec![Line::from(Span::styled(row.text.clone(), theme::panel_style()))]
            }
        };
        if row.is_some() {
            right.push(Line::from(""));
            match &glyph {
                Some(fp) => {
                    for chunk in widgets::group_fingerprint(fp) {
                        right.push(Line::from(Span::styled(chunk, theme::title_style())));
                    }
                }
                None => right.push(Line::from(Span::styled(
                    if matches!(panel, Panel::Revocation) {
                        "(no glyph — legacy directly-issued certificate)"
                    } else {
                        "(no glyph for this request)"
                    },
                    theme::hint_style(),
                ))),
            }
            if let Some(row) = row
                && !row.detail.is_empty()
            {
                right.push(Line::from(""));
                for (label, value) in &row.detail {
                    right.push(Line::from(vec![
                        Span::styled(format!("{label}: "), theme::hint_style()),
                        Span::styled(value.clone(), theme::panel_style()),
                    ]));
                }
            }
        }
        f.render_widget(
            Paragraph::new(right).wrap(Wrap { trim: true }).style(theme::panel_style()),
            cols[1],
        );
    }
}

/// Render a single-field form (connect / path prompt) as a themed panel filling
/// `area`, with the value in a focused white field and the terminal cursor at
/// its end.
pub(super) fn render_form(
    f: &mut Frame,
    area: Rect,
    title: &str,
    help: &[&str],
    value: &str,
    error: Option<&str>,
    hint: &str,
) {
    let block = theme::panel_block()
        .title(Span::styled(format!(" {title} "), theme::title_style()));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let rows = Layout::vertical([
        Constraint::Length(help.len() as u16),
        Constraint::Length(1), // spacer
        Constraint::Length(1), // field
        Constraint::Length(1), // error
        Constraint::Length(1), // spacer
        Constraint::Length(1), // hint
        Constraint::Min(0),
    ])
    .split(inner);
    let help_lines: Vec<Line> = help
        .iter()
        .map(|h| Line::from(Span::styled(h.to_string(), theme::hint_style())))
        .collect();
    f.render_widget(Paragraph::new(help_lines).style(theme::hint_style()), rows[0]);
    let fw = inner.width.saturating_sub(2).clamp(1, 48);
    let field_row = Rect { x: rows[2].x, y: rows[2].y, width: fw, height: 1 };
    f.render_widget(
        Paragraph::new(value.to_string()).style(theme::field_style()),
        field_row,
    );
    if let Some(e) = error {
        let err = Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT);
        f.render_widget(Paragraph::new(e.to_string()).style(err), rows[3]);
    }
    f.render_widget(Paragraph::new(hint.to_string()).style(theme::hint_style()), rows[5]);
    let cx = field_row.x
        + (value.chars().count() as u16).min(field_row.width.saturating_sub(1));
    f.set_cursor_position((cx, field_row.y));
}

/// The conventional admin-server port, pre-filled in the manual-connect form.
const DEFAULT_ADMIN_PORT: u16 = 4565;

/// Pre-filled in the gate-duration form. Long enough to be a plausible answer,
/// short enough that accepting it blindly is not the dangerous choice, and it
/// shows the format without needing to explain it.
const DEFAULT_GATE_DURATION: &str = "1h";

/// A typed duration into the absolute deadline the config stores. Duration in,
/// instant out: the deadline is what survives a restart, but nobody wants to
/// type a timestamp.
fn parse_gate_duration(text: &str) -> Result<ReadGate, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("enter a duration, such as 30m or 2h".to_string());
    }
    let d: std::time::Duration =
        text.parse::<humantime::Duration>().map_err(|e| format!("{e}"))?.into();
    let d = chrono::Duration::from_std(d).map_err(|_| "that is too long".to_string())?;
    Ok(ReadGate::Until(chrono::Utc::now() + d))
}

/// Render a `label: [ value ]` form row (value masked when `secret`). Returns the
/// cursor position when `focused` so the caller can place the terminal cursor.
fn labeled_field(
    f: &mut Frame,
    area: Rect,
    label: &str,
    value: &str,
    secret: bool,
    focused: bool,
) -> Option<(u16, u16)> {
    let cols =
        Layout::horizontal([Constraint::Length(10), Constraint::Min(0)]).split(area);
    let label_style = if focused {
        Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT)
    } else {
        theme::hint_style()
    };
    f.render_widget(Paragraph::new(format!("{label}:")).style(label_style), cols[0]);
    let shown =
        if secret { "•".repeat(value.chars().count()) } else { value.to_string() };
    let fw = cols[1].width.clamp(1, 42);
    let fr = Rect { x: cols[1].x, y: cols[1].y, width: fw, height: 1 };
    f.render_widget(Paragraph::new(shown).style(theme::field_style()), fr);
    focused.then(|| {
        let cx = fr.x + (value.chars().count() as u16).min(fr.width.saturating_sub(1));
        (cx, fr.y)
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    fn draw(state: &mut RemoteState, w: u16, h: u16) -> Terminal<TestBackend> {
        let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
        terminal
            .draw(|f| {
                let area = f.area();
                state.render(f, area);
            })
            .unwrap();
        terminal
    }

    /// Render a `RemoteState` into a test terminal and flatten the buffer to text.
    fn render(state: &mut RemoteState, w: u16, h: u16) -> String {
        draw(state, w, h)
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect()
    }

    /// A `RemoteState` on the AdminDomains screen with injected admin domains + poll state
    /// (bypassing the on-disk registry so the test is deterministic).
    fn clusters_state(
        clusters: Vec<KnownAdminDomain>,
        poll: Vec<PollState>,
    ) -> RemoteState {
        let mut s = RemoteState::new();
        s.domains = clusters;
        s.poll = poll;
        s.screen = Screen::AdminDomains;
        s
    }

    fn cluster(domain: &str, addr: &str, seed: &[u8]) -> (KnownAdminDomain, SocketAddr) {
        let addr: SocketAddr = addr.parse().unwrap();
        let fp = Fingerprint::of_der(seed);
        (
            KnownAdminDomain {
                domain: domain.to_string(),
                fingerprint: fp.text(),
                addrs: vec![addr],
            },
            addr,
        )
    }

    #[test]
    fn empty_cluster_list_shows_hint() {
        let mut s = clusters_state(vec![], vec![]);
        let out = render(&mut s, 100, 20);
        assert!(out.contains("No saved admin domains"), "empty hint missing: {out:?}");
    }

    #[test]
    fn panel_titles_are_title_case() {
        assert_eq!(Panel::Queue.title(), "Enrollment Queue");
        assert_eq!(Panel::Delegations.title(), "Delegation Requests");
        assert_eq!(Panel::Roster.title(), "Admin Roster");
        assert_eq!(Panel::Servers.title(), "Admin Servers");
        assert_eq!(Panel::Revocation.title(), "Issued Certificates");
    }

    #[test]
    fn gutter_none_at_cluster_list_some_when_drilled() {
        // The admin domain list is the tab landing (tab bar + global gutter).
        let mut s = clusters_state(vec![], vec![]);
        assert!(s.gutter().is_none(), "admin domain list should not be drilled in");
        // Drilling into a resolver cluster picker (a tool) owns the gutter.
        s.apply(RemoteUpdate::ResolverClusters {
            panel: Panel::Perms,
            bases: vec!["/".to_string()],
        });
        assert!(s.gutter().is_some(), "a drilled-in screen should provide gutter keys");
    }

    #[test]
    fn level_pick_lists_cluster_levels() {
        let mut s = RemoteState::new();
        s.apply(RemoteUpdate::ResolverClusters {
            panel: Panel::Perms,
            bases: vec!["/".to_string(), "/eu".to_string()],
        });
        let out = render(&mut s, 100, 20);
        assert!(out.contains("pick a resolver cluster"), "picker title missing: {out:?}");
        assert!(out.contains("/eu"), "resolver cluster base missing: {out:?}");
    }

    #[test]
    fn present_cluster_shows_domain_and_glyph() {
        let (c, addr) = cluster("hq.local", "10.0.0.1:4565", b"hq CA spki");
        let mut s = clusters_state(vec![c], vec![PollState::Present { addr }]);
        let terminal = draw(&mut s, 100, 20);
        assert_eq!(
            widgets::rendered_identicon_rows(terminal.backend().buffer()),
            widgets::IDENTICON_HEIGHT as usize
        );
        let out = render(&mut s, 100, 20);
        assert!(out.contains("hq.local"), "domain missing: {out:?}");
        assert!(out.contains("CA glyph"), "glyph label missing: {out:?}");
    }

    #[test]
    fn unverified_cluster_is_hidden() {
        // A saved admin domain whose identity did not verify (Absent) never shows —
        // the address may now be a different CA on this admin domain.
        let (c, _) = cluster("hq.local", "10.0.0.1:4565", b"hq CA spki");
        let mut s = clusters_state(vec![c], vec![PollState::Absent]);
        let out = render(&mut s, 100, 20);
        assert!(
            !out.contains("hq.local"),
            "unverified admin domain leaked into the list: {out:?}"
        );
        assert!(
            out.contains("No saved admin domain is reachable"),
            "absent hint missing: {out:?}"
        );
    }

    #[test]
    fn manual_form_shows_host_and_port() {
        let mut s = RemoteState::new();
        s.screen = Screen::Manual {
            host: "10.0.0.9".to_string(),
            port: "4565".to_string(),
            focus: ManualFocus::Host,
        };
        let out = render(&mut s, 100, 20);
        assert!(out.contains("Host"), "host field missing: {out:?}");
        assert!(out.contains("10.0.0.9"), "host value missing: {out:?}");
    }

    #[test]
    fn queue_panel_shows_request_detail_with_glyph() {
        let mut s = RemoteState::new();
        let fp = Fingerprint::of_der(b"a fake enrollment spki");
        s.screen = Screen::Panel(Panel::Queue);
        s.rows = vec![PanelRow {
            text: "admin-server enrollment @ 10.0.60.11:4565".to_string(),
            key: RowKey::Code(fp.text()),
            detail: vec![
                ("Listen".to_string(), "10.0.60.11:4565".to_string()),
                ("Roles".to_string(), "[Resolver, IdMap]".to_string()),
                ("Resolver cluster".to_string(), "create at /eu".to_string()),
                ("Resolver members".to_string(), "10.0.60.11:4564 Tls".to_string()),
            ],
        }];
        s.list.select(Some(0));
        let terminal = draw(&mut s, 80, 24);
        assert_eq!(
            widgets::rendered_identicon_rows(terminal.backend().buffer()),
            widgets::IDENTICON_HEIGHT as usize
        );
        let out = render(&mut s, 100, 30);
        assert!(out.contains("Enrollment Queue"), "queue title missing: {out:?}");
        assert!(
            out.contains("admin-server enrollment"),
            "request summary missing: {out:?}"
        );
        assert!(out.contains("Request"), "detail-pane title missing: {out:?}");
        // The detail pane shows the request's grouped hash; its first chunk is a
        // slice of the fingerprint text.
        let chunk = widgets::group_fingerprint(&fp).remove(0);
        assert!(out.contains(chunk.as_str()), "glyph hash missing from detail: {out:?}");
        assert!(out.contains("Roles"), "requested roles missing from detail: {out:?}");
        assert!(
            out.contains("Resolver, IdMap"),
            "role values missing from detail: {out:?}"
        );
        assert!(out.contains("/eu"), "admin domain base missing from detail: {out:?}");
        assert!(out.contains("10.0.60.11:4564"), "resolver members missing: {out:?}");
    }

    #[test]
    fn roster_panel_spells_out_policy_in_a_detail_pane() {
        // The roster is a two-pane view: a plain admin list on top, the selected
        // admin's authorities spelled out below — not crammed into the list line.
        let mut s = RemoteState::new();
        s.screen = Screen::Panel(Panel::Roster);
        s.rows = vec![PanelRow {
            text: "eu-ops               [role]".to_string(),
            key: RowKey::Name("eu-ops".to_string()),
            detail: vec![
                ("May issue certs for (SAN)".to_string(), "*.eu.ryu-oh.org".to_string()),
                ("Manage other admins".to_string(), "no".to_string()),
                ("Control services under".to_string(), "/eu".to_string()),
            ],
        }];
        s.list.select(Some(0));
        let out = render(&mut s, 100, 30);
        assert!(out.contains("Admin Roster"), "roster title missing: {out:?}");
        assert!(out.contains("Authority"), "detail-pane title missing: {out:?}");
        assert!(out.contains("May issue certs for"), "policy label missing: {out:?}");
        assert!(out.contains("*.eu.ryu-oh.org"), "policy value missing: {out:?}");
        assert!(
            out.contains("Control services under"),
            "service-scope label missing: {out:?}"
        );
    }

    /// The Servers panel with two rows: the CA, open, and a satellite still
    /// inside a 45-minute join gate.
    fn a_servers_panel() -> RemoteState {
        let ca = AdminServerId::new();
        let satellite = AdminServerId::new();
        let mut s = RemoteState::new();
        s.target = Some(PanelTarget::Remote(a_conn("10.0.0.1:4565")));
        s.screen = Screen::Panel(Panel::Servers);
        s.rows = vec![
            PanelRow {
                text: format!("/  {ca}  10.0.0.1:4565  [CA]"),
                key: RowKey::Server {
                    id: ca,
                    addr: "10.0.0.1:4565".parse().unwrap(),
                    cluster: "/".to_string(),
                    ca: true,
                    resolver: true,
                    gate: Some(ReadGate::No),
                },
                detail: vec![(
                    "Removal".to_string(),
                    "protected — replace the CA first".to_string(),
                )],
            },
            PanelRow {
                text: format!("/eu  {satellite}  10.0.60.11:4565  [Registered]"),
                key: RowKey::Server {
                    id: satellite,
                    addr: "10.0.60.11:4565".parse().unwrap(),
                    cluster: "/eu".to_string(),
                    ca: false,
                    resolver: true,
                    gate: Some(ReadGate::Until(
                        chrono::Utc::now() + chrono::Duration::minutes(45),
                    )),
                },
                detail: vec![("Roles".to_string(), "resolver".to_string())],
            },
        ];
        s
    }

    /// The satellite's id, read back out of the fixture's second row.
    fn satellite_of(s: &RemoteState) -> AdminServerId {
        match &s.rows[1].key {
            RowKey::Server { id, .. } => *id,
            _ => panic!("expected a server row"),
        }
    }

    #[test]
    fn server_panel_lists_identity_and_protects_ca() {
        let mut s = a_servers_panel();
        let satellite = satellite_of(&s);
        s.list.select(Some(0));
        assert!(matches!(
            s.on_key(KeyCode::Char('c')),
            Some(Action::Remote(RemoteAction::ReconcileCa { .. }))
        ));
        assert!(
            s.on_key(KeyCode::Char('x')).is_none(),
            "the CA row must never produce a remove action"
        );
        s.list.select(Some(1));
        let Some(Action::Remote(action @ RemoteAction::RemoveServer { server, .. })) =
            s.on_key(KeyCode::Char('x'))
        else {
            panic!("satellite row did not produce a remove action")
        };
        assert_eq!(server, satellite);
        let confirm = action.confirm_message().unwrap();
        assert!(confirm.contains(&satellite.to_string()));
        assert!(confirm.contains("No service will be restarted"));
        let out = render(&mut s, 120, 30);
        assert!(out.contains("Admin Servers"), "server title missing: {out:?}");
        assert!(out.contains("Server identity"), "detail title missing: {out:?}");
        assert!(out.contains("/eu"), "admin domain grouping missing: {out:?}");
    }

    /// The CA is gateable — unlike force-remove, taking a member out of
    /// service for readers is ordinary — and both directions are confirmed,
    /// because both have a way of going wrong.
    #[test]
    fn the_gate_chooser_offers_open_shut_and_a_deadline() {
        let mut s = a_servers_panel();
        // The CA row. Force-remove refuses it; the gate does not.
        s.list.select(Some(0));
        assert!(s.on_key(KeyCode::Char('x')).is_none());
        assert!(s.on_key(KeyCode::Char('g')).is_none(), "g opens a chooser, not an op");
        let out = render(&mut s, 120, 30);
        for label in GATE_CHOICES.iter().map(|(l, _)| *l) {
            assert!(out.contains(label), "choice {label:?} missing: {out:?}");
        }
        assert!(!out.contains("Yes"), "a gate is never offered as yes/no: {out:?}");
        // Shut, the second choice.
        s.on_key(KeyCode::Down);
        let Some(Action::Remote(action)) = s.on_key(KeyCode::Enter) else {
            panic!("choosing Shut did not produce an action")
        };
        assert!(matches!(action, RemoteAction::SetReadGate { gate: ReadGate::Yes, .. }));
        let confirm = action.confirm_message().expect("shutting must be confirmed");
        assert!(confirm.contains("Publishers keep writing"), "{confirm:?}");
        assert!(matches!(s.screen, Screen::Panel(Panel::Servers)));
    }

    #[test]
    fn a_deadline_is_typed_as_a_duration_prefilled_and_echoed_absolutely() {
        let mut s = a_servers_panel();
        s.list.select(Some(0));
        s.on_key(KeyCode::Char('g'));
        s.on_key(KeyCode::Down);
        s.on_key(KeyCode::Down);
        assert!(s.on_key(KeyCode::Enter).is_none(), "a deadline needs a follow-up");
        assert!(s.capturing_text(), "the duration form must capture keystrokes");
        let out = render(&mut s, 120, 30);
        assert!(out.contains(DEFAULT_GATE_DURATION), "no prefilled duration: {out:?}");
        assert!(out.contains("30m"), "the format is not shown: {out:?}");
        // Nonsense is refused in place rather than sent.
        for _ in 0..DEFAULT_GATE_DURATION.len() {
            s.on_key(KeyCode::Backspace);
        }
        for c in "soon".chars() {
            s.on_key(KeyCode::Char(c));
        }
        assert!(s.on_key(KeyCode::Enter).is_none(), "\"soon\" is not a duration");
        assert!(s.error.is_some(), "a bad duration must say so");
        for _ in 0..4 {
            s.on_key(KeyCode::Backspace);
        }
        for c in "90m".chars() {
            s.on_key(KeyCode::Char(c));
        }
        let Some(Action::Remote(action)) = s.on_key(KeyCode::Enter) else {
            panic!("a valid duration did not produce an action")
        };
        let RemoteAction::SetReadGate { gate: ReadGate::Until(t), .. } = action else {
            panic!("expected a deadline gate")
        };
        let left = t - chrono::Utc::now();
        assert!(
            left > chrono::Duration::minutes(89) && left <= chrono::Duration::minutes(90),
            "a duration must become that far in the future, got {left}"
        );
    }

    /// Opening a gate early is the quieter mistake — the member answers, from a
    /// namespace publishers have not finished rebuilding — so it is confirmed
    /// too, and the confirmation says how much time is being cut short.
    #[test]
    fn opening_a_live_deadline_early_is_confirmed_and_says_how_early() {
        let mut s = a_servers_panel();
        s.list.select(Some(1)); // the satellite, gated for another 45 minutes
        s.on_key(KeyCode::Char('g'));
        let Some(Action::Remote(action)) = s.on_key(KeyCode::Enter) else {
            panic!("choosing Open did not produce an action")
        };
        assert!(matches!(action, RemoteAction::SetReadGate { gate: ReadGate::No, .. }));
        let confirm = action.confirm_message().expect("opening early must be confirmed");
        assert!(confirm.contains("44m") || confirm.contains("45m"), "{confirm:?}");
        assert!(confirm.contains("early"), "{confirm:?}");
        assert!(confirm.contains("look absent"), "{confirm:?}");
    }

    #[test]
    fn a_server_with_no_resolver_has_no_gate_to_set() {
        let mut s = a_servers_panel();
        let RowKey::Server { resolver, gate, .. } = &mut s.rows[0].key else {
            panic!("expected a server row")
        };
        (*resolver, *gate) = (false, None);
        s.list.select(Some(0));
        assert!(s.on_key(KeyCode::Char('g')).is_none());
        assert!(
            matches!(s.screen, Screen::Panel(Panel::Servers)),
            "an ungateable row must not open the chooser"
        );
        assert!(s.error.as_deref().is_some_and(|e| e.contains("no resolver")));
    }

    fn a_conn(server: &str) -> RemoteConn {
        RemoteConn {
            server: server.parse().unwrap(),
            domain: "example.com".to_string(),
            confirmed_fp: Fingerprint::of_der(b"CA"),
            admin: "eric".to_string(),
        }
    }

    #[test]
    fn server_pick_lists_admin_servers_with_level() {
        let mut s = RemoteState::new();
        let mut state = ListState::default();
        state.select(Some(0));
        let root = AdminServerId::new();
        let ap = AdminServerId::new();
        s.screen = Screen::ServerPick {
            admin_servers: vec![
                ServiceServerRow {
                    target: ServiceTarget {
                        id: root,
                        addr: "10.0.0.11:4565".parse().unwrap(),
                    },
                    label: format!("10.0.0.11:4565         / {root}"),
                },
                ServiceServerRow {
                    target: ServiceTarget {
                        id: ap,
                        addr: "10.0.60.11:4565".parse().unwrap(),
                    },
                    label: format!("10.0.60.11:4565        /ap {ap}"),
                },
            ],
            state,
        };
        let out = render(&mut s, 100, 20);
        assert!(out.contains("pick an admin server"), "picker title missing: {out:?}");
        assert!(out.contains("10.0.0.11:4565"), "first server missing: {out:?}");
        assert!(out.contains("/ap"), "base column missing: {out:?}");
        assert!(out.contains(&root.to_string()), "immutable ID missing: {out:?}");
    }

    #[test]
    fn server_pick_enter_targets_that_one_server() {
        let mut s = RemoteState::new();
        s.target = Some(PanelTarget::Remote(a_conn("10.0.0.1:4565")));
        let mut state = ListState::default();
        state.select(Some(0));
        let addr: SocketAddr = "10.0.60.11:4565".parse().unwrap();
        let id = AdminServerId::new();
        let selected = ServiceTarget { id, addr };
        s.screen = Screen::ServerPick {
            admin_servers: vec![ServiceServerRow {
                target: selected,
                label: format!("10.0.60.11:4565  /ap {id}"),
            }],
            state,
        };
        match s.on_key(KeyCode::Enter) {
            Some(Action::Remote(RemoteAction::ServiceControl {
                server,
                op,
                units,
                ..
            })) => {
                assert_eq!(server, selected, "must preserve the picked identity");
                assert!(
                    matches!(op, ServiceOp::Status),
                    "initial open is a status listing"
                );
                assert!(units.is_empty(), "initial listing carries no units");
            }
            _ => panic!("expected a ServiceControl action for the picked server"),
        }
        // The picked server is remembered for the panel's control keys.
        assert_eq!(s.service_target, Some(selected));
    }

    #[test]
    fn duplicate_or_reused_addresses_cannot_change_the_selected_identity() {
        let mut s = RemoteState::new();
        s.target = Some(PanelTarget::Remote(a_conn("10.0.0.1:4565")));
        let addr = "10.0.60.11:4565".parse().unwrap();
        let first = ServiceTarget { id: AdminServerId::new(), addr };
        let selected = ServiceTarget { id: AdminServerId::new(), addr };
        let mut state = ListState::default();
        state.select(Some(1));
        s.screen = Screen::ServerPick {
            admin_servers: vec![
                ServiceServerRow {
                    target: first,
                    label: format!("{addr} /ap {}", first.id),
                },
                ServiceServerRow {
                    target: selected,
                    label: format!("{addr} /ap {}", selected.id),
                },
            ],
            state,
        };

        let Some(Action::Remote(RemoteAction::ServiceControl { server, .. })) =
            s.on_key(KeyCode::Enter)
        else {
            panic!("expected service-control action")
        };
        assert_eq!(server, selected);
        assert_ne!(server.id, first.id);
    }

    #[test]
    fn remote_services_render_like_local_with_definition() {
        // The remote services panel shares the Local Services surface's renderer:
        // a unit list plus the selected unit's Status and Definition, the latter
        // populated from the member-supplied definition over the wire. Selecting a
        // unit + 's' targets exactly that unit on the picked server.
        use netidx_activation::control::UnitState;
        use netidx_admin_proto::{ServiceUnit, ServiceUnitDef};
        let mut s = RemoteState::new();
        s.target = Some(PanelTarget::Remote(a_conn("10.0.0.1:4565")));
        let service_target = ServiceTarget {
            id: AdminServerId::new(),
            addr: "10.0.60.11:4565".parse().unwrap(),
        };
        s.service_target = Some(service_target);
        let su = ServiceUnit {
            unit: "resolver".to_string(),
            state: UnitState::Running { pid: Some(42) },
            definition: Some(ServiceUnitDef {
                exe: "/usr/bin/netidx".to_string(),
                args: vec!["resolver-server".to_string()],
                trigger: "OnStart".to_string(),
                restart: "rate-limited (1s)".to_string(),
            }),
        };
        s.apply(RemoteUpdate::ServiceRows {
            rows: vec![super::super::services::ServiceRow::from_service_unit(&su)],
        });
        let out = render(&mut s, 110, 20);
        assert!(
            out.contains("Services @ 10.0.60.11:4565"),
            "server-scoped title missing: {out:?}"
        );
        assert!(out.contains("resolver"), "unit name missing: {out:?}");
        assert!(
            out.contains("status:") && out.contains("running"),
            "status pane missing: {out:?}"
        );
        assert!(out.contains("42"), "pid missing: {out:?}");
        assert!(out.contains("/usr/bin/netidx"), "definition exe missing: {out:?}");
        match s.on_key(KeyCode::Char('s')) {
            Some(Action::Remote(RemoteAction::ServiceControl {
                server,
                units,
                op,
                ..
            })) => {
                assert_eq!(server, service_target);
                assert!(matches!(op, ServiceOp::Start), "expected Start");
                assert_eq!(
                    units,
                    vec!["resolver".to_string()],
                    "must target the selected unit"
                );
            }
            _ => panic!("expected a ServiceControl Start for the selected unit"),
        }
    }
}
