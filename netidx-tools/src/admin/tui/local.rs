//! Tab 1 — **Local**: detect this machine's netidx install and show its status,
//! or, on a fresh machine, offer the role menu that starts an install.
//!
//! Read-only detection today; the install / uninstall / renew actions land once
//! the [`TuiAnswerer`](super::answer::TuiAnswerer) exists.

use super::{action::Action, widgets};
use netidx_admin::{
    fingerprint::Fingerprint,
    paths,
    provenance::{InstallRecord, InstallRole},
    service::{self, ServiceParams, ServiceScope, ServiceStatus},
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use std::path::PathBuf;

/// A role the operator can install on a fresh machine.
#[derive(Clone, Copy)]
struct RoleChoice {
    role: InstallRole,
    title: &'static str,
    blurb: &'static str,
}

const ROLES: [RoleChoice; 3] = [
    RoleChoice {
        role: InstallRole::Workstation,
        title: "Workstation",
        blurb: "Turn this machine into a full netidx node: a local resolver serving a \
                /local namespace plus a matching client. Join an existing network to \
                enroll a TLS identity, or run standalone. The usual choice for a laptop \
                or desktop.",
    },
    RoleChoice {
        role: InstallRole::Resolver,
        title: "Resolver",
        blurb: "A network-facing resolver server — the directory that maps paths to \
                publishers for a whole network or a delegated subtree. Can mint a new \
                network's certificate authority and admin server, or enroll under an \
                existing one.",
    },
    RoleChoice {
        role: InstallRole::Publisher,
        title: "Publisher",
        blurb: "A client configuration for a host that publishes data: point it at a \
                network's resolvers with the right auth. Installs a certificate-renewal \
                service when the network uses TLS.",
    },
];

/// One detected install on this machine.
struct Detected {
    record: InstallRecord,
    service: ServiceStatus,
    /// Where the config + provenance record live (`~/.config` = user, `/etc` =
    /// system) — the scope a teardown targets.
    scope: ServiceScope,
    config_dir: PathBuf,
    ca: Option<Fingerprint>,
}

impl Detected {
    fn probe(record: InstallRecord, scope: ServiceScope, config_dir: PathBuf) -> Detected {
        let ca = record
            .network
            .as_ref()
            .and_then(|n| Fingerprint::parse_text(&n.ca_fingerprint).ok());
        let service = probe_service(&record);
        Detected { record, service, scope, config_dir, ca }
    }
}

/// Network-sync state for a detected install, filled in asynchronously by a
/// background check (the same reconcile the CLI `status`/`update` runs). Kept
/// out of [`Detected`] because probing it is a network round-trip, while
/// `Detected` is built synchronously from local files.
#[derive(Clone)]
pub(super) enum SyncState {
    /// A networked install not yet checked; the event loop launches a check.
    Unchecked,
    /// A background check is in flight.
    Checking,
    /// The config already matches the network — nothing to apply.
    InSync,
    /// The network has member servers this config lacks; each string is one
    /// pending addition (e.g. `client resolver 192.168.50.12:4564 (tls)`).
    OutOfSync(Vec<String>),
    /// The check couldn't reach the network; carries the error for display.
    Failed(String),
}

/// The OS-service state for a role. Resolver / publisher register a system-scope
/// `netidx@<user>` service even when their config is user-scope; a workstation
/// uses a user-scope service. Probe the scope that matches.
fn probe_service(record: &InstallRecord) -> ServiceStatus {
    let (scope, for_user) = match record.role {
        InstallRole::Workstation => (ServiceScope::User, None),
        InstallRole::Resolver | InstallRole::Publisher => {
            (ServiceScope::System, super::super::service::resolve_for_user(None).ok())
        }
    };
    let params = ServiceParams {
        scope,
        for_user,
        binary: std::path::PathBuf::new(),
        service_name: ServiceParams::DEFAULT_NAME.to_string(),
        activation_dir: None,
    };
    service::status(&params).unwrap_or(ServiceStatus::NotInstalled)
}

/// Detect every install recorded on this machine (user- and system-scope).
fn detect() -> Vec<Detected> {
    let mut out = Vec::new();
    let user_path = paths::user_install_record().ok();
    if let Ok(Some(record)) = InstallRecord::load_default() {
        let dir = paths::user_config_root().unwrap_or_default();
        out.push(Detected::probe(record, ServiceScope::User, dir));
    }
    let sys_path = paths::system_install_record();
    // Skip the system record if it's the very same file we already read as the
    // user record (unusual, but possible if the two roots coincide).
    if sys_path.exists() && user_path.as_deref() != Some(sys_path.as_path()) {
        if let Ok(record) = InstallRecord::load(&sys_path) {
            out.push(Detected::probe(record, ServiceScope::System, paths::system_config_root()));
        }
    }
    out
}

/// A context menu of the actions available for the selected install — the
/// role's `netidx admin <template> <subcommand>` surface.
struct ActionMenu {
    title: String,
    items: Vec<(String, Action)>,
    state: ListState,
}

/// Tab-1 state: the detected installs plus the role-menu cursor for the
/// fresh-machine case.
pub(super) struct LocalState {
    installs: Vec<Detected>,
    /// Per-install network-sync state, parallel to `installs` — filled in by a
    /// background check so the status card can render instantly from local
    /// files and gain the sync line once the network answers.
    sync: Vec<SyncState>,
    role_menu: ListState,
    /// Which detected install the lifecycle actions apply to.
    selected: usize,
    /// The open action menu for the selected install, if any.
    menu: Option<ActionMenu>,
}

impl LocalState {
    pub(super) fn new() -> LocalState {
        let mut role_menu = ListState::default();
        role_menu.select(Some(0));
        let installs = detect();
        let sync = vec![SyncState::Unchecked; installs.len()];
        LocalState { installs, sync, role_menu, selected: 0, menu: None }
    }

    /// Re-run detection (after an install/uninstall completes). Resets the sync
    /// state so the loop re-checks against the network.
    pub(super) fn refresh(&mut self) {
        self.installs = detect();
        self.sync = vec![SyncState::Unchecked; self.installs.len()];
        self.selected = self.selected.min(self.installs.len().saturating_sub(1));
        self.menu = None;
    }

    /// Networked installs whose sync hasn't been checked yet; marks each
    /// `Checking` so the event loop launches exactly one background check per
    /// install. Standalone (local-only) installs are never checked.
    pub(super) fn take_pending_checks(&mut self) -> Vec<(usize, InstallRole)> {
        let mut out = Vec::new();
        for i in 0..self.installs.len() {
            if self.installs[i].record.network.is_some()
                && matches!(self.sync[i], SyncState::Unchecked)
            {
                self.sync[i] = SyncState::Checking;
                out.push((i, self.installs[i].record.role));
            }
        }
        out
    }

    /// Fold in a finished background check's results.
    pub(super) fn apply_sync(&mut self, results: Vec<(usize, SyncState)>) {
        for (i, st) in results {
            if let Some(slot) = self.sync.get_mut(i) {
                *slot = st;
            }
        }
    }

    pub(super) fn on_key(&mut self, code: crossterm::event::KeyCode) -> Option<Action> {
        use crossterm::event::KeyCode::*;
        if self.installs.is_empty() {
            match code {
                Up | Char('k') => self.role_menu.select_previous(),
                Down | Char('j') => self.role_menu.select_next(),
                Enter => {
                    let role = ROLES[self.role_menu.selected().unwrap_or(0)].role;
                    return Some(Action::Install { role, dry_run: false });
                }
                Char('p') => {
                    let role = ROLES[self.role_menu.selected().unwrap_or(0)].role;
                    return Some(Action::Install { role, dry_run: true });
                }
                _ => {}
            }
            return None;
        }
        // With the action menu open, keys drive it.
        if let Some(menu) = &mut self.menu {
            match code {
                Up | Char('k') => menu.state.select_previous(),
                Down | Char('j') => menu.state.select_next(),
                Esc => self.menu = None,
                Enter => {
                    let mut menu = self.menu.take().unwrap();
                    let sel = menu.state.selected().unwrap_or(0).min(menu.items.len().saturating_sub(1));
                    return Some(menu.items.remove(sel).1);
                }
                _ => {}
            }
            return None;
        }
        match code {
            Up | Char('k') if self.selected > 0 => self.selected -= 1,
            Down | Char('j') if self.selected + 1 < self.installs.len() => self.selected += 1,
            // Enter opens the full action menu for the selected install.
            Enter => self.menu = Some(action_menu(&self.installs[self.selected])),
            // Quick shortcuts (also in the menu).
            Char('u') => return Some(uninstall_action(&self.installs[self.selected], false)),
            // Uppercase U applies the network sync (the Update action) — mnemonic
            // and distinct from lowercase `u` (uninstall). Only meaningful for a
            // networked install; the status card surfaces it when out of sync.
            Char('U') => {
                let d = &self.installs[self.selected];
                if d.record.network.is_some() {
                    return Some(Action::Update { role: d.record.role });
                }
            }
            Char('r') => {
                let d = &self.installs[self.selected];
                if d.record.network.is_some() {
                    return Some(Action::Renew { server: d.record.admin_server });
                }
            }
            _ => {}
        }
        None
    }

    pub(super) fn render(&mut self, f: &mut Frame, area: Rect) {
        if self.installs.is_empty() {
            self.render_role_menu(f, area);
        } else {
            self.render_installs(f, area);
        }
        if let Some(menu) = &self.menu {
            render_menu(f, area, menu);
        }
    }

    fn render_role_menu(&mut self, f: &mut Frame, area: Rect) {
        let cols =
            Layout::horizontal([Constraint::Length(24), Constraint::Min(0)]).split(area);
        let items: Vec<ListItem> = ROLES.iter().map(|r| ListItem::new(r.title)).collect();
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Install a role ")
                    .title_bottom(Line::from(" ↑/↓ select · Enter install · p preview ").dim()),
            )
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, cols[0], &mut self.role_menu);

        let sel = self.role_menu.selected().unwrap_or(0);
        let blurb = Paragraph::new(ROLES[sel].blurb).wrap(Wrap { trim: true }).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} ", ROLES[sel].title)),
        );
        f.render_widget(blurb, cols[1]);
    }

    fn render_installs(&self, f: &mut Frame, area: Rect) {
        let n = self.installs.len().max(1) as u32;
        let rows = Layout::vertical(vec![Constraint::Ratio(1, n); self.installs.len()])
            .split(area);
        for (i, (d, cell)) in self.installs.iter().zip(rows.iter()).enumerate() {
            render_install(f, d, &self.sync[i], *cell, i == self.selected);
        }
    }
}

/// Build the action menu for a detected install — its role's post-install
/// subcommand surface (`netidx admin <template> <subcommand>`).
fn action_menu(d: &Detected) -> ActionMenu {
    let role = d.record.role;
    let networked = d.record.network.is_some();
    let mut items: Vec<(String, Action)> = Vec::new();
    if networked {
        items.push(("Update — sync config with the network".to_string(), Action::Update { role }));
    }
    if role == InstallRole::Workstation && !networked {
        items.push(("Join a network".to_string(), Action::Join { dry_run: false }));
        items.push(("Preview join (dry run)".to_string(), Action::Join { dry_run: true }));
    }
    if role == InstallRole::Resolver {
        items.push(("Add a parent (delegate under)".to_string(), Action::AddParent));
        // Delegation requests land on the parent's *own* admin server, so offer
        // the review shortcut when this host runs one — not when `record.
        // admin_server` is set (that names the admin server this node enrolls
        // against, and is `None` on the CA host, which is exactly who reviews).
        if runs_local_admin_server() {
            items.push((
                "Review delegation requests".to_string(),
                Action::ReviewDelegations,
            ));
        }
    }
    if networked {
        items.push(("Renew certificates".to_string(), Action::Renew { server: d.record.admin_server }));
    }
    items.push(("Uninstall".to_string(), uninstall_action(d, false)));
    if owns_ca(d) {
        items.push((
            "Uninstall + destroy the CA".to_string(),
            uninstall_action(d, true),
        ));
    }
    let mut state = ListState::default();
    state.select(Some(0));
    ActionMenu { title: format!("{} actions", role_title(role)), items, state }
}

/// Whether this host runs its own admin server (so it can have a delegation
/// queue to review). Unix-only — remote admin is unix-only.
#[cfg(unix)]
fn runs_local_admin_server() -> bool {
    netidx_admin::admin_ops::local_admin_server_listen().is_some()
}

#[cfg(not(unix))]
fn runs_local_admin_server() -> bool {
    false
}

/// The uninstall action for a detected install (also the `u` shortcut).
/// `remove_ca` additionally deletes the CA directory (only offered when this
/// host actually holds one — see [`owns_ca`]).
fn uninstall_action(d: &Detected, remove_ca: bool) -> Action {
    // A resolver/publisher registers a system-scope service even with user-scope
    // config, so removing it needs root.
    let needs_root = d.scope == ServiceScope::System
        || matches!(d.record.role, InstallRole::Resolver | InstallRole::Publisher);
    Action::Uninstall {
        config_scope: d.scope,
        config_dir: d.config_dir.clone(),
        needs_root,
        remove_ca,
    }
}

/// Whether this host holds the network's CA (so a full teardown can offer to
/// destroy it). A plain enrolled node carries `network.ca_fingerprint` but no CA
/// directory, so key off the directory, not the record.
fn owns_ca(d: &Detected) -> bool {
    d.config_dir.join("ca").is_dir()
}

/// Render the action menu as a centered overlay.
fn render_menu(f: &mut Frame, screen: Rect, menu: &ActionMenu) {
    let items: Vec<ListItem> =
        menu.items.iter().map(|(label, _)| ListItem::new(label.clone())).collect();
    let h = (menu.items.len() as u16 + 3).min(screen.height.saturating_sub(2));
    let area = widgets::centered(56, h, screen);
    f.render_widget(Clear, area);
    let mut state = menu.state.clone();
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" {} ", menu.title))
                .title_bottom(Line::from(" ↑/↓ · Enter run · Esc close ").dim()),
        )
        .highlight_style(
            Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");
    f.render_stateful_widget(list, area, &mut state);
}

/// Render one detected install: a details column on the left and, when the host
/// joined a network, its CA identicon + fingerprint on the right. The selected
/// install is highlighted and shows its action keys.
fn render_install(f: &mut Frame, d: &Detected, sync: &SyncState, area: Rect, selected: bool) {
    let title = format!(" {} ", role_title(d.record.role));
    let mut block = Block::default().borders(Borders::ALL).title(title);
    if selected {
        // Surface the sync-apply shortcut in the hint only when there's
        // something to apply, so it doesn't clutter the in-sync case.
        let hint = if matches!(sync, SyncState::OutOfSync(_)) {
            " Enter actions · U sync now · u uninstall "
        } else {
            " Enter actions · u uninstall "
        };
        block = block
            .border_style(Style::default().fg(Color::Cyan))
            .title_bottom(Line::from(hint).dim());
    }
    let inner = block.inner(area);
    f.render_widget(block, area);

    let has_glyph = d.ca.is_some();
    let cols = if has_glyph {
        Layout::horizontal([Constraint::Min(0), Constraint::Length(20)]).split(inner)
    } else {
        Layout::horizontal([Constraint::Min(0)]).split(inner)
    };

    let mut lines = detail_lines(d);
    lines.extend(sync_lines(sync));
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), cols[0]);

    if let Some(fp) = &d.ca {
        let mut lines = vec![Line::from("CA glyph".dim())];
        lines.extend(widgets::identicon_lines(fp));
        f.render_widget(Paragraph::new(lines), cols[1]);
    }
}

fn detail_lines(d: &Detected) -> Vec<Line<'static>> {
    let r = &d.record;
    let mut lines = vec![
        kv("Role", r.role.as_str().to_string()),
        kv("Base", r.base.clone()),
        kv("Data-plane auth", r.auth.clone()),
    ];
    match &r.network {
        Some(net) => {
            lines.push(kv("Network", net.domain.clone()));
            lines.push(kv("CA fingerprint", net.ca_fingerprint.clone()));
        }
        None => lines.push(kv("Network", "standalone (local-only)".to_string())),
    }
    if let Some(addr) = r.admin_server {
        lines.push(kv("Admin server", addr.to_string()));
    }
    lines.push(service_line(d.service));
    lines.push(kv("Config", d.config_dir.display().to_string()));
    lines.push(kv("Installed", fmt_unix(r.created_unix)));
    lines
}

/// A `label: value` line with a dim label.
fn kv(label: &'static str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{label:>16}: "),
            Style::default().add_modifier(Modifier::DIM),
        ),
        Span::raw(value),
    ])
}

/// A `label: value` line whose value carries a status colour.
fn kv_status(label: &'static str, value: String, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{label:>16}: "),
            Style::default().add_modifier(Modifier::DIM),
        ),
        Span::styled(value, Style::default().fg(color)),
    ])
}

/// The status-card lines describing this install's network-sync state — the
/// TUI counterpart of the CLI `status` "in sync / out of sync" report.
fn sync_lines(sync: &SyncState) -> Vec<Line<'static>> {
    match sync {
        // A local-only install never gets here (never checked); render nothing.
        SyncState::Unchecked => Vec::new(),
        SyncState::Checking => {
            vec![kv_status("Network sync", "checking…".to_string(), Color::DarkGray)]
        }
        SyncState::InSync => {
            vec![kv_status("Network sync", "✓ in sync".to_string(), Color::Green)]
        }
        SyncState::Failed(e) => vec![kv_status(
            "Network sync",
            format!("could not check ({e})"),
            Color::DarkGray,
        )],
        SyncState::OutOfSync(changes) => {
            let mut lines = vec![kv_status(
                "Network sync",
                format!(
                    "⚠ {} new member server(s) — press U to apply",
                    changes.len()
                ),
                Color::Yellow,
            )];
            for c in changes {
                lines.push(Line::from(vec![
                    Span::raw(format!("{:>18}", "")),
                    Span::styled(
                        format!("+ {c}"),
                        Style::default().fg(Color::Yellow),
                    ),
                ]));
            }
            lines
        }
    }
}

/// Background network-sync check for the given installs — the quiet counterpart
/// of the Update action. Runs the same reconcile the CLI `status`/`update` does
/// (via [`super::lifecycle::update_plan`]) and maps each result to a
/// [`SyncState`]. Self-contained (owns its inputs, borrows no UI state) so the
/// event loop can poll it as a background future without a `spawn`.
pub(super) async fn check_sync(
    pending: Vec<(usize, InstallRole)>,
) -> Vec<(usize, SyncState)> {
    let mut out = Vec::with_capacity(pending.len());
    for (i, role) in pending {
        let st = match super::lifecycle::update_plan(role).await {
            Ok(plan) if plan.is_empty() => SyncState::InSync,
            Ok(plan) => SyncState::OutOfSync(
                plan.changes.iter().map(|c| c.text().to_string()).collect(),
            ),
            Err(e) => SyncState::Failed(format!("{e:#}")),
        };
        out.push((i, st));
    }
    out
}

fn service_line(status: ServiceStatus) -> Line<'static> {
    let (text, color) = match status {
        ServiceStatus::Active => ("active (running)", Color::Green),
        ServiceStatus::Inactive => ("installed, not running", Color::Yellow),
        ServiceStatus::NotInstalled => ("not installed", Color::DarkGray),
    };
    Line::from(vec![
        Span::styled(
            format!("{:>16}: ", "OS service"),
            Style::default().add_modifier(Modifier::DIM),
        ),
        Span::styled(text, Style::default().fg(color)),
    ])
}

fn role_title(role: InstallRole) -> &'static str {
    match role {
        InstallRole::Workstation => "Workstation",
        InstallRole::Resolver => "Resolver",
        InstallRole::Publisher => "Publisher",
    }
}

/// Format a unix timestamp as a local date-time, or the raw seconds if it's out
/// of range.
fn fmt_unix(secs: u64) -> String {
    match chrono::DateTime::from_timestamp(secs as i64, 0) {
        Some(dt) => dt.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M").to_string(),
        None => secs.to_string(),
    }
}
