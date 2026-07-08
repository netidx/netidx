//! Tab 1 — **Local**: detect this machine's netidx install and show its status,
//! or, on a fresh machine, offer the role menu that starts an install.
//!
//! Read-only detection today; the install / uninstall / renew actions land once
//! the [`TuiAnswerer`](super::answer::TuiAnswerer) exists.

use super::{action::Action, theme, widgets};
use netidx_activation::runtime::default_units_dir;
use netidx_admin::{
    fingerprint::Fingerprint,
    paths,
    provenance::{InstallRecord, InstallRole},
    service::{self, ServiceParams, ServiceScope, ServiceStatus},
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use std::path::{Path, PathBuf};

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
    /// Local admin-server CA tooling, present only when this install owns a CA.
    local_ca: Option<LocalCa>,
}

/// The local (no-auth, control-socket) admin-server CA tooling available on an
/// install that owns a CA — the paths its ops need plus a snapshot of the
/// credential state, computed once at detection (the `slots` status calls are
/// pure but touch the vault, so we don't repeat them every render frame).
struct LocalCa {
    ca_dir: PathBuf,
    cfg: Option<PathBuf>,
    /// The auto-approve (autorenew) credential slot exists.
    auto_approve_present: bool,
    /// The daemon config wires the autorenew keytab in.
    auto_approve_wired: bool,
    /// The recovery-password slot exists.
    recovery_present: bool,
    /// This CA is signed by an external PKI.
    external_signed: bool,
    /// The externally-signed cert is installed (vs awaiting a signature).
    external_installed: bool,
}

impl Detected {
    fn probe(record: InstallRecord, scope: ServiceScope, config_dir: PathBuf) -> Detected {
        // The CA glyph comes from the cluster identity recorded at install — set
        // for both a founding host (its own cluster) and a joining one.
        let ca = record
            .network
            .as_ref()
            .and_then(|n| Fingerprint::parse_text(&n.ca_fingerprint).ok());
        let service = probe_service(&record);
        let local_ca = probe_local_ca(&config_dir);
        Detected { record, service, scope, config_dir, ca, local_ca }
    }
}

/// Probe the local admin-server CA credential state for an install (unix-only —
/// the `slots` ops drive the `SO_PEERCRED` control socket).
#[cfg(unix)]
fn probe_local_ca(config_dir: &Path) -> Option<LocalCa> {
    use netidx_admin::admin_ops::slots;
    let ca_dir = config_dir.join("ca");
    if !ca_dir.is_dir() {
        return None;
    }
    let cfg = paths::discover_admin_server_config().ok();
    let aa = slots::auto_approve_status(&ca_dir, cfg.as_deref()).ok();
    let ext = slots::external_status(&ca_dir).ok();
    let recovery_present =
        slots::recovery_status(&ca_dir).map(|s| s.slot_present).unwrap_or(false);
    Some(LocalCa {
        auto_approve_present: aa.as_ref().map(|s| s.slot_present).unwrap_or(false),
        auto_approve_wired: aa.as_ref().map(|s| s.wired_in_config).unwrap_or(false),
        recovery_present,
        external_signed: ext.as_ref().map(|s| s.externally_signed).unwrap_or(false),
        external_installed: ext.as_ref().map(|s| s.cert_installed).unwrap_or(false),
        ca_dir,
        cfg,
    })
}

#[cfg(not(unix))]
fn probe_local_ca(_config_dir: &Path) -> Option<LocalCa> {
    None
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

/// Tab-1 state: the detected installs plus the role-menu cursor for the
/// fresh-machine case.
pub(super) struct LocalState {
    installs: Vec<Detected>,
    /// Per-install network-sync state, parallel to `installs` — filled in by a
    /// background check so the status view can render instantly from local files
    /// and gain the sync line once the network answers.
    sync: Vec<SyncState>,
    role_menu: ListState,
    /// Which detected install the action list + status apply to.
    selected: usize,
    /// Cursor over the selected install's inline action list (index 0 = Status).
    menu_state: ListState,
    /// Whether the detailed-status overlay is open over the action list.
    show_status: bool,
    /// On a fresh machine, whether the operator has dismissed the "not
    /// installed" welcome dialog and dropped into the install choices.
    welcome_seen: bool,
    /// The open local admin-server panel surface (roster, …) over the control
    /// socket, if the operator chose "Manage admins". Takes over the tab while
    /// open; shares the panel machinery with the Cluster tab (a `Local` target).
    admin: Option<super::remote::RemoteState>,
    /// The open Services surface (list + control + local unit CRUD) over the
    /// local activation supervisor, if the operator chose "Services". Takes over
    /// the tab while open.
    services: Option<super::services::ServicesState>,
}

impl LocalState {
    pub(super) fn new() -> LocalState {
        let mut role_menu = ListState::default();
        role_menu.select(Some(0));
        let mut menu_state = ListState::default();
        menu_state.select(Some(0));
        let installs = detect();
        let sync = vec![SyncState::Unchecked; installs.len()];
        LocalState {
            installs,
            sync,
            role_menu,
            selected: 0,
            menu_state,
            show_status: false,
            welcome_seen: false,
            admin: None,
            services: None,
        }
    }

    /// Open the local admin-server panel surface (control socket, no auth)
    /// directly on `panel` — the split "Admins" / "Permissions" entries. Takes
    /// over the Local tab until closed; returns the initial panel-refresh op.
    pub(super) fn open_admin(
        &mut self,
        cfg_path: PathBuf,
        ca_dir: PathBuf,
        panel: super::remote::Panel,
    ) -> Option<Action> {
        let (state, initial) =
            super::remote::RemoteState::local_panel(cfg_path, ca_dir, panel);
        self.admin = Some(state);
        initial
    }

    /// Apply a completed local-admin op's panel rows to the admin surface.
    pub(super) fn apply_admin(&mut self, update: super::remote::RemoteUpdate) {
        if let Some(admin) = &mut self.admin {
            admin.apply(update);
        }
    }

    /// Whether the local admin panel surface is open.
    pub(super) fn admin_open(&self) -> bool {
        self.admin.is_some()
    }

    /// Open the Services surface over the local activation supervisor at
    /// `units_dir` (always `default_units_dir()`).
    pub(super) fn open_services(&mut self, units_dir: PathBuf) {
        self.services = Some(super::services::ServicesState::new(units_dir));
    }

    /// Apply a completed services op's refreshed rows to the Services surface.
    pub(super) fn apply_services(&mut self, update: super::services::ServicesUpdate) {
        if let Some(services) = &mut self.services {
            services.apply(update);
        }
    }

    /// Whether the Services surface is open.
    pub(super) fn services_open(&self) -> bool {
        self.services.is_some()
    }

    /// Whether the Services surface has a text field focused (the name prompt).
    pub(super) fn services_capturing_text(&self) -> bool {
        self.services.as_ref().is_some_and(|s| s.capturing_text())
    }

    /// When drilled into a tool (the Services surface or the local admin panel
    /// surface), the tool's gutter keys — the App hides the tab bar and shows
    /// these instead of the global keys. `None` at the action list / role menu.
    pub(super) fn gutter(&self) -> Option<String> {
        if let Some(s) = &self.services {
            return Some(s.gutter().to_string());
        }
        if let Some(admin) = &self.admin {
            return admin.gutter();
        }
        None
    }

    /// Re-run detection (after an install/uninstall completes). Resets the sync
    /// state so the loop re-checks against the network.
    pub(super) fn refresh(&mut self) {
        self.installs = detect();
        self.sync = vec![SyncState::Unchecked; self.installs.len()];
        self.selected = self.selected.min(self.installs.len().saturating_sub(1));
        self.menu_state.select(Some(0));
        self.show_status = false;
        self.services = None;
        // If the machine is fresh again (everything was uninstalled), re-show
        // the welcome dialog.
        self.welcome_seen = self.welcome_seen && !self.installs.is_empty();
    }

    /// Whether the detailed-status overlay is open — the host routes keys here
    /// before its global shortcuts so any key dismisses it.
    pub(super) fn status_open(&self) -> bool {
        self.show_status
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

    /// Whether netidx is installed on this machine (any detected install).
    pub(super) fn installed(&self) -> bool {
        !self.installs.is_empty()
    }

    /// Whether this is a fresh machine (no detected install) — the welcome +
    /// role-menu state.
    #[cfg(test)]
    pub(super) fn is_fresh(&self) -> bool {
        self.installs.is_empty()
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
        // The Services surface takes over the tab while open; Esc from its unit
        // list closes it back to the Local tab.
        if self.services.is_some() {
            let at_list = self.services.as_ref().unwrap().at_list();
            if matches!(code, Esc) && at_list {
                self.services = None;
                return None;
            }
            return self.services.as_mut().unwrap().on_key(code);
        }
        // The local admin panel surface opens directly on a panel (Admins /
        // Permissions are their own top-level items), so Esc backs straight out
        // to the action list — there is no intermediate panel menu.
        if self.admin.is_some() {
            if matches!(code, Esc) {
                self.admin = None;
                return None;
            }
            return self.admin.as_mut().unwrap().on_key(code);
        }
        if self.installs.is_empty() {
            // The welcome dialog is up: any key dismisses it into the choices.
            if !self.welcome_seen {
                self.welcome_seen = true;
                return None;
            }
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
        // The detailed-status overlay swallows keys — any key dismisses it.
        if self.show_status {
            self.show_status = false;
            return None;
        }
        // The action list is the main view. Index 0 is Status; the rest are the
        // selected install's actions.
        let n_items = 1 + action_items(&self.installs[self.selected]).len();
        match code {
            Up | Char('k') => self.menu_state.select_previous(),
            Down | Char('j') => self.menu_state.select_next(),
            // Switch between multiple installs (a host with both a user- and a
            // system-scope install); no-op with a single install.
            Left if self.selected > 0 => {
                self.selected -= 1;
                self.menu_state.select(Some(0));
            }
            Right if self.selected + 1 < self.installs.len() => {
                self.selected += 1;
                self.menu_state.select(Some(0));
            }
            Enter => {
                let sel = self.menu_state.selected().unwrap_or(0).min(n_items - 1);
                if sel == 0 {
                    self.show_status = true;
                    return None;
                }
                let mut items = action_items(&self.installs[self.selected]);
                return Some(items.remove(sel - 1).1);
            }
            // Quick shortcuts (also in the list).
            Char('u') => return Some(uninstall_action(&self.installs[self.selected], false)),
            // Uppercase U applies the network sync (the Update action) — mnemonic
            // and distinct from lowercase `u` (uninstall). Only meaningful for a
            // networked install; the status overlay surfaces it when out of sync.
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
        if let Some(services) = &mut self.services {
            services.render(f, area);
            return;
        }
        if let Some(admin) = &mut self.admin {
            admin.render(f, area);
            return;
        }
        if self.installs.is_empty() {
            self.render_role_menu(f, area);
            if !self.welcome_seen {
                render_welcome(f, area);
            }
            return;
        }
        self.render_actions(f, area);
        if self.show_status {
            render_status_overlay(
                f,
                area,
                &self.installs[self.selected],
                &self.sync[self.selected],
            );
        }
    }

    fn render_role_menu(&mut self, f: &mut Frame, area: Rect) {
        let cols =
            Layout::horizontal([Constraint::Length(24), Constraint::Min(0)]).split(area);
        let items: Vec<ListItem> = ROLES.iter().map(|r| ListItem::new(r.title)).collect();
        let list = List::new(items)
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(" Install a Role ", theme::title_style())).title_bottom(
                Line::from(Span::styled(" ↑/↓ select · Enter install · p preview ", theme::hint_style())),
            ))
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, cols[0], &mut self.role_menu);

        let sel = self.role_menu.selected().unwrap_or(0);
        let blurb = Paragraph::new(ROLES[sel].blurb)
            .wrap(Wrap { trim: true })
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(format!(" {} ", ROLES[sel].title), theme::title_style())));
        f.render_widget(blurb, cols[1]);
    }

    /// The installed view: the selected install's action list, titled with the
    /// role and live service state (`Resolver (running)` / `Resolver (stopped)`),
    /// beside a pane describing the highlighted tool. Index 0 is the Status item;
    /// the rest are the role's actions.
    fn render_actions(&self, f: &mut Frame, area: Rect) {
        let cols =
            Layout::horizontal([Constraint::Min(0), Constraint::Length(42)]).split(area);
        let d = &self.installs[self.selected];
        let acts = action_items(d);
        let mut labels = vec!["Status".to_string()];
        labels.extend(acts.iter().map(|(l, _)| l.clone()));
        let items: Vec<ListItem> = labels.into_iter().map(ListItem::new).collect();
        let title = format!(" {} ({}) ", role_title(d.record.role), service_word(d.service));
        let hint = if self.installs.len() > 1 {
            " ↑/↓ select · Enter run · ‹/› switch install "
        } else {
            " ↑/↓ select · Enter run "
        };
        let mut st = self.menu_state;
        let list = List::new(items)
            .style(theme::panel_style())
            .block(
                theme::panel_block()
                    .title(Span::styled(title, theme::title_style()))
                    .title_bottom(Line::from(Span::styled(hint, theme::hint_style()))),
            )
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, cols[0], &mut st);
        let sel = self.menu_state.selected().unwrap_or(0);
        let desc = if sel == 0 {
            STATUS_DESC
        } else {
            acts.get(sel - 1).map_or("", |(_, a)| action_desc(a))
        };
        let blurb = Paragraph::new(desc)
            .wrap(Wrap { trim: true })
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(" Description ", theme::title_style())));
        f.render_widget(blurb, cols[1]);
    }
}

/// The Status item's description in the tool detail pane.
const STATUS_DESC: &str =
    "Full status detail for this install: its config, whether it is in sync with \
     the cluster, and its CA glyph.";

/// A one-line description of a Local-tab action, shown in the menu detail pane.
fn action_desc(action: &Action) -> &'static str {
    use Action::*;
    match action {
        Install { dry_run: false, .. } => "Install this role on the machine.",
        Install { dry_run: true, .. } => {
            "Preview installing this role — show the plan without changing anything."
        }
        Renew { .. } => {
            "Renew this host's TLS certificates from the cluster's CA now. The \
             auto-renew service does this automatically unless it has been turned off."
        }
        Update { .. } => {
            "Reconcile this host's resolver list with the cluster, adding or removing \
             resolvers as necessary."
        }
        Join { dry_run: false } => {
            "Graduate this local-only workstation onto a cluster, enrolling a TLS identity."
        }
        Join { dry_run: true } => "Preview joining a cluster, without changing anything.",
        AddParent => "Attach this resolver under a parent resolver by delegation.",
        ReviewDelegations => {
            "Review and approve requests from resolvers asking to attach under this one."
        }
        Remote(_) => "Connect to a remote admin server.",
        Uninstall { remove_ca: false, .. } => "Remove this install — its config and OS service.",
        Uninstall { remove_ca: true, .. } => {
            "Remove this install and destroy its certificate authority. Irreversible."
        }
        AutoApprove { rotate: true, .. } => {
            "Rotate this admin server's auto-renew credential. The auto-renew service \
             automatically renews expiring certificates for the cluster's members."
        }
        AutoApprove { rotate: false, .. } => {
            "Enable the auto-renew service, which automatically renews expiring \
             certificates for the cluster's members, without an admin approving each one."
        }
        RecoveryRotate { .. } => {
            "Mint a fresh CA recovery password. Use this if you lost or forgot the old \
             one — it retires the old password. Works only locally, on the CA machine."
        }
        ExternalEmitCsr { .. } => "Re-emit a renewal CSR for this externally-signed CA.",
        ExternalInstall { .. } => "Install the externally-signed CA certificate returned by your PKI.",
        OpenServices { .. } => {
            "Manage netidx services on this machine — list them, start/stop/restart, \
             and create, edit, or delete units."
        }
        // Never a menu item (dispatched from within the Services surface); present
        // only for exhaustiveness.
        Services(_) => "",
        ManageLocalAdmins { panel: super::remote::Panel::Perms, .. } => {
            "View and edit this host's permissions."
        }
        ManageLocalAdmins { .. } => {
            "Manage this admin server's admins and their scopes."
        }
    }
}

/// The parenthetical service state shown in an install's title.
fn service_word(status: ServiceStatus) -> &'static str {
    match status {
        ServiceStatus::Active => "running",
        ServiceStatus::Inactive => "stopped",
        ServiceStatus::NotInstalled => "no service",
    }
}

/// The detailed-status overlay — the former always-on card, now shown on demand
/// over the action list (any key closes). Left column is the record + sync
/// detail; the right column carries the cluster's CA glyph and fingerprint when
/// this host belongs to a network.
fn render_status_overlay(f: &mut Frame, screen: Rect, d: &Detected, sync: &SyncState) {
    let mut lines = detail_lines(d);
    lines.extend(sync_lines(sync));
    // The right column (when present) is the glyph label + 8 identicon rows + a
    // blank + the grouped fingerprint; size the dialog to whichever column is
    // taller so neither is clipped.
    let glyph_h = d.ca.as_ref().map_or(0, |fp| 10 + widgets::group_fingerprint(fp).len());
    let w = 90.min(screen.width.saturating_sub(4)).max(24);
    let h = ((lines.len().max(glyph_h)) as u16 + 2).min(screen.height); // + borders
    let area = widgets::centered(w, h, screen);
    widgets::shadow(f, area, screen);
    f.render_widget(Clear, area);
    let block = theme::dialog_block(&format!("{} status", role_title(d.record.role)))
        .title_bottom(Line::from(Span::styled(" any key to close ", theme::hint_style())));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let cols = if d.ca.is_some() {
        Layout::horizontal([Constraint::Min(0), Constraint::Length(24)]).split(inner)
    } else {
        Layout::horizontal([Constraint::Min(0)]).split(inner)
    };
    f.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }).style(theme::panel_style()),
        cols[0],
    );
    if let Some(fp) = &d.ca {
        let mut g = vec![Line::from(Span::styled("CA glyph", theme::hint_style()))];
        g.extend(widgets::identicon_lines(fp));
        g.push(Line::from(""));
        let hash = Style::default()
            .bg(theme::PANEL_BG)
            .fg(Color::Rgb(0, 0, 150))
            .add_modifier(Modifier::BOLD);
        for chunk in widgets::group_fingerprint(fp) {
            g.push(Line::from(Span::styled(chunk, hash)));
        }
        f.render_widget(Paragraph::new(g).style(theme::panel_style()), cols[1]);
    }
}

/// Build the action list for a detected install — its role's post-install
/// subcommand surface (`netidx admin <template> <subcommand>`). The Status entry
/// is prepended by the caller; these are the actionable items.
fn action_items(d: &Detected) -> Vec<(String, Action)> {
    let role = d.record.role;
    let networked = d.record.network.is_some();
    let mut items: Vec<(String, Action)> = Vec::new();
    if networked {
        items.push(("Update Resolvers".to_string(), Action::Update { role }));
    }
    if role == InstallRole::Workstation && !networked {
        items.push(("Join a Cluster".to_string(), Action::Join { dry_run: false }));
        items.push(("Preview Join (Dry Run)".to_string(), Action::Join { dry_run: true }));
    }
    if role == InstallRole::Resolver {
        items.push(("Add a Parent".to_string(), Action::AddParent));
        // Delegation requests land on the parent's *own* admin server, so offer
        // the review shortcut when this host runs one — not when `record.
        // admin_server` is set (that names the admin server this node enrolls
        // against, and is `None` on the CA host, which is exactly who reviews).
        if runs_local_admin_server() {
            items.push((
                "Review Delegation Requests".to_string(),
                Action::ReviewDelegations,
            ));
        }
    }
    if networked {
        items.push(("Renew Certificates".to_string(), Action::Renew { server: d.record.admin_server }));
    }
    // One Services surface over the local activation supervisor (no auth): list +
    // status, start/stop/restart, and local-only unit create/edit/delete. Present
    // whenever this machine has a supervisor unit directory.
    if let Some(units_dir) = default_units_dir() {
        items.push(("Services".to_string(), Action::OpenServices { units_dir }));
    }
    // Local admin-server CA tools — no auth (control socket), only for a node
    // that owns a CA. Most nodes have no `local_ca` and skip this entirely.
    if let Some(lca) = &d.local_ca {
        // The admin roster and this host's own permissions over the local control
        // socket, when an admin server is configured on this box.
        if let Some(cfg_path) = &lca.cfg {
            items.push((
                "Admins".to_string(),
                Action::ManageLocalAdmins {
                    cfg_path: cfg_path.clone(),
                    ca_dir: lca.ca_dir.clone(),
                    panel: super::remote::Panel::Roster,
                },
            ));
            items.push((
                "Permissions".to_string(),
                Action::ManageLocalAdmins {
                    cfg_path: cfg_path.clone(),
                    ca_dir: lca.ca_dir.clone(),
                    panel: super::remote::Panel::Perms,
                },
            ));
        }
        if lca.auto_approve_present {
            items.push((
                "Rotate Auto-Renew Credential".to_string(),
                Action::AutoApprove { rotate: true, ca_dir: lca.ca_dir.clone(), cfg: lca.cfg.clone() },
            ));
        } else {
            items.push((
                "Enable Auto-Renew".to_string(),
                Action::AutoApprove { rotate: false, ca_dir: lca.ca_dir.clone(), cfg: lca.cfg.clone() },
            ));
        }
        if lca.recovery_present {
            items.push((
                "Rotate Recovery Password".to_string(),
                Action::RecoveryRotate { ca_dir: lca.ca_dir.clone(), cfg: lca.cfg.clone() },
            ));
        }
        if lca.external_signed {
            items.push((
                "Emit Renewal CSR (External CA)".to_string(),
                Action::ExternalEmitCsr { ca_dir: lca.ca_dir.clone() },
            ));
            let label = if lca.external_installed {
                "Install Renewed Certificate (External CA)"
            } else {
                "Install Signed Certificate (External CA)"
            };
            items.push((label.to_string(), Action::ExternalInstall { ca_dir: lca.ca_dir.clone() }));
        }
    }
    // One Uninstall item; when this host owns a CA, the confirm flow asks whether
    // to also destroy it (see the chained confirm in the App loop).
    items.push(("Uninstall".to_string(), uninstall_action(d, false)));
    items
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


/// The one-time "netidx isn't installed" welcome dialog on a fresh machine.
/// The prose is one continuous string per paragraph so ratatui's `Paragraph`
/// wraps it to the dialog width at any terminal size; the box is sized to the
/// wrapped height via [`wrapped_rows`].
fn render_welcome(f: &mut Frame, screen: Rect) {
    let heading = "netidx isn't installed on this machine.";
    let body = "Choose an install option — a Workstation for a laptop or desktop, a \
                Resolver to run a network's directory, or a Publisher.";
    let prompt = " Press Enter to continue ";
    let w = 64.min(screen.width.saturating_sub(4)).max(24);
    let lines = vec![
        Line::from(Span::styled(heading, theme::panel_style().add_modifier(Modifier::BOLD))),
        Line::from(""),
        Line::from(Span::styled(body, theme::panel_style())),
        Line::from(""),
        Line::from(Span::styled(prompt, theme::selected_style())),
    ];
    let h = (widgets::wrapped_height(&lines, w - 2) + 2).min(screen.height); // + borders
    let area = widgets::centered(w, h, screen);
    widgets::shadow(f, area, screen);
    f.render_widget(Clear, area);
    f.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: true }).style(theme::panel_style()).block(theme::dialog_block("Welcome to netidx")),
        area,
    );
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
            lines.push(kv("Cluster", net.domain.clone()));
            lines.push(kv("CA fingerprint", net.ca_fingerprint.clone()));
        }
        None => lines.push(kv("Cluster", "standalone (local-only)".to_string())),
    }
    if let Some(addr) = r.admin_server {
        lines.push(kv("Admin server", addr.to_string()));
    }
    if let Some(lca) = &d.local_ca {
        let (aa, aa_c) = if lca.auto_approve_present {
            if lca.auto_approve_wired {
                ("active", theme::OK)
            } else {
                ("present (not wired)", theme::WARN)
            }
        } else {
            ("not set up", theme::HINT_FG)
        };
        lines.push(kv_status("Auto-approve", aa.to_string(), aa_c));
        let (rec, rec_c) =
            if lca.recovery_present { ("set", theme::OK) } else { ("MISSING", theme::WARN) };
        lines.push(kv_status("Recovery slot", rec.to_string(), rec_c));
        if lca.external_signed {
            let (ext, ext_c) = if lca.external_installed {
                ("installed", theme::OK)
            } else {
                ("awaiting signature", theme::WARN)
            };
            lines.push(kv_status("External CA", ext.to_string(), ext_c));
        }
    }
    lines.push(service_line(d.service));
    lines.push(kv("Config", d.config_dir.display().to_string()));
    lines.push(kv("Installed", fmt_unix(r.created_unix)));
    lines
}

/// A `label: value` line with a muted label.
fn kv(label: &'static str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:>16}: "), theme::hint_style()),
        Span::styled(value, theme::panel_style()),
    ])
}

/// A `label: value` line whose value carries a status colour.
fn kv_status(label: &'static str, value: String, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:>16}: "), theme::hint_style()),
        Span::styled(value, Style::default().bg(theme::PANEL_BG).fg(color)),
    ])
}

/// The status-card lines describing this install's network-sync state — the
/// TUI counterpart of the CLI `status` "in sync / out of sync" report.
fn sync_lines(sync: &SyncState) -> Vec<Line<'static>> {
    match sync {
        // A local-only install never gets here (never checked); render nothing.
        SyncState::Unchecked => Vec::new(),
        SyncState::Checking => {
            vec![kv_status("Cluster sync", "checking…".to_string(), theme::HINT_FG)]
        }
        SyncState::InSync => {
            vec![kv_status("Cluster sync", "✓ in sync".to_string(), theme::OK)]
        }
        SyncState::Failed(e) => vec![kv_status(
            "Cluster sync",
            format!("could not check ({e})"),
            theme::HINT_FG,
        )],
        SyncState::OutOfSync(changes) => {
            let mut lines = vec![kv_status(
                "Cluster sync",
                format!(
                    "⚠ {} new member server(s) — press U to apply",
                    changes.len()
                ),
                theme::WARN,
            )];
            for c in changes {
                lines.push(Line::from(vec![
                    Span::raw(format!("{:>18}", "")),
                    Span::styled(format!("+ {c}"), Style::default().bg(theme::PANEL_BG).fg(theme::WARN)),
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
        ServiceStatus::Active => ("active (running)", theme::OK),
        ServiceStatus::Inactive => ("installed, not running", theme::WARN),
        ServiceStatus::NotInstalled => ("not installed", theme::HINT_FG),
    };
    kv_status("OS service", text.to_string(), color)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_word_matches_state() {
        // The words Eric asked for in the install-title, e.g. `Resolver (running)`.
        assert_eq!(service_word(ServiceStatus::Active), "running");
        assert_eq!(service_word(ServiceStatus::Inactive), "stopped");
        assert_eq!(service_word(ServiceStatus::NotInstalled), "no service");
    }
}
