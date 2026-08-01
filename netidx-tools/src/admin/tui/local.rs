//! Tab 1 — **Local**: detect this machine's netidx install and show its status,
//! or, on a fresh machine, offer installation and restore choices.
//!
//! Read-only detection today; the install / uninstall / renew actions land once
//! the [`TuiAnswerer`](super::answer::TuiAnswerer) exists.

use super::{action::Action, theme, widgets};
use netidx_activation::runtime::default_units_dir;
use netidx_admin::{
    paths,
    provenance::{InstallRecord, InstallRole},
    reconcile::Change,
    renewd,
    service::{self, ServiceParams, ServiceScope, ServiceStatus},
};
use netidx_admin_proto::fingerprint::Fingerprint;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FreshAction {
    Install(InstallRole),
    Restore,
}

#[derive(Clone, Copy)]
struct FreshChoice {
    action: FreshAction,
    title: &'static str,
    blurb: &'static str,
}

const RESTORE_CHOICE: FreshChoice = FreshChoice {
    action: FreshAction::Restore,
    title: "Restore from Backup",
    blurb: "Restore a complete managed installation from a verified backup bundle. The \
            destination must be a clean machine or an identical interrupted restore.",
};

#[cfg(unix)]
const FRESH_CHOICES: [FreshChoice; 5] = [
    FreshChoice {
        action: FreshAction::Install(InstallRole::Ca),
        title: "CA",
        blurb: "Install the admin domain's certificate authority on this machine. It may be dedicated to this role; \
                resolver servers are installed separately and enroll with it.",
    },
    FreshChoice {
        action: FreshAction::Install(InstallRole::Workstation),
        title: "Workstation",
        blurb: "Turn this machine into a full netidx node: a local resolver serving a \
                /local namespace plus a matching client. Join an existing admin domain to \
                enroll a TLS identity, or run standalone. The usual choice for a laptop \
                or desktop.",
    },
    FreshChoice {
        action: FreshAction::Install(InstallRole::Resolver),
        title: "Resolver",
        blurb: "A network-facing resolver server — the directory that maps paths to \
                publishers for a whole admin domain or a delegated subtree. Can mint a new \
                admin domain's certificate authority and admin server, or enroll under an \
                existing one.",
    },
    FreshChoice {
        action: FreshAction::Install(InstallRole::Publisher),
        title: "Publisher",
        blurb: "A client configuration for a host that publishes data: point it at an \
                admin domain's resolvers with the right auth. Installs a certificate-renewal \
                service when the admin domain uses TLS.",
    },
    RESTORE_CHOICE,
];

#[cfg(not(unix))]
const FRESH_CHOICES: [FreshChoice; 4] = [
    FreshChoice {
        action: FreshAction::Install(InstallRole::Workstation),
        title: "Workstation",
        blurb: "Turn this machine into a full netidx node: a local resolver serving a \
                /local namespace plus a matching client. Join an existing admin domain to \
                enroll a TLS identity, or run standalone. The usual choice for a laptop \
                or desktop.",
    },
    FreshChoice {
        action: FreshAction::Install(InstallRole::Resolver),
        title: "Resolver",
        blurb: "A network-facing resolver server — the directory that maps paths to \
                publishers for a whole admin domain or a delegated subtree. Enrolls under an \
                existing CA.",
    },
    FreshChoice {
        action: FreshAction::Install(InstallRole::Publisher),
        title: "Publisher",
        blurb: "A client configuration for a host that publishes data: point it at an \
                admin domain's resolvers with the right auth. Installs a certificate-renewal \
                service when the admin domain uses TLS.",
    },
    RESTORE_CHOICE,
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
    /// Whether this host holds any TLS identity the CA could renew. False on a
    /// krb5 or anonymous host that enrolled no certificate — it has joined an
    /// admin domain, but there is nothing to renew.
    renewable: bool,
}

/// The local (no-auth, control-socket) admin-server CA tooling available on an
/// install that owns a CA — the paths its ops need, plus the credential state
/// once a background probe has answered. The paths are derived from local
/// files; the state is not, so it is filled in asynchronously (see
/// [`CaProbe`]).
/// Off unix there is no local CA, so `local_ca_paths` always answers `None` and
/// nothing below is ever built — the types stay compiled so the UI is one shape.
#[cfg_attr(not(unix), allow(dead_code))]
struct LocalCa {
    ca_dir: PathBuf,
    cfg: Option<PathBuf>,
    probe: CaProbe,
}

/// The credential-state half of [`LocalCa`], filled in by a background probe.
///
/// Reading it drives the daemon's local control socket, and that is an admin domain
/// round trip in all but name: connecting to a bound unix socket succeeds into
/// the backlog whether or not the daemon is accepting. Doing it inline on the
/// UI task froze the whole TUI behind a wedged daemon, so it lives here on the
/// same background-fill pattern as [`SyncState`].
#[derive(Clone)]
#[cfg_attr(not(unix), allow(dead_code))]
pub(super) enum CaProbe {
    /// Not yet probed; the event loop launches one.
    Unprobed,
    /// A probe is in flight.
    Probing,
    Ready(CaCredentials),
    /// The probe couldn't reach the CA tooling; carries the reason.
    Failed(String),
}

#[derive(Clone, Copy)]
#[cfg_attr(not(unix), allow(dead_code))]
pub(super) struct CaCredentials {
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

#[cfg_attr(not(unix), allow(dead_code))]
impl LocalCa {
    fn credentials(&self) -> Option<&CaCredentials> {
        match &self.probe {
            CaProbe::Ready(credentials) => Some(credentials),
            CaProbe::Unprobed | CaProbe::Probing | CaProbe::Failed(_) => None,
        }
    }
}

impl Detected {
    fn probe(
        record: InstallRecord,
        scope: ServiceScope,
        config_dir: PathBuf,
        renewable: bool,
    ) -> Detected {
        // The CA glyph comes from the admin domain identity recorded at install — set
        // for both a founding host (its own admin domain) and a joining one.
        let ca = record
            .admin_domain
            .as_ref()
            .and_then(|n| Fingerprint::parse_text(&n.ca_fingerprint).ok());
        let service = probe_service(&record);
        let local_ca = local_ca_paths(&config_dir);
        Detected { record, service, scope, config_dir, ca, local_ca, renewable }
    }
}

/// Whether this install owns a CA, and where its tooling lives. Local files
/// only — cheap enough for the UI task. The credential state it can't answer
/// from disk is left [`CaProbe::Unprobed`] for [`probe_local_cas`].
#[cfg(unix)]
fn local_ca_paths(config_dir: &Path) -> Option<LocalCa> {
    let ca_dir = config_dir.join("ca");
    if !ca_dir.is_dir() {
        return None;
    }
    Some(LocalCa {
        ca_dir,
        cfg: paths::discover_admin_server_config().ok(),
        probe: CaProbe::Unprobed,
    })
}

#[cfg(not(unix))]
fn local_ca_paths(_config_dir: &Path) -> Option<LocalCa> {
    None
}

/// Read the CA credential state for each `(index, ca_dir, cfg)` — the
/// background half of [`local_ca_paths`]. Self-contained (owns its inputs,
/// borrows no UI state) so the event loop can hold it as a plain future,
/// exactly like [`check_sync`].
#[cfg(unix)]
pub(super) async fn probe_local_cas(
    targets: Vec<(usize, PathBuf, Option<PathBuf>)>,
) -> Vec<(usize, CaProbe)> {
    use netidx_admin::ops::slots;
    let mut out = Vec::with_capacity(targets.len());
    for (index, ca_dir, cfg) in targets {
        let probed = async {
            let access = slots::CaAccess::open(&ca_dir, cfg).await?;
            slots::local_ca_status(&access, &ca_dir).await
        }
        .await;
        out.push(match probed {
            Ok(status) => (
                index,
                CaProbe::Ready(CaCredentials {
                    auto_approve_present: status.auto_approve.slot_present,
                    auto_approve_wired: status.auto_approve.wired_in_config,
                    recovery_present: status.recovery.slot_present,
                    external_signed: status.external.externally_signed,
                    external_installed: status.external.cert_installed,
                }),
            ),
            Err(e) => (index, CaProbe::Failed(format!("{e:#}"))),
        });
    }
    out
}

#[cfg(not(unix))]
pub(super) async fn probe_local_cas(
    _targets: Vec<(usize, PathBuf, Option<PathBuf>)>,
) -> Vec<(usize, CaProbe)> {
    Vec::new()
}

/// Admin domain-sync state for a detected install, filled in asynchronously by a
/// background check (the same reconcile the CLI `status`/`update` runs). Kept
/// out of [`Detected`] because probing it is an admin domain round-trip, while
/// `Detected` is built synchronously from local files.
#[derive(Clone)]
pub(super) enum SyncState {
    /// An install that has joined an admin domain not yet checked; the event loop launches a check.
    Unchecked,
    /// A background check is in flight.
    Checking,
    /// The config already matches the admin domain — nothing to apply.
    InSync,
    /// The config differs from the admin domain map; each [`Change`] is one
    /// pending edit and carries its own direction, so a removal renders as a
    /// removal here exactly as it does in the CLI preview.
    OutOfSync(Vec<Change>),
    /// The check couldn't reach the admin domain; carries the error for display.
    Failed(String),
}

/// The OS-service state for a role. Resolver / publisher register a system-scope
/// `netidx@<user>` service even when their config is user-scope; a workstation
/// uses a user-scope service. Probe the scope that matches.
fn probe_service(record: &InstallRecord) -> ServiceStatus {
    let (scope, for_user) = match record.role {
        InstallRole::Workstation => (ServiceScope::User, None),
        InstallRole::Ca | InstallRole::Resolver | InstallRole::Publisher => {
            (ServiceScope::System, netidx_admin::service::resolve_for_user(None).ok())
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
    // What the renew action would actually operate on: the identities renewd
    // discovers across this host's configs. Scanned once — it is the same set
    // for every install detected here.
    let renewable = !renewd::host_identities(None).is_empty();
    let user_path = paths::user_install_record().ok();
    if let Ok(Some(record)) = InstallRecord::load_default() {
        let dir = paths::user_config_root().unwrap_or_default();
        out.push(Detected::probe(record, ServiceScope::User, dir, renewable));
    }
    let sys_path = paths::system_install_record();
    // Skip the system record if it's the very same file we already read as the
    // user record (unusual, but possible if the two roots coincide).
    if sys_path.exists() && user_path.as_deref() != Some(sys_path.as_path()) {
        if let Ok(record) = InstallRecord::load(&sys_path) {
            out.push(Detected::probe(
                record,
                ServiceScope::System,
                paths::system_config_root(),
                renewable,
            ));
        }
    }
    out
}

/// Tab-1 state: the detected installs plus the role-menu cursor for the
/// fresh-machine case.
pub(super) struct LocalState {
    installs: Vec<Detected>,
    /// Per-install admin domain-sync state, parallel to `installs` — filled in by a
    /// background check so the status view can render instantly from local files
    /// and gain the sync line once the admin domain answers.
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
    /// open; shares the panel machinery with the Admin domain tab (a `Local` target).
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
    #[cfg(unix)]
    pub(super) fn open_admin(
        &mut self,
        cfg_path: PathBuf,
        panel: super::remote::Panel,
    ) -> Option<Action> {
        let (state, initial) = super::remote::RemoteState::local_panel(cfg_path, panel);
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
    /// state so the loop re-checks against the admin domain.
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

    /// Retry whatever failed last time this tab was up. Both background fills
    /// are one-shot — the event loop only launches a probe for a state that has
    /// never been tried — so a transient failure (an admin server briefly
    /// unreachable, or the daemon's control socket not yet bound while it
    /// starts) would otherwise persist for the whole session. Anything still in
    /// flight is left alone.
    pub(super) fn on_focus(&mut self) {
        for state in &mut self.sync {
            if matches!(state, SyncState::Failed(_)) {
                *state = SyncState::Unchecked;
            }
        }
        #[cfg(unix)]
        for install in &mut self.installs {
            if let Some(local_ca) = install.local_ca.as_mut()
                && matches!(local_ca.probe, CaProbe::Failed(_))
            {
                local_ca.probe = CaProbe::Unprobed;
            }
        }
    }

    /// Whether the detailed-status overlay is open — the host routes keys here
    /// before its global shortcuts so any key dismisses it.
    pub(super) fn status_open(&self) -> bool {
        self.show_status
    }

    /// Networked installs whose sync hasn't been checked yet; marks each
    /// `Checking` so the event loop launches exactly one background check per
    /// install. Standalone (local-only) installs are never checked.
    pub(super) fn take_pending_checks(&mut self) -> Vec<(usize, InstallRole, PathBuf)> {
        let mut out = Vec::new();
        for i in 0..self.installs.len() {
            if self.installs[i].record.admin_domain.is_some()
                && self.installs[i].record.role != InstallRole::Ca
                && matches!(self.sync[i], SyncState::Unchecked)
            {
                self.sync[i] = SyncState::Checking;
                out.push((
                    i,
                    self.installs[i].record.role,
                    self.installs[i].config_dir.clone(),
                ));
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

    /// Installs owning a CA whose credential state hasn't been read yet; marks
    /// each `Probing` so the event loop launches exactly one probe per install.
    pub(super) fn take_pending_ca_probes(
        &mut self,
    ) -> Vec<(usize, PathBuf, Option<PathBuf>)> {
        let mut out = Vec::new();
        for (i, install) in self.installs.iter_mut().enumerate() {
            if let Some(local_ca) = install.local_ca.as_mut()
                && matches!(local_ca.probe, CaProbe::Unprobed)
            {
                local_ca.probe = CaProbe::Probing;
                out.push((i, local_ca.ca_dir.clone(), local_ca.cfg.clone()));
            }
        }
        out
    }

    /// Fold in a finished CA probe. The action list gains entries when this
    /// lands, so re-clamp the cursor rather than leaving it past the end.
    pub(super) fn apply_ca_probes(&mut self, results: Vec<(usize, CaProbe)>) {
        for (i, probe) in results {
            if let Some(local_ca) =
                self.installs.get_mut(i).and_then(|d| d.local_ca.as_mut())
            {
                local_ca.probe = probe;
            }
        }
        if let Some(selected) = self.installs.get(self.selected) {
            let len = action_items(selected).len();
            let cursor = self.menu_state.selected().unwrap_or(0);
            self.menu_state.select(Some(cursor.min(len.saturating_sub(1))));
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
                    return Some(
                        match FRESH_CHOICES[self.role_menu.selected().unwrap_or(0)].action
                        {
                            FreshAction::Install(role) => {
                                Action::Install { role, dry_run: false }
                            }
                            FreshAction::Restore => Action::Restore,
                        },
                    );
                }
                Char('p') => {
                    if let FreshAction::Install(role) =
                        FRESH_CHOICES[self.role_menu.selected().unwrap_or(0)].action
                    {
                        return Some(Action::Install { role, dry_run: true });
                    }
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
            Char('u') => {
                return Some(uninstall_action(&self.installs[self.selected], false));
            }
            // Uppercase U applies the admin domain sync (the Update action) — mnemonic
            // and distinct from lowercase `u` (uninstall). Only meaningful for a
            // joined install; the status overlay surfaces it when out of sync.
            Char('U') => {
                let d = &self.installs[self.selected];
                if d.record.admin_domain.is_some() && d.record.role != InstallRole::Ca {
                    return Some(Action::Update {
                        role: d.record.role,
                        config_root: d.config_dir.clone(),
                    });
                }
            }
            Char('r') => {
                let d = &self.installs[self.selected];
                if d.record.admin_domain.is_some() && d.renewable {
                    return Some(Action::Renew);
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
        let items: Vec<ListItem> =
            FRESH_CHOICES.iter().map(|choice| ListItem::new(choice.title)).collect();
        let selected = self.role_menu.selected().unwrap_or(0);
        let hint = match FRESH_CHOICES[selected].action {
            FreshAction::Install(_) => " Enter · p preview ",
            FreshAction::Restore => " Enter restore ",
        };
        let list = List::new(items)
            .style(theme::panel_style())
            .block(
                theme::panel_block()
                    .title(Span::styled(" Set Up This Machine ", theme::title_style()))
                    .title_bottom(Line::from(Span::styled(hint, theme::hint_style()))),
            )
            .highlight_style(theme::selected_style())
            .highlight_symbol("▸ ");
        f.render_stateful_widget(list, cols[0], &mut self.role_menu);

        let blurb = Paragraph::new(FRESH_CHOICES[selected].blurb)
            .wrap(Wrap { trim: true })
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(
                format!(" {} ", FRESH_CHOICES[selected].title),
                theme::title_style(),
            )));
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
        let title =
            format!(" {} ({}) ", role_title(d.record.role), service_word(d.service));
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
            .block(
                theme::panel_block()
                    .title(Span::styled(" Description ", theme::title_style())),
            );
        f.render_widget(blurb, cols[1]);
    }
}

/// The Status item's description in the tool detail pane.
const STATUS_DESC: &str = "Full status detail for this install: its config, whether it is in sync with \
     the admin domain, and its CA glyph.";

/// A one-line description of a Local-tab action, shown in the menu detail pane.
fn action_desc(action: &Action) -> &'static str {
    use Action::*;
    match action {
        Install { dry_run: false, .. } => "Install this role on the machine.",
        Install { dry_run: true, .. } => {
            "Preview installing this role — show the plan without changing anything."
        }
        Renew => {
            "Renew this host's TLS certificates from the admin domain's CA now. The \
             auto-renew service does this automatically unless it has been turned off."
        }
        Update { .. } => {
            "Reconcile this host's resolver list with the admin domain, adding or removing \
             resolvers as necessary."
        }
        Join { dry_run: false } => {
            "Graduate this local-only workstation onto an admin domain, enrolling a TLS identity."
        }
        Join { dry_run: true } => {
            "Preview joining an admin domain, without changing anything."
        }
        AddParent { .. } => "Attach this resolver under a parent resolver by delegation.",
        Remote(_) => "Connect to a remote admin server.",
        Uninstall { remove_ca: false, .. } => {
            "Remove this install — its config and OS service."
        }
        Uninstall { remove_ca: true, .. } => {
            "Remove this install and destroy its certificate authority. Irreversible."
        }
        #[cfg(unix)]
        AutoApprove { rotate: true, .. } => {
            "Rotate this admin server's auto-renew credential. The auto-renew service \
             automatically renews expiring certificates for the admin domain's members."
        }
        #[cfg(unix)]
        AutoApprove { rotate: false, .. } => {
            "Enable the auto-renew service, which automatically renews expiring \
             certificates for the admin domain's members, without an admin approving each one."
        }
        #[cfg(unix)]
        RecoveryRotate { .. } => {
            "Mint a fresh CA recovery password. Use this if you lost or forgot the old \
             one — it retires the old password. Works only locally, on the CA machine."
        }
        Backup { .. } => {
            "Back up this complete managed installation. Ca state is captured \
             consistently while the CA remains online; machine credentials are re-enrolled on restore."
        }
        Restore => "Restore a complete installation from a verified backup bundle.",
        FinishRestore { .. } => "Finish re-enrolling roles after CA startup.",
        #[cfg(unix)]
        ExternalEmitCsr { .. } => {
            "Write a subordinate-CA CSR for your external PKI to sign."
        }
        #[cfg(unix)]
        ExternalInstall { .. } => {
            "Install the externally-signed CA certificate returned by your PKI."
        }
        OpenServices { .. } => {
            "Manage netidx services on this machine — list them, start/stop/restart, \
             and create, edit, or delete units."
        }
        // Never a menu item (dispatched from within the Services surface); present
        // only for exhaustiveness.
        Services(_) => "",
        #[cfg(unix)]
        ManageLocalAdmins { panel: super::remote::Panel::Perms, .. } => {
            "View and edit this host's permissions."
        }
        #[cfg(unix)]
        ManageLocalAdmins { .. } => "Manage this admin server's admins and their scopes.",
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
/// detail; the right column carries the admin domain's CA glyph and fingerprint when
/// this host belongs to an admin domain.
fn render_status_overlay(f: &mut Frame, screen: Rect, d: &Detected, sync: &SyncState) {
    let mut lines = detail_lines(d);
    lines.extend(sync_lines(sync));
    // The right column (when present) is the glyph label + identicon tile + a
    // blank + the grouped fingerprint; size the dialog to whichever column is
    // taller so neither is clipped.
    let glyph_h = d.ca.as_ref().map_or(0, |fp| {
        2 + widgets::IDENTICON_HEIGHT as usize + widgets::group_fingerprint(fp).len()
    });
    let w = 90.min(screen.width.saturating_sub(4)).max(24);
    let h = ((lines.len().max(glyph_h)) as u16 + 2).min(screen.height); // + borders
    let area = widgets::centered(w, h, screen);
    widgets::shadow(f, area, screen);
    f.render_widget(Clear, area);
    let block = theme::dialog_block(&format!("{} status", role_title(d.record.role)))
        .title_bottom(Line::from(Span::styled(
            " any key to close ",
            theme::hint_style(),
        )));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let cols = if d.ca.is_some() {
        Layout::horizontal([Constraint::Min(0), Constraint::Length(24)]).split(inner)
    } else {
        Layout::horizontal([Constraint::Min(0)]).split(inner)
    };
    // Deliberately not wrapped. These are aligned `label: value` lines, and a
    // wrapped value continues at column 0, which breaks the alignment of every
    // line below it. Clipping a long value (a lock error carrying a path) keeps
    // the card readable.
    f.render_widget(Paragraph::new(lines).style(theme::panel_style()), cols[0]);
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
    let joined = d.record.admin_domain.is_some();
    let mut items: Vec<(String, Action)> = Vec::new();
    if joined && role != InstallRole::Ca {
        items.push((
            "Update Resolvers".to_string(),
            Action::Update { role, config_root: d.config_dir.clone() },
        ));
    }
    if role == InstallRole::Workstation && !joined {
        items.push(("Join a Admin domain".to_string(), Action::Join { dry_run: false }));
        items
            .push(("Preview Join (Dry Run)".to_string(), Action::Join { dry_run: true }));
    }
    if role == InstallRole::Resolver {
        items.push((
            "Add a Parent".to_string(),
            Action::AddParent { config_root: d.config_dir.clone() },
        ));
        // Delegation review is deliberately NOT offered here: the daemon's
        // delegation handlers require an authenticated admin password and have no
        // local-superuser control-socket bypass (unlike roster/perms/rotate), so
        // it can't run over the local control socket. Review delegations from the
        // Admin domain tab (connect to this host's own admin server).
    }
    if joined && d.renewable {
        items.push(("Renew Certificates".to_string(), Action::Renew));
    }
    // One Services surface over the local activation supervisor (no auth): list +
    // status, start/stop/restart, and local-only unit create/edit/delete. Present
    // whenever this machine has a supervisor unit directory.
    if let Some(units_dir) = default_units_dir() {
        items.push(("Services".to_string(), Action::OpenServices { units_dir }));
    }
    // Local admin-server CA tools — no auth (control socket), only for a node
    // that owns a CA. Most nodes have no `local_ca` and skip this entirely.
    #[cfg(unix)]
    if let Some(lca) = &d.local_ca {
        // The admin roster and this host's own permissions over the local control
        // socket, when an admin server is configured on this box.
        if let Some(cfg_path) = &lca.cfg {
            items.push((
                "Admins".to_string(),
                Action::ManageLocalAdmins {
                    cfg_path: cfg_path.clone(),
                    panel: super::remote::Panel::Roster,
                },
            ));
            items.push((
                "Permissions".to_string(),
                Action::ManageLocalAdmins {
                    cfg_path: cfg_path.clone(),
                    panel: super::remote::Panel::Perms,
                },
            ));
        }
        // These depend on credential state the background probe reads, so they
        // appear once it lands. Only these — an early return here would also
        // drop the backup and uninstall entries below, which is precisely
        // backwards: a probe fails when the daemon is unreachable, which is
        // when an operator most needs the escape hatches.
        if let Some(credentials) = lca.credentials() {
            if credentials.auto_approve_present {
                items.push((
                    "Rotate Auto-Renew Credential".to_string(),
                    Action::AutoApprove {
                        rotate: true,
                        ca_dir: lca.ca_dir.clone(),
                        cfg: lca.cfg.clone(),
                    },
                ));
            } else {
                items.push((
                    "Enable Auto-Renew".to_string(),
                    Action::AutoApprove {
                        rotate: false,
                        ca_dir: lca.ca_dir.clone(),
                        cfg: lca.cfg.clone(),
                    },
                ));
            }
            if credentials.recovery_present {
                items.push((
                    "Rotate Recovery Password".to_string(),
                    Action::RecoveryRotate {
                        ca_dir: lca.ca_dir.clone(),
                        cfg: lca.cfg.clone(),
                    },
                ));
            }
            if credentials.external_signed {
                let emit_label = if credentials.external_installed {
                    "Emit Renewal CSR (External CA)"
                } else {
                    "Re-emit Signing CSR (External CA)"
                };
                items.push((
                    emit_label.to_string(),
                    Action::ExternalEmitCsr { ca_dir: lca.ca_dir.clone() },
                ));
                let label = if credentials.external_installed {
                    "Install Renewed Certificate (External CA)"
                } else {
                    "Install Signed Certificate (External CA)"
                };
                items.push((
                    label.to_string(),
                    Action::ExternalInstall { ca_dir: lca.ca_dir.clone() },
                ));
            }
        }
    }
    items.push((
        "Back Up This Install".to_string(),
        Action::Backup { config_root: d.config_dir.clone(), scope: d.scope },
    ));
    // One Uninstall item; when this host owns a CA, the confirm flow asks whether
    // to also destroy it (see the chained confirm in the App loop).
    items.push(("Uninstall".to_string(), uninstall_action(d, false)));
    items
}

/// The uninstall action for a detected install (also the `u` shortcut).
/// `remove_ca` additionally deletes the CA directory (only offered when this
/// host actually holds one — see [`owns_ca`]).
fn uninstall_action(d: &Detected, remove_ca: bool) -> Action {
    // A resolver/publisher registers a system-scope service even with user-scope
    // config, so removing it needs root.
    let needs_root = d.scope == ServiceScope::System
        || matches!(
            d.record.role,
            InstallRole::Ca | InstallRole::Resolver | InstallRole::Publisher
        );
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
    let body = "Choose a role to install — a Workstation for a laptop or desktop, a \
                Resolver to run an admin domain's directory, or a Publisher — or choose \
                Restore from Backup to recover any managed installation.";
    let prompt = " Press Enter to continue ";
    let w = 64.min(screen.width.saturating_sub(4)).max(24);
    let lines = vec![
        Line::from(Span::styled(
            heading,
            theme::panel_style().add_modifier(Modifier::BOLD),
        )),
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
        Paragraph::new(lines)
            .wrap(Wrap { trim: true })
            .style(theme::panel_style())
            .block(theme::dialog_block("Welcome to netidx")),
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
    match &r.admin_domain {
        Some(net) => {
            // The fingerprint is rendered in full beneath the glyph in the
            // right-hand column; repeating it here only overflows the label
            // column and wraps to column 0.
            lines.push(kv("Admin domain", net.domain.clone()));
        }
        None => lines.push(kv("Admin domain", "standalone (local-only)".to_string())),
    }
    if !r.admin_servers.is_empty() {
        let addrs: Vec<String> = r.admin_servers.iter().map(|a| a.to_string()).collect();
        lines.push(kv("Admin servers", addrs.join(", ")));
    }
    if let Some(lca) = &d.local_ca {
        match &lca.probe {
            CaProbe::Unprobed | CaProbe::Probing => {
                lines.push(kv_status(
                    "CA credentials",
                    "checking…".to_string(),
                    theme::HINT_FG,
                ));
            }
            CaProbe::Failed(e) => {
                lines.push(kv_status("CA credentials", e.clone(), theme::WARN));
            }
            CaProbe::Ready(credentials) => {
                let (aa, aa_c) = if credentials.auto_approve_present {
                    if credentials.auto_approve_wired {
                        ("active", theme::OK)
                    } else {
                        ("present (not wired)", theme::WARN)
                    }
                } else {
                    ("not set up", theme::HINT_FG)
                };
                lines.push(kv_status("Auto-approve", aa.to_string(), aa_c));
                let (rec, rec_c) = if credentials.recovery_present {
                    ("set", theme::OK)
                } else {
                    ("MISSING", theme::WARN)
                };
                lines.push(kv_status("Recovery slot", rec.to_string(), rec_c));
                if credentials.external_signed {
                    let (ext, ext_c) = if credentials.external_installed {
                        ("installed", theme::OK)
                    } else {
                        ("awaiting signature", theme::WARN)
                    };
                    lines.push(kv_status("External CA", ext.to_string(), ext_c));
                }
            }
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
        Span::styled(format!("{label:>17}: "), theme::hint_style()),
        Span::styled(value, theme::panel_style()),
    ])
}

/// A `label: value` line whose value carries a status colour.
fn kv_status(label: &'static str, value: String, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:>17}: "), theme::hint_style()),
        Span::styled(value, Style::default().bg(theme::PANEL_BG).fg(color)),
    ])
}

/// The status-card lines describing this install's admin domain-sync state — the
/// TUI counterpart of the CLI `status` "in sync / out of sync" report.
fn sync_lines(sync: &SyncState) -> Vec<Line<'static>> {
    match sync {
        // A local-only install never gets here (never checked); render nothing.
        SyncState::Unchecked => Vec::new(),
        SyncState::Checking => {
            vec![kv_status("Admin domain sync", "checking…".to_string(), theme::HINT_FG)]
        }
        SyncState::InSync => {
            vec![kv_status("Admin domain sync", "✓ in sync".to_string(), theme::OK)]
        }
        SyncState::Failed(e) => vec![kv_status(
            "Admin domain sync",
            format!("could not check ({e})"),
            theme::HINT_FG,
        )],
        SyncState::OutOfSync(changes) => {
            let mut lines = vec![kv_status(
                "Admin domain sync",
                format!("⚠ {} pending change(s) — press U to apply", changes.len()),
                theme::WARN,
            )];
            for c in changes {
                lines.push(Line::from(vec![
                    Span::raw(format!("{:>18}", "")),
                    Span::styled(
                        c.to_string(),
                        Style::default().bg(theme::PANEL_BG).fg(theme::WARN),
                    ),
                ]));
            }
            lines
        }
    }
}

/// Background admin domain-sync check for the given installs — the quiet counterpart
/// of the Update action. Runs the same reconcile the CLI `status`/`update` does
/// (via [`netidx_admin::sync::plan_for`]) and maps each result to a
/// [`SyncState`]. Self-contained (owns its inputs, borrows no UI state) so the
/// event loop can poll it as a background future without a `spawn`.
pub(super) async fn check_sync(
    pending: Vec<(usize, InstallRole, PathBuf)>,
) -> Vec<(usize, SyncState)> {
    let mut out = Vec::with_capacity(pending.len());
    for (i, role, config_root) in pending {
        let st = match netidx_admin::sync::plan_for(role, Some(&config_root)).await {
            Ok(plan) if plan.edits.is_empty() => SyncState::InSync,
            Ok(plan) => SyncState::OutOfSync(plan.edits.changes()),
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
        InstallRole::Ca => "CA",
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
    use netidx_admin::provenance::AdminDomainIdentity;
    use ratatui::{Terminal, backend::TestBackend};

    // The Local tab renders the same plan the CLI previews, so it owes the
    // operator the same verbs. It used to flatten each change to its subject
    // and prefix every line with `+`, which showed an auto-removal — the one
    // edit worth a second look before pressing U — as an addition.
    #[test]
    fn a_removal_renders_as_a_removal() {
        let lines = sync_lines(&SyncState::OutOfSync(vec![
            Change::Add("client resolver 10.0.0.16:4564 (tls)".into()),
            Change::Del("client resolver 10.0.0.99:4564".into()),
        ]));
        let rendered: Vec<String> = lines
            .iter()
            .map(|l| l.spans.iter().map(|s| s.content.as_ref()).collect())
            .collect();
        let body = rendered.join("\n");
        assert!(body.contains("+ add client resolver 10.0.0.16:4564"), "{body}");
        assert!(body.contains("- remove client resolver 10.0.0.99:4564"), "{body}");
        assert!(!body.contains("+ client resolver 10.0.0.99"), "{body}");
        // The summary counts edits; it must not call them all additions.
        assert!(body.contains("2 pending change(s)"), "{body}");
    }

    #[test]
    fn service_word_matches_state() {
        // The words Eric asked for in the install-title, e.g. `Resolver (running)`.
        assert_eq!(service_word(ServiceStatus::Active), "running");
        assert_eq!(service_word(ServiceStatus::Inactive), "stopped");
        assert_eq!(service_word(ServiceStatus::NotInstalled), "no service");
    }

    #[test]
    fn backup_action_is_explained_as_live_and_non_overwriting() {
        let action = Action::Backup {
            config_root: PathBuf::from("/etc/netidx"),
            scope: ServiceScope::System,
        };
        let desc = action_desc(&action);
        assert!(desc.contains("remains online"));
        assert!(desc.contains("complete managed installation"));
    }

    #[cfg(unix)]
    #[test]
    fn fresh_machine_offers_a_dedicated_ca_role() {
        assert_eq!(FRESH_CHOICES[0].action, FreshAction::Install(InstallRole::Ca));
        assert!(FRESH_CHOICES[0].title.contains("CA"));
        assert!(
            FRESH_CHOICES[0].blurb.contains("resolver servers are installed separately")
        );
    }

    #[test]
    fn fresh_machine_restore_is_an_explicit_menu_action() {
        let mut local = LocalState::new();
        if !local.is_fresh() {
            return;
        }
        local.welcome_seen = true;
        local.role_menu.select(Some(FRESH_CHOICES.len() - 1));
        assert_eq!(FRESH_CHOICES.last().unwrap().action, FreshAction::Restore);
        assert!(matches!(
            local.on_key(crossterm::event::KeyCode::Enter),
            Some(Action::Restore)
        ));
        assert!(local.on_key(crossterm::event::KeyCode::Char('r')).is_none());
    }

    #[test]
    fn status_glyph_not_clipped() {
        let fp = Fingerprint::of_der(b"installed admin domain CA");
        let detected = Detected {
            record: InstallRecord::new(
                InstallRole::Resolver,
                "/",
                "tls",
                Some(AdminDomainIdentity::new("example.com", &fp)),
                Some("127.0.0.1:4565".parse().unwrap()),
            ),
            service: ServiceStatus::Active,
            scope: ServiceScope::System,
            config_dir: PathBuf::from("/etc/netidx"),
            ca: Some(fp),
            local_ca: None,
            renewable: true,
        };
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
        terminal
            .draw(|f| render_status_overlay(f, f.area(), &detected, &SyncState::InSync))
            .unwrap();
        assert_eq!(
            widgets::rendered_identicon_rows(terminal.backend().buffer()),
            widgets::IDENTICON_HEIGHT as usize
        );
    }

    /// Both background fills are one-shot, so a transient failure would stick
    /// for the session. Returning to the tab must re-arm them — and must not
    /// disturb a probe still in flight.
    #[test]
    fn returning_to_the_tab_retries_what_failed() {
        let fp = Fingerprint::of_der(b"ca cert");
        // A resolver that owns a CA — the founding-host shape, and the one that
        // has both a sync check and a credential probe to retry. A pure CA
        // install is never sync-checked at all.
        let install = |probe: CaProbe| Detected {
            record: InstallRecord::new(
                InstallRole::Resolver,
                "/",
                "tls",
                Some(AdminDomainIdentity::new("example.com", &fp)),
                Some("127.0.0.1:4565".parse().unwrap()),
            ),
            service: ServiceStatus::Active,
            scope: ServiceScope::System,
            config_dir: PathBuf::from("/etc/netidx"),
            ca: Some(fp),
            local_ca: Some(LocalCa {
                ca_dir: PathBuf::from("/etc/netidx/ca"),
                cfg: Some(PathBuf::from("/etc/netidx/admin-server.json")),
                probe,
            }),
            renewable: true,
        };
        let mut local = LocalState::new();
        local.installs = vec![
            install(CaProbe::Failed("daemon not responding".to_string())),
            install(CaProbe::Probing),
        ];
        local.sync =
            vec![SyncState::Failed("unreachable".to_string()), SyncState::Checking];

        // Nothing is retried while the tab is away.
        assert!(local.take_pending_ca_probes().is_empty());
        assert!(local.take_pending_checks().is_empty());

        local.on_focus();
        // The failed pair is re-armed; the in-flight pair is untouched.
        assert_eq!(local.take_pending_ca_probes().len(), 1);
        assert_eq!(local.take_pending_checks().len(), 1);
        assert!(matches!(local.sync[1], SyncState::Checking));
        assert!(matches!(
            local.installs[1].local_ca.as_ref().unwrap().probe,
            CaProbe::Probing
        ));

        // Taking them marked both in flight, so a second focus is a no-op —
        // one probe per failure, not one per keypress.
        local.on_focus();
        assert!(local.take_pending_ca_probes().is_empty());
        assert!(local.take_pending_checks().is_empty());
    }

    /// A krb5 host joins an admin domain without enrolling a certificate, so it
    /// has nothing to renew — the offer must follow the identities renewd would
    /// actually find, not admin domain membership.
    #[test]
    fn a_host_with_no_certificates_is_not_offered_renewal() {
        let fp = Fingerprint::of_der(b"krb5 admin domain CA");
        let detected = |auth: &str, renewable: bool| Detected {
            record: InstallRecord::new(
                InstallRole::Publisher,
                "/",
                auth,
                Some(AdminDomainIdentity::new("example.com", &fp)),
                Some("127.0.0.1:4565".parse().unwrap()),
            ),
            service: ServiceStatus::NotInstalled,
            scope: ServiceScope::System,
            config_dir: PathBuf::from("/etc/netidx"),
            ca: Some(fp),
            local_ca: None,
            renewable,
        };
        let offers_renewal = |d: &Detected| {
            action_items(d).iter().any(|(_, a)| matches!(a, Action::Renew { .. }))
        };
        assert!(!offers_renewal(&detected("krb5", false)));
        assert!(offers_renewal(&detected("tls", true)));

        // Nor may the `r` shortcut run what the menu doesn't offer.
        let mut local = LocalState::new();
        local.installs = vec![detected("krb5", false)];
        local.sync = vec![SyncState::Unchecked];
        local.selected = 0;
        assert!(local.on_key(crossterm::event::KeyCode::Char('r')).is_none());
        local.installs = vec![detected("tls", true)];
        assert!(matches!(
            local.on_key(crossterm::event::KeyCode::Char('r')),
            Some(Action::Renew { .. })
        ));
    }

    #[test]
    fn system_install_actions_keep_the_selected_config_root() {
        let fp = Fingerprint::of_der(b"system install CA");
        let config_root = PathBuf::from("/etc/netidx");
        let detected = Detected {
            record: InstallRecord::new(
                InstallRole::Resolver,
                "/",
                "tls",
                Some(AdminDomainIdentity::new("example.com", &fp)),
                Some("127.0.0.1:4565".parse().unwrap()),
            ),
            service: ServiceStatus::Active,
            scope: ServiceScope::System,
            config_dir: config_root.clone(),
            ca: Some(fp),
            local_ca: None,
            renewable: true,
        };
        let actions = action_items(&detected);
        assert!(actions.iter().any(|(_, action)| matches!(
            action,
            Action::Update { config_root: root, .. } if root == &config_root
        )));
        assert!(actions.iter().any(|(_, action)| matches!(
            action,
            Action::AddParent { config_root: root } if root == &config_root
        )));
    }

    /// A CA install renders and offers its no-credential actions before the
    /// background probe has answered — the probe drives the local control
    /// socket, so the UI must never be waiting on it.
    #[test]
    fn a_ca_install_is_usable_before_its_credential_probe_lands() {
        let fp = Fingerprint::of_der(b"ca cert");
        let cfg = PathBuf::from("/etc/netidx/admin-server.json");
        let detected = |probe: CaProbe| Detected {
            record: InstallRecord::new(
                InstallRole::Ca,
                "/",
                "tls",
                Some(AdminDomainIdentity::new("example.com", &fp)),
                Some("127.0.0.1:4565".parse().unwrap()),
            ),
            service: ServiceStatus::Active,
            scope: ServiceScope::System,
            config_dir: PathBuf::from("/etc/netidx"),
            ca: Some(fp),
            local_ca: Some(LocalCa {
                ca_dir: PathBuf::from("/etc/netidx/CA"),
                cfg: Some(cfg.clone()),
                probe,
            }),
            renewable: true,
        };

        // Unprobed: the socket-free actions are already there, the
        // credential-dependent ones are not, and the status overlay says so.
        let unprobed = detected(CaProbe::Unprobed);
        let labels = |d: &Detected| -> Vec<String> {
            action_items(d).into_iter().map(|(label, _)| label).collect()
        };
        assert!(labels(&unprobed).iter().any(|l| l == "Admins"));
        assert!(!labels(&unprobed).iter().any(|l| l.contains("Auto-Renew")));
        // Only the credential-dependent entries wait on the probe. Backup and
        // uninstall sit after them in the list and must not be truncated away —
        // a wedged daemon is exactly when they are wanted.
        assert!(labels(&unprobed).iter().any(|l| l == "Back Up This Install"));
        assert!(labels(&unprobed).iter().any(|l| l == "Uninstall"));
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
        terminal
            .draw(|f| render_status_overlay(f, f.area(), &unprobed, &SyncState::InSync))
            .unwrap();
        let rendered = terminal.backend().buffer().content().iter().fold(
            String::new(),
            |mut acc, cell| {
                acc.push_str(cell.symbol());
                acc
            },
        );
        assert!(rendered.contains("checking"), "{rendered}");

        // Probed: the credential-dependent actions appear.
        let ready = detected(CaProbe::Ready(CaCredentials {
            auto_approve_present: true,
            auto_approve_wired: true,
            recovery_present: true,
            external_signed: false,
            external_installed: false,
        }));
        assert!(labels(&ready).iter().any(|l| l == "Rotate Auto-Renew Credential"));
        assert!(labels(&ready).iter().any(|l| l == "Rotate Recovery Password"));

        // A failed probe surfaces the reason instead of silently hiding them.
        let failed = detected(CaProbe::Failed("daemon not responding".to_string()));
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).unwrap();
        terminal
            .draw(|f| render_status_overlay(f, f.area(), &failed, &SyncState::InSync))
            .unwrap();
        let rendered = terminal.backend().buffer().content().iter().fold(
            String::new(),
            |mut acc, cell| {
                acc.push_str(cell.symbol());
                acc
            },
        );
        assert!(rendered.contains("not responding"), "{rendered}");
        assert!(labels(&failed).iter().any(|l| l == "Back Up This Install"));
        assert!(labels(&failed).iter().any(|l| l == "Uninstall"));
    }
}
