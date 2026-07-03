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
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};

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
    config_root: String,
    ca: Option<Fingerprint>,
}

impl Detected {
    fn probe(record: InstallRecord, config_root: String) -> Detected {
        let ca = record
            .network
            .as_ref()
            .and_then(|n| Fingerprint::parse_text(&n.ca_fingerprint).ok());
        let service = probe_service(&record);
        Detected { record, service, config_root, ca }
    }
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
        let root = paths::user_config_root()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        out.push(Detected::probe(record, root));
    }
    let sys_path = paths::system_install_record();
    // Skip the system record if it's the very same file we already read as the
    // user record (unusual, but possible if the two roots coincide).
    if sys_path.exists() && user_path.as_deref() != Some(sys_path.as_path()) {
        if let Ok(record) = InstallRecord::load(&sys_path) {
            out.push(Detected::probe(
                record,
                paths::system_config_root().display().to_string(),
            ));
        }
    }
    out
}

/// Tab-1 state: the detected installs plus the role-menu cursor for the
/// fresh-machine case.
pub(super) struct LocalState {
    installs: Vec<Detected>,
    role_menu: ListState,
    /// Which detected install the lifecycle actions apply to.
    selected: usize,
}

impl LocalState {
    pub(super) fn new() -> LocalState {
        let mut role_menu = ListState::default();
        role_menu.select(Some(0));
        LocalState { installs: detect(), role_menu, selected: 0 }
    }

    /// Re-run detection (after an install/uninstall completes).
    pub(super) fn refresh(&mut self) {
        self.installs = detect();
        self.selected = self.selected.min(self.installs.len().saturating_sub(1));
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
        match code {
            Up | Char('k') if self.selected > 0 => self.selected -= 1,
            Down | Char('j') if self.selected + 1 < self.installs.len() => self.selected += 1,
            Char('u') => {
                let d = &self.installs[self.selected];
                return Some(Action::Uninstall { scope: scope_of(d.record.role), remove_ca: false });
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
            render_install(f, d, *cell, i == self.selected);
        }
    }
}

/// Which service scope a role's OS service lives at — the scope its lifecycle
/// actions (uninstall) operate on. Mirrors [`probe_service`].
fn scope_of(role: InstallRole) -> ServiceScope {
    match role {
        InstallRole::Workstation => ServiceScope::User,
        InstallRole::Resolver | InstallRole::Publisher => ServiceScope::System,
    }
}

/// Render one detected install: a details column on the left and, when the host
/// joined a network, its CA identicon + fingerprint on the right. The selected
/// install is highlighted and shows its action keys.
fn render_install(f: &mut Frame, d: &Detected, area: Rect, selected: bool) {
    let title = format!(" {} ", role_title(d.record.role));
    let mut block = Block::default().borders(Borders::ALL).title(title);
    if selected {
        let hints = if d.record.network.is_some() {
            " u uninstall · r renew "
        } else {
            " u uninstall "
        };
        block = block
            .border_style(Style::default().fg(Color::Cyan))
            .title_bottom(Line::from(hints).dim());
    }
    let inner = block.inner(area);
    f.render_widget(block, area);

    let has_glyph = d.ca.is_some();
    let cols = if has_glyph {
        Layout::horizontal([Constraint::Min(0), Constraint::Length(20)]).split(inner)
    } else {
        Layout::horizontal([Constraint::Min(0)]).split(inner)
    };

    f.render_widget(Paragraph::new(detail_lines(d)).wrap(Wrap { trim: false }), cols[0]);

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
    lines.push(kv("Config", d.config_root.clone()));
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
