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
}

impl Panel {
    fn title(self) -> &'static str {
        match self {
            Panel::Queue => "Enrollment queue",
        }
    }
}

/// A rendered panel row: display text plus the opaque key an action needs (a
/// full code / name / serial). Cross-platform — the op formats `admin_ops` rows
/// into these so unix-only types never reach the UI state.
#[derive(Clone)]
pub(super) struct PanelRow {
    text: String,
    /// The full code an action on this row uses, when it has one (a renewal or
    /// a code-free row has `None`, so per-row approve/deny is a no-op there).
    code: Option<String>,
}

/// A remote-admin op the event loop runs (via [`Action::Remote`]).
pub(super) enum RemoteAction {
    /// Establish the session to `server` (glyph confirm + login).
    Connect { server: SocketAddr },
    /// (Re)list a panel.
    Refresh { conn: RemoteConn, panel: Panel },
    /// Approve one queued enrollment by its full code.
    Approve { conn: RemoteConn, code: String },
    /// Approve every verified renewal (code-free batch).
    ApproveRenewals { conn: RemoteConn },
    /// Deny one queued enrollment by its full code (reason prompted).
    Deny { conn: RemoteConn, code: String },
}

impl RemoteAction {
    pub(super) fn label(&self) -> String {
        match self {
            RemoteAction::Connect { .. } => "Connecting".to_string(),
            RemoteAction::Refresh { .. } => "Loading".to_string(),
            RemoteAction::Approve { .. } => "Approving".to_string(),
            RemoteAction::ApproveRenewals { .. } => "Approving renewals".to_string(),
            RemoteAction::Deny { .. } => "Denying".to_string(),
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
            | RemoteAction::Deny { conn, .. } => Some(conn.confirmed_fp),
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
        RemoteAction::Refresh { conn, panel } => refresh(ans, conn, panel).await,
        RemoteAction::Approve { conn, code } => approve(ans, conn, code).await,
        RemoteAction::ApproveRenewals { conn } => approve_renewals(ans, conn).await,
        RemoteAction::Deny { conn, code } => deny(ans, conn, code).await,
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
) -> Result<super::action::Outcome> {
    let rows = match panel {
        Panel::Queue => queue_rows(ans, &conn).await?,
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
            code: None,
        };
    }
    let code = item.code.as_ref().map(|c| c.text());
    let tail = match &code {
        Some(_) => format!("{:?}  ({}, from {})", item.kind, widgets::fmt_age(item.age_secs), item.peer),
        None => format!("{:?}  (unparseable CSR — deny only)", item.kind),
    };
    PanelRow { text: format!("{name}  {tail}"), code }
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

// ---- UI state (cross-platform) --------------------------------------------

/// Which Tab-2 screen is showing.
enum Screen {
    /// Enter an admin-server address to connect to.
    Connect,
    /// Pick a panel.
    Menu,
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
}

/// The panels offered in the menu (label + which panel).
const PANELS: [Panel; 1] = [Panel::Queue];

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
                if let Some(conn) = &self.conn {
                    self.list.select(None);
                    self.rows.clear();
                    return Some(Action::Remote(RemoteAction::Refresh { conn: conn.clone(), panel }));
                }
            }
            _ => {}
        }
        None
    }

    fn on_key_panel(&mut self, code: KeyCode, panel: Panel) -> Option<Action> {
        let conn = self.conn.clone()?;
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.list.select_previous(),
            KeyCode::Down | KeyCode::Char('j') => self.list.select_next(),
            KeyCode::Esc => self.screen = Screen::Menu,
            KeyCode::Char('r') => {
                return Some(Action::Remote(RemoteAction::Refresh { conn, panel }));
            }
            _ => match panel {
                Panel::Queue => return self.on_key_queue(code, conn),
            },
        }
        None
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

    /// The full code of the selected row, if it has one (not a renewal/unparseable).
    fn selected_code(&self) -> Option<String> {
        self.rows.get(self.list.selected()?)?.code.clone()
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
            Screen::Panel(panel) => self.render_panel(f, area, *panel),
        }
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
        };
        let mut st = self.list.clone();
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(" {} ", panel.title()))
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
