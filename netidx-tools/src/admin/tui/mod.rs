//! The interactive `netidx admin` TUI.
//!
//! Bare `netidx admin` (no subcommand) lands here. Where the strict CLI is a
//! thin non-interactive shell over `netidx_admin::{plan, admin_ops}` driven by
//! a [`FlagAnswerer`](super::answer_cli::FlagAnswerer), the TUI drives the same
//! library functions through a widget-backed [`TuiAnswerer`](answer::TuiAnswerer):
//! one code path, two frontends. It owns the process's only tokio runtime and
//! `.await`s those futures directly.
//!
//! The goal is that the strict CLI is reserved for automation and the TUI is the
//! better choice for everything else: it shows live system status and offers a
//! guided interface for install and post-install administration.
//!
//! ## Shape
//!
//! One tokio task (the one `run` blocks on) owns the terminal, the widget state,
//! and the crossterm event stream. An [`Action`] runs on a *spawned* task with a
//! [`TuiAnswerer`](answer::TuiAnswerer); every question it asks arrives here as a
//! [`UiRequest`] over a channel, is answered through a modal, and the reply is
//! sent back over a `oneshot`. The op's final result returns as
//! [`UiRequest::OpDone`].

mod action;
mod answer;
mod lifecycle;
mod local;
mod privileged;
mod remote;
mod theme;
mod widgets;

use action::{Action, Outcome};
use answer::{Modal, TuiAnswerer, UiRequest};
use anyhow::{Context, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::{StreamExt, stream::Fuse};
use netidx_admin::{
    answer::{Progress, Stage},
    fingerprint::Fingerprint,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Gauge, Paragraph, Tabs, Wrap},
};
use std::{
    collections::VecDeque,
    future::{self, Future},
    pin::Pin,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

/// Which top-level tab is focused.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    /// This machine: detected install, status, and install/lifecycle actions.
    Local,
    /// A remote (or this host's) admin server: enrollments, roster, perms, …
    Remote,
}

impl Tab {
    const ALL: [Tab; 2] = [Tab::Local, Tab::Remote];

    fn title(self) -> &'static str {
        match self {
            Tab::Local => "Local",
            Tab::Remote => "Cluster",
        }
    }

    fn index(self) -> usize {
        Tab::ALL.iter().position(|t| *t == self).unwrap_or(0)
    }
}

/// A finished action's summary, shown as a dismissible overlay.
struct ResultView {
    title: String,
    lines: Vec<String>,
    error: bool,
}

/// Top-level TUI state.
struct App {
    tab: Tab,
    local: local::LocalState,
    remote: remote::RemoteState,
    should_quit: bool,
    /// The question currently awaiting an answer, if any.
    modal: Option<Modal>,
    /// Blocking requests that arrived while a modal was already up.
    pending: VecDeque<UiRequest>,
    /// True while an action runs (its future is polled in the event loop).
    busy: bool,
    /// Label of the running action, for the activity header.
    activity: Option<String>,
    /// The current progress step and when it started (for a determinate bar).
    /// `Some` ⇒ show the progress modal.
    progress: Option<(Progress, Instant)>,
    /// Animation frame counter, advanced by the UI loop's timer while a progress
    /// modal is up — drives the indeterminate marquee.
    tick: u64,
    /// An out-of-band verification code the operator must relay while an action
    /// waits for a remote admin to approve.
    verification: Option<(String, Fingerprint)>,
    /// Notes and warnings from the running (or last) action.
    log: Vec<Line<'static>>,
    /// A finished action's result overlay.
    result: Option<ResultView>,
    /// A destructive action awaiting yes/no confirmation before it runs.
    confirm: Option<(String, Action)>,
}

impl App {
    fn new() -> App {
        App {
            tab: Tab::Local,
            local: local::LocalState::new(),
            remote: remote::RemoteState::new(),
            should_quit: false,
            modal: None,
            pending: VecDeque::new(),
            busy: false,
            activity: None,
            progress: None,
            tick: 0,
            verification: None,
            log: Vec::new(),
            result: None,
            confirm: None,
        }
    }

    /// Advance to the next tab (wrapping).
    fn next_tab(&mut self) {
        let i = (self.tab.index() + 1) % Tab::ALL.len();
        self.tab = Tab::ALL[i];
        // The connect default is read from this host's admin-server config,
        // which may have been created (a fresh CA install) after the TUI
        // started — so re-read it when the Remote tab gains focus.
        if self.tab == Tab::Remote {
            self.remote.refresh_connect_default();
        }
    }

    /// Switch the UI into the activity view for a just-started action.
    fn begin(&mut self, label: String) {
        self.busy = true;
        self.activity = Some(label);
        self.progress = None;
        self.verification = None;
        self.log.clear();
        self.result = None;
    }

    /// Record a finished action's result. Leaves any still-open modal (e.g. an
    /// un-acknowledged recovery-password modal) in place — it renders on top.
    fn finish_op(&mut self, out: Result<Outcome>) {
        self.busy = false;
        self.activity = None;
        self.progress = None;
        self.verification = None;
        match out {
            Ok(out) => {
                if out.refresh_local {
                    self.local.refresh();
                }
                if let Some(update) = out.remote {
                    // Route panel rows to whichever surface launched the op — the
                    // Local tab's local-admin surface, or the Cluster tab. Input
                    // is blocked while busy, so this state matches the launch.
                    if self.tab == Tab::Local && self.local.admin_open() {
                        self.local.apply_admin(update);
                    } else {
                        self.remote.apply(update);
                    }
                }
                // A quiet result (a silent panel re-query) shows no overlay.
                if !out.quiet {
                    self.result =
                        Some(ResultView { title: out.title, lines: out.lines, error: false });
                }
            }
            Err(e) => {
                self.result = Some(ResultView {
                    title: "Failed".to_string(),
                    lines: vec![format!("{e:#}")],
                    error: true,
                });
            }
        }
    }

    /// Apply a request from the running action. Blocking requests become a
    /// modal, queued behind any modal already up so none is ever lost.
    fn handle_request(&mut self, req: UiRequest) {
        match req {
            UiRequest::Note(m) => self.log.push(Line::from(m)),
            UiRequest::Warn(m) => self.log.push(Line::from(Span::styled(
                format!("warning: {m}"),
                Style::default().fg(Color::Yellow),
            ))),
            UiRequest::Progress(p) => self.progress = Some((p, Instant::now())),
            UiRequest::VerificationCode { purpose, code } => {
                self.verification = Some((purpose, code))
            }
            blocking => {
                // A question supersedes the progress bar: the op is no longer
                // working, it's waiting on the operator.
                self.progress = None;
                if self.modal.is_some() {
                    self.pending.push_back(blocking);
                } else {
                    self.modal = Modal::from_request(blocking);
                }
            }
        }
    }

    /// A modal just resolved: drop it and promote the next queued question.
    fn advance_modal(&mut self) {
        self.modal = self.pending.pop_front().and_then(Modal::from_request);
    }

    /// Route a key press. Global keys first, then the active overlay
    /// (modal → confirm → result), then the focused tab. Returns an [`Action`]
    /// for the event loop to run — either straight from the tab (no confirmation
    /// needed) or from a just-accepted confirmation.
    fn on_key(&mut self, code: KeyCode, mods: KeyModifiers) -> Option<Action> {
        if let KeyCode::Char('c') = code {
            if mods.contains(KeyModifiers::CONTROL) {
                self.should_quit = true;
                return None;
            }
        }
        if self.modal.is_some() {
            if self.modal.as_mut().unwrap().on_key(code) {
                self.advance_modal();
            }
            return None;
        }
        if self.confirm.is_some() {
            return match code {
                KeyCode::Char('y') | KeyCode::Char('Y') => self.confirm.take().map(|(_, a)| a),
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.confirm = None;
                    None
                }
                _ => None,
            };
        }
        if self.result.is_some() {
            self.result = None;
            return None;
        }
        if self.busy {
            return None;
        }
        let action = match code {
            KeyCode::Char('q') => {
                self.should_quit = true;
                return None;
            }
            KeyCode::Tab => {
                // No tabs on a fresh machine — there's nothing to switch to.
                if self.local.installed() {
                    self.next_tab();
                }
                return None;
            }
            // A fresh machine has only the install flow; route everything there.
            _ if !self.local.installed() => self.local.on_key(code),
            _ => match self.tab {
                Tab::Local => self.local.on_key(code),
                Tab::Remote => self.remote.on_key(code),
            },
        }?;
        match action {
            // Pure navigation: jump to the Remote tab's delegation panel.
            Action::ReviewDelegations => {
                self.tab = Tab::Remote;
                self.remote.focus_delegations();
                None
            }
            // Pure navigation: open the Local tab's local admin panel surface.
            Action::ManageLocalAdmins { cfg_path, ca_dir } => {
                self.local.open_admin(cfg_path, ca_dir);
                None
            }
            // Gate destructive actions behind a yes/no confirmation.
            action => match action.confirm_message() {
                Some(msg) => {
                    self.confirm = Some((msg, action));
                    None
                }
                None => Some(action),
            },
        }
    }

    fn render(&mut self, f: &mut Frame) {
        let screen = f.area();
        // Fill the whole screen with the installer backdrop so nothing renders
        // blue-on-blue and every panel floats on the blue field.
        f.render_widget(Block::default().style(theme::backdrop_style()), screen);
        if self.busy {
            // Full-screen wizard: the tab bar goes away while an op runs (you
            // can't do remote admin mid-install).
            let chunks =
                Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(screen);
            self.render_activity(f, chunks[0]);
            self.render_footer(f, chunks[1]);
        } else if self.local.installed() {
            let chunks = Layout::vertical([
                Constraint::Length(3), // tab bar
                Constraint::Min(0),    // body
                Constraint::Length(1), // footer / key hints
            ])
            .split(screen);
            self.render_tabs(f, chunks[0]);
            match self.tab {
                Tab::Local => self.local.render(f, chunks[1]),
                Tab::Remote => self.remote.render(f, chunks[1]),
            }
            self.render_footer(f, chunks[2]);
        } else {
            // Fresh machine: the install flow only — no tabs, no remote admin
            // (there's no local cluster to administer yet).
            let chunks =
                Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(screen);
            self.local.render(f, chunks[0]);
            self.render_footer(f, chunks[1]);
        }
        // Overlays, highest precedence first: a question hides the progress bar
        // (the op is waiting on the operator, not working).
        if let Some(m) = &self.modal {
            m.render(f, screen);
        } else if let Some((msg, _)) = &self.confirm {
            render_confirm(f, screen, msg);
        } else if let Some(r) = &self.result {
            render_result(f, screen, r);
        } else if self.progress.is_some() {
            self.render_progress(f, screen);
        }
    }

    fn render_tabs(&self, f: &mut Frame, area: Rect) {
        let titles = Tab::ALL.iter().map(|t| Line::from(t.title()));
        let tabs = Tabs::new(titles)
            .select(self.tab.index())
            .style(theme::panel_style())
            .block(theme::panel_block().title(Span::styled(" netidx admin ", theme::title_style())))
            .highlight_style(theme::selected_style());
        f.render_widget(tabs, area);
    }

    /// The busy view — a full-screen console dialog (gray panel with a thin blue
    /// fringe, never bare text on the backdrop) titled with the running action,
    /// showing its notes/warnings so far. The live progress and any verification
    /// code ride in the progress modal on top (see [`Self::render_progress`]);
    /// this is what's behind it.
    fn render_activity(&self, f: &mut Frame, area: Rect) {
        let title = self.activity.clone().unwrap_or_else(|| "Working".to_string());
        let dlg = area.inner(Margin::new(1, 1));
        let block = theme::dialog_block(&title);
        let inner = block.inner(dlg);
        f.render_widget(block, dlg);
        let mut lines: Vec<Line> = Vec::new();
        // Fallback: show the verification code here only if no progress modal is
        // up to carry it.
        if self.progress.is_none()
            && let Some((purpose, code)) = &self.verification
        {
            let accent =
                Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT).add_modifier(Modifier::BOLD);
            lines.push(Line::from(Span::styled(
                format!("{purpose} — read this code to the approving admin:"),
                accent,
            )));
            lines.push(Line::from(Span::styled(code.text(), theme::title_style())));
            lines.extend(widgets::identicon_lines(code));
            lines.push(Line::from(""));
        }
        lines.extend(self.log.iter().cloned());
        f.render_widget(
            Paragraph::new(lines).wrap(Wrap { trim: false }).style(theme::panel_style()),
            inner,
        );
    }

    /// The progress dialog floating in front of the busy backdrop: the current
    /// step, any verification code the operator must relay, and an animated bar
    /// (determinate over a known duration, else an indeterminate marquee).
    fn render_progress(&self, f: &mut Frame, screen: Rect) {
        let Some((progress, started)) = &self.progress else { return };
        let mut lines: Vec<Line> =
            vec![Line::from(Span::styled(progress.message.to_string(), theme::panel_style()))];
        if let Some((purpose, code)) = &self.verification {
            let accent = Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT).add_modifier(Modifier::BOLD);
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("{purpose} — read this code to the approving admin:"),
                accent,
            )));
            lines.push(Line::from(Span::styled(code.text(), theme::title_style())));
            lines.extend(widgets::identicon_lines(code));
        }
        let body_h = lines.len() as u16;
        let h = (body_h + 5).min(screen.height);
        let area = widgets::centered(60, h, screen);
        widgets::shadow(f, area, screen);
        f.render_widget(Clear, area);
        let block = theme::dialog_block(stage_title(progress.stage));
        let inner = block.inner(area);
        f.render_widget(block, area);
        let rows = Layout::vertical([
            Constraint::Length(body_h), // message + verification
            Constraint::Length(1),      // spacer
            Constraint::Length(1),      // bar
            Constraint::Min(0),
        ])
        .split(inner);
        f.render_widget(
            Paragraph::new(lines).wrap(Wrap { trim: true }).style(theme::panel_style()),
            rows[0],
        );
        match progress.duration {
            Some(dur) => {
                let ratio = (started.elapsed().as_secs_f64() / dur.as_secs_f64().max(0.001)).clamp(0.0, 1.0);
                let gauge = Gauge::default()
                    .gauge_style(Style::default().bg(theme::TROUGH).fg(theme::ACCENT))
                    .ratio(ratio)
                    .label(format!("{}%", (ratio * 100.0) as u16));
                f.render_widget(gauge, rows[2]);
            }
            None => f.render_widget(Paragraph::new(widgets::marquee(self.tick, rows[2].width)), rows[2]),
        }
    }

    fn render_footer(&self, f: &mut Frame, area: Rect) {
        let base = theme::backdrop_style();
        let key = base.add_modifier(Modifier::BOLD);
        let hint = if self.result.is_some() {
            Line::from(Span::styled(" any key to dismiss ", base))
        } else if self.modal.is_some() {
            Line::from(Span::styled(" answer above · Esc cancel ", base))
        } else if self.confirm.is_some() {
            Line::from(Span::styled(" y confirm · n/Esc cancel ", base))
        } else if self.busy {
            Line::from(Span::styled(" working… · Ctrl-C quit ", base))
        } else if self.local.installed() {
            Line::from(vec![
                Span::styled(" Tab", key),
                Span::styled(" switch  ", base),
                Span::styled("↑/↓", key),
                Span::styled(" navigate  ", base),
                Span::styled("q", key),
                Span::styled(" quit", base),
            ])
        } else {
            Line::from(vec![
                Span::styled(" ↑/↓", key),
                Span::styled(" navigate  ", base),
                Span::styled("q", key),
                Span::styled(" quit", base),
            ])
        };
        f.render_widget(Paragraph::new(hint).style(base), area);
    }
}

/// A human title for the current progress step.
fn stage_title(stage: Stage) -> &'static str {
    match stage {
        Stage::Discovering => "Searching the network",
        Stage::Enrolling => "Enrolling",
        Stage::WaitingApproval => "Waiting for approval",
        Stage::Applying => "Applying",
        Stage::Done => "Working",
    }
}

/// Render a finished action's result as a dismissible centered overlay.
fn render_result(f: &mut Frame, screen: Rect, r: &ResultView) {
    let lines: Vec<Line> = r
        .lines
        .iter()
        .flat_map(|l| l.split('\n'))
        .map(|s| Line::from(Span::styled(s.to_string(), theme::panel_style())))
        .collect();
    let h = (lines.len() as u16 + 4).clamp(6, screen.height);
    let area = widgets::centered(72, h, screen);
    widgets::shadow(f, area, screen);
    f.render_widget(Clear, area);
    let title_style = if r.error {
        Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT).add_modifier(Modifier::BOLD)
    } else {
        theme::title_style()
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::panel_style())
        .style(theme::panel_style())
        .title(Span::styled(format!(" {} ", r.title), title_style))
        .title_bottom(Line::from(Span::styled(" any key to dismiss ", theme::hint_style())));
    f.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }).style(theme::panel_style()).block(block),
        area,
    );
}

/// Render a destructive/verification yes/no confirmation as a centered overlay.
/// The message may contain `\n` (e.g. an approval showing the request code on its
/// own line); each becomes its own wrapped line and the popup sizes to fit.
fn render_confirm(f: &mut Frame, screen: Rect, msg: &str) {
    let mut lines: Vec<Line> =
        msg.split('\n').map(|l| Line::from(Span::styled(l.to_string(), theme::panel_style()))).collect();
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(" y confirm · n cancel ", theme::hint_style())));
    let h = (lines.len() as u16 + 2).clamp(8, screen.height);
    let area = widgets::centered(70, h, screen);
    widgets::shadow(f, area, screen);
    f.render_widget(Clear, area);
    let title = Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT).add_modifier(Modifier::BOLD);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::panel_style())
        .style(theme::panel_style())
        .title(Span::styled(" Confirm ", title));
    f.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: true }).style(theme::panel_style()).block(block),
        area,
    );
}

/// The type of a running action's future: self-contained (owns its answerer),
/// so it needs no borrow of the UI state and is driven directly in the loop.
type OpFuture = Pin<Box<dyn Future<Output = Result<Outcome>>>>;

/// Run the async event loop over `terminal` until the user quits.
///
/// One task owns everything. A running action is an [`OpFuture`] polled by a
/// `select!` arm; when it `.await`s a modal answer it simply pends, and the same
/// loop delivers the [`UiRequest`] and (later) the key that unblocks it. No
/// spawn, so no `Send`/`'static` plumbing and no dyn-`AsyncFnOnce` lifetime
/// grief.
async fn run_app(terminal: &mut ratatui::DefaultTerminal) -> Result<()> {
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiRequest>();
    let mut app = App::new();
    let mut op: Option<OpFuture> = None;
    // A quiet background network-sync check for the Local tab's installs. Runs
    // off the main op slot so the status card renders instantly from local
    // files and gains its sync line once the network answers; never sets `busy`.
    type SyncFuture = Pin<Box<dyn Future<Output = Vec<(usize, local::SyncState)>>>>;
    let mut sync_op: Option<SyncFuture> = None;
    // The crossterm reader. `None` only during a terminal-suspend, so its
    // background thread can't fight the child (editor / sudo) for stdin.
    let mut events: Option<Fuse<EventStream>> = Some(EventStream::new().fuse());
    // Animation clock — only consulted while a progress modal is up, so it costs
    // nothing when idle.
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    while !app.should_quit {
        terminal.draw(|f| app.render(f))?;
        let animating = app.progress.is_some();
        // Kick off the sync check when the Local tab is focused and has an
        // unchecked networked install. `take_pending_checks` marks them
        // `Checking`, so this launches exactly one check per install.
        if app.tab == Tab::Local && sync_op.is_none() {
            let pending = app.local.take_pending_checks();
            if !pending.is_empty() {
                sync_op = Some(Box::pin(local::check_sync(pending)));
            }
        }
        tokio::select! {
            biased;
            Some(req) = ui_rx.recv() => match req {
                // The editor request owns the terminal (suspend → $EDITOR →
                // resume), so the loop services it here rather than as a modal.
                answer::UiRequest::Editor { seed, validate, reply } => {
                    let edited =
                        run_suspended(&mut events, || privileged::edit_in_terminal(terminal, &seed, validate));
                    let _ = reply.send(edited);
                }
                other => app.handle_request(other),
            },
            out = async { match op.as_mut() { Some(f) => f.await, None => future::pending().await } } => {
                op = None;
                let result = complete_op(terminal, &mut app, &mut events, out);
                app.finish_op(result);
            }
            ev = async {
                match events.as_mut() {
                    Some(e) => e.select_next_some().await,
                    None => future::pending().await,
                }
            } => match ev {
                Ok(Event::Key(k)) if k.kind == KeyEventKind::Press => {
                    if let Some(action) = app.on_key(k.code, k.modifiers) {
                        launch(terminal, &mut app, &ui_tx, &mut op, &mut events, action);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    log::error!("terminal event error: {e:?}");
                    break;
                }
            },
            // Lowest priority: a finished background sync check just fills in the
            // Local tab's sync lines — no overlay, no `busy`.
            synced = async {
                match sync_op.as_mut() {
                    Some(f) => f.await,
                    None => future::pending().await,
                }
            } => {
                sync_op = None;
                app.local.apply_sync(synced);
            }
            // Lowest priority: advance the animation frame while a progress
            // modal is up so the bar/marquee redraws; inert otherwise.
            _ = async {
                if animating {
                    ticker.tick().await;
                } else {
                    future::pending::<()>().await
                }
            } => {
                app.tick = app.tick.wrapping_add(1);
            }
        }
    }
    Ok(())
}

/// Run `f` — a terminal-owning child (the `$EDITOR`, or a privileged
/// subprocess) — with the crossterm reader stopped. Its background thread reads
/// stdin continuously, so leaving it alive during a full-screen child (vim)
/// steals the child's keystrokes and desyncs the escape-sequence parser on
/// resume (Enter/Esc stop arriving). Dropping it first frees stdin; a fresh
/// reader afterward starts with a clean parser and discards whatever the child
/// left buffered — exactly what we want.
fn run_suspended<T>(events: &mut Option<Fuse<EventStream>>, f: impl FnOnce() -> T) -> T {
    *events = None;
    let out = f();
    *events = Some(EventStream::new().fuse());
    out
}

/// Start an action: op-future actions (install / renew) become the polled
/// `op`; the privileged, synchronous uninstall runs inline (it owns the
/// terminal to suspend for a password prompt).
fn launch(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    ui_tx: &mpsc::UnboundedSender<UiRequest>,
    op: &mut Option<OpFuture>,
    events: &mut Option<Fuse<EventStream>>,
    action: Action,
) {
    app.begin(action.label());
    match action {
        Action::Uninstall { config_scope, config_dir, needs_root, remove_ca } => {
            let out = run_suspended(events, || {
                privileged::uninstall(terminal, config_scope, config_dir, needs_root, remove_ca)
            })
                .map(|msg| Outcome {
                    title: "Uninstalled".to_string(),
                    lines: vec![msg],
                    refresh_local: true,
                    install_service: None,
                    remote: None,
                    quiet: false,
                });
            app.finish_op(out);
        }
        op_action => {
            // Reuse the confirmed CA glyph for remote panel ops so they don't
            // re-prompt for the identity on every call after connect.
            let ans = match op_action.accept_glyph() {
                Some(fp) => TuiAnswerer::with_glyph(ui_tx.clone(), fp),
                None => TuiAnswerer::new(ui_tx.clone()),
            };
            *op = Some(Box::pin(action::run_owned(ans, op_action)));
        }
    }
}

/// Fold an op's result: on success with a pending OS-service install, perform
/// that privileged step (suspending the terminal for its password prompt) and
/// append its outcome.
fn complete_op(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    events: &mut Option<Fuse<EventStream>>,
    out: Result<Outcome>,
) -> Result<Outcome> {
    let mut outcome = out?;
    if let Some(scope) = outcome.install_service.take() {
        app.log.push(Line::from("registering the OS service…"));
        match run_suspended(events, || privileged::install_service(terminal, scope)) {
            Ok(msg) => outcome.lines.push(msg),
            Err(e) => outcome.lines.push(format!("OS service registration failed: {e:#}")),
        }
    }
    Ok(outcome)
}

/// Entry point for bare `netidx admin`: build a runtime, take over the terminal,
/// and run the TUI, restoring the terminal on the way out (even on panic, via the
/// hook `ratatui::init` installs).
pub(crate) fn run() -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let mut terminal = ratatui::try_init()
        .context("initializing the terminal — `netidx admin` needs an interactive terminal; use a subcommand for scripts")?;
    let result = rt.block_on(run_app(&mut terminal));
    let _ = ratatui::try_restore();
    result
}

#[cfg(test)]
mod render_tests {
    //! Headless render smoke tests over a `TestBackend`: the `netidx admin` TUI
    //! needs a real TTY to run, so these render each new screen into an in-memory
    //! buffer to catch layout panics and confirm the behaviors added in the
    //! Debian-installer pass — pre-filled inputs, the create/connect radio, the
    //! progress bar, and the welcome dialog.

    use super::*;
    use answer::UiRequest;
    use crossterm::event::KeyCode;
    use netidx_admin::answer::{Field, Progress, Stage};
    use ratatui::{Terminal, backend::TestBackend};
    use tokio::sync::oneshot;

    /// Render `app` into a `w`×`h` test terminal and flatten the buffer to text.
    fn render_sized(app: &mut App, w: u16, h: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
        terminal.draw(|f| app.render(f)).unwrap();
        terminal.backend().buffer().content().iter().map(|c| c.symbol()).collect()
    }

    /// Render `app` into a 100×30 test terminal and flatten the buffer to text.
    fn render(app: &mut App) -> String {
        render_sized(app, 100, 30)
    }

    #[test]
    fn fresh_machine_shows_welcome() {
        let mut app = App::new();
        // Skip if the test host actually has an install detected.
        if !app.local.is_fresh() {
            return;
        }
        let s = render(&mut app);
        assert!(s.contains("netidx isn't installed"), "welcome text missing: {s:?}");
    }

    #[test]
    fn fresh_machine_has_no_tabs() {
        let mut app = App::new();
        if !app.local.is_fresh() {
            return;
        }
        // Dismiss the welcome dialog to reveal the role menu behind it.
        app.local.on_key(KeyCode::Enter);
        let s = render(&mut app);
        assert!(s.contains("Install a role"), "role menu missing: {s:?}");
        assert!(!s.contains("Cluster"), "tab bar should be hidden on a fresh machine: {s:?}");
    }

    #[test]
    fn welcome_wraps_at_narrow_width() {
        // On a narrow terminal the prose must wrap, not truncate — every word of
        // the body should still be present (the last word is the tell).
        let mut app = App::new();
        if !app.local.is_fresh() {
            return;
        }
        let s = render_sized(&mut app, 46, 24);
        assert!(s.contains("Publisher."), "welcome body truncated at narrow width: {s:?}");
    }

    #[test]
    fn progress_determinate_shows_percent() {
        let mut app = App::new();
        app.busy = true;
        app.progress =
            Some((Progress::timed(Stage::Discovering, "searching…", Duration::from_secs(3)), Instant::now()));
        let s = render(&mut app);
        assert!(s.contains("Searching the network"), "stage title missing: {s:?}");
        assert!(s.contains('%'), "no gauge percent label: {s:?}");
    }

    #[test]
    fn progress_marquee_does_not_panic() {
        let mut app = App::new();
        app.busy = true;
        app.progress = Some((Progress::new(Stage::WaitingApproval, "waiting…"), Instant::now()));
        let s = render(&mut app);
        assert!(s.contains("Waiting for approval"), "stage title missing: {s:?}");
    }

    #[test]
    fn text_modal_prefills_default() {
        let mut app = App::new();
        let (tx, _rx) = oneshot::channel();
        app.modal = Modal::from_request(UiRequest::Text {
            field: Field::Listen,
            default: Some("192.168.1.20".to_string()),
            required: false,
            reply: tx,
        });
        let s = render(&mut app);
        assert!(s.contains("192.168.1.20"), "default not pre-filled into the field: {s:?}");
    }

    #[test]
    fn long_help_is_not_truncated() {
        // The resolver-name help is several sentences; the dialog must grow to
        // fit it rather than clip (the tail of the help is the tell).
        let mut app = App::new();
        let (tx, _rx) = oneshot::channel();
        app.modal = Modal::from_request(UiRequest::Text {
            field: Field::ResolverName,
            default: Some("resolver".to_string()),
            required: false,
            reply: tx,
        });
        let s = render(&mut app);
        assert!(s.contains("resolver.example.com"), "help tail truncated: {s:?}");
    }

    #[test]
    fn local_admin_surface_shows_only_local_panels() {
        // The Local tab's admin surface (a Local target) offers only the
        // no-auth panels — Roster and Permissions — not the authenticated
        // Cluster-only panels (enrollment queue, delegations, revocation).
        let mut app = App::new();
        app.local.open_admin(
            std::path::PathBuf::from("/nonexistent/admin-server.json"),
            std::path::PathBuf::from("/nonexistent/ca"),
        );
        let s = render(&mut app);
        assert!(s.contains("Local admin server"), "local surface title missing: {s:?}");
        assert!(s.contains("Admin roster"), "roster panel missing: {s:?}");
        assert!(s.contains("Permissions"), "perms panel missing locally: {s:?}");
        assert!(!s.contains("Enrollment"), "queue panel should be absent locally: {s:?}");
        assert!(!s.contains("Delegation"), "delegation panel should be absent locally: {s:?}");
    }

    #[test]
    fn theme_backdrop_is_blue() {
        // In the full-screen busy view the activity backdrop fills the top-left.
        let mut app = App::new();
        app.busy = true;
        app.progress = Some((Progress::new(Stage::Discovering, "…"), Instant::now()));
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| app.render(f)).unwrap();
        assert_eq!(terminal.backend().buffer()[(0u16, 0u16)].bg, theme::BACKDROP);
    }

    #[test]
    fn choice_modal_shows_radio() {
        let mut app = App::new();
        let (tx, _rx) = oneshot::channel();
        app.modal = Modal::from_request(UiRequest::Choice {
            field: Field::ClusterMode,
            choices: vec!["Create a new cluster".into(), "Connect to an existing cluster".into()],
            default: Some("Create a new cluster".into()),
            reply: tx,
        });
        let s = render(&mut app);
        assert!(s.contains("(*)"), "selected radio marker missing: {s:?}");
        assert!(s.contains("Create a new cluster"), "choice label missing: {s:?}");
    }
}
