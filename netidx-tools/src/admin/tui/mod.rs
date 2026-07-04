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
mod widgets;

use action::{Action, Outcome};
use answer::{Modal, TuiAnswerer, UiRequest};
use anyhow::{Context, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::{StreamExt, stream::Fuse};
use netidx_admin::fingerprint::Fingerprint;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Tabs, Wrap},
};
use std::{
    collections::VecDeque,
    future::{self, Future},
    pin::Pin,
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
            Tab::Remote => "Remote admin",
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
    /// The latest progress line from the running action.
    status: Option<String>,
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
            status: None,
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
    }

    /// Switch the UI into the activity view for a just-started action.
    fn begin(&mut self, label: String) {
        self.busy = true;
        self.activity = Some(label);
        self.status = None;
        self.verification = None;
        self.log.clear();
        self.result = None;
    }

    /// Record a finished action's result. Leaves any still-open modal (e.g. an
    /// un-acknowledged recovery-password modal) in place — it renders on top.
    fn finish_op(&mut self, out: Result<Outcome>) {
        self.busy = false;
        self.activity = None;
        self.status = None;
        self.verification = None;
        match out {
            Ok(out) => {
                if out.refresh_local {
                    self.local.refresh();
                }
                if let Some(update) = out.remote {
                    self.remote.apply(update);
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
            UiRequest::Progress(p) => self.status = Some(p.message.to_string()),
            UiRequest::VerificationCode { purpose, code } => {
                self.verification = Some((purpose, code))
            }
            blocking => {
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
                self.next_tab();
                return None;
            }
            _ => match self.tab {
                Tab::Local => self.local.on_key(code),
                Tab::Remote => self.remote.on_key(code),
            },
        }?;
        // Pure navigation from the Local tab: jump to the Remote tab's delegation
        // panel rather than running an op.
        if let Action::ReviewDelegations = action {
            self.tab = Tab::Remote;
            self.remote.focus_delegations();
            return None;
        }
        // Gate destructive actions behind a yes/no confirmation.
        match action.confirm_message() {
            Some(msg) => {
                self.confirm = Some((msg, action));
                None
            }
            None => Some(action),
        }
    }

    fn render(&mut self, f: &mut Frame) {
        let chunks = Layout::vertical([
            Constraint::Length(3), // tab bar
            Constraint::Min(0),    // body
            Constraint::Length(1), // footer / key hints
        ])
        .split(f.area());
        self.render_tabs(f, chunks[0]);
        if self.busy {
            self.render_activity(f, chunks[1]);
        } else {
            match self.tab {
                Tab::Local => self.local.render(f, chunks[1]),
                Tab::Remote => self.remote.render(f, chunks[1]),
            }
        }
        self.render_footer(f, chunks[2]);
        let area = f.area();
        if let Some(m) = &self.modal {
            m.render(f, area);
        } else if let Some((msg, _)) = &self.confirm {
            render_confirm(f, area, msg);
        } else if let Some(r) = &self.result {
            render_result(f, area, r);
        }
    }

    fn render_tabs(&self, f: &mut Frame, area: Rect) {
        let titles = Tab::ALL.iter().map(|t| Line::from(t.title()));
        let tabs = Tabs::new(titles)
            .select(self.tab.index())
            .block(Block::default().borders(Borders::ALL).title(" netidx admin "))
            .highlight_style(
                Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD),
            );
        f.render_widget(tabs, area);
    }

    /// The busy view: the running action's progress, any verification code, and
    /// its notes/warnings so far.
    fn render_activity(&self, f: &mut Frame, area: Rect) {
        let title = self.activity.clone().unwrap_or_else(|| "Working".to_string());
        let mut lines: Vec<Line> = Vec::new();
        match &self.status {
            Some(s) => lines.push(Line::from(vec![
                Span::styled("● ", Style::default().fg(Color::Cyan)),
                Span::raw(s.to_string()),
            ])),
            None => lines.push(Line::from("● working…".dim())),
        }
        if let Some((purpose, code)) = &self.verification {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("{purpose} — read this code to the approving admin:"),
                Style::default().fg(Color::Yellow),
            )));
            lines.push(Line::from(Span::styled(code.text(), Style::default().fg(Color::Cyan))));
            lines.extend(widgets::identicon_lines(code));
        }
        if !self.log.is_empty() {
            lines.push(Line::from(""));
            lines.extend(self.log.iter().cloned());
        }
        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .block(Block::default().borders(Borders::ALL).title(format!(" {title} "))),
            area,
        );
    }

    fn render_footer(&self, f: &mut Frame, area: Rect) {
        let hint = if self.result.is_some() {
            Line::from(" any key to dismiss ".dim())
        } else if self.modal.is_some() {
            Line::from(" answer above · Esc cancel ".dim())
        } else if self.confirm.is_some() {
            Line::from(" y confirm · n/Esc cancel ".dim())
        } else if self.busy {
            Line::from(" working… · Ctrl-C quit ".dim())
        } else {
            Line::from(vec![
                Span::styled("Tab", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" switch  "),
                Span::styled("↑/↓", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" navigate  "),
                Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(" quit"),
            ])
            .dim()
        };
        f.render_widget(Paragraph::new(hint), area);
    }
}

/// Render a finished action's result as a dismissible centered overlay.
fn render_result(f: &mut Frame, screen: Rect, r: &ResultView) {
    let lines: Vec<Line> = r
        .lines
        .iter()
        .flat_map(|l| l.split('\n'))
        .map(|s| Line::from(s.to_string()))
        .collect();
    let h = (lines.len() as u16 + 4).clamp(6, screen.height);
    let area = widgets::centered(72, h, screen);
    f.render_widget(Clear, area);
    let color = if r.error { Color::Red } else { Color::Green };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(color))
        .title(format!(" {} ", r.title))
        .title_bottom(Line::from(" any key to dismiss ").dim());
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }).block(block), area);
}

/// Render a destructive action's yes/no confirmation as a centered overlay.
fn render_confirm(f: &mut Frame, screen: Rect, msg: &str) {
    let lines = vec![
        Line::from(msg.to_string()),
        Line::from(""),
        Line::from(" y confirm · n cancel ".dim()),
    ];
    let area = widgets::centered(66, 8, screen);
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .title(" Confirm ");
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }).block(block), area);
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
    // The crossterm reader. `None` only during a terminal-suspend, so its
    // background thread can't fight the child (editor / sudo) for stdin.
    let mut events: Option<Fuse<EventStream>> = Some(EventStream::new().fuse());
    while !app.should_quit {
        terminal.draw(|f| app.render(f))?;
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
