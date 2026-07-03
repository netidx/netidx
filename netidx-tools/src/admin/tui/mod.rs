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

mod local;
mod remote;
mod widgets;

use anyhow::{Context, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
};

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

/// Top-level TUI state.
struct App {
    tab: Tab,
    local: local::LocalState,
    remote: remote::RemoteState,
    should_quit: bool,
}

impl App {
    fn new() -> App {
        App {
            tab: Tab::Local,
            local: local::LocalState::new(),
            remote: remote::RemoteState::new(),
            should_quit: false,
        }
    }

    /// Advance to the next tab (wrapping).
    fn next_tab(&mut self) {
        let i = (self.tab.index() + 1) % Tab::ALL.len();
        self.tab = Tab::ALL[i];
    }

    /// Handle one key press (already filtered to `KeyEventKind::Press`).
    /// Global keys are handled here; the rest is delegated to the focused tab.
    fn on_key(&mut self, code: KeyCode, mods: KeyModifiers) {
        match code {
            KeyCode::Char('c') if mods.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true
            }
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => self.next_tab(),
            _ => match self.tab {
                Tab::Local => self.local.on_key(code),
                Tab::Remote => self.remote.on_key(code),
            },
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
        match self.tab {
            Tab::Local => self.local.render(f, chunks[1]),
            Tab::Remote => self.remote.render(f, chunks[1]),
        }
        render_footer(f, chunks[2]);
    }

    fn render_tabs(&self, f: &mut Frame, area: Rect) {
        let titles = Tab::ALL.iter().map(|t| Line::from(t.title()));
        let tabs = Tabs::new(titles)
            .select(self.tab.index())
            .block(Block::default().borders(Borders::ALL).title(" netidx admin "))
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            );
        f.render_widget(tabs, area);
    }
}

fn render_footer(f: &mut Frame, area: Rect) {
    let hint = Line::from(vec![
        Span::styled("Tab", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" switch  "),
        Span::styled("↑/↓", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" navigate  "),
        Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(" quit"),
    ])
    .dim();
    f.render_widget(Paragraph::new(hint), area);
}

/// Run the async event loop over `terminal` until the user quits.
async fn run_app(terminal: &mut ratatui::DefaultTerminal) -> Result<()> {
    let mut app = App::new();
    let mut events = EventStream::new().fuse();
    while !app.should_quit {
        terminal.draw(|f| app.render(f))?;
        tokio::select! {
            biased;
            ev = events.select_next_some() => match ev {
                Ok(Event::Key(k)) if k.kind == KeyEventKind::Press => app.on_key(k.code, k.modifiers),
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
