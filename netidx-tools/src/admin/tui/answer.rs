//! The [`TuiAnswerer`] — the widget-backed [`Answerer`] the TUI hands to the
//! library ops, plus the modal widgets that collect each answer.
//!
//! The op (an install planner or a remote-admin operation) runs on a spawned
//! task and calls `ans.text().await`, `ans.confirm().await`, … The answerer
//! turns each call into a [`UiRequest`] sent over a channel to the UI loop and
//! parks on a `oneshot` for the reply. The UI loop pops the request into a
//! [`Modal`], collects the answer through key events, and sends it back. Sync
//! notifications (`note`/`warn`/`progress`/`show_verification_code`) are
//! fire-and-forget; `show_recovery_password` is the one sync call that must
//! block until acknowledged, which is safe because it runs on the spawned op
//! task, never the UI task.

use super::{theme, widgets};
use anyhow::{Result, anyhow};
use crossterm::event::KeyCode;
use netidx_admin::{
    admin_client::CaIdentity,
    admin_proto::Secret,
    answer::{Answerer, Field, NetworkChoice, NetworkOption, Progress},
    fingerprint::Fingerprint,
};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

/// A `$EDITOR` validator: parses+normalizes the edited text, or reports why it's
/// invalid so the operator can re-edit. Runs in the UI loop (terminal suspended)
/// and returns the normalized text to send. Boxed so it can travel the request
/// channel; the op supplies the right one (policy JSON, perms JSON, …).
pub(super) type EditValidator = Box<dyn Fn(&str) -> Result<String> + Send>;

/// A request from the op task to the UI loop. The blocking question variants
/// carry a `oneshot` the UI answers; the rest are fire-and-forget.
pub(super) enum UiRequest {
    Text {
        field: Field,
        default: Option<String>,
        required: bool,
        reply: oneshot::Sender<Result<Option<String>>>,
    },
    Secret {
        field: Field,
        reply: oneshot::Sender<Result<Secret>>,
    },
    Choice {
        field: Field,
        choices: Vec<String>,
        default: Option<String>,
        reply: oneshot::Sender<Result<String>>,
    },
    /// Pick a discovered cluster (each shown with its CA glyph + fingerprint),
    /// or the trailing "enter an address manually" option.
    SelectNetwork {
        networks: Vec<NetworkOption>,
        reply: oneshot::Sender<Result<NetworkChoice>>,
    },
    Confirm {
        field: Field,
        default: bool,
        reply: oneshot::Sender<Result<bool>>,
    },
    Announce {
        title: String,
        body: String,
        reply: oneshot::Sender<Result<()>>,
    },
    AnnounceIdentity {
        body: String,
        code: Fingerprint,
        reply: oneshot::Sender<Result<()>>,
    },
    Identity {
        identity: Box<CaIdentity>,
        reply: oneshot::Sender<Result<bool>>,
    },
    /// Suspend the TUI, drop the operator into `$EDITOR` on `seed`, validate the
    /// result (re-editing on failure), and reply with the normalized text. Not a
    /// modal — the UI loop services it directly because it must own the terminal
    /// to suspend it. See [`super::privileged::edit_in_terminal`].
    Editor {
        seed: String,
        validate: EditValidator,
        reply: oneshot::Sender<Result<String>>,
    },
    Recovery {
        password: String,
    },
    VerificationCode {
        purpose: String,
        code: Fingerprint,
    },
    Note(String),
    Warn(String),
    Progress(Progress),
}

/// The [`Answerer`] the TUI hands to library ops. Cheap to build (just a channel
/// sender); one per op run.
pub(super) struct TuiAnswerer {
    tx: UnboundedSender<UiRequest>,
    /// A pre-confirmed CA fingerprint. When set, `confirm_identity` auto-accepts
    /// a matching identity instead of popping the modal — so remote-admin panels
    /// don't re-ask the operator to confirm the glyph on every op after connect
    /// (it still re-pins per op, so a changed cert errors). Mirrors
    /// [`FlagAnswerer`](super::super::answer_cli::FlagAnswerer)'s `--accept-glyph`.
    accept_glyph: Option<Fingerprint>,
}

impl TuiAnswerer {
    pub(super) fn new(tx: UnboundedSender<UiRequest>) -> TuiAnswerer {
        TuiAnswerer { tx, accept_glyph: None }
    }

    /// A TUI answerer that auto-accepts the given CA fingerprint (see
    /// [`Self::accept_glyph`]).
    pub(super) fn with_glyph(tx: UnboundedSender<UiRequest>, fp: Fingerprint) -> TuiAnswerer {
        TuiAnswerer { tx, accept_glyph: Some(fp) }
    }

    /// Edit `seed` in the operator's `$EDITOR` (via the UI loop, which suspends
    /// the terminal), validating+normalizing with `validate`. Returns the
    /// normalized text, or an error if the operator aborted. Inherent (not part
    /// of [`Answerer`]) — the editor loop is a TUI-only concern, so only the
    /// concrete op bodies that hold a `TuiAnswerer` reach it.
    pub(super) async fn edit(&self, seed: String, validate: EditValidator) -> Result<String> {
        self.ask(|reply| UiRequest::Editor { seed, validate, reply }).await
    }

    /// Send a question and await its reply, mapping a dropped channel (the UI
    /// closed, or the operator cancelled the modal) to a clean error.
    async fn ask<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T>>) -> UiRequest,
    ) -> Result<T> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(make(reply)).map_err(|_| anyhow!("the TUI was closed"))?;
        rx.await.map_err(|_| anyhow!("cancelled"))?
    }
}

#[async_trait::async_trait]
impl Answerer for TuiAnswerer {
    fn interactive(&self) -> bool {
        true
    }

    async fn text(
        &mut self,
        field: Field,
        provided: Option<String>,
        default: Option<&str>,
        required: bool,
    ) -> Result<Option<String>> {
        if let Some(v) = provided {
            return Ok(Some(v));
        }
        let default = default.map(str::to_owned);
        self.ask(|reply| UiRequest::Text { field, default, required, reply }).await
    }

    async fn choice(
        &mut self,
        field: Field,
        provided: Option<String>,
        choices: &[&str],
        default: Option<&str>,
    ) -> Result<String> {
        if let Some(v) = provided {
            return Ok(v);
        }
        let choices = choices.iter().map(|s| s.to_string()).collect();
        let default = default.map(str::to_owned);
        self.ask(|reply| UiRequest::Choice { field, choices, default, reply }).await
    }

    async fn select_network(&mut self, networks: &[NetworkOption]) -> Result<NetworkChoice> {
        let networks = networks.to_vec();
        self.ask(|reply| UiRequest::SelectNetwork { networks, reply }).await
    }

    async fn confirm(
        &mut self,
        field: Field,
        provided: Option<bool>,
        default: bool,
    ) -> Result<bool> {
        if let Some(v) = provided {
            return Ok(v);
        }
        self.ask(|reply| UiRequest::Confirm { field, default, reply }).await
    }

    async fn secret(&mut self, field: Field, provided: Option<Secret>) -> Result<Secret> {
        if let Some(v) = provided {
            return Ok(v);
        }
        self.ask(|reply| UiRequest::Secret { field, reply }).await
    }

    async fn announce(&mut self, title: &str, body: &str) -> Result<()> {
        let (title, body) = (title.to_string(), body.to_string());
        self.ask(|reply| UiRequest::Announce { title, body, reply }).await
    }

    async fn announce_identity(&mut self, body: &str, code: &Fingerprint) -> Result<()> {
        let (body, code) = (body.to_string(), *code);
        self.ask(|reply| UiRequest::AnnounceIdentity { body, code, reply }).await
    }

    async fn confirm_identity(&mut self, identity: &CaIdentity) -> Result<bool> {
        if let Some(expected) = &self.accept_glyph {
            return Ok(&identity.fingerprint == expected);
        }
        let identity = Box::new(identity.clone());
        self.ask(|reply| UiRequest::Identity { identity, reply }).await
    }

    fn show_verification_code(&mut self, purpose: &str, code: &Fingerprint) {
        let _ = self.tx.send(UiRequest::VerificationCode { purpose: purpose.to_string(), code: *code });
    }

    fn progress(&mut self, progress: Progress) {
        let _ = self.tx.send(UiRequest::Progress(progress));
    }

    fn note(&mut self, message: &str) {
        let _ = self.tx.send(UiRequest::Note(message.to_string()));
    }

    fn warn(&mut self, message: &str) {
        let _ = self.tx.send(UiRequest::Warn(message.to_string()));
    }

    fn show_recovery_password(&mut self, password: &str) {
        // Fire-and-forget: the op runs on the UI task, so we cannot block here.
        // The modal queue keeps this un-missable — it stays up (and any later
        // question queues behind it) until the operator acknowledges it.
        let _ = self.tx.send(UiRequest::Recovery { password: password.to_string() });
    }
}

/// A pending modal question, holding its UI state and the reply channel back to
/// the parked op. Created by the UI loop from a blocking [`UiRequest`].
pub(super) enum Modal {
    Text {
        field: Field,
        required: bool,
        secret: bool,
        /// The live edit buffer — seeded from the field's default so the
        /// operator sees and edits the value the install would use, rather than
        /// an empty box with a hidden fallback.
        input: String,
        error: Option<String>,
        reply: Option<TextReply>,
    },
    Choice {
        field: Field,
        choices: Vec<String>,
        state: ListState,
        reply: Option<oneshot::Sender<Result<String>>>,
    },
    /// Pick one of the discovered clusters (rendered with its glyph +
    /// fingerprint) or the trailing manual-entry row. Selection index
    /// `networks.len()` is the manual row.
    SelectNetwork {
        networks: Vec<NetworkOption>,
        state: ListState,
        reply: Option<oneshot::Sender<Result<NetworkChoice>>>,
    },
    Confirm {
        field: Field,
        yes: bool,
        reply: Option<oneshot::Sender<Result<bool>>>,
    },
    Identity {
        identity: Box<CaIdentity>,
        reply: Option<oneshot::Sender<Result<bool>>>,
    },
    Announce {
        title: String,
        body: String,
        reply: Option<oneshot::Sender<Result<()>>>,
    },
    AnnounceIdentity {
        body: String,
        code: Fingerprint,
        reply: Option<oneshot::Sender<Result<()>>>,
    },
    Recovery {
        password: String,
    },
}

/// A text modal answers either an optional free-text field or a secret; the two
/// differ only in reply type.
pub(super) enum TextReply {
    Text(oneshot::Sender<Result<Option<String>>>),
    Secret(oneshot::Sender<Result<Secret>>),
}

impl Modal {
    /// Build a modal from a blocking request. Returns `None` for the
    /// non-blocking request variants (which the UI loop handles as log/notice).
    pub(super) fn from_request(req: UiRequest) -> Option<Modal> {
        match req {
            UiRequest::Text { field, default, required, reply } => Some(Modal::Text {
                field,
                required,
                secret: false,
                // Pre-fill the default so it's visible and editable (Debian
                // style); an empty box now means "no value".
                input: default.unwrap_or_default(),
                error: None,
                reply: Some(TextReply::Text(reply)),
            }),
            UiRequest::Secret { field, reply } => Some(Modal::Text {
                field,
                required: true,
                secret: true,
                input: String::new(),
                error: None,
                reply: Some(TextReply::Secret(reply)),
            }),
            UiRequest::Choice { field, choices, default, reply } => {
                let sel = default
                    .and_then(|d| choices.iter().position(|c| *c == d))
                    .unwrap_or(0);
                let mut state = ListState::default();
                state.select(Some(sel));
                Some(Modal::Choice { field, choices, state, reply: Some(reply) })
            }
            UiRequest::SelectNetwork { networks, reply } => {
                let mut state = ListState::default();
                state.select(Some(0));
                Some(Modal::SelectNetwork { networks, state, reply: Some(reply) })
            }
            UiRequest::Confirm { field, default, reply } => {
                Some(Modal::Confirm { field, yes: default, reply: Some(reply) })
            }
            UiRequest::Identity { identity, reply } => {
                Some(Modal::Identity { identity, reply: Some(reply) })
            }
            UiRequest::Announce { title, body, reply } => {
                Some(Modal::Announce { title, body, reply: Some(reply) })
            }
            UiRequest::AnnounceIdentity { body, code, reply } => {
                Some(Modal::AnnounceIdentity { body, code, reply: Some(reply) })
            }
            UiRequest::Recovery { password } => Some(Modal::Recovery { password }),
            _ => None,
        }
    }

    /// The field this modal is answering, for help text (identity/recovery have
    /// none).
    fn field(&self) -> Option<Field> {
        match self {
            Modal::Text { field, .. }
            | Modal::Choice { field, .. }
            | Modal::Confirm { field, .. } => Some(*field),
            Modal::SelectNetwork { .. }
            | Modal::Identity { .. }
            | Modal::Announce { .. }
            | Modal::AnnounceIdentity { .. }
            | Modal::Recovery { .. } => None,
        }
    }

    /// Handle a key. Returns `true` when the modal has resolved (answer sent)
    /// and should be removed.
    pub(super) fn on_key(&mut self, code: KeyCode) -> bool {
        match self {
            Modal::Text { required, secret, input, error, reply, .. } => match code {
                KeyCode::Esc => {
                    send_text(reply.take(), Err(anyhow!("cancelled")));
                    true
                }
                KeyCode::Enter => {
                    // The answer is whatever's in the field. Empty is rejected
                    // when required, else means "no value" (the default, if any,
                    // was pre-filled and could have been left in place).
                    if input.is_empty() {
                        if *required {
                            *error = Some("a value is required".to_string());
                            false
                        } else {
                            resolve_text(reply.take(), None, *secret);
                            true
                        }
                    } else {
                        resolve_text(reply.take(), Some(std::mem::take(input)), *secret);
                        true
                    }
                }
                KeyCode::Backspace => {
                    input.pop();
                    false
                }
                KeyCode::Char(c) => {
                    input.push(c);
                    false
                }
                _ => false,
            },
            Modal::Choice { choices, state, reply, .. } => match code {
                KeyCode::Esc => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Err(anyhow!("cancelled")));
                    }
                    true
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    let i = state.selected().unwrap_or(0).saturating_sub(1);
                    state.select(Some(i));
                    false
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let last = choices.len().saturating_sub(1);
                    let i = state.selected().map_or(0, |i| (i + 1).min(last));
                    state.select(Some(i));
                    false
                }
                KeyCode::Enter => {
                    let sel = state.selected().unwrap_or(0).min(choices.len().saturating_sub(1));
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(choices[sel].clone()));
                    }
                    true
                }
                _ => false,
            },
            // The list is the discovered networks followed by one manual-entry
            // row, so the last selectable index (`networks.len()`) is Manual.
            Modal::SelectNetwork { networks, state, reply } => match code {
                KeyCode::Esc => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Err(anyhow!("cancelled")));
                    }
                    true
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    let i = state.selected().unwrap_or(0).saturating_sub(1);
                    state.select(Some(i));
                    false
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let i = state.selected().map_or(0, |i| (i + 1).min(networks.len()));
                    state.select(Some(i));
                    false
                }
                KeyCode::Enter => {
                    let sel = state.selected().unwrap_or(0).min(networks.len());
                    let choice = if sel == networks.len() {
                        NetworkChoice::Manual
                    } else {
                        NetworkChoice::Discovered(sel)
                    };
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(choice));
                    }
                    true
                }
                _ => false,
            },
            Modal::Confirm { yes, reply, .. } => match code {
                KeyCode::Esc => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Err(anyhow!("cancelled")));
                    }
                    true
                }
                KeyCode::Left | KeyCode::Right | KeyCode::Tab | KeyCode::Char('h') | KeyCode::Char('l') => {
                    *yes = !*yes;
                    false
                }
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(true));
                    }
                    true
                }
                KeyCode::Char('n') | KeyCode::Char('N') => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(false));
                    }
                    true
                }
                KeyCode::Enter => {
                    let v = *yes;
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(v));
                    }
                    true
                }
                _ => false,
            },
            Modal::Identity { reply, .. } => match code {
                KeyCode::Char('a') | KeyCode::Char('A') | KeyCode::Enter => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(true));
                    }
                    true
                }
                KeyCode::Esc | KeyCode::Char('r') | KeyCode::Char('R') | KeyCode::Char('n') => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(false));
                    }
                    true
                }
                _ => false,
            },
            Modal::Announce { reply, .. } | Modal::AnnounceIdentity { reply, .. } => match code
            {
                KeyCode::Enter | KeyCode::Char(' ') => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(()));
                    }
                    true
                }
                KeyCode::Esc => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Err(anyhow!("cancelled")));
                    }
                    true
                }
                _ => false,
            },
            Modal::Recovery { .. } => matches!(code, KeyCode::Enter | KeyCode::Char(' ')),
        }
    }

    pub(super) fn render(&self, f: &mut Frame, screen: Rect) {
        match self {
            Modal::Text { secret, input, error, .. } => {
                let field = self.field().unwrap();
                let w = 64u16.min(screen.width.saturating_sub(4)).max(30);
                // Size the help area to the wrapped help so it's never truncated.
                let help_h = widgets::wrapped_rows(field.help(), w - 2);
                let inner_rows = help_h + 1 /*spacer*/ + 1 /*field*/ + 1 /*error*/ + 1 /*spacer*/ + 1 /*buttons*/;
                let h = (inner_rows + 2).min(screen.height);
                let area = widgets::centered(w, h, screen);
                widgets::shadow(f, area, screen);
                f.render_widget(Clear, area);
                let block = theme::dialog_block(field.label());
                let inner = block.inner(area);
                f.render_widget(block, area);
                let rows = Layout::vertical([
                    Constraint::Length(help_h), // help
                    Constraint::Length(1),      // spacer
                    Constraint::Length(1),      // input field
                    Constraint::Length(1),      // error (if any)
                    Constraint::Length(1),      // spacer
                    Constraint::Length(1),      // buttons
                    Constraint::Min(0),
                ])
                .split(inner);
                f.render_widget(
                    Paragraph::new(field.help()).style(theme::hint_style()).wrap(Wrap { trim: true }),
                    rows[0],
                );
                let shown = if *secret { "•".repeat(input.chars().count()) } else { input.clone() };
                let field_row = rows[2];
                f.render_widget(Paragraph::new(shown).style(theme::field_style()), field_row);
                if let Some(e) = error {
                    let err = Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT);
                    f.render_widget(Paragraph::new(e.clone()).style(err), rows[3]);
                }
                let buttons =
                    Line::from(vec![theme::button("Continue", true), Span::raw("  "), theme::button("Cancel", false)]);
                f.render_widget(
                    Paragraph::new(buttons).alignment(Alignment::Center).style(theme::panel_style()),
                    rows[5],
                );
                // Focus: put the terminal cursor at the end of the field.
                let cx = field_row.x + (input.chars().count() as u16).min(field_row.width.saturating_sub(1));
                f.set_cursor_position((cx, field_row.y));
            }
            Modal::Choice { field, choices, state, .. } => {
                let sel = state.selected().unwrap_or(0).min(choices.len().saturating_sub(1));
                let w = 64u16.min(screen.width.saturating_sub(4)).max(30);
                let help_h = widgets::wrapped_rows(field.help(), w - 2);
                let list_h = choices.len() as u16;
                let h = (help_h + 1 + list_h + 2).min(screen.height);
                let area = widgets::centered(w, h, screen);
                widgets::shadow(f, area, screen);
                f.render_widget(Clear, area);
                let block = theme::dialog_block(field.label()).title_bottom(Line::from(Span::styled(
                    " ↑/↓ move · Enter select · Esc cancel ",
                    theme::hint_style(),
                )));
                let inner = block.inner(area);
                f.render_widget(block, area);
                let rows = Layout::vertical([
                    Constraint::Length(help_h), // help
                    Constraint::Length(1),      // spacer
                    Constraint::Min(0),         // radio list
                ])
                .split(inner);
                f.render_widget(
                    Paragraph::new(field.help()).style(theme::hint_style()).wrap(Wrap { trim: true }),
                    rows[0],
                );
                let items: Vec<ListItem> = choices
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let marker = if i == sel { "(*) " } else { "( ) " };
                        ListItem::new(format!("{marker}{c}"))
                    })
                    .collect();
                let mut st = *state;
                let list = List::new(items)
                    .style(theme::panel_style())
                    .highlight_style(theme::selected_style());
                f.render_stateful_widget(list, rows[2], &mut st);
            }
            Modal::SelectNetwork { networks, state, .. } => {
                const MANUAL: &str = "Enter an address manually…";
                let sel = state.selected().unwrap_or(0).min(networks.len());
                let w = 68u16.min(screen.width.saturating_sub(4)).max(40);
                // Body holds the list (left) beside the selected glyph (right, 8
                // identicon rows + a blank + up to 3 fingerprint lines).
                let body_h = (networks.len() as u16 + 1).max(12);
                let h = (1 /*header*/ + 1 /*spacer*/ + body_h + 2 /*borders*/).min(screen.height);
                let area = widgets::centered(w, h, screen);
                widgets::shadow(f, area, screen);
                f.render_widget(Clear, area);
                let block = theme::dialog_block(Field::SelectNetwork.label()).title_bottom(
                    Line::from(Span::styled(
                        " ↑/↓ move · Enter select · Esc cancel ",
                        theme::hint_style(),
                    )),
                );
                let inner = block.inner(area);
                f.render_widget(block, area);
                let rows = Layout::vertical([
                    Constraint::Length(1), // header
                    Constraint::Length(1), // spacer
                    Constraint::Min(0),    // list | glyph
                ])
                .split(inner);
                let header = if networks.is_empty() {
                    "No clusters found on the local network."
                } else {
                    "netidx clusters discovered on the local network:"
                };
                f.render_widget(
                    Paragraph::new(header).style(theme::hint_style()).wrap(Wrap { trim: true }),
                    rows[0],
                );
                let cols = Layout::horizontal([Constraint::Min(20), Constraint::Length(26)])
                    .split(rows[2]);
                let items: Vec<ListItem> = networks
                    .iter()
                    .map(|n| ListItem::new(n.domain.clone()))
                    .chain(std::iter::once(ListItem::new(MANUAL)))
                    .collect();
                let mut st = *state;
                let list = List::new(items)
                    .style(theme::panel_style())
                    .highlight_style(theme::selected_style());
                f.render_stateful_widget(list, cols[0], &mut st);
                // Right: the selected network's glyph + grouped fingerprint, or a
                // hint for the manual-entry row.
                let glyph = match networks.get(sel) {
                    Some(n) => {
                        let mut lines = widgets::identicon_lines(&n.identity.fingerprint);
                        lines.push(Line::from(""));
                        let fp = Style::default()
                            .bg(theme::PANEL_BG)
                            .fg(Color::Rgb(0, 0, 150))
                            .add_modifier(Modifier::BOLD);
                        for chunk in widgets::group_fingerprint(&n.identity.fingerprint) {
                            lines.push(Line::from(Span::styled(chunk, fp)));
                        }
                        lines
                    }
                    None => vec![Line::from(Span::styled(
                        "Type an admin-server address (host:port) to connect directly.",
                        theme::hint_style(),
                    ))],
                };
                // trim:false — the identicon rows carry leading "off" cells as
                // spaces; trimming them would shift the glyph and corrupt it.
                f.render_widget(
                    Paragraph::new(glyph).style(theme::panel_style()).wrap(Wrap { trim: false }),
                    cols[1],
                );
            }
            Modal::Confirm { field, yes, .. } => {
                let opts =
                    Line::from(vec![theme::button("Yes", *yes), Span::raw("   "), theme::button("No", !*yes)]);
                let lines = vec![
                    Line::from(Span::styled(field.help(), theme::hint_style())),
                    Line::from(""),
                    opts,
                    Line::from(""),
                    Line::from(Span::styled(" ←/→ · y/n · Enter · Esc cancel ", theme::hint_style())),
                ];
                popup(f, screen, field.label(), lines, 60);
            }
            Modal::Identity { identity, .. } => {
                let roles =
                    identity.roles.iter().map(|r| format!("{r:?}")).collect::<Vec<_>>().join(", ");
                let label = |s: &str| Span::styled(s.to_string(), theme::hint_style());
                let val = |s: String| Span::styled(s, theme::panel_style());
                let mut lines = vec![
                    Line::from(Span::styled(
                        "Verify this is the cluster you intend to trust, out of band,",
                        theme::panel_style(),
                    )),
                    Line::from(Span::styled(
                        "then accept. Everything after is pinned to this fingerprint.",
                        theme::hint_style(),
                    )),
                    Line::from(""),
                    Line::from(vec![label("  domain: "), val(identity.domain.clone())]),
                    Line::from(vec![label("   roles: "), val(roles)]),
                    Line::from(""),
                ];
                lines.extend(widgets::identicon_lines(&identity.fingerprint));
                lines.push(Line::from(""));
                let fp = Style::default().bg(theme::PANEL_BG).fg(Color::Rgb(0, 0, 150)).add_modifier(Modifier::BOLD);
                for chunk in widgets::group_fingerprint(&identity.fingerprint) {
                    lines.push(Line::from(Span::styled(chunk, fp)));
                }
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(" a/Enter accept · Esc/r reject ", theme::hint_style())));
                popup(f, screen, "Confirm cluster identity", lines, 60);
            }
            Modal::Announce { title, body, .. } => {
                let lines = vec![
                    Line::from(Span::styled(body.clone(), theme::panel_style())),
                    Line::from(""),
                    Line::from(Span::styled(" Press Enter to continue ", theme::selected_style())),
                ];
                popup(f, screen, title, lines, 64);
            }
            Modal::AnnounceIdentity { body, code, .. } => {
                let mut lines = vec![
                    Line::from(Span::styled(body.clone(), theme::panel_style())),
                    Line::from(""),
                ];
                lines.extend(widgets::identicon_lines(code));
                lines.push(Line::from(""));
                let fp = Style::default()
                    .bg(theme::PANEL_BG)
                    .fg(Color::Rgb(0, 0, 150))
                    .add_modifier(Modifier::BOLD);
                for chunk in widgets::group_fingerprint(code) {
                    lines.push(Line::from(Span::styled(chunk, fp)));
                }
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    " Press Enter to continue ",
                    theme::selected_style(),
                )));
                popup(f, screen, "Certificate authority created", lines, 64);
            }
            Modal::Recovery { password, .. } => {
                let pw = Style::default()
                    .bg(Color::Rgb(255, 249, 196))
                    .fg(Color::Rgb(0, 0, 0))
                    .add_modifier(Modifier::BOLD);
                let lines = vec![
                    Line::from(Span::styled(
                        "CA recovery password — shown once, never stored:",
                        theme::panel_style(),
                    )),
                    Line::from(""),
                    Line::from(Span::styled(password.clone(), pw)),
                    Line::from(""),
                    Line::from(Span::styled(
                        "Use this password to unlock the CA key in an emergency. Write it \
                         down and store it in a safe place now — it is the only off-box \
                         credential that can unlock the CA key, and there is no second \
                         chance to read it.",
                        theme::panel_style(),
                    )),
                    Line::from(""),
                    Line::from(Span::styled(" Enter — I have saved it ", theme::selected_style())),
                ];
                popup(f, screen, "CA recovery password", lines, 66);
            }
        }
    }
}

fn resolve_text(reply: Option<TextReply>, value: Option<String>, _secret: bool) {
    match reply {
        Some(TextReply::Text(tx)) => {
            let _ = tx.send(Ok(value));
        }
        Some(TextReply::Secret(tx)) => {
            let _ = tx.send(Ok(Secret(value.unwrap_or_default())));
        }
        None => {}
    }
}

fn send_text(reply: Option<TextReply>, err: Result<()>) {
    let e = err.unwrap_err();
    match reply {
        Some(TextReply::Text(tx)) => {
            let _ = tx.send(Err(e));
        }
        Some(TextReply::Secret(tx)) => {
            let _ = tx.send(Err(e));
        }
        None => {}
    }
}

/// Draw a shadowed, centered dialog popup with the given title and body lines,
/// sized to the height the lines wrap to at width `w` so nothing is truncated.
fn popup(f: &mut Frame, screen: Rect, title: &str, lines: Vec<Line<'static>>, w: u16) {
    let h = (widgets::wrapped_height(&lines, w.saturating_sub(2)) + 2).min(screen.height);
    let area = widgets::centered(w, h, screen);
    widgets::shadow(f, area, screen);
    f.render_widget(Clear, area);
    let body = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .style(theme::panel_style())
        .block(theme::dialog_block(title));
    f.render_widget(body, area);
}
