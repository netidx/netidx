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

use super::widgets;
use anyhow::{Result, anyhow};
use crossterm::event::KeyCode;
use netidx_admin::{
    admin_client::CaIdentity,
    admin_proto::Secret,
    answer::{Answerer, Field, Progress},
    fingerprint::Fingerprint,
};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

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
    Confirm {
        field: Field,
        default: bool,
        reply: oneshot::Sender<Result<bool>>,
    },
    Identity {
        identity: Box<CaIdentity>,
        reply: oneshot::Sender<Result<bool>>,
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
}

impl TuiAnswerer {
    pub(super) fn new(tx: UnboundedSender<UiRequest>) -> TuiAnswerer {
        TuiAnswerer { tx }
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

    async fn confirm_identity(&mut self, identity: &CaIdentity) -> Result<bool> {
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
        default: Option<String>,
        required: bool,
        secret: bool,
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
    Confirm {
        field: Field,
        yes: bool,
        reply: Option<oneshot::Sender<Result<bool>>>,
    },
    Identity {
        identity: Box<CaIdentity>,
        reply: Option<oneshot::Sender<Result<bool>>>,
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
                default,
                required,
                secret: false,
                input: String::new(),
                error: None,
                reply: Some(TextReply::Text(reply)),
            }),
            UiRequest::Secret { field, reply } => Some(Modal::Text {
                field,
                default: None,
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
            UiRequest::Confirm { field, default, reply } => {
                Some(Modal::Confirm { field, yes: default, reply: Some(reply) })
            }
            UiRequest::Identity { identity, reply } => {
                Some(Modal::Identity { identity, reply: Some(reply) })
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
            Modal::Identity { .. } | Modal::Recovery { .. } => None,
        }
    }

    /// Handle a key. Returns `true` when the modal has resolved (answer sent)
    /// and should be removed.
    pub(super) fn on_key(&mut self, code: KeyCode) -> bool {
        match self {
            Modal::Text { required, default, secret, input, error, reply, .. } => match code {
                KeyCode::Esc => {
                    send_text(reply.take(), Err(anyhow!("cancelled")));
                    true
                }
                KeyCode::Enter => {
                    if input.is_empty() {
                        match (default.as_ref(), *required) {
                            (Some(d), _) => {
                                resolve_text(reply.take(), Some(d.clone()), *secret);
                                true
                            }
                            (None, true) => {
                                *error = Some("a value is required".to_string());
                                false
                            }
                            (None, false) => {
                                resolve_text(reply.take(), None, *secret);
                                true
                            }
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
                    state.select_previous();
                    false
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    state.select_next();
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
            Modal::Recovery { .. } => matches!(code, KeyCode::Enter | KeyCode::Char(' ')),
        }
    }

    pub(super) fn render(&self, f: &mut Frame, screen: Rect) {
        match self {
            Modal::Text { secret, input, error, .. } => {
                let field = self.field().unwrap();
                let shown = if *secret { "•".repeat(input.chars().count()) } else { input.clone() };
                let mut lines = vec![
                    Line::from(field.help().dim()),
                    Line::from(""),
                    Line::from(vec![Span::raw("> "), Span::styled(shown, Style::default().add_modifier(Modifier::BOLD))]),
                ];
                if let Some(e) = error {
                    lines.push(Line::from(Span::styled(e.clone(), Style::default().fg(Color::Red))));
                }
                lines.push(Line::from(""));
                lines.push(Line::from(" Enter accept · Esc cancel ".dim()));
                popup(f, screen, field.label(), lines, 60, 9);
            }
            Modal::Choice { field, choices, state, .. } => {
                let items: Vec<ListItem> = choices.iter().map(|c| ListItem::new(c.clone())).collect();
                let height = (choices.len() as u16 + 4).min(screen.height.saturating_sub(4));
                let area = widgets::centered(50, height, screen);
                f.render_widget(Clear, area);
                let mut st = state.clone();
                let list = List::new(items)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title(format!(" {} ", field.label()))
                            .title_bottom(Line::from(" ↑/↓ · Enter select · Esc cancel ").dim()),
                    )
                    .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD))
                    .highlight_symbol("▸ ");
                f.render_stateful_widget(list, area, &mut st);
            }
            Modal::Confirm { field, yes, .. } => {
                let opts = Line::from(vec![
                    button("Yes", *yes),
                    Span::raw("   "),
                    button("No", !*yes),
                ]);
                let lines = vec![
                    Line::from(field.help().dim()),
                    Line::from(""),
                    opts,
                    Line::from(""),
                    Line::from(" ←/→ · y/n · Enter · Esc cancel ".dim()),
                ];
                popup(f, screen, field.label(), lines, 60, 9);
            }
            Modal::Identity { identity, .. } => {
                let roles = identity
                    .roles
                    .iter()
                    .map(|r| format!("{r:?}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut lines = vec![
                    Line::from("Verify this is the network you intend to trust, out of band,"),
                    Line::from("then accept. Everything after is pinned to this fingerprint.".dim()),
                    Line::from(""),
                    Line::from(vec![Span::styled("  domain: ".to_string(), Style::default().dim()), Span::raw(identity.domain.clone())]),
                    Line::from(vec![Span::styled("   roles: ".to_string(), Style::default().dim()), Span::raw(roles)]),
                    Line::from(""),
                ];
                lines.extend(widgets::identicon_lines(&identity.fingerprint));
                lines.push(Line::from(""));
                for chunk in group_fingerprint(&identity.fingerprint) {
                    lines.push(Line::from(Span::styled(chunk, Style::default().fg(Color::Cyan))));
                }
                lines.push(Line::from(""));
                lines.push(Line::from(" a/Enter accept · Esc/r reject ".dim()));
                popup(f, screen, "Confirm network identity", lines, 60, 22);
            }
            Modal::Recovery { password, .. } => {
                let lines = vec![
                    Line::from(Span::styled(
                        "CA RECOVERY PASSWORD — shown once, never stored.",
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                    )),
                    Line::from(""),
                    Line::from(Span::styled(
                        password.clone(),
                        Style::default().fg(Color::White).bg(Color::DarkGray).add_modifier(Modifier::BOLD),
                    )),
                    Line::from(""),
                    Line::from("Write it down and lock it in a safe now. It is the only off-box"),
                    Line::from("credential that can unlock the CA key. There is no second chance.".dim()),
                    Line::from(""),
                    Line::from(Span::styled(" Enter — I have saved it ", Style::default().fg(Color::Black).bg(Color::Yellow))),
                ];
                popup(f, screen, "Save this now", lines, 66, 12);
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

/// A highlighted/plain button span for the confirm modal.
fn button(label: &str, selected: bool) -> Span<'static> {
    let text = format!("  {label}  ");
    if selected {
        Span::styled(text, Style::default().fg(Color::Black).bg(Color::Cyan).add_modifier(Modifier::BOLD))
    } else {
        Span::styled(text, Style::default().add_modifier(Modifier::DIM))
    }
}

/// The fingerprint text split into its space-separated 5-char groups, four per
/// line, for a compact block in the identity modal.
fn group_fingerprint(fp: &Fingerprint) -> Vec<String> {
    let text = fp.text();
    let groups: Vec<&str> = text.split(' ').collect();
    groups.chunks(4).map(|c| c.join(" ")).collect()
}

/// Draw a bordered, centered popup with the given title and body lines.
fn popup(f: &mut Frame, screen: Rect, title: &str, lines: Vec<Line<'static>>, w: u16, h: u16) {
    let area = widgets::centered(w, h, screen);
    f.render_widget(Clear, area);
    let body = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .block(Block::default().borders(Borders::ALL).title(format!(" {title} ")));
    f.render_widget(body, area);
}
