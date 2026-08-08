//! The [`TuiAnswerer`] — the widget-backed [`Answerer`] the TUI hands to the
//! library ops, plus the modal widgets that collect each answer.
//!
//! The op (an install planner or a remote-admin operation) runs on a spawned
//! task and calls `ans.text().await`, `ans.confirm().await`, … The answerer
//! turns each call into a [`UiRequest`] sent over a channel to the UI loop and
//! parks on a `oneshot` for the reply. The UI loop pops the request into a
//! [`Modal`], collects the answer through key events, and sends it back. Sync
//! notifications (`note`/`warn`/`progress`/`show_verification_code`) are
//! fire-and-forget. Recovery-password delivery is an awaited question: the op
//! does not commit a new CA until the operator explicitly acknowledges it.

use super::{theme, widgets};
use anyhow::{Result, anyhow};
use crossterm::event::{KeyCode, KeyModifiers};
use netidx_admin::{
    answer::{
        AdminDomainChoice, AdminDomainOption, Answerer, Field, OneTimeSecret, Progress,
    },
    transport::CaIdentity,
};
use netidx_admin_proto::{Secret, fingerprint::Fingerprint};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
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

/// One selectable resolver in the parent picker — display only; the caller holds
/// the parallel address data (the `ResolverAddr` + owning admin) by index.
#[cfg(unix)]
pub(super) struct ParentRow {
    /// e.g. `resolver-eu-a  10.0.60.15:4564`.
    pub(super) label: String,
    /// The resolver's cluster base, shown as info.
    pub(super) base: String,
}

/// The operator's pick from the parent picker.
#[cfg(unix)]
pub(super) enum ParentSelection {
    /// The ticked resolver rows (indices into the offered slice).
    Resolvers(Vec<usize>),
    /// None of them — enter an admin-server address manually instead.
    Manual,
}

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
    /// Pick a discovered admin domain (each shown with its CA glyph + fingerprint),
    /// or the trailing "enter an address manually" option.
    SelectAdminDomain {
        admin_domains: Vec<AdminDomainOption>,
        reply: oneshot::Sender<Result<AdminDomainChoice>>,
    },
    /// Multi-select the parent's resolver servers (each with its resolver cluster base), or the
    /// trailing "enter an address manually" option.
    #[cfg(unix)]
    SelectParent {
        rows: Vec<ParentRow>,
        reply: oneshot::Sender<Result<ParentSelection>>,
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
    OneTimeSecret {
        secret: OneTimeSecret,
        password: String,
        reply: oneshot::Sender<Result<()>>,
    },
    VerificationCode {
        purpose: String,
        code: Fingerprint,
    },
    ClearVerificationCode,
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
    pub(super) fn with_glyph(
        tx: UnboundedSender<UiRequest>,
        fp: Fingerprint,
    ) -> TuiAnswerer {
        TuiAnswerer { tx, accept_glyph: Some(fp) }
    }

    /// Edit `seed` in the operator's `$EDITOR` (via the UI loop, which suspends
    /// the terminal), validating+normalizing with `validate`. Returns the
    /// normalized text, or an error if the operator aborted. Inherent (not part
    /// of [`Answerer`]) — the editor loop is a TUI-only concern, so only the
    /// concrete op bodies that hold a `TuiAnswerer` reach it.
    pub(super) async fn edit(
        &self,
        seed: String,
        validate: EditValidator,
    ) -> Result<String> {
        self.ask(|reply| UiRequest::Editor { seed, validate, reply }).await
    }

    /// Multi-select the parent's resolver servers from the admin domain map, or fall
    /// back to a typed address. Inherent (TUI-only), like [`Self::edit`] — the
    /// strict CLI takes an explicit `--parent-*` instead.
    #[cfg(unix)]
    pub(super) async fn select_parent(
        &self,
        rows: Vec<ParentRow>,
    ) -> Result<ParentSelection> {
        self.ask(|reply| UiRequest::SelectParent { rows, reply }).await
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

    async fn select_admin_domain(
        &mut self,
        admin_domains: &[AdminDomainOption],
    ) -> Result<AdminDomainChoice> {
        let admin_domains = admin_domains.to_vec();
        self.ask(|reply| UiRequest::SelectAdminDomain { admin_domains, reply }).await
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
        let _ = self.tx.send(UiRequest::VerificationCode {
            purpose: purpose.to_string(),
            code: *code,
        });
    }

    fn clear_verification_code(&mut self) {
        let _ = self.tx.send(UiRequest::ClearVerificationCode);
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

    async fn show_one_time_secret(
        &mut self,
        secret: OneTimeSecret,
        password: &str,
    ) -> Result<()> {
        let password = password.to_string();
        self.ask(|reply| UiRequest::OneTimeSecret { secret, password, reply }).await
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
    /// Pick one of the discovered admin domains (rendered with its glyph +
    /// fingerprint) or the trailing manual-entry row. Selection index
    /// `admin domains.len()` is the manual row.
    SelectAdminDomain {
        admin_domains: Vec<AdminDomainOption>,
        state: ListState,
        reply: Option<oneshot::Sender<Result<AdminDomainChoice>>>,
    },
    /// Multi-select parent resolvers (checkbox per row) with a trailing
    /// manual-entry row. `checked` parallels `rows`; the cursor index
    /// `rows.len()` is the manual row.
    #[cfg(unix)]
    SelectParent {
        rows: Vec<ParentRow>,
        checked: Vec<bool>,
        state: ListState,
        reply: Option<oneshot::Sender<Result<ParentSelection>>>,
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
    OneTimeSecret {
        secret: OneTimeSecret,
        password: String,
        reply: Option<oneshot::Sender<Result<()>>>,
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
            UiRequest::SelectAdminDomain { admin_domains, reply } => {
                let mut state = ListState::default();
                state.select(Some(0));
                Some(Modal::SelectAdminDomain {
                    admin_domains,
                    state,
                    reply: Some(reply),
                })
            }
            #[cfg(unix)]
            UiRequest::SelectParent { rows, reply } => {
                let mut state = ListState::default();
                state.select(Some(0));
                let checked = vec![false; rows.len()];
                Some(Modal::SelectParent { rows, checked, state, reply: Some(reply) })
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
            UiRequest::OneTimeSecret { secret, password, reply } => {
                Some(Modal::OneTimeSecret { secret, password, reply: Some(reply) })
            }
            // Non-blocking requests: they carry no reply channel, so there is
            // nothing to put on screen and nothing to answer. Every BLOCKING
            // variant must be handled above — one that reached here would
            // never get its reply, and the running op would hang forever.
            UiRequest::Editor { .. }
            | UiRequest::VerificationCode { .. }
            | UiRequest::ClearVerificationCode
            | UiRequest::Note(_)
            | UiRequest::Warn(_)
            | UiRequest::Progress(_) => None,
        }
    }

    /// The field this modal is answering, for help text (identity/recovery have
    /// none).
    fn field(&self) -> Option<Field> {
        match self {
            Modal::Text { field, .. }
            | Modal::Choice { field, .. }
            | Modal::Confirm { field, .. } => Some(*field),
            #[cfg(unix)]
            Modal::SelectParent { .. } => None,
            Modal::SelectAdminDomain { .. }
            | Modal::Identity { .. }
            | Modal::Announce { .. }
            | Modal::AnnounceIdentity { .. }
            | Modal::OneTimeSecret { .. } => None,
        }
    }

    /// Handle a key. Returns `true` when the modal has resolved (answer sent)
    /// and should be removed.
    #[cfg(all(test, unix))]
    pub(super) fn on_key(&mut self, code: KeyCode) -> bool {
        self.on_key_with_modifiers(code, KeyModifiers::NONE)
    }

    /// Modifier-aware input path used by the real terminal. Keeping the plain
    /// [`Self::on_key`] wrapper makes modal unit tests concise.
    pub(super) fn on_key_with_modifiers(
        &mut self,
        code: KeyCode,
        modifiers: KeyModifiers,
    ) -> bool {
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
                KeyCode::Char('u') if modifiers.contains(KeyModifiers::CONTROL) => {
                    input.clear();
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
                    let sel = state
                        .selected()
                        .unwrap_or(0)
                        .min(choices.len().saturating_sub(1));
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(choices[sel].clone()));
                    }
                    true
                }
                _ => false,
            },
            // The list is the discovered admin domains, then a "poll for more" row,
            // then a manual-entry row: indices `admin domains.len()` and
            // `admin domains.len() + 1` respectively.
            Modal::SelectAdminDomain { admin_domains, state, reply } => match code {
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
                    let i = state
                        .selected()
                        .map_or(0, |i| (i + 1).min(admin_domains.len() + 1));
                    state.select(Some(i));
                    false
                }
                KeyCode::Enter => {
                    let sel = state.selected().unwrap_or(0).min(admin_domains.len() + 1);
                    let choice = if sel == admin_domains.len() {
                        AdminDomainChoice::PollMore
                    } else if sel == admin_domains.len() + 1 {
                        AdminDomainChoice::Manual
                    } else {
                        AdminDomainChoice::Discovered(sel)
                    };
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(choice));
                    }
                    true
                }
                _ => false,
            },
            // rows.len() rows + one trailing manual row; Space ticks a resolver,
            // Enter confirms the ticked set (or the cursor row if none ticked),
            // Enter on the manual row picks Manual.
            #[cfg(unix)]
            Modal::SelectParent { rows, checked, state, reply } => match code {
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
                    let i = state.selected().map_or(0, |i| (i + 1).min(rows.len()));
                    state.select(Some(i));
                    false
                }
                KeyCode::Char(' ') => {
                    if let Some(c) = state.selected().and_then(|i| checked.get_mut(i)) {
                        *c = !*c;
                    }
                    false
                }
                KeyCode::Enter => {
                    let sel = state.selected().unwrap_or(0).min(rows.len());
                    let choice = if sel == rows.len() {
                        ParentSelection::Manual
                    } else {
                        let picks: Vec<usize> = checked
                            .iter()
                            .enumerate()
                            .filter_map(|(i, &c)| c.then_some(i))
                            .collect();
                        // Enter with nothing ticked picks the row under the cursor,
                        // so a single-parent choice needs no Space.
                        let picks = if picks.is_empty() { vec![sel] } else { picks };
                        ParentSelection::Resolvers(picks)
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
                KeyCode::Left
                | KeyCode::Right
                | KeyCode::Tab
                | KeyCode::Char('h')
                | KeyCode::Char('l') => {
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
                KeyCode::Esc
                | KeyCode::Char('r')
                | KeyCode::Char('R')
                | KeyCode::Char('n') => {
                    if let Some(tx) = reply.take() {
                        let _ = tx.send(Ok(false));
                    }
                    true
                }
                _ => false,
            },
            Modal::Announce { reply, .. } | Modal::AnnounceIdentity { reply, .. } => {
                match code {
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
                }
            }
            Modal::OneTimeSecret { reply, .. } => match code {
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
        }
    }

    pub(super) fn render(&self, f: &mut Frame, screen: Rect) {
        match self {
            Modal::Text { secret, input, error, .. } => {
                let field = self.field().unwrap();
                let w = 64u16.min(screen.width.saturating_sub(4)).max(30);
                // Size the help area to the wrapped help so it's never truncated.
                let help_h = widgets::wrapped_rows(field.help(), w - 2);
                // No buttons: the field is the whole interaction — Enter submits,
                // Esc cancels (the footer says so). The value is the answer.
                let inner_rows = help_h + 1 /*spacer*/ + 1 /*field*/ + 1 /*error*/;
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
                    Constraint::Min(0),
                ])
                .split(inner);
                f.render_widget(
                    Paragraph::new(field.help())
                        .style(theme::hint_style())
                        .wrap(Wrap { trim: true }),
                    rows[0],
                );
                let shown = if *secret {
                    "•".repeat(input.chars().count())
                } else {
                    input.clone()
                };
                let field_row = rows[2];
                f.render_widget(
                    Paragraph::new(shown).style(theme::field_style()),
                    field_row,
                );
                if let Some(e) = error {
                    let err = Style::default().bg(theme::PANEL_BG).fg(theme::ACCENT);
                    f.render_widget(Paragraph::new(e.clone()).style(err), rows[3]);
                }
                // Focus: put the terminal cursor at the end of the field.
                let cx = field_row.x
                    + (input.chars().count() as u16)
                        .min(field_row.width.saturating_sub(1));
                f.set_cursor_position((cx, field_row.y));
            }
            Modal::Choice { field, choices, state, .. } => {
                let sel =
                    state.selected().unwrap_or(0).min(choices.len().saturating_sub(1));
                let w = 64u16.min(screen.width.saturating_sub(4)).max(30);
                let help_h = widgets::wrapped_rows(field.help(), w - 2);
                let list_h = choices.len() as u16;
                let h = (help_h + 1 + list_h + 2).min(screen.height);
                let area = widgets::centered(w, h, screen);
                widgets::shadow(f, area, screen);
                f.render_widget(Clear, area);
                let block = theme::dialog_block(field.label()).title_bottom(Line::from(
                    Span::styled(
                        " ↑/↓ move · Enter select · Esc cancel ",
                        theme::hint_style(),
                    ),
                ));
                let inner = block.inner(area);
                f.render_widget(block, area);
                let rows = Layout::vertical([
                    Constraint::Length(help_h), // help
                    Constraint::Length(1),      // spacer
                    Constraint::Min(0),         // radio list
                ])
                .split(inner);
                f.render_widget(
                    Paragraph::new(field.help())
                        .style(theme::hint_style())
                        .wrap(Wrap { trim: true }),
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
            Modal::SelectAdminDomain { admin_domains, state, .. } => {
                const POLL_MORE: &str = "Search again for more admin domains";
                const MANUAL: &str = "Enter an address manually…";
                let sel = state.selected().unwrap_or(0).min(admin_domains.len() + 1);
                let w = 68u16.min(screen.width.saturating_sub(4)).max(40);
                // Body holds the list (left) beside the selected glyph (right, 10
                // tile rows + a blank + up to 3 fingerprint lines). The list is
                // the admin domains plus the poll-more and manual-entry rows.
                let body_h =
                    (admin_domains.len() as u16 + 2).max(widgets::IDENTICON_HEIGHT + 4);
                let h = (1 /*header*/ + 1 /*spacer*/ + body_h + 2/*borders*/)
                    .min(screen.height);
                let area = widgets::centered(w, h, screen);
                widgets::shadow(f, area, screen);
                f.render_widget(Clear, area);
                let block = theme::dialog_block(Field::SelectAdminDomain.label())
                    .title_bottom(Line::from(Span::styled(
                        " ↑/↓ move · Enter select · Esc cancel ",
                        theme::hint_style(),
                    )));
                let inner = block.inner(area);
                f.render_widget(block, area);
                let rows = Layout::vertical([
                    Constraint::Length(1), // header
                    Constraint::Length(1), // spacer
                    Constraint::Min(0),    // list | glyph
                ])
                .split(inner);
                let header = if admin_domains.is_empty() {
                    "No admin domains found on the local network."
                } else {
                    "netidx admin domains discovered on the local network:"
                };
                f.render_widget(
                    Paragraph::new(header)
                        .style(theme::hint_style())
                        .wrap(Wrap { trim: true }),
                    rows[0],
                );
                let cols =
                    Layout::horizontal([Constraint::Min(20), Constraint::Length(26)])
                        .split(rows[2]);
                let items: Vec<ListItem> = admin_domains
                    .iter()
                    .map(|n| ListItem::new(n.domain.clone()))
                    .chain([ListItem::new(POLL_MORE), ListItem::new(MANUAL)])
                    .collect();
                let mut st = *state;
                let list = List::new(items)
                    .style(theme::panel_style())
                    .highlight_style(theme::selected_style());
                f.render_stateful_widget(list, cols[0], &mut st);
                // Right: the selected admin domain's glyph + grouped fingerprint, or a
                // hint for the poll-more / manual-entry rows.
                let glyph = match admin_domains.get(sel) {
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
                    None if sel == admin_domains.len() => vec![Line::from(Span::styled(
                        "Browse the network again and add any admin domains that answer.",
                        theme::hint_style(),
                    ))],
                    None => vec![Line::from(Span::styled(
                        "Type an admin-server address (host:port) to connect directly.",
                        theme::hint_style(),
                    ))],
                };
                // trim:false — the identicon rows carry leading "off" cells as
                // spaces; trimming them would shift the glyph and corrupt it.
                f.render_widget(
                    Paragraph::new(glyph)
                        .style(theme::panel_style())
                        .wrap(Wrap { trim: false }),
                    cols[1],
                );
            }
            #[cfg(unix)]
            Modal::SelectParent { rows, checked, state, .. } => {
                const MANUAL: &str = "Enter an address manually…";
                let w = 76u16.min(screen.width.saturating_sub(4)).max(48);
                let body_h = (rows.len() as u16 + 1).max(6);
                let h = (1 /*header*/ + 1 /*spacer*/ + body_h + 2/*borders*/)
                    .min(screen.height);
                let area = widgets::centered(w, h, screen);
                widgets::shadow(f, area, screen);
                f.render_widget(Clear, area);
                let block = theme::dialog_block("Select the parent resolver(s)")
                    .title_bottom(Line::from(Span::styled(
                        " ↑/↓ move · Space tick · Enter confirm · Esc cancel ",
                        theme::hint_style(),
                    )));
                let inner = block.inner(area);
                f.render_widget(block, area);
                let vrows = Layout::vertical([
                    Constraint::Length(1), // header
                    Constraint::Length(1), // spacer
                    Constraint::Min(0),    // list
                ])
                .split(inner);
                let header = if rows.is_empty() {
                    "No resolver servers in the map — enter an address manually:"
                } else {
                    "Tick the resolvers that make up the parent, then Enter:"
                };
                f.render_widget(
                    Paragraph::new(header)
                        .style(theme::hint_style())
                        .wrap(Wrap { trim: true }),
                    vrows[0],
                );
                let items: Vec<ListItem> = rows
                    .iter()
                    .enumerate()
                    .map(|(i, r)| {
                        let marker = if checked.get(i).copied().unwrap_or(false) {
                            "[x]"
                        } else {
                            "[ ]"
                        };
                        ListItem::new(Line::from(vec![
                            Span::styled(
                                format!("{marker} {:<40}", r.label),
                                theme::panel_style(),
                            ),
                            Span::styled(r.base.clone(), theme::hint_style()),
                        ]))
                    })
                    .chain(std::iter::once(ListItem::new(Line::from(Span::styled(
                        MANUAL,
                        theme::hint_style(),
                    )))))
                    .collect();
                let mut st = *state;
                let list = List::new(items)
                    .style(theme::panel_style())
                    .highlight_style(theme::selected_style());
                f.render_stateful_widget(list, vrows[2], &mut st);
            }
            Modal::Confirm { field, yes, .. } => {
                let opts = Line::from(vec![
                    theme::button("Yes", *yes),
                    Span::raw("   "),
                    theme::button("No", !*yes),
                ]);
                let lines = vec![
                    Line::from(Span::styled(field.help(), theme::hint_style())),
                    Line::from(""),
                    opts,
                    Line::from(""),
                    Line::from(Span::styled(
                        " ←/→ · y/n · Enter · Esc cancel ",
                        theme::hint_style(),
                    )),
                ];
                popup(f, screen, field.label(), lines, 60);
            }
            Modal::Identity { identity, .. } => {
                let roles = identity
                    .roles
                    .iter()
                    .map(|r| match r {
                        netidx_admin_proto::Role::Ca => "CA",
                        netidx_admin_proto::Role::Resolver => "resolver",
                        netidx_admin_proto::Role::IdMap => "id-map",
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let label = |s: &str| Span::styled(s.to_string(), theme::hint_style());
                let val = |s: String| Span::styled(s, theme::panel_style());
                let mut lines = vec![
                    // These are fixed Lines, not a wrapped Paragraph, so each
                    // must fit the dialog's 58-column interior on its own.
                    Line::from(Span::styled(
                        "Verify out of band that this is the admin domain you",
                        theme::panel_style(),
                    )),
                    Line::from(Span::styled(
                        "intend to trust. Everything after pins to this glyph.",
                        theme::hint_style(),
                    )),
                    Line::from(""),
                    Line::from(vec![label("  domain: "), val(identity.domain.clone())]),
                    Line::from(vec![label("   roles: "), val(roles)]),
                    Line::from(""),
                ];
                lines.extend(widgets::identicon_lines(&identity.fingerprint));
                lines.push(Line::from(""));
                let fp = Style::default()
                    .bg(theme::PANEL_BG)
                    .fg(Color::Rgb(0, 0, 150))
                    .add_modifier(Modifier::BOLD);
                for chunk in widgets::group_fingerprint(&identity.fingerprint) {
                    lines.push(Line::from(Span::styled(chunk, fp)));
                }
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    " a/Enter accept · Esc/r reject ",
                    theme::hint_style(),
                )));
                popup(f, screen, "Confirm admin domain identity", lines, 60);
            }
            Modal::Announce { title, body, .. } => {
                let mut lines = body
                    .lines()
                    .map(|line| {
                        Line::from(Span::styled(line.to_string(), theme::panel_style()))
                    })
                    .collect::<Vec<_>>();
                lines.push(Line::from(""));
                lines.push(Line::from(Span::styled(
                    " Press Enter to continue ",
                    theme::selected_style(),
                )));
                popup(f, screen, title, lines, 64);
            }
            Modal::AnnounceIdentity { body, code, .. } => {
                let mut lines = body
                    .lines()
                    .map(|line| {
                        Line::from(Span::styled(line.to_string(), theme::panel_style()))
                    })
                    .collect::<Vec<_>>();
                lines.push(Line::from(""));
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
            Modal::OneTimeSecret { secret, password, .. } => {
                let pw = Style::default()
                    .bg(Color::Rgb(255, 249, 196))
                    .fg(Color::Rgb(0, 0, 0))
                    .add_modifier(Modifier::BOLD);
                let (title, heading, advice) = match secret {
                    OneTimeSecret::CaRecovery => (
                        "CA recovery password".to_string(),
                        "CA recovery password — shown once, never stored:".to_string(),
                        "Use this password to unlock the CA key in an emergency. Write it \
                         down and store it in a safe place now — it is the only off-box \
                         credential that can unlock the CA key, and there is no second \
                         chance to read it."
                            .to_string(),
                    ),
                    OneTimeSecret::AdminPassword { admin } => (
                        format!("One-time password for {admin}"),
                        format!("{admin}'s one-time password — shown once, never stored:"),
                        format!(
                            "Give this to {admin} over a channel you trust. It lets them \
                             set a password and nothing else — every other command is \
                             refused until they do. There is no second chance to read it, \
                             but you can issue another with Reset password."
                        ),
                    ),
                };
                let lines = vec![
                    Line::from(Span::styled(heading, theme::panel_style())),
                    Line::from(""),
                    Line::from(Span::styled(password.clone(), pw)),
                    Line::from(""),
                    Line::from(Span::styled(advice, theme::panel_style())),
                    Line::from(""),
                    Line::from(Span::styled(
                        " Enter — I have saved it ",
                        theme::selected_style(),
                    )),
                ];
                popup(f, screen, &title, lines, 66);
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use netidx_admin::{ca, plan::ca_setup};
    use netidx_admin_proto::Role;
    use ratatui::{Terminal, backend::TestBackend};
    use std::time::Duration;

    fn rows() -> Vec<ParentRow> {
        vec![
            ParentRow { label: "10.0.0.1:4564".to_string(), base: "/".to_string() },
            ParentRow { label: "10.0.60.1:4564".to_string(), base: "/ap".to_string() },
        ]
    }

    fn select_parent_modal() -> (Modal, oneshot::Receiver<Result<ParentSelection>>) {
        let (tx, rx) = oneshot::channel();
        let modal =
            Modal::from_request(UiRequest::SelectParent { rows: rows(), reply: tx })
                .unwrap();
        (modal, rx)
    }

    fn offline_ca_opts(dir: std::path::PathBuf) -> ca_setup::NewCaOpts {
        ca_setup::NewCaOpts {
            dir,
            common_name: Some("CA.example.com".into()),
            domain: Some("example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec![],
            key_bits: 2048,
            ca_validity: Duration::from_secs(30 * 86400),
            leaf_validity: ca::DEFAULT_LEAF_VALIDITY,
            ca_renew_threshold: ca::DEFAULT_CA_RENEW_THRESHOLD,
            admin: Some("root".into()),
            allowed_san: vec!["*.example.com".into()],
            max_validity: ca::DEFAULT_LEAF_VALIDITY,
            id_map_groups: vec!["users".into()],
            server_enroll_scopes: vec!["/".into()],
            server_enroll_roles: Role::Resolver | Role::IdMap,
            insecure_no_tpm: true,
            setup_server: Some(false),
            listen: None,
            listen_hint: None,
            units_dir: None,
        }
    }

    fn draw(modal: &Modal, w: u16, h: u16) -> Terminal<TestBackend> {
        let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
        terminal.draw(|f| modal.render(f, f.area())).unwrap();
        terminal
    }

    fn render(modal: &Modal, w: u16, h: u16) -> String {
        draw(modal, w, h)
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect()
    }

    #[test]
    fn ctrl_u_clears_a_prefilled_text_field() {
        let (tx, mut rx) = oneshot::channel();
        let mut modal = Modal::from_request(UiRequest::Text {
            field: Field::AdminDomainName,
            default: Some("local".into()),
            required: false,
            reply: tx,
        })
        .unwrap();
        assert!(!modal.on_key_with_modifiers(KeyCode::Char('u'), KeyModifiers::CONTROL,));
        for c in "netidx.test".chars() {
            assert!(!modal.on_key(KeyCode::Char(c)));
        }
        assert!(modal.on_key(KeyCode::Enter));
        assert_eq!(rx.try_recv().unwrap().unwrap(), Some("netidx.test".into()));
    }

    #[test]
    fn renders_checkboxes_levels_and_manual_row() {
        let (modal, _rx) = select_parent_modal();
        let out = render(&modal, 90, 16);
        assert!(out.contains("[ ]"), "unticked marker missing: {out:?}");
        assert!(
            out.contains("10.0.0.1:4564") && out.contains("10.0.60.1:4564"),
            "labels: {out:?}"
        );
        assert!(out.contains("/ap"), "base column missing: {out:?}");
        assert!(out.contains("Enter an address manually"), "manual row missing: {out:?}");
    }

    #[test]
    fn space_ticks_the_cursor_row() {
        let (mut modal, _rx) = select_parent_modal();
        assert!(!modal.on_key(KeyCode::Char(' ')), "Space must not close the modal");
        let out = render(&modal, 90, 16);
        assert!(out.contains("[x]"), "ticked marker missing after Space: {out:?}");
    }

    #[test]
    fn enter_confirms_the_ticked_set() {
        let (mut modal, mut rx) = select_parent_modal();
        modal.on_key(KeyCode::Char(' ')); // tick row 0
        modal.on_key(KeyCode::Down); // move cursor to row 1
        modal.on_key(KeyCode::Char(' ')); // tick row 1
        assert!(modal.on_key(KeyCode::Enter), "Enter must close the modal");
        match rx.try_recv().unwrap().unwrap() {
            ParentSelection::Resolvers(idxs) => assert_eq!(idxs, vec![0, 1]),
            ParentSelection::Manual => panic!("expected Resolvers, got Manual"),
        }
    }

    #[test]
    fn enter_with_nothing_ticked_picks_the_cursor_row() {
        let (mut modal, mut rx) = select_parent_modal();
        modal.on_key(KeyCode::Down); // cursor on row 1, nothing ticked
        assert!(modal.on_key(KeyCode::Enter));
        match rx.try_recv().unwrap().unwrap() {
            ParentSelection::Resolvers(idxs) => assert_eq!(idxs, vec![1]),
            ParentSelection::Manual => panic!("expected Resolvers, got Manual"),
        }
    }

    #[test]
    fn enter_on_the_manual_row_picks_manual() {
        let (mut modal, mut rx) = select_parent_modal();
        // Two rows + a trailing manual row: from row 0, two Downs lands on it.
        modal.on_key(KeyCode::Down);
        modal.on_key(KeyCode::Down);
        assert!(modal.on_key(KeyCode::Enter));
        match rx.try_recv().unwrap().unwrap() {
            ParentSelection::Manual => {}
            ParentSelection::Resolvers(_) => panic!("expected Manual, got Resolvers"),
        }
    }

    #[test]
    fn recovery_modal_requires_and_reports_explicit_acknowledgement() {
        let (tx, mut rx) = oneshot::channel();
        let mut modal = Modal::from_request(UiRequest::OneTimeSecret {
            secret: OneTimeSecret::CaRecovery,
            password: "AAAA BBBB".into(),
            reply: tx,
        })
        .unwrap();
        assert!(!modal.on_key(KeyCode::Char('x')));
        assert!(matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        assert!(modal.on_key(KeyCode::Enter));
        assert!(rx.try_recv().unwrap().is_ok());
    }

    #[test]
    fn identity_announcement_glyph_not_clipped() {
        let (tx, _rx) = oneshot::channel();
        let modal = Modal::from_request(UiRequest::AnnounceIdentity {
            body: "The certificate authority was created.".to_string(),
            code: Fingerprint::of_der(b"new CA identity"),
            reply: tx,
        })
        .unwrap();
        let terminal = draw(&modal, 90, 24);
        assert_eq!(
            widgets::rendered_identicon_rows(terminal.backend().buffer()),
            widgets::IDENTICON_HEIGHT as usize
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelling_recovery_ack_rolls_back_new_ca() {
        let scratch = tempfile::tempdir().unwrap();
        let ca_dir = scratch.path().join("ca");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ans = TuiAnswerer::new(tx);
        let opts = offline_ca_opts(ca_dir.clone());
        let config_lock =
            netidx_admin::config_lock::ConfigDirLock::acquire(scratch.path()).unwrap();
        let create = tokio::spawn(async move {
            ca_setup::create_vaulted_ca(&mut ans, &config_lock, opts).await
        });

        let mut cancelled = false;
        while let Some(req) = rx.recv().await {
            match req {
                UiRequest::AnnounceIdentity { body, code, reply } => {
                    let mut modal = Modal::from_request(UiRequest::AnnounceIdentity {
                        body,
                        code,
                        reply,
                    })
                    .unwrap();
                    assert!(modal.on_key(KeyCode::Enter));
                }
                UiRequest::OneTimeSecret { secret, password, reply } => {
                    let mut modal = Modal::from_request(UiRequest::OneTimeSecret {
                        secret,
                        password,
                        reply,
                    })
                    .unwrap();
                    assert!(modal.on_key(KeyCode::Esc));
                    cancelled = true;
                    break;
                }
                UiRequest::Warn(_) | UiRequest::Note(_) | UiRequest::Progress(_) => {}
                _ => panic!("unexpected question during offline CA creation"),
            }
        }
        assert!(cancelled, "recovery acknowledgement was never requested");
        assert!(create.await.unwrap().is_err());
        assert!(!ca_dir.exists(), "cancelled CA must never become live");
        assert_eq!(
            std::fs::read_dir(scratch.path())
                .unwrap()
                .filter(|entry| {
                    entry
                        .as_ref()
                        .is_ok_and(|entry| entry.file_name() != ".CA.netidx.lock")
                })
                .count(),
            0,
            "staged CA state must be removed when the operation is cancelled"
        );
    }

    #[test]
    fn esc_cancels() {
        let (mut modal, mut rx) = select_parent_modal();
        assert!(modal.on_key(KeyCode::Esc), "Esc must close the modal");
        assert!(rx.try_recv().unwrap().is_err(), "Esc must send a cancel error");
    }

    fn select_admin_domain_modal() -> (Modal, oneshot::Receiver<Result<AdminDomainChoice>>)
    {
        // Empty admin domain list: index 0 is the poll-more row, index 1 the manual row.
        let (tx, rx) = oneshot::channel();
        let modal = Modal::from_request(UiRequest::SelectAdminDomain {
            admin_domains: vec![],
            reply: tx,
        })
        .unwrap();
        (modal, rx)
    }

    #[test]
    fn select_admin_domain_enter_polls_more() {
        let (mut modal, mut rx) = select_admin_domain_modal();
        assert!(modal.on_key(KeyCode::Enter), "Enter must close the modal");
        assert_eq!(rx.try_recv().unwrap().unwrap(), AdminDomainChoice::PollMore);
    }

    #[test]
    fn select_admin_domain_manual_is_the_last_row() {
        let (mut modal, mut rx) = select_admin_domain_modal();
        modal.on_key(KeyCode::Down); // past poll-more, onto manual
        assert!(modal.on_key(KeyCode::Enter));
        assert_eq!(rx.try_recv().unwrap().unwrap(), AdminDomainChoice::Manual);
    }
}
