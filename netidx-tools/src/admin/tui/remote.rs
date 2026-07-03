//! Tab 2 — **Remote admin**: connect to a (local or remote) admin server and
//! drive its enrollment queue, delegation queue, admin roster, permissions,
//! service control, and revocation over `netidx_admin::admin_ops`.
//!
//! Scaffolded here; the panels land once the [`TuiAnswerer`](super::answer::TuiAnswerer)
//! and connection flow exist.

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::Rect,
    text::Line,
    widgets::{Block, Borders, Paragraph, Wrap},
};

/// Tab-2 state.
pub(super) struct RemoteState {}

impl RemoteState {
    pub(super) fn new() -> RemoteState {
        RemoteState {}
    }

    pub(super) fn on_key(&mut self, _code: KeyCode) {}

    pub(super) fn render(&mut self, f: &mut Frame, area: Rect) {
        let body = if cfg!(unix) {
            Paragraph::new(vec![
                Line::from("Connect to an admin server to manage a network:"),
                Line::from(""),
                Line::from("  • approve or deny pending enrollments and delegations"),
                Line::from("  • view and edit the admin roster and permissions"),
                Line::from("  • start / stop / restart cluster services"),
                Line::from("  • list and revoke issued certificates"),
                Line::from(""),
                Line::from("(coming up)"),
            ])
        } else {
            Paragraph::new(
                "Remote administration is only available on unix hosts (it drives the \
                 openssl-backed CA admin path).",
            )
        };
        f.render_widget(
            body.wrap(Wrap { trim: true })
                .block(Block::default().borders(Borders::ALL).title(" Remote admin ")),
            area,
        );
    }
}
