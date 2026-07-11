//! The Debian-installer look: a blue backdrop with light-gray dialog panels,
//! black text, and a red accent for focus and selection.
//!
//! Every color the TUI uses lives here so the palette is tunable in one place.
//! Values are explicit `Rgb` (not the 16 terminal-palette slots) so the look is
//! the same regardless of the user's terminal theme — which matters because the
//! whole point is to fill the screen with a specific blue and draw gray panels
//! on top of it.

use ratatui::{
    style::{Color, Modifier, Style},
    text::Span,
    widgets::{Block, Borders},
};

// -- palette ------------------------------------------------------------------

/// The screen backdrop (classic installer blue).
pub(super) const BACKDROP: Color = Color::Rgb(0, 40, 120);
/// Text/chrome drawn directly on the backdrop.
pub(super) const BACKDROP_FG: Color = Color::Rgb(205, 212, 228);
/// Dialog/panel fill.
pub(super) const PANEL_BG: Color = Color::Rgb(199, 199, 199);
/// Text on a panel.
pub(super) const PANEL_FG: Color = Color::Rgb(0, 0, 0);
/// Muted text on a panel (help/hints) — an explicit gray, since `Modifier::DIM`
/// reads unpredictably over an explicit background.
pub(super) const HINT_FG: Color = Color::Rgb(88, 88, 88);
/// The focus/selection accent (Debian red).
pub(super) const ACCENT: Color = Color::Rgb(170, 0, 0);
/// Text on the accent.
pub(super) const ACCENT_FG: Color = Color::Rgb(255, 255, 255);
/// A text input field (a white inset on the gray panel).
pub(super) const FIELD_BG: Color = Color::Rgb(255, 255, 255);
/// Text in a field.
pub(super) const FIELD_FG: Color = Color::Rgb(0, 0, 0);
/// The trough behind a progress bar.
pub(super) const TROUGH: Color = Color::Rgb(150, 150, 150);
/// A positive/healthy status color, legible on a panel or the backdrop.
pub(super) const OK: Color = Color::Rgb(0, 120, 0);
/// A caution status color, legible on a panel or the backdrop.
pub(super) const WARN: Color = Color::Rgb(176, 96, 0);
/// A dialog's drop shadow, drawn one cell down-and-right on the backdrop.
pub(super) const SHADOW: Color = Color::Rgb(0, 22, 66);

// -- styles -------------------------------------------------------------------

pub(super) fn backdrop_style() -> Style {
    Style::default().bg(BACKDROP).fg(BACKDROP_FG)
}

pub(super) fn panel_style() -> Style {
    Style::default().bg(PANEL_BG).fg(PANEL_FG)
}

pub(super) fn hint_style() -> Style {
    Style::default().bg(PANEL_BG).fg(HINT_FG)
}

pub(super) fn title_style() -> Style {
    Style::default().bg(PANEL_BG).fg(PANEL_FG).add_modifier(Modifier::BOLD)
}

/// The style of a selected list row / focused control.
pub(super) fn selected_style() -> Style {
    Style::default().bg(ACCENT).fg(ACCENT_FG).add_modifier(Modifier::BOLD)
}

/// The style of a text-input field.
pub(super) fn field_style() -> Style {
    Style::default().bg(FIELD_BG).fg(FIELD_FG)
}

// -- building blocks ----------------------------------------------------------

/// A bordered dialog panel with a bold title — the standard modal frame.
pub(super) fn dialog_block(title: &str) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(panel_style())
        .style(panel_style())
        .title(Span::styled(format!(" {title} "), title_style()))
}

/// A bordered panel with no title accent — for status cards and tab bodies.
pub(super) fn panel_block() -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(panel_style())
        .style(panel_style())
}

/// A Debian-style `<Label>` button span; the focused one is drawn in the accent.
pub(super) fn button(label: &str, focused: bool) -> Span<'static> {
    let text = format!(" <{label}> ");
    if focused {
        Span::styled(text, selected_style())
    } else {
        Span::styled(text, Style::default().bg(PANEL_BG).fg(PANEL_FG))
    }
}
