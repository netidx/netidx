//! Small shared rendering helpers for the admin TUI.

use netidx_admin::fingerprint::Fingerprint;
use ratatui::{
    layout::{Constraint, Flex, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
};

/// Render a fingerprint's 8×8 identicon as ratatui lines, in the sigil's own
/// color — the same cells the CLI's [`Fingerprint::identicon`] draws, but as
/// styled spans rather than embedded ANSI. Each "on" cell is a colored `██`.
pub(super) fn identicon_lines(fp: &Fingerprint) -> Vec<Line<'static>> {
    let (r, g, b) = fp.identicon_color();
    let on = Style::default().fg(Color::Rgb(r, g, b));
    fp.identicon_cells()
        .into_iter()
        .map(|row| {
            let spans = row
                .into_iter()
                .map(|cell| if cell { Span::styled("██", on) } else { Span::raw("  ") })
                .collect::<Vec<_>>();
            Line::from(spans)
        })
        .collect()
}

/// A rectangle centered in `area`, `width`×`height` cells, clamped to `area`.
pub(super) fn centered(width: u16, height: u16, area: Rect) -> Rect {
    let [row] = Layout::vertical([Constraint::Length(height.min(area.height))])
        .flex(Flex::Center)
        .areas(area);
    let [cell] = Layout::horizontal([Constraint::Length(width.min(area.width))])
        .flex(Flex::Center)
        .areas(row);
    cell
}
