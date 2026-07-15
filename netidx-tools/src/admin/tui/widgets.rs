//! Small shared rendering helpers for the admin TUI.

use super::theme;
use netidx_admin::fingerprint::Fingerprint;
use ratatui::{
    Frame,
    layout::{Constraint, Flex, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::Block,
};

pub(super) const IDENTICON_WIDTH: u16 = 20;
pub(super) const IDENTICON_HEIGHT: u16 = 10;
const IDENTICON_BORDER_ROW: &str = "                    ";

/// Render a fingerprint's 8×8 identicon as ratatui lines on a black tile, in
/// the sigil's own color — the same cells the CLI's [`Fingerprint::identicon`]
/// draws, but as styled spans rather than embedded ANSI.
pub(super) fn identicon_lines(fp: &Fingerprint) -> Vec<Line<'static>> {
    let (r, g, b) = fp.identicon_color();
    let off = Style::default().bg(theme::GLYPH_BG);
    let on = off.fg(Color::Rgb(r, g, b));
    std::iter::once(Line::from(Span::styled(IDENTICON_BORDER_ROW, off)))
        .chain(fp.identicon_cells().into_iter().map(|row| {
            let spans = std::iter::once(Span::styled("  ", off))
                .chain(row.into_iter().map(|cell| {
                    Span::styled(
                        if cell { "██" } else { "  " },
                        if cell { on } else { off },
                    )
                }))
                .chain(std::iter::once(Span::styled("  ", off)))
                .collect::<Vec<_>>();
            Line::from(spans)
        }))
        .chain(std::iter::once(Line::from(Span::styled(IDENTICON_BORDER_ROW, off))))
        .collect()
}

#[cfg(test)]
pub(super) fn rendered_identicon_rows(buffer: &ratatui::buffer::Buffer) -> usize {
    (0..buffer.area().height)
        .filter(|&y| {
            let mut run = 0;
            for x in 0..buffer.area().width {
                if buffer[(x, y)].bg == theme::GLYPH_BG {
                    run += 1;
                    if run >= IDENTICON_WIDTH {
                        return true;
                    }
                } else {
                    run = 0;
                }
            }
            false
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identicon_has_a_black_background() {
        let fp = Fingerprint::of_der(b"glyph background test");
        let lines = identicon_lines(&fp);
        assert_eq!(lines.len(), IDENTICON_HEIGHT as usize);
        assert!(lines.iter().all(|line| line.width() == IDENTICON_WIDTH as usize));
        assert!(
            lines
                .iter()
                .flat_map(|line| &line.spans)
                .all(|span| span.style.bg == Some(theme::GLYPH_BG))
        );
        for line in [lines.first().unwrap(), lines.last().unwrap()] {
            assert!(line.spans.iter().all(|span| span.content.trim().is_empty()));
        }
        for line in &lines[1..lines.len() - 1] {
            assert!(line.spans.first().unwrap().content.trim().is_empty());
            assert!(line.spans.last().unwrap().content.trim().is_empty());
        }
    }
}

/// A fingerprint's grouped-base32 text laid out four 5-char groups per line — a
/// compact hash block to pair with an identicon.
pub(super) fn group_fingerprint(fp: &Fingerprint) -> Vec<String> {
    let text = fp.text();
    let groups: Vec<&str> = text.split(' ').collect();
    groups.chunks(4).map(|c| c.join(" ")).collect()
}

/// A compact "how long ago" for a queue/delegation row's age in seconds.
pub(super) fn fmt_age(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// A certificate's `not_after` unix timestamp as a `YYYY-MM-DD` expiry date
/// (or the raw seconds if it somehow falls outside chrono's range).
pub(super) fn fmt_expiry(not_after_unix: u64) -> String {
    match chrono::DateTime::from_timestamp(not_after_unix as i64, 0) {
        Some(dt) => dt.format("%Y-%m-%d").to_string(),
        None => format!("@{not_after_unix}"),
    }
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

/// Draw a dialog's drop shadow — a dark rectangle one cell down-and-right of
/// `area`, clamped to `screen`. Call before rendering the panel over `area`; the
/// panel overdraws all but the bottom-right L that reads as a shadow, floating
/// the dialog above the backdrop.
pub(super) fn shadow(f: &mut Frame, area: Rect, screen: Rect) {
    let (sx, sy) = (area.x + 1, area.y + 1);
    let sw = area.width.min(screen.right().saturating_sub(sx));
    let sh = area.height.min(screen.bottom().saturating_sub(sy));
    if sw == 0 || sh == 0 {
        return;
    }
    let shadow = Rect { x: sx, y: sy, width: sw, height: sh };
    f.render_widget(Block::default().style(Style::default().bg(theme::SHADOW)), shadow);
}

/// The plain text of a `Line`, concatenating its spans — for measuring how the
/// line will wrap.
pub(super) fn line_text(line: &Line) -> String {
    line.spans.iter().map(|s| s.content.as_ref()).collect()
}

/// The number of rows a greedy whitespace word-wrap (matching `Paragraph`'s
/// wrapping) produces for `text` at `width` columns — used to size a dialog to
/// its wrapped prose. Assumes no single word exceeds `width` (true for UI copy).
pub(super) fn wrapped_rows(text: &str, width: u16) -> u16 {
    let width = width.max(1) as usize;
    let mut rows = 1u16;
    let mut col = 0usize;
    for word in text.split_whitespace() {
        let wlen = word.chars().count();
        let need = if col == 0 { wlen } else { col + 1 + wlen };
        if need <= width {
            col = need;
        } else {
            rows = rows.saturating_add(1);
            col = wlen.min(width);
        }
    }
    rows
}

/// The total rows a `Paragraph` of `lines` (each wrapped independently) occupies
/// at `width` columns.
pub(super) fn wrapped_height(lines: &[Line], width: u16) -> u16 {
    lines.iter().map(|l| wrapped_rows(&line_text(l), width)).sum()
}

/// An indeterminate "marquee" progress bar `width` cells wide: a filled segment
/// that bounces left-and-right, positioned by the animation `tick`.
pub(super) fn marquee(tick: u64, width: u16) -> Line<'static> {
    let width = width.max(1) as usize;
    let seg = (width / 4).max(1);
    let travel = width.saturating_sub(seg);
    let pos = if travel == 0 {
        0
    } else {
        // triangle wave: 0 → travel → 0 over 2*travel ticks
        let p = (tick as usize) % (travel * 2);
        if p <= travel { p } else { travel * 2 - p }
    };
    let trough = Style::default().bg(theme::TROUGH);
    let fill = Style::default().bg(theme::ACCENT);
    Line::from(vec![
        Span::styled(" ".repeat(pos), trough),
        Span::styled(" ".repeat(seg), fill),
        Span::styled(" ".repeat(width - pos - seg), trough),
    ])
}
