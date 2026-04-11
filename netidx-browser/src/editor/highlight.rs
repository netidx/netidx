//! Graphix syntax highlighter for iced's text_editor widget.
//!
//! Implements `iced_core::text::highlighter::Highlighter` with a simple
//! line-local lexer covering all graphix token types.

use iced_core::text::highlighter::{self, Highlighter};
use iced_core::Color;
use std::ops::Range;

/// Token categories produced by the highlighter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TokenKind {
    Keyword,
    TypeName,
    Literal,
    Comment,
    String,
    Variant,
    Label,
    TypeVar,
    Operator,
    Normal,
}

/// Settings for the graphix highlighter.
/// The `version` field is bumped when content is replaced wholesale,
/// forcing the highlighter to be recreated (iced only recreates when
/// settings change).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GxSettings {
    pub version: u64,
}

/// Graphix syntax highlighter. Stateless per-line scanning.
pub(crate) struct GxHighlighter {
    current_line: usize,
}

impl Highlighter for GxHighlighter {
    type Settings = GxSettings;
    type Highlight = TokenKind;
    type Iterator<'a> = std::vec::IntoIter<(Range<usize>, TokenKind)>;

    fn new(_settings: &Self::Settings) -> Self {
        Self { current_line: 0 }
    }

    fn update(&mut self, _new_settings: &Self::Settings) {
        // Reset to force re-highlighting all lines when settings change
        // (e.g. after content replacement bumps the version)
        self.current_line = 0;
    }

    fn change_line(&mut self, line: usize) {
        // Track the earliest changed line — iced re-highlights from
        // current_line() forward, so we must keep the minimum.
        self.current_line = self.current_line.min(line);
    }

    fn highlight_line(&mut self, line: &str) -> Self::Iterator<'_> {
        self.current_line += 1;
        highlight_line(line).into_iter()
    }

    fn current_line(&self) -> usize {
        self.current_line
    }
}

/// Map a token kind to a color for the dark theme.
pub(crate) fn to_format(
    kind: &TokenKind,
    _theme: &graphix_package_gui::theme::GraphixTheme,
) -> highlighter::Format<iced_core::Font> {
    let color = match kind {
        TokenKind::Keyword => Color::from_rgb(0.4, 0.6, 1.0),     // light blue
        TokenKind::TypeName => Color::from_rgb(0.3, 0.8, 0.8),    // cyan
        TokenKind::Literal => Color::from_rgb(0.9, 0.7, 0.3),     // orange
        TokenKind::Comment => Color::from_rgb(0.5, 0.5, 0.5),     // gray
        TokenKind::String => Color::from_rgb(0.8, 0.6, 0.4),      // warm brown
        TokenKind::Variant => Color::from_rgb(0.5, 0.9, 0.5),     // green
        TokenKind::Label => Color::from_rgb(0.8, 0.5, 0.9),       // purple
        TokenKind::TypeVar => Color::from_rgb(0.6, 0.8, 0.9),     // light cyan
        TokenKind::Operator => Color::from_rgb(0.9, 0.9, 0.5),    // yellow
        TokenKind::Normal => return highlighter::Format::default(),
    };
    highlighter::Format { color: Some(color), font: None }
}

// ---- Lexer ----

static KEYWORDS: &[&str] = &[
    "let", "rec", "mod", "use", "type", "fn", "cast", "select",
    "if", "try", "catch", "throws", "as", "with", "where",
];

static TYPE_NAMES: &[&str] = &[
    "bool", "string", "bytes",
    "i8", "u8", "i16", "u16", "i32", "u32", "v32", "z32",
    "i64", "u64", "v64", "z64", "f32", "f64",
    "decimal", "datetime", "duration",
    "Any", "any", "Array", "Map",
];

static LITERALS: &[&str] = &["true", "false", "null", "ok"];

fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn highlight_line(line: &str) -> Vec<(Range<usize>, TokenKind)> {
    let mut spans = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let c = chars[i];

        // Doc comment ///
        if c == '/' && i + 2 < len && chars[i + 1] == '/' && chars[i + 2] == '/' {
            spans.push((i..len, TokenKind::Comment));
            break;
        }

        // Line comment //
        if c == '/' && i + 1 < len && chars[i + 1] == '/' {
            spans.push((i..len, TokenKind::Comment));
            break;
        }

        // Raw string r'...'
        if c == 'r' && i + 1 < len && chars[i + 1] == '\'' {
            let start = i;
            i += 2;
            while i < len && chars[i] != '\'' {
                if chars[i] == '\\' && i + 1 < len {
                    i += 1;
                }
                i += 1;
            }
            if i < len {
                i += 1; // closing quote
            }
            spans.push((start..i, TokenKind::String));
            continue;
        }

        // Double-quoted string
        if c == '"' {
            let start = i;
            i += 1;
            while i < len && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < len {
                    i += 1;
                }
                i += 1;
            }
            if i < len {
                i += 1; // closing quote
            }
            spans.push((start..i, TokenKind::String));
            continue;
        }

        // Variant `Tag
        if c == '`' && i + 1 < len && chars[i + 1].is_ascii_uppercase() {
            let start = i;
            i += 1;
            while i < len && is_ident_char(chars[i]) {
                i += 1;
            }
            spans.push((start..i, TokenKind::Variant));
            continue;
        }

        // Type variable 'a
        if c == '\'' && i + 1 < len && chars[i + 1].is_ascii_lowercase() {
            let start = i;
            i += 1;
            while i < len && is_ident_char(chars[i]) {
                i += 1;
            }
            spans.push((start..i, TokenKind::TypeVar));
            continue;
        }

        // Labeled parameter #name
        if c == '#' && i + 1 < len && (chars[i + 1].is_alphabetic() || chars[i + 1] == '_') {
            let start = i;
            i += 1;
            while i < len && is_ident_char(chars[i]) {
                i += 1;
            }
            spans.push((start..i, TokenKind::Label));
            continue;
        }

        // Identifier or keyword
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < len && is_ident_char(chars[i]) {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let kind = if KEYWORDS.contains(&word.as_str()) {
                TokenKind::Keyword
            } else if TYPE_NAMES.contains(&word.as_str()) {
                TokenKind::TypeName
            } else if LITERALS.contains(&word.as_str()) {
                TokenKind::Literal
            } else {
                TokenKind::Normal
            };
            if kind != TokenKind::Normal {
                spans.push((start..i, kind));
            }
            continue;
        }

        // Numbers
        if c.is_ascii_digit() {
            let start = i;
            // Hex/binary/octal
            if c == '0' && i + 1 < len {
                match chars[i + 1] {
                    'x' | 'X' => {
                        i += 2;
                        while i < len && (chars[i].is_ascii_hexdigit() || chars[i] == '_') {
                            i += 1;
                        }
                        spans.push((start..i, TokenKind::Literal));
                        continue;
                    }
                    'b' | 'B' => {
                        i += 2;
                        while i < len && (chars[i] == '0' || chars[i] == '1' || chars[i] == '_') {
                            i += 1;
                        }
                        spans.push((start..i, TokenKind::Literal));
                        continue;
                    }
                    'o' | 'O' => {
                        i += 2;
                        while i < len && (('0'..='7').contains(&chars[i]) || chars[i] == '_') {
                            i += 1;
                        }
                        spans.push((start..i, TokenKind::Literal));
                        continue;
                    }
                    _ => {}
                }
            }
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '_') {
                i += 1;
            }
            // Float
            if i < len && chars[i] == '.' && i + 1 < len && chars[i + 1].is_ascii_digit() {
                i += 1;
                while i < len && (chars[i].is_ascii_digit() || chars[i] == '_') {
                    i += 1;
                }
                // Exponent
                if i < len && (chars[i] == 'e' || chars[i] == 'E') {
                    i += 1;
                    if i < len && (chars[i] == '+' || chars[i] == '-') {
                        i += 1;
                    }
                    while i < len && (chars[i].is_ascii_digit() || chars[i] == '_') {
                        i += 1;
                    }
                }
            }
            spans.push((start..i, TokenKind::Literal));
            continue;
        }

        // Multi-char operators
        if i + 1 < len {
            let two: String = chars[i..i + 2].iter().collect();
            match two.as_str() {
                "<-" | "=>" | "->" | "::" | "==" | "!=" | "<=" | ">="
                | "&&" | "||" | "+?" | "-?" | "*?" | "/?" | "%?" => {
                    spans.push((i..i + 2, TokenKind::Operator));
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char operators
        if matches!(c, '+' | '-' | '*' | '/' | '%' | '~' | '?' | '&' | '@' | '!' | '<' | '>' | '=' | '|' | '^') {
            spans.push((i..i + 1, TokenKind::Operator));
            i += 1;
            continue;
        }

        // Everything else (whitespace, punctuation) — skip
        i += 1;
    }

    spans
}
