//! Human-comparable fingerprint of a CA certificate: a SHA-256 over the
//! cert's DER encoding, rendered both as grouped base32 text and as a
//! colored 8×8 identicon.
//!
//! The point is syncthing's trick — distinct CAs look obviously
//! different at a glance, so the operator joining a deployment can
//! verify out of band that they're talking to the real CA *before*
//! sending the admin password. The CA admin sees this same artifact at
//! `ca init` and communicates it (in person, Signal, …); the join CLI
//! shows the artifact of whatever cert the daemon actually presented,
//! and the two are eyeball-compared.
//!
//! Pure Rust (sha2, no openssl), so the join client computes the
//! identical fingerprint on every platform — Windows included, where
//! the openssl-backed `ca` module isn't available.

use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

/// SHA-256 of a certificate's DER encoding.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Fingerprint([u8; 32]);

/// How much color the renderer may use. The CLI picks this from the
/// environment (`NO_COLOR`, `COLORTERM`, whether stdout is a TTY); the
/// engine stays oblivious to terminal state and just renders what it's
/// told.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ColorMode {
    /// 24-bit `\x1b[38;2;r;g;b` escapes.
    Truecolor,
    /// 256-color cube `\x1b[38;5;n`.
    Ansi256,
    /// No escapes — on/off cells distinguished by block vs space only.
    Mono,
}

impl ColorMode {
    /// Best color mode for the current process's stdout: `Mono` when
    /// `NO_COLOR` is set or stdout isn't a TTY, `Truecolor` when
    /// `COLORTERM` advertises it, else `Ansi256`.
    pub fn detect() -> Self {
        use std::io::IsTerminal;
        if std::env::var_os("NO_COLOR").is_some() || !std::io::stdout().is_terminal() {
            return ColorMode::Mono;
        }
        match std::env::var("COLORTERM") {
            Ok(v) if v.contains("truecolor") || v.contains("24bit") => {
                ColorMode::Truecolor
            }
            _ => ColorMode::Ansi256,
        }
    }
}

const B32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

impl Fingerprint {
    /// Fingerprint a certificate from its DER bytes.
    pub fn of_der(der: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(der);
        let out = h.finalize();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&out);
        Self(bytes)
    }

    /// Fingerprint the first certificate in a PEM bundle. Parses to DER
    /// first so the result is independent of PEM whitespace/line-ending
    /// quirks — the DER is the canonical thing both ends hash.
    pub fn of_pem(pem: &[u8]) -> Result<Self> {
        let mut rd = std::io::Cursor::new(pem);
        let der = rustls_pemfile::certs(&mut rd)
            .next()
            .ok_or_else(|| anyhow!("no certificate found in PEM"))?
            .map_err(|e| anyhow!("parsing PEM certificate: {e}"))?;
        Ok(Self::of_der(&der))
    }

    /// The raw 32-byte digest.
    pub fn bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Grouped uppercase base32 (RFC 4648, no padding), in 5-char
    /// groups separated by spaces — the careful-compare form.
    pub fn text(&self) -> String {
        let raw = base32(&self.0);
        let mut out = String::with_capacity(raw.len() + raw.len() / 5);
        for (i, c) in raw.chars().enumerate() {
            if i > 0 && i % 5 == 0 {
                out.push(' ');
            }
            out.push(c);
        }
        out
    }

    /// The first 8 base32 characters — enough to name a CA casually in
    /// prose or logs. The full `text()` is the one to compare on.
    pub fn short(&self) -> String {
        base32(&self.0[..5])[..8].to_string()
    }

    /// A colored 8×8 identicon as a multi-line string. The grid is
    /// horizontally symmetric (left 4 columns mirrored to the right),
    /// which reads as a deliberate sigil rather than noise; cells are
    /// on/off from the first 32 hash bits, and a single dominant color
    /// (biased into the bright half so it shows on dark terminals) is
    /// derived from the last 3 hash bytes so each CA also has a
    /// recognizable hue.
    pub fn identicon(&self, color: ColorMode) -> String {
        let h = &self.0;
        let (r, g, b) = (96 + h[29] / 2, 96 + h[30] / 2, 96 + h[31] / 2);
        let on: String = match color {
            ColorMode::Truecolor => format!("\x1b[38;2;{r};{g};{b}m██\x1b[0m"),
            ColorMode::Ansi256 => {
                format!("\x1b[38;5;{}m██\x1b[0m", ansi256(r, g, b))
            }
            ColorMode::Mono => "██".to_string(),
        };
        let off = "  ";
        let mut out = String::new();
        out.push_str("┌────────────────┐\n");
        for row in 0..8usize {
            out.push('│');
            for col in 0..8usize {
                // Mirror the right half onto the left.
                let src = if col < 4 { col } else { 7 - col };
                let bit = row * 4 + src; // 0..32
                let on_bit = (h[bit / 8] >> (7 - (bit % 8))) & 1 == 1;
                out.push_str(if on_bit { on.as_str() } else { off });
            }
            out.push_str("│\n");
        }
        out.push_str("└────────────────┘");
        out
    }
}

/// RFC 4648 base32 (uppercase, no padding).
fn base32(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 8 / 5 + 1);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for &byte in bytes {
        acc = (acc << 8) | byte as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(B32[((acc >> bits) & 0x1f) as usize] as char);
        }
        // Drop the bits we just consumed so `acc` can't overflow.
        acc &= (1 << bits) - 1;
    }
    if bits > 0 {
        out.push(B32[((acc << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}

/// Map an RGB triple to the nearest index in the xterm-256 6×6×6 color
/// cube (indices 16..232).
fn ansi256(r: u8, g: u8, b: u8) -> u8 {
    let q = |v: u8| (v as u16 * 5 / 255) as u8;
    16 + 36 * q(r) + 6 * q(g) + q(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_is_rfc4648() {
        // Known RFC 4648 vectors (no padding).
        assert_eq!(base32(b""), "");
        assert_eq!(base32(b"f"), "MY");
        assert_eq!(base32(b"fo"), "MZXQ");
        assert_eq!(base32(b"foo"), "MZXW6");
        assert_eq!(base32(b"foobar"), "MZXW6YTBOI");
    }

    #[test]
    fn deterministic_and_distinct() {
        let a = Fingerprint::of_der(b"cert-a");
        let a2 = Fingerprint::of_der(b"cert-a");
        let b = Fingerprint::of_der(b"cert-b");
        assert_eq!(a, a2);
        assert_eq!(a.text(), a2.text());
        assert_eq!(a.identicon(ColorMode::Mono), a2.identicon(ColorMode::Mono));
        assert_ne!(a, b);
        assert_ne!(a.text(), b.text());
    }

    #[test]
    fn text_shape() {
        let f = Fingerprint::of_der(b"x");
        let t = f.text();
        // 256 bits → 52 base32 chars; grouped in 5s → 51/5 = 10 spaces.
        assert_eq!(t.chars().filter(|c| *c != ' ').count(), 52);
        assert!(t.chars().all(|c| c == ' ' || B32.contains(&(c as u8))));
        assert_eq!(f.short().len(), 8);
    }

    #[test]
    fn identicon_is_symmetric() {
        let f = Fingerprint::of_der(b"symmetry");
        for line in f.identicon(ColorMode::Mono).lines() {
            // Strip the border, then check the 8 cells (2 chars each)
            // mirror around the centre.
            let inner: Vec<char> = line.chars().collect();
            if inner.first() != Some(&'│') {
                continue;
            }
            let cells: String = inner[1..inner.len() - 1].iter().collect();
            let cells: Vec<char> = cells.chars().collect();
            assert_eq!(cells.len(), 16);
            for col in 0..8 {
                let mirror = 7 - col;
                assert_eq!(
                    cells[col * 2], cells[mirror * 2],
                    "column {col} not mirrored"
                );
            }
        }
    }

    #[test]
    fn of_pem_matches_of_der() {
        // A syntactically valid PEM cert block (framing only; the bytes
        // inside need not be a real X.509 — rustls-pemfile decodes the
        // base64 to DER and we hash that).
        let der = b"\x30\x03\x02\x01\x05"; // tiny DER-ish blob
        let b64 = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(der)
        };
        let pem =
            format!("-----BEGIN CERTIFICATE-----\n{b64}\n-----END CERTIFICATE-----\n");
        let from_pem = Fingerprint::of_pem(pem.as_bytes()).unwrap();
        assert_eq!(from_pem, Fingerprint::of_der(der));
    }
}
