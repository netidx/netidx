//! Human-comparable certificate fingerprint, rendered as grouped base32 text
//! and as a colored 8×8 identicon.
//!
//! The point is syncthing's trick — distinct identities look obviously
//! different at a glance, so the operator joining a deployment can
//! verify out of band that they're talking to the real trust domain
//! *before* sending the admin password. The CA admin sees this same
//! artifact at `ca init` and communicates it (in person, Signal, …);
//! the join CLI shows the artifact of whatever the daemon actually
//! presented, and the two are eyeball-compared.
//!
//! **What gets hashed**: for a trust domain/CA identity, the CA
//! certificate's *public key* (its SubjectPublicKeyInfo DER, via
//! [`Fingerprint::of_cert_der`]) — NOT the certificate itself. The key
//! is the thing that's unique to the trust domain: a same-key certificate
//! renewal leaves the glyph on the office wiki valid, while a key
//! rotation (a new controller) changes it, as it must. Request codes
//! in queued enrollment hash the CSR's SPKI for the same reason, so
//! both glyphs in the system are fingerprints of keys.
//!
//! Pure Rust (sha2 + x509-parser, no openssl), so the join client
//! computes the identical fingerprint on every platform — Windows
//! included, where the openssl-backed `ca` module isn't available.

use anyhow::{Result, anyhow, bail};
use sha2::{Digest, Sha256};

/// A SHA-256 digest rendered for human comparison.
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
    /// Hash raw bytes. Use the semantic constructors where one exists —
    /// [`of_cert_der`](Self::of_cert_der) for an identity glyph; this
    /// is the building block (and hashes an already-extracted SPKI,
    /// e.g. a CSR's, directly).
    pub fn of_der(der: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(der);
        let out = h.finalize();
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&out);
        Self(bytes)
    }

    /// The identity fingerprint of an X.509 certificate: a hash of its
    /// *public key* (SubjectPublicKeyInfo DER), not of the certificate.
    /// Stable across same-key certificate renewals; changes iff the key
    /// — the actual controller of the identity — changes.
    pub fn of_cert_der(der: &[u8]) -> Result<Self> {
        use x509_parser::prelude::{FromDer, X509Certificate};
        let (_, cert) = X509Certificate::from_der(der)
            .map_err(|e| anyhow!("parsing certificate: {e}"))?;
        Ok(Self::of_der(cert.public_key().raw))
    }

    /// [`of_cert_der`](Self::of_cert_der) for the first certificate in
    /// a PEM bundle.
    pub fn of_cert_pem(pem: &[u8]) -> Result<Self> {
        let mut rd = std::io::Cursor::new(pem);
        let der = rustls_pemfile::certs(&mut rd)
            .next()
            .ok_or_else(|| anyhow!("no certificate found in PEM"))?
            .map_err(|e| anyhow!("parsing PEM certificate: {e}"))?;
        Self::of_cert_der(&der)
    }

    // NB: there is deliberately no `of_pem`/of-the-cert-bytes identity
    // constructor — hashing the certificate instead of its key is
    // exactly the mistake that would break every printed glyph at the
    // first CA renewal.

    /// Parse the grouped-base32 form [`text`](Self::text) produces
    /// (spaces and case ignored) back into a fingerprint — used to
    /// re-render glyphs stored as text, e.g. in the issuance index.
    pub fn parse_text(s: &str) -> Result<Self> {
        let mut bytes = [0u8; 32];
        let mut acc: u32 = 0;
        let mut bits: u32 = 0;
        let mut i = 0;
        for c in s.chars() {
            if c == ' ' {
                continue;
            }
            let v = B32
                .iter()
                .position(|&b| b as char == c.to_ascii_uppercase())
                .ok_or_else(|| anyhow!("invalid base32 character {c:?}"))?;
            acc = (acc << 5) | v as u32;
            bits += 5;
            if bits >= 8 {
                bits -= 8;
                if i >= 32 {
                    bail!("fingerprint text too long");
                }
                bytes[i] = ((acc >> bits) & 0xff) as u8;
                i += 1;
                acc &= (1 << bits) - 1;
            }
        }
        if i != 32 {
            bail!("fingerprint text too short ({i} of 32 bytes)");
        }
        Ok(Self(bytes))
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
        let (r, g, b) = self.identicon_color();
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
        for row in self.identicon_cells() {
            out.push('│');
            for on_bit in row {
                out.push_str(if on_bit { on.as_str() } else { off });
            }
            out.push_str("│\n");
        }
        out.push_str("└────────────────┘");
        out
    }

    /// The identicon's 8×8 on/off grid (the same cells [`Self::identicon`]
    /// renders): horizontally mirrored, taken from the first 32 hash bits.
    /// Exposed so a GUI/TUI can draw the same sigil in its own styling
    /// instead of parsing the pre-rendered ANSI string.
    pub fn identicon_cells(&self) -> [[bool; 8]; 8] {
        let h = &self.0;
        let mut cells = [[false; 8]; 8];
        for (row, cells_row) in cells.iter_mut().enumerate() {
            for (col, cell) in cells_row.iter_mut().enumerate() {
                // Mirror the right half onto the left.
                let src = if col < 4 { col } else { 7 - col };
                let bit = row * 4 + src; // 0..32
                *cell = (h[bit / 8] >> (7 - (bit % 8))) & 1 == 1;
            }
        }
        cells
    }

    /// The identicon's dominant color (biased into the bright half so it
    /// shows on dark terminals), from the last 3 hash bytes — the same hue
    /// [`Self::identicon`] uses.
    pub fn identicon_color(&self) -> (u8, u8, u8) {
        let h = &self.0;
        (96 + h[29] / 2, 96 + h[30] / 2, 96 + h[31] / 2)
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
    fn text_round_trips_through_parse() {
        let f = Fingerprint::of_der(b"round trip me");
        assert_eq!(Fingerprint::parse_text(&f.text()).unwrap(), f);
        // Case and spacing are display artifacts, not content.
        assert_eq!(
            Fingerprint::parse_text(&f.text().to_lowercase().replace(' ', "")).unwrap(),
            f
        );
        assert!(Fingerprint::parse_text("TOO SHORT").is_err());
        assert!(Fingerprint::parse_text(&format!("{}AAAAAAAA", f.text())).is_err());
        assert!(Fingerprint::parse_text("0!@#").is_err());
    }

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
                    cells[col * 2],
                    cells[mirror * 2],
                    "column {col} not mirrored"
                );
            }
        }
    }

    #[test]
    fn cert_fingerprint_is_of_the_key_not_the_cert() {
        use rcgen::{CertificateParams, KeyPair, SerialNumber};
        // Two different certificates over the SAME key — a same-key
        // renewal. The certs differ on the wire (serials), but the
        // identity glyph must not change: it's printed on the office
        // wiki and must outlive any one cert's validity window.
        let key = KeyPair::generate().unwrap();
        let mut p1 = CertificateParams::new(vec!["a.example.com".to_string()]).unwrap();
        p1.serial_number = Some(SerialNumber::from(vec![1u8]));
        let c1 = p1.self_signed(&key).unwrap();
        let mut p2 = CertificateParams::new(vec!["a.example.com".to_string()]).unwrap();
        p2.serial_number = Some(SerialNumber::from(vec![2u8]));
        let c2 = p2.self_signed(&key).unwrap();
        assert_ne!(c1.der().as_ref(), c2.der().as_ref(), "renewal produced a new cert");
        let f1 = Fingerprint::of_cert_der(c1.der().as_ref()).unwrap();
        let f2 = Fingerprint::of_cert_der(c2.der().as_ref()).unwrap();
        assert_eq!(f1, f2, "same key ⇒ same glyph across renewal");
        // PEM and DER forms agree.
        assert_eq!(Fingerprint::of_cert_pem(c1.pem().as_bytes()).unwrap(), f1);
        // A different key is a different identity.
        let other = KeyPair::generate().unwrap();
        let c3 = CertificateParams::new(vec!["a.example.com".to_string()])
            .unwrap()
            .self_signed(&other)
            .unwrap();
        assert_ne!(
            Fingerprint::of_cert_der(c3.der().as_ref()).unwrap(),
            f1,
            "new key ⇒ new glyph"
        );
    }
}
