//! Machine-generated passwords, in an alphabet a human can transcribe.
//!
//! Two credentials in this system are chosen by the machine rather than by a
//! person: the off-box CA recovery password, and the one-time key an admin
//! receives from `ca admin add-role` / `ca admin reset-password`. Both get
//! read off a screen and typed somewhere else — onto paper, into a safe, down
//! a phone line — so both are rendered in Crockford base32, which has no
//! character pair a reader can confuse.
//!
//! Cross-platform on purpose. The vault that stores these is unix-only, but a
//! Windows admin client generates a one-time key for a unix CA, so the
//! alphabet cannot live behind the vault's `#[cfg(unix)]`.

use rand::Rng;
use zeroize::Zeroizing;

/// Crockford base32 alphabet (digits + uppercase, excluding I L O U — the
/// characters easiest to confuse written down or read aloud). The recovery
/// password is rendered in this alphabet so it survives a trip through a
/// safe and a human's handwriting.
const CROCKFORD32: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Bytes of entropy in a recovery password — 160 bits (a multiple of 5, so
/// it renders to exactly 32 base32 chars with no padding).
const RECOVERY_ENTROPY_BYTES: usize = 20;

/// Generate a fresh machine-chosen password: 160 bits rendered as 32
/// Crockford base32 characters. This canonical string is the actual slot
/// password; [`group_crockford_password`] renders it in quads for the
/// operator to copy, and [`normalize_crockford_password`] folds a re-typed
/// copy back to it. Never persisted — shown exactly once.
///
/// Used for both passwords no human chooses: the off-box CA recovery
/// credential, and the one-time key an admin gets from `ca admin
/// reset-password` / `ca admin add-role`. The alphabet is the point — it
/// excludes I, L, O and U, so a password that has to survive a safe, a sticky
/// note, or being read down a phone line has no confusable characters to
/// begin with.
pub fn gen_crockford_password() -> Zeroizing<String> {
    let mut bytes = Zeroizing::new([0u8; RECOVERY_ENTROPY_BYTES]);
    rand::rng().fill_bytes(&mut bytes[..]);
    let mut out = Zeroizing::new(String::with_capacity(32));
    let (mut acc, mut bits) = (0u16, 0u32);
    for &b in bytes.iter() {
        acc = (acc << 8) | b as u16;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(CROCKFORD32[((acc >> bits) & 0x1f) as usize] as char);
        }
    }
    // 160 bits / 5 == 32 chars exactly; no leftover bits to pad.
    out
}

/// Render a generated password in 4-character quads separated by spaces
/// (e.g. `45QD 567D 8H2K …`) for the boxed one-time display. Grouping only
/// aids transcription; [`normalize_crockford_password`] strips it back out.
/// The result is `Zeroizing` (it holds the full secret) — the same care
/// [`gen_crockford_password`] takes, kept across this hop.
pub fn group_crockford_password(pw: &str) -> Zeroizing<String> {
    let mut out = Zeroizing::new(String::with_capacity(pw.len() + pw.len() / 4));
    for (i, c) in pw.chars().enumerate() {
        if i > 0 && i % 4 == 0 {
            out.push(' ');
        }
        out.push(c);
    }
    out
}

/// Fold an operator-typed generated password back to the canonical form
/// [`gen_crockford_password`] produced: drop whitespace and hyphens,
/// uppercase, and apply Crockford's digit substitutions (O→0, I/L→1) so a
/// transcription that confused those characters still unlocks. The result is
/// `Zeroizing` — it is the secret that goes to `unlock`.
///
/// Only ever applied to a password this module generated, on both sides: a
/// one-time slot is *derived* from the folded form (see [`fold_if_one_time`])
/// as well as verified against it, so "a `must_change` slot's stored password
/// is canonical" holds by construction rather than by every client
/// remembering to send a canonical one. A slot holding a human-chosen password
/// is authenticated against exactly what was typed — folding those would
/// silently collapse distinct passwords together.
pub fn normalize_crockford_password(typed: &str) -> Zeroizing<String> {
    Zeroizing::new(
        typed
            .chars()
            .filter_map(|c| match c.to_ascii_uppercase() {
                ' ' | '-' | '\t' | '\n' | '\r' => None,
                'O' => Some('0'),
                'I' | 'L' => Some('1'),
                c => Some(c),
            })
            .collect(),
    )
}

/// The form a password is stored and checked in. A one-time password is
/// always a generated Crockford key, so it is folded to canonical at both
/// ends; a chosen password is used exactly as typed.
///
/// Every derivation for a `must_change` slot goes through here, which is what
/// makes the two ends agree without any caller having to know the rule — a
/// client that sent a lower-cased or space-grouped key would otherwise install
/// a credential that could never authenticate.
pub fn fold_if_one_time(password: &str, must_change: bool) -> Zeroizing<String> {
    if must_change {
        normalize_crockford_password(password)
    } else {
        Zeroizing::new(password.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_password_roundtrips_through_display_and_reentry() {
        let pw = gen_crockford_password();
        // 160 bits of Crockford base32 == 32 chars from the alphabet.
        assert_eq!(pw.len(), 32);
        assert!(pw.chars().all(|c| CROCKFORD32.contains(&(c as u8))));
        // Grouped for display: 8 quads separated by 7 spaces.
        let shown = group_crockford_password(&pw);
        assert_eq!(shown.split(' ').count(), 8);
        assert!(shown.split(' ').all(|q| q.len() == 4));
        // Re-typing the grouped form (or with confusable chars) folds back.
        assert_eq!(*normalize_crockford_password(&shown), *pw);
        // Two fresh passwords differ (RNG is actually consulted).
        assert_ne!(*gen_crockford_password(), *pw);
        // Crockford leniency: O→0, I/L→1, lowercase, stray hyphens.
        assert_eq!(normalize_crockford_password("o0-iI lL-ab").as_str(), "001111AB");
    }
}
