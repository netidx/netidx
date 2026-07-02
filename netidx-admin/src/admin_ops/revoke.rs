//! Certificate revocation as query + action.
//!
//! Revocation is irreversible, so it keeps two independent gates the old
//! interactive ceremony enforced (its destructive y/n confirm is replaced by
//! the explicit action plus its selector): a **serial** is an exact
//! per-certificate id; a **name** revokes every live certificate carrying it,
//! but only when they all share one public key. `spki_fp` is per-*key* (the
//! enrollment CSR fingerprint reused at issuance), so a re-keyed renewal has a
//! different glyph — a bare-name selector spanning more than one distinct glyph
//! is refused (narrow by `--serial`), and `--assert-glyph`, when given, must
//! equal the glyph of *every* target.
//!
//! The query [`issued`] lists the index (serial, name, glyph, expiry, revoked)
//! so an operator reads the serial or glyph to feed the action — the same
//! query→action shape as the queue and delegation groups.

use super::open_admin_session;
use crate::{
    admin_client,
    admin_proto::{IssuedEntry, Secret},
    answer::Answerer,
    fingerprint::Fingerprint,
};
use anyhow::{Context, Result, bail};
use std::{net::SocketAddr, path::PathBuf};

/// Which certificate(s) to revoke.
pub enum RevokeSelector {
    /// Exactly one certificate, by serial.
    Serial(u64),
    /// Every live certificate carrying this name (case-insensitive) — provided
    /// they all share one public key (see the glyph gate).
    Name(String),
}

/// Parse the `<id-or-name>` positional: a bare `u64` is a serial, anything else
/// is a name.
pub fn parse_selector(s: &str) -> RevokeSelector {
    match s.parse::<u64>() {
        Ok(serial) => RevokeSelector::Serial(serial),
        Err(_) => RevokeSelector::Name(s.to_string()),
    }
}

/// The `ca issued` query: the CA's issued-certificate index. `include_revoked`
/// keeps already-revoked rows too (default live-only); `name_filter`, when set,
/// keeps only rows whose name contains it (case-insensitive).
pub async fn issued(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    include_revoked: bool,
    name_filter: Option<&str>,
) -> Result<Vec<IssuedEntry>> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let entries = admin_client::list_issued(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        &sess.identity,
    )
    .await?;
    let filt = name_filter.map(str::to_lowercase);
    Ok(entries
        .into_iter()
        .filter(|e| include_revoked || !e.revoked)
        .filter(|e| match &filt {
            Some(f) => e.name.to_lowercase().contains(f.as_str()),
            None => true,
        })
        .collect())
}

/// The outcome of a revocation.
pub struct RevokeOutcome {
    /// The certificates that were revoked.
    pub revoked: Vec<IssuedEntry>,
    /// Non-fatal follow-up warnings from the server (CRL push, etc.).
    pub warnings: Vec<String>,
}

/// The `ca revoke <id-or-name> --reason <text>` action. Re-lists the issued
/// index, resolves the selector to its live target(s), enforces the glyph gate,
/// then revokes (the daemon re-signs the CRL). **Irreversible.**
pub async fn revoke(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    selector: RevokeSelector,
    assert_glyph: Option<Fingerprint>,
    reason: &str,
) -> Result<RevokeOutcome> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let entries = admin_client::list_issued(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        &sess.identity,
    )
    .await?;
    let live = entries.iter().filter(|e| !e.revoked);
    let targets: Vec<&IssuedEntry> = match &selector {
        RevokeSelector::Serial(serial) => {
            let t: Vec<_> = live.filter(|e| e.serial == *serial).collect();
            if t.is_empty() {
                bail!("no live certificate with serial {serial}");
            }
            t
        }
        RevokeSelector::Name(name) => {
            let t: Vec<_> = live.filter(|e| e.name.eq_ignore_ascii_case(name)).collect();
            if t.is_empty() {
                bail!("no live certificate for {name:?}");
            }
            t
        }
    };
    // The glyph gate. `spki_fp` is per public key; revoking a whole name is
    // only unambiguous when every live cert for it shares one key.
    enforce_glyph_gate(&targets, assert_glyph.as_ref())?;
    let serials: Vec<u64> = targets.iter().map(|t| t.serial).collect();
    let revoked: Vec<IssuedEntry> = targets.iter().map(|t| (*t).clone()).collect();
    let warnings = admin_client::revoke(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        serials,
        reason,
        &sess.identity,
    )
    .await?;
    Ok(RevokeOutcome { revoked, warnings })
}

/// Enforce the revocation glyph gate. With `assert_glyph`, it must equal the
/// `spki_fp` of every target (refuse on any mismatch). Without it, refuse a
/// target set spanning more than one distinct glyph — a bare-name selector that
/// would revoke certs for two different keys must be narrowed (by serial, or by
/// `--assert-glyph` to confirm the intended key).
fn enforce_glyph_gate(
    targets: &[&IssuedEntry],
    assert_glyph: Option<&Fingerprint>,
) -> Result<()> {
    let glyphs: Vec<Fingerprint> = targets
        .iter()
        .map(|e| {
            Fingerprint::parse_text(&e.spki_fp).with_context(|| {
                format!(
                    "certificate serial {} has an unparseable glyph {:?}",
                    e.serial, e.spki_fp
                )
            })
        })
        .collect::<Result<_>>()?;
    match assert_glyph {
        Some(expected) => {
            for (t, g) in targets.iter().zip(glyphs.iter()) {
                if g != expected {
                    bail!(
                        "--assert-glyph does not match certificate serial {} (its glyph \
                         is {}) — refusing; every target must carry the asserted glyph",
                        t.serial,
                        g.text(),
                    );
                }
            }
        }
        None => {
            if let Some(first) = glyphs.first() {
                if glyphs.iter().any(|g| g != first) {
                    bail!(
                        "this name spans more than one public key (certificates for \
                         different keys) — refusing a whole-name revoke. Narrow it with \
                         --serial <n>, or pass --assert-glyph <fp> to confirm the \
                         intended key."
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn glyph(seed: &str) -> Fingerprint {
        Fingerprint::parse_text(&seed.repeat(52)).unwrap()
    }

    fn entry(serial: u64, name: &str, g: &Fingerprint) -> IssuedEntry {
        IssuedEntry {
            serial,
            name: name.to_string(),
            spki_fp: g.text(),
            not_after_unix: 0,
            revoked: false,
        }
    }

    fn gate(targets: &[IssuedEntry], assert: Option<&Fingerprint>) -> Result<()> {
        let refs: Vec<&IssuedEntry> = targets.iter().collect();
        enforce_glyph_gate(&refs, assert)
    }

    #[test]
    fn single_glyph_no_assert_ok() {
        let g = glyph("A");
        let t = [entry(1, "a.example", &g), entry(2, "a.example", &g)];
        gate(&t, None).unwrap();
    }

    #[test]
    fn multi_glyph_no_assert_refused() {
        let (a, b) = (glyph("A"), glyph("B"));
        let t = [entry(1, "a.example", &a), entry(2, "a.example", &b)];
        let e = gate(&t, None).unwrap_err();
        assert!(format!("{e:#}").contains("more than one public key"));
    }

    #[test]
    fn assert_matches_every_target_ok() {
        let g = glyph("A");
        let t = [entry(1, "a.example", &g), entry(2, "a.example", &g)];
        gate(&t, Some(&g)).unwrap();
    }

    #[test]
    fn assert_mismatch_refused() {
        let (a, b) = (glyph("A"), glyph("B"));
        // Two targets, one carries a different glyph than the asserted one.
        let t = [entry(1, "a.example", &a), entry(2, "a.example", &b)];
        let e = gate(&t, Some(&a)).unwrap_err();
        assert!(format!("{e:#}").contains("assert-glyph"));
    }

    #[test]
    fn single_serial_target_ok() {
        // A serial resolves to exactly one target — always passes the gate.
        let g = glyph("C");
        let t = [entry(7, "x.example", &g)];
        gate(&t, None).unwrap();
    }
}
