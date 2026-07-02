//! Offline (pre-daemon) CA issuance glue — the non-interactive half of
//! `ca sign` / `ca issue`, shared with the install flow and the daemon's own
//! sign path.
//!
//! "Offline" issuance runs while *no* admin server owns the CA: it takes the
//! same exclusive flock the daemon would, allocates a serial from (and commits
//! back into) the store the daemon reads, and records the issuance so the cert
//! is revocable like any other. Everything here is pure of operator I/O — the
//! CA is already unlocked and the decisions already made; the Answerer-driven
//! orchestration lives in [`crate::admin_ops::offline`].

use crate::{
    admin_proto::{NodeKind, SERVING_SAN},
    ca::{Ca, IssueParams, IssuedFiles, SanEntry},
    ca_store::{CAStore, CaDir, QueuedReq},
};
use anyhow::{Context, Result, anyhow, bail};
use std::{net::IpAddr, path::PathBuf, time::Duration};

/// The first DNS SAN of a leaf — the identity name the index keys on.
pub fn first_dns_san(san: &[SanEntry]) -> Option<String> {
    san.iter().find_map(|s| match s {
        SanEntry::Dns(d) => Some(d.clone()),
        _ => None,
    })
}

/// Parse `--san` strings (`<kind>:<value>`), defaulting to `dns:<fallback_cn>`
/// when none are given.
pub fn parse_sans(raw: &[String], fallback_cn: &str) -> Result<Vec<SanEntry>> {
    if raw.is_empty() {
        return Ok(vec![SanEntry::Dns(fallback_cn.to_string())]);
    }
    raw.iter().map(|s| parse_san_one(s)).collect()
}

/// Parse a single `<kind>:<value>` SAN string.
pub fn parse_san_one(s: &str) -> Result<SanEntry> {
    let (kind, val) = s
        .split_once(':')
        .ok_or_else(|| anyhow!("SAN must be in the form <kind>:<value>: {s:?}"))?;
    if val.is_empty() {
        bail!("SAN value must not be empty: {s:?}");
    }
    Ok(match kind {
        "dns" => SanEntry::Dns(val.to_string()),
        "ip" => SanEntry::Ip(
            val.parse::<IpAddr>().map_err(|e| anyhow!("invalid SAN ip {val:?}: {e}"))?,
        ),
        "uri" => SanEntry::Uri(val.to_string()),
        "email" => SanEntry::Email(val.to_string()),
        other => bail!("unknown SAN kind {other:?}; expected dns / ip / uri / email"),
    })
}

/// Refuse to mint the admin server's reserved serving name from the local CLI,
/// mirroring the network sign path's refusal. The reserved name is the linchpin
/// of the trust model; only the admin-server setup flow (which signs it
/// directly) and the policy-gated network Enroll may issue it.
pub fn ensure_san_not_reserved(san: &[SanEntry]) -> Result<()> {
    for s in san {
        if let SanEntry::Dns(d) = s
            && d.eq_ignore_ascii_case(SERVING_SAN)
        {
            bail!(
                "{SERVING_SAN:?} is reserved for the admin server's serving certificate \
                 and can't be issued here"
            );
        }
    }
    Ok(())
}

/// Make a CN safe to embed in a filename. CNs are usually hostnames (already
/// safe), but the field is free-form text, so replace anything outside
/// `[A-Za-z0-9._-]` with `_`. The result is always a single path component — no
/// separators survive — so a defaulted output path can't traverse out of the
/// cwd. Empty input collapses to `_` so we never produce a bare extension.
pub fn sanitize_filename(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') { c } else { '_' }
        })
        .collect();
    if out.is_empty() { "_".to_string() } else { out }
}

/// Default `request` CSR path: `./<cn>.csr`.
pub fn default_csr_filename(cn: &str) -> PathBuf {
    PathBuf::from(format!("{}.csr", sanitize_filename(cn)))
}

/// Default `sign` cert path: `./<csr-cn>.pem`, or `./certificate.pem` when the
/// CSR carries no CN.
pub fn default_cert_filename(csr_cn: Option<&str>) -> PathBuf {
    let stem = match csr_cn {
        Some(cn) => sanitize_filename(cn),
        None => "certificate".to_string(),
    };
    PathBuf::from(format!("{stem}.pem"))
}

/// Record an offline (pre-daemon) issuance in the CA store, exactly as the
/// daemon records its own — seeding the serial from, and committing back into,
/// the same store the daemon reads, so serials stay unique and the bootstrap
/// cert is revocable. `csr_pem` is empty when the key was generated internally.
pub fn record_offline_issuance(
    store: &mut CAStore,
    serial: u64,
    kind: NodeKind,
    name: &str,
    csr_pem: &str,
    cert_pem: &str,
    validity: Duration,
) -> Result<()> {
    let req = QueuedReq::new(
        kind,
        csr_pem.to_string(),
        name.to_string(),
        validity,
        "(offline issue)".to_string(),
        None,
        None,
    );
    store.commit_issuance(&req, serial, name, cert_pem, &[])
}

/// Issue a leaf offline, allocating a fresh serial and recording the issuance.
/// Returns the written files.
pub fn issue_and_record(
    ca: &Ca,
    kind: NodeKind,
    mut params: IssueParams,
) -> Result<IssuedFiles> {
    let ca_dir = ca.directory().to_path_buf();
    // Take the same exclusive flock the daemon holds: offline issuance is only
    // legitimate before the daemon owns the CA, and the serial is allocated
    // from (and committed back to) the store the daemon seeds its in-memory
    // counter from. Without this lock a `ca issue` run against a live daemon
    // would mint the serial the daemon allocates next, a duplicate X.509 serial.
    let cadir = CaDir::open(&ca_dir)
        .context("cannot issue offline: a running admin server owns this CA")?;
    let serial = cadir.store.lock().next_serial()?;
    params.serial = serial;
    let name =
        first_dns_san(&params.san).unwrap_or_else(|| params.subject.common_name.clone());
    let validity = params.validity;
    let issued = ca.issue(&params)?;
    let cert_pem = std::fs::read_to_string(&issued.certificate).with_context(|| {
        format!("reading issued cert {}", issued.certificate.display())
    })?;
    // `ca.issue` already wrote the key + cert to disk. If recording the issuance
    // fails, roll those back: an un-recorded cert is invisible to `next_serial`,
    // so leaving it would let its serial be handed out again.
    if let Err(e) = record_offline_issuance(
        &mut cadir.store.lock(),
        serial,
        kind,
        &name,
        "",
        &cert_pem,
        validity,
    ) {
        let _ = std::fs::remove_file(&issued.certificate);
        let _ = std::fs::remove_file(&issued.private_key);
        return Err(e);
    }
    Ok(issued)
}

/// Sign an external CSR offline, allocating a fresh serial and recording the
/// issuance. Returns the leaf PEM.
pub fn sign_and_record(
    ca: &Ca,
    kind: NodeKind,
    csr_pem: &[u8],
    san: &[SanEntry],
    name: &str,
    validity: Duration,
) -> Result<Vec<u8>> {
    let ca_dir = ca.directory().to_path_buf();
    // See `issue_and_record`: hold the daemon's exclusive flock so offline
    // signing can't race the daemon's serial counter.
    let cadir = CaDir::open(&ca_dir)
        .context("cannot sign offline: a running admin server owns this CA")?;
    let serial = cadir.store.lock().next_serial()?;
    let cert = ca.sign_request(csr_pem, san, validity, serial)?;
    let cert_str = std::str::from_utf8(&cert).context("signed cert is not utf8")?;
    let csr_str = std::str::from_utf8(csr_pem).unwrap_or("");
    record_offline_issuance(
        &mut cadir.store.lock(),
        serial,
        kind,
        name,
        csr_str,
        cert_str,
        validity,
    )?;
    Ok(cert)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_serving_san_is_refused() {
        assert!(ensure_san_not_reserved(&[SanEntry::Dns(SERVING_SAN.to_string())]).is_err());
        // DNS is case-insensitive — an upper/mixed-case variant is the same
        // reserved name and must also be refused.
        assert!(ensure_san_not_reserved(&[SanEntry::Dns(SERVING_SAN.to_uppercase())]).is_err());
        // A normal name is fine.
        assert!(
            ensure_san_not_reserved(&[SanEntry::Dns("resolver.example.com".to_string())]).is_ok()
        );
    }

    #[test]
    fn san_parser() {
        assert!(matches!(
            parse_san_one("dns:example.com").unwrap(),
            SanEntry::Dns(s) if s == "example.com"
        ));
        assert!(matches!(
            parse_san_one("ip:127.0.0.1").unwrap(),
            SanEntry::Ip(_)
        ));
        assert!(parse_san_one("uri:https://x").is_ok());
        assert!(parse_san_one("email:a@b").is_ok());
        assert!(parse_san_one("bogus").is_err());
        assert!(parse_san_one("bogus:x").is_err());
        assert!(parse_san_one("ip:not-an-ip").is_err());
        for kind in ["dns", "ip", "uri", "email"] {
            assert!(
                parse_san_one(&format!("{kind}:")).is_err(),
                "empty {kind} value must be rejected"
            );
        }
    }

    #[test]
    fn parse_sans_defaults_to_dns_cn() {
        let v = parse_sans(&[], "host.example.com").unwrap();
        assert!(matches!(&v[..], [SanEntry::Dns(d)] if d == "host.example.com"));
    }

    #[test]
    fn sanitize_filename_keeps_safe_chars_replaces_rest() {
        assert_eq!(sanitize_filename("alice.example.com"), "alice.example.com");
        assert_eq!(sanitize_filename("host-1_test"), "host-1_test");
        assert_eq!(sanitize_filename("a/b"), "a_b");
        assert_eq!(sanitize_filename("../etc/passwd"), ".._etc_passwd");
        assert_eq!(sanitize_filename("with space"), "with_space");
        assert_eq!(sanitize_filename("weird*<>chars"), "weird___chars");
        assert_eq!(sanitize_filename(""), "_");
    }

    #[test]
    fn default_filenames() {
        assert_eq!(default_csr_filename("alice.example.com"), PathBuf::from("alice.example.com.csr"));
        assert_eq!(default_csr_filename("../sneaky"), PathBuf::from(".._sneaky.csr"));
        assert_eq!(
            default_cert_filename(Some("alice.example.com")),
            PathBuf::from("alice.example.com.pem")
        );
        assert_eq!(default_cert_filename(None), PathBuf::from("certificate.pem"));
    }
}
