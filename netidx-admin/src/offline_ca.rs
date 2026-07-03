//! Offline (pre-daemon) CA issuance glue — the non-interactive half of
//! `ca sign` / `ca issue`, shared with the install flow and the daemon's own
//! sign path.
//!
//! "Offline" issuance runs while *no* admin server owns the CA: it takes the
//! same exclusive flock the daemon would, allocates a serial from (and commits
//! back into) the store the daemon reads, and records the issuance so the cert
//! is revocable like any other. The SAN/serial/issuance-recording primitives
//! are pure of operator I/O — the CA is already unlocked and the decisions
//! already made. The one Answerer-driven piece is [`open_ca`] (and the
//! [`unlock_held`] it composes the pure unlock primitives with), the single
//! CA-open entry point shared by the offline sign/issue orchestration
//! ([`crate::admin_ops::offline`]), the install flow, and `ca external`.

use crate::{
    admin_proto::{NodeKind, SERVING_SAN},
    admin_server::read_autorenew_password,
    answer::{Answerer, Field},
    ca::{Ca, IssueParams, IssuedFiles, SanEntry},
    ca_store::{CAStore, CaDir, QueuedReq},
    ca_vault::{self, CAVault, Unlocked},
    paths,
};
use anyhow::{Context, Result, anyhow, bail};
use std::{
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

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

// -- CA master-key unlock (offline) ------------------------------------------
//
// The offline sign/issue path unlocks the CA's keyslot vault to recover the
// signing key. Two credentials can do it: the box's own `autorenew` slot, read
// from its keytab (no human secret typed), or the off-box `recovery` password
// the operator keeps in a safe. These are the pure primitives — try the keytab,
// fold-and-unlock a typed recovery password, recombine the recovered key with
// the cert. The Answerer that prompts for the recovery password (and holds one
// flock across both attempts) lives in [`crate::admin_ops::offline`].

/// `${config}/netidx/autorenew.keytab` — deliberately NOT in the CA dir: never
/// back this file up; recreating it is one `ca auto-approve --rotate`.
pub fn autorenew_keytab_path() -> Result<PathBuf> {
    Ok(paths::user_config_root()?.join("autorenew.keytab"))
}

/// The outcome of trying the box's autorenew keytab as an unlock credential.
/// [`Absent`](KeytabOutcome::Absent) is the normal case for an offline CA with
/// no autorenew slot (the keytab file simply isn't there — a silent fall back
/// to the recovery password). [`Failed`](KeytabOutcome::Failed) means the
/// keytab was present but could not be read/unsealed or did not unlock this CA
/// (a different CA dir, a cleared TPM) — the caller notes it, then falls back.
pub enum KeytabOutcome {
    Unlocked(Unlocked),
    Absent,
    Failed(anyhow::Error),
}

/// Try to unlock the vault at `cadir` with the box's autorenew credential read
/// from `keytab`. Pure: it reads (unsealing if needed) the keytab and attempts
/// the unlock, but never prompts and never falls back — the caller decides what
/// to do with [`Failed`](KeytabOutcome::Failed). The keytab path is a parameter
/// (rather than [`autorenew_keytab_path`]) so this stays testable against a
/// tempdir.
pub fn try_unlock_with_keytab(cadir: &CaDir, keytab: &Path) -> KeytabOutcome {
    if !keytab.exists() {
        return KeytabOutcome::Absent;
    }
    let pw = match read_autorenew_password(keytab) {
        Ok(pw) => pw,
        Err(e) => return KeytabOutcome::Failed(e),
    };
    match cadir.vault.read().unlock(&pw) {
        Ok(u) => KeytabOutcome::Unlocked(u),
        Err(e) => KeytabOutcome::Failed(e),
    }
}

/// Unlock the vault at `cadir` with an operator-typed recovery password,
/// folding it back to canonical form first (so a transcription that grouped the
/// quads for readability, or confused O/0 and I/L/1, still unlocks). Pure: the
/// caller obtains `typed` from the operator (via the Answerer) and hands it
/// here.
pub fn unlock_with_recovery(cadir: &CaDir, typed: &str) -> Result<Unlocked> {
    let pw = ca_vault::normalize_recovery_password(typed);
    cadir.vault.read().unlock(&pw)
}

/// Recombine an already-unlocked master key with the CA cert on disk to produce
/// a signer. The final step of both unlock paths, split out so the flock held
/// while unlocking can drop before this reads the (public) cert.
pub fn load_ca_from_unlocked(dir: &Path, unlocked: &Unlocked) -> Result<Ca> {
    let cert = std::fs::read(dir.join("certificate.pem"))
        .with_context(|| format!("reading CA cert in {}", dir.display()))?;
    Ca::from_pem(dir.to_path_buf(), &unlocked.ca_key_pem, &cert)
        .with_context(|| format!("loading CA at {}", dir.display()))
}

/// Open the CA at `dir` as a signer, unlocking a keyslot vault when present.
///
/// Vaulted (current) CAs: try the box's own autorenew keytab first (no human
/// secret), and only if that is absent or fails ask for the off-box recovery
/// password via the Answerer. One [`CaDir`] flock is held across **both**
/// attempts, so a running admin server can't slip in between them; it drops
/// before the returned [`Ca`] is used to sign (which re-opens its own `CaDir`
/// for serial allocation). Legacy `private.key` CAs open directly, prompting
/// for the key passphrase only if the key turns out to be encrypted.
///
/// The strict answerer supplies the recovery password from
/// `--recovery-password-file` / `--recovery-password-stdin` (or errors naming
/// them) — which replaces the old "stdin is not a TTY" guard; the interactive
/// frontends prompt. This is the single CA-open entry point — every command
/// that signs offline (`ca issue` / `ca sign`, the resolver's local-CA
/// issuance, `ca external`) goes through it, so they all transparently handle
/// both vault formats.
pub async fn open_ca(ans: &mut dyn Answerer, dir: &Path) -> Result<Ca> {
    if CAVault::exists(dir) {
        // Scope the flock to the unlock: it drops when `unlocked` is bound,
        // before `load_ca_from_unlocked` (which only reads the public cert) and
        // before the caller's sign/issue re-opens its own CaDir.
        let unlocked = {
            let cadir = CaDir::open(dir).context(
                "cannot open the CA offline: a running admin server owns it — stop it first",
            )?;
            unlock_held(ans, &cadir, dir).await?
        };
        load_ca_from_unlocked(dir, &unlocked)
    } else {
        // Legacy single-key CA — prompt for the key passphrase only if the
        // on-disk key turns out to be encrypted.
        match Ca::open(dir, None) {
            Ok(ca) => Ok(ca),
            Err(e) if format!("{e:#}").contains("encrypted") => {
                let pw = ans.secret(Field::KeyPassword, None).await?;
                Ca::open(dir, Some(pw.as_str()))
                    .with_context(|| format!("opening CA at {}", dir.display()))
            }
            Err(e) => Err(e).with_context(|| format!("opening CA at {}", dir.display())),
        }
    }
}

/// [`open_ca`] at the conventional `${basedir}/ca/` location.
pub async fn open_default_ca(ans: &mut dyn Answerer) -> Result<Ca> {
    open_ca(ans, &paths::user_ca_dir()?).await
}

/// Unlock the vault at `cadir` while the caller holds its flock: try the box's
/// autorenew keytab first (no human secret), falling back to the operator's
/// recovery password (via the Answerer) on absence or failure. Shared by
/// [`open_ca`] (which drops the flock before signing) and `ca external` (which
/// keeps it), so both fold a typed recovery password to canonical form — the
/// fix for the external path's former raw-password unlock.
pub async fn unlock_held(
    ans: &mut dyn Answerer,
    cadir: &CaDir,
    dir: &Path,
) -> Result<Unlocked> {
    let keytab = autorenew_keytab_path()?;
    match try_unlock_with_keytab(cadir, &keytab) {
        KeytabOutcome::Unlocked(u) => Ok(u),
        // No autorenew slot on this box — the normal offline case.
        KeytabOutcome::Absent => recovery_unlock(ans, cadir, dir).await,
        // A keytab was there but didn't unlock this CA (different CA dir,
        // cleared TPM): note it, then fall back to the recovery password.
        KeytabOutcome::Failed(e) => {
            ans.note(&format!(
                "the autorenew keytab did not unlock this CA ({e:#}); falling back \
                 to the recovery password"
            ));
            recovery_unlock(ans, cadir, dir).await
        }
    }
}

/// Prompt for the recovery password (via the Answerer) and unlock `cadir` with
/// it. The fold-back to canonical form happens in [`unlock_with_recovery`].
async fn recovery_unlock(
    ans: &mut dyn Answerer,
    cadir: &CaDir,
    dir: &Path,
) -> Result<Unlocked> {
    let typed = ans.secret(Field::RecoveryPassword, None).await?;
    unlock_with_recovery(cadir, typed.as_str())
        .with_context(|| format!("unlocking the CA vault at {}", dir.display()))
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

    // -- unlock split --------------------------------------------------------
    //
    // These exercise the CA-master-key unlock the offline sign/issue path
    // depends on, which had NO coverage before the 8b split (the issuance
    // integration tests build `Ca` directly and never unlock a vault). Stand up
    // a real vaulted CA in a tempdir — a recovery slot (off-box password) plus
    // an autorenew slot (the box's keytab) — exactly as the install flow does,
    // then unlock it both ways.

    /// A vaulted CA in `dir` with a `recovery` slot and an `autorenew` slot,
    /// mirroring `admin_server`'s test setup and the real install path. Returns
    /// `(recovery_password, autorenew_password)`; the recovery password is
    /// freshly minted (as at init) so we can feed it back to `unlock`.
    fn vaulted_ca(dir: &Path) -> (zeroize::Zeroizing<String>, String) {
        use crate::ca::{CaParams, Subject};
        let params = CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn("test-ca"),
            san: vec![SanEntry::Dns("test-ca".to_string())],
            key_bits: 2048, // smaller for test speed
            validity: Duration::from_secs(30 * 86400),
        };
        Ca::init(&params, None).unwrap();
        let key = std::fs::read(dir.join("private.key")).unwrap();
        let mut vault = ca_vault::CAVault::new(dir.to_path_buf());
        let recovery_pw = ca_vault::gen_recovery_password();
        vault
            .create(&key, ca_vault::RECOVERY_ADMIN, &recovery_pw, crate::ca_policy::recovery_policy())
            .unwrap();
        std::fs::remove_file(dir.join("private.key")).unwrap();
        let autorenew_pw = "renew-secret-01234".to_string();
        vault
            .add_signing_slot(
                &recovery_pw,
                crate::admin_server::AUTORENEW_ADMIN,
                &autorenew_pw,
                crate::ca_policy::autorenew_policy(),
            )
            .unwrap();
        (recovery_pw, autorenew_pw)
    }

    #[test]
    fn recovery_password_unlocks_canonical_and_grouped() {
        let dir = tempfile::tempdir().unwrap();
        let (recovery_pw, _) = vaulted_ca(dir.path());
        let cadir = CaDir::open(dir.path()).unwrap();
        // The canonical minted form unlocks.
        let u = unlock_with_recovery(&cadir, &recovery_pw).unwrap();
        assert_eq!(u.admin, ca_vault::RECOVERY_ADMIN);
        // The grouped form — what the operator reads back out of the safe —
        // must ALSO unlock: `unlock_with_recovery` folds the quads back out.
        // Without the normalize step the spaces would make this fail, so this
        // is the regression guard for the normalize round-trip.
        let grouped = ca_vault::group_recovery_password(&recovery_pw);
        assert_ne!(&*grouped, &*recovery_pw, "grouping must actually change the string");
        let u2 = unlock_with_recovery(&cadir, &grouped).unwrap();
        assert_eq!(u2.admin, ca_vault::RECOVERY_ADMIN);
        // The recovered key recombines with the cert into a usable signer.
        load_ca_from_unlocked(dir.path(), &u2).unwrap();
    }

    #[test]
    fn wrong_recovery_password_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let _ = vaulted_ca(dir.path());
        let cadir = CaDir::open(dir.path()).unwrap();
        assert!(unlock_with_recovery(&cadir, "NOT THE PASSWORD").is_err());
    }

    #[test]
    fn keytab_absent_is_absent_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let _ = vaulted_ca(dir.path());
        let cadir = CaDir::open(dir.path()).unwrap();
        let keytab = dir.path().join("does-not-exist.keytab");
        assert!(matches!(try_unlock_with_keytab(&cadir, &keytab), KeytabOutcome::Absent));
    }

    #[test]
    fn keytab_unlocks_with_the_box_credential() {
        let dir = tempfile::tempdir().unwrap();
        let (_, autorenew_pw) = vaulted_ca(dir.path());
        // A plaintext keytab is just the password; the install path seals it to
        // the TPM when it can, and `read_autorenew_password` handles both.
        let keytab = dir.path().join("autorenew.keytab");
        std::fs::write(&keytab, &autorenew_pw).unwrap();
        let cadir = CaDir::open(dir.path()).unwrap();
        match try_unlock_with_keytab(&cadir, &keytab) {
            KeytabOutcome::Unlocked(u) => {
                assert_eq!(u.admin, crate::admin_server::AUTORENEW_ADMIN)
            }
            _ => panic!("expected the keytab to unlock the autorenew slot"),
        }
    }

    #[test]
    fn keytab_with_wrong_password_fails_not_absent() {
        let dir = tempfile::tempdir().unwrap();
        let _ = vaulted_ca(dir.path());
        let keytab = dir.path().join("autorenew.keytab");
        std::fs::write(&keytab, "wrong-password").unwrap();
        let cadir = CaDir::open(dir.path()).unwrap();
        // A present-but-wrong keytab is Failed (→ caller notes it and falls back
        // to the recovery password), NOT Absent (→ silent fall back).
        assert!(matches!(try_unlock_with_keytab(&cadir, &keytab), KeytabOutcome::Failed(_)));
    }
}
