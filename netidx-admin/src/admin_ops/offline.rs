//! Offline CA sign/issue behind the [`Answerer`] seam.
//!
//! Unlike the remote-admin groups in this module, `ca sign` / `ca issue` are
//! **local**: no admin server, no `--server`, no glyph. They unlock the CA's
//! keyslot vault directly (holding the daemon's exclusive flock) and mint a
//! cert on the box. The pure primitives — the unlock split, `sign_and_record`,
//! `issue_and_record`, the SAN helpers — live in [`crate::offline_ca`]; the
//! orchestration here is thin, and the only operator interaction is the
//! conditional CA recovery password (tried keytab-first) plus, for `sign`, the
//! optional id-map registration.

use crate::{
    admin_proto::{NodeKind, Secret},
    answer::{Answerer, Field},
    atomic,
    ca::{self, Ca, CsrSummary, IssueParams, SanEntry, Subject},
    ca_store::CaDir,
    ca_vault::{CAVault, Unlocked},
    id_map,
    offline_ca::{self, KeytabOutcome},
};
use anyhow::{Context, Result, anyhow, bail};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

// -- CA open (unlock) --------------------------------------------------------

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
/// them) — which replaces the old "stdin is not a TTY" guard.
pub async fn open_ca(ans: &mut dyn Answerer, dir: &Path) -> Result<Ca> {
    if CAVault::exists(dir) {
        // Scope the flock to the unlock: it drops when `unlocked` is bound,
        // before `load_ca_from_unlocked` (which only reads the public cert) and
        // before the caller's sign/issue re-opens its own CaDir.
        let unlocked = {
            let cadir = CaDir::open(dir).context(
                "cannot open the CA offline: a running admin server owns it — stop it first",
            )?;
            let keytab = offline_ca::autorenew_keytab_path()?;
            match offline_ca::try_unlock_with_keytab(&cadir, &keytab) {
                KeytabOutcome::Unlocked(u) => u,
                // No autorenew slot on this box — the normal offline case.
                KeytabOutcome::Absent => recovery_unlock(ans, &cadir, dir).await?,
                // A keytab was there but didn't unlock this CA (different CA
                // dir, cleared TPM): note it, then fall back.
                KeytabOutcome::Failed(e) => {
                    ans.note(&format!(
                        "the autorenew keytab did not unlock this CA ({e:#}); \
                         falling back to the recovery password"
                    ));
                    recovery_unlock(ans, &cadir, dir).await?
                }
            }
        };
        offline_ca::load_ca_from_unlocked(dir, &unlocked)
    } else {
        // Legacy single-key CA.
        match Ca::open(dir, None) {
            Ok(ca) => Ok(ca),
            Err(e) if format!("{e:#}").contains("encrypted") => {
                let pw = ans.secret(Field::RecoveryPassword, None).await?;
                Ca::open(dir, Some(pw.as_str()))
                    .with_context(|| format!("opening CA at {}", dir.display()))
            }
            Err(e) => Err(e).with_context(|| format!("opening CA at {}", dir.display())),
        }
    }
}

/// Prompt for the recovery password (via the Answerer) and unlock `cadir` with
/// it. The fold-back to canonical form happens in [`offline_ca::unlock_with_recovery`].
async fn recovery_unlock(
    ans: &mut dyn Answerer,
    cadir: &CaDir,
    dir: &Path,
) -> Result<Unlocked> {
    let typed = ans.secret(Field::RecoveryPassword, None).await?;
    offline_ca::unlock_with_recovery(cadir, typed.as_str())
        .with_context(|| format!("unlocking the CA vault at {}", dir.display()))
}

// -- ca sign -----------------------------------------------------------------

/// How the signed cert's SubjectAltName is chosen. The CA is authoritative, so
/// this overrides whatever the CSR claims.
pub enum SignSan {
    /// Use exactly these SANs (from `--san`).
    Explicit(Vec<SanEntry>),
    /// Inherit the SAN the CSR carries (`--accept-csr-san`).
    InheritCsr,
    /// Neither flag given — ask the operator whether to inherit the CSR's SAN.
    /// The strict answerer errors (`--accept-csr-san` / `--san` required).
    Ask,
}

/// What to do about registering the newly signed identity in the local id-map.
pub enum IdMapAction {
    /// `--no-id-map`: never register.
    Skip,
    /// `--id-map-group`: register with these groups (and `--uid`) non-interactively.
    Register { groups: Vec<String>, uid: Option<u32> },
    /// Neither flag: prompt interactively when a map exists; strict CLI skips
    /// (matching the old non-TTY behaviour — scripts register explicitly).
    Ask,
}

/// The prior id-map record an [`IdMapResult::Registered`] replaced, if any.
pub struct PrevIdentity {
    pub uid: u32,
    pub primary_group: String,
}

/// A successful id-map registration.
pub struct IdMapRegistration {
    pub name: String,
    pub uid: u32,
    pub primary: String,
    /// `Some` when this updated an existing entry.
    pub previous: Option<PrevIdentity>,
}

/// The outcome of the (optional) post-sign id-map step — why it did nothing, or
/// what it registered. The CLI turns each variant into a one-line message.
pub enum IdMapResult {
    /// Not attempted (`--no-id-map`, or `Ask` under a non-interactive frontend).
    NotRequested,
    /// The cert had no usable identity name (no DNS SAN and no CN).
    NoIdentityName,
    /// No local id-map exists to register into.
    NoMap { path: PathBuf },
    /// The operator declined the interactive prompt.
    Declined,
    /// Registered (or updated) the identity.
    Registered(IdMapRegistration),
}

/// The result of `ca sign`, for the CLI to print.
pub struct SignOutcome {
    pub summary: CsrSummary,
    /// The SAN embedded in the signed cert.
    pub san: Vec<SanEntry>,
    /// The identity name (first DNS SAN, else the CN).
    pub name: String,
    /// Where the signed cert was written.
    pub out: PathBuf,
    pub id_map: IdMapResult,
}

/// Sign an external CSR offline: unlock the CA, resolve the SAN, refuse the
/// reserved serving name, sign + record (allocating a serial under the flock),
/// write the cert, then optionally register the identity in the local id-map.
/// The SAN is resolved (and fails fast) before the CA is unlocked, so a
/// bad/missing SAN errors before any password prompt. `out` defaults to
/// `./<csr-cn>.pem` (`./certificate.pem` for a CN-less CSR).
pub async fn ca_sign(
    ans: &mut dyn Answerer,
    ca_dir: PathBuf,
    csr_pem: Vec<u8>,
    san: SignSan,
    validity: Duration,
    out: Option<PathBuf>,
    id_map: IdMapAction,
) -> Result<SignOutcome> {
    let summary = ca::inspect_csr(&csr_pem).context("inspecting CSR")?;
    let out = out
        .unwrap_or_else(|| offline_ca::default_cert_filename(summary.common_name.as_deref()));
    let san = resolve_san(ans, san, &summary).await?;
    offline_ca::ensure_san_not_reserved(&san)?;
    let name = offline_ca::first_dns_san(&san)
        .or_else(|| summary.common_name.clone())
        .unwrap_or_default();
    let ca = open_ca(ans, &ca_dir).await?;
    let cert_pem =
        offline_ca::sign_and_record(&ca, NodeKind::Client, &csr_pem, &san, &name, validity)?;
    atomic::write_atomic(&out, &cert_pem, 0o644)
        .with_context(|| format!("writing certificate to {}", out.display()))?;
    let id_map = register_id_map(ans, &summary, &san, id_map).await?;
    Ok(SignOutcome { summary, san, name, out, id_map })
}

/// Resolve which SAN to embed, preserving the CLI's decision table: explicit
/// `--san` wins; `--accept-csr-san` inherits the CSR's; neither asks (strict
/// errors). Inheriting an empty CSR SAN is an error either way.
async fn resolve_san(
    ans: &mut dyn Answerer,
    san: SignSan,
    summary: &CsrSummary,
) -> Result<Vec<SanEntry>> {
    match san {
        SignSan::Explicit(v) => Ok(v),
        SignSan::InheritCsr => {
            if summary.san.is_empty() {
                bail!(
                    "--accept-csr-san was set but the CSR carries no SAN; pass \
                     --san <kind>:<value> to specify one"
                );
            }
            Ok(summary.san.clone())
        }
        SignSan::Ask => {
            let inherit = ans.confirm(Field::AcceptCsrSan, None, true).await?;
            if !inherit {
                bail!(
                    "rejected — re-run with --san <kind>:<value> (one or more) to \
                     override the CSR's SAN"
                );
            }
            if summary.san.is_empty() {
                bail!(
                    "CSR carries no SAN to inherit; pass --san <kind>:<value> (one \
                     or more) to specify one"
                );
            }
            Ok(summary.san.clone())
        }
    }
}

/// The post-sign id-map registration (sign only). Skips cleanly when there is
/// no map / no identity name; registers non-interactively for `Register`, and
/// prompts (groups then uid) for `Ask` under an interactive frontend. Group and
/// uid validity is enforced by [`id_map::upsert_identity`].
async fn register_id_map(
    ans: &mut dyn Answerer,
    summary: &CsrSummary,
    san: &[SanEntry],
    action: IdMapAction,
) -> Result<IdMapResult> {
    // `groups`/`uid` are `Some` when supplied by flags; `None` means "prompt"
    // (interactive) — `Ask` under a strict answerer has already returned above.
    let (flag_groups, flag_uid) = match action {
        IdMapAction::Skip => return Ok(IdMapResult::NotRequested),
        IdMapAction::Register { groups, uid } => (Some(groups), uid),
        IdMapAction::Ask => {
            if !ans.interactive() {
                return Ok(IdMapResult::NotRequested);
            }
            (None, None)
        }
    };
    // The id-map identity name is what the resolver sees on the wire: the
    // cert's first DNS SAN, falling back to the CN.
    let identity_name = san
        .iter()
        .find_map(|s| if let SanEntry::Dns(d) = s { Some(d.clone()) } else { None })
        .or_else(|| summary.common_name.clone());
    let identity_name = match identity_name {
        Some(n) => n,
        None => return Ok(IdMapResult::NoIdentityName),
    };
    let map_path = id_map::user_id_map_path()?;
    let mut map = match id_map::load(&map_path) {
        Ok(m) => m,
        Err(_) => return Ok(IdMapResult::NoMap { path: map_path }),
    };
    let groups: Vec<String> = match flag_groups {
        Some(g) => g,
        None => {
            // Interactive: show valid groups, prompt (blank = decline).
            let mut names: Vec<&str> = map.groups.keys().map(|k| k.as_str()).collect();
            names.sort_unstable();
            ans.note(&format!("available groups: {}", names.join(", ")));
            let typed = ans
                .text(Field::IdMapGroups, None, Some("users"), false)
                .await?
                .unwrap_or_default();
            let parsed: Vec<String> = typed
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if parsed.is_empty() {
                return Ok(IdMapResult::Declined);
            }
            parsed
        }
    };
    if groups.is_empty() {
        bail!("no id-map groups specified — at least the primary group is required");
    }
    let uid = match flag_uid {
        Some(u) => u,
        None => {
            let default = id_map::next_uid(&map).to_string();
            let typed = ans
                .text(Field::Uid, None, Some(&default), true)
                .await?
                .ok_or_else(|| anyhow!("a uid is required to register in the id-map"))?;
            typed.trim().parse::<u32>().with_context(|| format!("parsing uid {typed:?}"))?
        }
    };
    let primary = groups[0].clone();
    let secondary: Vec<&str> = groups[1..].iter().map(|s| s.as_str()).collect();
    let previous = id_map::upsert_identity(&mut map, &identity_name, uid, &primary, &secondary)?
        .map(|old| PrevIdentity { uid: old.uid, primary_group: old.primary_group.to_string() });
    id_map::save(&map_path, &map)?;
    Ok(IdMapResult::Registered(IdMapRegistration {
        name: identity_name,
        uid,
        primary,
        previous,
    }))
}

// -- ca issue ----------------------------------------------------------------

/// The result of `ca issue`, for the CLI to print.
pub struct IssueOutcome {
    pub cn: String,
    pub private_key: PathBuf,
    pub certificate: PathBuf,
}

/// Issue a fresh key + cert offline: refuse the reserved serving name, unlock
/// the CA, generate the key, sign it, and record the issuance (serial allocated
/// under the flock). `leaf_password` encrypts the on-disk private key when set
/// (the bare CLI passes `None`; the install flow, which knows where a netidx
/// process finds the passphrase, is the encrypted-leaf path).
pub async fn ca_issue(
    ans: &mut dyn Answerer,
    ca_dir: PathBuf,
    subject: Subject,
    san: Vec<SanEntry>,
    key_bits: u32,
    validity: Duration,
    out_dir: PathBuf,
    leaf_password: Option<Secret>,
) -> Result<IssueOutcome> {
    offline_ca::ensure_san_not_reserved(&san)?;
    let cn = subject.common_name.clone();
    let ca = open_ca(ans, &ca_dir).await?;
    let issued = offline_ca::issue_and_record(
        &ca,
        NodeKind::Client,
        IssueParams {
            subject,
            san,
            key_bits,
            validity,
            out_dir,
            password: leaf_password.map(|s| s.as_str().to_string()),
            serial: 0, // assigned by issue_and_record
        },
    )?;
    Ok(IssueOutcome { cn, private_key: issued.private_key, certificate: issued.certificate })
}
