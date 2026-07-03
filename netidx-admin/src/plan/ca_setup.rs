//! The CA-creation "brain": build a new vaulted CA, seal its off-box
//! recovery credential, stand up the founding superuser role admin and the
//! box's autorenew slot, and issue the bootstrap certificates the daemon
//! serves under.
//!
//! Lifted out of the `netidx-tools` CLI so every frontend — the strict CLI,
//! the ratatui TUI, Atlas — drives the identical flow. No `prompt::`, no
//! `println!`/`eprintln!`: every question, note, and warning goes through the
//! [`Answerer`] seam, and the security-critical refusals (no TPM, plaintext
//! keytab) stay hard `bail!`s.

use crate::{
    admin_proto::NodeKind,
    admin_server::AUTORENEW_ADMIN,
    answer::{Answerer, Field},
    atomic,
    ca::{self, Ca, CaLifetimes, CaParams, IssueParams, IssuedFiles, SanEntry, Subject},
    ca_store, ca_vault,
    fingerprint::Fingerprint,
    offline_ca, paths,
    plan::{enroll, server_setup, service::ServiceNeed},
    tls,
};
use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};
use zeroize::Zeroizing;

/// Inputs to [`create_vaulted_ca`], the single new-CA entry point.
/// Fields are primitive so callers (the `ca init` command *and* the
/// resolver install) don't need the engine's `Subject` / `SanEntry`
/// types — `create_vaulted_ca` builds those internally.
pub struct NewCaOpts {
    pub dir: PathBuf,
    /// CA cert CN. `None` ⇒ prompt, defaulting to `ca.<domain>` when
    /// `domain` is set (see [`default_ca_cn`]).
    pub common_name: Option<String>,
    /// The TLS domain this CA serves (e.g. `ryu-oh.org`), when known —
    /// threaded from the resolver install, which already asks for it.
    /// Seeds the CN default (`ca.<domain>`) and the admin policy
    /// suggestion (`*.<domain>`). `None` for a bare `ca init` with no
    /// `--domain`.
    pub domain: Option<String>,
    pub country: Option<String>,
    pub state: Option<String>,
    pub locality: Option<String>,
    pub organization: Option<String>,
    /// Raw `--san` strings for the CA cert; empty ⇒ `dns:<cn>`.
    pub san: Vec<String>,
    pub key_bits: u32,
    /// Validity stamped on the CA cert itself.
    pub ca_validity: Duration,
    /// Default validity for leaves the CA issues (e.g. the serving cert).
    pub leaf_validity: Duration,
    /// Renew the CA cert once its remaining lifetime drops below this.
    pub ca_renew_threshold: Duration,
    /// Superuser (role) admin name; `None` ⇒ prompt, defaulting to the
    /// current unix user. Created only when the admin server is set up
    /// (a role admin authenticates to the daemon; an offline CA has none).
    pub admin: Option<String>,
    /// Superuser's server-signing scope globs; empty ⇒ prompt (default
    /// `*.<domain>` when `domain` is set, else derived from the CN).
    pub allowed_san: Vec<String>,
    pub max_validity: Duration,
    /// Superuser's id-map groups; empty ⇒ prompt (default `users`).
    pub id_map_groups: Vec<String>,
    /// Whether the superuser may enroll admin servers; `None` ⇒
    /// prompt, defaulting to yes (someone has to be able to grow the
    /// network).
    pub may_enroll_servers: Option<bool>,
    /// Proceed without a TPM / Secure Enclave (autorenew keytab written
    /// in plaintext). A loud warning is printed; test CAs only.
    pub insecure_no_tpm: bool,
    /// `None` ⇒ prompt "set up the admin server?"; `Some(b)` ⇒ forced.
    pub setup_server: Option<bool>,
    /// Explicit `--listen` for the CA server (skips the prompt).
    pub listen: Option<SocketAddr>,
    /// IP to suggest for the CA server's listen address when prompting
    /// (e.g. the resolver being created in the same flow). `None` ⇒
    /// fall back to an existing resolver's IP, then the public IP.
    pub listen_hint: Option<IpAddr>,
    /// Where to drop the `ca` activation unit (already resolved).
    /// `None` ⇒ don't write a unit (e.g. `--no-units`); the server is
    /// still configured for manual `ca serve`.
    pub units_dir: Option<PathBuf>,
}

/// Create (or replace) the autorenew slot + keytab. `recovery_password`
/// authorizes the re-mint: the old autorenew slot is removed first, so the
/// authorizing credential must be a *different* signing slot — in the
/// server-only model that is the `recovery` password (which is also why
/// rotating autorenew, e.g. after a TPM clear, needs the recovery
/// password). Returns the keytab path.
///
/// The keytab is TPM-sealed when the host has a usable TPM 2.0: at
/// rest the slot password is a CA-key-decryption credential (any signing
/// slot password unlocks the vault's master key), so a plaintext keytab
/// makes every disk image and backup of this host a CA compromise.
/// Sealed, the file is inert anywhere but this machine. A host with no
/// TPM (or a flaky one — setup must not dead-end) falls back to the
/// plaintext keytab with a note saying what that costs.
pub fn setup_autorenew_slot(
    ans: &mut dyn Answerer,
    cadir: &ca_store::CaDir,
    recovery_password: &str,
    insecure_no_tpm: bool,
) -> Result<PathBuf> {
    // Replace-not-fail: rotation and re-runs both land here.
    let exists = cadir
        .vault
        .read()
        .list_admins()?
        .iter()
        .any(|info| info.admin == AUTORENEW_ADMIN);
    if exists {
        cadir.vault.write().remove_slot(AUTORENEW_ADMIN, false)?;
    }
    let password = ca_vault::random_signing_password();
    cadir.vault.write().add_signing_slot(
        recovery_password,
        AUTORENEW_ADMIN,
        &password,
        crate::ca_policy::autorenew_policy(),
    )?;
    let keytab = offline_ca::autorenew_keytab_path()?;
    // `available()` (which the caller's TPM gate checked) only proves the
    // device opened, NOT that a seal will succeed — a present-but-locked or
    // busy TPM fails here. The seal decision is the real one: a plaintext
    // keytab is a master-key-equivalent credential, so falling back to it
    // silently would defeat the whole gate. Only `--insecure-no-tpm` accepts
    // that, and then loudly; otherwise we refuse and roll the slot back.
    match netidx_tpm::seal(password.as_bytes()) {
        Ok(blob) => {
            atomic::write_atomic(&keytab, &blob, 0o600)?;
            ans.note(&format_compact!(
                "  the keytab is sealed to this machine's {} — copied \
                 anywhere else (disk image, backup) it is useless",
                netidx_tpm::MECHANISM
            ));
        }
        Err(e) if insecure_no_tpm => {
            atomic::write_atomic(&keytab, password.as_bytes(), 0o600)?;
            ans.warn(&format_compact!(
                "the autorenew keytab is PLAINTEXT ({} sealing failed: {e:#}). \
                 Any backup or disk image of this machine now contains a \
                 credential that unlocks the CA key. You accepted this with \
                 --insecure-no-tpm.",
                netidx_tpm::MECHANISM
            ));
        }
        Err(e) => {
            // Refuse: undo the slot we just minted so the vault is unchanged,
            // and don't write the plaintext keytab. The operator can fix the
            // TPM and re-run, or opt in with --insecure-no-tpm.
            let _ = cadir.vault.write().remove_slot(AUTORENEW_ADMIN, false);
            bail!(
                "the autorenew credential could not be sealed to this host's {mech} \
                 ({e:#}). Writing it in plaintext would be equivalent to backing up the \
                 CA key, so this is refused. Fix the {mech} (e.g. clear an owner-auth or \
                 dictionary-attack lockout) and re-run `netidx admin ca auto-approve`, or \
                 pass --insecure-no-tpm to accept a plaintext keytab (test CAs only).",
                mech = netidx_tpm::MECHANISM
            );
        }
    }
    Ok(keytab)
}

/// Seal a freshly generated CA key into the vault's `recovery` slot and
/// persist the lifetime policy, under one flock held for the rest of init.
/// Shared by the self-signed [`create_vaulted_ca`] and the external-sign
/// bootstrap. On a mid-write failure, roll back whatever init committed so
/// the dir isn't a keyless half-CA that blocks a clean retry.
pub fn seal_ca_recovery(
    dir: &Path,
    key_pem: &Zeroizing<Vec<u8>>,
    lifetimes: CaLifetimes,
) -> Result<(Zeroizing<String>, ca_store::CaDir)> {
    let recovery_pw = ca_vault::gen_recovery_password();
    let cadir = ca_store::CaDir::open(dir).context("opening the new CA directory")?;
    if let Err(e) = cadir.vault.write().create(
        key_pem,
        ca_vault::RECOVERY_ADMIN,
        &recovery_pw,
        crate::ca_policy::recovery_policy(),
    ) {
        let _ = std::fs::remove_file(dir.join("certificate.pem"));
        let _ = std::fs::remove_file(dir.join("serial"));
        return Err(e).context("sealing CA key into the vault");
    }
    lifetimes.store(dir).context("writing CA lifetimes")?;
    Ok((recovery_pw, cadir))
}

/// **The** entry point for building a new vaulted CA, shared verbatim
/// by `netidx admin ca init` and the `netidx admin resolver install`
/// "create a new CA" branch — so the operator gets the identical
/// experience (admin/policy, identicon, the "set up the CA server?"
/// question) either way.
///
/// Returns the in-memory signing [`Ca`] (use it to issue certs before
/// it drops — e.g. the resolver issues its own identity from it) and
/// the [`ServiceNeed`] the caller folds into its single service offer.
/// This function never offers the service itself; that's the caller's
/// end-of-process step, so a resolver install can merge this need with
/// its own and offer once.
pub async fn create_vaulted_ca(
    ans: &mut dyn Answerer,
    opts: NewCaOpts,
) -> Result<(Ca, ServiceNeed)> {
    // CN first (matching the prompt order `ca init` had before this was
    // centralized here): an explicit `--cn` / threaded value wins,
    // otherwise prompt with the `ca.<domain>` default when we know the
    // domain.
    let common_name =
        resolve_ca_cn(ans, opts.common_name.clone(), opts.domain.as_deref()).await?;
    // The admin-server config wants a concrete domain (it's what the
    // network is grouped by in discovery). Prefer the threaded one;
    // fall back to the CN's domain part, which `resolve_ca_cn` makes
    // likely (`ca.<domain>`).
    let domain = match &opts.domain {
        Some(d) if !d.is_empty() => d.clone(),
        _ => match common_name.split_once('.') {
            Some((_, d)) if !d.is_empty() => d.to_string(),
            _ => common_name.clone(),
        },
    };
    // Refuse to build a CA on a host that can't seal the box credential
    // (or loudly warn under --insecure-no-tpm) BEFORE anything touches
    // disk, so a refused init leaves the dir clean and retryable.
    tpm_gate(ans, opts.insecure_no_tpm)?;
    let san = offline_ca::parse_sans(&opts.san, &common_name)?;

    // Generate the CA (its key is returned, never written to disk in
    // plaintext) and seal it into the vault under the `recovery` slot —
    // the off-box break-glass credential whose generated password is shown
    // once and never stored.
    let (ca, key_pem) = Ca::init_vaulted(&CaParams {
        directory: opts.dir.clone(),
        subject: Subject {
            common_name: common_name.clone(),
            country: opts.country.clone(),
            state: opts.state.clone(),
            locality: opts.locality.clone(),
            organization: opts.organization.clone(),
        },
        san,
        key_bits: opts.key_bits,
        validity: opts.ca_validity,
    })?;
    // Seal the key into the recovery slot and persist the lifetime policy
    // (self-signed CA — externally_signed is false). One flock is held for
    // the rest of init.
    let (recovery_pw, cadir) = seal_ca_recovery(
        &opts.dir,
        &key_pem,
        CaLifetimes {
            leaf_validity: opts.leaf_validity,
            ca_renew_threshold: opts.ca_renew_threshold,
            externally_signed: false,
        },
    )?;

    ans.note(&format_compact!("created a new CA at {}", opts.dir.display()));
    show_recovery_password(ans, &recovery_pw);
    show_ca_identity(ans, &opts.dir)?;
    ans.note(
        "Share the fingerprint/identicon above with anyone joining, so they \
         can verify they're talking to the real CA before sending a password.",
    );

    let set_up_server =
        ans.confirm(Field::SetupAdminServer, opts.setup_server, true).await?;
    let need = if set_up_server {
        // setup_server signs the serving cert through the offline issuance
        // path, which takes the CA flock itself — so release ours first,
        // then reacquire for the remaining slot setup. During init no daemon
        // competes for the brand-new dir, so the brief unlock is safe; this
        // is the same drop-and-reopen the offline `ca issue`/`sign` paths use.
        drop(cadir);
        let need = server_setup::setup_server(
            ans,
            server_setup::SetupArgs {
                ca_dir: &opts.dir,
                ca: &ca,
                domain: &domain,
                listen: opts.listen,
                listen_hint: opts.listen_hint,
                units_dir: opts.units_dir.as_deref(),
            },
        )
        .await?;
        let cadir = ca_store::CaDir::open(&opts.dir)
            .context("reopening the CA directory after serving-cert setup")?;
        // The box's `autorenew` credential — the only signing key the
        // daemon ever holds, and what it signs on a role admin's behalf
        // with. Mandatory for a server CA. Authorized by the recovery
        // password we just minted; sealed to the TPM (or plaintext under
        // --insecure-no-tpm, which the gate above already warned about).
        let keytab =
            setup_autorenew_slot(ans, &cadir, &recovery_pw, opts.insecure_no_tpm)?;
        let cfg_path = server_setup::set_ca_autorenew(&keytab)?;
        ans.note(&format_compact!(
            "automatic renewal approval enabled:\n\
             \x20 slot:   {AUTORENEW_ADMIN:?} (empty issuance scope)\n\
             \x20 keytab: {} (0600 — do NOT back this file up;\n\
             \x20         rotate anytime with `netidx admin ca auto-approve --rotate`)\n\
             \x20 config: {} (roles.ca.autorenew)",
            keytab.display(),
            cfg_path.display()
        ));
        // The founding SUPERUSER role admin: it directs the server (mint
        // admins, edit perms, enroll servers) but wraps no MK, so its
        // password can NEVER unlock the CA key — only the server signs.
        setup_superuser(ans, &cadir, &opts, &common_name).await?;
        need
    } else {
        // An offline CA has no daemon to sign on anyone's behalf, so it
        // grows no autorenew slot and no role admins: the recovery password
        // is the operator's credential for local `ca issue` / `ca sign`.
        ServiceNeed::NONE
    };
    Ok((ca, need))
}

/// Build the [`NewCaOpts`] for the founding CA a resolver install stands
/// up when it creates a network's trust root — shared by the TLS
/// "generate" branch and the krb5/anonymous admin-plane branch so the two
/// cannot drift. Unlike `ca init` (the explicit tuning flow, which
/// interrogates the founding admin), an install applies a sensible
/// zero-prompt founding-admin policy: issue `*.<domain>`, place enrolled
/// nodes in the `users` id-map group, and may enroll admin servers. Say
/// what it is (and how to change it) with [`announce_founding_policy`].
pub fn founding_ca_opts(
    dir: PathBuf,
    domain: String,
    insecure_no_tpm: bool,
    setup_server: Option<bool>,
    listen_hint: Option<IpAddr>,
    units_dir: Option<PathBuf>,
) -> NewCaOpts {
    let common_name = Some(default_ca_cn(&domain));
    let allowed_san = vec![format!("*.{domain}")];
    NewCaOpts {
        dir,
        common_name,
        domain: Some(domain),
        country: None,
        state: None,
        locality: None,
        organization: None,
        san: vec![],
        key_bits: ca::DEFAULT_KEY_BITS,
        ca_validity: ca::DEFAULT_CA_VALIDITY,
        leaf_validity: ca::DEFAULT_LEAF_VALIDITY,
        ca_renew_threshold: ca::DEFAULT_CA_RENEW_THRESHOLD,
        admin: None,
        allowed_san,
        max_validity: ca::DEFAULT_LEAF_VALIDITY,
        id_map_groups: vec!["users".to_string()],
        may_enroll_servers: Some(true),
        insecure_no_tpm,
        setup_server,
        listen: None,
        listen_hint,
        units_dir,
    }
}

/// Tell the operator the founding-admin policy [`founding_ca_opts`]
/// applied, and how to change it — announced by both install branches in
/// place of the `ca init` interrogation.
pub fn announce_founding_policy(ans: &mut dyn Answerer, domain: &str) {
    ans.note(&format_compact!(
        "  the CA's founding admin will issue *.{domain} certificates, place \
         enrolled nodes in the 'users' id-map group, and may enroll admin \
         servers — change any of this later with `netidx admin ca admin \
         set-policy`."
    ));
    ans.note(
        "  (chaining this CA to an existing PKI is a separate up-front choice: \
         create it beforehand with `netidx admin ca init --external-sign`.)",
    );
}

/// Refuse to build a CA on a host with no usable TPM / Secure Enclave —
/// before any disk write — unless the operator explicitly accepts the cost
/// with `--insecure-no-tpm`, in which case warn loudly. The autorenew
/// credential is sealed to the box's TPM precisely so a stolen backup is
/// inert; without sealing it sits in plaintext in every backup.
pub fn tpm_gate(ans: &mut dyn Answerer, insecure_no_tpm: bool) -> Result<()> {
    if netidx_tpm::available() {
        return Ok(());
    }
    let mech = netidx_tpm::MECHANISM;
    if !insecure_no_tpm {
        bail!(
            "this host has no usable {mech}. A CA's autorenew credential is sealed \
             to the {mech} so a stolen backup or disk image of this machine is inert \
             on its own. Without it, that credential sits in PLAINTEXT in every \
             backup — equivalent to backing up the CA key.\n\n\
             Run the CA on hardware with a TPM 2.0 / Secure Enclave, or pass \
             --insecure-no-tpm to override (test CAs only)."
        );
    }
    ans.warn(&format_compact!(
        "--insecure-no-tpm — no {mech} sealing on this host. The autorenew keytab \
         will be written in PLAINTEXT, so any backup or disk image of this machine \
         then contains a credential that unlocks the CA key. Use this for TEST CAs \
         only."
    ));
    Ok(())
}

/// Present the recovery password exactly once through the dedicated
/// [`Answerer::show_recovery_password`] seam (a CLI boxes it with a
/// store-it-in-a-safe warning, a TUI forces acknowledgment). It is never
/// persisted, so this is the only time it is shown.
pub fn show_recovery_password(ans: &mut dyn Answerer, pw: &str) {
    let grouped = ca_vault::group_recovery_password(pw);
    ans.show_recovery_password(&grouped);
}

/// Show the CA's own identity (fingerprint + identicon) as an out-of-band
/// verification code, so anyone joining can match it before trusting the CA.
pub fn show_ca_identity(ans: &mut dyn Answerer, ca_dir: &Path) -> Result<()> {
    let cert = std::fs::read(ca_dir.join("certificate.pem"))
        .with_context(|| format!("reading CA cert in {}", ca_dir.display()))?;
    let fp = Fingerprint::of_cert_pem(&cert)?;
    ans.show_verification_code("CA identity", &fp);
    Ok(())
}

/// Create the founding superuser ROLE admin (operator names it + sets its
/// password). Full authority — broad issuance scope, may enroll servers,
/// edits perms anywhere, and manages other admins — yet it wraps no master
/// key, so its password can never unlock the CA. Only minted for a server
/// CA (a role admin authenticates to the daemon).
pub async fn setup_superuser(
    ans: &mut dyn Answerer,
    cadir: &ca_store::CaDir,
    opts: &NewCaOpts,
    cn: &str,
) -> Result<()> {
    let name = ans
        .text(
            Field::AdminName,
            opts.admin.clone(),
            enroll::current_username().as_deref(),
            // Not a required-explicit decision: the founding admin defaults to
            // the installing OS user (the interactive flow's default too), so a
            // non-interactive install without `--admin-*` still completes.
            false,
        )
        .await?
        .context("superuser name required (no current OS username to default to)")?;
    if name.trim().is_empty() {
        bail!("superuser name must not be empty");
    }
    if ca_vault::is_reserved_admin(&name) {
        bail!(
            "{name:?} is a reserved signing-slot name; choose another for the superuser"
        );
    }
    let mut policy = gather_policy(
        ans,
        PolicyInputs {
            allow_san: &opts.allowed_san,
            max_validity: opts.max_validity,
            id_map_groups: &opts.id_map_groups,
            may_enroll_servers: opts.may_enroll_servers,
            // The superuser always manages admins — pass it so gather_policy
            // doesn't ask (rather than forcing it after the fact).
            may_manage_admins: Some(true),
            perms_scope: &[],
            service_scope: &[],
        },
        // The superuser founds the network, so may-enroll defaults to yes.
        true,
        cn,
        opts.domain.as_deref(),
    )
    .await?;
    // What makes it the superuser: authority over the whole tree (perms +
    // service control). (Issuance scope, may-enroll, and admin-management
    // came from gather_policy above.)
    policy.perms_edit_scopes = vec!["/".to_string()];
    policy.service_control_scopes = vec!["/".to_string()];
    let mut secret = ans.secret(Field::AdminPassword, None).await?;
    let pw = Zeroizing::new(std::mem::take(&mut secret.0));
    cadir.vault.write().add_role_slot(&name, &pw, policy)?;
    ans.note(&format_compact!(
        "superuser role admin {name:?} created — it manages admins, edits perms, \
         and enrolls servers, but never unlocks the CA key (the server signs)."
    ));
    Ok(())
}

/// The default CA common name for a domain, following the same
/// `<name>.<domain>` convention as every other netidx identity — the CA
/// is just the `ca` node (e.g. `ryu-oh.org` → `ca.ryu-oh.org`).
pub fn default_ca_cn(domain: &str) -> String {
    format!("ca.{domain}")
}

/// Resolve the CA common name: an explicit value wins; otherwise prompt,
/// defaulting to `ca.<domain>` when a domain is known, or requiring an
/// explicit answer when it isn't.
pub async fn resolve_ca_cn(
    ans: &mut dyn Answerer,
    provided: Option<String>,
    domain: Option<&str>,
) -> Result<String> {
    let cn = match domain {
        Some(d) if !d.is_empty() => {
            let default = default_ca_cn(d);
            ans.text(Field::CaCommonName, provided, Some(&default), true).await?
        }
        _ => ans.text(Field::CaCommonName, provided, None, true).await?,
    };
    cn.context("CA common name required")
}

/// The DNS SAN on an existing CA's own cert, used to seed the policy
/// suggestion when scoping admins on an already-built CA (`admin add` /
/// `admin set-policy`). Empty if it can't be read — the prompt then has
/// no domain to suggest.
pub fn existing_ca_cn(dir: &Path) -> String {
    tls::extract_dns_san_from_pem(&dir.join("certificate.pem")).unwrap_or_default()
}

/// The `*.<domain>` SAN suggestion, from an explicit domain or by stripping the
/// CA CN's leftmost label; `*` when neither yields a domain. Prefer an explicit
/// domain (e.g. threaded from the resolver install, which already asked for
/// it) — `*.<domain>` matches the `<user>.<domain>` SAN convention exactly.
fn san_suggestion(cn: &str, domain: Option<&str>) -> String {
    match domain {
        Some(d) if !d.is_empty() => format!("*.{d}"),
        _ => match cn.split_once('.') {
            Some((_, d)) if !d.is_empty() => format!("*.{d}"),
            _ => "*".to_string(),
        },
    }
}

/// The raw policy knobs an admin collects before building a [`Policy`]. The
/// booleans route through the answerer (so strict mode requires them
/// explicitly, while an interactive frontend gets the supplied default); the
/// scope lists are typed inputs taken straight from flags — a signing admin
/// gets perms/service scopes only when explicitly granted.
pub struct PolicyInputs<'a> {
    /// SAN globs this admin may issue (empty ⇒ answerer suggests `*.<domain>`).
    pub allow_san: &'a [String],
    /// Max validity this admin may issue.
    pub max_validity: Duration,
    /// id-map groups this admin may assign (empty ⇒ answerer default `users`).
    pub id_map_groups: &'a [String],
    /// Whether this admin may enroll new admin servers.
    pub may_enroll_servers: Option<bool>,
    /// Whether this admin may manage the roster (add / rescope / remove admins).
    pub may_manage_admins: Option<bool>,
    /// Netidx paths this admin may edit perms under.
    pub perms_scope: &'a [String],
    /// Netidx paths this admin may control services under.
    pub service_scope: &'a [String],
}

/// Assemble a [`Policy`](ca_vault::Policy) from the flag-supplied
/// [`PolicyInputs`], asking the answerer for any knob not supplied by a flag.
/// `cn`/`domain` seed the `*.<domain>` SAN suggestion. `enroll_default` is the
/// interactive default for the may-enroll-servers confirm (strict mode ignores
/// it and requires the flag): the founding superuser founds the network, so it
/// defaults to yes; an added admin defaults to no.
///
/// Shared by the founding-superuser setup ([`setup_superuser`]) and the remote
/// `ca admin add-role` / `set-policy` actions, so the two cannot drift.
pub async fn gather_policy(
    ans: &mut dyn Answerer,
    inputs: PolicyInputs<'_>,
    enroll_default: bool,
    cn: &str,
    domain: Option<&str>,
) -> Result<ca_vault::Policy> {
    let allowed_san = if !inputs.allow_san.is_empty() {
        inputs.allow_san.to_vec()
    } else {
        // The issuance scope is as security-relevant as the may-enroll /
        // may-manage grants below, so strict mode must require it explicitly
        // rather than silently granting the `*.<domain>` suggestion (which now
        // only pre-fills the interactive frontends). `required = true`.
        let suggestion = san_suggestion(cn, domain);
        let answer = ans.text(Field::AllowSan, None, Some(&suggestion), true).await?;
        vec![answer.unwrap_or(suggestion)]
    };
    let id_map_groups = if !inputs.id_map_groups.is_empty() {
        // `--id-map-group ''` is the explicit "none" — filter it out so the
        // resulting policy is empty (registration disabled) rather than
        // containing an empty group name.
        inputs
            .id_map_groups
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        // The *allowed set*: which groups this admin may assign when enrolling
        // a node (the actual choice happens per-enrollment, in the
        // SignRequest). Blank takes the default; a bare `-` disables
        // registration entirely (this admin's signs never register identities).
        let answer = ans
            .text(Field::IdMapGroups, None, Some("users"), false)
            .await?
            .unwrap_or_else(|| "users".to_string());
        enroll::parse_id_map_answer(&answer)
    };
    let may_enroll_servers = ans
        .confirm(Field::MayEnrollServers, inputs.may_enroll_servers, enroll_default)
        .await?;
    let may_manage_admins =
        ans.confirm(Field::MayManageAdmins, inputs.may_manage_admins, false).await?;
    let trim = |scopes: &[String]| -> Vec<String> {
        scopes.iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
    };
    Ok(ca_vault::Policy {
        allowed_san,
        max_validity: inputs.max_validity,
        id_map_groups,
        may_enroll_servers,
        perms_edit_scopes: trim(inputs.perms_scope),
        may_manage_admins,
        service_control_scopes: trim(inputs.service_scope),
    })
}

/// True if the default CA location holds a usable CA — both the cert
/// and the private key. (A cert with no key is a trust anchor we
/// imported, not a CA we can sign with.)
pub fn default_ca_present() -> bool {
    match paths::user_ca_dir() {
        Ok(dir) => {
            // A CA exists if its cert is present and *either* a vault
            // (the current format) or a legacy unencrypted/encrypted
            // `private.key` is alongside it.
            dir.join("certificate.pem").is_file()
                && (ca_vault::CAVault::exists(&dir) || dir.join("private.key").is_file())
        }
        Err(_) => false,
    }
}

/// Issue an identity (CN = SAN-DNS = `name`) from `ca` into `out_dir`.
/// Returns the issued file paths. The caller chooses `out_dir`: the
/// install flow issues into a staging dir and lets `apply()` copy the
/// result into the canonical location, so nothing under the config
/// tree is touched until the apply phase.
///
/// `password = Some(p)` encrypts the on-disk private key with `p`
/// (PKCS#8 + AES-256-CBC). `None` writes an unencrypted key.
pub fn issue_identity(
    ca: &Ca,
    name: &str,
    out_dir: PathBuf,
    password: Option<&str>,
) -> Result<IssuedFiles> {
    issue_identity_into(ca, name, out_dir, ca::DEFAULT_KEY_BITS, password)
}

/// Inner form of [`issue_identity`] with the destination directory
/// and key size as parameters — lets tests issue into a tempdir with
/// a fast key.
pub fn issue_identity_into(
    ca: &Ca,
    name: &str,
    out_dir: PathBuf,
    key_bits: u32,
    password: Option<&str>,
) -> Result<IssuedFiles> {
    offline_ca::issue_and_record(
        ca,
        NodeKind::Client,
        IssueParams {
            subject: Subject::cn(name),
            // Exactly one DNS SAN, matching the CN — that's what the
            // netidx TLS validator requires of a member-server cert.
            san: vec![SanEntry::Dns(name.to_string())],
            key_bits,
            validity: ca::DEFAULT_LEAF_VALIDITY,
            out_dir,
            password: password.map(|s| s.to_string()),
            serial: 0, // assigned by issue_and_record
        },
    )
    .with_context(|| format!("issuing certificate for {name}"))
}
