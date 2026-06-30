use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand};
use netidx_conf::{
    atomic,
    ca::{self, Ca, CaParams, IssueParams, IssuedFiles, SanEntry, Subject},
    ca_vault, conf_client, conf_local,
    conf_proto::{self, NodeKind},
    fingerprint::{ColorMode, Fingerprint},
    paths, tls,
};
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};
use zeroize::Zeroizing;

use super::{init, prompt, service};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// create a new local CA (keyslot vault; can serve via `conf server`)
    Init(InitParams),
    /// issue a leaf certificate from a CA
    Issue(IssueArgs),
    /// sign an externally-supplied CSR file with a local CA
    Sign(SignArgs),
    /// work the enrollment queue: approve or deny pending requests
    Approve(ApproveArgs),
    /// list local CAs
    List,
    /// manage CA admin keyslots (add / revoke / set-policy / list)
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
    /// show the CA's fingerprint + identicon for out-of-band verification
    Fingerprint(FingerprintArgs),
    /// revoke certificates by name (or serial) and re-sign the CRL
    Revoke(RevokeArgs),
    /// set up or rotate the auto-approve slot, so the running conf server
    /// approves verified renewals in-process (no human per renewal)
    AutoApprove(AutoApproveArgs),
    /// manage the off-box recovery credential (rotate it on the CA box)
    Recovery {
        #[command(subcommand)]
        cmd: RecoveryCmd,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum RecoveryCmd {
    /// mint a fresh recovery password on the CA box (authorized by the
    /// box's autorenew keytab, so a lost recovery password is recoverable
    /// while the machine lives). The new password is printed once.
    Rotate(RecoveryRotateArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RecoveryRotateArgs {
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct AutoApproveArgs {
    /// Rotate the auto-approve slot: revoke the old keyslot, mint a new
    /// long random password, and rewrite the keytab — the one-command
    /// response to a leaked keytab. Restart the conf server afterwards to
    /// pick up the new credential.
    #[arg(long)]
    pub rotate: bool,
    /// Proceed even when this host has no usable TPM / Secure Enclave.
    /// DANGER: the autorenew keytab is then written in PLAINTEXT — every
    /// backup or disk image of this machine becomes a CA compromise.
    #[arg(long = "insecure-no-tpm")]
    pub insecure_no_tpm: bool,
}

#[derive(Args, Debug)]
pub(crate) struct ApproveArgs {
    /// Conf server whose enrollment queue to work. Defaults to this
    /// host's own conf server, then mDNS discovery — so an enrollment
    /// admin can approve from their workstation without shell access to
    /// the CA host.
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// CA dir, used only to verify the conf server's identity against the
    /// local CA cert when one is present.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct RevokeArgs {
    /// The identity to revoke — every live certificate carrying this
    /// name. Interactive list when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// Revoke a single certificate by serial number instead.
    #[arg(long, conflicts_with = "name")]
    pub serial: Option<u64>,
    /// Revocation reason, recorded in the index. Prompted when omitted.
    #[arg(long)]
    pub reason: Option<String>,
    /// Conf server to revoke through. Defaults to this host's own conf
    /// server, else discovered on the local network.
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// CA dir, used only to verify the conf server's identity against the
    /// local CA cert when one is present.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCmd {
    /// add an admin keyslot (a new password that can sign)
    Add(AdminAddArgs),
    /// add a role keyslot: authenticates and may edit perms in scope, but
    /// can NEVER unlock the CA key or sign certs
    AddRole(AdminAddRoleArgs),
    /// revoke an admin keyslot
    Remove(AdminRemoveArgs),
    /// replace an admin's issuance policy (allowed SANs / max validity)
    SetPolicy(AdminSetPolicyArgs),
    /// list admin keyslots and their issuance policy
    List(AdminScopeArgs),
}

#[derive(Args, Debug)]
pub(crate) struct AdminScopeArgs {
    /// Manage admins on a REMOTE CA over the conf plane (instead of the
    /// local vault). Authenticates as a `may_manage_admins` role admin;
    /// the operator confirms the CA's fingerprint before any password.
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// Override the CA directory. Defaults to `${basedir}/ca/`.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct AdminAddArgs {
    /// Name of the new admin. Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// SAN glob this admin may issue (repeatable). Prompted when omitted.
    #[arg(long = "allow-san", num_args = 1)]
    pub allow_san: Vec<String>,
    /// Max validity this admin may issue (e.g. 730d, 10m). Default 730d.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub max_validity: Duration,
    /// id-map groups for identities signed by this admin (repeatable;
    /// first is primary). Prompted when omitted; an explicit empty
    /// string disables registration.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// Whether this admin may enroll new conf servers. Prompted when
    /// omitted (default no for added admins).
    #[arg(long)]
    pub may_enroll_servers: Option<bool>,
    /// Netidx path this (signing) admin may also edit perms under
    /// (repeatable, e.g. /eu). Empty unless granted. For a perms-only
    /// admin use `admin add-role` instead.
    #[arg(long = "perms-scope", num_args = 1)]
    pub perms_scope: Vec<String>,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct AdminAddRoleArgs {
    /// Name of the new role admin. Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// SAN glob the server may sign on this role's behalf (repeatable).
    /// Prompted when omitted; empty for a role that issues nothing.
    #[arg(long = "allow-san", num_args = 1)]
    pub allow_san: Vec<String>,
    /// Max validity this role may issue (e.g. 730d, 10m). Default 730d.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub max_validity: Duration,
    /// id-map groups this role may assign when enrolling (repeatable;
    /// first is primary). Prompted when omitted; empty disables it.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// Whether this role may enroll new conf servers. Prompted when omitted.
    #[arg(long)]
    pub may_enroll_servers: Option<bool>,
    /// Netidx path this role may edit perms under (repeatable, e.g. /eu,
    /// or / for the whole tree). Prompted when omitted.
    #[arg(long = "perms-scope", num_args = 1)]
    pub perms_scope: Vec<String>,
    /// Netidx path this role may control services under (restart/start/stop
    /// the activation units of the cluster serving that path; repeatable).
    #[arg(long = "service-scope", num_args = 1)]
    pub service_scope: Vec<String>,
    /// Mint the role admin on a REMOTE CA over the conf plane. The CA
    /// enforces no-escalation (you may only grant ⊆ your own authority).
    #[arg(long)]
    pub server: Option<SocketAddr>,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct AdminSetPolicyArgs {
    /// Name of the admin whose policy to replace. Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// SAN glob this admin may issue (repeatable). Replaces the existing
    /// list. Prompted when omitted, defaulting to `*.<ca-domain>`.
    #[arg(long = "allow-san", num_args = 1)]
    pub allow_san: Vec<String>,
    /// Max validity this admin may issue (e.g. 730d, 10m). Default 730d.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub max_validity: Duration,
    /// id-map groups for identities signed by this admin (repeatable;
    /// first is primary). Prompted when omitted; an explicit empty
    /// string disables registration.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// Whether this admin may enroll new conf servers. Prompted when
    /// omitted.
    #[arg(long)]
    pub may_enroll_servers: Option<bool>,
    /// Netidx path this admin may edit perms under (repeatable). Replaces
    /// the existing list.
    #[arg(long = "perms-scope", num_args = 1)]
    pub perms_scope: Vec<String>,
    /// Netidx path this admin may control services under (repeatable).
    /// Replaces the existing list.
    #[arg(long = "service-scope", num_args = 1)]
    pub service_scope: Vec<String>,
    /// Rescope a role admin on a REMOTE CA over the conf plane (CA enforces
    /// no-escalation).
    #[arg(long)]
    pub server: Option<SocketAddr>,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct AdminRemoveArgs {
    /// Name of the admin to revoke. Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// Allow removing the last admin (locks the CA permanently).
    #[arg(long)]
    pub force: bool,
    /// Remove a role admin on a REMOTE CA over the conf plane.
    #[arg(long)]
    pub server: Option<SocketAddr>,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct FingerprintArgs {
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct JoinArgs {
    /// Conf server address (`ip:port`). When omitted, discovered over
    /// mDNS (with a manual-address fallback prompt).
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// The TLS identity name to request (one DNS SAN). Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// The admin name to authenticate as. Prompted when omitted.
    #[arg(long)]
    pub admin: Option<String>,
    /// Validity to request (e.g. 730d, 10m). Default 730d (capped by server policy).
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub validity: Duration,
    /// id-map groups to register the identity with (repeatable; first
    /// is primary). Prompted when omitted; an explicit empty string
    /// skips registration.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
}

#[derive(Args, Debug)]
pub(crate) struct InitParams {
    /// Common Name on the CA cert. Prompted for when stdin is a TTY
    /// and this flag is omitted; the prompt defaults to `ca.<domain>`
    /// when `--domain` is given.
    #[arg(long)]
    pub cn: Option<String>,
    /// TLS domain this CA serves (e.g. `ryu-oh.org`). Seeds the CN
    /// default (`ca.<domain>`) and the first admin's issuance policy
    /// suggestion (`*.<domain>`). Optional — a bare CA without it is
    /// unchanged.
    #[arg(long)]
    pub domain: Option<String>,
    #[arg(long)]
    pub country: Option<String>,
    #[arg(long)]
    pub state: Option<String>,
    #[arg(long)]
    pub locality: Option<String>,
    #[arg(short = 'O', long)]
    pub organization: Option<String>,
    /// SubjectAltName entry. Repeatable. Form: `dns:<name>`,
    /// `ip:<addr>`, `uri:<uri>`, or `email:<addr>`. Defaults to a
    /// single `dns:<cn>` if not given.
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    #[arg(long, default_value = "4096")]
    pub key_bits: u32,
    /// Validity for the CA cert itself. Default 7300d (20 years).
    #[arg(long, value_parser = humantime::parse_duration, default_value = "7300d")]
    pub ca_validity: Duration,
    /// Default validity for certs this CA issues (e.g. the conf server's
    /// serving cert). Default 730d.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub leaf_validity: Duration,
    /// Renew the CA cert once its remaining lifetime drops below this.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "820d")]
    pub ca_renew_threshold: Duration,
    /// The superuser (role) admin's name. This is the founding admin who
    /// can mint other admins, edit perms, and enroll servers — but never
    /// unlocks the CA key (the server signs on its behalf). Prompted when
    /// omitted, defaulting to the current unix user.
    #[arg(long)]
    pub admin: Option<String>,
    /// SAN glob the superuser may have the server sign (repeatable).
    /// Prompted when omitted — e.g. `*.example.com`.
    #[arg(long = "allow-san", num_args = 1)]
    pub allow_san: Vec<String>,
    /// Max validity the superuser may issue (e.g. 730d, 10m). Default 730d.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub max_validity: Duration,
    /// id-map groups the superuser may assign when enrolling
    /// (repeatable; first is primary). Prompted when omitted.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// Whether the superuser may enroll new conf servers. Prompted
    /// when omitted (default yes for the founding admin).
    #[arg(long)]
    pub may_enroll_servers: Option<bool>,
    /// Proceed even when this host has no usable TPM / Secure Enclave.
    /// DANGER: the autorenew credential is then written in PLAINTEXT, so
    /// every backup or disk image of this machine is a CA compromise. Test
    /// CAs only.
    #[arg(long = "insecure-no-tpm")]
    pub insecure_no_tpm: bool,
    /// Set up the CA server (issue a serving cert + write server.json)
    /// without prompting. By default `ca init` asks.
    #[arg(long)]
    pub with_server: bool,
    /// Skip the CA-server setup entirely (offline CA only).
    #[arg(long, conflicts_with = "with_server")]
    pub no_server: bool,
    /// Address the CA server should listen on when set up. Default
    /// `0.0.0.0:<ca-port>`.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Where to drop the CA server's activation unit. Defaults to the
    /// user activation dir (same place the resolver/id-map units go).
    #[arg(long = "units-dir")]
    pub units_dir: Option<PathBuf>,
    /// After setting up the CA server, also register netidx as an OS
    /// service without prompting. Mutually exclusive with
    /// `--no-service`.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    pub with_service: bool,
    /// Skip the OS-service prompt after setting up the CA server.
    #[arg(long = "no-service")]
    pub no_service: bool,
    /// Override the directory the CA is created in. Defaults to
    /// `${basedir}/ca/` — one CA per netidx install.
    #[arg(long)]
    pub dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct IssueArgs {
    /// Common Name for the issued cert. Prompted when omitted.
    #[arg(long)]
    pub cn: Option<String>,
    #[arg(long)]
    pub country: Option<String>,
    #[arg(long)]
    pub state: Option<String>,
    #[arg(long)]
    pub locality: Option<String>,
    #[arg(short = 'O', long)]
    pub organization: Option<String>,
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    #[arg(long, default_value = "4096")]
    pub key_bits: u32,
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub validity: Duration,
    /// Override the CA's directory. Defaults to `${basedir}/ca/`.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    /// Where to write the issued `private.key` + `certificate.pem`.
    /// Prompted when omitted.
    #[arg(short, long = "out")]
    pub out_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct RequestArgs {
    /// Common Name for the requested cert. Prompted when omitted.
    #[arg(long)]
    pub cn: Option<String>,
    #[arg(long)]
    pub country: Option<String>,
    #[arg(long)]
    pub state: Option<String>,
    #[arg(long)]
    pub locality: Option<String>,
    #[arg(short = 'O', long)]
    pub organization: Option<String>,
    /// SubjectAltName entry. Repeatable. Form: `dns:<name>`,
    /// `ip:<addr>`, `uri:<uri>`, or `email:<addr>`. Defaults to a
    /// single `dns:<cn>` if not given.
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    #[arg(long, default_value = "4096")]
    pub key_bits: u32,
    /// Output path for the generated private key (mode 0600).
    /// Defaults to `./private.key`; the default path refuses to
    /// overwrite an existing file (an explicit `--out-key` does not).
    #[arg(long)]
    pub out_key: Option<PathBuf>,
    /// Output path for the generated CSR (mode 0644). Defaults to
    /// `./<cn>.csr`.
    #[arg(long)]
    pub out_csr: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct SignArgs {
    /// Path to the CSR (PEM-encoded) to sign. Prompted when omitted.
    /// (To approve queued enrollment requests instead of signing a CSR
    /// file, use `netidx conf ca approve`.)
    pub csr_path: Option<PathBuf>,
    /// SubjectAltName entry to embed in the signed cert. Repeatable.
    /// The CA is authoritative — these override whatever the CSR
    /// claims. One of `--san` or `--accept-csr-san` must be passed:
    /// the CLI deliberately does not silently inherit SAN from the
    /// CSR, since an absent-minded admin signing whatever was
    /// requested is the most likely failure mode of a CA tool.
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    /// Accept the CSR's embedded SAN as-is. The summary is still
    /// printed before signing; this flag just makes the
    /// inherit-from-CSR decision explicit rather than implicit.
    #[arg(long)]
    pub accept_csr_san: bool,
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub validity: Duration,
    /// Override the CA's directory. Defaults to `${basedir}/ca/`.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    /// Where to write the signed certificate (mode 0644). Defaults
    /// to `./<csr-cn>.pem` (or `./certificate.pem` if the CSR has no
    /// CN).
    #[arg(short, long)]
    pub out: Option<PathBuf>,
    /// Skip the post-sign id-map registration prompt. The default on
    /// a TTY (when a local id-map exists) is to prompt for groups
    /// and add the identity to the map; this flag suppresses that
    /// entirely. Non-TTY callers already skip the prompt by default,
    /// so this is mostly useful for interactive sessions where you
    /// want to handle id-map registration separately (or not at
    /// all).
    #[arg(long)]
    pub no_id_map: bool,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Init(p) => init(p),
        Cmd::Issue(p) => issue(p),
        Cmd::Sign(p) => sign(p),
        Cmd::Approve(p) => approve(p),
        Cmd::List => list(),
        Cmd::Admin { cmd } => admin(cmd),
        Cmd::Fingerprint(p) => fingerprint(p),
        Cmd::Revoke(p) => revoke(p),
        Cmd::AutoApprove(p) => auto_approve(p),
        Cmd::Recovery { cmd } => recovery(cmd),
    }
}

// -- ca auto-approve ----------------------------------------------------------

/// The dedicated autorenew slot: a name nobody types and an
/// empty-scope policy — over the wire its password can approve
/// verified renewals and *nothing else* (no SANs, no groups, no
/// enrollment). The narrow slot, not the daemon, is what bounds the
/// blast radius of a leaked keytab; the keytab itself lives outside
/// the CA dir so CA-dir backups stay harmless on their own, and
/// `--rotate` is the one-command kill-and-replace.
pub(super) const AUTORENEW_ADMIN: &str = netidx_conf::conf_server::AUTORENEW_ADMIN;

fn autorenew_policy() -> ca_vault::Policy {
    netidx_conf::ca_policy::autorenew_policy()
}

/// `${config}/netidx/autorenew.keytab` — deliberately NOT in the CA
/// dir: never back this file up; recreating it is one `--rotate`.
fn autorenew_keytab_path() -> Result<PathBuf> {
    Ok(paths::user_config_root()?.join("autorenew.keytab"))
}

/// A long random password for the autorenew slot (256 bits, hex). The
/// autorenew slot is a master-key-wrapping signing credential, so keep its
/// plaintext in a `Zeroizing` buffer that wipes on drop (it is dropped right
/// after sealing / writing the keytab).
fn random_password() -> Zeroizing<String> {
    netidx_conf::ca_vault::random_signing_password()
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
pub(super) fn setup_autorenew_slot(
    cadir: &netidx_conf::ca_store::CaDir,
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
    let password = random_password();
    cadir.vault.write().add_signing_slot(
        recovery_password,
        AUTORENEW_ADMIN,
        &password,
        autorenew_policy(),
    )?;
    let keytab = autorenew_keytab_path()?;
    // `available()` (which the caller's TPM gate checked) only proves the
    // device opened, NOT that a seal will succeed — a present-but-locked or
    // busy TPM fails here. The seal decision is the real one: a plaintext
    // keytab is a master-key-equivalent credential, so falling back to it
    // silently would defeat the whole gate. Only `--insecure-no-tpm` accepts
    // that, and then loudly; otherwise we refuse and roll the slot back.
    match netidx_tpm::seal(password.as_bytes()) {
        Ok(blob) => {
            atomic::write_atomic(&keytab, &blob, 0o600)?;
            println!(
                "  the keytab is sealed to this machine's {} — copied \
                 anywhere else (disk image, backup) it is useless",
                netidx_tpm::MECHANISM
            );
        }
        Err(e) if insecure_no_tpm => {
            atomic::write_atomic(&keytab, password.as_bytes(), 0o600)?;
            eprintln!("================================================================");
            eprintln!(
                "WARNING: the autorenew keytab is PLAINTEXT ({} sealing failed: {e:#}).",
                netidx_tpm::MECHANISM
            );
            eprintln!(
                "Any backup or disk image of this machine now contains a credential"
            );
            eprintln!(
                "that unlocks the CA key. You accepted this with --insecure-no-tpm."
            );
            eprintln!("================================================================");
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
                 dictionary-attack lockout) and re-run `netidx conf ca auto-approve`, or \
                 pass --insecure-no-tpm to accept a plaintext keytab (test CAs only).",
                mech = netidx_tpm::MECHANISM
            );
        }
    }
    Ok(keytab)
}

/// Set up (or rotate) the autorenew slot and point this host's
/// conf-server config at its keytab. Approval itself is the running
/// daemon's job now — it reads the keytab named here and approves
/// verified renewals in-process — so this command just manages the
/// credential. `--rotate` is the same operation framed as a leaked-keytab
/// response: [`setup_autorenew_slot`] always replaces the slot, so enable
/// and rotate share one path and differ only in what they print.
fn auto_approve(p: AutoApproveArgs) -> Result<()> {
    env_logger::init();
    let dir = ca_dir_for(None)?;
    let cfg_path = paths::discover_conf_server_config().ok();
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    // Hot-swap path: the running daemon owns the CA, so it rotates the box
    // credential in-process (re-wraps the slot, swaps the live key, rewrites
    // the keytab) with no downtime — no flock, no recovery password.
    if let Some(cfg) = &cfg_path
        && rt.block_on(conf_local::daemon_running(cfg))
    {
        let warning = rt.block_on(conf_local::rotate_autorenew(cfg))?;
        println!(
            "auto-approve rotated (hot-swapped on the running conf server, no downtime)"
        );
        if let Some(w) = warning {
            eprintln!("WARNING: {w}");
        }
        return Ok(());
    }
    // Offline / first-time setup: the daemon is down, so take the flock and
    // mint the slot directly. Same gate as init: refuse on a TPM-less host
    // unless the operator opts into a plaintext keytab. (Re-minting the box
    // credential here is exactly the moment a plaintext fallback would leak
    // it.)
    tpm_gate(p.insecure_no_tpm)?;
    // Re-minting autorenew removes the old slot first, so the recovery
    // password (not the old keytab) is what authorizes it — and after a TPM
    // clear the old keytab is unsealable anyway, so recovery is the only way
    // in. Normalize a re-typed copy (spaces/case/confusables fold away).
    let typed = Zeroizing::new(collect_existing_password(
        "the CA recovery password (printed once at init; authorizes re-minting the \
         autorenew credential)",
    )?);
    let recovery = ca_vault::normalize_recovery_password(&typed);
    let cadir = netidx_conf::ca_store::CaDir::open(&dir).context(
        "setting up autorenew needs exclusive access; the conf server must be stopped",
    )?;
    let keytab = setup_autorenew_slot(&cadir, &recovery, p.insecure_no_tpm)?;
    let verb = if p.rotate { "rotated" } else { "enabled" };
    println!("auto-approve {verb}:");
    println!("  slot:   {AUTORENEW_ADMIN:?} (empty issuance scope)");
    println!("  keytab: {} (0600 — do NOT back this file up)", keytab.display());
    // Rotation rewrote the keytab's contents but not its path, so pointing
    // the config at it is correct whether we just created or replaced it.
    match super::server::set_ca_autorenew(&keytab) {
        Ok(cfg_path) => {
            println!("  config: {} (roles.ca.autorenew)", cfg_path.display());
            println!("  restart the conf server to pick up the keytab.");
        }
        Err(e) => {
            println!("  note: could not update the conf-server config ({e:#}).");
            println!("        set roles.ca.autorenew to the keytab path and");
            println!("        restart the conf server.");
        }
    }
    Ok(())
}

// -- ca revoke ----------------------------------------------------------------

/// Revoke certificates and re-sign the CRL — over RPC. The daemon owns
/// the CA: it holds the issuance index, signs the CRL, and serves it
/// (`GetCrl`). The CLI is a pure client — it lists the issued set
/// (`ListIssued`), lets the admin pick by name/serial/interactively, and
/// sends `Revoke`. By name is the common case; the daemon's list maps
/// names to serials so the admin never hunts serial numbers.
///
/// CR claude for estokes: the old local-file revoke also (a) copied the
/// fresh CRL beside this host's resolver for instant enforcement and (b)
/// offered to drop the revoked identity from the local id-map. Both
/// needed direct file access the CLI no longer has now the daemon owns
/// the CA. The CRL still re-signs (in the daemon) and distributes via
/// `GetCrl`; the local-resolver fast-path and the id-map cleanup are
/// dropped here. If we want them back they belong in the daemon's
/// `handle_revoke` (it has the resolver role config and can reach id-map
/// peers) — flagging the behavior change rather than hiding it.
fn revoke(p: RevokeArgs) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    // Find the CA: an explicit `--server`, else this host's own conf
    // server, else discover one on the network (glyph-confirmed).
    let (server, known_identity) = match p.server.or_else(local_conf_server_listen) {
        Some(s) => (s, None),
        None => match init::discover_network(NodeKind::Client)? {
            init::ConfServers::Have(net) => {
                let ca = net.info.ca_addr.ok_or_else(|| {
                    anyhow!("network {:?} reported no CA; cannot revoke", net.info.domain)
                })?;
                (ca, Some(net.identity))
            }
            init::ConfServers::DontHave => {
                bail!("no conf server found or selected; pass --server")
            }
            init::ConfServers::NotProbed => {
                bail!("--server is required when stdin is not a TTY")
            }
        },
    };
    // Confirm who we're talking to. If this host holds the CA cert, verify
    // silently against it; otherwise glyph-confirm with the operator.
    let identity = match known_identity {
        Some(identity) => identity,
        None => {
            let identity = rt
                .block_on(conf_client::fetch_identity(server, NodeKind::Client))
                .with_context(|| format!("contacting conf server {server}"))?;
            let local_fp = ca_dir_for(p.ca_dir.clone())
                .ok()
                .and_then(|d| std::fs::read(d.join("certificate.pem")).ok())
                .and_then(|pem| Fingerprint::of_cert_pem(&pem).ok());
            match local_fp {
                Some(fp) if fp == identity.fingerprint => {
                    println!("verified {server} against the local CA");
                }
                _ => {
                    init::show_network_identity(server, &identity);
                    if !prompt::confirm(
                        "does this match what your CA admin gave you?",
                        false,
                    )? {
                        bail!("CA identity was not confirmed; nothing was sent");
                    }
                }
            }
            identity
        }
    };
    let admin = match env_user_name() {
        Some(user) => prompt::string_with_default("admin name", None, &user)?,
        None => prompt::required_string("admin name", None)?,
    };
    let password = Zeroizing::new(collect_existing_password(&format!(
        "CA password for admin {admin:?}"
    ))?);
    // The daemon owns the index — read the issued set from it.
    let entries = rt.block_on(conf_client::list_issued(
        server,
        &admin,
        password.as_str(),
        &identity,
    ))?;
    let live: Vec<&conf_proto::IssuedEntry> =
        entries.iter().filter(|e| !e.revoked).collect();
    if live.is_empty() {
        println!("no live certificates in the index — nothing to revoke");
        return Ok(());
    }
    let targets: Vec<&conf_proto::IssuedEntry> = if let Some(serial) = p.serial {
        let t: Vec<_> = live.iter().copied().filter(|e| e.serial == serial).collect();
        if t.is_empty() {
            bail!("no live certificate with serial {serial}");
        }
        t
    } else {
        let name = match p.name {
            Some(n) => n,
            None => {
                // Distinct names, most recently issued first.
                let mut names: Vec<&str> = Vec::new();
                for e in live.iter().rev() {
                    if !e.name.is_empty()
                        && !names.iter().any(|n| n.eq_ignore_ascii_case(&e.name))
                    {
                        names.push(&e.name);
                    }
                }
                if names.is_empty() {
                    bail!(
                        "live certificates exist but none carry a DNS name; \
                         revoke by --serial"
                    );
                }
                println!("live identities:");
                for (i, n) in names.iter().enumerate() {
                    let count =
                        live.iter().filter(|e| e.name.eq_ignore_ascii_case(n)).count();
                    println!("  {}) {n}  ({count} cert(s))", i + 1);
                }
                let answer = prompt::required_string(
                    "identity # to revoke (or 'q' to quit)",
                    None,
                )?;
                if answer.eq_ignore_ascii_case("q") {
                    return Ok(());
                }
                match answer.parse::<usize>() {
                    Ok(i) if (1..=names.len()).contains(&i) => names[i - 1].to_string(),
                    _ => bail!("enter a number between 1 and {}", names.len()),
                }
            }
        };
        let t: Vec<_> =
            live.iter().copied().filter(|e| e.name.eq_ignore_ascii_case(&name)).collect();
        if t.is_empty() {
            bail!("no live certificate for {name:?}");
        }
        t
    };
    // Show what's about to die — including the identity glyph the
    // enrollment showed, so the admin can sanity-check it's the right
    // entity.
    println!();
    for t in &targets {
        println!(
            "  serial {}  {}",
            t.serial,
            if t.name.is_empty() { "(no name)" } else { &t.name },
        );
        if let Ok(fp) = Fingerprint::parse_text(&t.spki_fp) {
            println!("  SHA256  {}", fp.text());
            println!("{}", fp.identicon(ColorMode::detect()));
        }
    }
    if !prompt::confirm(
        &format!("revoke {} certificate(s)? This cannot be undone", targets.len()),
        false,
    )? {
        bail!("revocation aborted; nothing was changed");
    }
    let reason = match p.reason {
        Some(r) => r,
        None => prompt::string_with_default(
            "revocation reason (recorded in the index)",
            None,
            "unspecified",
        )?,
    };
    let serials: Vec<u64> = targets.iter().map(|t| t.serial).collect();
    let warnings = rt.block_on(conf_client::revoke(
        server,
        &admin,
        password.as_str(),
        serials,
        &reason,
        &identity,
    ))?;
    println!("revoked {} certificate(s); the daemon re-signed the CRL", targets.len());
    for w in warnings {
        println!("  warning: {w}");
    }
    Ok(())
}

fn ca_dir_for(override_: Option<PathBuf>) -> Result<PathBuf> {
    match override_ {
        Some(p) => Ok(p),
        None => paths::user_ca_dir(),
    }
}

/// Inputs to [`create_vaulted_ca`], the single new-CA entry point.
/// Fields are primitive so callers (the `ca init` command *and* the
/// resolver install) don't need the engine's `Subject` / `SanEntry`
/// types — `create_vaulted_ca` builds those internally.
pub(super) struct NewCaOpts {
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
    /// current unix user. Created only when the conf server is set up
    /// (a role admin authenticates to the daemon; an offline CA has none).
    pub admin: Option<String>,
    /// Superuser's server-signing scope globs; empty ⇒ prompt (default
    /// `*.<domain>` when `domain` is set, else derived from the CN).
    pub allowed_san: Vec<String>,
    pub max_validity: Duration,
    /// Superuser's id-map groups; empty ⇒ prompt (default `users`).
    pub id_map_groups: Vec<String>,
    /// Whether the superuser may enroll conf servers; `None` ⇒
    /// prompt, defaulting to yes (someone has to be able to grow the
    /// network).
    pub may_enroll_servers: Option<bool>,
    /// Proceed without a TPM / Secure Enclave (autorenew keytab written
    /// in plaintext). A loud warning is printed; test CAs only.
    pub insecure_no_tpm: bool,
    /// `None` ⇒ prompt "set up the conf server?"; `Some(b)` ⇒ forced.
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

/// **The** entry point for building a new vaulted CA, shared verbatim
/// by `netidx conf ca init` and the `netidx conf resolver install`
/// "create a new CA" branch — so the operator gets the identical
/// experience (admin/policy, identicon, the "set up the CA server?"
/// question) either way.
///
/// Returns the in-memory signing [`Ca`] (use it to issue certs before
/// it drops — e.g. the resolver issues its own identity from it) and
/// the [`ServiceNeed`](service::ServiceNeed) the caller folds into its
/// single service offer. This function never offers the service itself;
/// that's the caller's end-of-process step, so a resolver install can
/// merge this need with its own and offer once.
pub(super) fn create_vaulted_ca(opts: NewCaOpts) -> Result<(Ca, service::ServiceNeed)> {
    // CN first (matching the prompt order `ca init` had before this was
    // centralized here): an explicit `--cn` / threaded value wins,
    // otherwise prompt with the `ca.<domain>` default when we know the
    // domain.
    let common_name = resolve_ca_cn(opts.common_name.clone(), opts.domain.as_deref())?;
    // The conf-server config wants a concrete domain (it's what the
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
    tpm_gate(opts.insecure_no_tpm)?;
    let san = parse_sans(&opts.san, &common_name)?;

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
    let recovery_pw = ca_vault::gen_recovery_password();
    // One flock for the whole init: the new dir is ours exclusively until
    // every slot is minted. The helpers below borrow this same handle.
    let cadir = netidx_conf::ca_store::CaDir::open(&opts.dir)
        .context("opening the new CA directory")?;
    // Should sealing fail mid-write, roll back the cert + serial
    // `init_vaulted` committed so the dir isn't a keyless half-CA that
    // blocks a clean retry. The in-memory key zeroizes on the way out.
    if let Err(e) = cadir.vault.write().create(
        &key_pem,
        ca_vault::RECOVERY_ADMIN,
        &recovery_pw,
        recovery_policy(),
    ) {
        let _ = std::fs::remove_file(opts.dir.join("certificate.pem"));
        let _ = std::fs::remove_file(opts.dir.join("serial"));
        return Err(e).context("sealing CA key into the vault");
    }
    // Persist the CA's configurable lifetime policy under the same flock.
    ca::CaLifetimes {
        leaf_validity: opts.leaf_validity,
        ca_renew_threshold: opts.ca_renew_threshold,
    }
    .store(&opts.dir)
    .context("writing CA lifetimes")?;

    println!("created a new CA at {}", opts.dir.display());
    print_recovery_password(&recovery_pw);
    show_ca_identity(&opts.dir)?;
    println!();
    println!(
        "Share the fingerprint/identicon above with anyone joining, so they can\n\
         verify they're talking to the real CA before sending a password."
    );

    let set_up_server = match opts.setup_server {
        Some(b) => b,
        None => prompt::confirm(
            "set up the conf server (so nodes can discover the network and \
             request certs over it)?",
            true,
        )?,
    };
    let need = if set_up_server {
        // setup_server signs the serving cert through the offline issuance
        // path, which takes the CA flock itself — so release ours first,
        // then reacquire for the remaining slot setup. During init no daemon
        // competes for the brand-new dir, so the brief unlock is safe; this
        // is the same drop-and-reopen the offline `ca issue`/`sign` paths use.
        drop(cadir);
        let need = super::server::setup_server(super::server::SetupArgs {
            ca_dir: &opts.dir,
            ca: &ca,
            domain: &domain,
            listen: opts.listen,
            listen_hint: opts.listen_hint,
            units_dir: opts.units_dir.as_deref(),
        })?;
        let cadir = netidx_conf::ca_store::CaDir::open(&opts.dir)
            .context("reopening the CA directory after serving-cert setup")?;
        // The box's `autorenew` credential — the only signing key the
        // daemon ever holds, and what it signs on a role admin's behalf
        // with. Mandatory for a server CA. Authorized by the recovery
        // password we just minted; sealed to the TPM (or plaintext under
        // --insecure-no-tpm, which the gate above already warned about).
        let keytab = setup_autorenew_slot(&cadir, &recovery_pw, opts.insecure_no_tpm)?;
        let cfg_path = super::server::set_ca_autorenew(&keytab)?;
        println!("automatic renewal approval enabled:");
        println!("  slot:   {AUTORENEW_ADMIN:?} (empty issuance scope)");
        println!("  keytab: {} (0600 — do NOT back this file up;", keytab.display());
        println!("          rotate anytime with `netidx conf ca auto-approve --rotate`)");
        println!("  config: {} (roles.ca.autorenew)", cfg_path.display());
        // The founding SUPERUSER role admin: it directs the server (mint
        // admins, edit perms, enroll servers) but wraps no MK, so its
        // password can NEVER unlock the CA key — only the server signs.
        setup_superuser(&cadir, &opts, &common_name)?;
        need
    } else {
        // An offline CA has no daemon to sign on anyone's behalf, so it
        // grows no autorenew slot and no role admins: the recovery password
        // is the operator's credential for local `ca issue` / `ca sign`.
        service::ServiceNeed::NONE
    };
    Ok((ca, need))
}

/// The `recovery` signing slot's policy: the same narrow, no-standing-wire-
/// authority shape as the autorenew slot. Its power is being a *signing*
/// slot (it unlocks the key for on-box `ca issue` / break-glass), not any
/// issuance policy — that authority lives in role admins. Empty here keeps a
/// leaked-then-typed recovery password from issuing arbitrary certs over the
/// wire (it can still revoke, which every signing slot can).
fn recovery_policy() -> ca_vault::Policy {
    netidx_conf::ca_policy::recovery_policy()
}

/// Refuse to build a CA on a host with no usable TPM / Secure Enclave —
/// before any disk write — unless the operator explicitly accepts the cost
/// with `--insecure-no-tpm`, in which case warn loudly. The autorenew
/// credential is sealed to the box's TPM precisely so a stolen backup is
/// inert; without sealing it sits in plaintext in every backup.
fn tpm_gate(insecure_no_tpm: bool) -> Result<()> {
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
    eprintln!("================================================================");
    eprintln!("WARNING: --insecure-no-tpm — no {mech} sealing on this host.");
    eprintln!("The autorenew keytab will be written in PLAINTEXT, so any backup");
    eprintln!("or disk image of this machine then contains a credential that");
    eprintln!("unlocks the CA key. Use this for TEST CAs only.");
    eprintln!("================================================================");
    Ok(())
}

/// Print the recovery password exactly once, boxed, with the store-it-in-a-
/// safe warning. It is never persisted (only the sealed autorenew keytab
/// carries a separate box credential), so this is the only time it is shown.
fn print_recovery_password(pw: &str) {
    let grouped = ca_vault::group_recovery_password(pw);
    let shown = grouped.as_str();
    let bar = "─".repeat(shown.chars().count() + 2);
    println!();
    println!("┌{bar}┐");
    println!("│ {shown} │");
    println!("└{bar}┘");
    println!("This is the CA RECOVERY PASSWORD. Write it down and lock it in a safe.");
    println!("It is shown ONCE and never stored. It is the only OFF-box credential");
    println!("that can unlock the CA key — to mint a new admin or rotate the box's");
    println!("own credential. If you lose it AND this machine, the CA is unrecoverable;");
    println!("while the machine lives you can mint a fresh one with");
    println!("`netidx conf ca recovery rotate`.");
    println!();
}

/// Create the founding superuser ROLE admin (operator names it + sets its
/// password). Full authority — broad issuance scope, may enroll servers,
/// edits perms anywhere, and manages other admins — yet it wraps no master
/// key, so its password can never unlock the CA. Only minted for a server
/// CA (a role admin authenticates to the daemon).
fn setup_superuser(
    cadir: &netidx_conf::ca_store::CaDir,
    opts: &NewCaOpts,
    cn: &str,
) -> Result<()> {
    let name = match env_user_name() {
        Some(user) => prompt::string_with_default(
            "superuser admin name",
            opts.admin.clone(),
            &user,
        )?,
        None => prompt::required_string("superuser admin name", opts.admin.clone())?,
    };
    if name.trim().is_empty() {
        bail!("superuser name must not be empty");
    }
    if ca_vault::is_reserved_admin(&name) {
        bail!(
            "{name:?} is a reserved signing-slot name; choose another for the superuser"
        );
    }
    let mut policy = prompt_policy(
        &PolicyArgs {
            allow_san: &opts.allowed_san,
            max_validity: opts.max_validity,
            id_map_groups: &opts.id_map_groups,
            may_enroll_servers: opts.may_enroll_servers,
            perms_scope: &[],
            service_scope: &[],
        },
        // The superuser founds the network, so it defaults to may-enroll.
        true,
        cn,
        opts.domain.as_deref(),
    )?;
    // What makes it the superuser: authority over the whole tree (perms +
    // service control) and the right to mint/scope other admins. (Issuance
    // scope + enroll came from the prompt above.)
    policy.perms_edit_scopes = vec!["/".to_string()];
    policy.service_control_scopes = vec!["/".to_string()];
    policy.may_manage_admins = true;
    let pw = collect_required_password(&format!("password for superuser {name:?}"))?;
    cadir.vault.write().add_role_slot(&name, &pw, policy)?;
    println!();
    println!("superuser role admin {name:?} created — it manages admins, edits perms,");
    println!("and enrolls servers, but never unlocks the CA key (the server signs).");
    Ok(())
}

/// `conf ca recovery rotate`: mint a fresh recovery password on the CA box.
/// Authorized by the box's own autorenew keytab (read + unsealed), so a lost
/// recovery password is recoverable while the machine lives — without it.
fn recovery(cmd: RecoveryCmd) -> Result<()> {
    match cmd {
        RecoveryCmd::Rotate(a) => recovery_rotate(a),
    }
}

fn recovery_rotate(a: RecoveryRotateArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    let cfg_path = paths::discover_conf_server_config().ok();
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    // Prefer the running daemon: it owns the CA and re-wraps the recovery
    // slot with its own on-box autorenew credential, so no flock contention
    // and no offline keytab handling here.
    if let Some(cfg) = &cfg_path
        && rt.block_on(conf_local::daemon_running(cfg))
    {
        let new_pw = rt.block_on(conf_local::rotate_recovery(cfg))?;
        println!("rotated the recovery password (via the running conf server)");
        print_recovery_password(&new_pw);
        return Ok(());
    }
    // Offline break-glass (daemon down): take the flock and rotate the slot
    // directly. The autorenew keytab is the on-box authority: it holds a
    // signing-slot password (sealed to this machine), and re-minting the
    // recovery slot recovers MK to wrap it. This is why rotation works
    // without the lost recovery password — but only on the box that holds
    // the keytab.
    if !ca_vault::CAVault::exists(&dir) {
        bail!("no vault-protected CA at {}", dir.display());
    }
    let keytab = autorenew_keytab_path()?;
    let autorenew_pw = netidx_conf::conf_server::read_autorenew_password(&keytab)
        .with_context(|| {
            format!(
                "rotating the recovery password needs the autorenew keytab ({}); it \
                 authorizes the re-mint on the CA box. (Set one up with \
                 `netidx conf ca auto-approve`.)",
                keytab.display()
            )
        })?;
    let cadir = netidx_conf::ca_store::CaDir::open(&dir).context(
        "rotating recovery needs exclusive access; stop the conf server first",
    )?;
    // Confirm the keytab credential actually unlocks this CA BEFORE removing
    // the old recovery slot — a stale keytab must not leave the CA with no
    // recovery slot. (The recovered key is dropped/zeroized immediately.)
    cadir.vault.read().unlock(&autorenew_pw).with_context(|| {
        format!(
            "the autorenew keytab ({}) did not unlock this CA — its credential is \
             stale. Re-mint it with `netidx conf ca auto-approve --rotate` (needs the \
             recovery password) and try again.",
            keytab.display()
        )
    })?;
    // Drop the old recovery slot, then mint a fresh one. Autorenew remains
    // the signing slot throughout, so the master key is never orphaned; if
    // the re-mint fails, autorenew still unlocks and the rotate can be
    // retried.
    let exists = cadir
        .vault
        .read()
        .list_admins()?
        .iter()
        .any(|i| i.admin == ca_vault::RECOVERY_ADMIN);
    if exists {
        cadir.vault.write().remove_slot(ca_vault::RECOVERY_ADMIN, false)?;
    }
    let new_pw = ca_vault::gen_recovery_password();
    cadir.vault.write().add_signing_slot(
        &autorenew_pw,
        ca_vault::RECOVERY_ADMIN,
        &new_pw,
        recovery_policy(),
    )?;
    println!("rotated the recovery password for the CA at {}", dir.display());
    print_recovery_password(&new_pw);
    Ok(())
}

fn init(p: InitParams) -> Result<()> {
    let directory = ca_dir_for(p.dir)?;
    // `ca init` always wants the unit when a server is set up (it has no
    // `--no-units`); default the dir so the activation supervisor finds
    // it.
    let units_dir = Some(match p.units_dir {
        Some(d) => d,
        None => paths::user_activation_dir()?,
    });
    let setup_server = if p.no_server {
        Some(false)
    } else if p.with_server {
        Some(true)
    } else {
        None
    };
    let (_ca, need) = create_vaulted_ca(NewCaOpts {
        dir: directory,
        common_name: p.cn,
        domain: p.domain,
        country: p.country,
        state: p.state,
        locality: p.locality,
        organization: p.organization,
        san: p.san,
        key_bits: p.key_bits,
        ca_validity: p.ca_validity,
        leaf_validity: p.leaf_validity,
        ca_renew_threshold: p.ca_renew_threshold,
        admin: p.admin,
        allowed_san: p.allow_san,
        max_validity: p.max_validity,
        id_map_groups: p.id_map_groups,
        may_enroll_servers: p.may_enroll_servers,
        insecure_no_tpm: p.insecure_no_tpm,
        setup_server,
        listen: p.listen,
        // No resolver in this flow; default_ca_listen_ip falls back to
        // an existing resolver's IP, then the public IP.
        listen_hint: None,
        units_dir,
    })?;

    // Single end-of-process hook — the same one the `conf install`
    // templates use.
    service::offer(
        need,
        service::ServiceGate {
            dry_run: false,
            no_service: p.no_service,
            with_service: p.with_service,
        },
    )
}

// -- ca admin -----------------------------------------------------------------

/// The preamble for a `--server` admin op: confirm WHO the conf server is
/// (silently against the local CA cert when this host holds it, else
/// glyph-confirm with the operator — before any password is typed), then
/// prompt for the managing admin's name + password. Returns the runtime, the
/// pinned identity, and the admin credentials. Mirrors `revoke` / `approve`.
pub(super) fn remote_admin_preamble(
    server: SocketAddr,
    ca_dir: Option<PathBuf>,
) -> Result<(tokio::runtime::Runtime, conf_client::CaIdentity, String, Zeroizing<String>)>
{
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let identity = rt
        .block_on(conf_client::fetch_identity(server, NodeKind::Client))
        .with_context(|| format!("contacting conf server {server}"))?;
    let local_fp = ca_dir_for(ca_dir)
        .ok()
        .and_then(|d| std::fs::read(d.join("certificate.pem")).ok())
        .and_then(|pem| Fingerprint::of_cert_pem(&pem).ok());
    match local_fp {
        Some(fp) if fp == identity.fingerprint => {
            println!("verified {server} against the local CA");
        }
        _ => {
            init::show_network_identity(server, &identity);
            if !prompt::confirm("does this match what your CA admin gave you?", false)? {
                bail!("CA identity was not confirmed; nothing was sent");
            }
        }
    }
    let admin = match env_user_name() {
        Some(user) => prompt::string_with_default("your admin name", None, &user)?,
        None => prompt::required_string("your admin name", None)?,
    };
    let password = Zeroizing::new(collect_existing_password(&format!(
        "CA password for admin {admin:?}"
    ))?);
    Ok((rt, identity, admin, password))
}

/// Print the admin roster (local `list` and remote `list --server` share
/// this), one line per admin: name, tier, and full policy incl.
/// `may_manage_admins`.
fn print_admin_list(admins: &[ca_vault::AdminInfo]) {
    if admins.is_empty() {
        println!("(no admins — this CA is not vault-protected)");
    }
    for info in admins {
        let tier = match info.kind {
            ca_vault::SlotKind::Signing => "signing",
            ca_vault::SlotKind::Role => "role",
        };
        let pol = &info.policy;
        println!(
            "{} [{tier}]: allowed_san={:?} max_validity={} id_map_groups={:?} \
             may_enroll_servers={} may_manage_admins={} perms_edit_scopes={:?} \
             service_control_scopes={:?}",
            info.admin,
            pol.allowed_san,
            humantime::format_duration(pol.max_validity),
            pol.id_map_groups,
            pol.may_enroll_servers,
            pol.may_manage_admins,
            pol.perms_edit_scopes,
            pol.service_control_scopes
        );
    }
}

fn admin(cmd: AdminCmd) -> Result<()> {
    match cmd {
        AdminCmd::Add(_) => {
            // Signing slots are fixed to recovery + autorenew in the
            // server-only model: minting a third MK-holder is exactly the
            // backup-crackable extra key the design removes. Authority is
            // granted to ROLE admins, which the server signs on behalf of.
            bail!(
                "`ca admin add` is gone: the only signing keyslots are `recovery` \
                 and `autorenew`, fixed at init. To grant a new admin authority, use \
                 `netidx conf ca admin add-role <name>` — a role admin edits perms, \
                 manages admins, and (with --may-enroll-servers) enrolls servers, all \
                 without ever unlocking the CA key (the server signs for it)."
            )
        }
        AdminCmd::AddRole(a) => {
            let name = prompt::required_string("new role admin name", a.name.clone())?;
            if ca_vault::is_reserved_admin(&name) {
                bail!(
                    "{name:?} is a reserved signing-slot name (recovery / autorenew) \
                     and cannot be a role admin"
                );
            }
            let policy_args = PolicyArgs {
                allow_san: &a.allow_san,
                max_validity: a.max_validity,
                id_map_groups: &a.id_map_groups,
                may_enroll_servers: a.may_enroll_servers,
                perms_scope: &a.perms_scope,
                service_scope: &a.service_scope,
            };
            // Remote: the CA enforces no-escalation (granted policy ⊆ the
            // managing admin's). Local: on-box FS access is the authority.
            if let Some(server) = a.server {
                let (rt, identity, admin, password) =
                    remote_admin_preamble(server, a.ca_dir.clone())?;
                let policy = prompt_policy(
                    &policy_args,
                    false,
                    &default_ca_cn(&identity.domain),
                    Some(&identity.domain),
                )?;
                let new_pw = collect_required_password(&format!(
                    "password for new role admin {name:?}"
                ))?;
                rt.block_on(conf_client::add_role_admin(
                    server,
                    NodeKind::Client,
                    &identity,
                    &admin,
                    password.as_str(),
                    &name,
                    &new_pw,
                    policy,
                ))?;
                println!("added role admin {name:?} on the CA at {server}");
                return Ok(());
            }
            // Local: the running daemon owns the vault; it trusts this peer
            // as a superuser over the control socket (no password). A role
            // with no authority at all is allowed (a placeholder to scope
            // later). The CN default is read from the local CA's public cert.
            let cfg_path = paths::discover_conf_server_config()?;
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            let policy = prompt_policy(
                &policy_args,
                false,
                &existing_ca_cn(&ca_dir_for(a.ca_dir.clone())?),
                None,
            )?;
            let new_pw = collect_required_password(&format!(
                "password for new role admin {name:?}"
            ))?;
            rt.block_on(conf_local::add_role_admin(&cfg_path, &name, &new_pw, policy))?;
            println!("added role admin {name:?} (via the local conf server)");
            Ok(())
        }
        AdminCmd::SetPolicy(a) => {
            let name =
                prompt::required_string("admin whose policy to set", a.name.clone())?;
            if ca_vault::is_reserved_admin(&name) {
                bail!(
                    "{name:?} is a system-managed signing slot; its narrow policy is \
                     fixed and must not be widened. Manage authority through role admins."
                );
            }
            let policy_args = PolicyArgs {
                allow_san: &a.allow_san,
                max_validity: a.max_validity,
                id_map_groups: &a.id_map_groups,
                may_enroll_servers: a.may_enroll_servers,
                perms_scope: &a.perms_scope,
                service_scope: &a.service_scope,
            };
            if let Some(server) = a.server {
                let (rt, identity, admin, password) =
                    remote_admin_preamble(server, a.ca_dir.clone())?;
                let policy = prompt_policy(
                    &policy_args,
                    false,
                    &default_ca_cn(&identity.domain),
                    Some(&identity.domain),
                )?;
                rt.block_on(conf_client::set_admin_policy(
                    server,
                    NodeKind::Client,
                    &identity,
                    &admin,
                    password.as_str(),
                    &name,
                    policy,
                ))?;
                println!("updated policy for admin {name:?} on the CA at {server}");
                return Ok(());
            }
            // Local: rescope through the daemon (superuser over the control
            // socket). Rescoping touches only the slot's plaintext policy,
            // never MK.
            let cfg_path = paths::discover_conf_server_config()?;
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            let policy = prompt_policy(
                &policy_args,
                false,
                &existing_ca_cn(&ca_dir_for(a.ca_dir.clone())?),
                None,
            )?;
            rt.block_on(conf_local::set_admin_policy(&cfg_path, &name, policy))?;
            println!("updated policy for admin {name:?} (via the local conf server)");
            Ok(())
        }
        AdminCmd::Remove(a) => {
            let name = prompt::required_string("admin to revoke", a.name.clone())?;
            if ca_vault::is_reserved_admin(&name) {
                bail!(
                    "{name:?} is a system-managed signing slot and cannot be removed \
                     directly (that would orphan the CA key or the box credential). \
                     Rotate recovery with `netidx conf ca recovery rotate`, or \
                     autorenew with `netidx conf ca auto-approve --rotate`."
                );
            }
            if let Some(server) = a.server {
                let (rt, identity, admin, password) =
                    remote_admin_preamble(server, a.ca_dir.clone())?;
                rt.block_on(conf_client::remove_admin(
                    server,
                    NodeKind::Client,
                    &identity,
                    &admin,
                    password.as_str(),
                    &name,
                ))?;
                println!("removed role admin {name:?} on the CA at {server}");
                return Ok(());
            }
            // Local: revoke through the daemon (superuser over the control
            // socket). `a.force` is ignored here — the daemon's last-manager
            // and reserved-slot guards still apply, preventing an orphaned CA
            // key.
            let cfg_path = paths::discover_conf_server_config()?;
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            rt.block_on(conf_local::remove_admin(&cfg_path, &name))?;
            println!("removed role admin {name:?} (via the local conf server)");
            Ok(())
        }
        AdminCmd::List(a) => {
            if let Some(server) = a.server {
                let (rt, identity, admin, password) =
                    remote_admin_preamble(server, a.ca_dir.clone())?;
                let admins = rt.block_on(conf_client::list_admins(
                    server,
                    NodeKind::Client,
                    &identity,
                    &admin,
                    password.as_str(),
                ))?;
                print_admin_list(&admins);
                return Ok(());
            }
            let cfg_path = paths::discover_conf_server_config()?;
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            let admins = rt.block_on(conf_local::list_admins(&cfg_path))?;
            print_admin_list(&admins);
            Ok(())
        }
    }
}

// -- ca fingerprint -----------------------------------------------------------

fn fingerprint(p: FingerprintArgs) -> Result<()> {
    let dir = ca_dir_for(p.ca_dir)?;
    show_ca_identity(&dir)
}

// -- ca join (the client) -----------------------------------------------------

pub(crate) fn join(p: JoinArgs) -> Result<()> {
    use super::init::{self, ConfServers};
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let (server, identity) = match p.server {
        // An explicit address: confirm WHO we've reached before any
        // credential is entered. `fetch_identity` sends nothing secret
        // and closes before returning.
        Some(server) => {
            let identity = rt
                .block_on(conf_client::fetch_identity(server, NodeKind::Client))
                .with_context(|| format!("contacting conf server {server}"))?;
            init::show_network_identity(server, &identity);
            if !prompt::confirm("does this match what your CA admin gave you?", false)? {
                bail!("CA identity was not confirmed; nothing was sent");
            }
            (server, identity)
        }
        // No address: this is "the CA flow not called by a higher
        // level flow" — probe for the network ourselves (browse →
        // confirm glyph → aggregate, with a manual-address fallback).
        None => match init::discover_network(NodeKind::Client)? {
            ConfServers::Have(net) => {
                let ca = net.info.ca_addr.ok_or_else(|| {
                    anyhow!(
                        "network {:?} reported no CA; cannot request a certificate",
                        net.identity.domain
                    )
                })?;
                (ca, net.identity)
            }
            ConfServers::DontHave => {
                bail!("no conf server found or selected; pass --server to specify one")
            }
            ConfServers::NotProbed => {
                bail!("--server is required when stdin is not a TTY")
            }
        },
    };
    let name = prompt::required_string("TLS identity name to request", p.name)?;
    let groups = init::prompt_id_map_groups(
        &p.id_map_groups,
        init::default_id_map_groups(NodeKind::Client),
    )?;
    let admin = prompt::required_string("admin name", p.admin)?;
    let password = Zeroizing::new(collect_existing_password(&format!(
        "CA password for admin {admin:?}"
    ))?);
    let issued = rt.block_on(conf_client::request_cert(
        server,
        NodeKind::Client,
        &name,
        &admin,
        password,
        p.validity,
        groups,
        &identity,
    ))?;
    let dir = tls::identity_dir(&name)?;
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    atomic::write_atomic(
        &dir.join("certificate.pem"),
        issued.cert_pem.as_bytes(),
        0o644,
    )?;
    atomic::write_atomic(
        &dir.join("private.key"),
        issued.private_key_pem.as_bytes(),
        0o600,
    )?;
    atomic::write_atomic(&dir.join("trusted.pem"), issued.trusted_pem.as_bytes(), 0o644)?;
    println!("installed identity {name:?} in {}", dir.display());
    for w in &issued.warnings {
        println!("  warning: {w}");
    }
    Ok(())
}

// -- shared helpers -----------------------------------------------------------

fn show_ca_identity(ca_dir: &std::path::Path) -> Result<()> {
    let cert = std::fs::read(ca_dir.join("certificate.pem"))
        .with_context(|| format!("reading CA cert in {}", ca_dir.display()))?;
    let fp = Fingerprint::of_cert_pem(&cert)?;
    println!("CA fingerprint:");
    println!("  SHA256  {}", fp.text());
    println!("{}", fp.identicon(ColorMode::detect()));
    Ok(())
}

/// The current unix user, if discoverable, to seed the admin-name
/// prompt's default. `None` when neither env var is set (e.g. a daemon
/// context), in which case the caller prompts with no default.
pub(super) fn env_user_name() -> Option<String> {
    for var in ["USER", "LOGNAME"] {
        if let Ok(v) = std::env::var(var)
            && !v.is_empty()
        {
            return Some(v);
        }
    }
    None
}

/// CLI-provided policy inputs; whatever is absent gets prompted.
struct PolicyArgs<'a> {
    allow_san: &'a [String],
    max_validity: Duration,
    id_map_groups: &'a [String],
    may_enroll_servers: Option<bool>,
    /// Netidx paths this admin may edit perms under. Taken straight from
    /// the flag (no prompt) — a signing admin gets perms scopes only when
    /// explicitly granted; role admins are minted by `admin add-role`.
    perms_scope: &'a [String],
    /// Netidx paths this admin may control services under (restart/start/
    /// stop the activation units of the cluster serving that path). Taken
    /// straight from the flag, like `perms_scope`.
    service_scope: &'a [String],
}

fn prompt_policy(
    args: &PolicyArgs,
    enroll_default: bool,
    cn: &str,
    domain: Option<&str>,
) -> Result<ca_vault::Policy> {
    let allowed_san = if !args.allow_san.is_empty() {
        args.allow_san.to_vec()
    } else {
        // Prefer an explicit domain (e.g. threaded from the resolver
        // install, which already asked for it) — `*.<domain>` matches the
        // `<user>.<domain>` SAN convention exactly. With no domain, fall
        // back to stripping the CN's leftmost label, which is right when
        // the CN is `<host>.<domain>` but only a guess otherwise.
        let suggestion = match domain {
            Some(d) if !d.is_empty() => format!("*.{d}"),
            _ => match cn.split_once('.') {
                Some((_, domain)) if !domain.is_empty() => format!("*.{domain}"),
                _ => "*".to_string(),
            },
        };
        let entry = prompt::string_with_default(
            "SAN names this admin may issue (glob, e.g. *.example.com)",
            None,
            &suggestion,
        )?;
        vec![entry]
    };
    let id_map_groups = if !args.id_map_groups.is_empty() {
        // `--id-map-group ''` is the explicit "none" — filter it out so
        // the resulting policy is empty (registration disabled) rather
        // than containing an empty group name.
        args.id_map_groups
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        // The *allowed set*: which groups this admin may assign when
        // enrolling a node (the actual choice happens per-enrollment,
        // in the SignRequest). Blank takes the default; a bare `-`
        // disables registration entirely (this admin's signs never
        // register id-map identities).
        let entry = prompt::string_with_default(
            "id-map groups this admin may assign when enrolling \
             (comma-separated; Enter for default, '-' for none)",
            None,
            "users",
        )?;
        init::parse_id_map_answer(&entry)
    };
    let may_enroll_servers = match args.may_enroll_servers {
        Some(b) => b,
        None => prompt::confirm(
            "may this admin enroll new conf servers (more privileged than any \
             SAN glob)?",
            enroll_default,
        )?,
    };
    let trim_paths = |scopes: &[String]| -> Vec<String> {
        scopes.iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
    };
    let perms_edit_scopes = trim_paths(args.perms_scope);
    let service_control_scopes = trim_paths(args.service_scope);
    Ok(ca_vault::Policy {
        allowed_san,
        max_validity: args.max_validity,
        id_map_groups,
        may_enroll_servers,
        perms_edit_scopes,
        // Phase 4 wires a --may-manage-admins flag through PolicyArgs.
        may_manage_admins: false,
        service_control_scopes,
    })
}

/// The default CA common name for a domain, following the same
/// `<name>.<domain>` convention as every other netidx identity — the CA
/// is just the `ca` node (e.g. `ryu-oh.org` → `ca.ryu-oh.org`).
pub(super) fn default_ca_cn(domain: &str) -> String {
    format!("ca.{domain}")
}

/// Resolve the CA common name: an explicit value wins; otherwise prompt,
/// defaulting to `ca.<domain>` when a domain is known (non-TTY then takes
/// that default), or requiring an explicit answer when it isn't.
fn resolve_ca_cn(provided: Option<String>, domain: Option<&str>) -> Result<String> {
    if let Some(cn) = provided {
        return Ok(cn);
    }
    match domain {
        Some(d) if !d.is_empty() => {
            prompt::string_with_default("CA common name", None, &default_ca_cn(d))
        }
        _ => prompt::required_string("CA common name", None),
    }
}

/// The DNS SAN on an existing CA's own cert, used to seed the policy
/// suggestion when scoping admins on an already-built CA (`admin add` /
/// `admin set-policy`). Empty if it can't be read — the prompt then has
/// no domain to suggest.
fn existing_ca_cn(dir: &Path) -> String {
    netidx_conf::tls::extract_dns_san_from_pem(&dir.join("certificate.pem"))
        .unwrap_or_default()
}

/// Prompt twice for a new password (confirmed, non-empty). Bails on a
/// non-TTY — a vaulted CA must have a real password. The result (and the
/// confirmation temporary) are `Zeroizing` — this mints a credential, so its
/// plaintext shouldn't linger in freed heap.
fn collect_required_password(label: &str) -> Result<Zeroizing<String>> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Err(anyhow!("{label}: a password is required but stdin is not a TTY"));
    }
    loop {
        let pw = Zeroizing::new(rpassword::prompt_password(format!("{label}: "))?);
        if pw.is_empty() {
            eprintln!("password must not be empty");
            continue;
        }
        let again = Zeroizing::new(rpassword::prompt_password("again: ")?);
        if *again != *pw {
            eprintln!("passwords did not match; try again");
            continue;
        }
        return Ok(pw);
    }
}

/// Prompt once for an existing password (no confirmation).
pub(super) fn collect_existing_password(label: &str) -> Result<String> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Err(anyhow!("{label}: stdin is not a TTY"));
    }
    Ok(rpassword::prompt_password(format!("{label}: "))?)
}

fn issue(p: IssueArgs) -> Result<()> {
    let directory = ca_dir_for(p.ca_dir)?;
    let cn = prompt::required_string("certificate common name", p.cn)?;
    let out_dir = prompt::required_path("output directory for key + cert", p.out_dir)?;
    let ca = open_ca(&directory)?;
    let san = parse_sans(&p.san, &cn)?;
    ensure_san_not_reserved(&san)?;
    let issued = issue_and_record(
        &ca,
        NodeKind::Client,
        IssueParams {
            subject: Subject {
                common_name: cn.clone(),
                country: p.country,
                state: p.state,
                locality: p.locality,
                organization: p.organization,
            },
            san,
            key_bits: p.key_bits,
            validity: p.validity,
            out_dir,
            // Leaf key encryption is wired through the install flow
            // (`netidx conf init`), where the engine knows how to plumb
            // an askpass entry into the emitted client config. The bare
            // `ca issue` CLI deliberately stays unencrypted: callers
            // here are doing manual cert issuance and don't necessarily
            // have a netidx config to receive the askpass.
            password: None,
            serial: 0, // assigned by issue_and_record
        },
    )?;
    println!("issued cert:");
    println!("  cn:          {}", cn);
    println!("  private key: {}", issued.private_key.display());
    println!("  certificate: {}", issued.certificate.display());
    Ok(())
}

pub(crate) fn request(p: RequestArgs) -> Result<()> {
    let cn = prompt::required_string("requested certificate common name", p.cn)?;
    // `--out-key` default is `./private.key`, but the *default* path
    // refuses to clobber: re-running `request` in the same dir would
    // otherwise silently destroy a key the operator may not have used
    // yet. An explicit `--out-key` overwrites freely — that's the
    // operator's call.
    let out_key = match p.out_key {
        Some(path) => path,
        None => {
            let default = PathBuf::from("private.key");
            if default.exists() {
                bail!(
                    "./private.key already exists — refusing to overwrite a \
                     private key. Pass --out-key <path>, or move the existing \
                     file."
                );
            }
            default
        }
    };
    // The CSR and (later) the signed cert are cheap to regenerate, so
    // their CWD defaults overwrite freely.
    let out_csr = p.out_csr.unwrap_or_else(|| default_csr_filename(&cn));
    let san = parse_sans(&p.san, &cn)?;
    let kr = ca::generate_csr(
        &Subject {
            common_name: cn.clone(),
            country: p.country,
            state: p.state,
            locality: p.locality,
            organization: p.organization,
        },
        &san,
        p.key_bits,
        // Bare `ca request` CLI doesn't encrypt the key — same
        // rationale as the `ca issue` CLI: encrypted leaf keys are
        // wired through `netidx conf init`, which knows how to set
        // the matching `tls.askpass` in the emitted config.
        None,
    )?;
    atomic::write_atomic(&out_key, &kr.private_key_pem, 0o600)
        .with_context(|| format!("writing private key to {:?}", out_key))?;
    atomic::write_atomic(&out_csr, &kr.csr_pem, 0o644)
        .with_context(|| format!("writing CSR to {:?}", out_csr))?;
    println!("wrote private key (0600): {}", out_key.display());
    println!("wrote CSR        (0644): {}", out_csr.display());
    println!();
    println!("# Next step: hand the CSR to a CA admin who runs");
    println!("#   netidx conf ca sign {} --out <cert.pem>", out_csr.display());
    Ok(())
}

fn sign(mut p: SignArgs) -> Result<()> {
    let csr_path = prompt::required_path("path to the CSR to sign", p.csr_path.take())?;
    let directory = ca_dir_for(p.ca_dir.take())?;
    let ca = open_ca(&directory)?;
    let csr_pem = std::fs::read(&csr_path)
        .with_context(|| format!("reading CSR {}", csr_path.display()))?;
    let summary = ca::inspect_csr(&csr_pem).context("inspecting CSR")?;
    println!("CSR summary:");
    println!("  path:        {}", csr_path.display());
    println!("  cn:          {}", summary.common_name.as_deref().unwrap_or("(none)"));
    println!("  key bits:    {}", summary.key_bits);
    if summary.san.is_empty() {
        println!("  san:         (none in CSR)");
    } else {
        println!("  san:");
        for entry in &summary.san {
            println!("    - {}", san_display(entry));
        }
    }
    // `--out` defaults to `./<csr-cn>.pem` once we've read the CN out
    // of the CSR (falling back to `./certificate.pem` for a CN-less
    // CSR). Certs are cheap to regenerate, so the default overwrites
    // freely.
    let out = p
        .out
        .take()
        .unwrap_or_else(|| default_cert_filename(summary.common_name.as_deref()));
    let san = resolve_sign_san(&p, &summary)?;
    ensure_san_not_reserved(&san)?;
    println!("  signing SAN:");
    for entry in &san {
        println!("    - {}", san_display(entry));
    }
    let name =
        first_dns_san(&san).or_else(|| summary.common_name.clone()).unwrap_or_default();
    let cert_pem =
        sign_and_record(&ca, NodeKind::Client, &csr_pem, &san, &name, p.validity)?;
    atomic::write_atomic(&out, &cert_pem, 0o644)
        .with_context(|| format!("writing certificate to {:?}", out))?;
    println!("\nsigned cert (0644): {}", out.display());
    maybe_register_in_id_map(&summary, &san, p.no_id_map)?;
    Ok(())
}

/// After signing a cert, optionally register the new identity in
/// the local id-map (the file the resolver consults to map TLS SANs
/// to unix uid + groups). Silently skipped when:
/// - `--no-id-map` was passed, or
/// - no local id-map exists at the canonical user path, or
/// - the CSR carries no usable identity name (no SAN DNS entry and
///   no CN), or
/// - stdin is not a TTY (scripts use `netidx conf component id-map set-user`
///   for explicit non-interactive registration; we don't want a
///   level-1 prompt to silently write a wrong UID).
///
/// On a TTY with an id-map present, prompts for groups (level-1,
/// default `users`) and uid (level-1, default = max(existing) + 1
/// starting from 1000). First group in the list is the primary;
/// the rest become secondary memberships. Refuses any group not
/// already in the map — there's no "create group on the fly" path
/// here because that would let a typo silently introduce a
/// privilege-bearing group.
fn maybe_register_in_id_map(
    summary: &ca::CsrSummary,
    san: &[SanEntry],
    no_id_map: bool,
) -> Result<()> {
    use netidx_conf::id_map;
    if no_id_map {
        return Ok(());
    }
    if !prompt::stdin_is_tty() {
        return Ok(());
    }
    // The identity NAME in the id-map is what the resolver sees on
    // the wire: the cert's SAN alt-name, i.e. the first DNS SAN.
    // Fall back to the CN if no DNS SAN (unlikely; netidx rejects
    // such certs anyway, but the prompt path shouldn't crash).
    let identity_name = san
        .iter()
        .find_map(|s| if let SanEntry::Dns(d) = s { Some(d.clone()) } else { None })
        .or_else(|| summary.common_name.clone());
    let identity_name = match identity_name {
        Some(n) => n,
        None => {
            println!("(no DNS SAN / CN — skipping id-map registration)");
            return Ok(());
        }
    };
    let map_path = id_map::user_id_map_path()?;
    let mut map = match id_map::load(&map_path) {
        Ok(m) => m,
        Err(_) => {
            // Most common cause: no map yet. Tell the operator
            // exactly what's missing so they can `id-map init` if
            // they want one, but don't fail the sign.
            println!(
                "(no local id-map at {} — skipping registration; \
                 create one with `netidx conf component id-map init`)",
                map_path.display(),
            );
            return Ok(());
        }
    };
    if !prompt::confirm(
        &format!("register identity {identity_name:?} in the local id-map?"),
        true,
    )? {
        return Ok(());
    }
    // List groups so the operator knows what's valid; sorted for
    // readable output and stable across runs.
    let mut group_names: Vec<&str> = map.groups.keys().map(|k| k.as_str()).collect();
    group_names.sort_unstable();
    println!("available groups: {}", group_names.join(", "));
    let groups_str = prompt::string_with_default(
        "groups (comma-separated; first is primary)",
        None,
        "users",
    )?;
    let groups: Vec<&str> =
        groups_str.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    if groups.is_empty() {
        bail!("no groups specified — at least the primary group is required");
    }
    let (primary, secondary): (&str, &[&str]) = (groups[0], &groups[1..]);
    let uid: u32 =
        prompt::parsed_with_default("uid", None, &id_map::next_uid(&map).to_string())?;
    let prev =
        id_map::upsert_identity(&mut map, &identity_name, uid, primary, secondary)?;
    id_map::save(&map_path, &map)?;
    match prev {
        Some(old) => println!(
            "updated id-map: {identity_name} (was uid={} primary={})",
            old.uid,
            old.primary_group.as_str(),
        ),
        None => println!("added to id-map: {identity_name} uid={uid} primary={primary}"),
    }
    Ok(())
}

/// `ca approve` — interactive enrollment-queue mode: list the pending
/// requests on the conf server, review one at a time (matching the
/// request code the enrollee read out — the fingerprint of the CSR's
/// public key, computed locally from the CSR, never trusted from the
/// wire), and approve (choosing the id-map groups) or deny. Works from
/// anywhere that can reach the conf server — enrollment admins don't
/// need shell access to the CA host.
fn approve(p: ApproveArgs) -> Result<()> {
    use super::init::{self, ConfServers};
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    // Where's the conf server? `--server`, else this host's own conf
    // server, else discovery (browse → confirm → aggregate).
    let (server, discovered_identity) = match p.server.or_else(local_conf_server_listen) {
        Some(s) => (s, None),
        None => match init::discover_network(NodeKind::Client)? {
            ConfServers::Have(net) => {
                let ca = net.info.ca_addr.ok_or_else(|| {
                    anyhow!(
                        "network {:?} reported no CA; there is no queue to work",
                        net.identity.domain
                    )
                })?;
                (ca, Some(net.identity))
            }
            ConfServers::DontHave => {
                bail!("no conf server found or selected; pass --server to specify one")
            }
            ConfServers::NotProbed => {
                bail!("--server is required when stdin is not a TTY")
            }
        },
    };
    let identity = match discovered_identity {
        // Discovery already glyph-confirmed the network.
        Some(identity) => identity,
        None => {
            let identity = rt
                .block_on(conf_client::fetch_identity(server, NodeKind::Client))
                .with_context(|| format!("contacting conf server {server}"))?;
            // On the CA host itself (or any box with the CA dir), the
            // local CA cert is the trust anchor — verify automatically
            // rather than asking the admin to confirm their own glyph.
            let local_fp = ca_dir_for(p.ca_dir.clone())
                .ok()
                .and_then(|d| std::fs::read(d.join("certificate.pem")).ok())
                .and_then(|pem| Fingerprint::of_cert_pem(&pem).ok());
            match local_fp {
                Some(fp) if fp == identity.fingerprint => {
                    println!("verified {server} against the local CA");
                }
                _ => {
                    init::show_network_identity(server, &identity);
                    if !prompt::confirm(
                        "does this match what your CA admin gave you?",
                        false,
                    )? {
                        bail!("CA identity was not confirmed; nothing was sent");
                    }
                }
            }
            identity
        }
    };
    let admin = match env_user_name() {
        Some(user) => prompt::string_with_default("admin name", None, &user)?,
        None => prompt::required_string("admin name", None)?,
    };
    let password = Zeroizing::new(collect_existing_password(&format!(
        "CA password for admin {admin:?}"
    ))?);
    loop {
        let queue = rt.block_on(conf_client::list_queue(
            server,
            &admin,
            password.as_str(),
            &identity,
        ))?;
        if queue.is_empty() {
            println!("the signing queue is empty (pass a CSR path to sign a file)");
            return Ok(());
        }
        // Verified renewals first, as a batch: the server proved
        // possession of the live key for the same name, so there is no
        // code to match and no groups to choose — approving them all is
        // honest, not careless. The ceremony stays for new identities.
        let renewals: Vec<&netidx_conf::conf_proto::QueueEntry> =
            queue.iter().filter(|e| e.verified_renewal).collect();
        if !renewals.is_empty() {
            println!();
            println!("verified renewals (proof of possession; no code to match):");
            for e in &renewals {
                println!(
                    "  {}  kind {:?}  age {}  from {}",
                    e.requested_name,
                    e.kind,
                    fmt_age(e.age_secs),
                    e.peer,
                );
            }
            if prompt::confirm(
                &format!("approve all {} verified renewal(s)?", renewals.len()),
                true,
            )? {
                for e in &renewals {
                    match rt.block_on(conf_client::approve(
                        server,
                        &admin,
                        password.as_str(),
                        &e.id,
                        vec![],
                        &identity,
                    )) {
                        Ok(_) => println!("  renewed {:?}", e.requested_name),
                        Err(err) => {
                            println!("  renewing {:?} failed: {err:#}", e.requested_name)
                        }
                    }
                }
                continue; // re-list
            }
        }
        let new_requests: Vec<&netidx_conf::conf_proto::QueueEntry> =
            queue.iter().filter(|e| !e.verified_renewal).collect();
        if new_requests.is_empty() {
            println!("(only unapproved renewals remain)");
            return Ok(());
        }
        println!();
        println!("pending signing requests:");
        for (i, e) in new_requests.iter().enumerate() {
            let code = conf_client::csr_fingerprint(&e.csr_pem)
                .map(|f| f.short())
                .unwrap_or_else(|_| "????????".to_string());
            // A conf-server enrollment is a bigger trust decision than
            // a user cert — say so in the list, not just the detail.
            let what = match e.enroll_listen {
                Some(listen) => format!("CONF-SERVER ENROLLMENT at {listen}"),
                None => e.requested_name.clone(),
            };
            println!(
                "  {}) {}  code {}  kind {:?}  age {}  from {}",
                i + 1,
                what,
                code,
                e.kind,
                fmt_age(e.age_secs),
                e.peer,
            );
        }
        let answer =
            prompt::required_string("request # to review (or 'q' to quit)", None)?;
        if answer.eq_ignore_ascii_case("q") {
            return Ok(());
        }
        let entry = match answer.parse::<usize>() {
            Ok(n) if (1..=new_requests.len()).contains(&n) => new_requests[n - 1],
            _ => {
                eprintln!("enter a number between 1 and {}, or 'q'", new_requests.len());
                continue;
            }
        };
        let fp = conf_client::csr_fingerprint(&entry.csr_pem)
            .context("the queued CSR does not parse — deny it")?;
        println!();
        match entry.enroll_listen {
            Some(listen) => {
                println!("  CONF-SERVER ENROLLMENT — approving signs the reserved");
                println!(
                    "  serving name {:?} and registers the new",
                    conf_proto::SERVING_SAN
                );
                println!("  conf server at {listen} as a peer. It will answer");
                println!("  discovery and present the network identity to joiners.");
                println!("  (requires your policy's may_enroll_servers)");
            }
            None => {
                println!("  name:     {}", entry.requested_name);
                println!(
                    "  validity: {} (capped by your policy)",
                    humantime::format_duration(entry.requested_validity)
                );
            }
        }
        println!("  kind:     {:?}", entry.kind);
        println!("  from:     {}", entry.peer);
        println!("  request code:");
        println!("  SHA256  {}", fp.text());
        println!("{}", fp.identicon(ColorMode::detect()));
        // The mutual-glyph moment: the enrollee's terminal shows this
        // same code; the requester sent it over a channel the admin
        // trusts. A mismatch means the queue entry is NOT the request
        // the admin thinks it is.
        if !prompt::confirm("does this code match what the requester sent you?", false)? {
            if prompt::confirm("deny this request?", true)? {
                let reason = prompt::string_with_default(
                    "denial reason (shown to the requester)",
                    None,
                    "request code mismatch",
                )?;
                rt.block_on(conf_client::deny(
                    server,
                    &admin,
                    password.as_str(),
                    &entry.id,
                    &reason,
                    &identity,
                ))?;
                println!("denied.");
            }
            continue;
        }
        let action: String = prompt::choice_with_default(
            "action",
            None,
            &["approve", "deny", "skip"],
            "approve",
        )?;
        match action.as_str() {
            "approve" => {
                // The admin knows who they're enrolling — the groups
                // are chosen here, bounded by this admin's policy. A
                // conf server isn't a user: enrollments never register
                // in the id-map, so there is nothing to ask.
                let groups = if entry.enroll_listen.is_some() {
                    vec![]
                } else {
                    init::prompt_id_map_groups(
                        &[],
                        init::default_id_map_groups(entry.kind),
                    )?
                };
                let warnings = rt.block_on(conf_client::approve(
                    server,
                    &admin,
                    password.as_str(),
                    &entry.id,
                    groups,
                    &identity,
                ))?;
                println!(
                    "approved and signed {:?} — the requester's install picks it \
                     up on its next poll.",
                    entry.requested_name
                );
                for w in warnings {
                    println!("  warning: {w}");
                }
            }
            "deny" => {
                let reason = prompt::required_string(
                    "denial reason (shown to the requester)",
                    None,
                )?;
                rt.block_on(conf_client::deny(
                    server,
                    &admin,
                    password.as_str(),
                    &entry.id,
                    &reason,
                    &identity,
                ))?;
                println!("denied.");
            }
            _ => continue,
        }
    }
}

/// This host's conf-server address from its own `conf-server.json`,
/// loopback-adjusted when it binds all interfaces.
pub(super) fn local_conf_server_listen() -> Option<SocketAddr> {
    let path = paths::discover_conf_server_config().ok()?;
    let cfg = netidx_conf::conf_server_config::ConfServerConfig::load(&path).ok()?;
    let mut addr = cfg.listen;
    if addr.ip().is_unspecified() {
        addr.set_ip(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    }
    Some(addr)
}

pub(super) fn fmt_age(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Decide which SAN to embed in the signed cert.
///
/// - `--san …` (one or more) → use those as-is, override the CSR.
/// - `--accept-csr-san` → use whatever the CSR carries (no prompt).
/// - Both flags → error: the explicit choice makes the implicit
///   acceptance redundant, and combining them would quietly hide
///   whether `--san` came from the operator's intent or from an
///   earlier shell-history copy of the CSR's contents.
/// - Neither flag → level-1 prompt: the CSR summary (incl. its SAN)
///   was already printed by `sign`; ask the admin whether to accept
///   that SAN as-is, defaulting to yes. A non-TTY caller also takes
///   the default — scripts that want to be explicit can still pass
///   `--san` or `--accept-csr-san`. The bare "neither flag" case
///   used to bail and tell the operator to re-run with one of the
///   flags; that was a UX wart for the common interactive case.
fn resolve_sign_san(p: &SignArgs, summary: &ca::CsrSummary) -> Result<Vec<SanEntry>> {
    match (p.san.is_empty(), p.accept_csr_san) {
        (false, false) => p.san.iter().map(|s| parse_san_one(s)).collect(),
        (true, true) => {
            if summary.san.is_empty() {
                bail!(
                    "--accept-csr-san was set but the CSR carries no SAN; pass \
                     --san <kind>:<value> to specify one"
                );
            }
            Ok(summary.san.clone())
        }
        (false, true) => bail!(
            "pass either --san <kind>:<value> (one or more) or --accept-csr-san, \
             not both"
        ),
        (true, false) => {
            if summary.san.is_empty() {
                bail!(
                    "CSR carries no SAN to inherit; pass --san <kind>:<value> \
                     (one or more) to specify one"
                );
            }
            if prompt::confirm("use the CSR's SAN as the signed cert's SAN?", true)? {
                Ok(summary.san.clone())
            } else {
                bail!(
                    "rejected — re-run with --san <kind>:<value> (one or \
                     more) to override the CSR's SAN"
                );
            }
        }
    }
}

fn san_display(s: &SanEntry) -> String {
    match s {
        SanEntry::Dns(d) => format!("dns:{d}"),
        SanEntry::Ip(ip) => format!("ip:{ip}"),
        SanEntry::Uri(u) => format!("uri:{u}"),
        SanEntry::Email(e) => format!("email:{e}"),
    }
}

/// Make a CN safe to embed in a filename. CNs are usually hostnames
/// (already safe), but the field is free-form text, so replace
/// anything outside `[A-Za-z0-9._-]` with `_`. The result is always
/// a single path component — no separators survive — so a defaulted
/// output path can't traverse out of the cwd. Empty input collapses
/// to `_` so we never produce a bare extension like `.csr`.
pub(super) fn sanitize_filename(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() { "_".to_string() } else { out }
}

/// Default `request` CSR path: `./<cn>.csr`.
pub(super) fn default_csr_filename(cn: &str) -> PathBuf {
    PathBuf::from(format!("{}.csr", sanitize_filename(cn)))
}

/// Default `sign` cert path: `./<csr-cn>.pem`, or `./certificate.pem`
/// when the CSR carries no CN.
fn default_cert_filename(csr_cn: Option<&str>) -> PathBuf {
    let stem = match csr_cn {
        Some(cn) => sanitize_filename(cn),
        None => "certificate".to_string(),
    };
    PathBuf::from(format!("{stem}.pem"))
}

fn list() -> Result<()> {
    let dir = match paths::user_ca_dir() {
        Ok(p) => p,
        Err(_) => {
            println!("# no user config dir on this platform");
            return Ok(());
        }
    };
    if !dir.join("certificate.pem").is_file() {
        println!("# no CA at {} — run `netidx conf ca init` first", dir.display());
        return Ok(());
    }
    println!("CA at {}", dir.display());
    if let Ok(cert) = std::fs::read(dir.join("certificate.pem"))
        && let Ok(fp) = Fingerprint::of_cert_pem(&cert)
    {
        println!(
            "  fingerprint: {} … (`netidx conf ca fingerprint` for the full id)",
            fp.short()
        );
    }
    // Key storage: the current format is the keyslot vault (key in
    // `vault.json`, not `private.key`), so detect that before falling
    // back to the legacy single-key format.
    if ca_vault::CAVault::exists(&dir) {
        // The daemon owns the vault now, so the admin roster comes over the
        // local control socket. Without a running daemon we can detect the
        // vault format but not list its admins.
        let cfg_path = paths::discover_conf_server_config().ok();
        let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
        match &cfg_path {
            Some(p) if rt.block_on(conf_local::daemon_running(p)) => {
                let admins = rt
                    .block_on(conf_local::list_admins(p))
                    .map(|a| a.into_iter().map(|i| i.admin).collect::<Vec<_>>())
                    .unwrap_or_default();
                if admins.is_empty() {
                    println!("  key:    keyslot vault");
                } else {
                    println!("  key:    keyslot vault — admins: {}", admins.join(", "));
                }
            }
            _ => {
                println!("  key:    keyslot vault (start the conf server to list admins)")
            }
        }
    } else if dir.join("private.key").is_file() {
        println!("  key:    private.key (legacy single-key format)");
    } else {
        println!("  key:    MISSING — CA cannot sign");
    }
    // Conf server.
    let cfg = paths::discover_conf_server_config()
        .ok()
        .and_then(|p| netidx_conf::conf_server_config::ConfServerConfig::load(&p).ok());
    match cfg {
        Some(c) => println!("  server: configured (listen {})", c.listen),
        None => println!("  server: not configured"),
    }
    Ok(())
}

// -- generate-flow helpers ---------------------------------------------------
//
// Used by `netidx conf resolver install --auth tls` to offer a
// "just generate the resolver certificate" path: for a small org the
// resolver host is commonly the CA host too, and making that one-step
// is the whole point.

/// True if the default CA location holds a usable CA — both the cert
/// and the private key. (A cert with no key is a trust anchor we
/// imported, not a CA we can sign with.)
pub(super) fn default_ca_present() -> bool {
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

/// Open the CA at `dir` as a signer. Handles both formats:
/// - **vaulted** (current): prompt for an admin password and unlock the
///   keyslot vault to recover the signing key.
/// - **legacy** `private.key`: unencrypted open, prompting only if the
///   key turns out to be encrypted.
///
/// A non-TTY caller that would need a password bails rather than
/// hanging. This is the single CA-open entry point — every command that
/// signs (`issue`, `sign`, the resolver's local-CA issuance) goes
/// through it, so they all transparently handle vaulted CAs.
pub(super) fn open_ca(dir: &std::path::Path) -> Result<Ca> {
    if ca_vault::CAVault::exists(dir) {
        // Offline issuance takes the CA flock for the whole unlock — a running
        // conf server owns the CA, so this fails fast if one is up. The handle
        // drops at the `return` below, releasing the flock before the issue /
        // sign paths re-open their own CaDir for serial allocation.
        let cadir = netidx_conf::ca_store::CaDir::open(dir)
            .context("opening the CA to sign offline (a running conf server owns it — stop it first)")?;
        // Daily on-box use unlocks with the box's own autorenew credential —
        // read + unsealed from its keytab, no human secret typed. Fall back
        // to the recovery password only when the keytab is absent or doesn't
        // unlock this CA (an offline CA with no autorenew, a different CA dir,
        // or a dead TPM).
        let from_keytab =
            autorenew_keytab_path().ok().filter(|k| k.exists()).and_then(|keytab| {
                match netidx_conf::conf_server::read_autorenew_password(&keytab) {
                    Ok(pw) => match cadir.vault.read().unlock(&pw) {
                        Ok(u) => Some(u),
                        Err(e) => {
                            eprintln!(
                                "note: the autorenew keytab did not unlock this CA \
                                 ({e:#}); falling back to the recovery password"
                            );
                            None
                        }
                    },
                    Err(e) => {
                        eprintln!(
                            "note: could not read the autorenew keytab ({e:#}); \
                             falling back to the recovery password"
                        );
                        None
                    }
                }
            });
        let unlocked = match from_keytab {
            Some(u) => u,
            None => {
                if !prompt::stdin_is_tty() {
                    bail!(
                        "the CA at {} is vault-protected and the autorenew keytab did \
                         not unlock it; it needs the recovery password, but stdin is \
                         not a TTY",
                        dir.display(),
                    );
                }
                let typed = Zeroizing::new(collect_existing_password(
                    "the CA recovery password (from your safe; printed once at init)",
                )?);
                let pw = ca_vault::normalize_recovery_password(&typed);
                cadir.vault.read().unlock(&pw).with_context(|| {
                    format!("unlocking the CA vault at {}", dir.display())
                })?
            }
        };
        let cert = std::fs::read(dir.join("certificate.pem"))
            .with_context(|| format!("reading CA cert in {}", dir.display()))?;
        return Ca::from_pem(dir.to_path_buf(), &unlocked.ca_key_pem, &cert)
            .with_context(|| format!("loading CA at {}", dir.display()));
    }
    // Legacy `private.key` CA.
    match Ca::open(dir, None) {
        Ok(ca) => Ok(ca),
        Err(e) if format!("{e:#}").contains("encrypted") => {
            if !prompt::stdin_is_tty() {
                bail!(
                    "the CA at {} has an encrypted private key and stdin is \
                     not a TTY; cannot prompt for the password",
                    dir.display(),
                );
            }
            let pw = rpassword::prompt_password("CA password: ")
                .context("reading CA password")?;
            Ca::open(dir, Some(&pw))
                .with_context(|| format!("opening CA at {}", dir.display()))
        }
        Err(e) => Err(e).with_context(|| format!("opening CA at {}", dir.display())),
    }
}

/// [`open_ca`] at the conventional `${basedir}/ca/` location.
pub(super) fn open_default_ca() -> Result<Ca> {
    open_ca(&paths::user_ca_dir()?)
}

/// Refuse to mint the conf server's reserved serving name from the
/// local CLI, mirroring the network sign path's refusal. The reserved
/// name is the linchpin of the trust model; only the conf-server setup
/// flow (which signs it directly) and the policy-gated network Enroll
/// may issue it.
fn ensure_san_not_reserved(san: &[SanEntry]) -> Result<()> {
    for s in san {
        if let SanEntry::Dns(d) = s
            && d.eq_ignore_ascii_case(conf_proto::SERVING_SAN)
        {
            bail!(
                "{:?} is reserved for the conf server's serving certificate and \
                 can't be issued here",
                conf_proto::SERVING_SAN
            );
        }
    }
    Ok(())
}

/// Issue an identity (CN = SAN-DNS = `name`) from `ca` into `out_dir`.
/// Returns the issued file paths. The caller chooses `out_dir`: the
/// install flow issues into a staging dir and lets `apply()` copy the
/// result into the canonical location, so nothing under the config
/// tree is touched until the apply phase.
///
/// `password = Some(p)` encrypts the on-disk private key with `p`
/// (PKCS#8 + AES-256-CBC). `None` writes an unencrypted key.
/// Encrypted-key callers in the install flow also wire an
/// `askpass` entry into the emitted client config so netidx can
/// decrypt the key at startup.
/// The first DNS SAN of a leaf — the identity name the index keys on.
fn first_dns_san(san: &[SanEntry]) -> Option<String> {
    san.iter().find_map(|s| match s {
        SanEntry::Dns(d) => Some(d.clone()),
        _ => None,
    })
}

/// Record an offline (pre-daemon) issuance in the CA store, exactly as the
/// daemon records its own. Offline issuance happens during bootstrap —
/// before the daemon takes ownership of the CA — so it must seed the
/// serial from, and commit back into, the same store the daemon reads:
/// that keeps serials unique across the bootstrap certs and every later
/// daemon issuance, and makes a bootstrap cert revocable like any other.
/// `csr_pem` is empty when the key was generated internally (the
/// revoke-UI glyph is then simply absent).
pub(super) fn record_offline_issuance(
    store: &mut netidx_conf::ca_store::CAStore,
    serial: u64,
    kind: NodeKind,
    name: &str,
    csr_pem: &str,
    cert_pem: &str,
    validity: Duration,
) -> Result<()> {
    let req = netidx_conf::ca_store::QueuedReq::new(
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

/// Issue a leaf offline, allocating a fresh serial and recording the
/// issuance (see [`record_offline_issuance`]). Returns the written files.
fn issue_and_record(
    ca: &Ca,
    kind: NodeKind,
    mut params: IssueParams,
) -> Result<IssuedFiles> {
    let ca_dir = ca.directory().to_path_buf();
    // Take the same exclusive flock the daemon holds: offline issuance is
    // only legitimate before the daemon owns the CA, and the serial is
    // allocated from (and committed back to) the store the daemon seeds
    // its in-memory counter from. Without this lock a `ca issue` run
    // against a live daemon would mint the serial the daemon allocates
    // next, producing a duplicate X.509 serial.
    let cadir = netidx_conf::ca_store::CaDir::open(&ca_dir)
        .context("cannot issue offline: a running conf server owns this CA")?;
    let serial = cadir.store.lock().next_serial()?;
    params.serial = serial;
    let name =
        first_dns_san(&params.san).unwrap_or_else(|| params.subject.common_name.clone());
    let validity = params.validity;
    let issued = ca.issue(&params)?;
    let cert_pem = std::fs::read_to_string(&issued.certificate).with_context(|| {
        format!("reading issued cert {}", issued.certificate.display())
    })?;
    // `ca.issue` already wrote the key + cert to disk. If recording the
    // issuance fails, roll those back: an un-recorded cert is invisible to
    // `next_serial`, so leaving it would let its serial be handed out again.
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

/// Sign an external CSR offline, allocating a fresh serial and recording
/// the issuance (see [`record_offline_issuance`]). Returns the leaf PEM.
pub(super) fn sign_and_record(
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
    let cadir = netidx_conf::ca_store::CaDir::open(&ca_dir)
        .context("cannot sign offline: a running conf server owns this CA")?;
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

pub(super) fn issue_identity(
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
fn issue_identity_into(
    ca: &Ca,
    name: &str,
    out_dir: PathBuf,
    key_bits: u32,
    password: Option<&str>,
) -> Result<IssuedFiles> {
    issue_and_record(
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

fn parse_sans(raw: &[String], fallback_cn: &str) -> Result<Vec<SanEntry>> {
    if raw.is_empty() {
        return Ok(vec![SanEntry::Dns(fallback_cn.to_string())]);
    }
    raw.iter().map(|s| parse_san_one(s)).collect()
}

fn parse_san_one(s: &str) -> Result<SanEntry> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_serving_san_is_refused() {
        let reserved = conf_proto::SERVING_SAN;
        assert!(ensure_san_not_reserved(&[SanEntry::Dns(reserved.to_string())]).is_err());
        // DNS is case-insensitive — an upper/mixed-case variant is the
        // same reserved name and must also be refused.
        assert!(
            ensure_san_not_reserved(&[SanEntry::Dns(reserved.to_uppercase())]).is_err()
        );
        // A normal name (and a non-DNS SAN type) is fine.
        assert!(
            ensure_san_not_reserved(&[SanEntry::Dns("resolver.example.com".to_string())])
                .is_ok()
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
            SanEntry::Ip(ip) if ip == "127.0.0.1".parse::<IpAddr>().unwrap()
        ));
        assert!(parse_san_one("uri:https://x").is_ok());
        assert!(parse_san_one("email:a@b").is_ok());
        assert!(parse_san_one("bogus").is_err());
        assert!(parse_san_one("bogus:x").is_err());
        assert!(parse_san_one("ip:not-an-ip").is_err());
        // Empty values are rejected for every kind.
        for kind in ["dns", "ip", "uri", "email"] {
            assert!(
                parse_san_one(&format!("{kind}:")).is_err(),
                "empty {kind}: should be rejected",
            );
        }
    }

    #[test]
    fn san_defaults_to_dns_cn() {
        let v = parse_sans(&[], "host.example.com").unwrap();
        assert_eq!(v.len(), 1);
        assert!(matches!(&v[0], SanEntry::Dns(s) if s == "host.example.com"));
    }

    #[test]
    fn request_then_sign_round_trip() {
        // The full client/admin handoff: client generates key+CSR
        // locally; admin uses `Ca::sign_request` to mint a cert.
        let scratch = tempfile::tempdir().unwrap();
        let key_path = scratch.path().join("client.key");
        let csr_path = scratch.path().join("client.csr");
        // Use 2048 for test speed — production is 4096.
        request(RequestArgs {
            cn: Some("client.example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:client.example.com".into()],
            key_bits: 2048,
            out_key: Some(key_path.clone()),
            out_csr: Some(csr_path.clone()),
        })
        .unwrap();
        assert!(key_path.exists());
        assert!(csr_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "key must be 0600");
            let mode = std::fs::metadata(&csr_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o644, "csr must be 0644");
        }

        // Admin side: stand up a tiny CA and sign the CSR.
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("test-ca"),
                san: vec![SanEntry::Dns("test-ca".into())],
                key_bits: 2048,
                validity: Duration::from_secs(30 * 86400),
            },
            None,
        )
        .unwrap();
        let cert_path = scratch.path().join("client.pem");
        sign(SignArgs {
            csr_path: Some(csr_path.clone()),
            san: vec![],
            // Explicit accept: the round trip flow simulates the admin
            // who has looked at the CSR and is happy to sign as-is.
            accept_csr_san: true,
            validity: Duration::from_secs(30 * 86400),
            ca_dir: Some(ca_dir.clone()),
            out: Some(cert_path.clone()),
            no_id_map: true,
        })
        .unwrap();
        assert!(cert_path.exists());
        // Confirm we actually wrote a PEM-encoded leaf cert; the
        // engine-side `Ca::sign_request` test already verifies that
        // signed certs chain back to the CA.
        let bytes = std::fs::read(&cert_path).unwrap();
        assert!(bytes.starts_with(b"-----BEGIN CERTIFICATE-----"));
    }

    #[test]
    fn sign_without_flags_accepts_csr_san_by_default() {
        // With neither --san nor --accept-csr-san, `sign` now drops
        // through a level-1 prompt (default Y). In test builds
        // `prompt::stdin_is_tty()` is pinned to `false`, so
        // `prompt::confirm` returns the default, which means signing
        // succeeds and the resulting cert carries the CSR's SAN. The
        // interactive path is "type 'n' to reject and bail" — covered
        // by smoke-testing the built binary.
        let scratch = tempfile::tempdir().unwrap();
        let csr_path = scratch.path().join("client.csr");
        let key_path = scratch.path().join("client.key");
        request(RequestArgs {
            cn: Some("x.example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:x.example.com".into()],
            key_bits: 2048,
            out_key: Some(key_path),
            out_csr: Some(csr_path.clone()),
        })
        .unwrap();
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("strict-ca"),
                san: vec![SanEntry::Dns("strict-ca".into())],
                key_bits: 2048,
                validity: Duration::from_secs(30 * 86400),
            },
            None,
        )
        .unwrap();
        let out_cert = scratch.path().join("out.pem");
        sign(SignArgs {
            csr_path: Some(csr_path),
            san: vec![],
            accept_csr_san: false,
            validity: Duration::from_secs(30 * 86400),
            ca_dir: Some(ca_dir),
            out: Some(out_cert.clone()),
            no_id_map: true,
        })
        .unwrap();
        // The (true, false) branch went through the prompt-default-Y
        // path: same code as `accept_csr_san=true`, so it would have
        // bailed pre-change with "must pass either --san or
        // --accept-csr-san". Output cert is a real PEM X.509.
        netidx_conf::tls::validate_pem_cert_file(&out_cert).unwrap();
    }

    #[test]
    fn sign_without_flags_bails_when_csr_has_no_san() {
        // The "no SAN to inherit" branch — there's nothing to default
        // to, so the confirm-prompt path is skipped and we bail with
        // a clear "pass --san …" message regardless of TTY.
        let scratch = tempfile::tempdir().unwrap();
        // Build a CSR with no SAN by going through generate_csr directly
        // (request() always wires up dns:<cn> by default).
        let kr = ca::generate_csr(&Subject::cn("no-san"), &[], 2048, None).unwrap();
        let csr_path = scratch.path().join("no-san.csr");
        std::fs::write(&csr_path, &kr.csr_pem).unwrap();
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("strict-ca"),
                san: vec![SanEntry::Dns("strict-ca".into())],
                key_bits: 2048,
                validity: Duration::from_secs(30 * 86400),
            },
            None,
        )
        .unwrap();
        let err = sign(SignArgs {
            csr_path: Some(csr_path),
            san: vec![],
            accept_csr_san: false,
            validity: Duration::from_secs(30 * 86400),
            ca_dir: Some(ca_dir),
            out: Some(scratch.path().join("out.pem")),
            no_id_map: true,
        })
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("--san"), "error should mention --san: {msg}");
    }

    #[test]
    fn sign_refuses_both_san_and_accept_flag() {
        let scratch = tempfile::tempdir().unwrap();
        let csr_path = scratch.path().join("client.csr");
        let key_path = scratch.path().join("client.key");
        request(RequestArgs {
            cn: Some("x.example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:x.example.com".into()],
            key_bits: 2048,
            out_key: Some(key_path),
            out_csr: Some(csr_path.clone()),
        })
        .unwrap();
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("conflict-ca"),
                san: vec![SanEntry::Dns("conflict-ca".into())],
                key_bits: 2048,
                validity: Duration::from_secs(30 * 86400),
            },
            None,
        )
        .unwrap();
        let err = sign(SignArgs {
            csr_path: Some(csr_path),
            san: vec!["dns:x.example.com".into()],
            accept_csr_san: true,
            validity: Duration::from_secs(30 * 86400),
            ca_dir: Some(ca_dir),
            out: Some(scratch.path().join("out.pem")),
            no_id_map: true,
        })
        .unwrap_err();
        assert!(format!("{err:#}").contains("not both"));
    }

    #[test]
    fn sanitize_filename_keeps_safe_chars_replaces_rest() {
        // Typical hostnames pass through untouched.
        assert_eq!(sanitize_filename("alice.example.com"), "alice.example.com");
        assert_eq!(sanitize_filename("host-1_test"), "host-1_test");
        // Separators and spaces become `_` — no path component can
        // escape the cwd.
        assert_eq!(sanitize_filename("a/b"), "a_b");
        assert_eq!(sanitize_filename("../etc/passwd"), ".._etc_passwd");
        assert_eq!(sanitize_filename("with space"), "with_space");
        assert_eq!(sanitize_filename("weird*<>chars"), "weird___chars");
        // Empty collapses to `_` so we never produce a bare extension.
        assert_eq!(sanitize_filename(""), "_");
    }

    #[test]
    fn default_filenames() {
        assert_eq!(
            default_csr_filename("alice.example.com"),
            PathBuf::from("alice.example.com.csr"),
        );
        assert_eq!(
            default_cert_filename(Some("alice.example.com")),
            PathBuf::from("alice.example.com.pem"),
        );
        // CN-less CSR falls back to a fixed name.
        assert_eq!(default_cert_filename(None), PathBuf::from("certificate.pem"),);
        // Slashes in the CN can't produce a traversing path.
        assert_eq!(default_csr_filename("../sneaky"), PathBuf::from(".._sneaky.csr"),);
    }

    #[test]
    fn issue_identity_into_round_trip() {
        // Stand up a tiny CA, issue an identity from it into a temp
        // dir, and confirm the files land. 2048-bit keys keep the
        // test fast; the real `issue_identity` uses the 4096 default.
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = Ca::init(
            &CaParams {
                directory: ca_dir.path().to_path_buf(),
                subject: Subject::cn("test-ca"),
                san: vec![SanEntry::Dns("test-ca".into())],
                key_bits: 2048,
                validity: Duration::from_secs(30 * 86400),
            },
            None,
        )
        .unwrap();
        let out = tempfile::tempdir().unwrap();
        let issued = issue_identity_into(
            &ca,
            "resolver.example.com",
            out.path().to_path_buf(),
            2048,
            None,
        )
        .unwrap();
        assert!(issued.certificate.exists());
        assert!(issued.private_key.exists());
        let cert = std::fs::read(&issued.certificate).unwrap();
        assert!(cert.starts_with(b"-----BEGIN CERTIFICATE-----"));
        let key = std::fs::read(&issued.private_key).unwrap();
        assert!(key.starts_with(b"-----BEGIN PRIVATE KEY-----"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                std::fs::metadata(&issued.private_key).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "issued key must be 0600");
        }
    }

    #[test]
    fn prompt_required_uses_provided_value() {
        // Sanity: when a value is provided, no prompt fires (so the
        // test is safe to run in CI where stdin is not a TTY).
        assert_eq!(
            prompt::required_string("ignored", Some("hello".to_string())).unwrap(),
            "hello"
        );
    }

    #[test]
    fn prompt_required_fails_without_tty() {
        // In test builds `prompt::stdin_is_tty()` is pinned to
        // `false`, so an omitted required arg must bail rather than
        // hang. We exercise the non-TTY branch by passing `None`.
        let r = prompt::required_string("test prompt", None);
        assert!(r.is_err());
        let msg = format!("{:#}", r.unwrap_err());
        assert!(msg.contains("not a TTY"), "should report non-TTY context: {msg}");
    }

    /// An offline CA (no conf server) is minted with exactly one signing
    /// slot — `recovery`, holding a generated password never typed — and no
    /// role admins (a role admin needs a daemon to authenticate to). The
    /// whole path runs with no prompts and no TTY. `--insecure-no-tpm`
    /// keeps it from refusing on a TPM-less CI host.
    #[test]
    fn offline_ca_init_makes_exactly_the_recovery_slot() {
        let scratch = tempfile::tempdir().unwrap();
        let dir = scratch.path().join("ca");
        let (_ca, _need) = create_vaulted_ca(NewCaOpts {
            dir: dir.clone(),
            common_name: Some("ca.example.com".into()),
            domain: Some("example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec![],
            key_bits: 2048, // test speed; production is 4096
            ca_validity: Duration::from_secs(30 * 86400),
            leaf_validity: ca::DEFAULT_LEAF_VALIDITY,
            ca_renew_threshold: ca::DEFAULT_CA_RENEW_THRESHOLD,
            admin: Some("super".into()),
            allowed_san: vec!["*.example.com".into()],
            max_validity: Duration::from_secs(730 * 86400),
            id_map_groups: vec!["users".into()],
            may_enroll_servers: Some(true),
            insecure_no_tpm: true,
            setup_server: Some(false),
            listen: None,
            listen_hint: None,
            units_dir: None,
        })
        .unwrap();
        // Exactly the recovery signing slot, and nothing else — no autorenew
        // (no daemon), no superuser role (offline).
        assert_eq!(
            netidx_conf::ca_store::CaDir::open(&dir)
                .unwrap()
                .vault
                .read()
                .signing_slot_names()
                .unwrap(),
            vec![ca_vault::RECOVERY_ADMIN.to_string()]
        );
        let admins = netidx_conf::ca_store::CaDir::open(&dir)
            .unwrap()
            .vault
            .read()
            .list_admins()
            .unwrap();
        assert_eq!(admins.len(), 1, "offline CA has only the recovery slot");
        assert_eq!(admins[0].admin, ca_vault::RECOVERY_ADMIN);
        assert_eq!(admins[0].kind, ca_vault::SlotKind::Signing);
    }

    /// `ca admin add` is gone — it must error and point the operator at
    /// `add-role` (signing slots are fixed to recovery + autorenew).
    #[test]
    fn admin_add_is_rejected_pointing_to_add_role() {
        let r = admin(AdminCmd::Add(AdminAddArgs {
            name: Some("x".into()),
            allow_san: vec![],
            max_validity: Duration::from_secs(730 * 86400),
            id_map_groups: vec![],
            may_enroll_servers: None,
            perms_scope: vec![],
            ca_dir: Some("/nonexistent".into()),
        }));
        let msg = format!("{:#}", r.unwrap_err());
        assert!(msg.contains("add-role"), "must point at add-role: {msg}");
    }
}
