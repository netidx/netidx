use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Subcommand};
use netidx_admin::{
    admin_client, admin_local,
    admin_ops::{
        self, offline as offline_ops, queue as ca_ops, revoke as revoke_ops, roster as roster_ops,
        slots as slots_ops,
    },
    admin_proto::{self, NodeKind},
    atomic,
    ca::{self, Ca, CaParams, SanEntry, Subject},
    ca_vault,
    fingerprint::{ColorMode, Fingerprint},
    offline_ca::{default_csr_filename, parse_san_one, parse_sans},
    paths, tls,
};
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};
use zeroize::Zeroizing;

use super::{
    answer_cli::{RemoteAuthFlags, make_offline_answerer},
    init, prompt, service,
};

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// create a new local CA (keyslot vault; can serve via `admin server`)
    Init(InitParams),
    /// issue a leaf certificate from a CA
    Issue(IssueArgs),
    /// sign an externally-supplied CSR file with a local CA
    Sign(SignArgs),
    /// print a CSR's contents (CN, key bits, SAN) without signing it
    InspectCsr(InspectCsrArgs),
    /// list the pending enrollment queue (each request keyed by its code)
    Queue(QueueArgs),
    /// approve one pending enrollment request by its code (or `--renewals` to
    /// approve the verified-renewal batch)
    Approve(ApproveArgs),
    /// deny one pending enrollment request by its code
    Deny(DenyArgs),
    /// list local CAs
    List,
    /// manage CA admin keyslots (add / revoke / set-policy / list)
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
    /// show a CA's fingerprint + identicon for out-of-band verification — this
    /// host's own CA, or (given an `ip:port`) the identity a remote admin
    /// server presents
    Fingerprint(FingerprintArgs),
    /// list the CA's issued certificates (serial, name, glyph, expiry, status)
    Issued(IssuedArgs),
    /// revoke certificate(s) by serial or name and re-sign the CRL
    Revoke(RevokeArgs),
    /// set up or rotate the auto-approve slot, so the running admin server
    /// approves verified renewals in-process (no human per renewal)
    AutoApprove(AutoApproveArgs),
    /// manage the off-box recovery credential (rotate it on the CA box)
    Recovery {
        #[command(subcommand)]
        cmd: RecoveryCmd,
    },
    /// manage an externally-signed (intermediate) CA: (re-)emit its CSR or
    /// install a signed certificate
    External {
        #[command(subcommand)]
        cmd: ExternalCmd,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum ExternalCmd {
    /// (Re-)emit a CSR for the CA certificate for your PKI to sign.
    EmitCsr(ExternalDirArgs),
    /// Install a signed certificate (first install also finishes
    /// admin-server setup; later installs renew the cert).
    Install(ExternalInstallArgs),
    /// Show the external-CA state (externally-signed? cert installed?
    /// awaiting a signature?).
    Status(ExternalDirArgs),
    /// Compatibility shim: with no argument dispatches to `emit-csr`, with
    /// a signed certificate to `install`.
    Renew(ExternalRenewArgs),
}

#[derive(Args, Debug)]
pub(crate) struct ExternalDirArgs {
    /// Override the CA directory (defaults to `${basedir}/ca/`).
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    #[command(flatten)]
    pub recovery: RecoveryAuth,
}

#[derive(Args, Debug)]
pub(crate) struct ExternalInstallArgs {
    /// The externally-signed CA certificate to install.
    pub signed_cert: PathBuf,
    /// The external root that signed the CA cert, when it is not included
    /// as a trailing PEM block in the signed-certificate file.
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// Override the CA directory (defaults to `${basedir}/ca/`).
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    #[command(flatten)]
    pub recovery: RecoveryAuth,
}

#[derive(Args, Debug)]
pub(crate) struct ExternalRenewArgs {
    /// The externally-signed CA certificate to install. Omit to (re-)emit
    /// a CSR for your PKI to sign.
    pub signed_cert: Option<PathBuf>,
    /// The external root that signed the CA cert, when it is not included
    /// as a trailing PEM block in the signed-certificate file.
    #[arg(long)]
    pub root: Option<PathBuf>,
    /// Override the CA directory (defaults to `${basedir}/ca/`).
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    #[command(flatten)]
    pub recovery: RecoveryAuth,
}

#[derive(Subcommand, Debug)]
pub(crate) enum RecoveryCmd {
    /// mint a fresh recovery password on the CA box (authorized by the
    /// box's autorenew keytab, so a lost recovery password is recoverable
    /// while the machine lives). The new password is printed once.
    Rotate(RecoveryRotateArgs),
    /// show whether the CA has a recovery slot and whether the on-box
    /// authority (the autorenew keytab) needed to rotate it is present.
    Status(RecoveryRotateArgs),
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
    /// response to a leaked keytab. Restart the admin server afterwards to
    /// pick up the new credential.
    #[arg(long)]
    pub rotate: bool,
    /// Show the autorenew credential's state (slot present? keytab present /
    /// sealed? wired in the config?) instead of setting it up.
    #[arg(long)]
    pub status: bool,
    /// Proceed even when this host has no usable TPM / Secure Enclave.
    /// DANGER: the autorenew keytab is then written in PLAINTEXT — every
    /// backup or disk image of this machine becomes a CA compromise.
    #[arg(long = "insecure-no-tpm")]
    pub insecure_no_tpm: bool,
    #[command(flatten)]
    pub recovery: RecoveryAuth,
}

#[derive(Args, Debug)]
pub(crate) struct QueueArgs {
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct ApproveArgs {
    /// The request code to approve (as shown by `ca queue`; the code groups
    /// may be passed space-separated without quoting). Selects and asserts
    /// the request — a mismatch or ambiguous prefix is refused. Omit with
    /// `--renewals`.
    #[arg(value_name = "CODE", num_args = 1.., conflicts_with = "renewals")]
    code: Vec<String>,
    /// Approve every verified renewal in one batch. A verified renewal is a
    /// cryptographic proof of possession of the live key for the same name,
    /// so there is no code to match; id-map groups stay as they were.
    #[arg(long)]
    renewals: bool,
    /// Register the new identity in these id-map groups (repeatable; the
    /// first is primary). Defaults to the per-kind default; ignored for a
    /// server enrollment or `--renewals`.
    #[arg(long = "id-map-group")]
    id_map_group: Vec<String>,
    /// Do not register the new identity in the local id-map.
    #[arg(long = "no-id-map", conflicts_with = "id_map_group")]
    no_id_map: bool,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct DenyArgs {
    /// The request code to deny (as shown by `ca queue`).
    #[arg(value_name = "CODE", num_args = 1..)]
    code: Vec<String>,
    /// The reason shown to the waiting enrollee.
    #[arg(long = "reason")]
    reason: String,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct IssuedArgs {
    /// Include already-revoked certificates in the listing.
    #[arg(long)]
    all: bool,
    /// Only show certificates whose name contains this substring
    /// (case-insensitive).
    #[arg(long)]
    name: Option<String>,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct RevokeArgs {
    /// What to revoke: a serial number (exactly one certificate) or a name
    /// (every live certificate carrying it — when they share one key). Read
    /// the serial or glyph off `ca issued`.
    #[arg(value_name = "ID-OR-NAME")]
    target: String,
    /// The revocation reason, recorded in the index.
    #[arg(long = "reason")]
    reason: String,
    /// Require every target to carry this SPKI glyph (as shown by `ca issued`);
    /// refuse on any mismatch. Needed to revoke a name that spans >1 key.
    #[arg(long = "assert-glyph")]
    assert_glyph: Option<String>,
    #[command(flatten)]
    auth: RemoteAuthFlags,
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
    /// Which CA to list. With `--server` this is a remote admin plane
    /// (glyph-confirmed, `may_manage_admins` admin); without it, this host's
    /// own admin server over its local control socket.
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

/// The policy knobs shared by `ca admin add-role` and `set-policy`. Booleans are
/// `Option`: supplied → used; omitted → the answerer decides (required in
/// non-interactive mode). Empty `--allow-san` / `--id-map-group` take the
/// per-domain defaults.
#[derive(Args, Debug)]
pub(crate) struct PolicyFlags {
    /// SAN glob this admin may issue (repeatable). Defaults to `*.<domain>`.
    #[arg(long = "allow-san", num_args = 1)]
    allow_san: Vec<String>,
    /// Max validity this admin may issue (e.g. 730d, 10m). Default 730d.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    max_validity: Duration,
    /// id-map groups this admin may assign when enrolling (repeatable; first is
    /// primary; an explicit empty string disables registration).
    #[arg(long = "id-map-group", num_args = 1)]
    id_map_groups: Vec<String>,
    /// Whether this admin may enroll new admin servers (required
    /// non-interactively).
    #[arg(long)]
    may_enroll_servers: Option<bool>,
    /// Whether this admin may manage the roster — add / rescope / remove admins
    /// (required non-interactively). The CA still enforces no-escalation.
    #[arg(long)]
    may_manage_admins: Option<bool>,
    /// Netidx path this admin may edit perms under (repeatable, e.g. /eu).
    #[arg(long = "perms-scope", num_args = 1)]
    perms_scope: Vec<String>,
    /// Netidx path this admin may control services under (repeatable).
    #[arg(long = "service-scope", num_args = 1)]
    service_scope: Vec<String>,
}

impl PolicyFlags {
    fn inputs(&self) -> roster_ops::PolicyInputs<'_> {
        roster_ops::PolicyInputs {
            allow_san: &self.allow_san,
            max_validity: self.max_validity,
            id_map_groups: &self.id_map_groups,
            may_enroll_servers: self.may_enroll_servers,
            may_manage_admins: self.may_manage_admins,
            perms_scope: &self.perms_scope,
            service_scope: &self.service_scope,
        }
    }
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
    /// Whether this admin may enroll new admin servers. Prompted when
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
    /// Name of the new role admin.
    #[arg(value_name = "NAME")]
    name: String,
    /// Read the new role admin's initial password from a file (never on the
    /// command line).
    #[arg(long = "new-password-file")]
    new_password_file: PathBuf,
    #[command(flatten)]
    policy: PolicyFlags,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct AdminSetPolicyArgs {
    /// Name of the admin whose policy to replace (wholesale).
    #[arg(value_name = "NAME")]
    name: String,
    #[command(flatten)]
    policy: PolicyFlags,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct AdminRemoveArgs {
    /// Name of the role admin to remove.
    #[arg(value_name = "NAME")]
    name: String,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct FingerprintArgs {
    /// Admin server address (`ip:port`) to fetch and display the glyph for —
    /// the network identity to verify out of band before enrolling against it
    /// (pass the confirmed value to a join's `--accept-glyph`). When omitted,
    /// show this host's own local CA glyph.
    pub server: Option<SocketAddr>,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct JoinArgs {
    /// Admin server address (`ip:port`). When omitted, discovered over
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
    /// Default validity for certs this CA issues (e.g. the admin server's
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
    /// Whether the superuser may enroll new admin servers. Prompted
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
    /// Run the CA as an intermediate: generate the key + a CSR requesting a
    /// CA cert, then stop. Get the CSR signed by your existing PKI and
    /// install it with `ca external install <signed-cert>`. The CA cert will
    /// NOT auto-renew (netidx does not hold your PKI's key).
    #[arg(long = "external-sign")]
    pub external_sign: bool,
}

/// The recovery-password flags shared by the offline CA-vault commands
/// (`ca sign` / `ca issue`). The CA unlocks with the box's autorenew keytab
/// when present, so these are only consulted on a fall back to the off-box
/// recovery password (or a legacy encrypted key).
#[derive(Args, Debug)]
pub(crate) struct RecoveryAuth {
    /// Read the CA recovery password from a file (never on the command line).
    /// Only needed when the box's autorenew keytab can't unlock the CA.
    #[arg(long = "recovery-password-file")]
    pub recovery_password_file: Option<PathBuf>,
    /// Read the CA recovery password from stdin.
    #[arg(long = "recovery-password-stdin", conflicts_with = "recovery_password_file")]
    pub recovery_password_stdin: bool,
}

impl RecoveryAuth {
    fn answerer(&self) -> Result<super::answer_cli::FlagAnswerer> {
        make_offline_answerer(
            self.recovery_password_file.as_deref(),
            self.recovery_password_stdin,
        )
    }
}

#[derive(Args, Debug)]
pub(crate) struct IssueArgs {
    /// Common Name for the issued cert. Required.
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
    /// Where to write the issued `private.key` + `certificate.pem`. Required.
    #[arg(short, long = "out")]
    pub out_dir: Option<PathBuf>,
    #[command(flatten)]
    pub recovery: RecoveryAuth,
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
    /// file, use `netidx admin ca approve`.)
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
    /// Skip the post-sign id-map registration entirely. Interactive
    /// sessions otherwise prompt (when a local id-map exists); scripts
    /// register explicitly with `--id-map-group`.
    #[arg(long)]
    pub no_id_map: bool,
    /// Register the signed identity in the local id-map under these groups
    /// (repeatable; the first is primary). Enables non-interactive id-map
    /// registration; requires `--uid`. Omit to prompt (interactive) or skip
    /// (strict).
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_group: Vec<String>,
    /// The unix uid the signed identity maps to. Required with
    /// `--id-map-group`.
    #[arg(long, requires = "id_map_group")]
    pub uid: Option<u32>,
    #[command(flatten)]
    pub recovery: RecoveryAuth,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Init(p) => init(p),
        Cmd::Issue(p) => issue(p),
        Cmd::Sign(p) => sign(p),
        Cmd::InspectCsr(p) => inspect_csr(p),
        Cmd::Queue(f) => queue(f),
        Cmd::Approve(p) => approve(p),
        Cmd::Deny(f) => deny(f),
        Cmd::List => list(),
        Cmd::Admin { cmd } => admin(cmd),
        Cmd::Fingerprint(p) => fingerprint(p),
        Cmd::Issued(f) => issued(f),
        Cmd::Revoke(p) => revoke(p),
        Cmd::AutoApprove(p) => auto_approve(p),
        Cmd::Recovery { cmd } => recovery(cmd),
        Cmd::External { cmd } => external(cmd),
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
pub(super) const AUTORENEW_ADMIN: &str = netidx_admin::admin_server::AUTORENEW_ADMIN;

fn autorenew_policy() -> ca_vault::Policy {
    netidx_admin::ca_policy::autorenew_policy()
}

/// `${config}/netidx/autorenew.keytab` — deliberately NOT in the CA
/// dir: never back this file up; recreating it is one `--rotate`. The
/// canonical definition lives in the library (offline sign/issue reads it
/// too); this delegates so the two never drift.
fn autorenew_keytab_path() -> Result<PathBuf> {
    netidx_admin::offline_ca::autorenew_keytab_path()
}

/// A long random password for the autorenew slot (256 bits, hex). The
/// autorenew slot is a master-key-wrapping signing credential, so keep its
/// plaintext in a `Zeroizing` buffer that wipes on drop (it is dropped right
/// after sealing / writing the keytab).
fn random_password() -> Zeroizing<String> {
    netidx_admin::ca_vault::random_signing_password()
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
    cadir: &netidx_admin::ca_store::CaDir,
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
                 dictionary-attack lockout) and re-run `netidx admin ca auto-approve`, or \
                 pass --insecure-no-tpm to accept a plaintext keytab (test CAs only).",
                mech = netidx_tpm::MECHANISM
            );
        }
    }
    Ok(keytab)
}

/// Set up (or rotate) the autorenew slot and point this host's
/// admin-server config at its keytab. Approval itself is the running
/// daemon's job now — it reads the keytab named here and approves
/// verified renewals in-process — so this command just manages the
/// credential. `--rotate` is the same operation framed as a leaked-keytab
/// response: [`setup_autorenew_slot`] always replaces the slot, so enable
/// and rotate share one path and differ only in what they print.
fn auto_approve(p: AutoApproveArgs) -> Result<()> {
    env_logger::init();
    let dir = ca_dir_for(None)?;
    let cfg = paths::discover_admin_server_config().ok();
    if p.status {
        let s = slots_ops::auto_approve_status(&dir, cfg.as_deref())?;
        println!("auto-approve status:");
        println!("  autorenew slot: {}", if s.slot_present { "present" } else { "absent" });
        let keytab = if !s.keytab_present {
            "absent".to_string()
        } else if s.keytab_sealed {
            format!("{} (sealed to this machine)", s.keytab.display())
        } else {
            format!("{} (PLAINTEXT — do not back up)", s.keytab.display())
        };
        println!("  keytab:         {keytab}");
        println!("  wired in config: {}", if s.wired_in_config { "yes" } else { "no" });
        return Ok(());
    }
    let mut ans = p.recovery.answerer()?;
    let out = runtime()?
        .block_on(slots_ops::auto_approve(&mut ans, dir, cfg, p.rotate, p.insecure_no_tpm))?;
    match out {
        slots_ops::AutoApproveOutcome::HotSwapped { warning } => {
            println!(
                "auto-approve rotated (hot-swapped on the running admin server, no \
                 downtime)"
            );
            if let Some(w) = warning {
                eprintln!("WARNING: {w}");
            }
        }
        slots_ops::AutoApproveOutcome::Offline { rotate, keytab, cfg_path, cfg_error } => {
            println!("auto-approve {}:", if rotate { "rotated" } else { "enabled" });
            println!("  slot:   {AUTORENEW_ADMIN:?} (empty issuance scope)");
            println!("  keytab: {} (0600 — do NOT back this file up)", keytab.display());
            match (cfg_path, cfg_error) {
                (Some(cfg_path), _) => {
                    println!("  config: {} (roles.ca.autorenew)", cfg_path.display());
                    println!("  restart the admin server to pick up the keytab.");
                }
                (None, err) => {
                    if let Some(e) = err {
                        println!("  note: could not update the admin-server config ({e}).");
                    }
                    println!("        set roles.ca.autorenew to the keytab path and");
                    println!("        restart the admin server.");
                }
            }
        }
    }
    Ok(())
}

// -- ca revoke ----------------------------------------------------------------

/// Format a unix timestamp (seconds) as a UTC date for the issued listing.
fn fmt_unix(secs: u64) -> String {
    use chrono::{DateTime, Utc};
    match DateTime::<Utc>::from_timestamp(secs as i64, 0) {
        Some(dt) => dt.format("%Y-%m-%d").to_string(),
        None => format!("@{secs}"),
    }
}

/// `ca issued` — list the CA's issued-certificate index (the query half of
/// revocation). Read a serial or glyph off this to feed `ca revoke`.
fn issued(f: IssuedArgs) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let entries = runtime()?.block_on(revoke_ops::issued(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        f.all,
        f.name.as_deref(),
    ))?;
    if entries.is_empty() {
        println!("no matching certificates in the index");
        return Ok(());
    }
    println!("issued certificates:");
    for e in &entries {
        let status = if e.revoked { "REVOKED" } else { "live" };
        let name = if e.name.is_empty() { "(no name)" } else { &e.name };
        println!(
            "  serial {}  {}  [{}]  expires {}",
            e.serial,
            name,
            status,
            fmt_unix(e.not_after_unix),
        );
        println!("    glyph {}", e.spki_fp);
    }
    println!(
        "\nrevoke with `netidx admin ca revoke <serial-or-name> --reason <text>` \
         (add --assert-glyph <glyph> to pin a key)."
    );
    Ok(())
}

/// `ca revoke <id-or-name> --reason <text>` — revoke certificate(s) and re-sign
/// the CRL over RPC (the daemon owns the CA index and CRL). `<id-or-name>` is a
/// serial (one cert) or a name (every live cert for it, when they share one
/// key); `--assert-glyph` pins the intended key. Irreversible.
///
/// CR claude for estokes: the old local-file revoke also (a) copied the fresh
/// CRL beside this host's resolver for instant enforcement and (b) offered to
/// drop the revoked identity from the local id-map. Both needed direct file
/// access the CLI no longer has now the daemon owns the CA. The CRL still
/// re-signs (in the daemon) and distributes via `GetCrl`; the local-resolver
/// fast-path and the id-map cleanup remain dropped (unchanged from the prior
/// RPC revoke). If we want them back they belong in the daemon's `handle_revoke`.
fn revoke(f: RevokeArgs) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let assert_glyph = f
        .assert_glyph
        .as_deref()
        .map(Fingerprint::parse_text)
        .transpose()
        .context("parsing --assert-glyph")?;
    let selector = revoke_ops::parse_selector(&f.target);
    let out = runtime()?.block_on(revoke_ops::revoke(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        selector,
        assert_glyph,
        &f.reason,
    ))?;
    println!(
        "revoked {} certificate(s); the daemon re-signed the CRL:",
        out.revoked.len()
    );
    for e in &out.revoked {
        let name = if e.name.is_empty() { "(no name)" } else { &e.name };
        println!("  serial {}  {}", e.serial, name);
    }
    for w in out.warnings {
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

/// Seal a freshly generated CA key into the vault's `recovery` slot and
/// persist the lifetime policy, under one flock held for the rest of init.
/// Shared by the self-signed [`create_vaulted_ca`] and the external-sign
/// bootstrap. On a mid-write failure, roll back whatever init committed so
/// the dir isn't a keyless half-CA that blocks a clean retry.
fn seal_ca_recovery(
    dir: &Path,
    key_pem: &Zeroizing<Vec<u8>>,
    lifetimes: ca::CaLifetimes,
) -> Result<(Zeroizing<String>, netidx_admin::ca_store::CaDir)> {
    let recovery_pw = ca_vault::gen_recovery_password();
    let cadir = netidx_admin::ca_store::CaDir::open(dir)
        .context("opening the new CA directory")?;
    if let Err(e) = cadir.vault.write().create(
        key_pem,
        ca_vault::RECOVERY_ADMIN,
        &recovery_pw,
        recovery_policy(),
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
    // Seal the key into the recovery slot and persist the lifetime policy
    // (self-signed CA — externally_signed is false). One flock is held for
    // the rest of init.
    let (recovery_pw, cadir) = seal_ca_recovery(
        &opts.dir,
        &key_pem,
        ca::CaLifetimes {
            leaf_validity: opts.leaf_validity,
            ca_renew_threshold: opts.ca_renew_threshold,
            externally_signed: false,
        },
    )?;

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
            "set up the admin server (so nodes can discover the network and \
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
        let cadir = netidx_admin::ca_store::CaDir::open(&opts.dir)
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
        println!(
            "          rotate anytime with `netidx admin ca auto-approve --rotate`)"
        );
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
    netidx_admin::ca_policy::recovery_policy()
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
    println!("`netidx admin ca recovery rotate`.");
    println!();
}

/// Create the founding superuser ROLE admin (operator names it + sets its
/// password). Full authority — broad issuance scope, may enroll servers,
/// edits perms anywhere, and manages other admins — yet it wraps no master
/// key, so its password can never unlock the CA. Only minted for a server
/// CA (a role admin authenticates to the daemon).
fn setup_superuser(
    cadir: &netidx_admin::ca_store::CaDir,
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

/// `admin ca recovery {rotate,status}`: mint a fresh recovery password on the
/// CA box (authorized by the box's own autorenew keytab, so a lost recovery
/// password is recoverable while the machine lives), or report the recovery
/// slot's state.
fn recovery(cmd: RecoveryCmd) -> Result<()> {
    match cmd {
        RecoveryCmd::Rotate(a) => recovery_rotate(a),
        RecoveryCmd::Status(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let s = slots_ops::recovery_status(&dir)?;
            println!("recovery status:");
            println!("  recovery slot:  {}", if s.slot_present { "present" } else { "absent" });
            println!(
                "  autorenew keytab (offline re-mint authority): {}",
                if s.keytab_present { "present" } else { "absent" }
            );
            Ok(())
        }
    }
}

fn recovery_rotate(a: RecoveryRotateArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    let cfg = paths::discover_admin_server_config().ok();
    // The library shows the new password once (via the Answerer); we only
    // narrate which path ran. The recovery-rotate offline path authorizes with
    // the autorenew keytab, not a typed recovery password, so a bare
    // FlagAnswerer (no secret) is all the CLI needs.
    let mut ans = make_offline_answerer(None, false)?;
    let out = runtime()?.block_on(slots_ops::recovery_rotate(&mut ans, dir, cfg))?;
    match out {
        slots_ops::RecoveryRotateOutcome::HotSwapped => {
            println!("rotated the recovery password (via the running admin server)")
        }
        slots_ops::RecoveryRotateOutcome::Offline { ca_dir } => {
            println!("rotated the recovery password for the CA at {}", ca_dir.display())
        }
    }
    Ok(())
}

// -- ca external (intermediate CA signed by an external PKI) -----------------

fn external(cmd: ExternalCmd) -> Result<()> {
    match cmd {
        ExternalCmd::EmitCsr(a) => external_emit_csr(a),
        ExternalCmd::Install(a) => external_install(a),
        ExternalCmd::Status(a) => external_status(a),
        ExternalCmd::Renew(a) => external_renew(a),
    }
}

/// Refuse the `external` ops on a CA that isn't externally-signed, with the
/// same clear pointer the overloaded `renew` gave.
fn ensure_externally_signed(dir: &Path) -> Result<()> {
    if !ca::CaLifetimes::load(dir)?.externally_signed {
        bail!(
            "{} is not an externally-signed CA — create one with \
             `netidx admin ca init --external-sign`",
            dir.display()
        );
    }
    Ok(())
}

fn external_emit_csr(a: ExternalDirArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    ensure_externally_signed(&dir)?;
    let mut ans = a.recovery.answerer()?;
    let csr_path = runtime()?.block_on(slots_ops::external_emit_csr(&mut ans, dir))?;
    println!("wrote {} — get it signed by your PKI, then run:", csr_path.display());
    println!("  netidx admin ca external install <signed-cert.pem> [--root <root.pem>]");
    Ok(())
}

fn external_install(a: ExternalInstallArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    ensure_externally_signed(&dir)?;
    let mut ans = a.recovery.answerer()?;
    let out = runtime()?.block_on(slots_ops::external_install_cert(
        &mut ans,
        dir,
        &a.signed_cert,
        a.root.as_deref(),
    ))?;
    report_external_install(out)
}

fn report_external_install(out: slots_ops::ExternalInstallOutcome) -> Result<()> {
    use slots_ops::ExternalInstallOutcome;
    match out {
        ExternalInstallOutcome::OfflineCa => {
            println!("installed the externally-signed CA certificate.");
            println!("CA-cert auto-renewal is DISABLED (external issuer).");
            Ok(())
        }
        ExternalInstallOutcome::FirstInstall { need, cfg_path } => {
            println!("installed the externally-signed CA certificate.");
            println!("admin server configured ({})", cfg_path.display());
            println!(
                "CA-cert auto-renewal is DISABLED (external issuer); re-run \
                 `netidx admin ca external install` when your PKI re-signs it."
            );
            service::offer(
                need,
                service::ServiceGate { dry_run: false, no_service: false, with_service: false },
            )
        }
        ExternalInstallOutcome::Renewal => {
            println!("renewed the CA certificate — enrolled nodes adopt it on their");
            println!("next renewal (glyph unchanged; existing certificates stay valid).");
            Ok(())
        }
    }
}

fn external_status(a: ExternalDirArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    let s = slots_ops::external_status(&dir)?;
    println!("external CA status:");
    println!("  externally signed: {}", if s.externally_signed { "yes" } else { "no" });
    println!(
        "  certificate:       {}",
        if s.cert_installed { "installed" } else { "not installed" }
    );
    match &s.pending {
        Some((cn, domain)) => println!(
            "  pending:           awaiting a signed cert for {cn:?} (domain {domain})"
        ),
        None => println!("  pending:           none"),
    }
    Ok(())
}

/// Compatibility shim for the old overloaded `ca external renew`: no argument
/// (re-)emits a CSR; a signed certificate installs it.
fn external_renew(a: ExternalRenewArgs) -> Result<()> {
    match a.signed_cert {
        None => external_emit_csr(ExternalDirArgs { ca_dir: a.ca_dir, recovery: a.recovery }),
        Some(signed_cert) => external_install(ExternalInstallArgs {
            signed_cert,
            root: a.root,
            ca_dir: a.ca_dir,
            recovery: a.recovery,
        }),
    }
}

/// Phase 1 of `ca init --external-sign`: generate the CA key + a CSR for its
/// certificate, seal the vault (recovery slot always; for a served CA also
/// the box autorenew slot and the superuser role slot), mark the CA
/// externally-signed, and write the CSR + a marker. No `certificate.pem` is
/// written — its absence is the "awaiting external cert" state. Returns no
/// `ServiceNeed`: the server is stood up in phase 2, once the cert exists.
fn external_bootstrap(opts: NewCaOpts) -> Result<service::ServiceNeed> {
    let common_name = resolve_ca_cn(opts.common_name.clone(), opts.domain.as_deref())?;
    let domain = match &opts.domain {
        Some(d) if !d.is_empty() => d.clone(),
        _ => match common_name.split_once('.') {
            Some((_, d)) if !d.is_empty() => d.to_string(),
            _ => common_name.clone(),
        },
    };
    let set_up_server = match opts.setup_server {
        Some(b) => b,
        None => prompt::confirm(
            "set up the admin server (so nodes can discover the network and \
             request certs over it)?",
            true,
        )?,
    };
    // The TPM gate matters only for a served CA — the autorenew keytab is
    // the sole TPM-sealed artifact; an offline external CA has none.
    if set_up_server {
        tpm_gate(opts.insecure_no_tpm)?;
    }
    let san = parse_sans(&opts.san, &common_name)?;
    let (key_pem, csr_pem) = Ca::init_vaulted_external(&CaParams {
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
    let (recovery_pw, cadir) = seal_ca_recovery(
        &opts.dir,
        &key_pem,
        ca::CaLifetimes {
            leaf_validity: opts.leaf_validity,
            ca_renew_threshold: opts.ca_renew_threshold,
            externally_signed: true,
        },
    )?;
    println!(
        "created the CA key at {} (awaiting an externally-signed certificate)",
        opts.dir.display()
    );
    print_recovery_password(&recovery_pw);
    // Write the CSR and the marker BEFORE the fallible/interactive slot
    // setup, so an interrupted bootstrap leaves a CA that `ca external
    // renew` can continue rather than a dead-ended half-CA.
    let csr_path = default_csr_filename(&common_name);
    atomic::write_atomic(&csr_path, &csr_pem, 0o644)
        .with_context(|| format!("writing CSR to {}", csr_path.display()))?;
    slots_ops::ExternalPending {
        cn: common_name.clone(),
        domain,
        country: opts.country.clone(),
        state: opts.state.clone(),
        locality: opts.locality.clone(),
        organization: opts.organization.clone(),
        san: opts.san.clone(),
        setup_server: set_up_server,
        listen: opts.listen,
        units_dir: opts.units_dir.clone(),
    }
    .store(&opts.dir)?;
    if set_up_server {
        // Mint the box autorenew slot now (it needs the recovery password,
        // which we hold here) so phase 2 can unlock passwordlessly; it is
        // wired to the server config in phase 2. The superuser role slot
        // needs no CA cert, so it is minted here too.
        let keytab = setup_autorenew_slot(&cadir, &recovery_pw, opts.insecure_no_tpm)?;
        println!("provisioned the automatic-renewal (leaf) approval slot:");
        println!(
            "  slot:   {AUTORENEW_ADMIN:?} (empty scope; wired to the server in phase 2)"
        );
        println!("  keytab: {} (0600 — do NOT back this file up)", keytab.display());
        setup_superuser(&cadir, &opts, &common_name)?;
    }
    println!();
    println!(
        "wrote {} — get it signed by your PKI as a subordinate CA, then run:",
        csr_path.display()
    );
    println!("  netidx admin ca external install <signed-cert.pem> [--root <root.pem>]");
    println!();
    println!(
        "NOTE: an externally-signed CA certificate does NOT auto-renew (netidx \
         does not hold your PKI's key)."
    );
    // Phase 1 stands up no server yet, so there is nothing to offer.
    Ok(service::ServiceNeed::NONE)
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
    let opts = NewCaOpts {
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
    };
    // `--external-sign` runs the CA as an intermediate: phase 1 makes the
    // key + a CSR and stops; `ca external install <signed-cert>` installs the
    // signed cert. Otherwise this is the normal self-signed CA.
    let need = if p.external_sign {
        external_bootstrap(opts)?
    } else {
        create_vaulted_ca(opts)?.1
    };

    // Single end-of-process hook — the same one the `admin install`
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

/// Read the new role admin's initial password from `--new-password-file` (never
/// on argv) into a [`Secret`].
fn read_new_password(path: &Path) -> Result<admin_proto::Secret> {
    let s = std::fs::read_to_string(path)
        .with_context(|| format!("reading --new-password-file {}", path.display()))?;
    Ok(admin_proto::Secret(s.trim_end_matches(['\n', '\r']).to_string()))
}

/// The CA CN + domain that seed the `*.<domain>` SAN suggestion: a remote CA
/// reports its domain in the pinned identity; a local CA is read off its own
/// certificate.
fn policy_context(
    target: &admin_ops::AdminTarget,
    ca_dir: Option<&Path>,
) -> (String, Option<String>) {
    match target {
        admin_ops::AdminTarget::Remote { session } => {
            let domain = session.identity.domain.to_string();
            (default_ca_cn(&domain), Some(domain))
        }
        admin_ops::AdminTarget::Local { .. } => {
            let dir =
                ca_dir.map(Path::to_path_buf).or_else(|| paths::user_ca_dir().ok());
            let cn = dir.map(|d| existing_ca_cn(&d)).unwrap_or_default();
            (cn, None)
        }
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
                 `netidx admin ca admin add-role <name>` — a role admin edits perms, \
                 manages admins, and (with --may-enroll-servers) enrolls servers, all \
                 without ever unlocking the CA key (the server signs for it)."
            )
        }
        AdminCmd::AddRole(a) => admin_add_role(a),
        AdminCmd::SetPolicy(a) => admin_set_policy(a),
        AdminCmd::Remove(a) => admin_remove(a),
        AdminCmd::List(a) => admin_list(a),
    }
}

/// `ca admin add-role <name>` — mint a role admin (local control socket or
/// pinned remote plane; the CA enforces no-escalation remotely).
fn admin_add_role(a: AdminAddRoleArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(admin_ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    let (cn, domain) = policy_context(&target, a.auth.ca_dir.as_deref());
    let policy =
        rt.block_on(roster_ops::gather_policy(&mut ans, a.policy.inputs(), &cn, domain.as_deref()))?;
    let new_password = read_new_password(&a.new_password_file)?;
    rt.block_on(roster_ops::add_role_admin(&target, &a.name, &new_password, policy))?;
    report_admin_target("added role admin", &a.name, &target);
    Ok(())
}

/// `ca admin set-policy <name>` — replace an admin's policy wholesale.
fn admin_set_policy(a: AdminSetPolicyArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(admin_ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    let (cn, domain) = policy_context(&target, a.auth.ca_dir.as_deref());
    let policy =
        rt.block_on(roster_ops::gather_policy(&mut ans, a.policy.inputs(), &cn, domain.as_deref()))?;
    rt.block_on(roster_ops::set_admin_policy(&target, &a.name, policy))?;
    report_admin_target("updated policy for admin", &a.name, &target);
    Ok(())
}

/// `ca admin remove <name>` — revoke a role admin (the daemon's last-manager and
/// reserved-slot guards still apply).
fn admin_remove(a: AdminRemoveArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(admin_ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    rt.block_on(roster_ops::remove_admin(&target, &a.name))?;
    report_admin_target("removed role admin", &a.name, &target);
    Ok(())
}

/// `ca admin list` — the roster (tier + policy per admin).
fn admin_list(a: AdminScopeArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(admin_ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    let admins = rt.block_on(roster_ops::list_admins(&target))?;
    print_admin_list(&admins);
    Ok(())
}

/// Report a roster mutation, naming which CA it hit.
fn report_admin_target(what: &str, name: &str, target: &admin_ops::AdminTarget) {
    match target {
        admin_ops::AdminTarget::Remote { session } => {
            println!("{what} {name:?} on the CA at {}", session.server)
        }
        admin_ops::AdminTarget::Local { .. } => {
            println!("{what} {name:?} (via the local admin server)")
        }
    }
}

// -- ca fingerprint -----------------------------------------------------------

fn fingerprint(p: FingerprintArgs) -> Result<()> {
    match p.server {
        // Remote: fetch the identity the admin server presents and show its
        // glyph, so an operator can verify it out of band before enrolling.
        Some(addr) => {
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            let identity = rt
                .block_on(admin_client::fetch_identity(addr, NodeKind::Client))
                .with_context(|| format!("contacting admin server {addr}"))?;
            init::show_network_identity(addr, &identity);
            Ok(())
        }
        // Local: this host's own CA.
        None => {
            let dir = ca_dir_for(p.ca_dir)?;
            show_ca_identity(&dir)
        }
    }
}

// -- ca join (the client) -----------------------------------------------------

pub(crate) fn join(p: JoinArgs) -> Result<()> {
    use super::init::{self, AdminServers};
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let (server, identity) = match p.server {
        // An explicit address: confirm WHO we've reached before any
        // credential is entered. `fetch_identity` sends nothing secret
        // and closes before returning.
        Some(server) => {
            let identity = rt
                .block_on(admin_client::fetch_identity(server, NodeKind::Client))
                .with_context(|| format!("contacting admin server {server}"))?;
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
            AdminServers::Have(net) => {
                let ca = net.info.ca_addr.ok_or_else(|| {
                    anyhow!(
                        "network {:?} reported no CA; cannot request a certificate",
                        net.identity.domain
                    )
                })?;
                (ca, net.identity)
            }
            AdminServers::DontHave => {
                bail!("no admin server found or selected; pass --server to specify one")
            }
            AdminServers::NotProbed => {
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
    let issued = rt.block_on(admin_client::request_cert(
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
            "may this admin enroll new admin servers (more privileged than any \
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
    netidx_admin::tls::extract_dns_san_from_pem(&dir.join("certificate.pem"))
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
    let cn = p.cn.ok_or_else(|| anyhow!("--cn is required (the certificate common name)"))?;
    let out_dir = p
        .out_dir
        .ok_or_else(|| anyhow!("--out is required (the output dir for key + cert)"))?;
    let san = parse_sans(&p.san, &cn)?;
    let subject = Subject {
        common_name: cn,
        country: p.country,
        state: p.state,
        locality: p.locality,
        organization: p.organization,
    };
    let mut ans = p.recovery.answerer()?;
    // Leaf key encryption is wired through the install flow (`netidx admin
    // init`), where the engine knows how to plumb an askpass entry into the
    // emitted client config. The bare `ca issue` CLI deliberately stays
    // unencrypted: callers here are doing manual cert issuance and don't
    // necessarily have a netidx config to receive the askpass.
    let out = runtime()?.block_on(offline_ops::ca_issue(
        &mut ans,
        directory,
        subject,
        san,
        p.key_bits,
        p.validity,
        out_dir,
        None,
    ))?;
    println!("issued cert:");
    println!("  cn:          {}", out.cn);
    println!("  private key: {}", out.private_key.display());
    println!("  certificate: {}", out.certificate.display());
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
        // wired through `netidx admin init`, which knows how to set
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
    println!("#   netidx admin ca sign {} --out <cert.pem>", out_csr.display());
    Ok(())
}

fn sign(mut p: SignArgs) -> Result<()> {
    let csr_path = p
        .csr_path
        .take()
        .ok_or_else(|| anyhow!("a CSR path is required (the PEM CSR to sign)"))?;
    let directory = ca_dir_for(p.ca_dir.take())?;
    let csr_pem = std::fs::read(&csr_path)
        .with_context(|| format!("reading CSR {}", csr_path.display()))?;
    let san = sign_san_choice(&p)?;
    let id_map = id_map_choice(&p);
    let mut ans = p.recovery.answerer()?;
    let out = runtime()?.block_on(offline_ops::ca_sign(
        &mut ans,
        directory,
        csr_pem,
        san,
        p.validity,
        p.out.take(),
        id_map,
    ))?;
    print_sign_outcome(&csr_path, &out);
    Ok(())
}

/// Build the SAN choice from `--san` / `--accept-csr-san`, preserving the
/// mutually-exclusive decision table (the library resolves what `Ask` means).
fn sign_san_choice(p: &SignArgs) -> Result<offline_ops::SignSan> {
    Ok(match (p.san.is_empty(), p.accept_csr_san) {
        (false, false) => {
            let san = p.san.iter().map(|s| parse_san_one(s)).collect::<Result<Vec<_>>>()?;
            offline_ops::SignSan::Explicit(san)
        }
        (false, true) => bail!(
            "pass either --san <kind>:<value> (one or more) or --accept-csr-san, not both"
        ),
        (true, true) => offline_ops::SignSan::InheritCsr,
        (true, false) => offline_ops::SignSan::Ask,
    })
}

/// Build the post-sign id-map action from `--no-id-map` / `--id-map-group` /
/// `--uid` (`--uid` requires `--id-map-group`, enforced by clap).
fn id_map_choice(p: &SignArgs) -> offline_ops::IdMapAction {
    if p.no_id_map {
        offline_ops::IdMapAction::Skip
    } else if !p.id_map_group.is_empty() {
        offline_ops::IdMapAction::Register { groups: p.id_map_group.clone(), uid: p.uid }
    } else {
        offline_ops::IdMapAction::Ask
    }
}

fn print_sign_outcome(csr_path: &Path, out: &offline_ops::SignOutcome) {
    print_csr_summary(csr_path, &out.summary);
    println!("  signing SAN:");
    for entry in &out.san {
        println!("    - {}", san_display(entry));
    }
    println!("\nsigned cert (0644): {}", out.out.display());
    print_id_map_result(&out.id_map);
}

fn print_id_map_result(r: &offline_ops::IdMapResult) {
    use offline_ops::IdMapResult;
    match r {
        IdMapResult::NotRequested | IdMapResult::Declined => {}
        IdMapResult::NoIdentityName => {
            println!("(no DNS SAN / CN — skipping id-map registration)")
        }
        IdMapResult::NoMap { path } => println!(
            "(no local id-map at {} — skipping registration; create one with \
             `netidx admin component id-map init`)",
            path.display(),
        ),
        IdMapResult::Registered(reg) => match &reg.previous {
            Some(old) => println!(
                "updated id-map: {} (was uid={} primary={})",
                reg.name, old.uid, old.primary_group,
            ),
            None => {
                println!("added to id-map: {} uid={} primary={}", reg.name, reg.uid, reg.primary)
            }
        },
    }
}

#[derive(Args, Debug)]
pub(crate) struct InspectCsrArgs {
    /// Path to the CSR (PEM-encoded) to inspect.
    pub csr_path: PathBuf,
}

fn inspect_csr(p: InspectCsrArgs) -> Result<()> {
    let csr_pem = std::fs::read(&p.csr_path)
        .with_context(|| format!("reading CSR {}", p.csr_path.display()))?;
    let summary = ca::inspect_csr(&csr_pem).context("inspecting CSR")?;
    print_csr_summary(&p.csr_path, &summary);
    Ok(())
}

/// Print the standard CSR summary block (`path`, `cn`, `key bits`, `san`),
/// shared by `sign` and `inspect-csr`.
fn print_csr_summary(csr_path: &Path, summary: &ca::CsrSummary) {
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
}

/// `ca queue` — list the pending enrollment queue, each request keyed by its
/// code (the CSR public-key fingerprint the enrollee's terminal showed,
/// recomputed here from the CSR — never trusted from the wire). Verified
/// renewals are listed separately: they carry a cryptographic proof of
/// possession, so there is no code to match — approve them with
/// `ca approve --renewals`.
fn queue(f: QueueArgs) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let items = runtime()?.block_on(ca_ops::list_queue(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
    ))?;
    let renewals: Vec<_> = items.iter().filter(|i| i.verified_renewal).collect();
    let pending: Vec<_> = items.iter().filter(|i| !i.verified_renewal).collect();
    if renewals.is_empty() && pending.is_empty() {
        println!("the enrollment queue is empty");
        return Ok(());
    }
    if !renewals.is_empty() {
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
        println!("  approve them all with `netidx admin ca approve --renewals`.");
        if !pending.is_empty() {
            println!();
        }
    }
    if !pending.is_empty() {
        println!("pending enrollment requests:");
        for e in &pending {
            let what = match e.enroll_listen {
                Some(listen) => format!("CONF-SERVER ENROLLMENT at {listen}"),
                None => e.requested_name.clone(),
            };
            println!(
                "  {}  kind {:?}  age {}  from {}",
                what,
                e.kind,
                fmt_age(e.age_secs),
                e.peer,
            );
            match e.code {
                Some(code) => println!("    code {}", code.text()),
                None => println!("    (unparseable CSR — can only be denied)"),
            }
        }
        println!(
            "\napprove with `netidx admin ca approve <code>` after matching the code \
             out of band."
        );
    }
    Ok(())
}

/// `ca approve <code>` / `ca approve --renewals` — approve one pending request
/// whose recomputed code matches, or the whole verified-renewal batch.
fn approve(f: ApproveArgs) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let rt = runtime()?;
    if f.renewals {
        let results = rt.block_on(ca_ops::approve_renewals(
            &mut ans,
            server,
            f.auth.ca_dir.clone(),
            f.auth.admin.clone(),
            None,
        ))?;
        if results.is_empty() {
            println!("no verified renewals to approve");
            return Ok(());
        }
        for r in &results {
            match &r.error {
                None => println!("  renewed {:?}", r.requested_name),
                Some(e) => println!("  renewing {:?} failed: {e}", r.requested_name),
            }
        }
        return Ok(());
    }
    if f.code.is_empty() {
        bail!("provide a request code (as shown by `ca queue`), or `--renewals`");
    }
    let code = f.code.join(" ");
    let out = rt.block_on(ca_ops::approve(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        &code,
        &f.id_map_group,
        f.no_id_map,
    ))?;
    match out.enroll_listen {
        Some(listen) => println!(
            "approved CONF-SERVER ENROLLMENT — {listen} is now a registered \
             admin-server peer."
        ),
        None if out.id_map_groups.is_empty() => println!(
            "approved and signed {:?} (no id-map registration).",
            out.requested_name
        ),
        None => println!(
            "approved and signed {:?} — id-map groups {:?}.",
            out.requested_name, out.id_map_groups
        ),
    }
    println!("the requester's install picks up the cert on its next poll.");
    for w in out.warnings {
        println!("  warning: {w}");
    }
    Ok(())
}

/// `ca deny <code> --reason <text>` — deny the one pending request whose
/// recomputed code matches.
fn deny(f: DenyArgs) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let code = f.code.join(" ");
    let name = runtime()?.block_on(ca_ops::deny(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        &code,
        &f.reason,
    ))?;
    println!("denied {name:?}.");
    Ok(())
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

fn san_display(s: &SanEntry) -> String {
    match s {
        SanEntry::Dns(d) => format!("dns:{d}"),
        SanEntry::Ip(ip) => format!("ip:{ip}"),
        SanEntry::Uri(u) => format!("uri:{u}"),
        SanEntry::Email(e) => format!("email:{e}"),
    }
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
        println!("# no CA at {} — run `netidx admin ca init` first", dir.display());
        return Ok(());
    }
    println!("CA at {}", dir.display());
    if let Ok(cert) = std::fs::read(dir.join("certificate.pem"))
        && let Ok(fp) = Fingerprint::of_cert_pem(&cert)
    {
        println!(
            "  fingerprint: {} … (`netidx admin ca fingerprint` for the full id)",
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
        let cfg_path = paths::discover_admin_server_config().ok();
        let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
        match &cfg_path {
            Some(p) if rt.block_on(admin_local::daemon_running(p)) => {
                let admins = rt
                    .block_on(admin_local::list_admins(p))
                    .map(|a| a.into_iter().map(|i| i.admin).collect::<Vec<_>>())
                    .unwrap_or_default();
                if admins.is_empty() {
                    println!("  key:    keyslot vault");
                } else {
                    println!("  key:    keyslot vault — admins: {}", admins.join(", "));
                }
            }
            _ => {
                println!(
                    "  key:    keyslot vault (start the admin server to list admins)"
                )
            }
        }
    } else if dir.join("private.key").is_file() {
        println!("  key:    private.key (legacy single-key format)");
    } else {
        println!("  key:    MISSING — CA cannot sign");
    }
    // Admin server.
    let cfg = paths::discover_admin_server_config().ok().and_then(|p| {
        netidx_admin::admin_server_config::AdminServerConfig::load(&p).ok()
    });
    match cfg {
        Some(c) => println!("  server: configured (listen {})", c.listen),
        None => println!("  server: not configured"),
    }
    Ok(())
}

// -- generate-flow helpers ---------------------------------------------------
//
// Used by `netidx admin resolver install --auth tls` to offer a
// "just generate the resolver certificate" path: for a small org the
// resolver host is commonly the CA host too, and making that one-step
// is the whole point.

/// Refuse to mint the admin server's reserved serving name from the
/// local CLI, mirroring the network sign path's refusal. The reserved
/// name is the linchpin of the trust model; only the admin-server setup
/// flow (which signs it directly) and the policy-gated network Enroll
/// may issue it.

#[cfg(test)]
mod tests {
    use super::*;

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
            id_map_group: vec![],
            uid: None,
            recovery: RecoveryAuth { recovery_password_file: None, recovery_password_stdin: false },
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
    fn sign_without_san_flags_errors_in_strict_mode() {
        // With neither --san nor --accept-csr-san, the strict FlagAnswerer
        // has no way to answer the "inherit the CSR's SAN?" question, so it
        // errors naming the flag rather than silently defaulting. This is
        // the strict-mode contract: every decision is explicit. (The
        // interactive TUI answerer prompts instead.)
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
        let err = sign(SignArgs {
            csr_path: Some(csr_path),
            san: vec![],
            accept_csr_san: false,
            validity: Duration::from_secs(30 * 86400),
            ca_dir: Some(ca_dir),
            out: Some(scratch.path().join("out.pem")),
            no_id_map: true,
            id_map_group: vec![],
            uid: None,
            recovery: RecoveryAuth { recovery_password_file: None, recovery_password_stdin: false },
        })
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--accept-csr-san"),
            "error should name --accept-csr-san: {msg}"
        );
    }

    #[test]
    fn sign_accept_csr_san_bails_when_csr_has_no_san() {
        // The "no SAN to inherit" branch: --accept-csr-san on a CSR that
        // carries no SAN has nothing to inherit, so it bails telling the
        // operator to pass --san.
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
            accept_csr_san: true,
            validity: Duration::from_secs(30 * 86400),
            ca_dir: Some(ca_dir),
            out: Some(scratch.path().join("out.pem")),
            no_id_map: true,
            id_map_group: vec![],
            uid: None,
            recovery: RecoveryAuth { recovery_password_file: None, recovery_password_stdin: false },
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
            id_map_group: vec![],
            uid: None,
            recovery: RecoveryAuth { recovery_password_file: None, recovery_password_stdin: false },
        })
        .unwrap_err();
        assert!(format!("{err:#}").contains("not both"));
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
        let issued = netidx_admin::plan::ca_setup::issue_identity_into(
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

    /// An offline CA (no admin server) is minted with exactly one signing
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
            netidx_admin::ca_store::CaDir::open(&dir)
                .unwrap()
                .vault
                .read()
                .signing_slot_names()
                .unwrap(),
            vec![ca_vault::RECOVERY_ADMIN.to_string()]
        );
        let admins = netidx_admin::ca_store::CaDir::open(&dir)
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
