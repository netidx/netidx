#[cfg(unix)]
use anyhow::anyhow;
use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use netidx_admin::{
    answer::{Answerer, OneTimeSecret},
    atomic,
    csr::{self, Subject},
    ops::{
        self, queue as ca_ops, revoke as revoke_ops, roster as roster_ops,
        servers as server_ops,
    },
    paths,
    plan::{ca_setup, enroll},
    tls, transport,
};
#[cfg(unix)]
use netidx_admin::{
    ca, ca_vault,
    config_lock::ConfigDirLock,
    csr::SanEntry,
    local,
    ops::{offline as offline_ops, slots as slots_ops},
    plan,
};
use netidx_admin_proto::{
    self as admin_proto, NodeKind,
    fingerprint::{ColorMode, Fingerprint},
    policy::{AdminInfo, Policy, SlotKind},
};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(all(test, unix))]
use netidx_admin::ca::{Ca, CaParams};

#[cfg(unix)]
use super::answer_cli::make_offline_answerer;
#[cfg(unix)]
use super::service;
use super::{answer_cli::RemoteAuthFlags, init};

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

fn parse_server_role(value: &str) -> std::result::Result<admin_proto::Role, String> {
    match value.to_ascii_lowercase().replace('_', "-").as_str() {
        "resolver" => Ok(admin_proto::Role::Resolver),
        "id-map" | "idmap" => Ok(admin_proto::Role::IdMap),
        "ca" => Err("the CA role cannot be granted to an enrollee".to_string()),
        _ => Err("expected resolver or id-map".to_string()),
    }
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install this machine as the admin domain's CA and certificate authority
    #[cfg(unix)]
    Install(CaInstallArgs),
    /// create a new local CA (keyslot vault; can serve via `admin server`)
    #[cfg(unix)]
    Init(InitParams),
    /// issue a leaf certificate from a CA
    #[cfg(unix)]
    Issue(IssueArgs),
    /// sign an externally-supplied CSR file with a local CA
    #[cfg(unix)]
    Sign(SignArgs),
    /// print a CSR's contents (CN, key bits, SAN) without signing it
    #[cfg(unix)]
    InspectCsr(InspectCsrArgs),
    /// list the pending enrollment queue (each request keyed by its code)
    Queue(QueueArgs),
    /// approve one pending enrollment request by its code (or `--renewals` to
    /// approve the verified-renewal batch)
    Approve(ApproveArgs),
    /// deny one pending enrollment request by its code
    Deny(DenyArgs),
    /// list local CAs
    #[cfg(unix)]
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
    /// list every CA-authoritative admin-server identity, grouped by resolver cluster
    Servers(ServersArgs),
    /// permanently revoke and remove one dead admin-server identity
    RemoveServer(RemoveServerArgs),
    /// re-send the CA's current address, authoritative map, and CRL to
    /// every registered node (safe to repeat after partial failure)
    ReconcileCa(ReconcileCaArgs),
    /// set up or rotate the auto-approve slot, so the running admin server
    /// approves verified renewals in-process (no human per renewal)
    #[cfg(unix)]
    AutoApprove(AutoApproveArgs),
    /// manage the off-box recovery credential (rotate it on the CA box)
    #[cfg(unix)]
    Recovery {
        #[command(subcommand)]
        cmd: RecoveryCmd,
    },
    /// manage an externally-signed (intermediate) CA: (re-)emit its CSR or
    /// install a signed certificate
    #[cfg(unix)]
    External {
        #[command(subcommand)]
        cmd: ExternalCmd,
    },
}

#[derive(Args, Debug)]
pub(crate) struct CaInstallArgs {
    /// The admin domain's domain name (default: local).
    #[arg(long)]
    domain: Option<String>,
    /// CA admin-server listen address.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// Run the CA as an intermediate signed by an external PKI.
    #[arg(long = "external-sign")]
    external_sign: bool,
    /// Proceed without TPM/Secure-Enclave sealing (test deployments only).
    #[arg(long = "insecure-no-tpm")]
    insecure_no_tpm: bool,
    /// Print the installation plan without changing anything.
    #[arg(long = "dry-run")]
    dry_run: bool,
    /// Do not write activation units.
    #[arg(long = "no-units")]
    no_units: bool,
    /// Register the activation supervisor as an OS service.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    with_service: bool,
    /// Do not register an OS service.
    #[arg(long = "no-service")]
    no_service: bool,
    /// Read the founding CA administrator password from a file.
    #[arg(long = "admin-password-file")]
    admin_password_file: Option<PathBuf>,
    /// Read the founding CA administrator password from stdin.
    #[arg(long = "admin-password-stdin", conflicts_with = "admin_password_file")]
    admin_password_stdin: bool,
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
    /// Override the CA directory (defaults to `${basedir}/CA/`).
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
    /// Override the CA directory (defaults to `${basedir}/CA/`).
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    /// When this install stands up the admin server (the first install of a
    /// served external CA), register netidx as an OS service. This is the
    /// default; the flag only skips the prompt on a TTY. Pass `--no-service`
    /// to opt out.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    pub with_service: bool,
    /// Do not register netidx as an OS service after standing up the admin
    /// server. Registering is the default; run `netidx admin host
    /// service install` later.
    #[arg(long = "no-service")]
    pub no_service: bool,
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
    /// Override the CA directory (defaults to `${basedir}/CA/`).
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
pub(crate) struct ReconcileCaArgs {
    #[command(flatten)]
    auth: RemoteAuthFlags,
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

#[derive(Args, Debug)]
pub(crate) struct ServersArgs {
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct RemoveServerArgs {
    /// Exact immutable server UUID, copied from `ca servers`. This is the
    /// destructive target assertion; names and addresses are not accepted.
    #[arg(value_name = "SERVER-ID")]
    server_id: admin_proto::AdminServerId,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCmd {
    /// add an admin keyslot (a new password that can sign)
    Add(AdminAddArgs),
    /// add a role keyslot: authenticates and may edit perms in scope, but
    /// can NEVER unlock the CA key or sign certs. Prints a one-time password
    /// the new admin must replace before it can do anything
    AddRole(AdminAddRoleArgs),
    /// revoke an admin keyslot
    Remove(AdminRemoveArgs),
    /// replace an admin's issuance policy (allowed SANs / max validity)
    SetPolicy(AdminSetPolicyArgs),
    /// list admin keyslots and their issuance policy
    List(AdminScopeArgs),
    /// change your own password (the current one authorizes the change)
    ChangePassword(AdminChangePasswordArgs),
    /// issue an admin a one-time password, revoking the one they have and
    /// ending their sessions. They must set a new password before anything
    /// else works
    ResetPassword(AdminResetPasswordArgs),
}

#[derive(Args, Debug)]
pub(crate) struct AdminChangePasswordArgs {
    /// Read the new password from a file (never on the command line).
    /// Prompted when omitted.
    #[arg(long = "new-password-file")]
    new_password_file: Option<PathBuf>,
    /// Read the new password from stdin. Only usable when the *current*
    /// password comes from a file — one stdin cannot carry two secrets.
    #[arg(
        long = "new-password-stdin",
        conflicts_with_all = ["new_password_file"]
    )]
    new_password_stdin: bool,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct AdminResetPasswordArgs {
    /// The admin whose password to reset.
    #[arg(value_name = "NAME")]
    name: String,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

#[derive(Args, Debug)]
pub(crate) struct AdminScopeArgs {
    /// Which CA to list. With `--server` this is a remote admin plane
    /// (glyph-confirmed, `may_manage_admins` admin); without it, this host's
    /// own admin server over its local control socket.
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

/// The policy knobs shared by `ca admin add-role` and `set-policy`.
/// `may_manage_admins` is an `Option`: supplied → used; omitted → the answerer
/// decides. Empty SAN/id-map inputs take per-domain defaults; enrollment scope
/// and role grants remain empty unless explicitly supplied.
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
    /// Cluster base under which this admin may enroll servers (repeatable).
    #[arg(long = "server-enroll-scope", num_args = 1)]
    server_enroll_scopes: Vec<String>,
    /// Server role this admin may grant (repeatable: resolver or id-map).
    #[arg(long = "server-enroll-role", num_args = 1, value_parser = parse_server_role)]
    server_enroll_roles: Vec<admin_proto::Role>,
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
    fn inputs(&self) -> ca_setup::PolicyInputs<'_> {
        ca_setup::PolicyInputs {
            allow_san: &self.allow_san,
            max_validity: self.max_validity,
            id_map_groups: &self.id_map_groups,
            server_enroll_scopes: &self.server_enroll_scopes,
            server_enroll_roles: self.server_enroll_roles.iter().copied().collect(),
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
    /// the admin domain identity to verify out of band before enrolling against it
    /// (pass the confirmed value to a join's `--accept-glyph`). When omitted,
    /// show this host's own local CA glyph.
    pub server: Option<SocketAddr>,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct JoinArgs {
    /// Admin server address (`ip:port`). Required — admin domain discovery is
    /// interactive only.
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// The TLS identity name to request (one DNS SAN).
    #[arg(long)]
    pub name: Option<String>,
    /// The admin name to authenticate as.
    #[arg(long)]
    pub admin: Option<String>,
    /// Validity to request (e.g. 730d, 10m). Default 730d (capped by server policy).
    #[arg(long, value_parser = humantime::parse_duration, default_value = "730d")]
    pub validity: Duration,
    /// id-map groups to register the identity with (repeatable; first
    /// is primary); an explicit empty string skips registration. Defaults
    /// to the per-kind default.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// The admin server's CA fingerprint, obtained out of band (view it with
    /// `netidx admin ca fingerprint <ip:port>`); confirms the admin domain identity.
    #[arg(long = "accept-glyph")]
    pub accept_glyph: Option<String>,
    /// Read the admin password from a file (never on the command line).
    #[arg(long = "password-file")]
    pub password_file: Option<PathBuf>,
    /// Read the admin password from stdin.
    #[arg(long = "password-stdin", conflicts_with = "password_file")]
    pub password_stdin: bool,
    /// Protection for the issued private key.
    #[arg(long = "key-protection")]
    pub key_protection: Option<netidx_admin::plan::enroll::KeyProtArg>,
    /// Replace an identity of this name that is already installed.
    #[arg(long)]
    pub force: bool,
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
    /// Cluster base under which the superuser may enroll servers (repeatable).
    /// Defaults to `/` for the founding admin.
    #[arg(long = "server-enroll-scope", num_args = 1)]
    pub server_enroll_scopes: Vec<String>,
    /// Server role the superuser may grant (repeatable: resolver or id-map).
    /// Defaults to both roles for the founding admin.
    #[arg(long = "server-enroll-role", num_args = 1, value_parser = parse_server_role)]
    pub server_enroll_roles: Vec<admin_proto::Role>,
    /// Proceed even when this host has no usable TPM / Secure Enclave.
    /// DANGER: the autorenew credential is then written in PLAINTEXT, so
    /// every backup or disk image of this machine is a CA compromise. Test
    /// CAs only.
    #[arg(long = "insecure-no-tpm")]
    pub insecure_no_tpm: bool,
    /// Set up the CA (issue a serving cert + write server.json)
    /// without prompting. By default `ca init` asks.
    #[arg(long)]
    pub with_server: bool,
    /// Skip the CA-server setup entirely (offline CA only).
    #[arg(long, conflicts_with = "with_server")]
    pub no_server: bool,
    /// Address the CA should listen on when set up. Default
    /// `0.0.0.0:<ca-port>`.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Where to drop the CA's activation unit. Defaults to the
    /// user activation dir (same place the resolver/id-map units go).
    #[arg(long = "units-dir")]
    pub units_dir: Option<PathBuf>,
    /// After setting up the CA, also register netidx as an OS
    /// service without prompting. Mutually exclusive with
    /// `--no-service`.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    pub with_service: bool,
    /// Skip the OS-service prompt after setting up the CA.
    #[arg(long = "no-service")]
    pub no_service: bool,
    /// Read the founding superuser's password from a file (never on the
    /// command line). Required when the CA is set up (`--with-server`),
    /// which mints the superuser role admin.
    #[arg(long = "password-file")]
    pub password_file: Option<PathBuf>,
    /// Read the founding superuser's password from stdin instead of a file.
    #[arg(long = "password-stdin", conflicts_with = "password_file")]
    pub password_stdin: bool,
    /// Override the directory the CA is created in. Defaults to
    /// `${basedir}/CA/` — one CA per netidx install.
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

#[cfg(unix)]
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
    /// Override the CA's directory. Defaults to `${basedir}/CA/`.
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
    /// Override the CA's directory. Defaults to `${basedir}/CA/`.
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
    /// registration. Omit to prompt (interactive) or skip (strict).
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_group: Vec<String>,
    #[command(flatten)]
    pub recovery: RecoveryAuth,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        #[cfg(unix)]
        Cmd::Install(p) => install_ca(p),
        #[cfg(unix)]
        Cmd::Init(p) => init(p),
        #[cfg(unix)]
        Cmd::Issue(p) => issue(p),
        #[cfg(unix)]
        Cmd::Sign(p) => sign(p),
        #[cfg(unix)]
        Cmd::InspectCsr(p) => inspect_csr(p),
        Cmd::Queue(f) => queue(f),
        Cmd::Approve(p) => approve(p),
        Cmd::Deny(f) => deny(f),
        #[cfg(unix)]
        Cmd::List => list(),
        Cmd::Admin { cmd } => admin(cmd),
        Cmd::Fingerprint(p) => fingerprint(p),
        Cmd::Issued(f) => issued(f),
        Cmd::Revoke(p) => revoke(p),
        Cmd::Servers(p) => servers(p),
        Cmd::RemoveServer(p) => remove_server(p),
        Cmd::ReconcileCa(p) => reconcile_ca(p),
        #[cfg(unix)]
        Cmd::AutoApprove(p) => auto_approve(p),
        #[cfg(unix)]
        Cmd::Recovery { cmd } => recovery(cmd),
        #[cfg(unix)]
        Cmd::External { cmd } => external(cmd),
    }
}

#[cfg(unix)]
fn install_ca(p: CaInstallArgs) -> Result<()> {
    let mut ans = super::answer_cli::FlagAnswerer::install(
        None,
        false,
        p.admin_password_file.as_deref(),
        p.admin_password_stdin,
        None,
        false,
        None,
    )?;
    let mode = if p.dry_run {
        plan::install::InstallMode::DryRun
    } else {
        plan::install::InstallMode::apply_user_config_blocking()?
    };
    let common = plan::install::InstallCommon {
        mode,
        force: false,
        no_units: p.no_units,
        with_service: p.with_service,
        no_service: p.no_service,
    };
    let input = netidx_admin::plan::install::ca::CaInput {
        domain: p.domain,
        listen: p.listen,
        units_dir: None,
        external_sign: Some(p.external_sign),
        insecure_no_tpm: p.insecure_no_tpm,
        common,
    };
    let out =
        runtime()?.block_on(netidx_admin::plan::install::ca::run_ca(&mut ans, input))?;
    if let Some(scope) = out.service {
        service::install_with_defaults(scope.into())?;
    }
    if let Some(csr) = out.pending_external {
        // The engine's note already said what to do with it; all this adds is
        // where it actually landed, which that note gives only relative to the
        // working directory.
        println!("subordinate-CA CSR: {}", csr.display());
    }
    Ok(())
}

// -- CA auto-approve ----------------------------------------------------------

/// The dedicated autorenew slot: a name nobody types and an
/// empty-scope policy — over the wire its password can approve
/// verified renewals and *nothing else* (no SANs, no groups, no
/// enrollment). The narrow slot, not the daemon, is what bounds the
/// blast radius of a leaked keytab; the keytab itself lives outside
/// the CA dir so CA-dir backups stay harmless on their own, and
/// `--rotate` is the one-command kill-and-replace.
#[cfg(unix)]
pub(super) const AUTORENEW_ADMIN: &str = netidx_admin::AUTORENEW_ADMIN;

#[cfg(unix)]
/// Set up (or rotate) the autorenew slot and point this host's
/// admin-server config at its keytab. Approval itself is the running
/// daemon's job now — it reads the keytab named here and approves
/// verified renewals in-process — so this command just manages the
/// credential. `--rotate` is the same operation framed as a leaked-keytab
/// response: the library's `setup_autorenew_slot` always replaces the slot, so
/// enable and rotate share one path and differ only in what they print.
fn auto_approve(p: AutoApproveArgs) -> Result<()> {
    env_logger::init();
    let dir = ca_dir_for(None)?;
    let cfg = paths::discover_admin_server_config().ok();
    if p.status {
        let s = runtime()?.block_on(async {
            let access = slots_ops::CaAccess::open(&dir, cfg).await?;
            Ok::<_, anyhow::Error>(
                slots_ops::local_ca_status(&access, &dir).await?.auto_approve,
            )
        })?;
        println!("auto-approve status:");
        println!(
            "  autorenew slot: {}",
            if s.slot_present { "present" } else { "absent" }
        );
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
    let out = runtime()?.block_on(async {
        let access = slots_ops::CaAccess::open(&dir, cfg).await?;
        slots_ops::auto_approve(&mut ans, &access, dir, p.rotate, p.insecure_no_tpm).await
    })?;
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
        slots_ops::AutoApproveOutcome::Offline { rotate, keytab, wiring } => {
            use slots_ops::AutorenewWiring;
            println!("auto-approve {}:", if rotate { "rotated" } else { "enabled" });
            println!("  slot:   {AUTORENEW_ADMIN:?} (empty issuance scope)");
            println!("  keytab: {} (0600 — do NOT back this file up)", keytab.display());
            match wiring {
                AutorenewWiring::Updated(cfg_path) => {
                    println!("  config: {} (roles.CA.autorenew)", cfg_path.display());
                    println!("  restart the admin server to pick up the keytab.");
                }
                AutorenewWiring::NoCaConfig => {
                    println!("  config: no CA config owns this CA");
                }
                AutorenewWiring::Failed(e) => {
                    println!("  note: could not update the admin-server config ({e}).");
                    println!("        set roles.CA.autorenew to the keytab path and");
                    println!("        restart the admin server.");
                }
            }
        }
    }
    Ok(())
}

// -- CA revoke ----------------------------------------------------------------

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
        Some(&f.reason),
    ))?;
    println!(
        "revoked {} certificate(s); the daemon re-signed the CRL:",
        out.revoked.len()
    );
    for e in &out.revoked {
        let name = if e.name.is_empty() { "(no name)" } else { &e.name };
        println!("  serial {}  {}", e.serial, name);
    }
    if let Some(operation_id) = out.operation_id {
        println!("  CRL distribution operation: {operation_id}");
    }
    for peer in &out.peers {
        match &peer.error {
            None => println!("  updated server {} at {}", peer.server, peer.addr),
            Some(error) => {
                println!("  FAILED server {} at {}: {error}", peer.server, peer.addr)
            }
        }
    }
    for w in out.warnings {
        println!("  warning: {w}");
    }
    Ok(())
}

fn role_name(role: admin_proto::Role) -> &'static str {
    match role {
        admin_proto::Role::Ca => "CA",
        admin_proto::Role::Resolver => "resolver",
        admin_proto::Role::IdMap => "id-map",
    }
}

/// `ca servers` — display the immutable identities from the CA's
/// authoritative map. Cluster headings are intentionally separate from the
/// mutable admin and resolver addresses so an operator copies the UUID, not an
/// address, into the destructive command.
fn servers(f: ServersArgs) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let rows = runtime()?.block_on(server_ops::list_servers(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
    ))?;
    if rows.is_empty() {
        println!("the authoritative server map is empty");
        return Ok(());
    }
    let mut group: Option<(Option<admin_proto::ResolverClusterId>, Option<String>)> =
        None;
    for row in rows {
        let row_group = (row.cluster, row.cluster_base.clone());
        if group.as_ref() != Some(&row_group) {
            group = Some(row_group);
            match (row.cluster_base.as_deref(), row.cluster, row.cluster_state) {
                (Some(base), Some(id), Some(state)) => {
                    println!("resolver cluster {base}  {id}  [{state:?}]")
                }
                _ => println!("no resolver cluster"),
            }
        }
        let roles = row.roles.iter().map(role_name).collect::<Vec<_>>().join(",");
        println!(
            "  {}  {}  [{:?}]{}",
            row.id,
            row.addr,
            row.state,
            if row.ca { "  CA" } else { "" }
        );
        println!(
            "    roles {}  resolver {}  reads {}",
            if roles.is_empty() { "-" } else { &roles },
            row.resolver
                .as_ref()
                .map(|member| member.addr.to_string())
                .as_deref()
                .unwrap_or("-"),
            server_ops::read_gate_label(row.read_gate)
        );
    }
    println!(
        "\nPermanent removal requires the exact UUID: `netidx admin ca remove-server \
         <server-id>`."
    );
    Ok(())
}

/// `ca remove-server <uuid>` — irreversible dead-node recovery. The exact UUID
/// is the strict CLI's target assertion; ca-side policy is still the
/// authority, and the active CA is unconditionally protected.
fn remove_server(f: RemoveServerArgs) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let out = runtime()?.block_on(server_ops::remove_server(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        f.server_id,
    ))?;
    if out.removed {
        println!("permanently removed server {}", f.server_id);
    } else {
        println!(
            "server {} was already absent; reconciled surviving topology",
            f.server_id
        );
    }
    println!("  authoritative map version: {}", out.version);
    println!("  serving certificates revoked: {}", out.revoked);
    if let Some(operation_id) = out.operation_id {
        println!("  removal operation: {operation_id}");
    }
    if !out.affected_clusters.is_empty() {
        println!("  affected clusters: {}", out.affected_clusters.join(", "));
    }
    // Whether the departing member was told to stop answering subscribers. It
    // keeps whatever it already holds, so a member that could not be reached
    // goes on serving a snapshot that only decays — say so rather than let the
    // operator assume it went quiet.
    match &out.gated {
        None => {}
        Some(peer) if peer.error.is_none() => {
            println!("  read gate: {} is no longer answering read clients", peer.addr)
        }
        Some(peer) => println!(
            "  ! read gate: could not reach {} to stop it answering read clients: {}\n    \
             it is still serving whatever it last held — stop that machine.",
            peer.addr,
            peer.error.as_deref().unwrap_or("unknown error")
        ),
    }
    let crl_failed: Vec<_> =
        out.crl_peers.iter().filter(|peer| peer.error.is_some()).collect();
    println!(
        "  CRL targets updated: {}/{}",
        out.crl_peers.len() - crl_failed.len(),
        out.crl_peers.len()
    );
    for peer in crl_failed {
        println!(
            "  ! CRL server {} at {}: {}",
            peer.server,
            peer.addr,
            peer.error.as_deref().unwrap_or("unknown error")
        );
    }
    let failed: Vec<_> = out.peers.iter().filter(|peer| peer.error.is_some()).collect();
    println!(
        "  topology targets updated: {}/{}",
        out.peers.len() - failed.len(),
        out.peers.len()
    );
    for peer in failed {
        println!(
            "  ! server {} at {}: {}",
            peer.server,
            peer.addr,
            peer.error.as_deref().unwrap_or("unknown error")
        );
    }
    if !out.peers.is_empty() {
        println!(
            "No service was restarted. If the written topology requires a restart, \
             roll each affected resolver cluster manually: restart one member, wait \
             the resolver delay-reads period for publishers to republish, then restart \
             the next member."
        );
    }
    if !out.peers.is_empty() && !out.peers.iter().all(|peer| peer.error.is_none()) {
        println!(
            "The authoritative removal succeeded, but topology is not fully \
             reconciled. Restore the failed target and repeat an idempotent topology \
             reconciliation before restarting it."
        );
    }
    Ok(())
}

fn reconcile_ca(a: ReconcileCaArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let (operation_id, peers) = runtime()?.block_on(server_ops::reconcile_ca(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    let failed: Vec<_> = peers.iter().filter(|peer| peer.error.is_some()).collect();
    println!("CA reconciliation operation: {operation_id}");
    println!("  targets updated: {}/{}", peers.len() - failed.len(), peers.len());
    for peer in failed {
        println!(
            "  ! server {} at {}: {}",
            peer.server,
            peer.addr,
            peer.error.as_deref().unwrap_or("unknown error")
        );
    }
    if !peers.iter().all(|peer| peer.error.is_none()) {
        println!(
            "Restore the failed targets, then repeat this command; reconciliation is idempotent."
        );
    }
    Ok(())
}

fn ca_dir_for(override_: Option<PathBuf>) -> Result<PathBuf> {
    match override_ {
        Some(p) => Ok(p),
        None => paths::user_ca_dir(),
    }
}

#[cfg(unix)]
/// `admin ca recovery {rotate,status}`: mint a fresh recovery password on the
/// CA box (authorized by the box's own autorenew keytab, so a lost recovery
/// password is recoverable while the machine lives), or report the recovery
/// slot's state.
fn recovery(cmd: RecoveryCmd) -> Result<()> {
    match cmd {
        RecoveryCmd::Rotate(a) => recovery_rotate(a),
        RecoveryCmd::Status(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let cfg = paths::discover_admin_server_config().ok();
            let s = runtime()?.block_on(async {
                let access = slots_ops::CaAccess::open(&dir, cfg).await?;
                Ok::<_, anyhow::Error>(
                    slots_ops::local_ca_status(&access, &dir).await?.recovery,
                )
            })?;
            println!("recovery status:");
            println!(
                "  recovery slot:  {}",
                if s.slot_present { "present" } else { "absent" }
            );
            println!(
                "  autorenew keytab (offline re-mint authority): {}",
                if s.keytab_present { "present" } else { "absent" }
            );
            Ok(())
        }
    }
}

#[cfg(unix)]
fn recovery_rotate(a: RecoveryRotateArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    let cfg = paths::discover_admin_server_config().ok();
    // The library shows the new password once (via the Answerer); we only
    // narrate which path ran. The recovery-rotate offline path authorizes with
    // the autorenew keytab, not a typed recovery password, so a bare
    // FlagAnswerer (no secret) is all the CLI needs.
    let mut ans = make_offline_answerer(None, false)?;
    let out = runtime()?.block_on(async {
        let access = slots_ops::CaAccess::open(&dir, cfg).await?;
        slots_ops::recovery_rotate(&mut ans, &access, dir).await
    })?;
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

// -- CA external (intermediate CA signed by an external PKI) -----------------

#[cfg(unix)]
fn external(cmd: ExternalCmd) -> Result<()> {
    match cmd {
        ExternalCmd::EmitCsr(a) => external_emit_csr(a),
        ExternalCmd::Install(a) => external_install(a),
        ExternalCmd::Status(a) => external_status(a),
        ExternalCmd::Renew(a) => external_renew(a),
    }
}

#[cfg(unix)]
fn external_emit_csr(a: ExternalDirArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    let mut ans = a.recovery.answerer()?;
    let csr_path = runtime()?.block_on(async {
        let access = slots_ops::CaAccess::open(&dir, None).await?;
        slots_ops::external_csr(&mut ans, &access, dir).await
    })?;
    println!("wrote {} — get it signed by your PKI, then run:", csr_path.display());
    println!("  netidx admin ca external install <signed-cert.pem> [--root <root.pem>]");
    Ok(())
}

#[cfg(unix)]
fn external_install(a: ExternalInstallArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    let mut ans = a.recovery.answerer()?;
    let gate = plan::service::ServiceGate {
        dry_run: false,
        no_service: a.no_service,
        with_service: a.with_service,
    };
    let rt = runtime()?;
    let scope = rt.block_on(async {
        let access = slots_ops::CaAccess::open(&dir, None).await?;
        let out = slots_ops::external_install(
            &mut ans,
            &access,
            dir,
            &a.signed_cert,
            a.root.as_deref(),
        )
        .await?;
        report_external_install(&mut ans, out, gate).await
    })?;
    if let Some(scope) = scope {
        service::install_with_defaults(scope.into())?;
    }
    Ok(())
}

#[cfg(unix)]
async fn report_external_install(
    ans: &mut dyn Answerer,
    out: slots_ops::ExternalInstallOutcome,
    gate: plan::service::ServiceGate,
) -> Result<Option<netidx_admin::service::ServiceScope>> {
    use slots_ops::ExternalInstallOutcome;
    match out {
        ExternalInstallOutcome::OfflineCa => {
            ans.note(
                "installed the externally-signed CA certificate.\n\
                 CA-cert auto-renewal is DISABLED (external issuer).",
            );
            Ok(None)
        }
        ExternalInstallOutcome::FirstInstall { need, cfg_path } => {
            ans.note(&format!(
                "installed the externally-signed CA certificate.\n\
                 admin server configured ({})\n\
                 CA-cert auto-renewal is DISABLED (external issuer); re-run \
                 `netidx admin ca external install` when your PKI re-signs it.",
                cfg_path.display()
            ));
            plan::service::offer(ans, need, gate).await
        }
        ExternalInstallOutcome::Renewal => {
            ans.note(
                "renewed the CA certificate — enrolled nodes adopt it on their \
                 next renewal (glyph unchanged; existing certificates stay valid).\n\
                 Its admin server was not running; start it to serve the new cert.",
            );
            Ok(None)
        }
        ExternalInstallOutcome::HotRenewed { ca_fingerprint } => {
            ans.note(&format!(
                "renewed the externally-signed CA without stopping it\n  \
                 identity: {ca_fingerprint}"
            ));
            Ok(None)
        }
    }
}

#[cfg(unix)]
fn external_status(a: ExternalDirArgs) -> Result<()> {
    let dir = ca_dir_for(a.ca_dir)?;
    let cfg = paths::discover_admin_server_config().ok();
    let s = runtime()?.block_on(async {
        let access = slots_ops::CaAccess::open(&dir, cfg).await?;
        Ok::<_, anyhow::Error>(slots_ops::local_ca_status(&access, &dir).await?.external)
    })?;
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
#[cfg(unix)]
fn external_renew(a: ExternalRenewArgs) -> Result<()> {
    match a.signed_cert {
        None => {
            external_emit_csr(ExternalDirArgs { ca_dir: a.ca_dir, recovery: a.recovery })
        }
        Some(signed_cert) => external_install(ExternalInstallArgs {
            signed_cert,
            root: a.root,
            ca_dir: a.ca_dir,
            with_service: false,
            no_service: false,
            recovery: a.recovery,
        }),
    }
}

#[cfg(unix)]
/// Phase 1 of `ca init --external-sign`: generate the CA key + a CSR for its
/// certificate, seal the vault (recovery slot always; for a served CA also
/// the box autorenew slot and the superuser role slot), mark the CA
/// externally-signed, and write the CSR + a marker. No `certificate.pem` is
/// written — its absence is the "awaiting external cert" state. Returns no
/// `ServiceNeed`: the server is stood up in phase 2, once the cert exists.
async fn external_bootstrap(
    ans: &mut dyn Answerer,
    config_lock: &ConfigDirLock,
    opts: ca_setup::NewCaOpts,
) -> Result<service::ServiceNeed> {
    ca_setup::create_vaulted_external_ca(ans, config_lock, opts).await?;
    Ok(service::ServiceNeed::NONE)
}

#[cfg(unix)]
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
    // Preserve the `ca.<domain>` convenience: with a domain but no explicit
    // --cn, derive the CN so strict mode need not demand --cn as well.
    let common_name = p.cn.or_else(|| p.domain.as_deref().map(ca_setup::default_ca_cn));
    let opts = ca_setup::NewCaOpts {
        dir: directory,
        common_name,
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
        server_enroll_scopes: p.server_enroll_scopes,
        server_enroll_roles: p.server_enroll_roles.into_iter().collect(),
        insecure_no_tpm: p.insecure_no_tpm,
        setup_server,
        listen: p.listen,
        // No resolver in this flow; the library falls back to an existing
        // resolver's IP, then the public IP.
        listen_hint: None,
        units_dir,
    };
    // The founding superuser's password (minted only when a server is set up)
    // comes from --password-file / --password-stdin. `ca init` creates the CA,
    // it never joins an admin domain, so there is no glyph to confirm.
    let mut ans = super::answer_cli::make_flag_answerer(
        p.password_file.as_deref(),
        p.password_stdin,
        None,
    )?;
    let gate = plan::service::ServiceGate {
        dry_run: false,
        no_service: p.no_service,
        with_service: p.with_service,
    };
    let rt = runtime()?;
    let scope = rt.block_on(async {
        let config_lock = ConfigDirLock::acquire_for_ca_dir(&opts.dir).await?;
        // `--external-sign` runs the CA as an intermediate: phase 1 makes the
        // key + a CSR and stops; `ca external install <signed-cert>` installs
        // the signed cert. Otherwise this is the normal self-signed CA.
        let need = if p.external_sign {
            external_bootstrap(&mut ans, &config_lock, opts).await?
        } else {
            ca_setup::create_vaulted_ca(&mut ans, &config_lock, opts).await?.1
        };
        // Single end-of-process hook — the same decision the `admin install`
        // templates make; the privileged install stays in this frontend.
        plan::service::offer(&mut ans, need, gate).await
    })?;
    if let Some(scope) = scope {
        service::install_with_defaults(scope.into())?;
    }
    Ok(())
}

// -- CA admin -----------------------------------------------------------------

/// Print the admin roster (local `list` and remote `list --server` share
/// this), one line per admin: name, tier, and full policy incl.
/// `may_manage_admins`.
fn print_admin_list(admins: &[AdminInfo]) {
    if admins.is_empty() {
        println!("(no admins — this CA is not vault-protected)");
    }
    for info in admins {
        let tier = match info.kind {
            SlotKind::Signing => "signing",
            SlotKind::Role => "role",
        };
        // Worth a column of its own: an admin holding a one-time password can
        // do nothing until they replace it, which is otherwise indistinguishable
        // from a working admin that just isn't logging in.
        let tier = if info.must_change {
            format!("{tier}, must change password")
        } else {
            tier.to_string()
        };
        // Destructured with no `..`, so a new capability can't be added to
        // Policy without an operator ever being shown it.
        let Policy {
            allowed_san,
            max_validity,
            id_map_groups,
            server_enroll_scopes,
            server_enroll_roles,
            perms_edit_scopes,
            may_manage_admins,
            service_control_scopes,
        } = &info.policy;
        println!(
            "{} [{tier}]: allowed_san={allowed_san:?} max_validity={} \
             id_map_groups={id_map_groups:?} \
             server_enroll_scopes={server_enroll_scopes:?} \
             server_enroll_roles={server_enroll_roles:?} \
             may_manage_admins={may_manage_admins} \
             perms_edit_scopes={perms_edit_scopes:?} \
             service_control_scopes={service_control_scopes:?}",
            info.admin,
            humantime::format_duration(*max_validity),
        );
    }
}

/// The CA CN + domain that seed the `*.<domain>` SAN suggestion: a remote CA
/// reports its domain in the pinned identity; a local CA is read off its own
/// certificate.
fn policy_context(
    target: &ops::AdminTarget,
    ca_dir: Option<&Path>,
) -> (String, Option<String>) {
    #[cfg(not(unix))]
    let _ = ca_dir;
    match target {
        ops::AdminTarget::Remote { session } => {
            let domain = session.identity.domain.to_string();
            (ca_setup::default_ca_cn(&domain), Some(domain))
        }
        #[cfg(unix)]
        ops::AdminTarget::Local { .. } => {
            let dir = ca_dir.map(Path::to_path_buf).or_else(|| paths::user_ca_dir().ok());
            let cn = dir.map(|d| ca_setup::existing_ca_cn(&d)).unwrap_or_default();
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
        AdminCmd::ChangePassword(a) => admin_change_password(a),
        AdminCmd::ResetPassword(a) => admin_reset_password(a),
    }
}

/// `ca admin add-role <name>` — mint a role admin (local control socket or
/// pinned remote plane; the CA enforces no-escalation remotely).
fn admin_add_role(a: AdminAddRoleArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    let (cn, domain) = policy_context(&target, a.auth.ca_dir.as_deref());
    // Added admins default to may-enroll = no (enroll_default = false); only the
    // founding superuser defaults to yes.
    let policy = rt.block_on(ca_setup::gather_policy(
        &mut ans,
        a.policy.inputs(),
        false,
        &cn,
        domain.as_deref(),
    ))?;
    let password = rt.block_on(roster_ops::add_role_admin(&target, &a.name, policy))?;
    report_admin_target("added role admin", &a.name, &target);
    rt.block_on(show_one_time_password(&mut ans, &a.name, &password))?;
    Ok(())
}

/// `ca admin change-password` — replace your own password. The password you
/// authenticate with is the one being replaced, which is why this works while
/// a one-time password has everything else refused — and why a cached session
/// is not enough: `--password-file` (or the prompt) is required even when one
/// is held.
fn admin_change_password(a: AdminChangePasswordArgs) -> Result<()> {
    let new_password = super::answer_cli::read_new_password_secret(
        a.new_password_file.as_deref(),
        a.new_password_stdin,
    )?;
    let mut ans = a.auth.answerer()?.with_new_password(new_password);
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    // `None`: the answerer holds the new password (or will prompt for it), so
    // it is read once, in one place, whichever way it was supplied.
    rt.block_on(roster_ops::change_password(&mut ans, &target, None))?;
    println!("password changed");
    Ok(())
}

/// `ca admin reset-password <name>` — issue `name` a one-time password. Ends
/// their sessions, so it doubles as "lock this admin out now".
fn admin_reset_password(a: AdminResetPasswordArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    let password = rt.block_on(roster_ops::reset_password(&target, &a.name))?;
    report_admin_target("reset the password for admin", &a.name, &target);
    rt.block_on(show_one_time_password(&mut ans, &a.name, &password))?;
    Ok(())
}

/// Show a one-time password through the shown-once seam, so it gets the same
/// boxed treatment (and the same "there is no second chance" framing) as the
/// CA recovery password.
async fn show_one_time_password(
    ans: &mut dyn Answerer,
    admin: &str,
    password: &str,
) -> Result<()> {
    ca_setup::show_generated_password(
        ans,
        OneTimeSecret::AdminPassword { admin: admin.to_string() },
        password,
    )
    .await
}

/// `ca admin set-policy <name>` — replace an admin's policy wholesale.
fn admin_set_policy(a: AdminSetPolicyArgs) -> Result<()> {
    let mut ans = a.auth.answerer()?;
    let server = a.auth.server_addr()?;
    let rt = runtime()?;
    let target = rt.block_on(ops::resolve_admin_target(
        &mut ans,
        server,
        a.auth.ca_dir.clone(),
        a.auth.admin.clone(),
        None,
    ))?;
    let (cn, domain) = policy_context(&target, a.auth.ca_dir.as_deref());
    let policy = rt.block_on(ca_setup::gather_policy(
        &mut ans,
        a.policy.inputs(),
        false,
        &cn,
        domain.as_deref(),
    ))?;
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
    let target = rt.block_on(ops::resolve_admin_target(
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
    let target = rt.block_on(ops::resolve_admin_target(
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
fn report_admin_target(what: &str, name: &str, target: &ops::AdminTarget) {
    match target {
        ops::AdminTarget::Remote { session } => {
            println!("{what} {name:?} on the CA at {}", session.server)
        }
        #[cfg(unix)]
        ops::AdminTarget::Local { .. } => {
            println!("{what} {name:?} (via the local admin server)")
        }
    }
}

// -- CA fingerprint -----------------------------------------------------------

fn fingerprint(p: FingerprintArgs) -> Result<()> {
    match p.server {
        // Remote: fetch the identity the admin server presents and show its
        // glyph, so an operator can verify it out of band before enrolling.
        Some(addr) => {
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            let identity = rt
                .block_on(transport::fetch_identity(addr, NodeKind::Client))
                .with_context(|| format!("contacting admin server {addr}"))?;
            init::show_admin_domain_identity(addr, &identity);
            Ok(())
        }
        // Local: this host's own CA.
        None => {
            let dir = ca_dir_for(p.ca_dir)?;
            show_ca_identity(&dir)
        }
    }
}

// -- CA join (the client) -----------------------------------------------------

pub(crate) fn join(p: JoinArgs) -> Result<()> {
    let mut ans = super::answer_cli::make_flag_answerer(
        p.password_file.as_deref(),
        p.password_stdin,
        p.accept_glyph.as_deref(),
    )?;
    let rt = runtime()?;
    rt.block_on(join_async(&mut ans, p))
}

async fn join_async(ans: &mut dyn Answerer, p: JoinArgs) -> Result<()> {
    // Non-interactive: the admin domain must be named explicitly (discovery is an
    // interactive-only step) and its identity confirmed out of band via the
    // glyph. `fetch_identity` sends nothing secret and closes before returning.
    let server = p.server.context(
        "--server <ip:port> is required (admin domain discovery is interactive only)",
    )?;
    let identity = transport::fetch_identity(server, NodeKind::Client)
        .await
        .with_context(|| format!("contacting admin server {server}"))?;
    init::show_admin_domain_identity(server, &identity);
    if !ans.confirm_identity(&identity).await? {
        bail!(
            "CA identity was not confirmed (--accept-glyph mismatch); nothing was sent"
        );
    }
    let (joined, _staging) = enroll::join_admin_domain(
        ans,
        enroll::JoinRequest {
            suggested_name: p.name.as_deref(),
            key_protection: p.key_protection,
            // With `--admin` this is the synchronous signed request it has
            // always been; without one it queues and waits for a remote admin
            // to approve, which it previously could not do at all.
            admin: p.admin.clone(),
            id_map_groups: &p.id_map_groups,
            validity: Some(p.validity),
            refuse_if_installed: !p.force,
            ..enroll::JoinRequest::new(server, NodeKind::Client, &identity)
        },
    )
    .await?;
    let installed = tls::install_identity_for_user(
        &joined.name,
        &joined.certificate,
        &joined.private_key,
        &joined.trusted,
        p.force,
    )?;
    println!("installed identity {:?} in {}", joined.name, installed.directory.display());
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

#[cfg(unix)]
fn issue(p: IssueArgs) -> Result<()> {
    let directory = ca_dir_for(p.ca_dir)?;
    let cn =
        p.cn.ok_or_else(|| anyhow!("--cn is required (the certificate common name)"))?;
    let out_dir = p
        .out_dir
        .ok_or_else(|| anyhow!("--out is required (the output dir for key + cert)"))?;
    let san = csr::parse_sans(&p.san, &cn)?;
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
    let out = runtime()?.block_on(async {
        let lock = ConfigDirLock::acquire_for_ca_dir(&directory).await?;
        offline_ops::ca_issue(
            &mut ans, &lock, directory, subject, san, p.key_bits, p.validity, out_dir,
            None,
        )
        .await
    })?;
    println!("issued cert:");
    println!("  cn:          {}", out.cn);
    println!("  private key: {}", out.private_key.display());
    println!("  certificate: {}", out.certificate.display());
    Ok(())
}

pub(crate) fn request(p: RequestArgs) -> Result<()> {
    let cn = p.cn.context("--cn is required (the requested certificate common name)")?;
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
    let out_csr = p.out_csr.unwrap_or_else(|| csr::default_csr_filename(&cn));
    let san = csr::parse_sans(&p.san, &cn)?;
    // The key is written unencrypted — same rationale as the `ca issue` CLI:
    // encrypted leaf keys are wired through `netidx admin init`, which knows
    // how to set the matching `tls.askpass` in the emitted config.
    let kr = csr::generate_key_and_csr(
        &Subject {
            common_name: cn.clone(),
            country: p.country,
            state: p.state,
            locality: p.locality,
            organization: p.organization,
        },
        &san,
    )?;
    atomic::write_atomic(&out_key, kr.private_key_pem.as_bytes(), 0o600)
        .with_context(|| format!("writing private key to {:?}", out_key))?;
    atomic::write_atomic(&out_csr, kr.csr_pem.as_bytes(), 0o644)
        .with_context(|| format!("writing CSR to {:?}", out_csr))?;
    println!("wrote private key (0600): {}", out_key.display());
    println!("wrote CSR        (0644): {}", out_csr.display());
    println!();
    println!("# Next step: hand the CSR to a CA admin who runs");
    println!("#   netidx admin ca sign {} --out <cert.pem>", out_csr.display());
    Ok(())
}

#[cfg(unix)]
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
    let out = runtime()?.block_on(async {
        let ca_lock = ConfigDirLock::acquire_for_ca_dir(&directory).await?;
        offline_ops::ca_sign(
            &mut ans,
            &ca_lock,
            directory,
            csr_pem,
            san,
            p.validity,
            p.out.take(),
            id_map,
        )
        .await
    })?;
    print_sign_outcome(&csr_path, &out);
    Ok(())
}

#[cfg(unix)]
/// Build the SAN choice from `--san` / `--accept-csr-san`, preserving the
/// mutually-exclusive decision table (the library resolves what `Ask` means).
fn sign_san_choice(p: &SignArgs) -> Result<offline_ops::SignSan> {
    Ok(match (p.san.is_empty(), p.accept_csr_san) {
        (false, false) => {
            let san = p
                .san
                .iter()
                .map(|s| csr::parse_san_one(s))
                .collect::<Result<Vec<_>>>()?;
            offline_ops::SignSan::Explicit(san)
        }
        (false, true) => bail!(
            "pass either --san <kind>:<value> (one or more) or --accept-csr-san, not both"
        ),
        (true, true) => offline_ops::SignSan::InheritCsr,
        (true, false) => offline_ops::SignSan::Ask,
    })
}

/// Build the post-sign id-map action from `--no-id-map` / `--id-map-group`.
/// Which guard the map is written under is the engine's call, not a flag's.
#[cfg(unix)]
fn id_map_choice(p: &SignArgs) -> offline_ops::IdMapAction {
    if p.no_id_map {
        offline_ops::IdMapAction::Skip
    } else if !p.id_map_group.is_empty() {
        offline_ops::IdMapAction::Register { groups: p.id_map_group.clone() }
    } else {
        offline_ops::IdMapAction::Ask
    }
}

#[cfg(unix)]
fn print_sign_outcome(csr_path: &Path, out: &offline_ops::SignOutcome) {
    print_csr_summary(csr_path, &out.summary);
    println!("  signing SAN:");
    for entry in &out.san {
        println!("    - {}", san_display(entry));
    }
    println!("\nsigned cert (0644): {}", out.out.display());
    print_id_map_result(&out.id_map);
}

#[cfg(unix)]
fn print_id_map_result(r: &offline_ops::IdMapResult) {
    use offline_ops::IdMapResult;
    match r {
        IdMapResult::NotRequested | IdMapResult::Declined => {}
        IdMapResult::NoIdentityName => {
            println!("(no DNS SAN / CN — skipping id-map registration)")
        }
        // Offline signing is the break-glass path, so it registers into the
        // local file directly rather than through the admin plane — but there
        // is nothing to register into if the host was never installed with an
        // id-map role. The installers drop a starter map; `admin id-map` is
        // how it is managed once a CA is reachable again.
        IdMapResult::NoMap { path } => println!(
            "(no local id-map at {} — skipping registration; this host has no \
             id-map role. Install one, or manage the admin domain's id-map with \
             `netidx admin id-map`)",
            path.display(),
        ),
        IdMapResult::Registered(reg) => match &reg.previous {
            Some(old) => println!(
                "updated id-map: {} (was primary={})",
                reg.name, old.primary_group,
            ),
            None => println!("added to id-map: {} primary={}", reg.name, reg.primary),
        },
    }
}

#[derive(Args, Debug)]
pub(crate) struct InspectCsrArgs {
    /// Path to the CSR (PEM-encoded) to inspect.
    pub csr_path: PathBuf,
}

#[cfg(unix)]
fn inspect_csr(p: InspectCsrArgs) -> Result<()> {
    let csr_pem = std::fs::read(&p.csr_path)
        .with_context(|| format!("reading CSR {}", p.csr_path.display()))?;
    let summary = ca::inspect_csr(&csr_pem).context("inspecting CSR")?;
    print_csr_summary(&p.csr_path, &summary);
    Ok(())
}

#[cfg(unix)]
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
            if let Some(serial) = e.replaces_serial {
                println!(
                    "    restores certificate serial {serial} (approval revokes it)"
                );
            }
            if let Some(listen) = e.enroll_listen {
                println!("    listen {listen}");
                println!("    roles {:?}", e.requested_roles);
                if let Some(old) = e.replaces {
                    println!(
                        "    replaces failed server {old} (old certificates will be revoked)"
                    );
                }
                match &e.cluster {
                    Some(admin_proto::ResolverClusterPlacement::Create { .. }) => {
                        println!(
                            "    resolver cluster create at {}",
                            e.cluster_base.as_deref().unwrap_or("(unknown base)")
                        )
                    }
                    Some(admin_proto::ResolverClusterPlacement::Join { cluster }) => {
                        println!(
                            "    resolver cluster {cluster} at {}",
                            e.cluster_base.as_deref().unwrap_or("(unknown base)")
                        )
                    }
                    None => println!("    resolver cluster (missing)"),
                }
                println!("    resolver members:");
                for member in &e.resolver_members {
                    println!("      {}  {:?}", member.addr, member.auth);
                }
            }
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
            "approved CONF-SERVER ENROLLMENT — issued the grant for {listen}; \
             it becomes a registered routing target when that daemon starts."
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
        Some(&f.reason),
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

#[cfg(unix)]
fn san_display(s: &SanEntry) -> String {
    match s {
        SanEntry::Dns(d) => format!("dns:{d}"),
        SanEntry::Ip(ip) => format!("ip:{ip}"),
        SanEntry::Uri(u) => format!("uri:{u}"),
        SanEntry::Email(e) => format!("email:{e}"),
    }
}

#[cfg(unix)]
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
            Some(p) if rt.block_on(local::daemon_running(p)) => {
                let admins = rt
                    .block_on(local::list_admins(p))
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
    let cfg = paths::discover_admin_server_config()
        .ok()
        .and_then(|p| netidx_admin::admin_server_config::load(&p).ok());
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
/// local CLI, mirroring the admin domain sign path's refusal. The reserved
/// name is the linchpin of the trust model; only the admin-server setup
/// flow (which signs it directly) and the policy-gated admin domain Enroll
/// may issue it.

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{FromArgMatches, Parser};
    #[cfg(unix)]
    use netidx_admin_proto::policy::RECOVERY_ADMIN;

    #[derive(Debug, Parser)]
    struct TestCaCli {
        #[command(subcommand)]
        cmd: Cmd,
    }

    #[test]
    fn remove_server_cli_requires_and_parses_exact_uuid() {
        let id = admin_proto::AdminServerId::new();
        let parsed = TestCaCli::try_parse_from([
            "CA",
            "remove-server",
            &id.to_string(),
            "--server",
            "10.0.0.1:4565",
            "--admin",
            "root",
            "--password-stdin",
            "--accept-glyph",
            "AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AA",
        ])
        .unwrap();
        let Cmd::RemoveServer(args) = parsed.cmd else {
            panic!("expected remove-server")
        };
        assert_eq!(args.server_id, id);

        let err = TestCaCli::try_parse_from(["CA", "remove-server"])
            .expect_err("a destructive target must be explicit");
        assert!(err.to_string().contains("SERVER-ID"));
    }

    #[test]
    fn reconcile_ca_cli_is_explicit() {
        let parsed = TestCaCli::try_parse_from([
            "CA",
            "reconcile-ca",
            "--server",
            "10.0.0.2:4565",
            "--admin",
            "root",
            "--password-stdin",
            "--accept-glyph",
            "AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AAAAA AA",
        ])
        .unwrap();
        assert!(matches!(parsed.cmd, Cmd::ReconcileCa(_)));
    }

    #[test]
    fn ca_surface_has_no_backup_or_recover_ca_aliases() {
        assert!(TestCaCli::try_parse_from(["CA", "backup", "/tmp/b"]).is_err());
        assert!(TestCaCli::try_parse_from(["CA", "recover-ca"]).is_err());
    }

    #[cfg(unix)]
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
            recovery: RecoveryAuth {
                recovery_password_file: None,
                recovery_password_stdin: false,
            },
        })
        .unwrap();
        assert!(cert_path.exists());
        // Confirm we actually wrote a PEM-encoded leaf cert; the
        // engine-side `Ca::sign_request` test already verifies that
        // signed certs chain back to the CA.
        let bytes = std::fs::read(&cert_path).unwrap();
        assert!(bytes.starts_with(b"-----BEGIN CERTIFICATE-----"));
    }

    #[cfg(unix)]
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
            recovery: RecoveryAuth {
                recovery_password_file: None,
                recovery_password_stdin: false,
            },
        })
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--accept-csr-san"),
            "error should name --accept-csr-san: {msg}"
        );
    }

    #[cfg(unix)]
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
            recovery: RecoveryAuth {
                recovery_password_file: None,
                recovery_password_stdin: false,
            },
        })
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("--san"), "error should mention --san: {msg}");
    }

    #[cfg(unix)]
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
            recovery: RecoveryAuth {
                recovery_password_file: None,
                recovery_password_stdin: false,
            },
        })
        .unwrap_err();
        assert!(format!("{err:#}").contains("not both"));
    }

    #[cfg(unix)]
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
        let issued = runtime()
            .unwrap()
            .block_on(async {
                let lock = netidx_admin::config_lock::ConfigDirLock::acquire_for_ca_dir(
                    ca_dir.path(),
                )
                .await?;
                netidx_admin::plan::ca_setup::issue_identity_into(
                    &lock,
                    &ca,
                    "resolver.example.com",
                    out.path().to_path_buf(),
                    2048,
                    None,
                    &[],
                )
                .await
            })
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

    /// An offline CA (no admin server) is minted with exactly one signing
    /// slot — `recovery`, holding a generated password never typed — and no
    /// role admins (a role admin needs a daemon to authenticate to). The
    /// whole path runs with no prompts and no TTY. `--insecure-no-tpm`
    /// keeps it from refusing on a TPM-less CI host.
    #[cfg(unix)]
    #[test]
    fn offline_ca_init_makes_exactly_the_recovery_slot() {
        let scratch = tempfile::tempdir().unwrap();
        let dir = scratch.path().join("ca");
        // Drive the library's create_vaulted_ca through the strict answerer —
        // the exact path `ca init` takes. Offline (setup_server: Some(false)),
        // so no superuser password or glyph is asked for.
        let mut ans =
            crate::admin::answer_cli::make_flag_answerer(None, false, None).unwrap();
        let config_lock = ConfigDirLock::acquire(scratch.path()).unwrap();
        let (_ca, _need) = runtime()
            .unwrap()
            .block_on(ca_setup::create_vaulted_ca(
                &mut ans,
                &config_lock,
                ca_setup::NewCaOpts {
                    dir: dir.clone(),
                    common_name: Some("CA.example.com".into()),
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
                    server_enroll_scopes: vec!["/".into()],
                    server_enroll_roles: admin_proto::Role::Resolver
                        | admin_proto::Role::IdMap,
                    insecure_no_tpm: true,
                    setup_server: Some(false),
                    listen: None,
                    listen_hint: None,
                    units_dir: None,
                },
            ))
            .unwrap();
        // Exactly the recovery signing slot, and nothing else — no autorenew
        // (no daemon), no superuser role (offline).
        let cadir = runtime()
            .unwrap()
            .block_on(async {
                let lock =
                    netidx_admin::config_lock::ConfigDirLock::acquire_for_ca_dir(&dir)
                        .await?;
                netidx_admin::ca_store::CaDir::open(lock, &dir).await
            })
            .unwrap();
        assert_eq!(
            cadir.vault.signing_slot_names().unwrap(),
            vec![RECOVERY_ADMIN.to_string()]
        );
        let admins = cadir.vault.list_admins().unwrap();
        assert_eq!(admins.len(), 1, "offline CA has only the recovery slot");
        assert_eq!(admins[0].admin, RECOVERY_ADMIN);
        assert_eq!(admins[0].kind, SlotKind::Signing);
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
            perms_scope: vec![],
            ca_dir: Some("/nonexistent".into()),
        }));
        let msg = format!("{:#}", r.unwrap_err());
        assert!(msg.contains("add-role"), "must point at add-role: {msg}");
    }

    /// 730 days is what a netidx certificate may live. It used to be written
    /// out three times — the clap defaults here, the CA's own constant, and
    /// the TUI's policy template — which is exactly the shape a value drifts
    /// in. There is one now; this fails if a fourth appears.
    #[test]
    fn every_max_validity_default_is_the_one_leaf_validity() {
        use netidx_admin::plan::ca_setup::{DEFAULT_LEAF_VALIDITY, policy_template};
        let of = |name: &'static str, augment: fn(clap::Command) -> clap::Command| {
            (name, augment(clap::Command::new(name)))
        };
        for (name, cmd) in [
            of("policy flags", <PolicyFlags as clap::Args>::augment_args),
            of("admin add", <AdminAddArgs as clap::Args>::augment_args),
            of("tls join", <JoinArgs as clap::Args>::augment_args),
        ] {
            let flag = cmd
                .get_arguments()
                .find(|a| a.get_long() == Some("max-validity"))
                .or_else(|| {
                    cmd.get_arguments().find(|a| a.get_long() == Some("validity"))
                })
                .expect("the validity flag");
            let default = flag
                .get_default_values()
                .first()
                .expect("a default")
                .to_str()
                .expect("utf8");
            assert_eq!(
                humantime::parse_duration(default).unwrap(),
                DEFAULT_LEAF_VALIDITY,
                "{name} default {default:?} disagrees with the engine"
            );
        }
        assert_eq!(policy_template().max_validity, DEFAULT_LEAF_VALIDITY);
    }

    /// `tls join` gained the enrollment ceremony every other join already
    /// used, so its surface has to offer the two choices that ceremony makes.
    #[test]
    fn tls_join_offers_key_protection_and_refuses_to_clobber_by_default() {
        let cmd = <JoinArgs as clap::Args>::augment_args(clap::Command::new("join"));
        let longs: Vec<_> = cmd.get_arguments().filter_map(|a| a.get_long()).collect();
        assert!(longs.contains(&"key-protection"), "{longs:?}");
        assert!(longs.contains(&"force"), "{longs:?}");
        let parsed = JoinArgs::from_arg_matches(
            &cmd.clone()
                .try_get_matches_from(["join", "--server", "10.0.0.1:4565"])
                .unwrap(),
        )
        .unwrap();
        assert!(!parsed.force, "replacing an identity must be asked for");
        assert!(parsed.admin.is_none(), "no --admin means queue for approval");
    }
}
