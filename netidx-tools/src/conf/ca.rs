use anyhow::{anyhow, Context, Result};
use netidx_conf::{
    atomic,
    ca::{self, Ca, CaParams, IssueParams, IssuedFiles, SanEntry, Subject},
    ca_vault, conf_client,
    conf_proto::{self, NodeKind},
    fingerprint::{ColorMode, Fingerprint},
    paths, tls,
};
use clap::{Args, Subcommand};
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

use super::{prompt, service};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// create a new local CA (keyslot vault; can serve via `conf server`)
    Init(InitParams),
    /// issue a leaf certificate from a CA
    Issue(IssueArgs),
    /// generate a private key + CSR locally, to be signed by a CA elsewhere
    Request(RequestArgs),
    /// sign an externally-supplied CSR with a local CA
    Sign(SignArgs),
    /// list local CAs
    List,
    /// manage CA admin keyslots (add / revoke / set-policy / list)
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
    /// show the CA's fingerprint + identicon for out-of-band verification
    Fingerprint(FingerprintArgs),
    /// request a certificate from a conf server and install it
    Join(JoinArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCmd {
    /// add an admin keyslot (a new password that can sign)
    Add(AdminAddArgs),
    /// revoke an admin keyslot
    Remove(AdminRemoveArgs),
    /// replace an admin's issuance policy (allowed SANs / max validity)
    SetPolicy(AdminSetPolicyArgs),
    /// list admin keyslots and their issuance policy
    List(AdminScopeArgs),
}

#[derive(Args, Debug)]
pub(crate) struct AdminScopeArgs {
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
    /// Max validity (days) this admin may issue. Default 730.
    #[arg(long, default_value = "730")]
    pub max_validity_days: u32,
    /// id-map groups for identities signed by this admin (repeatable;
    /// first is primary). Prompted when omitted; an explicit empty
    /// string disables registration.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// Whether this admin may enroll new conf servers. Prompted when
    /// omitted (default no for added admins).
    #[arg(long)]
    pub may_enroll_servers: Option<bool>,
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
    /// Max validity (days) this admin may issue. Default 730.
    #[arg(long, default_value = "730")]
    pub max_validity_days: u32,
    /// id-map groups for identities signed by this admin (repeatable;
    /// first is primary). Prompted when omitted; an explicit empty
    /// string disables registration.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// Whether this admin may enroll new conf servers. Prompted when
    /// omitted.
    #[arg(long)]
    pub may_enroll_servers: Option<bool>,
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
    /// Validity (days) to request. Default 730 (capped by server policy).
    #[arg(long, default_value = "730")]
    pub validity_days: u32,
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
    #[arg(long, default_value = "7300")]
    pub validity_days: u32,
    /// The first admin's name (keyslot label). Prompted when omitted,
    /// defaulting to the current unix user.
    #[arg(long)]
    pub admin: Option<String>,
    /// SAN glob the first admin may issue (repeatable). Prompted when
    /// omitted — e.g. `*.example.com`.
    #[arg(long = "allow-san", num_args = 1)]
    pub allow_san: Vec<String>,
    /// Max validity (days) the first admin may issue. Default 730.
    #[arg(long, default_value = "730")]
    pub max_validity_days: u32,
    /// id-map groups for identities signed by the first admin
    /// (repeatable; first is primary). Prompted when omitted.
    #[arg(long = "id-map-group", num_args = 1)]
    pub id_map_groups: Vec<String>,
    /// Whether the first admin may enroll new conf servers. Prompted
    /// when omitted (default yes for the founding admin).
    #[arg(long)]
    pub may_enroll_servers: Option<bool>,
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
    #[arg(long, default_value = "730")]
    pub validity_days: u32,
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
    /// Path to the CSR (PEM-encoded) to sign. When omitted, `sign`
    /// runs in queue mode: list the pending signing requests on the
    /// conf server and approve/deny them interactively.
    pub csr_path: Option<PathBuf>,
    /// Conf server to work the queue on (queue mode). Defaults to
    /// this host's own conf server, then mDNS discovery — so an
    /// enrollment admin can approve from their workstation without
    /// shell access to the CA host.
    #[arg(long)]
    pub server: Option<SocketAddr>,
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
    #[arg(long, default_value = "730")]
    pub validity_days: u32,
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
        Cmd::Request(p) => request(p),
        Cmd::Sign(p) => sign(p),
        Cmd::List => list(),
        Cmd::Admin { cmd } => admin(cmd),
        Cmd::Fingerprint(p) => fingerprint(p),
        Cmd::Join(p) => join(p),
    }
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
    pub validity_days: u32,
    /// First admin name; `None` ⇒ prompt, defaulting to the current
    /// unix user.
    pub admin: Option<String>,
    /// First admin's issuance policy globs; empty ⇒ prompt (default
    /// `*.<domain>` when `domain` is set, else derived from the CN).
    pub allowed_san: Vec<String>,
    pub max_validity_days: u32,
    /// First admin's id-map groups; empty ⇒ prompt (default `users`).
    pub id_map_groups: Vec<String>,
    /// Whether the first admin may enroll conf servers; `None` ⇒
    /// prompt, defaulting to yes (someone has to be able to grow the
    /// network).
    pub may_enroll_servers: Option<bool>,
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
/// by `netidx conf ca init` and the `netidx conf install resolver`
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
    let common_name = resolve_ca_cn(opts.common_name, opts.domain.as_deref())?;
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
    // `--admin` short-circuits the prompt; otherwise ask, seeding the
    // default with the current unix user. On a non-TTY (automation) the
    // default is taken silently, preserving the old auto-name behavior.
    let admin = match env_user_name() {
        Some(user) => prompt::string_with_default("CA admin name", opts.admin, &user)?,
        None => prompt::required_string("CA admin name", opts.admin)?,
    };
    // Validate the vault inputs *before* anything is written to disk:
    // `Ca::init_vaulted` commits `certificate.pem` + `serial`, and only
    // then does `ca_vault::create` run — so a bad input rejected there
    // (e.g. an empty admin name slipping past the CLI as `--admin ''`)
    // would leave a half-built CA with no signing key that also blocks a
    // retry (the dir then looks like an existing CA).
    if admin.trim().is_empty() {
        bail!("admin name must not be empty");
    }
    let san = parse_sans(&opts.san, &common_name)?;
    let password = collect_required_password(&format!(
        "set a CA password for admin {admin:?} (this signs certs)"
    ))?;
    let policy = prompt_policy(
        &PolicyArgs {
            allow_san: &opts.allowed_san,
            max_validity_days: opts.max_validity_days,
            id_map_groups: &opts.id_map_groups,
            may_enroll_servers: opts.may_enroll_servers,
        },
        // The founding admin defaults to being able to grow the
        // network — someone has to.
        true,
        &common_name,
        opts.domain.as_deref(),
    )?;

    // Generate the CA with its key returned (never written to disk in
    // plaintext) and seal it into the vault under the first admin.
    let (ca, key_pem) = Ca::init_vaulted(&CaParams {
        directory: opts.dir.clone(),
        subject: Subject {
            common_name,
            country: opts.country,
            state: opts.state,
            locality: opts.locality,
            organization: opts.organization,
        },
        san,
        key_bits: opts.key_bits,
        validity_days: opts.validity_days,
    })?;
    // Belt and suspenders: should sealing still fail (e.g. an I/O error
    // mid-write), roll back the cert + serial that `init_vaulted`
    // committed so the directory isn't a keyless half-CA that blocks a
    // clean retry. The in-memory key is dropped (zeroized) on the way
    // out, so nothing sensitive is left behind.
    if let Err(e) = ca_vault::create(&opts.dir, &key_pem, &admin, &password, policy) {
        let _ = std::fs::remove_file(opts.dir.join("certificate.pem"));
        let _ = std::fs::remove_file(opts.dir.join("serial"));
        return Err(e).context("sealing CA key into the vault");
    }

    println!("created a new CA at {}", opts.dir.display());
    println!("  admin {admin:?} can sign; the CA key is encrypted at rest (keyslot vault)");
    println!();
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
        super::server::setup_server(super::server::SetupArgs {
            ca_dir: &opts.dir,
            ca: &ca,
            domain: &domain,
            listen: opts.listen,
            listen_hint: opts.listen_hint,
            units_dir: opts.units_dir.as_deref(),
        })?
    } else {
        service::ServiceNeed::NONE
    };
    Ok((ca, need))
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
        validity_days: p.validity_days,
        admin: p.admin,
        allowed_san: p.allow_san,
        max_validity_days: p.max_validity_days,
        id_map_groups: p.id_map_groups,
        may_enroll_servers: p.may_enroll_servers,
        setup_server,
        listen: p.listen,
        // No resolver in this flow; default_ca_listen_ip falls back to
        // an existing resolver's IP, then the public IP.
        listen_hint: None,
        units_dir,
    })?;

    // Single end-of-process hook — the same one the `conf install`
    // templates use.
    service::offer(need, service::ServiceGate {
        dry_run: false,
        no_service: p.no_service,
        with_service: p.with_service,
    })
}

// -- ca admin -----------------------------------------------------------------

fn admin(cmd: AdminCmd) -> Result<()> {
    match cmd {
        AdminCmd::Add(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let name = prompt::required_string("new admin name", a.name)?;
            // Seed the policy suggestion from the CA's own cert domain
            // (e.g. `ca.ryu-oh.org` → `*.ryu-oh.org`). Added admins
            // default to NOT being able to enroll conf servers.
            let policy = prompt_policy(
                &PolicyArgs {
                    allow_san: &a.allow_san,
                    max_validity_days: a.max_validity_days,
                    id_map_groups: &a.id_map_groups,
                    may_enroll_servers: a.may_enroll_servers,
                },
                false,
                &existing_ca_cn(&dir),
                None,
            )?;
            let existing = collect_existing_password(
                "your own (existing) admin password — unlocks the CA key to enroll the new admin",
            )?;
            let new_pw =
                collect_required_password(&format!("password for new admin {name:?}"))?;
            ca_vault::add_admin(&dir, &existing, &name, &new_pw, policy)?;
            println!("added admin {name:?}");
            Ok(())
        }
        AdminCmd::SetPolicy(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let name = prompt::required_string("admin whose policy to set", a.name)?;
            let policy = prompt_policy(
                &PolicyArgs {
                    allow_san: &a.allow_san,
                    max_validity_days: a.max_validity_days,
                    id_map_groups: &a.id_map_groups,
                    may_enroll_servers: a.may_enroll_servers,
                },
                false,
                &existing_ca_cn(&dir),
                None,
            )?;
            // Report the resolved policy (the prompt may have filled it),
            // not the raw flag.
            let summary = format!(
                "allowed_san={:?} max_validity_days={} id_map_groups={:?} \
                 may_enroll_servers={}",
                policy.allowed_san,
                policy.max_validity_days,
                policy.id_map_groups,
                policy.may_enroll_servers
            );
            // Authority: any current admin's password (the same flat
            // model as add/remove). You don't need the target's.
            let auth = collect_existing_password(&format!(
                "your own admin password (authorizes setting policy for {name:?})"
            ))?;
            ca_vault::set_policy(&dir, &auth, &name, policy)?;
            println!("updated policy for admin {name:?}: {summary}");
            Ok(())
        }
        AdminCmd::Remove(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let name = prompt::required_string("admin to revoke", a.name)?;
            // The authorizing password is the operator's OWN (any
            // current admin's) — never the departed admin's. You revoke
            // a slot by name; you don't need its password.
            let auth = collect_existing_password(&format!(
                "your own admin password (authorizes revoking {name:?})"
            ))?;
            ca_vault::remove_admin(&dir, &auth, &name, a.force)?;
            println!("revoked admin {name:?}");
            Ok(())
        }
        AdminCmd::List(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let admins = ca_vault::list_admins(&dir)?;
            if admins.is_empty() {
                println!("(no admins — this CA is not vault-protected)");
            }
            for (name, pol) in admins {
                println!(
                    "{name}: allowed_san={:?} max_validity_days={} \
                     id_map_groups={:?} may_enroll_servers={}",
                    pol.allowed_san,
                    pol.max_validity_days,
                    pol.id_map_groups,
                    pol.may_enroll_servers
                );
            }
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

fn join(p: JoinArgs) -> Result<()> {
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
        p.validity_days,
        groups,
        &identity,
    ))?;
    let dir = tls::identity_dir(&name)?;
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    atomic::write_atomic(&dir.join("certificate.pem"), issued.cert_pem.as_bytes(), 0o644)?;
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
fn env_user_name() -> Option<String> {
    for var in ["USER", "LOGNAME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

/// CLI-provided policy inputs; whatever is absent gets prompted.
struct PolicyArgs<'a> {
    allow_san: &'a [String],
    max_validity_days: u32,
    id_map_groups: &'a [String],
    may_enroll_servers: Option<bool>,
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
        // in the SignRequest). Empty answer ⇒ this admin's signs never
        // register id-map identities.
        let entry = prompt::string_with_default(
            "id-map groups this admin may assign when enrolling \
             (comma-separated; empty for none)",
            None,
            "users",
        )?;
        entry
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };
    let may_enroll_servers = match args.may_enroll_servers {
        Some(b) => b,
        None => prompt::confirm(
            "may this admin enroll new conf servers (more privileged than any \
             SAN glob)?",
            enroll_default,
        )?,
    };
    Ok(ca_vault::Policy {
        allowed_san,
        max_validity_days: args.max_validity_days,
        id_map_groups,
        may_enroll_servers,
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
/// non-TTY — a vaulted CA must have a real password.
fn collect_required_password(label: &str) -> Result<String> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Err(anyhow!(
            "{label}: a password is required but stdin is not a TTY"
        ));
    }
    loop {
        let pw = rpassword::prompt_password(format!("{label}: "))?;
        if pw.is_empty() {
            eprintln!("password must not be empty");
            continue;
        }
        let again = rpassword::prompt_password("again: ")?;
        if again != pw {
            eprintln!("passwords did not match; try again");
            continue;
        }
        return Ok(pw);
    }
}

/// Prompt once for an existing password (no confirmation).
fn collect_existing_password(label: &str) -> Result<String> {
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
    let issued = ca.issue(&IssueParams {
        subject: Subject {
            common_name: cn.clone(),
            country: p.country,
            state: p.state,
            locality: p.locality,
            organization: p.organization,
        },
        san,
        key_bits: p.key_bits,
        validity_days: p.validity_days,
        out_dir,
        // Leaf key encryption is wired through the install flow
        // (`netidx conf init`), where the engine knows how to plumb
        // an askpass entry into the emitted client config. The bare
        // `ca issue` CLI deliberately stays unencrypted: callers
        // here are doing manual cert issuance and don't necessarily
        // have a netidx config to receive the askpass.
        password: None,
    })?;
    println!("issued cert:");
    println!("  cn:          {}", cn);
    println!("  private key: {}", issued.private_key.display());
    println!("  certificate: {}", issued.certificate.display());
    Ok(())
}

fn request(p: RequestArgs) -> Result<()> {
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
    if p.csr_path.is_none() {
        return sign_queue(p);
    }
    let csr_path =
        prompt::required_path("path to the CSR to sign", p.csr_path.take())?;
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
    let cert_pem = ca.sign_request(&csr_pem, &san, p.validity_days)?;
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
/// - stdin is not a TTY (scripts use `netidx conf id-map set-user`
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
                 create one with `netidx conf id-map init`)",
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
    let mut group_names: Vec<&str> =
        map.groups.keys().map(|k| k.as_str()).collect();
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
        bail!(
            "no groups specified — at least the primary group is required"
        );
    }
    let (primary, secondary): (&str, &[&str]) = (groups[0], &groups[1..]);
    let uid: u32 = prompt::parsed_with_default(
        "uid",
        None,
        &id_map::next_uid(&map).to_string(),
    )?;
    let prev = id_map::upsert_identity(&mut map, &identity_name, uid, primary, secondary)?;
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

/// Interactive queue mode: list the pending signing requests on the
/// conf server, review one at a time (matching the request code the
/// enrollee read out — the fingerprint of the CSR's public key,
/// computed locally from the CSR, never trusted from the wire), and
/// approve (choosing the id-map groups) or deny. Works from anywhere
/// that can reach the conf server — enrollment admins don't need shell
/// access to the CA host.
fn sign_queue(p: SignArgs) -> Result<()> {
    use super::init::{self, ConfServers};
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    // Where's the conf server? `--server`, else this host's own conf
    // server, else discovery (browse → confirm → aggregate).
    let (server, discovered_identity) = match p.server.or_else(local_conf_server_listen)
    {
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
        println!();
        println!("pending signing requests:");
        for (i, e) in queue.iter().enumerate() {
            let code = conf_client::csr_fingerprint(&e.csr_pem)
                .map(|f| f.short())
                .unwrap_or_else(|_| "????????".to_string());
            println!(
                "  {}) {}  code {}  kind {:?}  age {}  from {}",
                i + 1,
                e.requested_name,
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
            Ok(n) if (1..=queue.len()).contains(&n) => &queue[n - 1],
            _ => {
                eprintln!("enter a number between 1 and {}, or 'q'", queue.len());
                continue;
            }
        };
        let fp = conf_client::csr_fingerprint(&entry.csr_pem)
            .context("the queued CSR does not parse — deny it")?;
        println!();
        println!("  name:     {}", entry.requested_name);
        println!("  kind:     {:?}", entry.kind);
        println!("  validity: {} days (capped by your policy)", entry.requested_validity_days);
        println!("  from:     {}", entry.peer);
        println!("  request code:");
        println!("  SHA256  {}", fp.text());
        println!("{}", fp.identicon(ColorMode::detect()));
        // The mutual-glyph moment: the enrollee's terminal shows this
        // same code; the requester sent it over a channel the admin
        // trusts. A mismatch means the queue entry is NOT the request
        // the admin thinks it is.
        if !prompt::confirm("does this code match what the requester sent you?", false)?
        {
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
                // are chosen here, bounded by this admin's policy.
                let groups = init::prompt_id_map_groups(
                    &[],
                    init::default_id_map_groups(entry.kind),
                )?;
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
fn local_conf_server_listen() -> Option<SocketAddr> {
    let path = paths::discover_conf_server_config().ok()?;
    let cfg = netidx_conf::conf_server_config::ConfServerConfig::load(&path).ok()?;
    let mut addr = cfg.listen;
    if addr.ip().is_unspecified() {
        addr.set_ip(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    }
    Some(addr)
}

fn fmt_age(secs: u64) -> String {
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
    if let Ok(cert) = std::fs::read(dir.join("certificate.pem")) {
        if let Ok(fp) = Fingerprint::of_cert_pem(&cert) {
            println!(
                "  fingerprint: {} … (`netidx conf ca fingerprint` for the full id)",
                fp.short()
            );
        }
    }
    // Key storage: the current format is the keyslot vault (key in
    // `vault.json`, not `private.key`), so detect that before falling
    // back to the legacy single-key format.
    if ca_vault::exists(&dir) {
        let admins = ca_vault::list_admins(&dir)
            .map(|a| a.into_iter().map(|(n, _)| n).collect::<Vec<_>>())
            .unwrap_or_default();
        if admins.is_empty() {
            println!("  key:    keyslot vault");
        } else {
            println!("  key:    keyslot vault — admins: {}", admins.join(", "));
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
// Used by `netidx conf install resolver --auth tls` to offer a
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
                && (ca_vault::exists(&dir) || dir.join("private.key").is_file())
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
    if ca_vault::exists(dir) {
        if !prompt::stdin_is_tty() {
            bail!(
                "the CA at {} is vault-protected and needs an admin password, \
                 but stdin is not a TTY",
                dir.display(),
            );
        }
        let pw = collect_existing_password("your CA admin password")?;
        let unlocked = ca_vault::unlock(dir, &pw)
            .with_context(|| format!("unlocking the CA vault at {}", dir.display()))?;
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
        if let SanEntry::Dns(d) = s {
            if d.eq_ignore_ascii_case(conf_proto::SERVING_SAN) {
                bail!(
                    "{:?} is reserved for the conf server's serving certificate and \
                     can't be issued here",
                    conf_proto::SERVING_SAN
                );
            }
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
    ca.issue(&IssueParams {
        subject: Subject::cn(name),
        // Exactly one DNS SAN, matching the CN — that's what the
        // netidx TLS validator requires of a member-server cert.
        san: vec![SanEntry::Dns(name.to_string())],
        key_bits,
        validity_days: ca::DEFAULT_LEAF_VALIDITY_DAYS,
        out_dir,
        password: password.map(|s| s.to_string()),
    })
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
            val.parse::<IpAddr>()
                .map_err(|e| anyhow!("invalid SAN ip {val:?}: {e}"))?,
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
        assert!(ensure_san_not_reserved(&[SanEntry::Dns(reserved.to_uppercase())]).is_err());
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
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let cert_path = scratch.path().join("client.pem");
        sign(SignArgs {
            server: None,
            csr_path: Some(csr_path.clone()),
            san: vec![],
            // Explicit accept: the round trip flow simulates the admin
            // who has looked at the CSR and is happy to sign as-is.
            accept_csr_san: true,
            validity_days: 30,            ca_dir: Some(ca_dir.clone()),
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
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let out_cert = scratch.path().join("out.pem");
        sign(SignArgs {
            server: None,
            csr_path: Some(csr_path),
            san: vec![],
            accept_csr_san: false,
            validity_days: 30,            ca_dir: Some(ca_dir),
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
        let kr = ca::generate_csr(
            &Subject::cn("no-san"),
            &[],
            2048,
            None,
        )
        .unwrap();
        let csr_path = scratch.path().join("no-san.csr");
        std::fs::write(&csr_path, &kr.csr_pem).unwrap();
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("strict-ca"),
                san: vec![SanEntry::Dns("strict-ca".into())],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let err = sign(SignArgs {
            server: None,
            csr_path: Some(csr_path),
            san: vec![],
            accept_csr_san: false,
            validity_days: 30,            ca_dir: Some(ca_dir),
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
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let err = sign(SignArgs {
            server: None,
            csr_path: Some(csr_path),
            san: vec!["dns:x.example.com".into()],
            accept_csr_san: true,
            validity_days: 30,            ca_dir: Some(ca_dir),
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
        assert_eq!(
            default_cert_filename(None),
            PathBuf::from("certificate.pem"),
        );
        // Slashes in the CN can't produce a traversing path.
        assert_eq!(
            default_csr_filename("../sneaky"),
            PathBuf::from(".._sneaky.csr"),
        );
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
                validity_days: 30,
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
        assert!(
            msg.contains("not a TTY"),
            "should report non-TTY context: {msg}"
        );
    }
}
