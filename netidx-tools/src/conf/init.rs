//! `netidx conf install <type>` — render and apply a `RenderedTemplate`
//! for one of the three v1 templates.

use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::config::DefaultAuthMech;
use netidx_conf::{
    ca_join, ca_proto,
    fingerprint::ColorMode,
    netshape::NetShape,
    paths,
    template::{
        self, AuthChoice, ParentRef, ReferralAuth, RenderedTemplate, TlsIdentitySpec,
    },
};
use zeroize::Zeroizing;
// `tls::identity_dir` is referenced only by the unix-gated TLS cert
// generation flows (`generate_and_wait_for_parent_cert`,
// `resolver_tls_generate`); on Windows the import would be unused.
#[cfg(unix)]
use netidx_conf::tls;
use std::{
    io::IsTerminal,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
};
use clap::{Args, Subcommand};

// `ca` submodule depends on netidx_conf::ca which is unix-only.
#[cfg(unix)]
use super::ca;
use super::{prompt, service};

#[derive(Subcommand, Debug)]
pub(crate) enum Params {
    /// local-auth resolver + matching client
    Workstation(WorkstationFlags),
    /// single network-facing resolver-server
    Resolver(ResolverFlags),
    /// publisher-host config pointing at a remote cluster
    Publisher(PublisherFlags),
}

pub(crate) fn run(p: Params) -> Result<()> {
    match p {
        Params::Workstation(f) => run_workstation(f),
        Params::Resolver(f) => run_resolver(f),
        Params::Publisher(f) => run_publisher(f),
    }
}

// -- Common -------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthKind {
    Anonymous,
    Local,
    Krb5,
    Tls,
}

impl FromStr for AuthKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "anonymous" => Ok(Self::Anonymous),
            "local" => Ok(Self::Local),
            "krb5" => Ok(Self::Krb5),
            "tls" => Ok(Self::Tls),
            _ => bail!("auth must be one of anonymous|local|krb5|tls"),
        }
    }
}

impl AuthKind {
    fn default_mech(self) -> DefaultAuthMech {
        match self {
            Self::Anonymous => DefaultAuthMech::Anonymous,
            Self::Local => DefaultAuthMech::Local,
            Self::Krb5 => DefaultAuthMech::Krb5,
            Self::Tls => DefaultAuthMech::Tls,
        }
    }
}

#[derive(Args, Debug, Clone)]
struct TlsIdentityFlags {
    /// Source path of our certificate.
    #[arg(long = "tls-cert")]
    cert: Option<PathBuf>,
    /// Source path of our private key.
    #[arg(long = "tls-key")]
    key: Option<PathBuf>,
    /// Source path of the trusted-CA bundle.
    #[arg(long = "tls-trusted")]
    trusted: Option<PathBuf>,
    /// Our SAN — drives the install subdirectory under
    /// `~/.config/netidx/tls/<our-name>/`. Optional: defaults to
    /// the DNS SAN inside `--tls-cert` so the on-disk name always
    /// agrees with what netidx will see on the wire.
    #[arg(long = "tls-our-name")]
    our_name: Option<String>,
    /// Server domain pattern this identity covers — the key in
    /// `tls.identities`. Closest reverse-domain match wins.
    /// Optional: defaults to the *domain* part of `our_name` (e.g.
    /// SAN `mazikeen.local` ⇒ key `local`), matching the
    /// interactive cascade.
    #[arg(long = "tls-server-pattern")]
    server_pattern: Option<String>,
}

impl TlsIdentityFlags {
    /// Return `Some(spec)` if any flag in this group is populated,
    /// `None` if none are. Errors if a required flag is missing
    /// once the group is "active" — but `--tls-our-name` and
    /// `--tls-server-pattern` are derivable from the cert SAN, so
    /// they're optional.
    fn to_spec(&self) -> Result<Option<TlsIdentitySpec>> {
        let any = self.cert.is_some()
            || self.key.is_some()
            || self.trusted.is_some()
            || self.our_name.is_some()
            || self.server_pattern.is_some();
        if !any {
            return Ok(None);
        }
        let cert = self.cert.as_ref().context("--tls-cert required")?.clone();
        let key = self.key.as_ref().context("--tls-key required")?.clone();
        let trusted =
            self.trusted.as_ref().context("--tls-trusted required")?.clone();
        // Default `our_name` to the cert's DNS SAN — same trick the
        // interactive cascade uses for BYO certs. Asking the
        // operator to type the SAN that's already in the cert just
        // lets them get it wrong; reading it is always correct.
        let our_name = match &self.our_name {
            Some(s) => s.clone(),
            None => netidx_conf::tls::extract_dns_san_from_pem(&cert).with_context(
                || {
                    format!(
                        "deriving --tls-our-name from {} — supply a cert with a \
                         DNS SubjectAlternativeName entry, or pass \
                         --tls-our-name explicitly",
                        cert.display(),
                    )
                },
            )?,
        };
        // Default `server_pattern` to the *domain* part of the SAN.
        // netidx keys `tls.identities` by trust domain (one entry
        // covers any host SAN under that domain via the
        // reverse-domain prefix match), so the domain is almost
        // always what the operator wants. Override with
        // `--tls-server-pattern` if a more specific key is needed.
        let server_pattern = match &self.server_pattern {
            Some(s) => s.clone(),
            None => netidx_conf::tls::domain_from_san(&our_name)
                .with_context(|| {
                    format!(
                        "deriving --tls-server-pattern from SAN {our_name:?} — \
                         supply a `<user>.<domain>` SAN, or pass \
                         --tls-server-pattern explicitly",
                    )
                })?
                .to_string(),
        };
        Ok(Some(TlsIdentitySpec {
            server_pattern: ArcStr::from(server_pattern),
            our_name: ArcStr::from(our_name),
            certificate: cert,
            private_key: key,
            trusted,
            dest_dir: None,
            // CLI-flag path doesn't carry an askpass override.
            // Operators driving the CLI non-interactively are
            // expected to bring their own (unencrypted) key — or
            // edit `tls.askpass` in the emitted config by hand.
            askpass: None,
        }))
    }
}

#[derive(Args, Debug, Clone)]
struct ParentFlags {
    /// Parent referral address. Required to enable a parent.
    /// Currently supports a single address; for multiple addresses,
    /// edit the generated resolver.json.
    #[arg(long = "parent-addr")]
    parent_addr: Option<SocketAddr>,
    /// Auth scheme for the parent. Required when `--parent-addr` is set.
    #[arg(long = "parent-auth")]
    parent_auth: Option<AuthKind>,
    /// Parent's Kerberos SPN (with `--parent-auth krb5`).
    #[arg(long = "parent-spn")]
    parent_spn: Option<String>,
    /// Parent's local-auth socket path (with `--parent-auth local`).
    #[arg(long = "parent-socket")]
    parent_socket: Option<PathBuf>,
    /// Parent's TLS server name (with `--parent-auth tls`).
    #[arg(long = "parent-tls-name")]
    parent_tls_name: Option<String>,
    /// Netidx path at which **this** resolver attaches in the
    /// parent's namespace. Everything *above* this path in the tree
    /// is handled by the parent; everything *at* and *below* this
    /// path lives in this resolver's local store.
    ///
    /// Default: the resolver's own base path. For the workstation
    /// template (base `/local`) that means `/local`; for the
    /// standalone-resolver template (typically `/`) it means `/`.
    /// Override only if you want this resolver's tree to attach at
    /// a different path in the parent than where it serves locally
    /// (rare — the two usually match by convention).
    #[arg(long = "parent-path")]
    parent_path: Option<String>,
    /// TTL in seconds.
    #[arg(long = "parent-ttl")]
    parent_ttl: Option<u16>,
}

impl ParentFlags {
    /// True if the operator passed any `--parent-*` flag. Used by
    /// callers to decide between flag-driven construction and the
    /// interactive prompt cascade — the prompt should only fire when
    /// nothing was specified, so a script that passes `--parent-spn`
    /// without `--parent-addr` fails loudly instead of silently
    /// dropping the flag on a non-TTY.
    fn any_set(&self) -> bool {
        let Self {
            parent_addr,
            parent_auth,
            parent_spn,
            parent_socket,
            parent_tls_name,
            parent_path,
            parent_ttl,
        } = self;
        parent_addr.is_some()
            || parent_auth.is_some()
            || parent_spn.is_some()
            || parent_socket.is_some()
            || parent_tls_name.is_some()
            || parent_path.is_some()
            || parent_ttl.is_some()
    }

    /// Build a `ParentRef` from the parent-* flags. `default_path`
    /// is the netidx path at which the current resolver attaches in
    /// the parent's namespace when the operator didn't pass
    /// `--parent-path` — callers supply the surrounding template's
    /// base, since that's the conventional value.
    fn to_parent_ref(&self, default_path: &str) -> Result<Option<ParentRef>> {
        let addr = match self.parent_addr {
            Some(a) => a,
            None => {
                // Catch the silent-misuse case: any of the
                // parent-* satellite flags without --parent-addr
                // would otherwise just no-op into "no parent."
                if self.any_set() {
                    bail!("--parent-* flags require --parent-addr");
                }
                return Ok(None);
            }
        };
        let kind =
            self.parent_auth.context("--parent-auth required with --parent-addr")?;
        let auth = match kind {
            AuthKind::Anonymous => ReferralAuth::Anonymous,
            AuthKind::Local => ReferralAuth::Local(ArcStr::from(
                self.parent_socket
                    .as_ref()
                    .context("--parent-socket required for parent-auth local")?
                    .to_string_lossy()
                    .as_ref(),
            )),
            AuthKind::Krb5 => ReferralAuth::Krb5(ArcStr::from(
                self.parent_spn
                    .as_deref()
                    .context("--parent-spn required for parent-auth krb5")?,
            )),
            AuthKind::Tls => ReferralAuth::Tls(ArcStr::from(
                self.parent_tls_name
                    .as_deref()
                    .context("--parent-tls-name required for parent-auth tls")?,
            )),
        };
        let path = self
            .parent_path
            .as_deref()
            .unwrap_or(default_path);
        Ok(Some(ParentRef {
            path: ArcStr::from(path),
            ttl: self.parent_ttl,
            addrs: vec![(addr, auth)],
        }))
    }
}

#[derive(Args, Debug, Clone)]
struct CommonFlags {
    /// Print the plan and exit without writing anything.
    #[arg(long = "dry-run")]
    dry_run: bool,
    /// Overwrite existing config files. Without this, `install`
    /// errors if any target path is non-empty.
    #[arg(long = "force")]
    force: bool,
    /// Don't drop activation unit files.
    #[arg(long = "no-units")]
    no_units: bool,
    /// After the templated install succeeds, also register netidx as
    /// an OS service. Default (on a TTY) is to prompt; pass this to
    /// install non-interactively. Mutually exclusive with
    /// `--no-service`.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    with_service: bool,
    /// Skip the post-install service prompt. Default (on a TTY) is
    /// to prompt; pass this to suppress the prompt entirely.
    #[arg(long = "no-service")]
    no_service: bool,
}

/// Resolve `--units-dir` / `--no-units` into the template's
/// `units_dir: Option<PathBuf>` contract:
/// - `--no-units` ⇒ `None` (skip writing units).
/// - `--units-dir <p>` ⇒ `Some(p)`.
/// - neither ⇒ `Some(<user activation dir>)` (canonical default).
///
/// The template layer treats `None` as "skip" and never invents a
/// default itself, so this resolution has to live in the CLI.
fn resolve_units_dir(
    common: &CommonFlags,
    units_dir: Option<&Path>,
) -> Result<Option<PathBuf>> {
    if common.no_units {
        Ok(None)
    } else if let Some(d) = units_dir {
        Ok(Some(d.to_path_buf()))
    } else {
        Ok(Some(paths::user_activation_dir()?))
    }
}

/// Resolve `--netidx-binary` to an *absolute* path for the activation
/// unit's `ExecStart`. netidx-activation doesn't search `$PATH`, so a
/// bare or relative path silently breaks at supervisor start. None →
/// `std::env::current_exe()` so the installed unit runs the same
/// binary the operator invoked. An explicit relative path is
/// rejected: if the operator wants `which netidx`, they can pass the
/// resolved output.
fn resolve_netidx_binary(provided: Option<PathBuf>) -> Result<PathBuf> {
    match provided {
        Some(p) if p.is_absolute() => Ok(p),
        Some(p) => bail!(
            "--netidx-binary must be an absolute path (got {p:?}); \
             netidx-activation does not search $PATH"
        ),
        None => std::env::current_exe().context(
            "could not determine current netidx binary; \
             pass --netidx-binary <absolute-path>",
        ),
    }
}

fn check_no_overwrite(rt: &RenderedTemplate, force: bool) -> Result<()> {
    if force {
        return Ok(());
    }
    let mut existing: Vec<PathBuf> = Vec::new();
    if let Some((p, _)) = &rt.client_config
        && p.exists()
    {
        existing.push(p.clone());
    }
    if let Some((p, _)) = &rt.resolver_config
        && p.exists()
    {
        existing.push(p.clone());
    }
    if let Some((p, _)) = &rt.perms_file
        && p.exists()
    {
        existing.push(p.clone());
    }
    // Activation unit files are real generated artifacts written by
    // `apply()` via `ActivationDir::save` with no per-unit existence
    // guard, so they need the same --force protection as the configs
    // above. Without this, moving the main config paths off-default
    // (or running a template that produces *only* units) would let a
    // non-forced re-install silently clobber operator-edited
    // `resolver.unit` / `container.unit` / `id-map.unit` files.
    if let Some(dir) = &rt.units_dir {
        for name in rt.units.keys() {
            let p = netidx_conf::activation::unit_path_in(dir, name);
            if p.exists() {
                existing.push(p);
            }
        }
    }
    // TLS install copies cert/key/CA into <dest_dir>/, overwriting
    // unconditionally (see `install_identity`'s docstring). If an
    // operator already populated the directory by hand or by an earlier
    // run, a re-install without --force would silently replace their
    // material — including the private key.
    //
    // Exception: the local-CA "generate" path issues the cert and key
    // *straight into* the canonical identity dir, which is also the
    // install destination — so the copy job's source and destination
    // are the same file. Those are files this run just produced, not
    // pre-existing operator material, and "installing" them is the
    // identity copy: it can't clobber anything. Only a destination that
    // differs from its source can overwrite something we didn't create.
    for job in &rt.tls_install {
        let srcs = [&job.certificate_src, &job.private_key_src, &job.trusted_src];
        for (dst, src) in
            netidx_conf::tls::installed_files_in(&job.dest_dir).iter().zip(srcs)
        {
            if dst.exists() && dst != src {
                existing.push(dst.clone());
            }
        }
    }
    if existing.is_empty() {
        Ok(())
    } else {
        let lines: Vec<String> =
            existing.iter().map(|p| format!("  {}", p.display())).collect();
        bail!(
            "refusing to overwrite existing config(s) without --force:\n{}",
            lines.join("\n"),
        )
    }
}

// -- workstation --------------------------------------------------------------

#[derive(Args, Debug)]
pub(crate) struct WorkstationFlags {
    #[command(flatten)]
    parent: ParentFlags,
    #[command(flatten)]
    tls: TlsIdentityFlags,
    /// `default_auth` on the client config. Defaults to `local`.
    /// Override only when the workstation hosts publishers that
    /// network subscribers must reach.
    #[arg(long = "default-auth")]
    default_auth: Option<AuthKind>,
    /// Base path of the local resolver cluster (default `/local`).
    #[arg(long = "base", default_value = "/local")]
    base: String,
    /// Port the local resolver listens on. Default 4654 — chosen to
    /// not clash with 59200, the port the process-spawned automatic
    /// local resolver uses when no netidx config exists.
    #[arg(long = "listen-port")]
    listen_port: Option<u16>,
    /// Local-auth unix socket path.
    #[arg(long = "local-socket")]
    local_socket: Option<PathBuf>,
    #[arg(long = "client-config")]
    client_config_path: Option<PathBuf>,
    #[arg(long = "resolver-config")]
    resolver_config_path: Option<PathBuf>,
    #[arg(long = "units-dir")]
    units_dir: Option<PathBuf>,
    #[arg(long = "netidx-binary")]
    netidx_binary: Option<PathBuf>,
    /// Skip emitting the default `container` activation unit. By
    /// default a workstation gets both `resolver` and `container`
    /// units; pass this when you don't want a container service.
    #[arg(long = "no-container")]
    no_container: bool,
    /// Override the perms-file owner. By default the workstation
    /// install grants `<base>` → `<current-unix-user>` → `swlpd` so
    /// the operator has full rights to the local-resolver namespace
    /// without further setup. Pass `--owner alice` to grant `alice`
    /// instead — useful when installing as root on behalf of another
    /// user. Implies `--with-perms` (and conflicts with `--no-perms`).
    #[arg(long = "owner", conflicts_with = "no_perms")]
    owner: Option<String>,
    /// Skip the auto-seeded perms file entirely. The workstation
    /// resolver will load with an empty perms map and `Deny` every
    /// non-anonymous operation — only useful when perms are managed
    /// out-of-band.
    #[arg(long = "no-perms")]
    no_perms: bool,
    /// Where to write the perms file. Defaults to
    /// `~/.config/netidx/perms.json` (same as the resolver template).
    #[arg(long = "perms-path")]
    perms_path: Option<PathBuf>,
    #[command(flatten)]
    common: CommonFlags,
}

/// Resolve the workstation owner: explicit `--owner` first, else the
/// current Unix user via `nix::unistd`. On unix, bails when the current
/// uid has no passwd entry (distroless / scratch containers, uid-mapped
/// namespaces, some CI runners) — silently producing `None` here would
/// flow through to an empty `PMap`, which the local-auth resolver
/// treats as deny-everything and the install would report success while
/// being fundamentally broken. Operators in that situation must pass
/// `--owner <name>` (or `--no-perms` to skip perms generation
/// entirely).
///
/// Returns `Ok(None)` only on non-unix platforms (Windows) when no
/// explicit `--owner` was passed — the engine then emits an empty
/// perms map and the operator is expected to author one themselves.
#[cfg(unix)]
fn resolve_workstation_owner(provided: Option<String>) -> Result<Option<ArcStr>> {
    if let Some(s) = provided {
        return Ok(Some(ArcStr::from(s.as_str())));
    }
    let uid = nix::unistd::Uid::current();
    match nix::unistd::User::from_uid(uid) {
        Ok(Some(u)) => Ok(Some(ArcStr::from(u.name.as_str()))),
        Ok(None) => bail!(
            "could not resolve current uid ({uid}) to a passwd entry. \
             This usually means you're running in a container or namespace \
             without an /etc/passwd entry for your uid. Pass --owner <name> \
             to name the workstation owner explicitly, or --no-perms to skip \
             perms generation entirely."
        ),
        Err(e) => bail!(
            "getpwuid_r failed for current uid ({uid}): {e}. \
             Pass --owner <name> or --no-perms to proceed."
        ),
    }
}

#[cfg(not(unix))]
fn resolve_workstation_owner(provided: Option<String>) -> Result<Option<ArcStr>> {
    Ok(provided.map(|s| ArcStr::from(s.as_str())))
}

fn run_workstation(f: WorkstationFlags) -> Result<()> {
    let cli_tls_id = f.tls.to_spec()?;
    let mut tls_identities = vec![];
    if let Some(spec) = cli_tls_id {
        tls_identities.push(spec);
    }
    // Holds the staging tempdirs for any CA-server-joined identity until
    // `finish()` (apply) installs them; must outlive the whole flow.
    let mut tls_staging: Vec<tempfile::TempDir> = Vec::new();
    // Parent: CLI flags fully populate it when `--parent-addr` is
    // given; otherwise walk the operator through the cascade ("is
    // there a network-wide resolver? if so, what auth? if TLS, do
    // you have a cert or should we generate a CSR?"). The CLI-flag
    // path stays headless-friendly for scripting; the prompt path is
    // the discoverable default.
    //
    // `--parent-path` defaults to the workstation's own base (the
    // path at which this resolver attaches in the parent's namespace).
    // For the workstation that's `/local` by convention.
    let parent_default_path = f.base.as_str();
    // Route through `to_parent_ref` when any `--parent-*` flag is set
    // (not just `--parent-addr`) so a script that passes `--parent-spn`
    // without `--parent-addr` hits the misuse bail inside
    // `to_parent_ref` rather than silently dropping the flag values
    // into the prompt cascade — which, on a non-TTY, returns `None`
    // and produces a workstation with no parent referral at all. Same
    // contract `run_resolver` already has.
    let parent = if f.parent.any_set() {
        f.parent.to_parent_ref(parent_default_path)?
    } else {
        match prompt_parent_referral(parent_default_path)? {
            None => None,
            Some((parent_ref, maybe_ident)) => {
                if let Some(si) = maybe_ident {
                    tls_identities.push(si.spec);
                    tls_staging.extend(si.staging);
                }
                Some(parent_ref)
            }
        }
    };
    let owner = if f.no_perms {
        None
    } else {
        resolve_workstation_owner(f.owner.clone())?
    };
    // Struct-literal construction so adding a field to
    // WorkstationParams forces a compile error here rather than
    // silently leaving the new field defaulted (14th commandment).
    let params = netidx_conf::template::workstation::WorkstationParams {
        parent,
        tls_identities,
        default_auth: f.default_auth.map(|k| k.default_mech()),
        base: ArcStr::from(f.base.clone()),
        listen_port: f.listen_port,
        local_socket: f.local_socket,
        client_config_path: f.client_config_path,
        resolver_config_path: f.resolver_config_path,
        owner,
        perms_seed: None,
        with_perms_file: !f.no_perms,
        perms_path: f.perms_path,
        units_dir: resolve_units_dir(&f.common, f.units_dir.as_deref())?,
        netidx_binary: resolve_netidx_binary(f.netidx_binary)?,
        with_container: !f.no_container,
    };
    let rt = template::workstation(&params)?;
    // A workstation runs in the operator's session; a user-scope
    // systemd / launchd service is the right level — no sudo needed.
    finish(rt, &f.common, service::ServiceNeed::at(service::ScopeArg::User))
}

/// Interactive cascade for the workstation's optional parent
/// referral. Returns `Ok(None)` when the operator presses return at
/// the address prompt (the level-1 default "none"). Returns
/// `Ok(Some((ref, identity)))` otherwise; the optional identity is
/// the TLS identity to add to `tls_identities` when parent auth is
/// TLS and the operator brought their own cert.
///
/// `default_path` is the netidx path at which the current resolver
/// attaches in the parent's namespace — typically the resolver's
/// own base (`/local` for workstation). Operators rarely want to
/// override it, so the cascade doesn't prompt for it; the CLI's
/// `--parent-path` flag remains the override.
///
/// For the TLS "generate" path this function **bails** with a
/// next-steps message — the install can't complete without a signed
/// cert, so silently dropping the parent referral would be a worse
/// surprise than asking the operator to re-run once they've had the
/// CSR signed. The CLI flag path (`--parent-addr … --tls-cert …`)
/// is always available for the non-interactive case.
fn prompt_parent_referral(
    default_path: &str,
) -> Result<Option<(ParentRef, Option<StagedIdentity>)>> {
    // The upstream resolver IP is the one thing the operator has to
    // know (blank ⇒ no parent); the port is prompted separately with
    // the conventional 4564 default.
    let addr = match prompt::optional_parsed::<std::net::IpAddr>(
        "network-wide resolver IP (blank for none)",
        None,
    )? {
        Some(ip) => prompt_resolver_port(ip)?,
        None => return Ok(None),
    };
    let kind: AuthKind = prompt::choice_with_default(
        "parent auth scheme",
        None,
        &["anonymous", "local", "krb5", "tls"],
        "tls",
    )?;
    let (auth, identity) = match kind {
        AuthKind::Anonymous => (ReferralAuth::Anonymous, None),
        AuthKind::Local => {
            let socket = prompt::required_path("parent local-auth socket path", None)?;
            (
                ReferralAuth::Local(ArcStr::from(socket.to_string_lossy().as_ref())),
                None,
            )
        }
        AuthKind::Krb5 => {
            let spn = prompt::required_string(
                "parent Kerberos SPN (e.g. host/resolver.example.com@REALM)",
                None,
            )?;
            (ReferralAuth::Krb5(ArcStr::from(spn.as_str())), None)
        }
        AuthKind::Tls => {
            let server_name = prompt_resolver_tls_name(
                Some(addr),
                "parent TLS server name",
                None,
            )?;
            // identity is required for TLS — either bring one or
            // (the generate path diverges via `bail!`). Suggest our own
            // SAN as `<user>.<domain>`, the domain taken from the
            // resolver's SAN we just resolved.
            let suggested = suggest_client_san(&server_name);
            let staged =
                prompt_tls_client_identity(Some(addr.ip()), suggested.as_deref())?;
            (ReferralAuth::Tls(ArcStr::from(server_name.as_str())), Some(staged))
        }
    };
    Ok(Some((
        ParentRef {
            path: ArcStr::from(default_path),
            ttl: None,
            addrs: vec![(addr, auth)],
        },
        identity,
    )))
}

/// Validity (days) requested from a CA server. The server caps it to
/// the admin's policy, so this is just an upper bound.
const JOIN_VALIDITY_DAYS: u32 = 730;

/// A TLS identity obtained from a CA server, with its files **staged**
/// in a tempdir. `apply()` performs the (force-gated) install into the
/// canonical `~/.config/netidx/tls/<name>/` layout.
struct JoinedIdentity {
    name: String,
    certificate: PathBuf,
    private_key: PathBuf,
    trusted: PathBuf,
}

/// A client TLS identity plus the tempdir its files are staged in until
/// `apply()` installs them. `staging` is `None` for the BYO-cert /
/// wait-for-CSR paths, whose files already sit at their canonical home.
struct StagedIdentity {
    spec: TlsIdentitySpec,
    staging: Option<tempfile::TempDir>,
}

/// Offer to obtain a TLS identity from a CA server over the network
/// instead of the local-CA / CSR flow. `default_ip` (the upstream
/// resolver, usually co-located with the CA) seeds the address prompt.
/// Returns the installed identity, or `None` if the operator has no CA
/// server — the caller then runs the existing flow. Cross-platform:
/// this is also how a node with no openssl (Windows) gets a TLS cert.
fn maybe_join_ca_server(
    default_ip: Option<IpAddr>,
    kind: ca_proto::NodeKind,
    suggested_name: Option<&str>,
) -> Result<Option<(JoinedIdentity, tempfile::TempDir)>> {
    if !prompt::confirm(
        "get this TLS cert from a CA server over the network?",
        default_ip.is_some(),
    )? {
        return Ok(None);
    }
    let ip = match default_ip {
        Some(i) => {
            prompt::parsed_with_default::<IpAddr>("CA server IP", None, &i.to_string())?
        }
        None => prompt::required_parsed::<IpAddr>("CA server IP", None)?,
    };
    let port = prompt::parsed_with_default::<u16>(
        "CA server port",
        None,
        &ca_proto::DEFAULT_PORT.to_string(),
    )?;
    let addr = SocketAddr::new(ip, port);
    let name_label = "TLS identity name to request (the cert's DNS SAN)";
    let name = match suggested_name {
        Some(s) => prompt::string_with_default(name_label, None, s)?,
        None => prompt::required_string(name_label, None)?,
    };
    let admin = prompt::required_string("CA admin name", None)?;
    let password = Zeroizing::new(rpassword::prompt_password(format!(
        "CA password for admin {admin}: "
    ))?);

    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let issued = rt.block_on(ca_join::request_cert(
        addr,
        kind,
        &name,
        &admin,
        password,
        JOIN_VALIDITY_DAYS,
        |fp| {
            println!("The CA presented this identity:");
            println!("  SHA256  {}", fp.text());
            println!("{}", fp.identicon(ColorMode::detect()));
            prompt::confirm("does this match what your CA admin gave you?", false)
        },
    ))?;

    // Stage the issued files in a tempdir; the template's `apply()` does
    // the real install into the canonical identity dir, gated by
    // `check_no_overwrite` (`--force`). Writing straight to the
    // canonical dir here would bypass that guard — silently clobbering
    // an existing private key — and would leave the identity installed
    // even if a later install step failed. The returned tempdir must
    // outlive `finish()`; the caller keeps it alive.
    let staging = tempfile::TempDir::new().context("creating tls staging dir")?;
    let certificate = staging.path().join("certificate.pem");
    let private_key = staging.path().join("private.key");
    let trusted = staging.path().join("trusted.pem");
    netidx_conf::atomic::write_atomic(&certificate, issued.cert_pem.as_bytes(), 0o644)?;
    netidx_conf::atomic::write_atomic(
        &private_key,
        issued.private_key_pem.as_bytes(),
        0o600,
    )?;
    netidx_conf::atomic::write_atomic(&trusted, issued.trusted_pem.as_bytes(), 0o644)?;
    println!("got TLS identity {name:?} from CA server {addr}");
    Ok(Some((JoinedIdentity { name, certificate, private_key, trusted }, staging)))
}

/// Convert an installed [`JoinedIdentity`] into a client-side
/// [`TlsIdentitySpec`]. The files are already at their canonical home,
/// so `dest_dir` is `None` and the engine's install step is the
/// harmless self-copy the `check_no_overwrite` guard already allows.
fn joined_to_spec(j: JoinedIdentity) -> TlsIdentitySpec {
    let server_pattern = netidx_conf::tls::domain_from_san(&j.name)
        .map(|d| d.to_string())
        .unwrap_or_else(|_| j.name.clone());
    TlsIdentitySpec {
        server_pattern: ArcStr::from(server_pattern.as_str()),
        our_name: ArcStr::from(j.name.as_str()),
        certificate: j.certificate,
        private_key: j.private_key,
        trusted: j.trusted,
        dest_dir: None,
        askpass: None,
    }
}

/// Convert an installed [`JoinedIdentity`] into the resolver's own
/// [`AuthChoice::Tls`].
fn joined_to_auth(j: JoinedIdentity) -> AuthChoice {
    AuthChoice::Tls {
        name: ArcStr::from(j.name.as_str()),
        certificate: j.certificate,
        private_key: j.private_key,
        trusted: j.trusted,
        askpass: None,
    }
}

/// Interactive cascade for a client-side TLS identity: a cert/key
/// pair the operator will use to authenticate to *some* upstream
/// server. Used by both the workstation parent-referral flow and
/// the publisher template — they both need exactly the same
/// "bring a cert or generate a CSR" prompt and produce a
/// `TlsIdentitySpec` that goes into `client.tls.identities`.
///
/// For the "generate" path: writes a key + CSR locally, prints
/// instructions, then waits for the operator to confirm they've
/// placed the signed cert and trusted-CA bundle at the canonical
/// install location — only then does the install proceed, so by
/// the time the daemon starts the cert files are guaranteed to be
/// on disk. Local-CA-issue is deliberately *not* offered: in this
/// flow the upstream is a different trust domain (the parent
/// resolver, or whoever the publisher talks to), and the local
/// CA's certs wouldn't be trusted there. Operators wanting to use
/// their local CA can run `netidx conf ca issue` and then point
/// `--tls-cert / --tls-key / --tls-trusted` at the result.
fn prompt_tls_client_identity(
    upstream_ip: Option<IpAddr>,
    suggested_name: Option<&str>,
) -> Result<StagedIdentity> {
    // First offer the network path: a CA server signs our CSR on the
    // spot, no files to shuttle. Works on every platform (rcgen, not
    // openssl), so it's also how a Windows node gets a TLS identity.
    if let Some((j, staging)) =
        maybe_join_ca_server(upstream_ip, ca_proto::NodeKind::Client, suggested_name)?
    {
        return Ok(StagedIdentity { spec: joined_to_spec(j), staging: Some(staging) });
    }
    // On unix the operator can choose 'generate' and we'll make a
    // key + CSR for them via the openssl-backed `ca` module. On
    // non-unix that module isn't available, so the prompt only
    // accepts an explicit cert path.
    #[cfg(unix)]
    let cert_default = "generate";
    #[cfg(not(unix))]
    let cert_default = "";
    #[cfg(unix)]
    let cert_label = "your TLS cert (path, or 'generate' to make a new key + CSR)";
    #[cfg(not(unix))]
    let cert_label =
        "your TLS cert path (CSR generation is unix-only; bring a pre-issued cert)";
    let cert_choice = prompt::string_with_default(cert_label, None, cert_default)?;
    #[cfg(unix)]
    let is_generate = cert_choice == "generate";
    #[cfg(not(unix))]
    let is_generate = false;
    let (our_name, certificate, private_key, trusted, askpass) = if is_generate {
        #[cfg(unix)]
        {
            generate_and_wait_for_parent_cert(suggested_name)?
        }
        #[cfg(not(unix))]
        {
            unreachable!("generate path is unix-only")
        }
    } else {
        if cert_choice.is_empty() {
            bail!(
                "TLS cert path required (CSR generation is unix-only — \
                 provide a pre-issued cert on this platform)"
            );
        }
        let certificate = PathBuf::from(cert_choice);
        let private_key = prompt::required_path("your TLS private key path", None)?;
        let trusted = prompt::required_path(
            "trusted CA bundle (signs the parent's cert)",
            None,
        )?;
        // Extract the identity name from the cert's DNS SAN rather
        // than asking the operator. netidx's runtime keys identities
        // by the cert's `alt_name` at TLS-load time, so the on-disk
        // install dir (and any matching elsewhere) must use that
        // same name — asking would let the operator type something
        // that disagrees with the cert, producing an install that
        // looks fine on disk but doesn't match at runtime.
        let our_name = netidx_conf::tls::extract_dns_san_from_pem(&certificate)
            .with_context(|| {
                format!(
                    "deriving TLS identity name from {} — supply a cert \
                     with a DNS SubjectAlternativeName entry",
                    certificate.display()
                )
            })?;
        // BYO-cert path: the key already exists, we don't touch its
        // encryption. The operator is responsible for placing the
        // password into the system keychain (or attaching an
        // askpass) if they brought an encrypted key. We don't
        // prompt for an askpass here because we don't know whether
        // the key is encrypted at all — guessing wrong would either
        // bury an extraneous `askpass` line in the config or skip a
        // needed one. Operators with encrypted external keys can
        // edit `tls.askpass` after install.
        (our_name, certificate, private_key, trusted, None)
    };
    // Key the entry in `tls.identities` by the *domain* part of our
    // SAN, not the full SAN. netidx's convention is
    // `<user>.<domain>` (e.g. `mazikeen.local`) — one identity entry
    // covers the whole domain (`local`) and the runtime matches any
    // host under it via the reverse-domain prefix match in
    // `tls::get_match`. Keying by the full SAN would still match,
    // but it forces a separate identity per host where one per
    // domain is what operators actually want. `our_name` (the
    // install dir name) stays as the full SAN so multiple hosts'
    // certs don't clobber each other on disk.
    let server_pattern = netidx_conf::tls::domain_from_san(our_name.as_str())
        .with_context(|| {
            format!(
                "deriving identity domain from cert SAN {:?}",
                our_name
            )
        })?;
    // These paths are already at (or, for BYO, point directly at) their
    // final home, so no staging tempdir is needed.
    Ok(StagedIdentity {
        spec: TlsIdentitySpec {
            server_pattern: ArcStr::from(server_pattern),
            our_name: ArcStr::from(our_name.as_str()),
            certificate,
            private_key,
            trusted,
            // dest_dir = None resolves to the canonical
            // `~/.config/netidx/tls/<our-name>/`. For the generate path
            // the cert and trusted are already at exactly those paths,
            // so the engine's "install" step ends up reading each file
            // and writing it back — a no-op-ish round-trip. For the
            // explicit-path case it's a real copy as before.
            dest_dir: None,
            askpass,
        },
        staging: None,
    })
}

/// Generate a key + CSR, then loop until the operator confirms
/// they've placed the signed cert and trusted-CA bundle at the
/// canonical install location. Returns
/// `(our_name, certificate_path, private_key_path, trusted_path)` —
/// key/cert/trusted all under `~/.config/netidx/tls/<our-name>/`.
///
/// The CSR itself lands in the *current working directory* as
/// `./<our-name>.csr` (matching `netidx conf ca request`), not in
/// the identity dir — it's something the operator hands off to the
/// CA admin, so it needs to be where they'll naturally look for it
/// (attach to an email, scp, etc.), not buried under XDG config.
///
/// The install proceeds only after the cert files validate, so
/// downstream config save + activation start are guaranteed to find
/// readable certs. Non-TTY callers bail immediately if the files
/// aren't already there — scripted installs should use `--tls-cert`
/// directly.
#[cfg(unix)]
fn generate_and_wait_for_parent_cert(
    suggested_name: Option<&str>,
) -> Result<(String, PathBuf, PathBuf, PathBuf, Option<PathBuf>)> {
    let name_label = "your TLS identity name (CN for the CSR; cert SAN)";
    let our_name = match suggested_name {
        Some(s) => prompt::string_with_default(name_label, None, s)?,
        None => prompt::required_string(name_label, None)?,
    };
    let (cert_path, key_path, trusted_path, askpass) =
        generate_csr_and_wait_for_cert(&our_name)?;
    Ok((our_name, cert_path, key_path, trusted_path, askpass))
}

/// Walk the canonical places askpass programs live and return the
/// first one we find. Used as the level-1 default for the
/// "askpass program" prompt that fires when the operator chooses
/// to encrypt a private key. Order is:
///
/// 1. `$SSH_ASKPASS` if set and pointing at a real file.
/// 2. PATH lookups for the common command names.
/// 3. Known absolute paths from the major distros.
///
/// Returns `None` if nothing matched — the prompt then falls back
/// to "no default" so the operator must type a path explicitly (or
/// hit Enter to leave askpass unset).
#[cfg(unix)]
fn find_askpass() -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let executable = |p: &Path| {
        std::fs::metadata(p)
            .map(|m| m.is_file() && (m.permissions().mode() & 0o111) != 0)
            .unwrap_or(false)
    };
    // 1. SSH_ASKPASS — the OS-standard way to point at one. If the
    //    operator set this, respect it before searching.
    if let Some(p) = std::env::var_os("SSH_ASKPASS") {
        let p = PathBuf::from(p);
        if executable(&p) {
            return Some(p);
        }
    }
    // 2. PATH lookup. ssh-askpass is the de-facto name; the Gnome
    //    and KDE wrappers ship variants under their own names.
    const PATH_NAMES: &[&str] =
        &["ssh-askpass", "ksshaskpass", "ssh-askpass-gnome", "ssh-askpass-fullscreen"];
    if let Some(path_env) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_env) {
            for name in PATH_NAMES {
                let p = dir.join(name);
                if executable(&p) {
                    return Some(p);
                }
            }
        }
    }
    // 3. Common absolute install paths. Most distros tuck their
    //    askpass into `libexec` rather than on PATH (so they don't
    //    accidentally shadow other tools), which the PATH search
    //    above misses by design.
    const ABS_PATHS: &[&str] = &[
        "/usr/libexec/openssh/x11-ssh-askpass",
        "/usr/libexec/openssh/gnome-ssh-askpass",
        "/usr/libexec/openssh/gnome-ssh-askpass3",
        "/usr/libexec/openssh/ssh-askpass",
        "/usr/libexec/ssh-askpass",
        "/usr/lib/openssh/ssh-askpass",
        "/usr/lib/openssh/gnome-ssh-askpass3",
        "/usr/lib/ssh/ssh-askpass",
    ];
    for p in ABS_PATHS {
        let p = PathBuf::from(p);
        if executable(&p) {
            return Some(p);
        }
    }
    None
}

/// Prompt for a private-key password, and if one is given, for an
/// askpass program with [`find_askpass`]'s discovery result as the
/// level-1 default. Returns `(Some(password), Some(askpass))` if
/// the operator chose encryption, `(None, None)` if they didn't
/// (blank password, no TTY, etc.).
///
/// `key_path` is the canonical on-disk location the encrypted key
/// will eventually live at. When a password is collected, this
/// function *also* writes it into the system keychain under
/// `("netidx", key_path)` — that's where `netidx::tls::load_private_key`
/// looks first at startup. Pre-populating the keychain lets the
/// resolver server (which has no `askpass` field in its
/// per-member-server `Auth::Tls` schema) still decrypt its own
/// key without operator intervention. The `askpass` we return is
/// for the client-side `cfile::Tls.askpass` fallback, in case the
/// keychain entry is missing or the keyring isn't unlocked.
#[cfg(unix)]
fn collect_key_password_and_askpass(
    key_path: &Path,
    name: &str,
) -> Result<(Option<String>, Option<PathBuf>)> {
    // Skip everything on non-interactive runs. Scripted installs
    // shouldn't hang at an rpassword prompt, and they have no
    // realistic way to feed in a password anyway.
    if !std::io::stdin().is_terminal() {
        return Ok((None, None));
    }
    // Name the identity so an operator setting up several certs in one
    // session knows which key this password is for.
    let pw = rpassword::prompt_password(format!(
        "private key password for {name} (blank for no encryption): "
    ))?;
    if pw.is_empty() {
        return Ok((None, None));
    }
    let again =
        rpassword::prompt_password(format!("private key password for {name} (again): "))?;
    if again != pw {
        bail!("passwords did not match");
    }
    // Search for an askpass program and prompt the operator to
    // confirm or override it. A blank answer takes the default; an
    // operator who explicitly wants no askpass can type an empty
    // string when the default is itself empty.
    let discovered = find_askpass();
    let default = discovered
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    let answer = prompt::string_with_default(
        "askpass program (used to ask for the key password at startup)",
        None,
        &default,
    )?;
    let askpass = if answer.is_empty() { None } else { Some(PathBuf::from(answer)) };
    // Save into the system keychain so the resolver server can
    // decrypt its key without an askpass at startup. Failure here
    // isn't fatal — the keychain might be locked, sandboxed, or
    // missing entirely, and the askpass fallback still lets the
    // client config work. We log and continue.
    if let Err(e) = netidx::tls::save_password_for_key(
        &key_path.to_string_lossy(),
        &pw,
    ) {
        eprintln!(
            "warning: failed to save key password to the system keychain ({e:#}); \
             the resolver may need an askpass at startup. \
             Re-run with the keychain unlocked, or pre-populate the entry manually.",
        );
    }
    Ok((Some(pw), askpass))
}

/// Generate a private key + CSR for `name`, write the key to the
/// canonical identity dir and the CSR to CWD, print operator
/// next-steps, then block until both the signed cert and the
/// trusted-CA bundle appear at their expected paths. Returns
/// `(cert_path, key_path, trusted_path, askpass_path)`.
///
/// Shared by the two "no local CA available, BYO the cert" flows:
/// the parent referral path (operator's resolver attaches to an
/// upstream that signs the parent's cert), and the resolver TLS
/// path when the operator declines to create a local CA. Both end
/// up in the same place: a generated key + CSR locally, a
/// "drop the signed cert here when ready" wait, and an
/// `AuthChoice::Tls` (or `TlsIdentitySpec`) pointing at the
/// canonical install paths.
///
/// If the operator picks a password at the prompt, the key is
/// written as encrypted PKCS#8 and the askpass program they chose
/// (defaulting to whatever [`find_askpass`] discovers) is returned
/// so the caller can plumb it into the emitted client config.
///
/// The CSR itself lands in the *current working directory* as
/// `./<name>.csr` (matching `netidx conf ca request`), not in the
/// identity dir — the operator hands it off to a CA admin, so it
/// needs to be where they'll naturally look for it.
#[cfg(unix)]
fn generate_csr_and_wait_for_cert(
    name: &str,
) -> Result<(PathBuf, PathBuf, PathBuf, Option<PathBuf>)> {
    let dest_dir = tls::identity_dir(name)?;
    std::fs::create_dir_all(&dest_dir).with_context(|| {
        format!("creating identity dir {}", dest_dir.display())
    })?;
    let key_path = dest_dir.join("private.key");
    let cert_path = dest_dir.join("certificate.pem");
    let trusted_path = dest_dir.join("trusted.pem");
    let csr_path = super::ca::default_csr_filename(name);

    if key_path.exists() {
        bail!(
            "private key already exists at {} — refusing to overwrite. \
             Move it aside, or re-run with explicit --tls-cert / \
             --tls-key / --tls-trusted to point at an existing identity.",
            key_path.display(),
        );
    }
    let (password, askpass) = collect_key_password_and_askpass(&key_path, name)?;
    let kr = netidx_conf::ca::generate_csr(
        &netidx_conf::ca::Subject::cn(name.to_string()),
        &[netidx_conf::ca::SanEntry::Dns(name.to_string())],
        2048,
        password.as_deref(),
    )
    .context("generating private key + CSR")?;
    netidx_conf::atomic::write_atomic(&key_path, &kr.private_key_pem, 0o600)
        .with_context(|| format!("writing private key to {}", key_path.display()))?;
    netidx_conf::atomic::write_atomic(&csr_path, &kr.csr_pem, 0o644)
        .with_context(|| format!("writing CSR to {}", csr_path.display()))?;

    println!();
    println!("Generated TLS identity '{}':", name);
    println!("  private key (0600): {}", key_path.display());
    println!("  CSR         (0644): {}", csr_path.display());
    if password.is_some() {
        println!("  private key is encrypted; password saved to the system keychain.");
    }
    println!();
    println!("Next steps:");
    println!("  1. Send {} to your CA admin to sign.", csr_path.display());
    println!("  2. Place the signed certificate at:");
    println!("       {}", cert_path.display());
    println!("  3. Place the trusted-CA bundle (the cert that signs the");
    println!("     signing CA's cert chain) at:");
    println!("       {}", trusted_path.display());
    println!();

    wait_for_cert_files(&cert_path, &trusted_path)?;
    Ok((cert_path, key_path, trusted_path, askpass))
}

/// Loop until `cert_path` and `trusted_path` both exist and parse as
/// X.509 certificates. On a TTY: prompt the operator to confirm and
/// re-check; on each "not ready" verdict, print *why* and loop. On a
/// non-TTY caller: check once and bail if anything is missing —
/// scripted installs shouldn't hang waiting for human input.
#[cfg(unix)]
fn wait_for_cert_files(cert_path: &Path, trusted_path: &Path) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        return check_cert_files_present(cert_path, trusted_path).with_context(|| {
            "non-interactive install: required cert files missing. \
             Place them at the paths above, or re-run with explicit \
             --tls-cert / --tls-trusted"
                .to_string()
        });
    }
    use std::io::Write;
    loop {
        eprint!("Press Enter once both files are in place (or Ctrl-C to abort): ");
        std::io::stderr().flush().ok();
        let mut buf = String::new();
        if std::io::stdin().read_line(&mut buf)? == 0 {
            bail!("EOF at confirmation prompt — install aborted");
        }
        match check_cert_files_present(cert_path, trusted_path) {
            Ok(()) => return Ok(()),
            Err(e) => {
                eprintln!("not ready yet: {e:#}");
                continue;
            }
        }
    }
}

/// Verify both files exist and parse as PEM-encoded X.509. We
/// deliberately don't check the cert is signed by the trusted bundle
/// (that's the runtime TLS handshake's job) and don't check the cert
/// matches our private key (the netidx config validator will catch
/// that at save time and give a more precise error). The point is to
/// catch the obvious "you forgot to drop the file" case before the
/// engine's config-validation step.
#[cfg(unix)]
fn check_cert_files_present(cert: &Path, trusted: &Path) -> Result<()> {
    // Lives in `tls` (cross-platform, rustls-pemfile backed), not
    // `ca` (unix-only, openssl) — even though this caller itself is
    // currently cfg(unix). Splitting the validator out gives a
    // Windows install path a way to validate operator-provided
    // certs without us having to add an openssl Windows toolchain.
    netidx_conf::tls::validate_pem_cert_file(cert)?;
    netidx_conf::tls::validate_pem_cert_file(trusted)?;
    Ok(())
}

// -- standalone-resolver ------------------------------------------------------

/// Conventional netidx resolver port — what the `--listen` prompt
/// suggests appended to the discovered address.
const DEFAULT_RESOLVER_PORT: u16 = 4564;

/// Prompt for a resolver-server port given an IP the operator already
/// supplied, defaulting to the conventional 4564, and combine the two.
/// Factors the "…and the port, which you can usually just accept" half
/// shared by every resolver-address prompt across the templates — the
/// IP is the one thing the operator has to know, the port is a
/// keystroke.
fn prompt_resolver_port(ip: std::net::IpAddr) -> Result<SocketAddr> {
    let port = prompt::parsed_with_default::<u16>(
        "resolver port",
        None,
        &DEFAULT_RESOLVER_PORT.to_string(),
    )?;
    Ok(SocketAddr::new(ip, port))
}

/// The conventional leftmost label of a resolver's TLS SAN, and the
/// default TLS domain. netidx keys a TLS identity by its domain (the SAN
/// is `<name>.<domain>`, e.g. `resolver.ryu-oh.org`), so `local` covers
/// the single-host / no-DNS case the way `BindCfg::Local` does for binds.
const DEFAULT_RESOLVER_NAME: &str = "resolver";
const DEFAULT_TLS_DOMAIN: &str = "local";

/// Best-effort default for the TLS name a resolver presents. Probes the
/// resolver itself — the authoritative source, since it's the exact SAN a
/// client must match — and falls back to the `resolver.local` convention
/// when the probe can't reach or read it. Either way the value is only a
/// default the operator confirms; correctness is enforced later by the
/// trusted-CA bundle, so a wrong guess fails closed at connect time.
fn resolver_tls_name_default(addr: SocketAddr) -> String {
    let convention = format!("{DEFAULT_RESOLVER_NAME}.{DEFAULT_TLS_DOMAIN}");
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return convention,
    };
    match rt.block_on(netidx_conf::resolver_probe::probe_resolver_tls_name(addr)) {
        Ok(Some(name)) => {
            println!("probed resolver {addr}: it serves TLS name {name:?}");
            name
        }
        Ok(None) => {
            println!(
                "note: resolver {addr} served a cert with no DNS SAN; \
                 defaulting to the {convention:?} convention"
            );
            convention
        }
        Err(e) => {
            println!(
                "note: could not probe resolver {addr} for its TLS name \
                 ({e}); defaulting to the {convention:?} convention"
            );
            convention
        }
    }
}

/// Prompt for the TLS name a resolver serves (the client's
/// `Auth::Tls { name }`). A CLI-provided value short-circuits; otherwise,
/// when we know the resolver's address, prefill the default by probing it
/// (convention fallback). With no address to probe, prompt with no
/// default.
fn prompt_resolver_tls_name(
    addr: Option<SocketAddr>,
    label: &str,
    provided: Option<String>,
) -> Result<String> {
    if let Some(p) = provided {
        return Ok(p);
    }
    match addr {
        Some(addr) => {
            let default = resolver_tls_name_default(addr);
            prompt::string_with_default(label, None, &default)
        }
        None => prompt::required_string(label, None),
    }
}

/// Prompt for a resolver's *own* TLS SAN in two parts — a domain (default
/// `local`, e.g. `ryu-oh.org`) and the leftmost name (default `resolver`)
/// — and join them into `<name>.<domain>`. A `--tls-name` value
/// short-circuits both prompts with the full SAN.
fn prompt_resolver_own_tls_name(provided: Option<String>) -> Result<String> {
    if let Some(full) = provided {
        return Ok(full);
    }
    let domain = prompt::string_with_default(
        "TLS domain (e.g. ryu-oh.org)",
        None,
        DEFAULT_TLS_DOMAIN,
    )?;
    let name = prompt::string_with_default(
        "resolver name (the leftmost label of its cert SAN)",
        None,
        DEFAULT_RESOLVER_NAME,
    )?;
    let (name, domain) = (name.trim(), domain.trim());
    if name.is_empty() {
        bail!("resolver name must not be empty");
    }
    if domain.is_empty() {
        bail!("TLS domain must not be empty");
    }
    Ok(format!("{name}.{domain}"))
}

/// Best-effort current username, for prompt defaults only — a
/// *suggestion*, so it never fails (returns `None` if it can't tell).
/// Prefers the passwd entry on unix so it agrees with the workstation
/// owner [`resolve_workstation_owner`] derives the same way.
fn current_username() -> Option<String> {
    #[cfg(unix)]
    {
        let uid = nix::unistd::Uid::current();
        if let Ok(Some(u)) = nix::unistd::User::from_uid(uid) {
            return Some(u.name);
        }
    }
    for var in ["USER", "LOGNAME", "USERNAME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

/// Suggest a client TLS SAN of the form `<user>.<domain>`, taking the
/// domain from the resolver's own SAN — netidx's `<user>.<domain>`
/// identity convention (resolver `resolver.ryu-oh.org` + user `eric` →
/// `eric.ryu-oh.org`). `None` if the user can't be determined or the
/// resolver SAN carries no domain (e.g. a single-label name).
fn suggest_client_san(resolver_san: &str) -> Option<String> {
    let user = current_username()?;
    let domain = netidx_conf::tls::domain_from_san(resolver_san).ok()?;
    Some(format!("{user}.{domain}"))
}

#[derive(Args, Debug)]
pub(crate) struct ResolverFlags {
    /// Auth scheme this resolver exposes (anonymous, local, krb5,
    /// tls). Prompted when omitted.
    #[arg(long = "auth")]
    auth: Option<AuthKind>,
    /// Kerberos SPN (with `--auth krb5`).
    #[arg(long = "spn")]
    spn: Option<String>,
    /// Local-auth socket path (with `--auth local`).
    #[arg(long = "socket")]
    socket: Option<PathBuf>,
    /// Resolver's own TLS name — the full cert SAN, e.g.
    /// `resolver.ryu-oh.org` (with `--auth tls`). Prompted in two parts
    /// (domain, then name) when omitted, defaulting to `resolver.local`.
    #[arg(long = "tls-name")]
    tls_name: Option<String>,
    /// Source path of the resolver's certificate, or the literal
    /// `generate` to issue one from the local CA — creating that CA
    /// first if none exists. The interactive prompt defaults to
    /// `generate`, which is the painless path for the common
    /// "resolver host is also the CA host" case.
    #[arg(long = "tls-cert")]
    tls_cert: Option<PathBuf>,
    /// Source path of the resolver's private key (with `--auth tls`).
    /// Not needed when `--tls-cert` is `generate`.
    #[arg(long = "tls-key")]
    tls_key: Option<PathBuf>,
    /// Source path of the trusted-CA bundle (with `--auth tls`). Not
    /// needed when `--tls-cert` is `generate` — the generating CA's
    /// own certificate becomes the trust anchor.
    #[arg(long = "tls-trusted")]
    tls_trusted: Option<PathBuf>,
    /// The resolver's advertised address — what clients connect to.
    /// Must be a concrete address (not `0.0.0.0`). The interactive
    /// prompt suggests an IPv4 address discovered from the machine's
    /// network interfaces: the first public IPv4, else the first
    /// private-network IPv4, else loopback. IPv6 is accepted if you
    /// type it explicitly, but not suggested as the default. Use
    /// `--bind` if the socket should bind somewhere other than the
    /// advertised address.
    #[arg(long = "listen")]
    listen: Option<SocketAddr>,
    /// Override the bind address (default: same as `listen`'s ip).
    /// This is where the socket actually binds — it *may* be
    /// `0.0.0.0` to listen on every interface even when `--listen`
    /// advertises one concrete address.
    #[arg(long = "bind")]
    bind: Option<std::net::IpAddr>,
    /// Base path (default `/`).
    #[arg(long = "base", default_value = "/")]
    base: String,
    /// Path to a seed perms.json. Its contents are written to
    /// `--perms-path` (or `~/.config/netidx/perms.json`). When
    /// omitted, the template auto-seeds a per-user-playground layout
    /// (full rights for `$[user]` under `/users/$[user]`, read+write
    /// for the `users` group under `/users`). Pass `--no-perms` to
    /// skip emitting a perms file entirely.
    #[arg(long = "perms-seed")]
    perms_seed: Option<PathBuf>,
    /// Where to write the perms file. Defaults to
    /// `~/.config/netidx/perms.json`.
    #[arg(long = "perms-path")]
    perms_path: Option<PathBuf>,
    /// Skip emitting a perms file (and the corresponding
    /// `include_permissions` reference). Use when perms are managed
    /// out-of-band by some other tool / process. Mutually exclusive
    /// with `--perms-seed`.
    #[arg(long = "no-perms", conflicts_with = "perms_seed")]
    no_perms: bool,
    #[command(flatten)]
    parent: ParentFlags,
    #[arg(long = "resolver-config")]
    resolver_config_path: Option<PathBuf>,
    #[arg(long = "units-dir")]
    units_dir: Option<PathBuf>,
    #[arg(long = "netidx-binary")]
    netidx_binary: Option<PathBuf>,
    /// Skip auto-installing the id-mapper daemon. When set, the
    /// resolver uses its `IdMapType::Command` default (`/bin/id`) and
    /// no `id-map.unit` is written. When unset:
    /// - `--auth tls` prompts with default Y (cert SANs have no
    ///   meaningful `/bin/id` translation path).
    /// - `--auth krb5` prompts with default N — most kerberos sites
    ///   have a system-level IdM (FreeIPA, AD, OpenIDM) handling
    ///   principal → uid via SSSD / nsswitch already.
    /// - Anonymous / Local auth never use the daemon.
    ///
    /// For `--auth krb5`, id-map entries are keyed by the full
    /// kerberos principal including realm (e.g. `eric@RYU-OH.ORG`).
    #[arg(long = "no-id-map")]
    no_id_map: bool,
    /// Override the id-map socket path (default
    /// `${dirs::config_dir}/netidx/id-map.sock`).
    #[arg(long = "id-map-socket")]
    id_map_socket: Option<PathBuf>,
    /// Override the id-map JSON path (default
    /// `${dirs::config_dir}/netidx/id-map.json`).
    #[arg(long = "id-map-path")]
    id_map_path: Option<PathBuf>,
    /// Skip writing a client.json pointing at this resolver. By
    /// default `install resolver` drops a local client config — for
    /// TLS auth it reuses the resolver's own cert — so commands
    /// like `netidx resolver list` work from the resolver host
    /// without extra setup. Pass this if a different client config
    /// already exists, or if the resolver host should not also be a
    /// client.
    #[arg(long = "no-client")]
    no_client: bool,
    /// Override the client config path (default
    /// `${dirs::config_dir}/netidx/client.json`). Ignored when
    /// `--no-client` is set.
    #[arg(long = "client-config")]
    client_config_path: Option<PathBuf>,
    #[command(flatten)]
    common: CommonFlags,
}

fn run_resolver(mut f: ResolverFlags) -> Result<()> {
    // Resolve required args with interactive prompting before we
    // start building the params struct. `--auth` and `--listen` are
    // level-1 prompts: they have sensible defaults (tls, the
    // conventional resolver port), so a blank answer is fine.
    let kind: AuthKind = prompt::choice_with_default(
        "auth scheme",
        f.auth,
        &["anonymous", "local", "krb5", "tls"],
        "tls",
    )?;
    f.auth = Some(kind);
    // Shape detection (incl. cloud-metadata probe) only fires when
    // we actually need a default — i.e. when `--listen` or `--bind`
    // weren't given explicitly. A `OnceCell` so the probe runs at
    // most once even if both prompts need it. On a cloud VM with a
    // private NIC + NAT'd public IP, both prompts get cloud-shaped
    // defaults; on a non-cloud host with both flags given we never
    // pay the metadata round-trip at all.
    let shape: std::cell::OnceCell<NetShape> = std::cell::OnceCell::new();
    let listen: SocketAddr = if let Some(l) = f.listen {
        l
    } else {
        let s = shape.get_or_init(NetShape::detect);
        if s.needs_operator_hint() {
            eprintln!(
                "note: detected container environment with no NETIDX_PUBLIC_IP \
                 env var and no reachable cloud metadata. The suggested IP below \
                 is the container's private IP — only useful for internal \
                 traffic. Override with the externally-visible address (or set \
                 NETIDX_PUBLIC_IP / pass --listen).",
            );
        }
        // Ask for the IP and port separately. The IP is the one thing
        // the operator actually has to know; the port has a
        // conventional default they can take with a keystroke. The IP
        // is also what the CA-server prompt suggests as its default, so
        // a whole TLS setup only needs the operator to type one address.
        let ip = prompt::parsed_with_default::<std::net::IpAddr>(
            "advertised IP (what clients connect to)",
            None,
            &s.advertised_ip().to_string(),
        )?;
        prompt_resolver_port(ip)?
    };
    // Bind: silent in the normal case (defaults to listen.ip()), but
    // level-1 prompted in the cloud-elastic case where the resolver
    // advertises a public IP while binding to a private NIC. Without
    // this the resolver would try to bind to the public IP and fail
    // with EADDRNOTAVAIL.
    let bind = if let Some(b) = f.bind {
        Some(b)
    } else {
        let s = shape.get_or_init(NetShape::detect);
        match s.resolver_bind_override() {
            Some(private) => Some(prompt::parsed_with_default::<std::net::IpAddr>(
                "bind address (local NIC the resolver actually binds to)",
                None,
                &private.to_string(),
            )?),
            None => None,
        }
    };
    // Local-client bind override: when the resolver listens on a
    // public IP while binding to a private NIC (cloud-elastic), the
    // local client's publisher cannot bind to the public IP — it's
    // NAT'd onto the private NIC, not present on any local interface.
    // Emit the `BindCfg::Elastic` form so the local publisher binds to
    // the private subnet but advertises the public IP, matching how
    // the standalone publisher template handles the same shape.
    //
    // Only kicks in when shape was actually detected (operator didn't
    // pass both --listen and --bind) and the shape is cloud-elastic.
    // Other shapes fall through to the template's default (bind/32),
    // which already does the right thing for symmetric setups.
    let local_client_bind = match shape.get() {
        Some(s @ NetShape::CloudElastic { .. }) => Some(s.publisher_bind_suggestion()),
        _ => None,
    };
    // The auth-scheme sub-args (tls cert paths, krb5 spn, local
    // socket) are level-2 prompts inside `resolver_self_auth` — so
    // defaulting `--auth` to tls walks the operator through the cert
    // paths rather than dead-ending on "--tls-name required".
    //
    // `_tls_staging` holds the TLS-issuance staging tempdir (Some only
    // on the local-CA-issue path). It must outlive `finish()` below so
    // the issued cert/key survive until `apply()` copies them into the
    // canonical location — hence a named binding scoped to the whole
    // function, not a discard.
    // Resolve the activation units dir up front: if the TLS flow stands
    // up a CA server, its `ca.unit` must land in the *same* dir as the
    // resolver/id-map units so the one supervisor (and the one system
    // service we offer below) runs them all.
    let units_dir = resolve_units_dir(&f.common, f.units_dir.as_deref())?;
    let (auth, _tls_staging) =
        resolver_self_auth(&f, Some(listen.ip()), units_dir.as_deref())?;
    let perms_seed = match &f.perms_seed {
        Some(p) => Some(netidx_conf::perms::load_perms(p)?),
        None => None,
    };
    // `--parent-path` defaults to this resolver's base: in the
    // referral model, the parent path is the path at which **this**
    // resolver attaches in the parent's namespace, which is just
    // wherever this resolver hosts its own tree.
    let parent_default_path = f.base.clone();
    let with_id_map = resolve_id_map_choice(&auth, f.no_id_map)?;
    let params = netidx_conf::template::resolver::ResolverParams {
        auth,
        base: ArcStr::from(f.base),
        listen,
        bind,
        parent: f.parent.to_parent_ref(&parent_default_path)?,
        perms_seed,
        with_perms_file: !f.no_perms,
        perms_path: f.perms_path,
        resolver_config_path: f.resolver_config_path,
        units_dir,
        netidx_binary: resolve_netidx_binary(f.netidx_binary)?,
        with_id_map,
        id_map_path: f.id_map_path,
        id_map_socket: f.id_map_socket,
        with_local_client: !f.no_client,
        client_config_path: f.client_config_path,
        local_client_bind,
    };
    let rt = template::resolver(&params)?;
    // A standalone resolver is a network-facing daemon — system-scope
    // is what makes it boot-triggered and visible to the OS. The
    // service-install flow will re-exec under sudo if needed.
    finish(rt, &f.common, service::ServiceNeed::at(service::ScopeArg::System))
}

/// Decide whether to install the id-mapper daemon alongside the
/// resolver. `--no-id-map` is always honoured (skip). Otherwise the
/// default depends on auth: TLS gets the daemon (cert SANs have no
/// other lookup path through `id`), Krb5 defaults to NO on the
/// assumption that most kerberos sites already have a system-level
/// IdM (FreeIPA, AD, OpenIDM) handling principal → uid via SSSD /
/// nsswitch — `prompt::confirm` flips silently to the default on
/// a non-TTY. Anonymous and Local don't use the daemon.
fn resolve_id_map_choice(auth: &AuthChoice, no_id_map: bool) -> Result<bool> {
    if no_id_map {
        return Ok(false);
    }
    match auth {
        AuthChoice::Tls { .. } => prompt::confirm(
            "install the netidx id-mapper daemon (maps TLS cert identities \
             to unix uids)?",
            true,
        ),
        AuthChoice::Krb5 { .. } => prompt::confirm(
            "install the netidx id-mapper daemon? Most kerberos sites use a \
             system-level IdM (FreeIPA, AD, OpenIDM) and answer N here; \
             answer Y only if you don't have one and want netidx to map \
             principals to uids itself",
            false,
        ),
        AuthChoice::Anonymous | AuthChoice::Local { .. } => Ok(false),
    }
}

/// Resolve the resolver's own auth choice. The optional `TempDir` is
/// the TLS-issuance staging guard (see [`resolver_tls_generate`]); the
/// caller must keep it alive until `apply()` has run. All non-TLS
/// schemes (and the BYO / wait-for-CSR TLS paths) return `None`.
fn resolver_self_auth(
    f: &ResolverFlags,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
) -> Result<(AuthChoice, Option<tempfile::TempDir>)> {
    // `f.auth` was resolved (with a level-1 prompt) upstream in
    // `run_resolver`; treat it as guaranteed-Some. The
    // per-scheme sub-args are level-2 prompts — once the operator
    // has chosen a scheme, the things that scheme needs are not
    // optional.
    let auth = f.auth.expect("auth resolved before resolver_self_auth");
    match auth {
        AuthKind::Anonymous => Ok((AuthChoice::Anonymous, None)),
        AuthKind::Local => Ok((
            AuthChoice::Local {
                path: prompt::required_path(
                    "local-auth socket path",
                    f.socket.clone(),
                )?,
            },
            None,
        )),
        AuthKind::Krb5 => Ok((
            AuthChoice::Krb5 {
                spn: ArcStr::from(
                    prompt::required_string("kerberos SPN", f.spn.clone())?.as_str(),
                ),
            },
            None,
        )),
        AuthKind::Tls => resolver_tls_auth(f, default_ca_ip, units_dir),
    }
}

/// Resolve the resolver's TLS identity. The certificate is either an
/// explicit path the operator supplies, or the literal `generate` —
/// in which case we either request it from a CA server or issue one
/// from the local CA (creating that CA if none exists). The interactive
/// prompt defaults to `generate`: for the common small-org case where
/// the resolver host is also the CA host, hitting return through the
/// prompts gets you a working setup.
fn resolver_tls_auth(
    f: &ResolverFlags,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
) -> Result<(AuthChoice, Option<tempfile::TempDir>)> {
    let name = prompt_resolver_own_tls_name(f.tls_name.clone())?;
    // 'generate' (issue from local CA) is unix-only — the CA module
    // depends on openssl which we don't ship to Windows.
    #[cfg(unix)]
    let (label, default) = (
        "resolver certificate (a path, or 'generate' to issue one from a local CA)",
        "generate",
    );
    #[cfg(not(unix))]
    let (label, default) = (
        "resolver certificate path (CA-issued; local issuance is unix-only)",
        "",
    );
    let cert_choice = prompt::string_with_default(
        label,
        f.tls_cert.as_ref().map(|p| p.to_string_lossy().into_owned()),
        default,
    )?;
    #[cfg(unix)]
    let is_generate = cert_choice == "generate";
    #[cfg(not(unix))]
    let is_generate = false;
    if is_generate {
        #[cfg(unix)]
        {
            return resolver_tls_generate(f, &name, default_ca_ip, units_dir);
        }
        #[cfg(not(unix))]
        {
            let _ = (default_ca_ip, units_dir);
            unreachable!("generate path is unix-only")
        }
    }
    if cert_choice.is_empty() {
        bail!(
            "resolver certificate path required (local issuance is \
             unix-only — provide a pre-issued cert on this platform)"
        );
    }
    // Explicit cert path — the operator is bringing their own
    // identity, so the key and trusted-CA bundle are required too.
    // No staging dir: the sources are wherever the operator put them,
    // and the copy into the canonical dir happens in `apply()`.
    Ok((
        AuthChoice::Tls {
            name: ArcStr::from(name.as_str()),
            certificate: PathBuf::from(cert_choice),
            private_key: prompt::required_path(
                "path to the resolver private key",
                f.tls_key.clone(),
            )?,
            trusted: prompt::required_path(
                "path to the trusted CA bundle",
                f.tls_trusted.clone(),
            )?,
            // BYO-cert path: the key already exists. We don't know
            // whether it's encrypted, and guessing wrong either buries
            // an extraneous askpass in the config or skips a needed
            // one — same trade-off as the parent-referral BYO branch.
            // Operators can edit `tls.askpass` post-install.
            askpass: None,
        },
        None,
    ))
}

/// Issue a resolver certificate from the local CA, creating the CA
/// first if there isn't one. Returns an [`AuthChoice::Tls`] pointing
/// at the issued files, plus an optional staging-dir guard.
///
/// The local-CA-issue path issues into a **staging tempdir** rather
/// than the canonical install location, and returns that `TempDir` so
/// the caller can keep it alive until `apply()` has copied the files
/// into place. This keeps the prompt phase side-effect-free under the
/// config tree: `check_no_overwrite` sees a pristine destination, so a
/// re-install without `--force` is caught *before* the issued cert/key
/// could clobber an existing identity. The BYO-CSR fallthrough
/// (`generate_csr_and_wait_for_cert`) returns `None` — it must use the
/// canonical dir as the operator's cert-drop rendezvous.
///
/// Under `--dry-run` this issues nothing — it prints what it would
/// do and returns the *intended* canonical paths (and no staging dir).
/// `apply()` doesn't run in a dry run, so those paths are never read;
/// they exist only so the template can render its plan.
#[cfg(unix)]
fn resolver_tls_generate(
    f: &ResolverFlags,
    name: &str,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
) -> Result<(AuthChoice, Option<tempfile::TempDir>)> {
    // First offer the network path: a CA server signs our CSR on the
    // spot. The issued files are written to a staging tempdir; we hand
    // it back so the caller can hold it across the template install,
    // which does the --force-gated copy to the canonical location.
    if !f.common.dry_run {
        // The resolver requests its own already-decided SAN, so default
        // the join's name prompt to it.
        if let Some((j, staging)) = maybe_join_ca_server(
            default_ca_ip,
            ca_proto::NodeKind::Resolver,
            Some(name),
        )? {
            return Ok((joined_to_auth(j), Some(staging)));
        }
    }
    let ca_dir = paths::user_ca_dir()?;
    let ca_cert = ca_dir.join("certificate.pem");
    // `identity_dir` also validates `name` (no path separators) — do
    // it up front so a bad name fails before we touch the CA. It is the
    // *canonical* install location: where `apply()` copies the issued
    // files and what the keychain entry is keyed on, even though
    // issuance itself writes to the staging dir below.
    let identity_dir = tls::identity_dir(name)?;

    if f.common.dry_run {
        if ca::default_ca_present() {
            println!(
                "[dry-run] would issue a resolver certificate '{name}' from the \
                 local CA at {}",
                ca_dir.display(),
            );
        } else {
            println!(
                "[dry-run] would create a new local CA at {}, then issue a \
                 resolver certificate '{name}' from it",
                ca_dir.display(),
            );
        }
        return Ok((
            AuthChoice::Tls {
                name: ArcStr::from(name),
                certificate: identity_dir.join("certificate.pem"),
                private_key: identity_dir.join("private.key"),
                trusted: ca_cert,
                askpass: None,
            },
            None,
        ));
    }

    // Resolve the CA (open existing, create new, or fall through to
    // BYO-CSR) before collecting the password — that way an operator
    // who abandons CA creation doesn't have to type a password they
    // won't end up using. The BYO-CSR branch collects its own
    // password inside `generate_csr_and_wait_for_cert`.
    let ca = if ca::default_ca_present() {
        println!("issuing from the local CA at {}", ca_dir.display());
        ca::open_default_ca()?
    } else {
        // No CA — offer to create one. Declining falls through to
        // the same "generate a key + CSR locally and wait for the
        // operator to drop in a signed cert" flow the parent-cert
        // prompt uses for the same situation: the operator has
        // their own CA (corporate PKI, etc.) and wants to take the
        // CSR there rather than spinning up a local one.
        if !prompt::confirm(
            &format!(
                "no CA found at {} — create a new local CA now?",
                ca_dir.display()
            ),
            true,
        )? {
            let (certificate, private_key, trusted, askpass) =
                generate_csr_and_wait_for_cert(name)?;
            return Ok((
                AuthChoice::Tls {
                    name: ArcStr::from(name),
                    certificate,
                    private_key,
                    trusted,
                    askpass,
                },
                None,
            ));
        }
        // Create the CA via the SAME entry point as `netidx conf ca
        // init` — admin/policy, identicon, and the "set up the CA
        // server?" question all included. We already know the domain
        // from the resolver's TLS name (e.g. `resolver.ryu-oh.org` →
        // `ryu-oh.org`), so name the CA `ca.<domain>` per the
        // `<name>.<domain>` convention and pass the domain through so the
        // first admin's policy defaults to `*.<domain>` — no extra typing
        // and no mismatch with the names this deployment will issue.
        let domain = netidx_conf::tls::domain_from_san(name)
            .map(|d| d.to_string())
            .unwrap_or_else(|_| name.to_string());
        let (created, _need) = ca::create_vaulted_ca(ca::NewCaOpts {
            dir: ca_dir.clone(),
            common_name: Some(ca::default_ca_cn(&domain)),
            domain: Some(domain),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec![],
            key_bits: netidx_conf::ca::DEFAULT_KEY_BITS,
            validity_days: netidx_conf::ca::DEFAULT_CA_VALIDITY_DAYS,
            admin: None,
            allowed_san: vec![],
            max_validity_days: netidx_conf::ca::DEFAULT_LEAF_VALIDITY_DAYS,
            setup_server: None,
            listen: None,
            // The CA co-locates with this resolver — suggest its IP for
            // the CA server's listen address.
            listen_hint: default_ca_ip,
            units_dir: units_dir.map(|p| p.to_path_buf()),
        })?;
        // The returned `ServiceNeed` (System, if the CA server was set
        // up) is intentionally dropped: this resolver install always
        // ends with a single system-service offer (it installs the
        // resolver unit), and the `ca.unit` we just wrote lands in the
        // resolver's own units dir, so that one service supervises it.
        created
    };
    // Local-CA-issue path: prompt for an optional leaf-key password
    // (and an askpass program if one is given). The password is saved
    // to the system keychain keyed on the *canonical* key path (where
    // the daemon will read it from), so the resolver server can decrypt
    // at startup without needing askpass wiring (the rfile::Auth::Tls
    // schema has none); the askpass goes into the emitted client config
    // as the fallback if the keychain entry is ever missing.
    let key_path = identity_dir.join("private.key");
    let (password, askpass) = collect_key_password_and_askpass(&key_path, name)?;
    // Issue into a staging dir, not the canonical location: `apply()`
    // is the only thing that should write under the config tree. The
    // returned guard keeps the staging files alive until apply() copies
    // them into place, then drops (cleaning the tempdir up).
    let staging = tempfile::TempDir::new().context("creating tls staging dir")?;
    println!("issuing resolver certificate '{name}' (this may take a moment)...");
    let issued =
        ca::issue_identity(&ca, name, staging.path().to_path_buf(), password.as_deref())?;
    println!("issued resolver certificate:");
    println!("  name:        {name}");
    // Show the *installed* paths apply() will create, not the transient
    // staging paths the files currently sit in.
    println!("  certificate: {}", identity_dir.join("certificate.pem").display());
    println!("  private key: {}", identity_dir.join("private.key").display());
    println!("  trusted CA:  {}", ca_cert.display());
    if password.is_some() {
        println!("  private key is encrypted; password saved to the system keychain.");
    }
    Ok((
        AuthChoice::Tls {
            name: ArcStr::from(name),
            certificate: issued.certificate,
            private_key: issued.private_key,
            trusted: ca_cert,
            askpass,
        },
        Some(staging),
    ))
}

// -- client-only --------------------------------------------------------------

#[derive(Args, Debug)]
pub(crate) struct PublisherFlags {
    /// Cluster address (repeatable). All addresses share the auth
    /// scheme; for heterogeneous setups, edit the generated JSON.
    /// Prompted (single address) when omitted.
    #[arg(long = "addr", num_args = 1)]
    addrs: Vec<SocketAddr>,
    /// Auth scheme (anonymous|local|krb5|tls). Prompted when omitted.
    #[arg(long = "auth")]
    auth: Option<AuthKind>,
    #[arg(long = "spn")]
    spn: Option<String>,
    #[arg(long = "socket")]
    socket: Option<PathBuf>,
    /// Server's TLS name (when `--auth tls`).
    #[arg(long = "tls-server-name")]
    tls_server_name: Option<String>,
    #[command(flatten)]
    tls: TlsIdentityFlags,
    /// Override `default_auth` on the client config. None ⇒ derive
    /// from `--auth`.
    #[arg(long = "default-auth")]
    default_auth: Option<AuthKind>,
    #[arg(long = "base", default_value = "/")]
    base: String,
    #[arg(long = "config")]
    config_path: Option<PathBuf>,
    /// `default_bind_config` string (e.g. `10.0.0.5/32` for an exact
    /// interface, `10.0.0.0/24` for a subnet match). Default: the
    /// first public/private IPv4 enumerated on this host, formatted
    /// as `<ip>/32`. Use `local` to bind to 127.0.0.1 (only safe when
    /// the resolver is also on loopback).
    #[arg(long = "bind")]
    bind: Option<String>,
    #[command(flatten)]
    common: CommonFlags,
}

fn run_publisher(mut f: PublisherFlags) -> Result<()> {
    // `--auth` is a level-1 prompt (default tls); the cluster
    // address has no universal default, so it stays level 2.
    let kind: AuthKind = prompt::choice_with_default(
        "auth scheme",
        f.auth,
        &["anonymous", "local", "krb5", "tls"],
        "tls",
    )?;
    f.auth = Some(kind);
    if f.addrs.is_empty() {
        let ip = prompt::required_parsed::<std::net::IpAddr>(
            "cluster IP (the resolver to connect to, e.g. 10.0.0.1)",
            None,
        )?;
        f.addrs.push(prompt_resolver_port(ip)?);
    }
    let per_addr_auth = publisher_per_addr_auth(&f)?;
    let addrs: Vec<(SocketAddr, ReferralAuth)> =
        f.addrs.iter().map(|a| (*a, per_addr_auth.clone())).collect();
    let mut tls_identities = vec![];
    // Holds the staging tempdir(s) for any CA-server-joined identity
    // until `finish` (which runs the template's --force-gated install)
    // returns. Dropping a TempDir deletes its contents, so this must
    // outlive the `finish` call below.
    let mut tls_staging: Vec<tempfile::TempDir> = Vec::new();
    if let Some(spec) = f.tls.to_spec()? {
        tls_identities.push(spec);
    } else if matches!(f.auth, Some(AuthKind::Tls)) {
        // Interactive TLS path: `--auth tls` without `--tls-cert`
        // (etc.) used to dead-end at the template's
        // "default_auth=Tls requires at least one tls_identity"
        // check. Mirror the workstation/resolver UX instead — walk
        // the operator through a cert-or-generate cascade so they
        // can either point at an existing cert or get a key+CSR
        // produced on the spot. The cluster IP seeds the CA-server
        // address default.
        let upstream = f.addrs.first().map(|a| a.ip());
        // Suggest our SAN as `<user>.<domain>`, the domain coming from
        // the resolver's TLS name the operator just gave.
        let suggested = match &per_addr_auth {
            ReferralAuth::Tls(san) => suggest_client_san(san),
            _ => None,
        };
        let si = prompt_tls_client_identity(upstream, suggested.as_deref())?;
        tls_identities.push(si.spec);
        tls_staging.extend(si.staging);
    }
    let default_auth = f.default_auth.map(|k| k.default_mech());
    // Level-1 prompt: same loopback-mixing pitfall as the resolver
    // template (leaving this unset lands the publisher on
    // `BindCfg::Local` = 127.0.0.1, which a non-loopback resolver
    // rejects). The suggestion is `<subnet>/<prefix>` built from the
    // discovered NIC's netmask so the publisher matches any local
    // interface on that network — wider and more useful than `/32`.
    // On a cloud VM the suggestion is the `BindCfg::Elastic` form
    // (`<public>@<private-subnet>/<prefix>`) so traffic registers
    // with the public IP while binding to the private NIC.
    //
    // Only compute the shape (which probes cloud metadata) when we
    // actually need a default — avoid the metadata-service round-trip
    // when `--bind` was passed explicitly.
    let bind: String = if let Some(b) = f.bind {
        b
    } else {
        let shape = NetShape::detect();
        if shape.needs_operator_hint() {
            eprintln!(
                "note: detected container environment with no NETIDX_PUBLIC_IP \
                 env var and no reachable cloud metadata service. The suggested \
                 bind below contains a `<PUBLIC_IP>` placeholder you must \
                 replace with the externally-visible IP this publisher's \
                 traffic appears from (or set NETIDX_PUBLIC_IP / pass --bind).",
            );
        }
        let suggestion = shape.publisher_bind_suggestion();
        let answer = prompt::string_with_default(
            "publisher bind (BindCfg, e.g. 10.0.0.0/24, 10.0.0.5/32, local, \
             54.32.224.1@172.23.112.0/24)",
            None,
            &suggestion,
        )?;
        if answer.contains('<') {
            bail!(
                "publisher bind contains a placeholder ({answer:?}); set \
                 NETIDX_PUBLIC_IP in the environment or pass --bind with the \
                 public IP filled in",
            );
        }
        answer
    };
    let default_bind_config = Some(bind);
    let params = netidx_conf::template::publisher::PublisherParams {
        addrs,
        default_auth,
        tls_identities,
        base: ArcStr::from(f.base),
        config_path: f.config_path,
        default_bind_config,
    };
    let rt = template::publisher(&params)?;
    // No daemon to supervise on a client-only host — nothing to
    // install as an OS service.
    finish(rt, &f.common, service::ServiceNeed::NONE)
}

fn publisher_per_addr_auth(f: &PublisherFlags) -> Result<ReferralAuth> {
    // `f.auth` was resolved (level-1 prompt) upstream. The per-scheme
    // sub-args are level-2 prompts — once a scheme is chosen the
    // things it needs are not optional.
    let auth = f.auth.expect("auth resolved before publisher_per_addr_auth");
    Ok(match auth {
        AuthKind::Anonymous => ReferralAuth::Anonymous,
        AuthKind::Local => ReferralAuth::Local(ArcStr::from(
            prompt::required_path("local-auth socket path", f.socket.clone())?
                .to_string_lossy()
                .as_ref(),
        )),
        AuthKind::Krb5 => ReferralAuth::Krb5(ArcStr::from(
            prompt::required_string("kerberos SPN", f.spn.clone())?.as_str(),
        )),
        AuthKind::Tls => ReferralAuth::Tls(ArcStr::from(
            prompt_resolver_tls_name(
                f.addrs.first().copied(),
                "server TLS name",
                f.tls_server_name.clone(),
            )?
            .as_str(),
        )),
    })
}

// -- Apply / dry-run ----------------------------------------------------------

fn finish(
    rt: RenderedTemplate,
    common: &CommonFlags,
    need: service::ServiceNeed,
) -> Result<()> {
    println!("{}", rt.describe());
    if !common.dry_run {
        check_no_overwrite(&rt, common.force)?;
        rt.apply().context("applying template")?;
        println!("ok");
    }
    // Single end-of-process hook: offer the OS service (or print the
    // dry-run note). Sub-steps with their own units merge their needs
    // into `need` before we get here, so this fires exactly once.
    service::offer(need, service::ServiceGate {
        dry_run: common.dry_run,
        no_service: common.no_service,
        with_service: common.with_service,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use netidx_conf::template::TlsCopyJob;
    use std::collections::BTreeMap;

    #[test]
    fn suggest_client_san_uses_resolver_domain() {
        // A single-label resolver SAN has no domain to borrow → no
        // suggestion (regardless of whether a username is found).
        assert_eq!(suggest_client_san("localhost"), None);
        // Multi-label resolver SAN → `<user>.<domain>` with the
        // resolver's leftmost label stripped. Oracle the username off
        // the same source the implementation uses so the test can't
        // disagree with it across environments.
        match current_username() {
            Some(user) => {
                assert_eq!(
                    suggest_client_san("resolver.ryu-oh.org"),
                    Some(format!("{user}.ryu-oh.org"))
                );
                assert_eq!(
                    suggest_client_san("resolver.local"),
                    Some(format!("{user}.local"))
                );
            }
            None => {
                assert_eq!(suggest_client_san("resolver.ryu-oh.org"), None);
            }
        }
    }

    fn empty_rt() -> RenderedTemplate {
        RenderedTemplate {
            client_config: None,
            resolver_config: None,
            perms_file: None,
            id_map_file: None,
            units: BTreeMap::new(),
            units_dir: None,
            tls_install: Vec::new(),
        }
    }

    // The BYO-CSR generate flows (resolver-declines-local-CA, and the
    // client/parent identity prompt) deliberately use the canonical
    // identity dir as the operator's cert-drop rendezvous: the key is
    // generated there and the signed cert + trusted bundle are dropped
    // there. So the copy job's source == destination for all three
    // files. The guard must treat a self-copy as a no-op, not as
    // clobbering operator material. (The local-CA-issue path avoids
    // this entirely now by staging in a tempdir — see
    // `resolver_tls_generate`.)
    #[test]
    fn self_copy_install_is_not_an_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().to_path_buf();
        let [cert, key, _trusted] = netidx_conf::tls::installed_files_in(&dest);
        // The issued cert + key already live at their install paths.
        std::fs::write(&cert, b"cert").unwrap();
        std::fs::write(&key, b"key").unwrap();
        // The trusted CA is the one genuine copy: a different source, and
        // its destination (dest/trusted.pem) doesn't exist yet.
        let ca_src = dir.path().join("ca-certificate.pem");
        std::fs::write(&ca_src, b"ca").unwrap();

        let mut rt = empty_rt();
        rt.tls_install.push(TlsCopyJob {
            cn: "resolver.example.com".to_string(),
            dest_dir: dest,
            certificate_src: cert,
            private_key_src: key,
            trusted_src: ca_src,
        });
        check_no_overwrite(&rt, false).unwrap();
    }

    // A BYO-cert install whose source is elsewhere must still refuse to
    // clobber an identity already at the destination without --force.
    #[test]
    fn foreign_source_over_existing_dest_is_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("dest");
        std::fs::create_dir_all(&dest).unwrap();
        let [cert_dst, key_dst, _trusted_dst] =
            netidx_conf::tls::installed_files_in(&dest);
        // An identity already installed at the destination.
        std::fs::write(&cert_dst, b"old cert").unwrap();
        std::fs::write(&key_dst, b"old key").unwrap();
        // Operator brings their own cert/key/ca from a different dir.
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let cert_src = src.join("certificate.pem");
        let key_src = src.join("private.key");
        let ca_src = src.join("ca.pem");
        for p in [&cert_src, &key_src, &ca_src] {
            std::fs::write(p, b"new").unwrap();
        }

        let mut rt = empty_rt();
        rt.tls_install.push(TlsCopyJob {
            cn: "resolver.example.com".to_string(),
            dest_dir: dest,
            certificate_src: cert_src,
            private_key_src: key_src,
            trusted_src: ca_src,
        });
        // Without --force this must bail (cert + key dests pre-exist).
        assert!(check_no_overwrite(&rt, false).is_err());
        // With --force it proceeds.
        check_no_overwrite(&rt, true).unwrap();
    }
}
