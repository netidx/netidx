//! `netidx conf <role> install` — render and apply a `RenderedTemplate`
//! for one of the three v1 templates.

use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::config::DefaultAuthMech;
// Qualified `conf_proto::` uses are all in the unix-only conf-server
// enrollment path; the items below are cross-platform.
use clap::Args;
#[cfg(unix)]
use netidx_conf::conf_proto;
use netidx_conf::tls;
use netidx_conf::{
    conf_client,
    conf_proto::{InfoAuth, NodeKind, Role},
    discovery,
    fingerprint::ColorMode,
    netshape::NetShape,
    paths,
    provenance::{InstallRecord, InstallRole, NetworkIdentity},
    template::{
        self, AuthChoice, ParentRef, ReferralAuth, RenderedTemplate, TlsIdentitySpec,
        resolver::IdMapMode,
    },
};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use zeroize::Zeroizing;

// `ca` submodule depends on netidx_conf::ca which is unix-only.
#[cfg(unix)]
use super::ca;
use super::{prompt, service};

// The three install entry points are exposed to the per-role command
// modules (`conf::roles::*`), which own the `<role> install` surface.
// `init` stays the shared install engine + helpers.

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

    fn as_str(self) -> &'static str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Local => "local",
            Self::Krb5 => "krb5",
            Self::Tls => "tls",
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
        let trusted = self.trusted.as_ref().context("--tls-trusted required")?.clone();
        // Default `our_name` to the cert's DNS SAN — same trick the
        // interactive cascade uses for BYO certs. Asking the
        // operator to type the SAN that's already in the cert just
        // lets them get it wrong; reading it is always correct.
        let our_name = match &self.our_name {
            Some(s) => s.clone(),
            None => {
                netidx_conf::tls::extract_dns_san_from_pem(&cert).with_context(|| {
                    format!(
                        "deriving --tls-our-name from {} — supply a cert with a \
                         DNS SubjectAlternativeName entry, or pass \
                         --tls-our-name explicitly",
                        cert.display(),
                    )
                })?
            }
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
        let path = self.parent_path.as_deref().unwrap_or(default_path);
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
    /// How new private keys are protected at rest: `seal` (bind to
    /// this machine's TPM / Secure Enclave), `password` (typed;
    /// interactive only), or `none`. Default: seal when the host has
    /// usable sealing hardware (prompted on a TTY), else none.
    #[arg(long = "key-protection")]
    key_protection: Option<KeyProtArg>,
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
/// entirely). Unix-only: the workstation role (its sole caller) is
/// unix-only.
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

/// `workstation install` is unix-only (Local auth + activation
/// supervisor). Fail fast here with a pointer to the publisher role,
/// rather than running the whole discovery/enrollment cascade and only
/// erroring at template-render time.
#[cfg(not(unix))]
pub(crate) fn run_workstation(_f: WorkstationFlags) -> Result<()> {
    bail!("{}", netidx_conf::template::workstation::UNSUPPORTED_MSG)
}

#[cfg(unix)]
pub(crate) fn run_workstation(f: WorkstationFlags) -> Result<()> {
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
    // Provenance for the install record: set when the workstation joins
    // a discovered (glyph-confirmed) network below.
    let mut net_prov: (Option<NetworkIdentity>, Option<SocketAddr>) = (None, None);
    let parent = if f.parent.any_set() {
        f.parent.to_parent_ref(parent_default_path)?
    } else {
        // Ask the network before asking the human: a discovered (and
        // glyph-confirmed) conf server answers everything the prompt
        // cascade would have asked — every resolver address with its
        // auth, and (TLS networks) where to get our client cert. The
        // probe outcome rides into the manual cascade so a declined
        // discovery is never re-offered.
        let probe = discover_network(NodeKind::Workstation)?;
        net_prov = network_provenance(&probe);
        match probe.have() {
            Some(net) => {
                let have_identity = !tls_identities.is_empty();
                let addrs = network_addrs_and_identity(
                    net,
                    NodeKind::Workstation,
                    have_identity,
                    f.key_protection,
                    &mut tls_identities,
                    &mut tls_staging,
                )?;
                Some(ParentRef {
                    path: ArcStr::from(parent_default_path),
                    ttl: None,
                    addrs,
                })
            }
            None => match prompt_parent_referral(
                parent_default_path,
                f.key_protection,
                &probe,
            )? {
                None => None,
                Some((parent_ref, maybe_ident)) => {
                    if let Some(si) = maybe_ident {
                        tls_identities.push(si.spec);
                        tls_staging.extend(si.staging);
                    }
                    Some(parent_ref)
                }
            },
        }
    };
    let owner =
        if f.no_perms { None } else { resolve_workstation_owner(f.owner.clone())? };
    // Struct-literal construction so adding a field to
    // WorkstationParams forces a compile error here rather than
    // silently leaving the new field defaulted (14th commandment).
    let units_dir = resolve_units_dir(&f.common, f.units_dir.as_deref())?;
    let has_tls = !tls_identities.is_empty();
    let post_apply_units_dir = units_dir.clone();
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
        units_dir,
        netidx_binary: resolve_netidx_binary(f.netidx_binary)?,
        with_container: !f.no_container,
    };
    let rt = template::workstation(&params)?;
    let (network, conf_server) = net_prov;
    // The workstation's own resolver is local-auth; the network it refers
    // up to (if any) carries its auth inside the parent referral.
    let record = InstallRecord::new(
        InstallRole::Workstation,
        f.base.clone(),
        "local",
        network,
        conf_server,
    );
    // A workstation runs in the operator's session; a user-scope
    // systemd / launchd service is the right level — no sudo needed.
    finish_with(
        rt,
        &f.common,
        service::ServiceNeed::at(service::ScopeArg::User),
        record,
        // TLS identities expire: install the renewal daemon alongside.
        move || match (&post_apply_units_dir, has_tls) {
            (Some(d), true) => install_renew_unit(d),
            _ => Ok(()),
        },
    )
}

#[derive(Args, Debug)]
pub(crate) struct WorkstationJoinFlags {
    /// Show the join plan without writing anything.
    #[arg(long = "dry-run")]
    pub dry_run: bool,
    /// How a newly-enrolled private key is protected at rest: `seal`,
    /// `password`, or `none`. Only relevant when joining a TLS network
    /// (where `join` enrolls a client certificate).
    #[arg(long = "key-protection")]
    pub key_protection: Option<KeyProtArg>,
}

/// `workstation join` — graduate a local-only workstation to a networked
/// one: discover + glyph-confirm a network, enroll a client cert if it's
/// TLS, and attach the local resolver to it via a parent referral —
/// without a reinstall or a hand-edit. The marker records the joined
/// (pinned) network so later `status`/`update` can re-pin to it.
pub(crate) fn run_workstation_join(f: WorkstationJoinFlags) -> Result<()> {
    let mut rec = InstallRecord::load_default()?.context(
        "no install record found — `workstation join` operates on an existing \
         workstation install",
    )?;
    if rec.role != InstallRole::Workstation {
        bail!(
            "this host is a {} install, not a workstation — `join` is a \
             workstation operation",
            rec.role.as_str(),
        );
    }
    if let Some(net) = &rec.network {
        bail!(
            "this workstation has already joined network {:?}. Re-joining a \
             different network isn't supported yet (uninstall + reinstall to \
             switch).",
            net.domain,
        );
    }
    let rpath = paths::discover_resolver_config()
        .context("no resolver config found — is this a workstation install?")?;
    let cpath = paths::discover_client_config()
        .context("no client config found — is this a workstation install?")?;
    // Discover + glyph-confirm the network to join (the one human trust
    // decision), then map its resolvers — enrolling a client cert when the
    // network is TLS.
    let probe = discover_network(NodeKind::Workstation)?;
    let net = probe.have().context(
        "no network was selected to join (nothing discovered, or the offer was \
         declined)",
    )?;
    let mut tls_identities: Vec<TlsIdentitySpec> = Vec::new();
    // Holds the enrolled cert's staging tempdir until `apply()` copies it
    // into place — must outlive the apply below.
    let mut tls_staging: Vec<tempfile::TempDir> = Vec::new();
    let addrs = network_addrs_and_identity(
        net,
        NodeKind::Workstation,
        false,
        f.key_protection,
        &mut tls_identities,
        &mut tls_staging,
    )?;
    let parent = ParentRef { path: ArcStr::from(rec.base.as_str()), ttl: None, addrs };
    // Capture the confirmed identity for the marker before applying.
    let network =
        NetworkIdentity::new(net.identity.domain.clone(), &net.identity.fingerprint);
    let conf_server = net.info.reached.first().copied();
    let rt =
        netidx_conf::template::attach_to_network(&rpath, &cpath, parent, tls_identities)?;
    println!("{}", rt.describe());
    if f.dry_run {
        return Ok(());
    }
    rt.apply().context("applying the join")?;
    println!("ok");
    rec.network = Some(network);
    rec.conf_server = conf_server;
    rec.save_default().context("updating the install record")?;
    println!(
        "joined network {:?} — restart the local resolver to use it",
        rec.network.as_ref().expect("just set").domain,
    );
    Ok(())
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
#[cfg(unix)]
fn prompt_parent_referral(
    default_path: &str,
    kp: Option<KeyProtArg>,
    probe: &ConfServers,
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
            (ReferralAuth::Local(ArcStr::from(socket.to_string_lossy().as_ref())), None)
        }
        AuthKind::Krb5 => {
            let spn = prompt::required_string(
                "parent resolver's kerberos SPN (e.g. netidx/resolver.example.com@REALM)",
                None,
            )?;
            (ReferralAuth::Krb5(ArcStr::from(spn.as_str())), None)
        }
        AuthKind::Tls => {
            let server_name =
                prompt_resolver_tls_name(Some(addr), "parent TLS server name", None)?;
            // identity is required for TLS — either bring one or
            // (the generate path diverges via `bail!`). Suggest our own
            // SAN as `<user>.<domain>`, the domain taken from the
            // resolver's SAN we just resolved.
            let suggested = suggest_client_san(&server_name);
            let staged = prompt_tls_client_identity(suggested.as_deref(), kp, probe)?;
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

/// Validity requested from a CA server. The server caps it to the
/// admin's policy, so this is just an upper bound.
const JOIN_VALIDITY: Duration = Duration::from_secs(730 * 86400);

/// A TLS identity obtained from a CA server, with its files **staged**
/// in a tempdir. `apply()` performs the (force-gated) install into the
/// canonical `~/.config/netidx/tls/<name>/` layout.
struct JoinedIdentity {
    name: String,
    certificate: PathBuf,
    private_key: PathBuf,
    trusted: PathBuf,
    /// The client-config askpass fallback, when the operator chose
    /// password protection for the key.
    askpass: Option<PathBuf>,
}

/// A client TLS identity plus the tempdir its files are staged in until
/// `apply()` installs them. `staging` is `None` for the BYO-cert /
/// wait-for-CSR paths, whose files already sit at their canonical home.
struct StagedIdentity {
    spec: TlsIdentitySpec,
    staging: Option<tempfile::TempDir>,
}

/// Obtain a TLS identity from the network's conf server when one is
/// known (or discoverable), instead of the local-CA / CSR flow.
///
/// Keyed on what the calling flow already knows ([`ConfServers`]):
/// `Have` joins with no further questions (the identity was already
/// glyph-confirmed); `DontHave` returns `None` silently — the operator
/// already said there is no conf server, asking again would be
/// nagging; `NotProbed` runs discovery right here (browse → confirm →
/// aggregate, with its manual-address fallback). Cross-platform: this
/// is also how a node with no openssl (Windows) gets a TLS cert.
fn maybe_join_ca_server(
    probe: &ConfServers,
    kind: NodeKind,
    suggested_name: Option<&str>,
    kp: Option<KeyProtArg>,
) -> Result<Option<(JoinedIdentity, tempfile::TempDir)>> {
    let probed_here;
    let net = match probe {
        ConfServers::DontHave => return Ok(None),
        ConfServers::Have(net) => net,
        ConfServers::NotProbed => match discover_network(kind)? {
            ConfServers::Have(net) => {
                probed_here = net;
                &probed_here
            }
            ConfServers::DontHave | ConfServers::NotProbed => return Ok(None),
        },
    };
    let Some(ca_addr) = net.info.ca_addr else {
        println!(
            "note: network {:?} reported no CA; falling back to local \
             certificate setup",
            net.identity.domain,
        );
        return Ok(None);
    };
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let (j, staging) =
        join_network(&rt, ca_addr, kind, suggested_name, kp, &net.identity)?;
    Ok(Some((j, staging)))
}

/// Print a confirmed-or-not network identity: domain, claimed roles,
/// fingerprint text + identicon.
pub(super) fn show_network_identity(
    addr: SocketAddr,
    identity: &conf_client::CaIdentity,
) {
    println!(
        "The conf server at {addr} serves network {:?} (roles: {}) and presented \
         this identity:",
        identity.domain,
        describe_roles(&identity.roles),
    );
    println!("  SHA256  {}", identity.fingerprint.text());
    println!("{}", identity.fingerprint.identicon(ColorMode::detect()));
}

fn describe_roles(roles: &[Role]) -> String {
    if roles.is_empty() {
        return "none".to_string();
    }
    roles
        .iter()
        .map(|r| match r {
            Role::Ca => "ca",
            Role::Resolver => "resolver",
            Role::IdMap => "id-map",
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// How often a waiting enrollee checks on its queued request. Each
/// poll is one short pinned connection, so waiting holds nothing open.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Obtain a cert from an **already confirmed** network — every
/// connection pins to `identity`, aborting before sending anything
/// secret if the server changed underneath us. Returns the issued
/// identity staged in a tempdir (the template's `--force`-gated
/// `apply()` installs it).
///
/// The default path queues a signing request and waits for an admin to
/// approve it remotely (`netidx conf ca approve`): the enrollee shows a
/// request code (the CSR key's fingerprint) the admin matches out of
/// band, and the admin — who knows who they're enrolling — chooses the
/// id-map groups at approval. The synchronous path (an admin present
/// at this machine types their password) remains one answer away; it's
/// the only path where the groups are chosen here.
fn join_network(
    rt: &tokio::runtime::Runtime,
    addr: SocketAddr,
    kind: NodeKind,
    suggested_name: Option<&str>,
    kp: Option<KeyProtArg>,
    identity: &conf_client::CaIdentity,
) -> Result<(JoinedIdentity, tempfile::TempDir)> {
    let name_label = "TLS identity name to request (the cert's DNS SAN)";
    let name = match suggested_name {
        Some(s) => prompt::string_with_default(name_label, None, s)?,
        None => prompt::required_string(name_label, None)?,
    };
    // Key protection is decided before the request: the operator is
    // here now, and the queued path may wait on a remote admin for a
    // long time after.
    let protection =
        choose_key_protection(kp, &tls::identity_dir(&name)?.join("private.key"), &name)?;
    let admin_here = prompt::confirm(
        "is a CA admin at this machine to enter their password now? \
         (No: queue the request for remote approval)",
        false,
    )?;
    let issued = if admin_here {
        // The admin chooses the new identity's id-map groups here, at
        // enrollment — the per-admin policy is the *allowed set* the
        // server validates this choice against. Infrastructure
        // identities (resolvers, conf servers) don't act as users, so
        // they default to no registration; everything else defaults to
        // `users`.
        let groups = prompt_id_map_groups(&[], default_id_map_groups(kind))?;
        let admin = prompt::required_string("CA admin name", None)?;
        let password = Zeroizing::new(rpassword::prompt_password(format!(
            "CA password for admin {admin}: "
        ))?);
        rt.block_on(conf_client::request_cert(
            addr,
            kind,
            &name,
            &admin,
            password,
            JOIN_VALIDITY,
            groups,
            identity,
        ))?
    } else {
        let pending = rt.block_on(conf_client::enqueue(
            addr,
            kind,
            &name,
            JOIN_VALIDITY,
            identity,
        ))?;
        println!("request queued. Your request code is:");
        println!("  SHA256  {}", pending.fingerprint.text());
        println!("{}", pending.fingerprint.identicon(ColorMode::detect()));
        println!(
            "send this code to your CA admin (chat, phone — any channel you \
             trust); they approve with `netidx conf ca approve` after \
             matching it. Waiting for approval (Ctrl-C to abort; the request \
             expires on its own)..."
        );
        loop {
            std::thread::sleep(POLL_INTERVAL);
            match rt.block_on(conf_client::poll(addr, kind, &pending, identity))? {
                conf_client::PollOutcome::Pending => continue,
                conf_client::PollOutcome::Issued(issued) => break issued,
                conf_client::PollOutcome::Denied(reason) => {
                    bail!("the CA admin denied this request: {reason}")
                }
                conf_client::PollOutcome::Expired => bail!(
                    "the request expired before an admin approved it; re-run \
                     to queue a new one"
                ),
            }
        }
    };

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
    // Apply the protection decided up front: the key is encrypted
    // before it ever touches disk (sealed and password cases), and a
    // sealed password's blob is staged beside it — the identity
    // installer copies sidecars with their keys.
    let key_payload = match protection.password() {
        Some(pw) => Zeroizing::new(
            netidx::tls::encrypt_private_key(&issued.private_key_pem, pw)
                .context("encrypting the issued private key")?,
        ),
        None => issued.private_key_pem.clone(),
    };
    netidx_conf::atomic::write_atomic(&private_key, key_payload.as_bytes(), 0o600)?;
    protection.write_sidecar(&private_key)?;
    netidx_conf::atomic::write_atomic(&trusted, issued.trusted_pem.as_bytes(), 0o644)?;
    println!("got TLS identity {name:?} from conf server {addr}");
    for w in &issued.warnings {
        println!("  warning: {w}");
    }
    Ok((
        JoinedIdentity {
            name,
            certificate,
            private_key,
            trusted,
            askpass: protection.askpass(),
        },
        staging,
    ))
}

/// Install the renewal-daemon activation unit — every host with TLS
/// identities gets one, so certificate lifecycle is nobody's chore.
/// Idempotent overwrite.
fn install_renew_unit(units_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(units_dir)
        .with_context(|| format!("creating activation dir {}", units_dir.display()))?;
    let netidx_binary = std::env::current_exe()
        .context("could not determine current netidx binary for the renew unit")?;
    let unit = netidx_conf::template::services::renew::unit(
        &netidx_conf::template::services::renew::RenewServiceParams { netidx_binary },
    )?;
    let dir = netidx_conf::activation::ActivationDir::open(Some(units_dir))?;
    dir.save("renew", &unit).context("writing the renew activation unit")?;
    println!(
        "activation unit → {}",
        netidx_conf::activation::unit_path_in(units_dir, "renew").display()
    );
    Ok(())
}

/// The id-map group default suggested at enrollment, by node kind:
/// infrastructure identities (resolvers, conf servers) don't act as
/// users, so they default to no registration.
pub(super) fn default_id_map_groups(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Resolver | NodeKind::ConfServer => "",
        NodeKind::Publisher | NodeKind::Client | NodeKind::Workstation => "users",
    }
}

/// Prompt for the id-map groups to assign a new identity
/// (comma-separated, first is primary). CLI-`provided` values
/// short-circuit (`--id-map-group ''` is the explicit "none"); an
/// empty answer means no registration.
pub(super) fn prompt_id_map_groups(
    provided: &[String],
    default: &str,
) -> Result<Vec<String>> {
    let raw: Vec<String> = if !provided.is_empty() {
        provided.to_vec()
    } else {
        prompt::string_with_default(
            "id-map groups for this identity (comma-separated, first is \
             primary; empty for none)",
            None,
            default,
        )?
        .split(',')
        .map(|s| s.to_string())
        .collect()
    };
    Ok(raw.iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
}

/// How long the install flows browse mDNS for conf servers.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);

/// A discovered-and-confirmed netidx network: the operator-confirmed
/// conf-plane identity plus the aggregated picture of the network
/// (every connection behind `info` was pinned to that identity).
pub(super) struct DiscoveredNetwork {
    pub(super) identity: conf_client::CaIdentity,
    pub(super) info: conf_client::NetworkInfo,
}

/// What the calling flow knows about conf servers on this network.
/// Threaded into every sub-flow that could otherwise offer a network
/// join, so the operator is asked at most once.
// `Have` is ~200 bytes vs the dataless variants; this is a one-shot
// value on an interactive CLI's stack — boxing it would trade nothing
// for an allocation.
#[allow(clippy::large_enum_variant)]
pub(super) enum ConfServers {
    /// A network was discovered and its identity glyph-confirmed: use
    /// it, ask nothing further.
    Have(DiscoveredNetwork),
    /// We probed (and/or the operator declined): there is none. Never
    /// offer a network join again in this run.
    DontHave,
    /// Nobody has checked (CLI-flag path, dry-run, non-TTY). A
    /// sub-flow that wants a conf server may probe itself.
    NotProbed,
}

impl ConfServers {
    pub(super) fn have(&self) -> Option<&DiscoveredNetwork> {
        match self {
            ConfServers::Have(net) => Some(net),
            ConfServers::DontHave | ConfServers::NotProbed => None,
        }
    }
}

/// Find the network this node should join: browse mDNS, group what's
/// found by domain, let the operator pick (falling back to a manual
/// conf-server address when discovery finds nothing), then fetch and
/// glyph-confirm the network's identity and aggregate `GetInfo` across
/// its conf servers (peer walk — one reachable server is enough).
///
/// [`ConfServers::DontHave`] ⇒ the operator concluded there is no conf
/// server (nothing found / declined); callers continue with the manual
/// prompt cascade and never re-offer a network join.
/// [`ConfServers::NotProbed`] is returned only on a non-TTY — scripted
/// installs use CLI flags.
pub(super) fn discover_network(kind: NodeKind) -> Result<ConfServers> {
    if !prompt::stdin_is_tty() {
        return Ok(ConfServers::NotProbed);
    }
    println!(
        "searching for netidx conf component servers on the local network \
         ({}s)...",
        DISCOVERY_TIMEOUT.as_secs()
    );
    let found = discovery::browse_or_empty(DISCOVERY_TIMEOUT);
    // Group the (unauthenticated, hint-only) beacons by domain.
    let mut domains: BTreeMap<String, Vec<discovery::Discovered>> = BTreeMap::new();
    for d in found {
        domains.entry(d.domain.clone()).or_default().push(d);
    }
    let manual_fallback = || -> Result<Option<Vec<SocketAddr>>> {
        Ok(prompt::optional_parsed::<SocketAddr>(
            "address of an existing conf server to join (ip:port), blank if there is none",
            None,
        )?
        .map(|a| vec![a]))
    };
    let seeds: Vec<SocketAddr> = if domains.is_empty() {
        println!("no conf servers found.");
        match manual_fallback()? {
            Some(s) => s,
            None => return Ok(ConfServers::DontHave),
        }
    } else {
        let chosen: Option<String> = if domains.len() == 1 {
            let (domain, servers) = domains.iter().next().unwrap();
            let use_it = prompt::confirm(
                &format!(
                    "found netidx network {domain:?} ({} conf server(s)) — join it?",
                    servers.len()
                ),
                true,
            )?;
            use_it.then(|| domain.clone())
        } else {
            let names: Vec<String> = domains.keys().cloned().collect();
            let opts: Vec<&str> =
                names.iter().map(|s| s.as_str()).chain(["none"]).collect();
            let choice: String = prompt::choice_with_default(
                "multiple netidx networks found — which to join ('none' for \
                 manual setup)",
                None,
                &opts,
                opts[0],
            )?;
            (choice != "none").then_some(choice)
        };
        match chosen {
            Some(domain) => {
                let servers =
                    domains.remove(&domain).expect("chosen domain came from the map");
                servers.iter().flat_map(|d| d.socket_addrs()).collect()
            }
            None => match manual_fallback()? {
                Some(s) => s,
                None => return Ok(ConfServers::DontHave),
            },
        }
    };
    confirm_seeds(&seeds, kind)
}

/// Fetch the network identity from the first reachable seed and have the
/// operator confirm it — the single human trust decision; everything after
/// is pinned to the confirmed fingerprint. Then map the network. Shared by
/// mDNS discovery and the explicit `--parent-conf-server` / manual-address
/// paths, so "is a CA reachable?" has ONE answer feeding the
/// create-vs-enroll decision.
fn confirm_seeds(seeds: &[SocketAddr], kind: NodeKind) -> Result<ConfServers> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let mut fetched = None;
    for addr in seeds {
        match rt.block_on(conf_client::fetch_identity(*addr, kind)) {
            Ok(id) => {
                fetched = Some((*addr, id));
                break;
            }
            Err(e) => println!("note: conf server {addr} could not be queried: {e:#}"),
        }
    }
    let Some((addr, identity)) = fetched else {
        bail!("no conf server could be reached")
    };
    show_network_identity(addr, &identity);
    if !prompt::confirm("does this match what your network admin gave you?", false)? {
        bail!("the network identity was not confirmed; nothing was sent");
    }
    let info = rt
        .block_on(conf_client::aggregate(seeds, kind, &identity))
        .context("mapping the network (GetInfo peer walk)")?;
    Ok(ConfServers::Have(DiscoveredNetwork { identity, info }))
}

/// Confirm a network reachable at one explicit address — the WAN parent
/// given via `--parent-conf-server`, where there is no mDNS. Resolving the
/// parent into a `Have` BEFORE the create-vs-enroll decision is what makes a
/// satellite enroll its cert from the existing CA and never mint its own.
fn confirm_network_at(addr: SocketAddr, kind: NodeKind) -> Result<ConfServers> {
    confirm_seeds(&[addr], kind)
}

/// Map a confirmed network's resolvers into per-address referral auth,
/// obtaining a client TLS identity from the network's CA when the
/// network runs TLS and the caller doesn't already have one
/// (`have_identity`). The identity was already glyph-confirmed in
/// [`discover_network`] — no second confirmation; the signing
/// connection still pins to it.
fn network_addrs_and_identity(
    net: &DiscoveredNetwork,
    kind: NodeKind,
    have_identity: bool,
    kp: Option<KeyProtArg>,
    tls_identities: &mut Vec<TlsIdentitySpec>,
    tls_staging: &mut Vec<tempfile::TempDir>,
) -> Result<Vec<(SocketAddr, ReferralAuth)>> {
    if net.info.resolvers.is_empty() {
        bail!(
            "the conf servers of network {:?} reported no resolvers — is the \
             resolver host's conf server down? (manual setup: re-run and \
             leave the conf-server prompts blank)",
            net.identity.domain,
        );
    }
    println!(
        "network {:?}: {} resolver(s)",
        net.identity.domain,
        net.info.resolvers.len()
    );
    let mut addrs = Vec::new();
    let mut needs_tls = false;
    for r in &net.info.resolvers {
        let auth = match &r.auth {
            InfoAuth::Anonymous => ReferralAuth::Anonymous,
            InfoAuth::Krb5 { spn } => ReferralAuth::Krb5(ArcStr::from(spn.as_str())),
            InfoAuth::Tls { name } => {
                needs_tls = true;
                ReferralAuth::Tls(ArcStr::from(name.as_str()))
            }
        };
        println!("  {} ({})", r.addr, describe_info_auth(&r.auth));
        addrs.push((r.addr, auth));
    }
    if needs_tls && !have_identity {
        let Some(ca_addr) = net.info.ca_addr else {
            bail!(
                "network {:?} uses TLS but none of its conf servers reported \
                 a CA — cannot obtain a client certificate",
                net.identity.domain,
            )
        };
        // Default identity name: a service host is best identified by its
        // hostname (`publisher.<domain>`), a personal machine by its user
        // (`alice.<domain>`). The domain is the TLS-attested one from the
        // server hello.
        let base = match kind {
            NodeKind::Publisher => current_hostname(),
            _ => current_username(),
        };
        let suggested = base.map(|n| format!("{n}.{}", net.identity.domain));
        let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
        let (j, staging) =
            join_network(&rt, ca_addr, kind, suggested.as_deref(), kp, &net.identity)?;
        tls_identities.push(joined_to_spec(j));
        tls_staging.push(staging);
    }
    Ok(addrs)
}

fn describe_info_auth(a: &InfoAuth) -> String {
    match a {
        InfoAuth::Anonymous => "anonymous".to_string(),
        InfoAuth::Krb5 { spn } => format!("krb5, spn {spn}"),
        InfoAuth::Tls { name } => format!("tls, name {name}"),
    }
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
        askpass: j.askpass,
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
        askpass: j.askpass,
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
    suggested_name: Option<&str>,
    kp: Option<KeyProtArg>,
    probe: &ConfServers,
) -> Result<StagedIdentity> {
    // First the network path: a conf server signs our CSR on the
    // spot, no files to shuttle. Works on every platform (rcgen, not
    // openssl), so it's also how a Windows node gets a TLS identity.
    // What we already know about conf servers (`probe`) decides
    // whether this asks anything at all.
    if let Some((j, staging)) =
        maybe_join_ca_server(probe, NodeKind::Client, suggested_name, kp)?
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
            generate_and_wait_for_parent_cert(suggested_name, kp)?
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
        let trusted =
            prompt::required_path("trusted CA bundle (signs the parent's cert)", None)?;
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
            format!("deriving identity domain from cert SAN {:?}", our_name)
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
/// `./<our-name>.csr` (matching `netidx conf component tls request`), not in
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
    kp: Option<KeyProtArg>,
) -> Result<(String, PathBuf, PathBuf, PathBuf, Option<PathBuf>)> {
    let name_label = "your TLS identity name (CN for the CSR; cert SAN)";
    let our_name = match suggested_name {
        Some(s) => prompt::string_with_default(name_label, None, s)?,
        None => prompt::required_string(name_label, None)?,
    };
    let (cert_path, key_path, trusted_path, askpass) =
        generate_csr_and_wait_for_cert(&our_name, kp)?;
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

/// How a private key is protected at rest. Decided once per identity
/// at issue time by [`choose_key_protection`]; every key-producing
/// flow acts on the same three cases.
enum KeyProtection {
    /// Encrypted under a random password sealed to this machine's
    /// TPM. The blob is written beside the key as `<key>.tpm`
    /// ([`netidx_conf::tls::sealed_sidecar`]); the key is useless
    /// off-host.
    Sealed { password: Zeroizing<String>, blob: Vec<u8> },
    /// Encrypted under a typed password (saved to the system keychain;
    /// `askpass` is the client-config fallback when the keychain is
    /// locked or missing).
    Password { password: String, askpass: Option<PathBuf> },
    /// Plaintext — file modes are the only protection.
    None,
}

impl KeyProtection {
    /// The encryption password, if any (what the openssl issue paths
    /// take).
    fn password(&self) -> Option<&str> {
        match self {
            KeyProtection::Sealed { password, .. } => Some(password),
            KeyProtection::Password { password, .. } => Some(password),
            KeyProtection::None => None,
        }
    }

    fn askpass(&self) -> Option<PathBuf> {
        match self {
            KeyProtection::Password { askpass, .. } => askpass.clone(),
            KeyProtection::Sealed { .. } | KeyProtection::None => None,
        }
    }

    /// Write the sealed-password sidecar beside `key` (no-op for the
    /// other variants). Call with wherever the key file actually
    /// lands — staging dirs included; the identity installer copies
    /// sidecars along with their keys.
    fn write_sidecar(&self, key: &Path) -> Result<()> {
        if let KeyProtection::Sealed { blob, .. } = self {
            netidx_conf::atomic::write_atomic(
                &netidx_conf::tls::sealed_sidecar(key),
                blob,
                0o600,
            )?;
        }
        Ok(())
    }
}

/// The `--key-protection` flag: like [`choose_key_protection`]'s
/// interactive choice, but scriptable. `password` is inherently
/// interactive (it prompts), so headless installs use seal or none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyProtArg {
    Seal,
    Password,
    None,
}

impl FromStr for KeyProtArg {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "seal" => Ok(Self::Seal),
            "password" => Ok(Self::Password),
            "none" => Ok(Self::None),
            _ => bail!("key-protection must be one of seal|password|none"),
        }
    }
}

/// Decide how a new identity's private key is protected at rest:
/// **seal** (TPM / Secure Enclave — the default whenever the host has
/// one), **password** (typed, keychain + askpass), or **none**.
///
/// `key_path` is the canonical on-disk location the key will live at —
/// the keychain entry (password case) is keyed on it. On a non-TTY the
/// default applies silently: seal when the hardware is usable, none
/// otherwise. Hardware that fails at seal time degrades to the
/// no-hardware behavior with a warning — setup must not dead-end on
/// flaky hardware.
fn choose_key_protection(
    flag: Option<KeyProtArg>,
    key_path: &Path,
    name: &str,
) -> Result<KeyProtection> {
    let tpm = netidx_tpm::available();
    let choice = match flag {
        Some(KeyProtArg::Seal) => "seal".to_string(),
        Some(KeyProtArg::Password) => "password".to_string(),
        Some(KeyProtArg::None) => "none".to_string(),
        None if !prompt::stdin_is_tty() => {
            if tpm {
                "seal".to_string()
            } else {
                "none".to_string()
            }
        }
        None => {
            let (options, default): (&[&str], &str) = if tpm {
                (&["seal", "password", "none"], "seal")
            } else {
                (&["password", "none"], "none")
            };
            prompt::choice_with_default(
                &format!(
                    "private key protection for {name} (seal: bind to this \
                     machine's {}; password: typed at issue, kept in the \
                     keychain; none: file modes only)",
                    netidx_tpm::MECHANISM
                ),
                None,
                options,
                default,
            )?
        }
    };
    match choice.as_str() {
        "seal" => {
            let password = netidx_tpm::random_secret();
            match netidx_tpm::seal(password.as_bytes()) {
                Ok(blob) => {
                    println!(
                        "  the key for {name} will be sealed to this machine's \
                         {} — copied anywhere else it is useless",
                        netidx_tpm::MECHANISM
                    );
                    Ok(KeyProtection::Sealed { password, blob })
                }
                Err(e) => {
                    eprintln!(
                        "warning: {} sealing failed ({e:#}); falling back to an \
                         unprotected key. Re-run once the hardware is usable, or \
                         choose password protection interactively.",
                        netidx_tpm::MECHANISM
                    );
                    Ok(KeyProtection::None)
                }
            }
        }
        "password" => {
            // Name the identity so an operator setting up several
            // certs in one session knows which key this password is
            // for.
            let pw =
                rpassword::prompt_password(format!("private key password for {name}: "))?;
            if pw.is_empty() {
                bail!("empty password; choose 'none' for an unprotected key");
            }
            let again = rpassword::prompt_password(format!(
                "private key password for {name} (again): "
            ))?;
            if again != pw {
                bail!("passwords did not match");
            }
            // Search for an askpass program and prompt the operator to
            // confirm or override it. A blank answer takes the
            // default; an operator who explicitly wants no askpass can
            // type an empty string when the default is itself empty.
            #[cfg(unix)]
            let discovered = find_askpass();
            #[cfg(not(unix))]
            let discovered: Option<PathBuf> = None;
            let default = discovered
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            let answer = prompt::string_with_default(
                "askpass program (used to ask for the key password at startup)",
                None,
                &default,
            )?;
            let askpass =
                if answer.is_empty() { None } else { Some(PathBuf::from(answer)) };
            // Save into the system keychain so the resolver server can
            // decrypt its key without an askpass at startup. Failure
            // here isn't fatal — the keychain might be locked,
            // sandboxed, or missing entirely, and the askpass fallback
            // still lets the client config work. We log and continue.
            if let Err(e) =
                netidx::tls::save_password_for_key(&key_path.to_string_lossy(), &pw)
            {
                eprintln!(
                    "warning: failed to save key password to the system keychain \
                     ({e:#}); the resolver may need an askpass at startup. \
                     Re-run with the keychain unlocked, or pre-populate the entry \
                     manually.",
                );
            }
            Ok(KeyProtection::Password { password: pw, askpass })
        }
        _ => Ok(KeyProtection::None),
    }
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
/// `./<name>.csr` (matching `netidx conf component tls request`), not in the
/// identity dir — the operator hands it off to a CA admin, so it
/// needs to be where they'll naturally look for it.
#[cfg(unix)]
fn generate_csr_and_wait_for_cert(
    name: &str,
    kp: Option<KeyProtArg>,
) -> Result<(PathBuf, PathBuf, PathBuf, Option<PathBuf>)> {
    let dest_dir = tls::identity_dir(name)?;
    std::fs::create_dir_all(&dest_dir)
        .with_context(|| format!("creating identity dir {}", dest_dir.display()))?;
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
    let protection = choose_key_protection(kp, &key_path, name)?;
    let askpass = protection.askpass();
    let kr = netidx_conf::ca::generate_csr(
        &netidx_conf::ca::Subject::cn(name.to_string()),
        &[netidx_conf::ca::SanEntry::Dns(name.to_string())],
        2048,
        protection.password(),
    )
    .context("generating private key + CSR")?;
    netidx_conf::atomic::write_atomic(&key_path, &kr.private_key_pem, 0o600)
        .with_context(|| format!("writing private key to {}", key_path.display()))?;
    protection.write_sidecar(&key_path)?;
    netidx_conf::atomic::write_atomic(&csr_path, &kr.csr_pem, 0o644)
        .with_context(|| format!("writing CSR to {}", csr_path.display()))?;

    println!();
    println!("Generated TLS identity '{}':", name);
    println!("  private key (0600): {}", key_path.display());
    println!("  CSR         (0644): {}", csr_path.display());
    match &protection {
        KeyProtection::Sealed { .. } => {
            println!(
                "  private key is encrypted; the password is sealed to this \
                 machine's {} beside it.",
                netidx_tpm::MECHANISM
            );
        }
        KeyProtection::Password { .. } => {
            println!(
                "  private key is encrypted; password saved to the system keychain."
            );
        }
        KeyProtection::None => (),
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
    use std::io::IsTerminal;
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
    let name =
        prompt::string_with_default("this resolver's name", None, DEFAULT_RESOLVER_NAME)?;
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
/// This machine's hostname (short form) — the natural identity name for a
/// service host (a publisher), as opposed to the unix user for a personal
/// workstation.
fn current_hostname() -> Option<String> {
    #[cfg(unix)]
    {
        if let Ok(h) = nix::unistd::gethostname()
            && let Ok(s) = h.into_string()
            && !s.is_empty()
        {
            return Some(s);
        }
    }
    std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty())
}

fn current_username() -> Option<String> {
    #[cfg(unix)]
    {
        let uid = nix::unistd::Uid::current();
        if let Ok(Some(u)) = nix::unistd::User::from_uid(uid) {
            return Some(u.name);
        }
    }
    for var in ["USER", "LOGNAME", "USERNAME"] {
        if let Ok(v) = std::env::var(var)
            && !v.is_empty()
        {
            return Some(v);
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
    /// - `--auth tls` installs it automatically (informing you, not
    ///   asking): cert SANs have no `/bin/id` translation path, so the
    ///   daemon is required.
    /// - `--auth krb5` prompts with default N — most kerberos sites
    ///   have a system-level IdM (FreeIPA, AD, OpenIDM) handling
    ///   principal → uid via SSSD / nsswitch already.
    /// - Anonymous / Local auth never use the daemon.
    ///
    /// For `--auth krb5`, id-map entries are keyed by the full
    /// kerberos principal including realm (e.g. `eric@RYU-OH.ORG`).
    #[arg(long = "no-id-map")]
    no_id_map: bool,
    /// Skip conf-server setup entirely (expert). On a fresh krb5 /
    /// anonymous network this also skips the conf-plane CA. A host
    /// without a conf server is invisible to discovery, and if no conf
    /// server exists anywhere on the network, certificate renewal and
    /// future zero-touch installs don't work at all.
    #[arg(long = "no-conf-server")]
    no_conf_server: bool,
    /// Set this resolver up as a CHILD of an existing network: give the
    /// parent's conf-server address (`ip:port`). The install requests
    /// delegation of a subtree (`--delegate-subtree`) and, once the parent
    /// admin approves, bakes the parent referral into the config — no
    /// restart. Distinct from the peer-join discovery path. Unix-only.
    #[arg(long = "parent-conf-server")]
    parent_conf_server: Option<SocketAddr>,
    /// The subtree this resolver will own under the parent (with
    /// `--parent-conf-server`), e.g. `/eu`. Prompted if omitted.
    #[arg(long = "delegate-subtree")]
    delegate_subtree: Option<String>,
    /// How new private keys are protected at rest: `seal` (bind to
    /// this machine's TPM / Secure Enclave), `password` (typed;
    /// interactive only), or `none`. Default: seal when the host has
    /// usable sealing hardware (prompted on a TTY), else none.
    #[arg(long = "key-protection")]
    key_protection: Option<KeyProtArg>,
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

pub(crate) fn run_resolver(mut f: ResolverFlags) -> Result<()> {
    // Ask the network before asking the human: a second (or third…)
    // resolver discovers the existing network and imports its settings
    // — auth scheme, domain, where the CA is. Peer resolvers stay
    // mutually unaware; only installers aggregate the full picture.
    // The probe outcome rides through the whole install: once the
    // operator has said "no conf server", nothing downstream offers a
    // network join again.
    let probe = if let Some(parent) = f.parent_conf_server {
        if f.common.dry_run {
            // dry-run can't run the live confirm; the parent match below
            // bails on dry-run with a clear message.
            ConfServers::NotProbed
        } else {
            // An explicit WAN parent (no mDNS): confirm it and pin the
            // network. This makes `probe.have()` Some, so the install
            // ENROLLS this resolver's cert from the parent's CA and the
            // "create a local CA" branches become unreachable — a satellite
            // shares the one trust domain, it never mints its own.
            confirm_network_at(parent, NodeKind::Resolver)?
        }
    } else if f.auth.is_none() && !f.common.dry_run {
        discover_network(NodeKind::Resolver)?
    } else {
        ConfServers::NotProbed
    };
    // Resolve required args with interactive prompting before we
    // start building the params struct. `--auth` and `--listen` are
    // level-1 prompts: they have sensible defaults (tls, the
    // conventional resolver port), so a blank answer is fine. A
    // discovered network's auth scheme wins — a resolver joining a
    // network must speak what its peers speak.
    // A delegated child (`--parent-conf-server`) keeps the data-plane auth
    // the operator chose — a /eu subtree may run krb5 under a TLS parent —
    // so only the trust-domain/CA decision comes from the parent, never its
    // auth. A plain discovered peer still imports its cluster's scheme.
    let imported_auth = if f.parent_conf_server.is_some() {
        None
    } else {
        probe.have().and_then(network_auth_kind)
    };
    let kind: AuthKind = match imported_auth {
        Some(k) => {
            println!(
                "importing auth scheme from the network: {}",
                match k {
                    AuthKind::Anonymous => "anonymous",
                    AuthKind::Local => "local",
                    AuthKind::Krb5 => "krb5",
                    AuthKind::Tls => "tls",
                }
            );
            k
        }
        None => {
            if f.auth.is_none() && prompt::stdin_is_tty() {
                println!(
                    "auth scheme — how clients prove who they are to this resolver:"
                );
                println!(
                    "  anonymous  no authentication; any client may connect (labs, trusted LANs)"
                );
                println!(
                    "  tls        certificate-based identity — the recommended default for a network"
                );
                println!(
                    "  krb5       Kerberos; choose this only if your site already runs it"
                );
            }
            prompt::choice_with_default(
                "auth scheme",
                f.auth,
                &["anonymous", "krb5", "tls"],
                "tls",
            )?
        }
    };
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
    let ResolvedAuth { choice: auth, staging: _tls_staging, netidx_ca } = match probe
        .have()
    {
        Some(net) => resolver_auth_from_network(&f, net, kind)?,
        None => resolver_self_auth(&f, Some(listen.ip()), units_dir.as_deref(), &probe)?,
    };
    // First server of a new network with a non-TLS data plane: the
    // conf plane still needs its trust root (it is always TLS — the
    // glyph confirm, enrollment, and server-to-server pushes all hang
    // off the CA), so create one even though the data plane is
    // krb5/anonymous. Mandatory on krb5, a question on anonymous —
    // see [`conf_plane_decision`]. The TLS path gets its CA inside
    // `resolver_tls_generate`.
    #[cfg(unix)]
    if probe.have().is_none()
        && f.parent_conf_server.is_none()
        && !f.common.dry_run
        && matches!(kind, AuthKind::Krb5 | AuthKind::Anonymous)
        && !ca::default_ca_present()
        && match conf_plane_decision(kind, f.no_conf_server) {
            ConfPlane::Mandatory => {
                println!(
                    "setting up the conf server for this network. The conf \
                     plane is TLS even on a krb5 data plane — it anchors \
                     discovery, enrollment, and certificate renewal. \
                     (expert opt-out: --no-conf-server)"
                );
                true
            }
            ConfPlane::Ask => prompt::confirm(
                "set up a conf server for this network? (creates a CA used only \
                 to secure the conf plane — data-plane auth stays as chosen)",
                true,
            )?,
            ConfPlane::Skip => false,
        }
    {
        let domain = prompt::string_with_default(
            "network domain (groups this network in discovery, e.g. ryu-oh.org)",
            None,
            DEFAULT_TLS_DOMAIN,
        )?;
        // The returned `ServiceNeed` is intentionally dropped: this
        // resolver install always ends with a single system-service
        // offer, and the conf-server unit lands in the resolver's own
        // units dir, so that one service supervises it.
        let (_ca, _need) = ca::create_vaulted_ca(ca::NewCaOpts {
            dir: paths::user_ca_dir()?,
            common_name: Some(ca::default_ca_cn(&domain)),
            domain: Some(domain),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec![],
            key_bits: netidx_conf::ca::DEFAULT_KEY_BITS,
            ca_validity: netidx_conf::ca::DEFAULT_CA_VALIDITY,
            leaf_validity: netidx_conf::ca::DEFAULT_LEAF_VALIDITY,
            ca_renew_threshold: netidx_conf::ca::DEFAULT_CA_RENEW_THRESHOLD,
            admin: None,
            allowed_san: vec![],
            max_validity: netidx_conf::ca::DEFAULT_LEAF_VALIDITY,
            id_map_groups: vec![],
            may_enroll_servers: None,
            insecure_no_tpm: false,
            setup_server: Some(true),
            listen: None,
            listen_hint: Some(listen.ip()),
            units_dir: units_dir.clone(),
        })?;
    }
    let perms_seed = match &f.perms_seed {
        Some(p) => Some(netidx_conf::perms::load_perms(p)?),
        None => None,
    };
    // `--parent-path` defaults to this resolver's base: in the
    // referral model, the parent path is the path at which **this**
    // resolver attaches in the parent's namespace, which is just
    // wherever this resolver hosts its own tree.
    let parent_default_path = f.base.clone();
    let id_map = resolve_id_map_choice(&auth, f.no_id_map)?;
    let no_conf_server = f.no_conf_server;
    // The conf-server step after apply() needs the *actual* config
    // paths this install produces — resolve the template's defaults
    // the same way it will.
    let resolver_config_actual = match &f.resolver_config_path {
        Some(p) => p.clone(),
        None => netidx_conf::resolver::default_save_path()?,
    };
    // Only the netidx id-mapper writes an id-map.json the post-apply
    // step needs to know about; Platform / None have no such file.
    let id_map_actual = if matches!(id_map, IdMapMode::Netidx) {
        Some(match &f.id_map_path {
            Some(p) => p.clone(),
            None => netidx_conf::id_map::user_id_map_path()?,
        })
    } else {
        None
    };
    let post_apply_units_dir = units_dir.clone();
    // Build the install record before the post-apply closure moves
    // `probe`. A resolver joining a discovered network pins that
    // network's identity; a fresh first resolver records none (it is the
    // root — `resolver update` is a later pass).
    let (network, conf_server) = network_provenance(&probe);
    let record = InstallRecord::new(
        InstallRole::Resolver,
        f.base.clone(),
        f.auth.map(|k| k.as_str()).unwrap_or("tls"),
        network,
        conf_server,
    );
    // Install-time child: delegate this resolver under a parent (the same
    // ceremony as `add-parent`, run inline) and bake the resulting parent
    // referral into the config the install writes — no restart needed.
    // Distinct from the peer-join discovery path above.
    let parent = match f.parent_conf_server {
        None => f.parent.to_parent_ref(&parent_default_path)?,
        Some(parent_conf) => {
            #[cfg(unix)]
            {
                if f.common.dry_run {
                    // delegate_under_parent runs the real ceremony — it
                    // enqueues a request on the parent and blocks until a
                    // remote admin approves (which mutates the parent
                    // cluster). That is not a no-op, so it cannot honor
                    // --dry-run's "write nothing" contract.
                    bail!(
                        "--dry-run can't preview an install-time delegation: \
                         --parent-conf-server runs a live, interactive approval \
                         ceremony with the parent admin (it enqueues a request on \
                         the parent and blocks until they approve). Re-run without \
                         --dry-run, or drop --parent-conf-server to preview a \
                         standalone install."
                    );
                }
                let child_auth = authchoice_to_info(&auth)?;
                let child = vec![netidx_conf::conf_proto::ResolverAddr {
                    addr: listen,
                    auth: child_auth,
                }];
                let subtree = prompt::required_string(
                    "subtree this resolver will own under the parent (e.g. /eu)",
                    f.delegate_subtree.clone(),
                )?;
                // The probe already glyph-confirmed this parent (it had to,
                // to enroll our cert from its CA), so pass that identity in —
                // the operator confirms the parent's glyph exactly once.
                let parent_addrs = super::delegation::delegate_under_parent(
                    parent_conf,
                    &subtree,
                    child,
                    probe.have().map(|n| &n.identity),
                )?;
                Some(ParentRef {
                    path: ArcStr::from(subtree.as_str()),
                    ttl: None,
                    addrs: parent_addrs
                        .into_iter()
                        .map(|r| {
                            (r.addr, super::delegation::info_to_referral_auth(&r.auth))
                        })
                        .collect(),
                })
            }
            #[cfg(not(unix))]
            {
                let _ = parent_conf;
                bail!("delegation (--parent-conf-server) is unix-only")
            }
        }
    };
    let params = netidx_conf::template::resolver::ResolverParams {
        auth,
        base: ArcStr::from(f.base),
        listen,
        bind,
        parent,
        perms_seed,
        with_perms_file: !f.no_perms,
        perms_path: f.perms_path,
        resolver_config_path: f.resolver_config_path,
        units_dir,
        netidx_binary: resolve_netidx_binary(f.netidx_binary)?,
        id_map,
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
    finish_with(
        rt,
        &f.common,
        service::ServiceNeed::at(service::ScopeArg::System),
        record,
        // Conf-server step, after the configs it points at exist: a
        // discovered network ⇒ enroll a new conf server here; a fresh
        // network ⇒ add this host's roles to the config the CA setup
        // wrote (no conf server here ⇒ nothing to do). Then the
        // renewal daemon, on any host with certificates our CA can
        // renew (a netidx-CA-issued resolver identity, or a
        // conf-server serving cert) — an external-PKI identity renews
        // through that PKI, so the daemon would only log failures.
        move || {
            #[cfg(unix)]
            post_apply_conf_server(
                probe.have(),
                kind,
                no_conf_server,
                listen,
                post_apply_units_dir.as_deref(),
                resolver_config_actual,
                id_map_actual,
            )?;
            #[cfg(not(unix))]
            {
                let _ = (&probe, kind, no_conf_server, listen);
                let _ = (resolver_config_actual, id_map_actual);
            }
            if let Some(d) = post_apply_units_dir.as_deref()
                && (netidx_ca || paths::discover_conf_server_config().is_ok())
            {
                install_renew_unit(d)?;
            }
            Ok(())
        },
    )
}

/// What the resolver install does about the conf plane (the conf
/// server, and on non-TLS networks the conf-plane CA that anchors it).
/// This function IS the install-profile matrix — documented in
/// design/conf-server.md (Install profiles) and exhaustively tested
/// below; change all three together.
// cfg(unix): only the unix-gated resolver install stands up conf
// servers (the CA signer is openssl/unix).
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfPlane {
    /// Set it up. Announce what's happening; don't ask.
    Mandatory,
    /// Default-yes question.
    Ask,
    /// Never offer (host-local auth), or the operator opted out.
    Skip,
}

/// TLS already creates the CA on a fresh network (it signs the data
/// plane), and krb5/anonymous still need the conf plane's TLS trust
/// root for discovery, enrollment, and renewal — declining it on a TLS
/// or krb5 network produces a network where certificate renewal and
/// zero-touch installs can never work, so neither is offered as a
/// question. Anonymous networks may genuinely not want the machinery
/// (lab/dev setups), so they're asked. Local auth is host-local by
/// definition: nothing to discover, nothing to enroll.
///
/// The same rule covers joining an existing network: enrolling a conf
/// server queues for remote approval like any other request (the
/// admin's `may_enroll_servers` gate runs at approval), so no admin
/// needs to be at this keyboard and there is no reason for the join
/// side of the matrix to differ.
#[cfg(unix)]
fn conf_plane_decision(kind: AuthKind, no_conf_server: bool) -> ConfPlane {
    if no_conf_server {
        return ConfPlane::Skip;
    }
    match kind {
        AuthKind::Tls | AuthKind::Krb5 => ConfPlane::Mandatory,
        AuthKind::Anonymous => ConfPlane::Ask,
        AuthKind::Local => ConfPlane::Skip,
    }
}

/// Decide how the resolver maps authenticated identities to unix
/// uid/gid (see [`IdMapMode`]). `--no-id-map` forces `Platform` (the
/// historical "no daemon, /bin/id" behaviour). TLS always installs the
/// netidx id-mapper — cert SANs have no `/bin/id` translation, so any
/// other choice denies every non-anonymous operation; that's not a
/// choice, it's a trap. Krb5 is a three-way question: platform (a site
/// IdM resolves full principals), netidx (map them yourself), or none
/// (perms keyed on the raw principal — simplest with no IdM). The
/// non-TTY default is `Platform`, matching the old assumption that most
/// kerberos sites already run a system IdM. Anonymous and Local don't
/// use the daemon.
fn resolve_id_map_choice(auth: &AuthChoice, no_id_map: bool) -> Result<IdMapMode> {
    if no_id_map {
        return Ok(IdMapMode::Platform);
    }
    match auth {
        AuthChoice::Tls { .. } => {
            println!(
                "installing the netidx id-mapper daemon (maps TLS cert \
                 identities to unix uids; skip with --no-id-map)"
            );
            Ok(IdMapMode::Netidx)
        }
        AuthChoice::Krb5 { .. } => {
            if prompt::stdin_is_tty() {
                println!(
                    "how to map kerberos principals to unix ids for permission checks:"
                );
                println!(
                    "  platform  the system's `id`/nsswitch resolves full principals \
                          — use this with a site IdM (FreeIPA, AD, sssd)"
                );
                println!(
                    "  netidx    run the netidx id-mapper and map principals yourself \
                          (no system IdM needed)"
                );
                println!(
                    "  none      don't map — permissions are keyed on the raw \
                          principal (simplest; no IdM, no daemon)"
                );
            }
            let choice: String = prompt::choice_with_default(
                "principal mapping",
                None,
                &["platform", "netidx", "none"],
                "platform",
            )?;
            Ok(match choice.as_str() {
                "netidx" => IdMapMode::Netidx,
                "none" => IdMapMode::None,
                _ => IdMapMode::Platform,
            })
        }
        AuthChoice::Anonymous | AuthChoice::Local { .. } => Ok(IdMapMode::Platform),
    }
}

/// The resolver's resolved auth identity. `staging` is the
/// TLS-issuance staging guard (see [`resolver_tls_generate`]); the
/// caller must keep it alive until `apply()` has run. `netidx_ca` ⇒
/// the TLS identity chains to this network's netidx CA (issued locally
/// or network-joined), so the renewal daemon can renew it; false for
/// external-PKI identities — their renewal belongs to that PKI — and
/// for all non-TLS schemes.
struct ResolvedAuth {
    choice: AuthChoice,
    staging: Option<tempfile::TempDir>,
    netidx_ca: bool,
}

impl ResolvedAuth {
    fn external(choice: AuthChoice) -> Self {
        ResolvedAuth { choice, staging: None, netidx_ca: false }
    }
}

/// Resolve the resolver's own auth choice.
/// A best-effort default krb5 SPN for this resolver,
/// `netidx/<fqdn>@<REALM>` — realm from `/etc/krb5.conf`'s
/// `default_realm`. `None` when the realm can't be read: without it
/// there's no useful default, so the operator must supply the whole SPN.
///
/// Kerberos service principals are FQDN-based by convention
/// (`service/host.domain@REALM`) — that is the name `hostname -f` yields
/// and the one an admin's keytab will carry. The local hostname is
/// usually the short form, so qualify it: keep it as-is if it already has
/// a domain, otherwise borrow the realm's domain (the krb5 convention is
/// realm == the upper-cased DNS domain). The operator edits the suggested
/// default when their realm doesn't follow that convention.
fn default_krb5_spn() -> Option<String> {
    let conf = std::fs::read_to_string("/etc/krb5.conf").ok()?;
    let realm = conf.lines().find_map(|l| {
        l.trim()
            .strip_prefix("default_realm")
            .and_then(|r| r.trim_start().strip_prefix('='))
            .map(|v| v.trim().to_string())
    })?;
    let host = current_hostname().unwrap_or_else(|| "resolver".to_string());
    let fqdn = if host.contains('.') {
        host
    } else {
        format!("{host}.{}", realm.to_lowercase())
    };
    Some(format!("netidx/{fqdn}@{realm}"))
}

fn resolver_self_auth(
    f: &ResolverFlags,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
    probe: &ConfServers,
) -> Result<ResolvedAuth> {
    // `f.auth` was resolved (with a level-1 prompt) upstream in
    // `run_resolver`; treat it as guaranteed-Some. The
    // per-scheme sub-args are level-2 prompts — once the operator
    // has chosen a scheme, the things that scheme needs are not
    // optional.
    let auth = f.auth.expect("auth resolved before resolver_self_auth");
    match auth {
        AuthKind::Anonymous => Ok(ResolvedAuth::external(AuthChoice::Anonymous)),
        AuthKind::Local => bail!(
            "the resolver template does not support local auth: local (unix-socket) \
             auth only authenticates clients on the same machine, so it cannot serve \
             a network. For a single-machine setup use `netidx conf \
             workstation install`; for a network resolver choose anonymous, krb5, \
             or tls."
        ),
        AuthKind::Krb5 => {
            let spn = match default_krb5_spn() {
                Some(def) => prompt::string_with_default(
                    "kerberos SPN (the service principal clients authenticate to)",
                    f.spn.clone(),
                    &def,
                )?,
                None => prompt::required_string(
                    "kerberos SPN, e.g. netidx/resolver.example.com@EXAMPLE.COM",
                    f.spn.clone(),
                )?,
            };
            Ok(ResolvedAuth::external(AuthChoice::Krb5 {
                spn: ArcStr::from(spn.as_str()),
            }))
        }
        AuthKind::Tls => resolver_tls_auth(f, default_ca_ip, units_dir, probe),
    }
}

/// Printed on the external-PKI resolver paths ('csr' and BYO cert):
/// without a netidx CA there is no conf plane on this network — the
/// capabilities lost are worth a sentence before the operator commits.
fn note_external_pki(name: &str) {
    println!(
        "note: external-PKI identity {name:?} — without a netidx CA this \
         network has no conf plane: no discovery for future installs, no \
         queued enrollment, and certificate renewal stays with your PKI \
         (the netidx renewal daemon is not installed)."
    );
}

/// Resolve the resolver's TLS identity. The certificate is an explicit
/// path the operator supplies, the literal `generate` (request it from
/// a discovered conf server, or issue from the local CA — creating
/// that CA if none exists), or the literal `csr` (external PKI:
/// generate a key + CSR here, the operator gets it signed elsewhere).
/// The interactive prompt defaults to `generate`: for the common
/// small-org case where the resolver host is also the CA host, hitting
/// return through the prompts gets you a working setup. The `csr` and
/// path forms are the expert escape into a foreign PKI — they carry no
/// conf plane, and say so.
fn resolver_tls_auth(
    f: &ResolverFlags,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
    probe: &ConfServers,
) -> Result<ResolvedAuth> {
    let name = prompt_resolver_own_tls_name(f.tls_name.clone())?;
    // The flag form of `--tls-cert` is a path, the literal `generate`, or
    // `csr`. Ask the easy yes/no question first: most operators want the
    // batteries-included CA, and only the ones who don't should have to
    // think about cert paths. `generate` / `csr` (local key + CSR via
    // openssl) are unix-only — the CA module depends on openssl.
    let flag = f.tls_cert.as_ref().map(|p| p.to_string_lossy().into_owned());
    #[cfg(unix)]
    {
        let use_built_in_ca = match flag.as_deref() {
            Some("generate") => true,
            Some(_) => false,
            None => prompt::confirm(
                "use netidx's built-in certificate authority? It creates a local \
                 CA (if there isn't one already) and issues this resolver's \
                 certificate from it — the zero-setup path. Answer n to use a \
                 certificate from your own PKI instead.",
                true,
            )?,
        };
        if use_built_in_ca {
            return resolver_tls_generate(f, &name, default_ca_ip, units_dir, probe);
        }
    }
    #[cfg(not(unix))]
    let _ = (default_ca_ip, units_dir, probe);
    // External PKI: an existing cert path, or (unix only) `csr` to make a
    // key + CSR here for your PKI to sign.
    #[cfg(unix)]
    let cert_label = "path to this resolver's certificate, or 'csr' to make a \
                      key + CSR here for your PKI to sign";
    #[cfg(not(unix))]
    let cert_label = "resolver certificate path (CA-issued; local issuance is unix-only)";
    let cert_choice = prompt::string_with_default(cert_label, flag, "")?;
    #[cfg(unix)]
    {
        if cert_choice == "csr" {
            note_external_pki(&name);
            let (certificate, private_key, trusted, askpass) =
                generate_csr_and_wait_for_cert(&name, f.key_protection)?;
            return Ok(ResolvedAuth::external(AuthChoice::Tls {
                name: ArcStr::from(name.as_str()),
                certificate,
                private_key,
                trusted,
                askpass,
            }));
        }
    }
    if cert_choice.is_empty() {
        bail!(
            "a certificate path is required (or, on unix, 'csr' to generate a \
             key + CSR for your PKI to sign)"
        );
    }
    // Explicit cert path — the operator is bringing their own
    // identity, so the key and trusted-CA bundle are required too.
    // No staging dir: the sources are wherever the operator put them,
    // and the copy into the canonical dir happens in `apply()`.
    note_external_pki(&name);
    Ok(ResolvedAuth::external(AuthChoice::Tls {
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
    }))
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
/// could clobber an existing identity.
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
    probe: &ConfServers,
) -> Result<ResolvedAuth> {
    // First the network path: a conf server signs our CSR on the
    // spot. Whether this asks anything is decided by `probe` — in
    // particular, an operator who already said "no conf server" at the
    // discovery phase is not asked again. The issued files are written
    // to a staging tempdir; we hand it back so the caller can hold it
    // across the template install, which does the --force-gated copy
    // to the canonical location.
    if !f.common.dry_run {
        // The resolver requests its own already-decided SAN, so default
        // the join's name prompt to it.
        if let Some((j, staging)) =
            maybe_join_ca_server(probe, NodeKind::Resolver, Some(name), f.key_protection)?
        {
            return Ok(ResolvedAuth {
                choice: joined_to_auth(j),
                staging: Some(staging),
                netidx_ca: true,
            });
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
        return Ok(ResolvedAuth {
            choice: AuthChoice::Tls {
                name: ArcStr::from(name),
                certificate: identity_dir.join("certificate.pem"),
                private_key: identity_dir.join("private.key"),
                trusted: ca_cert,
                askpass: None,
            },
            staging: None,
            netidx_ca: true,
        });
    }

    let ca = if ca::default_ca_present() {
        println!("issuing from the local CA at {}", ca_dir.display());
        ca::open_default_ca()?
    } else {
        // No CA — this is the first resolver of a new TLS network, so
        // the CA is created right here, no question asked: it signs
        // the data plane *and* anchors the conf plane (discovery,
        // enrollment, renewal). The operator who wants an external PKI
        // instead chose 'csr' or a cert path one prompt ago.
        //
        // Created via the SAME entry point as `netidx conf ca init` —
        // admin/policy and identicon included. We already know the
        // domain from the resolver's TLS name (e.g.
        // `resolver.ryu-oh.org` → `ryu-oh.org`), so name the CA
        // `ca.<domain>` per the `<name>.<domain>` convention and pass
        // the domain through so the first admin's policy defaults to
        // `*.<domain>` — no extra typing and no mismatch with the
        // names this deployment will issue.
        // Belt-and-suspenders: a resolver told about a parent conf server
        // must enroll from that network's CA, never mint its own. The probe
        // (confirm_network_at) already routes such installs to the enroll
        // path, so reaching here with a parent set would be a bug.
        if f.parent_conf_server.is_some() {
            bail!(
                "about to create a local CA while --parent-conf-server is set; a \
                 delegated child must enroll from the parent's CA, not create its \
                 own trust domain (internal: the probe should have prevented this)"
            );
        }
        println!(
            "no CA found at {} — creating the network's CA (it signs this \
             resolver's certificate and anchors discovery, enrollment, and \
             renewal)",
            ca_dir.display()
        );
        let domain = netidx_conf::tls::domain_from_san(name)
            .map(|d| d.to_string())
            .unwrap_or_else(|_| name.to_string());
        // The built-in-CA path applies a sensible default issuance policy
        // rather than interrogating a newcomer about SAN globs and id-map
        // groups — but say what it is and how to change it. The explicit
        // `ca init` flow is where the founding admin's policy gets tuned.
        let allowed_san = vec![format!("*.{domain}")];
        println!(
            "  the CA's founding admin will issue *.{domain} certificates, place \
             enrolled nodes in the 'users' id-map group, and may enroll conf \
             servers — change any of this later with `netidx conf ca admin \
             set-policy`."
        );
        let setup_server = match conf_plane_decision(AuthKind::Tls, f.no_conf_server) {
            ConfPlane::Mandatory => Some(true),
            ConfPlane::Skip => Some(false),
            // TLS is never a question — see the matrix.
            ConfPlane::Ask => unreachable!("tls conf plane is not Ask"),
        };
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
            ca_validity: netidx_conf::ca::DEFAULT_CA_VALIDITY,
            leaf_validity: netidx_conf::ca::DEFAULT_LEAF_VALIDITY,
            ca_renew_threshold: netidx_conf::ca::DEFAULT_CA_RENEW_THRESHOLD,
            admin: None,
            allowed_san,
            max_validity: netidx_conf::ca::DEFAULT_LEAF_VALIDITY,
            id_map_groups: vec!["users".to_string()],
            may_enroll_servers: Some(true),
            insecure_no_tpm: false,
            setup_server,
            listen: None,
            // The CA co-locates with this resolver — suggest its IP for
            // the conf server's listen address.
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
    // Local-CA-issue path: choose how the leaf key is protected at
    // rest (TPM seal when the host has one; or a typed password saved
    // to the system keychain keyed on the *canonical* key path, where
    // the daemon will read it from; or nothing). The askpass goes into
    // the emitted client config as the password case's fallback.
    let key_path = identity_dir.join("private.key");
    let protection = choose_key_protection(f.key_protection, &key_path, name)?;
    // Issue into a staging dir, not the canonical location: `apply()`
    // is the only thing that should write under the config tree. The
    // returned guard keeps the staging files alive until apply() copies
    // them into place, then drops (cleaning the tempdir up).
    let staging = tempfile::TempDir::new().context("creating tls staging dir")?;
    println!("issuing resolver certificate '{name}' (this may take a moment)...");
    let issued = ca::issue_identity(
        &ca,
        name,
        staging.path().to_path_buf(),
        protection.password(),
    )?;
    // The sealed password rides beside the staged key; apply()'s
    // identity install copies sidecars with their keys.
    protection.write_sidecar(&issued.private_key)?;
    println!("issued resolver certificate:");
    println!("  name:        {name}");
    // Show the *installed* paths apply() will create, not the transient
    // staging paths the files currently sit in.
    println!("  certificate: {}", identity_dir.join("certificate.pem").display());
    println!("  private key: {}", identity_dir.join("private.key").display());
    println!("  trusted CA:  {}", ca_cert.display());
    match &protection {
        KeyProtection::Sealed { .. } => {
            println!(
                "  private key is encrypted; the password is sealed to this \
                 machine's {} beside it.",
                netidx_tpm::MECHANISM
            );
        }
        KeyProtection::Password { .. } => {
            println!(
                "  private key is encrypted; password saved to the system keychain."
            );
        }
        KeyProtection::None => (),
    }
    Ok(ResolvedAuth {
        choice: AuthChoice::Tls {
            name: ArcStr::from(name),
            certificate: issued.certificate,
            private_key: issued.private_key,
            trusted: ca_cert,
            askpass: protection.askpass(),
        },
        staging: Some(staging),
        netidx_ca: true,
    })
}

/// The auth scheme a discovered network's resolvers use (the first
/// resolver's — netidx allows per-member schemes but mixed clusters
/// are vanishingly rare). `None` when the network reported no
/// resolvers; the caller falls back to prompting.
fn network_auth_kind(net: &DiscoveredNetwork) -> Option<AuthKind> {
    net.info.resolvers.first().map(|r| match &r.auth {
        InfoAuth::Anonymous => AuthKind::Anonymous,
        InfoAuth::Krb5 { .. } => AuthKind::Krb5,
        InfoAuth::Tls { .. } => AuthKind::Tls,
    })
}

/// Resolve this resolver's own auth by importing from a confirmed
/// network: TLS ⇒ request our resolver cert from the network's CA
/// (suggested `resolver.<domain>`, identity already glyph-confirmed);
/// krb5 ⇒ prompt for this host's SPN (a peer's is shown as the shape
/// to follow); anonymous ⇒ anonymous.
fn resolver_auth_from_network(
    f: &ResolverFlags,
    net: &DiscoveredNetwork,
    kind: AuthKind,
) -> Result<ResolvedAuth> {
    match kind {
        AuthKind::Anonymous => Ok(ResolvedAuth::external(AuthChoice::Anonymous)),
        AuthKind::Local => bail!(
            "a network-discovered resolver cannot use local auth (it is \
             host-local by definition)"
        ),
        AuthKind::Krb5 => {
            if let Some(example) = net.info.resolvers.iter().find_map(|r| match &r.auth {
                InfoAuth::Krb5 { spn } => Some(spn.as_str()),
                _ => None,
            }) {
                println!(
                    "note: an existing resolver on this network uses SPN {example:?}"
                );
            }
            Ok(ResolvedAuth::external(AuthChoice::Krb5 {
                spn: ArcStr::from(
                    prompt::required_string(
                        "kerberos SPN for this resolver",
                        f.spn.clone(),
                    )?
                    .as_str(),
                ),
            }))
        }
        AuthKind::Tls => {
            let Some(ca_addr) = net.info.ca_addr else {
                bail!(
                    "network {:?} uses TLS but none of its conf servers \
                     reported a CA — cannot obtain the resolver certificate",
                    net.identity.domain,
                )
            };
            let suggested = match &f.tls_name {
                Some(n) => n.clone(),
                None => format!("{DEFAULT_RESOLVER_NAME}.{}", net.identity.domain),
            };
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            let (j, staging) = join_network(
                &rt,
                ca_addr,
                NodeKind::Resolver,
                Some(&suggested),
                f.key_protection,
                &net.identity,
            )?;
            Ok(ResolvedAuth {
                choice: joined_to_auth(j),
                staging: Some(staging),
                netidx_ca: true,
            })
        }
    }
}

/// The resolver install's post-apply conf-server step. Three cases:
/// (1) joining an existing network whose CA we do NOT hold ⇒
/// [`enroll_conf_server`] (a brand new conf server here, serving cert minted
/// by the network's CA). (2) A fresh network we just created, OR a
/// "discovered" network whose CA *this host already holds* (it ran `ca init`
/// and is now adding a resolver) ⇒ the ca-role `conf-server.json` already
/// exists; merge this host's resolver / id-map roles into it, preserving the
/// `ca` role. (3) No config at all ⇒ the operator declined a conf server —
/// nothing to do.
#[cfg(unix)]
fn post_apply_conf_server(
    discovered: Option<&DiscoveredNetwork>,
    kind: AuthKind,
    no_conf_server: bool,
    resolver_listen: SocketAddr,
    units_dir: Option<&Path>,
    resolver_config: PathBuf,
    id_map: Option<PathBuf>,
) -> Result<()> {
    match discovered {
        // A "discovered" network whose CA this host already holds is our OWN
        // network: this host bootstrapped the CA (`ca init`, or an earlier
        // install) and is now adding a resolver. It already serves the conf
        // plane with the `ca` role, so it must MERGE the new resolver/id-map
        // roles into that config — never enroll a fresh conf server, whose
        // join-shape config drops the `ca` role and silently disables signing
        // (observed in the lab). Same handling as the fresh-network arm below.
        Some(net) if host_holds_ca(net) => {
            match conf_plane_decision(kind, no_conf_server) {
                // Honor an explicit `--no-conf-server` (and Local auth) the same way
                // the enroll arm does — don't advertise this resolver — even though
                // the conf server itself keeps running here (it's the CA).
                ConfPlane::Skip => Ok(()),
                _ => merge_resolver_roles(resolver_config, id_map),
            }
        }
        Some(net) => enroll_conf_server(
            net,
            kind,
            no_conf_server,
            resolver_listen,
            units_dir,
            resolver_config,
            id_map,
        ),
        None => merge_resolver_roles(resolver_config, id_map),
    }
}

/// True when this host already holds the CA for the just-discovered network —
/// i.e. the network is our own (this host ran `ca init`, or created the CA on
/// an earlier install). We compare the local CA cert's fingerprint against the
/// discovered identity so we only short-circuit for genuinely our own CA, never
/// a different network that merely happens to be reachable on the wire.
#[cfg(unix)]
fn host_holds_ca(net: &DiscoveredNetwork) -> bool {
    use netidx_conf::fingerprint::Fingerprint;
    if !ca::default_ca_present() {
        return false;
    }
    let Ok(ca_dir) = paths::user_ca_dir() else {
        return false;
    };
    let Ok(pem) = std::fs::read(ca_dir.join("certificate.pem")) else {
        return false;
    };
    matches!(Fingerprint::of_cert_pem(&pem), Ok(fp) if fp == net.identity.fingerprint)
}

/// Merge this host's resolver / id-map roles into the existing
/// `conf-server.json`, preserving every other role (notably `ca`). Used both
/// when there's no discovered network (a fresh network we just created) and
/// when the discovered network is our own CA host. No existing config ⇒ the
/// operator declined a conf server here, so there's nothing to update.
#[cfg(unix)]
fn merge_resolver_roles(resolver_config: PathBuf, id_map: Option<PathBuf>) -> Result<()> {
    use netidx_conf::conf_server_config::{IdMapRole, ResolverRole};
    if paths::discover_conf_server_config().is_err() {
        return Ok(());
    }
    let path = super::server::update_roles(|roles| {
        roles.resolver = Some(ResolverRole { config: resolver_config });
        if let Some(map) = id_map {
            roles.id_map = Some(IdMapRole { map });
        }
    })?;
    println!("updated conf-server roles in {}", path.display());
    Ok(())
}

/// Enroll a conf server on this (non-CA) host: the network's CA signs
/// our reserved-SAN serving cert (admin-authorized, policy-gated), we
/// install the serving identity + `conf-server.json` with this host's
/// roles, and drop the activation unit. The CA records us as a peer as
/// a side effect of the enrollment.
///
/// An admin at this machine authorizes synchronously with their
/// password; otherwise the enrollment **queues** for remote approval
/// under the same request-code ceremony as a cert join (the approving
/// admin needs `may_enroll_servers`). A denied or expired enrollment
/// is a note, not a failure — the resolver this install produced
/// works; it just isn't advertised to discovery from this host.
#[cfg(unix)]
fn enroll_conf_server(
    net: &DiscoveredNetwork,
    kind: AuthKind,
    no_conf_server: bool,
    resolver_listen: SocketAddr,
    units_dir: Option<&Path>,
    resolver_config: PathBuf,
    id_map: Option<PathBuf>,
) -> Result<()> {
    use netidx_conf::conf_server_config::{
        ConfServerConfig, IdMapRole, ResolverRole, Roles,
    };
    let Some(ca_addr) = net.info.ca_addr else {
        println!(
            "note: network {:?} reported no CA; skipping conf-server setup on \
             this host",
            net.identity.domain,
        );
        return Ok(());
    };
    match conf_plane_decision(kind, no_conf_server) {
        ConfPlane::Skip => return Ok(()),
        ConfPlane::Mandatory => println!(
            "enrolling a conf server on this host — it advertises this \
             resolver to future installs and renews its certificates. \
             (expert opt-out: --no-conf-server)"
        ),
        ConfPlane::Ask => {
            if !prompt::confirm(
                "set up a conf server on this host (advertises this resolver \
                 to future installs)?",
                true,
            )? {
                println!(
                    "note: skipped — discovery only sees hosts running a conf \
                     server, so future installs won't learn about this resolver \
                     from this host"
                );
                return Ok(());
            }
        }
    }
    let ip = prompt::parsed_with_default::<IpAddr>(
        "conf server listen IP",
        None,
        &resolver_listen.ip().to_string(),
    )?;
    let port = prompt::parsed_with_default::<u16>(
        "conf server listen port",
        None,
        &conf_proto::DEFAULT_PORT.to_string(),
    )?;
    let listen = SocketAddr::new(ip, port);
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let admin_here = prompt::confirm(
        "is a CA admin at this machine to enter their password now? \
         (No: queue the enrollment for remote approval)",
        false,
    )?;
    let issued = if admin_here {
        let admin = prompt::required_string(
            "CA admin name (authorizes enrolling this conf server)",
            None,
        )?;
        let password = Zeroizing::new(rpassword::prompt_password(format!(
            "CA password for admin {admin}: "
        ))?);
        rt.block_on(conf_client::enroll(
            ca_addr,
            &admin,
            password,
            listen,
            &net.identity,
        ))?
    } else {
        let pending =
            rt.block_on(conf_client::enqueue_enroll(ca_addr, listen, &net.identity))?;
        println!("enrollment queued. Your request code is:");
        println!("  SHA256  {}", pending.fingerprint.text());
        println!("{}", pending.fingerprint.identicon(ColorMode::detect()));
        println!(
            "send this code to your CA admin (chat, phone — any channel you \
             trust); they approve with `netidx conf ca approve` (their policy \
             must grant may_enroll_servers). Waiting for approval (Ctrl-C to \
             abort; the request expires on its own)..."
        );
        loop {
            std::thread::sleep(POLL_INTERVAL);
            match rt.block_on(conf_client::poll(
                ca_addr,
                NodeKind::ConfServer,
                &pending,
                &net.identity,
            ))? {
                conf_client::PollOutcome::Pending => continue,
                conf_client::PollOutcome::Issued(issued) => break issued,
                conf_client::PollOutcome::Denied(reason) => {
                    println!(
                        "note: the CA admin denied the enrollment ({reason}); \
                         this resolver works, but won't be advertised to \
                         future installs from this host"
                    );
                    return Ok(());
                }
                conf_client::PollOutcome::Expired => {
                    println!(
                        "note: the enrollment request expired before an admin \
                         approved it; this resolver works, but won't be \
                         advertised to future installs from this host. Re-run \
                         the install to queue a new enrollment."
                    );
                    return Ok(());
                }
            }
        }
    };
    for w in &issued.warnings {
        println!("  warning: {w}");
    }
    // Serving identity: chain = [issued leaf, confirmed CA] so clients
    // receive the CA cert at the end of the chain, exactly like the CA
    // host's own conf server.
    let dir = paths::user_config_root()?.join("conf-server");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let mut chain = issued.cert_pem.clone().into_bytes();
    chain.extend_from_slice(net.identity.ca_pem().as_bytes());
    let serving_cert = dir.join("cert.pem");
    let serving_key = dir.join("key.pem");
    let trusted = dir.join("trusted.pem");
    netidx_conf::atomic::write_atomic(&serving_cert, &chain, 0o644)?;
    match netidx_conf::tls::write_private_key_maybe_sealed(
        &serving_key,
        &issued.private_key_pem,
    )? {
        netidx_conf::tls::KeyWrite::Sealed => {
            println!("  serving key sealed to this machine's {}", netidx_tpm::MECHANISM);
        }
        netidx_conf::tls::KeyWrite::Plain(e) => {
            println!(
                "  note: serving key is plaintext ({} sealing unavailable: {e:#})",
                netidx_tpm::MECHANISM
            );
        }
    }
    netidx_conf::atomic::write_atomic(&trusted, issued.trusted_pem.as_bytes(), 0o644)?;
    let cfg = ConfServerConfig {
        domain: net.identity.domain.clone(),
        listen,
        serving_cert,
        serving_key,
        trusted,
        roles: Roles {
            ca: None,
            resolver: Some(ResolverRole { config: resolver_config }),
            id_map: id_map.map(|map| IdMapRole { map }),
        },
        ca_addr: Some(ca_addr),
        peers: net.info.reached.clone(),
        mdns: true,
        activation_units_dir: None,
    };
    let cfg_path = paths::user_conf_server_config()?;
    cfg.save(&cfg_path)?;
    println!("conf server configured:");
    println!("  config:   {}", cfg_path.display());
    println!("  listen:   {listen}");
    println!("  domain:   {}", net.identity.domain);
    if let Some(units_dir) = units_dir {
        super::server::install_unit(units_dir, &cfg_path)?;
    } else {
        println!(
            "  (--no-units: no activation unit written; run it yourself with\n\
             \x20  netidx conf component server run -c {})",
            cfg_path.display()
        );
    }
    Ok(())
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
    /// Resolver's Kerberos SPN (with `--auth krb5`), e.g.
    /// `netidx/resolver.example.com@REALM`.
    #[arg(long = "spn")]
    spn: Option<String>,
    /// Resolver's local-auth socket path (with `--auth local`).
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
    /// Where to drop the renewal-daemon activation unit (installed for
    /// TLS setups — certificates expire and nobody should have to
    /// remember that). Defaults to the user activation dir.
    #[arg(long = "units-dir")]
    units_dir: Option<PathBuf>,
    /// How new private keys are protected at rest: `seal` (bind to
    /// this machine's TPM / Secure Enclave), `password` (typed;
    /// interactive only), or `none`. Default: seal when the host has
    /// usable sealing hardware (prompted on a TTY), else none.
    #[arg(long = "key-protection")]
    key_protection: Option<KeyProtArg>,
    #[command(flatten)]
    common: CommonFlags,
}

pub(crate) fn run_publisher(mut f: PublisherFlags) -> Result<()> {
    let mut tls_identities = vec![];
    // Holds the staging tempdir(s) for any conf-server-joined identity
    // until `finish` (which runs the template's --force-gated install)
    // returns. Dropping a TempDir deletes its contents, so this must
    // outlive the `finish` call below.
    let mut tls_staging: Vec<tempfile::TempDir> = Vec::new();
    if let Some(spec) = f.tls.to_spec()? {
        tls_identities.push(spec);
    }
    // Ask the network before asking the human: with no `--addr` /
    // `--auth`, a discovered (and glyph-confirmed) conf server yields
    // every resolver address with its auth — and, on TLS networks, our
    // client cert. The probe outcome rides into the manual cascade so
    // a declined discovery is never re-offered.
    let probe = if f.addrs.is_empty() && f.auth.is_none() {
        discover_network(NodeKind::Publisher)?
    } else {
        ConfServers::NotProbed
    };
    let addrs: Vec<(SocketAddr, ReferralAuth)> = match probe.have() {
        Some(net) => {
            let have_identity = !tls_identities.is_empty();
            network_addrs_and_identity(
                net,
                NodeKind::Publisher,
                have_identity,
                f.key_protection,
                &mut tls_identities,
                &mut tls_staging,
            )?
        }
        None => {
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
            if tls_identities.is_empty() && matches!(f.auth, Some(AuthKind::Tls)) {
                // Interactive TLS path: `--auth tls` without `--tls-cert`
                // (etc.) used to dead-end at the template's
                // "default_auth=Tls requires at least one tls_identity"
                // check. Mirror the workstation/resolver UX instead — walk
                // the operator through a cert-or-generate cascade so they
                // can either point at an existing cert or get a key+CSR
                // produced on the spot.
                // Suggest our SAN as `<user>.<domain>`, the domain coming
                // from the resolver's TLS name the operator just gave.
                let suggested = match &per_addr_auth {
                    ReferralAuth::Tls(san) => suggest_client_san(san),
                    _ => None,
                };
                let si = prompt_tls_client_identity(
                    suggested.as_deref(),
                    f.key_protection,
                    &probe,
                )?;
                tls_identities.push(si.spec);
                tls_staging.extend(si.staging);
            }
            f.addrs.iter().map(|a| (*a, per_addr_auth.clone())).collect()
        }
    };
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
    // TLS publishers get the renewal daemon (the one daemon a
    // "client-only" host runs — certificates expire); everything else
    // stays service-free.
    let has_tls = !tls_identities.is_empty();
    let units_dir = if has_tls {
        resolve_units_dir(&f.common, f.units_dir.as_deref())?
    } else {
        None
    };
    let need = if units_dir.is_some() {
        // A publisher is typically a headless (often cloud) host, so a
        // system service that starts at boot — no login session needed —
        // is the right default, like the resolver.
        service::ServiceNeed::at(service::ScopeArg::System)
    } else {
        service::ServiceNeed::NONE
    };
    let (network, conf_server) = network_provenance(&probe);
    let record = InstallRecord::new(
        InstallRole::Publisher,
        f.base.clone(),
        f.auth.map(|k| k.as_str()).unwrap_or("tls"),
        network,
        conf_server,
    );
    let params = netidx_conf::template::publisher::PublisherParams {
        addrs,
        default_auth,
        tls_identities,
        base: ArcStr::from(f.base),
        config_path: f.config_path,
        default_bind_config,
    };
    let rt = template::publisher(&params)?;
    finish_with(rt, &f.common, need, record, move || match &units_dir {
        Some(d) => install_renew_unit(d),
        None => Ok(()),
    })
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
            prompt::required_string(
                "resolver's kerberos SPN (e.g. netidx/resolver.example.com@REALM)",
                f.spn.clone(),
            )?
            .as_str(),
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

/// Map this resolver's chosen data-plane auth to the `InfoAuth` the
/// delegation handshake exchanges (the child's address carries it). Local
/// auth is host-local and can't serve a delegated network subtree.
#[cfg(unix)]
fn authchoice_to_info(a: &AuthChoice) -> Result<netidx_conf::conf_proto::InfoAuth> {
    use netidx_conf::conf_proto::InfoAuth;
    match a {
        AuthChoice::Anonymous => Ok(InfoAuth::Anonymous),
        AuthChoice::Krb5 { spn } => Ok(InfoAuth::Krb5 { spn: spn.to_string() }),
        AuthChoice::Tls { name, .. } => Ok(InfoAuth::Tls { name: name.to_string() }),
        AuthChoice::Local { .. } => bail!(
            "a local-auth resolver can't be delegated a network subtree \
             (its auth is host-local)"
        ),
    }
}

/// Extract install provenance from a network probe: the glyph-confirmed
/// network identity (domain + CA fingerprint) to pin later lifecycle ops
/// to, and a reachable conf-server address to start from. `(None, None)`
/// when the install didn't join a *discovered* network — a CLI-flag
/// parent, the manual prompt cascade, or no parent at all carry no
/// confirmed identity, so they record none and a later `join` supplies
/// it.
fn network_provenance(
    probe: &ConfServers,
) -> (Option<NetworkIdentity>, Option<SocketAddr>) {
    match probe.have() {
        Some(net) => {
            let id = NetworkIdentity::new(
                net.identity.domain.clone(),
                &net.identity.fingerprint,
            );
            (Some(id), net.info.reached.first().copied())
        }
        None => (None, None),
    }
}

/// Describe + apply the rendered template, with a post-apply step that
/// runs after it has been installed (and never on `--dry-run`). The
/// resolver install uses the step to stand up / update this host's
/// conf server, which points at config files that only exist once
/// `apply()` has run.
fn finish_with(
    rt: RenderedTemplate,
    common: &CommonFlags,
    need: service::ServiceNeed,
    record: InstallRecord,
    post_apply: impl FnOnce() -> Result<()>,
) -> Result<()> {
    println!("{}", rt.describe());
    if !common.dry_run {
        check_no_overwrite(&rt, common.force)?;
        rt.apply().context("applying template")?;
        println!("ok");
        post_apply()?;
        // Record what we installed and the network it joined
        // (identity-pinned), so lifecycle ops (`status`/`update`) know
        // what this host is and can re-pin to the same CA before
        // trusting a conf server's picture of the network.
        record.save_default().context("writing the install record")?;
    }
    // Single end-of-process hook: offer the OS service (or print the
    // dry-run note). Sub-steps with their own units merge their needs
    // into `need` before we get here, so this fires exactly once.
    service::offer(
        need,
        service::ServiceGate {
            dry_run: common.dry_run,
            no_service: common.no_service,
            with_service: common.with_service,
        },
    )
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
            warnings: Vec::new(),
        }
    }

    // The install-profile matrix, exhaustively. This test and
    // design/conf-server.md (Install profiles) mirror
    // `conf_plane_decision`; change all three together.
    #[cfg(unix)]
    #[test]
    fn the_conf_plane_matrix() {
        use AuthKind::*;
        use ConfPlane::*;
        // Declining the conf plane on a TLS or krb5 network breaks
        // renewal + zero-touch installs forever, so neither is a
        // question — fresh network or joining one (enrollment queues
        // for remote approval, so no admin is needed at this
        // keyboard). Anonymous networks may not want the machinery;
        // local auth has nothing to discover.
        for (kind, want) in
            [(Tls, Mandatory), (Krb5, Mandatory), (Anonymous, Ask), (Local, Skip)]
        {
            assert_eq!(conf_plane_decision(kind, false), want, "{kind:?}");
        }
        // The expert opt-out beats everything.
        for kind in [Tls, Krb5, Anonymous, Local] {
            assert_eq!(conf_plane_decision(kind, true), Skip);
        }
    }

    // TLS gets the id-mapper unconditionally (no prompt — in test
    // builds stdin_is_tty() is pinned false, so a prompt would flip to
    // its default and hide a regression here); krb5 falls to its
    // default `platform` choice; anonymous and local don't map through
    // the daemon. `--no-id-map` forces platform.
    #[test]
    fn id_map_choice_per_profile() {
        let tls = AuthChoice::Tls {
            name: ArcStr::from("resolver.example.com"),
            certificate: PathBuf::from("/x/cert.pem"),
            private_key: PathBuf::from("/x/key.pem"),
            trusted: PathBuf::from("/x/ca.pem"),
            askpass: None,
        };
        let krb5 = AuthChoice::Krb5 { spn: ArcStr::from("host/x@REALM") };
        let local = AuthChoice::Local { path: PathBuf::from("/x/sock") };
        assert_eq!(resolve_id_map_choice(&tls, false).unwrap(), IdMapMode::Netidx);
        assert_eq!(resolve_id_map_choice(&tls, true).unwrap(), IdMapMode::Platform);
        assert_eq!(resolve_id_map_choice(&krb5, false).unwrap(), IdMapMode::Platform);
        assert_eq!(
            resolve_id_map_choice(&AuthChoice::Anonymous, false).unwrap(),
            IdMapMode::Platform
        );
        assert_eq!(resolve_id_map_choice(&local, false).unwrap(), IdMapMode::Platform);
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
