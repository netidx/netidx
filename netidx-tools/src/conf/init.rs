//! `netidx conf install <type>` — render and apply a `RenderedTemplate`
//! for one of the three v1 templates.

use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::config::DefaultAuthMech;
use netidx_conf::{
    paths,
    template::{
        self, AuthChoice, ParentRef, ReferralAuth, RenderedTemplate, TlsIdentitySpec,
    },
};
// `tls::identity_dir` is referenced only by the unix-gated TLS cert
// generation flows (`generate_and_wait_for_parent_cert`,
// `resolver_tls_generate`); on Windows the import would be unused.
#[cfg(unix)]
use netidx_conf::tls;
use std::{
    io::IsTerminal,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
};
use structopt::StructOpt;

// `ca` submodule depends on netidx_conf::ca which is unix-only.
#[cfg(unix)]
use super::ca;
use super::{cloud, prompt, service};

#[derive(StructOpt, Debug)]
pub(crate) enum Params {
    #[structopt(
        name = "workstation",
        about = "local-auth resolver + matching client"
    )]
    Workstation(WorkstationFlags),
    #[structopt(
        name = "resolver",
        about = "single network-facing resolver-server"
    )]
    Resolver(ResolverFlags),
    #[structopt(
        name = "publisher",
        about = "publisher-host config pointing at a remote cluster"
    )]
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

#[derive(StructOpt, Debug, Clone)]
struct TlsIdentityFlags {
    /// Source path of our certificate.
    #[structopt(long = "tls-cert")]
    cert: Option<PathBuf>,
    /// Source path of our private key.
    #[structopt(long = "tls-key")]
    key: Option<PathBuf>,
    /// Source path of the trusted-CA bundle.
    #[structopt(long = "tls-trusted")]
    trusted: Option<PathBuf>,
    /// Our SAN — drives the install subdirectory under
    /// `~/.config/netidx/tls/<our-name>/`.
    #[structopt(long = "tls-our-name")]
    our_name: Option<String>,
    /// Server domain pattern this identity covers — the key in
    /// `tls.identities`. Closest reverse-domain match wins.
    #[structopt(long = "tls-server-pattern")]
    server_pattern: Option<String>,
}

impl TlsIdentityFlags {
    /// Return `Some(spec)` if all five flags are populated, `None` if
    /// none are, error if partially populated.
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
        let our_name =
            self.our_name.as_ref().context("--tls-our-name required")?.clone();
        let server_pattern = self
            .server_pattern
            .as_ref()
            .context("--tls-server-pattern required")?
            .clone();
        Ok(Some(TlsIdentitySpec {
            server_pattern: ArcStr::from(server_pattern),
            our_name: ArcStr::from(our_name),
            certificate: cert,
            private_key: key,
            trusted,
            dest_dir: None,
        }))
    }
}

#[derive(StructOpt, Debug, Clone)]
struct ParentFlags {
    /// Parent referral address. Required to enable a parent.
    /// Currently supports a single address; for multiple addresses,
    /// edit the generated resolver.json.
    #[structopt(long = "parent-addr")]
    parent_addr: Option<SocketAddr>,
    /// Auth scheme for the parent. Required when `--parent-addr` is set.
    #[structopt(long = "parent-auth")]
    parent_auth: Option<AuthKind>,
    /// Parent's Kerberos SPN (with `--parent-auth krb5`).
    #[structopt(long = "parent-spn")]
    parent_spn: Option<String>,
    /// Parent's local-auth socket path (with `--parent-auth local`).
    #[structopt(long = "parent-socket")]
    parent_socket: Option<PathBuf>,
    /// Parent's TLS server name (with `--parent-auth tls`).
    #[structopt(long = "parent-tls-name")]
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
    #[structopt(long = "parent-path")]
    parent_path: Option<String>,
    /// TTL in seconds.
    #[structopt(long = "parent-ttl")]
    parent_ttl: Option<u16>,
}

impl ParentFlags {
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
                if self.parent_auth.is_some()
                    || self.parent_spn.is_some()
                    || self.parent_socket.is_some()
                    || self.parent_tls_name.is_some()
                    || self.parent_ttl.is_some()
                    || self.parent_path.is_some()
                {
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

#[derive(StructOpt, Debug, Clone)]
struct CommonFlags {
    /// Print the plan and exit without writing anything.
    #[structopt(long = "dry-run")]
    dry_run: bool,
    /// Overwrite existing config files. Without this, `install`
    /// errors if any target path is non-empty.
    #[structopt(long = "force")]
    force: bool,
    /// Don't drop activation unit files.
    #[structopt(long = "no-units")]
    no_units: bool,
    /// After the templated install succeeds, also register netidx as
    /// an OS service. Default (on a TTY) is to prompt; pass this to
    /// install non-interactively. Mutually exclusive with
    /// `--no-service`.
    #[structopt(long = "with-service", conflicts_with = "no_service")]
    with_service: bool,
    /// Skip the post-install service prompt. Default (on a TTY) is
    /// to prompt; pass this to suppress the prompt entirely.
    #[structopt(long = "no-service")]
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
    let mut existing: Vec<&Path> = Vec::new();
    if let Some((p, _)) = &rt.client_config
        && p.exists()
    {
        existing.push(p);
    }
    if let Some((p, _)) = &rt.resolver_config
        && p.exists()
    {
        existing.push(p);
    }
    if let Some((p, _)) = &rt.perms_file
        && p.exists()
    {
        existing.push(p);
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

#[derive(StructOpt, Debug)]
pub(crate) struct WorkstationFlags {
    #[structopt(flatten)]
    parent: ParentFlags,
    #[structopt(flatten)]
    tls: TlsIdentityFlags,
    /// `default_auth` on the client config. Defaults to `local`.
    /// Override only when the workstation hosts publishers that
    /// network subscribers must reach.
    #[structopt(long = "default-auth")]
    default_auth: Option<AuthKind>,
    /// Base path of the local resolver cluster (default `/local`).
    #[structopt(long = "base", default_value = "/local")]
    base: String,
    /// Port the local resolver listens on. Default 4654 — chosen to
    /// not clash with 59200, the port the process-spawned automatic
    /// local resolver uses when no netidx config exists.
    #[structopt(long = "listen-port")]
    listen_port: Option<u16>,
    /// Local-auth unix socket path.
    #[structopt(long = "local-socket")]
    local_socket: Option<PathBuf>,
    #[structopt(long = "client-config")]
    client_config_path: Option<PathBuf>,
    #[structopt(long = "resolver-config")]
    resolver_config_path: Option<PathBuf>,
    #[structopt(long = "units-dir")]
    units_dir: Option<PathBuf>,
    #[structopt(long = "netidx-binary")]
    netidx_binary: Option<PathBuf>,
    /// Skip emitting the default `container` activation unit. By
    /// default a workstation gets both `resolver` and `container`
    /// units; pass this when you don't want a container service.
    #[structopt(long = "no-container")]
    no_container: bool,
    /// Override the perms-file owner. By default the workstation
    /// install grants `<base>` → `<current-unix-user>` → `swlpd` so
    /// the operator has full rights to the local-resolver namespace
    /// without further setup. Pass `--owner alice` to grant `alice`
    /// instead — useful when installing as root on behalf of another
    /// user. Implies `--with-perms` (and conflicts with `--no-perms`).
    #[structopt(long = "owner", conflicts_with = "no_perms")]
    owner: Option<String>,
    /// Skip the auto-seeded perms file entirely. The workstation
    /// resolver will load with an empty perms map and `Deny` every
    /// non-anonymous operation — only useful when perms are managed
    /// out-of-band.
    #[structopt(long = "no-perms")]
    no_perms: bool,
    /// Where to write the perms file. Defaults to
    /// `~/.config/netidx/perms.json` (same as the resolver template).
    #[structopt(long = "perms-path")]
    perms_path: Option<PathBuf>,
    #[structopt(flatten)]
    common: CommonFlags,
}

/// Resolve the workstation owner: explicit `--owner` first, else the
/// current Unix user via `nix::unistd`. Returns `None` only if we're
/// on a platform without `nix` (Windows) and no explicit `--owner`
/// was passed — the engine then emits an empty perms map.
#[cfg(unix)]
fn resolve_workstation_owner(provided: Option<String>) -> Option<ArcStr> {
    if let Some(s) = provided {
        return Some(ArcStr::from(s.as_str()));
    }
    let uid = nix::unistd::Uid::current();
    nix::unistd::User::from_uid(uid)
        .ok()
        .flatten()
        .map(|u| ArcStr::from(u.name.as_str()))
}

#[cfg(not(unix))]
fn resolve_workstation_owner(provided: Option<String>) -> Option<ArcStr> {
    provided.map(|s| ArcStr::from(s.as_str()))
}

fn run_workstation(f: WorkstationFlags) -> Result<()> {
    let cli_tls_id = f.tls.to_spec()?;
    let mut tls_identities = vec![];
    if let Some(spec) = cli_tls_id {
        tls_identities.push(spec);
    }
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
    let parent = if f.parent.parent_addr.is_some() {
        f.parent.to_parent_ref(parent_default_path)?
    } else {
        match prompt_parent_referral(parent_default_path)? {
            None => None,
            Some((parent_ref, maybe_ident)) => {
                if let Some(ident) = maybe_ident {
                    tls_identities.push(ident);
                }
                Some(parent_ref)
            }
        }
    };
    let owner = if f.no_perms {
        None
    } else {
        resolve_workstation_owner(f.owner.clone())
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
    finish(rt, &f.common, Some(service::ScopeArg::User))
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
) -> Result<Option<(ParentRef, Option<TlsIdentitySpec>)>> {
    let addr: SocketAddr = match prompt::optional_parsed::<SocketAddr>(
        "network-wide resolver address (ip:port)",
        None,
    )? {
        Some(a) => a,
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
            let server_name =
                prompt::required_string("parent TLS server name", None)?;
            // identity is required for TLS — either bring one or
            // (the generate path diverges via `bail!`)
            let ident = prompt_parent_tls_identity(&server_name)?;
            (ReferralAuth::Tls(ArcStr::from(server_name.as_str())), Some(ident))
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

/// TLS sub-cascade for the parent prompt. Returns the identity to
/// install + reference from the client config. For the "generate"
/// path this generates a key + CSR, prints instructions, then waits
/// for the operator to confirm they've placed the signed cert and
/// trusted-CA bundle at the canonical install location — only then
/// does the install proceed, so by the time the activation server
/// starts the cert files are guaranteed to be on disk.
fn prompt_parent_tls_identity(parent_server_name: &str) -> Result<TlsIdentitySpec> {
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
    let (our_name, certificate, private_key, trusted) = if is_generate {
        #[cfg(unix)]
        {
            generate_and_wait_for_parent_cert()?
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
        let our_name = prompt::required_string(
            "your TLS identity name (cert SAN; installed at ~/.config/netidx/tls/<name>/)",
            None,
        )?;
        (our_name, certificate, private_key, trusted)
    };
    // The default reverse-domain key is the parent's TLS name
    // unaltered — netidx's match is "closest reverse-domain prefix
    // wins", so the exact name is the most specific possible entry.
    // The operator can broaden it later by editing client.json.
    let server_pattern = prompt::string_with_default(
        "server pattern (reverse-domain key in tls.identities)",
        None,
        parent_server_name,
    )?;
    Ok(TlsIdentitySpec {
        server_pattern: ArcStr::from(server_pattern.as_str()),
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
) -> Result<(String, PathBuf, PathBuf, PathBuf)> {
    let our_name = prompt::required_string(
        "your TLS identity name (CN for the CSR; cert SAN)",
        None,
    )?;
    let dest_dir = tls::identity_dir(&our_name)?;
    std::fs::create_dir_all(&dest_dir).with_context(|| {
        format!("creating identity dir {}", dest_dir.display())
    })?;
    let key_path = dest_dir.join("private.key");
    let cert_path = dest_dir.join("certificate.pem");
    let trusted_path = dest_dir.join("trusted.pem");
    // CSR lives in CWD — same convention as `netidx conf ca request`.
    let csr_path = super::ca::default_csr_filename(&our_name);

    if key_path.exists() {
        bail!(
            "private key already exists at {} — refusing to overwrite. \
             Move it aside, or re-run with explicit --tls-cert / \
             --tls-key / --tls-trusted to point at an existing identity.",
            key_path.display(),
        );
    }
    let kr = netidx_conf::ca::generate_csr(
        &netidx_conf::ca::Subject::cn(our_name.clone()),
        &[netidx_conf::ca::SanEntry::Dns(our_name.clone())],
        2048,
    )
    .context("generating private key + CSR")?;
    netidx_conf::atomic::write_atomic(&key_path, &kr.private_key_pem, 0o600)
        .with_context(|| format!("writing private key to {}", key_path.display()))?;
    netidx_conf::atomic::write_atomic(&csr_path, &kr.csr_pem, 0o644)
        .with_context(|| format!("writing CSR to {}", csr_path.display()))?;

    println!();
    println!("Generated TLS identity '{}':", our_name);
    println!("  private key (0600): {}", key_path.display());
    println!("  CSR         (0644): {}", csr_path.display());
    println!();
    println!("Next steps:");
    println!("  1. Send {} to your CA admin to sign.", csr_path.display());
    println!("  2. Place the signed certificate at:");
    println!("       {}", cert_path.display());
    println!("  3. Place the trusted-CA bundle (the cert that signs the parent's");
    println!("     cert; usually the upstream CA's certificate.pem) at:");
    println!("       {}", trusted_path.display());
    println!();

    wait_for_cert_files(&cert_path, &trusted_path)?;
    Ok((our_name, cert_path, key_path, trusted_path))
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

/// How a candidate interface address ranks as the resolver's
/// advertised address. Loopback isn't a variant — it's the
/// fallback when nothing classifies, so [`classify`] returns `None`
/// for it (and for anything else that can't be advertised:
/// link-local, multicast, broadcast, unspecified).
#[derive(Debug, PartialEq, Eq)]
enum AddrClass {
    Public,
    Private,
}

/// Classify an IPv4 interface address for use as the resolver's
/// advertised address. `None` means "don't suggest this one" —
/// loopback (the final fallback, handled by [`pick_advertised_ip`]),
/// link-local, multicast, broadcast, and unspecified all return
/// `None`.
///
/// IPv4 only: netidx's IPv6 support isn't well exercised, so the
/// tooling doesn't *volunteer* a v6 address as the default. An
/// operator who needs one can still type it at the `--listen`
/// prompt — we just don't suggest it.
fn classify(v4: Ipv4Addr) -> Option<AddrClass> {
    if v4.is_loopback()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_multicast()
        || v4.is_unspecified()
    {
        None
    } else if v4.is_private() {
        Some(AddrClass::Private)
    } else {
        // Not provably-public (the stable stdlib has no `is_global`),
        // but a non-private, non-special v4 address on a real
        // interface is public in practice.
        Some(AddrClass::Public)
    }
}

/// Pick the best routable v4 interface for advertising: first public,
/// else first private, else `None`. Returns the interface's
/// `(ip, netmask)` so callers can derive both an exact `--listen`
/// address and a subnet-shaped `BindCfg::Match`. IPv6 candidates are
/// skipped entirely (see [`classify`]).
fn pick_advertised_v4_interface(
    ifaces: &[if_addrs::Interface],
) -> Option<(Ipv4Addr, Ipv4Addr)> {
    let mut first_private: Option<(Ipv4Addr, Ipv4Addr)> = None;
    for i in ifaces {
        let if_addrs::IfAddr::V4(v4) = &i.addr else { continue };
        match classify(v4.ip) {
            Some(AddrClass::Public) => return Some((v4.ip, v4.netmask)),
            Some(AddrClass::Private) => {
                first_private.get_or_insert((v4.ip, v4.netmask));
            }
            None => {}
        }
    }
    first_private
}

/// Network shape of the host running `conf install`, used to drive
/// the `--listen` / `--bind` defaults for the resolver and publisher
/// templates. Computed once per CLI invocation; the resolver and
/// publisher prompts both consume it so the suggested address(es)
/// stay consistent across the two flows.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum NetShape {
    /// At least one routable public IPv4 interface — advertise it
    /// directly, no NAT trickery.
    Public { ip: Ipv4Addr, netmask: Ipv4Addr },
    /// Cloud VM: the local NIC is private, but the cloud metadata
    /// service told us the public IP that NAT routes onto it.
    /// Publishers need `BindCfg::Elastic` (`<public>@<private>/<n>`);
    /// the resolver needs `addr=<public>`, `bind_addr=<private>`.
    CloudElastic {
        public: Ipv4Addr,
        private: Ipv4Addr,
        netmask: Ipv4Addr,
    },
    /// Container with no public-IP hint: we have a private NIC but
    /// no `NETIDX_PUBLIC_IP` env var, no reachable cloud metadata,
    /// and `/.dockerenv` / cgroup markers tell us we're in a
    /// container. The bind suggestion is a `<PUBLIC_IP>` placeholder
    /// the operator must fill in — silently emitting just the
    /// container subnet would land traffic on the bridge-internal IP
    /// and fail to register externally.
    ContainerPrivate { private: Ipv4Addr, netmask: Ipv4Addr },
    /// Private-only host (LAN / dev box). Suggest the private IP
    /// directly; no NAT involved.
    Private { ip: Ipv4Addr, netmask: Ipv4Addr },
    /// Nothing routable; suggest loopback so the operator at least
    /// gets a valid string at the prompt.
    Loopback,
}

impl NetShape {
    /// Enumerate the local interfaces and, if the only routable v4
    /// is RFC1918, race the cloud metadata endpoints to see if a
    /// public IP NATs onto it. Best-effort: any failure walks back
    /// to the underlying private/loopback shape.
    pub(crate) fn detect() -> Self {
        let ifaces = match if_addrs::get_if_addrs() {
            Ok(i) => i,
            Err(_) => return NetShape::Loopback,
        };
        Self::from_interfaces(
            &ifaces,
            cloud::env_public_ip(),
            cloud::detect_public_ip,
            cloud::detect_container(),
        )
    }

    /// Pure version of [`Self::detect`] for unit testing — every
    /// I/O-bound input is supplied by the caller. Decision order:
    /// 1. `NETIDX_PUBLIC_IP` (explicit operator override)
    /// 2. Discovered NIC is public → advertise directly
    /// 3. Cloud metadata returns a public IP → CloudElastic
    /// 4. We're in a container → ContainerPrivate (needs hint)
    /// 5. Otherwise → bare Private
    fn from_interfaces(
        ifaces: &[if_addrs::Interface],
        env_public_ip: Option<Ipv4Addr>,
        detect_cloud: impl FnOnce() -> Option<Ipv4Addr>,
        in_container: bool,
    ) -> Self {
        let Some((ip, netmask)) = pick_advertised_v4_interface(ifaces) else {
            return NetShape::Loopback;
        };
        // Explicit env override wins outright: the operator is telling
        // us what the outside world sees, and that's authoritative.
        // When the override happens to match the NIC IP we collapse
        // to Public — emitting `54.32.224.1@54.32.224.0/24` is valid
        // but uglier than the bare subnet form.
        if let Some(public) = env_public_ip {
            return if public == ip {
                NetShape::Public { ip, netmask }
            } else {
                NetShape::CloudElastic { public, private: ip, netmask }
            };
        }
        if !ip.is_private() {
            return NetShape::Public { ip, netmask };
        }
        // Private NIC — only now is a metadata roundtrip worth it.
        if let Some(public) = detect_cloud() {
            return NetShape::CloudElastic { public, private: ip, netmask };
        }
        // No cloud metadata, but in a container — the operator must
        // supply the public IP (NETIDX_PUBLIC_IP or --bind/--listen).
        if in_container {
            return NetShape::ContainerPrivate { private: ip, netmask };
        }
        NetShape::Private { ip, netmask }
    }

    /// Suggested `<ip>` for the resolver's `--listen` (also the
    /// publisher's `--addr` if it were prompted): the address clients
    /// will actually connect to. For `ContainerPrivate` we don't
    /// *know* the external address — the private IP is the only
    /// concrete answer we have, and the CLI warns the operator
    /// alongside.
    pub(crate) fn advertised_ip(&self) -> Ipv4Addr {
        match self {
            NetShape::Public { ip, .. } => *ip,
            NetShape::CloudElastic { public, .. } => *public,
            NetShape::ContainerPrivate { private, .. } => *private,
            NetShape::Private { ip, .. } => *ip,
            NetShape::Loopback => Ipv4Addr::LOCALHOST,
        }
    }

    /// Suggested `bind_addr` for the resolver. `None` when the
    /// publisher default (=== `listen.ip()`) is already correct;
    /// `Some` only in the cloud-elastic case where the resolver
    /// advertises one IP and binds to a different one locally.
    pub(crate) fn resolver_bind_override(&self) -> Option<Ipv4Addr> {
        match self {
            NetShape::CloudElastic { private, .. } => Some(*private),
            _ => None,
        }
    }

    /// True when the suggestion is incomplete — the operator must
    /// supply something the host can't infer. The CLI prints a
    /// guidance line before the prompt in that case so the
    /// `<PUBLIC_IP>` placeholder isn't a mystery.
    pub(crate) fn needs_operator_hint(&self) -> bool {
        matches!(self, NetShape::ContainerPrivate { .. })
    }

    /// Suggested `BindCfg` string for the publisher template's
    /// `--bind` prompt. Falls back to `local` when nothing routable
    /// was found; emits a `<PUBLIC_IP>` placeholder in the container
    /// case so the operator notices the missing piece rather than
    /// shipping a config that binds to the container-internal IP.
    pub(crate) fn publisher_bind_suggestion(&self) -> String {
        match self {
            NetShape::Public { ip, netmask } | NetShape::Private { ip, netmask } => {
                let prefix = u32::from(*netmask).count_ones();
                let network = Ipv4Addr::from(u32::from(*ip) & u32::from(*netmask));
                format!("{network}/{prefix}")
            }
            NetShape::CloudElastic { public, private, netmask } => {
                // BindCfg::Elastic form: `<public>@<private-subnet>/<prefix>`.
                // Publisher binds to any local NIC on the private
                // subnet but advertises the public IP to the resolver.
                let prefix = u32::from(*netmask).count_ones();
                let network = Ipv4Addr::from(u32::from(*private) & u32::from(*netmask));
                format!("{public}@{network}/{prefix}")
            }
            NetShape::ContainerPrivate { private, netmask } => {
                let prefix = u32::from(*netmask).count_ones();
                let network = Ipv4Addr::from(u32::from(*private) & u32::from(*netmask));
                format!("<PUBLIC_IP>@{network}/{prefix}")
            }
            NetShape::Loopback => "local".to_string(),
        }
    }
}

#[derive(StructOpt, Debug)]
pub(crate) struct ResolverFlags {
    /// Auth scheme this resolver exposes (anonymous, local, krb5,
    /// tls). Prompted when omitted.
    #[structopt(long = "auth")]
    auth: Option<AuthKind>,
    /// Kerberos SPN (with `--auth krb5`).
    #[structopt(long = "spn")]
    spn: Option<String>,
    /// Local-auth socket path (with `--auth local`).
    #[structopt(long = "socket")]
    socket: Option<PathBuf>,
    /// Resolver's own TLS name (with `--auth tls`).
    #[structopt(long = "tls-name")]
    tls_name: Option<String>,
    /// Source path of the resolver's certificate, or the literal
    /// `generate` to issue one from the local CA — creating that CA
    /// first if none exists. The interactive prompt defaults to
    /// `generate`, which is the painless path for the common
    /// "resolver host is also the CA host" case.
    #[structopt(long = "tls-cert")]
    tls_cert: Option<PathBuf>,
    /// Source path of the resolver's private key (with `--auth tls`).
    /// Not needed when `--tls-cert` is `generate`.
    #[structopt(long = "tls-key")]
    tls_key: Option<PathBuf>,
    /// Source path of the trusted-CA bundle (with `--auth tls`). Not
    /// needed when `--tls-cert` is `generate` — the generating CA's
    /// own certificate becomes the trust anchor.
    #[structopt(long = "tls-trusted")]
    tls_trusted: Option<PathBuf>,
    /// The resolver's advertised address — what clients connect to.
    /// Must be a concrete address (not `0.0.0.0`). The interactive
    /// prompt suggests an IPv4 address discovered from the machine's
    /// network interfaces: the first public IPv4, else the first
    /// private-network IPv4, else loopback. IPv6 is accepted if you
    /// type it explicitly, but not suggested as the default. Use
    /// `--bind` if the socket should bind somewhere other than the
    /// advertised address.
    #[structopt(long = "listen")]
    listen: Option<SocketAddr>,
    /// Override the bind address (default: same as `listen`'s ip).
    /// This is where the socket actually binds — it *may* be
    /// `0.0.0.0` to listen on every interface even when `--listen`
    /// advertises one concrete address.
    #[structopt(long = "bind")]
    bind: Option<std::net::IpAddr>,
    /// Base path (default `/`).
    #[structopt(long = "base", default_value = "/")]
    base: String,
    /// Path to a seed perms.json. Its contents are written to
    /// `--perms-path` (or `~/.config/netidx/perms.json`). When
    /// omitted, the template auto-seeds a per-user-playground layout
    /// (full rights for `$[user]` under `/users/$[user]`, read+write
    /// for the `users` group under `/users`). Pass `--no-perms` to
    /// skip emitting a perms file entirely.
    #[structopt(long = "perms-seed")]
    perms_seed: Option<PathBuf>,
    /// Where to write the perms file. Defaults to
    /// `~/.config/netidx/perms.json`.
    #[structopt(long = "perms-path")]
    perms_path: Option<PathBuf>,
    /// Skip emitting a perms file (and the corresponding
    /// `include_permissions` reference). Use when perms are managed
    /// out-of-band by some other tool / process. Mutually exclusive
    /// with `--perms-seed`.
    #[structopt(long = "no-perms", conflicts_with = "perms_seed")]
    no_perms: bool,
    #[structopt(flatten)]
    parent: ParentFlags,
    #[structopt(long = "resolver-config")]
    resolver_config_path: Option<PathBuf>,
    #[structopt(long = "units-dir")]
    units_dir: Option<PathBuf>,
    #[structopt(long = "netidx-binary")]
    netidx_binary: Option<PathBuf>,
    /// Skip auto-installing the id-mapper daemon. By default `--auth
    /// tls` installs an `id-map.unit` and a starter `id-map.json`
    /// alongside the resolver, and wires the resolver to talk to it
    /// over a unix socket. Pass this when running under an external
    /// id mapper or with `IdMapType::Command` (default behavior for
    /// non-TLS auth is unchanged).
    #[structopt(long = "no-id-map")]
    no_id_map: bool,
    /// Override the id-map socket path (default
    /// `${dirs::config_dir}/netidx/id-map.sock`).
    #[structopt(long = "id-map-socket")]
    id_map_socket: Option<PathBuf>,
    /// Override the id-map JSON path (default
    /// `${dirs::config_dir}/netidx/id-map.json`).
    #[structopt(long = "id-map-path")]
    id_map_path: Option<PathBuf>,
    /// Skip writing a client.json pointing at this resolver. By
    /// default `install resolver` drops a local client config — for
    /// TLS auth it reuses the resolver's own cert — so commands
    /// like `netidx resolver list` work from the resolver host
    /// without extra setup. Pass this if a different client config
    /// already exists, or if the resolver host should not also be a
    /// client.
    #[structopt(long = "no-client")]
    no_client: bool,
    /// Override the client config path (default
    /// `${dirs::config_dir}/netidx/client.json`). Ignored when
    /// `--no-client` is set.
    #[structopt(long = "client-config")]
    client_config_path: Option<PathBuf>,
    #[structopt(flatten)]
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
                 env var and no reachable cloud metadata. The suggested listen \
                 address below is the container's private IP — only useful for \
                 internal traffic. Override with the externally-visible address \
                 (or set NETIDX_PUBLIC_IP / pass --listen).",
            );
        }
        let suggestion =
            SocketAddr::new(s.advertised_ip().into(), DEFAULT_RESOLVER_PORT).to_string();
        prompt::parsed_with_default(
            "advertised address (what clients connect to)",
            None,
            &suggestion,
        )?
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
    // The auth-scheme sub-args (tls cert paths, krb5 spn, local
    // socket) are level-2 prompts inside `resolver_self_auth` — so
    // defaulting `--auth` to tls walks the operator through the cert
    // paths rather than dead-ending on "--tls-name required".
    let auth = resolver_self_auth(&f)?;
    let perms_seed = match &f.perms_seed {
        Some(p) => Some(netidx_conf::perms::load_perms(p)?),
        None => None,
    };
    // `--parent-path` defaults to this resolver's base: in the
    // referral model, the parent path is the path at which **this**
    // resolver attaches in the parent's namespace, which is just
    // wherever this resolver hosts its own tree.
    let parent_default_path = f.base.clone();
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
        units_dir: resolve_units_dir(&f.common, f.units_dir.as_deref())?,
        netidx_binary: resolve_netidx_binary(f.netidx_binary)?,
        with_id_map: !f.no_id_map,
        id_map_path: f.id_map_path,
        id_map_socket: f.id_map_socket,
        with_local_client: !f.no_client,
        client_config_path: f.client_config_path,
    };
    let rt = template::resolver(&params)?;
    // A standalone resolver is a network-facing daemon — system-scope
    // is what makes it boot-triggered and visible to the OS. The
    // service-install flow will re-exec under sudo if needed.
    finish(rt, &f.common, Some(service::ScopeArg::System))
}

fn resolver_self_auth(f: &ResolverFlags) -> Result<AuthChoice> {
    // `f.auth` was resolved (with a level-1 prompt) upstream in
    // `run_resolver`; treat it as guaranteed-Some. The
    // per-scheme sub-args are level-2 prompts — once the operator
    // has chosen a scheme, the things that scheme needs are not
    // optional.
    let auth = f.auth.expect("auth resolved before resolver_self_auth");
    match auth {
        AuthKind::Anonymous => Ok(AuthChoice::Anonymous),
        AuthKind::Local => Ok(AuthChoice::Local {
            path: prompt::required_path(
                "local-auth socket path",
                f.socket.clone(),
            )?,
        }),
        AuthKind::Krb5 => Ok(AuthChoice::Krb5 {
            spn: ArcStr::from(
                prompt::required_string("kerberos SPN", f.spn.clone())?.as_str(),
            ),
        }),
        AuthKind::Tls => resolver_tls_auth(f),
    }
}

/// Resolve the resolver's TLS identity. The certificate is either an
/// explicit path the operator supplies, or the literal `generate` —
/// in which case we issue one from the local CA (creating that CA if
/// none exists). The interactive prompt defaults to `generate`: for
/// the common small-org case where the resolver host is also the CA
/// host, hitting return through the prompts gets you a working
/// self-signed setup.
fn resolver_tls_auth(f: &ResolverFlags) -> Result<AuthChoice> {
    let name =
        prompt::required_string("resolver TLS name", f.tls_name.clone())?;
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
            return resolver_tls_generate(f, &name);
        }
        #[cfg(not(unix))]
        {
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
    Ok(AuthChoice::Tls {
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
    })
}

/// Issue a resolver certificate from the local CA, creating the CA
/// first if there isn't one. Returns an [`AuthChoice::Tls`] pointing
/// at the issued files.
///
/// Under `--dry-run` this issues nothing — it prints what it would
/// do and returns the *intended* paths. `apply()` doesn't run in a
/// dry run, so those paths are never read; they exist only so the
/// template can render its plan.
#[cfg(unix)]
fn resolver_tls_generate(
    f: &ResolverFlags,
    name: &str,
) -> Result<AuthChoice> {
    let ca_dir = paths::user_ca_dir()?;
    let ca_cert = ca_dir.join("certificate.pem");
    // `identity_dir` also validates `name` (no path separators) — do
    // it up front so a bad name fails before we touch the CA.
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
        return Ok(AuthChoice::Tls {
            name: ArcStr::from(name),
            certificate: identity_dir.join("certificate.pem"),
            private_key: identity_dir.join("private.key"),
            trusted: ca_cert,
        });
    }

    let ca = if ca::default_ca_present() {
        println!("issuing from the local CA at {}", ca_dir.display());
        ca::open_default_ca()?
    } else {
        // No CA — offer to create one. Declining drops back to the
        // bring-your-own-cert path.
        if !prompt::confirm(
            &format!(
                "no CA found at {} — create a new local CA now?",
                ca_dir.display()
            ),
            true,
        )? {
            bail!(
                "cannot generate a resolver certificate without a CA; re-run \
                 with explicit --tls-cert/--tls-key/--tls-trusted, or create a \
                 CA first with `netidx conf ca init`"
            );
        }
        ca::create_default_ca()?
    };
    println!("issuing resolver certificate '{name}' (this may take a moment)...");
    let issued = ca::issue_identity(&ca, name)?;
    println!("issued resolver certificate:");
    println!("  name:        {name}");
    println!("  certificate: {}", issued.certificate.display());
    println!("  private key: {}", issued.private_key.display());
    println!("  trusted CA:  {}", ca_cert.display());
    Ok(AuthChoice::Tls {
        name: ArcStr::from(name),
        certificate: issued.certificate,
        private_key: issued.private_key,
        trusted: ca_cert,
    })
}

// -- client-only --------------------------------------------------------------

#[derive(StructOpt, Debug)]
pub(crate) struct PublisherFlags {
    /// Cluster address (repeatable). All addresses share the auth
    /// scheme; for heterogeneous setups, edit the generated JSON.
    /// Prompted (single address) when omitted.
    #[structopt(long = "addr", number_of_values = 1)]
    addrs: Vec<SocketAddr>,
    /// Auth scheme (anonymous|local|krb5|tls). Prompted when omitted.
    #[structopt(long = "auth")]
    auth: Option<AuthKind>,
    #[structopt(long = "spn")]
    spn: Option<String>,
    #[structopt(long = "socket")]
    socket: Option<PathBuf>,
    /// Server's TLS name (when `--auth tls`).
    #[structopt(long = "tls-server-name")]
    tls_server_name: Option<String>,
    #[structopt(flatten)]
    tls: TlsIdentityFlags,
    /// Override `default_auth` on the client config. None ⇒ derive
    /// from `--auth`.
    #[structopt(long = "default-auth")]
    default_auth: Option<AuthKind>,
    #[structopt(long = "base", default_value = "/")]
    base: String,
    #[structopt(long = "config")]
    config_path: Option<PathBuf>,
    /// `default_bind_config` string (e.g. `10.0.0.5/32` for an exact
    /// interface, `10.0.0.0/24` for a subnet match). Default: the
    /// first public/private IPv4 enumerated on this host, formatted
    /// as `<ip>/32`. Use `local` to bind to 127.0.0.1 (only safe when
    /// the resolver is also on loopback).
    #[structopt(long = "bind")]
    bind: Option<String>,
    #[structopt(flatten)]
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
        let addr: SocketAddr =
            prompt::required_parsed("cluster address (e.g. 10.0.0.1:4564)", None)?;
        f.addrs.push(addr);
    }
    let per_addr_auth = publisher_per_addr_auth(&f)?;
    let addrs: Vec<(SocketAddr, ReferralAuth)> =
        f.addrs.iter().map(|a| (*a, per_addr_auth.clone())).collect();
    let mut tls_identities = vec![];
    if let Some(spec) = f.tls.to_spec()? {
        tls_identities.push(spec);
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
    finish(rt, &f.common, None)
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
            prompt::required_string(
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
    service_scope: Option<service::ScopeArg>,
) -> Result<()> {
    println!("{}", rt.describe());
    if common.dry_run {
        if let Some(scope) = service_scope {
            let label = match scope {
                service::ScopeArg::User => "user-scope",
                service::ScopeArg::System => "system-scope (sudo)",
            };
            println!(
                "[dry-run] would offer to install netidx as a {label} OS service"
            );
        }
        return Ok(());
    }
    check_no_overwrite(&rt, common.force)?;
    rt.apply().context("applying template")?;
    println!("ok");
    if let Some(scope) = service_scope {
        maybe_install_service(common, scope)?;
    }
    Ok(())
}

/// Post-install hook. Resolves the three input states for the
/// service-install gate:
/// - `--no-service` → skip silently (operator opted out explicitly).
/// - `--with-service` → install unconditionally (no prompt).
/// - otherwise: prompt on a TTY (default yes), or print a note on a
///   non-TTY so the operator knows the flag exists.
fn maybe_install_service(
    common: &CommonFlags,
    scope: service::ScopeArg,
) -> Result<()> {
    if common.no_service {
        return Ok(());
    }
    let install_now = if common.with_service {
        true
    } else if std::io::stdout().is_terminal() && std::io::stdin().is_terminal() {
        let label = match scope {
            service::ScopeArg::User => "user-scope (no sudo)",
            service::ScopeArg::System => "system-scope (sudo required)",
        };
        prompt::confirm(
            &format!("install netidx as an OS service now ({label})?"),
            true,
        )?
    } else {
        eprintln!(
            "note: pass --with-service to register netidx as an OS service \
             (run `netidx conf service install` later if you prefer)"
        );
        false
    };
    if install_now {
        service::install_with_defaults(scope)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    #[test]
    fn classify_v4() {
        // Special / non-advertisable ranges → None.
        assert_eq!(classify(v4("127.0.0.1")), None); // loopback
        assert_eq!(classify(v4("169.254.3.4")), None); // link-local
        assert_eq!(classify(v4("0.0.0.0")), None); // unspecified
        assert_eq!(classify(v4("255.255.255.255")), None); // broadcast
        assert_eq!(classify(v4("224.0.0.1")), None); // multicast
        // RFC1918 private ranges.
        assert_eq!(classify(v4("10.0.0.5")), Some(AddrClass::Private));
        assert_eq!(classify(v4("172.16.9.9")), Some(AddrClass::Private));
        assert_eq!(classify(v4("192.168.1.42")), Some(AddrClass::Private));
        // Everything else on a real interface is public in practice.
        assert_eq!(classify(v4("8.8.8.8")), Some(AddrClass::Public));
        assert_eq!(classify(v4("203.0.113.7")), Some(AddrClass::Public));
    }

    fn iface(ip: &str, netmask: &str) -> if_addrs::Interface {
        if_addrs::Interface {
            name: "test".to_string(),
            addr: if_addrs::IfAddr::V4(if_addrs::Ifv4Addr {
                ip: v4(ip),
                netmask: v4(netmask),
                prefixlen: u32::from(v4(netmask)).count_ones() as u8,
                broadcast: None,
            }),
            index: None,
            oper_status: if_addrs::IfOperStatus::Up,
            is_p2p: false,
            #[cfg(windows)]
            adapter_name: "test".to_string(),
        }
    }

    #[test]
    fn pick_advertised_interface_returns_subnet() {
        // Public wins; netmask carried through unchanged.
        let ifs = [iface("192.168.1.5", "255.255.255.0"), iface("8.8.8.8", "255.255.255.252")];
        assert_eq!(
            pick_advertised_v4_interface(&ifs),
            Some((v4("8.8.8.8"), v4("255.255.255.252"))),
        );
        // No public → first private with its own netmask.
        let ifs = [iface("127.0.0.1", "255.0.0.0"), iface("10.0.0.5", "255.255.0.0")];
        assert_eq!(
            pick_advertised_v4_interface(&ifs),
            Some((v4("10.0.0.5"), v4("255.255.0.0"))),
        );
        // Nothing routable → None (the caller picks a fallback string).
        let ifs = [iface("127.0.0.1", "255.0.0.0")];
        assert_eq!(pick_advertised_v4_interface(&ifs), None);
    }

    #[test]
    fn netshape_public_skips_cloud_probe() {
        // Directly-attached public NIC → no need to ask metadata.
        // The cloud probe must NOT be called (panics if it is).
        let ifs = [iface("8.8.8.8", "255.255.255.0")];
        let shape = NetShape::from_interfaces(&ifs, None, || panic!("cloud probe ran"), false);
        assert_eq!(
            shape,
            NetShape::Public { ip: v4("8.8.8.8"), netmask: v4("255.255.255.0") },
        );
    }

    #[test]
    fn netshape_private_with_cloud_metadata_is_elastic() {
        let ifs = [iface("10.0.0.5", "255.255.255.0")];
        let shape = NetShape::from_interfaces(&ifs, None, || Some(v4("54.32.224.1")), false);
        assert_eq!(
            shape,
            NetShape::CloudElastic {
                public: v4("54.32.224.1"),
                private: v4("10.0.0.5"),
                netmask: v4("255.255.255.0"),
            },
        );
        assert_eq!(shape.advertised_ip(), v4("54.32.224.1"));
        assert_eq!(shape.resolver_bind_override(), Some(v4("10.0.0.5")));
        // Publisher BindCfg::Elastic — masked subnet on the right
        // side, public IP on the left.
        assert_eq!(
            shape.publisher_bind_suggestion(),
            "54.32.224.1@10.0.0.0/24",
        );
    }

    #[test]
    fn netshape_private_without_cloud_falls_through() {
        // Private NIC, no metadata service → ordinary private host.
        let ifs = [iface("192.168.1.5", "255.255.255.0")];
        let shape = NetShape::from_interfaces(&ifs, None, || None, false);
        assert_eq!(
            shape,
            NetShape::Private { ip: v4("192.168.1.5"), netmask: v4("255.255.255.0") },
        );
        assert_eq!(shape.advertised_ip(), v4("192.168.1.5"));
        assert_eq!(shape.resolver_bind_override(), None);
        assert_eq!(shape.publisher_bind_suggestion(), "192.168.1.0/24");
    }

    #[test]
    fn netshape_loopback_when_nothing_routable() {
        let ifs = [iface("127.0.0.1", "255.0.0.0")];
        // Cloud probe MUST NOT run when there's no routable NIC to
        // pair a public IP with — gating on "private NIC found" is
        // the cheap-out-on-non-cloud-hosts optimization.
        let shape = NetShape::from_interfaces(&ifs, None, || panic!("cloud probe ran"), false);
        assert_eq!(shape, NetShape::Loopback);
        assert_eq!(shape.advertised_ip(), Ipv4Addr::LOCALHOST);
        assert_eq!(shape.resolver_bind_override(), None);
        assert_eq!(shape.publisher_bind_suggestion(), "local");
    }

    /// Container with a private bridge IP, no cloud, no env hint.
    /// The publisher suggestion is the `<PUBLIC_IP>` placeholder so
    /// the operator notices the missing piece rather than silently
    /// shipping a bind that only routes container-internal traffic.
    #[test]
    fn netshape_container_private_emits_placeholder_suggestion() {
        // Typical Docker bridge: 172.17.0.0/16.
        let ifs = [iface("172.17.0.2", "255.255.0.0")];
        let shape = NetShape::from_interfaces(&ifs, None, || None, /* in_container = */ true);
        assert_eq!(
            shape,
            NetShape::ContainerPrivate {
                private: v4("172.17.0.2"),
                netmask: v4("255.255.0.0"),
            },
        );
        assert!(shape.needs_operator_hint());
        assert_eq!(
            shape.publisher_bind_suggestion(),
            "<PUBLIC_IP>@172.17.0.0/16",
        );
        // Resolver bind override stays None — the resolver template
        // doesn't get a special elastic hint here; the CLI warns
        // instead so the operator knows to override --listen.
        assert_eq!(shape.resolver_bind_override(), None);
    }

    /// `NETIDX_PUBLIC_IP` overrides everything else: cloud probe
    /// doesn't run (panic if it does) and the container path is
    /// bypassed.
    #[test]
    fn netshape_env_override_short_circuits_detection() {
        let ifs = [iface("172.17.0.2", "255.255.0.0")];
        let shape = NetShape::from_interfaces(
            &ifs,
            Some(v4("54.32.224.1")),
            || panic!("cloud probe ran despite env override"),
            true, // in_container — should still defer to env var
        );
        assert_eq!(
            shape,
            NetShape::CloudElastic {
                public: v4("54.32.224.1"),
                private: v4("172.17.0.2"),
                netmask: v4("255.255.0.0"),
            },
        );
        assert!(!shape.needs_operator_hint());
    }

    /// When `NETIDX_PUBLIC_IP` happens to equal the discovered NIC
    /// (e.g. the operator set it from `curl ifconfig.me` on a host
    /// with a directly-attached public IP), collapse to `Public`
    /// rather than emit a redundant `54.32.224.1@54.32.224.0/24`.
    #[test]
    fn netshape_env_matching_nic_collapses_to_public() {
        let ifs = [iface("54.32.224.1", "255.255.255.0")];
        let shape = NetShape::from_interfaces(
            &ifs,
            Some(v4("54.32.224.1")),
            || panic!("cloud probe ran"),
            false,
        );
        assert_eq!(
            shape,
            NetShape::Public { ip: v4("54.32.224.1"), netmask: v4("255.255.255.0") },
        );
    }
}
