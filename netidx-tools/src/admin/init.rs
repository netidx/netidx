//! `netidx admin <role> install` — render and apply a `RenderedTemplate`
//! for one of the three v1 templates.

use anyhow::{Context, Result};
use arcstr::ArcStr;
// Qualified `admin_proto::` uses are all in the unix-only admin-server
// enrollment path; the items below are cross-platform.
use clap::Args;
use netidx_admin::{
    admin_client,
    admin_proto::{NodeKind, Role},
    config_lock::ConfigDirLock,
    fingerprint::ColorMode,
    paths,
    plan::AuthKind,
    template::{ParentRef, ReferralAuth},
};
// Re-exported so the sibling admin submodules keep calling
// `init::resolve_admin_server_addr`; the impl now lives in the engine.
pub(super) use netidx_admin::plan::resolve_admin_server_addr;
use std::{net::SocketAddr, path::PathBuf, str::FromStr};

use super::service;

// The three install entry points are exposed to the per-role command
// modules (`admin::roles::*`), which own the `<role> install` surface.
// `init` stays the shared install engine + helpers.

// -- Common -------------------------------------------------------------------

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
    /// Read the leaf private-key encryption password (only under
    /// `--key-protection password`) from this file. Strict mode only — never
    /// pass a secret on the command line. Distinct from the admin / recovery
    /// passwords so one file can't silently set two different secrets.
    #[arg(long = "key-password-file")]
    key_password_file: Option<PathBuf>,
    /// Read the leaf key password from stdin instead (strict mode).
    #[arg(long = "key-password-stdin", conflicts_with = "key_password_file")]
    key_password_stdin: bool,
    /// Read the founding admin's password (only when this install creates a new
    /// CA) from this file. Strict mode only.
    #[arg(long = "admin-password-file")]
    admin_password_file: Option<PathBuf>,
    /// Read the founding admin's password from stdin instead (strict mode).
    #[arg(long = "admin-password-stdin", conflicts_with = "admin_password_file")]
    admin_password_stdin: bool,
    /// Read the CA recovery password (only when unlocking an existing local CA
    /// to issue this host's certificate) from this file. Strict mode only.
    #[arg(long = "recovery-password-file")]
    recovery_password_file: Option<PathBuf>,
    /// Read the CA recovery password from stdin instead (strict mode).
    #[arg(long = "recovery-password-stdin", conflicts_with = "recovery_password_file")]
    recovery_password_stdin: bool,
    /// A CA fingerprint obtained out of band; confirms a network's
    /// identity non-interactively (e.g. with `--parent-admin-server`).
    #[arg(long = "accept-glyph")]
    accept_glyph: Option<String>,
}

impl CommonFlags {
    /// The install-wide flags the library planner acts on.
    fn install_common(&self) -> Result<netidx_admin::plan::install::InstallCommon> {
        use netidx_admin::plan::install::{InstallCommon, InstallMode};
        let mode = if self.dry_run {
            InstallMode::DryRun
        } else {
            InstallMode::Apply {
                config_lock: ConfigDirLock::acquire(paths::user_config_root()?)?,
            }
        };
        Ok(InstallCommon {
            mode,
            force: self.force,
            no_units: self.no_units,
            with_service: self.with_service,
            no_service: self.no_service,
        })
    }
}

/// Build the strict-CLI [`FlagAnswerer`] from an install's common flags: read
/// the key / admin / recovery passwords from their own `--*-password-file` /
/// `--*-password-stdin` pairs (never argv, and never collapsed to one secret)
/// and parse the out-of-band `--accept-glyph` fingerprint.
fn build_answerer(common: &CommonFlags) -> Result<super::answer_cli::FlagAnswerer> {
    super::answer_cli::FlagAnswerer::install(
        common.key_password_file.as_deref(),
        common.key_password_stdin,
        common.admin_password_file.as_deref(),
        common.admin_password_stdin,
        common.recovery_password_file.as_deref(),
        common.recovery_password_stdin,
        super::answer_cli::parse_glyph(common.accept_glyph.as_deref())?,
    )
}

/// Drive a library install cascade to completion under one tokio runtime, then
/// act on the service scope it decides (the privileged install stays in this
/// frontend). `fut` borrows the caller's [`FlagAnswerer`], which must outlive
/// this call.
fn finish_install(
    fut: impl std::future::Future<
        Output = Result<Option<netidx_admin::service::ServiceScope>>,
    >,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    if let Some(scope) = rt.block_on(fut)? {
        service::install_with_defaults(scope.into())?;
    }
    Ok(())
}

/// The clap `KeyProtArg` and the library's are structurally identical (the CLI
/// keeps its own until the still-interactive `ca` duplicates that share it
/// relocate in task 12); convert at the adapter boundary.
pub(super) fn lib_kp(
    k: Option<KeyProtArg>,
) -> Option<netidx_admin::plan::enroll::KeyProtArg> {
    use netidx_admin::plan::enroll::KeyProtArg as L;
    k.map(|k| match k {
        KeyProtArg::Seal => L::Seal,
        KeyProtArg::Password => L::Password,
        KeyProtArg::None => L::None,
    })
}

// -- workstation --------------------------------------------------------------

#[derive(Args, Debug)]
pub(crate) struct WorkstationFlags {
    #[command(flatten)]
    parent: ParentFlags,
    /// Enroll against this admin server (`ip:port`) instead of mDNS
    /// discovery — the non-interactive join path. On a TLS network this
    /// enrolls a client certificate (queued for admin approval); confirm the
    /// network out of band with `--accept-glyph` (view the glyph via `netidx
    /// admin ca fingerprint <ip:port>`). Ignored when a `--parent-*` flag
    /// specifies the parent explicitly.
    #[arg(long = "admin-server")]
    admin_server: Option<SocketAddr>,
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
    /// Override the perms-file owner. By default the workstation install grants
    /// `<base>` → `<current Local-auth user>` → `swlpd` so the operator has full
    /// rights immediately (a passwd username on Unix, `DOMAIN\user` on Windows).
    /// Pass an explicit identity when installing on behalf of another user.
    /// Conflicts with `--no-perms`.
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

/// `workstation install` needs the activation supervisor + Local auth,
/// which exist on unix and Windows. On any other platform fail fast with
/// a pointer to the publisher role, rather than running the whole
/// discovery/enrollment cascade and only erroring at template-render time.
#[cfg(not(any(unix, windows)))]
pub(crate) fn run_workstation(_f: WorkstationFlags) -> Result<()> {
    bail!("{}", netidx_admin::template::workstation::UNSUPPORTED_MSG)
}

#[cfg(any(unix, windows))]
pub(crate) fn run_workstation(f: WorkstationFlags) -> Result<()> {
    let mut ans = build_answerer(&f.common)?;
    let input = workstation_input(f)?;
    finish_install(netidx_admin::plan::install::workstation::run_workstation(
        &mut ans, input,
    ))
}

#[cfg(any(unix, windows))]
fn workstation_input(
    f: WorkstationFlags,
) -> Result<netidx_admin::plan::install::workstation::WorkstationInput> {
    use netidx_admin::plan::install::workstation::WorkstationInput;
    let explicit_parent =
        if f.parent.any_set() { f.parent.to_parent_ref(&f.base)? } else { None };
    let mut input = WorkstationInput::defaults(f.common.install_common()?);
    input.explicit_parent = explicit_parent;
    input.admin_server = f.admin_server;
    input.default_auth = f.default_auth;
    input.base = f.base;
    input.listen_port = f.listen_port;
    input.local_socket = f.local_socket;
    input.client_config_path = f.client_config_path;
    input.resolver_config_path = f.resolver_config_path;
    input.units_dir = f.units_dir;
    input.netidx_binary = f.netidx_binary;
    input.key_protection = lib_kp(f.key_protection);
    input.with_container = !f.no_container;
    input.owner = f.owner.map(ArcStr::from);
    input.with_perms_file = !f.no_perms;
    input.perms_path = f.perms_path;
    Ok(input)
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
    /// The network's admin server (`ip:port`, or a host resolved with the
    /// default admin port). Names the network directly instead of discovering
    /// it — required in strict mode (discovery is interactive-only).
    #[arg(long = "admin-server")]
    pub admin_server: Option<String>,
    /// The network's CA fingerprint, obtained out of band; confirms the
    /// network's identity non-interactively (required with `--admin-server`).
    #[arg(long = "accept-glyph")]
    pub accept_glyph: Option<String>,
    /// Under `--key-protection password`, read the leaf key password from this
    /// file (strict mode; never on the command line).
    #[arg(long = "password-file")]
    pub password_file: Option<PathBuf>,
    /// Read the leaf key password from stdin instead (strict mode).
    #[arg(long = "password-stdin", conflicts_with = "password_file")]
    pub password_stdin: bool,
}

/// `workstation join` — graduate a local-only workstation to a networked
/// one: select + glyph-confirm a network (by `--admin-server` or discovery),
/// enroll a client cert if it's TLS, and attach the local resolver to it via a
/// parent referral — without a reinstall or a hand-edit. The marker records the
/// joined (pinned) network so later `status`/`update` can re-pin to it.
pub(crate) fn run_workstation_join(f: WorkstationJoinFlags) -> Result<()> {
    // The only secret is the leaf key password (under --key-protection
    // password); the network is named by --admin-server + glyph-confirmed by
    // --accept-glyph, so `join` works with no TTY.
    let mut ans = super::answer_cli::make_flag_answerer(
        f.password_file.as_deref(),
        f.password_stdin,
        f.accept_glyph.as_deref(),
    )?;
    let admin_server =
        f.admin_server.as_deref().map(resolve_admin_server_addr).transpose()?;
    let mode = if f.dry_run {
        netidx_admin::plan::install::InstallMode::DryRun
    } else {
        netidx_admin::plan::install::InstallMode::Apply {
            config_lock: ConfigDirLock::acquire(paths::user_config_root()?)?,
        }
    };
    let input = netidx_admin::plan::install::workstation::WorkstationJoinInput {
        mode,
        key_protection: lib_kp(f.key_protection),
        admin_server,
    };
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    rt.block_on(netidx_admin::plan::install::workstation::run_workstation_join(
        &mut ans, input,
    ))
}

/// Print a confirmed-or-not network identity: domain, claimed roles,
/// fingerprint text + identicon.
pub(super) fn show_network_identity(
    addr: SocketAddr,
    identity: &admin_client::CaIdentity,
) {
    println!(
        "The admin server at {addr} serves network {:?} (roles: {}) and presented \
         this identity:",
        identity.domain,
        describe_roles(identity.roles),
    );
    println!("  SHA256  {}", identity.fingerprint.text());
    println!("{}", identity.fingerprint.identicon(ColorMode::detect()));
}

fn describe_roles(roles: enumflags2::BitFlags<Role>) -> String {
    if roles.is_empty() {
        return "none".to_string();
    }
    roles
        .into_iter()
        .map(|role| match role {
            Role::Ca => "ca",
            Role::Resolver => "resolver",
            Role::IdMap => "id-map",
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The id-map group default suggested at enrollment, by node kind:
/// infrastructure identities (resolvers, admin servers) don't act as
/// users, so they default to no registration.
pub(super) fn default_id_map_groups(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Resolver | NodeKind::AdminServer => "",
        NodeKind::Publisher | NodeKind::Client | NodeKind::Workstation => "users",
    }
}

/// Parse a typed id-map-groups answer into the group list. A bare `-`
/// is the explicit "no groups" sentinel — blank input is taken by the
/// prompt's default, so it can't double as "none"; otherwise the answer
/// is the comma-separated list, trimmed of surrounding space and blanks.
pub(super) fn parse_id_map_answer(answer: &str) -> Vec<String> {
    if answer.trim() == "-" {
        return Vec::new();
    }
    answer
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
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

// -- standalone-resolver ------------------------------------------------------

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
    /// For `--auth krb5`, how to map kerberos principals to unix ids:
    /// `platform` (a site IdM / `/bin/id` resolves full principals),
    /// `netidx` (the netidx id-mapper maps them), or `none` (perms keyed on
    /// the raw principal, no uid/gid mapping — the right choice on a krb5
    /// host with no system IdM). Ignored for other auth schemes.
    #[arg(long = "id-map-mode")]
    id_map_mode: Option<String>,
    /// Skip admin-server setup entirely (expert). On a fresh krb5 /
    /// anonymous network this also skips the admin-plane CA. A host
    /// without a admin server is invisible to discovery, and if no admin
    /// server exists anywhere on the network, certificate renewal and
    /// future zero-touch installs don't work at all.
    #[arg(long = "no-admin-server")]
    no_admin_server: bool,
    /// Explicitly set up an admin server for this network. Only meaningful on an
    /// anonymous data plane, where an admin server is optional — TLS/krb5 set one
    /// up automatically. Required (with `--no-admin-server` as the opposite) to
    /// make an anonymous install's choice non-interactively.
    #[arg(long = "with-admin-server", conflicts_with = "no_admin_server")]
    with_admin_server: bool,
    /// Proceed even when this host has no usable TPM / Secure Enclave.
    /// Only relevant when this install mints a new CA (the netidx-CA TLS
    /// resolver path with no existing CA, or a admin plane on a
    /// krb5/anonymous network). DANGER: the CA's autorenew credential is
    /// then written in PLAINTEXT, so every backup or disk image of this
    /// machine is a CA compromise. Test CAs only.
    #[arg(long = "insecure-no-tpm")]
    insecure_no_tpm: bool,
    /// Set this resolver up as a CHILD of an existing network: give the
    /// parent's admin-server address (`ip:port`). The install requests
    /// delegation of a subtree (`--delegate-subtree`) and, once the parent
    /// admin approves, bakes the parent referral into the config — no
    /// restart. Distinct from the peer-join discovery path. Unix-only.
    #[arg(long = "parent-admin-server")]
    parent_admin_server: Option<SocketAddr>,
    /// The subtree this resolver will own under the parent (with
    /// `--parent-admin-server`), e.g. `/eu`. Omit to install as a peer
    /// of the base cluster instead of requesting delegation.
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

pub(crate) fn run_resolver(f: ResolverFlags) -> Result<()> {
    let mut ans = build_answerer(&f.common)?;
    let input = resolver_input(f)?;
    finish_install(netidx_admin::plan::install::resolver::run_resolver(&mut ans, input))
}

fn resolver_input(
    f: ResolverFlags,
) -> Result<netidx_admin::plan::install::resolver::ResolverInput> {
    use netidx_admin::plan::install::resolver::ResolverInput;
    // Route through `to_parent_ref` only when a `--parent-*` flag is set, so a
    // stray `--parent-spn` without `--parent-addr` hits the misuse bail there
    // rather than silently dropping into the discovery cascade.
    let explicit_parent =
        if f.parent.any_set() { f.parent.to_parent_ref(&f.base)? } else { None };
    Ok(ResolverInput {
        common: f.common.install_common()?,
        auth: f.auth,
        spn: f.spn,
        tls_name: f.tls_name,
        listen: f.listen,
        bind: f.bind,
        base: f.base,
        perms_seed: f.perms_seed,
        perms_path: f.perms_path,
        no_perms: f.no_perms,
        explicit_parent,
        resolver_config_path: f.resolver_config_path,
        units_dir: f.units_dir,
        netidx_binary: f.netidx_binary,
        no_id_map: f.no_id_map,
        id_map_mode: f.id_map_mode,
        no_admin_server: f.no_admin_server,
        with_admin_server: f.with_admin_server,
        insecure_no_tpm: f.insecure_no_tpm,
        parent_admin_server: f.parent_admin_server,
        delegate_subtree: f.delegate_subtree,
        key_protection: lib_kp(f.key_protection),
        id_map_socket: f.id_map_socket,
        id_map_path: f.id_map_path,
        no_client: f.no_client,
        client_config_path: f.client_config_path,
    })
}

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
    /// Enroll against this admin server (`ip:port`) instead of mDNS
    /// discovery — the non-interactive join path. On a TLS network this
    /// enrolls a client certificate (queued for admin approval); confirm the
    /// network out of band with `--accept-glyph` (view the glyph via `netidx
    /// admin ca fingerprint <ip:port>`). Takes precedence over `--addr` /
    /// `--auth`.
    #[arg(long = "admin-server")]
    admin_server: Option<SocketAddr>,
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

pub(crate) fn run_publisher(f: PublisherFlags) -> Result<()> {
    let mut ans = build_answerer(&f.common)?;
    let input = publisher_input(f)?;
    finish_install(netidx_admin::plan::install::publisher::run_publisher(&mut ans, input))
}

fn publisher_input(
    f: PublisherFlags,
) -> Result<netidx_admin::plan::install::publisher::PublisherInput> {
    Ok(netidx_admin::plan::install::publisher::PublisherInput {
        common: f.common.install_common()?,
        addrs: f.addrs,
        auth: f.auth,
        admin_server: f.admin_server,
        spn: f.spn,
        socket: f.socket,
        tls_server_name: f.tls_server_name,
        default_auth: f.default_auth,
        base: f.base,
        config_path: f.config_path,
        bind: f.bind,
        units_dir: f.units_dir,
        key_protection: lib_kp(f.key_protection),
    })
}
