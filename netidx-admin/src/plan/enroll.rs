//! Admin domain discovery + certificate enrollment — the keystone every role
//! install shares.
//!
//! Discover a netidx admin domain over mDNS, **glyph-confirm** its identity (the
//! one human trust decision; everything after pins to the confirmed
//! fingerprint), and enroll a TLS certificate from its CA — all through the
//! [`Answerer`] seam, so the strict CLI, the TUI, and Atlas drive the same
//! flow. No `prompt::`, no `println!`, no per-call tokio runtimes: the whole
//! subgraph is `async` and speaks to the operator only through `ans`.

use super::resolve_admin_server_seeds;
use crate::{
    admin_proto::{InfoAuth, NodeKind},
    answer::{AdminDomainChoice, AdminDomainOption, Answerer, Field, Progress, Stage},
    atomic,
    discovery::{self},
    template::{AuthChoice, ReferralAuth, TlsIdentitySpec},
    tls,
    transport::{self, AdminDomainInfo, CaIdentity, PollOutcome},
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tempfile::TempDir;
use zeroize::Zeroizing;

/// The upper bound the interactive install flows browse mDNS for admin servers —
/// the ceiling for the case where mDNS is slow or nothing is there.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
/// The minimum the interactive flows browse before an early exit: collect at
/// least this long so a second admin domain (a VM, a satellite office) that answers a
/// beat later still makes the list, then early-exit once we have anything.
const DISCOVERY_SETTLE: Duration = Duration::from_secs(3);
/// Per-address bound on the CA-identity fetch a discovered beacon triggers.
/// mDNS is LAN-only, so an advertised address that doesn't answer promptly (a
/// stale or virtual-interface address — a docker bridge, a downed member) is
/// unreachable; time it out and try the beacon's next address rather than
/// blocking the whole discovery on one black-hole address.
const IDENTITY_FETCH_TIMEOUT: Duration = Duration::from_secs(3);
/// Validity requested from the CA. The server caps it to the admin's
/// policy, so this is just an upper bound.
const JOIN_VALIDITY: Duration = crate::plan::ca_setup::DEFAULT_LEAF_VALIDITY;
/// How often a waiting enrollee polls its queued request. Each poll is one
/// short pinned connection, so waiting holds nothing open.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

// -- discovered-admin domain types -------------------------------------------------

/// A discovered-and-confirmed netidx admin domain: the operator-confirmed
/// admin-plane identity plus the aggregated picture of the admin domain (every
/// connection behind `info` was pinned to that identity).
pub struct DiscoveredAdminDomain {
    pub identity: CaIdentity,
    pub info: AdminDomainInfo,
}

/// What the calling flow knows about admin servers on this admin domain. Threaded
/// into every sub-flow that could otherwise offer an admin domain join, so the
/// operator is asked at most once.
#[allow(clippy::large_enum_variant)]
pub enum AdminServers {
    /// An admin domain was discovered and its identity glyph-confirmed: use it, ask
    /// nothing further.
    Have(DiscoveredAdminDomain),
    /// We probed (and/or the operator declined): there is none. Never offer a
    /// admin domain join again in this run.
    DontHave,
    /// Nobody has checked (strict CLI, dry-run). A sub-flow that wants an
    /// admin server may probe itself.
    NotProbed,
}

impl AdminServers {
    /// The confirmed admin domain, if one was found and accepted.
    pub fn have(&self) -> Option<&DiscoveredAdminDomain> {
        match self {
            AdminServers::Have(net) => Some(net),
            AdminServers::DontHave | AdminServers::NotProbed => None,
        }
    }
}

/// A TLS identity obtained from the CA, with its files **staged** in a
/// tempdir. The template's `--force`-gated `apply()` installs it into the
/// canonical `~/.config/netidx/tls/<name>/` layout.
pub struct JoinedIdentity {
    pub name: String,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub trusted: PathBuf,
    /// The client-config askpass fallback, when the operator chose password
    /// protection for the key.
    pub askpass: Option<PathBuf>,
}

/// A client TLS identity plus the tempdir its files are staged in until
/// `apply()` installs them.
pub struct StagedIdentity {
    pub spec: TlsIdentitySpec,
    pub staging: Option<TempDir>,
}

fn identity_spec(
    name: &str,
    certificate: PathBuf,
    private_key: PathBuf,
    trusted: PathBuf,
    askpass: Option<PathBuf>,
) -> TlsIdentitySpec {
    let server_pattern = tls::domain_from_san(name)
        .map(str::to_string)
        .unwrap_or_else(|_| name.to_string());
    TlsIdentitySpec {
        server_pattern: ArcStr::from(server_pattern.as_str()),
        our_name: ArcStr::from(name),
        certificate,
        private_key,
        trusted,
        dest_dir: None,
        askpass,
    }
}

fn planned_identity_spec(name: &str) -> Result<TlsIdentitySpec> {
    let dir = tls::identity_dir(name)?;
    Ok(identity_spec(
        name,
        dir.join("certificate.pem"),
        dir.join("private.key"),
        dir.join("trusted.pem"),
        None,
    ))
}

/// Convert an installed [`JoinedIdentity`] into a client-side
/// [`TlsIdentitySpec`]. The files are already at their canonical home, so
/// `dest_dir` is `None` and the engine's install step is the harmless
/// self-copy the `check_no_overwrite` guard already allows.
pub fn joined_to_spec(j: JoinedIdentity) -> TlsIdentitySpec {
    identity_spec(&j.name, j.certificate, j.private_key, j.trusted, j.askpass)
}

/// Convert an installed [`JoinedIdentity`] into the resolver's own
/// [`AuthChoice::Tls`].
pub fn joined_to_auth(j: JoinedIdentity) -> AuthChoice {
    AuthChoice::Tls {
        name: ArcStr::from(j.name.as_str()),
        certificate: j.certificate,
        private_key: j.private_key,
        trusted: j.trusted,
        askpass: j.askpass,
    }
}

// -- key protection -----------------------------------------------------------

/// The `--key-protection` flag: like the interactive choice in
/// [`choose_key_protection`], but scriptable. `password` needs a typed secret,
/// so headless installs use seal or none (or a `--password-file`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyProtArg {
    Seal,
    Password,
    None,
}

impl std::str::FromStr for KeyProtArg {
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

/// How a private key is protected at rest. Decided once per identity at issue
/// time by [`choose_key_protection`]; every key-producing flow acts on the
/// same three cases.
pub enum KeyProtection {
    /// Encrypted under a random password sealed to this machine's TPM. The
    /// blob is written beside the key as `<key>.tpm`; the key is useless
    /// off-host.
    Sealed { password: Zeroizing<String>, blob: Vec<u8> },
    /// Encrypted under a typed password (saved to the system keychain;
    /// `askpass` is the client-config fallback when the keychain is locked or
    /// missing).
    Password { password: String, askpass: Option<PathBuf> },
    /// Plaintext — file modes are the only protection.
    None,
}

impl KeyProtection {
    /// The encryption password, if any (what the openssl issue paths take).
    pub fn password(&self) -> Option<&str> {
        match self {
            KeyProtection::Sealed { password, .. } => Some(password),
            KeyProtection::Password { password, .. } => Some(password),
            KeyProtection::None => None,
        }
    }

    /// The client-config askpass fallback, if the password case set one.
    pub fn askpass(&self) -> Option<PathBuf> {
        match self {
            KeyProtection::Password { askpass, .. } => askpass.clone(),
            KeyProtection::Sealed { .. } | KeyProtection::None => None,
        }
    }

    /// Write the sealed-password sidecar beside `key` (no-op for the other
    /// variants). Call with wherever the key file actually lands — staging
    /// dirs included; the identity installer copies sidecars with their keys.
    pub fn write_sidecar(&self, key: &Path) -> Result<()> {
        if let KeyProtection::Sealed { blob, .. } = self {
            atomic::write_atomic(&tls::sealed_sidecar(key), blob, 0o600)?;
        }
        Ok(())
    }
}

/// Walk the canonical places askpass programs live and return the first one we
/// find, used as the default for the askpass prompt. `$SSH_ASKPASS`, then PATH
/// lookups, then known distro absolute paths. `None` if nothing matched.
#[cfg(unix)]
pub fn find_askpass() -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let executable = |p: &Path| {
        std::fs::metadata(p)
            .map(|m| m.is_file() && (m.permissions().mode() & 0o111) != 0)
            .unwrap_or(false)
    };
    if let Some(p) = std::env::var_os("SSH_ASKPASS") {
        let p = PathBuf::from(p);
        if executable(&p) {
            return Some(p);
        }
    }
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

/// Decide how a new identity's private key is protected at rest: **seal** (TPM
/// / Secure Enclave — the default whenever the host has one), **password**
/// (typed, keychain + askpass), or **none**. `key_path` is the canonical
/// on-disk location the key will live at (the keychain entry is keyed on it).
/// Non-interactive: seal when the hardware is usable, none otherwise. Hardware
/// that fails at seal time degrades to the no-hardware behavior with a warning.
pub async fn choose_key_protection(
    ans: &mut dyn Answerer,
    flag: Option<KeyProtArg>,
    key_path: &Path,
    name: &str,
) -> Result<KeyProtection> {
    let tpm = netidx_tpm::available();
    let choice = match flag {
        Some(KeyProtArg::Seal) => "seal".to_string(),
        Some(KeyProtArg::Password) => "password".to_string(),
        Some(KeyProtArg::None) => "none".to_string(),
        None if !ans.interactive() => {
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
            ans.choice(Field::KeyProtection, None, options, Some(default)).await?
        }
    };
    match choice.as_str() {
        "seal" => {
            let password = netidx_tpm::random_secret();
            match netidx_tpm::seal(password.as_bytes()) {
                Ok(blob) => {
                    ans.note(&format_compact!(
                        "the key for {name} will be sealed to this machine's {} \
                         — copied anywhere else it is useless",
                        netidx_tpm::MECHANISM
                    ));
                    Ok(KeyProtection::Sealed { password, blob })
                }
                Err(e) => {
                    ans.warn(&format_compact!(
                        "{} sealing failed ({e:#}); falling back to an \
                         unprotected key. Re-run once the hardware is usable, or \
                         choose password protection.",
                        netidx_tpm::MECHANISM
                    ));
                    Ok(KeyProtection::None)
                }
            }
        }
        "password" => {
            let mut pw = ans.secret(Field::KeyPassword, None).await?;
            if pw.0.is_empty() {
                bail!("empty password; choose 'none' for an unprotected key");
            }
            #[cfg(unix)]
            let discovered = find_askpass();
            #[cfg(not(unix))]
            let discovered: Option<PathBuf> = None;
            let default = discovered
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            let answer = ans
                .text(Field::Askpass, None, Some(&default), false)
                .await?
                .unwrap_or_default();
            let askpass = if answer.is_empty() || answer.trim() == "-" {
                None
            } else {
                Some(PathBuf::from(answer))
            };
            if let Err(e) =
                netidx::tls::save_password_for_key(&key_path.to_string_lossy(), &pw.0)
            {
                ans.warn(&format_compact!(
                    "failed to save key password to the system keychain ({e:#}); \
                     the resolver may need an askpass at startup.",
                ));
            }
            Ok(KeyProtection::Password { password: std::mem::take(&mut pw.0), askpass })
        }
        _ => Ok(KeyProtection::None),
    }
}

// -- id-map groups ------------------------------------------------------------

/// The id-map group default suggested at enrollment, by node kind:
/// infrastructure identities (resolvers, admin servers) don't act as users, so
/// they default to no registration.
pub fn default_id_map_groups(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Resolver | NodeKind::AdminServer => "",
        NodeKind::Publisher | NodeKind::Client | NodeKind::Workstation => "users",
    }
}

/// Parse a typed id-map-groups answer into the group list. A bare `-` is the
/// explicit "no groups" sentinel — blank input is taken by the prompt's
/// default, so it can't double as "none"; otherwise the answer is the
/// comma-separated list, trimmed of surrounding space and blanks.
pub fn parse_id_map_answer(answer: &str) -> Vec<String> {
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

/// The id-map groups to assign a new identity (comma-separated, first is
/// primary). `provided` values short-circuit; interactively a blank answer
/// takes `default`, a bare `-` is the explicit "no groups" sentinel.
pub async fn prompt_id_map_groups(
    ans: &mut dyn Answerer,
    provided: &[String],
    default: &str,
) -> Result<Vec<String>> {
    if !provided.is_empty() {
        return Ok(provided
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect());
    }
    let answer = ans
        .text(Field::IdMapGroups, None, Some(default), false)
        .await?
        .unwrap_or_else(|| default.to_string());
    Ok(parse_id_map_answer(&answer))
}

// -- discovery ----------------------------------------------------------------

fn describe_info_auth(a: &InfoAuth) -> String {
    match a {
        InfoAuth::Anonymous => "anonymous".to_string(),
        InfoAuth::Krb5 { spn } => format!("krb5, spn {spn}"),
        InfoAuth::Tls { name } => format!("tls, name {name}"),
    }
}

/// Parse an optional operator-typed admin-server address into seeds; `None`
/// (or blank) means "there is none".
fn manual_seeds(answer: Option<String>) -> Result<Option<Vec<SocketAddr>>> {
    match answer {
        Some(s) if !s.trim().is_empty() => Ok(Some(resolve_admin_server_seeds(&s)?)),
        _ => Ok(None),
    }
}

/// An admin domain found by [`discover_admin_domains`]: its TLS domain, the admin-server
/// address(es) that advertised it over mDNS, and — when one answered — the CA
/// identity they present (whose `fingerprint` is the glyph a script passes to
/// `--accept-glyph`), or the reason none did.
pub struct DiscoveredAdminDomainReport {
    pub domain: String,
    pub admin_servers: Vec<SocketAddr>,
    pub identity: Result<CaIdentity, String>,
}

/// Why a discovery pass that heard beacons produced no usable admin domain.
///
/// `None` when at least one verified, or when nothing was advertised at all —
/// the confusing case is specifically "something answered, none of it usable",
/// which looks identical to "found nothing" unless the reasons are surfaced.
/// The usual cause is an admin server advertising an address this host cannot
/// reach.
pub fn discovery_diagnosis(
    reports: &[DiscoveredAdminDomainReport],
) -> Option<Vec<String>> {
    if reports.is_empty() || reports.iter().any(|r| r.identity.is_ok()) {
        return None;
    }
    let mut lines = vec![format!(
        "Found {} advertised admin server(s), but none answered with a CA identity:",
        reports.len()
    )];
    for report in reports {
        let addrs = report
            .admin_servers
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        if let Err(why) = &report.identity {
            lines.push(format!("{:?} at {addrs}: {why}", report.domain));
        }
    }
    lines.push(String::new());
    lines.push(
        "The admin server may be advertising an address this host can't reach \
         (check its listen address / firewall)."
            .to_string(),
    );
    Some(lines)
}

/// Browse mDNS for netidx admin servers and, for each distinct admin domain, fetch
/// the CA identity from the first reachable server. A pure read-only QUERY —
/// no prompts, no decisions — so, unlike the interactive [`discover_admin_domain`]
/// cascade (which the strict CLI disables), it is valid in strict/scripted
/// mode: a script runs it, reads an admin domain's admin-server address + glyph, and
/// feeds them to `--admin-server` / `--accept-glyph`.
///
/// `early_exit = Some(settle)` browses at least `settle` and then returns as
/// soon as an admin domain is found (up to `timeout`) — the interactive join, where
/// one admin domain is expected. `None` browses the whole `timeout` to enumerate
/// every admin domain — the strict enumerator.
pub async fn discover_admin_domains(
    timeout: Duration,
    kind: NodeKind,
    early_exit: Option<Duration>,
) -> Vec<DiscoveredAdminDomainReport> {
    // The mDNS browse blocks for `timeout`; keep it off the async worker.
    // `early_exit = Some(settle)` waits at least `settle` then early-exits once a
    // admin domain is found (the interactive join); `None` waits the full window to
    // list every admin domain (the strict enumerator).
    let found = tokio::task::spawn_blocking(move || match early_exit {
        Some(settle) => discovery::browse_first_or_empty(timeout, settle),
        None => discovery::browse_or_empty(timeout),
    })
    .await
    .unwrap_or_default();
    // Group the (unauthenticated, hint-only) beacons by domain; the identity
    // fetched next is the authenticated fact.
    let mut domains: BTreeMap<String, Vec<SocketAddr>> = BTreeMap::new();
    for d in &found {
        domains.entry(d.domain.clone()).or_default().extend(d.socket_addrs());
    }
    let mut out = Vec::new();
    for (domain, mut admin_servers) in domains {
        admin_servers.sort();
        admin_servers.dedup();
        let mut identity = Err("no advertised admin server was reachable".to_string());
        for addr in &admin_servers {
            let fetched = tokio::time::timeout(
                IDENTITY_FETCH_TIMEOUT,
                transport::fetch_identity(*addr, kind),
            )
            .await;
            match fetched {
                Ok(Ok(id)) => {
                    identity = Ok(id);
                    break;
                }
                Ok(Err(e)) => {
                    identity = Err(format_compact!("{addr}: {e:#}").into_string())
                }
                Err(_) => {
                    identity = Err(format_compact!(
                        "{addr}: no response within {IDENTITY_FETCH_TIMEOUT:?}"
                    )
                    .into_string())
                }
            }
        }
        out.push(DiscoveredAdminDomainReport { domain, admin_servers, identity });
    }
    out
}

/// Find the admin domain this node should join: browse mDNS, group by domain, let
/// the operator pick (falling back to a manual admin-server address when
/// discovery finds nothing), then fetch and glyph-confirm the admin domain's
/// identity and aggregate `GetInfo` across its admin servers.
///
/// [`AdminServers::DontHave`] ⇒ the operator concluded there is none (nothing
/// found / declined); [`AdminServers::NotProbed`] is returned only for a
/// non-interactive frontend — the strict CLI uses explicit flags instead.
pub async fn discover_admin_domain(
    ans: &mut dyn Answerer,
    kind: NodeKind,
) -> Result<AdminServers> {
    if !ans.interactive() {
        return Ok(AdminServers::NotProbed);
    }
    // Role-specific framing: a resolver can found a new admin domain, so it defaults
    // to that; a workstation can't found one, so it either joins an admin domain (the
    // default) or installs stand-alone. Either way the non-join option means
    // "no parent admin domain" → `DontHave`. A publisher is not offered the choice at
    // all: it publishes to somebody's resolver or it does nothing, so a
    // stand-alone publisher is not a thing and it always joins.
    let membership = match kind {
        NodeKind::Resolver => Some((
            Field::AdminDomainMode,
            "Create a new admin domain (creates a CA)",
            "Use an existing CA",
            "Create a new admin domain (creates a CA)",
        )),
        NodeKind::Client | NodeKind::Workstation | NodeKind::AdminServer => Some((
            Field::Membership,
            "Install stand alone",
            "Join an admin domain",
            "Join an admin domain",
        )),
        NodeKind::Publisher => None,
    };
    // Decide first, discover second — so the flow is identical however many
    // admin domains happen to be on the network.
    if let Some((field, standalone, join, default)) = membership {
        let choice = ans.choice(field, None, &[standalone, join], Some(default)).await?;
        if choice != join {
            return Ok(AdminServers::DontHave);
        }
    }
    // Connecting: browse the local network for admin domains and fetch each one's CA
    // identity, so the operator can recognize the one they mean by its glyph. The
    // operator can browse again ("poll for more") to pick up an admin domain that
    // answered late; new ones are appended, deduped by domain.
    let mut options: Vec<AdminDomainOption> = Vec::new();
    let mut servers: Vec<Vec<SocketAddr>> = Vec::new();
    discover_into(ans, kind, &mut options, &mut servers).await;
    // Pick a discovered admin domain by its glyph, enter an admin-server address
    // manually, or poll again for more.
    let seeds: Vec<SocketAddr> = 'pick: loop {
        match ans.select_admin_domain(&options).await? {
            AdminDomainChoice::Discovered(i) => match servers.get(i) {
                Some(s) => break 'pick s.clone(),
                None => return Ok(AdminServers::DontHave),
            },
            AdminDomainChoice::Manual => loop {
                let typed = ans.text(Field::AdminServerAddr, None, None, true).await?;
                match manual_seeds(typed) {
                    Ok(Some(s)) => break 'pick s,
                    Ok(None) => return Ok(AdminServers::DontHave),
                    Err(e) => ans.warn(&format_compact!("{e:#}")),
                }
            },
            AdminDomainChoice::PollMore => {
                match discover_into(ans, kind, &mut options, &mut servers).await {
                    0 => ans.note("no additional admin domains found"),
                    n => ans.note(&format_compact!("found {n} more admin domain(s)")),
                }
            }
        }
    };
    confirm_seeds(ans, &seeds, kind).await
}

/// Browse the network once and append any newly-discovered admin domains (deduped by
/// domain) to `options`/`servers`, fetching each one's CA identity so it can be
/// picked by its glyph. Returns how many were newly added. A previously-listed
/// admin domain is skipped; a previously-unreachable one is re-attempted (it may
/// answer now), which is exactly what "poll for more" is for.
async fn discover_into(
    ans: &mut dyn Answerer,
    kind: NodeKind,
    options: &mut Vec<AdminDomainOption>,
    servers: &mut Vec<Vec<SocketAddr>>,
) -> usize {
    ans.progress(Progress::timed(
        Stage::Discovering,
        "searching for a netidx admin domain on the local network…",
        DISCOVERY_TIMEOUT,
    ));
    let reports =
        discover_admin_domains(DISCOVERY_TIMEOUT, kind, Some(DISCOVERY_SETTLE)).await;
    let mut added = 0;
    for r in reports {
        if options.iter().any(|o| o.domain == r.domain) {
            continue;
        }
        match r.identity {
            Ok(identity) => {
                options.push(AdminDomainOption { domain: r.domain, identity });
                servers.push(r.admin_servers);
                added += 1;
            }
            Err(e) => ans.note(&format_compact!("skipping {:?}: {e}", r.domain)),
        }
    }
    added
}

/// Fetch the admin domain identity from the first reachable seed and have the
/// operator glyph-confirm it — the single human trust decision; everything
/// after is pinned to the confirmed fingerprint. Then map the admin domain.
pub async fn confirm_seeds(
    ans: &mut dyn Answerer,
    seeds: &[SocketAddr],
    kind: NodeKind,
) -> Result<AdminServers> {
    let mut fetched = None;
    for addr in seeds {
        match transport::fetch_identity(*addr, kind).await {
            Ok(id) => {
                fetched = Some((*addr, id));
                break;
            }
            Err(e) => {
                ans.note(&format_compact!(
                    "admin server {addr} could not be queried: {e:#}"
                ));
            }
        }
    }
    let Some((_addr, identity)) = fetched else {
        bail!("no admin server could be reached")
    };
    if !ans.confirm_identity(&identity).await? {
        bail!("the admin domain identity was not confirmed; nothing was sent");
    }
    let info = transport::aggregate(seeds, kind, &identity)
        .await
        .context("mapping the admin domain (GetInfo peer walk)")?;
    Ok(AdminServers::Have(DiscoveredAdminDomain { identity, info }))
}

/// Confirm an admin domain reachable at one explicit address — the WAN parent given
/// via `--parent-admin-server`, where there is no mDNS. Resolving the parent
/// into a `Have` BEFORE the create-vs-enroll decision is what makes a
/// satellite enroll its cert from the existing CA and never mint its own.
pub async fn confirm_admin_domain_at(
    ans: &mut dyn Answerer,
    addr: SocketAddr,
    kind: NodeKind,
) -> Result<AdminServers> {
    confirm_seeds(ans, &[addr], kind).await
}

/// Map a confirmed admin domain's resolvers into per-address referral auth,
/// obtaining a client TLS identity from the admin domain's CA when the admin domain runs
/// TLS and the caller doesn't already have one (`have_identity`). The identity
/// was already glyph-confirmed in [`discover_admin_domain`] — no second
/// confirmation; the signing connection still pins to it.
#[allow(clippy::too_many_arguments)]
pub async fn admin_domain_addrs_and_identity(
    ans: &mut dyn Answerer,
    net: &DiscoveredAdminDomain,
    kind: NodeKind,
    have_identity: bool,
    dry_run: bool,
    kp: Option<KeyProtArg>,
    tls_identities: &mut Vec<TlsIdentitySpec>,
    tls_staging: &mut Vec<TempDir>,
) -> Result<Vec<(SocketAddr, ReferralAuth)>> {
    if net.info.resolvers.is_empty() {
        bail!(
            "the admin servers of admin domain {:?} reported no resolvers — is the \
             resolver host's admin server down? (manual setup: re-run and leave \
             the admin-server prompts blank)",
            net.identity.domain,
        );
    }
    ans.note(&format_compact!(
        "admin domain {:?}: {} resolver(s)",
        net.identity.domain,
        net.info.resolvers.len()
    ));
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
        ans.note(&format_compact!("  {} ({})", r.addr, describe_info_auth(&r.auth)));
        addrs.push((r.addr, auth));
    }
    if needs_tls && !have_identity {
        let Some(ca_addr) = net.info.ca_addr else {
            bail!(
                "admin domain {:?} uses TLS but none of its admin servers reported a \
                 CA — cannot obtain a client certificate",
                net.identity.domain,
            )
        };
        // Default identity name: a service host is best identified by its
        // hostname (`publisher.<domain>`), a personal machine by its user
        // (`alice.<domain>`). The domain is the TLS-attested one.
        let base = match kind {
            NodeKind::Publisher => current_hostname(),
            NodeKind::Resolver
            | NodeKind::Client
            | NodeKind::Workstation
            | NodeKind::AdminServer => current_username(),
        };
        let suggested = base.map(|n| format!("{n}.{}", net.identity.domain));
        if dry_run {
            let identity = planned_tls_client_identity(ans, suggested.as_deref()).await?;
            ans.note(&format_compact!(
                "[dry-run] would enroll TLS identity {:?} from admin server {ca_addr}",
                identity.spec.our_name
            ));
            tls_identities.push(identity.spec);
        } else {
            let (j, staging) = join_admin_domain(
                ans,
                JoinRequest {
                    suggested_name: suggested.as_deref(),
                    key_protection: kp,
                    ..JoinRequest::new(ca_addr, kind, &net.identity)
                },
            )
            .await?;
            tls_identities.push(joined_to_spec(j));
            tls_staging.push(staging);
        }
    }
    Ok(addrs)
}

/// Obtain a TLS identity from the admin domain's admin server when one is known (or
/// discoverable), instead of the local-CA / CSR flow. Keyed on what the
/// calling flow already knows ([`AdminServers`]): `Have` joins with no further
/// questions; `DontHave` returns `None`; `NotProbed` runs discovery here.
/// Cross-platform: this is also how a node with no openssl gets a TLS cert.
pub async fn maybe_join_ca_server(
    ans: &mut dyn Answerer,
    probe: &AdminServers,
    kind: NodeKind,
    suggested_name: Option<&str>,
    kp: Option<KeyProtArg>,
) -> Result<Option<(JoinedIdentity, TempDir)>> {
    let Some((ca_addr, identity)) = selected_ca_server(ans, probe, kind).await? else {
        return Ok(None);
    };
    let joined = join_admin_domain(
        ans,
        JoinRequest {
            suggested_name,
            key_protection: kp,
            ..JoinRequest::new(ca_addr, kind, &identity)
        },
    )
    .await?;
    Ok(Some(joined))
}

async fn selected_ca_server(
    ans: &mut dyn Answerer,
    probe: &AdminServers,
    kind: NodeKind,
) -> Result<Option<(SocketAddr, CaIdentity)>> {
    let probed_here;
    let net = match probe {
        AdminServers::DontHave => return Ok(None),
        AdminServers::Have(net) => net,
        AdminServers::NotProbed => match discover_admin_domain(ans, kind).await? {
            AdminServers::Have(net) => {
                probed_here = net;
                &probed_here
            }
            AdminServers::DontHave | AdminServers::NotProbed => return Ok(None),
        },
    };
    let Some(ca_addr) = net.info.ca_addr else {
        ans.note(&format_compact!(
            "admin domain {:?} reported no CA; falling back to local certificate setup",
            net.identity.domain,
        ));
        return Ok(None);
    };
    Ok(Some((ca_addr, net.identity.clone())))
}

/// Poll a queued request until it settles (approved / denied / expired),
/// sleeping [`POLL_INTERVAL`] between checks. Each poll is one short pinned
/// connection, so waiting holds nothing open. Never returns
/// [`PollOutcome::Pending`] — it loops on it. Shared by every queued-approval
/// flow (cert join, admin-server enroll) so they don't each re-implement the
/// wait.
///
/// A comms error between polls is treated as transient and retried (the admin
/// server may restart or blip while we wait for a human to approve; the queued
/// request is durable server-side). Only a settled protocol outcome ends the
/// wait — giving up on the first connection error would abandon an
/// already-approved request on a single blip.
pub async fn await_issuance(
    ans: &mut dyn Answerer,
    addr: SocketAddr,
    kind: NodeKind,
    pending: &transport::PendingEnrollment,
    identity: &CaIdentity,
) -> Result<PollOutcome> {
    let (purpose, message) = match kind {
        NodeKind::AdminServer => (
            "admin-server enrollment request",
            "waiting for a CA admin to approve this admin-server enrollment…",
        ),
        NodeKind::Resolver
        | NodeKind::Publisher
        | NodeKind::Client
        | NodeKind::Workstation => {
            ("enrollment request", "waiting for a CA admin to approve this request…")
        }
    };
    ans.show_verification_code(purpose, &pending.fingerprint);
    ans.progress(Progress::new(Stage::WaitingApproval, message));
    let settled = loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        match transport::poll(addr, kind, pending, identity).await {
            Ok(PollOutcome::Pending) => continue,
            Ok(settled) => break settled,
            Err(e) => {
                ans.progress(Progress::new(
                    Stage::WaitingApproval,
                    format_compact!(
                        "admin server unreachable ({e:#}); still waiting for approval…"
                    ),
                ));
                continue;
            }
        }
    };
    ans.clear_verification_code();
    Ok(settled)
}

/// Everything a join needs beyond the admin domain identity the caller has
/// already confirmed. [`JoinRequest::new`] fills in every default, so a caller
/// names only what it means to change.
pub struct JoinRequest<'a> {
    /// The admin server to enroll with.
    pub addr: SocketAddr,
    /// What kind of node this identity is for.
    pub kind: NodeKind,
    /// Pre-fill for the identity name. `None` derives one.
    pub suggested_name: Option<&'a str>,
    /// How to protect the issued private key.
    pub key_protection: Option<KeyProtArg>,
    /// The certificate this one replaces, so the CA can revoke it.
    pub replaces_serial: Option<u64>,
    /// Request the certificate synchronously as this admin, rather than
    /// queueing it for remote approval. A scripted caller holding an admin
    /// credential says so here; without it an interactive operator is asked
    /// and a scripted one queues.
    pub admin: Option<String>,
    /// id-map groups to register the identity with. Empty asks, or takes the
    /// per-kind default when nobody is there to ask.
    pub id_map_groups: &'a [String],
    /// The certificate lifetime to request. `None` asks for the standard one.
    pub validity: Option<Duration>,
    /// Refuse, before asking the CA for anything, if this name already has an
    /// identity installed in the canonical user directory. Only a caller that
    /// installs *there* wants this — an install or a restore stages elsewhere
    /// and is gated by its own `--force`.
    pub refuse_if_installed: bool,
    /// The confirmed CA identity every connection here pins to.
    pub identity: &'a CaIdentity,
}

impl<'a> JoinRequest<'a> {
    /// A join with every choice defaulted: derive the name, no key protection,
    /// replace nothing, queue for approval, standard groups and lifetime.
    pub fn new(addr: SocketAddr, kind: NodeKind, identity: &'a CaIdentity) -> Self {
        JoinRequest {
            addr,
            kind,
            suggested_name: None,
            key_protection: None,
            replaces_serial: None,
            admin: None,
            id_map_groups: &[],
            validity: None,
            refuse_if_installed: false,
            identity,
        }
    }
}

/// Obtain a cert from an **already confirmed** admin domain — every connection
/// pins to [`JoinRequest::identity`]. Returns the issued identity staged in a
/// tempdir (the template's `--force`-gated `apply()`, or
/// [`crate::tls::install_identity_for_user`], installs it).
///
/// The default path queues a signing request and waits for an admin to approve
/// it remotely (`netidx admin ca approve`): the enrollee shows a request code
/// (the CSR key's fingerprint) the admin matches out of band. The synchronous
/// path — an admin is at this machine, or a script holds their credential —
/// is one answer away; it is the only path where the id-map groups are chosen
/// here.
pub async fn join_admin_domain(
    ans: &mut dyn Answerer,
    req: JoinRequest<'_>,
) -> Result<(JoinedIdentity, TempDir)> {
    let JoinRequest {
        addr,
        kind,
        suggested_name,
        key_protection,
        replaces_serial,
        admin,
        id_map_groups,
        validity,
        refuse_if_installed,
        identity,
    } = req;
    let validity = validity.unwrap_or(JOIN_VALIDITY);
    let name = prompt_identity_name(ans, suggested_name).await?;
    // Before the CA is asked for anything: an issued certificate this host
    // then refuses to install is still live at the CA, and would block the
    // retry the refusal recommends.
    if refuse_if_installed {
        tls::refuse_to_clobber_user_identity(&name)?;
    }
    // Key protection is decided before the request: the operator is here now,
    // and the queued path may wait on a remote admin for a long time after.
    let protection = choose_key_protection(
        ans,
        key_protection,
        &tls::identity_dir(&name)?.join("private.key"),
        &name,
    )
    .await?;
    // A caller that already holds an admin credential *is* an admin standing
    // by. One that does not and cannot prompt has nobody to type a password,
    // so it takes the queued (remote-approval) path.
    let admin_here = match &admin {
        Some(_) => true,
        None => ans.interactive() && ans.confirm(Field::AdminHere, None, false).await?,
    };
    let issued = if admin_here {
        // The admin chooses the new identity's id-map groups here; the
        // per-admin policy is the allowed set the server validates against.
        let groups =
            prompt_id_map_groups(ans, id_map_groups, default_id_map_groups(kind)).await?;
        let admin = match admin {
            Some(admin) => admin,
            None => ans
                .text(Field::AdminName, None, None, true)
                .await?
                .context("a CA admin name is required")?,
        };
        let mut pw_secret = ans.secret(Field::AdminPassword, None).await?;
        let password = Zeroizing::new(std::mem::take(&mut pw_secret.0));
        transport::request_cert_replacing(
            addr,
            kind,
            &name,
            &admin,
            password,
            validity,
            groups,
            replaces_serial,
            identity,
        )
        .await?
    } else {
        let pending = match replaces_serial {
            Some(serial) => {
                transport::enqueue_replacing(
                    addr, kind, &name, validity, serial, identity,
                )
                .await?
            }
            None => transport::enqueue(addr, kind, &name, validity, identity).await?,
        };
        match await_issuance(ans, addr, kind, &pending, identity).await? {
            PollOutcome::Issued(issued) => issued,
            PollOutcome::Denied(reason) => {
                bail!("the CA admin denied this request: {reason}")
            }
            PollOutcome::Expired => bail!(
                "the request expired before an admin approved it; re-run to \
                 queue a new one"
            ),
            PollOutcome::Pending => unreachable!("await_issuance never returns Pending"),
        }
    };

    // Stage the issued files in a tempdir; the template's `apply()` does the
    // real install into the canonical identity dir, gated by
    // `check_no_overwrite` (`--force`). The returned tempdir must outlive
    // `apply()`; the caller keeps it alive.
    let staging = TempDir::new().context("creating tls staging dir")?;
    let certificate = staging.path().join("certificate.pem");
    let private_key = staging.path().join("private.key");
    let trusted = staging.path().join("trusted.pem");
    atomic::write_atomic(&certificate, issued.cert_pem.as_bytes(), 0o644)?;
    // Apply the protection decided up front: the key is encrypted before it
    // ever touches disk, and a sealed password's blob is staged beside it.
    let key_payload = match protection.password() {
        Some(pw) => Zeroizing::new(
            netidx::tls::encrypt_private_key(&issued.private_key_pem, pw)
                .context("encrypting the issued private key")?,
        ),
        None => issued.private_key_pem.clone(),
    };
    atomic::write_atomic(&private_key, key_payload.as_bytes(), 0o600)?;
    protection.write_sidecar(&private_key)?;
    atomic::write_atomic(&trusted, issued.trusted_pem.as_bytes(), 0o644)?;
    ans.note(&format_compact!("got TLS identity {name:?} from admin server {addr}"));
    for w in &issued.warnings {
        ans.warn(w);
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

async fn prompt_identity_name(
    ans: &mut dyn Answerer,
    suggested_name: Option<&str>,
) -> Result<String> {
    ans.text(
        Field::TlsName,
        None,
        suggested_name,
        // Not a required-explicit decision: the enrolling node's identity
        // defaults to `<user-or-host>.<domain>` (the interactive default),
        // so a non-interactive join still enrolls without an identity flag.
        false,
    )
    .await?
    .context("a TLS identity name is required (no default could be derived)")
}

async fn planned_tls_client_identity(
    ans: &mut dyn Answerer,
    suggested_name: Option<&str>,
) -> Result<StagedIdentity> {
    let name = prompt_identity_name(ans, suggested_name).await?;
    Ok(StagedIdentity { spec: planned_identity_spec(&name)?, staging: None })
}

/// Obtain a client-side TLS identity by enrolling over the admin plane: a
/// discovered admin server signs our CSR on the spot (cross-platform, rcgen).
/// Requires a reachable admin server; there is no in-wizard bring-your-own-cert
/// path (that is a self-managed setup, configured by hand).
pub async fn prompt_tls_client_identity(
    ans: &mut dyn Answerer,
    suggested_name: Option<&str>,
    kp: Option<KeyProtArg>,
    probe: &AdminServers,
    dry_run: bool,
) -> Result<StagedIdentity> {
    let identity = if dry_run {
        match selected_ca_server(ans, probe, NodeKind::Client).await? {
            Some((ca_addr, _)) => {
                let identity = planned_tls_client_identity(ans, suggested_name).await?;
                ans.note(&format_compact!(
                    "[dry-run] would enroll TLS identity {:?} from admin server {ca_addr}",
                    identity.spec.our_name
                ));
                Some(identity)
            }
            None => None,
        }
    } else {
        maybe_join_ca_server(ans, probe, NodeKind::Client, suggested_name, kp).await?.map(
            |(j, staging)| StagedIdentity {
                spec: joined_to_spec(j),
                staging: Some(staging),
            },
        )
    };
    match identity {
        Some(identity) => Ok(identity),
        None => bail!(
            "a TLS identity via the wizard requires a reachable admin server to \
             enroll against; none was found. To run TLS without an admin server, \
             configure the identity by hand (cert, key, and trusted-CA bundle)."
        ),
    }
}

// -- host identity (prompt defaults) ------------------------------------------

/// This machine's hostname (short form) — the natural identity name for a
/// service host, as opposed to the unix user for a personal workstation.
pub fn current_hostname() -> Option<String> {
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

/// Best-effort current username, for prompt defaults only — a *suggestion*, so
/// it never fails. Prefers the passwd entry on unix so it agrees with the
/// workstation owner derived the same way.
pub fn current_username() -> Option<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_identity_uses_only_the_intended_canonical_paths() {
        let name = "dry-run.example.com";
        let dir = tls::identity_dir(name).unwrap();
        let spec = planned_identity_spec(name).unwrap();

        assert_eq!(&*spec.server_pattern, "example.com");
        assert_eq!(&*spec.our_name, name);
        assert_eq!(spec.certificate, dir.join("certificate.pem"));
        assert_eq!(spec.private_key, dir.join("private.key"));
        assert_eq!(spec.trusted, dir.join("trusted.pem"));
        assert!(spec.dest_dir.is_none());
        assert!(spec.askpass.is_none());
    }
}
