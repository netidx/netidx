//! Template engine: produce a complete `RenderedTemplate` (configs +
//! activation units + TLS install jobs) from a typed parameter struct.
//! The caller drives `apply()` to write everything atomically, or
//! `describe()` to render a human-readable plan for `--dry-run`.
//!
//! Three templates:
//! - [`workstation`] — local Local-auth resolver under `/local` plus a
//!   matching client; optional TLS identity for upstream connections.
//! - [`standalone_resolver`] — a single-machine resolver-server config
//!   plus optional seed perms.
//! - [`client_only`] — minimal client config pointing at explicit
//!   addresses.
//!
//! TLS in v1 is **explicit-paths only**: the operator supplies cert,
//! key, and trusted CA paths via [`AuthChoice::Tls`]. The template
//! emits a [`TlsCopyJob`] in the [`RenderedTemplate`] that
//! [`apply()`](RenderedTemplate::apply) carries out using
//! [`crate::tls::install_identity`]. Auto-issuance via the CA module
//! is documented in FUTURE.md.

use crate::{
    activation, client, id_map as id_map_engine, perms,
    resolver as resolver_engine, tls as tlsmod,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::{
    config::{DefaultAuthMech, file as cfile},
    resolver_server::config::{PMap, file as rfile},
};
use netidx_activation::file::Unit;
use netidx_id_map::file::IdMap;
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
};

pub mod publisher;
pub mod resolver;
pub mod services;
pub mod workstation;

pub use publisher::publisher;
pub use resolver::resolver;
pub use workstation::workstation;

/// Auth scheme the operator picks for *upstream* connections (in the
/// case of workstation: the parent referral and the client's identity
/// used when following it; in the case of resolver: the resolver's
/// own exposed auth; in the case of publisher: the client's identity).
#[derive(Debug, Clone)]
pub enum AuthChoice {
    Anonymous,
    Local {
        /// Path to the local-auth unix socket. Must be absolute.
        path: PathBuf,
    },
    Krb5 {
        /// Service Principal Name.
        spn: ArcStr,
    },
    Tls {
        /// Our SAN — what our cert claims as our identity.
        name: ArcStr,
        /// Source paths the template will copy into the canonical
        /// `~/.config/netidx/tls/<name>/` layout.
        certificate: PathBuf,
        private_key: PathBuf,
        trusted: PathBuf,
        /// Path to an askpass program — written to the
        /// `cfile::Tls.askpass` slot of any client config the
        /// template emits. Used when the private key is encrypted:
        /// netidx invokes this program at TLS-load time to fetch
        /// the passphrase if the system keychain doesn't have it.
        /// `None` leaves the config slot unset (the keychain is
        /// then the only source of decryption).
        askpass: Option<PathBuf>,
    },
}

/// One referral from the local resolver to its parent. Mirrors the
/// shape of `netidx::resolver_server::config::file::Referral` but with
/// our simpler `ReferralAuth` so the template inputs stay engine-side.
#[derive(Debug, Clone)]
pub struct ParentRef {
    /// Netidx path where the parent attaches. Use `/` for the root.
    pub path: ArcStr,
    pub ttl: Option<u16>,
    pub addrs: Vec<(SocketAddr, ReferralAuth)>,
}

/// Per-address auth for a parent referral.
#[derive(Debug, Clone)]
pub enum ReferralAuth {
    Anonymous,
    Local(ArcStr),
    Krb5(ArcStr),
    Tls(ArcStr),
}

impl ReferralAuth {
    fn into_referral_file(self) -> rfile::RefAuth {
        match self {
            Self::Anonymous => rfile::RefAuth::Anonymous,
            Self::Local(p) => rfile::RefAuth::Local(p),
            Self::Krb5(s) => rfile::RefAuth::Krb5(s),
            Self::Tls(n) => rfile::RefAuth::Tls(n),
        }
    }

    pub(crate) fn into_client_file(self) -> cfile::Auth {
        match self {
            Self::Anonymous => cfile::Auth::Anonymous,
            Self::Local(p) => cfile::Auth::Local(p),
            Self::Krb5(s) => cfile::Auth::Krb5(s),
            Self::Tls(n) => cfile::Auth::Tls(n),
        }
    }

}

/// One TLS identity to install and reference from `tls.identities`.
/// The cert/key/CA paths are copied into `dest_dir` (or the canonical
/// `~/.config/netidx/tls/<our_name>/` if `dest_dir` is `None`) and
/// `tls.identities[<server_pattern>]` points at the copies. Both the
/// install job and the in-config path use the same resolution, so
/// the generated config always references the installed location.
#[derive(Debug, Clone)]
pub struct TlsIdentitySpec {
    /// Domain pattern the identity is registered under in
    /// `tls.identities`. Netidx picks identities by reverse-domain
    /// match against the server's TLS name, so `"example.com"`
    /// covers any server SAN under `*.example.com`.
    pub server_pattern: ArcStr,
    /// Our SAN — drives the canonical install subdirectory name.
    pub our_name: ArcStr,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub trusted: PathBuf,
    /// Override the install destination. `None` ⇒ the canonical
    /// `${user_tls_dir}/<our_name>/` location.
    pub dest_dir: Option<PathBuf>,
    /// Path to an askpass program. When the private key is
    /// encrypted, netidx invokes this program at TLS-load time to
    /// fetch the passphrase (after checking the system keychain).
    /// Lives at the `cfile::Tls` *section* level in the on-disk
    /// config — not per-identity — but we carry it on the spec so
    /// the install flow can declare it alongside the matching key.
    /// When multiple identities share a section, the first
    /// non-`None` askpass wins.
    pub askpass: Option<PathBuf>,
}

impl TlsIdentitySpec {
    /// Resolve the install destination. Used by both `install_job`
    /// and the in-config path generation so they always agree.
    pub(crate) fn dest_dir(&self) -> Result<PathBuf> {
        match &self.dest_dir {
            Some(p) => Ok(p.clone()),
            None => tlsmod::identity_dir(self.our_name.as_str()),
        }
    }

    fn install_job(&self) -> Result<TlsCopyJob> {
        Ok(TlsCopyJob {
            cn: self.our_name.to_string(),
            dest_dir: self.dest_dir()?,
            certificate_src: self.certificate.clone(),
            private_key_src: self.private_key.clone(),
            trusted_src: self.trusted.clone(),
        })
    }
}

/// A pending "copy a TLS identity into place" operation. `apply`
/// executes these via [`crate::tls::install_identity`].
#[derive(Debug, Clone)]
pub struct TlsCopyJob {
    pub cn: String,
    pub dest_dir: PathBuf,
    pub certificate_src: PathBuf,
    pub private_key_src: PathBuf,
    pub trusted_src: PathBuf,
}

/// A bundle of artifacts a template intends to produce. Nothing is
/// written until `apply()` is called — that two-phase split lets the
/// CLI's `--dry-run` print the plan, lets tests assert against it,
/// and keeps the templates pure.
#[derive(Debug)]
pub struct RenderedTemplate {
    pub client_config: Option<(PathBuf, client::ClientConfig)>,
    pub resolver_config: Option<(PathBuf, resolver_engine::ResolverConfig)>,
    pub perms_file: Option<(PathBuf, PMap)>,
    /// Starter id-map JSON to drop on disk. Only set when a template
    /// wants the id-mapper daemon installed (currently
    /// `standalone-resolver --auth tls`). `apply()` writes this
    /// **only if the target file doesn't already exist** — re-running
    /// a template must not clobber operator edits.
    pub id_map_file: Option<(PathBuf, IdMap)>,
    /// Activation units keyed by basename (no `.unit` suffix). Empty
    /// when `units_dir` is `None`.
    pub units: BTreeMap<String, Unit>,
    /// Where to drop the unit files. `None` ⇒ skip the unit-write step
    /// (e.g. `--no-units`).
    pub units_dir: Option<PathBuf>,
    pub tls_install: Vec<TlsCopyJob>,
}

impl RenderedTemplate {
    /// Apply: install TLS identities, validate every artifact, then
    /// write atomically.
    ///
    /// Ordering matters here:
    /// - TLS identities install first because both `ClientConfig::validate`
    ///   and `ResolverConfig::validate` round-trip through
    ///   `Config::from_file`, which **opens the cert/key/CA paths from
    ///   disk** (via `Tls::load` on the client side and the TLS server
    ///   cert checks on the resolver side). A config that references
    ///   installed paths would otherwise fail validation on first
    ///   apply because those files don't exist yet.
    /// - The perms file lands before the resolver config save for the
    ///   same reason: when a template wires the perms path into
    ///   `include_permissions`, `Config::from_file` opens it during
    ///   resolver validation.
    /// - Cross-unit structural validation runs before any unit write
    ///   so trigger conflicts surface before we touch the activation
    ///   dir.
    /// - Per-config `save()` re-validates internally, so the final
    ///   write is gated on a post-install validation pass.
    ///
    /// **No rollback** on partial failure: a crash between, say, the
    /// client save and the resolver save leaves the client save on
    /// disk. The atomic primitives keep each individual file
    /// consistent, but the bundle is not transactional.
    pub fn apply(&self) -> Result<()> {
        // 1) Install TLS identities first so validators can find them.
        for job in &self.tls_install {
            tlsmod::install_identity(&tlsmod::InstallIdentity {
                cn: &job.cn,
                dest_dir: &job.dest_dir,
                certificate_src: &job.certificate_src,
                private_key_src: &job.private_key_src,
                trusted_src: &job.trusted_src,
            })
            .with_context(|| format!("installing TLS identity {}", job.cn))?;
        }

        // 2) Structural validate units as a set.
        activation::validate(&self.units).context("activation units validation")?;

        // 3) Save perms file before any config validation: resolver
        // configs that reference it via `include_permissions` would
        // otherwise fail to validate on first apply.
        if let Some((p, m)) = &self.perms_file {
            perms::save_perms(p, m)
                .with_context(|| format!("saving perms to {p:?}"))?;
        }

        // 3.5) Drop the starter id-map JSON in place, but only when
        // the target file doesn't already exist — operators who have
        // hand-edited it would not want a re-run of `init` to flatten
        // their map back to empty.
        if let Some((p, m)) = &self.id_map_file
            && !p.exists()
        {
            id_map_engine::save(p, m)
                .with_context(|| format!("saving id-map to {p:?}"))?;
        }

        // 4) Save configs (each re-validates internally).
        if let Some((p, c)) = &self.client_config {
            c.save(p).with_context(|| format!("saving client config to {p:?}"))?;
        }
        if let Some((p, r)) = &self.resolver_config {
            r.save(p)
                .with_context(|| format!("saving resolver config to {p:?}"))?;
        }

        // 5) Save activation units.
        if let Some(dir) = &self.units_dir
            && !self.units.is_empty()
        {
            let ad = activation::ActivationDir::open(Some(dir.as_path()))?;
            for (name, unit) in &self.units {
                ad.save(name, unit)
                    .with_context(|| format!("saving unit {name}"))?;
            }
        }

        Ok(())
    }

    /// Human-readable summary suitable for `--dry-run`.
    pub fn describe(&self) -> String {
        let mut out = String::new();
        if let Some((p, _)) = &self.client_config {
            let _ = writeln!(out, "client config → {p:?}");
        }
        if let Some((p, _)) = &self.resolver_config {
            let _ = writeln!(out, "resolver config → {p:?}");
        }
        if let Some((p, m)) = &self.perms_file {
            let _ = writeln!(out, "perms file → {p:?} ({} entries)", m.0.len());
        }
        if let Some((p, _)) = &self.id_map_file {
            let _ = writeln!(out, "id-map JSON → {p:?} (starter; preserved if file already exists)");
        }
        if let Some(dir) = &self.units_dir {
            for name in self.units.keys() {
                let _ = writeln!(out, "activation unit → {dir:?}/{name}.unit");
            }
        } else if !self.units.is_empty() {
            let _ = writeln!(out, "(units rendered but no units_dir; skipping write)");
        }
        for job in &self.tls_install {
            let _ = writeln!(
                out,
                "TLS identity {} → {:?} (cert: {:?}, key: {:?}, ca: {:?})",
                job.cn,
                job.dest_dir,
                job.certificate_src,
                job.private_key_src,
                job.trusted_src,
            );
        }
        if out.is_empty() {
            out.push_str("(empty plan)");
        }
        out
    }
}

// --- shared helpers used by individual templates ----------------------------

/// Build the `rfile::Auth` (resolver-side, includes TLS cert paths)
/// from an `AuthChoice`. For TLS the cert paths are the *installed*
/// paths (under `tls_dest`), not the original sources — `apply()`
/// installs into `tls_dest` before the resolver tries to load the
/// cert. Used by `standalone_resolver`, where the resolver owns its
/// identity.
pub(crate) fn resolver_auth_from(
    choice: &AuthChoice,
    tls_dest: &Path,
) -> rfile::Auth {
    match choice {
        AuthChoice::Anonymous => rfile::Auth::Anonymous,
        AuthChoice::Local { path } => {
            rfile::Auth::Local(ArcStr::from(path.to_string_lossy().as_ref()))
        }
        AuthChoice::Krb5 { spn } => rfile::Auth::Krb5(spn.clone()),
        AuthChoice::Tls { name, .. } => {
            let [certificate, private_key, trusted] =
                tlsmod::installed_files_in(tls_dest);
            rfile::Auth::Tls {
                name: name.clone(),
                trusted: ArcStr::from(trusted.to_string_lossy().as_ref()),
                certificate: ArcStr::from(certificate.to_string_lossy().as_ref()),
                private_key: ArcStr::from(private_key.to_string_lossy().as_ref()),
            }
        }
    }
}

/// Build a `cfile::Tls` from a list of [`TlsIdentitySpec`]s. Each
/// spec contributes one entry to `tls.identities`, keyed by its
/// `server_pattern`. Returns `Ok(None)` if the list is empty.
///
/// Path resolution goes through `TlsIdentitySpec::dest_dir`, the same
/// resolver `install_job` uses — so the in-config paths and the
/// install location always agree. A failure to resolve the destination
/// (e.g. no platform user-config dir) is propagated to the caller
/// rather than papered over with a `/tmp` fallback that would silently
/// diverge from where the install actually writes.
pub(crate) fn client_tls_section_from(
    identities: &[TlsIdentitySpec],
) -> Result<Option<cfile::Tls>> {
    if identities.is_empty() {
        return Ok(None);
    }
    // Reject duplicates explicitly so the operator finds out at
    // template time, not after silently losing one identity to
    // last-write-wins through the BTreeMap insert (or to two install
    // jobs racing on the same dest_dir).
    let mut seen_patterns: std::collections::BTreeSet<&str> =
        std::collections::BTreeSet::new();
    let mut seen_names: std::collections::BTreeSet<&str> =
        std::collections::BTreeSet::new();
    for spec in identities {
        if !seen_patterns.insert(spec.server_pattern.as_str()) {
            bail!(
                "duplicate TLS identity for server_pattern {:?}; the second one would silently overwrite the first in tls.identities",
                spec.server_pattern,
            );
        }
        if !seen_names.insert(spec.our_name.as_str()) {
            bail!(
                "two TLS identities share our_name {:?}; their install jobs would clobber each other on disk",
                spec.our_name,
            );
        }
    }

    let mut map = BTreeMap::new();
    let mut default = None;
    // `cfile::Tls.askpass` is a single section-level field; if
    // multiple identities have different askpass paths set we
    // take the first non-`None` and ignore the rest. In practice a
    // single template emits one identity, so this case is mostly
    // theoretical — but it's better to make the precedence explicit
    // than to silently let last-write-wins through a BTreeMap.
    let mut askpass: Option<String> = None;
    for spec in identities {
        let dest = spec.dest_dir()?;
        let [certificate, private_key, trusted] = tlsmod::installed_files_in(&dest);
        let identity = cfile::TlsIdentity {
            trusted: trusted.to_string_lossy().into_owned(),
            certificate: certificate.to_string_lossy().into_owned(),
            private_key: private_key.to_string_lossy().into_owned(),
        };
        let key = spec.server_pattern.to_string();
        if default.is_none() {
            default = Some(key.clone());
        }
        if askpass.is_none() {
            askpass = spec
                .askpass
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned());
        }
        map.insert(key, identity);
    }
    Ok(Some(cfile::Tls {
        default_identity: default,
        identities: map,
        askpass,
    }))
}

/// Build a TLS section for the *resolver-side* identity (used by
/// `standalone_resolver` when its `auth` is `AuthChoice::Tls`). Same
/// as the resolver's `Auth::Tls { ... }` but the install job is
/// produced separately.
pub(crate) fn resolver_tls_copy_job(
    choice: &AuthChoice,
) -> Result<Option<TlsCopyJob>> {
    let AuthChoice::Tls {
        name, certificate, private_key, trusted, askpass: _,
    } = choice
    else {
        return Ok(None);
    };
    let dest = tlsmod::identity_dir(name.as_str())?;
    Ok(Some(TlsCopyJob {
        cn: name.to_string(),
        dest_dir: dest,
        certificate_src: certificate.clone(),
        private_key_src: private_key.clone(),
        trusted_src: trusted.clone(),
    }))
}

/// Derive a sensible `DefaultAuthMech` from the auth schemes appearing
/// in a list of parent / addrs entries. Picks the "most upstream-y"
/// scheme present: Tls > Krb5 > Local > Anonymous. With no entries,
/// returns `Local` (workstation talking only to its local resolver).
pub(crate) fn derive_default_auth<I>(auths: I) -> DefaultAuthMech
where
    I: IntoIterator,
    I::Item: AsRef<ReferralAuth>,
{
    let mut has_tls = false;
    let mut has_krb5 = false;
    let mut has_local = false;
    let mut any = false;
    for a in auths {
        any = true;
        match a.as_ref() {
            ReferralAuth::Tls(_) => has_tls = true,
            ReferralAuth::Krb5(_) => has_krb5 = true,
            ReferralAuth::Local(_) => has_local = true,
            ReferralAuth::Anonymous => {}
        }
    }
    if has_tls {
        DefaultAuthMech::Tls
    } else if has_krb5 {
        DefaultAuthMech::Krb5
    } else if has_local {
        DefaultAuthMech::Local
    } else if any {
        DefaultAuthMech::Anonymous
    } else {
        // No upstream endpoints at all — workstation talks only to
        // its loopback resolver. Local-machine convention.
        DefaultAuthMech::Local
    }
}

impl AsRef<ReferralAuth> for ReferralAuth {
    fn as_ref(&self) -> &ReferralAuth {
        self
    }
}

/// Convert our `ParentRef` into a `rfile::Referral`.
pub(crate) fn parent_into_file(p: ParentRef) -> rfile::Referral {
    rfile::Referral {
        path: p.path,
        ttl: p.ttl,
        addrs: p
            .addrs
            .into_iter()
            .map(|(a, auth)| (a, auth.into_referral_file()))
            .collect(),
    }
}

