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
//! is documented in design/netidx-admin-future.md.

use crate::{
    activation, client, config_lock::ConfigDirLock, id_map as id_map_engine, perms,
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

#[cfg(test)]
pub(crate) struct TestIdentity {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[cfg(test)]
pub(crate) fn test_identity(
    ca_dir: &Path,
    identity_dir: &Path,
    name: &str,
) -> TestIdentity {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec!["test-ca".to_string()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages =
        vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![name.to_string()]).unwrap();
    params.extended_key_usages =
        vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
    let cert = params.signed_by(&key, &issuer).unwrap();
    std::fs::create_dir_all(ca_dir).unwrap();
    std::fs::create_dir_all(identity_dir).unwrap();
    std::fs::write(ca_dir.join("certificate.pem"), ca_cert.pem()).unwrap();
    let certificate = identity_dir.join("certificate.pem");
    let private_key = identity_dir.join("private.key");
    std::fs::write(&certificate, cert.pem()).unwrap();
    std::fs::write(&private_key, key.serialize_pem()).unwrap();
    TestIdentity { certificate, private_key }
}

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

    /// The scheme's canonical lowercase name (matches `AuthKind::as_str`), for
    /// recording the effective data-plane auth of a config assembled from
    /// per-referral auths rather than a single chosen `AuthKind`.
    pub(crate) fn scheme_str(&self) -> &'static str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Local(_) => "local",
            Self::Krb5(_) => "krb5",
            Self::Tls(_) => "tls",
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
    /// Coherence warnings: the plan is valid but the parameter
    /// combination is one only an expert should want (e.g. a TLS
    /// resolver with no id-mapper). Rendered by `describe()`, so they
    /// surface on dry runs too.
    pub warnings: Vec<ArcStr>,
}

impl RenderedTemplate {
    fn managed_path_refs(&self) -> impl Iterator<Item = &Path> {
        let Self {
            client_config,
            resolver_config,
            perms_file,
            id_map_file,
            units: _,
            units_dir,
            tls_install,
            warnings: _,
        } = self;
        client_config
            .iter()
            .map(|(path, _)| path.as_path())
            .chain(resolver_config.iter().map(|(path, _)| path.as_path()))
            .chain(perms_file.iter().map(|(path, _)| path.as_path()))
            .chain(id_map_file.iter().map(|(path, _)| path.as_path()))
            .chain(units_dir.iter().map(PathBuf::as_path))
            .chain(tls_install.iter().map(|job| job.dest_dir.as_path()))
    }

    /// Destinations this role template owns. Stored in install provenance so a
    /// later role-level backup can prove it did not silently omit a custom
    /// path outside the normal config root.
    pub fn managed_paths(&self) -> Vec<PathBuf> {
        let mut paths: Vec<_> = self.managed_path_refs().map(Path::to_path_buf).collect();
        paths.sort();
        paths.dedup();
        paths
    }

    /// Validate and read every fallible input that can be checked without
    /// touching an install destination. This is deliberately modest: each
    /// final file write is already atomic, and config validation that opens
    /// the final TLS/perms paths still has to run after those dependencies are
    /// installed. The useful guarantee here is that a missing later TLS source,
    /// invalid unit set, invalid id-map, or serialization error is found before
    /// the first destination write.
    pub fn preflight(&self) -> Result<()> {
        activation::validate(&self.units).context("activation units validation")?;

        if let Some((path, map)) = &self.id_map_file
            && !path.exists()
        {
            map.validate().context("id-map structural validation")?;
        }

        if let Some((_, config)) = &self.client_config {
            serde_json::to_vec_pretty(&config.0)
                .context("serializing client config during preflight")?;
        }
        if let Some((path, config)) = &self.resolver_config {
            serde_json::to_vec_pretty(config.as_file())
                .context("serializing resolver config during preflight")?;
            let prospective_perms =
                self.perms_file.as_ref().map(|(path, perms)| (path.as_path(), perms));
            config
                .preflight_permission_topology(path, prospective_perms)
                .context("validating resolver permission topology during preflight")?;
        }
        if let Some((_, permissions)) = &self.perms_file {
            serde_json::to_vec_pretty(permissions)
                .context("serializing permissions during preflight")?;
        }

        // Read the complete source bundle before install_identity writes the
        // first destination. In particular, don't install certificate.pem and
        // only then discover that private.key or trusted.pem is unreadable.
        for job in &self.tls_install {
            for source in [&job.certificate_src, &job.private_key_src, &job.trusted_src] {
                let bytes = zeroize::Zeroizing::new(std::fs::read(source).with_context(
                    || {
                        format!(
                            "reading TLS source {} during preflight",
                            source.display()
                        )
                    },
                )?);
                if bytes.is_empty() {
                    anyhow::bail!("TLS source {} is empty", source.display());
                }
            }
            let sidecar = tlsmod::sealed_sidecar(&job.private_key_src);
            if sidecar.exists() {
                let _ = zeroize::Zeroizing::new(std::fs::read(&sidecar).with_context(
                    || {
                        format!(
                            "reading sealed TLS key sidecar {} during preflight",
                            sidecar.display()
                        )
                    },
                )?);
            }
        }
        Ok(())
    }

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
    /// A preflight pass catches fallible inputs before the first destination
    /// write. There is intentionally no multi-file journal: the atomic
    /// primitives keep each file consistent, while interruption between files
    /// may leave a partial but parseable bundle.
    pub fn apply(&self, config_lock: &ConfigDirLock) -> Result<()> {
        for path in self.managed_path_refs() {
            config_lock.require_contained(path)?;
        }
        self.preflight()?;

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

        // 2) Save perms file before any config validation: resolver
        // configs that reference it via `include_permissions` would
        // otherwise fail to validate on first apply.
        if let Some((p, m)) = &self.perms_file {
            perms::save_perms(p, m).with_context(|| format!("saving perms to {p:?}"))?;
        }

        // 2.5) Drop the starter id-map JSON in place, but only when
        // the target file doesn't already exist — operators who have
        // hand-edited it would not want a re-run of `init` to flatten
        // their map back to empty.
        if let Some((p, m)) = &self.id_map_file
            && !p.exists()
        {
            id_map_engine::save(p, m)
                .with_context(|| format!("saving id-map to {p:?}"))?;
        }

        // 3) Save configs (each re-validates internally).
        if let Some((p, c)) = &self.client_config {
            c.save(p).with_context(|| format!("saving client config to {p:?}"))?;
        }
        if let Some((p, r)) = &self.resolver_config {
            r.save(p).with_context(|| format!("saving resolver config to {p:?}"))?;
        }

        // 4) Save activation units.
        if let Some(dir) = &self.units_dir
            && !self.units.is_empty()
        {
            let ad = activation::ActivationDir::open(Some(dir.as_path()))?;
            for (name, unit) in &self.units {
                ad.save(name, unit).with_context(|| format!("saving unit {name}"))?;
            }
        }

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn apply_test(&self, root: &Path) -> Result<()> {
        let config_lock = ConfigDirLock::acquire(root)?;
        self.apply(&config_lock)
    }

    /// Human-readable summary suitable for `--dry-run`.
    pub fn describe(&self) -> String {
        let mut out = String::new();
        if let Some((p, c)) = &self.client_config {
            let _ = writeln!(out, "client config → {p:?}");
            // Surface the connection the config encodes so a --dry-run
            // actually previews "am I pointing at the right resolver with
            // the right auth?" rather than just naming the output file.
            let cfg = &c.0;
            for (addr, auth) in &cfg.addrs {
                let _ =
                    writeln!(out, "    resolver {addr} ({})", describe_client_auth(auth));
            }
            if let Some(bind) = &cfg.default_bind_config {
                let _ = writeln!(out, "    publisher bind: {bind}");
            }
        }
        if let Some((p, r)) = &self.resolver_config {
            let _ = writeln!(out, "resolver config → {p:?}");
            // Show what this resolver listens as, and (the easy thing to
            // get wrong) the parent referral the operator just typed —
            // its address and auth/SPN — so --dry-run previews it.
            let cfg = &r.0;
            for m in &cfg.member_servers {
                let _ = writeln!(
                    out,
                    "    listen {} (bind {}, {})",
                    m.addr,
                    m.bind_addr,
                    describe_member_auth(&m.auth),
                );
            }
            if let Some(parent) = &cfg.parent {
                for (addr, auth) in &parent.addrs {
                    let _ = writeln!(
                        out,
                        "    parent {addr} ({}) — attaches at {:?}",
                        describe_ref_auth(auth),
                        parent.path,
                    );
                }
            }
        }
        if let Some((p, m)) = &self.perms_file {
            let _ = writeln!(out, "perms file → {p:?} ({} entries)", m.0.len());
        }
        if let Some((p, _)) = &self.id_map_file {
            let _ = writeln!(
                out,
                "id-map JSON → {p:?} (starter; preserved if file already exists)"
            );
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
        for w in &self.warnings {
            let _ = writeln!(out, "warning: {w}");
        }
        if out.is_empty() {
            out.push_str("(empty plan)");
        }
        out
    }
}

/// Build the edits that attach an already-installed workstation/client
/// to an admin domain — the engine half of `workstation join`. Loads the
/// existing resolver + client configs, adds `parent` to the resolver,
/// sets the client's `default_auth` (derived from the parent's auth) and
/// TLS identities, and returns a [`RenderedTemplate`] that touches ONLY
/// those two configs plus the cert install. Perms, units, and every
/// unrelated field are preserved — this edits, it never re-renders. Use
/// the returned template's `describe()`/`apply()` like any install.
///
/// Errors if the resolver already carries a parent referral: re-joining
/// a different admin domain is a separate, more careful operation.
pub fn attach_to_admin_domain(
    resolver_config_path: &Path,
    client_config_path: &Path,
    parent: ParentRef,
    tls_identities: Vec<TlsIdentitySpec>,
) -> Result<RenderedTemplate> {
    // The resolver-only half (set the parent referral) is shared with
    // `resolver add-parent`.
    let mut rt = set_parent_referral(resolver_config_path, parent.clone())?;

    let mut ccfg = client::ClientConfig::load(client_config_path).with_context(|| {
        format!("loading client config {}", client_config_path.display())
    })?;
    ccfg.0.default_auth = derive_default_auth(parent.addrs.iter().map(|(_, a)| a));
    if let Some(tls) = client_tls_section_from(&tls_identities)? {
        // A local-only workstation has no TLS section; a join that
        // enrolled a cert adds one.
        ccfg.0.tls = Some(tls);
    }

    rt.client_config = Some((client_config_path.to_path_buf(), ccfg));
    rt.tls_install =
        tls_identities.iter().map(|s| s.install_job()).collect::<Result<Vec<_>>>()?;
    Ok(rt)
}

/// Set the `parent` referral on an existing resolver config — the
/// resolver-only half of attaching to an admin domain, for `resolver
/// add-parent` (and reused by [`attach_to_admin_domain`]). Returns a
/// [`RenderedTemplate`] touching only the resolver config (no client
/// edit, no cert install). Refuses a resolver that already has a parent —
/// re-parenting a different admin domain is a separate, more careful op.
pub fn set_parent_referral(
    resolver_config_path: &Path,
    parent: ParentRef,
) -> Result<RenderedTemplate> {
    let rcfg = resolver_engine::ResolverConfig::load(resolver_config_path).with_context(
        || format!("loading resolver config {}", resolver_config_path.display()),
    )?;
    set_parent_referral_on(resolver_config_path, rcfg, parent)
}

pub(crate) fn set_parent_referral_on(
    resolver_config_path: &Path,
    mut rcfg: resolver_engine::ResolverConfig,
    parent: ParentRef,
) -> Result<RenderedTemplate> {
    if rcfg.as_file().parent.is_some() {
        bail!(
            "this resolver already has a parent referral — it's already attached \
             to an admin domain. Re-parenting isn't supported yet (uninstall + \
             reinstall to switch admin domains)."
        );
    }
    rcfg.as_file_mut().parent = Some(parent_into_file(parent));
    Ok(RenderedTemplate {
        client_config: None,
        resolver_config: Some((resolver_config_path.to_path_buf(), rcfg)),
        perms_file: None,
        id_map_file: None,
        units: BTreeMap::new(),
        units_dir: None,
        tls_install: Vec::new(),
        warnings: Vec::new(),
    })
}

/// Whether the resolver already carries exactly this parent referral. Address
/// order is not significant: CA topology fanout canonicalizes it, but
/// hand-written netidx configuration does not have to.
pub fn parent_referral_matches(
    resolver_config_path: &Path,
    expected: &ParentRef,
) -> Result<bool> {
    let rcfg = resolver_engine::ResolverConfig::load(resolver_config_path).with_context(
        || format!("loading resolver config {}", resolver_config_path.display()),
    )?;
    Ok(parent_referral_matches_config(&rcfg, expected))
}

pub(crate) fn parent_referral_matches_config(
    rcfg: &resolver_engine::ResolverConfig,
    expected: &ParentRef,
) -> bool {
    let Some(actual) = rcfg.as_file().parent.as_ref() else {
        return false;
    };
    if actual.path != expected.path
        || actual.ttl != expected.ttl
        || actual.addrs.len() != expected.addrs.len()
    {
        return false;
    }
    expected.addrs.iter().all(|(addr, auth)| {
        actual.addrs.iter().any(|(got_addr, got_auth)| {
            got_addr == addr
                && matches!(
                    (got_auth, auth),
                    (rfile::RefAuth::Anonymous, ReferralAuth::Anonymous)
                        | (rfile::RefAuth::Local(_), ReferralAuth::Local(_))
                        | (rfile::RefAuth::Krb5(_), ReferralAuth::Krb5(_))
                        | (rfile::RefAuth::Tls(_), ReferralAuth::Tls(_))
                )
                && match (got_auth, auth) {
                    (rfile::RefAuth::Local(got), ReferralAuth::Local(want))
                    | (rfile::RefAuth::Krb5(got), ReferralAuth::Krb5(want))
                    | (rfile::RefAuth::Tls(got), ReferralAuth::Tls(want)) => got == want,
                    (rfile::RefAuth::Anonymous, ReferralAuth::Anonymous) => true,
                    _ => false,
                }
        })
    })
}

/// One-line description of a client-side resolver auth, for the
/// `--dry-run` plan and `status`.
pub fn describe_client_auth(auth: &cfile::Auth) -> String {
    match auth {
        cfile::Auth::Anonymous => "anonymous".to_string(),
        cfile::Auth::Krb5(spn) => format!("krb5, spn {spn}"),
        cfile::Auth::Local(path) => format!("local, socket {path}"),
        cfile::Auth::Tls(name) => format!("tls, name {name}"),
    }
}

/// One-line description of a resolver member-server's auth.
pub fn describe_member_auth(auth: &rfile::Auth) -> String {
    match auth {
        rfile::Auth::Anonymous => "anonymous".to_string(),
        rfile::Auth::Krb5(spn) => format!("krb5, spn {spn}"),
        rfile::Auth::Local(path) => format!("local, socket {path}"),
        rfile::Auth::Tls { name, .. } => format!("tls, name {name}"),
    }
}

/// One-line description of a parent referral's auth.
pub fn describe_ref_auth(auth: &rfile::RefAuth) -> String {
    match auth {
        rfile::RefAuth::Anonymous => "anonymous".to_string(),
        rfile::RefAuth::Krb5(spn) => format!("krb5, spn {spn}"),
        rfile::RefAuth::Local(path) => format!("local, socket {path}"),
        rfile::RefAuth::Tls(name) => format!("tls, name {name}"),
    }
}

// --- shared helpers used by individual templates ----------------------------

/// Build the `rfile::Auth` (resolver-side, includes TLS cert paths)
/// from an `AuthChoice`. For TLS the cert paths are the *installed*
/// paths (under `tls_dest`), not the original sources — `apply()`
/// installs into `tls_dest` before the resolver tries to load the
/// cert. Used by `standalone_resolver`, where the resolver owns its
/// identity.
pub(crate) fn resolver_auth_from(choice: &AuthChoice, tls_dest: &Path) -> rfile::Auth {
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
            askpass = spec.askpass.as_ref().map(|p| p.to_string_lossy().into_owned());
        }
        map.insert(key, identity);
    }
    Ok(Some(cfile::Tls { default_identity: default, identities: map, askpass }))
}

/// Build a TLS section for the *resolver-side* identity (used by
/// `standalone_resolver` when its `auth` is `AuthChoice::Tls`). Same
/// as the resolver's `Auth::Tls { ... }` but the install job is
/// produced separately.
pub(crate) fn resolver_tls_copy_job(
    choice: &AuthChoice,
    config_dir: &Path,
) -> Result<Option<TlsCopyJob>> {
    let AuthChoice::Tls { name, certificate, private_key, trusted, askpass: _ } = choice
    else {
        return Ok(None);
    };
    let dest = tlsmod::identity_dir_in(&config_dir.join("tls"), name.as_str())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    const LOCAL_MEMBER: &str = r#"{"addr":"127.0.0.1:4654","bind_addr":"127.0.0.1","auth":"Anonymous","hello_timeout":10,"max_connections":768,"pid_file":"","reader_ttl":60,"writer_ttl":120,"id_map_command":null,"id_map_type":"DoNotMap","id_map_timeout":3600}"#;
    const LOCAL_CLIENT: &str = r#"{"base":"/local","addrs":[["127.0.0.1:4654","Anonymous"]],"tls":null,"default_auth":"Local","default_bind_config":null}"#;

    fn anon_parent() -> ParentRef {
        ParentRef {
            path: ArcStr::from("/local"),
            ttl: None,
            addrs: vec![("10.0.0.1:4564".parse().unwrap(), ReferralAuth::Anonymous)],
        }
    }

    #[test]
    fn attach_to_admin_domain_adds_parent_and_derives_default_auth() {
        let dir = tempfile::tempdir().unwrap();
        // A local-only workstation: resolver with no parent, client with
        // default_auth Local.
        let rpath = write(
            dir.path(),
            "resolver.json",
            &format!(
                r#"{{"children":[],"parent":null,"member_servers":[{LOCAL_MEMBER}],"perms":{{}},"include_permissions":[]}}"#
            ),
        );
        let cpath = write(dir.path(), "client.json", LOCAL_CLIENT);

        let rt = attach_to_admin_domain(&rpath, &cpath, anon_parent(), vec![]).unwrap();
        rt.apply_test(dir.path()).unwrap();

        // The resolver gained the parent referral...
        let rcfg = resolver_engine::ResolverConfig::load(&rpath).unwrap();
        let parent = rcfg.as_file().parent.as_ref().expect("parent referral added");
        assert_eq!(parent.addrs.len(), 1);
        assert_eq!(parent.addrs[0].0, "10.0.0.1:4564".parse().unwrap());
        // ...and the client's default_auth is derived from the parent (anon).
        let ccfg = client::ClientConfig::load(&cpath).unwrap();
        assert!(matches!(ccfg.0.default_auth, DefaultAuthMech::Anonymous));
    }

    #[test]
    fn attach_refuses_a_resolver_that_already_has_a_parent() {
        let dir = tempfile::tempdir().unwrap();
        let rpath = write(
            dir.path(),
            "resolver.json",
            &format!(
                r#"{{"children":[],"parent":{{"path":"/local","ttl":null,"addrs":[["10.9.9.9:4564","Anonymous"]]}},"member_servers":[{LOCAL_MEMBER}],"perms":{{}},"include_permissions":[]}}"#
            ),
        );
        let cpath = write(dir.path(), "client.json", LOCAL_CLIENT);
        assert!(attach_to_admin_domain(&rpath, &cpath, anon_parent(), vec![]).is_err());
    }

    #[test]
    fn approved_parent_match_is_idempotent_and_order_independent() {
        let dir = tempfile::tempdir().unwrap();
        let rpath = write(
            dir.path(),
            "resolver.json",
            &format!(
                r#"{{"children":[],"parent":{{"path":"/ap","ttl":null,"addrs":[["10.0.0.2:4564","Anonymous"],["10.0.0.1:4564","Anonymous"]]}},"member_servers":[{LOCAL_MEMBER}],"perms":{{}},"include_permissions":[]}}"#
            ),
        );
        let expected = ParentRef {
            path: "/ap".into(),
            ttl: None,
            addrs: vec![
                ("10.0.0.1:4564".parse().unwrap(), ReferralAuth::Anonymous),
                ("10.0.0.2:4564".parse().unwrap(), ReferralAuth::Anonymous),
            ],
        };
        assert!(parent_referral_matches(&rpath, &expected).unwrap());
        let wrong = ParentRef { path: "/other".into(), ..expected };
        assert!(!parent_referral_matches(&rpath, &wrong).unwrap());
    }
}
