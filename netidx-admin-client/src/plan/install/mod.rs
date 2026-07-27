//! The install planner: the shared tail every role install runs (describe →
//! apply → post-apply → service offer), plus the small pure helpers around it.
//!
//! The role-specific cascades (`resolver`, `workstation`, `publisher`) live in
//! the submodules; this module holds what they share. Everything speaks to the
//! operator through the [`Answerer`] seam only.

use crate::{
    activation,
    answer::{Answerer, Field},
    config_lock::ConfigDirLock,
    paths,
    plan::{
        enroll::{self, AdminServers},
        service::{ServiceGate, ServiceNeed, offer},
    },
    provenance::{InstallRecord, TrustDomainIdentity},
    resolver_probe,
    service::ServiceScope,
    template::RenderedTemplate,
    tls,
};
use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

/// role install cascades.
pub mod publisher;
pub mod workstation;

// -- environment-shape facade (feature-gated) ---------------------------------
//
// The bind/listen suggestions come from `netshape`, which is behind the
// `cloud-detect` feature (it pulls an HTTP client + interface enumeration).
// The install planner routes every use through these helpers so the cascades
// stay cfg-free and the crate still builds without the feature — degrading to
// "no suggestion, ask the operator" rather than failing to compile.

/// The IP a trust domain daemon on this host should advertise, if the environment
/// can be probed. `None` without `cloud-detect`.
///
/// `NetShape::detect` does blocking work — interface enumeration plus a
/// cloud-metadata probe that drives its own current-thread runtime — so it
/// runs on a blocking thread rather than the async worker, where the nested
/// runtime would panic ("cannot start a runtime from within a runtime").
#[cfg(feature = "cloud-detect")]
pub async fn detected_advertised_ip() -> Option<IpAddr> {
    tokio::task::spawn_blocking(|| {
        crate::netshape::NetShape::detect().advertised_ip().into()
    })
    .await
    .ok()
}

/// See the `cloud-detect` variant.
#[cfg(not(feature = "cloud-detect"))]
pub async fn detected_advertised_ip() -> Option<IpAddr> {
    None
}

/// A suggested publisher bind (`BindCfg`) plus whether the environment needs
/// an operator hint (a `<PUBLIC_IP>` placeholder to fill). `(None, false)`
/// without `cloud-detect`.
#[cfg(feature = "cloud-detect")]
pub async fn publisher_bind_shape() -> (Option<String>, bool) {
    tokio::task::spawn_blocking(|| {
        let shape = crate::netshape::NetShape::detect();
        (Some(shape.publisher_bind_suggestion()), shape.needs_operator_hint())
    })
    .await
    .unwrap_or((None, false))
}

/// See the `cloud-detect` variant.
#[cfg(not(feature = "cloud-detect"))]
pub async fn publisher_bind_shape() -> (Option<String>, bool) {
    (None, false)
}

/// The environment shape a resolver install needs to suggest its listen / bind
/// addresses. Computed once (detection probes cloud metadata); a resolver
/// install computes it lazily, only when `--listen`/`--bind` weren't given.
pub struct ResolverShape {
    /// The IP to advertise to clients (the listen default). `None` when the
    /// environment can't be probed (`cloud-detect` off).
    pub advertised_ip: Option<IpAddr>,
    /// The environment couldn't determine a public IP (container with no
    /// metadata) — the suggestion is only the private IP; warn about it.
    pub needs_operator_hint: bool,
    /// A separate local NIC to bind to when advertising a different (NAT'd)
    /// public IP.
    pub bind_override: Option<IpAddr>,
    /// The local publisher's `BindCfg::Elastic` string on a cloud-elastic
    /// host (advertise the public IP, bind the private subnet).
    pub elastic_local_client_bind: Option<String>,
}

pub fn warn_incomplete_resolver_address(ans: &mut dyn Answerer, shape: &ResolverShape) {
    if shape.needs_operator_hint {
        ans.warn(
            "detected container environment with no NETIDX_PUBLIC_IP env var \
             and no reachable cloud metadata. The suggested IP is the \
             container's private IP — only useful for internal traffic. \
             Override with the externally-visible address (or set \
             NETIDX_PUBLIC_IP / pass --listen).",
        );
    }
}

/// Detect the resolver environment shape (see [`ResolverShape`]).
#[cfg(feature = "cloud-detect")]
pub async fn detect_resolver_shape() -> ResolverShape {
    tokio::task::spawn_blocking(|| {
        use crate::netshape::NetShape;
        let s = NetShape::detect();
        ResolverShape {
            advertised_ip: Some(s.advertised_ip().into()),
            needs_operator_hint: s.needs_operator_hint(),
            bind_override: s.resolver_bind_override().map(IpAddr::V4),
            elastic_local_client_bind: match &s {
                NetShape::CloudElastic { .. } => Some(s.publisher_bind_suggestion()),
                NetShape::Public { .. }
                | NetShape::ContainerPrivate { .. }
                | NetShape::Private { .. }
                | NetShape::Loopback => None,
            },
        }
    })
    .await
    .unwrap_or(ResolverShape {
        advertised_ip: None,
        needs_operator_hint: false,
        bind_override: None,
        elastic_local_client_bind: None,
    })
}

/// See the `cloud-detect` variant.
#[cfg(not(feature = "cloud-detect"))]
pub async fn detect_resolver_shape() -> ResolverShape {
    ResolverShape {
        advertised_ip: None,
        needs_operator_hint: false,
        bind_override: None,
        elastic_local_client_bind: None,
    }
}

// -- environment resolvers (paths) --------------------------------------------

/// Resolve `--units-dir` / `--no-units` into the template's
/// `units_dir: Option<PathBuf>` contract: `no_units` ⇒ `None`; an explicit
/// dir ⇒ `Some`; neither ⇒ `Some(<user activation dir>)`.
pub fn resolve_units_dir(
    no_units: bool,
    units_dir: Option<&Path>,
) -> Result<Option<PathBuf>> {
    if no_units {
        Ok(None)
    } else if let Some(d) = units_dir {
        Ok(Some(d.to_path_buf()))
    } else {
        Ok(Some(crate::paths::user_activation_dir()?))
    }
}

/// Resolve `--netidx-binary` to an *absolute* path for an activation unit's
/// `ExecStart` (netidx-activation doesn't search `$PATH`). `None` ⇒
/// `current_exe()`; a relative path is rejected.
pub fn resolve_netidx_binary(provided: Option<PathBuf>) -> Result<PathBuf> {
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

/// The install-wide flags every role's `install` shares, lifted off its clap
/// struct into a plain input the library acts on.
#[derive(Debug, Clone)]
pub enum InstallMode {
    DryRun,
    Apply { config_lock: ConfigDirLock },
}

impl InstallMode {
    pub fn is_dry_run(&self) -> bool {
        matches!(self, Self::DryRun)
    }

    pub fn config_lock(&self) -> Option<&ConfigDirLock> {
        match self {
            Self::DryRun => None,
            Self::Apply { config_lock } => Some(config_lock),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstallCommon {
    /// Print the plan and change nothing.
    pub mode: InstallMode,
    /// Overwrite existing config files instead of refusing.
    pub force: bool,
    /// Don't drop activation unit files.
    pub no_units: bool,
    /// Register the OS service without asking.
    pub with_service: bool,
    /// Skip the OS-service offer entirely.
    pub no_service: bool,
}

/// Refuse to clobber operator material: bail (unless `force`) if any config,
/// perms file, activation unit, or TLS install destination this template would
/// write already exists. A TLS copy job whose source *is* its destination is
/// the local-CA generate path installing files it just produced — never a
/// clobber, so it's exempt.
pub fn check_no_overwrite(rt: &RenderedTemplate, force: bool) -> Result<()> {
    if force {
        return Ok(());
    }
    let mut existing: Vec<std::path::PathBuf> = Vec::new();
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
    // Activation unit files are real generated artifacts written by `apply()`
    // with no per-unit existence guard, so they need the same --force
    // protection as the configs above.
    if let Some(dir) = &rt.units_dir {
        for name in rt.units.keys() {
            let p = activation::unit_path_in(dir, name);
            if p.exists() {
                existing.push(p);
            }
        }
    }
    // TLS install copies cert/key/CA into <dest_dir>/, overwriting
    // unconditionally. Only a destination that differs from its source can
    // overwrite something this run didn't create.
    for job in &rt.tls_install {
        let srcs = [&job.certificate_src, &job.private_key_src, &job.trusted_src];
        for (dst, src) in tls::installed_files_in(&job.dest_dir).iter().zip(srcs) {
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

/// Extract from a probe the trust domain identity (domain + CA fingerprint) to pin
/// later lifecycle ops to, and a reachable admin-server address to start from.
/// `(None, None)` when the install didn't join a *discovered* trust domain (a
/// CLI-flag parent, the manual prompt cascade, or no parent at all carry no
/// confirmed identity).
pub fn trust_domain_provenance(
    probe: &AdminServers,
) -> (Option<TrustDomainIdentity>, Option<SocketAddr>) {
    match probe.have() {
        Some(net) => {
            let id = TrustDomainIdentity::new(
                net.identity.domain.clone(),
                &net.identity.fingerprint,
            );
            (Some(id), net.info.reached.first().copied())
        }
        None => (None, None),
    }
}

/// Drop the certificate-renewal activation unit into `units_dir`, so every
/// host with TLS identities renews on its own. Idempotent overwrite.
pub fn install_renew_unit(ans: &mut dyn Answerer, units_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(units_dir)
        .with_context(|| format!("creating activation dir {}", units_dir.display()))?;
    let netidx_binary = std::env::current_exe()
        .context("could not determine current netidx binary for the renew unit")?;
    let unit = crate::template::services::renew::unit(
        &crate::template::services::renew::RenewServiceParams { netidx_binary },
    )?;
    let dir = activation::ActivationDir::open(Some(units_dir))?;
    dir.save("renew", &unit).context("writing the renew activation unit")?;
    ans.note(&format_compact!(
        "activation unit → {}",
        activation::unit_path_in(units_dir, "renew").display()
    ));
    Ok(())
}

/// Describe + apply the rendered template, record the usable core install,
/// then run a post-apply step (never on `--dry-run`) and make the single
/// end-of-install OS-service decision. Returns the scope the frontend should
/// install a service at, or `None`.
///
/// `post_apply` receives the `Answerer` and install-wide config lock. It is an
/// `AsyncFnOnce` because standing up the admin server is trust domain I/O.
pub async fn finish_with(
    ans: &mut dyn Answerer,
    rt: RenderedTemplate,
    common: &InstallCommon,
    need: ServiceNeed,
    record: InstallRecord,
    post_apply: impl AsyncFnOnce(&mut dyn Answerer, &ConfigDirLock) -> Result<()>,
) -> Result<Option<ServiceScope>> {
    finish_with_record_path(ans, rt, common, need, record, None, post_apply).await
}

async fn finish_with_record_path(
    ans: &mut dyn Answerer,
    rt: RenderedTemplate,
    common: &InstallCommon,
    need: ServiceNeed,
    mut record: InstallRecord,
    record_path_override: Option<&Path>,
    post_apply: impl AsyncFnOnce(&mut dyn Answerer, &ConfigDirLock) -> Result<()>,
) -> Result<Option<ServiceScope>> {
    ans.note(&rt.describe());
    match &common.mode {
        InstallMode::DryRun => {}
        InstallMode::Apply { config_lock } => {
            check_no_overwrite(&rt, common.force)?;
            let record_path = match record_path_override {
                Some(path) => path.to_path_buf(),
                None => paths::user_install_record()
                    .context("resolving the install-record destination")?,
            };
            let record_path = config_lock.require_contained(record_path)?;
            record.set_managed_paths(rt.managed_paths());
            rt.apply(config_lock).context("applying template")?;
            record
                .save_async(config_lock, &record_path)
                .await
                .context("writing the install record")?;
            ans.note("ok");
            post_apply(ans, config_lock).await.with_context(|| {
                format!(
                    "the {} core install completed and is recorded at {}, but its \
                     post-install setup did not finish",
                    record.role.as_str(),
                    record_path.display()
                )
            })?;
        }
    }
    offer(
        ans,
        need,
        ServiceGate {
            dry_run: common.mode.is_dry_run(),
            no_service: common.no_service,
            with_service: common.with_service,
        },
    )
    .await
}

// -- shared prompt helpers (resolver addressing + TLS names) ------------------

/// The conventional resolver port.
pub const DEFAULT_RESOLVER_PORT: u16 = 4564;
/// The conventional leftmost label of a resolver's TLS SAN.
pub const DEFAULT_RESOLVER_NAME: &str = "resolver";
/// The default TLS domain (`<name>.<domain>`, e.g. `resolver.ryu-oh.org`;
/// `local` covers the single-host / no-DNS case the way `BindCfg::Local` does).
pub const DEFAULT_TLS_DOMAIN: &str = "local";

/// Prompt for a resolver port (default 4564) and pair it with `ip`. Re-asks on a
/// malformed port when interactive rather than aborting the install.
pub async fn prompt_resolver_port(
    ans: &mut dyn Answerer,
    ip: IpAddr,
    provided: Option<u16>,
) -> Result<SocketAddr> {
    if let Some(p) = provided {
        return Ok(SocketAddr::new(ip, p));
    }
    let default = DEFAULT_RESOLVER_PORT.to_string();
    loop {
        let raw = ans.text(Field::ResolverPort, None, Some(&default), false).await?;
        let s = raw.as_deref().map(str::trim).unwrap_or("");
        if s.is_empty() {
            return Ok(SocketAddr::new(ip, DEFAULT_RESOLVER_PORT));
        }
        match s.parse::<u16>() {
            Ok(port) => return Ok(SocketAddr::new(ip, port)),
            Err(_) if ans.interactive() => {
                ans.warn(&format!("{s:?} is not a valid port (1-65535) — try again"))
            }
            Err(e) => return Err(anyhow::Error::new(e).context("invalid resolver port")),
        }
    }
}

/// Ask for a resolver address as either a bare IP (then prompt for the port) or
/// a full `host:port`, re-asking on a malformed entry instead of aborting the
/// whole install. `default_ip` is offered on blank input; `required` makes a
/// value mandatory. Returns `None` only when not required and left blank.
///
/// In non-interactive (strict) mode the underlying [`Answerer::text`] errors on
/// a missing required value and this bails on a bad one, so it never loops.
pub async fn prompt_ip_or_addr(
    ans: &mut dyn Answerer,
    field: Field,
    default_ip: Option<&str>,
    required: bool,
) -> Result<Option<SocketAddr>> {
    loop {
        let raw = ans.text(field, None, default_ip, required).await?;
        let s = raw.as_deref().map(str::trim).unwrap_or("").to_string();
        if s.is_empty() {
            return Ok(None);
        }
        if let Ok(addr) = s.parse::<SocketAddr>() {
            return Ok(Some(addr));
        }
        if let Ok(ip) = s.parse::<IpAddr>() {
            return Ok(Some(prompt_resolver_port(ans, ip, None).await?));
        }
        if !ans.interactive() {
            bail!("{s:?} is not a valid IP or host:port (pass {})", field.flag());
        }
        ans.warn(&format!(
            "{s:?} is not a valid IP or host:port — enter e.g. 192.168.1.10 or 192.168.1.10:4564"
        ));
    }
}

/// Best-effort default for the TLS name a resolver presents. Probes the
/// resolver itself — the authoritative source, since it's the exact SAN a
/// client must match — and falls back to the `resolver.local` convention when
/// the probe can't reach or read it. Either way the value is only a default
/// the operator confirms; a wrong guess fails closed at connect time.
pub async fn resolver_tls_name_default(
    ans: &mut dyn Answerer,
    addr: SocketAddr,
) -> String {
    let convention = format!("{DEFAULT_RESOLVER_NAME}.{DEFAULT_TLS_DOMAIN}");
    match resolver_probe::probe_resolver_tls_name(addr).await {
        Ok(Some(name)) => {
            ans.note(&format_compact!(
                "probed resolver {addr}: it serves TLS name {name:?}"
            ));
            name
        }
        Ok(None) => {
            ans.note(&format_compact!(
                "resolver {addr} served a cert with no DNS SAN; defaulting to \
                 the {convention:?} convention"
            ));
            convention
        }
        Err(e) => {
            ans.note(&format_compact!(
                "could not probe resolver {addr} for its TLS name ({e}); \
                 defaulting to the {convention:?} convention"
            ));
            convention
        }
    }
}

/// Prompt for the TLS name a resolver serves (the client's `Auth::Tls`
/// `name`). A provided value short-circuits; otherwise, when we know the
/// resolver's address, prefill the default by probing it (convention
/// fallback). With no address to probe, prompt with no default. `field`
/// distinguishes the server's own name (`TlsName`) from a parent's
/// (`ParentTlsName`).
pub async fn prompt_resolver_tls_name(
    ans: &mut dyn Answerer,
    addr: Option<SocketAddr>,
    field: Field,
    provided: Option<String>,
) -> Result<String> {
    if let Some(p) = provided {
        return Ok(p);
    }
    match addr {
        Some(addr) => {
            let default = resolver_tls_name_default(ans, addr).await;
            Ok(ans.text(field, None, Some(&default), false).await?.unwrap_or(default))
        }
        None => {
            ans.text(field, None, None, true).await?.context("a TLS name is required")
        }
    }
}

/// Prompt for a resolver's *own* TLS SAN in two parts — a domain (default
/// `local`) and the leftmost name (default `resolver`) — joined into
/// `<name>.<domain>`. A `--tls-name` value short-circuits both with the full
/// SAN. `known_domain` (the control plane's already-chosen domain) short-
/// circuits just the domain part: the CA issues `*.<domain>`, so the resolver's
/// name must share that domain — we reuse it and prompt only for the label.
pub async fn prompt_resolver_own_tls_name(
    ans: &mut dyn Answerer,
    provided: Option<String>,
    known_domain: Option<&str>,
) -> Result<String> {
    if let Some(full) = provided {
        return Ok(full);
    }
    let domain = match known_domain {
        Some(d) => d.to_string(),
        None => ans
            .text(Field::TlsDomain, None, Some(DEFAULT_TLS_DOMAIN), false)
            .await?
            .unwrap_or_else(|| DEFAULT_TLS_DOMAIN.to_string()),
    };
    let name = ans
        .text(Field::ResolverName, None, Some(DEFAULT_RESOLVER_NAME), false)
        .await?
        .unwrap_or_else(|| DEFAULT_RESOLVER_NAME.to_string());
    let (name, domain) = (name.trim(), domain.trim());
    if name.is_empty() {
        bail!("resolver name must not be empty");
    }
    if domain.is_empty() {
        bail!("TLS domain must not be empty");
    }
    Ok(format!("{name}.{domain}"))
}

/// Suggest a client TLS SAN of the form `<user>.<domain>`, taking the domain
/// from the resolver's own SAN (netidx's identity convention). `None` if the
/// user can't be determined or the resolver SAN carries no domain.
pub fn suggest_client_san(resolver_san: &str) -> Option<String> {
    let user = enroll::current_username()?;
    let domain = tls::domain_from_san(resolver_san).ok()?;
    Some(format!("{user}.{domain}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admin_proto::Secret,
        answer::{Progress, TrustDomainChoice, TrustDomainOption},
        fingerprint::Fingerprint,
        provenance::InstallRole,
        template::{RenderedTemplate, TlsCopyJob},
        transport::CaIdentity,
    };
    use std::collections::BTreeMap;

    struct TestAnswerer;

    #[async_trait::async_trait]
    impl Answerer for TestAnswerer {
        fn interactive(&self) -> bool {
            false
        }

        async fn text(
            &mut self,
            _field: Field,
            _provided: Option<String>,
            _default: Option<&str>,
            _required: bool,
        ) -> Result<Option<String>> {
            unreachable!()
        }

        async fn choice(
            &mut self,
            _field: Field,
            _provided: Option<String>,
            _choices: &[&str],
            _default: Option<&str>,
        ) -> Result<String> {
            unreachable!()
        }

        async fn select_trust_domain(
            &mut self,
            _domains: &[TrustDomainOption],
        ) -> Result<TrustDomainChoice> {
            unreachable!()
        }

        async fn confirm(
            &mut self,
            _field: Field,
            _provided: Option<bool>,
            _default: bool,
        ) -> Result<bool> {
            unreachable!()
        }

        async fn secret(
            &mut self,
            _field: Field,
            _provided: Option<Secret>,
        ) -> Result<Secret> {
            unreachable!()
        }

        async fn announce(&mut self, _title: &str, _body: &str) -> Result<()> {
            unreachable!()
        }

        async fn announce_identity(
            &mut self,
            _body: &str,
            _code: &Fingerprint,
        ) -> Result<()> {
            unreachable!()
        }

        async fn confirm_identity(&mut self, _identity: &CaIdentity) -> Result<bool> {
            unreachable!()
        }

        fn show_verification_code(&mut self, _purpose: &str, _code: &Fingerprint) {
            unreachable!()
        }

        fn clear_verification_code(&mut self) {}

        fn progress(&mut self, _progress: Progress) {
            unreachable!()
        }

        fn note(&mut self, _message: &str) {}

        fn warn(&mut self, _message: &str) {}

        async fn show_recovery_password(&mut self, _password: &str) -> Result<()> {
            unreachable!()
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

    // `check_no_overwrite` must treat a copy job whose source == destination
    // as a no-op, never as clobbering operator material.
    #[test]
    fn self_copy_install_is_not_an_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().to_path_buf();
        let [cert, key, _trusted] = tls::installed_files_in(&dest);
        std::fs::write(&cert, b"cert").unwrap();
        std::fs::write(&key, b"key").unwrap();
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

    // A staged install whose source dir differs from the destination must
    // still refuse to clobber an identity already at the destination.
    #[test]
    fn foreign_source_over_existing_dest_is_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("dest");
        std::fs::create_dir_all(&dest).unwrap();
        let [cert_dst, key_dst, _trusted_dst] = tls::installed_files_in(&dest);
        std::fs::write(&cert_dst, b"old cert").unwrap();
        std::fs::write(&key_dst, b"old key").unwrap();
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
        assert!(check_no_overwrite(&rt, false).is_err());
        check_no_overwrite(&rt, true).unwrap();
    }

    #[test]
    fn preflight_reads_the_whole_tls_bundle_before_installing_any_of_it() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dest = dir.path().join("dest");
        std::fs::create_dir_all(&src).unwrap();
        let cert_src = src.join("certificate.pem");
        let missing_key = src.join("private.key");
        let trusted_src = src.join("trusted.pem");
        std::fs::write(&cert_src, b"cert").unwrap();
        std::fs::write(&trusted_src, b"trusted").unwrap();

        let mut rt = empty_rt();
        rt.tls_install.push(TlsCopyJob {
            cn: "resolver.example.com".to_string(),
            dest_dir: dest.clone(),
            certificate_src: cert_src,
            private_key_src: missing_key,
            trusted_src,
        });
        assert!(rt.apply_test(dir.path()).is_err());
        assert!(!dest.exists(), "preflight failure must precede destination writes");
    }

    #[test]
    fn preflight_rejects_invalid_resolver_topology_before_writing_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let resolver_path = dir.path().join("resolver.json");
        let perms_path = dir.path().join("perms.json");
        let raw = format!(
            r#"{{
                "children":[{{"path":"/eu","ttl":null,"addrs":[["10.0.0.1:4564","Anonymous"]]}}],
                "parent":null,
                "member_servers":[{{"addr":"10.0.0.2:4564","bind_addr":"10.0.0.2","auth":"Anonymous","hello_timeout":10,"max_connections":768,"pid_file":"","reader_ttl":60,"writer_ttl":120,"id_map_command":null,"id_map_type":"DoNotMap","id_map_timeout":3600}}],
                "perms":{{}},
                "include_permissions":[{:?}]
            }}"#,
            perms_path
        );
        let resolver: netidx::resolver_server::config::file::Config =
            serde_json::from_str(&raw).unwrap();
        let mut rt = empty_rt();
        rt.resolver_config = Some((resolver_path.clone(), resolver.into()));
        rt.perms_file = Some((perms_path.clone(), crate::perms::default_seed("/eu")));

        let err = rt.apply_test(dir.path()).unwrap_err();
        assert!(format!("{err:#}").contains("permission entry for child: /eu"));
        assert!(!perms_path.exists(), "preflight failure must precede perms write");
        assert!(!resolver_path.exists(), "preflight failure must precede config write");
    }

    #[test]
    fn apply_rejects_a_managed_path_outside_the_locked_directory() {
        let locked = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let perms_path = outside.path().join("perms.json");
        let mut rt = empty_rt();
        rt.perms_file = Some((perms_path.clone(), crate::perms::empty()));

        let config_lock = ConfigDirLock::acquire(locked.path()).unwrap();
        let err = rt.apply(&config_lock).unwrap_err();
        assert!(format!("{err:#}").contains("outside config directory"));
        assert!(!perms_path.exists());
    }

    #[tokio::test]
    async fn post_apply_failure_leaves_the_core_install_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let perms_path = dir.path().join("perms.json");
        let record_path = dir.path().join("install.json");
        let mut rt = empty_rt();
        rt.perms_file = Some((perms_path.clone(), crate::perms::empty()));
        let record =
            InstallRecord::new(InstallRole::Resolver, "/", "anonymous", None, None);
        let mut expected = record.clone();
        expected.set_managed_paths(vec![perms_path.clone()]);
        let check_record = record_path.clone();
        let check_perms = perms_path.clone();
        let mut ans = TestAnswerer;
        let config_lock = ConfigDirLock::acquire(dir.path()).unwrap();
        let err = finish_with_record_path(
            &mut ans,
            rt,
            &InstallCommon {
                mode: InstallMode::Apply { config_lock },
                force: false,
                no_units: true,
                with_service: false,
                no_service: true,
            },
            ServiceNeed::NONE,
            record.clone(),
            Some(&record_path),
            async move |_ans, _config_lock| {
                assert!(check_record.exists());
                assert!(check_perms.exists());
                bail!("simulated trust domain failure")
            },
        )
        .await
        .unwrap_err();

        let message = format!("{err:#}");
        assert!(message.contains("core install completed"));
        assert!(message.contains("simulated trust domain failure"));
        assert_eq!(InstallRecord::load(&record_path).unwrap(), expected);
    }
}
