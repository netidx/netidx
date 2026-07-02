//! The install planner: the shared tail every role install runs (describe →
//! apply → post-apply → service offer), plus the small pure helpers around it.
//!
//! The role-specific cascades (`resolver`, `workstation`, `publisher`) live in
//! the submodules; this module holds what they share. Everything speaks to the
//! operator through the [`Answerer`] seam only.

use crate::{
    activation,
    answer::{Answerer, Field},
    plan::{
        enroll::{self, AdminServers},
        service::{ServiceGate, ServiceNeed, offer},
    },
    provenance::{InstallRecord, NetworkIdentity},
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
pub mod resolver;
pub mod workstation;

// -- environment-shape facade (feature-gated) ---------------------------------
//
// The bind/listen suggestions come from `netshape`, which is behind the
// `cloud-detect` feature (it pulls an HTTP client + interface enumeration).
// The install planner routes every use through these helpers so the cascades
// stay cfg-free and the crate still builds without the feature — degrading to
// "no suggestion, ask the operator" rather than failing to compile.

/// The IP a network daemon on this host should advertise, if the environment
/// can be probed. `None` without `cloud-detect`.
#[cfg(feature = "cloud-detect")]
pub fn detected_advertised_ip() -> Option<IpAddr> {
    Some(crate::netshape::NetShape::detect().advertised_ip().into())
}

/// See the `cloud-detect` variant.
#[cfg(not(feature = "cloud-detect"))]
pub fn detected_advertised_ip() -> Option<IpAddr> {
    None
}

/// A suggested publisher bind (`BindCfg`) plus whether the environment needs
/// an operator hint (a `<PUBLIC_IP>` placeholder to fill). `(None, false)`
/// without `cloud-detect`.
#[cfg(feature = "cloud-detect")]
pub fn publisher_bind_shape() -> (Option<String>, bool) {
    let shape = crate::netshape::NetShape::detect();
    (Some(shape.publisher_bind_suggestion()), shape.needs_operator_hint())
}

/// See the `cloud-detect` variant.
#[cfg(not(feature = "cloud-detect"))]
pub fn publisher_bind_shape() -> (Option<String>, bool) {
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

/// Detect the resolver environment shape (see [`ResolverShape`]).
#[cfg(feature = "cloud-detect")]
pub fn detect_resolver_shape() -> ResolverShape {
    use crate::netshape::NetShape;
    let s = NetShape::detect();
    ResolverShape {
        advertised_ip: Some(s.advertised_ip().into()),
        needs_operator_hint: s.needs_operator_hint(),
        bind_override: s.resolver_bind_override().map(IpAddr::V4),
        elastic_local_client_bind: match &s {
            NetShape::CloudElastic { .. } => Some(s.publisher_bind_suggestion()),
            _ => None,
        },
    }
}

/// See the `cloud-detect` variant.
#[cfg(not(feature = "cloud-detect"))]
pub fn detect_resolver_shape() -> ResolverShape {
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
#[derive(Debug, Clone, Copy)]
pub struct InstallCommon {
    /// Print the plan and change nothing.
    pub dry_run: bool,
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

/// Extract from a probe the network identity (domain + CA fingerprint) to pin
/// later lifecycle ops to, and a reachable admin-server address to start from.
/// `(None, None)` when the install didn't join a *discovered* network (a
/// CLI-flag parent, the manual prompt cascade, or no parent at all carry no
/// confirmed identity).
pub fn network_provenance(
    probe: &AdminServers,
) -> (Option<NetworkIdentity>, Option<SocketAddr>) {
    match probe.have() {
        Some(net) => {
            let id =
                NetworkIdentity::new(net.identity.domain.clone(), &net.identity.fingerprint);
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

/// Describe + apply the rendered template, then run a post-apply step (after
/// the install, never on `--dry-run`), then make the single end-of-install
/// OS-service decision. Returns the scope the frontend should install a
/// service at, or `None`.
///
/// `post_apply` receives the `Answerer` because its steps (standing up this
/// host's admin server, dropping the renew unit) themselves report to the
/// operator; it is an `AsyncFnOnce` because standing up the admin server is
/// network I/O.
pub async fn finish_with(
    ans: &mut dyn Answerer,
    rt: RenderedTemplate,
    common: &InstallCommon,
    need: ServiceNeed,
    record: InstallRecord,
    post_apply: impl AsyncFnOnce(&mut dyn Answerer) -> Result<()>,
) -> Result<Option<ServiceScope>> {
    ans.note(&rt.describe());
    if !common.dry_run {
        check_no_overwrite(&rt, common.force)?;
        rt.apply().context("applying template")?;
        ans.note("ok");
        post_apply(ans).await?;
        // Record what we installed and the network it joined (identity-pinned),
        // so lifecycle ops know what this host is and can re-pin to the same CA.
        record.save_default().context("writing the install record")?;
    }
    offer(
        ans,
        need,
        ServiceGate {
            dry_run: common.dry_run,
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

/// Prompt for a resolver port (default 4564) and pair it with `ip`.
pub async fn prompt_resolver_port(
    ans: &mut dyn Answerer,
    ip: IpAddr,
    provided: Option<u16>,
) -> Result<SocketAddr> {
    let port = match provided {
        Some(p) => p,
        None => ans
            .text(
                Field::ResolverPort,
                None,
                Some(&DEFAULT_RESOLVER_PORT.to_string()),
                false,
            )
            .await?
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<u16>())
            .transpose()
            .context("invalid resolver port")?
            .unwrap_or(DEFAULT_RESOLVER_PORT),
    };
    Ok(SocketAddr::new(ip, port))
}

/// Best-effort default for the TLS name a resolver presents. Probes the
/// resolver itself — the authoritative source, since it's the exact SAN a
/// client must match — and falls back to the `resolver.local` convention when
/// the probe can't reach or read it. Either way the value is only a default
/// the operator confirms; a wrong guess fails closed at connect time.
pub async fn resolver_tls_name_default(ans: &mut dyn Answerer, addr: SocketAddr) -> String {
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
/// SAN.
pub async fn prompt_resolver_own_tls_name(
    ans: &mut dyn Answerer,
    provided: Option<String>,
) -> Result<String> {
    if let Some(full) = provided {
        return Ok(full);
    }
    let domain = ans
        .text(Field::TlsDomain, None, Some(DEFAULT_TLS_DOMAIN), false)
        .await?
        .unwrap_or_else(|| DEFAULT_TLS_DOMAIN.to_string());
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
    use crate::template::{RenderedTemplate, TlsCopyJob};
    use std::collections::BTreeMap;

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
}
