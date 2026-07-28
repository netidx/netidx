//! Top-level installation backup and restore.
//!
//! A restore is deliberately an install: configs are staged at the current
//! platform's canonical root, machine credentials are reissued, and the OS
//! service recorded in the bundle is recreated.

use super::{
    answer_cli, init,
    service::{self as service_cli, ScopeArg},
};
use anyhow::{Context, Result, bail};
use clap::Args;
use netidx_admin_client::{
    config_lock::ConfigDirLock,
    paths,
    plan::AuthKind,
    plan::enroll::{self, DiscoveredAdminDomain},
    provenance::{InstallRecord, InstallRole},
    service::{ServiceParams, ServiceScope, ServiceStatus},
    tls, transport,
};
use netidx_admin_proto::NodeKind;
#[cfg(unix)]
use netidx_admin_server::{
    install_bundle::{self, BundleScope, Component, IdentityKind, ServiceIntent},
    plan::install::resolver::enroll_admin_server,
};
#[cfg(unix)]
use std::str::FromStr;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

#[derive(Args, Debug)]
pub(crate) struct BackupArgs {
    /// New backup bundle directory. Existing paths are never overwritten.
    pub target: PathBuf,
    /// Select the user or system install when both exist.
    #[arg(long)]
    pub scope: Option<ScopeArg>,
    /// Override the managed configuration root.
    #[arg(long = "config-dir")]
    pub config_dir: Option<PathBuf>,
    /// OS service name to record (default: netidx).
    #[arg(long, default_value = "netidx")]
    pub service_name: String,
    /// Account used by a system-scope service.
    #[arg(long)]
    pub for_user: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct RestoreArgs {
    /// Bundle created by `netidx admin backup`.
    pub bundle: PathBuf,
    /// Override the destination configuration root.
    #[arg(long = "config-dir")]
    pub config_dir: Option<PathBuf>,
    /// Override the restored admin-server listen address.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Override the advertised address of a resolver co-located with the
    /// restored CA.
    #[arg(long = "resolver-listen")]
    pub resolver_listen: Option<SocketAddr>,
    /// Override the local bind IP of a resolver co-located with the restored
    /// CA. When only --resolver-listen changes, its IP is the default.
    #[arg(long = "resolver-bind")]
    pub resolver_bind: Option<IpAddr>,
    /// Explicitly attest that the old CA cannot run. Required for a
    /// CA restore because two copies of the same CA identity
    /// would violate the admin domain's single-writer boundary.
    #[arg(long = "old-ca-fenced")]
    pub old_ca_fenced: bool,
    /// Read the CA recovery password from a file.
    #[arg(long = "recovery-password-file")]
    pub recovery_password_file: Option<PathBuf>,
    /// Read the CA recovery password from stdin.
    #[arg(long = "recovery-password-stdin", conflicts_with = "recovery_password_file")]
    pub recovery_password_stdin: bool,
    /// Fresh externally-signed intermediate CA certificate, required when the
    /// bundled CA certificate has expired.
    #[arg(long = "external-cert")]
    pub external_cert: Option<PathBuf>,
    /// External root certificate when it is not appended to --external-cert.
    #[arg(long = "external-root")]
    pub external_root: Option<PathBuf>,
    /// Protection for freshly enrolled non-ca TLS keys.
    #[arg(long = "key-protection")]
    pub key_protection: Option<init::KeyProtArg>,
    /// Password for `--key-protection password`.
    #[arg(long = "key-password-file")]
    pub key_password_file: Option<PathBuf>,
    /// Read the key password from stdin.
    #[arg(long = "key-password-stdin", conflicts_with = "key_password_file")]
    pub key_password_stdin: bool,
    /// Override the CA/bootstrap address recorded in the bundle.
    #[arg(long = "admin-server")]
    pub admin_server: Option<String>,
    /// Restore the recorded OS service even if the source install had none.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    pub with_service: bool,
    /// Restore configs and credentials but do not register an OS service.
    #[arg(long = "no-service")]
    pub no_service: bool,
    /// Override the service name stored in the bundle.
    #[arg(long = "service-name")]
    pub service_name: Option<String>,
    /// Override the account used by a restored system service.
    #[arg(long = "for-user")]
    pub for_user: Option<String>,
    /// Permit plaintext replacement CA credentials when no TPM or
    /// Secure Enclave is usable. Test installations only.
    #[arg(long = "insecure-no-tpm")]
    pub insecure_no_tpm: bool,
}

fn restore_addresses(
    manifest: &install_bundle::Manifest,
    args: &RestoreArgs,
) -> Result<install_bundle::RestoreAddresses> {
    let resolver = match (args.resolver_listen, args.resolver_bind) {
        (None, None) => None,
        (listen, bind) => {
            let original = manifest.resolver_endpoint.context(
                "--resolver-listen/--resolver-bind require a backup of a CA with a co-located resolver",
            )?;
            let listen = listen.unwrap_or(original.listen);
            Some(install_bundle::ResolverEndpoint {
                listen,
                bind: bind.unwrap_or_else(|| {
                    if args.resolver_listen.is_some() {
                        listen.ip()
                    } else {
                        original.bind
                    }
                }),
            })
        }
    };
    Ok(install_bundle::RestoreAddresses { admin_listen: args.listen, resolver })
}

fn scope_root(scope: BundleScope) -> Result<PathBuf> {
    match scope {
        BundleScope::User => paths::user_config_root(),
        BundleScope::System => Ok(paths::system_config_root()),
    }
}

fn find_install(a: &BackupArgs) -> Result<(PathBuf, BundleScope, InstallRecord)> {
    if let Some(root) = &a.config_dir {
        let record = InstallRecord::load(&root.join("install.json"))?;
        let scope = match a.scope.unwrap_or(ScopeArg::User) {
            ScopeArg::User => BundleScope::User,
            ScopeArg::System => BundleScope::System,
        };
        return Ok((root.clone(), scope, record));
    }
    let user = paths::user_config_root()?;
    let system = paths::system_config_root();
    let candidates = match a.scope {
        Some(ScopeArg::User) => vec![(user, BundleScope::User)],
        Some(ScopeArg::System) => vec![(system, BundleScope::System)],
        None => vec![(user, BundleScope::User), (system, BundleScope::System)],
    };
    let mut found = Vec::new();
    for (root, scope) in candidates {
        let record = root.join("install.json");
        if record.is_file() {
            found.push((root, scope, InstallRecord::load(&record)?));
        }
    }
    match found.len() {
        0 => bail!("no netidx-admin managed install found"),
        1 => Ok(found.remove(0)),
        _ => bail!("both user and system installs exist; select one with --scope"),
    }
}

fn service_intent(a: &BackupArgs, record: &InstallRecord) -> Result<ServiceIntent> {
    let scope = match record.role {
        InstallRole::Workstation => ServiceScope::User,
        InstallRole::Ca | InstallRole::Resolver | InstallRole::Publisher => {
            ServiceScope::System
        }
    };
    let for_user = match scope {
        ServiceScope::User => None,
        ServiceScope::System => Some(service_cli::resolve_for_user(a.for_user.clone())?),
    };
    let status = netidx_admin_client::service::status(&ServiceParams {
        scope,
        for_user: for_user.clone(),
        binary: PathBuf::new(),
        service_name: a.service_name.clone(),
        activation_dir: None,
    })?;
    Ok(ServiceIntent {
        scope: match scope {
            ServiceScope::User => BundleScope::User,
            ServiceScope::System => BundleScope::System,
        },
        name: a.service_name.clone(),
        for_user,
        installed: status != ServiceStatus::NotInstalled,
    })
}

pub(crate) fn backup(a: BackupArgs) -> Result<()> {
    let (root, scope, record) = find_install(&a)?;
    let target = if a.target.is_absolute() {
        a.target.clone()
    } else {
        std::env::current_dir()?.join(&a.target)
    };
    let service = service_intent(&a, &record)?;
    let has_ca = root.join("ca").is_dir();
    #[cfg(unix)]
    let ca_tmp = tempfile::tempdir().context("creating ca-backup staging directory")?;
    let ca: Option<PathBuf> = if has_ca {
        #[cfg(unix)]
        {
            let cfg = root.join("admin-server.json");
            let inner = ca_tmp.path().join("ca");
            tokio::runtime::Runtime::new()?
                .block_on(netidx_admin_client::local::backup(&cfg, &inner))?;
            Some(inner)
        }
        #[cfg(not(unix))]
        {
            bail!("a CA backup can only be captured on its unix host")
        }
    } else {
        None
    };
    let out = install_bundle::create(
        &root,
        record,
        scope,
        Some(service),
        ca.as_deref(),
        &target,
    )?;
    println!("created {} backup at {}", out.role.as_str(), out.target.display());
    println!("  components: {:?}", out.components);
    println!("  files:      {} ({} bytes)", out.files, out.bytes);
    println!("  re-enroll:  {} machine credential(s)", out.identities_to_reenroll);
    println!("  manifest SHA-256: {}", out.manifest_sha256);
    Ok(())
}

fn desired_service(
    manifest: &install_bundle::Manifest,
    a: &RestoreArgs,
) -> Option<ServiceScope> {
    if a.no_service {
        return None;
    }
    let wanted = a.with_service || manifest.service.as_ref().is_some_and(|s| s.installed);
    if !wanted {
        return None;
    }
    Some(
        match manifest.service.as_ref().map(|s| s.scope).unwrap_or(manifest.config_scope)
        {
            BundleScope::User => ServiceScope::User,
            BundleScope::System => ServiceScope::System,
        },
    )
}

fn install_service(
    manifest: &install_bundle::Manifest,
    args: &RestoreArgs,
    scope: ServiceScope,
) -> Result<()> {
    match &manifest.service {
        Some(intent) => service_cli::install_restored(
            scope.into(),
            args.service_name.clone().unwrap_or_else(|| intent.name.clone()),
            args.for_user.clone().or_else(|| intent.for_user.clone()),
        ),
        None if args.service_name.is_some() || args.for_user.is_some() => {
            service_cli::install_restored(
                scope.into(),
                args.service_name
                    .clone()
                    .unwrap_or_else(|| ServiceParams::DEFAULT_NAME.to_string()),
                args.for_user.clone(),
            )
        }
        None => service_cli::install_with_defaults(scope.into()),
    }
}

pub(super) async fn admin_domain_for_restore(
    manifest: &install_bundle::Manifest,
    override_: Option<&str>,
) -> Result<Option<(SocketAddr, DiscoveredAdminDomain)>> {
    let Some(admin_domain) = &manifest.install.admin_domain else { return Ok(None) };
    let mut seed = match override_ {
        Some(addr) => init::resolve_admin_server_addr(addr)?,
        None => manifest
            .install
            .admin_servers
            .first()
            .copied()
            .or(manifest.admin_listen)
            .context(
                "the backup has no admin-server hint; pass --admin-server <host[:port]>",
            )?,
    };
    if seed.ip().is_unspecified() {
        seed.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    }
    let first = {
        let mut last = None;
        let mut found = None;
        for _ in 0..30 {
            match transport::fetch_identity(seed, NodeKind::Client).await {
                Ok(identity) => {
                    found = Some(identity);
                    break;
                }
                Err(error) => {
                    last = Some(error);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        found.ok_or_else(|| {
            last.unwrap_or_else(|| anyhow::anyhow!("the admin server did not start"))
        })?
    };
    if !admin_domain.matches(&first.fingerprint)? {
        bail!("the server at {seed} belongs to a different CA than the backup");
    }
    let info = transport::aggregate(&[seed], NodeKind::Client, &first).await?;
    let ca = info.ca_addr.context("the verified admin domain reported no CA")?;
    let identity = transport::fetch_identity(ca, NodeKind::Client).await?;
    if !identity.ca || !admin_domain.matches(&identity.fingerprint)? {
        bail!("the discovered CA did not match the backup's pinned CA identity");
    }
    let info = transport::aggregate(&[ca], NodeKind::Client, &identity).await?;
    Ok(Some((ca, DiscoveredAdminDomain { identity, info })))
}

fn node_kind(kind: IdentityKind) -> NodeKind {
    match kind {
        IdentityKind::Client => NodeKind::Client,
        IdentityKind::Publisher => NodeKind::Publisher,
        IdentityKind::Workstation => NodeKind::Workstation,
        IdentityKind::Resolver => NodeKind::Resolver,
        IdentityKind::AdminServer => NodeKind::AdminServer,
    }
}

pub(super) fn identities_complete(
    root: &Path,
    manifest: &install_bundle::Manifest,
) -> bool {
    manifest.identities.iter().all(|recipe| {
        if recipe.kind == IdentityKind::AdminServer {
            #[cfg(unix)]
            return netidx_admin_client::admin_server_config::load(
                &root.join("admin-server.json"),
            )
            .is_ok_and(|cfg| {
                Some(cfg.server_id) != manifest.previous_admin_server
                    && install_bundle::identity_files_usable(
                        &cfg.serving_cert,
                        &cfg.serving_key,
                        &cfg.trusted,
                    )
            });
            #[cfg(not(unix))]
            return false;
        }
        install_bundle::identity_files_usable(
            &root.join(&recipe.certificate),
            &root.join(&recipe.private_key),
            &root.join(&recipe.trusted),
        )
    })
}

pub(super) async fn reenroll_data_identities(
    ans: &mut dyn netidx_admin_client::answer::Answerer,
    root: &Path,
    manifest: &install_bundle::Manifest,
    ca: SocketAddr,
    net: &DiscoveredAdminDomain,
    key_protection: Option<netidx_admin_client::plan::enroll::KeyProtArg>,
) -> Result<()> {
    for recipe in
        manifest.identities.iter().filter(|i| i.kind != IdentityKind::AdminServer)
    {
        if install_bundle::identity_files_usable(
            &root.join(&recipe.certificate),
            &root.join(&recipe.private_key),
            &root.join(&recipe.trusted),
        ) {
            ans.note(&format!(
                "{} identity {:?} is already restored",
                recipe.kind.label(),
                recipe.name
            ));
            continue;
        }
        let old_certificate = root.join(&recipe.certificate);
        let replaces_serial = install_bundle::certificate_serial(&old_certificate)
            .with_context(|| {
                format!(
                    "reading restore replacement serial from {}",
                    old_certificate.display()
                )
            })?;
        let (identity, _staging) = enroll::join_admin_domain_replacing(
            ans,
            ca,
            node_kind(recipe.kind),
            Some(&recipe.name),
            key_protection,
            Some(replaces_serial),
            &net.identity,
        )
        .await?;
        let destination = root
            .join(&recipe.private_key)
            .parent()
            .context("identity private-key path has no parent")?
            .to_path_buf();
        tls::install_identity(&tls::InstallIdentity {
            cn: &identity.name,
            dest_dir: &destination,
            certificate_src: &identity.certificate,
            private_key_src: &identity.private_key,
            trusted_src: &identity.trusted,
        })?;
        ans.note(&format!(
            "restored fresh {} identity {:?}",
            recipe.kind.label(),
            recipe.name
        ));
    }
    Ok(())
}

#[cfg(unix)]
pub(super) async fn reenroll_satellite_admin(
    ans: &mut dyn netidx_admin_client::answer::Answerer,
    config_lock: &ConfigDirLock,
    root: &Path,
    manifest: &install_bundle::Manifest,
    net: &DiscoveredAdminDomain,
    listen_override: Option<SocketAddr>,
) -> Result<()> {
    if !manifest.identities.iter().any(|i| i.kind == IdentityKind::AdminServer)
        || manifest.components.contains(&Component::Ca)
    {
        return Ok(());
    }
    let config_path = root.join("admin-server.json");
    if let Ok(cfg) = netidx_admin_client::admin_server_config::load(&config_path)
        && Some(cfg.server_id) != manifest.previous_admin_server
        && install_bundle::identity_files_usable(
            &cfg.serving_cert,
            &cfg.serving_key,
            &cfg.trusted,
        )
    {
        ans.note("the satellite admin-server identity is already restored");
        return Ok(());
    }
    let resolver_path = root.join("resolver.json");
    let resolver = netidx_admin_client::resolver::ResolverConfig::load(&resolver_path)?;
    let resolver_listen = match (manifest.previous_admin_server, net.info.ca_addr) {
        (Some(old), Some(ca)) => {
            let map = transport::get_map_pinned(ca, NodeKind::AdminServer, &net.identity)
                .await?;
            map.admin_servers
                .iter()
                .find(|server| server.id == old)
                .and_then(|server| server.resolver.as_ref())
                .map(|resolver| resolver.addr)
                .with_context(|| {
                    format!(
                        "the authoritative map has no resolver ownership for failed server {old}"
                    )
                })?
        }
        _ => {
            resolver
                .resolver_addrs()
                .first()
                .context("the restored resolver has no advertisable member")?
                .addr
        }
    };
    let id_map = root.join("id-map.json").is_file().then(|| root.join("id-map.json"));
    let units = root.join("activation");
    let units = units.is_dir().then_some(units.as_path());
    let auth = AuthKind::from_str(&manifest.install.auth)?;
    if !enroll_admin_server(
        ans,
        net,
        auth,
        false,
        true,
        resolver_listen,
        resolver.base_path(),
        units,
        resolver_path,
        id_map,
        listen_override.or(manifest.admin_listen),
        manifest.previous_admin_server,
        config_lock,
    )
    .await?
    {
        bail!("the restored resolver's admin-server enrollment did not complete");
    }
    Ok(())
}

/// The recovered CA must start before its co-located identities can
/// enroll. Units that needed those absent keys may therefore have died once;
/// after enrollment, start only non-running units. Healthy units (especially
/// the CA) are never restarted here.
pub(super) async fn start_restored_units(root: &Path) -> Result<()> {
    use netidx_activation::control::{
        ControlOp, ControlRequest, ControlResponse, UnitState, control,
    };
    let units_dir = root.join("activation");
    if !units_dir.is_dir() {
        return Ok(());
    }
    let statuses = match control(
        &units_dir,
        &ControlRequest { op: ControlOp::Status, units: Vec::new() },
    )
    .await?
    {
        ControlResponse::Ok { units } => units,
        ControlResponse::Err { reason } => bail!(reason),
    };
    let units = statuses
        .into_iter()
        .filter(|unit| !matches!(unit.state, UnitState::Running { .. }))
        .map(|unit| unit.unit)
        .collect::<Vec<_>>();
    if units.is_empty() {
        return Ok(());
    }
    match control(&units_dir, &ControlRequest { op: ControlOp::Start, units }).await? {
        ControlResponse::Ok { .. } => Ok(()),
        ControlResponse::Err { reason } => bail!(reason),
    }
}

#[cfg(unix)]
pub(super) async fn reconcile_restored_ca(
    root: &Path,
) -> Result<netidx_admin_proto::OperationId> {
    use poolshark::local::LPooled;
    use std::fmt::Write as _;

    let (operation_id, peers) =
        netidx_admin_client::local::reconcile_ca(&root.join("admin-server.json")).await?;
    let mut failures: LPooled<String> = LPooled::take();
    for peer in peers {
        if let Some(error) = peer.error {
            let separator = if failures.is_empty() { "" } else { "; " };
            let _ =
                write!(failures, "{separator}{} at {}: {error}", peer.server, peer.addr,);
        }
    }
    if !failures.is_empty() {
        bail!("CA reconciliation operation {operation_id} failed: {failures}");
    }
    Ok(operation_id)
}

pub(crate) fn restore(a: RestoreArgs) -> Result<()> {
    let bundle = a.bundle.canonicalize().context("canonicalizing backup bundle")?;
    let preflight = install_bundle::verify(&bundle)?;
    let has_ca = preflight.components.contains(&Component::Ca);
    if has_ca && !a.old_ca_fenced {
        bail!(
            "CA restore requires --old-ca-fenced; do not continue until the old CA cannot run"
        );
    }
    let root = match &a.config_dir {
        Some(root) => root.clone(),
        None => scope_root(preflight.config_scope)?,
    };
    let mut config_lock = Some(ConfigDirLock::acquire(&root)?);
    let addresses = restore_addresses(&preflight, &a)?;
    let resolver_relocated = addresses
        .resolver
        .zip(preflight.resolver_endpoint)
        .is_some_and(|(replacement, original)| replacement != original);
    let service = desired_service(&preflight, &a);
    if has_ca
        && (!preflight.identities.is_empty() || resolver_relocated)
        && service.is_none()
    {
        bail!(
            "a CA with co-located roles requires restoring its OS service so it can finish enrollment and hierarchy reconciliation"
        );
    }
    println!("restoring {:?} to {}", preflight.components, root.display());
    if !preflight.identities.is_empty() {
        println!(
            "  fresh enrollment required for {} machine credential(s)",
            preflight.identities.len()
        );
    }
    let manifest =
        install_bundle::restore_files_with_addresses(&bundle, &root, addresses)?;

    #[cfg(unix)]
    if has_ca {
        let ca_dir = root.join("ca");
        let cfg = root.join("admin-server.json");
        if !install_bundle::ca_recovered(&bundle, &cfg)? {
            if !install_bundle::ca_snapshot_prepared(&bundle, &ca_dir, &cfg)? {
                netidx_admin_server::backup::restore(
                    config_lock.as_ref().expect("restore lock held"),
                    &bundle.join(install_bundle::CA_DIR),
                    &ca_dir,
                    &cfg,
                )
                .context("restoring the verified CA state")?;
            }
            let mut recovery = answer_cli::FlagAnswerer::install(
                a.key_password_file.as_deref(),
                a.key_password_stdin,
                None,
                false,
                a.recovery_password_file.as_deref(),
                a.recovery_password_stdin,
                None,
            )?;
            let runtime = tokio::runtime::Runtime::new()?;
            let lifetimes = netidx_admin_server::ca::CaLifetimes::load(&ca_dir)?;
            let expired = netidx_admin_server::ca::ca_cert_needs_renewal(
                &ca_dir,
                std::time::Duration::ZERO,
            );
            if lifetimes.externally_signed && expired && a.external_cert.is_none() {
                bail!(
                    "the bundled external-CA certificate has expired; have the external \
                     PKI re-sign this CA key and repeat restore with --external-cert \
                     <certificate> [--external-root <certificate>]"
                );
            }
            if let Some(signed) = &a.external_cert {
                if !lifetimes.externally_signed {
                    bail!("--external-cert was supplied for a self-signed netidx CA");
                }
                runtime.block_on(
                    netidx_admin_server::ops::slots::external_install_cert(
                        &mut recovery,
                        config_lock.as_ref().expect("restore lock held"),
                        ca_dir.clone(),
                        signed,
                        a.external_root.as_deref(),
                    ),
                )?;
            }
            runtime.block_on(netidx_admin_server::ops::slots::recover_ca(
                &mut recovery,
                config_lock.as_ref().expect("restore lock held"),
                ca_dir,
                cfg.clone(),
                manifest.admin_listen,
                addresses.resolver.map(|resolver| resolver.listen),
                a.insecure_no_tpm,
            ))?;
        }
        // The inner CA snapshot uses portable recovered-* role paths;
        // reconnect it to the complete role configs restored by the outer bundle.
        let mut cfgv = netidx_admin_client::admin_server_config::load_for_recovery(&cfg)?;
        if let Some(role) = cfgv.roles.resolver.as_mut() {
            role.config = root.join("resolver.json");
        }
        if let Some(role) = cfgv.roles.id_map.as_mut() {
            role.map = root.join("id-map.json");
        }
        netidx_admin_client::admin_server_config::save(
            config_lock.as_ref().expect("restore lock held"),
            &cfg,
            &cfgv,
        )?;
        manifest.install.save(
            config_lock.as_ref().expect("restore lock held"),
            &root.join("install.json"),
        )?;
    }
    #[cfg(not(unix))]
    if has_ca {
        bail!("CA restore is supported only on unix")
    }

    // A recovered CA must be reachable before its co-located resolver
    // identities can pass through the normal enrollment ceremony.
    if has_ca {
        drop(config_lock.take());
        if let Some(scope) = service {
            install_service(&manifest, &a, scope)?;
        }
    }

    if !manifest.identities.is_empty() && !identities_complete(&root, &manifest) {
        let mut ans = answer_cli::FlagAnswerer::install(
            a.key_password_file.as_deref(),
            a.key_password_stdin,
            None,
            false,
            a.recovery_password_file.as_deref(),
            a.recovery_password_stdin,
            manifest
                .install
                .admin_domain
                .as_ref()
                .map(|n| {
                    netidx_admin_proto::fingerprint::Fingerprint::parse_text(
                        &n.ca_fingerprint,
                    )
                })
                .transpose()?,
        )?;
        let rt = tokio::runtime::Runtime::new()?;
        let Some((ca, net)) =
            rt.block_on(admin_domain_for_restore(&manifest, a.admin_server.as_deref()))?
        else {
            bail!("the backup contains TLS identities but no admin domain identity");
        };
        rt.block_on(reenroll_data_identities(
            &mut ans,
            &root,
            &manifest,
            ca,
            &net,
            init::lib_kp(a.key_protection),
        ))?;
        #[cfg(unix)]
        if !has_ca {
            rt.block_on(reenroll_satellite_admin(
                &mut ans,
                config_lock.as_ref().context("restore lock not held")?,
                &root,
                &manifest,
                &net,
                a.listen,
            ))?;
        }
        if has_ca && service.is_some() {
            rt.block_on(start_restored_units(&root))?;
        }
    }
    if !has_ca {
        drop(config_lock.take());
        if let Some(scope) = service {
            install_service(&manifest, &a, scope)?;
        }
    }
    #[cfg(unix)]
    if has_ca && resolver_relocated {
        let operation_id =
            tokio::runtime::Runtime::new()?.block_on(reconcile_restored_ca(&root))?;
        println!("  resolver hierarchy reconciled (operation {operation_id})");
    }
    println!(
        "restore complete: {} is installed and ready",
        manifest.install.role.as_str()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Parser, Subcommand};

    #[derive(Debug, Parser)]
    struct Cli {
        #[command(subcommand)]
        command: Command,
    }

    #[derive(Debug, Subcommand)]
    enum Command {
        Backup(BackupArgs),
        Restore(RestoreArgs),
    }

    #[test]
    fn backup_and_restore_are_top_level_commands() {
        let parsed =
            Cli::try_parse_from(["admin", "backup", "/srv/netidx-backup"]).unwrap();
        assert!(matches!(parsed.command, Command::Backup(_)));
        let parsed = Cli::try_parse_from([
            "admin",
            "restore",
            "/srv/netidx-backup",
            "--old-ca-fenced",
            "--recovery-password-stdin",
            "--listen",
            "10.1.0.4:5565",
            "--resolver-listen",
            "203.0.113.4:5564",
            "--resolver-bind",
            "10.1.0.4",
        ])
        .unwrap();
        let Command::Restore(args) = parsed.command else { panic!("restore") };
        assert!(args.old_ca_fenced);
        assert!(args.recovery_password_stdin);
        assert_eq!(args.listen, Some("10.1.0.4:5565".parse().unwrap()));
        assert_eq!(args.resolver_listen, Some("203.0.113.4:5564".parse().unwrap()));
        assert_eq!(args.resolver_bind, Some("10.1.0.4".parse().unwrap()));
    }
}
