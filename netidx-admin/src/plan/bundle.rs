//! Capturing an installation into a bundle, and restoring one.
//!
//! A restore is deliberately an install: configs are staged at the current
//! platform's canonical root, machine credentials are reissued, and the OS
//! service recorded in the bundle is recreated. [`crate::install_bundle`] owns
//! the on-disk format; this module owns the ceremony around it, driven through
//! the [`Answerer`] seam so the strict CLI and the TUI share one implementation.
//!
//! Registering the OS service is the one step that stays with the frontend: it
//! is privileged and terminal-owning (a sudo password prompt), and the TUI must
//! return to its event loop to suspend the terminal for it. So [`restore_stage`]
//! reports what to do [`Next`], the frontend performs only that, and
//! [`restore_finish`] resumes. *When* the service is needed differs — a bundle
//! with a CA must have it running before its co-located identities can enroll
//! against it, one without wants it last — and that rule lives here, not in
//! each frontend.

use crate::{
    admin_proto::NodeKind,
    answer::Answerer,
    config_lock::ConfigDirLock,
    install_bundle::{
        self, BackupOutcome, BundleScope, Component, IdentityKind, Manifest,
        RestoreAddresses, ServiceIntent,
    },
    paths,
    plan::enroll::{self, DiscoveredAdminDomain, KeyProtArg},
    provenance::{InstallRecord, InstallRole},
    service::{ServiceParams, ServiceScope, ServiceStatus},
    tls, transport,
};
use anyhow::{Context, Result, bail};
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use crate::plan::{AuthKind, install::resolver::enroll_admin_server};
#[cfg(unix)]
use std::str::FromStr;

/// Which install to capture, and how to describe its OS service in the bundle.
pub struct BackupInput {
    /// Where to write the bundle. Must be absolute.
    pub target: PathBuf,
    /// Select the user or system install when both exist.
    pub scope: Option<BundleScope>,
    /// Override the managed configuration root.
    pub config_dir: Option<PathBuf>,
    /// OS service name to record.
    pub service_name: String,
    /// Account used by a system-scope service.
    pub for_user: Option<String>,
}

/// Everything a restore needs that isn't in the bundle: destination overrides,
/// the credentials to unlock a recovered CA, and the operator's explicit
/// attestation that the old CA is fenced.
pub struct RestoreInput {
    /// The bundle directory, as produced by [`backup`].
    pub bundle: PathBuf,
    /// Override the destination configuration root.
    pub config_dir: Option<PathBuf>,
    /// Override the restored admin-server listen address.
    pub listen: Option<SocketAddr>,
    /// Override the advertised address of a co-located resolver.
    pub resolver_listen: Option<SocketAddr>,
    /// Override that resolver's local bind IP.
    pub resolver_bind: Option<IpAddr>,
    /// The operator attests the old CA cannot run. Two copies of one CA
    /// identity would break the admin domain's single-writer boundary.
    pub old_ca_fenced: bool,
    /// Fresh externally-signed intermediate CA certificate, required when the
    /// bundled CA certificate has expired.
    pub external_cert: Option<PathBuf>,
    /// External root certificate when it is not appended to `external_cert`.
    pub external_root: Option<PathBuf>,
    /// Protection for freshly enrolled non-CA TLS keys.
    pub key_protection: Option<KeyProtArg>,
    /// Override the CA/bootstrap address recorded in the bundle.
    pub admin_server: Option<String>,
    /// Register the recorded OS service even if the source install had none.
    pub with_service: bool,
    /// Restore configs and credentials but register no OS service.
    pub no_service: bool,
    /// Override the service name stored in the bundle.
    pub service_name: Option<String>,
    /// Override the account used by a restored system service.
    pub for_user: Option<String>,
    /// Permit plaintext replacement CA credentials when no TPM or Secure
    /// Enclave is usable. Test installations only.
    pub insecure_no_tpm: bool,
}

/// What the frontend must do between [`restore_stage`] and [`restore_finish`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// Register the OS service at this scope, then call [`restore_finish`].
    /// A recovered CA has to be reachable before its co-located identities can
    /// pass through the normal enrollment ceremony.
    ServiceThenFinish(ServiceScope),
    /// Nothing privileged to do yet — call [`restore_finish`], which reports
    /// any service still needed once enrollment is done.
    Finish,
}

/// The restore in progress, handed back to [`restore_finish`].
pub struct Staged {
    /// What to do before resuming.
    pub next: Next,
    /// The destination configuration root.
    pub root: PathBuf,
    /// The restored manifest.
    pub manifest: Manifest,
    has_ca: bool,
    resolver_relocated: bool,
    service: Option<ServiceScope>,
    listen: Option<SocketAddr>,
    admin_server: Option<String>,
    key_protection: Option<KeyProtArg>,
    lock: Option<ConfigDirLock>,
}

impl Staged {
    /// The role this bundle installs.
    pub fn role(&self) -> InstallRole {
        self.manifest.install.role
    }
}

/// What a finished restore leaves the operator to know.
pub struct RestoreOutcome {
    /// The role now installed and ready.
    pub role: InstallRole,
    /// An OS service the frontend must still register.
    pub service_needed: Option<ServiceScope>,
    /// The CA hierarchy reconciliation, when a relocated resolver forced one.
    pub reconciled: Option<crate::admin_proto::OperationId>,
}

fn scope_root(scope: BundleScope) -> Result<PathBuf> {
    match scope {
        BundleScope::User => paths::user_config_root(),
        BundleScope::System => Ok(paths::system_config_root()),
    }
}

/// Find the install to back up: the explicit root, or whichever of the user /
/// system roots holds a record — refusing to guess when both do.
fn find_install(input: &BackupInput) -> Result<(PathBuf, BundleScope, InstallRecord)> {
    if let Some(root) = &input.config_dir {
        let record = InstallRecord::load(&root.join("install.json"))?;
        return Ok((root.clone(), input.scope.unwrap_or(BundleScope::User), record));
    }
    let user = paths::user_config_root()?;
    let system = paths::system_config_root();
    let candidates = match input.scope {
        Some(BundleScope::User) => vec![(user, BundleScope::User)],
        Some(BundleScope::System) => vec![(system, BundleScope::System)],
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

fn service_intent(input: &BackupInput, record: &InstallRecord) -> Result<ServiceIntent> {
    let scope = match record.role {
        InstallRole::Workstation => ServiceScope::User,
        InstallRole::Ca | InstallRole::Resolver | InstallRole::Publisher => {
            ServiceScope::System
        }
    };
    let for_user = match scope {
        ServiceScope::User => None,
        ServiceScope::System => {
            Some(crate::service::resolve_for_user(input.for_user.clone())?)
        }
    };
    let status = crate::service::status(&ServiceParams {
        scope,
        for_user: for_user.clone(),
        binary: PathBuf::new(),
        service_name: input.service_name.clone(),
        activation_dir: None,
    })?;
    Ok(ServiceIntent {
        scope: match scope {
            ServiceScope::User => BundleScope::User,
            ServiceScope::System => BundleScope::System,
        },
        name: input.service_name.clone(),
        for_user,
        installed: status != ServiceStatus::NotInstalled,
    })
}

/// Capture this host's managed installation into a portable bundle. A CA, when
/// present, is snapshotted through its protected local control socket so the
/// inner bundle stays CA-signed and consistent.
pub async fn backup(ans: &mut dyn Answerer, input: BackupInput) -> Result<BackupOutcome> {
    let (root, scope, record) = find_install(&input)?;
    let service = service_intent(&input, &record)?;
    let has_ca = root.join("ca").is_dir();
    #[cfg(unix)]
    let ca_tmp = tempfile::tempdir().context("creating ca-backup staging directory")?;
    let ca: Option<PathBuf> = if has_ca {
        #[cfg(unix)]
        {
            ans.note("capturing the CA through its local control socket");
            let cfg = root.join("admin-server.json");
            let inner = ca_tmp.path().join("ca");
            crate::local::backup(&cfg, &inner).await?;
            Some(inner)
        }
        #[cfg(not(unix))]
        {
            bail!("a CA backup can only be captured on its unix host")
        }
    } else {
        None
    };
    install_bundle::create(
        &root,
        record,
        scope,
        Some(service),
        ca.as_deref(),
        &input.target,
    )
}

fn restore_addresses(
    manifest: &Manifest,
    input: &RestoreInput,
) -> Result<RestoreAddresses> {
    let resolver = match (input.resolver_listen, input.resolver_bind) {
        (None, None) => None,
        (listen, bind) => {
            let original = manifest.resolver_endpoint.context(
                "--resolver-listen/--resolver-bind require a backup of a CA with a co-located resolver",
            )?;
            let listen = listen.unwrap_or(original.listen);
            Some(install_bundle::ResolverEndpoint {
                listen,
                bind: bind.unwrap_or_else(|| {
                    if input.resolver_listen.is_some() {
                        listen.ip()
                    } else {
                        original.bind
                    }
                }),
            })
        }
    };
    Ok(RestoreAddresses { admin_listen: input.listen, resolver })
}

fn desired_service(manifest: &Manifest, input: &RestoreInput) -> Option<ServiceScope> {
    if input.no_service {
        return None;
    }
    let wanted =
        input.with_service || manifest.service.as_ref().is_some_and(|s| s.installed);
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

/// The service parameters a frontend needs to register the restored service:
/// the bundle's recorded intent, with the caller's overrides applied.
pub fn restored_service(
    manifest: &Manifest,
    service_name: Option<String>,
    for_user: Option<String>,
) -> (String, Option<String>) {
    match &manifest.service {
        Some(intent) => (
            service_name.unwrap_or_else(|| intent.name.clone()),
            for_user.or_else(|| intent.for_user.clone()),
        ),
        None => (
            service_name.unwrap_or_else(|| ServiceParams::DEFAULT_NAME.to_string()),
            for_user,
        ),
    }
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

/// Discover and verify the admin domain a restored host must re-enroll against,
/// pinned to the CA identity the bundle recorded. Polls, because a
/// just-restored CA takes a moment to come up.
pub async fn admin_domain_for_restore(
    manifest: &Manifest,
    override_: Option<&str>,
) -> Result<Option<(SocketAddr, DiscoveredAdminDomain)>> {
    let Some(admin_domain) = &manifest.install.admin_domain else { return Ok(None) };
    let mut seed = match override_ {
        Some(addr) => crate::plan::resolve_admin_server_addr(addr)?,
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
        seed.set_ip(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
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

/// Whether every identity the bundle expects is already present and usable —
/// so a repeated restore doesn't re-enroll what it already fixed.
pub fn identities_complete(root: &Path, manifest: &Manifest) -> bool {
    manifest.identities.iter().all(|recipe| {
        if recipe.kind == IdentityKind::AdminServer {
            #[cfg(unix)]
            return crate::admin_server_config::load(&root.join("admin-server.json"))
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

/// Reissue every non-admin-server machine credential the bundle lists, naming
/// the certificate each one replaces so the CA can revoke it.
pub async fn reenroll_data_identities(
    ans: &mut dyn Answerer,
    root: &Path,
    manifest: &Manifest,
    ca: SocketAddr,
    net: &DiscoveredAdminDomain,
    key_protection: Option<KeyProtArg>,
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

/// Re-enroll the admin server of a restored *satellite* — a host that runs one
/// but does not hold the CA. The CA's own admin server is recovered from the
/// bundle instead, so this is a no-op there.
#[cfg(unix)]
pub async fn reenroll_satellite_admin(
    ans: &mut dyn Answerer,
    config_lock: &ConfigDirLock,
    root: &Path,
    manifest: &Manifest,
    net: &DiscoveredAdminDomain,
    listen_override: Option<SocketAddr>,
) -> Result<()> {
    if !manifest.identities.iter().any(|i| i.kind == IdentityKind::AdminServer)
        || manifest.components.contains(&Component::Ca)
    {
        return Ok(());
    }
    let config_path = root.join("admin-server.json");
    if let Ok(cfg) = crate::admin_server_config::load(&config_path)
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
    let resolver = crate::resolver::ResolverConfig::load(&resolver_path)?;
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

/// The recovered CA must start before its co-located identities can enroll.
/// Units that needed those absent keys may therefore have died once; after
/// enrollment, start only non-running units. Healthy units (especially the CA)
/// are never restarted here.
pub async fn start_restored_units(root: &Path) -> Result<()> {
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

/// Re-send the recovered CA's address, map, and CRL to every registered node.
/// Any peer that could not be reached is a failure: a half-reconciled hierarchy
/// is exactly the inconsistency the operator needs to know about.
#[cfg(unix)]
pub async fn reconcile_restored_ca(
    root: &Path,
) -> Result<crate::admin_proto::OperationId> {
    use poolshark::local::LPooled;
    use std::fmt::Write as _;

    let (operation_id, peers) =
        crate::local::reconcile_ca(&root.join("admin-server.json")).await?;
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

/// Verify the bundle, stage its files at the destination root, and recover a
/// bundled CA. Stops at the point the frontend must register the OS service;
/// [`Staged::next`] says whether that is now or after [`restore_finish`].
pub async fn restore_stage(
    ans: &mut dyn Answerer,
    input: &RestoreInput,
) -> Result<Staged> {
    let bundle = input.bundle.canonicalize().context("canonicalizing backup bundle")?;
    let preflight = install_bundle::verify(&bundle)?;
    let has_ca = preflight.components.contains(&Component::Ca);
    if has_ca && !input.old_ca_fenced {
        bail!(
            "CA restore requires --old-ca-fenced; do not continue until the old CA cannot run"
        );
    }
    let root = match &input.config_dir {
        Some(root) => root.clone(),
        None => scope_root(preflight.config_scope)?,
    };
    let mut lock = Some(ConfigDirLock::acquire_async(&root).await?);
    let addresses = restore_addresses(&preflight, input)?;
    let resolver_relocated = addresses
        .resolver
        .zip(preflight.resolver_endpoint)
        .is_some_and(|(replacement, original)| replacement != original);
    let service = desired_service(&preflight, input);
    if has_ca
        && (!preflight.identities.is_empty() || resolver_relocated)
        && service.is_none()
    {
        bail!(
            "a CA with co-located roles requires restoring its OS service so it can finish enrollment and hierarchy reconciliation"
        );
    }
    ans.note(&format!("restoring {:?} to {}", preflight.components, root.display()));
    if !preflight.identities.is_empty() {
        ans.note(&format!(
            "fresh enrollment required for {} machine credential(s)",
            preflight.identities.len()
        ));
    }
    let manifest =
        install_bundle::restore_files_with_addresses(&bundle, &root, addresses)?;

    #[cfg(not(unix))]
    if has_ca {
        bail!("CA restore is supported only on unix")
    }
    #[cfg(unix)]
    if has_ca {
        recover_bundled_ca(
            ans,
            lock.as_ref().expect("restore lock held"),
            &bundle,
            &root,
            &manifest,
            &addresses,
            input,
        )
        .await?;
    }

    // A recovered CA must be reachable before its co-located identities can
    // pass through the normal enrollment ceremony, so its service goes up now.
    let next = match (has_ca, service) {
        (true, Some(scope)) => {
            drop(lock.take());
            Next::ServiceThenFinish(scope)
        }
        (true, None) => {
            drop(lock.take());
            Next::Finish
        }
        (false, _) => Next::Finish,
    };
    Ok(Staged {
        next,
        root,
        manifest,
        has_ca,
        resolver_relocated,
        service,
        listen: input.listen,
        admin_server: input.admin_server.clone(),
        key_protection: input.key_protection,
        lock,
    })
}

#[cfg(unix)]
async fn recover_bundled_ca(
    ans: &mut dyn Answerer,
    config_lock: &ConfigDirLock,
    bundle: &Path,
    root: &Path,
    manifest: &Manifest,
    addresses: &RestoreAddresses,
    input: &RestoreInput,
) -> Result<()> {
    let ca_dir = root.join("ca");
    let cfg = root.join("admin-server.json");
    if !install_bundle::ca_recovered(bundle, &cfg)? {
        if !install_bundle::ca_snapshot_prepared(bundle, &ca_dir, &cfg)? {
            crate::backup::restore(
                config_lock,
                &bundle.join(install_bundle::CA_DIR),
                &ca_dir,
                &cfg,
            )
            .context("restoring the verified CA state")?;
        }
        let lifetimes = crate::ca::CaLifetimes::load(&ca_dir)?;
        let expired =
            crate::ca::ca_cert_needs_renewal(&ca_dir, std::time::Duration::ZERO);
        if lifetimes.externally_signed && expired && input.external_cert.is_none() {
            bail!(
                "the bundled external-CA certificate has expired; have the external \
                 PKI re-sign this CA key and repeat restore with --external-cert \
                 <certificate> [--external-root <certificate>]"
            );
        }
        if let Some(signed) = &input.external_cert {
            if !lifetimes.externally_signed {
                bail!("--external-cert was supplied for a self-signed netidx CA");
            }
            crate::ops::slots::external_install_cert(
                ans,
                config_lock,
                ca_dir.clone(),
                signed,
                input.external_root.as_deref(),
            )
            .await?;
        }
        crate::ops::slots::recover_ca(
            ans,
            config_lock,
            ca_dir,
            cfg.clone(),
            manifest.admin_listen,
            addresses.resolver.map(|resolver| resolver.listen),
            input.insecure_no_tpm,
        )
        .await?;
    }
    // The inner CA snapshot uses portable recovered-* role paths; reconnect it
    // to the complete role configs restored by the outer bundle.
    let mut cfgv = crate::admin_server_config::load_for_recovery(&cfg)?;
    if let Some(role) = cfgv.roles.resolver.as_mut() {
        role.config = root.join("resolver.json");
    }
    if let Some(role) = cfgv.roles.id_map.as_mut() {
        role.map = root.join("id-map.json");
    }
    crate::admin_server_config::save(config_lock, &cfg, &cfgv)?;
    manifest.install.save(config_lock, &root.join("install.json"))?;
    Ok(())
}

/// Reissue the machine credentials the bundle could not carry, bring the
/// restored units up, and reconcile the hierarchy if the resolver moved.
pub async fn restore_finish(
    ans: &mut dyn Answerer,
    mut staged: Staged,
) -> Result<RestoreOutcome> {
    let Staged { root, manifest, has_ca, .. } = &staged;
    if !manifest.identities.is_empty() && !identities_complete(root, manifest) {
        let Some((ca, net)) =
            admin_domain_for_restore(manifest, staged.admin_server.as_deref()).await?
        else {
            bail!("the backup contains TLS identities but no admin domain identity");
        };
        reenroll_data_identities(ans, root, manifest, ca, &net, staged.key_protection)
            .await?;
        #[cfg(unix)]
        if !has_ca {
            reenroll_satellite_admin(
                ans,
                staged.lock.as_ref().context("restore lock not held")?,
                root,
                manifest,
                &net,
                staged.listen,
            )
            .await?;
        }
        if *has_ca && staged.service.is_some() {
            start_restored_units(root).await?;
        }
    }
    let service_needed = if staged.has_ca {
        None
    } else {
        drop(staged.lock.take());
        staged.service
    };
    let reconciled = {
        #[cfg(unix)]
        {
            if staged.has_ca && staged.resolver_relocated {
                Some(reconcile_restored_ca(&staged.root).await?)
            } else {
                None
            }
        }
        #[cfg(not(unix))]
        {
            None
        }
    };
    Ok(RestoreOutcome { role: staged.manifest.install.role, service_needed, reconciled })
}
