//! Portable backups of a complete `netidx admin` managed installation.
//!
//! The bundle stores configuration and activation intent, never live process
//! state or a private key sealed to the source machine.  A controller's
//! point-in-time CA snapshot is embedded as `controller/`; that inner bundle
//! remains CA-signed and is produced by the running controller under its
//! mutation barrier.

use crate::{
    atomic,
    provenance::{InstallRecord, InstallRole},
};
use anyhow::{Context, Result, bail};
use serde_derive::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Component as PathComponent, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const FORMAT_VERSION: u32 = 2;
pub const MANIFEST_FILE: &str = "manifest.json";
pub const CONTROLLER_DIR: &str = "controller";
const FILES_DIR: &str = "files";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BundleScope {
    User,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Component {
    Controller,
    Workstation,
    Resolver,
    Publisher,
    IdMap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceIntent {
    pub scope: BundleScope,
    pub name: String,
    pub for_user: Option<String>,
    pub installed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentityKind {
    Client,
    Publisher,
    Workstation,
    Resolver,
    AdminServer,
}

impl IdentityKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Publisher => "publisher",
            Self::Workstation => "workstation",
            Self::Resolver => "resolver",
            Self::AdminServer => "admin server",
        }
    }
}

/// A machine credential deliberately absent from the payload.  The public
/// certificate is retained both to recover the requested name and to make the
/// restore plan inspectable before it contacts the controller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityRecipe {
    pub kind: IdentityKind,
    pub name: String,
    pub certificate: String,
    pub private_key: String,
    pub trusted: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: String,
    pub bytes: u64,
    pub mode: u32,
    pub sha256: String,
}

/// The resolver endpoint owned by a co-located controller, including the
/// actual local bind IP when the advertised address is behind NAT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolverEndpoint {
    pub listen: SocketAddr,
    pub bind: IpAddr,
}

/// Address choices applied while restoring an installation onto a host.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreAddresses {
    pub admin_listen: Option<SocketAddr>,
    pub resolver: Option<ResolverEndpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub created_unix: u64,
    pub install: InstallRecord,
    /// Absolute source root. Restore rewrites this prefix in textual managed
    /// configs and activation units to the destination platform's root.
    pub source_config_root: String,
    pub config_scope: BundleScope,
    pub components: Vec<Component>,
    pub service: Option<ServiceIntent>,
    pub identities: Vec<IdentityRecipe>,
    pub controller_bundle: bool,
    /// Previous admin-server listen address. Satellite restore uses it as the
    /// proposed address in the new enrollment grant; controller restore uses
    /// it unless the operator supplies a replacement address.
    pub admin_listen: Option<SocketAddr>,
    /// Co-located controller resolver endpoint, when the CA-owned map has one.
    pub resolver_endpoint: Option<ResolverEndpoint>,
    pub previous_admin_server: Option<netidx_admin_proto::AdminServerId>,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, Clone)]
pub struct BackupOutcome {
    pub target: PathBuf,
    pub role: InstallRole,
    pub components: Vec<Component>,
    pub files: u64,
    pub bytes: u64,
    pub identities_to_reenroll: usize,
    pub manifest_sha256: String,
}

/// The restored identity's three files are present, parse, and the certificate
/// matches the private key. Used by idempotent restore to distinguish a
/// completed enrollment from the old public certificate retained as metadata.
pub fn identity_files_usable(
    certificate: &Path,
    private_key: &Path,
    trusted: &Path,
) -> bool {
    (|| -> Result<()> {
        let cert_pem = fs::read(certificate)?;
        let certs = rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if certs.is_empty() {
            bail!("identity certificate is empty");
        }
        let key = netidx::tls::load_private_key(None, &private_key.to_string_lossy())?;
        let trusted_pem = fs::read(trusted)?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut std::io::Cursor::new(trusted_pem)) {
            roots.add(cert?)?;
        }
        if roots.is_empty() {
            bail!("identity trust bundle is empty");
        }
        let _ = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)?;
        Ok(())
    })()
    .is_ok()
}

#[derive(Debug)]
struct Captured {
    relative: PathBuf,
    bytes: Vec<u8>,
    mode: u32,
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn restored_file_matches(path: &str, actual: &[u8], expected: &[u8]) -> bool {
    if actual == expected {
        return true;
    }
    // Backup flattens resolver permission includes into a self-contained file and
    // serializes it again. JSON object order is not significant, so that portable
    // form must compare equal to an otherwise unchanged live resolver config.
    path == "resolver.json"
        && matches!(
            (
                serde_json::from_slice::<serde_json::Value>(actual),
                serde_json::from_slice::<serde_json::Value>(expected),
            ),
            (Ok(actual), Ok(expected)) if actual == expected
        )
}

#[cfg(unix)]
fn file_mode(meta: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o777
}

#[cfg(not(unix))]
fn file_mode(_meta: &fs::Metadata) -> u32 {
    0o600
}

fn safe_relative(path: &Path) -> Result<String> {
    if path.is_absolute()
        || path.components().any(|c| !matches!(c, PathComponent::Normal(_)))
    {
        bail!("invalid backup-relative path {}", path.display());
    }
    Ok(path.to_string_lossy().replace('\\', "/"))
}

fn relative_to(root: &Path, path: &Path) -> Result<String> {
    let canonical = path.canonicalize().with_context(|| {
        format!("canonicalizing managed identity path {}", path.display())
    })?;
    safe_relative(canonical.strip_prefix(root).with_context(|| {
        format!(
            "managed identity path {} is outside config root {}; portable backup \
             requires all generated identity files below the config root",
            path.display(),
            root.display()
        )
    })?)
}

fn cert_name(path: &Path) -> Result<String> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let pem = fs::read(path)
        .with_context(|| format!("reading identity certificate {}", path.display()))?;
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .next()
        .context("certificate file is empty")?
        .context("parsing certificate PEM")?;
    let (_, _cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| anyhow!("parsing certificate {}: {e}", path.display()))?;
    crate::tls::first_dns_san_from_der(der.as_ref())
        .ok_or_else(|| anyhow!("certificate {} has no DNS identity", path.display()))
}

/// Serial of the first certificate in a PEM file. Netidx CA serials fit u64.
pub fn certificate_serial(path: &Path) -> Result<u64> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let pem = fs::read(path)
        .with_context(|| format!("reading identity certificate {}", path.display()))?;
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .next()
        .context("certificate file is empty")?
        .context("parsing certificate PEM")?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| anyhow!("parsing certificate {}: {e}", path.display()))?;
    cert.tbs_certificate
        .serial
        .to_string()
        .parse()
        .context("certificate serial does not fit u64")
}

fn push_identity(
    out: &mut Vec<IdentityRecipe>,
    root: &Path,
    kind: IdentityKind,
    certificate: PathBuf,
    private_key: PathBuf,
    trusted: PathBuf,
) -> Result<()> {
    let private_key = relative_to(root, &private_key)?;
    if let Some(existing) = out.iter_mut().find(|i| i.private_key == private_key) {
        // Resolver installs commonly reuse the resolver's TLS identity in the
        // local client config. Classify that one key by its authoritative
        // server use, not by whichever config happened to be scanned first;
        // resolver identities deliberately permit multiple live replicas.
        if kind == IdentityKind::Resolver {
            existing.kind = IdentityKind::Resolver;
        }
        return Ok(());
    }
    out.push(IdentityRecipe {
        kind,
        name: cert_name(&certificate)?,
        certificate: relative_to(root, &certificate)?,
        private_key,
        trusted: relative_to(root, &trusted)?,
    });
    Ok(())
}

/// Inspect the installed configs rather than guessing identity directories.
/// Every exact private-key path returned here is excluded from the payload.
pub fn identity_recipes(root: &Path, role: InstallRole) -> Result<Vec<IdentityRecipe>> {
    let mut out = Vec::new();
    let client = root.join("client.json");
    if client.is_file() {
        let cfg: netidx::config::file::Config =
            serde_json::from_slice(&fs::read(&client)?)
                .with_context(|| format!("parsing {}", client.display()))?;
        if let Some(tls) = cfg.tls {
            let kind = match role {
                InstallRole::Workstation => IdentityKind::Workstation,
                InstallRole::Publisher => IdentityKind::Publisher,
                _ => IdentityKind::Client,
            };
            for (_, id) in tls.identities {
                push_identity(
                    &mut out,
                    root,
                    kind,
                    id.certificate.into(),
                    id.private_key.into(),
                    id.trusted.into(),
                )?;
            }
        }
    }
    let resolver = root.join("resolver.json");
    if resolver.is_file() {
        use netidx::resolver_server::config::file::Auth;
        let cfg: netidx::resolver_server::config::file::Config =
            serde_json::from_slice(&fs::read(&resolver)?)
                .with_context(|| format!("parsing {}", resolver.display()))?;
        for member in cfg.member_servers {
            if let Auth::Tls { trusted, certificate, private_key, .. } = member.auth {
                push_identity(
                    &mut out,
                    root,
                    IdentityKind::Resolver,
                    certificate.as_str().into(),
                    private_key.as_str().into(),
                    trusted.as_str().into(),
                )?;
            }
        }
    }
    #[cfg(unix)]
    {
        let config = root.join("admin-server.json");
        if config.is_file() {
            let cfg = crate::admin_server_config::load(&config)?;
            push_identity(
                &mut out,
                root,
                IdentityKind::AdminServer,
                cfg.serving_cert,
                cfg.serving_key,
                cfg.trusted,
            )?;
        }
    }
    out.sort_by(|a, b| a.private_key.cmp(&b.private_key));
    Ok(out)
}

fn components(root: &Path, role: InstallRole) -> Vec<Component> {
    let mut out = Vec::new();
    match role {
        InstallRole::Controller => out.push(Component::Controller),
        InstallRole::Workstation => out.push(Component::Workstation),
        InstallRole::Resolver => out.push(Component::Resolver),
        InstallRole::Publisher => out.push(Component::Publisher),
    }
    for component in [
        root.join("ca").is_dir().then_some(Component::Controller),
        root.join("resolver.json").is_file().then_some(Component::Resolver),
        root.join("id-map.json").is_file().then_some(Component::IdMap),
    ]
    .into_iter()
    .flatten()
    {
        if !out.contains(&component) {
            out.push(component);
        }
    }
    out
}

fn capture_tree(
    root: &Path,
    dir: &Path,
    omitted: &BTreeSet<PathBuf>,
    out: &mut Vec<Captured>,
) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let relative = path.strip_prefix(root).expect("entry below root");
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            bail!("refusing symlink in backup source: {}", path.display());
        }
        if entry.file_name().to_string_lossy().starts_with(".tmp") {
            continue;
        }
        if relative.components().next() == Some(PathComponent::Normal("ca".as_ref())) {
            continue;
        }
        if meta.is_dir() {
            capture_tree(root, &path, omitted, out)?;
        } else if meta.is_file() {
            if omitted.contains(&path)
                || path.extension().is_some_and(|e| e.eq_ignore_ascii_case("tpm"))
                || path.file_name().is_some_and(|n| n == "autorenew.keytab")
                || path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with(".tmp"))
            {
                continue;
            }
            out.push(Captured {
                relative: relative.to_path_buf(),
                bytes: fs::read(&path)?,
                mode: file_mode(&meta),
            });
        } else if path.extension().is_some_and(|extension| extension == "sock") {
            // Local-auth and protected controller sockets are runtime state.
            // They commonly live beside their config and may be active during
            // an online backup; recreating them is the daemon's job.
            continue;
        } else {
            bail!("refusing special file in backup source: {}", path.display());
        }
    }
    Ok(())
}

fn flatten_resolver_permissions(root: &Path, captured: &mut [Captured]) -> Result<()> {
    let Some(file) =
        captured.iter_mut().find(|file| file.relative == Path::new("resolver.json"))
    else {
        return Ok(());
    };
    let source = root.join("resolver.json");
    let resolver = crate::resolver::ResolverConfig::load(&source)?;
    let mut config = resolver.into_file();
    netidx::resolver_server::config::resolve_relative_includes(&mut config, &source)?;
    config.perms = netidx::resolver_server::config::merge_perms_only(&config)?;
    config.include_permissions.clear();
    file.bytes = serde_json::to_vec_pretty(&config)
        .context("encoding self-contained resolver config for backup")?;
    Ok(())
}

#[cfg(unix)]
fn overlay_controller_roles(
    controller: &Path,
    captured: &mut Vec<Captured>,
) -> Result<()> {
    crate::backup::verify(controller).context("verifying embedded controller backup")?;
    for (source, destination) in
        [("roles/resolver.json", "resolver.json"), ("roles/id-map.json", "id-map.json")]
    {
        let source = controller.join(source);
        if !source.is_file() {
            continue;
        }
        let bytes = fs::read(&source)?;
        let meta = fs::metadata(&source)?;
        match captured.iter_mut().find(|file| file.relative == Path::new(destination)) {
            Some(file) => {
                file.bytes = bytes;
                file.mode = file_mode(&meta);
            }
            None => captured.push(Captured {
                relative: destination.into(),
                bytes,
                mode: file_mode(&meta),
            }),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn controller_resolver_endpoint(
    controller: &Path,
    resolver_config: Option<&[u8]>,
) -> Result<Option<ResolverEndpoint>> {
    let inner = crate::backup::verify(controller)
        .context("verifying embedded controller backup")?;
    let map: netidx_admin_proto::NetworkMap =
        serde_json::from_slice(&fs::read(controller.join("ca/netmap.json"))?)
            .context("parsing the controller backup's authoritative network map")?;
    if map.controller != inner.controller {
        bail!("embedded controller map identity does not match its signed manifest");
    }
    let Some(owned) = map.controller_entry().and_then(|server| server.resolver.as_ref())
    else {
        return Ok(None);
    };
    let bytes = resolver_config.context(
        "the controller map owns a resolver endpoint but the backup has no resolver config",
    )?;
    let config: netidx::resolver_server::config::file::Config =
        serde_json::from_slice(bytes).context("parsing the backed-up resolver config")?;
    let mut matching =
        config.member_servers.iter().filter(|member| member.addr == owned.addr);
    let member = matching.next().with_context(|| {
        format!(
            "the controller's owned resolver endpoint {} is absent from resolver.json",
            owned.addr,
        )
    })?;
    if matching.next().is_some() {
        bail!(
            "the controller's owned resolver endpoint {} appears more than once in resolver.json",
            owned.addr,
        );
    }
    Ok(Some(ResolverEndpoint { listen: member.addr, bind: member.bind_addr }))
}

#[cfg(unix)]
fn copy_controller_bundle(source: &Path, dest: &Path) -> Result<()> {
    fn copy_dir(source: &Path, dest: &Path) -> Result<()> {
        fs::create_dir_all(dest)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let meta = fs::symlink_metadata(entry.path())?;
            if meta.file_type().is_symlink() {
                bail!("refusing symlink in controller bundle");
            }
            let target = dest.join(entry.file_name());
            if meta.is_dir() {
                copy_dir(&entry.path(), &target)?;
            } else if meta.is_file() {
                atomic::write_atomic(
                    &target,
                    &fs::read(entry.path())?,
                    file_mode(&meta),
                )?;
            } else {
                bail!("refusing special file in controller bundle");
            }
        }
        Ok(())
    }
    copy_dir(source, dest)
}

/// Create a portable installation bundle. `controller_bundle`, when present,
/// must already have been captured through the protected local RPC.
pub fn create(
    root: &Path,
    install: InstallRecord,
    config_scope: BundleScope,
    service: Option<ServiceIntent>,
    controller_bundle: Option<&Path>,
    target: &Path,
) -> Result<BackupOutcome> {
    if !target.is_absolute() {
        bail!("backup target must be absolute (got {})", target.display());
    }
    if target.starts_with(root) {
        bail!("backup target must not be inside the live config root");
    }
    let root = root
        .canonicalize()
        .with_context(|| format!("canonicalizing config root {}", root.display()))?;
    let parent = target.parent().context("backup target has no parent")?;
    fs::create_dir_all(parent)?;
    let parent = parent.canonicalize()?;
    let target = parent.join(target.file_name().context("backup target has no name")?);
    if target.exists() {
        bail!("refusing to overwrite existing backup target {}", target.display());
    }
    if target.starts_with(&root) {
        bail!("backup target must not be inside the live config root");
    }
    for path in &install.managed_paths {
        let managed = path.canonicalize().with_context(|| {
            format!("canonicalizing managed install path {}", path.display())
        })?;
        if !managed.starts_with(&root) {
            bail!(
                "managed install path {} is outside config root {}; refusing an \
                 incomplete portable backup. Move the managed file below the config \
                 root or reinstall with canonical paths first",
                path.display(),
                root.display(),
            );
        }
    }
    let all_identities = identity_recipes(&root, install.role)?;
    #[cfg(unix)]
    let admin_config =
        crate::admin_server_config::load(&root.join("admin-server.json")).ok();
    #[cfg(unix)]
    let admin_listen = admin_config.as_ref().map(|cfg| cfg.listen);
    #[cfg(unix)]
    let previous_admin_server = admin_config.as_ref().map(|cfg| cfg.server_id);
    #[cfg(not(unix))]
    let admin_listen = None;
    #[cfg(not(unix))]
    let previous_admin_server = None;
    let mut identities = all_identities.clone();
    if controller_bundle.is_some() {
        identities.retain(|identity| identity.kind != IdentityKind::AdminServer);
    }
    let mut omitted = all_identities
        .iter()
        .flat_map(|i| {
            let key = root.join(&i.private_key);
            [key.clone(), crate::tls::sealed_sidecar(&key)]
        })
        .collect::<BTreeSet<_>>();
    // The signed inner controller bundle owns this file and rewrites its
    // machine-bound serving paths during recovery.
    if controller_bundle.is_some() {
        omitted.insert(root.join("admin-server.json"));
        // The signed inner recovery engine materializes these portable role
        // copies while rebinding a controller. The outer bundle already
        // overlays the live resolver/id-map configs from its new inner
        // snapshot, so carrying old scratch copies makes a later restore
        // collide with the inner engine's own destinations.
        omitted.insert(root.join("recovered-resolver.json"));
        omitted.insert(root.join("recovered-id-map.json"));
    }
    let mut captured = Vec::new();
    capture_tree(&root, &root, &omitted, &mut captured)?;
    flatten_resolver_permissions(&root, &mut captured)?;
    if let Some(controller) = controller_bundle {
        #[cfg(unix)]
        overlay_controller_roles(controller, &mut captured)?;
        #[cfg(not(unix))]
        {
            let _ = controller;
            bail!("controller backup is supported only on unix");
        }
    }
    captured.sort_by(|a, b| a.relative.cmp(&b.relative));
    let files = captured
        .iter()
        .map(|f| {
            Ok(ManifestFile {
                path: safe_relative(&f.relative)?,
                bytes: f.bytes.len() as u64,
                mode: f.mode,
                sha256: digest(&f.bytes),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let components = components(&root, install.role);
    #[cfg(unix)]
    let resolver_endpoint = match controller_bundle {
        Some(controller) if components.contains(&Component::Resolver) => {
            controller_resolver_endpoint(
                controller,
                captured
                    .iter()
                    .find(|file| file.relative == Path::new("resolver.json"))
                    .map(|file| file.bytes.as_slice()),
            )?
        }
        _ => None,
    };
    #[cfg(not(unix))]
    let resolver_endpoint = None;
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        created_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        install: install.clone(),
        source_config_root: root.to_string_lossy().into_owned(),
        config_scope,
        components: components.clone(),
        service,
        identities: identities.clone(),
        controller_bundle: controller_bundle.is_some(),
        admin_listen,
        resolver_endpoint,
        previous_admin_server,
        files,
    };
    let stage =
        tempfile::Builder::new().prefix(".netidx-install-backup-").tempdir_in(&parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(stage.path(), fs::Permissions::from_mode(0o700))?;
    }
    for file in &captured {
        atomic::write_atomic(
            &stage.path().join(FILES_DIR).join(&file.relative),
            &file.bytes,
            file.mode,
        )?;
    }
    if let Some(controller) = controller_bundle {
        #[cfg(unix)]
        {
            crate::backup::verify(controller)
                .context("verifying embedded controller backup")?;
            copy_controller_bundle(controller, &stage.path().join(CONTROLLER_DIR))?;
        }
        #[cfg(not(unix))]
        {
            let _ = controller;
            bail!("controller backup is supported only on unix");
        }
    }
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let manifest_sha256 = digest(&manifest_bytes);
    atomic::write_atomic(&stage.path().join(MANIFEST_FILE), &manifest_bytes, 0o600)?;
    let bytes = captured.iter().map(|f| f.bytes.len() as u64).sum();
    let staged = stage.keep();
    if let Err(e) = atomic::publish_dir(&staged, &target) {
        let _ = fs::remove_dir_all(&staged);
        return Err(e);
    }
    Ok(BackupOutcome {
        target,
        role: install.role,
        components,
        files: captured.len() as u64,
        bytes,
        identities_to_reenroll: identities.len(),
        manifest_sha256,
    })
}

/// Verify the manifest and every captured config before any restore write.
pub fn verify(bundle: &Path) -> Result<Manifest> {
    let bytes = fs::read(bundle.join(MANIFEST_FILE))?;
    let manifest: Manifest =
        serde_json::from_slice(&bytes).context("parsing install backup manifest")?;
    if manifest.format_version != FORMAT_VERSION {
        bail!(
            "unsupported install backup format {} (expected {FORMAT_VERSION})",
            manifest.format_version
        );
    }
    let mut seen = BTreeSet::new();
    for file in &manifest.files {
        let relative = Path::new(&file.path);
        let _ = safe_relative(relative)?;
        if !seen.insert(file.path.clone()) {
            bail!("duplicate backup manifest path {}", file.path);
        }
        let path = bundle.join(FILES_DIR).join(relative);
        let meta = fs::symlink_metadata(&path)
            .with_context(|| format!("backup file {} is missing", path.display()))?;
        if !meta.file_type().is_file() {
            bail!("backup entry {} is not a regular file", path.display());
        }
        let contents = fs::read(&path)?;
        if contents.len() as u64 != file.bytes || digest(&contents) != file.sha256 {
            bail!("backup file {} failed its manifest hash", path.display());
        }
    }
    let install_entry = manifest
        .files
        .iter()
        .find(|file| file.path == "install.json")
        .context("backup contains no install.json")?;
    let captured_install: InstallRecord = serde_json::from_slice(&fs::read(
        bundle.join(FILES_DIR).join(&install_entry.path),
    )?)
    .context("parsing captured install.json")?;
    if captured_install != manifest.install {
        bail!("captured install.json does not match the backup manifest");
    }
    for identity in &manifest.identities {
        for path in [&identity.certificate, &identity.private_key, &identity.trusted] {
            let _ = safe_relative(Path::new(path))?;
        }
        if seen.contains(&identity.private_key)
            || seen.contains(
                &crate::tls::sealed_sidecar(Path::new(&identity.private_key))
                    .to_string_lossy()
                    .replace('\\', "/"),
            )
        {
            bail!(
                "machine credential {} was included in the portable payload",
                identity.private_key
            );
        }
    }
    if manifest.controller_bundle {
        #[cfg(unix)]
        {
            crate::backup::verify(&bundle.join(CONTROLLER_DIR))
                .context("verifying embedded controller recovery bundle")?;
            let outer_resolver = manifest
                .files
                .iter()
                .find(|file| file.path == "resolver.json")
                .map(|file| fs::read(bundle.join(FILES_DIR).join(&file.path)))
                .transpose()?;
            let inner_resolver =
                match fs::read(bundle.join(CONTROLLER_DIR).join("roles/resolver.json")) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => return Err(error.into()),
                };
            if outer_resolver != inner_resolver {
                bail!(
                    "the resolver config in the install bundle does not match the signed controller snapshot"
                );
            }
            let endpoint = controller_resolver_endpoint(
                &bundle.join(CONTROLLER_DIR),
                inner_resolver.as_deref(),
            )?;
            if endpoint != manifest.resolver_endpoint {
                bail!(
                    "the resolver endpoint in the install manifest does not match the signed controller snapshot"
                );
            }
        }
        #[cfg(not(unix))]
        bail!("controller restore is supported only on unix");
    } else if manifest.resolver_endpoint.is_some() {
        bail!(
            "a backup without a controller may not claim a controller resolver endpoint"
        );
    }
    Ok(manifest)
}

/// Whether a previous restore attempt has already completed the irreversible
/// controller-rebinding phase. This is the resume discriminator between the
/// pristine inner snapshot (whose old machine key is absent) and a fully
/// rebound controller that must not be recovered a second time.
#[cfg(unix)]
pub fn controller_recovered(bundle: &Path, config: &Path) -> Result<bool> {
    let inner_dir = bundle.join(CONTROLLER_DIR);
    let inner = crate::backup::verify(&inner_dir)?;
    let config_parent =
        config.parent().context("restored admin-server config has no parent")?;
    let cfg = match crate::admin_server_config::load(config) {
        Ok(cfg) => cfg,
        Err(_) => return Ok(false),
    };
    if cfg.server_id != inner.controller
        || cfg.home_ca_fingerprint != inner.ca_fingerprint
        || !cfg.serving_key.is_file()
    {
        return Ok(false);
    }
    for (source, destination) in [
        ("roles/resolver.json", "recovered-resolver.json"),
        ("roles/id-map.json", "recovered-id-map.json"),
    ] {
        let source = inner_dir.join(source);
        if source.is_file()
            && fs::read(config_parent.join(destination)).ok() != fs::read(source).ok()
        {
            return Ok(false);
        }
    }
    let autorenew = cfg.roles.ca.as_ref().and_then(|role| role.autorenew.as_ref());
    Ok(autorenew.is_some_and(|path| path.is_file()))
}

/// The inner controller snapshot is present but has not yet been rebound. An
/// external-CA restore may legitimately replace only `certificate.pem` and
/// `trusted.pem` (with the same CA key) before machine credential generation;
/// recognize that retryable intermediate state without accepting arbitrary
/// divergence in the vault, map, issuance records, or admin config.
#[cfg(unix)]
pub fn controller_snapshot_prepared(
    bundle: &Path,
    ca_dir: &Path,
    config: &Path,
) -> Result<bool> {
    let inner_dir = bundle.join(CONTROLLER_DIR);
    let inner = crate::backup::verify(&inner_dir)?;
    let cfg: crate::admin_server_config::AdminServerConfig =
        match crate::admin_server_config::load_for_recovery(config) {
            Ok(cfg) => cfg,
            Err(_) => return Ok(false),
        };
    let mut expected: crate::admin_server_config::AdminServerConfig =
        serde_json::from_slice(&fs::read(inner_dir.join("admin-server.json"))?)?;
    let config_parent = config.parent().unwrap_or_else(|| Path::new("."));
    if let Some(role) = expected.roles.resolver.as_mut() {
        role.config = config_parent.join("recovered-resolver.json");
    }
    if let Some(role) = expected.roles.id_map.as_mut() {
        role.map = config_parent.join("recovered-id-map.json");
    }
    if cfg.server_id != inner.controller
        || cfg.home_ca_fingerprint != inner.ca_fingerprint
        || serde_json::to_vec_pretty(&cfg)? != serde_json::to_vec_pretty(&expected)?
    {
        return Ok(false);
    }
    let live_cert = match fs::read(ca_dir.join("certificate.pem")) {
        Ok(cert) => cert,
        Err(_) => return Ok(false),
    };
    if netidx_admin_proto::fingerprint::Fingerprint::of_cert_pem(&live_cert)?.text()
        != inner.ca_fingerprint
    {
        return Ok(false);
    }
    for file in inner.files.iter().filter(|file| file.path.starts_with("ca/")) {
        let relative = Path::new(&file.path).strip_prefix("ca").unwrap();
        if relative == Path::new("certificate.pem")
            || relative == Path::new("trusted.pem")
        {
            continue;
        }
        if fs::read(ca_dir.join(relative)).ok()
            != fs::read(inner_dir.join(&file.path)).ok()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Atomically publish the captured configuration at `root`. Existing roots are
/// refused: restore is an install, never an implicit merge with live state.
pub fn restore_files(bundle: &Path, root: &Path) -> Result<Manifest> {
    restore_files_with_addresses(bundle, root, RestoreAddresses::default())
}

fn local_client_bind(endpoint: ResolverEndpoint) -> Option<String> {
    if endpoint.listen.ip().is_loopback() && endpoint.bind.is_loopback() {
        return None;
    }
    let prefix = if endpoint.bind.is_ipv4() { 32 } else { 128 };
    Some(if endpoint.listen.ip() == endpoint.bind {
        format!("{}/{prefix}", endpoint.bind)
    } else {
        format!("{}@{}/{prefix}", endpoint.listen.ip(), endpoint.bind)
    })
}

fn restored_bytes(
    file: &ManifestFile,
    source: Vec<u8>,
    manifest: &Manifest,
    root: &Path,
    addresses: RestoreAddresses,
) -> Result<Vec<u8>> {
    let destination = root.to_string_lossy();
    let mut restored = match std::str::from_utf8(&source) {
        Ok(text) if manifest.source_config_root != destination => {
            text.replace(&manifest.source_config_root, &destination).into_bytes()
        }
        _ => source,
    };
    if file.path == "install.json"
        && addresses.admin_listen.is_some()
        && addresses.admin_listen != manifest.install.admin_server
        && manifest.components.contains(&Component::Controller)
    {
        let mut install: InstallRecord = serde_json::from_slice(&restored)?;
        install.admin_server = addresses.admin_listen;
        restored = serde_json::to_vec_pretty(&install)?;
    }
    let Some(replacement) = addresses.resolver else { return Ok(restored) };
    let original = manifest.resolver_endpoint.context(
        "this backup has no co-located controller resolver endpoint to relocate",
    )?;
    if replacement == original {
        return Ok(restored);
    }
    match file.path.as_str() {
        "resolver.json" => {
            let mut config: netidx::resolver_server::config::file::Config =
                serde_json::from_slice(&restored)?;
            let mut matching = config
                .member_servers
                .iter_mut()
                .filter(|member| member.addr == original.listen);
            let member = matching.next().with_context(|| {
                format!(
                    "the controller's owned resolver endpoint {} is absent from resolver.json",
                    original.listen,
                )
            })?;
            member.addr = replacement.listen;
            member.bind_addr = replacement.bind;
            if matching.next().is_some() {
                bail!(
                    "the controller's owned resolver endpoint {} appears more than once in resolver.json",
                    original.listen,
                );
            }
            restored = serde_json::to_vec_pretty(&config)?;
        }
        "client.json" => {
            let mut config: netidx::config::file::Config =
                serde_json::from_slice(&restored)?;
            let mut changed = false;
            for (addr, _) in &mut config.addrs {
                if *addr == original.listen {
                    *addr = replacement.listen;
                    changed = true;
                }
            }
            if changed {
                config.default_bind_config = local_client_bind(replacement);
                restored = serde_json::to_vec_pretty(&config)?;
            }
        }
        _ => {}
    }
    Ok(restored)
}

/// Restore with replacement host addresses. A controller's admin address and
/// co-located resolver endpoint are applied to every local config before the
/// staged directory is published.
pub fn restore_files_with_addresses(
    bundle: &Path,
    root: &Path,
    addresses: RestoreAddresses,
) -> Result<Manifest> {
    let manifest = verify(bundle)?;
    if addresses.resolver.is_some() && manifest.resolver_endpoint.is_none() {
        bail!("this backup has no co-located controller resolver endpoint to relocate");
    }
    let relocate_manifest = |mut manifest: Manifest| -> Result<Manifest> {
        let source = Path::new(&manifest.source_config_root);
        for path in &mut manifest.install.managed_paths {
            if let Ok(relative) = path.strip_prefix(source) {
                *path = root.join(relative);
            }
        }
        if let Some(listen) = addresses.admin_listen {
            manifest.admin_listen = Some(listen);
            if manifest.components.contains(&Component::Controller) {
                manifest.install.admin_server = Some(listen);
            }
        }
        if let Some(resolver) = addresses.resolver {
            manifest.resolver_endpoint = Some(resolver);
        }
        manifest.source_config_root = root.to_string_lossy().into_owned();
        Ok(manifest)
    };
    if root.exists() {
        let mut restored_identity_files = BTreeSet::new();
        for identity in &manifest.identities {
            let certificate = root.join(&identity.certificate);
            let key = root.join(&identity.private_key);
            let trusted = root.join(&identity.trusted);
            if identity_files_usable(&certificate, &key, &trusted) {
                restored_identity_files.insert(identity.certificate.clone());
                restored_identity_files.insert(identity.trusted.clone());
                if let Some(parent) = Path::new(&identity.certificate).parent() {
                    let crl = parent.join("crl.pem");
                    if root.join(&crl).is_file() {
                        restored_identity_files
                            .insert(crl.to_string_lossy().replace('\\', "/"));
                    }
                }
            }
        }
        #[cfg(unix)]
        if manifest
            .identities
            .iter()
            .any(|identity| identity.kind == IdentityKind::AdminServer)
            && crate::admin_server_config::load(&root.join("admin-server.json"))
                .is_ok_and(|cfg| {
                    Some(cfg.server_id) != manifest.previous_admin_server
                        && identity_files_usable(
                            &cfg.serving_cert,
                            &cfg.serving_key,
                            &cfg.trusted,
                        )
                })
        {
            restored_identity_files.insert("admin-server.json".to_string());
        }
        let complete = manifest.files.iter().all(|file| {
            if restored_identity_files.contains(&file.path) {
                return true;
            }
            let Ok(source) = fs::read(bundle.join(FILES_DIR).join(&file.path)) else {
                return false;
            };
            let expected = match restored_bytes(file, source, &manifest, root, addresses)
            {
                Ok(expected) => expected,
                Err(_) => return false,
            };
            fs::read(root.join(&file.path))
                .is_ok_and(|actual| restored_file_matches(&file.path, &actual, &expected))
        });
        if complete {
            return relocate_manifest(manifest);
        }
        bail!(
            "restore destination {} already contains different state; refusing to merge a backup with a live install",
            root.display()
        );
    }
    let parent = root.parent().context("config root has no parent")?;
    fs::create_dir_all(parent)?;
    let stage =
        tempfile::Builder::new().prefix(".netidx-install-restore-").tempdir_in(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(stage.path(), fs::Permissions::from_mode(0o700))?;
    }
    for file in &manifest.files {
        let source = fs::read(bundle.join(FILES_DIR).join(&file.path))?;
        let restored = restored_bytes(file, source, &manifest, root, addresses)?;
        atomic::write_atomic(&stage.path().join(&file.path), &restored, file.mode)?;
    }
    let staged = stage.keep();
    if let Err(e) = atomic::publish_dir(&staged, root) {
        let _ = fs::remove_dir_all(&staged);
        return Err(e);
    }
    relocate_manifest(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> InstallRecord {
        InstallRecord::new(InstallRole::Publisher, "/", "anonymous", None, None)
    }

    #[test]
    fn ordinary_bundle_round_trip_and_no_overwrite() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().join("source");
        fs::create_dir_all(root.join("activation")).unwrap();
        let rec = record();
        fs::write(root.join("install.json"), serde_json::to_vec(&rec).unwrap()).unwrap();
        let client = netidx::config::file::ConfigBuilder::default()
            .addrs(Vec::new())
            .build()
            .unwrap();
        fs::write(root.join("client.json"), serde_json::to_vec(&client).unwrap())
            .unwrap();
        fs::write(root.join("activation/renew.unit"), b"{}").unwrap();
        let bundle = td.path().join("bundle");
        let out = create(&root, rec, BundleScope::User, None, None, &bundle).unwrap();
        assert_eq!(out.role, InstallRole::Publisher);
        let restored = td.path().join("restored");
        restore_files(&bundle, &restored).unwrap();
        assert_eq!(
            fs::read(root.join("install.json")).unwrap(),
            fs::read(restored.join("install.json")).unwrap()
        );
        restore_files(&bundle, &restored).unwrap();
        fs::write(restored.join("install.json"), b"different").unwrap();
        assert!(restore_files(&bundle, &restored).is_err());
    }

    #[test]
    fn resolver_json_object_order_is_semantically_equivalent() {
        assert!(restored_file_matches(
            "resolver.json",
            br#"{"perms":{"/":{"users":"swl","resolver":"swlpd"}}}"#,
            br#"{"perms":{"/":{"resolver":"swlpd","users":"swl"}}}"#,
        ));
        assert!(!restored_file_matches(
            "id-map.json",
            br#"{"identities":{"alice":{}}}"#,
            br#"{"identities":{}}"#,
        ));
        assert!(!restored_file_matches("resolver.json", b"invalid", b"also invalid"));
    }

    #[test]
    fn controller_resolver_restore_rewrites_one_owned_endpoint_everywhere() {
        use netidx::{config::file as cfile, resolver_server::config::file as rfile};

        let old_admin: SocketAddr = "10.0.0.1:4565".parse().unwrap();
        let new_admin: SocketAddr = "10.1.0.4:5565".parse().unwrap();
        let old = ResolverEndpoint {
            listen: "10.0.0.1:4564".parse().unwrap(),
            bind: "10.0.0.1".parse().unwrap(),
        };
        let new = ResolverEndpoint {
            listen: "203.0.113.4:5564".parse().unwrap(),
            bind: "10.1.0.4".parse().unwrap(),
        };
        let mut install = InstallRecord::new(
            InstallRole::Resolver,
            "/",
            "anonymous",
            None,
            Some(old_admin),
        );
        install.created_unix = 1;
        let manifest = Manifest {
            format_version: FORMAT_VERSION,
            created_unix: 1,
            install: install.clone(),
            source_config_root: "/old/netidx".into(),
            config_scope: BundleScope::System,
            components: vec![Component::Controller, Component::Resolver],
            service: None,
            identities: vec![],
            controller_bundle: true,
            admin_listen: Some(old_admin),
            resolver_endpoint: Some(old),
            previous_admin_server: None,
            files: vec![],
        };
        let file = |path: &str| ManifestFile {
            path: path.into(),
            bytes: 0,
            mode: 0o600,
            sha256: String::new(),
        };
        let resolver = rfile::ConfigBuilder::default()
            .member_servers(vec![
                rfile::MemberServerBuilder::default()
                    .addr(old.listen)
                    .bind_addr(old.bind)
                    .auth(rfile::Auth::Anonymous)
                    .build()
                    .unwrap(),
                rfile::MemberServerBuilder::default()
                    .addr("10.0.0.2:4564".parse().unwrap())
                    .bind_addr("10.0.0.2".parse().unwrap())
                    .auth(rfile::Auth::Anonymous)
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();
        let client = cfile::Config {
            base: "/".into(),
            addrs: vec![
                (old.listen, cfile::Auth::Anonymous),
                ("10.0.0.2:4564".parse().unwrap(), cfile::Auth::Anonymous),
            ],
            tls: None,
            default_auth: netidx::config::DefaultAuthMech::Anonymous,
            default_bind_config: Some("10.0.0.1/32".into()),
        };
        let addresses =
            RestoreAddresses { admin_listen: Some(new_admin), resolver: Some(new) };

        let resolver: rfile::Config = serde_json::from_slice(
            &restored_bytes(
                &file("resolver.json"),
                serde_json::to_vec(&resolver).unwrap(),
                &manifest,
                Path::new("/new/netidx"),
                addresses,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(resolver.member_servers[0].addr, new.listen);
        assert_eq!(resolver.member_servers[0].bind_addr, new.bind);
        assert_eq!(
            resolver.member_servers[1].addr,
            "10.0.0.2:4564".parse::<SocketAddr>().unwrap()
        );

        let client: cfile::Config = serde_json::from_slice(
            &restored_bytes(
                &file("client.json"),
                serde_json::to_vec(&client).unwrap(),
                &manifest,
                Path::new("/new/netidx"),
                addresses,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(client.addrs[0].0, new.listen);
        assert_eq!(client.addrs[1].0, "10.0.0.2:4564".parse().unwrap());
        assert_eq!(
            client.default_bind_config.as_deref(),
            Some("203.0.113.4@10.1.0.4/32")
        );

        let restored_install: InstallRecord = serde_json::from_slice(
            &restored_bytes(
                &file("install.json"),
                serde_json::to_vec(&install).unwrap(),
                &manifest,
                Path::new("/new/netidx"),
                addresses,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(restored_install.admin_server, Some(new_admin));
    }

    #[test]
    fn manifest_tampering_is_detected() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().join("source");
        fs::create_dir_all(&root).unwrap();
        let rec = record();
        fs::write(root.join("install.json"), serde_json::to_vec(&rec).unwrap()).unwrap();
        let bundle = td.path().join("bundle");
        create(&root, rec, BundleScope::User, None, None, &bundle).unwrap();
        fs::write(bundle.join("files/install.json"), b"evil").unwrap();
        assert!(verify(&bundle).is_err());
    }

    #[test]
    fn refuses_to_silently_omit_a_managed_path_outside_the_config_root() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().join("source");
        fs::create_dir_all(&root).unwrap();
        let mut rec = record();
        let external = td.path().join("external/client.json");
        fs::create_dir_all(external.parent().unwrap()).unwrap();
        fs::write(&external, b"{}").unwrap();
        rec.set_managed_paths(vec![external]);
        fs::write(root.join("install.json"), serde_json::to_vec(&rec).unwrap()).unwrap();
        let error =
            create(&root, rec, BundleScope::User, None, None, &td.path().join("bundle"))
                .unwrap_err();
        assert!(error.to_string().contains("incomplete portable backup"));
    }

    #[cfg(unix)]
    #[test]
    fn live_local_sockets_are_runtime_state_not_backup_failures() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().join("source");
        fs::create_dir_all(&root).unwrap();
        let rec = record();
        fs::write(root.join("install.json"), serde_json::to_vec(&rec).unwrap()).unwrap();
        let _socket =
            match std::os::unix::net::UnixListener::bind(root.join("admin.sock")) {
                Ok(socket) => socket,
                // Some hermetic test runners prohibit AF_UNIX even below a writable
                // temporary directory. The behavior is exercised wherever the
                // platform permits constructing the special file.
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    return;
                }
                Err(error) => panic!("binding test socket: {error}"),
            };
        let bundle = td.path().join("bundle");
        create(&root, rec, BundleScope::User, None, None, &bundle).unwrap();
        assert!(!bundle.join("files/admin.sock").exists());
    }

    #[test]
    fn backup_target_cannot_be_inside_the_live_install() {
        let td = tempfile::tempdir().unwrap();
        let root = td.path().join("source");
        fs::create_dir_all(&root).unwrap();
        let rec = record();
        fs::write(root.join("install.json"), serde_json::to_vec(&rec).unwrap()).unwrap();
        let target = root.join("backups/one");
        assert!(create(&root, rec, BundleScope::User, None, None, &target).is_err());
        assert!(!root.join("backups").exists());
    }

    #[test]
    fn resolver_permission_includes_are_made_self_contained() {
        use netidx::resolver_server::config::file as rfile;
        let td = tempfile::tempdir().unwrap();
        let root = td.path().join("source");
        fs::create_dir_all(&root).unwrap();
        let rec = record();
        fs::write(root.join("install.json"), serde_json::to_vec(&rec).unwrap()).unwrap();
        fs::write(root.join("perms.json"), r#"{"/eu":{"alice":"swl"}}"#).unwrap();
        let member = rfile::MemberServerBuilder::default()
            .addr("127.0.0.1:4564".parse().unwrap())
            .bind_addr("127.0.0.1".parse().unwrap())
            .auth(rfile::Auth::Anonymous)
            .build()
            .unwrap();
        let config = rfile::ConfigBuilder::default()
            .member_servers(vec![member])
            .include_permissions(vec!["perms.json".into()])
            .build()
            .unwrap();
        fs::write(root.join("resolver.json"), serde_json::to_vec(&config).unwrap())
            .unwrap();
        let bundle = td.path().join("bundle");
        create(&root, rec, BundleScope::User, None, None, &bundle).unwrap();
        let restored: rfile::Config = serde_json::from_slice(
            &fs::read(bundle.join("files/resolver.json")).unwrap(),
        )
        .unwrap();
        assert!(restored.include_permissions.is_empty());
        assert!(restored.perms.0.contains_key("/eu"));
    }

    #[test]
    fn portable_bundle_omits_machine_private_keys_and_records_reenrollment() {
        use arcstr::ArcStr;
        use netidx::resolver_server::config::file as rfile;
        let td = tempfile::tempdir().unwrap();
        let root = td.path().join("source");
        let identity = root.join("tls/resolver.example.com");
        fs::create_dir_all(&identity).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["resolver.example.com".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap()
            .pem();
        fs::write(identity.join("certificate.pem"), &cert).unwrap();
        fs::write(identity.join("trusted.pem"), &cert).unwrap();
        fs::write(identity.join("private.key"), b"machine secret").unwrap();
        fs::write(identity.join("private.key.tpm"), b"machine sidecar").unwrap();
        let member = rfile::MemberServerBuilder::default()
            .addr("127.0.0.1:4564".parse().unwrap())
            .bind_addr("127.0.0.1".parse().unwrap())
            .auth(rfile::Auth::Tls {
                name: ArcStr::from("resolver.example.com"),
                trusted: ArcStr::from(
                    identity.join("trusted.pem").to_string_lossy().as_ref(),
                ),
                certificate: ArcStr::from(
                    identity.join("certificate.pem").to_string_lossy().as_ref(),
                ),
                private_key: ArcStr::from(
                    identity.join("private.key").to_string_lossy().as_ref(),
                ),
            })
            .build()
            .unwrap();
        let config =
            rfile::ConfigBuilder::default().member_servers(vec![member]).build().unwrap();
        fs::write(root.join("resolver.json"), serde_json::to_vec(&config).unwrap())
            .unwrap();
        let rec = InstallRecord::new(InstallRole::Resolver, "/", "tls", None, None);
        fs::write(root.join("install.json"), serde_json::to_vec(&rec).unwrap()).unwrap();
        let bundle = td.path().join("bundle");
        create(&root, rec, BundleScope::User, None, None, &bundle).unwrap();
        let manifest = verify(&bundle).unwrap();
        assert_eq!(manifest.identities.len(), 1);
        assert_eq!(manifest.identities[0].kind, IdentityKind::Resolver);
        assert_eq!(manifest.identities[0].name, "resolver.example.com");
        assert!(!bundle.join("files/tls/resolver.example.com/private.key").exists());
        assert!(!bundle.join("files/tls/resolver.example.com/private.key.tpm").exists());
        assert!(bundle.join("files/tls/resolver.example.com/certificate.pem").exists());

        let restored = td.path().join("restored");
        restore_files(&bundle, &restored).unwrap();
        let restored_identity = restored.join("tls/resolver.example.com");
        fs::write(restored_identity.join("private.key"), key.serialize_pem()).unwrap();
        assert!(identity_files_usable(
            &restored_identity.join("certificate.pem"),
            &restored_identity.join("private.key"),
            &restored_identity.join("trusted.pem"),
        ));
        // A resumed restore accepts a completed enrollment even though its public
        // credential files no longer match the backup's enrollment metadata.
        let fresh_key = rcgen::KeyPair::generate().unwrap();
        let fresh_cert =
            rcgen::CertificateParams::new(vec!["resolver.example.com".into()])
                .unwrap()
                .self_signed(&fresh_key)
                .unwrap()
                .pem();
        fs::write(restored_identity.join("certificate.pem"), &fresh_cert).unwrap();
        fs::write(restored_identity.join("trusted.pem"), &fresh_cert).unwrap();
        fs::write(restored_identity.join("private.key"), fresh_key.serialize_pem())
            .unwrap();
        restore_files(&bundle, &restored).unwrap();

        // A partial write is not mistaken for a completed enrollment.
        fs::write(restored_identity.join("certificate.pem"), &cert).unwrap();
        assert!(restore_files(&bundle, &restored).is_err());
    }
}
