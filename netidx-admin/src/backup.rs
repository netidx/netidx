//! Point-in-time controller recovery bundles.
//!
//! Capture is called while the running daemon holds the mutable state, issuance
//! store, and vault guards. Bytes are collected into memory under that pause;
//! target I/O happens afterwards through a sibling staging directory and one
//! atomic directory publish. Machine-bound serving keys, TPM sidecars,
//! autorenew keytabs, sessions, and lock/temp files are intentionally absent.

use crate::{
    admin_proto::AdminServerId, admin_server_config::AdminServerConfig, atomic,
    fingerprint::Fingerprint,
};
use anyhow::{Context, Result, bail};
use openssl::{
    hash::MessageDigest,
    pkey::PKey,
    sign::{Signer, Verifier},
    x509::X509,
};
use serde_derive::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub const FORMAT_VERSION: u32 = 1;
pub const MANIFEST_FILE: &str = "manifest.json";
pub const MANIFEST_SIGNATURE_FILE: &str = "manifest.sig";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestFile {
    pub path: String,
    pub bytes: u64,
    pub mode: u32,
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub format_version: u32,
    pub created_unix: u64,
    pub ca_fingerprint: String,
    pub controller: AdminServerId,
    pub map_version: u64,
    pub highest_serial: u64,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug)]
struct SnapshotFile {
    path: PathBuf,
    bytes: Vec<u8>,
    mode: u32,
    original_path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct Snapshot {
    files: Vec<SnapshotFile>,
    manifest: Manifest,
    signature: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BackupOutcome {
    pub target: PathBuf,
    pub ca_fingerprint: String,
    pub controller: AdminServerId,
    pub map_version: u64,
    pub highest_serial: u64,
    pub files: u64,
    pub bytes: u64,
    pub manifest_sha256: String,
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
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

fn bundle_path(path: &Path) -> Result<String> {
    if path.is_absolute() || path.components().any(|c| !matches!(c, Component::Normal(_)))
    {
        bail!("invalid backup-relative path {}", path.display());
    }
    Ok(path.to_string_lossy().replace('\\', "/"))
}

fn capture_file(
    files: &mut Vec<SnapshotFile>,
    source: &Path,
    destination: PathBuf,
    original_path: Option<PathBuf>,
) -> Result<()> {
    let meta = fs::symlink_metadata(source)
        .with_context(|| format!("reading metadata for {}", source.display()))?;
    if !meta.file_type().is_file() {
        bail!("backup source {} is not a regular file", source.display());
    }
    let bytes = fs::read(source)
        .with_context(|| format!("capturing backup file {}", source.display()))?;
    files.push(SnapshotFile {
        path: destination,
        bytes,
        mode: file_mode(&meta),
        original_path,
    });
    Ok(())
}

fn capture_resolver_config(
    files: &mut Vec<SnapshotFile>,
    source: &Path,
    destination: PathBuf,
) -> Result<()> {
    let meta = fs::symlink_metadata(source)?;
    let resolver = crate::resolver::ResolverConfig::load(source)?;
    let mut config = resolver.into_file();
    netidx::resolver_server::config::resolve_relative_includes(&mut config, source)?;
    let merged = netidx::resolver_server::config::merge_perms_only(&config)?;
    config.perms = merged;
    config.include_permissions.clear();
    let bytes = serde_json::to_vec_pretty(&config)
        .context("encoding self-contained resolver backup config")?;
    files.push(SnapshotFile {
        path: destination,
        bytes,
        mode: file_mode(&meta),
        original_path: Some(source.to_path_buf()),
    });
    Ok(())
}

fn capture_ca_tree(
    files: &mut Vec<SnapshotFile>,
    ca_dir: &Path,
    dir: &Path,
) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))?
    {
        let entry = entry?;
        let source = entry.path();
        let relative = source.strip_prefix(ca_dir).expect("entry below CA directory");
        let first = relative.components().next();
        // `server/` contains the TPM-bound serving identity recovery replaces.
        // `ca.lock` is process state. A temp file cannot exist while the backup
        // barrier is held unless it predates the daemon; never preserve one.
        if first == Some(Component::Normal("server".as_ref()))
            || relative == Path::new("ca.lock")
            || entry.file_name() == "admin.sock"
            || entry.file_name().to_string_lossy().starts_with(".tmp")
        {
            continue;
        }
        if relative == Path::new("private.key") {
            bail!(
                "refusing to back up a plaintext legacy CA private.key; migrate this CA \
                 into the recovery-slot vault first"
            );
        }
        let meta = fs::symlink_metadata(&source)?;
        if meta.file_type().is_symlink() {
            bail!("refusing symlink in CA backup source: {}", source.display());
        }
        if meta.is_dir() {
            capture_ca_tree(files, ca_dir, &source)?;
        } else if meta.is_file() {
            capture_file(files, &source, Path::new("ca").join(relative), None)?;
        } else {
            bail!("refusing special file in CA backup source: {}", source.display());
        }
    }
    Ok(())
}

/// Capture the controller's recovery assets into memory. The caller must hold
/// every owner of mutable recovery state for this entire call.
pub fn capture(
    cfg: &AdminServerConfig,
    cfg_path: &Path,
    ca_dir: &Path,
    map_version: u64,
    highest_serial: u64,
    ca_key_pem: &[u8],
) -> Result<Snapshot> {
    let mut files = Vec::new();
    capture_ca_tree(&mut files, ca_dir, ca_dir)?;
    capture_file(
        &mut files,
        cfg_path,
        PathBuf::from("admin-server.json"),
        Some(cfg_path.to_path_buf()),
    )?;
    if let Some(role) = &cfg.roles.resolver {
        capture_resolver_config(
            &mut files,
            &role.config,
            PathBuf::from("roles/resolver.json"),
        )?;
    }
    if let Some(role) = &cfg.roles.id_map {
        capture_file(
            &mut files,
            &role.map,
            PathBuf::from("roles/id-map.json"),
            Some(role.map.clone()),
        )?;
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let manifest_files = files
        .iter()
        .map(|file| {
            Ok(ManifestFile {
                path: bundle_path(&file.path)?,
                bytes: file.bytes.len() as u64,
                mode: file.mode,
                sha256: digest(&file.bytes),
                original_path: file
                    .original_path
                    .as_ref()
                    .map(|p| p.to_string_lossy().into_owned()),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        created_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        ca_fingerprint: cfg.home_ca_fingerprint.clone(),
        controller: cfg.server_id,
        map_version,
        highest_serial,
        files: manifest_files,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let key = PKey::private_key_from_pem(ca_key_pem)
        .context("parsing CA backup signing key")?;
    let mut signer = Signer::new(MessageDigest::sha256(), &key)?;
    signer.update(&manifest_bytes)?;
    let signature = signer.sign_to_vec()?;
    Ok(Snapshot { files, manifest, signature })
}

fn normalize_target(target: &Path) -> Result<PathBuf> {
    if !target.is_absolute() {
        bail!("backup target must be an absolute path (got {})", target.display());
    }
    if target.exists() {
        bail!("refusing to overwrite existing backup target {}", target.display());
    }
    let parent = target.parent().context("backup target has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating backup parent {}", parent.display()))?;
    let parent = parent
        .canonicalize()
        .with_context(|| format!("canonicalizing backup parent {}", parent.display()))?;
    let name = target.file_name().context("backup target has no final component")?;
    Ok(parent.join(name))
}

/// Durably publish a captured snapshot. No controller-state locks are held
/// here: all bytes and the manifest were fixed by [`capture`].
pub fn publish(
    snapshot: Snapshot,
    target: &Path,
    ca_dir: &Path,
) -> Result<BackupOutcome> {
    let target = normalize_target(target)?;
    let ca_dir = ca_dir.canonicalize().unwrap_or_else(|_| ca_dir.to_path_buf());
    if target.starts_with(&ca_dir) {
        bail!("backup target must not be inside the live CA directory");
    }
    let parent = target.parent().expect("normalized target has a parent");
    let stage = tempfile::Builder::new()
        .prefix(".netidx-backup-")
        .tempdir_in(parent)
        .with_context(|| {
            format!("creating backup staging directory in {}", parent.display())
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(stage.path(), fs::Permissions::from_mode(0o700))?;
    }
    let bytes = snapshot.files.iter().map(|f| f.bytes.len() as u64).sum();
    for file in &snapshot.files {
        atomic::write_atomic(&stage.path().join(&file.path), &file.bytes, file.mode)?;
    }
    let manifest_bytes = serde_json::to_vec_pretty(&snapshot.manifest)
        .context("encoding backup manifest")?;
    let manifest_sha256 = digest(&manifest_bytes);
    atomic::write_atomic(&stage.path().join(MANIFEST_FILE), &manifest_bytes, 0o600)?;
    atomic::write_atomic(
        &stage.path().join(MANIFEST_SIGNATURE_FILE),
        &snapshot.signature,
        0o600,
    )?;
    let files = snapshot.files.len() as u64;
    let staged = stage.keep();
    if let Err(e) = atomic::publish_dir(&staged, &target) {
        let _ = fs::remove_dir_all(&staged);
        return Err(e);
    }
    Ok(BackupOutcome {
        target,
        ca_fingerprint: snapshot.manifest.ca_fingerprint,
        controller: snapshot.manifest.controller,
        map_version: snapshot.manifest.map_version,
        highest_serial: snapshot.manifest.highest_serial,
        files,
        bytes,
        manifest_sha256,
    })
}

/// Verify every file in a bundle before recovery trusts any identity or path.
pub fn verify(bundle: &Path) -> Result<Manifest> {
    let manifest_path = bundle.join(MANIFEST_FILE);
    let bytes = fs::read(&manifest_path).with_context(|| {
        format!("reading backup manifest {}", manifest_path.display())
    })?;
    let manifest: Manifest =
        serde_json::from_slice(&bytes).context("parsing backup manifest")?;
    if manifest.format_version != FORMAT_VERSION {
        bail!(
            "unsupported backup format {} (expected {FORMAT_VERSION})",
            manifest.format_version
        );
    }
    let signature = fs::read(bundle.join(MANIFEST_SIGNATURE_FILE))
        .context("reading backup manifest signature")?;
    let cert = fs::read(bundle.join("ca/certificate.pem"))?;
    let cert = X509::from_pem(&cert).context("parsing backup CA certificate")?;
    let public_key = cert.public_key()?;
    let mut verifier = Verifier::new(MessageDigest::sha256(), &public_key)?;
    verifier.update(&bytes)?;
    if !verifier.verify(&signature)? {
        bail!("backup manifest signature does not verify against its CA certificate");
    }
    for file in &manifest.files {
        let relative = Path::new(&file.path);
        let _ = bundle_path(relative)?;
        let path = bundle.join(relative);
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
    let mut highest_serial = 0;
    for file in manifest.files.iter().filter(|file| {
        file.path.starts_with("ca/issued/") && file.path.ends_with(".json")
    }) {
        let record: crate::ca_store::IssuedRecord =
            serde_json::from_slice(&fs::read(bundle.join(&file.path))?)
                .with_context(|| format!("parsing issued record {}", file.path))?;
        highest_serial = highest_serial.max(record.serial);
    }
    if highest_serial != manifest.highest_serial {
        bail!(
            "backup issuance index highest serial {} does not match manifest {}",
            highest_serial,
            manifest.highest_serial
        );
    }
    let cert = fs::read(bundle.join("ca/certificate.pem"))?;
    let fp = Fingerprint::of_cert_pem(&cert)?.text();
    if fp != manifest.ca_fingerprint {
        bail!("backup CA certificate does not match the manifest fingerprint");
    }
    let cfg: AdminServerConfig =
        serde_json::from_slice(&fs::read(bundle.join("admin-server.json"))?)?;
    if cfg.server_id != manifest.controller
        || cfg.home_ca_fingerprint != manifest.ca_fingerprint
    {
        bail!("backup admin-server identity does not match its manifest");
    }
    let map: crate::admin_proto::NetworkMap =
        serde_json::from_slice(&fs::read(bundle.join("ca/netmap.json"))?)?;
    if map.controller != manifest.controller || map.version != manifest.map_version {
        bail!("backup network map does not match its manifest");
    }
    Ok(manifest)
}

/// Restore the CA directory and admin-server config from a verified bundle into
/// new destinations. Existing destinations are never overwritten. Captured
/// resolver/id-map files are restored beside the admin config and its role
/// paths are rewritten; machine-bound TLS identities remain recovery's job.
pub fn restore(bundle: &Path, ca_dir: &Path, config_path: &Path) -> Result<Manifest> {
    let manifest = verify(bundle)?;
    if ca_dir.exists() || config_path.exists() {
        let mut expected_cfg: AdminServerConfig =
            serde_json::from_slice(&fs::read(bundle.join("admin-server.json"))?)?;
        let config_parent = config_path.parent().unwrap_or_else(|| Path::new("."));
        let mut roles_match = true;
        if let Some(entry) =
            manifest.files.iter().find(|file| file.path == "roles/resolver.json")
        {
            let path = config_parent.join("recovered-resolver.json");
            roles_match &=
                fs::read(&path).ok() == fs::read(bundle.join(&entry.path)).ok();
            if let Some(role) = expected_cfg.roles.resolver.as_mut() {
                role.config = path;
            }
        }
        if let Some(entry) =
            manifest.files.iter().find(|file| file.path == "roles/id-map.json")
        {
            let path = config_parent.join("recovered-id-map.json");
            roles_match &=
                fs::read(&path).ok() == fs::read(bundle.join(&entry.path)).ok();
            if let Some(role) = expected_cfg.roles.id_map.as_mut() {
                role.map = path;
            }
        }
        let expected_cfg = serde_json::to_vec_pretty(&expected_cfg)?;
        let complete = ca_dir.is_dir()
            && config_path.is_file()
            && roles_match
            && manifest.files.iter().filter(|file| file.path.starts_with("ca/")).all(
                |file| {
                    let relative = Path::new(&file.path).strip_prefix("ca").unwrap();
                    fs::read(ca_dir.join(relative)).ok()
                        == fs::read(bundle.join(&file.path)).ok()
                },
            )
            && fs::read(config_path).ok() == Some(expected_cfg);
        if complete {
            // A prior attempt may have restored the bundle and then rejected a
            // mistyped recovery password before making any writes. Treat that
            // pristine state as the same idempotent restore.
            return Ok(manifest);
        }
        bail!(
            "restore destination already contains different state (CA {}, config {}); \
             refusing to overwrite it. If a prior recovery progressed past password \
             verification, resume without --backup or choose new destinations",
            ca_dir.display(),
            config_path.display()
        );
    }
    let ca_parent = ca_dir.parent().context("CA destination has no parent")?;
    fs::create_dir_all(ca_parent)?;
    let stage =
        tempfile::Builder::new().prefix(".netidx-ca-restore-").tempdir_in(ca_parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(stage.path(), fs::Permissions::from_mode(0o700))?;
    }
    for file in manifest.files.iter().filter(|file| file.path.starts_with("ca/")) {
        let relative =
            Path::new(&file.path).strip_prefix("ca").expect("filtered CA path");
        let contents = fs::read(bundle.join(&file.path))?;
        atomic::write_atomic(&stage.path().join(relative), &contents, file.mode)?;
    }
    let staged = stage.keep();
    if let Err(e) = atomic::publish_dir(&staged, ca_dir) {
        let _ = fs::remove_dir_all(&staged);
        return Err(e);
    }
    let cfg_entry = manifest
        .files
        .iter()
        .find(|file| file.path == "admin-server.json")
        .context("backup manifest contains no admin-server config")?;
    let mut cfg: AdminServerConfig =
        serde_json::from_slice(&fs::read(bundle.join("admin-server.json"))?)?;
    let config_parent = config_path.parent().unwrap_or_else(|| Path::new("."));
    let mut created_role_files = Vec::new();
    let role_result: Result<()> = (|| {
        if let Some(entry) =
            manifest.files.iter().find(|file| file.path == "roles/resolver.json")
        {
            let path = config_parent.join("recovered-resolver.json");
            if path.exists() {
                bail!(
                    "refusing to overwrite restored resolver config {}",
                    path.display()
                );
            }
            atomic::write_atomic(
                &path,
                &fs::read(bundle.join(&entry.path))?,
                entry.mode,
            )?;
            created_role_files.push(path.clone());
            if let Some(role) = cfg.roles.resolver.as_mut() {
                role.config = path;
            }
        }
        if let Some(entry) =
            manifest.files.iter().find(|file| file.path == "roles/id-map.json")
        {
            let path = config_parent.join("recovered-id-map.json");
            if path.exists() {
                bail!("refusing to overwrite restored id-map {}", path.display());
            }
            atomic::write_atomic(
                &path,
                &fs::read(bundle.join(&entry.path))?,
                entry.mode,
            )?;
            created_role_files.push(path.clone());
            if let Some(role) = cfg.roles.id_map.as_mut() {
                role.map = path;
            }
        }
        Ok(())
    })();
    let cfg = serde_json::to_vec_pretty(&cfg)?;
    if let Err(e) =
        role_result.and_then(|()| atomic::write_atomic(config_path, &cfg, cfg_entry.mode))
    {
        // `ca_dir` did not exist before this call and was created solely by us;
        // roll it back so the verified bundle remains cleanly retryable.
        let _ = fs::remove_dir_all(ca_dir);
        for path in created_role_files {
            let _ = fs::remove_file(path);
        }
        return Err(e);
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_traversal_paths() {
        assert!(bundle_path(Path::new("../vault.json")).is_err());
        assert!(bundle_path(Path::new("/vault.json")).is_err());
        assert!(bundle_path(Path::new("ca/vault.json")).is_ok());
    }

    #[test]
    fn resolver_backup_flattens_included_permissions() {
        use netidx::resolver_server::config::file as rfile;
        let dir = tempfile::tempdir().unwrap();
        let perms = dir.path().join("perms.json");
        fs::write(&perms, r#"{"/foo":{"alice":"swl"}}"#).unwrap();
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
        let source = dir.path().join("resolver.json");
        fs::write(&source, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        let mut files = Vec::new();
        capture_resolver_config(&mut files, &source, "roles/resolver.json".into())
            .unwrap();
        let captured: rfile::Config = serde_json::from_slice(&files[0].bytes).unwrap();
        assert!(captured.include_permissions.is_empty());
        assert!(captured.perms.0.contains_key("/foo"));
    }
}
