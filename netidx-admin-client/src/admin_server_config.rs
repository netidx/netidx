use crate::{atomic, config_lock::ConfigDirLock};
use anyhow::{Context, Result};
use std::path::Path;

pub use netidx_admin_proto::server_config::{
    AdminServerConfig, CaRole, IdMapRole, ResolverRole, Roles,
};

pub fn load_for_recovery(path: &Path) -> Result<AdminServerConfig> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading admin-server config {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
}

pub fn load(path: &Path) -> Result<AdminServerConfig> {
    let cfg = load_for_recovery(path)?;
    validate(&cfg)
        .with_context(|| format!("invalid admin-server config {}", path.display()))?;
    Ok(cfg)
}

pub async fn load_for_recovery_async(path: &Path) -> Result<AdminServerConfig> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("reading admin-server config {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
}

pub async fn load_async(path: &Path) -> Result<AdminServerConfig> {
    let cfg = load_for_recovery_async(path).await?;
    validate_async(&cfg)
        .await
        .with_context(|| format!("invalid admin-server config {}", path.display()))?;
    Ok(cfg)
}

pub fn validate(cfg: &AdminServerConfig) -> Result<()> {
    cfg.validate_structure()?;
    let pem = std::fs::read(&cfg.serving_cert).with_context(|| {
        format!("reading serving certificate {}", cfg.serving_cert.display())
    })?;
    cfg.validate_with_serving_cert(&pem)
}

pub async fn validate_async(cfg: &AdminServerConfig) -> Result<()> {
    cfg.validate_structure()?;
    let pem = tokio::fs::read(&cfg.serving_cert).await.with_context(|| {
        format!("reading serving certificate {}", cfg.serving_cert.display())
    })?;
    cfg.validate_with_serving_cert(&pem)
}

pub fn save(
    config_lock: &ConfigDirLock,
    path: &Path,
    cfg: &AdminServerConfig,
) -> Result<()> {
    let path = config_lock.require_contained(path)?;
    let bytes =
        serde_json::to_vec_pretty(cfg).context("serializing admin-server config")?;
    atomic::write_atomic(&path, &bytes, 0o644)
}

pub async fn save_async(
    config_lock: &ConfigDirLock,
    path: &Path,
    cfg: &AdminServerConfig,
) -> Result<()> {
    let path = config_lock.require_contained(path)?;
    let bytes =
        serde_json::to_vec_pretty(cfg).context("serializing admin-server config")?;
    atomic::write_atomic_async(&path, &bytes, 0o644).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use netidx_admin_proto::{AdminServerId, fingerprint::Fingerprint};
    use std::path::PathBuf;

    fn sample() -> AdminServerConfig {
        AdminServerConfig {
            domain: "ryu-oh.org".to_string(),
            server_id: AdminServerId::new(),
            home_ca_fingerprint: Fingerprint::of_der(b"sample-ca").text(),
            listen: "192.168.0.5:4565".parse().unwrap(),
            serving_cert: PathBuf::from("/etc/netidx/CA/server/cert.pem"),
            serving_key: PathBuf::from("/etc/netidx/CA/server/key.pem"),
            trusted: PathBuf::from("/etc/netidx/tls/trusted.pem"),
            roles: Roles {
                ca: Some(CaRole {
                    dir: PathBuf::from("/etc/netidx/CA"),
                    autorenew: Some(PathBuf::from("/etc/netidx/autorenew.keytab")),
                    session_absolute_lifetime: None,
                    session_idle_timeout: None,
                }),
                resolver: Some(ResolverRole {
                    config: PathBuf::from("/etc/netidx/resolver.json"),
                }),
                id_map: Some(IdMapRole { map: PathBuf::from("/etc/netidx/id-map.json") }),
            },
            ca_addr: None,
            peers: vec!["192.168.0.6:4565".parse().unwrap()],
            mdns: true,
            activation_units_dir: None,
        }
    }

    #[test]
    fn round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("admin-server.json");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        let cfg = sample();
        save(&lock, &path, &cfg).unwrap();
        assert_eq!(load_for_recovery(&path).unwrap(), cfg);
    }

    #[test]
    fn non_ca_requires_ca_addr() {
        let mut cfg = sample();
        cfg.roles.ca = None;
        assert!(cfg.validate_structure().unwrap_err().to_string().contains("ca_addr"));
    }
}
