//! Client config: load, edit, save, validate.
//!
//! Wraps [`netidx::config::file::Config`] in a newtype that adds
//! atomic save and a `validate` shortcut that round-trips through
//! the existing `Config::from_file` validator without consuming
//! the file value.

use crate::{atomic, paths};
use anyhow::{Context, Result};
use netidx::config::{Config, file};
use std::path::{Path, PathBuf};

/// A loaded client config. Wraps `netidx::config::file::Config`
/// so we can attach load/save/validate without polluting the
/// upstream type.
#[derive(Debug, Clone)]
pub struct ClientConfig(pub file::Config);

impl ClientConfig {
    /// Read a client config from JSON at `path`.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading client config {path:?}"))?;
        let cfg: file::Config = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing client config {path:?}"))?;
        Ok(Self(cfg))
    }

    /// Read the client config from the first existing standard location
    /// (see [`paths::discover_client_config`]).
    pub fn load_default() -> Result<Self> {
        Self::load(paths::discover_client_config()?)
    }

    /// Validate, then atomically save to `path` at mode 0o644.
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        self.validate().context("client config failed validation")?;
        atomic::write_atomic_pretty_json(path.as_ref(), &self.0)
    }

    /// Save to the user platform default path
    /// (`${dirs::config_dir}/netidx/client.json`).
    pub fn save_default(&self) -> Result<()> {
        self.save(paths::user_client_config()?)
    }

    /// Run the existing `Config::from_file` validator on a clone, so
    /// the same checks the runtime applies at startup also gate every
    /// save. Does not modify `self`.
    pub fn validate(&self) -> Result<()> {
        Config::from_file(self.0.clone()).map(|_| ())
    }

    /// Re-export the upstream builder so callers can construct configs
    /// programmatically without going through this crate.
    pub fn builder() -> file::ConfigBuilder {
        file::ConfigBuilder::default()
    }

    /// Borrow the inner file representation.
    pub fn as_file(&self) -> &file::Config {
        &self.0
    }

    /// Mutably borrow the inner file representation.
    pub fn as_file_mut(&mut self) -> &mut file::Config {
        &mut self.0
    }

    /// Consume self and return the inner file representation.
    pub fn into_file(self) -> file::Config {
        self.0
    }
}

impl From<file::Config> for ClientConfig {
    fn from(c: file::Config) -> Self {
        Self(c)
    }
}

/// Where this config would be saved by `save_default`.
pub fn default_save_path() -> Result<PathBuf> {
    paths::user_client_config()
}

#[cfg(test)]
mod tests {
    use super::*;
    use netidx::config::file::Auth;

    fn minimal() -> ClientConfig {
        ClientConfig(
            file::ConfigBuilder::default()
                .addrs(vec![("127.0.0.1:4564".parse().unwrap(), Auth::Anonymous)])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("client.json");
        let cfg = minimal();
        cfg.save(&p).unwrap();
        let back = ClientConfig::load(&p).unwrap();
        assert_eq!(back.0.addrs.len(), 1);
        // Re-save and assert byte-for-byte stability.
        let first = std::fs::read(&p).unwrap();
        back.save(&p).unwrap();
        let second = std::fs::read(&p).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn validate_rejects_bad_config() {
        // An empty addrs list is not valid (Config::from_file enforces
        // at least one address).
        let cfg =
            ClientConfig(file::ConfigBuilder::default().addrs(vec![]).build().unwrap());
        assert!(cfg.validate().is_err());
    }
}
