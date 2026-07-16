use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use std::{
    fmt,
    fs::{File, OpenOptions, TryLockError},
    path::{Component, Path, PathBuf},
};
use triomphe::Arc;

struct Inner {
    root: PathBuf,
    lock_path: PathBuf,
    _file: File,
}

#[derive(Clone)]
pub struct ConfigDirLock(Arc<Inner>);

impl fmt::Debug for ConfigDirLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigDirLock")
            .field("root", &self.0.root)
            .field("lock_path", &self.0.lock_path)
            .finish_non_exhaustive()
    }
}

impl ConfigDirLock {
    pub fn acquire(root: impl AsRef<Path>) -> Result<Self> {
        let root = normalize(root.as_ref())?;
        let name = root
            .file_name()
            .and_then(|name| name.to_str())
            .context("the filesystem root cannot be used as a netidx config directory")?;
        let parent = root.parent().context("the config directory has no parent")?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating config parent {}", parent.display()))?;
        let lock_path = parent.join(format_compact!(".{name}.netidx.lock").as_str());
        let file = open_lock(&lock_path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600)).with_context(
                || format!("setting permissions on {}", lock_path.display()),
            )?;
        }
        match file.try_lock() {
            Ok(()) => Ok(Self(Arc::new(Inner { root, lock_path, _file: file }))),
            Err(TryLockError::WouldBlock) => {
                bail!("another netidx administrative process owns {}", root.display())
            }
            Err(TryLockError::Error(e)) => {
                Err(e).with_context(|| format!("locking {}", lock_path.display()))
            }
        }
    }

    pub async fn acquire_async(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || Self::acquire(root))
            .await
            .context("config-directory lock task panicked")?
    }

    pub async fn acquire_for_ca_dir(ca_dir: impl AsRef<Path>) -> Result<Self> {
        let ca_dir = ca_dir.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || Self::acquire_for_ca_dir_sync(ca_dir))
            .await
            .context("config-directory lock task panicked")?
    }

    fn acquire_for_ca_dir_sync(ca_dir: impl AsRef<Path>) -> Result<Self> {
        let ca_dir = normalize(ca_dir.as_ref())?;
        let user =
            crate::paths::user_config_root().ok().and_then(|root| normalize(&root).ok());
        let system = normalize(&crate::paths::system_config_root()).ok();
        let root = user
            .into_iter()
            .chain(system)
            .find(|root| ca_dir.starts_with(root))
            .unwrap_or_else(|| ca_dir.clone());
        let lock = Self::acquire(root)?;
        lock.require_contained(ca_dir)?;
        Ok(lock)
    }

    pub fn root(&self) -> &Path {
        &self.0.root
    }

    pub fn contains(&self, path: impl AsRef<Path>) -> Result<bool> {
        Ok(normalize(path.as_ref())?.starts_with(&self.0.root))
    }

    pub fn require_descendant(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        let path = normalize(path.as_ref())?;
        if path == self.0.root || !path.starts_with(&self.0.root) {
            bail!(
                "CA directory {} must be below config directory {}",
                path.display(),
                self.0.root.display()
            );
        }
        Ok(path)
    }

    pub fn require_contained(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        let path = normalize(path.as_ref())?;
        if !path.starts_with(&self.0.root) {
            bail!(
                "path {} is outside config directory {}",
                path.display(),
                self.0.root.display()
            );
        }
        Ok(path)
    }

    #[cfg(test)]
    fn lock_path(&self) -> &Path {
        &self.0.lock_path
    }
}

fn open_lock(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).with_context(|| format!("opening {}", path.display()))
}

fn normalize(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().context("reading the current directory")?.join(path)
    };
    let mut existing = path.as_path();
    let mut suffix = Vec::new();
    while !existing.exists() {
        let name =
            existing.file_name().context("path has no existing ancestor")?.to_os_string();
        suffix.push(name);
        existing = existing.parent().context("path has no existing ancestor")?;
    }
    let mut normalized = existing
        .canonicalize()
        .with_context(|| format!("canonicalizing {}", existing.display()))?;
    for component in suffix.into_iter().rev() {
        normalized.push(component);
    }
    if normalized.components().any(|component| matches!(component, Component::ParentDir))
    {
        bail!("path {} contains an unresolved parent component", path.display());
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_owner_per_config_directory() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("netidx");
        let first = ConfigDirLock::acquire(&root).unwrap();
        assert_eq!(first.root(), root);
        assert!(ConfigDirLock::acquire(&root).is_err());
        drop(first);
        ConfigDirLock::acquire(&root).unwrap();
    }

    #[test]
    fn clones_retain_the_lock() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("netidx");
        let first = ConfigDirLock::acquire(&root).unwrap();
        let second = first.clone();
        drop(first);
        assert!(ConfigDirLock::acquire(&root).is_err());
        drop(second);
        ConfigDirLock::acquire(&root).unwrap();
    }

    #[test]
    fn lock_file_is_outside_replaceable_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("netidx");
        let lock = ConfigDirLock::acquire(&root).unwrap();
        assert_eq!(lock.lock_path(), parent.path().join(".netidx.netidx.lock"));
        std::fs::create_dir(&root).unwrap();
        std::fs::remove_dir(&root).unwrap();
        std::fs::create_dir(&root).unwrap();
        assert!(ConfigDirLock::acquire(&root).is_err());
    }

    #[test]
    fn ca_must_be_below_the_config_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("netidx");
        let lock = ConfigDirLock::acquire(&root).unwrap();
        assert_eq!(lock.require_descendant(root.join("ca")).unwrap(), root.join("ca"));
        assert!(lock.require_descendant(&root).is_err());
        assert!(lock.require_descendant(parent.path().join("ca")).is_err());
    }

    #[tokio::test]
    async fn standalone_ca_uses_its_directory_as_the_lock_root() {
        let parent = tempfile::tempdir().unwrap();
        let ca = parent.path().join("standalone-ca");
        let first = ConfigDirLock::acquire_for_ca_dir(&ca).await.unwrap();
        assert_eq!(first.root(), ca);
        assert_eq!(first.require_contained(&ca).unwrap(), ca);
        assert!(ConfigDirLock::acquire_for_ca_dir(&ca).await.is_err());
    }
}
