//! Atomic file writes: write to a sibling temp file, fsync, rename
//! over the destination, then fsync the directory so the rename
//! itself is durable. The temp file is created in the same directory
//! as the destination so the rename is atomic on the same filesystem.
//!
//! Unix file modes are applied via `std::os::unix::fs::PermissionsExt`
//! before the rename, so the file is never observable with the wrong
//! mode by another process.
//!
//! On Windows the mode argument is ignored, and the directory fsync
//! step is a no-op (NTFS rename atomicity does not require it; there
//! is no portable way to fsync a directory handle on Windows).

use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use serde::Serialize;
use std::{io::Write, path::Path};
use tokio::io::AsyncWriteExt;

/// Write `bytes` to `path` atomically (temp file + rename), with the
/// given unix `mode`. On Windows the mode is ignored.
pub fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let raw_dir = path
        .parent()
        .ok_or_else(|| anyhow!("atomic write target {:?} has no parent dir", path))?;
    // Normalize an empty parent (`path` is a bare filename) to "." so
    // tempfile creation and the directory fsync below don't trip.
    let dir: &Path =
        if raw_dir.as_os_str().is_empty() { Path::new(".") } else { raw_dir };
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating parent dir {dir:?}"))?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("creating temp file in {dir:?}"))?;
    tmp.as_file_mut()
        .write_all(bytes)
        .with_context(|| format!("writing temp file for {path:?}"))?;
    tmp.as_file_mut().sync_all().with_context(|| "fsync temp file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(mode))
            .with_context(|| {
                format!("setting mode {:o} on temp file for {path:?}", mode)
            })?;
    }
    #[cfg(not(unix))]
    let _ = mode;
    tmp.persist(path)
        .map_err(|e| anyhow!("atomic rename to {:?} failed: {}", path, e.error))?;
    // Without this, the rename can be lost on crash even though the
    // file's data was fsynced. Linux ext4/xfs/btrfs all need a
    // directory fsync to make the dirent change durable.
    #[cfg(unix)]
    fsync_dir(dir).with_context(|| format!("fsync parent dir {dir:?}"))?;
    Ok(())
}

pub async fn write_atomic_async(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let raw_dir = path
        .parent()
        .ok_or_else(|| anyhow!("atomic write target {:?} has no parent dir", path))?;
    let dir: &Path =
        if raw_dir.as_os_str().is_empty() { Path::new(".") } else { raw_dir };
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("creating parent dir {dir:?}"))?;
    let tmp = dir.join(format_compact!(".tmp-netidx-{}", uuid::Uuid::new_v4()).as_str());
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)
            .await
            .with_context(|| format!("creating temp file in {dir:?}"))?;
        file.write_all(bytes)
            .await
            .with_context(|| format!("writing temp file for {path:?}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))
                .await
                .with_context(|| {
                    format!("setting mode {:o} on temp file for {path:?}", mode)
                })?;
        }
        #[cfg(not(unix))]
        let _ = mode;
        file.sync_all().await.context("fsync temp file")?;
        drop(file);
        final_commit(|| {
            std::fs::rename(&tmp, path)
                .with_context(|| format!("atomic rename to {path:?}"))?;
            #[cfg(unix)]
            fsync_dir(dir).with_context(|| format!("fsync parent dir {dir:?}"))?;
            Ok(())
        })
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

/// Atomically publish a newly-created directory by renaming it to a sibling
/// destination that must not already exist. The caller builds all sensitive
/// state under `src`; until this succeeds, dropping its temporary-directory
/// owner rolls the state back.
pub fn publish_dir(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        bail!("refusing to replace existing directory {dst:?}");
    }
    let raw_parent = dst
        .parent()
        .ok_or_else(|| anyhow!("directory publish target {dst:?} has no parent"))?;
    let parent =
        if raw_parent.as_os_str().is_empty() { Path::new(".") } else { raw_parent };
    #[cfg(not(unix))]
    let _ = parent;
    std::fs::rename(src, dst)
        .with_context(|| format!("publishing staged directory {src:?} as {dst:?}"))?;
    #[cfg(unix)]
    fsync_dir(parent).with_context(|| format!("fsync parent dir {parent:?}"))?;
    Ok(())
}

pub async fn publish_dir_async(src: &Path, dst: &Path) -> Result<()> {
    if tokio::fs::try_exists(dst).await? {
        bail!("refusing to replace existing directory {dst:?}");
    }
    let raw_parent = dst
        .parent()
        .ok_or_else(|| anyhow!("directory publish target {dst:?} has no parent"))?;
    let parent =
        if raw_parent.as_os_str().is_empty() { Path::new(".") } else { raw_parent };
    #[cfg(not(unix))]
    let _ = parent;
    final_commit(|| {
        std::fs::rename(src, dst)
            .with_context(|| format!("publishing staged directory {src:?} as {dst:?}"))?;
        #[cfg(unix)]
        fsync_dir(parent).with_context(|| format!("fsync parent dir {parent:?}"))?;
        Ok(())
    })
}

fn final_commit(f: impl FnOnce() -> Result<()>) -> Result<()> {
    match tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

#[cfg(unix)]
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Convenience wrapper: pretty-print `val` as JSON, then [`write_atomic`]
/// at mode `0o644`.
pub fn write_atomic_pretty_json<T: Serialize>(path: &Path, val: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(val).context("serialize JSON")?;
    write_atomic(path, &bytes, 0o644)
}

pub async fn write_atomic_pretty_json_async<T: Serialize>(
    path: &Path,
    val: &T,
) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(val).context("serialize JSON")?;
    write_atomic_async(path, &bytes, 0o644).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_text() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("hello.txt");
        write_atomic(&p, b"hello", 0o600).unwrap();
        let read = std::fs::read(&p).unwrap();
        assert_eq!(read, b"hello");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&p).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn round_trip_json() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("data.json");
        let val = serde_json::json!({"x": 1, "y": [2, 3]});
        write_atomic_pretty_json(&p, &val).unwrap();
        let read: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        assert_eq!(read, val);
    }

    #[test]
    fn overwrite_existing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a");
        write_atomic(&p, b"first", 0o644).unwrap();
        write_atomic(&p, b"second", 0o644).unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"second");
    }

    #[test]
    fn creates_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("nested/sub/dir/data");
        write_atomic(&p, b"x", 0o644).unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"x");
    }

    #[test]
    fn publishes_new_directory_without_replacing_existing_state() {
        let parent = tempfile::tempdir().unwrap();
        let src = parent.path().join("staged");
        let dst = parent.path().join("live");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("value"), b"ready").unwrap();
        publish_dir(&src, &dst).unwrap();
        assert!(!src.exists());
        assert_eq!(std::fs::read(dst.join("value")).unwrap(), b"ready");

        let other = parent.path().join("other");
        std::fs::create_dir(&other).unwrap();
        assert!(publish_dir(&other, &dst).is_err());
        assert!(other.exists());
        assert_eq!(std::fs::read(dst.join("value")).unwrap(), b"ready");
    }

    #[tokio::test]
    async fn async_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("value");
        write_atomic_async(&path, b"one", 0o600).await.unwrap();
        write_atomic_async(&path, b"two", 0o600).await.unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), b"two");
    }
}
