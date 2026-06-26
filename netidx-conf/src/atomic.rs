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

use anyhow::{Context, Result};
use serde::Serialize;
use std::{io::Write, path::Path};

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
}
