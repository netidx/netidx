//! Atomic file writes: write to a sibling temp file, fsync, rename
//! over the destination, then fsync the directory so the rename
//! itself is durable. The temp file is created in the same directory
//! as the destination so the rename is atomic on the same filesystem.
//!
//! Unix file modes are applied at creation, before any payload is
//! written, and reasserted before the rename — `open(2)` narrows the
//! creation mode by the umask, it can never widen it. So the temp file
//! is never observable with a wider mode than requested, and secrets
//! (vault, keytabs, keys) are never briefly world-readable.
//!
//! On Windows the mode argument is ignored, and the directory fsync
//! step is a no-op (NTFS rename atomicity does not require it; there
//! is no portable way to fsync a directory handle on Windows).
//!
//! [`write_atomic`] and [`write_atomic_async`] settle for [`SETTLE`]
//! before returning, so two writes to the same path can never land in
//! one filesystem timestamp. [`write_staged`] is for exclusive staging
//! directories that no watcher follows: write, fsync, no settle.

use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use serde::Serialize;
use std::{io::Write, path::Path};

/// How long an atomic write waits before returning: one filesystem timestamp
/// tick, so two writes to the same path can never share a modification time.
///
/// The constant itself lives in `netidx-core` because the readers that depend
/// on this — the resolver, the netidx client, the id-map daemon — are in other
/// crates, and there is only one fact.
pub use netidx_core::utils::FS_TIMESTAMP_SETTLE as SETTLE;

fn parent_dir(path: &Path) -> Result<&Path> {
    let raw =
        path.parent().ok_or_else(|| anyhow!("path {:?} has no parent dir", path))?;
    Ok(if raw.as_os_str().is_empty() { Path::new(".") } else { raw })
}

fn create_with_mode(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode);
    }
    let mut file = options.open(path).with_context(|| format!("creating {path:?}"))?;
    file.write_all(bytes).with_context(|| format!("writing {path:?}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("setting mode {:o} on {path:?}", mode))?;
    }
    #[cfg(not(unix))]
    let _ = mode;
    file.sync_all().context("fsync file")?;
    Ok(())
}

async fn run_blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f).await.context("blocking filesystem task panicked")?
}

/// Write `bytes` to a new `path` inside an exclusive staging directory.
///
/// Mode is applied at creation and reasserted after the umask, then the
/// file is fsynced. There is no sibling rename and no [`SETTLE`]: the
/// caller publishes the whole tree with [`publish_dir`], and no follower
/// compares mtimes on the staging path.
pub fn write_staged(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let dir = parent_dir(path)?;
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating parent dir {dir:?}"))?;
    create_with_mode(path, bytes, mode)?;
    #[cfg(unix)]
    fsync_dir(dir).with_context(|| format!("fsync parent dir {dir:?}"))?;
    Ok(())
}

/// Write `bytes` to `path` atomically (temp file + rename), with the
/// given unix `mode`. On Windows the mode is ignored.
pub fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let dir = parent_dir(path)?;
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating parent dir {dir:?}"))?;
    let tmp = dir.join(format_compact!(".tmp-netidx-{}", uuid::Uuid::new_v4()).as_str());
    let result = (|| {
        create_with_mode(&tmp, bytes, mode)?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("atomic rename to {path:?}"))?;
        #[cfg(unix)]
        fsync_dir(dir).with_context(|| format!("fsync parent dir {dir:?}"))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    } else {
        std::thread::sleep(SETTLE);
    }
    result
}

// XCR claude for estokes: nice consolidation — 134 lines and the
// RuntimeFlavor sniffing gone, and moving the SETTLE onto the blocking
// thread is a real improvement. Two small things:
//
// - `bytes.to_vec()` copies on every call. Every caller hands us a fresh
//   `serde_json::to_vec_pretty` / `pem.as_bytes()`, so taking `Vec<u8>` (or
//   `impl Into<Vec<u8>>`) by value would make the copy disappear.
// - dropping `NamedTempFile` means a panic between create and rename now
//   leaks `.tmp-netidx-*` where Drop used to clean it up. The error path
//   handles the ordinary case and readers do reject the leftovers
//   (`valid_id` in ca_store, the `.tmp` skip in backup::capture), so this is
//   a note rather than a bug.
// grok: agreed both are notes, not bugs. Taking `Vec<u8>` by value just
// moves the copy to the `&[u8]` callers. A panic leak of a skipped
// `.tmp-netidx-*` is not worth a guard. Please delete this XCR — praise
// and nits should not stay in the source.
pub async fn write_atomic_async(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let path = path.to_path_buf();
    let bytes = bytes.to_vec();
    run_blocking(move || write_atomic(&path, &bytes, mode)).await
}

/// Atomically publish a newly-created directory by renaming it to a sibling
/// destination that must not already exist. The caller builds all sensitive
/// state under `src`; until this succeeds, dropping its temporary-directory
/// owner rolls the state back.
pub fn publish_dir(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        bail!("refusing to replace existing directory {dst:?}");
    }
    let parent = parent_dir(dst)?;
    #[cfg(not(unix))]
    let _ = parent;
    std::fs::rename(src, dst)
        .with_context(|| format!("publishing staged directory {src:?} as {dst:?}"))?;
    #[cfg(unix)]
    fsync_dir(parent).with_context(|| format!("fsync parent dir {parent:?}"))?;
    Ok(())
}

pub async fn publish_dir_async(src: &Path, dst: &Path) -> Result<()> {
    let src = src.to_path_buf();
    let dst = dst.to_path_buf();
    run_blocking(move || publish_dir(&src, &dst)).await
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

    /// The reason [`SETTLE`] exists: two writes to the same path must not land
    /// on one filesystem timestamp, or whatever is following the file compares
    /// two different configs and finds them equal.
    #[test]
    fn consecutive_writes_land_on_different_timestamps() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("resolver.json");
        let mtime = || std::fs::metadata(&p).unwrap().modified().unwrap();
        write_atomic(&p, b"{\"read_gated\":\"No\"}", 0o644).unwrap();
        let first = mtime();
        write_atomic(&p, b"{\"read_gated\":\"Yes\"}", 0o644).unwrap();
        let second = mtime();
        assert_ne!(first, second, "a follower comparing times would miss this write");
    }

    #[test]
    fn staged_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("nested/secret");
        write_staged(&p, b"hello", 0o600).unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"hello");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&p).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        assert!(write_staged(&p, b"again", 0o600).is_err());
    }

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

    /// The temp file must never be observable with a wider mode than the
    /// caller asked for — not even for the duration of the write. A secret
    /// (the CA vault, an autorenew keytab) written at the umask default and
    /// chmodded afterwards is readable by every local user while it lands.
    #[cfg(unix)]
    #[tokio::test]
    async fn async_temp_file_is_never_wider_than_requested() {
        use std::{
            os::unix::fs::PermissionsExt,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        let done = Arc::new(AtomicBool::new(false));
        let watcher = std::thread::spawn({
            let dir = dir.path().to_path_buf();
            let done = Arc::clone(&done);
            move || {
                let mut seen: Vec<u32> = Vec::new();
                while !done.load(Ordering::Acquire) {
                    let Ok(entries) = std::fs::read_dir(&dir) else { continue };
                    for entry in entries.flatten() {
                        let name = entry.file_name();
                        if !name.to_string_lossy().starts_with(".tmp-netidx-") {
                            continue;
                        }
                        if let Ok(md) = entry.metadata() {
                            seen.push(md.permissions().mode() & 0o777);
                        }
                    }
                }
                seen
            }
        });
        // Big enough that the write itself spans many watcher passes.
        let payload = vec![b'x'; 16 * 1024 * 1024];
        write_atomic_async(&path, &payload, 0o600).await.unwrap();
        done.store(true, Ordering::Release);
        let seen = watcher.join().unwrap();
        assert!(
            !seen.is_empty(),
            "watcher never caught the temp file — the test proves nothing"
        );
        let wide: Vec<String> =
            seen.iter().filter(|m| **m & !0o600 != 0).map(|m| format!("{m:o}")).collect();
        assert!(wide.is_empty(), "temp file was observable at modes {wide:?}");
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    }
}
