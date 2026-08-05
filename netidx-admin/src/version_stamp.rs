//! A model version recorded beside the file it describes.
//!
//! The CA holds the authoritative id-map shape and the authoritative perms
//! document, each versioned. A host records which version its own copy
//! reflects so the CA can tell a host that missed a change from one that is
//! current, from an integer it already receives on the register poll rather
//! than by fetching every host's state.
//!
//! Beside the file rather than inside it: both files have schemas read by
//! other daemons — the id-mapper and the resolver — which have no business
//! knowing about admin-domain bookkeeping, and the id-map's schema is
//! `deny_unknown_fields` besides.

use crate::atomic;
use anyhow::Result;
use std::path::{Path, PathBuf};

/// Where the stamp for `subject` lives: `<subject>.version`.
pub fn path<P: AsRef<Path>>(subject: P) -> PathBuf {
    let p = subject.as_ref();
    let mut name = p.file_name().unwrap_or_default().to_os_string();
    name.push(".version");
    p.with_file_name(name)
}

/// The version `subject` reflects, or `None` if it has never been stamped.
///
/// `None` reads as "behind everything", which is the safe direction: an
/// unknown host is reconciled rather than assumed current.
pub async fn read<P: AsRef<Path>>(subject: P) -> Option<u64> {
    let bytes = tokio::fs::read(path(subject)).await.ok()?;
    std::str::from_utf8(&bytes).ok()?.trim().parse().ok()
}

/// Record that `subject` now reflects `version`.
///
/// Never moves backwards. A late-arriving retry of an older change must not
/// make a current host look stale, which would cost a pointless reconcile on
/// every poll from then on.
pub async fn record<P: AsRef<Path>>(subject: P, version: u64) -> Result<()> {
    let subject = subject.as_ref();
    if read(subject).await.is_some_and(|have| have >= version) {
        return Ok(());
    }
    atomic::write_atomic_async(&path(subject), version.to_string().as_bytes(), 0o644)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stamp_sits_beside_its_subject() {
        assert_eq!(
            path("/etc/netidx/perms.json"),
            PathBuf::from("/etc/netidx/perms.json.version")
        );
    }

    #[tokio::test]
    async fn an_unstamped_subject_is_behind_everything() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read(dir.path().join("perms.json")).await, None);
    }

    #[tokio::test]
    async fn a_stamp_never_moves_backwards() {
        let dir = tempfile::tempdir().unwrap();
        let subject = dir.path().join("perms.json");
        record(&subject, 5).await.unwrap();
        assert_eq!(read(&subject).await, Some(5));
        // A retry of an older change must not make a current host look stale.
        record(&subject, 3).await.unwrap();
        assert_eq!(read(&subject).await, Some(5));
        record(&subject, 6).await.unwrap();
        assert_eq!(read(&subject).await, Some(6));
    }
}
