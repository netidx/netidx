//! Reading a config file together with the modification time of the very
//! descriptor it was read from. Shared by the client config and the
//! resolver-server config, which are both followed the same way.
//!
//! Every config netidx follows is reloaded by comparing the file's
//! modification time against the time recorded for what is running. The two
//! have to describe the same bytes, and asking the *path* for its time after
//! reading it does not: a write landing in between gets you the new time
//! attached to the old contents, and a poll comparing against that concludes
//! nothing has changed. Nothing then reloads again, ever.
//!
//! `fstat` on the descriptor the contents came from cannot do that. Every tool
//! that writes these files replaces them by renaming a new file over the top,
//! so the inode behind an open descriptor is never modified underneath us —
//! the time it reports belongs to the bytes in hand.
//!
//! The other half of this is that consecutive writes must not share a
//! modification time. Linux stamps inodes from a clock that only advances once
//! per timer tick, so `netidx-admin`'s atomic write settles for longer than a
//! tick before returning; see [`netidx_core::utils::FS_TIMESTAMP_SETTLE`],
//! which is where that duration is defined for everyone who depends on it.

use anyhow::{Context, Result};
use poolshark::local::LPooled;
use std::{fs::File, io::Read, path::Path, time::SystemTime};

/// Read a config file, returning its contents and the modification time of
/// the descriptor they came from. The caller parses the string it is handed,
/// so the time it records always belongs to the configuration it applied.
pub(crate) fn read(path: &Path) -> Result<(LPooled<String>, Option<SystemTime>)> {
    let mut file =
        File::open(path).with_context(|| format!("opening config {}", path.display()))?;
    // fstat, not stat: this is the file we are about to read, whatever is at
    // the path by the time we finish.
    let mtime = file.metadata().ok().and_then(|md| md.modified().ok());
    let mut s: LPooled<String> = LPooled::take();
    file.read_to_string(&mut s)
        .with_context(|| format!("reading config {}", path.display()))?;
    Ok((s, mtime))
}
