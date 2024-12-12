pub mod index;
pub mod reader;
pub mod writer;

#[cfg(test)]
mod test;

pub use reader::ArchiveCollectionReader;
pub use writer::ArchiveCollectionWriter;
pub use index::{ArchiveIndex, File};
use anyhow::Result;
use chrono::prelude::*;

#[cfg(unix)]
fn parse_name(s: &str) -> Result<DateTime<Utc>> {
    Ok(s.parse()?)
}

#[cfg(windows)]
fn parse_name(s: &str) -> Result<DateTime<Utc>> {
    let s = s.replace("I", ":");
    Ok(s.parse()?)
}

#[cfg(unix)]
fn to_name(ts: &DateTime<Utc>) -> String {
    ts.to_rfc3339()
}

#[cfg(windows)]
fn to_name(ts: &DateTime<Utc>) -> String {
    ts.to_rfc3339().replace(":", "I")
}
