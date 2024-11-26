use super::{ArchiveCollectionReader, ArchiveCollectionWriter};
use crate::logfile::BATCH_POOL;
use netidx::{
    path::Path,
    subscriber::{Event, Value},
};
use std::path::Path as FilePath;

#[test]
fn basic_test() {
    let dir = FilePath::new("test-collection");
    let paths = [Path::from("/foo/bar"), Path::from("/foo/baz")];
    
}
