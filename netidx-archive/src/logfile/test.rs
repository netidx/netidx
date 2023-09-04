use super::*;
use chrono::prelude::*;
use netidx::{
    path::Path,
    subscriber::{Event, Value},
};
use std::fs;
use std::path::Path as FilePath;

fn check_contents(t: &ArchiveReader, paths: &[Path], batches: usize) {
    t.check_remap_rescan(false).unwrap();
    assert_eq!(t.delta_batches(), batches);
    let mut cursor = Cursor::new();
    let (_, mut batch) = t.read_deltas(None, &mut cursor, batches).unwrap();
    let now = Utc::now();
    for (ts, b) in batch.drain(..) {
        let elapsed = (now - ts).num_seconds();
        assert!(elapsed <= 10 && elapsed >= -10);
        assert_eq!(Vec::len(&b), paths.len());
        for (BatchItem(id, v), p) in b.iter().zip(paths.iter()) {
            assert_eq!(Some(p), t.index().path_for_id(id));
            assert_eq!(v, &Event::Update(Value::U64(42)))
        }
    }
}

#[test]
fn basic_test() {
    let file = FilePath::new("test-data");
    let paths = [Path::from("/foo/bar"), Path::from("/foo/baz")];
    if FilePath::is_file(&file) {
        fs::remove_file(file).unwrap();
    }
    let mut batch = BATCH_POOL.take();
    let initial_size = {
        // check that we can open, and write an archive
        let mut t = ArchiveWriter::open(&file).unwrap();
        t.add_paths(&paths).unwrap();
        batch.extend(paths.iter().map(|p| {
            BatchItem(t.id_for_path(p).unwrap(), Event::Update(Value::U64(42)))
        }));
        t.add_batch(false, Utc::now(), &batch).unwrap();
        t.flush().unwrap();
        check_contents(&t.reader().unwrap(), &paths, 1);
        t.capacity()
    };
    {
        // check that we can close, reopen, and read the archive back
        let t = ArchiveReader::open(file).unwrap();
        check_contents(&t, &paths, 1);
    }
    {
        // check that we can reopen, and add to an archive
        let mut t = ArchiveWriter::open(&file).unwrap();
        t.add_batch(false, Utc::now(), &batch).unwrap();
        t.flush().unwrap();
        check_contents(&t.reader().unwrap(), &paths, 2);
    }
    {
        // check that we can reopen, and read the archive we added to
        let t = ArchiveReader::open(&file).unwrap();
        check_contents(&t, &paths, 2);
    }
    let n = {
        // check that we can grow the archive by remapping it on the fly
        let mut t = ArchiveWriter::open(&file).unwrap();
        let r = t.reader().unwrap();
        let mut n = 2;
        while t.capacity() == initial_size {
            t.add_batch(false, Utc::now(), &batch).unwrap();
            n += 1;
            check_contents(&r, &paths, n);
        }
        t.flush().unwrap();
        check_contents(&r, &paths, n);
        n
    };
    {
        // check that we can reopen, and read the archive we grew
        let t = ArchiveReader::open(&file).unwrap();
        check_contents(&t, &paths, n);
    }
    {
        // check that we can't open the archive twice for write
        let t = ArchiveWriter::open(&file).unwrap();
        let reader = t.reader().unwrap();
        check_contents(&reader, &paths, n);
        assert!(ArchiveWriter::open(&file).is_err());
    }
    if FilePath::is_file(&file) {
        fs::remove_file(file).unwrap();
    }
}
