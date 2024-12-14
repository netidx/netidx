use crate::{
    config::{Config, ConfigBuilder, RecordConfigBuilder},
    logfile::{BatchItem, Id, Seek, BATCH_POOL},
    logfile_collection::{
        ArchiveCollectionReader, ArchiveCollectionWriter, ArchiveIndex,
    },
};
use chrono::prelude::*;
use netidx::{
    path::Path,
    subscriber::{Event, Value},
};
use netidx_netproto::glob::GlobSet;
use std::{fs, ops::Bound, sync::Arc};

const PATH: &str = "test-collection";
const SHARD: &str = "0";

fn index_ts(i: i64) -> DateTime<Utc> {
    let ts = 1732726913;
    DateTime::from_timestamp(ts + i, 0).unwrap()
}

fn check(reader: &mut ArchiveCollectionReader, ids: &[Id], i: u64) {
    reader.seek(Seek::Beginning).unwrap();
    for j in 0..i {
        let (_, mut b) = reader.read_deltas(None, 1).unwrap();
        assert_eq!(b.len(), 1);
        let (ts, mut b) = b.pop_back().unwrap();
        assert_eq!(ts, index_ts(j as i64));
        assert_eq!(b.len(), 2);
        let item = b.pop().unwrap();
        assert_eq!(item, BatchItem(ids[1], Event::Update(Value::U64(j))));
        let item = b.pop().unwrap();
        assert_eq!(item, BatchItem(ids[0], Event::Update(Value::U64(j))));
    }
}

fn open_reader(
    config: &Arc<Config>,
    writer: &ArchiveCollectionWriter,
) -> ArchiveCollectionReader {
    ArchiveCollectionReader::new(
        ArchiveIndex::new(&config, SHARD).unwrap(),
        config.clone(),
        SHARD.into(),
        Some(writer.current_reader().unwrap()),
        Bound::Unbounded,
        Bound::Unbounded,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_test() {
    let _ = env_logger::try_init();
    let _ = fs::remove_dir_all(PATH);
    let paths = [Path::from("/foo/bar"), Path::from("/foo/baz")];
    let record = RecordConfigBuilder::default()
        .spec(GlobSet::new(true, []).unwrap())
        .build()
        .unwrap();
    let config = ConfigBuilder::default()
        .archive_directory(PATH)
        .record([(SHARD.into(), record)])
        .build()
        .unwrap();
    let config = Arc::new(config);
    let mut writer = ArchiveCollectionWriter::open(config.clone(), SHARD.into()).unwrap();
    let mut reader = open_reader(&config, &writer);
    writer.add_paths(&paths).unwrap();
    let ids = paths.clone().map(|p| writer.id_for_path(&p).unwrap());
    for i in 0..1000 {
        let rotate = i > 0 && i % 100 == 0;
        if rotate {
            writer.rotate_and_compress(index_ts((i - 1) as i64), None).await.unwrap();
        }
        check(&mut reader, &ids, i);
        // do this after the read through to ensure we can still read
        // the old archive after the rotate
        if rotate {
            reader = open_reader(&config, &writer);
        }
        // then check we can read the new reader
        check(&mut reader, &ids, i);
        let mut batch = BATCH_POOL.take();
        batch.push(BatchItem(ids[0], Event::Update(Value::U64(i))));
        batch.push(BatchItem(ids[1], Event::Update(Value::U64(i))));
        writer.add_batch(false, index_ts(i as i64), &batch).unwrap();
    }
    check(&mut reader, &ids, 1000);
    let _ = fs::remove_dir_all(PATH);
}
