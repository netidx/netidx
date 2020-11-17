use mapr::{MmapMut, MmapOptions};
use fs3::FileExt;
use std::{
    fs::File,
    collections::BTreeMap,
};
use netidx::{
    pack::Pack,
    publisher::Value,
    path::Path,
};

#[derive(Debug, Clone)]
pub(crate) struct PathMap(Vec<(Path, u64)>);

#[derive(Debug, Clone)]
pub(crate) struct Batch(Vec<(u64, Value)>);

#[derive(Debug, Clone)]
pub(crate) enum RecordData {
    UncommittedPathMap(PathMap),
    PathMap(PathMap),
    UncommittedBatch(Batch),
    Batch(Batch),
}

#[derive(Debug, Clone)]
pub(crate) struct Record {
    len: u32,
    data: RecordData,
}

struct Archive {
    
}
