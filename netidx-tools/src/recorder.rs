use futures::prelude::*;
use netidx::{
    config::Config,
    publisher::{BindCfg, Publisher, Val, Value},
    subscriber::{SubId, Dval},
    resolver::{Auth, ResolverRead, Glob},
    path::Path,
    pool::Pooled,
};
use netidx_protocols::archive::{Archive, ReadOnly, ReadWrite, BatchItem};
use tokio::runtime::Runtime;
use std::ops::Bound;

enum ToArchive {
    AddPaths(Vec<(Path, SubId)>),
    AddBatch(Pooled<Vec<(SubId, Value)>>),
    OpenCursor(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
    ReadCursor(u64, usize),
}

pub(crate) fn run(
    config: Config,
    foreground: bool,
    bind: Option<BindCfg>,
    publish_base: Option<Path>,
    auth: Auth,
    archive: String,
    spec: Vec<String>,
) {
    let publish_args = match (bind, publish_base) {
        (None, None) => None,
        (Some(bind), Some(publish_base)) => Some((bind, publish_base)),
        (Some(_), None) | (None, Some(_)) => {
            panic!("you must specify bind and publish_base to publish an archive")
        }
    };
    
    let rt = Runtime::new().expect("failed to init tokio runtime");
}
