use futures::prelude::*;
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, Publisher, Val, Value},
    resolver::{Auth, Glob, ResolverRead},
    subscriber::{Dval, SubId},
};
use netidx_protocols::archive::{
    Archive as ArchiveInner, BatchItem, Cursor, ReadOnly, ReadWrite,
};
use parking_lot::RwLock;
use std::{collections::HashMap, ops::Bound, sync::Arc};
use tokio::runtime::Runtime;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct Archive(Arc<RwLock<ArchiveInner>>);

async fn run_publisher(
    archive: Archive,
    publisher: Publisher,
    publish_base: Path,
) -> Result<()> {
    struct Session {
        published: HashMap<u64, Val>,
        cursor: Option<Cursor>,
        start: Val,
        end: Val,
        pos: Val,
        speed: Val,
        control: Val,
        idle: bool
    }
    let mut sessions: HashMap<Uuid, Session> = HashMap::new();
    let session = publisher.publish(publish_base.append("session"))?;
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
