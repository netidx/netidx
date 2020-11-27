use futures::{prelude::*, channel::mpsc};
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
use uuid::{adaptor::SimpleRef, Uuid};

#[derive(Debug, Clone)]
struct Archive(Arc<RwLock<ArchiveInner>>);

fn session_base(publish_base: &Path, id: Uuid) -> Path {
    let mut buf = [u8; SimpleRef::LENGTH];
    publish_base.append(id.to_simple_ref().encode_lower(&mut buf));
}

static START_DOC: &'static str = "The timestamp you want to replay to start at, or String(Unbounded) for the beginning of the archive. Default Unbounded.";
static END_DOC: &'static str = "Time timestamp you want to replay end at, or String(Unbounded) for the end of the archive. default Unbounded";
static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, String(Unlimited) = as fast as data can be read and sent. Default is 1";
static CONTROL_DOC: &'static str = "The current state of playback, {Stop, Pause, Play}. Default Stop. Pause, pause at the current position. Stop, stop and reset position to the start.";
static POS_DOC: &'static str = "The current playback position. Null if playback is stopped, otherwise the timestamp of the current record. Set to any timestamp where start <= t <= end to seek";

async fn run_session(
    archive: Archive,
    publisher: Publisher,
    publish_base: Path,
    session_id: Uuid,
) -> Result<()> {
    let (control_tx, control_rx) = mpsc::channel(3);
    let session_base = session_base(&publish_base, session_id);
    let mut published: HashMap<u64, Val> = HashMap::new();
    let mut cursor = Cursor::new();
    let start_doc = publisher.publish(
        session_base.append("start/doc"),
        Value::String(Chars::from(START_DOC)),
    )?;
    let start = publisher.publish(
        session_base.append("start/current"),
        Value::String(Chars::from("Unbounded")),
    )?;
    let end_doc = publisher
        .publish(session_base.append("end/doc"), Value::String(Chars::from(END_DOC)))?;
    let end = publisher.publish(
        session_base.append("end/current"),
        Value::String(Chars::from("Unbounded")),
    )?;
    let speed_doc = publisher.publish(
        session_base.append("speed/doc"),
        Value::String(Chars::from(SPEED_DOC)),
    )?;
    let speed = publisher.publish(session_base.append("speed/current"), Value::V32(1))?;
    let control_doc = publisher.publish(
        session_base.append("control/doc"),
        Value::String(Chars::from(CONTROL_DOC)),
    )?;
    let control = publisher.publish(
        session_base.append("control/current"),
        Value::String(Chars::from("Stop")),
    )?;
    let pos_doc = publisher
        .publish(session_base.append("pos/doc"), Value::String(Chars::from(POS_DOC)))?;
    let pos = publisher.publish(session_base.append("pos/current"), Value::Null)?;
}

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
        idle: bool,
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
