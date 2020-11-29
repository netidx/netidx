use futures::{channel::mpsc, prelude::*, future};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, Publisher, Val, Value},
    resolver::{Auth, Glob, ResolverRead},
    subscriber::{Dval, SubId},
};
use netidx_protocols::archive::{
    Archive, BatchItem, Cursor, MonotonicTimestamper, ReadOnly, ReadWrite, Timestamp,
};
use parking_lot::RwLock;
use std::{collections::HashMap, ops::Bound, sync::Arc};
use tokio::{runtime::Runtime, sync::broadcast, time};
use uuid::{adaptor::SimpleRef, Uuid};

#[derive(Debug, Clone)]
enum BCastMsg {
    Batch(Timestamp, Arc<Pooled<Vec<BatchItem>>>),
    FileRemapped,
    ServerStop,
}

fn session_base(publish_base: &Path, id: Uuid) -> Path {
    let mut buf = [u8; SimpleRef::LENGTH];
    publish_base.append(id.to_simple_ref().encode_lower(&mut buf));
}

static START_DOC: &'static str = "The timestamp you want to replay to start at, or String(Unbounded) for the beginning of the archive. Default Unbounded.";
static END_DOC: &'static str = "Time timestamp you want to replay end at, or String(Unbounded) for the end of the archive. default Unbounded";
static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, String(Unlimited) = as fast as data can be read and sent. Default is 1";
static STATE_DOC: &'static str = "The current state of playback, {Stop, Pause, Play}. Pause, pause at the current position. Stop, reset playback to the initial state, unpublish everything, and wait. Default Stop.";
static POS_DOC: &'static str = "The current playback position. Null if playback is stopped, otherwise the timestamp of the current record. Set to any timestamp where start <= t <= end to seek";

async fn run_session(
    bcast: broadcast::Receiver<BCastMsg>,
    mut archive: Archive<ReadOnly>,
    publisher: Publisher,
    publish_base: Path,
    session_id: Uuid,
) -> Result<()> {
    enum Speed {
        Paused,
        Limited(usize, time::Sleep),
        Unlimited,
        Tail
    }
    impl Speed {
        async fn next(&mut self) {
            match self {
                Speed::Paused | Speed::Tail => future::pending().await,
                Speed::Unlimited => (),
                Speed::Limited(rate, next) => next.await
            }
        }
    }
    struct State {
        published: HashMap<u64, Val>,
        current: Pooled<Vec<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
        next: usize,
        pos: Cursor,
        speed: Speed,
    }
    let (control_tx, control_rx) = mpsc::channel(3);
    let session_base = session_base(&publish_base, session_id);
    let mut cursor = Cursor::new();
    let start_doc = publisher.publish(
        session_base.append("control/start/doc"),
        Value::String(Chars::from(START_DOC)),
    )?;
    let start = publisher.publish(
        session_base.append("control/start/current"),
        Value::String(Chars::from("Unbounded")),
    )?;
    let end_doc = publisher.publish(
        session_base.append("control/end/doc"),
        Value::String(Chars::from(END_DOC)),
    )?;
    let end = publisher.publish(
        session_base.append("control/end/current"),
        Value::String(Chars::from("Unbounded")),
    )?;
    let speed_doc = publisher.publish(
        session_base.append("control/speed/doc"),
        Value::String(Chars::from(SPEED_DOC)),
    )?;
    let speed =
        publisher.publish(session_base.append("control/speed/current"), Value::V32(1))?;
    let state_doc = publisher.publish(
        session_base.append("control/state/doc"),
        Value::String(Chars::from(STATE_DOC)),
    )?;
    let state = publisher.publish(
        session_base.append("control/state/current"),
        Value::String(Chars::from("Stop")),
    )?;
    let pos_doc = publisher.publish(
        session_base.append("control/pos/doc"),
        Value::String(Chars::from(POS_DOC)),
    )?;
    let pos =
        publisher.publish(session_base.append("control/pos/current"), Value::Null)?;
    start.writes(control_tx.clone());
    end.writes(control_tx.clone());
    speed.writes(control_tx.clone());
    control.writes(control_tx.clone());
    pos.writes(control_tx);
}

async fn run_publisher(
    mut archive: Archive<ReadOnly>,
    publisher: Publisher,
    publish_base: Path,
) -> Result<()> {
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
