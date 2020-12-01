use futures::{channel::mpsc, future, prelude::*, select_biased};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, Publisher, Val, Value},
    resolver::{Auth, Glob, ResolverRead},
    subscriber::{Dval, Event, SubId},
};
use netidx_protocols::archive::{
    Archive, BatchItem, Cursor, MonotonicTimestamper, ReadOnly, ReadWrite, RemapRequired,
    Timestamp,
};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    ops::Bound,
    sync::Arc,
};
use tokio::{runtime::Runtime, sync::broadcast, task, time};
use uuid::{adaptor::SimpleRef, Uuid};

#[derive(Debug, Clone)]
enum BCastMsg {
    Batch(Timestamp, Arc<Pooled<Vec<BatchItem>>>),
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
    mut bcast: broadcast::Receiver<BCastMsg>,
    mut archive: Archive<ReadOnly>,
    publisher: Publisher,
    publish_base: Path,
    session_id: Uuid,
) -> Result<()> {
    enum Speed {
        Limited {
            rate: usize,
            next: time::Sleep,
            last: DateTime<Utc>,
            current: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
        },
        Unlimited(Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>),
        Paused,
        Tail,
    }
    let mut published: HashMap<u64, Val> = HashMap::new();
    let mut cursor = Cursor::new();
    let mut speed = Speed::Paused;
    async fn next(
        speed: &mut Speed,
        cursor: &mut Cursor,
        archive: &Archive<ReadOnly>,
    ) -> Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)> {
        match &mut self.speed {
            Speed::Paused | Speed::Tail => future::pending().await,
            Speed::Unlimited(batches) => match batches.pop_front() {
                Some(batch) => Ok(batch),
                None => {
                    batches =
                        task::block_in_place(|| archive.read_deltas(cursor, 100))?;
                    match batches.pop_front() {
                        Some(batch) => Ok(batch),
                        None => {
                            self.speed = Speed::Tail;
                            future::pending().await
                        }
                    }
                }
            },
            Speed::Limited { rate: _, next, last, current } => {
                use std::time::Duration;
                use tokio::time::Instant;
                if current.is_empty() {
                    current =
                        task::block_in_place(|| archive.read_deltas(cursor, 100))?;
                    if current.is_empty() {
                        self.speed = Speed::Tail;
                        return future::pending().await;
                    }
                }
                let (ts, batch) = current.pop_front().unwrap();
                last = ts;
                next.await;
                if current.is_empty() {
                    self.speed = Speed::Tail;
                } else {
                    let wait = (current[0].0 - last).num_milliseconds() / rate;
                    next.reset(Instant::now() + Duration::from_millis(wait));
                }
                Ok((ts, batch))
            }
        }
    }
    async fn process_batch(
        publisher: &Publisher,
        published: &mut HashMap<u64, Val>,
        data_base: &Path,
        archive: &Archive<ReadOnly>,
        pos: &Val,
        mut batch: (DateTime<Utc>, Pooled<Vec<BatchItem>>),
    ) -> Result<()> {
        for BatchItem(id, ev) in batch.1.drain(..) {
            match ev {
                Event::Unsubscribed => {
                    published.remove(&id);
                }
                Event::Update(v) => match published.get(&id) {
                    Some(val) => {
                        val.update(v);
                    }
                    None => {
                        let path = archive.path_for_id(&id).unwrap();
                        let path = data_base.append(&path);
                        let val = publisher.publish(path, v)?;
                        published.insert(id, val);
                    }
                },
            }
        }
        pos.update(Value::DateTime(batch.0));
        publisher.flush().await
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
    start.writes(control_tx.clone());
    let end_doc = publisher.publish(
        session_base.append("control/end/doc"),
        Value::String(Chars::from(END_DOC)),
    )?;
    let end = publisher.publish(
        session_base.append("control/end/current"),
        Value::String(Chars::from("Unbounded")),
    )?;
    end.writes(control_tx.clone());
    let speed_doc = publisher.publish(
        session_base.append("control/speed/doc"),
        Value::String(Chars::from(SPEED_DOC)),
    )?;
    let speed =
        publisher.publish(session_base.append("control/speed/current"), Value::V32(1))?;
    speed.writes(control_tx.clone());
    let state_doc = publisher.publish(
        session_base.append("control/state/doc"),
        Value::String(Chars::from(STATE_DOC)),
    )?;
    let state = publisher.publish(
        session_base.append("control/state/current"),
        Value::String(Chars::from("Stop")),
    )?;
    state.writes(control_tx.clone());
    let pos_doc = publisher.publish(
        session_base.append("control/pos/doc"),
        Value::String(Chars::from(POS_DOC)),
    )?;
    let pos =
        publisher.publish(session_base.append("control/pos/current"), Value::Null)?;
    pos.writes(control_tx);
    let mut control_rx = control_rx.fuse();
    let data_base = session_base.append("data");
    loop {
        select_biased! {
            r = bcast.recv() => match r {
                Err(broadcast::error::RecvError::Closed) => break Ok(()),
                Err(broadcast::error::RecvError::Lagged(missed)) => match speed {
                    Speed::Limited {..} | Speed::Unlimited | Speed::Paused => (),
                    Speed::Tail => {
                        // log?
                        archive = archive.mirror()?;
                        let mut batchs = task::block_in_place(|| {
                            archive.read_deltas(&mut cursor, missed)
                        })?;
                        for batch in batches.drain(..) {
                            process_batch(
                                &publisher,
                                &mut published,
                                &data_base,
                                &archive,
                                &pos,
                                batch
                            ).await?;
                        }
                    }
                }
                Ok(BCastMsg::Batch(ts, batch)) => match speed {
                    Speed::Limited {..} | Speed::Unlimited | Speed::Paused => ()
                    Speed::Tail => if cursor.contains(&ts) {
                        process_batch(
                            &publisher,
                            &mut published,
                            &data_base,
                            &archive,
                            &pos,
                            (ts, batch)
                        ).await?;
                        cursor.set_current(ts);
                    }
                },
            },
            r = control_rx.next() => match r {
                None => break Ok(()),
                Some(mut batch) => {
                    for req in batch.drain(..) {
                        
                    }
                },
            },
            r = next(&mut speed, &mut cursor, archive) => match r {
                Err(e) if e.downcast::<RemapRequired>().is_some() => {
                    archive = archive.mirror()?;
                }
                Err(e) => break Err(e),
                Ok(batch) => {
                    process_batch(
                        &publisher,
                        &mut published,
                        &data_base,
                        &archive,
                        &pos,
                        batch
                    ).await?;
                }
            }
        }
    }
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
