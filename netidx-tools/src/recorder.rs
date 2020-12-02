use futures::{channel::mpsc, future, prelude::*, select_biased};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, Publisher, Val, Value, WriteRequest},
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
    mem,
    ops::Bound,
    sync::Arc,
};
use tokio::{runtime::Runtime, sync::broadcast, task, time};
use uuid::{adaptor::SimpleRef, Uuid};

#[derive(Debug, Clone)]
enum BCastMsg {
    Batch(Timestamp, Arc<Pooled<Vec<BatchItem>>>),
}

mod publish {
    use super::*;

    static START_DOC: &'static str = "The timestamp you want to replay to start at, or Unbounded for the beginning of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. Default Unbounded.";
    static END_DOC: &'static str = "Time timestamp you want to replay end at, or Unbounded for the end of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. default Unbounded";
    static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, Unlimited = as fast as data can be read and sent. Default is Unlimited";
    static STATE_DOC: &'static str = "The current state of playback, {Stop, Pause, Play}. Pause, pause at the current position. Stop, reset playback to the initial state, unpublish everything, and wait. Default Stop.";
    static POS_DOC: &'static str = "The current playback position. Null if playback is stopped, otherwise the timestamp of the current record. Set to any timestamp where start <= t <= end to seek";

    fn session_base(publish_base: &Path, id: Uuid) -> Path {
        let mut buf = [u8; SimpleRef::LENGTH];
        publish_base.append(id.to_simple_ref().encode_lower(&mut buf));
    }

    fn parse_dt(s: &str) -> Result<DateTime<Utc>> {
        use diligent_date_parser::parse_date;
        let s = s.trim();
        if let Some(dt) = parse_date(s) {
            Ok(dt.with_timezone(&Utc))
        } else if s.starts_with(&['+', '-'])
            && s.ends_with(&['y', 'M', 'd', 'h', 'm', 's'])
            && s.is_ascii()
            && s.len() > 2
        {
            let dir = s[0];
            let mag = s[s.len() - 1];
            match s[1..s.len() - 1].parse::<f64>() {
                Err(_) => None,
                Some(quantity) => {
                    let now = Utc::now();
                    let quantity = if mag == 'y' {
                        quantity * 365.24 * 86400.
                    } else if mag == 'M' {
                        quantity * (365.24 / 12.) * 86400.
                    } else if mag == 'd' {
                        quantity * 86400.
                    } else if mag == 'h' {
                        quantity * 3600.
                    } else if mag == 'm' {
                        quantity * 60.
                    } else {
                        quantity
                    };
                    let offset = Duration::nanoseconds((quantity * 1e9).trunc() as i64);
                    if dir == '+' {
                        Some(now + offset)
                    } else {
                        Some(now - offset)
                    }
                }
            }
        } else {
            bail!("{} is not a valid datetime or offset", s)
        }
    }

    fn parse_bound(v: Value) -> Result<Bound<DateTime<Utc>>> {
        use Value::*;
        match v {
            U32(_) | V32(_) | I32(_) | Z32(_) | U64(_) | V64(_) | I64(_) | Z64(_)
            | F32(_) | F64(_) | Duration(_) | Bytes(_) | True | False | Null | Ok
            | Error(_) => bail!("unexpected value {:?}", v),
            DateTime(ts) => Bound::Include(ts),
            String(c) if c.trim() == "Unbounded" => Ok(Bound::Unbounded),
            String(c) => parse_dt(&*c),
        }
    }

    fn get_bound(r: WriteRequest) -> Option<Bound<DateTime<Utc>>> {
        match parse_bound(r.value) {
            Ok(b) => Some(b),
            Err(e) => {
                if let Some(reply) = r.send_result {
                    reply.send(Value::Error(Chars::from(format!("{}", e))))
                }
                None
            }
        }
    }

    fn bound_to_val(b: Bound<DateTime<Utc>>) -> Value {
        match b {
            Bound::Unlimited => Value::String(Chars::from("Unbounded")),
            Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum State {
        Stop,
        Play,
        Pause,
        Tail,
    }

    impl State {
        fn play(&self) -> bool {
            match self {
                State::Play => true,
                State::Pause | State::Stop | State::Tail => false,
            }
        }
    }

    #[derive(Debug)]
    enum Speed {
        Unlimited(Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>),
        Limited {
            rate: usize,
            next: time::Sleep,
            current: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
        },
    }

    #[derive(Debug)]
    struct T {
        publisher: Publisher,
        published: HashMap<u64, Val>,
        cursor: Cursor,
        speed: Speed,
        state: State,
        archive: Archive<ReadOnly>,
        session_base: Path,
        data_base: Path,
        start_doc: Val,
        start_ctl: Val,
        end_doc: Val,
        end_ctl: Val,
        speed_doc: Val,
        speed_ctl: Val,
        state_doc: Val,
        state_ctl: Val,
        pos_doc: Val,
        pos_ctl: Val,
    }

    impl T {
        async fn new(
            publisher: Publisher,
            archive: Archive<ReadOnly>,
            session_id: Uuid,
            publish_base: Path,
            control_tx: mpsc::Sender<Pooled<Vec<WriteRequest>>>,
        ) -> Result<T> {
            let session_base = session_base(&publish_base, session_id);
            let data_base = session_base.append("data");
            let start_doc = publisher.publish(
                session_base.append("control/start/doc"),
                Value::String(Chars::from(START_DOC)),
            )?;
            let end_doc = publisher.publish(
                session_base.append("control/end/doc"),
                Value::String(Chars::from(END_DOC)),
            )?;
            let speed_doc = publisher.publish(
                session_base.append("control/speed/doc"),
                Value::String(Chars::from(SPEED_DOC)),
            )?;
            let state_doc = publisher.publish(
                session_base.append("control/state/doc"),
                Value::String(Chars::from(STATE_DOC)),
            )?;
            let pos_doc = publisher.publish(
                session_base.append("control/pos/doc"),
                Value::String(Chars::from(POS_DOC)),
            )?;
            let start_ctl = publisher.publish(
                session_base.append("control/start/current"),
                Value::String(Chars::from("Unbounded")),
            )?;
            start.writes(control_tx.clone());
            let end_ctl = publisher.publish(
                session_base.append("control/end/current"),
                Value::String(Chars::from("Unbounded")),
            )?;
            end.writes(control_tx.clone());
            let speed_ctl = publisher
                .publish(session_base.append("control/speed/current"), Value::V32(1))?;
            speed.writes(control_tx.clone());
            let state_ctl = publisher.publish(
                session_base.append("control/state/current"),
                Value::String(Chars::from("Stop")),
            )?;
            state.writes(control_tx.clone());
            let pos_ctl = publisher
                .publish(session_base.append("control/pos/current"), Value::Null)?;
            pos.writes(control_tx);
            publisher.flush().await;
            Ok(T {
                publisher,
                published: HashMap::new(),
                cursor: Cursor::new(),
                speed: Speed::Unlimited(Pooled::orphan(VecDeque::new())),
                state: State::Stop,
                archive,
                session_base,
                data_base,
                start_doc,
                start_ctl,
                end_doc,
                end_ctl,
                speed_doc,
                speed_ctl,
                state_doc,
                state_ctl,
                pos_doc,
                pos_ctl,
            })
        }

        async fn next(&mut self) -> Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)> {
            if !self.state.play() {
                future::pending().await
            } else {
                match &mut self.speed {
                    Speed::Unlimited(batches) => match batches.pop_front() {
                        Some(batch) => Ok(batch),
                        None => {
                            batches = task::block_in_place(|| {
                                self.archive.read_deltas(cursor, 100)
                            })?;
                            match batches.pop_front() {
                                Some(batch) => Ok(batch),
                                None => {
                                    self.state = Speed::Tail;
                                    future::pending().await
                                }
                            }
                        }
                    },
                    Speed::Limited { rate, next, current } => {
                        use std::time::Duration;
                        use tokio::time::Instant;
                        if current.is_empty() {
                            current = task::block_in_place(|| {
                                self.archive.read_deltas(cursor, 100)
                            })?;
                            if current.is_empty() {
                                self.state = State::Tail;
                                return future::pending().await;
                            }
                        }
                        let (ts, batch) = current.pop_front().unwrap();
                        next.await;
                        if current.is_empty() {
                            self.state = State::Tail;
                        } else {
                            let wait = (current[0].0 - ts).num_milliseconds() / rate;
                            next.reset(Instant::now() + Duration::from_millis(wait));
                        }
                        Ok((ts, batch))
                    }
                }
            }
        }

        async fn process_batch(
            &mut self,
            mut batch: (DateTime<Utc>, Pooled<Vec<BatchItem>>),
        ) -> Result<()> {
            for BatchItem(id, ev) in batch.1.drain(..) {
                match ev {
                    Event::Unsubscribed => {
                        self.published.remove(&id);
                    }
                    Event::Update(v) => match self.published.get(&id) {
                        Some(val) => {
                            val.update(v);
                        }
                        None => {
                            let path = self.archive.path_for_id(&id).unwrap();
                            let path = self.data_base.append(&path);
                            let val = self.publisher.publish(path, v)?;
                            self.published.insert(id, val);
                        }
                    },
                }
            }
            self.pos.update(Value::DateTime(batch.0));
            self.publisher.flush().await
        }

        fn stop(&mut self) {
            self.state = State::Stop;
            self.state_ctl.update(Value::String(Chars::from("Stop")));
            self.published.clear();
            match &mut self.speed {
                Speed::Unlimited(v) => {
                    v.clear();
                }
                Speed::Limited { current, next, .. } => {
                    current.clear();
                    next.reset(time::Instant::now());
                }
            }
        }

        fn update_rate(&mut self, new_rate: Option<usize>) {
            match &mut self.speed {
                Speed::Limited { rate, current, .. } => match new_rate {
                    Some(new_rate) => {
                        *rate = new_rate;
                    }
                    None => {
                        let c = mem::replace(current, Pooled::orphan(VecDeque::new()));
                        self.speed = Speed::Unlimited(c);
                    }
                },
                Speed::Unlimited(v) => {
                    if let Some(new_rate) = new_rate {
                        let v = mem::replace(v, Pooled::orphan(VecDeque::new()));
                        self.speed = Speed::Limited {
                            rate: new_rate,
                            next: time::sleep(time::Instant::now()),
                            current: v,
                        };
                    }
                }
            }
        }
    }

    async fn session(
        mut bcast: broadcast::Receiver<BCastMsg>,
        archive: Archive<ReadOnly>,
        publisher: Publisher,
        publish_base: Path,
        session_id: Uuid,
    ) -> Result<()> {
        let (control_tx, control_rx) = mpsc::channel(3);
        let mut t =
            T::new(publisher, archive, session_id, publish_base, control_tx).await?;
        let mut control_rx = control_rx.fuse();
        loop {
            select_biased! {
                r = bcast.recv() => match r {
                    Err(broadcast::error::RecvError::Closed) => break Ok(()),
                    Err(broadcast::error::RecvError::Lagged(missed)) => match t.state {
                        State::Stop | State::Play | State::Pause => (),
                        State::Tail => {
                            // log?
                            t.archive = t.archive.mirror()?;
                            let mut batchs = task::block_in_place(|| {
                                t.archive.read_deltas(&mut cursor, missed)
                            })?;
                            for (ts, batch) in batches.drain(..) {
                                t.process_batch((ts, batch)).await?;
                                t.cursor.set_current(ts);
                            }
                        }
                    }
                    Ok(BCastMsg::Batch(ts, batch)) => match t.state {
                        State::Stop | State::Play | State::Pause => (),
                        Speed::Tail => if t.cursor.contains(&ts) {
                            t.process_batch((ts, batch)).await?;
                            t.cursor.set_current(ts);
                        }
                    },
                },
                r = control_rx.next() => match r {
                    None => break Ok(()),
                    Some(mut batch) => {
                        for req in batch.drain(..) {
                            if req.id == t.start_ctl.id() {
                                if let Some(new_start) = get_bound(req) {
                                    t.start_ctl.update(bound_to_val(new_start));
                                    t.cursor.set_start(new_start);
                                    if t.cursor.current().is_none() { t.stop(); }
                                }
                            }
                            if req.id == t.end.id() {
                                if let Some(new_end) = get_bound(req) {
                                    t.end_ctl.update(bound_to_val(new_end));
                                    t.cursor.set_end(new_end);
                                    if t.cursor.current().is_none() { t.stop(); }
                                }
                            }
                            if req.id == t.speed_ctl.id() {
                                if let Some(mp) = req.val.clone().cast_u64() {
                                    t.speed_ctl.update(Value::U64(mp));
                                } else if let Some(s) = req.val.cast_string() {
                                    if s.trim() == "Unlimited" {
                                        speed = Speed::Unlimited
                                    }
                                }
                            }
                            if req.id == state.id() {
                            }
                            if req.id == pos.id() {
                            }
                        }
                        t.publisher.flush().await
                    },
                },
                r = t.next() => match r {
                    Err(e) if e.downcast::<RemapRequired>().is_some() => {
                        t.archive = t.archive.mirror()?;
                    }
                    Err(e) => break Err(e),
                    Ok(batch) => { t.process_batch(batch).await?; }
                }
            }
        }
    }

    pub(super) async fn run(
        mut archive: Archive<ReadOnly>,
        publisher: Publisher,
        publish_base: Path,
    ) -> Result<()> {
        let mut sessions: HashMap<Uuid, Session> = HashMap::new();
        let session = publisher.publish(publish_base.append("session"))?;
    }
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
