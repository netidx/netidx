use anyhow::Result;
use bytes::Bytes;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use fxhash::FxBuildHasher;
use log::{info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    glob::{Glob, GlobSet},
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, Publisher, Val, Value, WriteRequest},
    resolver::{Auth, ChangeTracker, ResolverRead},
    subscriber::{Dval, Event, SubId, Subscriber},
};
use netidx_protocols::{
    archive::{
        ArchiveReader, ArchiveWriter, BatchItem, Cursor, Id, MonotonicTimestamper, Seek,
        Timestamp, BATCH_POOL,
    },
    cluster::Cluster,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    mem,
    ops::Bound,
    sync::Arc,
};
use tokio::{runtime::Runtime, sync::broadcast, task, time};
use uuid::{adapter::SimpleRef, Uuid};

#[derive(Debug, Clone)]
enum BCastMsg {
    Batch(Timestamp, Arc<Pooled<Vec<BatchItem>>>),
    Stop,
}

mod publish {
    use super::*;

    static START_DOC: &'static str = "The timestamp you want to replay to start at, or Unbounded for the beginning of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. Default Unbounded.";
    static END_DOC: &'static str = "Time timestamp you want to replay end at, or Unbounded for the end of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. default Unbounded";
    static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, 0.5 = 1/2 realtime, Unlimited = as fast as data can be read and sent. Default is Unlimited";
    static STATE_DOC: &'static str = "The current state of playback, {Stop, Pause, Play}. Pause, pause at the current position. Stop, reset playback to the initial state, unpublish everything, and wait. Default Stop.";
    static POS_DOC: &'static str = "The current playback position. Null if playback is stopped, otherwise the timestamp of the current record. Set to any timestamp where start <= t <= end to seek. Set to [+-][0-9]+ to seek a specific number of batches, e.g. +1 to single step forward -1 to single step back. Set to [+-][0-9]+[yMdhms] to step forward or back that amount of time, e.g. -1y step back 1 year.";

    fn session_base(publish_base: &Path, id: Uuid) -> Path {
        let mut buf = [0u8; SimpleRef::LENGTH];
        publish_base.append(id.to_simple_ref().encode_lower(&mut buf))
    }

    fn parse_bound(v: Value) -> Result<Bound<DateTime<Utc>>> {
        match v {
            Value::DateTime(ts) => Ok(Bound::Included(ts)),
            Value::String(c) if c.trim().to_lowercase().as_str() == "unbounded" => {
                Ok(Bound::Unbounded)
            }
            v => match v.cast_to::<Seek>()? {
                Seek::Absolute(ts) => Ok(Bound::Included(ts)),
                Seek::TimeRelative(offset) => Ok(Bound::Included(Utc::now() + offset)),
                Seek::BatchRelative(_) => bail!("invalid bound"),
            },
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
            Bound::Unbounded => Value::String(Chars::from("Unbounded")),
            Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum ClusterCmd {
        NotIdle,
        SeekTo(String),
        SetStart(Bound<DateTime<Utc>>),
        SetEnd(Bound<DateTime<Utc>>),
        SetSpeed(Option<f64>),
        SetState(State),
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
            rate: f64,
            next: time::Instant,
            current: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
        },
    }

    struct Controls {
        _start_doc: Val,
        start_ctl: Val,
        _end_doc: Val,
        end_ctl: Val,
        _speed_doc: Val,
        speed_ctl: Val,
        _state_doc: Val,
        state_ctl: Val,
        _pos_doc: Val,
        pos_ctl: Val,
    }

    impl Controls {
        async fn new(
            session_base: &Path,
            publisher: &Publisher,
            control_tx: &mpsc::Sender<Pooled<Vec<WriteRequest>>>,
        ) -> Result<Self> {
            let _start_doc = publisher.publish(
                session_base.append("control/start/doc"),
                Value::String(Chars::from(START_DOC)),
            )?;
            let _end_doc = publisher.publish(
                session_base.append("control/end/doc"),
                Value::String(Chars::from(END_DOC)),
            )?;
            let _speed_doc = publisher.publish(
                session_base.append("control/speed/doc"),
                Value::String(Chars::from(SPEED_DOC)),
            )?;
            let _state_doc = publisher.publish(
                session_base.append("control/state/doc"),
                Value::String(Chars::from(STATE_DOC)),
            )?;
            let _pos_doc = publisher.publish(
                session_base.append("control/pos/doc"),
                Value::String(Chars::from(POS_DOC)),
            )?;
            let start_ctl = publisher.publish(
                session_base.append("control/start/current"),
                Value::String(Chars::from("Unbounded")),
            )?;
            start_ctl.writes(control_tx.clone());
            let end_ctl = publisher.publish(
                session_base.append("control/end/current"),
                Value::String(Chars::from("Unbounded")),
            )?;
            end_ctl.writes(control_tx.clone());
            let speed_ctl = publisher
                .publish(session_base.append("control/speed/current"), Value::V32(1))?;
            speed_ctl.writes(control_tx.clone());
            let state_ctl = publisher.publish(
                session_base.append("control/state/current"),
                Value::String(Chars::from("Stop")),
            )?;
            state_ctl.writes(control_tx.clone());
            let pos_ctl = publisher
                .publish(session_base.append("control/pos/current"), Value::Null)?;
            pos_ctl.writes(control_tx.clone());
            publisher.flush(None).await;
            Ok(Controls {
                _start_doc,
                start_ctl,
                _end_doc,
                end_ctl,
                _speed_doc,
                speed_ctl,
                _state_doc,
                state_ctl,
                _pos_doc,
                pos_ctl,
            })
        }
    }

    struct T {
        idle: bool,
        publisher: Publisher,
        published: HashMap<Id, Val>,
        cursor: Cursor,
        speed: Speed,
        state: State,
        archive: ArchiveReader,
        session_base: Path,
        data_base: Path,
        cluster: Cluster<ClusterCmd>,
        controls: Option<Controls>,
        control_tx: mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    }

    impl T {
        async fn new(
            subscriber: Subscriber,
            publisher: Publisher,
            archive: ArchiveReader,
            session_id: Uuid,
            publish_base: Path,
            control_tx: mpsc::Sender<Pooled<Vec<WriteRequest>>>,
            shards: usize,
        ) -> Result<T> {
            let session_base = session_base(&publish_base, session_id);
            let data_base = session_base.append("data");
            let cluster =
                Cluster::new(&publisher, subscriber, data_base.append("cluster"), shards)
                    .await?;
            let controls = if cluster.primary() {
                Some(Controls::new(&session_base, &publisher, &control_tx).await?)
            } else {
                None
            };
            Ok(T {
                idle: false,
                publisher,
                published: HashMap::new(),
                cursor: Cursor::new(),
                speed: Speed::Unlimited(Pooled::orphan(VecDeque::new())),
                state: State::Stop,
                archive,
                session_base,
                data_base,
                cluster,
                controls,
                control_tx,
            })
        }

        async fn update_cluster(&mut self) -> Result<()> {
            self.cluster.poll_members().await?;
            match self.controls {
                None if self.cluster.primary() => {
                    self.controls = Some(
                        Controls::new(
                            &self.session_base,
                            &self.publisher,
                            &self.control_tx,
                        )
                        .await?,
                    );
                }
                Some(_) if !self.cluster.primary() => {
                    self.controls = None;
                }
                Some(_) | None => ()
            }
            Ok(())
        }

        async fn next(&mut self) -> Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)> {
            if !self.state.play() {
                future::pending().await
            } else {
                match &mut self.speed {
                    Speed::Unlimited(batches) => match batches.pop_front() {
                        Some(batch) => Ok(batch),
                        None => {
                            let archive = &self.archive;
                            let cursor = &mut self.cursor;
                            *batches = task::block_in_place(|| {
                                archive.read_deltas(cursor, 100)
                            })?;
                            match batches.pop_front() {
                                Some(batch) => Ok(batch),
                                None => {
                                    self.state = State::Tail;
                                    future::pending().await
                                }
                            }
                        }
                    },
                    Speed::Limited { rate, next, current } => {
                        use std::time::Duration;
                        use tokio::time::Instant;
                        if current.is_empty() {
                            let archive = &self.archive;
                            let cursor = &mut self.cursor;
                            *current = task::block_in_place(|| {
                                archive.read_deltas(cursor, 100)
                            })?;
                            if current.is_empty() {
                                self.state = State::Tail;
                                return future::pending().await;
                            }
                        }
                        let (ts, batch) = current.pop_front().unwrap();
                        time::sleep_until(*next).await;
                        if current.is_empty() {
                            self.state = State::Tail;
                        } else {
                            let wait = {
                                let ms = (current[0].0 - ts).num_milliseconds() as f64;
                                (ms / *rate).trunc() as u64
                            };
                            *next = Instant::now() + Duration::from_millis(wait);
                        }
                        Ok((ts, batch))
                    }
                }
            }
        }

        async fn process_batch(
            &mut self,
            batch: (DateTime<Utc>, &mut Vec<BatchItem>),
        ) -> Result<()> {
            for BatchItem(id, ev) in batch.1.drain(..) {
                let v = match ev {
                    Event::Unsubscribed => Value::Null,
                    Event::Update(v) => v,
                };
                match self.published.get(&id) {
                    Some(val) => {
                        val.update(v);
                    }
                    None => {
                        let path = self.archive.path_for_id(&id).unwrap();
                        let path = self.data_base.append(&path);
                        let val = self.publisher.publish(path, v)?;
                        self.published.insert(id, val);
                    }
                }
            }
            self.pos_ctl.update(Value::DateTime(batch.0));
            Ok(self.publisher.flush(None).await)
        }

        async fn process_bcast(
            &mut self,
            msg: Result<BCastMsg, broadcast::error::RecvError>,
        ) -> Result<()> {
            match msg {
                Err(broadcast::error::RecvError::Closed) => {
                    bail!("broadcast channel closed")
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => match self.state {
                    State::Stop | State::Play | State::Pause => Ok(()),
                    State::Tail => {
                        let archive = &self.archive;
                        let cursor = &mut self.cursor;
                        let mut batches = task::block_in_place(|| {
                            archive.read_deltas(cursor, missed as usize)
                        })?;
                        for (ts, mut batch) in batches.drain(..) {
                            self.process_batch((ts, &mut *batch)).await?;
                            self.cursor.set_current(ts);
                        }
                        Ok(())
                    }
                },
                Ok(BCastMsg::Batch(ts, batch)) => match self.state {
                    State::Stop | State::Play | State::Pause => Ok(()),
                    State::Tail => {
                        let dt = ts.datetime();
                        let pos = self.cursor.current().unwrap_or(chrono::MIN_DATETIME);
                        if self.cursor.contains(&dt) && pos < dt {
                            let mut batch = (*batch).clone();
                            self.process_batch((dt, &mut batch)).await?;
                            self.cursor.set_current(dt);
                        }
                        Ok(())
                    }
                },
                Ok(BCastMsg::Stop) => bail!("stop signal"),
            }
        }

        fn set_start(&mut self, new_start: Bound<DateTime<Utc>>) -> Result<()> {
            self.start_ctl.update(bound_to_val(new_start));
            self.cursor.set_start(new_start);
            if self.cursor.current().is_none() {
                self.stop()?;
            }
            Ok(())
        }

        fn set_end(&mut self, new_end: Bound<DateTime<Utc>>) -> Result<()> {
            self.end_ctl.update(bound_to_val(new_end));
            self.cursor.set_end(new_end);
            if self.cursor.current().is_none() {
                self.stop()?;
            }
            Ok(())
        }

        fn set_state(&mut self, state: State) {
            self.state = state;
            self.state_ctl.update(Value::from(match state {
                State::Stop => "stop",
                State::Play => "play",
                State::Pause => "pause",
                State::Tail => "tail",
            }))
        }

        async fn process_control_batch(
            &mut self,
            mut batch: Pooled<Vec<WriteRequest>>,
        ) -> Result<()> {
            for req in batch.drain(..) {
                if req.id == self.start_ctl.id() {
                    if let Some(new_start) = get_bound(req) {
                        self.set_start(new_start)?;
                        self.cluster.send_cmd(&ClusterCmd::SetStart(new_start));
                    }
                } else if req.id == self.end_ctl.id() {
                    if let Some(new_end) = get_bound(req) {
                        self.set_end(new_end)?;
                        self.cluster.send_cmd(&ClusterCmd::SetEnd(new_end));
                    }
                } else if req.id == self.speed_ctl.id() {
                    if let Ok(mp) = req.value.clone().cast_to::<f64>() {
                        self.set_speed(Some(mp));
                        self.cluster.send_cmd(&ClusterCmd::SetSpeed(Some(mp)));
                    } else if let Ok(s) = req.value.cast_to::<Chars>() {
                        if s.trim().to_lowercase().as_str() == "unlimited" {
                            self.set_speed(None);
                            self.cluster.send_cmd(&ClusterCmd::SetSpeed(None));
                        } else if let Some(reply) = req.send_result {
                            let e = Chars::from("invalid speed");
                            reply.send(Value::Error(e));
                        }
                    } else if let Some(reply) = req.send_result {
                        let e = Chars::from("invalid speed");
                        reply.send(Value::Error(e));
                    }
                } else if req.id == self.state_ctl.id() {
                    match req.value.cast_to::<Chars>() {
                        Err(_) => {
                            if let Some(reply) = req.send_result {
                                let e = Chars::from("expected string");
                                reply.send(Value::Error(e))
                            }
                        }
                        Ok(s) => {
                            let s = s.trim().to_lowercase();
                            if s.as_str() == "play" {
                                self.set_state(State::Play);
                                self.cluster.send_cmd(&ClusterCmd::SetState(State::Play));
                            } else if s.as_str() == "pause" {
                                self.set_state(State::Pause);
                                self.cluster
                                    .send_cmd(&ClusterCmd::SetState(State::Pause));
                            } else if s.as_str() == "stop" {
                                self.stop()?;
                                self.cluster.send_cmd(&ClusterCmd::SetState(State::Stop));
                            } else if let Some(reply) = req.send_result {
                                let e = format!("invalid command {}", s);
                                let e = Chars::from(e);
                                reply.send(Value::Error(e));
                            }
                        }
                    }
                } else if req.id == self.pos_ctl.id() {
                    match req.value.cast_to::<Seek>() {
                        Ok(pos) => {
                            self.seek(pos)?;
                            self.cluster.send_cmd(&ClusterCmd::SeekTo(pos.to_string()));
                        }
                        Err(e) => {
                            if let Some(reply) = req.send_result {
                                let e = Chars::from(format!("{}", e));
                                reply.send(Value::Error(e))
                            }
                        }
                    }
                }
            }
            Ok(self.publisher.flush(None).await)
        }

        fn process_control_cmd(&mut self, cmd: ClusterCmd) -> Result<()> {
            match cmd {
                ClusterCmd::SeekTo(s) => match s.parse::<Seek>() {
                    Ok(pos) => self.seek(pos),
                    Err(e) => {
                        warn!("invalid seek from cluster {}, {}", s, e);
                        Ok(())
                    }
                },
                ClusterCmd::SetStart(new_start) => self.set_start(new_start),
                ClusterCmd::SetEnd(new_end) => self.set_end(new_end),
                ClusterCmd::SetSpeed(sp) => Ok(self.set_speed(sp)),
                ClusterCmd::SetState(st) => Ok(self.set_state(st)),
                ClusterCmd::NotIdle => Ok(self.idle = false),
            }
        }

        fn reimage(&mut self) -> Result<()> {
            let img = task::block_in_place(|| self.archive.build_image(&self.cursor))?;
            let mut idx = self.archive.get_index();
            self.pos_ctl.update(match self.cursor.current() {
                Some(ts) => Value::DateTime(ts),
                None => match self.cursor.start() {
                    Bound::Unbounded => Value::Null,
                    Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
                },
            });
            for (id, path) in idx.drain(..) {
                let v = match img.get(&id) {
                    None | Some(Event::Unsubscribed) => Value::Null,
                    Some(Event::Update(v)) => v.clone(),
                };
                match self.published.get(&id) {
                    Some(val) => {
                        val.update(v);
                    }
                    None => {
                        let path = self.data_base.append(path.as_ref());
                        let val = self.publisher.publish(path, v)?;
                        self.published.insert(id, val);
                    }
                }
            }
            Ok(())
        }

        fn stop(&mut self) -> Result<()> {
            self.set_state(State::Stop);
            self.cursor.reset();
            self.pos_ctl.update(match self.cursor.start() {
                Bound::Unbounded => Value::Null,
                Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
            });
            match &mut self.speed {
                Speed::Unlimited(v) => {
                    v.clear();
                }
                Speed::Limited { current, next, .. } => {
                    current.clear();
                    *next = time::Instant::now();
                }
            }
            self.reimage()
        }

        fn seek(&mut self, seek: Seek) -> Result<()> {
            let current = match &mut self.speed {
                Speed::Unlimited(v) => v,
                Speed::Limited { current, next, .. } => {
                    *next = time::Instant::now();
                    current
                }
            };
            match current.pop_front() {
                None => (),
                Some((ts, _)) => {
                    self.cursor.set_current(ts);
                    current.clear()
                }
            }
            self.archive.seek(&mut self.cursor, seek);
            self.reimage()
        }

        fn set_speed(&mut self, new_rate: Option<f64>) {
            match new_rate {
                None => {
                    self.speed_ctl.update(Value::String(Chars::from("unlimited")));
                }
                Some(new_rate) => {
                    self.speed_ctl.update(Value::F64(new_rate));
                }
            }
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
                            next: time::Instant::now(),
                            current: v,
                        };
                    }
                }
            }
        }

        fn not_idle(&mut self) {
            self.idle = false;
            self.cluster.send_cmd(&ClusterCmd::NotIdle);
        }
    }

    async fn session(
        mut bcast: broadcast::Receiver<BCastMsg>,
        archive: ArchiveReader,
        subscriber: Subscriber,
        publisher: Publisher,
        publish_base: Path,
        session_id: Uuid,
        shards: usize,
    ) -> Result<()> {
        let (control_tx, control_rx) = mpsc::channel(3);
        let mut t = T::new(
            subscriber,
            publisher.clone(),
            archive,
            session_id,
            publish_base,
            control_tx,
            shards,
        )
        .await?;
        t.stop()?;
        let mut control_rx = control_rx.fuse();
        let mut idle_check = time::interval(std::time::Duration::from_secs(30));
        loop {
            select_biased! {
                _ = idle_check.tick().fuse() => {
                    t.update_cluster().await?;
                    let has_clients = publisher.clients() > t.cluster.others();
                    if !has_clients && t.idle {
                        break Ok(())
                    } else if has_clients {
                        t.not_idle()
                    } else {
                        t.idle = true;
                    }
                },
                _ = t.publisher.wait_any_new_client().fuse() => {
                    if publisher.clients() > t.cluster.others() {
                        t.not_idle()
                    }
                },
                m = bcast.recv().fuse() => t.process_bcast(m).await?,
                cmds = t.cluster.wait_cmds().fuse() => {
                    for cmd in cmds? {
                        t.process_control_cmd(cmd)?
                    }
                    publisher.flush(None).await;
                },
                r = control_rx.next() => match r {
                    None => break Ok(()),
                    Some(batch) => t.process_control_batch(batch).await?
                },
                r = t.next().fuse() => match r {
                    Err(e) => break Err(e),
                    Ok((ts, mut batch)) => { t.process_batch((ts, &mut *batch)).await?; }
                }
            }
        }
    }

    pub(super) async fn run(
        bcast: broadcast::Sender<BCastMsg>,
        archive: ArchiveReader,
        resolver: Config,
        desired_auth: Auth,
        bind_cfg: BindCfg,
        publish_base: Path,
        shards: usize,
    ) -> Result<()> {
        let subscriber = Subscriber::new(resolver.clone(), desired_auth.clone())?;
        let session_publisher =
            Publisher::new(resolver.clone(), desired_auth.clone(), bind_cfg.clone())
                .await?;
        let session_ctl =
            session_publisher.publish(publish_base.append("session"), Value::Null)?;
        let (control_tx, control_rx) = mpsc::channel(3);
        let mut cluster = Cluster::new(
            &session_publisher,
            subscriber.clone(),
            publish_base.clone(),
            shards,
        )
        .await?;
        let mut control_rx = control_rx.fuse();
        let mut bcast_rx = bcast.subscribe();
        let mut poll_members = time::interval(std::time::Duration::from_secs(30));
        session_ctl.writes(control_tx);
        let start_session = |session_id: Uuid| -> () {
            let publisher =
                Publisher::new(resolver.clone(), desired_auth.clone(), bind_cfg.clone())
                    .await?;
            let bcast = bcast.subscribe();
            let archive = archive.clone();
            let publish_base = publish_base.clone();
            let idstr = uuid_string(session_id);
            let subscriber = subscriber.clone();
            session_ctl.update_subscriber(&req.addr, idstr.clone().into());
            task::spawn(async move {
                let res = session(
                    bcast,
                    archive,
                    subscriber,
                    publisher,
                    publish_base,
                    session_id,
                    shards,
                )
                .await;
                match res {
                    Ok(()) => {
                        info!("session {} existed", idstr)
                    }
                    Err(e) => {
                        warn!("session {} exited {}", idstr, e)
                    }
                }
            });
        };
        loop {
            select_biased! {
                m = bcast_rx.recv().fuse() => match m {
                    Err(_) | Ok(BCastMsg::Batch(_, _)) => (),
                    Ok(BCastMsg::Stop) => break Ok(()),
                },
                _ = poll_members.tick().fuse() => {
                    cluster.poll_members().await?
                },
                cmds = cluster.wait_cmds().fuse() => {

                },
                m = control_rx.next() => match m {
                    None => break Ok(()),
                    Some(mut batch) => {
                        for req in batch.drain(..) {
                            if req.id == session_ctl.id() {
                                let session_id = Uuid::new_v4();
                                start_session(session_id);
                                cluster.send_cmd(&ClusterCmd::NewSession(session_id));
                                session_publisher.flush(None).await;
                            }
                        }
                    }
                },
            }
        }
    }
}

mod record {
    use super::*;

    #[derive(Debug)]
    struct CTS(BTreeMap<Path, ChangeTracker>);

    impl CTS {
        fn new(globs: &Vec<Glob>) -> CTS {
            let mut btm = BTreeMap::new();
            for glob in globs {
                let base = glob.base();
                match btm
                    .range::<str, (Bound<&str>, Bound<&str>)>((
                        Bound::Unbounded,
                        Bound::Excluded(base),
                    ))
                    .next_back()
                {
                    Some((p, _)) if Path::is_parent(p, base) => (),
                    None | Some(_) => {
                        let base = Path::from(Arc::from(base));
                        let ct = ChangeTracker::new(base.clone());
                        btm.insert(base, ct);
                    }
                }
            }
            CTS(btm)
        }

        async fn changed(&mut self, r: &ResolverRead) -> Result<bool> {
            let res =
                future::join_all(self.0.iter_mut().map(|(_, ct)| r.check_changed(ct)))
                    .await;
            for r in res {
                if r? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }

    async fn maybe_check(
        need_check: bool,
        resolver: &ResolverRead,
        cts: &mut CTS,
    ) -> Result<bool> {
        if !need_check {
            future::pending().await
        } else {
            cts.changed(resolver).await
        }
    }

    async fn maybe_list(
        need_list: bool,
        resolver: &ResolverRead,
        pat: &GlobSet,
    ) -> Result<Pooled<Vec<Path>>> {
        if !need_list {
            future::pending().await
        } else {
            resolver.list_matching(pat).await
        }
    }

    async fn maybe_interval(poll: &mut Option<time::Interval>) {
        match poll {
            None => future::pending().await,
            Some(poll) => {
                poll.tick().await;
            }
        }
    }

    pub(super) async fn run(
        bcast: broadcast::Sender<BCastMsg>,
        mut archive: ArchiveWriter,
        resolver: Config,
        desired_auth: Auth,
        poll_interval: Option<time::Duration>,
        image_frequency: Option<usize>,
        flush_frequency: Option<usize>,
        flush_interval: Option<time::Duration>,
        spec: Vec<Glob>,
    ) -> Result<()> {
        let (tx_batch, rx_batch) = mpsc::channel(3);
        let mut rx_batch = rx_batch.fuse();
        let mut by_subid: HashMap<SubId, Id, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher::default());
        let mut image: HashMap<SubId, Event, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher::default());
        let mut subscribed: HashMap<Path, Dval> = HashMap::new();
        let subscriber = Subscriber::new(resolver, desired_auth)?;
        let resolver = subscriber.resolver();
        let mut cts = CTS::new(&spec);
        let spec = GlobSet::new(true, spec)?;
        let flush_frequency = flush_frequency.map(|f| archive.block_size() * f);
        let mut bcast_rx = bcast.subscribe();
        let mut poll = poll_interval.map(time::interval);
        let mut flush = flush_interval.map(time::interval);
        let mut to_add = Vec::new();
        let mut timest = MonotonicTimestamper::new();
        let mut last_image = archive.len();
        let mut last_flush = archive.len();
        let mut need_check = false;
        let mut need_list = false;
        loop {
            select_biased! {
                m = bcast_rx.recv().fuse() => match m {
                    Err(_) | Ok(BCastMsg::Batch(_, _)) => (),
                    Ok(BCastMsg::Stop) => break,
                },
                _ = maybe_interval(&mut poll).fuse() => {
                    need_check = true;
                },
                _ = maybe_interval(&mut flush).fuse() => {
                    if archive.len() > last_flush {
                        task::block_in_place(|| -> Result<()> {
                            archive.flush()?;
                            Ok(last_flush = archive.len())
                        })?;
                    }
                }
                r = maybe_check(need_check, &resolver, &mut cts).fuse() => {
                    need_check = false;
                    need_list = r?;
                },
                r = maybe_list(need_list, &resolver, &spec).fuse() => {
                    need_list = false;
                    let mut paths = r?;
                    for path in paths.drain(..) {
                        if !subscribed.contains_key(&path) {
                            let dv = subscriber.durable_subscribe(path.clone());
                            let id = dv.id();
                            dv.updates(true, tx_batch.clone());
                            subscribed.insert(path.clone(), dv);
                            to_add.push((path, id));
                        }
                    }
                    task::block_in_place(|| {
                        let i = to_add.iter().map(|(ref p, _)| p);
                        archive.add_paths(i)
                    })?;
                    for (path, subid) in to_add.drain(..) {
                        if !by_subid.contains_key(&subid) {
                            let id = archive.id_for_path(&path).unwrap();
                            by_subid.insert(subid, id);
                        }
                    }
                },
                batch = rx_batch.next() => match batch {
                    None => break,
                    Some(mut batch) => {
                        let mut tbatch = BATCH_POOL.take();
                        let ts = timest.timestamp();
                        task::block_in_place(|| -> Result<()> {
                            for (subid, ev) in batch.drain(..) {
                                if image_frequency.is_some() {
                                    image.insert(subid, ev.clone());
                                }
                                tbatch.push(BatchItem(by_subid[&subid], ev));
                            }
                            archive.add_batch(false, ts, &tbatch)?;
                            match image_frequency {
                                None => (),
                                Some(freq) if archive.len() - last_image < freq => (),
                                Some(_) => {
                                    let mut b = BATCH_POOL.take();
                                    let ts = timest.timestamp();
                                    for (id, ev) in image.iter() {
                                        b.push(BatchItem(by_subid[id], ev.clone()));
                                    }
                                    archive.add_batch(true, ts, &b)?;
                                    last_image = archive.len();
                                }
                            }
                            match flush_frequency {
                                None => (),
                                Some(freq) if archive.len() - last_flush < freq => (),
                                Some(_) => {
                                    archive.flush()?;
                                    last_flush = archive.len();
                                }
                            }
                            Ok(())
                        })?;
                        let _ = bcast.send(BCastMsg::Batch(ts, Arc::new(tbatch)));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
async fn should_exit() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut quit = signal(SignalKind::quit())?;
    let mut intr = signal(SignalKind::interrupt())?;
    select_biased! {
        _ = term.recv().fuse() => Ok(()),
        _ = quit.recv().fuse() => Ok(()),
        _ = intr.recv().fuse() => Ok(()),
    }
}

#[cfg(windows)]
async fn should_exit() -> Result<()> {
    Ok(signal::ctrl_c().await?)
}

async fn run_async(
    config: Config,
    publish_args: Option<(BindCfg, Path)>,
    auth: Auth,
    image_frequency: Option<usize>,
    poll_interval: Option<time::Duration>,
    flush_frequency: Option<usize>,
    flush_interval: Option<time::Duration>,
    shards: usize,
    archive: String,
    spec: Vec<Glob>,
) {
    let mut wait = Vec::new();
    let (bcast_tx, bcast_rx) = broadcast::channel(100);
    drop(bcast_rx);
    let writer = if spec.is_empty() {
        None
    } else {
        Some(ArchiveWriter::open(archive.as_str()).unwrap())
    };
    if let Some((bind_cfg, publish_base)) = publish_args {
        let reader = writer
            .as_ref()
            .map(|w| w.reader().unwrap())
            .unwrap_or_else(|| ArchiveReader::open(archive.as_str()).unwrap());
        let bcast_tx = bcast_tx.clone();
        let config = config.clone();
        let auth = auth.clone();
        wait.push(task::spawn(async move {
            let res = publish::run(
                bcast_tx,
                reader,
                config,
                auth,
                bind_cfg,
                publish_base,
                shards,
            )
            .await;
            match res {
                Ok(()) => info!("archive publisher exited"),
                Err(e) => warn!("archive publisher exited with error: {}", e),
            }
        }));
    }
    if !spec.is_empty() {
        let bcast_tx = bcast_tx.clone();
        wait.push(task::spawn(async move {
            let res = record::run(
                bcast_tx,
                writer.unwrap(),
                config,
                auth,
                poll_interval,
                image_frequency,
                flush_frequency,
                flush_interval,
                spec,
            )
            .await;
            match res {
                Ok(()) => info!("archive writer exited"),
                Err(e) => warn!("archive writer exited with error: {}", e),
            }
        }));
    }
    let mut dead = future::join_all(wait).fuse();
    loop {
        select_biased! {
            _ = should_exit().fuse() => {
                let _ = bcast_tx.send(BCastMsg::Stop);
            },
            _ = dead => break
        }
    }
}

pub(crate) fn run(
    config: Config,
    _foreground: bool,
    bind: Option<BindCfg>,
    publish_base: Option<Path>,
    auth: Auth,
    image_frequency: usize,
    poll_interval: u64,
    flush_frequency: usize,
    flush_interval: u64,
    shards: usize,
    archive: String,
    spec: Vec<String>,
) {
    let image_frequency = if image_frequency == 0 { None } else { Some(image_frequency) };
    let poll_interval = if poll_interval == 0 {
        None
    } else {
        Some(time::Duration::from_secs(poll_interval))
    };
    let flush_frequency = if flush_frequency == 0 { None } else { Some(flush_frequency) };
    let flush_interval = if flush_interval == 0 {
        None
    } else {
        Some(time::Duration::from_secs(flush_interval))
    };
    let publish_args = match (bind, publish_base) {
        (None, None) => None,
        (Some(bind), Some(publish_base)) => Some((bind, publish_base)),
        (Some(_), None) | (None, Some(_)) => {
            panic!("you must specify bind and publish_base to publish an archive")
        }
    };
    if spec.is_empty() && publish_args.is_none() {
        panic!("you must specify a publish config, some paths to log, or both")
    }
    let spec = spec
        .into_iter()
        .map(Chars::from)
        .map(Glob::new)
        .collect::<Result<Vec<Glob>>>()
        .unwrap();
    let rt = Runtime::new().expect("failed to init tokio runtime");
    rt.block_on(run_async(
        config,
        publish_args,
        auth,
        image_frequency,
        poll_interval,
        flush_frequency,
        flush_interval,
        shards,
        archive,
        spec,
    ))
}
