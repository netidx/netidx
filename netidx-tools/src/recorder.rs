use anyhow::Result;
use chrono::prelude::*;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, Fuse},
    prelude::*,
    select_biased,
};
use fxhash::FxBuildHasher;
use log::{error, info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::Pooled,
    protocol::glob::{Glob, GlobSet},
    publisher::{BindCfg, Publisher, Val, Value, WriteRequest},
    resolver::{Auth, ChangeTracker, ResolverRead},
    subscriber::{Dval, Event, SubId, Subscriber},
    utils,
};
use netidx_protocols::{
    archive::{
        ArchiveReader, ArchiveWriter, BatchItem, Cursor, Id, MonotonicTimestamper,
        RecordTooLarge, Seek, Timestamp, BATCH_POOL,
    },
    cluster::{uuid_string, Cluster},
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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
    static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, 0.5 = 1/2 realtime, Unlimited = as fast as data can be read and sent. Default is 1";
    static STATE_DOC: &'static str = "The current state of playback, {pause, play, tail}. Tail, seek to the end of the archive and play any new messages that arrive. Default pause.";
    static POS_DOC: &'static str = "The current playback position. Null if the archive is empty, or the timestamp of the current record. Set to any timestamp where start <= t <= end to seek. Set to [+-][0-9]+ to seek a specific number of batches, e.g. +1 to single step forward -1 to single step back. Set to [+-][0-9]+[yMdhmsu] to step forward or back that amount of time, e.g. -1y step back 1 year. -1u to step back 1 microsecond. set to 'beginning' to seek to the beginning and 'end' to seek to the end. By default the initial position is set to 'beginning' when opening the archive.";

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
                Seek::Beginning => Ok(Bound::Unbounded),
                Seek::End => Ok(Bound::Unbounded),
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
        Play,
        Pause,
        Tail,
    }

    impl State {
        fn play(&self) -> bool {
            match self {
                State::Play => true,
                State::Pause | State::Tail => false,
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
                Value::String(Chars::from("pause")),
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
        publisher: Publisher,
        published: HashMap<Id, Val, FxBuildHasher>,
        cursor: Cursor,
        speed: Speed,
        state: State,
        archive: ArchiveReader,
        data_base: Path,
    }

    impl T {
        async fn new(
            publisher: Publisher,
            archive: ArchiveReader,
            data_base: Path,
        ) -> Result<T> {
            Ok(T {
                publisher,
                published: HashMap::with_hasher(FxBuildHasher::default()),
                cursor: Cursor::new(),
                speed: Speed::Limited {
                    rate: 1.,
                    next: time::Instant::now(),
                    current: Pooled::orphan(VecDeque::new()),
                },
                state: State::Pause,
                archive,
                data_base,
            })
        }

        async fn next(
            &mut self,
            controls: Option<&Controls>,
        ) -> Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)> {
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
                                    self.set_state(controls, State::Tail);
                                    self.publisher.flush(None).await;
                                    future::pending().await
                                }
                            }
                        }
                    },
                    Speed::Limited { rate, next, current } => {
                        use std::time::Duration;
                        use tokio::time::Instant;
                        if current.len() < 2 {
                            let archive = &self.archive;
                            let cursor = &mut self.cursor;
                            let mut cur = task::block_in_place(|| {
                                archive.read_deltas(cursor, 100)
                            })?;
                            for v in current.drain(..) {
                                cur.push_front(v);
                            }
                            *current = cur;
                            if current.is_empty() {
                                self.set_state(controls, State::Tail);
                                self.publisher.flush(None).await;
                                return future::pending().await;
                            }
                        }
                        let (ts, batch) = current.pop_front().unwrap();
                        time::sleep_until(*next).await;
                        if current.is_empty() {
                            self.set_state(controls, State::Tail);
                            self.publisher.flush(None).await;
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
            controls: Option<&Controls>,
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
            controls.map(|c| c.pos_ctl.update(Value::DateTime(batch.0)));
            Ok(self.publisher.flush(None).await)
        }

        async fn process_bcast(
            &mut self,
            controls: Option<&Controls>,
            msg: Result<BCastMsg, broadcast::error::RecvError>,
        ) -> Result<()> {
            match msg {
                Err(broadcast::error::RecvError::Closed) => {
                    bail!("broadcast channel closed")
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => match self.state {
                    State::Play | State::Pause => Ok(()),
                    State::Tail => {
                        let archive = &self.archive;
                        let cursor = &mut self.cursor;
                        let mut batches = task::block_in_place(|| {
                            archive.read_deltas(cursor, missed as usize)
                        })?;
                        for (ts, mut batch) in batches.drain(..) {
                            self.process_batch(controls, (ts, &mut *batch)).await?;
                            self.cursor.set_current(ts);
                        }
                        Ok(())
                    }
                },
                Ok(BCastMsg::Batch(ts, batch)) => match self.state {
                    State::Play | State::Pause => Ok(()),
                    State::Tail => {
                        let dt = ts.datetime();
                        let pos = self.cursor.current().unwrap_or(chrono::MIN_DATETIME);
                        if self.cursor.contains(&dt) && pos < dt {
                            let mut batch = (*batch).clone();
                            self.process_batch(controls, (dt, &mut batch)).await?;
                            self.cursor.set_current(dt);
                        }
                        Ok(())
                    }
                },
                Ok(BCastMsg::Stop) => bail!("stop signal"),
            }
        }

        fn set_start(
            &mut self,
            controls: Option<&Controls>,
            new_start: Bound<DateTime<Utc>>,
        ) -> Result<()> {
            controls.map(|c| c.start_ctl.update(bound_to_val(new_start)));
            self.cursor.set_start(new_start);
            if self.cursor.current().is_none() {
                self.seek(controls, Seek::Beginning)?;
            }
            Ok(())
        }

        fn set_end(
            &mut self,
            controls: Option<&Controls>,
            new_end: Bound<DateTime<Utc>>,
        ) -> Result<()> {
            controls.map(|c| c.end_ctl.update(bound_to_val(new_end)));
            self.cursor.set_end(new_end);
            if self.cursor.current().is_none() {
                self.seek(controls, Seek::Beginning)?;
            }
            Ok(())
        }

        fn set_state(&mut self, controls: Option<&Controls>, state: State) {
            self.state = state;
            controls.map(|c| {
                c.state_ctl.update(Value::from(match state {
                    State::Play => "play",
                    State::Pause => "pause",
                    State::Tail => "tail",
                }))
            });
        }

        async fn process_control_batch(
            &mut self,
            controls: &Controls,
            cluster: &Cluster<ClusterCmd>,
            mut batch: Pooled<Vec<WriteRequest>>,
        ) -> Result<()> {
            for req in batch.drain(..) {
                if req.id == controls.start_ctl.id() {
                    if let Some(new_start) = get_bound(req) {
                        self.set_start(Some(controls), new_start)?;
                        cluster.send_cmd(&ClusterCmd::SetStart(new_start));
                    }
                } else if req.id == controls.end_ctl.id() {
                    if let Some(new_end) = get_bound(req) {
                        self.set_end(Some(controls), new_end)?;
                        cluster.send_cmd(&ClusterCmd::SetEnd(new_end));
                    }
                } else if req.id == controls.speed_ctl.id() {
                    if let Ok(mp) = req.value.clone().cast_to::<f64>() {
                        self.set_speed(Some(controls), Some(mp));
                        cluster.send_cmd(&ClusterCmd::SetSpeed(Some(mp)));
                    } else if let Ok(s) = req.value.cast_to::<Chars>() {
                        if s.trim().to_lowercase().as_str() == "unlimited" {
                            self.set_speed(Some(controls), None);
                            cluster.send_cmd(&ClusterCmd::SetSpeed(None));
                        } else if let Some(reply) = req.send_result {
                            let e = Chars::from("invalid speed");
                            reply.send(Value::Error(e));
                        }
                    } else if let Some(reply) = req.send_result {
                        let e = Chars::from("invalid speed");
                        reply.send(Value::Error(e));
                    }
                } else if req.id == controls.state_ctl.id() {
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
                                self.set_state(Some(controls), State::Play);
                                cluster.send_cmd(&ClusterCmd::SetState(State::Play));
                            } else if s.as_str() == "pause" {
                                self.set_state(Some(controls), State::Pause);
                                cluster.send_cmd(&ClusterCmd::SetState(State::Pause));
                            } else if s.as_str() == "tail" {
                                self.seek(Some(controls), Seek::End)?;
                                self.set_state(Some(controls), State::Tail);
                                cluster.send_cmd(&ClusterCmd::SetState(State::Tail));
                            } else if let Some(reply) = req.send_result {
                                let e = format!("invalid command {}", s);
                                let e = Chars::from(e);
                                reply.send(Value::Error(e));
                            }
                        }
                    }
                } else if req.id == controls.pos_ctl.id() {
                    match req.value.cast_to::<Seek>() {
                        Ok(pos) => {
                            self.seek(Some(controls), pos)?;
                            match self.state {
                                State::Pause | State::Play => (),
                                State::Tail => {
                                    self.set_state(Some(controls), State::Play);
                                }
                            }
                            cluster.send_cmd(&ClusterCmd::SeekTo(pos.to_string()));
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
                    Ok(pos) => self.seek(None, pos),
                    Err(e) => {
                        warn!("invalid seek from cluster {}, {}", s, e);
                        Ok(())
                    }
                },
                ClusterCmd::SetStart(new_start) => self.set_start(None, new_start),
                ClusterCmd::SetEnd(new_end) => self.set_end(None, new_end),
                ClusterCmd::SetSpeed(sp) => Ok(self.set_speed(None, sp)),
                ClusterCmd::SetState(st) => Ok(self.set_state(None, st)),
                ClusterCmd::NotIdle => Ok(()),
            }
        }

        fn reimage(&mut self, controls: Option<&Controls>) -> Result<()> {
            let mut img =
                task::block_in_place(|| self.archive.build_image(&self.cursor))?;
            let mut idx = task::block_in_place(|| self.archive.get_index());
            controls.map(|c| {
                c.pos_ctl.update(match self.cursor.current() {
                    Some(ts) => Value::DateTime(ts),
                    None => match self.cursor.start() {
                        Bound::Unbounded => Value::Null,
                        Bound::Included(ts) | Bound::Excluded(ts) => Value::DateTime(ts),
                    },
                })
            });
            for (id, path) in idx.drain(..) {
                let v = match img.remove(&id) {
                    None | Some(Event::Unsubscribed) => Value::Null,
                    Some(Event::Update(v)) => v,
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

        fn seek(&mut self, controls: Option<&Controls>, seek: Seek) -> Result<()> {
            let current = match &mut self.speed {
                Speed::Unlimited(v) => v,
                Speed::Limited { current, next, .. } => {
                    *next = time::Instant::now();
                    current
                }
            };
            if let Some((ts, _)) = current.pop_front() {
                self.cursor.set_current(ts);
                current.clear()
            }
            self.archive.seek(&mut self.cursor, seek);
            self.reimage(controls)
        }

        fn set_speed(&mut self, controls: Option<&Controls>, new_rate: Option<f64>) {
            controls.map(|c| match new_rate {
                None => {
                    c.speed_ctl.update(Value::String(Chars::from("unlimited")));
                }
                Some(new_rate) => {
                    c.speed_ctl.update(Value::F64(new_rate));
                }
            });
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
    }

    async fn update_cluster(
        cluster: &mut Cluster<ClusterCmd>,
        controls: &mut Option<Controls>,
        session_base: &Path,
        publisher: &Publisher,
        control_tx: &mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    ) -> Result<()> {
        cluster.poll_members().await?;
        match controls {
            None if cluster.primary() => {
                *controls =
                    Some(Controls::new(&session_base, &publisher, &control_tx).await?);
            }
            Some(_) if !cluster.primary() => {
                *controls = None;
            }
            Some(_) | None => (),
        }
        Ok(())
    }

    fn not_idle(idle: &mut bool, cluster: &Cluster<ClusterCmd>) {
        *idle = false;
        cluster.send_cmd(&ClusterCmd::NotIdle);
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
        let session_base = session_base(&publish_base, session_id);
        let data_base = session_base.append("data");
        let mut cluster =
            Cluster::new(&publisher, subscriber, session_base.append("cluster"), shards)
                .await?;
        let mut controls = if cluster.primary() {
            Some(Controls::new(&session_base, &publisher, &control_tx).await?)
        } else {
            None
        };
        archive.check_remap_rescan()?;
        let mut t = T::new(publisher.clone(), archive, data_base).await?;
        t.seek(controls.as_ref(), Seek::Beginning)?;
        t.publisher.flush(None).await;
        let mut control_rx = control_rx.fuse();
        let mut idle_check = time::interval(std::time::Duration::from_secs(30));
        let mut idle = false;
        loop {
            select_biased! {
                _ = idle_check.tick().fuse() => {
                    update_cluster(
                        &mut cluster,
                        &mut controls,
                        &session_base,
                        &publisher,
                        &control_tx
                    ).await?;
                    let has_clients = publisher.clients() > cluster.others();
                    if !has_clients && idle {
                        break Ok(())
                    } else if has_clients {
                        not_idle(&mut idle, &cluster)
                    } else {
                        idle = true;
                    }
                },
                _ = publisher.wait_any_new_client().fuse() => {
                    if publisher.clients() > cluster.others() {
                        not_idle(&mut idle, &cluster)
                    }
                },
                m = bcast.recv().fuse() => t.process_bcast(controls.as_ref(), m).await?,
                cmds = cluster.wait_cmds().fuse() => {
                    for cmd in cmds? {
                        t.process_control_cmd(cmd)?
                    }
                    publisher.flush(None).await;
                },
                r = control_rx.next() => match r {
                    None => break Ok(()),
                    Some(batch) => if let Some(ref controls) = controls {
                        t.process_control_batch(controls, &cluster, batch).await?
                    }
                },
                r = t.next(controls.as_ref()).fuse() => match r {
                    Err(e) => break Err(e),
                    Ok((ts, mut batch)) => {
                        t.process_batch(controls.as_ref(), (ts, &mut *batch)).await?;
                    }
                }
            }
        }
    }

    async fn start_session(
        session_id: Uuid,
        bcast: &broadcast::Sender<BCastMsg>,
        resolver: &Config,
        desired_auth: &Auth,
        bind_cfg: &BindCfg,
        subscriber: &Subscriber,
        archive: &ArchiveReader,
        shards: usize,
        publish_base: &Path,
    ) -> Result<()> {
        let publisher =
            Publisher::new(resolver.clone(), desired_auth.clone(), bind_cfg.clone())
                .await?;
        let bcast = bcast.subscribe();
        let archive = archive.clone();
        let publish_base = publish_base.clone();
        let subscriber = subscriber.clone();
        let publisher_cl = publisher.clone();
        task::spawn(async move {
            let res = session(
                bcast,
                archive,
                subscriber,
                publisher_cl,
                publish_base,
                session_id,
                shards,
            )
            .await;
            if let Err(e) = publisher.shutdown().await {
                warn!("session {} publisher failed to shutdown {}", session_id, e);
            }
            match res {
                Ok(()) => {
                    info!("session {} existed", session_id)
                }
                Err(e) => {
                    error!("session {} exited {}", session_id, e)
                }
            }
        });
        Ok(())
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
        let mut cluster = Cluster::<Uuid>::new(
            &session_publisher,
            subscriber.clone(),
            publish_base.append("cluster"),
            shards,
        )
        .await?;
        let mut control_rx = control_rx.fuse();
        let mut bcast_rx = bcast.subscribe();
        let mut poll_members = time::interval(std::time::Duration::from_secs(30));
        session_ctl.writes(control_tx);
        loop {
            select_biased! {
                m = bcast_rx.recv().fuse() => match m {
                    Err(_) | Ok(BCastMsg::Batch(_, _)) => (),
                    Ok(BCastMsg::Stop) => break Ok(()),
                },
                _ = poll_members.tick().fuse() => {
                    cluster.poll_members().await?;
                },
                cmds = cluster.wait_cmds().fuse() => {
                    for session_id in cmds? {
                        start_session(
                            session_id,
                            &bcast,
                            &resolver,
                            &desired_auth,
                            &bind_cfg,
                            &subscriber,
                            &archive,
                            shards,
                            &publish_base
                        ).await?
                    }
                },
                m = control_rx.next() => match m {
                    None => break Ok(()),
                    Some(mut batch) => {
                        for req in batch.drain(..) {
                            if req.id == session_ctl.id() {
                                let session_id = Uuid::new_v4();
                                start_session(
                                    session_id,
                                    &bcast,
                                    &resolver,
                                    &desired_auth,
                                    &bind_cfg,
                                    &subscriber,
                                    &archive,
                                    shards,
                                    &publish_base,
                                ).await?;
                                cluster.send_cmd(&session_id);
                                session_ctl.update_subscriber(
                                    &req.addr,
                                    uuid_string(session_id).into()
                                );
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

    async fn maybe_interval(poll: &mut Option<time::Interval>) {
        match poll {
            None => future::pending().await,
            Some(poll) => {
                poll.tick().await;
            }
        }
    }

    type Lst = Option<Pooled<Vec<Pooled<Vec<Path>>>>>;

    async fn list_task(
        mut rx: mpsc::UnboundedReceiver<oneshot::Sender<Lst>>,
        resolver: ResolverRead,
        spec: Vec<Glob>,
    ) -> Result<()> {
        let mut cts = CTS::new(&spec);
        let spec = GlobSet::new(true, spec)?;
        while let Some(reply) = rx.next().await {
            if cts.changed(&resolver).await? {
                let _ = reply.send(Some(resolver.list_matching(&spec).await?));
            } else {
                let _ = reply.send(None);
            }
        }
        Ok(())
    }

    fn start_list_task(
        rx: mpsc::UnboundedReceiver<oneshot::Sender<Lst>>,
        resolver: ResolverRead,
        spec: Vec<Glob>,
    ) {
        task::spawn(async move {
            let r = list_task(rx, resolver, spec).await;
            match r {
                Err(e) => error!("list task exited with error {}", e),
                Ok(()) => info!("list task exited"),
            }
        });
    }

    async fn wait_list(pending: &mut Option<Fuse<oneshot::Receiver<Lst>>>) -> Lst {
        match pending {
            None => future::pending().await,
            Some(r) => match r.await {
                Ok(r) => r,
                Err(_) => None,
            },
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
        let (tx_list, rx_list) = mpsc::unbounded();
        let mut rx_batch = utils::Batched::new(rx_batch.fuse(), 1_000_000);
        let mut by_subid: HashMap<SubId, Id, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher::default());
        let mut image: HashMap<SubId, Event, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher::default());
        let mut subscribed: HashMap<Path, Dval> = HashMap::new();
        let subscriber = Subscriber::new(resolver, desired_auth)?;
        let flush_frequency = flush_frequency.map(|f| archive.block_size() * f);
        let mut bcast_rx = bcast.subscribe();
        let mut poll = poll_interval.map(time::interval);
        let mut flush = flush_interval.map(time::interval);
        let mut to_add = Vec::new();
        let mut timest = MonotonicTimestamper::new();
        let mut last_image = archive.len();
        let mut last_flush = archive.len();
        let mut pending_list: Option<Fuse<oneshot::Receiver<Lst>>> = None;
        let mut pending_batches: Vec<Pooled<Vec<(SubId, Event)>>> = Vec::new();
        start_list_task(rx_list, subscriber.resolver(), spec);
        loop {
            select_biased! {
                m = bcast_rx.recv().fuse() => match m {
                    Err(_) | Ok(BCastMsg::Batch(_, _)) => (),
                    Ok(BCastMsg::Stop) => break,
                },
                _ = maybe_interval(&mut poll).fuse() => {
                    if pending_list.is_none() {
                        let (tx, rx) = oneshot::channel();
                        let _ = tx_list.unbounded_send(tx);
                        pending_list = Some(rx.fuse());
                    }
                },
                _ = maybe_interval(&mut flush).fuse() => {
                    if archive.len() > last_flush {
                        task::block_in_place(|| -> Result<()> {
                            archive.flush()?;
                            Ok(last_flush = archive.len())
                        })?;
                    }
                }
                r = wait_list(&mut pending_list).fuse() => {
                    pending_list = None;
                    if let Some(mut batches) = r {
                        for mut batch in batches.drain(..) {
                            for path in batch.drain(..) {
                                if !subscribed.contains_key(&path) {
                                    let dv = subscriber.durable_subscribe(path.clone());
                                    let id = dv.id();
                                    dv.updates(true, tx_batch.clone());
                                    subscribed.insert(path.clone(), dv);
                                    to_add.push((path, id));
                                }
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
                    }
                },
                batch = rx_batch.next() => match batch {
                    None => break,
                    Some(utils::BatchItem::InBatch(batch)) => {
                        pending_batches.push(batch);
                    },
                    Some(utils::BatchItem::EndBatch) => {
                        let mut overflow = Vec::new();
                        let mut tbatch = BATCH_POOL.take();
                        let ts = timest.timestamp();
                        task::block_in_place(|| -> Result<()> {
                            for mut batch in pending_batches.drain(..) {
                                for (subid, ev) in batch.drain(..) {
                                    if image_frequency.is_some() {
                                        image.insert(subid, ev.clone());
                                    }
                                    tbatch.push(BatchItem(by_subid[&subid], ev));
                                }
                            }
                            loop { // handle batches >4 GiB
                                match archive.add_batch(false, ts, &tbatch) {
                                    Err(e) if e.is::<RecordTooLarge>() => {
                                        let at = tbatch.len() >> 1;
                                        overflow.push(tbatch.split_off(at));
                                    }
                                    Err(e) => bail!(e),
                                    Ok(()) => {
                                        match overflow.pop() {
                                            None => break,
                                            Some(b) => { tbatch = Pooled::orphan(b); }
                                        }
                                    }
                                }
                            }
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
                Err(e) => error!("archive publisher exited with error: {}", e),
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
                Err(e) => error!("archive writer exited with error: {}", e),
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
        (Some(bind), Some(publish_base)) => {
            match bind {
                BindCfg::Match { .. } => (),
                BindCfg::Exact(_) => {
                    panic!("exact bindcfgs are not supported for this publisher")
                }
            }
            Some((bind, publish_base))
        }
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
