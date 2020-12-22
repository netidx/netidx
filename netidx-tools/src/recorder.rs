use anyhow::Result;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use log::{info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, Publisher, Val, Value, WriteRequest},
    resolver::Auth,
    glob::{Glob, GlobSet},
    subscriber::{Subscriber, Event},
};
use netidx_protocols::archive::{
    ArchiveReader, ArchiveWriter, BatchItem, Cursor, Id, Seek, Timestamp,
};
use std::{
    collections::{HashMap, VecDeque},
    mem,
    ops::Bound,
    sync::Arc,
};
use tokio::{runtime::Runtime, sync::broadcast, task, time};
use uuid::{adapter::SimpleRef, Uuid};

#[derive(Debug, Clone)]
enum BCastMsg {
    Batch(Timestamp, Arc<Pooled<Vec<BatchItem>>>),
}

mod publish {
    use super::*;

    static START_DOC: &'static str = "The timestamp you want to replay to start at, or Unbounded for the beginning of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. Default Unbounded.";
    static END_DOC: &'static str = "Time timestamp you want to replay end at, or Unbounded for the end of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. default Unbounded";
    static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, 0.5 = 1/2 realtime, Unlimited = as fast as data can be read and sent. Default is Unlimited";
    static STATE_DOC: &'static str = "The current state of playback, {Stop, Pause, Play}. Pause, pause at the current position. Stop, reset playback to the initial state, unpublish everything, and wait. Default Stop.";
    static POS_DOC: &'static str = "The current playback position. Null if playback is stopped, otherwise the timestamp of the current record. Set to any timestamp where start <= t <= end to seek. Set to [+-][0-9]+ to seek a specific number of batches, e.g. +1 to single step forward -1 to single step back. Set to [+-][0-9]+[yMdhms] to step forward or back that amount of time, e.g. -1y step back 1 year.";

    fn uuid_string(id: Uuid) -> String {
        let mut buf = [0u8; SimpleRef::LENGTH];
        id.to_simple_ref().encode_lower(&mut buf).into()
    }

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
            rate: f64,
            next: time::Sleep,
            current: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
        },
    }

    struct T {
        publisher: Publisher,
        published: HashMap<Id, Val>,
        cursor: Cursor,
        speed: Speed,
        state: State,
        archive: ArchiveReader,
        data_base: Path,
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

    impl T {
        async fn new(
            publisher: Publisher,
            archive: ArchiveReader,
            session_id: Uuid,
            publish_base: Path,
            control_tx: mpsc::Sender<Pooled<Vec<WriteRequest>>>,
        ) -> Result<T> {
            let session_base = session_base(&publish_base, session_id);
            let data_base = session_base.append("data");
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
            pos_ctl.writes(control_tx);
            publisher.flush(None).await;
            Ok(T {
                publisher,
                published: HashMap::new(),
                cursor: Cursor::new(),
                speed: Speed::Unlimited(Pooled::orphan(VecDeque::new())),
                state: State::Stop,
                archive,
                data_base,
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
                        let mut next = next;
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
                        (&mut next).await;
                        if current.is_empty() {
                            self.state = State::Tail;
                        } else {
                            let wait = {
                                let ms = (current[0].0 - ts).num_milliseconds() as f64;
                                (ms / *rate).trunc() as u64
                            };
                            next.reset(Instant::now() + Duration::from_millis(wait));
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
            }
        }

        async fn process_control_batch(
            &mut self,
            mut batch: Pooled<Vec<WriteRequest>>,
        ) -> Result<()> {
            for req in batch.drain(..) {
                if req.id == self.start_ctl.id() {
                    if let Some(new_start) = get_bound(req) {
                        self.start_ctl.update(bound_to_val(new_start));
                        self.cursor.set_start(new_start);
                        if self.cursor.current().is_none() {
                            self.stop()?;
                        }
                    }
                } else if req.id == self.end_ctl.id() {
                    if let Some(new_end) = get_bound(req) {
                        self.end_ctl.update(bound_to_val(new_end));
                        self.cursor.set_end(new_end);
                        if self.cursor.current().is_none() {
                            self.stop()?;
                        }
                    }
                } else if req.id == self.speed_ctl.id() {
                    if let Ok(mp) = req.value.clone().cast_to::<f64>() {
                        self.set_speed(Some(mp));
                    } else if let Ok(s) = req.value.cast_to::<Chars>() {
                        if s.trim().to_lowercase().as_str() == "unlimited" {
                            self.set_speed(None);
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
                                self.state = State::Play;
                            } else if s.as_str() == "pause" {
                                self.state = State::Pause;
                            } else if s.as_str() == "stop" {
                                self.stop()?
                            } else if let Some(reply) = req.send_result {
                                let e = format!("invalid command {}", s);
                                let e = Chars::from(e);
                                reply.send(Value::Error(e));
                            }
                        }
                    }
                } else if req.id == self.pos_ctl.id() {
                    match req.value.cast_to::<Seek>() {
                        Ok(pos) => self.seek(pos)?,
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
            self.state = State::Stop;
            self.state_ctl.update(Value::String(Chars::from("Stop")));
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
                    next.reset(time::Instant::now());
                }
            }
            self.reimage()
        }

        fn seek(&mut self, seek: Seek) -> Result<()> {
            let current = match &mut self.speed {
                Speed::Unlimited(v) => v,
                Speed::Limited { current, next, .. } => {
                    next.reset(time::Instant::now());
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
                    self.speed_ctl.update(Value::String(Chars::from("Unlimited")));
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
                            next: time::sleep(std::time::Duration::from_secs(0)),
                            current: v,
                        };
                    }
                }
            }
        }
    }

    async fn wait_client_if_idle(publisher: &Publisher, idle: bool) {
        if idle {
            publisher.wait_any_client().await
        } else {
            future::pending().await
        }
    }

    async fn session(
        mut bcast: broadcast::Receiver<BCastMsg>,
        archive: ArchiveReader,
        publisher: Publisher,
        publish_base: Path,
        session_id: Uuid,
    ) -> Result<()> {
        let (control_tx, control_rx) = mpsc::channel(3);
        let mut t =
            T::new(publisher.clone(), archive, session_id, publish_base, control_tx)
                .await?;
        t.stop()?;
        let mut control_rx = control_rx.fuse();
        let mut idle = false;
        let mut idle_check = time::interval(std::time::Duration::from_secs(30)).fuse();
        loop {
            select_biased! {
                _ = idle_check.next() => {
                    let no_clients = publisher.clients() == 0;
                    if no_clients && idle {
                        break Ok(())
                    } else {
                        idle = no_clients;
                    }
                },
                _ = wait_client_if_idle(&publisher, idle).fuse() => {
                    idle = false;
                },
                m = bcast.recv().fuse() => t.process_bcast(m).await?,
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
    ) -> Result<()> {
        let session_publisher =
            Publisher::new(resolver.clone(), desired_auth.clone(), bind_cfg.clone())
                .await?;
        let session_ctl =
            session_publisher.publish(publish_base.append("session"), Value::Null)?;
        let (control_tx, mut control_rx) = mpsc::channel(3);
        session_ctl.writes(control_tx);
        while let Some(mut batch) = control_rx.next().await {
            for req in batch.drain(..) {
                if req.id == session_ctl.id() {
                    let publisher = Publisher::new(
                        resolver.clone(),
                        desired_auth.clone(),
                        bind_cfg.clone(),
                    )
                    .await?;
                    let bcast = bcast.subscribe();
                    let archive = archive.clone();
                    let publish_base = publish_base.clone();
                    let session_id = Uuid::new_v4();
                    let idstr = uuid_string(session_id);
                    session_ctl.update_subscriber(&req.addr, idstr.clone().into());
                    task::spawn(async move {
                        let res =
                            session(bcast, archive, publisher, publish_base, session_id)
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
                    session_publisher.flush(None).await;
                }
            }
        }
        Ok(())
    }
}

mod record {
    use super::*;

    pub(super) async fn run(
        bcast: broadcast::Sender<BCastMsg>,
        archive: ArchiveWriter,
        resolver: Config,
        desired_auth: Auth,
        spec: Vec<Glob>,
    ) -> Result<()> {
        let subscriber = Subscriber::new(resolver, desired_auth)?;
        let resolver = subscriber.resolver();
        Ok(())
    }
}

async fn run_async(
    config: Config,
    publish_args: Option<(BindCfg, Path)>,
    auth: Auth,
    archive: String,
    spec: Vec<Glob>,
) {
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
        task::spawn(async move {
            let res = publish::run(
                bcast_tx.clone(),
                reader,
                config.clone(),
                auth.clone(),
                bind_cfg,
                publish_base,
            )
            .await;
            match res {
                Ok(()) => info!("archive publisher exited"),
                Err(e) => warn!("archive publisher exited with error: {}", e),
            }
        });
    }
}

pub(crate) fn run(
    config: Config,
    _foreground: bool,
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
    if spec.is_empty() && publish_args.is_none() {
        panic!("you must specify a publish config, some paths to log, or both")
    }
    let spec = spec
        .into_iter()
        .map(|p| Glob::new(Path::from(p)))
        .collect::<Result<Vec<Glob>>>()
        .unwrap();
    let rt = Runtime::new().expect("failed to init tokio runtime");
    rt.block_on(run_async(config, publish_args, auth, archive, spec))
}
