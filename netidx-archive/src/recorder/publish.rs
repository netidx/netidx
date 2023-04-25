use super::logfile_collection::LogfileCollection;
use crate::{
    logfile::{ArchiveReader, BatchItem, Id, Seek},
    recorder::{BCastMsg, Config, PublishConfig},
};
use anyhow::{Error, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use fxhash::{FxHashMap, FxHashSet};
use log::{debug, error, info, warn};
use netidx::{
    chars::Chars,
    path::Path,
    pool::Pooled,
    protocol::value::FromValue,
    publisher::{
        self, ClId, PublishFlags, Publisher, UpdateBatch, Val, Value, WriteRequest,
    },
    resolver_client::{Glob, GlobSet},
    subscriber::{Event, Subscriber},
};
use netidx_derive::Pack;
use netidx_protocols::rpc::server::{RpcCall, RpcReply};
use netidx_protocols::{
    cluster::{uuid_string, Cluster},
    define_rpc,
    rpc::server::{ArgSpec, Proc},
    rpc_err,
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    str::FromStr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::broadcast, task, time};
use uuid::Uuid;

pub(crate) static START_DOC: &'static str = "The timestamp you want to replay to start at, or Unbounded for the beginning of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. Default Unbounded.";
pub(crate) static END_DOC: &'static str = "Time timestamp you want to replay end at, or Unbounded for the end of the archive. This can also be an offset from now in terms of [+-][0-9]+[.]?[0-9]*[yMdhms], e.g. -1.5d. default Unbounded";
static SPEED_DOC: &'static str = "How fast you want playback to run, e.g 1 = realtime speed, 10 = 10x realtime, 0.5 = 1/2 realtime, Unlimited = as fast as data can be read and sent. Default is 1";
static STATE_DOC: &'static str = "The current state of playback, {pause, play, tail}. Tail, seek to the end of the archive and play any new messages that arrive. Default pause.";
static POS_DOC: &'static str = "The current playback position. Null if the archive is empty, or the timestamp of the current record. Set to any timestamp where start <= t <= end to seek. Set to [+-][0-9]+ to seek a specific number of batches, e.g. +1 to single step forward -1 to single step back. Set to [+-][0-9]+[yMdhmsu] to step forward or back that amount of time, e.g. -1y step back 1 year. -1u to step back 1 microsecond. set to 'beginning' to seek to the beginning and 'end' to seek to the end. By default the initial position is set to 'beginning' when opening the archive.";
static PLAY_AFTER_DOC: &'static str = "Start playing after waiting the specified timeout";
pub(crate) static FILTER_DOC: &'static str = "Only publish paths matching the specified filter. e.g. [\"/**\"] would match everything";

fn session_base(publish_base: &Path, id: Uuid) -> Path {
    use uuid::fmt::Simple;
    let mut buf = [0u8; Simple::LENGTH];
    publish_base.append(Simple::from_uuid(id).encode_lower(&mut buf))
}

fn parse_speed(v: Value) -> Result<Option<f64>> {
    match v.clone().cast_to::<f64>() {
        Ok(speed) => Ok(Some(speed)),
        Err(_) => match v.cast_to::<Chars>() {
            Err(_) => bail!("expected a float, or unlimited"),
            Ok(s) => {
                if s.trim().to_lowercase().as_str() == "unlimited" {
                    Ok(None)
                } else {
                    bail!("expected a float, or unlimited")
                }
            }
        },
    }
}

pub(super) fn parse_bound(v: Value) -> Result<Bound<DateTime<Utc>>> {
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

#[derive(Debug, Clone, Pack)]
enum ClusterCmd {
    NotIdle,
    SeekTo(String),
    SetStart(Bound<DateTime<Utc>>),
    SetEnd(Bound<DateTime<Utc>>),
    SetSpeed(Option<f64>),
    SetState(State),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Pack)]
#[repr(u8)]
enum State {
    Play = 0,
    Pause = 1,
    Tail = 2,
}

struct AtomicState(AtomicU8);

impl AtomicState {
    fn new(s: State) -> Self {
        Self(AtomicU8::new(s as u8))
    }

    fn load(&self) -> State {
        match self.0.load(Ordering::Relaxed) {
            0 => State::Play,
            1 => State::Pause,
            2 => State::Tail,
            _ => unreachable!(),
        }
    }

    fn store(&self, s: State) {
        self.0.store(s as u8, Ordering::Relaxed)
    }
}

impl FromStr for State {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if s.as_str() == "play" {
            Ok(State::Play)
        } else if s.as_str() == "pause" {
            Ok(State::Pause)
        } else if s.as_str() == "tail" {
            Ok(State::Tail)
        } else {
            bail!("expected state [play, pause, tail]")
        }
    }
}

impl FromValue for State {
    fn from_value(v: Value) -> Result<Self> {
        Ok(v.cast_to::<Chars>()?.parse::<State>()?)
    }

    fn get(_: Value) -> Option<Self> {
        None
    }
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
    Unlimited,
    Limited {
        rate: f64,
        last_emitted: DateTime<Utc>,
        last_batch_ts: Option<DateTime<Utc>>,
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
        let start_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/start/current"),
            Value::String(Chars::from("Unbounded")),
        )?;
        publisher.writes(start_ctl.id(), control_tx.clone());
        let end_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/end/current"),
            Value::String(Chars::from("Unbounded")),
        )?;
        publisher.writes(end_ctl.id(), control_tx.clone());
        let speed_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/speed/current"),
            Value::F64(1.),
        )?;
        publisher.writes(speed_ctl.id(), control_tx.clone());
        let state_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/state/current"),
            Value::String(Chars::from("pause")),
        )?;
        publisher.writes(state_ctl.id(), control_tx.clone());
        let pos_ctl = publisher.publish_with_flags(
            PublishFlags::USE_EXISTING,
            session_base.append("control/pos/current"),
            Value::Null,
        )?;
        publisher.writes(pos_ctl.id(), control_tx.clone());
        publisher.flushed().await;
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

pub(super) fn parse_filter(globs: Vec<Chars>) -> Result<GlobSet> {
    let globs: Vec<Glob> =
        globs.into_iter().map(|s| Glob::new(s)).collect::<Result<_>>()?;
    Ok(GlobSet::new(true, globs)?)
}

struct NewSessionConfig {
    client: ClId,
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    speed: Option<f64>,
    pos: Option<Seek>,
    state: Option<State>,
    play_after: Option<Duration>,
    filter: Vec<Chars>,
}

impl NewSessionConfig {
    fn new(
        mut req: RpcCall,
        start: Value,
        end: Value,
        speed: Value,
        pos: Option<Seek>,
        state: Option<State>,
        play_after: Option<Duration>,
        filter: Vec<Chars>,
    ) -> Option<(NewSessionConfig, RpcReply)> {
        let start = match parse_bound(start) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid start {}", e)),
        };
        let end = match parse_bound(end) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid end {}", e)),
        };
        let speed = match parse_speed(speed) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid speed {}", e)),
        };
        if let Err(e) = parse_filter(filter.clone()) {
            rpc_err!(req.reply, format!("could not parse filter {}", e))
        }
        let s = NewSessionConfig {
            client: req.client,
            start,
            end,
            speed,
            pos,
            state,
            play_after,
            filter,
        };
        Some((s, req.reply))
    }
}

struct Session {
    controls: Controls,
    publisher: Publisher,
    published: FxHashMap<Id, Val>,
    published_ids: FxHashSet<publisher::Id>,
    filter: GlobSet,
    speed: Speed,
    state: Arc<AtomicState>,
    data_base: Path,
    log: LogfileCollection,
    pathindex: ArchiveReader,
    used: bool,
}

impl Session {
    async fn new(
        config: Arc<Config>,
        publisher: Publisher,
        head: Option<ArchiveReader>,
        pathindex: ArchiveReader,
        session_base: Path,
        filter: GlobSet,
        control_tx: &mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    ) -> Result<Self> {
        let controls = Controls::new(&session_base, &publisher, &control_tx).await?;
        let used =
            pathindex.index().iter_pathmap().any(|(_, path)| filter.is_match(path));
        let log =
            LogfileCollection::new(config, head, Bound::Unbounded, Bound::Unbounded)
                .await?;
        Ok(Self {
            controls,
            publisher,
            published: HashMap::default(),
            published_ids: HashSet::default(),
            speed: Speed::Limited {
                rate: 1.,
                last_emitted: DateTime::<Utc>::MIN_UTC,
                last_batch_ts: None,
            },
            filter,
            state: Arc::new(AtomicState::new(State::Pause)),
            data_base: session_base.append("data"),
            log,
            pathindex,
            used,
        })
    }

    async fn next(&mut self) -> Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)> {
        macro_rules! set_tail {
            () => {
                let mut cbatch = self.publisher.start_batch();
                self.set_state(&mut cbatch, State::Tail);
                cbatch.commit(None).await;
                break future::pending().await;
            };
        }
        if !self.used {
            return future::pending().await;
        }
        loop {
            if !self.state.load().play() {
                break future::pending().await;
            } else {
                match &mut self.speed {
                    Speed::Unlimited => match self.log.read_next()? {
                        None => {
                            set_tail!();
                        }
                        Some((ts, batch)) => {
                            self.log.seek(Seek::BatchRelative(1))?;
                            break Ok((ts, batch));
                        }
                    },
                    Speed::Limited { rate, last_emitted, last_batch_ts } => {
                        match self.log.read_next()? {
                            None => {
                                set_tail!();
                            }
                            Some((ts, batch)) => match last_batch_ts {
                                None => {
                                    *last_batch_ts = Some(ts);
                                    *last_emitted = Utc::now();
                                    self.log.seek(Seek::BatchRelative(1))?;
                                    break Ok((ts, batch));
                                }
                                Some(last_ts) => {
                                    let now = Utc::now();
                                    let total_wait = ts - *last_ts;
                                    let since_emitted = now - *last_emitted;
                                    if since_emitted < total_wait {
                                        let naptime = (total_wait - since_emitted)
                                            .to_std()
                                            .unwrap()
                                            .as_secs_f64();
                                        let naptime =
                                            Duration::from_secs_f64(naptime / *rate);
                                        time::sleep(naptime).await;
                                    }
                                    *last_batch_ts = Some(ts);
                                    *last_emitted = Utc::now();
                                    self.log.seek(Seek::BatchRelative(1))?;
                                    break Ok((ts, batch));
                                }
                            },
                        }
                    }
                }
            }
        }
    }

    async fn process_batch(
        &mut self,
        batch: (DateTime<Utc>, &mut Vec<BatchItem>),
    ) -> Result<()> {
        let pause = match self.log.end() {
            Bound::Unbounded => false,
            Bound::Excluded(dt) => &batch.0 >= dt,
            Bound::Included(dt) => &batch.0 > dt,
        };
        if pause {
            let mut cbatch = self.publisher.start_batch();
            self.set_state(&mut cbatch, State::Pause);
            cbatch.commit(None).await;
            Ok(())
        } else {
            let mut pbatch = self.publisher.start_batch();
            task::block_in_place(|| {
                self.pathindex.check_remap_rescan()?;
                let index = self.pathindex.index();
                for BatchItem(id, ev) in batch.1.drain(..) {
                    let v = match ev {
                        Event::Unsubscribed => Value::Null,
                        Event::Update(v) => v,
                    };
                    match self.published.get(&id) {
                        Some(val) => {
                            val.update(&mut pbatch, v);
                        }
                        None => {
                            if let Some(path) = index.path_for_id(&id) {
                                if self.filter.is_match(&path) {
                                    self.used = true;
                                    let path = self.data_base.append(&path);
                                    let val = self.publisher.publish(path, v)?;
                                    self.published_ids.insert(val.id());
                                    self.published.insert(id, val);
                                }
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            })?;
            self.controls.pos_ctl.update(&mut pbatch, batch.0);
            Ok(pbatch.commit(None).await)
        }
    }

    async fn process_bcast(
        &mut self,
        msg: Result<BCastMsg, broadcast::error::RecvError>,
    ) -> Result<()> {
        match msg {
            Err(broadcast::error::RecvError::Closed) => {
                bail!("broadcast channel closed")
            }
            Err(broadcast::error::RecvError::Lagged(missed)) => match self.state.load() {
                State::Play | State::Pause => Ok(()),
                State::Tail => {
                    // safe because it's impossible to get into state
                    // Tail without an archive.
                    let mut batches = self.log.read_deltas(missed as usize)?;
                    for (ts, mut batch) in batches.drain(..) {
                        self.process_batch((ts, &mut *batch)).await?;
                    }
                    Ok(())
                }
            },
            Ok(BCastMsg::Batch(ts, batch)) => match self.state.load() {
                State::Play | State::Pause => Ok(()),
                State::Tail => {
                    if let Some(cursor) = self.log.position() {
                        let pos = cursor.current().unwrap_or(DateTime::<Utc>::MIN_UTC);
                        if cursor.contains(&ts) && pos < ts {
                            let mut batch = (*batch).clone();
                            self.process_batch((ts, &mut batch)).await?;
                            if let Some(cursor) = self.log.position_mut() {
                                cursor.set_current(ts);
                            }
                        }
                    }
                    Ok(())
                }
            },
            Ok(BCastMsg::LogRotated(ts)) => {
                self.log.log_rotated(ts).await?;
                Ok(())
            }
            Ok(BCastMsg::NewCurrent(head)) => {
                self.log.set_head(head);
                Ok(())
            }
            Ok(BCastMsg::Stop) => bail!("stop signal"),
        }
    }

    fn set_start(
        &mut self,
        cbatch: &mut UpdateBatch,
        new_start: Bound<DateTime<Utc>>,
    ) -> Result<()> {
        self.controls.start_ctl.update(cbatch, bound_to_val(new_start));
        self.log.set_start(new_start);
        Ok(())
    }

    fn set_end(
        &mut self,
        cbatch: &mut UpdateBatch,
        new_end: Bound<DateTime<Utc>>,
    ) -> Result<()> {
        self.controls.end_ctl.update(cbatch, bound_to_val(new_end));
        self.log.set_end(new_end);
        Ok(())
    }

    fn set_state(&mut self, cbatch: &mut UpdateBatch, state: State) {
        match (self.state.load(), state) {
            (s0, s1) if s0 == s1 => (),
            (_, state) => {
                self.state.store(state);
                self.controls.state_ctl.update(
                    cbatch,
                    Value::from(match state {
                        State::Play => "play",
                        State::Pause => "pause",
                        State::Tail => "tail",
                    }),
                );
            }
        }
    }

    async fn apply_config(
        &mut self,
        cbatch: &mut UpdateBatch,
        cluster: &Cluster<ClusterCmd>,
        cfg: NewSessionConfig,
    ) -> Result<()> {
        self.set_start(cbatch, cfg.start)?;
        cluster.send_cmd(&ClusterCmd::SetStart(cfg.start));
        self.set_end(cbatch, cfg.end)?;
        cluster.send_cmd(&ClusterCmd::SetEnd(cfg.end));
        self.set_speed(cbatch, cfg.speed);
        cluster.send_cmd(&ClusterCmd::SetSpeed(cfg.speed));
        if let Some(pos) = cfg.pos {
            self.seek(cbatch, pos)?;
            cluster.send_cmd(&ClusterCmd::SeekTo(pos.to_string()));
        }
        if let Some(state) = cfg.state {
            self.set_state(cbatch, state);
            cluster.send_cmd(&ClusterCmd::SetState(state));
        }
        if let Some(play_after) = cfg.play_after {
            time::sleep(play_after).await;
            self.set_state(cbatch, State::Play);
            cluster.send_cmd(&ClusterCmd::SetState(State::Play));
        }
        Ok(())
    }

    async fn process_control_batch(
        &mut self,
        session_id: Uuid,
        cluster: &Cluster<ClusterCmd>,
        mut batch: Pooled<Vec<WriteRequest>>,
    ) -> Result<()> {
        let mut inst = HashMap::new();
        let mut cbatch = self.publisher.start_batch();
        for req in batch.drain(..) {
            inst.insert(req.id, req);
        }
        for (_, req) in inst {
            if req.id == self.controls.start_ctl.id() {
                info!("set start {}: {}", session_id, req.value);
                if let Some(new_start) = get_bound(req) {
                    self.set_start(&mut cbatch, new_start)?;
                    cluster.send_cmd(&ClusterCmd::SetStart(new_start));
                }
            } else if req.id == self.controls.end_ctl.id() {
                info!("set end {}: {}", session_id, req.value);
                if let Some(new_end) = get_bound(req) {
                    self.set_end(&mut cbatch, new_end)?;
                    cluster.send_cmd(&ClusterCmd::SetEnd(new_end));
                }
            } else if req.id == self.controls.speed_ctl.id() {
                info!("set speed {}: {}", session_id, req.value);
                match parse_speed(req.value) {
                    Ok(speed) => {
                        self.set_speed(&mut cbatch, speed);
                        cluster.send_cmd(&ClusterCmd::SetSpeed(speed));
                    }
                    Err(e) => {
                        warn!("tried to set invalid speed {}", e);
                        if let Some(reply) = req.send_result {
                            reply.send(Value::Error(Chars::from(format!("{}", e))));
                        }
                    }
                }
            } else if req.id == self.controls.state_ctl.id() {
                info!("set state {}: {}", session_id, req.value);
                match req.value.cast_to::<State>() {
                    Ok(state) => {
                        match (state, self.state.load()) {
                            (State::Play | State::Tail, State::Tail) => (),
                            (_, _) => self.set_state(&mut cbatch, state),
                        }
                        cluster.send_cmd(&ClusterCmd::SetState(state));
                    }
                    Err(e) => {
                        warn!("tried to set invalid state {}", e);
                        if let Some(reply) = req.send_result {
                            reply.send(Value::Error(Chars::from(format!("{}", e))))
                        }
                    }
                }
            } else if req.id == self.controls.pos_ctl.id() {
                info!("set pos {}: {}", session_id, req.value);
                match req.value.cast_to::<Seek>() {
                    Ok(pos) => {
                        self.seek(&mut cbatch, pos)?;
                        cluster.send_cmd(&ClusterCmd::SeekTo(pos.to_string()));
                    }
                    Err(e) => {
                        warn!("invalid set pos {}", e);
                        if let Some(reply) = req.send_result {
                            reply.send(Value::Error(Chars::from(format!("{}", e))))
                        }
                    }
                }
            }
        }
        Ok(cbatch.commit(None).await)
    }

    fn process_control_cmd(
        &mut self,
        idle: &mut bool,
        cbatch: &mut UpdateBatch,
        cmd: ClusterCmd,
    ) -> Result<()> {
        match cmd {
            ClusterCmd::SeekTo(s) => match s.parse::<Seek>() {
                Ok(pos) => self.seek(cbatch, pos),
                Err(e) => {
                    warn!("invalid seek from cluster {}, {}", s, e);
                    Ok(())
                }
            },
            ClusterCmd::SetStart(new_start) => self.set_start(cbatch, new_start),
            ClusterCmd::SetEnd(new_end) => self.set_end(cbatch, new_end),
            ClusterCmd::SetSpeed(sp) => Ok(self.set_speed(cbatch, sp)),
            ClusterCmd::SetState(st) => Ok(self.set_state(cbatch, st)),
            ClusterCmd::NotIdle => Ok(*idle = false),
        }
    }

    fn reimage(&mut self, pbatch: &mut UpdateBatch) -> Result<()> {
        if self.used {
            let mut idx = self.log.reimage()?;
            if let Some(cursor) = self.log.position() {
                self.controls.pos_ctl.update(
                    pbatch,
                    match cursor.current() {
                        Some(ts) => Value::DateTime(ts),
                        None => match cursor.start() {
                            Bound::Unbounded => Value::Null,
                            Bound::Included(ts) | Bound::Excluded(ts) => {
                                Value::DateTime(ts)
                            }
                        },
                    },
                );
            }
            self.pathindex.check_remap_rescan()?;
            let index = self.pathindex.index();
            for (id, path) in index.iter_pathmap() {
                let v = match idx.remove(id) {
                    None | Some(Event::Unsubscribed) => Value::Null,
                    Some(Event::Update(v)) => v,
                };
                match self.published.get(&id) {
                    Some(val) => val.update(pbatch, v),
                    None => {
                        if self.filter.is_match(&path) {
                            let path = self.data_base.append(path.as_ref());
                            let val = self.publisher.publish(path, v)?;
                            self.published_ids.insert(val.id());
                            self.published.insert(*id, val);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn seek(&mut self, pbatch: &mut UpdateBatch, seek: Seek) -> Result<()> {
        if self.used {
            self.log.seek(seek)?;
        }
        match self.state.load() {
            State::Tail => self.set_state(pbatch, State::Play),
            State::Pause | State::Play => (),
        }
        match &mut self.speed {
            Speed::Unlimited => (),
            Speed::Limited { last_batch_ts, .. } => {
                *last_batch_ts = None;
            }
        };
        self.reimage(pbatch)?;
        Ok(())
    }

    fn set_speed(&mut self, cbatch: &mut UpdateBatch, new_rate: Option<f64>) {
        match new_rate {
            None => {
                self.controls
                    .speed_ctl
                    .update(cbatch, Value::String(Chars::from("unlimited")));
            }
            Some(new_rate) => {
                self.controls.speed_ctl.update(cbatch, Value::F64(new_rate));
            }
        };
        match &mut self.speed {
            Speed::Limited { rate, .. } => match new_rate {
                Some(new_rate) => {
                    *rate = new_rate;
                }
                None => {
                    self.speed = Speed::Unlimited;
                }
            },
            Speed::Unlimited => {
                if let Some(new_rate) = new_rate {
                    self.speed = Speed::Limited {
                        rate: new_rate,
                        last_emitted: DateTime::<Utc>::MIN_UTC,
                        last_batch_ts: None,
                    };
                }
            }
        }
    }
}

fn not_idle(idle: &mut bool, cluster: &Cluster<ClusterCmd>) {
    *idle = false;
    cluster.send_cmd(&ClusterCmd::NotIdle);
}

async fn read_bcast(
    bcast: &mut broadcast::Receiver<BCastMsg>,
    state: &Arc<AtomicState>,
) -> std::result::Result<BCastMsg, broadcast::error::RecvError> {
    loop {
        let m = bcast.recv().await;
        match m {
            Err(e) => break Err(e),
            Ok(m) => match m {
                BCastMsg::Batch(_, _) => match state.load() {
                    State::Pause | State::Play => (),
                    State::Tail => break Ok(m),
                },
                BCastMsg::LogRotated(_) | BCastMsg::NewCurrent(_) | BCastMsg::Stop => {
                    break Ok(m)
                }
            },
        }
    }
}

async fn session(
    mut bcast: broadcast::Receiver<BCastMsg>,
    head: Option<ArchiveReader>,
    pathindex: ArchiveReader,
    subscriber: Subscriber,
    publisher: Publisher,
    session_id: Uuid,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    filter: GlobSet,
    cfg: Option<NewSessionConfig>,
) -> Result<()> {
    let (control_tx, control_rx) = mpsc::channel(3);
    let (events_tx, mut events_rx) = mpsc::unbounded();
    publisher.events(events_tx);
    let session_base = session_base(&publish_config.base, session_id);
    debug!("new session base {}", session_base);
    let mut cluster = Cluster::new(
        &publisher,
        subscriber,
        session_base.append("cluster"),
        publish_config.shards.unwrap_or(0),
    )
    .await?;
    debug!("cluster established");
    pathindex.check_remap_rescan()?;
    head.as_ref().map(|a| a.check_remap_rescan()).transpose()?;
    let mut t = Session::new(
        config.clone(),
        publisher.clone(),
        head,
        pathindex.clone(),
        session_base,
        filter,
        &control_tx,
    )
    .await?;
    let state = t.state.clone();
    let mut batch = publisher.start_batch();
    debug!("seeking to the end");
    t.seek(&mut batch, Seek::End)?;
    if let Some(cfg) = cfg {
        t.apply_config(&mut batch, &cluster, cfg).await?
    }
    batch.commit(None).await;
    let mut control_rx = control_rx.fuse();
    let mut idle_check = time::interval(std::time::Duration::from_secs(30));
    let mut idle = false;
    let mut used = 0;
    loop {
        select_biased! {
            r = t.next().fuse() => match r {
                Ok((ts, mut batch)) => { t.process_batch((ts, &mut *batch)).await?; }
                Err(e) => break Err(e),
            },
            e = events_rx.select_next_some() => match e {
                publisher::Event::Subscribe(id, _) => if t.published_ids.contains(&id) {
                    used += 1;
                },
                publisher::Event::Unsubscribe(id, _) => if t.published_ids.contains(&id) {
                    used -= 1;
                },
                publisher::Event::Destroyed(_) => (),
            },
            _ = idle_check.tick().fuse() => {
                let has_clients = used > 0;
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
            m = read_bcast(&mut bcast, &state).fuse() => t.process_bcast(m).await?,
            cmds = cluster.wait_cmds().fuse() => {
                let mut cbatch = publisher.start_batch();
                for cmd in cmds? {
                    t.process_control_cmd(&mut idle, &mut cbatch, cmd)?
                }
                cbatch.commit(None).await;
            },
            r = control_rx.next() => match r {
                None => break Ok(()),
                Some(batch) => {
                    t.process_control_batch(session_id, &cluster, batch).await?
                }
            },
        }
    }
}

struct SessionIdsInner {
    max_total: usize,
    max_by_client: usize,
    total: usize,
    by_client: FxHashMap<ClId, usize>,
}

#[derive(Clone)]
struct SessionIds(Arc<Mutex<SessionIdsInner>>);

impl SessionIds {
    fn new(max_total: usize, max_by_client: usize) -> Self {
        Self(Arc::new(Mutex::new(SessionIdsInner {
            max_total,
            max_by_client,
            total: 0,
            by_client: HashMap::default(),
        })))
    }

    fn add_session(&self, client: ClId) -> Option<SessionId> {
        let mut inner = self.0.lock();
        let inner = &mut *inner;
        let by_client = inner.by_client.entry(client).or_insert(0);
        if inner.total < inner.max_total && *by_client < inner.max_by_client {
            inner.total += 1;
            *by_client += 1;
            Some(SessionId(self.clone(), client))
        } else {
            None
        }
    }

    fn delete_session(&self, session: &SessionId) {
        let mut inner = self.0.lock();
        if let Some(c) = inner.by_client.get_mut(&session.1) {
            *c -= 1;
            inner.total -= 1;
        }
    }
}

struct SessionId(SessionIds, ClId);

impl Drop for SessionId {
    fn drop(&mut self) {
        self.0.delete_session(self)
    }
}

fn start_session(
    publisher: Publisher,
    session_id: Uuid,
    session_token: SessionId,
    bcast: &broadcast::Sender<BCastMsg>,
    pathindex: ArchiveReader,
    subscriber: &Subscriber,
    head: &Option<ArchiveReader>,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    filter: GlobSet,
    cfg: Option<NewSessionConfig>,
) {
    let bcast = bcast.subscribe();
    let head = head.clone();
    let subscriber = subscriber.clone();
    let publisher_cl = publisher.clone();
    let pathindex = pathindex.clone();
    task::spawn(async move {
        let res = session(
            bcast,
            head,
            pathindex,
            subscriber,
            publisher_cl,
            session_id,
            config.clone(),
            publish_config.clone(),
            filter,
            cfg,
        )
        .await;
        match res {
            Ok(()) => {
                info!("session {} existed", session_id)
            }
            Err(e) => {
                error!("session {} exited {}", session_id, e)
            }
        }
        drop(session_token)
    });
}

pub(super) async fn run(
    bcast: broadcast::Sender<BCastMsg>,
    mut bcast_rx: broadcast::Receiver<BCastMsg>,
    pathindex: ArchiveReader,
    subscriber: Subscriber,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    publisher: Publisher,
) -> Result<()> {
    let sessions = SessionIds::new(
        publish_config.max_sessions,
        publish_config.max_sessions_per_client,
    );
    let (control_tx, control_rx) = mpsc::channel(3);
    let _new_session: Result<Proc> = define_rpc!(
        &publisher,
        publish_config.base.append("session"),
        "create a new playback session",
        NewSessionConfig::new,
        Some(control_tx.clone()),
        start: Value = "Unbounded"; START_DOC,
        end: Value = "Unbounded"; END_DOC,
        speed: Value = "1."; SPEED_DOC,
        pos: Option<Seek> = Value::Null; POS_DOC,
        state: Option<State> = Value::Null; STATE_DOC,
        play_after: Option<Duration> = None::<Duration>; PLAY_AFTER_DOC,
        filter: Vec<Chars> = vec![Chars::from("/**")]; FILTER_DOC
    );
    let _new_session = _new_session?;
    let mut cluster = Cluster::<(ClId, Uuid, Vec<Chars>)>::new(
        &publisher,
        subscriber.clone(),
        publish_config.base.append(&publish_config.cluster).append("publish"),
        publish_config.shards.unwrap_or(0),
    )
    .await?;
    let mut control_rx = control_rx.fuse();
    let mut poll_members = time::interval(std::time::Duration::from_secs(30));
    let mut archive = None;
    loop {
        select_biased! {
            m = bcast_rx.recv().fuse() => match m {
                Err(_) => (),
                Ok(m) => match m {
                    BCastMsg::Batch(_, _) | BCastMsg::LogRotated(_) => (),
                    BCastMsg::NewCurrent(rdr) => archive = Some(rdr),
                    BCastMsg::Stop => break Ok(())
                }
            },
            _ = poll_members.tick().fuse() => {
                if let Err(e) = cluster.poll_members().await {
                    warn!("failed to poll cluster members, will retry {}", e)
                }
            },
            cmds = cluster.wait_cmds().fuse() => match cmds {
                Err(e) => {
                    error!("received unparsable cluster commands {}", e)
                }
                Ok(cmds) => for (client, session_id, filter) in cmds {
                    let filter = match parse_filter(filter) {
                        Ok(filter) => filter,
                        Err(e) => {
                            error!("can't parse filter from cluster {}", e);
                            continue
                        }
                    };
                    match sessions.add_session(client) {
                        None => {
                            error!("can't start session requested by cluster member, too many sessions")
                        },
                        Some(session_token) => {
                            start_session(
                                publisher.clone(),
                                session_id,
                                session_token,
                                &bcast,
                                pathindex.clone(),
                                &subscriber,
                                &archive,
                                config.clone(),
                                publish_config.clone(),
                                filter,
                                None
                            );
                        }
                    }
                }
            },
            m = control_rx.next() => match m {
                None => break Ok(()),
                Some((cfg, mut reply)) => {
                    match sessions.add_session(cfg.client) {
                        None => {
                            let m = format!("too many sessions, client {:?}", cfg.client);
                            reply.send(Value::Error(Chars::from(m)));
                        },
                        Some(session_token) => {
                            let filter_txt = cfg.filter.clone();
                            let filter = match parse_filter(cfg.filter.clone()) {
                                Ok(filter) => filter,
                                Err(e) => {
                                    warn!("failed to parse filters {}", e);
                                    continue
                                }
                            };
                            let session_id = Uuid::new_v4();
                            let client = cfg.client;
                            info!("start session {}", session_id);
                            start_session(
                                publisher.clone(),
                                session_id,
                                session_token,
                                &bcast,
                                pathindex.clone(),
                                &subscriber,
                                &archive,
                                config.clone(),
                                publish_config.clone(),
                                filter,
                                Some(cfg)
                            );
                            cluster.send_cmd(&(client, session_id, filter_txt));
                            reply.send(Value::from(uuid_string(session_id)));
                        }
                    }
                }
            },
        }
    }
}
