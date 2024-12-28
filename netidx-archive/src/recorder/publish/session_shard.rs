use super::{
    controls::NewSessionConfig, AtomicState, ClusterCmd, SessionBCastMsg, SessionUpdate,
    Speed,
};
use crate::{
    config::Config,
    logfile::{ArchiveReader, BatchItem, Id, Seek},
    logfile_collection::index::ArchiveIndex,
    logfile_collection::reader::ArchiveCollectionReader,
    recorder::{oneshot::FILTER, BCastMsg, ShardId, Shards, State},
};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{
    channel::mpsc::{self, UnboundedReceiver},
    future,
    prelude::*,
    select_biased,
};
use fxhash::{FxHashMap, FxHashSet};
use log::{debug, warn};
use netidx::{
    path::Path,
    pool::Pooled,
    publisher::{self, Publisher, UpdateBatch, Val, Value},
    resolver_client::GlobSet,
    subscriber::Event,
};
use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::broadcast, task};
use lltimer as time;

pub(super) struct SessionShard {
    shards: Arc<Shards>,
    id: ShardId,
    publisher: Publisher,
    published: FxHashMap<Id, Val>,
    published_ids: FxHashSet<publisher::Id>,
    session_bcast: broadcast::Sender<SessionBCastMsg>,
    filter: GlobSet,
    filterset: Pooled<FxHashSet<Id>>,
    speed: Speed,
    state: Arc<AtomicState>,
    data_base: Path,
    log: ArchiveCollectionReader,
    pathindex: ArchiveReader,
    used: bool,
    events_rx: Option<UnboundedReceiver<netidx::publisher::Event>>,
}

impl SessionShard {
    pub(super) async fn new(
        shards: Arc<Shards>,
        session_bcast: broadcast::Sender<SessionBCastMsg>,
        shard_id: ShardId,
        shard: ArcStr,
        config: Arc<Config>,
        publisher: Publisher,
        head: Option<ArchiveReader>,
        index: ArchiveIndex,
        pathindex: ArchiveReader,
        session_base: Path,
        filter: GlobSet,
        cfg: Option<NewSessionConfig>,
    ) -> Result<Self> {
        task::block_in_place(|| {
            pathindex.check_remap_rescan(false)?;
            head.as_ref().map(|a| a.check_remap_rescan(false)).transpose()?;
            Ok::<_, anyhow::Error>(())
        })?;
        let mut filterset = FILTER.take();
        filterset.extend(pathindex.index().iter_pathmap().filter_map(|(id, path)| {
            if filter.is_match(path) {
                Some(*id)
            } else {
                None
            }
        }));
        let used =
            pathindex.index().iter_pathmap().any(|(_, path)| filter.is_match(path));
        let log = ArchiveCollectionReader::new(
            index,
            config,
            shard,
            head,
            Bound::Unbounded,
            Bound::Unbounded,
        );
        let (events_tx, events_rx) = mpsc::unbounded();
        let mut t = Self {
            shards,
            id: shard_id,
            session_bcast,
            publisher,
            published: HashMap::default(),
            published_ids: HashSet::default(),
            speed: Speed::Limited {
                rate: 1.,
                last_emitted: DateTime::<Utc>::MIN_UTC,
                last_batch_ts: None,
            },
            filter,
            filterset,
            state: Arc::new(AtomicState::new(State::Pause)),
            data_base: session_base.append("data"),
            log,
            pathindex,
            used,
            events_rx: Some(events_rx),
        };
        t.publisher.events(events_tx);
        task::block_in_place(|| t.pathindex.check_remap_rescan(false))?;
        let mut batch = t.publisher.start_batch();
        debug!("seeking to the end");
        t.seek(&mut batch, Seek::End)?;
        if let Some(cfg) = cfg.clone() {
            t.apply_config(&mut batch, cfg).await?
        }
        batch.commit(None).await;
        t.publisher.flushed().await;
        Ok(t)
    }

    async fn next(&mut self) -> Result<(DateTime<Utc>, Pooled<Vec<BatchItem>>)> {
        macro_rules! set_tail {
            () => {
                let _ = self
                    .session_bcast
                    .send(SessionBCastMsg::Update(SessionUpdate::State(State::Tail)));
                self.set_state(State::Tail);
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
                    Speed::Unlimited => {
                        match self.log.read_next(Some(&self.filterset))? {
                            None => {
                                set_tail!();
                            }
                            Some((ts, batch)) => {
                                self.log.seek(Seek::Absolute(ts))?;
                                break Ok((ts, batch));
                            }
                        }
                    }
                    Speed::Limited { rate, last_emitted, last_batch_ts } => {
                        match self.log.read_next(Some(&self.filterset))? {
                            None => {
                                set_tail!();
                            }
                            Some((ts, batch)) => match last_batch_ts {
                                None => {
                                    *last_batch_ts = Some(ts);
                                    *last_emitted = Utc::now();
                                    self.log.seek(Seek::Absolute(ts))?;
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
                                    self.log.seek(Seek::Absolute(ts))?;
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
            let _ = self
                .session_bcast
                .send(SessionBCastMsg::Update(SessionUpdate::State(State::Pause)));
            self.set_state(State::Pause);
            Ok(())
        } else {
            let mut pbatch = self.publisher.start_batch();
            task::block_in_place(|| {
                self.pathindex.check_remap_rescan(false)?;
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
            let _ = self
                .session_bcast
                .send(SessionBCastMsg::Update(SessionUpdate::Pos(Some(batch.0))));
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
                    let (_, mut batches) =
                        self.log.read_deltas(Some(&self.filterset), missed as usize)?;
                    for (ts, mut batch) in batches.drain(..) {
                        self.process_batch((ts, &mut *batch)).await?;
                    }
                    Ok(())
                }
            },
            Ok(BCastMsg::Batch(ts, batch)) => match self.state.load() {
                State::Play | State::Pause => Ok(()),
                State::Tail => {
                    let cursor = self.log.position();
                    if cursor.contains(&ts) {
                        let mut batch = (*batch).clone();
                        self.process_batch((ts, &mut batch)).await?;
                        let cursor = self.log.position_mut();
                        cursor.set_current(ts);
                    }
                    Ok(())
                }
            },
            Ok(BCastMsg::TailInvalidated) => {
                if self.state.load() == State::Tail {
                    let _ = self
                        .session_bcast
                        .send(SessionBCastMsg::Update(SessionUpdate::State(State::Play)));
                    self.set_state(State::Play);
                }
                Ok(())
            }
            Ok(BCastMsg::LogRotated(ts)) => {
                let index = self.shards.indexes.read()[&self.id].clone();
                self.log.log_rotated(ts, index);
                Ok(())
            }
            Ok(BCastMsg::NewCurrent(head)) => {
                self.log.set_head(head);
                Ok(())
            }
        }
    }

    async fn process_session_bcast(
        &mut self,
        idle: &mut bool,
        batch: &mut UpdateBatch,
        m: Result<SessionBCastMsg, broadcast::error::RecvError>,
    ) -> Result<()> {
        match m {
            Err(broadcast::error::RecvError::Closed) => bail!("closed"),
            Err(broadcast::error::RecvError::Lagged(_)) => warn!("shard missed messages"),
            Ok(SessionBCastMsg::Update(_)) => (),
            Ok(SessionBCastMsg::Command(c)) => match c {
                ClusterCmd::NotIdle => {
                    *idle = false;
                }
                ClusterCmd::SeekTo(s) => self.seek(batch, s)?,
                ClusterCmd::SetEnd(e) => self.set_end(e)?,
                ClusterCmd::SetStart(s) => self.set_start(s)?,
                ClusterCmd::SetState(s) => self.set_state(s),
                ClusterCmd::SetSpeed(s) => self.set_speed(s),
                ClusterCmd::Terminate => bail!("terminated"),
            },
        }
        Ok(())
    }

    fn set_start(&mut self, new_start: Bound<DateTime<Utc>>) -> Result<()> {
        self.log.set_start(new_start);
        let _ = self
            .session_bcast
            .send(SessionBCastMsg::Update(SessionUpdate::Start(new_start)));
        Ok(())
    }

    fn set_end(&mut self, new_end: Bound<DateTime<Utc>>) -> Result<()> {
        self.log.set_end(new_end);
        let _ =
            self.session_bcast.send(SessionBCastMsg::Update(SessionUpdate::End(new_end)));
        Ok(())
    }

    fn set_state(&mut self, state: State) {
        match (self.state.load(), state) {
            (s0, s1) if s0 == s1 => (),
            (_, state) => {
                self.state.store(state);
                let _ = self
                    .session_bcast
                    .send(SessionBCastMsg::Update(SessionUpdate::State(state)));
            }
        }
    }

    async fn apply_config(
        &mut self,
        pbatch: &mut UpdateBatch,
        cfg: NewSessionConfig,
    ) -> Result<()> {
        self.set_start(cfg.start)?;
        self.set_end(cfg.end)?;
        self.set_speed(cfg.speed);
        if let Some(pos) = cfg.pos {
            self.seek(pbatch, pos)?;
        }
        if let Some(state) = cfg.state {
            self.set_state(state);
        }
        if let Some(play_after) = cfg.play_after {
            time::sleep(play_after).await;
            self.set_state(State::Play);
        }
        Ok(())
    }

    fn reimage(&mut self, pbatch: &mut UpdateBatch) -> Result<()> {
        if self.used {
            let mut idx = self.log.reimage(Some(&self.filterset))?;
            let cursor = self.log.position();
            let _ = self.session_bcast.send(SessionBCastMsg::Update(SessionUpdate::Pos(
                match cursor.current() {
                    Some(ts) => Some(ts),
                    None => match cursor.start() {
                        Bound::Unbounded => None,
                        Bound::Included(ts) | Bound::Excluded(ts) => Some(*ts),
                    },
                },
            )));
            self.pathindex.check_remap_rescan(false)?;
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
            State::Tail => self.set_state(State::Play),
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

    fn set_speed(&mut self, new_rate: Option<f64>) {
        let _ = self
            .session_bcast
            .send(SessionBCastMsg::Update(SessionUpdate::Speed(new_rate)));
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

    pub(super) async fn run(
        mut self,
        mut bcast: broadcast::Receiver<BCastMsg>,
        mut session_bcast_rx: broadcast::Receiver<SessionBCastMsg>,
    ) -> Result<()> {
        async fn read_bcast(
            bcast: &mut broadcast::Receiver<BCastMsg>,
            state: &Arc<AtomicState>,
        ) -> std::result::Result<BCastMsg, broadcast::error::RecvError> {
            loop {
                let m = bcast.recv().await;
                match m {
                    Err(e) => break Err(e),
                    Ok(m) => match m {
                        BCastMsg::Batch(_, _) | BCastMsg::TailInvalidated => {
                            match state.load() {
                                State::Pause | State::Play => (),
                                State::Tail => break Ok(m),
                            }
                        }
                        BCastMsg::LogRotated(_) | BCastMsg::NewCurrent(_) => break Ok(m),
                    },
                }
            }
        }
        let mut events_rx =
            self.events_rx.take().ok_or_else(|| anyhow!("missing events_rx"))?;
        let state = self.state.clone();
        let mut idle_check = time::interval(std::time::Duration::from_secs(30));
        let mut idle = false;
        let mut used = 0;
        loop {
            select_biased! {
                r = self.next().fuse() => match r {
                    Ok((ts, mut batch)) => {
                        self.process_batch((ts, &mut *batch)).await?;
                    }
                    Err(e) => break Err(e),
                },
                m = read_bcast(&mut bcast, &state).fuse() => {
                    self.process_bcast(m).await?
                },
                m = session_bcast_rx.recv().fuse() => {
                    let mut batch = self.publisher.start_batch();
                    self.process_session_bcast(&mut idle, &mut batch, m).await?;
                    batch.commit(None).await
                }
                e = events_rx.select_next_some() => match e {
                    publisher::Event::Subscribe(id, _) => if self.published_ids.contains(&id) {
                        used += 1;
                    },
                    publisher::Event::Unsubscribe(id, _) => if self.published_ids.contains(&id) {
                        used -= 1;
                    },
                    publisher::Event::Destroyed(_) => (),
                },
                _ = idle_check.tick().fuse() => {
                    let has_clients = used > 0;
                    if !has_clients && idle {
                        break Ok(())
                    } else if has_clients {
                        idle = false;
                        let _ = self.session_bcast.send(
                            SessionBCastMsg::Command(ClusterCmd::NotIdle)
                        );
                    } else {
                        idle = true;
                    }
                },
            }
        }
    }
}
