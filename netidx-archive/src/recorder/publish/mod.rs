pub(crate) mod controls;
mod session;
mod session_shard;

use crate::{
    config::{Config, PublishConfig},
    logfile::Seek,
    recorder::{
        publish::{controls::parse_filter, session::Session},
        Shards, State,
    },
};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use controls::{
    NewSessionConfig, END_DOC, FILTER_DOC, PLAY_AFTER_DOC, POS_DOC, SPEED_DOC, START_DOC,
    STATE_DOC,
};
use futures::{channel::mpsc, prelude::*, select_biased};
use fxhash::FxHashMap;
use log::{error, info, warn};
use netidx::{
    chars::Chars,
    publisher::{ClId, Publisher, Value},
    resolver_client::GlobSet,
    subscriber::Subscriber,
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
    collections::HashMap,
    ops::Bound,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{task, time};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Pack)]
enum ClusterCmd {
    NotIdle,
    SeekTo(Seek),
    SetStart(Bound<DateTime<Utc>>),
    SetEnd(Bound<DateTime<Utc>>),
    SetSpeed(Option<f64>),
    SetState(State),
    Terminate,
}

#[derive(Debug, Clone, Copy)]
enum SessionUpdate {
    Pos(Option<DateTime<Utc>>),
    Start(Bound<DateTime<Utc>>),
    End(Bound<DateTime<Utc>>),
    Speed(Option<f64>),
    State(State),
}

#[derive(Debug, Clone, Copy)]
enum SessionBCastMsg {
    Command(ClusterCmd),
    Update(SessionUpdate),
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

#[derive(Debug)]
enum Speed {
    Unlimited,
    Limited {
        rate: f64,
        last_emitted: DateTime<Utc>,
        last_batch_ts: Option<DateTime<Utc>>,
    },
}

#[derive(Debug, Clone)]
enum ExternalControl {
    Reindex,
    RemapRescan(Option<DateTime<Utc>>),
    Reopen(Option<DateTime<Utc>>),
}

impl ExternalControl {
    fn reindex(req: RpcCall) -> Option<(ExternalControl, RpcReply)> {
        Some((ExternalControl::Reindex, req.reply))
    }

    fn remap_rescan(mut req: RpcCall, ts: Value) -> Option<(ExternalControl, RpcReply)> {
        match ts {
            Value::DateTime(ts) => {
                Some((ExternalControl::RemapRescan(Some(ts)), req.reply))
            }
            Value::Null => Some((ExternalControl::RemapRescan(None), req.reply)),
            _ => rpc_err!(req.reply, "invalid timestamp"),
        }
    }

    fn reopen(mut req: RpcCall, ts: Value) -> Option<(ExternalControl, RpcReply)> {
        match ts {
            Value::DateTime(ts) => Some((ExternalControl::Reopen(Some(ts)), req.reply)),
            Value::Null => Some((ExternalControl::Reopen(None), req.reply)),
            _ => rpc_err!(req.reply, "invalid timestamp"),
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
    shards: Arc<Shards>,
    subscriber: &Subscriber,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    filter: GlobSet,
    cfg: Option<NewSessionConfig>,
    reply: Option<RpcReply>,
) {
    let subscriber = subscriber.clone();
    let publisher_cl = publisher.clone();
    task::spawn(async move {
        let session = Session::new(
            shards,
            subscriber,
            publisher_cl,
            session_id,
            config.clone(),
            publish_config.clone(),
            filter,
            cfg,
        )
        .await;
        match session {
            Err(e) => {
                error!("session {} initialization failed {e:?}", session_id);
                if let Some(mut reply) = reply {
                    let m = Value::Error(format!("initialization failed {e:?}").into());
                    reply.send(m)
                }
            }
            Ok(session) => {
                info!("session {} initialized", session_id);
                if let Some(mut reply) = reply {
                    reply.send(Value::from(uuid_string(session_id)))
                }
                match session.run().await {
                    Ok(()) => {
                        info!("session {} exited", session_id)
                    }
                    Err(e) => {
                        error!("session {} exited {}", session_id, e)
                    }
                }
            }
        }
        drop(session_token)
    });
}

pub(super) async fn run(
    shards: Arc<Shards>,
    subscriber: Subscriber,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    publisher: Publisher,
    init: futures::channel::oneshot::Sender<()>,
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
        publish_config.cluster_shards.unwrap_or(0),
    )
    .await?;
    let mut control_rx = control_rx.fuse();
    let (ecm_tx, ecm_rx) = mpsc::channel(10);
    let _reindex = Proc::new(
        &publisher,
        publish_config.base.append("reindex"),
        "external control, reindex archive file".into(),
        [] as [ArgSpec; 0],
        ExternalControl::reindex,
        Some(ecm_tx.clone()),
    )?;
    let _remap_rescan: Proc = define_rpc!(
        &publisher,
        publish_config.base.append("remap-rescan"),
        "external control, remap/rescan archive file",
        ExternalControl::remap_rescan,
        Some(ecm_tx.clone()),
        ts: Value = Value::Null; "timestamp of historical file to remap/rescan, null for head"
    )?;
    let _reopen: Proc = define_rpc!(
        &publisher,
        publish_config.base.append("reopen"),
        "external control, reopen archive file",
        ExternalControl::reopen,
        Some(ecm_tx.clone()),
        ts: Value = Value::Null; "timestamp of historical file to reopen, null for head"
    )?;
    let mut ecm_rx = ecm_rx.fuse();
    let ecm_reply = |res: Result<()>, mut reply: RpcReply| match res {
        Ok(()) => reply.send(Value::Ok),
        Err(e) => reply.send(Value::Error(e.to_string().into())),
    };
    publisher.flushed().await;
    let _ = init.send(());
    let mut poll_members = time::interval(std::time::Duration::from_secs(30));
    loop {
        select_biased! {
            _ = poll_members.tick().fuse() => {
                if let Err(e) = cluster.poll_members().await {
                    warn!("failed to poll cluster members, will retry {}", e)
                }
            },
            m = ecm_rx.next() => match m {
                None => break Ok(()),
                Some((ExternalControl::Reindex, reply)) =>
                    ecm_reply(shards.reindex(&config), reply),
                Some((ExternalControl::RemapRescan(ts), reply)) =>
                    ecm_reply(shards.remap_rescan(ts), reply),
                Some((ExternalControl::Reopen(ts), reply)) =>
                    ecm_reply(shards.reopen(ts), reply)
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
                                shards.clone(),
                                &subscriber,
                                config.clone(),
                                publish_config.clone(),
                                filter,
                                None,
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
                                shards.clone(),
                                &subscriber,
                                config.clone(),
                                publish_config.clone(),
                                filter,
                                Some(cfg),
                                Some(reply)
                            );
                            cluster.send_cmd(&(client, session_id, filter_txt));
                        }
                    }
                }
            },
        }
    }
}
