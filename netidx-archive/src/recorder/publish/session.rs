use super::{ClusterCmd, NewSessionConfig, SessionBCastMsg};
use crate::{
    config::{Config, PublishConfig},
    recorder::{
        publish::{controls::Controls, session_shard::SessionShard},
        Shards,
    },
};
use anyhow::Result;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use log::{debug, error, info, warn};
use netidx::{
    path::Path,
    publisher::{Publisher, WriteRequest},
    resolver_client::GlobSet,
    subscriber::Subscriber,
};
use netidx_protocols::cluster::Cluster;
use poolshark::Pooled;
use smallvec::{smallvec, SmallVec};
use std::sync::Arc;
use tokio::{sync::broadcast, task::JoinSet};
use uuid::Uuid;

fn session_base(publish_base: &Path, id: Uuid) -> Path {
    use uuid::fmt::Simple;
    let mut buf = [0u8; Simple::LENGTH];
    publish_base.append(Simple::from_uuid(id).encode_lower(&mut buf))
}

pub(super) struct Session {
    cluster: Cluster<ClusterCmd>,
    controls: Controls,
    shard_tasks: JoinSet<Result<()>>,
    control_rx: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    session_bcast: broadcast::Sender<SessionBCastMsg>,
    session_bcast_rx: broadcast::Receiver<SessionBCastMsg>,
    session_id: Uuid,
    publisher: Publisher,
}

impl Session {
    pub(super) async fn new(
        shards: Arc<Shards>,
        subscriber: Subscriber,
        publisher: Publisher,
        session_id: Uuid,
        config: Arc<Config>,
        publish_config: Arc<PublishConfig>,
        filter: GlobSet,
        cfg: Option<NewSessionConfig>,
    ) -> Result<Self> {
        let (control_tx, control_rx) = mpsc::channel(3);
        let (session_bcast, session_bcast_rx) = broadcast::channel(1000);
        let session_base = session_base(&publish_config.base, session_id);
        debug!("new session base {}", session_base);
        let cluster = Cluster::new(
            &publisher,
            subscriber,
            session_base.append("cluster"),
            publish_config.cluster_shards.unwrap_or(0),
        )
        .await?;
        debug!("cluster established");
        let controls = Controls::new(&session_base, &publisher, &control_tx).await?;
        let mut shard_tasks: JoinSet<Result<()>> = JoinSet::new();
        let mut shard_init: SmallVec<
            [futures::channel::oneshot::Receiver<Result<()>>; 8],
        > = smallvec![];
        for (id, pathindex) in shards.pathindexes.iter() {
            info!("starting shard {id:?} of session {session_id:?}");
            if let Some(spec) = shards.spec.get(id) {
                if !spec.is_empty() && filter.disjoint(spec) {
                    info!("shard {id:?} of session {session_id:?} is disjoint skipping");
                    continue;
                }
            }
            let shards = shards.clone();
            let id = *id;
            let pathindex = pathindex.clone();
            let publisher = publisher.clone();
            let shard = shards.by_id[&id].clone();
            let config = config.clone();
            let session_bcast = session_bcast.clone();
            let session_bcast_rx = session_bcast.subscribe();
            let bcast = shards.bcast[&id].subscribe();
            let head = shards.heads.read().get(&id).cloned();
            let index = shards.indexes.read()[&id].clone();
            let session_base = session_base.clone();
            let filter = filter.clone();
            let cfg = cfg.clone();
            let (tx_init, rx_init) = futures::channel::oneshot::channel();
            shard_init.push(rx_init);
            shard_tasks.spawn(async move {
                let t = SessionShard::new(
                    shards,
                    session_bcast.clone(),
                    id,
                    shard,
                    config,
                    publisher,
                    head,
                    index,
                    pathindex,
                    session_base,
                    filter,
                    cfg,
                )
                .await;
                match t {
                    Ok(t) => {
                        debug!("session init complete for shard {id:?} of session {session_id:?}");
                        let _ = tx_init.send(Ok(()));
                        match t.run(bcast, session_bcast_rx).await {
                            Ok(()) => {
                                info!("shard {id:?} of session {session_id:?} shutting down");
                                Ok(())
                            }
                            Err(e) => {
                                warn!("shard {id:?} of session {session_id:?} shutting down on error {e:?}");
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        warn!("session init failed for shard {id:?} of session {session_id:?} {e:?}");
                        let _ = tx_init.send(Err(e));
                        Ok::<(), anyhow::Error>(())
                    }
                }
            });
        }
        future::try_join_all(shard_init)
            .await
            .map_err(|_| anyhow!("shard task died before yielding a result"))?
            .into_iter()
            .collect::<Result<Vec<()>>>()?;
        Ok(Self {
            cluster,
            controls,
            shard_tasks,
            control_rx,
            session_bcast,
            session_bcast_rx,
            session_id,
            publisher,
        })
    }

    pub(super) async fn run(self) -> Result<()> {
        let Self {
            mut cluster,
            controls,
            mut shard_tasks,
            mut control_rx,
            mut session_bcast,
            mut session_bcast_rx,
            session_id,
            publisher,
        } = self;
        loop {
            select_biased! {
                r = control_rx.next() => match r {
                    None => {
                        info!("control channel dropped, exiting session {session_id:?}");
                        break Ok(())
                    },
                    Some(batch) => {
                        controls.process_writes(&mut session_bcast, session_id, batch)
                    }
                },
                cmds = cluster.wait_cmds().fuse() => {
                    for cmd in cmds? {
                        let _ = session_bcast.send(SessionBCastMsg::Command(cmd));
                    }
                },
                mut m = session_bcast_rx.recv().fuse() => {
                    let mut batch = publisher.start_batch();
                    loop {
                        match m {
                            Ok(m) => match m {
                                SessionBCastMsg::Command(c) => cluster.send_cmd(&c),
                                SessionBCastMsg::Update(u) => controls.process_update(&mut batch, u),
                            }
                            Err(e) => error!("session bcast for session {} error {}", session_id, e),
                        }
                        match session_bcast_rx.try_recv() {
                            Ok(new_m) => { m = Ok(new_m); }
                            Err(_) => {
                                batch.commit(None).await;
                                break
                            },
                        }
                    }
                }
                r = shard_tasks.join_next().fuse() => match r {
                    Some(Ok(Ok(()))) => (),
                    None => {
                        info!("session id {} shutting down", session_id);
                        break Ok(())
                    }
                    Some(Ok(Err(e))) => {
                        error!("shard of session {} failed {}", session_id, e);
                        let _ = session_bcast.send(SessionBCastMsg::Command(ClusterCmd::Terminate));
                    }
                    Some(Err(e)) => {
                        error!("session {} failed to join shard {}", session_id, e);
                        bail!("join error")
                    }
                },
            }
        }
    }
}
