use crate::{
    auth::{Permissions, UserInfo, ANONYMOUS},
    channel::Channel,
    chars::Chars,
    config,
    os::{Krb5ServerCtx, ServerCtx},
    pack::Pack,
    path::Path,
    pool::{Pool, Pooled},
    protocol::{
        publisher,
        resolver::v1::{
            ClientAuthRead, ClientAuthWrite, ClientHello, ClientHelloWrite, CtxId,
            FromRead, FromWrite, ReadyForOwnershipCheck, Resolved, Secret,
            ServerAuthWrite, ServerHelloRead, ServerHelloWrite, Table, ToRead, ToWrite,
        },
    },
    resolver_store,
    secstore::SecStore,
    utils,
};
use anyhow::Result;
use bytes::{Buf, Bytes};
use futures::{prelude::*, select};
use fxhash::FxBuildHasher;
use log::{debug, info, warn};
use parking_lot::{Mutex, RwLockWriteGuard};
use std::{
    collections::{HashMap, HashSet},
    mem,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot,
    },
    task,
    time::{self, Instant},
};

type ReadB = Vec<(u64, ToRead)>;
type ReadR = Vec<(u64, FromRead)>;
type WriteB = Vec<(u64, ToWrite)>;
type WriteR = Vec<(u64, FromWrite)>;

lazy_static! {
    static ref TO_READ_POOL: Pool<ReadB> = Pool::new(640);
    static ref FROM_READ_POOL: Pool<ReadR> = Pool::new(640);
    static ref TO_WRITE_POOL: Pool<WriteB> = Pool::new(640);
    static ref FROM_WRITE_POOL: Pool<WriteR> = Pool::new(640);
}

struct ReadRequest {
    uifo: Arc<UserInfo>,
    id: SocketAddr,
    batch: Pooled<ReadB>,
}

struct WriteRequest {
    uifo: Arc<UserInfo>,
    write_addr: SocketAddr,
    batch: Pooled<WriteB>,
}

#[derive(Clone)]
struct Shard {
    read: UnboundedSender<(Pooled<ReadRequest>, oneshot::Receiver<Pooled<ReadR>>)>,
    write: UnboundedSender<(Pooled<WriteRequest>, oneshot::Receiver<Pooled<WriteR>>)>,
}

impl Shard {
    fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
        secstore: SecStore,
    ) -> Self {
        let (read, read_rx) = unbounded_channel();
        let (write, write_rx) = unbounded_channel();
        task::spawn(async move {
            let mut store = resolver_store::Store::new(parent, children);
            loop {
                select! {
                    batch = read_rx.next() => match batch {
                        None => break,
                        Some((req, reply)) => {
                            let _ = reply.send(
                                Shard::process_read_batch(&mut store, &secstore, req)
                            );
                        }
                    }
                    batch = write_rx.next() => match batch {
                        None => break,
                        Some((req, reply)) => {
                            let _ = reply.send(
                                Shard::process_write_batch(&mut store, &secstore, req)
                            );
                        }
                    }
                }
            }
        });
        Shard { read, write }
    }

    fn process_read_batch(
        store: &mut resolver_store::Store,
        secstore: &SecStore,
        req: ReadRequest,
    ) -> Pooled<ReadR> {
        // things would need to be massively screwed for this to fail
        let now =
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let mut resp = FROM_READ_POOL.take();
        let sec = secstore.map(|s| s.store.read());
        resp.extend(req.batch.drain(..).map(|(id, m)| match m {
            ToRead::Resolve(path) => {
                if let Some(r) = store.check_referral(&path) {
                    (id, FromRead::Referral(r))
                } else {
                    match secstore {
                        None => {
                            let a = Resolved {
                                krb5_spns: Pooled::orphan(HashMap::with_hasher(
                                    FxBuildHasher::default(),
                                )),
                                resolver: id,
                                addrs: store.resolve(&path),
                                timestamp: now,
                                permissions: Permissions::all().bits(),
                            };
                            (id, FromRead::Resolved(a))
                        }
                        Some(ref secstore) => {
                            let perm = secstore.pmap().permissions(&*path, &*req.uifo);
                            if !perm.contains(Permissions::SUBSCRIBE) {
                                (id, FromRead::Denied);
                            } else {
                                let (krb5_spns, addrs) = store.resolve_and_sign(
                                    &**sec.as_ref().unwrap(),
                                    now,
                                    perm,
                                    &path,
                                );
                                let a = Resolved {
                                    krb5_spns,
                                    resolver: id,
                                    addrs,
                                    timestamp: now,
                                    permissions: perm.bits(),
                                };
                                (id, FromRead::Resolved(a))
                            }
                        }
                    }
                }
            }
            ToRead::List(path) => {
                if let Some(r) = store.check_referral(&path) {
                    (id, FromRead::Referral(r))
                } else {
                    let allowed = secstore
                        .map(|s| s.pmap().allowed(&*path, Permissions::LIST, &*req.uifo))
                        .unwrap_or(true);
                    if allowed {
                        (id, FromRead::List(store.list(&path)))
                    } else {
                        (id, FromRead::Denied)
                    }
                }
            }
            ToRead::Table(path) => {
                if let Some(r) = store.check_referral(&path) {
                    (id, FromRead::Referral(r))
                } else {
                    let allowed = secstore
                        .map(|s| s.pmap().allowed(&*path, Permissions::LIST, &*req.uifo))
                        .unwrap_or(true);
                    if !allowed {
                        (id, FromRead::Denied);
                    } else {
                        let rows = store.list(&path);
                        let cols = store.columns(&path);
                        (id, FromRead::Table(Table { rows, cols }));
                    }
                }
            }
        }));
        resp
    }

    fn process_write_batch(
        store: &mut resolver_store::Store,
        secstore: &SecStore,
        req: WriteRequest,
    ) -> Pooled<WriteR> {
        let uinfo = &*req.uinfo;
        let publish = |s: &mut resolver_store::Store,
                       path: Path,
                       default: bool|
         -> FromWrite {
            if !Path::is_absolute(&*path) {
                FromWrite::Error("absolute paths required".into())
            } else if let Some(r) = s.check_referral(&path) {
                FromWrite::Referral(r)
            } else {
                let perm = if default {
                    Permissions::PUBLISH_DEFAULT
                } else {
                    Permissions::PUBLISH
                };
                if secstore.map(|s| s.pmap().allowed(&*path, perm, uinfo)).unwrap_or(true)
                {
                    s.publish(path, write_addr, default);
                    FromWrite::Published
                } else {
                    FromWrite::Denied
                }
            }
        };
        let resp = FROM_WRITE_POOL.take();
        resp.extend(req.batch.drain(..).filter_map(|(id, m)| match m {
            ToWrite::Heartbeat | ToWrite::Clear => None,
            ToWrite::Publish(path) => Some((id, publish(store, path, false))),
            ToWrite::PublishDefault(path) => Some((id, publish(s, path, true))),
            ToWrite::Unpublish(path) => {
                if !Path::is_absolute(&*path) {
                    Some((id, FromWrite::Error("absolute paths required".into())))
                } else if let Some(r) = s.check_referral(&path) {
                    Some((id, FromWrite::Referral(r)))
                } else {
                    store.unpublish(path, write_addr);
                    Some((id, FromWrite::Unpublished))
                }
            }
        }));
        resp
    }
}

#[derive(Clone)]
struct Store(Vec<Shard>);

impl Store {
}
