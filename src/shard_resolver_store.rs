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

lazy_static! {
    static ref TO_READ_POOL: Pool<Vec<(u64, ToRead)>> = Pool::new(640);
    static ref FROM_READ_POOL: Pool<Vec<(u64, FromRead)>> = Pool::new(640);
    static ref TO_WRITE_POOL: Pool<Vec<(u64, ToWrite)>> = Pool::new(640);
    static ref FROM_WRITE_POOL: Pool<Vec<(u64, FromWrite)>> = Pool::new(640);
}

#[derive(Clone)]
struct Shard {
    read: UnboundedSender<(
        Pooled<Vec<(u64, ToRead)>>,
        oneshot::Receiver<Pooled<Vec<(u64, FromRead)>>>,
    )>,
    write: UnboundedSender<(
        Pooled<Vec<(u64, ToWrite)>>,
        oneshot::Receiver<Pooled<Vec<(u64, FromWrite)>>>,
    )>,
}

impl Shard {
    fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
        secstore: SecStore,
    ) -> Self {
        let (read_tx, read_rx) = unbounded_channel();
        let (write_tx, write_rx) = unbounded_channel();
        task::spawn(async move {
            let mut store = resolver_store::Store::new(parent, children);
            loop {
                select! {
                    batch = read_rx.next() => match batch {
                        None => break,
                        Some((batch, ret)) => {
                            let _ = ret.send(
                                Shard::process_read_batch(&mut store, &secstore, batch)
                            );
                        }
                    }
                    batch = write_rx.next() => match batch {
                        None => break,
                        Some((batch, ret)) => {
                            let _ = ret.send(
                                Shard::process_write_batch(&mut store, &secstore, batch)
                            );
                        }
                    }
                }
            }
        });
        Shard { read: read_tx, write: write_tx }
    }
}

#[derive(Clone)]
struct Store(Vec<Shard>);
