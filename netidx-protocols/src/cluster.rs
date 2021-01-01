use anyhow::Result;
use bytes::Bytes;
use futures::{channel::mpsc, prelude::*};
use log::{info, warn};
use netidx::{
    path::Path,
    pool::Pooled,
    publisher::{Publisher, Val, Value, WriteRequest},
    resolver::ChangeTracker,
    subscriber::{Dval, Event, Subscriber},
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::{HashMap, HashSet},
    iter,
    marker::PhantomData,
};
use tokio::time;
use uuid::{adapter::SimpleRef, Uuid};

pub fn uuid_string(id: Uuid) -> String {
    let mut buf = [0u8; SimpleRef::LENGTH];
    id.to_simple_ref().encode_lower(&mut buf).into()
}

/// Simple clustering based on netidx. Each member publishes a uuid to
/// a common base path, which is used to discover all other
/// members. Commands may be sent to and received from all other
/// members as broadcasts. On initialization all members wait until
/// there are at least `shards` other members before starting
/// operations. There can be more than `shards` members at any time,
/// and members can enter and leave the cluster at will. It is up to
/// the user to ensure state integrity under these constraints.
///
/// Messages are encoded to json, so don't expect fantastic
/// performance.
///
/// A random cluster member is elected 'primary' by an common
/// algorithm, the primary may change as members join and leave the
/// cluster, but with a stable member set all members will agree on
/// which one is the primary.
pub struct Cluster<T: Serialize + DeserializeOwned + 'static> {
    t: PhantomData<T>,
    ctrack: ChangeTracker,
    subscriber: Subscriber,
    our_path: Path,
    us: Val,
    others: HashMap<Path, Dval>,
    cmd: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    primary: bool,
}

impl<T: Serialize + DeserializeOwned + 'static> Cluster<T> {
    /// Create a new cluster directly under `base`. It's wise to
    /// ensure nothing else is publishing under `base`.
    pub async fn new(
        publisher: &Publisher,
        subscriber: Subscriber,
        base: Path,
        shards: usize,
    ) -> Result<Cluster<T>> {
        let (tx, cmd) = mpsc::channel(3);
        let id = Uuid::new_v4();
        let our_path = base.append(&uuid_string(id));
        let us = publisher.publish(our_path.clone(), Value::Null)?;
        let ctrack = ChangeTracker::new(base);
        us.writes(tx);
        publisher.flush(None).await;
        let others = HashMap::new();
        let t = PhantomData;
        let mut t =
            Cluster { t, ctrack, subscriber, our_path, us, cmd, others, primary: true };
        while t.others.len() < shards
            || t.others.values().any(|d| d.last() == Event::Unsubscribed)
        {
            info!("waiting for {} other shards", shards);
            t.poll_members().await?;
            time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(t)
    }

    /// Returns true if this cluster member is the primary, false
    /// otherwise. May change after `poll_members`.
    pub fn primary(&self) -> bool {
        self.primary
    }

    pub fn others(&self) -> usize {
        self.us.subscribed_len()
    }

    /// Poll the resolvers to see if any new members have joined the
    /// cluster, return true if new members have potentially joined,
    /// false if no new members have joined.
    pub async fn poll_members(&mut self) -> Result<bool> {
        if !self.subscriber.resolver().check_changed(&mut self.ctrack).await? {
            Ok(false)
        } else {
            let path = self.ctrack.path().clone();
            let mut l = self.subscriber.resolver().list(path).await?;
            let all = l.drain(..).filter(|p| p != &self.our_path).collect::<HashSet<_>>();
            self.others.retain(|p, _| all.contains(p));
            for path in all {
                if !self.others.contains_key(&path) {
                    let dv = self.subscriber.durable_subscribe(path.clone());
                    self.others.insert(path, dv);
                }
            }
            let mut paths =
                iter::once(&self.our_path).chain(self.others.keys()).collect::<Vec<_>>();
            paths.sort();
            self.primary = self.our_path == *paths[0];
            Ok(true)
        }
    }

    /// Wait for some commands from other members of the cluster.
    pub async fn wait_cmds(&mut self) -> Result<Vec<T>> {
        match self.cmd.next().await {
            None => bail!("cluster publish write stream ended"),
            Some(mut reqs) => {
                let mut cmds = Vec::new();
                for req in reqs.drain(..) {
                    if let Value::Bytes(b) = &req.value {
                        if let Ok(cmd) = serde_json::from_slice::<T>(&**b) {
                            cmds.push(cmd);
                            continue;
                        }
                    }
                    warn!("ignoring invalid cmd: {:?}", &req.value);
                }
                Ok(cmds)
            }
        }
    }

    /// Send a command out to other members of the cluster.
    pub fn send_cmd(&self, cmd: &T) {
        if self.others.len() > 0 {
            let cmd = serde_json::to_vec(cmd).unwrap();
            let cmd = Value::Bytes(Bytes::from(cmd));
            for other in self.others.values() {
                other.write(cmd.clone());
            }
        }
    }
}
