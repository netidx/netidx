use anyhow::Result;
use futures::{channel::mpsc, prelude::*};
use log::{info, warn};
use netidx::{
    pack::Pack,
    path::Path,
    publisher::{Publisher, Val, Value, WriteRequest},
    resolver_client::ChangeTracker,
    subscriber::{Dval, Event, Subscriber},
    utils,
};
use poolshark::global::GPooled;
use std::{
    collections::{HashMap, HashSet},
    iter,
    marker::PhantomData,
};
use tokio::time;
use uuid::Uuid;

pub fn uuid_string(id: Uuid) -> String {
    use uuid::fmt::Simple;
    let mut buf = [0u8; Simple::LENGTH];
    Simple::from_uuid(id).encode_lower(&mut buf).into()
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
/// A random cluster member is elected 'primary' by an common
/// algorithm, the primary may change as members join and leave the
/// cluster, but with a stable member set all members will agree on
/// which one is the primary.
pub struct Cluster<T: Pack> {
    t: PhantomData<T>,
    ctrack: ChangeTracker,
    publisher: Publisher,
    subscriber: Subscriber,
    our_path: Path,
    us: Val,
    others: HashMap<Path, Dval>,
    cmd: mpsc::Receiver<GPooled<Vec<WriteRequest>>>,
    primary: bool,
}

impl<T: Pack> Cluster<T> {
    /// Create a new cluster directly under `base`. It's wise to
    /// ensure nothing else is publishing under `base`.
    pub async fn new(
        publisher: &Publisher,
        subscriber: Subscriber,
        base: Path,
        shards: usize,
    ) -> Result<Cluster<T>> {
        let publisher = publisher.clone();
        let (tx, cmd) = mpsc::channel(3);
        let id = Uuid::new_v4();
        let our_path = base.append(&uuid_string(id));
        let us = publisher.publish(our_path.clone(), Value::Null)?;
        let ctrack = ChangeTracker::new(base);
        publisher.writes(us.id(), tx);
        publisher.flushed().await;
        let others = HashMap::new();
        let t = PhantomData;
        let mut t = Cluster {
            t,
            ctrack,
            publisher,
            subscriber,
            our_path,
            us,
            cmd,
            others,
            primary: true,
        };
        while t.subscribed_others() < shards {
            info!("waiting for {} other shards", shards);
            t.poll_members().await?;
            time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Ok(t)
    }

    /// Return your own path
    pub fn path(&self) -> Path {
        self.our_path.clone()
    }

    /// Returns true if this cluster member is the primary, false
    /// otherwise. May change after `poll_members`.
    pub fn primary(&self) -> bool {
        self.primary
    }

    fn subscribed_others(&self) -> usize {
        self.others.len()
            - self.others.values().filter(|d| d.last() == Event::Unsubscribed).count()
    }

    pub fn others(&self) -> usize {
        self.publisher.subscribed_len(&self.us.id())
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
                    let dv = self.subscriber.subscribe(path.clone());
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
        let mut buf = Vec::new();
        self.wait_cmds_buf(&mut buf).await?;
        Ok(buf)
    }

    /// Wait for some commands from other members of the cluster.
    ///
    /// Place the commands in the provided buffer.
    pub async fn wait_cmds_buf(&mut self, buf: &mut Vec<T>) -> Result<()> {
        match self.cmd.next().await {
            None => bail!("cluster publish write stream ended"),
            Some(mut reqs) => {
                for req in reqs.drain(..) {
                    if let Value::Bytes(b) = &req.value {
                        if let Ok(cmd) = Pack::decode(&mut &***b) {
                            buf.push(cmd);
                            continue;
                        }
                    }
                    warn!("ignoring invalid cmd: {:?}", &req.value);
                }
                Ok(())
            }
        }
    }

    /// Send a command out to other members of the cluster.
    pub fn send_cmd(&self, cmd: &T) {
        if self.others.len() > 0 {
            let cmd: Value = utils::pack(cmd).unwrap().freeze().into();
            for other in self.others.values() {
                other.write(cmd.clone());
            }
        }
    }

    /// Send a command to just one other, identified by it's path.
    pub fn send_cmd_to_one(&self, path: &Path, cmd: &T) {
        if let Some(other) = self.others.get(path) {
            let cmd = utils::pack(cmd).unwrap().freeze().into();
            other.write(cmd);
        }
    }
}
