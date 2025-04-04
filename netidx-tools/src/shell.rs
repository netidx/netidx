use anyhow::{Error, Result};
use arcstr::ArcStr;
use futures::channel::mpsc;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use indexmap::IndexMap;
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, DesiredAuth, Publisher, PublisherBuilder, Value, WriteRequest},
    subscriber::{self, Dval, SubId, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use netidx_bscript::{
    expr::ExprId, node::Node, BindId, Ctx, Event, ExecCtx, NoUserEvent,
};
use reedline::{DefaultPrompt, Reedline, Signal};
use smallvec::SmallVec;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    path::PathBuf,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{sync::oneshot, task};

#[derive(StructOpt, Debug)]
pub(crate) struct Params {
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. local, 192.168.0.0/16"
    )]
    bind: Option<BindCfg>,
    #[structopt(
        long = "publish-timeout",
        help = "require subscribers to consume values before timeout (seconds)"
    )]
    publish_timeout: Option<u64>,
    #[structopt(
        long = "subscribe-timeout",
        help = "cancel subscription unless it succeeds within timeout"
    )]
    subscribe_timeout: Option<u64>,
    #[structopt(name = "file", help = "script file to execute")]
    file: Option<PathBuf>,
}

type UpdateBatch = Pooled<Vec<(SubId, subscriber::Event)>>;
type WriteBatch = Pooled<Vec<WriteRequest>>;

struct ReplCtx {
    by_ref: FxHashMap<BindId, SmallVec<[ExprId; 3]>>,
    refed: FxHashMap<ExprId, usize>,
    var_updates: VecDeque<(BindId, Value)>,
    net_updates: VecDeque<(SubId, subscriber::Event)>,
    subscribed: FxHashMap<SubId, SmallVec<[ExprId; 3]>>,
    publisher: Publisher,
    subscriber: Subscriber,
    updates_tx: mpsc::Sender<UpdateBatch>,
    updates: mpsc::Receiver<UpdateBatch>,
    writes_tx: mpsc::Sender<WriteBatch>,
    writes: mpsc::Receiver<WriteBatch>,
}

impl ReplCtx {
    fn new(publisher: Publisher, subscriber: Subscriber) -> Self {
        let (updates_tx, updates) = mpsc::channel(3);
        let (writes_tx, writes) = mpsc::channel(3);
        Self {
            by_ref: HashMap::default(),
            refed: HashMap::default(),
            var_updates: VecDeque::new(),
            net_updates: VecDeque::new(),
            subscribed: HashMap::default(),
            publisher,
            subscriber,
            updates,
            updates_tx,
            writes,
            writes_tx,
        }
    }
}

impl Ctx for ReplCtx {
    fn call_rpc(
        &mut self,
        _name: Path,
        _args: Vec<(ArcStr, Value)>,
        _ref_by: ExprId,
        _id: BindId,
    ) {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.by_ref.clear();
        self.var_updates.clear();
    }

    fn durable_subscribe(
        &mut self,
        flags: UpdatesFlags,
        path: Path,
        ref_by: ExprId,
    ) -> Dval {
        let dval =
            self.subscriber.subscribe_updates(path, [(flags, self.updates_tx.clone())]);
        let exprs = self.subscribed.entry(dval.id()).or_default();
        if !exprs.contains(&ref_by) {
            exprs.push(ref_by);
        }
        dval
    }

    fn unsubscribe(&mut self, _path: Path, dv: Dval, ref_by: ExprId) {
        if let Some(exprs) = self.subscribed.get_mut(&dv.id()) {
            exprs.retain(|eid| eid != &ref_by);
            if exprs.is_empty() {
                self.subscribed.remove(&dv.id());
            }
        }
    }

    fn set_timer(&mut self, _id: BindId, _timeout: Duration, _ref_by: ExprId) {
        unimplemented!()
    }

    fn ref_var(&mut self, id: BindId, ref_by: ExprId) {
        let refs = self.by_ref.entry(id).or_default();
        if !refs.contains(&ref_by) {
            refs.push(ref_by);
        }
    }

    fn unref_var(&mut self, id: BindId, ref_by: ExprId) {
        if let Some(refs) = self.by_ref.get_mut(&id) {
            refs.retain(|eid| eid != &ref_by);
            if refs.is_empty() {
                self.by_ref.remove(&id);
            }
        }
    }

    fn set_var(&mut self, id: BindId, value: Value) {
        self.var_updates.push_back((id, value));
    }
}

async fn read_input() -> mpsc::UnboundedReceiver<(oneshot::Sender<()>, Result<Signal>)> {
    let (tx, rx) = mpsc::unbounded();
    task::spawn(async move {
        let mut line_editor = Reedline::create();
        let prompt = DefaultPrompt::default();
        loop {
            let r = task::block_in_place(|| {
                line_editor.read_line(&prompt).map_err(Error::from)
            });
            let (c_tx, c_rx) = oneshot::channel();
            if let Err(_) = tx.unbounded_send((c_tx, r)) {
                break;
            }
            let _ = c_rx.await;
        }
    });
    rx
}

struct Repl {
    ctx: ExecCtx<ReplCtx, NoUserEvent>,
    event: Event<NoUserEvent>,
    updated: FxHashSet<ExprId>,
    nodes: IndexMap<ExprId, Node<ReplCtx, NoUserEvent>, FxBuildHasher>,
}

impl Repl {
    fn new(publisher: Publisher, subscriber: Subscriber) -> Result<Self> {
        Ok(Self {
            ctx: ExecCtx::new(ReplCtx::new(publisher, subscriber)),
            event: Event::new(NoUserEvent),
            updated: HashSet::default(),
            nodes: IndexMap::default(),
        })
    }

    fn do_cycle(
        &mut self,
        init: Option<ExprId>,
        updates: Option<UpdateBatch>,
        _writes: Option<WriteBatch>,
    ) -> Option<Value> {
        macro_rules! push_event {
            ($id:expr, $v:expr, $event:ident, $refed:ident, $overflow:ident) => {
                match self.event.$event.entry($id) {
                    Entry::Vacant(e) => {
                        e.insert($v);
                        if let Some(exps) = self.ctx.user.$refed.get(&$id) {
                            self.updated.extend(exps.iter().copied())
                        }
                    }
                    Entry::Occupied(_) => {
                        self.ctx.user.$overflow.push_back(($id, $v));
                    }
                }
            };
        }
        if let Some(id) = init {
            self.updated.insert(id);
        }
        for _ in 0..self.ctx.user.var_updates.len() {
            let (id, v) = self.ctx.user.var_updates.pop_front().unwrap();
            push_event!(id, v, variables, by_ref, var_updates)
        }
        for _ in 0..self.ctx.user.net_updates.len() {
            let (id, v) = self.ctx.user.net_updates.pop_front().unwrap();
            push_event!(id, v, netidx, subscribed, net_updates)
        }
        if let Some(mut updates) = updates {
            for (id, v) in updates.drain(..) {
                push_event!(id, v, netidx, subscribed, net_updates)
            }
        }
        let res = self.nodes.iter_mut().fold(None, |_, (id, n)| {
            if self.updated.contains(id) {
                if Some(*id) == init {
                    self.event.init = true;
                } else {
                    self.event.init = false;
                }
                n.update(&mut self.ctx, &mut self.event)
            } else {
                None
            }
        });
        self.event.clear();
        self.updated.clear();
        res
    }
}

pub(super) async fn run(cfg: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let publisher = PublisherBuilder::new(cfg.clone()).bind_cfg(p.bind).build().await?;
    let subscriber = SubscriberBuilder::new(cfg).desired_auth(auth).build()?;
    Ok(())
}
