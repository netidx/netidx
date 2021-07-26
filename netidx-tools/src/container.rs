use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
    select_biased,
};
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use netidx::{
    chars::Chars,
    config,
    pack::Pack,
    path::Path,
    pool::{Pool, Pooled},
    publisher::{BindCfg, DefaultHandle, Id, Publisher, UpdateBatch, Val, WriteRequest},
    resolver::Auth,
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
};
use netidx_bscript::{
    expr::{Expr, ExprId},
    vm::{self, Apply, Ctx, ExecCtx, InitFn, Node, Register, RpcCallId},
};
use netidx_protocols::rpc::{self, server::Proc};
use parking_lot::Mutex;
use sled;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::Hash,
    mem,
    sync::Arc,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    runtime::Runtime,
    task,
    time::{self, Instant},
};

lazy_static! {
    static ref VAR_UPDATES: Pool<Vec<(Chars, Value)>> = Pool::new(5, 2048);
    static ref REFS: Pool<Vec<ExprId>> = Pool::new(5, 2048);
    static ref PKBUF: Pool<Vec<u8>> = Pool::new(5, 16384);
}

struct Refs {
    refs: FxHashSet<Path>,
    rpcs: FxHashSet<Path>,
    subs: FxHashSet<SubId>,
    vars: FxHashSet<Chars>,
}

impl Refs {
    fn new() -> Self {
        Refs {
            refs: HashSet::with_hasher(FxBuildHasher::default()),
            rpcs: HashSet::with_hasher(FxBuildHasher::default()),
            subs: HashSet::with_hasher(FxBuildHasher::default()),
            vars: HashSet::with_hasher(FxBuildHasher::default()),
        }
    }
}

struct Fifo {
    data_path: Path,
    data: Val,
    src_path: Path,
    src: Val,
    on_write_path: Path,
    on_write: Val,
    expr_id: Mutex<ExprId>,
    on_write_expr_id: Mutex<ExprId>,
}

struct PublishedVal {
    path: Path,
    val: Val,
}

#[derive(Clone)]
enum Published {
    Formula(Arc<Fifo>),
    Data(Arc<PublishedVal>),
}

struct UserEv(Path, Value);

enum LcEvent {
    Vars,
    RpcCall { name: Path, args: Vec<(Chars, Value)>, id: RpcCallId },
    RpcReply { name: Path, id: RpcCallId, result: Value },
}

struct Lc {
    var: FxHashMap<Chars, FxHashSet<ExprId>>,
    sub: FxHashMap<SubId, FxHashSet<ExprId>>,
    rpc: FxHashMap<Path, FxHashSet<ExprId>>,
    refs: FxHashMap<Path, FxHashSet<ExprId>>,
    forward_refs: FxHashMap<ExprId, Refs>,
    subscriber: Subscriber,
    publisher: Publisher,
    sub_updates: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    var_updates: Pooled<Vec<(Chars, Value)>>,
    published: FxHashMap<Id, Published>,
    events: mpsc::UnboundedSender<LcEvent>,
}

fn remove_eid_from_set<K: Hash + Eq>(
    tbl: &mut FxHashMap<K, FxHashSet<ExprId>>,
    key: K,
    expr_id: &ExprId,
) {
    if let Entry::Occupied(mut e) = tbl.entry(key) {
        let set = e.get_mut();
        set.remove(expr_id);
        if set.is_empty() {
            e.remove();
        }
    }
}

impl Lc {
    fn new(
        subscriber: Subscriber,
        publisher: Publisher,
        sub_updates: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
        events: mpsc::UnboundedSender<LcEvent>,
    ) -> Self {
        Self {
            var: HashMap::with_hasher(FxBuildHasher::default()),
            sub: HashMap::with_hasher(FxBuildHasher::default()),
            rpc: HashMap::with_hasher(FxBuildHasher::default()),
            refs: HashMap::with_hasher(FxBuildHasher::default()),
            forward_refs: HashMap::with_hasher(FxBuildHasher::default()),
            subscriber,
            publisher,
            sub_updates,
            var_updates: VAR_UPDATES.take(),
            published: HashMap::with_hasher(FxBuildHasher::default()),
            events,
        }
    }

    fn unref(&mut self, expr_id: ExprId) {
        if let Some(refs) = self.forward_refs.remove(&expr_id) {
            for path in refs.refs {
                remove_eid_from_set(&mut self.refs, path, &expr_id);
            }
            for path in refs.rpcs {
                remove_eid_from_set(&mut self.rpc, path, &expr_id);
            }
            for id in refs.subs {
                remove_eid_from_set(&mut self.sub, id, &expr_id);
            }
            for name in refs.vars {
                remove_eid_from_set(&mut self.var, name, &expr_id);
            }
        }
    }
}

impl Ctx for Lc {
    fn clear(&mut self) {}

    fn durable_subscribe(
        &mut self,
        flags: UpdatesFlags,
        path: Path,
        ref_id: ExprId,
    ) -> Dval {
        let dv = self.subscriber.durable_subscribe(path);
        dv.updates(flags, self.sub_updates.clone());
        self.sub
            .entry(dv.id())
            .or_insert_with(|| HashSet::with_hasher(FxBuildHasher::default()))
            .insert(ref_id);
        self.forward_refs.entry(ref_id).or_insert_with(Refs::new).subs.insert(dv.id());
        dv
    }

    fn ref_var(&mut self, name: Chars, ref_id: ExprId) {
        self.var
            .entry(name.clone())
            .or_insert_with(|| HashSet::with_hasher(FxBuildHasher::default()))
            .insert(ref_id);
        self.forward_refs.entry(ref_id).or_insert_with(Refs::new).vars.insert(name);
    }

    fn set_var(
        &mut self,
        variables: &mut HashMap<Chars, Value>,
        name: Chars,
        value: Value,
    ) {
        variables.insert(name.clone(), value.clone());
        self.var_updates.push((name, value));
    }

    fn call_rpc(
        &mut self,
        name: Path,
        args: Vec<(Chars, Value)>,
        ref_id: ExprId,
        id: RpcCallId,
    ) {
        self.rpc
            .entry(name.clone())
            .or_insert_with(|| HashSet::with_hasher(FxBuildHasher::default()))
            .insert(ref_id);
        self.forward_refs
            .entry(ref_id)
            .or_insert_with(Refs::new)
            .rpcs
            .insert(name.clone());
        let _: Result<_, _> =
            self.events.unbounded_send(LcEvent::RpcCall { name, args, id });
    }
}

struct Ref {
    path: Option<Chars>,
    current: Value,
}

impl Ref {
    fn get_path(from: &[Node<Lc, UserEv>]) -> Option<Chars> {
        match from {
            [path] => path.current().and_then(|v| v.get_as::<Chars>()),
            _ => None,
        }
    }

    fn get_current(ctx: &ExecCtx<Lc, UserEv>, path: &Option<Chars>) -> Value {
        match path {
            None => Value::Error(Chars::from("#REF")),
            Some(path) => match ctx.user.publisher.id(path) {
                None => Value::Error(Chars::from("#REF")),
                Some(id) => match ctx.user.published.get(&id) {
                    None => Value::Error(Chars::from("#REF")),
                    Some(Published::Data(p)) => {
                        ctx.user.publisher.current(&p.val.id()).unwrap_or(Value::Null)
                    }
                    Some(Published::Formula(fifo)) => {
                        ctx.user.publisher.current(&fifo.data.id()).unwrap_or(Value::Null)
                    }
                },
            },
        }
    }

    fn apply_ev(&mut self, ev: &vm::Event<UserEv>) -> Option<Value> {
        match ev {
            vm::Event::User(UserEv(path, value)) => {
                if Some(path.as_ref()) == self.path.as_ref().map(|c| c.as_ref()) {
                    self.current = value.clone();
                    Some(self.current.clone())
                } else {
                    None
                }
            }
            vm::Event::Netidx(_, _)
            | vm::Event::Rpc(_, _)
            | vm::Event::Variable(_, _) => None,
        }
    }
}

impl Register<Lc, UserEv> for Ref {
    fn register(ctx: &mut ExecCtx<Lc, UserEv>) {
        let f: InitFn<Lc, UserEv> = Arc::new(|ctx, from, _| {
            let path = Ref::get_path(from);
            let current = Ref::get_current(ctx, &path);
            Box::new(Ref { path, current })
        });
        ctx.functions.insert("ref".into(), f);
    }
}

impl Apply<Lc, UserEv> for Ref {
    fn current(&self) -> Option<Value> {
        Some(self.current.clone())
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<Lc, UserEv>,
        from: &mut [Node<Lc, UserEv>],
        event: &vm::Event<UserEv>,
    ) -> Option<Value> {
        match from {
            [path] => match path.update(ctx, event).map(|p| p.get_as::<Chars>()) {
                None => self.apply_ev(event),
                Some(new_path) => {
                    if new_path != self.path {
                        self.path = new_path;
                        match self.apply_ev(event) {
                            v @ Some(_) => v,
                            None => {
                                self.current = Ref::get_current(ctx, &self.path);
                                Some(self.current.clone())
                            }
                        }
                    } else {
                        self.apply_ev(event)
                    }
                }
            },
            from => {
                for n in from {
                    n.update(ctx, event);
                }
                None
            }
        }
    }
}

#[derive(StructOpt, Debug)]
pub(super) struct ContainerConfig {
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
    )]
    bind: BindCfg,
    #[structopt(long = "spn", help = "krb5 use <spn>")]
    pub(super) spn: Option<String>,
    #[structopt(
        long = "timeout",
        help = "require subscribers to consume values before timeout (seconds)"
    )]
    timeout: Option<u64>,
    #[structopt(long = "base-path", help = "the netidx path of the db")]
    base_path: Path,
    #[structopt(long = "db", help = "the db file")]
    db: String,
    #[structopt(long = "compress", help = "use zstd compression")]
    compress: bool,
    #[structopt(long = "compress-level", help = "zstd compression level")]
    compress_level: Option<u32>,
    #[structopt(long = "cache-size", help = "db page cache size in bytes")]
    cache_size: Option<u64>,
}

impl Published {
    fn val(&self) -> &Val {
        match self {
            Published::Formula(fi) => &fi.data,
            Published::Data(p) => &p.val,
        }
    }
}

enum Compiled {
    Formula { node: Node<Lc, UserEv>, data_id: Id },
    OnWrite(Node<Lc, UserEv>),
}

struct Container {
    cfg: ContainerConfig,
    _db: sled::Db,
    formulas: sled::Tree,
    data: sled::Tree,
    ctx: ExecCtx<Lc, UserEv>,
    compiled: FxHashMap<ExprId, Compiled>,
    sub_updates: mpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
    write_updates_tx: mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    write_updates_rx: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    publish_requests: DefaultHandle,
    _delete_path_rpc: Proc,
    delete_path_rx: mpsc::Receiver<(Path, oneshot::Sender<Value>)>,
    bscript_event: mpsc::UnboundedReceiver<LcEvent>,
    rpcs: FxHashMap<
        Path,
        (Instant, mpsc::UnboundedSender<(Vec<(Chars, Value)>, RpcCallId)>),
    >,
}

fn start_delete_rpc(
    publisher: &Publisher,
    base_path: &Path,
) -> Result<(Proc, mpsc::Receiver<(Path, oneshot::Sender<Value>)>)> {
    fn e(s: &'static str) -> Value {
        Value::Error(Chars::from(s))
    }
    let (tx, rx) = mpsc::channel(10);
    let proc = Proc::new(
        publisher,
        base_path.append(".api/delete"),
        Value::from("delete a path from the database"),
        vec![(Arc::from("path"), (Value::Null, Value::from("the path(s) to delete")))]
            .into_iter()
            .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => e("invalid argument, expected path"),
                    Some(mut paths) => {
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(Arc::from(&*path)),
                                _ => return e("invalid argument type, expected string"),
                            };
                            let (reply_tx, reply_rx) = oneshot::channel();
                            let _: Result<_, _> = tx.send((path, reply_tx)).await;
                            match reply_rx.await {
                                Err(_) => return e("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?;
    Ok((proc, rx))
}

fn store<V: Pack + 'static>(tree: &sled::Tree, path: &Path, value: &V) -> Result<()> {
    let mut path_buf = PKBUF.take();
    let mut val_buf = PKBUF.take();
    path.encode(&mut *path_buf)?;
    value.encode(&mut *val_buf)?;
    tree.insert(&**path_buf, &**val_buf)?;
    Ok(())
}

fn to_chars(value: Value) -> Chars {
    value.cast_to::<Chars>().ok().unwrap_or_else(|| Chars::from("null"))
}

fn check_path(base_path: &Path, path: Path) -> Result<Path> {
    if !path.starts_with(&**base_path) {
        bail!("non root path")
    }
    Ok(path)
}

impl Container {
    async fn new(cfg: config::Config, auth: Auth, ccfg: ContainerConfig) -> Result<Self> {
        let _db = sled::Config::default()
            .use_compression(ccfg.compress)
            .compression_factor(ccfg.compress_level.unwrap_or(5) as i32)
            .cache_capacity(ccfg.cache_size.unwrap_or(16 * 1024 * 1024))
            .path(&ccfg.db)
            .open()?;
        let formulas = _db.open_tree("formulas")?;
        let data = _db.open_tree("data")?;
        let publisher = Publisher::new(cfg.clone(), auth.clone(), ccfg.bind).await?;
        let publish_requests = publisher.publish_default(ccfg.base_path.clone())?;
        let subscriber = Subscriber::new(cfg, auth)?;
        let (sub_updates_tx, sub_updates) = mpsc::channel(3);
        let (write_updates_tx, write_updates_rx) = mpsc::channel(3);
        let (bs_tx, bs_rx) = mpsc::unbounded();
        let ctx = ExecCtx::new(Lc::new(subscriber, publisher, sub_updates_tx, bs_tx));
        let (_delete_path_rpc, delete_path_rx) =
            start_delete_rpc(&ctx.user.publisher, &ccfg.base_path)?;
        Ok(Container {
            cfg: ccfg,
            _db,
            formulas,
            data,
            ctx,
            compiled: HashMap::with_hasher(FxBuildHasher::default()),
            sub_updates,
            write_updates_tx,
            write_updates_rx,
            publish_requests,
            _delete_path_rpc,
            delete_path_rx,
            bscript_event: bs_rx,
            rpcs: HashMap::with_hasher(FxBuildHasher::default()),
        })
    }

    fn publish_formula(
        &mut self,
        path: Path,
        batch: &mut UpdateBatch,
        formula_txt: Chars,
        on_write_txt: Chars,
    ) -> Result<()> {
        let data = self.ctx.user.publisher.publish(path.clone(), Value::Null)?;
        let src_path = path.append(".formula");
        let src = self
            .ctx
            .user
            .publisher
            .publish(src_path.clone(), Value::from(formula_txt.clone()))?;
        let on_write_path = path.append(".on-write");
        let on_write = self
            .ctx
            .user
            .publisher
            .publish(on_write_path.clone(), Value::from(on_write_txt.clone()))?;
        let data_id = data.id();
        let src_id = src.id();
        let on_write_id = on_write.id();
        self.ctx.user.publisher.writes(data.id(), self.write_updates_tx.clone());
        self.ctx.user.publisher.writes(src.id(), self.write_updates_tx.clone());
        self.ctx.user.publisher.writes(on_write.id(), self.write_updates_tx.clone());
        let fifo = Arc::new(Fifo {
            data_path: path.clone(),
            data,
            src_path,
            src,
            on_write_path,
            on_write,
            expr_id: Mutex::new(ExprId::new()),
            on_write_expr_id: Mutex::new(ExprId::new()),
        });
        let published = Published::Formula(fifo.clone());
        self.ctx.user.published.insert(data_id, published.clone());
        self.ctx.user.published.insert(src_id, published.clone());
        self.ctx.user.published.insert(on_write_id, published);
        let expr = formula_txt.parse::<Expr>()?;
        let on_write_expr = on_write_txt.parse::<Expr>()?;
        let expr_id = expr.id;
        let on_write_expr_id = on_write_expr.id;
        let formula_node = Node::compile(&mut self.ctx, expr);
        let on_write_node = Node::compile(&mut self.ctx, on_write_expr);
        if let Some(value) = formula_node.current() {
            fifo.data.update(batch, value);
        }
        self.compiled.insert(expr_id, Compiled::Formula { node: formula_node, data_id });
        self.compiled.insert(on_write_expr_id, Compiled::OnWrite(on_write_node));
        *fifo.expr_id.lock() = expr_id;
        *fifo.on_write_expr_id.lock() = on_write_expr_id;
        Ok(())
    }

    fn publish_data(&mut self, path: Path, value: Value) -> Result<()> {
        let val = self.ctx.user.publisher.publish(path.clone(), value)?;
        let id = val.id();
        self.ctx.user.publisher.writes(val.id(), self.write_updates_tx.clone());
        self.ctx
            .user
            .published
            .insert(id, Published::Data(Arc::new(PublishedVal { path, val })));
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        let mut batch = self.ctx.user.publisher.start_batch();
        for res in self.data.iter() {
            let (path, value) = res?;
            let path = check_path(&self.cfg.base_path, Path::decode(&mut &*path)?)?;
            let value = Value::decode(&mut &*value)?;
            let _: Result<()> = self.publish_data(path, value);
        }
        for res in self.formulas.iter() {
            let (path, value) = res?;
            let path = check_path(&self.cfg.base_path, Path::decode(&mut &*path)?)?;
            let (formula_txt, on_write_txt) = <(Chars, Chars)>::decode(&mut &*value)?;
            // CR estokes: log errors
            let _: Result<()> =
                self.publish_formula(path, &mut batch, formula_txt, on_write_txt);
        }
        Ok(batch.commit(self.cfg.timeout.map(Duration::from_secs)).await)
    }

    fn process_var_updates(&mut self, batch: &mut UpdateBatch) {
        let mut refs = REFS.take();
        let mut n = 0;
        while self.ctx.user.var_updates.len() > 0 && n < 10 {
            let mut pending =
                mem::replace(&mut self.ctx.user.var_updates, VAR_UPDATES.take());
            for (name, value) in pending.drain(..) {
                if let Some(expr_ids) = self.ctx.user.var.get(&name) {
                    refs.extend(expr_ids.iter().copied());
                }
                self.update_expr_ids(batch, &mut refs, &vm::Event::Variable(name, value));
            }
            n += 1;
        }
        if self.ctx.user.var_updates.len() > 0 {
            let _: Result<_, _> = self.ctx.user.events.unbounded_send(LcEvent::Vars);
        }
    }

    fn update_expr_ids(
        &mut self,
        batch: &mut UpdateBatch,
        refs: &mut Pooled<Vec<ExprId>>,
        event: &vm::Event<UserEv>,
    ) {
        for expr_id in refs.drain(..) {
            match self.compiled.get_mut(&expr_id) {
                None => (),
                Some(Compiled::OnWrite(node)) => {
                    node.update(&mut self.ctx, &event);
                }
                Some(Compiled::Formula { node, data_id }) => {
                    if let Some(value) = node.update(&mut self.ctx, &event) {
                        if let Some(val) = self.ctx.user.published.get(data_id) {
                            val.val().update(batch, value);
                        }
                    }
                }
            }
        }
    }

    fn update_refs(
        &mut self,
        batch: &mut UpdateBatch,
        refs: &mut Pooled<Vec<ExprId>>,
        changed_path: &Path,
        value: Value,
    ) {
        if let Some(expr_ids) = self.ctx.user.refs.get(changed_path) {
            refs.extend(expr_ids.iter().copied());
        }
        self.update_expr_ids(
            batch,
            refs,
            &vm::Event::User(UserEv(changed_path.clone(), value)),
        );
        self.process_var_updates(batch);
    }

    fn process_subscriptions(
        &mut self,
        batch: &mut UpdateBatch,
        mut updates: Pooled<Vec<(SubId, Event)>>,
    ) {
        let mut refs = REFS.take();
        for (id, event) in updates.drain(..) {
            if let Event::Update(value) = event {
                if let Some(expr_ids) = self.ctx.user.sub.get(&id) {
                    refs.extend(expr_ids.iter().copied());
                }
                self.update_expr_ids(batch, &mut refs, &vm::Event::Netidx(id, value));
            }
        }
        self.process_var_updates(batch);
    }

    fn change_formula(
        &mut self,
        batch: &mut UpdateBatch,
        refs: &mut Pooled<Vec<ExprId>>,
        fifo: &Arc<Fifo>,
        value: Chars,
    ) -> Result<()> {
        fifo.src.update(batch, Value::String(value.clone()));
        let mut expr_id = fifo.expr_id.lock();
        self.compiled.remove(&expr_id);
        self.ctx.user.unref(*expr_id);
        let dv = match value.parse::<Expr>() {
            Ok(expr) => {
                *expr_id = expr.id;
                let node = Node::compile(&mut self.ctx, expr);
                let dv = node.current().unwrap_or(Value::Null);
                self.compiled.insert(
                    *expr_id,
                    Compiled::Formula { node, data_id: fifo.data.id() },
                );
                dv
            }
            Err(e) => {
                let e = Chars::from(format!("{}", e));
                Value::Error(e)
            }
        };
        fifo.data.update(batch, dv.clone());
        let path = &fifo.data_path;
        self.update_refs(batch, refs, &fifo.src_path, Value::String(value.clone()));
        self.update_refs(batch, refs, path, dv);
        let cur =
            self.ctx.user.publisher.current(&fifo.on_write.id()).unwrap_or(Value::Null);
        Ok(store(&self.formulas, path, &(value, to_chars(cur)))?)
    }

    fn change_on_write(
        &mut self,
        batch: &mut UpdateBatch,
        refs: &mut Pooled<Vec<ExprId>>,
        fifo: &Arc<Fifo>,
        value: Chars,
    ) -> Result<()> {
        fifo.on_write.update(batch, Value::String(value.clone()));
        let mut expr_id = fifo.on_write_expr_id.lock();
        self.compiled.remove(&expr_id);
        self.ctx.user.unref(*expr_id);
        match value.parse::<Expr>() {
            Ok(expr) => {
                *expr_id = expr.id;
                let node = Node::compile(&mut self.ctx, expr);
                self.compiled.insert(*expr_id, Compiled::OnWrite(node));
            }
            Err(_) => (), // CR estokes: log and report to user somehow
        }
        self.update_refs(batch, refs, &fifo.on_write_path, Value::String(value.clone()));
        let path = &fifo.data_path;
        let cur = self.ctx.user.publisher.current(&fifo.src.id()).unwrap_or(Value::Null);
        Ok(store(&self.formulas, path, &(to_chars(cur), value))?)
    }

    fn process_writes(
        &mut self,
        batch: &mut UpdateBatch,
        mut writes: Pooled<Vec<WriteRequest>>,
    ) {
        let mut refs = REFS.take();
        for req in writes.drain(..) {
            refs.clear();
            match self.ctx.user.published.get(&req.id) {
                None => (),
                Some(Published::Data(p)) => {
                    if let Err(_) = store(&self.data, &p.path, &req.value) {
                        continue;
                    }
                    p.val.update(batch, req.value.clone());
                    let path = p.path.clone();
                    self.update_refs(batch, &mut refs, &path, req.value);
                }
                Some(Published::Formula(fifo)) => {
                    let fifo = fifo.clone();
                    if fifo.src.id() == req.id {
                        // CR estokes: log
                        let _: Result<_, _> = self.change_formula(
                            batch,
                            &mut refs,
                            &fifo,
                            to_chars(req.value),
                        );
                    } else if fifo.on_write.id() == req.id {
                        // CR estokes: log
                        let _: Result<_, _> = self.change_on_write(
                            batch,
                            &mut refs,
                            &fifo,
                            to_chars(req.value),
                        );
                    } else if fifo.data.id() == req.id {
                        if let Some(Compiled::OnWrite(node)) =
                            self.compiled.get_mut(&fifo.on_write_expr_id.lock())
                        {
                            let path = fifo.data_path.clone();
                            let ev = vm::Event::User(UserEv(path, req.value));
                            node.update(&mut self.ctx, &ev);
                            self.process_var_updates(batch);
                        }
                    }
                }
            }
        }
    }

    fn process_publish_request(
        &mut self,
        batch: &mut UpdateBatch,
        path: Path,
        reply: oneshot::Sender<()>,
    ) {
        match check_path(&self.cfg.base_path, path) {
            Err(_) => {
                // this should not be possible, but in case of a bug, just do nothing
                let _: Result<_, _> = reply.send(());
            }
            Ok(path) => {
                let name = Path::basename(&path);
                if name == Some(".formula") || name == Some(".on-write") {
                    if let Some(path) = Path::dirname(&path) {
                        let path = Path::from(Arc::from(path));
                        // CR estokes: log errors
                        let _: Result<()> = self.publish_formula(
                            path,
                            batch,
                            Chars::from("null"),
                            Chars::from("null"),
                        );
                    }
                } else {
                    let _: Result<()> = self.publish_data(path, Value::Null);
                }
                let _: Result<_, _> = reply.send(());
            }
        }
    }

    fn delete_path(&mut self, batch: &mut UpdateBatch, path: Path) -> Result<()> {
        let path = check_path(&self.cfg.base_path, path)?;
        let bn = Path::basename(&path);
        if bn == Some(".formula") || bn == Some(".on-write") {
            bail!("won't delete .formula/.on-write, delete the base instead")
        } else if let Some(id) = self.ctx.user.publisher.id(&path) {
            let mut kbuf = PKBUF.take();
            path.encode(&mut *kbuf)?;
            let ref_err = Value::Error(Chars::from("#REF"));
            match self.ctx.user.published.remove(&id) {
                None => (),
                Some(Published::Data(_)) => {
                    let _ = self.data.remove(&*kbuf);
                }
                Some(Published::Formula(fifo)) => {
                    let fpath = path.append(".formula");
                    let opath = path.append(".on-write");
                    let id = fifo.expr_id.lock();
                    self.ctx.user.unref(*id);
                    self.compiled.remove(&id);
                    let id = fifo.on_write_expr_id.lock();
                    self.ctx.user.unref(*id);
                    self.compiled.remove(&id);
                    if let Some(id) = self.ctx.user.publisher.id(&fpath) {
                        self.ctx.user.published.remove(&id);
                    }
                    if let Some(id) = self.ctx.user.publisher.id(&opath) {
                        self.ctx.user.published.remove(&id);
                    }
                    let _ = self.data.remove(&*kbuf);
                    self.formulas.remove(&*kbuf)?;
                    self.update_refs(batch, &mut REFS.take(), &fpath, ref_err.clone());
                    self.update_refs(batch, &mut REFS.take(), &opath, ref_err.clone());
                }
            }
            self.update_refs(batch, &mut REFS.take(), &path, ref_err);
            Ok(())
        } else {
            bail!("no such path {}", path)
        }
    }

    fn get_rpc_proc(
        &mut self,
        name: &Path,
    ) -> mpsc::UnboundedSender<(Vec<(Chars, Value)>, RpcCallId)> {
        async fn rpc_task(
            reply: mpsc::UnboundedSender<LcEvent>,
            subscriber: Subscriber,
            name: Path,
            mut rx: mpsc::UnboundedReceiver<(Vec<(Chars, Value)>, RpcCallId)>,
        ) -> Result<()> {
            let proc = rpc::client::Proc::new(&subscriber, name.clone()).await?;
            while let Some((args, id)) = rx.next().await {
                let name = name.clone();
                let result = proc.call(args).await?;
                reply.unbounded_send(LcEvent::RpcReply { name, id, result })?
            }
            Ok(())
        }
        match self.rpcs.get_mut(name) {
            Some((ref mut last, ref proc)) => {
                *last = Instant::now();
                proc.clone()
            }
            None => {
                let (tx, rx) = mpsc::unbounded();
                task::spawn({
                    let to_gui = self.ctx.user.events.clone();
                    let sub = self.ctx.user.subscriber.clone();
                    let name = name.clone();
                    async move {
                        let _: Result<_, _> =
                            rpc_task(to_gui.clone(), sub, name.clone(), rx).await;
                    }
                });
                self.rpcs.insert(name.clone(), (Instant::now(), tx.clone()));
                tx
            }
        }
    }

    fn gc_rpcs(&mut self) {
        static MAX_RPC_AGE: Duration = Duration::from_secs(120);
        let now = Instant::now();
        self.rpcs.retain(|_, (last, _)| now - *last < MAX_RPC_AGE);
    }

    async fn process_bscript_event(&mut self, batch: &mut UpdateBatch, event: LcEvent) {
        match event {
            LcEvent::Vars => self.process_var_updates(batch),
            LcEvent::RpcReply { name, id, result } => {
                let mut refs = REFS.take();
                if let Some(expr_ids) = self.ctx.user.rpc.get(&name) {
                    refs.extend(expr_ids);
                }
                let e = vm::Event::Rpc(id, result);
                for id in refs.drain(..) {
                    match self.compiled.get_mut(&id) {
                        None => (),
                        Some(Compiled::Formula { node, .. }) => {
                            node.update(&mut self.ctx, &e);
                        }
                        Some(Compiled::OnWrite(node)) => {
                            node.update(&mut self.ctx, &e);
                        }
                    }
                }
            }
            LcEvent::RpcCall { name, mut args, id } => {
                for _ in 1..3 {
                    let proc = self.get_rpc_proc(&name);
                    match proc.unbounded_send((mem::replace(&mut args, vec![]), id)) {
                        Ok(()) => return (),
                        Err(e) => {
                            self.rpcs.remove(&name);
                            args = e.into_inner().0;
                        }
                    }
                }
                let result = Value::Error(Chars::from("failed to call rpc"));
                let _: Result<_, _> = self
                    .ctx
                    .user
                    .events
                    .unbounded_send(LcEvent::RpcReply { id, name, result });
            }
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut gc_rpcs = time::interval(Duration::from_secs(60));
        self.init().await?;
        loop {
            let mut batch = self.ctx.user.publisher.start_batch();
            select_biased! {
                r = self.publish_requests.select_next_some() => {
                    self.process_publish_request(&mut batch, r.0, r.1);
                }
                r = self.sub_updates.select_next_some() => {
                    self.process_subscriptions(&mut batch, r);
                }
                r = self.write_updates_rx.select_next_some() => {
                    self.process_writes(&mut batch, r);
                }
                r = self.delete_path_rx.select_next_some() => {
                    let _: Result<_, _> = r.1.send(match self.delete_path(&mut batch, r.0) {
                        Ok(()) => Value::Ok,
                        Err(e) => Value::Error(Chars::from(format!("{}", e))),
                    });
                }
                r = self.bscript_event.select_next_some() => {
                    self.process_bscript_event(&mut batch, r).await;
                }
                _ = gc_rpcs.tick().fuse() => {
                    self.gc_rpcs();
                }
                complete => break
            }
            let timeout = self.cfg.timeout.map(Duration::from_secs);
            batch.commit(timeout).await;
        }
        Ok(())
    }
}

pub(super) fn run(cfg: config::Config, auth: Auth, ccfg: ContainerConfig) {
    Runtime::new().expect("failed to create runtime").block_on(async move {
        let t = Container::new(cfg, auth, ccfg).await.expect("failed to create context");
        t.run().await.expect("container main loop failed")
    })
}
