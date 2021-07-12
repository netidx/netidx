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
    publisher::{BindCfg, DefaultHandle, Id, Publisher, Val, WriteRequest},
    resolver::Auth,
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
};
use netidx_bscript::{
    expr::{Expr, ExprId},
    vm::{self, Ctx, ExecCtx, Node},
};
use netidx_protocols::rpc::server::Proc;
use sled;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::Hash,
    mem, str,
    sync::Arc,
    time::Duration,
};
use structopt::StructOpt;
use tokio::runtime::Runtime;

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

struct Lc {
    var: FxHashMap<Chars, FxHashSet<ExprId>>,
    sub: FxHashMap<SubId, FxHashSet<ExprId>>,
    rpc: FxHashMap<Path, FxHashSet<ExprId>>,
    refs: FxHashMap<Path, FxHashSet<ExprId>>,
    forward_refs: FxHashMap<ExprId, Refs>,
    subscriber: Subscriber,
    sub_updates: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    var_updates: Pooled<Vec<(Chars, Value)>>,
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
        sub_updates: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        Self {
            var: HashMap::with_hasher(FxBuildHasher::default()),
            sub: HashMap::with_hasher(FxBuildHasher::default()),
            rpc: HashMap::with_hasher(FxBuildHasher::default()),
            refs: HashMap::with_hasher(FxBuildHasher::default()),
            forward_refs: HashMap::with_hasher(FxBuildHasher::default()),
            subscriber,
            sub_updates,
            var_updates: VAR_UPDATES.take(),
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

    fn call_rpc(&mut self, _name: Path, _args: Vec<(Chars, Value)>, _ref_id: ExprId) {
        unimplemented!()
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

enum Published {
    FormulaVal(Val),
    FormulaSrc(Val, Id),
    OnWrite(Val),
    Data(Val),
}

impl Published {
    fn val(&self) -> &Val {
        match self {
            Published::FormulaVal(val)
            | Published::FormulaSrc(val)
            | Published::OnWrite(val)
            | Published::Data(val) => val,
        }
    }
}

struct Fids {
    expr_id: ExprId,
    id: Id,
    data_id: Id,
}

struct Container {
    cfg: ContainerConfig,
    db: sled::Db,
    formulas: sled::Tree,
    on_writes: sled::Tree,
    data: sled::Tree,
    published_data: FxHashMap<Path, Id>,
    published_formulas: FxHashMap<Path, Fids>,
    published_on_writes: FxHashMap<Path, (Id, ExprId)>,
    ctx: ExecCtx<Lc, Value>,
    compiled: FxHashMap<ExprId, (Node<Lc, Value>, Option<Id>)>,
    published: FxHashMap<Id, Published>,
    publisher: Publisher,
    sub_updates: mpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
    write_updates_tx: mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    write_updates_rx: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    publish_requests: DefaultHandle,
    delete_path_rpc: Proc,
    delete_path_rx: mpsc::Receiver<Path>,
}

async fn start_delete_rpc(
    publisher: &Publisher,
    base_path: &Path,
) -> Result<(Proc, mpsc::Receiver<Path>)> {
    let (tx, rx) = mpsc::channel(10);
    let proc = Proc::new(
        publisher,
        base_path.append(".api/delete"),
        Value::from("delete a path from the database"),
        vec![(Arc::from("path"), (Value::Null, Value::from("the path to delete")))]
            .into_iter()
            .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => Value::Error(Chars::from("invalid argument, expected path")),
                    Some(Value::String(path)) => {
                        let path = Path::from(Arc::from(&*path));
                        let _: Result<_, _> = tx.send(path).await;
                        Value::Ok
                    }
                    Some(_) => Value::Error(Chars::from(
                        "invalid argument type, expected string",
                    )),
                }
            })
        }),
    )
    .await?;
    Ok((proc, rx))
}

impl Container {
    async fn new(cfg: config::Config, auth: Auth, ccfg: ContainerConfig) -> Result<Self> {
        let db = sled::Config::default()
            .use_compression(ccfg.compress)
            .compression_factor(ccfg.compress_level.unwrap_or(5) as i32)
            .cache_capacity(ccfg.cache_size.unwrap_or(16 * 1024 * 1024))
            .path(&ccfg.db)
            .open()?;
        let formulas = db.open_tree("formulas")?;
        let on_writes = db.open_tree("on_writes")?;
        let data = db.open_tree("data")?;
        let publisher = Publisher::new(cfg.clone(), auth.clone(), ccfg.bind).await?;
        let publish_requests = publisher.publish_default(ccfg.base_path.clone())?;
        let subscriber = Subscriber::new(cfg, auth)?;
        let (sub_updates_tx, sub_updates) = mpsc::channel(3);
        let (write_updates_tx, write_updates_rx) = mpsc::channel(3);
        let ctx = ExecCtx::new(Lc::new(subscriber, sub_updates_tx));
        let (delete_path_rpc, delete_path_rx) =
            start_delete_rpc(&publisher, &ccfg.base_path).await?;
        Ok(Container {
            cfg: ccfg,
            db,
            formulas,
            on_writes,
            data,
            published_data: HashMap::with_hasher(FxBuildHasher::default()),
            published_formulas: HashMap::with_hasher(FxBuildHasher::default()),
            published_on_writes: HashMap::with_hasher(FxBuildHasher::default()),
            ctx,
            compiled: HashMap::with_hasher(FxBuildHasher::default()),
            published: HashMap::with_hasher(FxBuildHasher::default()),
            publisher,
            sub_updates,
            write_updates_tx,
            write_updates_rx,
            publish_requests,
            delete_path_rpc,
            delete_path_rx,
        })
    }

    async fn init(&mut self) -> Result<()> {
        fn check_path(base_path: &Path, path: Path) -> Result<Path> {
            if !path.starts_with(&**base_path) {
                bail!("non root paths in the database")
            }
            Ok(path)
        }
        for res in self.data.iter() {
            let (path, value) = res?;
            let path = check_path(&self.cfg.base_path, Path::decode(&mut &*path)?)?;
            let value = Value::decode(&mut &*value)?;
            let val = self.publisher.publish(path.clone(), value)?;
            let id = val.id();
            val.writes(self.write_updates_tx.clone());
            self.published_data.insert(path, id);
            self.published.insert(id, Published::Data(val));
        }
        for res in self.formulas.iter() {
            let (path, value) = res?;
            let path = check_path(&self.cfg.base_path, Path::decode(&mut &*path)?)?;
            let exprtxt = Chars::decode(&mut &*value)?;
            let expr = exprtxt.parse::<Expr>()?;
            let expr_id = expr.id;
            let node = Node::compile(&mut self.ctx, expr);
            let val =
                self.publisher.publish(path.append(".formula"), Value::from(exprtxt))?;
            let dataval = self
                .publisher
                .publish(path.clone(), node.current().unwrap_or(Value::Null))?;
            let id = val.id();
            let data_id = dataval.id();
            val.writes(self.write_updates_tx.clone());
            dataval.writes(self.write_updates_tx.clone());
            self.published.insert(id, Published::FormulaSrc(val));
            self.published.insert(data_id, Published::FormulaVal(dataval));
            self.published_formulas.insert(path, Fids { expr_id, id, data_id });
            self.compiled.insert(expr_id, (node, Some(id)));
        }
        for res in self.on_writes.iter() {
            let (path, value) = res?;
            let path = check_path(&self.cfg.base_path, Path::decode(&mut &*path)?)?;
            let exprtxt = Chars::decode(&mut &*value)?;
            let expr = exprtxt.parse::<Expr>()?;
            let expr_id = expr.id;
            let node = Node::compile(&mut self.ctx, expr);
            let val =
                self.publisher.publish(path.append(".on_write"), Value::from(exprtxt))?;
            let id = val.id();
            val.writes(self.write_updates_tx.clone());
            self.published.insert(id, Published::OnWrite(val));
            self.published_on_writes.insert(path, (id, expr_id));
            self.compiled.insert(expr_id, (node, None));
        }
        Ok(self.publisher.flush(self.cfg.timeout.map(Duration::from_secs)).await)
    }

    fn update_expr_ids(
        &mut self,
        refs: &mut Pooled<Vec<ExprId>>,
        event: &vm::Event<Value>,
    ) {
        for expr_id in refs.drain(..) {
            if let Some((node, id)) = self.compiled.get_mut(&expr_id) {
                if let Some(value) = node.update(&mut self.ctx, &event) {
                    if let Some(val) = id.and_then(|id| self.published.get(&id)) {
                        val.val().update(value);
                    }
                }
            }
        }
    }

    fn process_var_updates(&mut self) {
        // CR estokes: deal with infinite loops
        let mut refs = REFS.take();
        while self.ctx.user.var_updates.len() > 0 {
            let mut pending =
                mem::replace(&mut self.ctx.user.var_updates, VAR_UPDATES.take());
            for (name, value) in pending.drain(..) {
                if let Some(expr_ids) = self.ctx.user.var.get(&name) {
                    refs.extend(expr_ids.iter().copied());
                }
                self.update_expr_ids(&mut refs, &vm::Event::Variable(name, value));
            }
        }
    }

    fn process_subscriptions(&mut self, mut updates: Pooled<Vec<(SubId, Event)>>) {
        let mut refs = REFS.take();
        for (id, event) in updates.drain(..) {
            if let Event::Update(value) = event {
                if let Some(expr_ids) = self.ctx.user.sub.get(&id) {
                    refs.extend(expr_ids.iter().copied());
                }
                self.update_expr_ids(&mut refs, &vm::Event::Netidx(id, value));
            }
        }
        self.process_var_updates();
    }

    fn process_writes(&mut self, mut writes: Pooled<Vec<WriteRequest>>) {
        let mut path_buf = PKBUF.take();
        let mut val_buf = PKBUF.take();
        let mut refs = REFS.take();
        for req in writes.drain(..) {
            path_buf.clear();
            val_buf.clear();
            refs.clear();
            match self.published.get(&req.id) {
                None => (),
                Some(Published::Data(val)) => {
                    if let Err(_) = val.path().encode(&mut *path_buf) {
                        continue; // CR estokes: log
                    }
                    if let Err(_) = req.value.encode(&mut *val_buf) {
                        continue; // CR estokes: log
                    }
                    if let Err(_) = self.data.insert(&**path_buf, &**val_buf) {
                        continue; // CR estokes: log
                    }
                    val.update(req.value.clone());
                    if let Some(expr_ids) = self.ctx.user.refs.get(val.path()) {
                        refs.extend(expr_ids.iter().copied());
                    }
                    self.update_expr_ids(&mut refs, &vm::Event::User(req.value));
                }
                Some(Published::FormulaSrc(val, data_id)) => {
                    if let Some(vpath) = Path::dirname(val.path()) {
                        if let Some(fids) = self.published_formulas.remove(vpath) {
                            self.ctx.user.unref(fids.expr_id);
                            self.compiled.remove(&fids.expr_id);
                        }
                        match req.value.cast_to::<Chars>() {
                            Ok(src) => match src.parse::<Expr>() {
                                Ok(expr) => {
                                    let node = Node::compile(&mut self.ctx, expr);
                                    let expr_id = node.id;
                                    
                                }
                                Err(e) => {
                                    let e = Chars::from(format!("{}", e));
                                    val.update(Value::Error(e))
                                }
                            }
                            Err(_) => {
                                val.update(Value::Error(Chars::from("Expected string")));
                            }
                        }
                    }
                }
                Some(Published::FormulaVal(_)) => unimplemented!(),
                Some(Published::OnWrite(_)) => unimplemented!(),
            }
        }
    }

    fn process_publish_request(&mut self, req: (Path, oneshot::Sender<()>)) {
        unimplemented!()
    }

    fn delete_path(&mut self, path: Path) {
        unimplemented!()
    }

    async fn run(mut self) -> Result<()> {
        self.init().await?;
        loop {
            select_biased! {
                r = self.publish_requests.select_next_some() => {
                    self.process_publish_request(r);
                }
                r = self.sub_updates.select_next_some() => {
                    self.process_subscriptions(r);
                }
                r = self.write_updates_rx.select_next_some() => {
                    self.process_writes(r);
                }
                r = self.delete_path_rx.select_next_some() => {
                    self.delete_path(r);
                }
                complete => break
            }
            let timeout = self.cfg.timeout.map(Duration::from_secs);
            self.publisher.flush(timeout).await;
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
