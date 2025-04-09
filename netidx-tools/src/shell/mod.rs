use anyhow::{Context, Error, Result};
use arcstr::ArcStr;
use completion::BComplete;
use core::fmt;
use flexi_logger::{FileSpec, Logger};
use futures::{channel::mpsc, stream, FutureExt, Stream, StreamExt};
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use indexmap::IndexMap;
use log::{error, info};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{
        self, BindCfg, DesiredAuth, Id, PublishFlags, Publisher, PublisherBuilder, Val,
        Value, WriteRequest,
    },
    resolver_client::ChangeTracker,
    subscriber::{self, Dval, SubId, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use netidx_bscript::{
    env::Env,
    expr::{self, Expr, ExprId, ExprKind, ModPath, ModuleResolver},
    node::{Node, NodeKind},
    BindId, Ctx, Event, ExecCtx, NoUserEvent,
};
use reedline::{
    default_emacs_keybindings, DefaultPrompt, DefaultPromptSegment, Emacs, IdeMenu,
    KeyCode, KeyModifiers, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal,
};
use smallvec::SmallVec;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    future,
    os::unix::ffi::OsStrExt,
    path::{Component, PathBuf},
    pin::Pin,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    fs, select,
    sync::oneshot,
    task,
    time::{self, Sleep},
};
mod completion;

#[derive(StructOpt, Debug)]
pub(crate) struct Params {
    #[structopt(
        long = "no-init",
        help = "do not attempt to load the init module in repl mode"
    )]
    no_init: bool,
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
    #[structopt(long = "log-dir", help = "log messages to the specified directory")]
    log_dir: Option<PathBuf>,
    #[structopt(name = "file", help = "script file or module to execute")]
    file: Option<PathBuf>,
}

type UpdateBatch = Pooled<Vec<(SubId, subscriber::Event)>>;
type WriteBatch = Pooled<Vec<WriteRequest>>;

#[derive(Debug)]
struct CouldNotResolve;

impl fmt::Display for CouldNotResolve {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "could not resolve module")
    }
}

struct ReplCtx {
    by_ref: FxHashMap<BindId, SmallVec<[ExprId; 3]>>,
    var_updates: VecDeque<(BindId, Value)>,
    net_updates: VecDeque<(SubId, subscriber::Event)>,
    subscribed: FxHashMap<SubId, SmallVec<[ExprId; 3]>>,
    published: FxHashMap<Id, Val>,
    change_trackers: FxHashMap<BindId, ChangeTracker>,
    list_queue: VecDeque<(BindId, Path, bool)>,
    cached: FxHashMap<BindId, Value>,
    batch: publisher::UpdateBatch,
    timers: stream::SelectAll<Pin<Box<dyn Stream<Item = BindId> + Send + Sync>>>,
    script: bool,
    publisher: Publisher,
    subscriber: Subscriber,
    updates_tx: mpsc::Sender<UpdateBatch>,
    updates: mpsc::Receiver<UpdateBatch>,
    writes_tx: mpsc::Sender<WriteBatch>,
    writes: mpsc::Receiver<WriteBatch>,
}

impl ReplCtx {
    fn new(publisher: Publisher, subscriber: Subscriber, script: bool) -> Self {
        let (updates_tx, updates) = mpsc::channel(3);
        let (writes_tx, writes) = mpsc::channel(3);
        let batch = publisher.start_batch();
        Self {
            by_ref: HashMap::default(),
            var_updates: VecDeque::new(),
            net_updates: VecDeque::new(),
            subscribed: HashMap::default(),
            published: HashMap::default(),
            list_queue: VecDeque::new(),
            change_trackers: HashMap::default(),
            cached: HashMap::default(),
            timers: stream::SelectAll::new(),
            batch,
            script,
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
        todo!()
    }

    fn clear(&mut self) {
        self.by_ref.clear();
        self.var_updates.clear();
    }

    fn subscribe(&mut self, flags: UpdatesFlags, path: Path, ref_by: ExprId) -> Dval {
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

    fn list(&mut self, id: BindId, path: Path) {
        self.list_queue.push_back((id, path, false));
    }

    fn list_table(&mut self, id: BindId, path: Path) {
        self.list_queue.push_back((id, path, true));
    }

    fn stop_list(&mut self, id: BindId) {
        self.change_trackers.remove(&id);
        self.list_queue.retain(|(i, _, _)| *i != id);
    }

    fn publish(&mut self, path: Path, value: Value, _ref_by: ExprId) -> Result<Id> {
        let val = self.publisher.publish_with_flags_and_writes(
            PublishFlags::empty(),
            path,
            value,
            Some(self.writes_tx.clone()),
        )?;
        let id = val.id();
        self.published.insert(id, val);
        Ok(id)
    }

    fn update(&mut self, id: Id, value: Value) {
        if let Some(val) = self.published.get(&id) {
            val.update(&mut self.batch, value);
        }
    }

    fn unpublish(&mut self, id: Id) {
        self.published.remove(&id);
    }

    fn set_timer(&mut self, id: BindId, timeout: Duration, _ref_by: ExprId) {
        let done: Pin<Box<dyn Stream<Item = BindId> + Send + Sync>> =
            Box::pin(time::sleep(timeout).into_stream().map(move |()| id));
        self.timers.push(done);
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
        self.var_updates.push_back((id, value.clone()));
        if !self.script {
            self.cached.insert(id, value);
        }
    }
}

type MaybeEnv = Option<Env<ReplCtx, NoUserEvent>>;

struct InputReader {
    go: Option<oneshot::Sender<MaybeEnv>>,
    recv: mpsc::UnboundedReceiver<(oneshot::Sender<MaybeEnv>, Result<Signal>)>,
}

impl InputReader {
    fn run(
        mut c_rx: oneshot::Receiver<MaybeEnv>,
    ) -> mpsc::UnboundedReceiver<(oneshot::Sender<MaybeEnv>, Result<Signal>)> {
        let (tx, rx) = mpsc::unbounded();
        task::spawn(async move {
            let mut keybinds = default_emacs_keybindings();
            keybinds.add_binding(
                KeyModifiers::NONE,
                KeyCode::Tab,
                ReedlineEvent::UntilFound(vec![
                    ReedlineEvent::Menu("completion".into()),
                    ReedlineEvent::MenuNext,
                ]),
            );
            let menu = IdeMenu::default().with_name("completion");
            let mut line_editor = Reedline::create()
                .with_menu(ReedlineMenu::EngineCompleter(Box::new(menu)))
                .with_edit_mode(Box::new(Emacs::new(keybinds)));
            let prompt = DefaultPrompt {
                left_prompt: DefaultPromptSegment::Basic("".into()),
                right_prompt: DefaultPromptSegment::Empty,
            };
            loop {
                match c_rx.await {
                    Err(_) => break, // shutting down
                    Ok(None) => (),
                    Ok(Some(env)) => {
                        line_editor =
                            line_editor.with_completer(Box::new(BComplete(env)));
                    }
                }
                let r = task::block_in_place(|| {
                    line_editor.read_line(&prompt).map_err(Error::from)
                });
                let (o_tx, o_rx) = oneshot::channel();
                c_rx = o_rx;
                if let Err(_) = tx.unbounded_send((o_tx, r)) {
                    break;
                }
            }
        });
        rx
    }

    fn new() -> Self {
        let (tx_go, rx_go) = oneshot::channel();
        let recv = Self::run(rx_go);
        Self { go: Some(tx_go), recv }
    }

    async fn read_line(&mut self, output: bool, env: MaybeEnv) -> Result<Signal> {
        if output {
            tokio::signal::ctrl_c().await?;
            Ok(Signal::CtrlC)
        } else {
            if let Some(tx) = self.go.take() {
                let _ = tx.send(env);
            }
            match self.recv.next().await {
                None => bail!("input stream ended"),
                Some((go, sig)) => {
                    self.go = Some(go);
                    sig
                }
            }
        }
    }
}

fn is_output(n: &Node<ReplCtx, NoUserEvent>) -> bool {
    match &n.kind {
        NodeKind::Bind { .. }
        | NodeKind::Lambda(_)
        | NodeKind::Use { .. }
        | NodeKind::Connect(_, _)
        | NodeKind::Module(_)
        | NodeKind::TypeDef { .. } => false,
        _ => true,
    }
}

#[derive(Clone, Copy)]
enum Init {
    None,
    One(ExprId),
    All,
}

struct Repl {
    ctx: ExecCtx<ReplCtx, NoUserEvent>,
    event: Event<NoUserEvent>,
    updated: FxHashSet<ExprId>,
    nodes: IndexMap<ExprId, Node<ReplCtx, NoUserEvent>, FxBuildHasher>,
    input: InputReader,
    resolvers: Vec<ModuleResolver>,
}

impl Repl {
    fn new(
        publisher: Publisher,
        subscriber: Subscriber,
        script: bool,
        subscribe_timeout: Option<Duration>,
    ) -> Result<Self> {
        let resolvers_default = || match dirs::data_dir() {
            None => vec![ModuleResolver::Files("".into())],
            Some(dd) => vec![
                ModuleResolver::Files("".into()),
                ModuleResolver::Files(dd.join("bscript")),
            ],
        };
        let resolvers = match std::env::var("BSCRIPT_MODPATH") {
            Err(_) => resolvers_default(),
            Ok(mp) => match ModuleResolver::parse_env(
                subscriber.clone(),
                subscribe_timeout,
                &mp,
            ) {
                Ok(r) => r,
                Err(e) => {
                    error!("failed to parse BSCRIPT_MODPATH, using default {e:?}");
                    resolvers_default()
                }
            },
        };
        Ok(Self {
            ctx: ExecCtx::new(ReplCtx::new(publisher, subscriber, script)),
            event: Event::new(NoUserEvent),
            updated: HashSet::default(),
            nodes: IndexMap::default(),
            input: InputReader::new(),
            resolvers,
        })
    }

    fn do_cycle(
        &mut self,
        init: Init,
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
        match init {
            Init::None => (),
            Init::One(id) => {
                self.updated.insert(id);
            }
            Init::All => {
                for id in self.nodes.keys() {
                    self.updated.insert(*id);
                }
            }
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
                macro_rules! do_init {
                    () => {{
                        self.event.init = true;
                        n.refs(&mut |id| {
                            if let Some(v) = self.ctx.user.cached.get(&id) {
                                if let Entry::Vacant(e) = self.event.variables.entry(id) {
                                    e.insert(v.clone());
                                }
                            }
                        });
                    }};
                }
                match init {
                    Init::All => do_init!(),
                    Init::One(i) if i == *id => do_init!(),
                    Init::One(_) | Init::None => {
                        self.event.init = false;
                    }
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

    fn cycle_ready(&self) -> bool {
        self.ctx.user.var_updates.len() > 0 || self.ctx.user.net_updates.len() > 0
    }

    async fn compile(
        &mut self,
        line: &str,
        init: &mut Init,
        output: &mut bool,
    ) -> Result<()> {
        let scope = ModPath::root();
        let spec = match line.parse::<Expr>() {
            Ok(spec) => spec,
            Err(_) => expr::parser::parse_expr(&line)?,
        };
        let spec = spec
            .resolve_modules(&scope, &self.resolvers)
            .await
            .context(CouldNotResolve)?;
        let top_id = spec.id;
        let n = Node::compile(&mut self.ctx, &scope, spec);
        match n.extract_err() {
            Some(e) => bail!("compile error: {e}"),
            None => {
                *output = is_output(&n);
                *init = Init::One(top_id);
                self.nodes.insert(top_id, n);
            }
        }
        Ok(())
    }

    async fn load(&mut self, file: &PathBuf) -> Result<()> {
        let scope = ModPath::root();
        let (scope, exprs) = match file.extension() {
            Some(e) if e.as_bytes() == b"bs" => {
                let s = fs::read_to_string(file).await?;
                (scope, expr::parser::parse_many_modexpr(&s)?)
            }
            Some(e) => bail!("invalid file extension {e:?}"),
            None => {
                let name = file
                    .components()
                    .map(|c| match c {
                        Component::RootDir
                        | Component::CurDir
                        | Component::ParentDir
                        | Component::Prefix(_) => bail!("invalid module name {file:?}"),
                        Component::Normal(s) => Ok(s),
                    })
                    .collect::<Result<Box<[_]>>>()?;
                if name.len() != 1 {
                    bail!("invalid module name {file:?}")
                }
                let name = String::from_utf8_lossy(name[0].as_bytes());
                let name = name
                    .parse::<ModPath>()
                    .with_context(|| "parsing module name {file:?}")?;
                let scope =
                    ModPath(Path::from_str(Path::dirname(&*name).unwrap_or(&*scope)));
                let name = Path::basename(&*name)
                    .ok_or_else(|| anyhow!("invalid module name {file:?}"))?;
                let name = ArcStr::from(name);
                let e = ExprKind::Module { export: true, name, value: None }.to_expr();
                (scope, vec![e])
            }
        };
        for expr in exprs {
            let expr = expr.resolve_modules(&scope, &self.resolvers).await?;
            let top_id = expr.id;
            let n = Node::compile(&mut self.ctx, &scope, expr);
            if let Some(e) = n.extract_err() {
                bail!("{e:?}")
            }
            self.nodes.insert(top_id, n);
        }
        if let Some(v) = self.do_cycle(Init::All, None, None) {
            println!("{v}")
        }
        Ok(())
    }
}

async fn or_never(b: bool) {
    if !b {
        future::pending().await
    }
}

pub(super) async fn run(cfg: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    if let Some(dir) = p.log_dir {
        let _ = Logger::try_with_env()?
            .log_to_file(
                FileSpec::default()
                    .directory(dir)
                    .basename("netidx-shell")
                    .use_timestamp(true),
            )
            .start()?;
    }
    info!("netidx shell starting");
    let publisher = PublisherBuilder::new(cfg.clone()).bind_cfg(p.bind).build().await?;
    let subscriber = SubscriberBuilder::new(cfg).desired_auth(auth).build()?;
    let script = p.file.is_some();
    let mut repl = Repl::new(
        publisher,
        subscriber,
        script,
        p.subscribe_timeout.map(|t| Duration::from_secs(t)),
    )?;
    let mut output = script;
    if let Some(file) = p.file.as_ref() {
        repl.load(file).await?
    } else if !p.no_init {
        match repl.compile("mod init;", &mut Init::None, &mut false).await {
            Err(e) if e.is::<CouldNotResolve>() => (),
            Err(e) => {
                eprintln!("error in init module: {e:?}")
            }
            Ok(()) => {
                if let Err(e) =
                    repl.compile("use init", &mut Init::None, &mut false).await
                {
                    eprintln!("error in init module: {e:?}");
                }
                let _ = repl.do_cycle(Init::All, None, None);
            }
        }
    }
    let mut newenv = (!script).then_some(repl.ctx.env.clone());
    loop {
        let ready = repl.cycle_ready();
        let mut updates = None;
        let mut writes = None;
        let mut input = None;
        let mut init = Init::None;
        select! {
            wr = repl.ctx.user.writes.select_next_some() => {
                writes = Some(wr);
                if let Ok(Some(up)) = repl.ctx.user.updates.try_next() {
                    updates = Some(up);
                }
            },
            up = repl.ctx.user.updates.select_next_some() => {
                updates = Some(up);
                if let Ok(Some(wr)) = repl.ctx.user.writes.try_next() {
                    writes = Some(wr);
                }
            },
            i = repl.input.read_line(output, newenv.take()) => match i {
                Ok(i) => {
                    input = Some(i);
                },
                Err(e) => {
                    eprintln!("error reading line {e:?}");
                }
            },
            _ = or_never(ready) => {
                if let Ok(Some(up)) = repl.ctx.user.updates.try_next() {
                    updates = Some(up);
                }
                if let Ok(Some(wr)) = repl.ctx.user.writes.try_next() {
                    writes = Some(wr);
                }
            }
        }
        match input {
            None => (),
            Some(Signal::CtrlC) if script => break Ok(()),
            Some(Signal::CtrlC) => {
                output = false;
                if let Some((_, n)) = repl.nodes.last() {
                    if is_output(n) {
                        let (_, n) = repl.nodes.pop().unwrap();
                        n.delete(&mut repl.ctx)
                    }
                }
            }
            Some(Signal::CtrlD) => break Ok(()),
            Some(Signal::Success(line)) => {
                match repl.compile(&line, &mut init, &mut output).await {
                    Err(e) => eprintln!("error: {e:?}"),
                    Ok(()) => {
                        newenv = Some(repl.ctx.env.clone());
                    }
                };
            }
        }
        if let Some(v) = repl.do_cycle(init, updates, writes) {
            if output {
                println!("{v}")
            }
        }
    }
}
