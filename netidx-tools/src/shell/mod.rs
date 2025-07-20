use anyhow::{Context, Result};
use arcstr::{literal, ArcStr};
use enumflags2::BitFlags;
use flexi_logger::{FileSpec, Logger};
use log::info;
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, DesiredAuth, PublisherBuilder, Value},
    subscriber::SubscriberBuilder,
};
use netidx_bscript::{
    expr::{ExprId, ModPath, ModuleResolver},
    rt::{BSConfig, BSCtx, BSHandle, CompExp, CouldNotResolve, RtEvent},
    typ::{format_with_flags, PrintFlag, TVal, Type},
    ExecCtx, NoUserEvent,
};
use reedline::Signal;
use std::{collections::HashMap, path::PathBuf, sync::LazyLock, time::Duration};
use structopt::StructOpt;
use tokio::{select, sync::mpsc};
use triomphe::Arc;
use tui::Tui;

mod completion;
mod input;
mod tui;

use input::InputReader;

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

type Env = netidx_bscript::env::Env<BSCtx, NoUserEvent>;

const TUITYP: LazyLock<Type> = LazyLock::new(|| Type::Ref {
    scope: ModPath::root(),
    name: ModPath::from(["tui", "Tui"]),
    params: Arc::from_iter([]),
});

async fn bs_init(
    cfg: Config,
    auth: DesiredAuth,
    p: &Params,
    sub: mpsc::Sender<Pooled<Vec<RtEvent>>>,
) -> Result<BSHandle> {
    if let Some(dir) = &p.log_dir {
        let _ = Logger::try_with_env()
            .context("initializing log")?
            .log_to_file(
                FileSpec::default()
                    .directory(dir)
                    .basename("netidx-shell")
                    .use_timestamp(false),
            )
            .start()
            .context("starting log")?;
    }
    info!("netidx shell starting");
    let publisher = PublisherBuilder::new(cfg.clone())
        .bind_cfg(p.bind)
        .build()
        .await
        .context("creating publisher")?;
    let subscriber = SubscriberBuilder::new(cfg)
        .desired_auth(auth)
        .build()
        .context("creating subscriber")?;
    let mut ctx = ExecCtx::new(BSCtx::new(publisher, subscriber));
    let (root, mods) = netidx_bscript_stdlib::register(&mut ctx, BitFlags::all())?;
    let root = ArcStr::from(format!("{root};\nmod tui"));
    let mods = vec![
        mods,
        ModuleResolver::VFS(HashMap::from_iter([
            (Path::from("/tui"), literal!(include_str!("tui/mod.bs"))),
            (
                Path::from("/tui/input_handler"),
                literal!(include_str!("tui/input_handler.bs")),
            ),
            (Path::from("/tui/text"), literal!(include_str!("tui/text.bs"))),
            (Path::from("/tui/paragraph"), literal!(include_str!("tui/paragraph.bs"))),
            (Path::from("/tui/block"), literal!(include_str!("tui/block.bs"))),
            (Path::from("/tui/scrollbar"), literal!(include_str!("tui/scrollbar.bs"))),
            (Path::from("/tui/layout"), literal!(include_str!("tui/layout.bs"))),
            (Path::from("/tui/tabs"), literal!(include_str!("tui/tabs.bs"))),
            (Path::from("/tui/barchart"), literal!(include_str!("tui/barchart.bs"))),
            (Path::from("/tui/chart"), literal!(include_str!("tui/chart.bs"))),
            (Path::from("/tui/sparkline"), literal!(include_str!("tui/sparkline.bs"))),
            (Path::from("/tui/line_gauge"), literal!(include_str!("tui/line_gauge.bs"))),
            (Path::from("/tui/gauge"), literal!(include_str!("tui/gauge.bs"))),
            (Path::from("/tui/list"), literal!(include_str!("tui/list.bs"))),
            (Path::from("/tui/table"), literal!(include_str!("tui/table.bs"))),
            (Path::from("/tui/calendar"), literal!(include_str!("tui/calendar.bs"))),
            (Path::from("/tui/canvas"), literal!(include_str!("tui/canvas.bs"))),
        ])),
    ];
    let mut bs = BSConfig::builder(ctx, sub);
    if let Some(s) = p.publish_timeout {
        bs = bs.publish_timeout(Duration::from_secs(s));
    }
    if let Some(s) = p.subscribe_timeout {
        bs = bs.subscribe_timeout(Duration::from_secs(s));
    }
    Ok(bs
        .root(root)
        .resolvers(mods)
        .build()
        .context("building rt config")?
        .start()
        .await
        .context("loading initial modules")?)
}

enum Output {
    None,
    Tui(Tui),
    Text(CompExp),
}

impl Output {
    fn from_expr(bs: &BSHandle, env: &Env, e: CompExp) -> Self {
        if TUITYP.contains(env, &e.typ).unwrap() {
            Self::Tui(Tui::start(bs, env.clone(), e))
        } else {
            Self::Text(e)
        }
    }

    async fn clear(&mut self) {
        match self {
            Self::None | Self::Text(_) => (),
            Self::Tui(tui) => tui.stop().await,
        }
        *self = Self::None
    }

    async fn process_update(&mut self, env: &Env, id: ExprId, v: Value) {
        match self {
            Self::None => (),
            Self::Tui(tui) => tui.update(id, v).await,
            Self::Text(e) => {
                if e.id == id {
                    println!("{}", TVal { env: &env, typ: &e.typ, v: &v })
                }
            }
        }
    }
}

async fn load_initial_env(
    bs: &BSHandle,
    p: &Params,
    newenv: &mut Option<Env>,
    output: &mut Output,
    exprs: &mut Vec<CompExp>,
) -> Result<Env> {
    let env;
    *newenv = if let Some(file) = p.file.as_ref() {
        let r = bs.load(file.clone()).await?;
        exprs.extend(r.exprs);
        env = bs.get_env().await?;
        if let Some(e) = exprs.pop() {
            *output = Output::from_expr(&bs, &env, e);
        }
        None
    } else if !p.no_init {
        match bs.compile("mod init".into()).await {
            Ok(res) => {
                env = res.env;
                exprs.extend(res.exprs);
                Some(env.clone())
            }
            Err(e) if e.is::<CouldNotResolve>() => {
                env = bs.get_env().await?;
                Some(env.clone())
            }
            Err(e) => {
                eprintln!("error in init module: {e:?}");
                env = bs.get_env().await?;
                Some(env.clone())
            }
        }
    } else {
        env = bs.get_env().await?;
        Some(env.clone())
    };
    Ok(env)
}

pub(super) async fn run(cfg: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let (tx, mut from_bs) = mpsc::channel(100);
    let bs = bs_init(cfg, auth, &p, tx).await?;
    let script = p.file.is_some();
    let mut input = InputReader::new();
    let mut output = Output::None;
    let mut newenv = None;
    let mut exprs = vec![];
    let mut env = load_initial_env(&bs, &p, &mut newenv, &mut output, &mut exprs).await?;
    if !script {
        println!("Welcome to the netidx shell");
        println!("Press ctrl-c to cancel, ctrl-d to exit, and tab for help")
    }
    loop {
        select! {
            batch = from_bs.recv() => match batch {
                None => bail!("bscript runtime is dead"),
                Some(mut batch) => {
                    for e in batch.drain(..) {
                        match e {
                            RtEvent::Updated(id, v) => output.process_update(&env, id, v).await,
                            RtEvent::Env(e) => {
                                env = e;
                                newenv = Some(env.clone());
                            }
                        }
                    }
                }
            },
            input = input.read_line(&mut output, &mut newenv) => {
                match input {
                    Err(e) => eprintln!("error reading line {e:?}"),
                    Ok(Signal::CtrlC) if script => break Ok(()),
                    Ok(Signal::CtrlC) => output.clear().await,
                    Ok(Signal::CtrlD) => break Ok(()),
                    Ok(Signal::Success(line)) => {
                        match bs.compile(ArcStr::from(line)).await {
                            Err(e) => eprintln!("error: {e:?}"),
                            Ok(res) => {
                                env = res.env;
                                newenv = Some(env.clone());
                                exprs.extend(res.exprs);
                                if exprs.last().map(|e| e.output).unwrap_or(false) {
                                    let e = exprs.pop().unwrap();
                                    let typ = e.typ
                                        .with_deref(|t| t.cloned())
                                        .unwrap_or_else(|| e.typ.clone());
                                    format_with_flags(
                                        PrintFlag::DerefTVars | PrintFlag::ReplacePrims,
                                        || println!("-: {}", typ)
                                    );
                                    output = Output::from_expr(&bs, &env, e);
                                } else {
                                    output.clear().await
                                }
                            }
                        }
                    }
                }
            },
        }
    }
}
