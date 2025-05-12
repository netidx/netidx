use anyhow::{Context, Error, Result};
use arcstr::ArcStr;
use completion::BComplete;
use flexi_logger::{FileSpec, Logger};
use futures::{channel::mpsc, StreamExt};
use log::info;
use netidx::{
    config::Config,
    publisher::{BindCfg, DesiredAuth, PublisherBuilder},
    subscriber::SubscriberBuilder,
};
use netidx_bscript::{
    env::Env,
    rt::{BSConfigBuilder, BSCtx, CompExp, CouldNotResolve, RtEvent},
    typ::TVal,
    NoUserEvent,
};
use reedline::{
    default_emacs_keybindings, DefaultPrompt, DefaultPromptSegment, Emacs, IdeMenu,
    KeyCode, KeyModifiers, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal,
};
use std::{path::PathBuf, time::Duration};
use structopt::StructOpt;
use tokio::{select, sync::oneshot, task};
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

type MaybeEnv = Option<Env<BSCtx, NoUserEvent>>;

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

pub(super) async fn run(cfg: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    if let Some(dir) = p.log_dir {
        let _ = Logger::try_with_env()
            .context("initializing log")?
            .log_to_file(
                FileSpec::default()
                    .directory(dir)
                    .basename("netidx-shell")
                    .use_timestamp(true),
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
    let script = p.file.is_some();
    let mut input = InputReader::new();
    let mut bs = BSConfigBuilder::default();
    bs.publisher(publisher).subscriber(subscriber);
    if let Some(s) = p.publish_timeout {
        bs.publish_timeout(Duration::from_secs(s));
    }
    if let Some(s) = p.subscribe_timeout {
        bs.subscribe_timeout(Duration::from_secs(s));
    }
    let bs = bs.build().context("building rt config")?.start();
    let (tx, mut output) = mpsc::channel(3);
    bs.subscribe(tx).context("subscribing to rt output")?;
    let mut output_expr: Option<CompExp> = None;
    let mut newenv = if let Some(file) = p.file.as_ref() {
        let mut r = bs.load(file.clone()).await?;
        if let Some(e) = r.exprs.pop() {
            output_expr = Some(e);
        }
        None
    } else if !p.no_init {
        match bs.compile("mod init;".into()).await {
            Ok(res) => Some(res.env),
            Err(e) if e.is::<CouldNotResolve>() => Some(bs.get_env().await?),
            Err(e) => {
                eprintln!("error in init module: {e:?}");
                Some(bs.get_env().await?)
            }
        }
    } else {
        Some(bs.get_env().await?)
    };
    loop {
        select! {
            RtEvent::Updated(id, v) = output.select_next_some() => {
                if let Some(e) = &output_expr {
                    if e.id == id {
                        println!("{}", TVal(&e.typ, &v))
                    }
                }
            },
            input = input.read_line(output_expr.is_some(), newenv.take()) => {
                match input {
                    Err(e) => eprintln!("error reading line {e:?}"),
                    Ok(Signal::CtrlC) if script => break Ok(()),
                    Ok(Signal::CtrlC) => {
                        if let Some(e) = output_expr.take() {
                            match bs.delete(e.id).await {
                                Err(e) => eprintln!("could not delete node {e:?}"),
                                Ok(env) => newenv = Some(env),
                            }
                        }
                    }
                    Ok(Signal::CtrlD) => break Ok(()),
                    Ok(Signal::Success(line)) => {
                        match bs.compile(ArcStr::from(line)).await {
                            Err(e) => eprintln!("error: {e:?}"),
                            Ok(mut res) => {
                                newenv = Some(res.env);
                                if let Some(e) = res.exprs.pop() {
                                    println!("-: {}", e.typ);
                                    if e.output {
                                        output_expr = Some(e);
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }
    }
}
