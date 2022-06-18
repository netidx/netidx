use anyhow::Result;
use daemonize::Daemonize;
use futures::{future::join_all, prelude::*, select_biased, stream::SelectAll};
use log::{error, warn};
use netidx::{
    config::Config,
    path::Path,
    publisher::{BindCfg, DefaultHandle, DesiredAuth, Publisher},
};
use serde_derive::Deserialize;
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, read_to_string},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::ExitStatus,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    process::{Child, Command},
    runtime::Runtime,
    signal::unix::{signal, SignalKind},
    sync::broadcast,
    task,
    time::{sleep, timeout, Instant},
};

#[derive(StructOpt, Debug)]
pub(super) struct Params {
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. local, 192.168.0.0/16",
        default_value = "local"
    )]
    bind: BindCfg,
    #[structopt(
        short = "p",
        long = "publishers",
        help = "path to the publishers definition file"
    )]
    publisher: PathBuf,
    #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
    foreground: bool,
    #[structopt(long = "pid-file", help = "write the pid to file")]
    pid_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
enum Restart {
    No,
    Yes,
    RateLimited(f64),
}

impl Default for Restart {
    fn default() -> Self {
        Self::RateLimited(1.)
    }
}

#[derive(Debug, Deserialize)]
struct PublisherCfg {
    exe: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    working_directory: Option<String>,
    #[serde(default)]
    uid: Option<u32>,
    #[serde(default)]
    gid: Option<u32>,
    #[serde(default)]
    restart: Restart,
}

impl PublisherCfg {
    fn validate(&self) -> Result<()> {
        let md = fs::metadata(&self.exe)?;
        if !md.is_file() {
            bail!("exe must be a file")
        }
        if md.permissions().mode() & 0b0000_0000_0100_1001 == 0 {
            bail!("exe must be executable")
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CfgPair {
    paths: Vec<Path>,
    cfg: PublisherCfg,
}

enum ProcStatus {
    NotStarted,
    Died(Instant),
    Running(Child),
}

async fn run_process(
    publisher: Publisher,
    base: BTreeSet<Path>,
    cfg: PublisherCfg,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let mut proc: ProcStatus = ProcStatus::NotStarted;
    let mut default: Option<SelectAll<DefaultHandle>> = Some(publish(&publisher, &base)?);
    fn publish(
        publisher: &Publisher,
        base: &BTreeSet<Path>,
    ) -> Result<SelectAll<DefaultHandle>> {
        Ok(base
            .iter()
            .map(|path| publisher.publish_default(path.clone()))
            .collect::<Result<SelectAll<_>>>()?)
    }
    async fn wait_proc(proc: &mut ProcStatus) -> Result<ExitStatus> {
        match proc {
            ProcStatus::Running(proc) => Ok(proc.wait().await?),
            ProcStatus::Died(_) | ProcStatus::NotStarted => future::pending().await,
        }
    }
    async fn wait_default(default: &mut Option<SelectAll<DefaultHandle>>) {
        match default {
            Some(default) => {
                let _ = default.select_next_some().await;
            }
            None => future::pending().await,
        }
    }
    fn start(
        proc: &mut ProcStatus,
        default: &mut Option<SelectAll<DefaultHandle>>,
        cfg: &PublisherCfg,
    ) {
        let mut c = Command::new(&cfg.exe);
        c.args(cfg.args.iter());
        if let Some(dir) = cfg.working_directory.as_ref() {
            c.current_dir(dir);
        }
        if let Some(uid) = cfg.uid {
            c.uid(uid);
        }
        if let Some(gid) = cfg.gid {
            c.gid(gid);
        }
        match c.spawn() {
            Err(e) => error!("failed to spawn process {} failed with {}", cfg.exe, e),
            Ok(child) => {
                *proc = ProcStatus::Running(child);
                *default = None;
            }
        }
    }
    loop {
        select_biased! {
            _ = shutdown.recv().fuse() => match &mut proc {
                ProcStatus::NotStarted | ProcStatus::Died(_) => break Ok(()),
                ProcStatus::Running(child) => {
                    match child.id() {
                        None => {
                            let _ = child.kill().await;
                        }
                        Some(pid) => {
                            let pid = nix::unistd::Pid::from_raw(pid as i32);
                            let term = nix::sys::signal::Signal::SIGTERM;
                            let _ = nix::sys::signal::kill(pid, Some(term));
                            let _ = timeout(Duration::from_secs(30), child.wait()).await;
                            let _ = child.kill().await;
                        }
                    }
                    break Ok(())
                }
            },
            e = wait_proc(&mut proc).fuse() => match e {
                Err(e) => {
                    error!("failed to wait for base {:?}, failed with {}", base, e);
                    sleep(Duration::from_secs(1)).await;
                }
                Ok(e) => {
                    warn!("process for base {:?} shutdown with {:?}", base, e);
                    proc = ProcStatus::Died(Instant::now());
                    default = Some(publish(&publisher, &base)?);
                }
            },
            () = wait_default(&mut default).fuse() => match &proc {
                ProcStatus::Running(_) => (),
                ProcStatus::NotStarted => start(&mut proc, &mut default, &cfg),
                ProcStatus::Died(when) => match cfg.restart {
                    Restart::No => (),
                    Restart::Yes => start(&mut proc, &mut default, &cfg),
                    Restart::RateLimited(secs) => {
                        let elapsed = when.elapsed();
                        if elapsed.as_secs_f64() > secs {
                            start(&mut proc, &mut default, &cfg)
                        } else {
                            sleep(Duration::from_secs_f64(secs) - elapsed).await;
                            start(&mut proc, &mut default, &cfg)
                        }
                    }
                }
            },
            complete => bail!("default handle finished"),
        }
    }
}

async fn run_server(
    cfg: Config,
    auth: DesiredAuth,
    params: Params,
    spec: HashMap<BTreeSet<Path>, PublisherCfg>,
) -> Result<()> {
    let publisher = Publisher::new(cfg, auth, params.bind).await?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigquit = signal(SignalKind::quit())?;
    let (tx, _rx) = broadcast::channel(10);
    let tasks = spec
        .into_iter()
        .map(|(path, cfg)| {
            let publisher = publisher.clone();
            let rx = tx.subscribe();
            task::spawn(async move {
                if let Err(e) = run_process(publisher, path.clone(), cfg, rx).await {
                    error!("handler for base {:?} died with {}", path, e)
                }
            })
        })
        .collect::<Vec<_>>();
    let shutdown = || async move {
        let _ = tx.send(());
        let _ = join_all(tasks).await;
        Ok(())
    };
    loop {
        select_biased! {
            _ = sigint.recv().fuse() => break shutdown().await,
            _ = sigterm.recv().fuse() => break shutdown().await,
            _ = sigquit.recv().fuse() => break shutdown().await,
            complete => break shutdown().await,
        }
    }
}

pub(super) fn run(cfg: Config, auth: DesiredAuth, params: Params) {
    let spec = read_to_string(&params.publisher).expect("failed to read publishers spec");
    let spec: Vec<CfgPair> =
        serde_json::from_str(&spec).expect("invalid publishers spec");
    let spec = {
        let mut tbl = HashMap::new();
        for cfg in spec.into_iter() {
            let mut pset = BTreeSet::new();
            cfg.cfg.validate().expect("invalid publisher specification");
            for path in cfg.paths {
                if !Path::is_absolute(&path) {
                    panic!("absolute paths are required")
                }
                if !pset.insert(path) {
                    panic!("paths within a group must be unique")
                }
            }
            if tbl.insert(pset, cfg.cfg).is_some() {
                panic!("publisher path sets must be unique")
            }
        }
        tbl
    };
    if !params.foreground {
        let mut d = Daemonize::new();
        if let Some(pid_file) = params.pid_file.as_ref() {
            d = d.pid_file(pid_file);
        }
        d.start().expect("failed to daemonize")
    }
    Runtime::new().expect("failed to init runtime").block_on(async move {
        if let Err(e) = run_server(cfg, auth, params, spec).await {
            error!("server task shutdown: {}", e)
        }
    });
}
