use anyhow::Result;
use daemonize::Daemonize;
use futures::{future::join_all, prelude::*, select_biased};
use log::{error, warn};
use netidx::{
    config::Config,
    path::Path,
    publisher::{BindCfg, DesiredAuth, Publisher},
};
use serde_derive::Deserialize;
use std::{
    collections::HashMap,
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
    task,
    time::{sleep, Instant},
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

#[derive(Debug, Deserialize)]
struct PublisherCfg {
    exe: String,
    args: Vec<String>,
    working_directory: Option<String>,
    uid: Option<u32>,
    gid: Option<u32>,
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

enum ProcStatus {
    NotStarted,
    Died(Instant, ExitStatus),
    Running(Child),
}

async fn run_process(publisher: Publisher, base: Path, cfg: PublisherCfg) -> Result<()> {
    let mut default = publisher.publish_default(base.clone())?;
    let mut proc: ProcStatus = ProcStatus::NotStarted;
    async fn wait(proc: &mut ProcStatus) -> Result<ExitStatus> {
        match proc {
            ProcStatus::Running(proc) => Ok(proc.wait().await?),
            ProcStatus::Died(_, _) | ProcStatus::NotStarted => future::pending().await,
        }
    }
    fn start(proc: &mut ProcStatus, cfg: &PublisherCfg) {
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
            }
        }
    }
    loop {
        select_biased! {
            e = wait(&mut proc).fuse() => match e {
                Err(e) => {
                    error!("failed to wait for base {}, failed with {}", base, e);
                    sleep(Duration::from_secs(1)).await;
                }
                Ok(e) => {
                    warn!("process for base {} shutdown with {:?}", base, e);
                    proc = ProcStatus::Died(Instant::now(), e);
                }
            },
            (_, _) = default.select_next_some() => match &proc {
                ProcStatus::Running(_) => (),
                ProcStatus::NotStarted => start(&mut proc, &cfg),
                ProcStatus::Died(when, _) => match cfg.restart {
                    Restart::No => (),
                    Restart::Yes => start(&mut proc, &cfg),
                    Restart::RateLimited(secs) => {
                        let elapsed = when.elapsed();
                        if elapsed.as_secs_f64() > secs {
                            start(&mut proc, &cfg)
                        } else {
                            sleep(Duration::from_secs_f64(secs) - elapsed).await;
                            start(&mut proc, &cfg)
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
    spec: HashMap<Path, PublisherCfg>,
) -> Result<()> {
    let publisher = Publisher::new(cfg, auth, params.bind).await?;
    join_all(spec.into_iter().map(|(path, cfg)| {
        let publisher = publisher.clone();
        task::spawn(async move {
            if let Err(e) = run_process(publisher, path.clone(), cfg).await {
                error!("handler for base {} died with {}", path, e)
            }
        })
    }))
    .await
    .into_iter()
    .collect::<std::result::Result<(), _>>()?;
    Ok(())
}

pub(super) fn run(cfg: Config, auth: DesiredAuth, params: Params) {
    let spec = read_to_string(&params.publisher).expect("failed to read publishers spec");
    let spec: Vec<(Path, PublisherCfg)> =
        serde_json::from_str(&spec).expect("invalid publishers spec");
    let spec = {
        let mut tbl = HashMap::new();
        for (path, cfg) in spec.into_iter() {
            cfg.validate().expect("invalid publisher specification");
            if tbl.insert(path, cfg).is_some() {
                panic!("publisher paths must be unique")
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
