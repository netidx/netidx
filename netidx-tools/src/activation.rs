use anyhow::Result;
use daemonize::Daemonize;
use futures::{future::join_all, prelude::*, select_biased, stream::SelectAll};
use log::{error, info, warn};
use netidx::{
    config::Config,
    path::Path,
    publisher::{BindCfg, DefaultHandle, DesiredAuth, Publisher, PublisherBuilder},
};
use serde_derive::Deserialize;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::ExitStatus,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    fs,
    process::{Child, Command},
    runtime::Runtime,
    signal::unix::{signal, SignalKind},
    sync::mpsc,
    task,
    time::{sleep, timeout, Instant},
};

#[derive(StructOpt, Debug)]
pub(super) struct Params {
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. local, 192.168.0.0/16"
    )]
    bind: Option<BindCfg>,
    #[structopt(
        short = "u",
        long = "units",
        help = "path to directory containing the unit files to manage"
    )]
    units: Option<PathBuf>,
    #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
    foreground: bool,
    #[structopt(long = "pid-file", help = "write the pid to file")]
    pid_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
enum Environment {
    Inherit(HashMap<String, String>),
    Replace(HashMap<String, String>),
}

impl Default for Environment {
    fn default() -> Self {
        Self::Inherit(HashMap::default())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ProcessCfg {
    exe: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    working_directory: Option<PathBuf>,
    #[serde(default)]
    uid: Option<u32>,
    #[serde(default)]
    gid: Option<u32>,
    #[serde(default)]
    restart: Restart,
    #[serde(default)]
    stdin: Option<PathBuf>,
    #[serde(default)]
    stdout: Option<PathBuf>,
    #[serde(default)]
    stderr: Option<PathBuf>,
    #[serde(default)]
    environment: Environment,
}

impl ProcessCfg {
    async fn validate(&self) -> Result<()> {
        let md = fs::metadata(&self.exe).await?;
        if !md.is_file() {
            bail!("exe must be a file")
        }
        if md.permissions().mode() & 0b0000_0000_0100_1001 == 0 {
            bail!("exe must be executable")
        }
        Ok(())
    }

    fn command(&self) -> Result<Command> {
        use std::fs;
        let mut c = Command::new(&self.exe);
        c.args(self.args.iter());
        if let Some(dir) = self.working_directory.as_ref() {
            c.current_dir(dir);
        }
        if let Some(uid) = self.uid {
            c.uid(uid);
        }
        if let Some(gid) = self.gid {
            c.gid(gid);
        }
        if let Some(stdin) = &self.stdin {
            c.stdin(fs::File::open(stdin)?);
        }
        if let Some(stdout) = &self.stdout {
            c.stdout(fs::OpenOptions::new().write(true).append(true).open(stdout)?);
        }
        if let Some(stderr) = &self.stderr {
            c.stderr(fs::OpenOptions::new().write(true).append(true).open(stderr)?);
        }
        match &self.environment {
            Environment::Inherit(overrides) => {
                c.envs(overrides.iter());
            }
            Environment::Replace(vars) => {
                c.env_clear();
                c.envs(vars.iter());
            }
        }
        Ok(c)
    }
}

#[derive(Debug, Clone, Deserialize)]
enum Trigger {
    OnStart,
    OnAccess(BTreeSet<Path>),
}

impl Default for Trigger {
    fn default() -> Self {
        Self::OnStart
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Unit {
    #[serde(default)]
    trigger: Trigger,
    process: ProcessCfg,
}

impl Unit {
    async fn load(path: Option<&PathBuf>) -> Result<HashMap<String, Unit>> {
        let path = path
            .cloned()
            .or_else(|| {
                dirs::config_dir().and_then(|mut p| {
                    p.push("netidx");
                    p.push("activation");
                    if task::block_in_place(|| std::path::Path::is_dir(&p)) {
                        Some(p)
                    } else {
                        None
                    }
                })
            })
            .or_else(|| {
                let p = PathBuf::from("/etc/netidx/activation");
                if task::block_in_place(|| std::path::Path::is_dir(&p)) {
                    Some(p)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                anyhow!("no unit directory specified and no default was found")
            })?;
        let units = {
            let mut hm: HashMap<String, Unit> = HashMap::new();
            let mut dirs = fs::read_dir(path).await?;
            while let Some(ent) = dirs.next_entry().await? {
                let name = ent.file_name().to_string_lossy().into_owned();
                let typ = ent.file_type().await?;
                if (typ.is_file() || typ.is_symlink()) && name.ends_with(".unit") {
                    match serde_json::from_str(&fs::read_to_string(ent.path()).await?) {
                        Ok(u) => {
                            hm.insert(ent.file_name().to_string_lossy().into_owned(), u);
                        }
                        Err(e) => {
                            error!(
                                "invalid unit definition {}: {}",
                                ent.path().to_string_lossy(),
                                e
                            )
                        }
                    }
                }
            }
            hm
        };
        let mut triggers = HashSet::new();
        for unit in units.values() {
            unit.process.validate().await?;
            match &unit.trigger {
                Trigger::OnStart => (),
                Trigger::OnAccess(paths) => {
                    for path in paths {
                        if !triggers.insert(path.clone()) {
                            bail!("conflicting unit trigger {}", path)
                        }
                    }
                }
            }
        }
        Ok(units)
    }
}

enum ProcStatus {
    NotStarted,
    Died(Instant),
    Running(Child),
}

enum ToProcess {
    Shutdown,
    Reconfigure(Unit),
}

struct Process {
    join_handle: task::JoinHandle<()>,
    tx: mpsc::UnboundedSender<ToProcess>,
}

impl Process {
    async fn run(
        publisher: Publisher,
        name: String,
        mut unit: Unit,
        mut rx: mpsc::UnboundedReceiver<ToProcess>,
    ) -> Result<()> {
        let mut proc: ProcStatus = ProcStatus::NotStarted;
        let mut default: Option<SelectAll<DefaultHandle>> =
            publish(&publisher, &unit.trigger)?;
        fn publish(
            publisher: &Publisher,
            trigger: &Trigger,
        ) -> Result<Option<SelectAll<DefaultHandle>>> {
            match trigger {
                Trigger::OnStart => Ok(None),
                Trigger::OnAccess(paths) => {
                    let handles = paths
                        .iter()
                        .map(|path| publisher.publish_default(path.clone()))
                        .collect::<Result<SelectAll<_>>>()?;
                    Ok(Some(handles))
                }
            }
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
            cfg: &ProcessCfg,
        ) {
            match task::block_in_place(|| cfg.command()) {
                Err(e) => error!("failed to setup process {} failed with {}", cfg.exe, e),
                Ok(mut c) => match c.spawn() {
                    Err(e) => {
                        error!("failed to spawn process {} failed with {}", cfg.exe, e)
                    }
                    Ok(child) => {
                        *proc = ProcStatus::Running(child);
                        *default = None;
                    }
                },
            }
        }
        async fn restart(
            proc: &mut ProcStatus,
            default: &mut Option<SelectAll<DefaultHandle>>,
            unit: &Unit,
            when: Instant,
        ) {
            match &unit.process.restart {
                Restart::No => (),
                Restart::Yes => start(proc, default, &unit.process),
                Restart::RateLimited(secs) => {
                    let elapsed = when.elapsed();
                    if elapsed.as_secs_f64() > *secs {
                        start(proc, default, &unit.process)
                    } else {
                        sleep(Duration::from_secs_f64(*secs) - elapsed).await;
                        start(proc, default, &unit.process)
                    }
                }
            }
        }
        async fn maybe_restart(
            proc: &mut ProcStatus,
            default: &mut Option<SelectAll<DefaultHandle>>,
            unit: &Unit,
        ) {
            match proc {
                ProcStatus::Running(_) => (),
                ProcStatus::NotStarted => match &unit.trigger {
                    Trigger::OnAccess(_) => (),
                    Trigger::OnStart => start(proc, default, &unit.process),
                },
                ProcStatus::Died(when) => match &unit.trigger {
                    Trigger::OnAccess(_) => (),
                    Trigger::OnStart => {
                        let when = *when;
                        restart(proc, default, unit, when).await
                    }
                },
            }
        }
        maybe_restart(&mut proc, &mut default, &unit).await;
        loop {
            select_biased! {
                m = rx.recv().fuse() => match m {
                    Some(ToProcess::Reconfigure(new_unit)) => {
                        unit = new_unit;
                        match proc {
                            ProcStatus::Running(_) => (),
                            ProcStatus::NotStarted | ProcStatus::Died(_) => {
                                default = publish(&publisher, &unit.trigger)?;
                                maybe_restart(&mut proc, &mut default, &unit).await
                            }
                        }
                    }
                    None | Some(ToProcess::Shutdown) =>  match &mut proc {
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
                    }
                },
                e = wait_proc(&mut proc).fuse() => match e {
                    Err(e) => {
                        error!("failed to wait for unit {}, failed with {}", name, e);
                        sleep(Duration::from_secs(1)).await;
                    }
                    Ok(e) => {
                        warn!("process for unit {} shutdown with {:?}", name, e);
                        proc = ProcStatus::Died(Instant::now());
                        default = publish(&publisher, &unit.trigger)?;
                        maybe_restart(&mut proc, &mut default, &unit).await
                    }
                },
                () = wait_default(&mut default).fuse() => match &proc {
                    ProcStatus::Running(_) => (),
                    ProcStatus::NotStarted => start(&mut proc, &mut default, &unit.process),
                    ProcStatus::Died(when) => {
                        let when = *when;
                        restart(&mut proc, &mut default, &unit, when).await
                    },
                },
                complete => bail!("default handle finished"),
            }
        }
    }

    fn new(publisher: Publisher, name: String, unit: Unit) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let join_handle = task::spawn(async move {
            match Self::run(publisher, name.clone(), unit, rx).await {
                Err(e) => error!("unit {} failed with {}", name, e),
                Ok(()) => info!("unit {} shutdown", name),
            }
        });
        Self { join_handle, tx }
    }

    fn reconfigure(&self, unit: Unit) {
        let _ = self.tx.send(ToProcess::Reconfigure(unit));
    }

    async fn shutdown(self) {
        let _ = self.tx.send(ToProcess::Shutdown);
        let _ = self.join_handle.await;
    }
}

async fn start_processes(
    publisher: &Publisher,
    units: &HashMap<String, Unit>,
    processes: &mut HashMap<String, Process>,
) {
    let to_kill = processes
        .keys()
        .filter(|k| !units.contains_key(k.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    join_all(
        to_kill.into_iter().filter_map(|k| processes.remove(&k)).map(|p| p.shutdown()),
    )
    .await;
    for (name, unit) in units {
        match processes.get_mut(name) {
            Some(proc) => proc.reconfigure(unit.clone()),
            None => {
                processes.insert(
                    name.clone(),
                    Process::new(publisher.clone(), name.clone(), unit.clone()),
                );
            }
        }
    }
}

async fn run_server(cfg: Config, auth: DesiredAuth, params: Params) -> Result<()> {
    let publisher = PublisherBuilder::new(cfg)
        .desired_auth(auth)
        .bind_cfg(params.bind)
        .build()
        .await?;
    let mut units = Unit::load(params.units.as_ref()).await?;
    let mut processes: HashMap<String, Process> = HashMap::new();
    let mut sighup = signal(SignalKind::hangup())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigquit = signal(SignalKind::quit())?;
    let shutdown =
        |p| async { start_processes(&publisher, &HashMap::default(), p).await };
    start_processes(&publisher, &units, &mut processes).await;
    loop {
        select_biased! {
            _ = sigint.recv().fuse() => break shutdown(&mut processes).await,
            _ = sigterm.recv().fuse() => break shutdown(&mut processes).await,
            _ = sigquit.recv().fuse() => break shutdown(&mut processes).await,
            _ = sighup.recv().fuse() => {
                match Unit::load(params.units.as_ref()).await {
                    Err(e) => error!("could not reconfigure, could not load units {}", e),
                    Ok(u) => {
                        units = u;
                        start_processes(&publisher, &units, &mut processes).await;
                        info!("units reloaded successfully")
                    }
                }
            }
            complete => break shutdown(&mut processes).await,
        }
    }
    Ok(())
}

pub(super) fn run(cfg: Config, auth: DesiredAuth, params: Params) {
    if !params.foreground {
        let mut d = Daemonize::new();
        if let Some(pid_file) = params.pid_file.as_ref() {
            d = d.pid_file(pid_file);
        }
        d.start().expect("failed to daemonize")
    }
    Runtime::new().expect("failed to init runtime").block_on(async move {
        if let Err(e) = run_server(cfg, auth, params).await {
            error!("server task shutdown: {}", e)
        }
    });
}
