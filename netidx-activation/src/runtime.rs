//! Activation daemon runtime.
//!
//! Lifted from `netidx-tools` so the supervisor can be embedded by
//! tooling (CLI, GUI, tests) and reasoned about independently of the
//! `netidx` binary's `main`.

use crate::file::{ProcessCfg, Restart, Trigger, Unit};
use anyhow::Result;
use futures::{future::join_all, prelude::*, select_biased, stream::SelectAll};
use log::{error, info, warn};
use netidx::{
    config::Config,
    publisher::{BindCfg, DefaultHandle, DesiredAuth, Publisher, PublisherBuilder},
};
use std::{
    collections::HashMap,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::ExitStatus,
    time::Duration,
};
use tokio::{
    fs,
    process::{Child, Command},
    signal::unix::{signal, SignalKind},
    sync::mpsc,
    task,
    time::{sleep, timeout, Instant},
};

/// Runtime helpers on `ProcessCfg`. Imported by callers that need to
/// drive a `ProcessCfg` against the operating system. Schema-only
/// consumers never see this trait.
pub trait ProcessCfgExt {
    /// Verify on disk that `exe` exists and is executable. This is a
    /// runtime check; the schema layer cannot perform it.
    fn validate(&self) -> impl Future<Output = Result<()>> + Send;

    /// Construct a `tokio::process::Command` from this configuration.
    fn command(&self) -> Result<Command>;
}

impl ProcessCfgExt for ProcessCfg {
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
            crate::file::Environment::Inherit(overrides) => {
                c.envs(overrides.iter());
            }
            crate::file::Environment::Replace(vars) => {
                c.env_clear();
                c.envs(vars.iter());
            }
        }
        Ok(c)
    }
}

/// Default unit-directory search:
/// `${dirs::config_dir}/netidx/activation` then
/// `/etc/netidx/activation`.
fn default_units_dir() -> Option<PathBuf> {
    if let Some(mut p) = dirs::config_dir() {
        p.push("netidx");
        p.push("activation");
        if std::path::Path::is_dir(&p) {
            return Some(p);
        }
    }
    let p = PathBuf::from("/etc/netidx/activation");
    if std::path::Path::is_dir(&p) {
        Some(p)
    } else {
        None
    }
}

/// Load every `*.unit` file from `dir` (or the default location). Keys
/// in the returned map are the file basenames *including* the `.unit`
/// suffix, matching historical behavior.
///
/// Performs structural cross-unit validation (no two `OnAccess` units
/// claim the same path) and async filesystem validation of every
/// referenced executable.
pub async fn load_units(dir: Option<&PathBuf>) -> Result<HashMap<String, Unit>> {
    let path = dir
        .cloned()
        .or_else(|| task::block_in_place(default_units_dir))
        .ok_or_else(|| {
            anyhow!("no unit directory specified and no default was found")
        })?;
    let mut hm: HashMap<String, Unit> = HashMap::new();
    let mut dirs = fs::read_dir(path).await?;
    while let Some(ent) = dirs.next_entry().await? {
        let name = ent.file_name().to_string_lossy().into_owned();
        let typ = ent.file_type().await?;
        if (typ.is_file() || typ.is_symlink()) && name.ends_with(".unit") {
            match serde_json::from_str(&fs::read_to_string(ent.path()).await?) {
                Ok(u) => {
                    hm.insert(name, u);
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
    for unit in hm.values() {
        unit.process.validate().await?;
    }
    crate::file::check_trigger_conflicts(
        hm.iter().map(|(name, u)| (name.as_str(), u)),
    )?;
    Ok(hm)
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
        async fn restart_proc(
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
                        restart_proc(proc, default, unit, when).await
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
                        restart_proc(&mut proc, &mut default, &unit, when).await
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

/// Parameters used to construct a [`Server`].
pub struct ServerParams {
    /// Publisher bind configuration. `None` lets the publisher pick.
    pub bind: Option<BindCfg>,
    /// Override the unit directory. `None` uses the default search.
    pub units_dir: Option<PathBuf>,
}

/// A running activation supervisor.
///
/// Construct with [`Server::new`]; that brings up the publisher,
/// loads the unit directory, and spawns a supervisor task for each
/// unit. Call [`Server::run`] to drive the SIGHUP / shutdown signal
/// loop until the server is asked to terminate.
pub struct Server {
    publisher: Publisher,
    processes: HashMap<String, Process>,
    units: HashMap<String, Unit>,
    units_dir: Option<PathBuf>,
}

impl Server {
    /// Build the publisher, load units, and spawn supervisor tasks.
    pub async fn new(
        cfg: Config,
        auth: DesiredAuth,
        params: ServerParams,
    ) -> Result<Self> {
        let publisher = PublisherBuilder::new(cfg)
            .desired_auth(auth)
            .bind_cfg(params.bind)
            .build()
            .await?;
        let units = load_units(params.units_dir.as_ref()).await?;
        let mut processes: HashMap<String, Process> = HashMap::new();
        start_processes(&publisher, &units, &mut processes).await;
        Ok(Self { publisher, processes, units, units_dir: params.units_dir })
    }

    /// Run until SIGINT / SIGTERM / SIGQUIT. SIGHUP triggers a unit
    /// reload from disk.
    pub async fn run(mut self) -> Result<()> {
        let mut sighup = signal(SignalKind::hangup())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigquit = signal(SignalKind::quit())?;
        loop {
            select_biased! {
                _ = sigint.recv().fuse() => break,
                _ = sigterm.recv().fuse() => break,
                _ = sigquit.recv().fuse() => break,
                _ = sighup.recv().fuse() => {
                    match load_units(self.units_dir.as_ref()).await {
                        Err(e) => error!("could not reconfigure, could not load units {}", e),
                        Ok(u) => {
                            self.units = u;
                            start_processes(&self.publisher, &self.units, &mut self.processes).await;
                            info!("units reloaded successfully")
                        }
                    }
                }
                complete => break,
            }
        }
        start_processes(&self.publisher, &HashMap::default(), &mut self.processes).await;
        Ok(())
    }
}

