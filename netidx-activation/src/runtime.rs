//! Activation daemon runtime.
//!
//! Lifted from `netidx-tools` so the supervisor can be embedded by
//! tooling (CLI, GUI, tests) and reasoned about independently of the
//! `netidx` binary's `main`.

use crate::{
    control::{self, ControlOp, ControlRequest, ControlResponse, UnitState, UnitStatus},
    file::{ProcessCfg, Restart, Trigger, Unit},
    platform::{self, SigEvent},
};
use anyhow::{Result, anyhow, bail};
use futures::{future::join_all, prelude::*, select_biased, stream::SelectAll};
use log::{error, info, warn};
use netidx::{
    config::Config,
    publisher::{BindCfg, DefaultHandle, DesiredAuth, Publisher, PublisherBuilder},
};
use std::{
    collections::HashMap,
    path::PathBuf,
    process::ExitStatus,
    time::Duration,
};
use tokio::{
    fs,
    process::Command,
    sync::{mpsc, oneshot},
    task,
    time::{Instant, sleep, timeout},
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
        platform::validate_exe(&md)?;
        Ok(())
    }

    fn command(&self) -> Result<Command> {
        use std::fs;
        let mut c = Command::new(&self.exe);
        c.args(self.args.iter());
        if let Some(dir) = self.working_directory.as_ref() {
            c.current_dir(dir);
        }
        platform::configure_privileges(&mut c, self.uid, self.gid)?;
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
pub fn default_units_dir() -> Option<PathBuf> {
    if let Some(mut p) = dirs::config_dir() {
        p.push("netidx");
        p.push("activation");
        if std::path::Path::is_dir(&p) {
            return Some(p);
        }
    }
    platform::system_units_dir()
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
        .ok_or_else(|| anyhow!("no unit directory specified and no default was found"))?;
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
    crate::file::check_trigger_conflicts(hm.iter().map(|(name, u)| (name.as_str(), u)))?;
    Ok(hm)
}

enum ProcStatus {
    NotStarted,
    Died(Instant),
    Running(platform::Spawned),
    /// Explicitly stopped via the control socket — every auto-restart path
    /// (crash, OnAccess, reconfigure) treats this as "leave it alone" until
    /// an explicit `Start`.
    Stopped,
}

enum ToProcess {
    Shutdown,
    Reconfigure(Unit),
    /// A control-socket op; the resulting [`UnitStatus`] is sent back on
    /// `reply` so the caller can report what happened.
    Control {
        op: ControlOp,
        reply: oneshot::Sender<UnitStatus>,
    },
}

/// Snapshot a process's runtime state for the control protocol.
fn unit_status(name: &str, proc: &ProcStatus) -> UnitStatus {
    let state = match proc {
        ProcStatus::NotStarted => UnitState::NotStarted,
        ProcStatus::Stopped => UnitState::Stopped,
        ProcStatus::Died(_) => UnitState::Died,
        ProcStatus::Running(spawned) => UnitState::Running { pid: spawned.id() },
    };
    UnitStatus { unit: name.to_string(), state }
}

/// Grace period an explicitly stop/restart'd process gets to exit on SIGTERM
/// before SIGKILL. Kept comfortably under the conf server's connection
/// timeout so a remote control op always gets a real reply (rather than the
/// connection being dropped mid-shutdown and the op reported as failed).
const CONTROL_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Grace period a child gets on final supervisor shutdown before the hard
/// kill. Longer than the control-op grace because a normal shutdown isn't
/// racing a remote control reply.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// Ask a running child to stop — gracefully first (SIGTERM on unix, the
/// shutdown event on Windows), then a hard kill after `grace` — and reap
/// it. A no-op for any non-running state.
async fn stop_proc(proc: &mut ProcStatus, grace: Duration) {
    if let ProcStatus::Running(spawned) = proc {
        platform::stop_proc(spawned, grace).await;
    }
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
        job: platform::Job,
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
                ProcStatus::Died(_) | ProcStatus::NotStarted | ProcStatus::Stopped => {
                    future::pending().await
                }
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
            job: &platform::Job,
        ) {
            match task::block_in_place(|| cfg.command()) {
                Err(e) => error!("failed to setup process {} failed with {}", cfg.exe, e),
                Ok(c) => match platform::spawn(c, Some(job)) {
                    Err(e) => {
                        error!("failed to spawn process {} failed with {}", cfg.exe, e)
                    }
                    Ok(spawned) => {
                        *proc = ProcStatus::Running(spawned);
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
            job: &platform::Job,
        ) {
            match &unit.process.restart {
                Restart::No => (),
                Restart::Yes => start(proc, default, &unit.process, job),
                Restart::RateLimited(secs) => {
                    let elapsed = when.elapsed();
                    if elapsed.as_secs_f64() > *secs {
                        start(proc, default, &unit.process, job)
                    } else {
                        sleep(Duration::from_secs_f64(*secs) - elapsed).await;
                        start(proc, default, &unit.process, job)
                    }
                }
            }
        }
        async fn maybe_restart(
            proc: &mut ProcStatus,
            default: &mut Option<SelectAll<DefaultHandle>>,
            unit: &Unit,
            job: &platform::Job,
        ) {
            match proc {
                ProcStatus::Running(_) | ProcStatus::Stopped => (),
                ProcStatus::NotStarted => match &unit.trigger {
                    Trigger::OnAccess(_) => (),
                    Trigger::OnStart => start(proc, default, &unit.process, job),
                },
                ProcStatus::Died(when) => match &unit.trigger {
                    Trigger::OnAccess(_) => (),
                    Trigger::OnStart => {
                        let when = *when;
                        restart_proc(proc, default, unit, when, job).await
                    }
                },
            }
        }
        maybe_restart(&mut proc, &mut default, &unit, &job).await;
        loop {
            select_biased! {
                m = rx.recv().fuse() => match m {
                    Some(ToProcess::Reconfigure(new_unit)) => {
                        unit = new_unit;
                        match proc {
                            // A manually-stopped unit stays stopped across a
                            // reconfigure; a running one keeps running.
                            ProcStatus::Running(_) | ProcStatus::Stopped => (),
                            ProcStatus::NotStarted | ProcStatus::Died(_) => {
                                default = publish(&publisher, &unit.trigger)?;
                                maybe_restart(&mut proc, &mut default, &unit, &job).await
                            }
                        }
                    }
                    Some(ToProcess::Control { op, reply }) => {
                        match op {
                            // Reload is supervisor-global; it's converted to a
                            // per-unit Status before reaching here, so a unit
                            // task never actually sees it.
                            ControlOp::Status | ControlOp::Reload => (),
                            ControlOp::Stop => {
                                stop_proc(&mut proc, CONTROL_SHUTDOWN_GRACE).await;
                                proc = ProcStatus::Stopped;
                                default = None;
                            }
                            ControlOp::Start => {
                                if !matches!(proc, ProcStatus::Running(_)) {
                                    // Force-start regardless of trigger (an
                                    // explicit operator action), clearing any
                                    // Stopped latch. `start` sets `default =
                                    // None`; we must NOT re-publish the trigger
                                    // paths here — the unit's own handles are
                                    // still live, so a re-publish would bail
                                    // ("already published") and kill the task.
                                    start(&mut proc, &mut default, &unit.process, &job);
                                }
                            }
                            ControlOp::Restart => {
                                stop_proc(&mut proc, CONTROL_SHUTDOWN_GRACE).await;
                                start(&mut proc, &mut default, &unit.process, &job);
                            }
                        }
                        let _ = reply.send(unit_status(&name, &proc));
                    }
                    None | Some(ToProcess::Shutdown) => {
                        stop_proc(&mut proc, SHUTDOWN_GRACE).await;
                        break Ok(())
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
                        maybe_restart(&mut proc, &mut default, &unit, &job).await
                    }
                },
                () = wait_default(&mut default).fuse() => match &proc {
                    ProcStatus::Running(_) | ProcStatus::Stopped => (),
                    ProcStatus::NotStarted => start(&mut proc, &mut default, &unit.process, &job),
                    ProcStatus::Died(when) => {
                        let when = *when;
                        restart_proc(&mut proc, &mut default, &unit, when, &job).await
                    },
                },
                complete => bail!("default handle finished"),
            }
        }
    }

    fn new(publisher: Publisher, name: String, unit: Unit, job: platform::Job) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let join_handle = task::spawn(async move {
            match Self::run(publisher, name.clone(), unit, rx, job).await {
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
    job: &platform::Job,
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
                    Process::new(publisher.clone(), name.clone(), unit.clone(), job.clone()),
                );
            }
        }
    }
}

/// Strip a trailing `.unit` so `resolver` and `resolver.unit` compare equal.
fn unit_key(s: &str) -> &str {
    s.strip_suffix(".unit").unwrap_or(s)
}

/// A `Reload` request from a control connection to the supervisor loop. The
/// loop reloads units from disk and replies with the new per-unit control
/// channel snapshot, over which the handler then reports `Status`.
type ReloadRequest = oneshot::Sender<HashMap<String, mpsc::UnboundedSender<ToProcess>>>;

/// Handle one control connection: read the request, drive the named units'
/// supervisor tasks, and reply with each unit's resulting status. `senders`
/// is a snapshot of the per-unit control channels taken at accept time;
/// `reload_tx` reaches the supervisor loop to service a `Reload`.
async fn handle_control_conn(
    mut stream: platform::ControlStream,
    senders: HashMap<String, mpsc::UnboundedSender<ToProcess>>,
    reload_tx: mpsc::Sender<ReloadRequest>,
) -> Result<()> {
    let req: ControlRequest = control::read_msg(&mut stream).await?;
    // A Reload is supervisor-global: ask the run loop to reload units from
    // disk, then report Status over the fresh unit set (its `units` list is
    // ignored). Every other op acts on the snapshot taken at accept time.
    let (op, senders, units) = if let ControlOp::Reload = req.op {
        let (tx, rx) = oneshot::channel();
        if reload_tx.send(tx).await.is_err() {
            let resp =
                ControlResponse::Err { reason: "supervisor is shutting down".into() };
            return control::write_msg(&mut stream, &resp).await;
        }
        match rx.await {
            Ok(new_senders) => (ControlOp::Status, new_senders, Vec::new()),
            Err(_) => {
                let resp = ControlResponse::Err { reason: "reload failed".into() };
                return control::write_msg(&mut stream, &resp).await;
            }
        }
    } else {
        (req.op, senders, req.units)
    };
    // Empty target list ⇒ every unit. Otherwise resolve each requested name
    // (suffix-insensitive) to a known unit, failing the whole request on an
    // unknown name so a typo isn't reported as success.
    let targets: Vec<String> = if units.is_empty() {
        senders.keys().cloned().collect()
    } else {
        let mut out = Vec::new();
        for want in &units {
            match senders.keys().find(|k| unit_key(k) == unit_key(want)) {
                Some(k) => out.push(k.clone()),
                None => {
                    let resp = ControlResponse::Err {
                        reason: format!("no unit matching {want:?}"),
                    };
                    return control::write_msg(&mut stream, &resp).await;
                }
            }
        }
        out
    };
    // Dispatch all units first, then await their replies concurrently — a
    // multi-unit request is bounded by the slowest unit, not the sum, so it
    // stays under the conf server's connection timeout.
    let mut pending = Vec::with_capacity(targets.len());
    for key in targets {
        let display = unit_key(&key).to_string();
        match senders.get(&key) {
            None => pending.push((display, None)),
            Some(tx) => {
                let (rtx, rrx) = oneshot::channel();
                // tx.send only fails if the supervisor task vanished (e.g. a
                // concurrent reload removed the unit); report it, don't hang.
                pending.push((
                    display,
                    tx.send(ToProcess::Control { op, reply: rtx })
                        .ok()
                        .map(|()| rrx),
                ));
            }
        }
    }
    let results = join_all(pending.into_iter().map(|(display, rrx)| async move {
        match rrx {
            None => UnitStatus { unit: display, state: UnitState::NotStarted },
            // The control op itself bounds the wait (CONTROL_SHUTDOWN_GRACE);
            // this is a backstop against a wedged supervisor task.
            Some(rrx) => match timeout(Duration::from_secs(25), rrx).await {
                Ok(Ok(mut st)) => {
                    st.unit = unit_key(&st.unit).to_string();
                    st
                }
                _ => UnitStatus { unit: display, state: UnitState::Died },
            },
        }
    }))
    .await;
    control::write_msg(&mut stream, &ControlResponse::Ok { units: results }).await
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
/// unit. Call [`Server::run`] to drive the shutdown / reload loop
/// until the server is asked to terminate.
pub struct Server {
    publisher: Publisher,
    processes: HashMap<String, Process>,
    units: HashMap<String, Unit>,
    units_dir: Option<PathBuf>,
    job: platform::Job,
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
        let job = platform::Job::new()?;
        let units = load_units(params.units_dir.as_ref()).await?;
        let mut processes: HashMap<String, Process> = HashMap::new();
        start_processes(&publisher, &units, &mut processes, &job).await;
        Ok(Self { publisher, processes, units, units_dir: params.units_dir, job })
    }

    /// Reload the unit directory from disk and reconcile. Driven on unix
    /// by SIGHUP and on every platform by the `Reload` control op.
    async fn reload(&mut self) {
        match load_units(self.units_dir.as_ref()).await {
            Err(e) => error!("could not reconfigure, could not load units {}", e),
            Ok(u) => {
                self.units = u;
                start_processes(&self.publisher, &self.units, &mut self.processes, &self.job)
                    .await;
                info!("units reloaded successfully")
            }
        }
    }

    /// Run until asked to shut down (SIGINT/SIGTERM/SIGQUIT on unix,
    /// ctrl-c / ctrl-break on Windows). A reload signal (SIGHUP on unix)
    /// triggers a unit reload from disk.
    pub async fn run(mut self) -> Result<()> {
        let mut signals = platform::Signals::new()?;
        // The local control endpoint (unix socket / named pipe) under the
        // unit directory. Lets the conf server (same host, same user) drive
        // a unit's start/stop/restart/status/reload on a role admin's
        // behalf. Best-effort: a bind failure logs and the supervisor still
        // serves units.
        let units_dir = self.units_dir.clone().or_else(default_units_dir);
        let mut control_listener =
            units_dir.as_deref().and_then(platform::bind_control);
        // Control handlers run in their own tasks; a `Reload` op reaches the
        // loop back through this channel so the reload runs here (where `self`
        // lives) and the handler gets the fresh unit set to report on.
        let (reload_tx, mut reload_rx) = mpsc::channel::<ReloadRequest>(8);
        loop {
            select_biased! {
                ev = signals.next().fuse() => match ev {
                    SigEvent::Shutdown => break,
                    SigEvent::Reload => self.reload().await,
                },
                req = reload_rx.recv().fuse() => {
                    if let Some(reply) = req {
                        self.reload().await;
                        let senders: HashMap<String, mpsc::UnboundedSender<ToProcess>> =
                            self.processes.iter().map(|(k, p)| (k.clone(), p.tx.clone())).collect();
                        let _ = reply.send(senders);
                    }
                }
                conn = platform::accept_control(&mut control_listener).fuse() => {
                    if let Some(stream) = conn {
                        if platform::peer_allowed(&stream) {
                            // Snapshot the per-unit control channels so a slow
                            // client never stalls this loop.
                            let senders: HashMap<String, mpsc::UnboundedSender<ToProcess>> =
                                self.processes.iter().map(|(k, p)| (k.clone(), p.tx.clone())).collect();
                            let reload_tx = reload_tx.clone();
                            task::spawn(async move {
                                if let Err(e) = handle_control_conn(stream, senders, reload_tx).await {
                                    warn!("activation control: connection error: {e}");
                                }
                            });
                        } else {
                            warn!("activation control: rejected a connection from a non-owner peer");
                        }
                    }
                }
                complete => break,
            }
        }
        start_processes(&self.publisher, &HashMap::default(), &mut self.processes, &self.job)
            .await;
        if let Some(d) = &units_dir {
            platform::remove_control(d);
        }
        Ok(())
    }
}
