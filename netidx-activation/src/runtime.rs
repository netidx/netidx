//! Activation daemon runtime.
//!
//! Lifted from `netidx-tools` so the supervisor can be embedded by
//! tooling (CLI, GUI, tests) and reasoned about independently of the
//! `netidx` binary's `main`.

use crate::{
    control::{self, ControlOp, ControlRequest, ControlResponse, UnitState, UnitStatus},
    file::{ProcessCfg, Restart, Trigger, Unit},
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
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    time::Duration,
};
use tokio::{
    fs,
    net::{UnixListener, UnixStream},
    process::{Child, Command},
    signal::unix::{SignalKind, signal},
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
pub fn default_units_dir() -> Option<PathBuf> {
    if let Some(mut p) = dirs::config_dir() {
        p.push("netidx");
        p.push("activation");
        if std::path::Path::is_dir(&p) {
            return Some(p);
        }
    }
    let p = PathBuf::from("/etc/netidx/activation");
    if std::path::Path::is_dir(&p) { Some(p) } else { None }
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
    Running(Child),
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
        ProcStatus::Running(child) => UnitState::Running { pid: child.id() },
    };
    UnitStatus { unit: name.to_string(), state }
}

/// Grace period an explicitly stop/restart'd process gets to exit on SIGTERM
/// before SIGKILL. Kept comfortably under the conf server's connection
/// timeout so a remote control op always gets a real reply (rather than the
/// connection being dropped mid-shutdown and the op reported as failed).
const CONTROL_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// SIGTERM (then SIGKILL) a running child and reap it. A no-op for any
/// non-running state.
async fn stop_proc(proc: &mut ProcStatus) {
    if let ProcStatus::Running(child) = proc {
        match child.id() {
            None => {
                let _ = child.kill().await;
            }
            Some(pid) => {
                let pid = nix::unistd::Pid::from_raw(pid as i32);
                let term = nix::sys::signal::Signal::SIGTERM;
                let _ = nix::sys::signal::kill(pid, Some(term));
                let _ = timeout(CONTROL_SHUTDOWN_GRACE, child.wait()).await;
                let _ = child.kill().await;
            }
        }
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
                ProcStatus::Running(_) | ProcStatus::Stopped => (),
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
                            // A manually-stopped unit stays stopped across a
                            // reconfigure; a running one keeps running.
                            ProcStatus::Running(_) | ProcStatus::Stopped => (),
                            ProcStatus::NotStarted | ProcStatus::Died(_) => {
                                default = publish(&publisher, &unit.trigger)?;
                                maybe_restart(&mut proc, &mut default, &unit).await
                            }
                        }
                    }
                    Some(ToProcess::Control { op, reply }) => {
                        match op {
                            ControlOp::Status => (),
                            ControlOp::Stop => {
                                stop_proc(&mut proc).await;
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
                                    start(&mut proc, &mut default, &unit.process);
                                }
                            }
                            ControlOp::Restart => {
                                stop_proc(&mut proc).await;
                                start(&mut proc, &mut default, &unit.process);
                            }
                        }
                        let _ = reply.send(unit_status(&name, &proc));
                    }
                    None | Some(ToProcess::Shutdown) =>  match &mut proc {
                        ProcStatus::NotStarted | ProcStatus::Died(_) | ProcStatus::Stopped => {
                            break Ok(())
                        }
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
                    ProcStatus::Running(_) | ProcStatus::Stopped => (),
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

/// Strip a trailing `.unit` so `resolver` and `resolver.unit` compare equal.
fn unit_key(s: &str) -> &str {
    s.strip_suffix(".unit").unwrap_or(s)
}

/// Whether the connecting peer is allowed to control units: the same
/// effective uid as the supervisor, or root. The 0600 socket mode already
/// enforces this at the kernel — this is defense-in-depth and an audit point.
fn peer_allowed(stream: &UnixStream) -> bool {
    match stream.peer_cred() {
        Ok(cred) => cred.uid() == 0 || cred.uid() == nix::unistd::geteuid().as_raw(),
        Err(e) => {
            error!("activation control: could not read peer credentials: {e}");
            false
        }
    }
}

/// Bind the control socket at `path`, mode 0600. Returns `None` (logged) if
/// it can't bind — the supervisor still runs, just without remote control.
fn bind_control(path: &Path) -> Option<UnixListener> {
    let _ = std::fs::remove_file(path); // clear a stale socket left by a crash
    match UnixListener::bind(path) {
        Ok(l) => {
            if let Err(e) =
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            {
                error!(
                    "activation control: could not set perms on {}: {e}",
                    path.display()
                );
            }
            info!("activation control socket listening at {}", path.display());
            Some(l)
        }
        Err(e) => {
            error!("activation control: could not bind {}: {e}", path.display());
            None
        }
    }
}

/// Pend forever when there is no listener, so the `select` arm is inert.
async fn accept_control(listener: &Option<UnixListener>) -> Option<UnixStream> {
    match listener {
        Some(l) => match l.accept().await {
            Ok((s, _)) => Some(s),
            Err(e) => {
                error!("activation control: accept failed: {e}");
                None
            }
        },
        None => future::pending().await,
    }
}

/// Handle one control connection: read the request, drive the named units'
/// supervisor tasks, and reply with each unit's resulting status. `senders`
/// is a snapshot of the per-unit control channels taken at accept time.
async fn handle_control_conn(
    mut stream: UnixStream,
    senders: HashMap<String, mpsc::UnboundedSender<ToProcess>>,
) -> Result<()> {
    let req: ControlRequest = control::read_msg(&mut stream).await?;
    // Empty target list ⇒ every unit. Otherwise resolve each requested name
    // (suffix-insensitive) to a known unit, failing the whole request on an
    // unknown name so a typo isn't reported as success.
    let targets: Vec<String> = if req.units.is_empty() {
        senders.keys().cloned().collect()
    } else {
        let mut out = Vec::new();
        for want in &req.units {
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
                    tx.send(ToProcess::Control { op: req.op, reply: rtx })
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
        // The local control socket — `<units_dir>/control.sock`, 0600. Lets
        // the conf server (same host, same user / root) drive a unit's
        // start/stop/restart/status on a role admin's behalf. Best-effort: a
        // bind failure logs and the supervisor still serves units.
        let control_socket = self
            .units_dir
            .clone()
            .or_else(default_units_dir)
            .map(|d| control::socket_path(&d));
        let control_listener = control_socket.as_deref().and_then(bind_control);
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
                conn = accept_control(&control_listener).fuse() => {
                    if let Some(stream) = conn {
                        if peer_allowed(&stream) {
                            // Snapshot the per-unit control channels so a slow
                            // client never stalls this loop.
                            let senders: HashMap<String, mpsc::UnboundedSender<ToProcess>> =
                                self.processes.iter().map(|(k, p)| (k.clone(), p.tx.clone())).collect();
                            task::spawn(async move {
                                if let Err(e) = handle_control_conn(stream, senders).await {
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
        start_processes(&self.publisher, &HashMap::default(), &mut self.processes).await;
        if let Some(path) = &control_socket {
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }
}
