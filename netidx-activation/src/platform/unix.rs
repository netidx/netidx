//! Unix implementation of the activation supervisor's platform layer.
//!
//! This is the original behavior, lifted out of `runtime.rs` verbatim:
//! a unix-domain control socket (0600 + `SO_PEERCRED`), SIGTERM→SIGKILL
//! child stop, `Command::uid/gid` privilege drop, and the
//! SIGINT/SIGTERM/SIGQUIT/SIGHUP signal loop.

use crate::{control, platform::SigEvent};
use anyhow::{Result, bail};
use futures::{future, prelude::*, select_biased};
use log::{error, info};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    time::Duration,
};
use tokio::{
    net::{UnixListener, UnixStream},
    process::{Child, Command},
    signal::unix::{Signal, SignalKind, signal},
    time::timeout,
};

/// A spawned, supervised child process. On unix the tokio [`Child`] is
/// all we need — graceful stop is a SIGTERM, so there is no per-child
/// handle to carry alongside it (cf. the Windows shutdown event).
pub(crate) struct Spawned {
    child: Child,
}

impl Spawned {
    pub(crate) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub(crate) async fn wait(&mut self) -> Result<ExitStatus> {
        Ok(self.child.wait().await?)
    }
}

/// Orphan-reaping handle. A no-op on unix — children are reaped via
/// `wait`, and the supervisor's own death is handled by the units
/// re-registering. `Clone` so each per-unit task can hold one (matches
/// the Windows job-object handle that must be shared).
#[derive(Clone)]
pub(crate) struct Job;

impl Job {
    pub(crate) fn new() -> Result<Job> {
        Ok(Job)
    }
}

/// Apply a unit's uid/gid privilege drop to the command.
pub(crate) fn configure_privileges(
    cmd: &mut Command,
    uid: Option<u32>,
    gid: Option<u32>,
) -> Result<()> {
    if let Some(uid) = uid {
        cmd.uid(uid);
    }
    if let Some(gid) = gid {
        cmd.gid(gid);
    }
    Ok(())
}

/// Spawn the configured command. `job` is unused on unix.
pub(crate) fn spawn(mut cmd: Command, _job: &Job) -> Result<Spawned> {
    let child = cmd.spawn()?;
    Ok(Spawned { child })
}

/// SIGTERM a running child, give it `grace` to exit, then SIGKILL and
/// reap. A no-op for a child that has already exited.
pub(crate) async fn stop_proc(proc: &mut Spawned, grace: Duration) {
    match proc.child.id() {
        None => {
            let _ = proc.child.kill().await;
        }
        Some(pid) => {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            let term = nix::sys::signal::Signal::SIGTERM;
            let _ = nix::sys::signal::kill(pid, Some(term));
            let _ = timeout(grace, proc.child.wait()).await;
            let _ = proc.child.kill().await;
        }
    }
}

/// Verify the exe carries a unix executable bit. The caller has already
/// confirmed it is a regular file.
pub(crate) fn validate_exe(md: &std::fs::Metadata) -> Result<()> {
    if md.permissions().mode() & 0b0000_0000_0100_1001 == 0 {
        bail!("exe must be executable")
    }
    Ok(())
}

/// System-wide unit directory fallback, used when there is no per-user
/// directory. `/etc/netidx/activation` on unix.
pub(crate) fn system_units_dir() -> Option<PathBuf> {
    let p = PathBuf::from("/etc/netidx/activation");
    if std::path::Path::is_dir(&p) { Some(p) } else { None }
}

// ---- local control transport ---------------------------------------------

pub(crate) struct ControlListener(UnixListener);

/// The connected control stream handed to `handle_control_conn`.
pub(crate) type ControlStream = UnixStream;

/// Bind the control socket at `<units_dir>/control.sock`, mode 0600.
/// Returns `None` (logged) on failure — the supervisor still runs, just
/// without remote control.
pub(crate) fn bind_control(units_dir: &Path) -> Option<ControlListener> {
    let path = control::socket_path(units_dir);
    let _ = std::fs::remove_file(&path); // clear a stale socket left by a crash
    match UnixListener::bind(&path) {
        Ok(l) => {
            if let Err(e) =
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            {
                error!(
                    "activation control: could not set perms on {}: {e}",
                    path.display()
                );
            }
            info!("activation control socket listening at {}", path.display());
            Some(ControlListener(l))
        }
        Err(e) => {
            error!("activation control: could not bind {}: {e}", path.display());
            None
        }
    }
}

/// Pend forever when there is no listener, so the `select` arm is inert.
pub(crate) async fn accept_control(
    listener: &mut Option<ControlListener>,
) -> Option<ControlStream> {
    match listener {
        Some(l) => match l.0.accept().await {
            Ok((s, _)) => Some(s),
            Err(e) => {
                error!("activation control: accept failed: {e}");
                None
            }
        },
        None => future::pending().await,
    }
}

/// Whether the connecting peer may control units: the same effective uid
/// as the supervisor, or root. The 0600 socket mode already enforces
/// this at the kernel — this is defense-in-depth and an audit point.
pub(crate) fn peer_allowed(stream: &ControlStream) -> bool {
    match stream.peer_cred() {
        Ok(cred) => cred.uid() == 0 || cred.uid() == nix::unistd::geteuid().as_raw(),
        Err(e) => {
            error!("activation control: could not read peer credentials: {e}");
            false
        }
    }
}

/// Remove the control endpoint on shutdown.
pub(crate) fn remove_control(units_dir: &Path) {
    let _ = std::fs::remove_file(control::socket_path(units_dir));
}

// ---- signal sources -------------------------------------------------------

/// The supervisor's signal sources. Owns the four unix signal streams so
/// they aren't re-registered each loop iteration.
pub(crate) struct Signals {
    sighup: Signal,
    sigint: Signal,
    sigterm: Signal,
    sigquit: Signal,
}

impl Signals {
    pub(crate) fn new() -> Result<Signals> {
        Ok(Signals {
            sighup: signal(SignalKind::hangup())?,
            sigint: signal(SignalKind::interrupt())?,
            sigterm: signal(SignalKind::terminate())?,
            sigquit: signal(SignalKind::quit())?,
        })
    }

    /// Await the next shutdown (SIGINT/SIGTERM/SIGQUIT) or reload
    /// (SIGHUP) signal. Shutdown arms are biased first.
    pub(crate) async fn next(&mut self) -> SigEvent {
        select_biased! {
            _ = self.sigint.recv().fuse() => SigEvent::Shutdown,
            _ = self.sigterm.recv().fuse() => SigEvent::Shutdown,
            _ = self.sigquit.recv().fuse() => SigEvent::Shutdown,
            _ = self.sighup.recv().fuse() => SigEvent::Reload,
        }
    }
}
