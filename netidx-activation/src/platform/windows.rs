//! Windows implementation of the activation supervisor's platform layer.
//!
//! Mirrors the unix impl's API, mapping each unix mechanism to its
//! Windows analog:
//! - SIGTERM→SIGKILL stop  →  a per-child manual-reset *shutdown event*
//!   the daemon waits on, then `TerminateProcess` as the backstop.
//! - process-group / PDEATHSIG reaping  →  a *Job Object* with
//!   `KILL_ON_JOB_CLOSE`, so children die if the supervisor exits.
//! - the unix-domain control socket  →  a *named pipe*.
//! - SIGINT/SIGTERM/SIGQUIT shutdown  →  console control events
//!   (ctrl-c / ctrl-break). There is no SIGHUP analog; reload arrives
//!   only via the control protocol, so the signal source never yields
//!   [`SigEvent::Reload`].

use crate::{control, platform::SigEvent};
use anyhow::{Context, Result, anyhow, bail};
use futures::{future, prelude::*, select_biased};
use log::{error, info};
use std::{
    ffi::c_void,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::windows::named_pipe::{NamedPipeServer, ServerOptions},
    process::{Child, Command},
    signal::windows::{CtrlBreak, CtrlC, ctrl_break, ctrl_c},
    time::{sleep, timeout},
};
use windows::{
    Win32::{
        Foundation::{HANDLE, HLOCAL, LocalFree},
        Security::{
            Authorization::{
                ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
            },
            PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES,
        },
        System::{
            JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JobObjectExtendedLimitInformation, SetInformationJobObject,
            },
            Threading::{CREATE_NO_WINDOW, CreateEventW, SetEvent},
        },
    },
    core::PCWSTR,
};

use crate::shutdown::SHUTDOWN_EVENT_VAR;

/// Per-process counter to keep shutdown-event names unique.
static EVENT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A spawned, supervised child plus the manual-reset event used to ask
/// it to shut down gracefully.
pub(crate) struct Spawned {
    child: Child,
    shutdown_event: OwnedHandle,
}

impl Spawned {
    pub(crate) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub(crate) async fn wait(&mut self) -> Result<ExitStatus> {
        Ok(self.child.wait().await?)
    }
}

/// A Job Object the supervised children are assigned to. Configured
/// `KILL_ON_JOB_CLOSE`, so when the last handle closes — i.e. the
/// supervisor exits or crashes — Windows terminates every child. `Clone`
/// (an `Arc` over the owned handle) so each per-unit task can hold one;
/// the job closes when the supervisor and all unit tasks have dropped
/// their clones.
#[derive(Clone)]
pub(crate) struct Job(Arc<OwnedHandle>);

impl Job {
    pub(crate) fn new() -> Result<Job> {
        let job = unsafe { CreateJobObjectW(None, PCWSTR::null()) }
            .context("CreateJobObjectW")?;
        let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const c_void,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        }
        .context("SetInformationJobObject(KILL_ON_JOB_CLOSE)")?;
        // SAFETY: CreateJobObjectW returned a valid, owned job handle.
        let owned = unsafe { OwnedHandle::from_raw_handle(job.0 as RawHandle) };
        Ok(Job(Arc::new(owned)))
    }

    fn assign(&self, child: RawHandle) -> Result<()> {
        let job = HANDLE(self.0.as_raw_handle() as *mut c_void);
        let proc = HANDLE(child as *mut c_void);
        unsafe { AssignProcessToJobObject(job, proc) }
            .context("AssignProcessToJobObject")?;
        Ok(())
    }
}

/// Windows units cannot drop to a uid/gid; reject any unit that asks.
pub(crate) fn configure_privileges(
    _cmd: &mut Command,
    uid: Option<u32>,
    gid: Option<u32>,
) -> Result<()> {
    if uid.is_some() || gid.is_some() {
        bail!(
            "uid/gid privilege drop is not supported on Windows; remove them from the unit"
        )
    }
    Ok(())
}

/// Spawn the configured command: create its shutdown event, pass the
/// event name in the environment, spawn, and assign the child to the job
/// for orphan reaping.
pub(crate) fn spawn(mut cmd: Command, job: &Job) -> Result<Spawned> {
    let n = EVENT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!(r"Local\netidx-shutdown-{}-{n}", std::process::id());
    let wname: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
    // Manual-reset so a daemon that hasn't reached its wait yet still
    // observes the signal; initially non-signaled.
    let handle =
        unsafe { CreateEventW(None, true.into(), false.into(), PCWSTR(wname.as_ptr())) }
            .context("CreateEventW(shutdown)")?;
    // SAFETY: CreateEventW returned a valid, owned event handle.
    let shutdown_event = unsafe { OwnedHandle::from_raw_handle(handle.0 as RawHandle) };
    cmd.env(SHUTDOWN_EVENT_VAR, &name);
    // The supervisor runs windowless (the Windows logon task starts the
    // GUI-subsystem `netidx-activation.exe`, which has no console). A console
    // child spawned with no flags would allocate — and show — its own console
    // window; CREATE_NO_WINDOW gives it a console with no window instead.
    cmd.creation_flags(CREATE_NO_WINDOW.0);
    let child = cmd.spawn()?;
    if let Some(h) = child.raw_handle() {
        // Best-effort: a child that crashes between spawn and assign
        // escapes the job (a microsecond race accepted for v1).
        if let Err(e) = job.assign(h) {
            error!("activation: could not assign child to job object: {e}");
        }
    }
    Ok(Spawned { child, shutdown_event })
}

/// Signal graceful shutdown via the child's event, give it `grace`, then
/// `TerminateProcess` as the backstop and reap. A daemon that ignores the
/// event simply hits the backstop.
pub(crate) async fn stop_proc(proc: &mut Spawned, grace: Duration) {
    let h = HANDLE(proc.shutdown_event.as_raw_handle() as *mut c_void);
    if let Err(e) = unsafe { SetEvent(h) } {
        error!("activation: SetEvent(shutdown) failed: {e}");
    }
    let _ = timeout(grace, proc.child.wait()).await;
    let _ = proc.child.kill().await;
}

/// No unix-style executable bit on Windows; the caller's `is_file` check
/// is sufficient.
pub(crate) fn validate_exe(_md: &std::fs::Metadata) -> Result<()> {
    Ok(())
}

/// No system-wide unit directory on the Windows workstation — units live
/// under the per-user `%APPDATA%\netidx\activation` only.
pub(crate) fn system_units_dir() -> Option<PathBuf> {
    None
}

// ---- local control transport (named pipe) --------------------------------

/// A named-pipe control listener. A `NamedPipeServer` instance serves one
/// client, so `accept_control` swaps in a fresh instance after each
/// connection.
pub(crate) struct ControlListener {
    name: String,
    pending: NamedPipeServer,
}

/// The connected control stream handed to `handle_control_conn`.
pub(crate) type ControlStream = NamedPipeServer;

struct LocalSecurityDescriptor(PSECURITY_DESCRIPTOR);

impl Drop for LocalSecurityDescriptor {
    fn drop(&mut self) {
        if !self.0.0.is_null() {
            unsafe {
                let _ = LocalFree(Some(HLOCAL(self.0.0)));
            }
        }
    }
}

/// Create one control-pipe instance with a protected DACL. The logged-in user,
/// SYSTEM, and Administrators receive full control; no inherited/default ACE
/// grants Everyone enough access to connect and hold a server task open.
fn create_control_pipe(name: &str, first: bool) -> Result<NamedPipeServer> {
    let owner = netidx::windows::current_user_sid_string()
        .context("resolving the activation supervisor owner SID")?;
    let sddl = format!("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;{owner})");
    let wide: Vec<u16> = sddl.encode_utf16().chain(std::iter::once(0)).collect();
    let mut descriptor = PSECURITY_DESCRIPTOR::default();
    unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            PCWSTR(wide.as_ptr()),
            SDDL_REVISION_1,
            &mut descriptor,
            None,
        )
    }
    .context("building the activation control-pipe DACL")?;
    let descriptor = LocalSecurityDescriptor(descriptor);
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0.0,
        bInheritHandle: false.into(),
    };
    let mut options = ServerOptions::new();
    options.first_pipe_instance(first).reject_remote_clients(true);
    // SAFETY: `attributes` and its LocalAlloc-backed security descriptor stay
    // alive until CreateNamedPipeW returns. Tokio does not retain the pointer.
    unsafe {
        options.create_with_security_attributes_raw(
            name,
            &mut attributes as *mut SECURITY_ATTRIBUTES as *mut c_void,
        )
    }
    .map_err(|error| anyhow!(error))
    .with_context(|| format!("creating protected activation control pipe {name}"))
}

/// Create the first instance of the control pipe. Returns `None`
/// (logged) on failure — the supervisor still runs, just without remote
/// control.
pub(crate) fn bind_control(units_dir: &Path) -> Option<ControlListener> {
    let name = control::pipe_name(units_dir);
    match create_control_pipe(&name, true) {
        Ok(pending) => {
            info!("activation control pipe listening at {name}");
            Some(ControlListener { name, pending })
        }
        Err(e) => {
            error!("activation control: could not create pipe {name}: {e}");
            None
        }
    }
}

/// Pend forever when there is no listener, so the `select` arm is inert.
/// Otherwise await the next client, then pre-create the next instance so
/// the listener always has a server ready.
pub(crate) async fn accept_control(
    listener: &mut Option<ControlListener>,
) -> Option<ControlStream> {
    match listener {
        None => future::pending().await,
        Some(l) => match l.pending.connect().await {
            // After connect returns there is no further await, so building
            // the next instance + swapping is atomic w.r.t. cancellation.
            Ok(()) => match create_control_pipe(&l.name, false) {
                Ok(next) => Some(std::mem::replace(&mut l.pending, next)),
                Err(e) => {
                    // Couldn't pre-create the next instance (resource
                    // pressure). Reset the connected instance back to
                    // listening and drop this one request — degraded, not
                    // a hang.
                    error!(
                        "activation control: could not create next pipe instance: {e}"
                    );
                    let _ = l.pending.disconnect();
                    None
                }
            },
            Err(e) => {
                error!("activation control: pipe connect failed: {e}");
                sleep(Duration::from_millis(100)).await;
                None
            }
        },
    }
}

/// The named-pipe DACL is the trust boundary (see `bind_control`). This
/// is the defense-in-depth audit hook; v1 relies on the DACL.
pub(crate) fn peer_allowed(_stream: &ControlStream) -> bool {
    true
}

/// Named pipes are not filesystem objects; nothing to clean up.
pub(crate) fn remove_control(_units_dir: &Path) {}

// ---- signal sources -------------------------------------------------------

/// The supervisor's shutdown signal sources: ctrl-c and ctrl-break. There
/// is no reload signal on Windows (reload comes via the control protocol).
pub(crate) struct Signals {
    ctrl_c: CtrlC,
    ctrl_break: CtrlBreak,
}

impl Signals {
    pub(crate) fn new() -> Result<Signals> {
        Ok(Signals { ctrl_c: ctrl_c()?, ctrl_break: ctrl_break()? })
    }

    pub(crate) async fn next(&mut self) -> SigEvent {
        select_biased! {
            _ = self.ctrl_c.recv().fuse() => SigEvent::Shutdown,
            _ = self.ctrl_break.recv().fuse() => SigEvent::Shutdown,
        }
    }
}
