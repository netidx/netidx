//! Child-side of the activation shutdown contract.
//!
//! A supervised netidx daemon calls [`wait`] and runs its clean-shutdown
//! path once it returns. The supervisor requests a graceful stop via
//! SIGTERM on unix and, on Windows, by signaling the per-child manual-
//! reset event whose name it passes in [`SHUTDOWN_EVENT_VAR`] (see
//! `platform::windows::spawn`). ctrl-c is always honored too, so an
//! interactively-run daemon stops the same way.
//!
//! The supervisor backs this with a hard kill (SIGKILL / TerminateProcess)
//! after a grace period, so a daemon that ignores the request — or whose
//! clean-shutdown path hangs — is still reaped.

/// Environment variable through which the supervisor passes a child its
/// Windows shutdown-event name. The supervisor side sets it in
/// `platform::windows::spawn`; the child side reads it here.
#[cfg(windows)]
pub const SHUTDOWN_EVENT_VAR: &str = "NETIDX_SHUTDOWN_EVENT";

/// Block until a shutdown is requested from any source.
pub async fn wait() {
    #[cfg(unix)]
    unix::wait().await;
    #[cfg(windows)]
    win::wait().await;
    #[cfg(not(any(unix, windows)))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(unix)]
mod unix {
    use tokio::signal::unix::{SignalKind, signal};

    pub(super) async fn wait() {
        // SIGTERM is how the supervisor asks for a graceful stop; ctrl-c
        // covers an interactive run. (SIGINT is delivered as ctrl-c.)
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = term.recv() => {},
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
}

#[cfg(windows)]
mod win {
    use super::SHUTDOWN_EVENT_VAR;
    use std::ffi::c_void;
    use windows::{
        Win32::{
            Foundation::{CloseHandle, HANDLE},
            System::Threading::{
                OpenEventW, SYNCHRONIZATION_ACCESS_RIGHTS, WaitForSingleObject,
            },
        },
        core::PCWSTR,
    };

    pub(super) async fn wait() {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = wait_event() => {},
        }
    }

    /// Wait for the supervisor's per-child shutdown event. If the variable
    /// is unset (run standalone) or the event can't be opened, pend
    /// forever — never *spuriously* signal shutdown; ctrl-c and the
    /// supervisor's hard-kill backstop still apply.
    async fn wait_event() {
        let name = match std::env::var(SHUTDOWN_EVENT_VAR) {
            Ok(n) => n,
            Err(_) => return std::future::pending().await,
        };
        // OpenEventW is a fast syscall, fine to run on the async thread.
        // Carry only the raw handle value (isize is Send; HANDLE is not)
        // into the blocking wait.
        let raw: Option<isize> = {
            let wname: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
            // SYNCHRONIZE (0x0010_0000) is enough to wait on the event.
            match unsafe {
                OpenEventW(
                    SYNCHRONIZATION_ACCESS_RIGHTS(0x0010_0000),
                    false,
                    PCWSTR(wname.as_ptr()),
                )
            } {
                Ok(h) => Some(h.0 as isize),
                Err(e) => {
                    log::warn!("activation: could not open shutdown event {name}: {e}");
                    None
                }
            }
        };
        match raw {
            None => std::future::pending().await,
            Some(raw) => {
                let _ = tokio::task::spawn_blocking(move || unsafe {
                    let h = HANDLE(raw as *mut c_void);
                    // INFINITE == 0xFFFF_FFFF
                    WaitForSingleObject(h, 0xFFFF_FFFF);
                    let _ = CloseHandle(h);
                })
                .await;
            }
        }
    }
}
