//! Platform abstraction for the activation supervisor.
//!
//! The supervisor loop in [`crate::runtime`] is platform-agnostic; the
//! OS-specific pieces — the local control transport, child process
//! stop/spawn, privilege handling, orphan reaping, and the signal
//! sources that drive shutdown/reload — live here, one impl per
//! platform. This mirrors `netidx::os` and `netidx-admin`'s
//! `service::platform`: a single shared body plus a thin per-OS module.

#[cfg(unix)]
#[path = "unix.rs"]
mod imp;

#[cfg(windows)]
#[path = "windows.rs"]
mod imp;

pub(crate) use imp::*;

/// What a platform signal source produced. The supervisor reacts to two
/// things: a request to shut down, or a request to reload its unit
/// directory. On unix these come from SIGINT/SIGTERM/SIGQUIT and SIGHUP
/// respectively; on Windows shutdown comes from console control events
/// and reload only ever arrives via the control protocol
/// ([`crate::control::ControlOp::Reload`]), so the signal source never
/// yields [`SigEvent::Reload`].
pub(crate) enum SigEvent {
    Shutdown,
    /// Only produced by the unix SIGHUP source; on Windows reload arrives
    /// via the control protocol, so this variant is never constructed.
    #[cfg_attr(not(unix), allow(dead_code))]
    Reload,
}
