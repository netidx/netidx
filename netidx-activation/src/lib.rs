//! Schema and runtime for netidx activation units.
//!
//! [`file`] holds the on-disk representation of a unit; [`runtime`] is
//! the daemon's process supervisor.

#[macro_use]
extern crate serde_derive;

pub mod control;
pub mod file;
#[cfg(any(unix, windows))]
mod platform;
#[cfg(any(unix, windows))]
pub mod runtime;
pub mod shutdown;

/// Supervised-child machinery, usable outside the activation daemon.
///
/// The platform layer's cross-platform quartet: [`process::spawn`] a
/// [`process::Spawned`] child, optionally inside a [`process::Job`] (a
/// Windows job object with `KILL_ON_JOB_CLOSE`, so children die with
/// their parent; a no-op handle on unix; pass `None` to spawn a child
/// that may outlive the parent), and [`process::stop_proc`] it gracefully —
/// SIGTERM → grace → SIGKILL on unix; the shutdown-event handshake with
/// `TerminateProcess` as the backstop on Windows. `Spawned` exposes
/// `wait` and the stdio pipe takers; ownership of the child is the
/// caller's concurrency discipline (give it to one task).
#[cfg(any(unix, windows))]
pub mod process {
    pub use crate::platform::{spawn, stop_proc, Job, Spawned};
}
