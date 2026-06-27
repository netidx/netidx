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
