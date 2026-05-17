//! Schema and runtime for netidx activation units.
//!
//! [`file`] holds the on-disk representation of a unit; [`runtime`] is
//! the daemon's process supervisor.

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate anyhow;

pub mod file;
#[cfg(unix)]
pub mod runtime;
