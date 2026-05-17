//! Schema and runtime for the netidx id-mapper daemon.
//!
//! [`file`] holds the on-disk JSON representation of the id-map;
//! [`runtime`] is the unix-socket daemon that answers queries from
//! the resolver's `IdMapType::Socket` mode.

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate anyhow;

pub mod file;
#[cfg(unix)]
pub mod runtime;
