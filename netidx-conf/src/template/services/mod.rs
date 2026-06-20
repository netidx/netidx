//! Service templates — typed shortcuts for "drop an activation unit
//! that runs <netidx subcommand> with sensible defaults." Used by
//! `netidx conf component activation add <service> …` to skip the manual
//! `--exe / --arg` plumbing.
//!
//! Each service is its own module so the parameter struct can be
//! customized to the subcommand's CLI. The shared shape — what the
//! activation engine consumes — is a `netidx_activation::file::Unit`
//! produced by the service's `unit(&Params)` function.

pub mod conf_server;
pub mod container;
pub mod id_map;
pub mod renew;
