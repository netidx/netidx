#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate netidx_core;

pub mod cluster;
pub mod rpc;
pub mod view;
pub mod channel;
pub mod pack_channel;
