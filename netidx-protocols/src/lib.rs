#[macro_use] extern crate serde_derive;
#[macro_use] extern crate combine;
#[macro_use] extern crate packed_struct_codegen;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate anyhow;
#[macro_use] extern crate netidx_core;

pub mod view;
pub mod archive;
pub mod cluster;
pub mod rpc;
mod parser;
