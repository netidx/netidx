#[macro_use] extern crate lazy_static;
#[macro_use] extern crate netidx_core;
#[macro_use] extern crate combine;
#[macro_use] extern crate serde_derive;

mod parser;
pub mod expr;
pub mod vm;
pub mod stdfn;
