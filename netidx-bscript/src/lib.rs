#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate netidx_core;
#[macro_use]
extern crate combine;
#[macro_use]
extern crate serde_derive;

pub mod expr;
pub mod stdfn;
pub mod vm;

#[cfg(test)]
mod tests;
