#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate netidx_core;
#[macro_use]
extern crate serde_derive;

pub mod glob;
pub mod publisher;
pub mod resolver;

pub use netidx_value as value;
pub use value::array as valarray;
pub use value::parser as value_parser;
pub use value::pbuf;

#[cfg(test)]
mod test;
