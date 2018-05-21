#![feature(proc_macro, generators, nll)]

extern crate gtk;
extern crate glib;
extern crate relm;
extern crate relm_core;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate tokio;
extern crate tokio_io;
extern crate futures_await as futures;
extern crate rand;
#[macro_use]
extern crate error_chain;

mod line_writer;
pub mod utils;
pub mod error;
pub mod path;
pub mod resolver_client;
pub mod resolver_server;
pub mod publisher;
pub mod subscriber;

use path::Path;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Input {
  name: String,
  description: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Output {
  name: String,
  description: String,
  path: Path
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Widget {
  name: String,
  description: String,
  ui: bool,
  inputs: HashMap<String, Input>,
  outputs: HashMap<String, Output>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Process {
  program: Widget,
  base: Path,
  connections: HashMap<String, Path>,
}

pub struct Workspace {
  base: Path,
  widgets: HashMap<String, Widget>,
  processes: Vec<Process>,
}
