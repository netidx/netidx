#![feature(proc_macro, proc_macro_non_items, generators)]
#[macro_use]
extern crate serde_derive;
extern crate tokio;
extern crate tokio_timer;
extern crate futures_await as futures;
extern crate serde;
extern crate serde_json;
extern crate json_pubsub;

use json_pubsub::path::Path;
use std::{time::Duration, path::PathBuf, net::SocketAddr};

#[derive(StructOpt, Debug)]
#[structopt(name = "json-pubsub")]
enum Opt {
  #[structopt(name = "resolver-server", about = "Run a resolver server")]
  Resolver_server {
    #[structopt(short = "c", long = "config", help = "override the default config file",
                parse(from_os_str))]
    config: Option<PathBuf>,
    #[structipt(short = "f", long = "foreground", help = "don't daemonize")]
    foreground: bool
  },
  #[structopt(name = "resolver", about = "Query a resolver server")]
  Resolver(Resolver),
  #[structopt(name = "publisher", about = "publish lines for stdin or a file")]
  Publisher {
    #[structopt(short = "f", long = "file", help = "publish the contents of file",
                default = "stdin", parse(from_os_str))]
    file: Option<PathBuf>,
    #[structopt(short = "s", long = "static", help = "publish a static value")]
    sval: Option<String>,
    #[structopt(name = "path")]
    path: Path
  },
  #[structopt(name = "subscriber", about = "subscribe and print json values")]
  Subscriber {
    #[structopt(short = "f", long = "file", help = "subscribe to all paths in file")]
    file: Option<PathBuf>,
    #[structopt(name = "path")]
    paths: Vec<Path>
  },
  #[structopt(name = "stress", about = "stress test")]
  Stress {
    #[structopt(short = "c", long = "config", help = "override default config")]
    config: Option<PathBuf>,
    #[structopt(subcommand)]
    cmd: Stress
  },
}

#[derive(StructOpt, Debug)]
struct Resolver {
  #[structopt(short = "c", long = "config", help = "override the default config file",
              parse(from_os_str))]
  config: Option<PathBuf>,
  #[structopt(subcommand)]
  cmd: ResolverCmd
}

#[derive(StructOpt, Debug)]
enum ResolverCmd {
  #[structopt(name = "list", about = "list entries in the resolver server")]
  List {
    #[structopt(short = "r", long = "recursive", help = "recurse to children")]
    recursive: bool,
    #[structopt(short = "l", long = "long", help = "long form, return additional info")]
    long: bool,
    #[structopt(name = "path")]
    path: Option<Path>
  },
  #[structopt(name = "add", about = "add a new entry")]
  Add {
    #[structopt(name = "path")]
    path: Path,
    #[structopt(name = "socketaddr")],
    socketaddr: SocketAddr
  },
  #[structopt(name = "remove", about = "remove an entry")]
  Remove {
    #[structopt(name = "path")]
    path: Path,
    #[structopt(name = "socketaddr")],
    socketaddr: SocketAddr
  }
}

#[derive(StructOpt, Bench)]
enum Stress {
  #[structopt(name = "publisher", about = "run a stress test publisher")]
  Publisher,
  #[structopt(name = "subscriber", about = "run a stress test subscriber")]
  Subscriber
}

/*
static RESOLVER: &str = "127.0.0.1:1234";

fn to_secs(t: Duration) -> f64 {
  t.as_secs() as f64 + (t.subsec_nanos() as f64 / 1e9)
}

mod publisher {
  use json_pubsub::{
    publisher::{Publisher, BindCfg},
    path::Path, error::*, resolver_client::Resolver
  };
  use tokio;
  use futures::prelude::*;
  use std::{time::Instant, net::SocketAddr};
  use RESOLVER;
  use to_secs;

  #[derive(Serialize, Deserialize, Debug)]
  pub(crate) struct V {
    pub field0: i64,
    pub field1: f64,
    pub field2: String
  }

  impl V {
    fn new() -> Self {
      V {
        field0: 0,
        field1: 42.,
        field2: "the answer to everything".into()
      }
    }
  }

  #[async]
  fn publish() -> Result<()> {
    let r = await!(Resolver::new(RESOLVER.parse::<SocketAddr>().unwrap()))?;
    let p = Publisher::new(r, BindCfg::Local)?;
    let v0 = await!(p.clone().publish(Path::from("/path0/path1/v0"), V::new()))?;
    let v1 = await!(p.clone().publish(Path::from("/path0/path1/v1"), V::new()))?;
    let mut v = V::new();
    let mut c = 0;
    await!(p.clone().wait_client(1))?;
    let start = Instant::now();
    loop {
      v.field0 += 1;
      v0.update(&v)?;
      c += 1;
      if c % 100000 == 0 {
        await!(p.clone().flush())?;
        await!(p.clone().wait_client(1))?;
        v1.update(&v);
        println!("{} msgs/s", (c as f64 / to_secs(start.elapsed())) as i64);
      }
    }
  }

  pub(crate) fn run_st() {
    let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
    rt.block_on(publish().map_err(|e| println!("error: {}", e))).unwrap();
  }

  pub(crate) fn run_mt() { tokio::run(publish().map_err(|e| println!("error: {}", e))) }
}

mod subscriber {
  use json_pubsub::{
    subscriber::Subscriber, path::Path,
    resolver_client::Resolver, error::*
  };
  use std::time::Instant;
  use tokio;
  use futures::prelude::*;
  use RESOLVER;
  use std::net::SocketAddr;
  use publisher::V;
  use to_secs;

  #[async]
  fn subscribe() -> Result<()> {
    let r = await!(Resolver::new(RESOLVER.parse::<SocketAddr>().unwrap()))?;
    let s = Subscriber::new(r);
    let s0 = await!(s.subscribe::<V>(Path::from("/path0/path1/v0")))?;
    let mut c : usize = 0;
    let start = Instant::now();
    let mut prev = v.field0;
    #[async]
    for v in s0.updates(1000000) {
      if c == 0 { prev = v.field0; }
      else if v.field0 != prev + 1 {
        println!("error out of seq, prev: {}, v: {:?}", prev, v);
      }
      prev = v.field0;
      c += 1;
      if c % 100000 == 0 {
        println!("{} msgs/s", (c as f64 / to_secs(start.elapsed())) as i64);
      }
    }
    Ok(())
  }

  pub(crate) fn run_st() {
    let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
    rt.block_on(subscribe().map_err(|_| ())).unwrap();
  }

  pub(crate) fn run_mt() {
    tokio::run(subscribe().map_err(|_| ()));
  }
}

mod resolver_server {
  use futures::{future, prelude::*};
  use json_pubsub::resolver_server::Server;
  use std::{net::SocketAddr, mem};
  use tokio;
  use RESOLVER;

  pub(crate) fn run() {
    tokio::run(async_block! {
      mem::forget(await!(Server::new(RESOLVER.parse::<SocketAddr>().unwrap())).unwrap());
      println!("resolver is running");
      let _ : Result<(), ()> = await!(future::empty());
      Ok(())
    });
  }
}

fn main() {
  match ::std::env::args().nth(1).unwrap().as_ref() {
    "resolver" => resolver_server::run(),
    "publisher" => publisher::run_mt(),
    "subscriber" => subscriber::run_mt(),
    bad => println!("invalid arg: {}", bad)
  }
}
*/
