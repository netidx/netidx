#[macro_use] extern crate serde_derive;
#[macro_use] extern crate json_pubsub;
#[macro_use] extern crate failure;
use json_pubsub::path::Path;
use std::{fs::read, path::PathBuf, net::SocketAddr};
use structopt::StructOpt;

mod resolver_server;
mod resolver;
mod publisher;
mod subscriber;

#[derive(Debug, Serialize, Deserialize)]
struct ResolverConfig {
    bind: SocketAddr,
    max_clients: usize,
    pid_file: PathBuf,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "json-pubsub")]
struct Opt {
    #[structopt(short = "c", long = "config",
                help = "override the default config file",
                default_value = "./resolver.conf",
                parse(from_os_str))]
    config: PathBuf,
    #[structopt(subcommand)]
    cmd: Sub,
}

#[derive(StructOpt, Debug)]
enum Sub {
    #[structopt(name = "resolver-server", about = "Run a resolver server")]
    ResolverServer {
        #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
        foreground: bool
    },
    #[structopt(name = "resolver", about = "Query a resolver server")]
    Resolver {
        #[structopt(subcommand)]
        cmd: ResolverCmd
    },
    #[structopt(name = "publisher",
                about = "publish path|data lines from stdin")]
    Publisher {
        #[structopt(short = "j", long = "json", help = "interpret data as json")]
        json: bool,
        #[structopt(
            short = "t", long = "timeout",
            help = "require subscribers to consume values before timeout seconds"
        )]
        timeout: Option<u64>,
    },
    #[structopt(name = "subscriber", about = "subscribe and print values")]
    Subscriber {
        #[structopt(name = "path")]
        paths: Vec<String>
    },
    /*
    #[structopt(name = "stress", about = "stress test")]
    Stress {
        #[structopt(short = "c", long = "config", help = "override default config")]
        config: Option<PathBuf>,
        #[structopt(subcommand)]
        cmd: Stress
    },
     */
}

#[derive(StructOpt, Debug)]
enum ResolverCmd {
    #[structopt(name = "resolve", about = "resolve an in the resolver server")]
    Resolve {
        path: Path,
    },
    #[structopt(name = "list", about = "list entries in the resolver server")]
    List {
        #[structopt(name = "path")]
        path: Option<Path>
    },
    #[structopt(name = "add", about = "add a new entry")]
    Add {
        #[structopt(name = "path")]
        path: Path,
        #[structopt(name = "socketaddr")]
        socketaddr: SocketAddr
    },
    #[structopt(name = "remove", about = "remove an entry")]
    Remove {
        #[structopt(name = "path")]
        path: Path,
        #[structopt(name = "socketaddr")]
        socketaddr: SocketAddr
    }
}

/*
#[derive(StructOpt, Bench)]
enum Stress {
    #[structopt(name = "publisher", about = "run a stress test publisher")]
    Publisher,
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber
}
 */

fn main() {
    let opt = Opt::from_args();
    let cfg =
        serde_json::from_slice(&*read(opt.config).expect("reading config"))
        .expect("parsing config");
    match opt.cmd {
        Sub::ResolverServer {foreground} => resolver_server::run(cfg, !foreground),
        Sub::Resolver {cmd} => resolver::run(cfg, cmd),
        Sub::Publisher {json, timeout} => publisher::run(cfg, json, timeout),
        Sub::Subscriber {paths} => subscriber::run(cfg, paths),
    }
}
