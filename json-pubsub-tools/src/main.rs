#[macro_use] extern crate serde_derive;
#[macro_use] extern crate structopt;
#[macro_use] extern crate json_pubsub;
#[macro_use] extern crate failure;
use json_pubsub::path::Path;
use std::{fs::read, path::PathBuf, net::SocketAddr};
use structopt::StructOpt;

mod resolver_server;
mod resolver;
mod publisher;

#[derive(Debug, Serialize, Deserialize)]
struct ResolverConfig {
    bind: SocketAddr,
    max_clients: usize,
    pid_file: PathBuf,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "json-pubsub")]
enum Opt {
    #[structopt(name = "resolver-server", about = "Run a resolver server")]
    ResolverServer {
        #[structopt(short = "c", long = "config",
                    help = "override the default config file",
                    default_value = "./resolver.conf",
                    parse(from_os_str))]
        config: PathBuf,
        #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
        foreground: bool
    },
    #[structopt(name = "resolver", about = "Query a resolver server")]
    Resolver {
        #[structopt(short = "c", long = "config",
                    help = "override the default config file",
                    default_value = "./resolver.conf",
                    parse(from_os_str))]
        config: PathBuf,
        #[structopt(subcommand)]
        cmd: ResolverCmd
    },
    #[structopt(name = "publisher",
                about = "publish path|data lines from stdin")]
    Publisher {
        #[structopt(short = "c",
                    long = "config",
                    help = "override the default config file",
                    default_value = "./resolver.conf",
                    parse(from_os_str))]
        config: PathBuf,
        #[structopt(short = "j", long = "json", help = "interpret data as json")]
        json: bool,
        #[structopt(
            short = "t", long = "timeout",
            help = "require subscribers to consume values before timeout seconds"
        )]
        timeout: Option<u64>,
    },
    /*
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
    let get_cfg = |path| -> ResolverConfig {
        serde_json::from_slice(&*read(path).expect("reading config"))
            .expect("parsing config")
    };
    match Opt::from_args() {
        Opt::ResolverServer {config, foreground} =>
            resolver_server::run(get_cfg(config), !foreground),
        Opt::Resolver {config, cmd} =>
            resolver::run(get_cfg(config), cmd),
        Opt::Publisher {config, json, timeout} =>
            publisher::run(get_cfg(config), json, timeout),
    }
}
