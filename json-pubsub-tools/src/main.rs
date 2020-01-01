#[macro_use] extern crate serde_derive;
#[macro_use] extern crate structopt;
use json_pubsub::path::Path;
use std::{fs::read, path::PathBuf, net::SocketAddr};
use structopt::StructOpt;

mod resolver_server;
mod resolver;

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
    Resolver(Resolver),
    /*
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
     */
}

#[derive(StructOpt, Debug)]
struct Resolver {
    #[structopt(short = "c", long = "config",
                help = "override the default config file",
                default_value = "./resolver.conf",
                parse(from_os_str))]
    config: PathBuf,
    #[structopt(subcommand)]
    cmd: ResolverCmd
}

#[derive(StructOpt, Debug)]
enum ResolverCmd {
    #[structopt(name = "list", about = "list entries in the resolver server")]
    List {
        #[structopt(short = "r", long = "recursive", help = "recurse to children")]
        recursive: bool,
        #[structopt(short = "l", long = "long",
                    help = "long form, return additional info")]
        long: bool,
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
    match Opt::from_args() {
        Opt::ResolverServer {config, foreground} => {
            let config: ResolverConfig = serde_json::from_slice(
                &*read(config).expect("reading config")
            ).expect("parsing config");
            resolver_server::run(config, !foreground)
        }
    }
}
