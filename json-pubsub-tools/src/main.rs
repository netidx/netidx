#![recursion_limit="1024"]
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate json_pubsub;
#[macro_use] extern crate failure;
use json_pubsub::{
    path::Path,
    resolver::Auth,
    config,
};
use std::{fs::read, path::PathBuf, net::SocketAddr};
use structopt::StructOpt;

mod resolver_server;
mod resolver;
mod publisher;
mod subscriber;
mod stress_publisher;
mod stress_subscriber;

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
        foreground: bool,
        #[structopt(short = "c", long = "config",
                    help = "override the default server config file",
                    default_value = "./resolver-server.conf",
                    parse(from_os_str))]
        config: PathBuf,
    },
    #[structopt(name = "resolver", about = "Query a resolver server")]
    Resolver {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(short = "p", long = "principal",
                    help = "use the specified krb5 principal")]
        principal: Option<String>,
        #[structopt(subcommand)]
        cmd: ResolverCmd
    },
    #[structopt(name = "publisher",
                about = "publish path|data lines from stdin")]
    Publisher {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(short = "p", long = "principal",
                    help = "use the specified krb5 principal")]
        principal: Option<String>,
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
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(short = "p", long = "principal",
                    help = "use the specified krb5 principal")]
        principal: Option<String>,
        #[structopt(name = "path")]
        paths: Vec<String>
    },
    #[structopt(name = "stress", about = "stress test")]
    Stress {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(short = "p", long = "principal",
                    help = "use the specified krb5 principal")]
        principal: Option<String>,
        #[structopt(subcommand)]
        cmd: Stress
    },
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

#[derive(StructOpt, Debug)]
enum Stress {
    #[structopt(name = "publisher", about = "run a stress test publisher")]
    Publisher {
        #[structopt(name = "nvals", default_value = "100")]
        nvals: usize,
        #[structopt(name = "vsize", default_value = "1")]
        vsize: usize,
    },
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber
}

fn auth(krb5: bool, principal: Option<String>) -> Auth {
    if !krb5 {
        Auth::Anonymous
    } else {
        Auth::Krb5 {principal}
    }
}

fn main() {
    let opt = Opt::from_args();
    let cfg: config::resolver::Config =
        serde_json::from_slice(&*read(opt.config).expect("reading config"))
        .expect("parsing config");
    match opt.cmd {
        Sub::ResolverServer {foreground, config} =>
            resolver_server::run(config, !foreground),
        Sub::Resolver {krb5, principal, cmd} =>
            resolver::run(cfg, cmd, auth(krb5, principal)),
        Sub::Publisher {krb5, principal, json, timeout} =>
            publisher::run(cfg, json, timeout, auth(krb5, principal)),
        Sub::Subscriber {krb5, principal, paths} =>
            subscriber::run(cfg, paths, auth(krb5, principal)),
        Sub::Stress {krb5, principal, cmd} => match cmd {
            Stress::Subscriber => stress_subscriber::run(cfg, auth(krb5, principal)),
            Stress::Publisher {nvals, vsize} =>
                stress_publisher::run(cfg, nvals, vsize, auth(krb5, principal)),
        }
    }
}
