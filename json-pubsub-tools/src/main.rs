#![recursion_limit = "1024"]
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate json_pubsub;
#[macro_use]
extern crate failure;
use json_pubsub::{config, path::Path, resolver::Auth};
use std::{fs::read, net::SocketAddr, path::PathBuf};
use structopt::StructOpt;

mod publisher;
mod resolver;
mod resolver_server;
mod stress_publisher;
mod stress_subscriber;
mod subscriber;

#[derive(StructOpt, Debug)]
#[structopt(name = "json-pubsub")]
struct Opt {
    #[structopt(
        short = "c",
        long = "config",
        help = "override the default config file",
        default_value = "./resolver.conf",
        parse(from_os_str)
    )]
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
        #[structopt(
            short = "c",
            long = "config",
            help = "override the default server config file",
            default_value = "./resolver-server.conf",
            parse(from_os_str)
        )]
        config: PathBuf,
    },
    #[structopt(name = "resolver", about = "Query a resolver server")]
    Resolver {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(long = "upn", help = "use the specified krb5 user principal name")]
        upn: Option<String>,
        #[structopt(subcommand)]
        cmd: ResolverCmd,
    },
    #[structopt(name = "publisher", about = "publish path|data lines from stdin")]
    Publisher {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(long = "upn", help = "use the specified krb5 user principal name")]
        upn: Option<String>,
        #[structopt(
            long = "spn",
            help = "use the specified krb5 service principal name"
        )]
        spn: Option<String>,
        #[structopt(short = "j", long = "json", help = "interpret data as json")]
        json: bool,
        #[structopt(
            short = "t",
            long = "timeout",
            help = "require subscribers to consume values before timeout seconds"
        )]
        timeout: Option<u64>,
    },
    #[structopt(name = "subscriber", about = "subscribe and print values")]
    Subscriber {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(long = "upn", help = "use the specified krb5 user principal name")]
        upn: Option<String>,
        #[structopt(name = "path")]
        paths: Vec<String>,
    },
    #[structopt(name = "stress", about = "stress test")]
    Stress {
        #[structopt(subcommand)]
        cmd: Stress,
    },
}

#[derive(StructOpt, Debug)]
enum ResolverCmd {
    #[structopt(name = "resolve", about = "resolve an in the resolver server")]
    Resolve { path: Path },
    #[structopt(name = "list", about = "list entries in the resolver server")]
    List {
        #[structopt(name = "path")]
        path: Option<Path>,
    },
    #[structopt(name = "add", about = "add a new entry")]
    Add {
        #[structopt(name = "path")]
        path: Path,
        #[structopt(name = "socketaddr")]
        socketaddr: SocketAddr,
    },
    #[structopt(name = "remove", about = "remove an entry")]
    Remove {
        #[structopt(name = "path")]
        path: Path,
        #[structopt(name = "socketaddr")]
        socketaddr: SocketAddr,
    },
}

#[derive(StructOpt, Debug)]
enum Stress {
    #[structopt(name = "publisher", about = "run a stress test publisher")]
    Publisher {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(long = "upn", help = "use the specified krb5 user principal name")]
        upn: Option<String>,
        #[structopt(
            long = "spn",
            help = "use the specified krb5 service principal name"
        )]
        spn: Option<String>,
        #[structopt(name = "nvals", default_value = "100")]
        nvals: usize,
        #[structopt(name = "vsize", default_value = "1")]
        vsize: usize,
    },
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber {
        #[structopt(short = "k", long = "krb5", help = "use kerberos v5 security")]
        krb5: bool,
        #[structopt(long = "upn", help = "use the specified krb5 user principal name")]
        upn: Option<String>,
    },
}

fn auth(krb5: bool, upn: Option<String>, spn: Option<String>) -> Auth {
    if !krb5 {
        Auth::Anonymous
    } else {
        Auth::Krb5 { upn, spn }
    }
}

fn main() {
    let opt = Opt::from_args();
    let cfg: config::resolver::Config =
        serde_json::from_slice(&*read(opt.config).expect("reading config"))
            .expect("parsing config");
    match opt.cmd {
        Sub::ResolverServer { foreground, config } => {
            resolver_server::run(config, !foreground)
        }
        Sub::Resolver { krb5, upn, cmd } => {
            resolver::run(cfg, cmd, auth(krb5, upn, None))
        }
        Sub::Publisher {
            krb5,
            upn,
            spn,
            json,
            timeout,
        } => publisher::run(cfg, json, timeout, auth(krb5, upn, spn)),
        Sub::Subscriber { krb5, upn, paths } => {
            subscriber::run(cfg, paths, auth(krb5, upn, None))
        }
        Sub::Stress { cmd } => match cmd {
            Stress::Subscriber { krb5, upn } => {
                stress_subscriber::run(cfg, auth(krb5, upn, None))
            }
            Stress::Publisher {
                krb5,
                upn,
                spn,
                nvals,
                vsize,
            } => stress_publisher::run(cfg, nvals, vsize, auth(krb5, upn, spn)),
        },
    }
}
