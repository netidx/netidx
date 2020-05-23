#![recursion_limit = "1024"]
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate json_pubsub;

use json_pubsub::{config, path::Path, publisher::BindCfg, resolver::Auth};
use log::warn;
use std::net::SocketAddr;
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
        help = "config, either file:PATH, or dns:NAME, or just dns for the default",
        default_value = "dns"
    )]
    config: String,
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
            long = "delay-reads",
            help = "don't allow read clients until 1 writer ttl has passed"
        )]
        delay_reads: bool,
        #[structopt(
            long = "id",
            help = "index of the address to bind to",
            default_value = "0"
        )]
        id: usize,
        #[structopt(
            short = "p",
            long = "permissions",
            help = "location of the permissions, file:PATH or dns:NAME, or just dns"
        )]
        permissions: Option<String>,
    },
    #[structopt(name = "resolver", about = "Query a resolver server")]
    Resolver {
        #[structopt(short = "k", long = "krb5", help = "use Kerberos 5")]
        krb5: bool,
        #[structopt(long = "upn", help = "krb5 use <upn> instead of the current user")]
        upn: Option<String>,
        #[structopt(subcommand)]
        cmd: ResolverCmd,
    },
    #[structopt(name = "publisher", about = "publish path|data lines from stdin")]
    Publisher {
        #[structopt(short = "k", long = "krb5", help = "use Kerberos 5")]
        krb5: bool,
        #[structopt(
            short = "b",
            long = "bind",
            help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
        )]
        bind: BindCfg,
        #[structopt(long = "upn", help = "krb5 use <upn> instead of the current user")]
        upn: Option<String>,
        #[structopt(long = "spn", help = "krb5 use <spn>")]
        spn: Option<String>,
        #[structopt(long = "type", help = "data type (use help for a list)")]
        typ: publisher::Typ,
        #[structopt(
            long = "timeout",
            help = "require subscribers to consume values before timeout (seconds)"
        )]
        timeout: Option<u64>,
    },
    #[structopt(name = "subscriber", about = "subscribe and print values")]
    Subscriber {
        #[structopt(short = "k", long = "krb5", help = "use Kerberos 5")]
        krb5: bool,
        #[structopt(long = "upn", help = "krb5 use <upn> instead of the current user")]
        upn: Option<String>,
        #[structopt(name = "paths")]
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
        #[structopt(short = "k", long = "krb5", help = "use Kerberos 5")]
        krb5: bool,
        #[structopt(
            short = "b",
            long = "bind",
            help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
        )]
        bind: BindCfg,
        #[structopt(long = "upn", help = "krb5 use <upn> instead of the current user")]
        upn: Option<String>,
        #[structopt(long = "spn", help = "krb5 use <spn>")]
        spn: Option<String>,
        #[structopt(name = "nvals", default_value = "100")]
        nvals: usize,
    },
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber {
        #[structopt(short = "k", long = "krb5", help = "use Kerberos 5")]
        krb5: bool,
        #[structopt(long = "upn", help = "krb5 use <upn> instead of the current user")]
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
    env_logger::init();
    let opt = Opt::from_args();
    let cfg = {
        if &opt.config == "dns" {
            config::Config::load_from_dns(None).expect("config")
        } else if opt.config.starts_with("dns:") {
            config::Config::load_from_dns(Some(opt.config.trim_start_matches("dns:")))
                .expect("config")
        } else if opt.config.starts_with("file:") {
            config::Config::load_from_file(opt.config.trim_start_matches("file:"))
                .expect("config")
        } else {
            panic!("{} unrecognized config location", opt.config);
        }
    };
    match opt.cmd {
        Sub::ResolverServer { foreground, delay_reads, id, permissions } => {
            let anon = match cfg.auth {
                config::Auth::Anonymous => true,
                config::Auth::Krb5(_) => false,
            };
            let permissions = match permissions {
                None if anon => config::PMap::default(),
                None => panic!("--permissions is required when using Kerberos"),
                Some(_) if anon => {
                    warn!("ignoring --permissions, server not using Kerberos");
                    config::PMap::default()
                }
                Some(p) => {
                    if p == "dns" {
                        config::PMap::load_from_dns(None).expect("permissions")
                    } else if p.starts_with("dns:") {
                        config::PMap::load_from_dns(Some(p.trim_start_matches("dns:")))
                            .expect("permissions")
                    } else if p.starts_with("file:") {
                        config::PMap::load_from_file(p.trim_start_matches("file:"))
                            .expect("permissions")
                    } else {
                        panic!("{} unrecognized permissions location", p)
                    }
                }
            };
            resolver_server::run(cfg, permissions, !foreground, delay_reads, id)
        }
        Sub::Resolver { krb5, upn, cmd } => {
            resolver::run(cfg, cmd, auth(krb5, upn, None))
        }
        Sub::Publisher { bind, krb5, upn, spn, typ, timeout } => {
            publisher::run(cfg, bind, typ, timeout, auth(krb5, upn, spn))
        }
        Sub::Subscriber { krb5, upn, paths } => {
            subscriber::run(cfg, paths, auth(krb5, upn, None))
        }
        Sub::Stress { cmd } => match cmd {
            Stress::Subscriber { krb5, upn } => {
                stress_subscriber::run(cfg, auth(krb5, upn, None))
            }
            Stress::Publisher { bind, krb5, upn, spn, nvals } => {
                stress_publisher::run(cfg, bind, nvals, auth(krb5, upn, spn))
            }
        },
    }
}
