#![recursion_limit = "2048"]
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate netidx;
#[macro_use] extern crate anyhow;
#[cfg(feature = "browser")]
#[macro_use] extern crate glib;

use log::warn;
use netidx::{config, path::Path, publisher::BindCfg, resolver::Auth};
use std::net::SocketAddr;
use structopt::StructOpt;

#[cfg(feature = "browser")]
mod browser;
mod publisher;
mod resolver;
mod stress_publisher;
mod stress_subscriber;
mod subscriber;

#[cfg(unix)]
mod resolver_server;

#[cfg(not(unix))]
mod resolver_server {
    use netidx::config;

    pub(crate) fn run(
        _config: config::Config,
        _permissions: config::PMap,
        _daemonize: bool,
        _delay_reads: bool,
        _id: usize,
    ) {
        todo!("the resolver server is not yet ported to this platform")
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "json-pubsub")]
struct Opt {
    #[structopt(
        short = "c",
        long = "config",
        help = "override the default config file location (~/.config/netidx.json)"
    )]
    config: Option<String>,
    #[structopt(short = "a", long = "anonymous", help = "disable Kerberos 5")]
    anon: bool,
    #[structopt(long = "upn", help = "krb5 use <upn> instead of the current user")]
    upn: Option<String>,
    #[structopt(subcommand)]
    cmd: Sub,
}

#[derive(StructOpt, Debug)]
enum Sub {
    #[structopt(name = "browser", about = "netidx browser")]
    Browser {
        #[structopt(name = "path")]
        path: Option<Path>,
    },
    #[structopt(name = "resolver-server", about = "run a resolver")]
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
            help = "location of the permissions file"
        )]
        permissions: Option<String>,
    },
    #[structopt(name = "resolver", about = "query the resolver")]
    Resolver {
        #[structopt(subcommand)]
        cmd: ResolverCmd,
    },
    #[structopt(name = "publisher", about = "publish data")]
    Publisher {
        #[structopt(
            short = "b",
            long = "bind",
            help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
        )]
        bind: BindCfg,
        #[structopt(long = "spn", help = "krb5 use <spn>")]
        spn: Option<String>,
        #[structopt(
            long = "timeout",
            help = "require subscribers to consume values before timeout (seconds)"
        )]
        timeout: Option<u64>,
    },
    #[structopt(name = "subscriber", about = "subscribe to values")]
    Subscriber {
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
    #[structopt(name = "table", about = "table descriptor for path")]
    Table {
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
        #[structopt(
            short = "b",
            long = "bind",
            help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
        )]
        bind: BindCfg,
        #[structopt(long = "spn", help = "krb5 use <spn>")]
        spn: Option<String>,
        #[structopt(
            long = "delay",
            help = "time in ms to wait between batches",
            default_value = "100"
        )]
        delay: u64,
        #[structopt(name = "rows", default_value = "100")]
        rows: usize,
        #[structopt(name = "cols", default_value = "10")]
        cols: usize,
    },
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber,
}

fn auth(
    anon: bool,
    cfg: &config::Config,
    upn: Option<String>,
    spn: Option<String>,
) -> Auth {
    if anon {
        Auth::Anonymous
    } else {
        match cfg.auth {
            config::Auth::Anonymous => Auth::Anonymous,
            config::Auth::Krb5(_) => Auth::Krb5 { upn, spn },
        }
    }
}

#[cfg(feature = "browser")]
fn start_browser(auth: Auth, cfg: config::Config, path: Option<Path>) {
    browser::run(cfg, auth, path.unwrap_or(Path::from("/")))
}

#[cfg(not(feature = "browser"))]
fn start_browser(_auth: Auth, _cfg: config::Config, _path: Option<Path>) {
    panic!("the browser is not available")
}

fn main() {
    env_logger::init();
    let opt = Opt::from_args();
    let cfg = match opt.config {
        None => config::Config::load_default().unwrap(),
        Some(path) => config::Config::load(path).unwrap(),
    };
    match opt.cmd {
        Sub::Browser { path } => {
            let auth = auth(opt.anon, &cfg, opt.upn, None);
            start_browser(auth, cfg, path)
        },
        Sub::ResolverServer { foreground, delay_reads, id, permissions } => {
            if !cfg!(unix) {
                todo!("the resolver server is not yet ported to this platform")
            }
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
                Some(p) => config::PMap::load(&p).unwrap(),
            };
            resolver_server::run(cfg, permissions, !foreground, delay_reads, id)
        }
        Sub::Resolver { cmd } => {
            let auth = auth(opt.anon, &cfg, opt.upn, None);
            resolver::run(cfg, cmd, auth)
        }
        Sub::Publisher { bind, spn, timeout } => {
            let auth = auth(opt.anon, &cfg, opt.upn, spn);
            publisher::run(cfg, bind, timeout, auth)
        }
        Sub::Subscriber { paths } => {
            let auth = auth(opt.anon, &cfg, opt.upn, None);
            subscriber::run(cfg, paths, auth)
        }
        Sub::Stress { cmd } => match cmd {
            Stress::Subscriber => {
                let auth = auth(opt.anon, &cfg, opt.upn, None);
                stress_subscriber::run(cfg, auth)
            }
            Stress::Publisher { bind, spn, delay, rows, cols } => {
                let auth = auth(opt.anon, &cfg, opt.upn, spn);
                stress_publisher::run(cfg, bind, delay, rows, cols, auth)
            }
        },
    }
}
