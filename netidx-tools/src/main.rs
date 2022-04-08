#![recursion_limit = "2048"]
#[cfg(unix)]
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate anyhow;
#[cfg(unix)]
#[macro_use]
extern crate serde_derive;
use log::warn;
use netidx::{config, path::Path, publisher::BindCfg, resolver::DesiredAuth};
use std::net::SocketAddr;
use structopt::StructOpt;

mod publisher;
mod resolver;
mod stress_publisher;
mod stress_subscriber;
mod subscriber;

#[cfg(unix)]
mod container;
#[cfg(unix)]
mod recorder;
#[cfg(unix)]
mod resolver_server;

#[cfg(not(unix))]
mod resolver_server {
    use netidx::config;

    pub(crate) fn run(
        _config: config::server::Config,
        _permissions: config::PMap,
        _daemonize: bool,
        _delay_reads: bool,
        _id: usize,
    ) {
        todo!("the resolver server is not yet ported to this platform")
    }
}

#[cfg(not(unix))]
mod recorder {
    use netidx::{
        config::server::Config, path::Path, publisher::BindCfg, resolver::DesiredAuth,
    };
    pub(crate) fn run(
        _config: Config,
        _foreground: bool,
        _bind: Option<BindCfg>,
        _publish_base: Option<Path>,
        _auth: Auth,
        _image_frequency: usize,
        _poll_interval: u64,
        _flush_frequency: usize,
        _flush_interval: u64,
        _shards: usize,
        _max_sessions: usize,
        _max_sessions_per_client: usize,
        _archive: String,
        _spec: Vec<String>,
    ) {
        todo!("the recorder is not yet ported to this platform")
    }
}

#[cfg(not(unix))]
mod container {
    use netidx::{config::client::Config, resolver::DesiredAuth};
    pub(crate) fn run(_config: Config, _auth: Auth, _ccfg: super::ContainerConfig) {
        todo!("the container is not yet ported to this platform")
    }
}

#[derive(StructOpt, Debug)]
struct ContainerConfig {
    #[structopt(short = "c", long = "config", help = "path to the client config")]
    config: Option<String>,
    #[structopt(short = "a", long = "auth", help = "auth mechanism")]
    auth: DesiredAuth,
    #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
    upn: Option<String>,
    #[structopt(long = "spn", help = "kerberos spn, only if auth = krb5")]
    spn: Option<String>,
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
    )]
    bind: BindCfg,
    #[structopt(
        long = "timeout",
        help = "require subscribers to consume values before timeout (seconds)"
    )]
    timeout: Option<u64>,
    #[structopt(long = "api-path", help = "the netidx path of the container api")]
    api_path: Path,
    #[structopt(long = "db", help = "the db file")]
    db: String,
    #[structopt(long = "compress", help = "use zstd compression")]
    compress: bool,
    #[structopt(long = "compress-level", help = "zstd compression level")]
    compress_level: Option<u32>,
    #[structopt(long = "cache-size", help = "db page cache size in bytes")]
    cache_size: Option<u64>,
    #[structopt(long = "sparse", help = "don't even advertise the contents of the db")]
    sparse: bool,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "netidx")]
enum Sub {
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
        #[structopt(short = "c", long = "config", help = "path to the server config")]
        config: String,
    },
    #[structopt(name = "resolver", about = "query the resolver")]
    Resolver {
        #[structopt(short = "c", long = "config", help = "path to the client config")]
        config: Option<String>,
        #[structopt(short = "a", long = "auth", help = "auth mechanism")]
        auth: DesiredAuth,
        #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
        upn: Option<String>,
        #[structopt(subcommand)]
        cmd: ResolverCmd,
    },
    #[structopt(name = "publisher", about = "publish data")]
    Publisher {
        #[structopt(short = "c", long = "config", help = "path to the client config")]
        config: Option<String>,
        #[structopt(short = "a", long = "auth", help = "auth mechanism")]
        auth: DesiredAuth,
        #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
        upn: Option<String>,
        #[structopt(long = "spn", help = "kerberos spn, only if auth = krb5")]
        spn: Option<String>,
        #[structopt(
            short = "b",
            long = "bind",
            help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
        )]
        bind: BindCfg,
        #[structopt(
            long = "timeout",
            help = "require subscribers to consume values before timeout (seconds)"
        )]
        timeout: Option<u64>,
    },
    #[structopt(name = "subscriber", about = "subscribe to values")]
    Subscriber {
        #[structopt(short = "c", long = "config", help = "path to the client config")]
        config: Option<String>,
        #[structopt(short = "a", long = "auth", help = "auth mechanism")]
        auth: DesiredAuth,
        #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
        upn: Option<String>,
        #[structopt(
            short = "o",
            long = "oneshot",
            help = "unsubscribe after printing one value for each subscription"
        )]
        oneshot: bool,
        #[structopt(
            short = "n",
            long = "no-stdin",
            help = "don't read commands from stdin"
        )]
        no_stdin: bool,
        #[structopt(
            short = "t",
            long = "subscribe-timeout",
            help = "cancel subscription unless it succeeds within timeout"
        )]
        subscribe_timeout: Option<u64>,
        #[structopt(name = "paths")]
        paths: Vec<String>,
    },
    #[structopt(name = "container", about = "a hierarchical database in netidx")]
    Container(ContainerConfig),
    #[structopt(name = "record", about = "record and republish archives")]
    Record {
        #[structopt(short = "c", long = "config", help = "path to the client config")]
        config: Option<String>,
        #[structopt(short = "a", long = "auth", help = "auth mechanism")]
        auth: DesiredAuth,
        #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
        upn: Option<String>,
        #[structopt(long = "spn", help = "kerberos spn, only if auth = krb5")]
        spn: Option<String>,
        #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
        foreground: bool,
        #[structopt(
            short = "b",
            long = "bind",
            help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
        )]
        bind: Option<BindCfg>,
        #[structopt(
            long = "publish-base",
            help = "base path for republishing the archive"
        )]
        publish_base: Option<Path>,
        #[structopt(
            long = "image-frequency",
            help = "How often to write a full image, 0 for never (67108864)",
            default_value = "67108864"
        )]
        image_frequency: usize,
        #[structopt(
            long = "poll-interval",
            help = "How often to poll the resolver, 0 for never (5)",
            default_value = "5"
        )]
        poll_interval: u64,
        #[structopt(
            long = "flush-frequency",
            help = "How often to flush changes in pages, 0 only on exit (65534 pages)",
            default_value = "65534"
        )]
        flush_frequency: usize,
        #[structopt(
            long = "flush-interval",
            help = "How often to flush changes (seconds), 0 disable (30)",
            default_value = "30"
        )]
        flush_interval: u64,
        #[structopt(
            long = "shards",
            help = "how many other recorder shards to expect",
            default_value = "0"
        )]
        shards: usize,
        #[structopt(
            long = "max-sessions",
            help = "how many total client sesions to allow",
            default_value = "256"
        )]
        max_sessions: usize,
        #[structopt(
            long = "max-sessions-per-client",
            help = "how many sesions to allow each client",
            default_value = "64"
        )]
        max_sessions_per_client: usize,
        #[structopt(long = "archive", help = "path to the archive file")]
        archive: String,
        #[structopt(long = "spec", help = "glob pattern to archive, can be repeated")]
        spec: Vec<String>,
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
        #[structopt(
            long = "no-structure",
            short = "n",
            help = "don't list structural items, only published paths"
        )]
        no_structure: bool,
        #[structopt(
            long = "watch",
            short = "w",
            help = "poll the resolver for new paths matching the specified pattern"
        )]
        watch: bool,
        #[structopt(name = "pattern")]
        path: Option<String>,
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
        #[structopt(short = "c", long = "config", help = "path to the client config")]
        config: Option<String>,
        #[structopt(short = "a", long = "auth", help = "auth mechanism")]
        auth: DesiredAuth,
        #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
        upn: Option<String>,
        #[structopt(long = "spn", help = "kerberos spn, only if auth = krb5")]
        spn: Option<String>,
        #[structopt(
            short = "b",
            long = "bind",
            help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
        )]
        bind: BindCfg,
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
    Subscriber {
        #[structopt(short = "c", long = "config", help = "path to the client config")]
        config: Option<String>,
        #[structopt(short = "a", long = "auth", help = "auth mechanism")]
        auth: DesiredAuth,
        #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
        upn: Option<String>,
    },
}

fn auth(desired: DesiredAuth, upn: Option<String>, spn: Option<String>) -> Result<Auth> {
    match desired {
        DesiredAuth::Anonymous | DesiredAuth::Local => match (upn, spn) {
            (None, None) => Ok(desired),
            (Some(_), _) | (_, Some(_)) => {
                bail!("upn/spn may not be specified for local or anonymous auth")
            }
        },
        DesiredAuth::Krb5 { .. } => Ok(DesiredAuth::Krb5 { upn, spn }),
    }
}

fn main() {
    env_logger::init();
    let opt = Opt::from_args();
    let cfg = match opt.config {
        None => config::Config::load_default().unwrap(),
        Some(path) => config::Config::load(path).unwrap(),
    };
    match opt.cmd {
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
        Sub::Subscriber { no_stdin, oneshot, subscribe_timeout, paths } => {
            let auth = auth(opt.anon, &cfg, opt.upn, None);
            subscriber::run(cfg, no_stdin, oneshot, subscribe_timeout, paths, auth)
        }
        Sub::Container(ccfg) => {
            let auth = auth(opt.anon, &cfg, opt.upn, ccfg.spn.clone());
            container::run(cfg, auth, ccfg)
        }
        Sub::Record {
            foreground,
            bind,
            spn,
            publish_base,
            image_frequency,
            poll_interval,
            flush_frequency,
            flush_interval,
            shards,
            max_sessions,
            max_sessions_per_client,
            archive,
            spec,
        } => {
            let auth = auth(opt.anon, &cfg, opt.upn, spn);
            recorder::run(
                cfg,
                foreground,
                bind,
                publish_base,
                auth,
                image_frequency,
                poll_interval,
                flush_frequency,
                flush_interval,
                shards,
                max_sessions,
                max_sessions_per_client,
                archive,
                spec,
            )
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
