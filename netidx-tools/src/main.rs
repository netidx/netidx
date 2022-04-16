#![recursion_limit = "2048"]
#[cfg(unix)]
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate anyhow;
#[cfg(unix)]
#[macro_use]
extern crate serde_derive;
use netidx::{config, resolver::DesiredAuth};
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

#[derive(StructOpt, Debug)]
struct ClientParams {
    #[structopt(short = "c", long = "config", help = "path to the client config")]
    config: Option<String>,
    #[structopt(short = "a", long = "auth", help = "auth mechanism", default_value = "krb5")]
    auth: DesiredAuth,
    #[structopt(long = "upn", help = "kerberos upn, only if auth = krb5")]
    upn: Option<String>,
    #[structopt(long = "spn", help = "kerberos spn, only if auth = krb5")]
    spn: Option<String>,
}

impl ClientParams {
    fn load(self) -> (config::client::Config, DesiredAuth) {
        let cfg = match self.config {
            None => config::client::Config::load_default()
                .expect("failed to load default netidx config"),
            Some(path) => {
                config::client::Config::load(path).expect("failed to load netidx config")
            }
        };
        let auth = match self.auth {
            DesiredAuth::Anonymous | DesiredAuth::Local => match (self.upn, self.spn) {
                (None, None) => self.auth,
                (Some(_), _) | (_, Some(_)) => {
                    panic!("upn/spn may not be specified for local or anonymous auth")
                }
            },
            DesiredAuth::Krb5 { .. } => {
                DesiredAuth::Krb5 { upn: self.upn, spn: self.spn }
            }
        };
        (cfg, auth)
    }
}

#[derive(StructOpt, Debug)]
enum Stress {
    #[structopt(name = "publisher", about = "run a stress test publisher")]
    Publisher(stress_publisher::Params),
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "netidx")]
enum Opt {
    #[cfg(unix)]
    #[structopt(name = "resolver-server", about = "run a resolver")]
    ResolverServer(resolver_server::Params),
    #[structopt(name = "resolver", about = "query the resolver")]
    Resolver {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(subcommand)]
        cmd: resolver::ResolverCmd,
    },
    #[structopt(name = "publisher", about = "publish data")]
    Publisher {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: publisher::Params,
    },
    #[structopt(name = "subscriber", about = "subscribe to values")]
    Subscriber {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: subscriber::Params,
    },
    #[cfg(unix)]
    #[structopt(name = "container", about = "a hierarchical database in netidx")]
    Container {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: container::Params,
    },
    #[cfg(unix)]
    #[structopt(name = "record", about = "record and republish archives")]
    Record {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: recorder::Params,
    },
    #[structopt(name = "stress", about = "stress test")]
    Stress {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(subcommand)]
        cmd: Stress,
    },
}

fn main() {
    env_logger::init();
    match Opt::from_args() {
        #[cfg(unix)]
        Opt::ResolverServer(p) => resolver_server::run(p),
        Opt::Resolver { common, cmd } => {
            let (cfg, auth) = common.load();
            resolver::run(cfg, auth, cmd)
        }
        Opt::Publisher { common, params } => {
            let (cfg, auth) = common.load();
            publisher::run(cfg, auth, params)
        }
        Opt::Subscriber { common, params } => {
            let (cfg, auth) = common.load();
            subscriber::run(cfg, auth, params)
        }
        #[cfg(unix)]
        Opt::Container { common, params } => {
            let (cfg, auth) = common.load();
            container::run(cfg, auth, params)
        }
        #[cfg(unix)]
        Opt::Record { common, params } => {
            let (cfg, auth) = common.load();
            recorder::run(cfg, auth, params)
        }
        Opt::Stress { common, cmd } => {
            let (cfg, auth) = common.load();
            match cmd {
                Stress::Subscriber => stress_subscriber::run(cfg, auth),
                Stress::Publisher(params) => stress_publisher::run(cfg, auth, params),
            }
        }
    }
}
