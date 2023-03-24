#![recursion_limit = "2048"]
mod publisher;
mod resolver;
mod stress_channel_publisher;
mod stress_channel_subscriber;
mod stress_publisher;
mod stress_subscriber;
mod subscriber;
mod wsproxy;

#[cfg(unix)]
mod activation;
#[cfg(unix)]
mod container;
#[cfg(unix)]
mod recorder;
#[cfg(unix)]
mod resolver_server;

#[macro_use]
extern crate anyhow;
#[cfg(unix)]
use std::path::PathBuf;

use netidx_tools_core::ClientParams;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
enum Stress {
    #[structopt(name = "publisher", about = "run a stress test publisher")]
    Publisher(stress_publisher::Params),
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber(stress_subscriber::Params),
    #[structopt(name = "channel_publisher", about = "run a stress channel publisher")]
    ChannelPublisher(stress_channel_publisher::Params),
    #[structopt(name = "channel_subscriber", about = "run a stress channel subscriber")]
    ChannelSubscriber(stress_channel_subscriber::Params),
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
        #[structopt(short = "c", long = "config", help = "recorder config file")]
        config: PathBuf,
        #[structopt(short = "e", long = "example", help = "print an example config file")]
        example: bool,
    },
    #[cfg(unix)]
    #[structopt(name = "activation", about = "manage netidx processes")]
    Activation {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: activation::Params,
    },
    #[structopt(name = "stress", about = "stress test")]
    Stress {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(subcommand)]
        cmd: Stress,
    },
    #[structopt(name = "wsproxy", about = "websocket proxy")]
    WsProxy {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        publisher: publisher::Params,
        #[structopt(flatten)]
        proxy: netidx_wsproxy::config::Config,
    }
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
        Opt::Record { config, example } => {
            recorder::run(config, example)
        }
        #[cfg(unix)]
        Opt::Activation { common, params } => {
            let (cfg, auth) = common.load();
            activation::run(cfg, auth, params)
        }
        Opt::Stress { common, cmd } => {
            let (cfg, auth) = common.load();
            match cmd {
                Stress::Subscriber(params) => stress_subscriber::run(cfg, auth, params),
                Stress::Publisher(params) => stress_publisher::run(cfg, auth, params),
                Stress::ChannelPublisher(params) => {
                    stress_channel_publisher::run(cfg, auth, params)
                }
                Stress::ChannelSubscriber(params) => {
                    stress_channel_subscriber::run(cfg, auth, params)
                }
            }
        }
        Opt::WsProxy { common, publisher, proxy } => {
            let (cfg, auth) = common.load();
            wsproxy::run(cfg, auth, publisher, proxy)
        }
    }
}
