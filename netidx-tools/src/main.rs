#![recursion_limit = "2048"]
mod publisher;
mod record_client;
mod resolver;
mod shell;
mod stress_channel_publisher;
mod stress_channel_subscriber;
mod stress_publisher;
mod stress_subscriber;
mod subscriber;
mod wsproxy;

#[cfg(unix)]
mod activation;
mod container;
#[cfg(unix)]
mod recorder;
mod resolver_server;

#[macro_use]
extern crate anyhow;
#[cfg(unix)]
use std::path::PathBuf;

use anyhow::Result;
use netidx_tools_core::ClientParams;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
enum Stress {
    #[structopt(name = "publisher", about = "run a stress test publisher")]
    Publisher {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: stress_publisher::Params,
    },
    #[structopt(name = "subscriber", about = "run a stress test subscriber")]
    Subscriber {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: stress_subscriber::Params,
    },
    #[structopt(name = "channel_publisher", about = "run a stress channel publisher")]
    ChannelPublisher {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: stress_channel_publisher::Params,
    },
    #[structopt(name = "channel_subscriber", about = "run a stress channel subscriber")]
    ChannelSubscriber {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: stress_channel_subscriber::Params,
    },
}

#[derive(StructOpt, Debug)]
#[structopt(name = "netidx")]
enum Opt {
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
    #[structopt(name = "shell", about = "bscript repl and script executor")]
    Shell {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: shell::Params,
    },
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
        config: Option<PathBuf>,
        #[structopt(
            short = "e",
            long = "example",
            help = "print an example config file"
        )]
        example: bool,
    },
    #[structopt(name = "record-client", about = "control the recorder")]
    RecordClient {
        #[structopt(subcommand)]
        cmd: record_client::Cmd,
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
    },
}

#[tokio::main]
async fn tokio_main() -> Result<()> {
    match Opt::from_args() {
        #[cfg(unix)]
        Opt::Activation { .. } => {
            panic!("activation server cannot be initialized from async");
        }
        Opt::ResolverServer(_) => {
            panic!("resolver server cannot be initialized from async")
        }
        Opt::Resolver { common, cmd } => {
            let (cfg, auth) = common.load();
            resolver::run(cfg, auth, cmd).await
        }
        Opt::Publisher { common, params } => {
            let (cfg, auth) = common.load();
            publisher::run(cfg, auth, params).await
        }
        Opt::Subscriber { common, params } => {
            let (cfg, auth) = common.load();
            subscriber::run(cfg, auth, params).await
        }
        Opt::Shell { common, params } => {
            let (cfg, auth) = common.load();
            shell::run(cfg, auth, params).await
        }
        Opt::Container { common, params } => {
            let (cfg, auth) = common.load();
            container::run(cfg, auth, params).await
        }
        Opt::RecordClient { cmd } => record_client::run(cmd).await,
        #[cfg(unix)]
        Opt::Record { config, example } => recorder::run(config, example).await,
        Opt::Stress { cmd } => match cmd {
            Stress::Subscriber { common, params } => {
                let (cfg, auth) = common.load();
                stress_subscriber::run(cfg, auth, params).await
            }
            Stress::Publisher { common, params } => {
                let (cfg, auth) = common.load();
                stress_publisher::run(cfg, auth, params).await
            }
            Stress::ChannelPublisher { common, params } => {
                let (cfg, auth) = common.load();
                stress_channel_publisher::run(cfg, auth, params).await
            }
            Stress::ChannelSubscriber { common, params } => {
                let (cfg, auth) = common.load();
                stress_channel_subscriber::run(cfg, auth, params).await
            }
        },
        Opt::WsProxy { common, publisher, proxy } => {
            let (cfg, auth) = common.load();
            wsproxy::run(cfg, auth, publisher, proxy).await
        }
    }
}

// Daemonization and tokio don't play well together. The best practice is to daemonize
// as early as possible, before the async runtime is initialized. This means we can't
// use the tokio_main macro on main, so we short-circuit ResolverServer handling here.
fn main() -> Result<()> {
    env_logger::init();
    let opt = Opt::from_args();
    match opt {
        Opt::ResolverServer(p) => resolver_server::run(p),
        #[cfg(unix)]
        Opt::Activation { common, params } => {
            let (cfg, auth) = common.load();
            activation::run(cfg, auth, params)
        }
        _ => tokio_main(),
    }
}
