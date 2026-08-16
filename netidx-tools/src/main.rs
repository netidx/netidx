#![recursion_limit = "2048"]
mod browser;
mod log_errors;
mod publisher;
mod record_client;
mod resolver;
mod stress_channel_publisher;
mod stress_channel_subscriber;
mod stress_publisher;
mod stress_subscriber;
mod subscriber;
mod wsproxy;

#[cfg(any(unix, windows))]
mod activation;
mod admin;
mod container;
#[cfg(unix)]
mod id_map;
#[cfg(unix)]
mod recorder;
mod resolver_server;

#[macro_use]
extern crate anyhow;
#[cfg(unix)]
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use netidx_tools_core::ClientParams;

#[derive(Subcommand, Debug)]
enum Stress {
    /// run a stress test publisher
    Publisher {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: stress_publisher::Params,
    },
    /// run a stress test subscriber
    Subscriber {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: stress_subscriber::Params,
    },
    /// run a stress channel publisher
    ChannelPublisher {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: stress_channel_publisher::Params,
    },
    /// run a stress channel subscriber
    ChannelSubscriber {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: stress_channel_subscriber::Params,
    },
}

#[cfg(unix)]
#[derive(Subcommand, Debug)]
enum IdMapCmd {
    /// run the id-mapper daemon
    Serve(id_map::Params),
}

#[derive(Parser, Debug)]
#[command(name = "netidx", version)]
enum Opt {
    /// run a resolver
    ResolverServer(resolver_server::Params),
    /// query the resolver
    Resolver {
        #[command(flatten)]
        common: ClientParams,
        #[command(subcommand)]
        cmd: resolver::ResolverCmd,
    },
    /// publish data
    Publisher {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: publisher::Params,
    },
    /// subscribe to values
    Subscriber {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: subscriber::Params,
    },
    /// a hierarchical database in netidx
    Container {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: container::Params,
    },
    /// record and republish archives
    #[cfg(unix)]
    Record {
        /// recorder config file
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// print an example config file
        #[arg(short, long)]
        example: bool,
    },
    /// control the recorder
    RecordClient {
        #[command(subcommand)]
        cmd: record_client::Cmd,
    },
    /// manage netidx processes
    #[cfg(any(unix, windows))]
    Activation {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        params: activation::Params,
    },
    /// id-mapper daemon (TLS-friendly group lookups)
    #[cfg(unix)]
    IdMap {
        #[command(subcommand)]
        cmd: IdMapCmd,
    },
    /// administrative control plane (run with no subcommand for the interactive TUI)
    Admin {
        #[command(subcommand)]
        params: Option<admin::Params>,
    },
    /// stress test
    Stress {
        #[command(subcommand)]
        cmd: Stress,
    },
    /// websocket proxy
    #[command(name = "wsproxy")]
    WsProxy {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        publisher: publisher::Params,
        #[command(flatten)]
        proxy: netidx_wsproxy::config::Config,
    },
    /// tui browser
    Browser {
        #[command(flatten)]
        common: ClientParams,
        #[command(flatten)]
        publisher: publisher::Params,
    },
}

fn main() -> Result<()> {
    netidx::config::Config::maybe_run_machine_local_resolver()?;
    match Opt::parse() {
        Opt::ResolverServer(p) => resolver_server::run(p),
        #[cfg(any(unix, windows))]
        Opt::Activation { common, params } => {
            let (cfg, auth) = common.load();
            activation::run(cfg, auth, params)
        }
        #[cfg(unix)]
        Opt::IdMap { cmd } => match cmd {
            IdMapCmd::Serve(p) => id_map::run(p),
        },
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
        Opt::Container { common, params } => {
            let (cfg, auth) = common.load();
            container::run(cfg, auth, params)
        }
        Opt::Admin { params } => admin::run(params),
        Opt::RecordClient { cmd } => record_client::run(cmd),
        #[cfg(unix)]
        Opt::Record { config, example } => recorder::run(config, example),
        Opt::Stress { cmd } => match cmd {
            Stress::Subscriber { common, params } => {
                let (cfg, auth) = common.load();
                stress_subscriber::run(cfg, auth, params)
            }
            Stress::Publisher { common, params } => {
                let (cfg, auth) = common.load();
                stress_publisher::run(cfg, auth, params)
            }
            Stress::ChannelPublisher { common, params } => {
                let (cfg, auth) = common.load();
                stress_channel_publisher::run(cfg, auth, params)
            }
            Stress::ChannelSubscriber { common, params } => {
                let (cfg, auth) = common.load();
                stress_channel_subscriber::run(cfg, auth, params)
            }
        },
        Opt::WsProxy { common, publisher, proxy } => {
            let (cfg, auth) = common.load();
            wsproxy::run(cfg, auth, publisher, proxy)
        }
        Opt::Browser { common, publisher } => {
            let (cfg, auth) = common.load();
            browser::run(cfg, auth, publisher)
        }
    }
}
