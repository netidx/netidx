//! `netidx admin host …` — the commands that act on **this machine**:
//! its activation units, its admin-server daemon, its TLS identity, its
//! OS service.
//!
//! That is the whole grouping rule. Everything else under `netidx admin`
//! acts on the admin domain — `perms`, `id-map`, `ca`, and the role
//! lifecycle commands all reach the admin server, which authorizes the
//! operation and propagates it. These act here, and nowhere else.
//!
//! Some of them still talk to the network — `tls join` asks a CA to sign,
//! `admin-agent` pulls the admin domain map — but what they *change* is
//! this host. That is the distinction, not whether a socket is opened.
//!
//! Nothing here edits an installation's config files in place. The admin
//! server owns the client config, the resolver config, the perms file, and
//! the id-map: it writes them and propagates to every host that holds a
//! copy. A second, local writer is how two hosts come to disagree, so
//! those editors are gone — see `admin perms` and `admin id-map` for the
//! managed path, `resolver status` / `resolver update` for the config.
//!
//! The role templates (`workstation` / `resolver` / `publisher`) and `ca`
//! wire these pieces together; reach for `host` when you want to operate
//! on one directly. This is a pure grouping layer — the implementations
//! live in the sibling modules it routes to.

use anyhow::Result;
use clap::Subcommand;

#[cfg(any(unix, windows))]
use super::activation;
#[cfg(unix)]
use super::server;
use super::{agent, service, tls};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// edit netidx-activation units
    #[cfg(any(unix, windows))]
    Activation {
        #[command(subcommand)]
        cmd: activation::Cmd,
    },
    /// run the admin server (admin domain discovery + setup daemon)
    #[cfg(unix)]
    Server {
        #[command(subcommand)]
        cmd: server::Cmd,
    },
    /// keep this host current with its admin domain (and renew its
    /// certificates, if it has any)
    AdminAgent {
        #[command(subcommand)]
        cmd: agent::Cmd,
    },
    /// this host's TLS identity (request a CSR, join a CA)
    Tls {
        #[command(subcommand)]
        cmd: tls::Cmd,
    },
    /// install netidx as an OS service
    Service {
        #[command(subcommand)]
        cmd: service::Cmd,
    },
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        #[cfg(any(unix, windows))]
        Cmd::Activation { cmd } => activation::run(cmd),
        #[cfg(unix)]
        Cmd::Server { cmd } => server::run(cmd),
        Cmd::AdminAgent { cmd } => agent::run(cmd),
        Cmd::Tls { cmd } => tls::run(cmd),
        Cmd::Service { cmd } => service::run(cmd),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(subcommand)]
        cmd: crate::admin::Params,
    }

    /// The `component` alias is load-bearing, not politeness. An install
    /// writes its admin-server and admin-agent activation units with the
    /// subcommand path baked into argv, and nothing rewrites those units when
    /// the binary is upgraded. Drop the alias and every host installed before
    /// the rename stops starting its daemon on the next upgrade — silently,
    /// because the supervisor just sees a process that exits non-zero.
    #[test]
    fn the_old_component_name_still_parses() {
        for name in ["host", "component"] {
            let cli = TestCli::try_parse_from([
                "netidx",
                name,
                "server",
                "run",
                "-c",
                "/etc/netidx/admin-server.json",
                "-f",
            ]);
            assert!(cli.is_ok(), "`netidx admin {name} server run` must parse");
        }
    }

    /// The exact argv the installed units carry, both spellings.
    #[test]
    fn the_installed_unit_argv_parses() {
        for name in ["host", "component"] {
            let cli =
                TestCli::try_parse_from(["netidx", name, "admin-agent", "run", "-f"]);
            assert!(cli.is_ok(), "`netidx admin {name} admin-agent run -f` must parse");
        }
    }
}
