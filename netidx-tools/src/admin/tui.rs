//! The interactive `netidx admin` TUI: the Graphix program in
//! `graphix-package-netidx-admin`, run by an embedded shell. Its flags
//! sit on the bare command; a subcommand runs instead of it.

use crate::gx;
use anyhow::Result;
use arcstr::{ArcStr, literal};
use clap::Args;
use graphix_shell::ShellBuilder;
use std::io::{IsTerminal, stdin, stdout};

#[derive(Args, Debug, Default)]
pub(crate) struct Params {
    /// open on the Admin Domain tab, connecting to this admin server
    #[arg(long)]
    server: Option<ArcStr>,
}

#[tokio::main]
pub(crate) async fn run(p: Params) -> Result<()> {
    if !(stdin().is_terminal() && stdout().is_terminal()) {
        bail!(
            "`netidx admin` needs an interactive terminal; use a subcommand for scripts"
        )
    }
    let shell = ShellBuilder::default()
        .add_packages(vec![Box::new(graphix_package_netidx_admin::P)])
        .program_args(p.server.into_iter().collect());
    gx::run_tui(shell, literal!(include_str!("tui.gx"))).await
}
