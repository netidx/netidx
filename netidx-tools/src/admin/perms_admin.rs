//! `netidx admin perms show|edit --at <path>` — remote permissions
//! administration, a thin CLI over [`netidx_admin::ops::perms`]. The
//! library reaches an admin server, glyph-confirms its CA, routes by the
//! authoritative CA, and performs an authenticated read or write.
//! `edit` runs the `$EDITOR` loop and local validation here — a frontend
//! concern — between the library's authenticated read and write.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin::{ops::perms as perms_ops, perms};
use netidx_admin_proto::PeerResult;

use super::{answer_cli::RemoteAuthFlags, editor};

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// show the permissions of the resolver cluster mounted at <path> (admin)
    Show(Flags),
    /// edit the permissions of the resolver cluster mounted at <path> (admin)
    Edit(Flags),
}

#[derive(Args, Debug)]
pub(crate) struct Flags {
    /// The hierarchy path whose resolver cluster's perms to act on (e.g. `/eu`, or `/`
    /// for the root resolver cluster).
    #[arg(long = "at")]
    at: String,
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Show(f) => show(f),
        Cmd::Edit(f) => edit(f),
    }
}

/// Resolve which CA this command acts against: a pinned remote session, or
/// this host's own daemon over its control socket when no `--server` is given.
fn target(
    rt: &tokio::runtime::Runtime,
    f: &Flags,
) -> Result<netidx_admin::ops::AdminTarget> {
    let mut ans = f.auth.answerer()?;
    rt.block_on(netidx_admin::ops::resolve_admin_target(
        &mut ans,
        f.auth.server_addr()?,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
    ))
}

fn show(f: Flags) -> Result<()> {
    let rt = runtime()?;
    let target = target(&rt, &f)?;
    let perms_json = rt.block_on(perms_ops::show_perms(&target, &f.at))?;
    println!("{}", perms::pretty(&perms_json)?);
    Ok(())
}

fn edit(f: Flags) -> Result<()> {
    let rt = runtime()?;
    let target = target(&rt, &f)?;
    // Seed the editor with the resolver cluster's current perms, then hand the
    // edited, locally-validated result to the library's authenticated write.
    let current = rt.block_on(perms_ops::show_perms(&target, &f.at))?;
    let edited =
        editor::edit_with_validation(&perms::pretty(&current)?, perms::normalize)?;
    let peers = rt.block_on(perms_ops::edit_perms(&target, &f.at, &edited))?;
    report_peers(&peers, &f.at);
    Ok(())
}

fn report_peers(peers: &[PeerResult], at: &str) {
    let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
    if failed.is_empty() {
        println!(
            "ok — perms at {at:?} updated on {} resolver cluster member(s).",
            peers.len()
        );
        println!("  restart the resolver server(s) to load the new perms.");
        return;
    }
    println!(
        "perms at {at:?}: {} of {} member(s) could NOT be updated:",
        failed.len(),
        peers.len()
    );
    for p in &failed {
        println!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("?"));
    }
    println!(
        "  the resolver cluster is INCONSISTENT. The edit is idempotent — re-run \
         `perms edit --at {at}` once the member(s) are back to converge."
    );
}
