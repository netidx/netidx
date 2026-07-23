//! `netidx admin perms show|edit --at <path>` — remote permissions
//! administration, a thin CLI over [`netidx_admin_client::ops::perms`]. The
//! library reaches an admin server, glyph-confirms its CA, routes by the
//! authoritative controller, and performs an authenticated read or write.
//! `edit` runs the `$EDITOR` loop and local validation here — a frontend
//! concern — between the library's authenticated read and write.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin_client::{ops::perms as perms_ops, perms};
use netidx_admin_proto::PeerResult;

use super::{answer_cli::RemoteAuthFlags, editor};

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// show the permissions of the cluster mounted at <path> (admin)
    Show(Flags),
    /// edit the permissions of the cluster mounted at <path> (admin)
    Edit(Flags),
}

#[derive(Args, Debug)]
pub(crate) struct Flags {
    /// The hierarchy path whose cluster's perms to act on (e.g. `/eu`, or `/`
    /// for the root cluster).
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

fn show(f: Flags) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let perms_json = runtime()?.block_on(perms_ops::show_perms(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        &f.at,
    ))?;
    println!("{}", pretty(&perms_json)?);
    Ok(())
}

fn edit(f: Flags) -> Result<()> {
    let mut ans = f.auth.answerer()?;
    let server = f.auth.server_addr()?;
    let rt = runtime()?;
    // Seed the editor with the cluster's current perms, then hand the edited,
    // locally-validated result to the library's authenticated write.
    let (session, current) = rt.block_on(perms_ops::open_perms_session(
        &mut ans,
        server,
        f.auth.ca_dir.clone(),
        f.auth.admin.clone(),
        None,
        &f.at,
    ))?;
    let edited = editor::edit_with_validation(&pretty(&current)?, validate)?;
    let peers =
        rt.block_on(perms_ops::edit_perms_with_session(&session, &f.at, &edited))?;
    report_peers(&peers, &f.at);
    Ok(())
}

/// Validate edited perms JSON in the editor loop: it must parse as a PMap and
/// every entry's bits must be valid. Returns the normalized JSON to send. The
/// CA re-validates the whole resolver config server-side; this just gives a
/// fast local re-edit on an obvious mistake. Shared with the TUI perms panel.
pub(crate) fn validate(s: &str) -> Result<String> {
    let pmap: perms::PMap = serde_json::from_str(s).context("not valid perms JSON")?;
    for (path, entity, bits) in perms::iter(&pmap) {
        netidx::resolver_server::auth::Permissions::try_from(bits.as_str())
            .with_context(|| {
                format!("invalid permission bits {bits:?} for {entity} at {path}")
            })?;
    }
    serde_json::to_string(&pmap).context("serializing perms")
}

/// Pretty-print perms JSON for display / editor seeding. Shared with the TUI
/// perms panel.
pub(crate) fn pretty(perms_json: &str) -> Result<String> {
    let v: serde_json::Value =
        serde_json::from_str(perms_json).context("parsing perms JSON")?;
    serde_json::to_string_pretty(&v).context("formatting perms JSON")
}

fn report_peers(peers: &[PeerResult], at: &str) {
    let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
    if failed.is_empty() {
        println!("ok — perms at {at:?} updated on {} cluster member(s).", peers.len());
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
        "  the cluster is INCONSISTENT. The edit is idempotent — re-run \
         `perms edit --at {at}` once the member(s) are back to converge."
    );
}
