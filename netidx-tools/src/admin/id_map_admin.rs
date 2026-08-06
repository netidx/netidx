//! `netidx admin id-map …` — id-map administration over the admin plane, a
//! thin CLI over [`netidx_admin::ops::id_map`].
//!
//! Every command goes through the admin server, which authorizes the caller
//! and records the operation in the admin domain's model; every id-map host
//! converges on it at its next register. There is deliberately no command that
//! edits an id-map file in place: the CA owns that file, so a second local
//! writer is how two hosts come to authorize differently.
//!
//! `show` reports the map of the host this command reaches. Every id-map host
//! holds the identical document, so which one answered does not matter — but
//! one may be behind, which `netidx admin drift` is for.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin::ops::id_map as id_map_ops;

use super::{
    answer_cli::RemoteAuthFlags,
    propagation::{Recorded, report_recorded},
};

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// print the id-map of the admin server this command reaches
    Show(Flags),
    /// create a group
    AddGroup {
        /// Group name.
        name: String,
        #[command(flatten)]
        flags: Flags,
    },
    /// remove a group (refused while an identity still holds it)
    RemoveGroup {
        /// Group name.
        name: String,
        #[command(flatten)]
        flags: Flags,
    },
    /// register an identity, creating any groups it names
    AddUser {
        /// Netidx name — the certificate's DNS SubjectAltName.
        name: String,
        /// Primary group.
        primary_group: String,
        /// Additional groups.
        #[arg(long = "group")]
        groups: Vec<String>,
        #[command(flatten)]
        flags: Flags,
    },
    /// remove an identity
    RemoveUser {
        /// Netidx name.
        name: String,
        #[command(flatten)]
        flags: Flags,
    },
    /// add an identity to a group
    AddMember {
        /// Netidx name.
        name: String,
        /// Group to add it to.
        group: String,
        #[command(flatten)]
        flags: Flags,
    },
    /// remove an identity from a group (not its primary group)
    RemoveMember {
        /// Netidx name.
        name: String,
        /// Group to remove it from.
        group: String,
        #[command(flatten)]
        flags: Flags,
    },
}

#[derive(Args, Debug)]
pub(crate) struct Flags {
    #[command(flatten)]
    auth: RemoteAuthFlags,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Show(f) => show(f),
        Cmd::AddGroup { name, flags } => {
            edit(flags, |t, rt| rt.block_on(id_map_ops::add_group(t, &name)))
        }
        Cmd::RemoveGroup { name, flags } => {
            edit(flags, |t, rt| rt.block_on(id_map_ops::remove_group(t, &name)))
        }
        Cmd::AddUser { name, primary_group, groups, flags } => edit(flags, |t, rt| {
            rt.block_on(id_map_ops::add_user(t, &name, &primary_group, &groups))
        }),
        Cmd::RemoveUser { name, flags } => {
            edit(flags, |t, rt| rt.block_on(id_map_ops::remove_user(t, &name)))
        }
        Cmd::AddMember { name, group, flags } => {
            edit(flags, |t, rt| rt.block_on(id_map_ops::add_member(t, &name, &group)))
        }
        Cmd::RemoveMember { name, group, flags } => {
            edit(flags, |t, rt| rt.block_on(id_map_ops::remove_member(t, &name, &group)))
        }
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
    let map = rt.block_on(id_map_ops::show_id_map(&target))?;
    println!("{}", serde_json::to_string_pretty(&map)?);
    Ok(())
}

/// Run one mutating op and report what it recorded. `changed: false` is not a
/// failure — the operator asked for something already true, which is what
/// makes re-running a command safe.
fn edit(
    f: Flags,
    op: impl FnOnce(
        &netidx_admin::ops::AdminTarget,
        &tokio::runtime::Runtime,
    ) -> Result<netidx_admin::ops::RecordedEdit>,
) -> Result<()> {
    let rt = runtime()?;
    let target = target(&rt, &f)?;
    report_recorded(&op(&target, &rt)?, Recorded::IdMap);
    Ok(())
}
