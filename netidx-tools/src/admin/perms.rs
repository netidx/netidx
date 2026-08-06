//! `netidx admin perms show|edit|set|remove --at <path>` — permissions
//! administration, a thin CLI over [`netidx_admin::ops::perms`]. The
//! library reaches an admin server, glyph-confirms its CA, routes by the
//! authoritative CA, and performs an authenticated read or write.
//! `edit` runs the `$EDITOR` loop and local validation here — a frontend
//! concern — between the library's authenticated read and write; `set` and
//! `remove` are the scriptable single-entry forms, where the library does
//! the whole read-modify-write.
//!
//! Every form goes through the admin server, which authorizes the `--at`
//! path, preflights the result against the resolver config, and propagates
//! to every member of the resolver cluster. There is deliberately no
//! command that edits a perms file in place: that is how two members of one
//! resolver cluster come to disagree.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin::{ops::perms as perms_ops, perms};

use super::{
    answer_cli::RemoteAuthFlags,
    editor,
    propagation::{Recorded, report_recorded},
};

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// show the permissions of the resolver cluster mounted at <path> (admin)
    Show(Flags),
    /// edit the permissions of the resolver cluster mounted at <path> (admin)
    Edit(Flags),
    /// set <bits> for <entity> at <path> in the resolver cluster mounted at
    /// --at, then propagate to every member
    Set {
        /// Netidx path the entry applies to.
        path: String,
        /// User or group name the entry applies to.
        entity: String,
        /// Permission bits (e.g. `swlpd`).
        bits: String,
        #[command(flatten)]
        flags: Flags,
    },
    /// remove <entity>'s entry at <path> in the resolver cluster mounted at
    /// --at, then propagate to every member
    Remove {
        /// Netidx path the entry applies to.
        path: String,
        /// User or group name the entry applies to.
        entity: String,
        #[command(flatten)]
        flags: Flags,
    },
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
        Cmd::Set { path, entity, bits, flags } => set(flags, path, entity, bits),
        Cmd::Remove { path, entity, flags } => remove(flags, path, entity),
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
    let perms = rt.block_on(perms_ops::show_perms(&target, &f.at))?;
    println!("{}", perms::render(&perms)?);
    Ok(())
}

fn edit(f: Flags) -> Result<()> {
    let rt = runtime()?;
    let target = target(&rt, &f)?;
    // Render the resolver cluster's current perms into the editor and parse
    // back what the operator leaves. This is one of the two places perms are
    // text; past it the document travels as itself.
    let current = rt.block_on(perms_ops::show_perms(&target, &f.at))?;
    let edited = editor::edit_with_validation(&perms::render(&current)?, perms::parse)?;
    let version = rt.block_on(perms_ops::edit_perms(&target, &f.at, &edited))?;
    report_recorded(
        &netidx_admin::ops::RecordedEdit { version, changed: true },
        Recorded::Perms { at: &f.at },
    );
    Ok(())
}

fn set(f: Flags, path: String, entity: String, bits: String) -> Result<()> {
    // Refuse a malformed argument before resolving a target, which may prompt
    // for a password and contact the CA. `set_entry` checks again — a library
    // op can't trust its caller — but only after the round trip.
    perms::validate_bits(&bits)?;
    let rt = runtime()?;
    let target = target(&rt, &f)?;
    let edit =
        rt.block_on(perms_ops::set_entry(&target, &f.at, &path, &entity, &bits))?;
    if edit.changed {
        println!("set {path}  {entity}  {bits}");
    }
    report_recorded(&edit, Recorded::Perms { at: &f.at });
    Ok(())
}

fn remove(f: Flags, path: String, entity: String) -> Result<()> {
    let rt = runtime()?;
    let target = target(&rt, &f)?;
    let edit = rt.block_on(perms_ops::remove_entry(&target, &f.at, &path, &entity))?;
    if edit.changed {
        println!("removed {path}  {entity}");
    }
    report_recorded(&edit, Recorded::Perms { at: &f.at });
    Ok(())
}
