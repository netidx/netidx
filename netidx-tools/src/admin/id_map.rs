//! `netidx admin component id-map …` — edit the id-map JSON file used by the
//! id-mapper daemon. Mirrors the perms CLI in shape: each subcommand
//! loads, mutates, and atomically saves the JSON file.
//!
//! A separate `init` subcommand creates an empty starter file (useful
//! for `netidx admin resolver install --auth tls`, which wires
//! up the daemon's activation unit but leaves the actual map empty
//! for the operator to fill in).
//!
//! `show` prints the loaded map for inspection; `edit` opens the
//! JSON in `$VISUAL`/`$EDITOR`, validates on save, and only then
//! atomically replaces the file (the daemon's file-watcher picks up
//! the reload).

use anyhow::{Context, Result};
use clap::Subcommand;
use netidx_admin::id_map::{self, IdMapSession};
use std::path::PathBuf;

use super::editor;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// create an empty id-map file if one doesn't exist
    Init {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Default uid returned for unknown queries.
        #[arg(long, default_value = "65534")]
        default_uid: u32,
        /// Default gid returned for unknown queries.
        #[arg(long, default_value = "65534")]
        default_gid: u32,
    },
    /// pretty-print the id-map JSON
    Show {
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// open the id-map JSON in $VISUAL / $EDITOR, validate on save
    Edit {
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// list identities and groups in a table
    List {
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// add a group (or update an existing one)
    AddGroup {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Group name.
        name: String,
        /// Numeric gid. Auto-allocated (≥1000, above any existing gid)
        /// when omitted; an existing group keeps its current gid
        /// unless this flag is given. The resolver only reads group
        /// names, never numeric gids — the value is decorative.
        gid: Option<u32>,
    },
    /// remove a group (fails if in use)
    RemoveGroup {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Group name.
        name: String,
    },
    /// add an identity (or update an existing one)
    AddUser {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Netidx name (typically the TLS SubjectAltName DNS entry).
        name: String,
        /// Primary group name (must already exist).
        primary_group: String,
        /// Numeric uid. Auto-allocated (≥1000, above any existing uid)
        /// when omitted; an existing identity keeps its current uid
        /// unless this flag is given. The resolver only reads uids in
        /// the local-auth (Unix-socket peer-cred) path, and that path
        /// is normally served by `/bin/id` rather than the id-map
        /// daemon — so for any realistic id-map-daemon deployment the
        /// uid is decorative and the auto-allocated value is fine.
        #[arg(long)]
        uid: Option<u32>,
        /// Secondary group memberships. Repeatable.
        #[arg(short, long = "group", num_args = 1)]
        groups: Vec<String>,
    },
    /// remove an identity
    RemoveUser {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Netidx name.
        name: String,
    },
    /// add an identity to a secondary group
    AddMember {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Netidx name.
        name: String,
        /// Group name.
        group: String,
    },
    /// remove an identity from a secondary group
    RemoveMember {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Netidx name.
        name: String,
        /// Group name.
        group: String,
    },
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    rt.block_on(async move {
        match cmd {
            Cmd::Show { file } => show(resolve(file)?),
            Cmd::List { file } => list(resolve(file)?),
            Cmd::Init { file, default_uid, default_gid } => {
                init(open(file).await?, default_uid, default_gid).await
            }
            Cmd::Edit { file } => edit(open(file).await?).await,
            Cmd::AddGroup { file, name, gid } => {
                add_group(open(file).await?, name, gid).await
            }
            Cmd::RemoveGroup { file, name } => {
                remove_group(open(file).await?, name).await
            }
            Cmd::AddUser { file, name, uid, primary_group, groups } => {
                add_user(open(file).await?, name, uid, primary_group, groups).await
            }
            Cmd::RemoveUser { file, name } => remove_user(open(file).await?, name).await,
            Cmd::AddMember { file, name, group } => {
                add_member(open(file).await?, name, group).await
            }
            Cmd::RemoveMember { file, name, group } => {
                remove_member(open(file).await?, name, group).await
            }
        }
    })
}

async fn open(file: Option<PathBuf>) -> Result<IdMapSession> {
    IdMapSession::open(file, None).await
}

fn resolve(file: Option<PathBuf>) -> Result<PathBuf> {
    match file {
        Some(p) => Ok(p),
        None => id_map::user_id_map_path().context("resolving default id-map path"),
    }
}

async fn init(mut m: IdMapSession, default_uid: u32, default_gid: u32) -> Result<()> {
    if m.existed() {
        bail!("{} already exists; refusing to overwrite", m.path().display());
    }
    let path = m.path().to_path_buf();
    id_map::set_defaults(m.map_mut(), default_uid, default_gid);
    m.save().await?;
    println!("initialized id-map at {}", path.display());
    Ok(())
}

fn show(file: PathBuf) -> Result<()> {
    let m = id_map::load(&file)?;
    let s = serde_json::to_string_pretty(&m)?;
    println!("{s}");
    Ok(())
}

async fn edit(mut m: IdMapSession) -> Result<()> {
    if !m.existed() {
        eprintln!("# {:?} does not exist — starting with an empty template", m.path());
    }
    let initial = serde_json::to_string_pretty(m.map())?;
    // `parse_bytes` does both serde + structural validation, so a botched edit
    // (typo, dangling group reference, ...) is rejected here and the operator
    // gets a re-edit prompt — the on-disk file is untouched until it passes.
    *m.map_mut() = editor::edit_with_validation(&initial, |s| {
        id_map::parse_bytes(s.as_bytes()).context("parsing edited id-map JSON")
    })?;
    let path = m.path().to_path_buf();
    m.save().await?;
    println!("saved {}", path.display());
    Ok(())
}

fn list(file: PathBuf) -> Result<()> {
    let m = id_map::load(&file)?;
    println!("# defaults: uid={} gid={}", m.default_uid, m.default_gid);
    if m.groups.is_empty() {
        println!("# (no groups)");
    } else {
        println!("# groups:");
        let w = m.groups.keys().map(|k| k.len()).max().unwrap_or(0);
        for (name, g) in &m.groups {
            println!("  {:<w$}  gid={}", name.as_str(), g.gid, w = w);
        }
    }
    if m.identities.is_empty() {
        println!("# (no identities)");
    } else {
        println!("# identities:");
        let w = m.identities.keys().map(|k| k.len()).max().unwrap_or(0);
        for (name, ident) in &m.identities {
            let extra: Vec<&str> = ident.groups.iter().map(|g| g.as_str()).collect();
            println!(
                "  {:<w$}  uid={}  primary={}  groups=[{}]",
                name.as_str(),
                ident.uid,
                ident.primary_group.as_str(),
                extra.join(","),
                w = w,
            );
        }
    }
    Ok(())
}

async fn add_group(mut m: IdMapSession, name: String, gid: Option<u32>) -> Result<()> {
    let gid = gid
        .or_else(|| m.map().groups.get(name.as_str()).map(|g| g.gid))
        .unwrap_or_else(|| id_map::next_gid(m.map()));
    let prev = id_map::upsert_group(m.map_mut(), &name, gid);
    m.save().await?;
    match prev {
        Some(old) => println!("group {name}: gid {old} → {gid}"),
        None => println!("added group {name} (gid={gid})"),
    }
    Ok(())
}

async fn remove_group(mut m: IdMapSession, name: String) -> Result<()> {
    id_map::remove_group(m.map_mut(), &name)?;
    m.save().await?;
    println!("removed group {name}");
    Ok(())
}

async fn add_user(
    mut m: IdMapSession,
    name: String,
    uid: Option<u32>,
    primary_group: String,
    groups: Vec<String>,
) -> Result<()> {
    let uid = uid
        .or_else(|| m.map().identities.get(name.as_str()).map(|i| i.uid))
        .unwrap_or_else(|| id_map::next_uid(m.map()));
    let group_refs: Vec<&str> = groups.iter().map(|s| s.as_str()).collect();
    let prev =
        id_map::upsert_identity(m.map_mut(), &name, uid, &primary_group, &group_refs)?;
    m.save().await?;
    match prev {
        Some(old) => println!(
            "updated {name} (was uid={} primary={} groups={:?})",
            old.uid,
            old.primary_group.as_str(),
            old.groups.iter().map(|g| g.as_str()).collect::<Vec<_>>(),
        ),
        None => println!("added identity {name} (uid={uid})"),
    }
    Ok(())
}

async fn remove_user(mut m: IdMapSession, name: String) -> Result<()> {
    match id_map::remove_identity(m.map_mut(), &name) {
        Some(_) => {
            m.save().await?;
            println!("removed identity {name}");
        }
        None => println!("no such identity {name}"),
    }
    Ok(())
}

async fn add_member(mut m: IdMapSession, name: String, group: String) -> Result<()> {
    id_map::add_group_member(m.map_mut(), &name, &group)?;
    m.save().await?;
    println!("{name} ∈ {group}");
    Ok(())
}

async fn remove_member(mut m: IdMapSession, name: String, group: String) -> Result<()> {
    id_map::remove_group_member(m.map_mut(), &name, &group)?;
    m.save().await?;
    println!("{name} ∉ {group}");
    Ok(())
}
