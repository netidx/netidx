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
use netidx_admin::{config_lock::ConfigDirLock, id_map};
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
    match cmd {
        Cmd::Init { file, default_uid, default_gid } => {
            let (lock, file) = resolve_locked(file)?;
            init(&lock, file, default_uid, default_gid)
        }
        Cmd::Show { file } => show(resolve(file)?),
        Cmd::Edit { file } => {
            let (lock, file) = resolve_locked(file)?;
            edit(&lock, file)
        }
        Cmd::List { file } => list(resolve(file)?),
        Cmd::AddGroup { file, name, gid } => {
            let (lock, file) = resolve_locked(file)?;
            add_group(&lock, file, name, gid)
        }
        Cmd::RemoveGroup { file, name } => {
            let (lock, file) = resolve_locked(file)?;
            remove_group(&lock, file, name)
        }
        Cmd::AddUser { file, name, uid, primary_group, groups } => {
            let (lock, file) = resolve_locked(file)?;
            add_user(&lock, file, name, uid, primary_group, groups)
        }
        Cmd::RemoveUser { file, name } => {
            let (lock, file) = resolve_locked(file)?;
            remove_user(&lock, file, name)
        }
        Cmd::AddMember { file, name, group } => {
            let (lock, file) = resolve_locked(file)?;
            add_member(&lock, file, name, group)
        }
        Cmd::RemoveMember { file, name, group } => {
            let (lock, file) = resolve_locked(file)?;
            remove_member(&lock, file, name, group)
        }
    }
}

fn resolve_locked(file: Option<PathBuf>) -> Result<(ConfigDirLock, PathBuf)> {
    let file = resolve(file)?;
    let lock = ConfigDirLock::acquire_for_file(&file)?;
    Ok((lock, file))
}

fn resolve(file: Option<PathBuf>) -> Result<PathBuf> {
    match file {
        Some(p) => Ok(p),
        None => id_map::user_id_map_path().context("resolving default id-map path"),
    }
}

fn init(
    config_lock: &ConfigDirLock,
    file: PathBuf,
    default_uid: u32,
    default_gid: u32,
) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    if file.exists() {
        bail!("{} already exists; refusing to overwrite", file.display());
    }
    let mut m = id_map::empty();
    id_map::set_defaults(&mut m, default_uid, default_gid);
    id_map::save(&file, &m)?;
    println!("initialized id-map at {}", file.display());
    Ok(())
}

fn show(file: PathBuf) -> Result<()> {
    let m = id_map::load(&file)?;
    let s = serde_json::to_string_pretty(&m)?;
    println!("{s}");
    Ok(())
}

fn edit(config_lock: &ConfigDirLock, file: PathBuf) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    let initial = if file.exists() {
        let m = id_map::load(&file)?;
        serde_json::to_string_pretty(&m)?
    } else {
        eprintln!("# {file:?} does not exist — starting with an empty template");
        serde_json::to_string_pretty(&id_map::empty())?
    };
    // `parse_bytes` does both serde + structural validation, so a
    // botched edit (typo, dangling group reference, ...) is rejected
    // here and the operator gets a re-edit prompt — the on-disk file
    // is untouched until validation passes.
    let validated = editor::edit_with_validation(&initial, |s| {
        id_map::parse_bytes(s.as_bytes()).context("parsing edited id-map JSON")
    })?;
    id_map::save(&file, &validated)?;
    println!("saved {}", file.display());
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

fn load_or_empty(file: &std::path::Path) -> Result<id_map::IdMap> {
    if file.exists() { id_map::load(file) } else { Ok(id_map::empty()) }
}

/// Next free id above the conventional Linux user floor of 1000 and
/// any existing id. The resolver doesn't read these values for the
/// id-map daemon's TLS/Kerberos path; an operator who needs a
/// specific value passes the explicit `--uid` / `--gid` flag.
fn next_id(existing: impl Iterator<Item = u32>) -> u32 {
    let highest = existing.max().unwrap_or(0);
    highest.max(999).saturating_add(1)
}

fn next_user_uid(m: &id_map::IdMap) -> u32 {
    next_id(m.identities.values().map(|i| i.uid))
}

fn next_group_gid(m: &id_map::IdMap) -> u32 {
    next_id(m.groups.values().map(|g| g.gid))
}

fn add_group(
    config_lock: &ConfigDirLock,
    file: PathBuf,
    name: String,
    gid: Option<u32>,
) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    let mut m = load_or_empty(&file)?;
    let gid = gid
        .or_else(|| m.groups.get(name.as_str()).map(|g| g.gid))
        .unwrap_or_else(|| next_group_gid(&m));
    let prev = id_map::upsert_group(&mut m, &name, gid);
    id_map::save(&file, &m)?;
    match prev {
        Some(old) => println!("group {name}: gid {old} → {gid}"),
        None => println!("added group {name} (gid={gid})"),
    }
    Ok(())
}

fn remove_group(config_lock: &ConfigDirLock, file: PathBuf, name: String) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    let mut m = id_map::load(&file)?;
    id_map::remove_group(&mut m, &name)?;
    id_map::save(&file, &m)?;
    println!("removed group {name}");
    Ok(())
}

fn add_user(
    config_lock: &ConfigDirLock,
    file: PathBuf,
    name: String,
    uid: Option<u32>,
    primary_group: String,
    groups: Vec<String>,
) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    let mut m = load_or_empty(&file)?;
    let uid = uid
        .or_else(|| m.identities.get(name.as_str()).map(|i| i.uid))
        .unwrap_or_else(|| next_user_uid(&m));
    let group_refs: Vec<&str> = groups.iter().map(|s| s.as_str()).collect();
    let prev = id_map::upsert_identity(&mut m, &name, uid, &primary_group, &group_refs)?;
    id_map::save(&file, &m)?;
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

fn remove_user(config_lock: &ConfigDirLock, file: PathBuf, name: String) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    let mut m = id_map::load(&file)?;
    match id_map::remove_identity(&mut m, &name) {
        Some(_) => {
            id_map::save(&file, &m)?;
            println!("removed identity {name}");
        }
        None => println!("no such identity {name}"),
    }
    Ok(())
}

fn add_member(
    config_lock: &ConfigDirLock,
    file: PathBuf,
    name: String,
    group: String,
) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    let mut m = id_map::load(&file)?;
    id_map::add_group_member(&mut m, &name, &group)?;
    id_map::save(&file, &m)?;
    println!("{name} ∈ {group}");
    Ok(())
}

fn remove_member(
    config_lock: &ConfigDirLock,
    file: PathBuf,
    name: String,
    group: String,
) -> Result<()> {
    let file = config_lock.require_contained(file)?;
    let mut m = id_map::load(&file)?;
    id_map::remove_group_member(&mut m, &name, &group)?;
    id_map::save(&file, &m)?;
    println!("{name} ∉ {group}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_to_end_editing_flow() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("id-map.json");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        init(&lock, f.clone(), 65534, 65534).unwrap();
        // Groups first, then identities — the validate step enforces
        // referential integrity on every save.
        add_group(&lock, f.clone(), "users".into(), Some(100)).unwrap();
        add_group(&lock, f.clone(), "wheel".into(), Some(10)).unwrap();
        add_user(
            &lock,
            f.clone(),
            "alice.example.com".into(),
            Some(1000),
            "users".into(),
            vec!["wheel".into()],
        )
        .unwrap();
        // Reload via the engine to confirm what we wrote.
        let m = id_map::load(&f).unwrap();
        assert_eq!(m.lookup_by_name("alice.example.com").unwrap().uid, 1000);
        assert_eq!(m.groups.len(), 2);

        // Add-member is idempotent; remove-member won't drop the primary.
        add_member(&lock, f.clone(), "alice.example.com".into(), "wheel".into()).unwrap();
        assert!(
            remove_member(&lock, f.clone(), "alice.example.com".into(), "users".into(),)
                .is_err(),
            "removing primary via remove-member must error",
        );

        remove_user(&lock, f.clone(), "alice.example.com".into()).unwrap();
        let m = id_map::load(&f).unwrap();
        assert!(m.identities.is_empty());

        // remove-group succeeds now that nothing references them.
        remove_group(&lock, f.clone(), "wheel".into()).unwrap();
        let m = id_map::load(&f).unwrap();
        assert_eq!(m.groups.len(), 1);
    }

    #[test]
    fn init_refuses_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("id-map.json");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        init(&lock, f.clone(), 65534, 65534).unwrap();
        let err = init(&lock, f, 65534, 65534).unwrap_err();
        assert!(format!("{err:#}").contains("already exists"));
    }

    #[test]
    fn add_user_rejects_unknown_primary() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("id-map.json");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        init(&lock, f.clone(), 65534, 65534).unwrap();
        let err = add_user(&lock, f, "alice".into(), Some(1000), "ghost".into(), vec![])
            .unwrap_err();
        assert!(format!("{err:#}").contains("primary_group"), "got {err:#}",);
    }
}
