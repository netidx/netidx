use anyhow::{Context, Result};
use clap::Subcommand;
use netidx_admin::{paths, perms};
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// list perm entries
    List {
        /// perms file (default: ~/.config/netidx/perms.json)
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// filter to a specific path
        #[arg(short, long)]
        path: Option<String>,
    },
    /// set <bits> for <entity> at <path>
    Set {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Netidx path the entry applies to.
        path: String,
        /// User or group name the entry applies to.
        entity: String,
        /// Permission bits (e.g. `swlpd`).
        bits: String,
    },
    /// remove <entity>'s entry at <path>
    Remove {
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// Netidx path the entry applies to.
        path: String,
        /// User or group name the entry applies to.
        entity: String,
    },
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::List { file, path } => list(resolve(file)?, path),
        Cmd::Set { file, path, entity, bits } => {
            set(resolve(file)?, path, entity, bits)
        }
        Cmd::Remove { file, path, entity } => {
            remove(resolve(file)?, path, entity)
        }
    }
}

fn resolve(file: Option<PathBuf>) -> Result<PathBuf> {
    match file {
        Some(p) => Ok(p),
        None => paths::user_perms_file().context("resolving default perms path"),
    }
}

fn load_or_empty(
    file: &std::path::Path,
) -> Result<netidx::resolver_server::config::PMap> {
    if file.exists() { perms::load_perms(file) } else { Ok(perms::empty()) }
}

fn list(file: PathBuf, path_filter: Option<String>) -> Result<()> {
    if !file.exists() {
        println!("# {file:?} does not exist yet — no entries");
        return Ok(());
    }
    let pmap = perms::load_perms(&file)?;
    let mut rows: Vec<(String, String, String)> = match &path_filter {
        Some(p) => perms::iter_path(&pmap, p)
            .map(|(e, b)| (p.clone(), e.to_string(), b.to_string()))
            .collect(),
        None => perms::iter(&pmap)
            .map(|(p, e, b)| (p.to_string(), e.to_string(), b.to_string()))
            .collect(),
    };
    rows.sort();
    if rows.is_empty() {
        println!("# no entries match");
    } else {
        let pw = rows.iter().map(|(p, _, _)| p.len()).max().unwrap_or(0);
        let ew = rows.iter().map(|(_, e, _)| e.len()).max().unwrap_or(0);
        for (p, e, b) in rows {
            println!("{p:pw$}  {e:ew$}  {b}");
        }
    }
    Ok(())
}

fn set(file: PathBuf, path: String, entity: String, bits: String) -> Result<()> {
    netidx::resolver_server::auth::Permissions::try_from(bits.as_str())
        .with_context(|| format!("validating bits {bits:?}"))?;
    let mut pmap = load_or_empty(&file)?;
    perms::add_entry(&mut pmap, &path, &entity, &bits)?;
    perms::save_perms(&file, &pmap)?;
    println!("set {path}  {entity}  {bits}");
    Ok(())
}

fn remove(file: PathBuf, path: String, entity: String) -> Result<()> {
    if !file.exists() {
        bail!("perms file {file:?} does not exist");
    }
    let mut pmap = perms::load_perms(&file)?;
    perms::remove_entry(&mut pmap, &path, &entity);
    perms::save_perms(&file, &pmap)?;
    println!("removed {path}  {entity}");
    Ok(())
}
