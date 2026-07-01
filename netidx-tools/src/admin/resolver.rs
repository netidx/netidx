use anyhow::{Context, Result};
use clap::Subcommand;
use netidx_admin::{paths, resolver::ResolverConfig};
use std::path::PathBuf;

use super::editor;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// load and pretty-print the resolver config
    Show {
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// open the resolver-server config in $VISUAL / $EDITOR, validate on save
    Edit {
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Show { file } => show(file),
        Cmd::Edit { file } => edit(file),
    }
}

fn show(file: Option<PathBuf>) -> Result<()> {
    let source = match file {
        Some(p) => p,
        None => paths::discover_resolver_config()?,
    };
    let cfg = ResolverConfig::load(&source)?;
    // Validate with the source path so relative `include_permissions`
    // resolve against the config dir, not the process cwd.
    cfg.validate_for_path(&source)?;
    println!("{}", serde_json::to_string_pretty(cfg.as_file())?);
    Ok(())
}

fn edit(file: Option<PathBuf>) -> Result<()> {
    let target = match file {
        Some(p) => p,
        None => paths::user_resolver_config()?,
    };
    let initial = if target.exists() {
        let cfg = ResolverConfig::load(&target)?;
        serde_json::to_string_pretty(cfg.as_file())?
    } else {
        eprintln!("# {target:?} does not exist — starting with an empty template");
        starter_template()
    };
    let validated: ResolverConfig = editor::edit_with_validation(&initial, |s| {
        let file: netidx::resolver_server::config::file::Config =
            serde_json::from_str(s).context("parsing edited JSON")?;
        let wrapped = ResolverConfig::from(file);
        wrapped.validate_for_path(&target).context("config failed validation")?;
        Ok(wrapped)
    })?;
    validated.save(&target)?;
    println!("saved {}", target.display());
    Ok(())
}

fn starter_template() -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "parent": null,
        "children": [],
        "member_servers": [],
        "perms": {}
    }))
    .expect("starter template is well-formed JSON")
}
