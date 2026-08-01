use anyhow::{Context, Result};
use clap::Subcommand;
use netidx_admin::{client::ClientConfig, config_lock::ConfigDirLock, paths};
use std::path::PathBuf;

use super::editor;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// load and pretty-print the client config
    Show {
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// open the client config in $VISUAL / $EDITOR, validate on save
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
    let cfg = match file {
        Some(p) => ClientConfig::load(p)?,
        None => ClientConfig::load(paths::discover_client_config()?)?,
    };
    cfg.validate()?;
    println!("{}", serde_json::to_string_pretty(cfg.as_file())?);
    Ok(())
}

fn edit(file: Option<PathBuf>) -> Result<()> {
    let target = match file {
        Some(p) => p,
        None => paths::user_client_config()?,
    };
    // The same guard `component resolver edit` takes over its own file: this
    // writes into the managed config directory, so it must not race a
    // concurrent `netidx admin` command any more than that one may.
    let config_lock = ConfigDirLock::acquire_for_file(&target)?;
    let target = config_lock.require_contained(target)?;
    let initial = if target.exists() {
        let cfg = ClientConfig::load(&target)?;
        serde_json::to_string_pretty(cfg.as_file())?
    } else {
        eprintln!("# {target:?} does not exist — starting with an empty template");
        starter_template()
    };
    let validated: ClientConfig = editor::edit_with_validation(&initial, |s| {
        let file: netidx::config::file::Config =
            serde_json::from_str(s).context("parsing edited JSON")?;
        let wrapped = ClientConfig::from(file);
        wrapped.validate().context("config failed validation")?;
        Ok(wrapped)
    })?;
    validated.save(&target)?;
    println!("saved {}", target.display());
    Ok(())
}

fn starter_template() -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "base": "/",
        "addrs": [],
        "default_auth": "Krb5"
    }))
    .expect("starter template is well-formed JSON")
}
