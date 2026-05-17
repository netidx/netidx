use anyhow::{Context, Result};
use netidx_conf::{client::ClientConfig, paths};
use std::path::PathBuf;
use structopt::StructOpt;

use super::editor;

#[derive(StructOpt, Debug)]
pub(crate) enum Cmd {
    #[structopt(name = "show", about = "load and pretty-print the client config")]
    Show {
        #[structopt(long = "file", short = "f")]
        file: Option<PathBuf>,
    },
    #[structopt(
        name = "edit",
        about = "open the client config in $VISUAL / $EDITOR, validate on save"
    )]
    Edit {
        #[structopt(long = "file", short = "f")]
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
