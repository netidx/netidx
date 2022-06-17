use anyhow::{anyhow, Result};
use daemonize::Daemonize;
use futures::{
    channel::mpsc::{self, Receiver},
    prelude::*,
    stream::SelectAll,
};
use fxhash::FxBuildHasher;
use log::{error, warn};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    protocol::resolver::TargetAuth,
    publisher::{BindCfg, DesiredAuth, Id, Publisher, Typ, Val, Value, WriteRequest},
    utils,
};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::From,
    fs::{self, read_to_string},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader},
    runtime::Runtime,
    signal, task,
};

#[derive(StructOpt, Debug)]
pub(super) struct Params {
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. local, 192.168.0.0/16",
        default_value = "local"
    )]
    bind: BindCfg,
    #[structopt(
        short = "p",
        long = "publishers",
        help = "path to the publishers definition file"
    )]
    publisher: PathBuf,
    #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
    foreground: bool,
    #[structopt(long = "pid-file", help = "write the pid to file")]
    pid_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
enum Restart {
    No,
    Yes,
    RateLimited(f64),
}

#[derive(Debug, Deserialize)]
struct PublisherCfg {
    exe: String,
    args: Vec<String>,
    uid: Option<u32>,
    gid: Option<u32>,
    restart: Restart,
}

impl PublisherCfg {
    fn validate(&self) -> Result<()> {
        let md = fs::metadata(&self.exe)?;
        if !md.is_file() {
            bail!("exe must be a file")
        }
        if md.permissions().mode() & 0b0000_0000_0100_1001 == 0 {
            bail!("exe must be executable")
        }
        Ok(())
    }
}

async fn run_server(
    cfg: Config,
    auth: DesiredAuth,
    params: Params,
    spec: HashMap<Path, PublisherCfg>,
) -> Result<()> {
    let publisher = Publisher::new(cfg, auth, params.bind).await?;
    let defaults = spec
        .keys()
        .map(|path| publisher.publish_default(path.clone()))
        .collect::<Result<SelectAll<_>>>()?;
    
    Ok(())
}

pub(super) fn run(cfg: Config, auth: DesiredAuth, params: Params) {
    let spec = read_to_string(&params.publisher).expect("failed to read publishers spec");
    let spec: Vec<(Path, PublisherCfg)> =
        serde_json::from_str(&spec).expect("invalid publishers spec");
    let spec = {
        let mut tbl = HashMap::new();
        for (path, cfg) in spec.into_iter() {
            if tbl.insert(path, cfg).is_some() {
                panic!("publisher paths must be unique")
            }
        }
        tbl
    };
    for cfg in spec.values() {
        cfg.validate().expect("invalid publisher specification")
    }
    if !params.foreground {
        let mut d = Daemonize::new();
        if let Some(pid_file) = params.pid_file.as_ref() {
            d = d.pid_file(pid_file);
        }
        d.start().expect("failed to daemonize")
    }
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async move {
        if let Err(e) = run_server(cfg, auth, params, spec).await {
            error!("server task shutdown: {}", e)
        }
    });
}
