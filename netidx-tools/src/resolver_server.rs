use anyhow::{Context, Result};
#[cfg(unix)]
use daemonize::Daemonize;
#[cfg(not(unix))]
use futures::future;
#[cfg(unix)]
use log::{info, warn};
#[cfg(unix)]
use netidx::resolver_server::config;
use netidx::resolver_server::{
    config::{file, Config},
    Server,
};
use std::path::PathBuf;
use structopt::StructOpt;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

#[derive(StructOpt, Debug)]
pub(crate) struct Params {
    #[structopt(short = "c", long = "config", help = "path to the server config")]
    config: String,
    #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
    #[allow(dead_code)]
    foreground: bool,
    #[structopt(
        long = "delay-reads",
        help = "don't allow read clients until 1 writer ttl has passed"
    )]
    delay_reads: bool,
    #[structopt(
        long = "id",
        help = "index of the member server to run",
        default_value = "0"
    )]
    id: usize,
}

#[cfg(unix)]
#[tokio::main]
async fn tokio_run(
    config: Config,
    baseline: file::Config,
    config_path: PathBuf,
    params: Params,
) -> Result<()> {
    let server = Server::new(config, params.delay_reads, params.id)
        .await
        .context("starting server")?;
    run_signal_loop(server, config_path, params.id, baseline).await
}

#[cfg(not(unix))]
#[tokio::main]
async fn tokio_run(config: Config, params: Params) -> Result<()> {
    let server = Server::new(config, params.delay_reads, params.id)
        .await
        .context("starting server")?;
    // No SIGHUP on Windows yet. The server stays up until the process
    // is killed.
    let _server = server;
    let _ = params;
    future::pending::<Result<()>>().await
}

#[cfg(unix)]
async fn run_signal_loop(
    server: Server,
    config_path: PathBuf,
    id: usize,
    baseline: file::Config,
) -> Result<()> {
    // Diff baseline for structural-field warnings. Frozen at startup
    // and **never updated**: the fields we compare (parent, children,
    // member_servers) are not applied live — the running server still
    // has the startup values. Diffing against startup means an
    // operator who edits a field and then reverts it gets the warn
    // once and silence after; an operator who edits without
    // reverting keeps getting the warn on every SIGHUP, which is
    // honest: "your edit still hasn't taken effect; restart to apply."
    let mut sighup =
        signal(SignalKind::hangup()).context("registering SIGHUP handler")?;
    let _ = id;
    loop {
        let _ = sighup.recv().await;
        match handle_sighup(&server, &config_path, &baseline).await {
            Ok(()) => info!("perms reloaded successfully"),
            Err(e) => warn!("SIGHUP perms reload failed: {e:#}"),
        }
    }
}

#[cfg(unix)]
async fn handle_sighup(
    server: &Server,
    config_path: &std::path::Path,
    baseline: &file::Config,
) -> Result<()> {
    info!("SIGHUP received, re-reading {:?}", config_path);
    let new_file = load_file_config(config_path)?;
    // Merge include_permissions + inline perms WITHOUT going through
    // `Config::from_file`: the full validator opens TLS cert files,
    // re-validates addrs / parent / children / member_servers — none
    // of which are applied live on SIGHUP, and one of which (TLS
    // file I/O) could spuriously fail mid-rotation. The merge helper
    // only touches `include_permissions` + `perms` and surfaces the
    // structural-diff warning separately.
    let new_perms = config::merge_perms_only(&new_file)
        .context("merging perms (include_permissions + inline)")?;
    warn_structural_changes(baseline, &new_file);
    server.reload_perms(&new_perms).await.context("swapping live PMap")?;
    Ok(())
}

#[cfg(unix)]
fn load_file_config(path: &std::path::Path) -> Result<file::Config> {
    // Goes through `Config::load_file` (not raw serde_json) so that
    // relative `include_permissions` paths are resolved against the
    // config file's parent directory — matching the startup load
    // path exactly.
    Config::load_file(path).with_context(|| format!("reading {:?}", path))
}

/// Emit one WARN per non-perms field that changed between the
/// startup snapshot and the reloaded config. These fields are not
/// applied live — restart is required.
///
/// Comparison is by serialized JSON so we get a stable, structural
/// equality without depending on `PartialEq` impls. None of the
/// compared fields contain hash maps with nondeterministic ordering.
#[cfg(unix)]
fn warn_structural_changes(orig: &file::Config, new: &file::Config) {
    macro_rules! cmp {
        ($field:ident) => {
            let a = serde_json::to_string(&orig.$field).ok();
            let b = serde_json::to_string(&new.$field).ok();
            if a != b {
                warn!(
                    "config field '{}' changed; restart required for this to take effect",
                    stringify!($field),
                );
            }
        };
    }
    cmp!(parent);
    cmp!(children);
    cmp!(member_servers);
}

#[cfg(unix)]
pub(crate) fn run(params: Params) -> Result<()> {
    env_logger::init();
    // Canonicalize before daemonizing so SIGHUP can find the
    // config after `daemonize` chdirs to `/`.
    let config_path = std::path::Path::new(&params.config)
        .canonicalize()
        .with_context(|| format!("canonicalizing config path {:?}", params.config))?;
    // Load the file once at startup. We need:
    //   - the file::Config for the SIGHUP baseline diff
    //   - the file::Config to set pid_file before daemonizing
    //   - the validated Config for the running server
    // All three come from this single load via `Config::from_file` —
    // there is no second read of the file from disk.
    let mut file_cfg = load_file_config(&config_path)?;
    if !params.foreground {
        let member = &mut file_cfg.member_servers[params.id];
        member.pid_file.set_extension(params.id.to_string());
        Daemonize::new()
            .pid_file(&member.pid_file)
            .start()
            .context("failed to daemonize")?;
    }
    let baseline = file_cfg.clone();
    let config = Config::from_file(file_cfg)
        .context("validating resolver server config")?;
    tokio_run(config, baseline, config_path, params)
}

#[cfg(windows)]
pub(crate) fn run(params: Params) -> Result<()> {
    env_logger::init();
    let config_path = PathBuf::from(&params.config);
    let config = Config::load(&config_path)
        .context("failed to load resolver server config")?;
    tokio_run(config, params)
}
