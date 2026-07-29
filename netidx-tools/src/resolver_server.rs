use anyhow::{Context, Result};
use arcstr::ArcStr;
use clap::Args;
#[cfg(unix)]
use daemonize::Daemonize;
use enumflags2::make_bitflags;
use extended_notify::{
    ArcPath, EventBatch, EventKind, Interest, Watched, Watcher, WatcherConfigBuilder,
};
use log::{info, warn};
use netidx::resolver_server::{
    NotApplied, Server,
    config::{Config, file},
};
use std::path::PathBuf;
#[cfg(unix)]
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::mpsc;

#[derive(Args, Debug)]
pub(crate) struct Params {
    /// path to the server config
    #[arg(short, long)]
    config: String,
    /// don't daemonize
    #[arg(short, long)]
    #[allow(dead_code)]
    foreground: bool,
    /// don't allow read clients until 1 writer ttl has passed
    #[arg(long)]
    delay_reads: bool,
    /// index of the member server to run
    #[arg(long, default_value = "0")]
    id: usize,
}

// What we tell the operator when the file watcher is unavailable. On
// unix SIGHUP is still a working reload trigger; on platforms without
// unix signals the watcher was the only trigger, so losing it disables
// live reload entirely.
#[cfg(unix)]
const RELOAD_FALLBACK: &str = "continuing with SIGHUP-only reload";
#[cfg(not(unix))]
const RELOAD_FALLBACK: &str = "live config reload is now disabled";

/// SIGHUP-driven reload trigger. On unix this is a real signal stream;
/// elsewhere it simply never fires, leaving the file watcher as the
/// only reload trigger. Keeping it as an always-present (if pending)
/// select arm means the reload loop's `select!` never ends up with all
/// branches disabled — which would otherwise panic on Windows the
/// moment the watcher arm is gated off.
enum Sighup {
    #[cfg(unix)]
    Signal(Signal),
    #[cfg(not(unix))]
    Never,
}

impl Sighup {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self::Signal(
                signal(SignalKind::hangup()).context("registering SIGHUP handler")?,
            ))
        }
        #[cfg(not(unix))]
        {
            Ok(Self::Never)
        }
    }

    async fn recv(&mut self) {
        match self {
            #[cfg(unix)]
            Self::Signal(s) => {
                s.recv().await;
            }
            #[cfg(not(unix))]
            Self::Never => std::future::pending::<()>().await,
        }
    }
}

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
    run_reload_loop(server, config_path, baseline).await
}

/// Reload loop, driven by SIGHUP (unix) and/or on-disk changes to the
/// config or any `include_permissions` file (all platforms). Both
/// triggers run the same reload.
async fn run_reload_loop(
    server: Server,
    config_path: PathBuf,
    baseline: file::Config,
) -> Result<()> {
    // `baseline` is the startup snapshot, and it is **never updated**. It is
    // used only to notice a `member_servers` edit, which is the one field
    // nothing about a running server can act on. See `members_changed`.
    let mut sighup = Sighup::new()?;

    // File-watch path: drive the same reload as SIGHUP whenever the
    // config or any include_permissions file changes on disk. The
    // watcher is best-effort — if it fails to start, log and fall
    // back to SIGHUP (unix) or no live reload (elsewhere).
    //
    // `_watched` is held purely for its Drop side effect (RAII watch
    // stop). It's reassigned when the include set changes; the
    // assignment is meaningful even though the value isn't read
    // again — the prior Vec drops, ending those watches.
    let (events_tx, mut events_rx) = mpsc::channel::<EventBatch>(64);
    #[allow(unused_assignments)]
    let (watcher_opt, mut _watched, mut included_paths) =
        match start_watcher_for(&config_path, &baseline, events_tx) {
            Ok((w, h, p)) => (Some(w), h, p),
            Err(e) => {
                warn!(
                    "resolver: failed to install config-file watcher: {e:#}; \
                     {RELOAD_FALLBACK}"
                );
                (None, Vec::new(), Vec::new())
            }
        };
    // Gate on this rather than just on the channel state: when the
    // watcher fails to start, `events_tx` was already dropped inside
    // `start_watcher_for` and `events_rx.recv()` would return `None`
    // immediately — the same path codex flagged as a daemon-kill,
    // just at startup. Initialising from `watcher_opt.is_some()`
    // covers both that case and post-start watcher death uniformly.
    let mut watcher_alive = watcher_opt.is_some();

    loop {
        let trigger = tokio::select! {
            // A shutdown request (ctrl-c / SIGTERM, or the activation
            // supervisor's Windows event) ends the loop cleanly, dropping
            // the server rather than waiting to be hard-killed.
            _ = netidx_activation::shutdown::wait() => break Ok(()),
            _ = sighup.recv() => Some("SIGHUP"),
            batch = events_rx.recv(), if watcher_alive => match batch {
                None => {
                    // The watcher task / channel died. File-change
                    // reloads are off from here on. Disabling the
                    // select arm via `watcher_alive` avoids the
                    // busy-loop that would otherwise come from
                    // `recv()` returning `None` immediately for every
                    // subsequent poll on a closed receiver.
                    warn!(
                        "resolver: file watcher channel closed; {RELOAD_FALLBACK}"
                    );
                    watcher_alive = false;
                    continue;
                }
                Some(b) if is_established_only(&b) => continue,
                Some(_) => Some("file change"),
            },
        };
        let Some(trigger) = trigger else { break Ok(()) };
        info!("resolver: {trigger} — reloading");
        match handle_reload(&server, &config_path, &baseline).await {
            Ok(new_file) => {
                info!("config reloaded successfully");
                // Re-arm the whole watch set after EVERY reload, not just
                // when `include_permissions` changed.
                //
                // Every writer of these files — the admin plane, and most
                // editors — replaces them by renaming a temp file over the
                // top, which gives the path a new inode. A watch that
                // followed the old inode is then watching something that no
                // longer has a name, and nothing here would ever notice: the
                // event channel stays open, so the loop goes on waiting for
                // events that cannot arrive. Observed in the lab as a
                // resolver that applied the first pushed change and silently
                // ignored every one after it, with `/proc/<pid>/fdinfo`
                // showing the config's watch simply gone.
                //
                // Dropping the old handles first makes the new ones genuinely
                // new watches rather than possible aliases of dead ones. The
                // gap that opens is a few syscalls wide, and a change landing
                // inside it would have to beat a reload that has only just
                // finished reading the file.
                if watcher_alive && let Some(w) = watcher_opt.as_ref() {
                    if new_file.include_permissions != included_paths {
                        info!(
                            "include_permissions changed ({} → {} paths)",
                            included_paths.len(),
                            new_file.include_permissions.len(),
                        );
                    }
                    _watched = Vec::new();
                    match watch_all(w, &config_path, &new_file.include_permissions) {
                        Ok(new_handles) => {
                            _watched = new_handles;
                            included_paths = new_file.include_permissions.clone();
                        }
                        Err(e) => warn!(
                            "resolver: re-arming the watch set failed: {e:#}; \
                             file-change reloads are off until restart — {RELOAD_FALLBACK}"
                        ),
                    }
                }
            }
            Err(e) => warn!("resolver: reload failed: {e:#}"),
        }
    }
}

async fn handle_reload(
    server: &Server,
    config_path: &std::path::Path,
    baseline: &file::Config,
) -> Result<file::Config> {
    info!("re-reading {:?}", config_path);
    let new_file = load_file_config(config_path)?;
    warn_members_changed(baseline, &new_file);
    let not_applied = server.reload(&new_file).await.context("applying the config")?;
    warn_not_applied(&not_applied);
    Ok(new_file)
}

/// Start the file watcher and arm watches for the main config plus
/// every `include_permissions` entry from `baseline`. Returns the
/// watcher (kept alive for the lifetime of the daemon), the active
/// `Watched` handles (one per path; drop to stop), and the path list
/// the handles correspond to (used downstream to detect changes that
/// require a rebuild).
fn start_watcher_for(
    config_path: &std::path::Path,
    baseline: &file::Config,
    events_tx: mpsc::Sender<EventBatch>,
) -> Result<(Watcher, Vec<Watched>, Vec<ArcStr>)> {
    let watcher = WatcherConfigBuilder::default()
        .event_handler(events_tx)
        .build()
        .context("building config-file watcher")?
        .start()
        .context("starting config-file watcher")?;
    let watched = watch_all(&watcher, config_path, &baseline.include_permissions)
        .context("arming initial watch set")?;
    Ok((watcher, watched, baseline.include_permissions.clone()))
}

/// Add watches for the main config + each include_permissions path.
/// Returns the resulting `Watched` handles in the same order as the
/// input (main config first, then includes). Caller is responsible
/// for keeping the handles alive — drop ends the watch.
fn watch_all(
    watcher: &Watcher,
    config_path: &std::path::Path,
    include_paths: &[ArcStr],
) -> Result<Vec<Watched>> {
    // Established is included so the receiver loop can log when
    // watches are armed (and the `is_established_only` guard
    // suppresses the would-be reload that those synthetic events
    // would otherwise trigger). Modify / Create / Delete are the
    // real-change interests; Create covers the rename-into-place that
    // atomic-write tools produce.
    let interests = make_bitflags!(Interest::{Established | Modify | Create | Delete});
    let mut handles = Vec::with_capacity(1 + include_paths.len());
    handles.push(
        watcher
            .add(ArcPath::from(config_path), interests)
            .context("watching main config")?,
    );
    for p in include_paths {
        let path = std::path::PathBuf::from(p.as_str());
        handles.push(
            watcher
                .add(ArcPath::from(path.as_path()), interests)
                .with_context(|| format!("watching include_permissions {p:?}"))?,
        );
    }
    Ok(handles)
}

/// True if every event in the batch is the synthetic `Established`
/// event. Those fire once per watch when the watcher arms and don't
/// represent an on-disk change, so they shouldn't trigger a reload.
fn is_established_only(batch: &EventBatch) -> bool {
    batch.iter().all(|(_, e)| matches!(e.event, EventKind::Event(Interest::Established)))
}

fn load_file_config(path: &std::path::Path) -> Result<file::Config> {
    // Goes through `Config::load_file` (not raw serde_json) so that
    // relative `include_permissions` paths are resolved against the
    // config file's parent directory — matching the startup load
    // path exactly.
    Config::load_raw(path).with_context(|| format!("reading {:?}", path))
}

/// Has `member_servers` changed since startup?
///
/// It is the one field a reload can do nothing at all with: it says which
/// server this process is and how it listens, both settled when the listener
/// was bound. Referral *addresses* are applied live, and the rest of what a
/// reload has to refuse comes back from the server itself as
/// [`NotApplied`].
///
/// Compared against the startup snapshot rather than the previous reload, so
/// an operator who edits and reverts is warned once, and one who edits and
/// leaves it keeps being told their change still hasn't taken effect.
/// Comparison is by serialized JSON: structural equality without depending on
/// `PartialEq`, and none of these fields have nondeterministic ordering.
fn members_changed(orig: &file::Config, new: &file::Config) -> bool {
    // `read_gated` lives inside `member_servers` and is the one field in there
    // that a reload applies immediately, so comparing the array as-is would
    // tell an operator to roll the cluster every time they opened or shut a
    // gate — the exact thing the gate exists to avoid.
    let without_gates = |cfg: &file::Config| {
        serde_json::to_string(
            &cfg.member_servers
                .iter()
                .map(|m| file::MemberServer {
                    read_gated: Default::default(),
                    ..m.clone()
                })
                .collect::<Vec<_>>(),
        )
        .ok()
    };
    without_gates(orig) != without_gates(new)
}

fn warn_members_changed(orig: &file::Config, new: &file::Config) {
    if members_changed(orig, new) {
        warn!(
            "config field 'member_servers' changed; the running resolver was \
             not restarted — use a manual one-member-at-a-time rolling restart \
             to apply the change"
        );
    }
}

fn warn_not_applied(not_applied: &NotApplied) {
    for path in not_applied.children_added.iter() {
        warn!(
            "child cluster {path} was added to the config; where a child \
             attaches is fixed when the server starts, so restart to serve it"
        );
    }
    for path in not_applied.children_removed.iter() {
        warn!(
            "child cluster {path} was removed from the config; the running \
             resolver still refers clients to it, restart to stop"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arcstr::ArcStr;

    fn empty_config() -> file::Config {
        file::ConfigBuilder::default().member_servers(vec![]).build().unwrap()
    }

    #[test]
    fn a_member_edit_is_reported_for_manual_restart() {
        let original = empty_config();
        let mut changed = original.clone();
        changed.member_servers.push(
            file::MemberServerBuilder::default()
                .addr("127.0.0.1:4564".parse().unwrap())
                .bind_addr("127.0.0.1".parse().unwrap())
                .auth(file::Auth::Anonymous)
                .build()
                .unwrap(),
        );
        assert!(!members_changed(&original, &original));
        assert!(members_changed(&original, &changed));
    }

    /// The gate is the one field inside `member_servers` that a reload applies
    /// straight away, so changing it must not tell an operator to roll the
    /// cluster — that is the very thing a gate exists to avoid.
    #[test]
    fn a_read_gate_change_is_not_a_member_edit() {
        let mut original = empty_config();
        original.member_servers.push(
            file::MemberServerBuilder::default()
                .addr("127.0.0.1:4564".parse().unwrap())
                .bind_addr("127.0.0.1".parse().unwrap())
                .auth(file::Auth::Anonymous)
                .build()
                .unwrap(),
        );
        for gate in [
            netidx::resolver_server::config::ReadGate::Yes,
            netidx::resolver_server::config::ReadGate::Until(chrono::Utc::now()),
            netidx::resolver_server::config::ReadGate::No,
        ] {
            let mut changed = original.clone();
            changed.member_servers[0].read_gated = gate;
            assert!(!members_changed(&original, &changed), "{gate:?}");
        }
        // ... but a real member edit alongside it still is one.
        let mut changed = original.clone();
        changed.member_servers[0].read_gated =
            netidx::resolver_server::config::ReadGate::Yes;
        changed.member_servers[0].addr = "127.0.0.1:4565".parse().unwrap();
        assert!(members_changed(&original, &changed));
    }

    #[test]
    fn a_referral_edit_is_not_reported_here() {
        // Referral addresses are applied live, and a change to where a child
        // attaches comes back from `Server::reload` as `NotApplied` — neither
        // belongs in the member diff.
        let original = empty_config();
        let mut changed = original.clone();
        changed.children.push(file::Referral {
            path: ArcStr::from("/eu"),
            ttl: None,
            addrs: vec![],
        });
        assert!(!members_changed(&original, &changed));
    }
}

pub(crate) fn run(params: Params) -> Result<()> {
    env_logger::init();
    // Canonicalize on unix so SIGHUP / the watcher can still find the
    // config after `daemonize` chdirs to `/`. On Windows we keep the
    // path as given — `canonicalize` there yields a `\\?\` extended
    // path the watcher doesn't reliably handle, and there's no chdir
    // to canonicalize against.
    #[cfg(unix)]
    let config_path = std::path::Path::new(&params.config)
        .canonicalize()
        .with_context(|| format!("canonicalizing config path {:?}", params.config))?;
    #[cfg(not(unix))]
    let config_path = PathBuf::from(&params.config);
    // Load the file once at startup. We need:
    //   - the file::Config for the reload baseline diff
    //   - the file::Config to set pid_file before daemonizing (unix)
    //   - the validated Config for the running server
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut file_cfg = load_file_config(&config_path)?;
    #[cfg(unix)]
    if !params.foreground {
        let member = &mut file_cfg.member_servers[params.id];
        member.pid_file.set_extension(params.id.to_string());
        Daemonize::new()
            .pid_file(&member.pid_file)
            .start()
            .context("failed to daemonize")?;
    }
    let baseline = file_cfg.clone();
    let config =
        Config::from_file(file_cfg).context("validating resolver server config")?;
    tokio_run(config, baseline, config_path, params)
}
