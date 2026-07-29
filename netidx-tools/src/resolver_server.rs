use anyhow::{Context, Result};
use arcstr::ArcStr;
use clap::Args;
#[cfg(unix)]
use daemonize::Daemonize;
use log::{info, warn};
use netidx::resolver_server::{
    NotApplied, Server,
    config::{Config, file},
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::PathBuf,
    time::Duration,
};
#[cfg(unix)]
use tokio::signal::unix::{Signal, SignalKind, signal};

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

/// SIGHUP-driven "reload now" trigger. On unix this is a real signal stream;
/// elsewhere it simply never fires, leaving the poll as the only trigger.
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

/// How often the config and its `include_permissions` files are read to see
/// whether they have changed. The same interval a netidx client polls its own
/// config on.
const POLL_INTERVAL: Duration = Duration::from_secs(30);

/// A fingerprint of every file the running configuration is made of: the main
/// config, then each `include_permissions` entry in order, with a marker for
/// one that isn't there.
///
/// This is a poll, and it compares content, because the filesystem-watch
/// version of it did not work. Every tool that writes these files — the admin
/// plane certainly, and most editors — replaces them by renaming a new file
/// over the top, so a watch has to follow the path onto a fresh inode each
/// time. When it silently failed to, the daemon had no way to tell: the event
/// channel stayed open and the loop went on waiting for events that could not
/// arrive, and a resolver applied the first change pushed to it and ignored
/// every one after. There are at most a handful of small files here. Reading
/// them twice a minute costs nothing and cannot go quietly deaf.
///
/// Comparing content rather than mtime also means a rewrite that changes
/// nothing is not a reload, and a torn read needs no special handling — it
/// hashes differently from the finished file, so the next pass picks that up.
async fn fingerprint(config_path: &std::path::Path, includes: &[ArcStr]) -> u64 {
    async fn hash_into(h: &mut DefaultHasher, path: &std::path::Path) {
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                1u8.hash(h);
                bytes.hash(h)
            }
            Err(_) => 0u8.hash(h),
        }
    }
    let mut h = DefaultHasher::new();
    hash_into(&mut h, config_path).await;
    for p in includes {
        hash_into(&mut h, std::path::Path::new(p.as_str())).await;
    }
    h.finish()
}

/// Reload loop, driven by SIGHUP (unix) and by on-disk changes to the config
/// or any `include_permissions` file (all platforms). Both triggers run the
/// same reload.
async fn run_reload_loop(
    server: Server,
    config_path: PathBuf,
    baseline: file::Config,
) -> Result<()> {
    // `baseline` is the startup snapshot, and it is **never updated**. It is
    // used only to notice a `member_servers` edit, which is the one field
    // nothing about a running server can act on. See `members_changed`.
    let mut sighup = Sighup::new()?;
    // The include set comes from the running config, so it follows an edit
    // that adds or drops an include file.
    let mut includes = baseline.include_permissions.clone();
    let mut current = fingerprint(&config_path, &includes).await;

    loop {
        let trigger = tokio::select! {
            // A shutdown request (ctrl-c / SIGTERM, or the activation
            // supervisor's Windows event) ends the loop cleanly, dropping
            // the server rather than waiting to be hard-killed.
            _ = netidx_activation::shutdown::wait() => break Ok(()),
            _ = sighup.recv() => Some("SIGHUP"),
            _ = tokio::time::sleep(POLL_INTERVAL) => {
                let latest = fingerprint(&config_path, &includes).await;
                if latest == current {
                    continue;
                }
                current = latest;
                Some("file change")
            }
        };
        let Some(trigger) = trigger else { break Ok(()) };
        info!("resolver: {trigger} — reloading");
        match handle_reload(&server, &config_path, &baseline).await {
            Ok(new_file) => {
                info!("config reloaded successfully");
                if new_file.include_permissions != includes {
                    info!(
                        "include_permissions changed ({} → {} paths)",
                        includes.len(),
                        new_file.include_permissions.len(),
                    );
                    includes = new_file.include_permissions.clone();
                }
                // Re-read after applying, so that a file which changed again
                // while we were reloading is noticed on the next pass, and so
                // that a SIGHUP doesn't leave a stale fingerprint behind.
                current = fingerprint(&config_path, &includes).await;
            }
            // Leave the fingerprint where it is: a config we could not apply
            // gets tried again when it changes — which is what an operator
            // fixing it does — rather than failing identically every 30s.
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

    /// The case a filesystem watch got wrong: the admin plane, and most
    /// editors, replace a config by renaming a new file over the top, so the
    /// path gets a fresh inode every time. Nothing here follows an inode.
    #[tokio::test]
    async fn a_rename_over_changes_the_fingerprint_every_time() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("resolver.json");
        let tmp = dir.path().join("resolver.json.tmp");
        std::fs::write(&cfg, b"v0").unwrap();
        let mut last = fingerprint(&cfg, &[]).await;
        for round in 1..=4 {
            std::fs::write(&tmp, format!("v{round}")).unwrap();
            std::fs::rename(&tmp, &cfg).unwrap();
            let now = fingerprint(&cfg, &[]).await;
            assert_ne!(now, last, "replace {round} went unnoticed");
            last = now;
        }
        // A rewrite that changes nothing is not a change.
        std::fs::write(&tmp, b"v4").unwrap();
        std::fs::rename(&tmp, &cfg).unwrap();
        assert_eq!(fingerprint(&cfg, &[]).await, last);
    }

    /// An `include_permissions` file is part of the configuration, and a
    /// missing one has to be distinguishable from an empty one — otherwise
    /// deleting it would look like no change at all.
    #[tokio::test]
    async fn includes_are_part_of_the_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("resolver.json");
        let perms = dir.path().join("perms.json");
        std::fs::write(&cfg, b"{}").unwrap();
        let includes = [ArcStr::from(perms.to_str().unwrap())];

        let absent = fingerprint(&cfg, &includes).await;
        std::fs::write(&perms, b"").unwrap();
        let empty = fingerprint(&cfg, &includes).await;
        assert_ne!(absent, empty, "a missing include must not hash as an empty one");

        std::fs::write(&perms, b"{\"/\":{}}").unwrap();
        let filled = fingerprint(&cfg, &includes).await;
        assert_ne!(empty, filled);

        // ... and the same bytes under a different name are a different set.
        let other = dir.path().join("other.json");
        std::fs::write(&other, b"{\"/\":{}}").unwrap();
        let renamed = fingerprint(&cfg, &[ArcStr::from(other.to_str().unwrap())]).await;
        assert_eq!(filled, renamed, "content is what is hashed, not the path");
        assert_ne!(
            filled,
            fingerprint(&cfg, &[]).await,
            "dropping the include is a change"
        );
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
