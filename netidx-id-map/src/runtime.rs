//! Unix-socket daemon that answers id-map queries from the resolver.
//!
//! The wire protocol is identical to the resolver's existing
//! `IdMapType::Socket` mode (see `netidx/src/os/unix.rs`):
//!
//! ```text
//! client → "<query>\n"     where <query> is a netidx name or a uid
//! server → "uid=N(name) gid=M(primary) groups=M(primary),G(g),..."
//! server closes the connection
//! ```
//!
//! The current [`crate::file::IdMap`] lives as a plain `Arc<IdMap>`
//! local to [`run_loop`]; each accepted connection is handed a clone of
//! it, and a reload just rebinds that local to a fresh `Arc`. There is
//! no shared lock — only `run_loop` ever mutates the binding, and it
//! does so between `select` branches. Two event sources, both handled
//! as extra arms of that one `select`, can trigger a re-read:
//!
//! - An `extended-notify` watch on the config file fires whenever
//!   the file is modified or replaced (atomic-write rename-into-place
//!   shows up as a Create event for the parent-dir watcher). This is
//!   the normal path — operators editing `id-map.json` directly or
//!   running `netidx conf id-map …` get reload-for-free. The watcher
//!   forwards its event batches straight onto a channel (the
//!   `extended-notify` crate implements its handler trait for an mpsc
//!   sender) and `run_loop` reloads on anything that arrives — we don't
//!   inspect the events. That includes the synthetic `Established`
//!   event, so the watch going live triggers one reload that re-reads
//!   the file and catches any change made between startup and arming.
//!   If the watcher can't be set up on a given platform we just lose
//!   live reload and fall back to SIGHUP.
//! - `SIGHUP` is kept as a manual override for the rare case where a
//!   filesystem doesn't deliver notifications (network mounts, some
//!   container setups) or the operator wants to force a reload after
//!   editing through some channel the watcher didn't catch.
//!
//! Either way, parse failures keep the last-known-good map in place
//! so a botched edit can't lock out every TLS identity. Apart from the
//! watcher's internal task (owned by the `extended-notify` crate), the
//! only tasks this module spawns are `run_loop` and one short-lived
//! task per accepted connection.
//!
//! This file is `unix`-only — `tokio::net::UnixListener` and
//! `tokio::signal::unix` aren't available elsewhere and the resolver's
//! `Socket` mode is itself unix-only.
//!
//! ## Hardening
//!
//! Each accepted connection runs inside a 10s timeout and reads at
//! most 1 KiB before discarding the query — both bound the cost of a
//! confused/malicious peer that opens connections and never sends a
//! complete line. The 1 KiB ceiling is well above any realistic name
//! length but small enough to make a slow-loris flood cheap to shed.

use crate::file::{self, IdMap, Query};
use anyhow::{Context, Result, bail};
use enumflags2::make_bitflags;
use extended_notify::{
    ArcPath, EventBatch, Interest, Watched, Watcher, WatcherConfigBuilder,
};
use log::{debug, info, warn};
use poolshark::local::LPooled;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    signal::unix::{Signal, SignalKind, signal},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

/// Caps on per-connection work. Both are deliberately small — a
/// well-behaved resolver writes a single short line and reads the
/// response in milliseconds.
const PER_CONN_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_QUERY_BYTES: usize = 1024;

/// Max in-flight queries the daemon will handle at once. Mirrors
/// the cap used by `local_auth::AuthServer` in the resolver
/// (`netidx/src/os/unix.rs:210`). A local peer with socket
/// permission could otherwise open thousands of slow connections;
/// past this cap new connections are accepted then immediately
/// dropped, which is cheap on the kernel side.
const MAX_INFLIGHT: usize = 32;

/// Parameters used to start the daemon.
#[derive(Debug, Clone)]
pub struct ServerParams {
    /// Where the daemon will bind its unix socket. The socket file is
    /// `unlink`ed first if it already exists — the daemon is the
    /// single owner of this path.
    pub socket: PathBuf,
    /// File mode applied to the socket after bind (default 0o600 so
    /// only the daemon user can connect; set 0o660 + a group when
    /// running under a service account that needs to share with the
    /// resolver).
    pub socket_mode: u32,
    /// Path to the id-map JSON config. Parsed once at startup, then
    /// re-parsed automatically whenever the file changes (via the
    /// extended-notify watch) or `SIGHUP` is received.
    pub config: PathBuf,
}

impl ServerParams {
    pub fn new(socket: PathBuf, config: PathBuf) -> Self {
        Self { socket, socket_mode: 0o600, config }
    }
}

/// A running id-map daemon. Drop the server to shut it down (the main
/// loop receives a oneshot and exits; in-flight requests continue
/// until their timeout). The file watcher shuts down implicitly when
/// its handles drop.
pub struct Server {
    _stop: oneshot::Sender<()>,
    join: Option<JoinHandle<()>>,
    // The watcher's background task stays alive as long as either
    // `_watcher` or `_watched` holds a sender into its command
    // channel; `_watched` also tears down the specific path watch on
    // drop, which is what we want at Server shutdown. Both are held
    // here so they live exactly as long as the Server does. Dropping
    // them closes the file-changed channel, which the main loop just
    // stops selecting on — the SIGHUP arm and queries keep working.
    _watcher: Option<Watcher>,
    _watched: Option<Watched>,
}

impl Server {
    /// Start the daemon: bind the socket, parse the config, install the
    /// config-file watcher, and spawn the main loop (which also arms
    /// SIGHUP). Returns when the listener is accepting connections.
    pub async fn start(params: ServerParams) -> Result<Server> {
        let initial = load_config(&params.config)
            .with_context(|| format!("loading {:?}", params.config))?;
        let map = Arc::new(initial);

        // Probe before unlinking: if something is already listening
        // on this socket, abort rather than silently steal it.
        // Activation supervision already enforces single-instance
        // per unit; this defends against direct `netidx id-map serve`
        // misuse and against a stale path racing a fresh start.
        //
        // We `lstat` (not `stat`) the path so a symlink at --socket is
        // treated as "not a socket" rather than followed. Only an
        // actual socket inode is eligible for cleanup — anything else
        // (regular file, FIFO, directory, symlink) is almost certainly
        // operator misconfiguration and we'd rather bail than unlink it.
        use std::os::unix::fs::FileTypeExt;
        match tokio::fs::symlink_metadata(&params.socket).await {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("stat {:?} before bind", params.socket));
            }
            Ok(md) if md.file_type().is_socket() => {
                match UnixStream::connect(&params.socket).await {
                    Ok(_) => {
                        bail!(
                            "another id-map daemon already listening on {:?}; \
                             refusing to take the socket",
                            params.socket
                        );
                    }
                    Err(_) => {
                        // Stale socket — safe to clean up. The connect
                        // probe is best-effort: a live peer that's hung
                        // could also produce a connect error, but the
                        // operator-facing outcome is the same (we bind,
                        // they didn't notice they had a hung daemon).
                        let _ = tokio::fs::remove_file(&params.socket).await;
                    }
                }
            }
            Ok(md) => {
                bail!(
                    "refusing to bind id-map socket: {:?} exists but is not a \
                     unix socket (file type: {:?}). Pick a different --socket \
                     path or remove this file by hand.",
                    params.socket,
                    md.file_type()
                );
            }
        }
        let listener = UnixListener::bind(&params.socket)
            .with_context(|| format!("binding {:?}", params.socket))?;
        set_socket_mode(&params.socket, params.socket_mode).with_context(|| {
            format!("chmod {:o} on {:?}", params.socket_mode, params.socket)
        })?;

        let (stop_tx, stop_rx) = oneshot::channel();
        let config_path = params.config.clone();
        // The watcher forwards event batches here; `run_loop` reloads on
        // any of them. Bounded because changes are debounced and rare,
        // and a couple of events coalescing into one reload is fine.
        let (watch_tx, watch_rx) = mpsc::channel(16);
        // Filesystem watcher for live reload. If it can't be set up on
        // this platform we log once and carry on — SIGHUP is still the
        // manual fallback, and the daemon serves the initial map either
        // way. (On the failure path `watch_tx` is dropped, so the main
        // loop's receiver closes and it simply stops watching.)
        let (watcher, watched) = match start_config_watcher(config_path.clone(), watch_tx)
        {
            Ok((w, h)) => (Some(w), Some(h)),
            Err(e) => {
                warn!(
                    "id-map: config-file watcher unavailable: {e:#}; \
                         SIGHUP still reloads"
                );
                (None, None)
            }
        };
        let join = tokio::spawn(run_loop(listener, config_path, map, watch_rx, stop_rx));

        info!(
            "id-map daemon ready on {} (config {})",
            params.socket.display(),
            params.config.display(),
        );
        Ok(Server {
            _stop: stop_tx,
            join: Some(join),
            _watcher: watcher,
            _watched: watched,
        })
    }

    /// Signal shutdown and block until the main loop exits. The
    /// previous `join`-by-self API deadlocked on a live server
    /// because `self` still owned `_stop` while awaiting the loop
    /// task — the `oneshot::Receiver` in `run_loop` would never fire,
    /// so the task would never exit. This method destructures `Self`
    /// up front so `_stop` (and the watcher handles) drop *before* we
    /// start awaiting the loop.
    ///
    /// The only other way to stop the daemon is dropping the `Server`
    /// (same drop order via the struct's implicit `Drop`, just without
    /// the await). Both paths produce the same shutdown sequence; this
    /// one additionally lets the caller observe the loop-task result.
    pub async fn shutdown_and_join(self) -> Result<()> {
        // Exhaustive destructure — adding a new field to `Server`
        // forces this site to declare what its drop order should be,
        // rather than silently letting it ride along on the implicit
        // end-of-function drop (which is what caused the original
        // deadlock).
        let Self { _stop, mut join, _watcher, _watched } = self;
        // Drop the stop sender (and watcher handles) before awaiting
        // the join, so the main loop's `select` sees the closed
        // oneshot and breaks rather than blocking here forever.
        drop(_stop);
        drop(_watcher);
        drop(_watched);
        if let Some(h) = join.take() {
            h.await.context("id-map main loop task panicked")?;
        }
        Ok(())
    }
}

/// The daemon's single long-lived task. It owns the current map,
/// serves queries by handing each accepted connection a clone of it,
/// and folds both reload triggers — SIGHUP and the file watcher — into
/// the same `select` so a reload is just a between-branches rebind of
/// the local `map`. The only other tasks are the per-connection
/// handlers it spawns.
async fn run_loop(
    listener: UnixListener,
    config_path: PathBuf,
    mut map: Arc<IdMap>,
    mut watch_events: mpsc::Receiver<EventBatch>,
    stop: oneshot::Receiver<()>,
) {
    let inflight = Arc::new(AtomicUsize::new(0));
    // SIGHUP is the manual reload override (see module docs). If the
    // handler can't be installed we log and carry on — the file
    // watcher is the normal reload path and queries still work. Held
    // in an `Option` because there may be no handler at all; the
    // refutable `Some(())` pattern below disables the arm when it's
    // absent.
    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(s) => Some(s),
        Err(e) => {
            warn!("id-map: failed to install SIGHUP handler: {e}");
            None
        }
    };
    tokio::pin!(stop);
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => {
                debug!("id-map main loop shutdown");
                break;
            }
            // Manual reload via SIGHUP. The refutable `Some(())`
            // disables this arm when no handler is installed or the
            // signal stream ends, rather than firing on `None`.
            Some(()) = recv_sighup(&mut sighup) => {
                info!(
                    "id-map: SIGHUP received, reloading {}",
                    config_path.display()
                );
                reload(&config_path, &mut map).await;
            }
            // Live reload from the file watcher. We don't inspect the
            // batch — any event means re-read the file. The refutable
            // `Some(_)` disables this arm when the watcher channel is
            // closed (watcher failed to start, or dropped at shutdown)
            // instead of spinning on an immediately-ready `None`.
            Some(_batch) = watch_events.recv() => {
                info!(
                    "id-map: detected change to {}, reloading",
                    config_path.display()
                );
                reload(&config_path, &mut map).await;
            }
            r = listener.accept() => match r {
                Ok((client, _addr)) => {
                    // Cap in-flight queries; drop excess connections
                    // immediately so a misbehaving peer can't
                    // accumulate state. The kernel keeps backlog
                    // bounded, and the resolver's `Mapper::Socket`
                    // path retries (see the netidx-side change in
                    // os/unix.rs).
                    if inflight.load(Ordering::Relaxed) >= MAX_INFLIGHT {
                        drop(client);
                        continue;
                    }
                    inflight.fetch_add(1, Ordering::Relaxed);
                    // Hand the connection a snapshot of the current
                    // map; a concurrent reload only affects later
                    // connections.
                    let map = Arc::clone(&map);
                    let inflight_clone = Arc::clone(&inflight);
                    tokio::spawn(async move {
                        if let Err(e) = timeout(
                            PER_CONN_TIMEOUT,
                            handle_one(client, map),
                        )
                        .await
                        .map_err(|_| anyhow!("per-connection timeout"))
                        .and_then(|r| r)
                        {
                            warn!("id-map request failed: {e:#}");
                        }
                        inflight_clone.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                Err(e) => {
                    warn!("id-map accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

/// SIGHUP source for the `select`. When no handler is installed the
/// future never resolves, so the arm is never chosen; when the stream
/// ends it yields `None`, which the arm's refutable `Some(())` pattern
/// treats the same way (skip). A plain `mpsc::Receiver` needs no such
/// helper — its `recv()` already returns `Option`, so it can be matched
/// with a refutable pattern directly.
async fn recv_sighup(sighup: &mut Option<Signal>) -> Option<()> {
    match sighup {
        Some(s) => s.recv().await,
        None => std::future::pending().await,
    }
}

/// Re-read the config off-thread and rebind `map` on success. A parse
/// or IO failure logs and keeps the last-known-good map so a botched
/// edit can't lock out every TLS identity. `spawn_blocking` keeps the
/// disk read off the runtime thread so the per-connection handlers
/// still make progress on a single-threaded runtime while we wait.
async fn reload(config_path: &Path, map: &mut Arc<IdMap>) {
    let path = config_path.to_path_buf();
    match tokio::task::spawn_blocking(move || load_config(&path)).await {
        Ok(Ok(new)) => {
            *map = Arc::new(new);
            info!("id-map: reload OK");
        }
        Ok(Err(e)) => {
            warn!("id-map: reload failed, keeping last-known-good map: {e:#}")
        }
        Err(e) => warn!("id-map: reload task panicked: {e:#}"),
    }
}

async fn handle_one(mut client: UnixStream, map: Arc<IdMap>) -> Result<()> {
    let mut buf: LPooled<Vec<u8>> = LPooled::take();
    let mut chunk = [0u8; 256];
    loop {
        // Bound the read to the remaining capacity so a malicious
        // peer can't push the buffer past MAX_QUERY_BYTES between
        // our pre-read check and our post-read check. The previous
        // `> MAX_QUERY_BYTES` post-check let one 256-byte chunk
        // overshoot.
        let remaining = MAX_QUERY_BYTES.saturating_sub(buf.len());
        if remaining == 0 {
            bail!("query exceeded {MAX_QUERY_BYTES} byte cap with no newline");
        }
        let take = remaining.min(chunk.len());
        let n = client.read(&mut chunk[..take]).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.contains(&b'\n') {
            break;
        }
    }
    let line = std::str::from_utf8(&buf)
        .context("query was not valid UTF-8")?
        .lines()
        .next()
        .ok_or_else(|| anyhow!("query was empty"))?;
    // Apply the same delimiter check we run on stored names. The
    // unknown-name fallback echoes the query into the `uid=N(<query>)`
    // field; without this guard, a query like `evil) gid=0(root`
    // would inject a fake `gid=0(root)` token that the resolver's
    // `Mapper::parse_output` would happily treat as the identity's
    // primary group. We have to defend against the wire-side input
    // here — `IdMap::validate` only covers what's *stored* in the
    // map.
    file::check_name_chars("id-map query", line)?;
    debug!("id-map query: {line}");
    let response = match Query::parse(line) {
        Query::Uid(u) => map.format_id_line_for_uid(u),
        Query::Name(n) => map.format_id_line_for_name(n),
    };
    debug!("id-map response: {}", response.trim_end());
    client.write_all(response.as_bytes()).await?;
    client.shutdown().await?;
    Ok(())
}

/// Start the config-file watcher, forwarding its event batches onto
/// `events`. `extended-notify` implements its `EventHandler` trait for
/// an mpsc sender directly, so we don't need a custom handler — the
/// main loop reloads on any batch it receives. Returns the ownership
/// handles; dropping either stops the watch.
fn start_config_watcher(
    config_path: PathBuf,
    events: mpsc::Sender<EventBatch>,
) -> Result<(Watcher, Watched)> {
    let watcher = WatcherConfigBuilder::default()
        .event_handler(events)
        .build()
        .context("building config-file watcher")?
        .start()
        .context("starting config-file watcher")?;
    // Modify covers in-place writes; Create covers the rename-to side
    // of an atomic write (a fresh inode appearing at the path); Delete
    // covers `rm` followed by a fresh write. Established is the
    // synthetic "watch is now armed" event: because the main loop
    // reloads on any event without inspecting it, subscribing to it
    // buys one reload right after the watch goes live. That costs a
    // single spurious re-read per run but closes the window between the
    // synchronous startup read and the watch arming — a change landing
    // in that gap would otherwise never be observed.
    let interests = make_bitflags!(Interest::{Established | Modify | Create | Delete});
    let watched =
        watcher.add(ArcPath::from(config_path), interests).context("adding watch")?;
    Ok((watcher, watched))
}

fn load_config(path: &Path) -> Result<IdMap> {
    // Synchronous on purpose: callers from async contexts wrap us in
    // `spawn_blocking`; the synchronous startup path runs before the
    // runtime is doing anything else.
    let bytes =
        std::fs::read(path).with_context(|| format!("reading id-map config {path:?}"))?;
    file::parse_bytes(&bytes)
}

#[cfg(unix)]
fn set_socket_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{Group, Identity};
    use arcstr::ArcStr;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn write_sample(path: &Path) {
        let mut groups = BTreeMap::new();
        groups.insert(ArcStr::from("users"), Group { gid: 100 });
        groups.insert(ArcStr::from("wheel"), Group { gid: 10 });
        let mut identities = BTreeMap::new();
        identities.insert(
            ArcStr::from("alice.example.com"),
            Identity {
                uid: 1000,
                primary_group: ArcStr::from("users"),
                groups: vec![ArcStr::from("wheel")],
            },
        );
        let cfg = IdMap { default_uid: 65534, default_gid: 65534, groups, identities };
        std::fs::write(path, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
    }

    async fn query(socket: &Path, q: &str) -> String {
        let mut s = UnixStream::connect(socket).await.unwrap();
        s.write_all(format!("{q}\n").as_bytes()).await.unwrap();
        s.shutdown().await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn end_to_end_name_query() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let s = query(&sock_path, "alice.example.com").await;
        assert!(s.contains("uid=1000(alice.example.com)"), "got {s:?}");
        assert!(s.contains("gid=100(users)"), "got {s:?}");
        assert!(s.contains("groups=100(users),10(wheel)"), "got {s:?}");
    }

    #[tokio::test]
    async fn end_to_end_uid_query() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let s = query(&sock_path, "1000").await;
        assert!(s.contains("uid=1000(alice.example.com)"), "got {s:?}");
    }

    #[tokio::test]
    async fn unknown_name_falls_back_to_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let s = query(&sock_path, "ghost.example.com").await;
        assert!(s.contains("uid=65534(ghost.example.com)"), "got {s:?}");
    }

    #[tokio::test]
    async fn refuses_to_steal_socket_from_live_daemon() {
        // Start a daemon, then try to start a second one on the same
        // socket. The probe must catch the live first instance and
        // refuse rather than silently take over.
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let _first = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let r = Server::start(ServerParams {
            socket: sock_path,
            socket_mode: 0o600,
            config: cfg_path,
        })
        .await;
        let err = r.err().expect("second start must fail");
        assert!(
            format!("{err:#}").contains("already listening"),
            "expected already-listening error, got: {err:#}",
        );
    }

    #[tokio::test]
    async fn injection_query_does_not_leak_into_response() {
        // The attack: a TLS client with a SAN of `evil) gid=0(root`
        // gets its SAN passed verbatim as a query. Without filtering,
        // the unknown-name fallback would echo it into
        // `uid=65534(evil) gid=0(root) gid=65534(...)` and the
        // resolver's `Mapper::parse_output` would read the injected
        // `gid=0(root)` as the primary group. The daemon must refuse
        // any query containing delimiter characters.
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let s = query(&sock_path, "evil) gid=0(root").await;
        // The connection closes with no valid id line. We don't care
        // what (if anything) the daemon wrote; what matters is no
        // injected `gid=0(root)` reaches the resolver's parser.
        assert!(
            !s.contains("gid=0(root)"),
            "injected gid token leaked into response: {s:?}"
        );
        assert!(
            !s.contains("uid=65534(evil"),
            "query was echoed into the uid field: {s:?}"
        );
    }

    #[tokio::test]
    async fn slow_loris_rejected_at_cap() {
        // Send MAX_QUERY_BYTES+ bytes with no newline; server must
        // give up rather than buffer indefinitely.
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let mut s = UnixStream::connect(&sock_path).await.unwrap();
        // Send 2 KiB of `a` (no newline). The daemon should respond
        // with nothing useful — the connection completes with no
        // valid id line. We just assert the read terminates and
        // doesn't deliver a full /bin/id-format response.
        let payload = vec![b'a'; 2048];
        let _ = s.write_all(&payload).await;
        let _ = s.shutdown().await;
        let mut out = String::new();
        let _ = s.read_to_string(&mut out).await;
        assert!(
            !out.contains("uid="),
            "no valid id line should leak past the cap; got: {out:?}",
        );
    }

    /// Edit the config on disk, then poll the daemon *through the
    /// socket* until it reflects the new identity (or we hit the 10s
    /// ceiling). Querying the live interface, rather than peeking at
    /// internal state, is the real end-to-end check that a reload
    /// reached the path that serves clients. The watcher's default
    /// debounce timeout is 250ms, so a reload should arrive within a
    /// few hundred ms; the long ceiling is just to be CI-tolerant on
    /// slow filesystems.
    #[tokio::test]
    async fn config_watcher_reloads_on_modify() {
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        // Initial map has alice but not bob. The watcher arms its
        // parent-dir inotify watch on its own task shortly after
        // start; the two socket round-trips below give it ample time
        // before we touch the file, and the reload is observed via the
        // poll loop regardless.
        let s = query(&sock_path, "alice.example.com").await;
        assert!(s.contains("uid=1000(alice.example.com)"), "got {s:?}");
        // bob is unknown, so the daemon falls back to the default uid.
        let s = query(&sock_path, "bob.example.com").await;
        assert!(s.contains("uid=65534(bob.example.com)"), "got {s:?}");

        // Edit the file: add bob. We use the same atomic-write
        // pattern the `netidx conf id-map` tools use, since the
        // rename-into-place path is the most demanding for the
        // watcher to catch.
        let mut groups = BTreeMap::new();
        groups.insert(arcstr::ArcStr::from("users"), Group { gid: 100 });
        let mut identities = BTreeMap::new();
        identities.insert(
            arcstr::ArcStr::from("alice.example.com"),
            Identity {
                uid: 1000,
                primary_group: arcstr::ArcStr::from("users"),
                groups: vec![],
            },
        );
        identities.insert(
            arcstr::ArcStr::from("bob.example.com"),
            Identity {
                uid: 1001,
                primary_group: arcstr::ArcStr::from("users"),
                groups: vec![],
            },
        );
        let cfg = IdMap { default_uid: 65534, default_gid: 65534, groups, identities };
        let bytes = serde_json::to_vec_pretty(&cfg).unwrap();
        // Atomic write: temp + rename — the most demanding path for the
        // watcher, since the target gets a fresh inode.
        let tmp = cfg_path.with_extension("json.tmp");
        std::fs::write(&tmp, &bytes).unwrap();
        std::fs::rename(&tmp, &cfg_path).unwrap();

        // Poll for up to ~10s. The edit can land before the watch is
        // armed, but because we subscribe to `Established` the watch
        // going live triggers a reload that re-reads the file and picks
        // bob up regardless of arming timing. (If the watch armed
        // first, the Create from the rename catches it instead.) So a
        // single up-front write is enough — no need to keep re-writing.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let s = query(&sock_path, "bob.example.com").await;
            if s.contains("uid=1001(bob.example.com)") {
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!("watcher did not reload within 10s; last bob query: {s:?}",);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn refuses_to_load_malformed_config_at_startup() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        std::fs::write(&cfg_path, "this is not json").unwrap();
        let sock_path = dir.path().join("id-map.sock");
        let r = Server::start(ServerParams {
            socket: sock_path,
            socket_mode: 0o600,
            config: cfg_path,
        })
        .await;
        assert!(r.is_err());
    }

    /// `shutdown_and_join` on a live server must terminate the
    /// listener task and return — *not* hang waiting for a `_stop`
    /// signal that can only fire after `self` is fully dropped. This
    /// is the regression guard for the original `join`-by-self
    /// deadlock that codex flagged. Without the destructure +
    /// explicit drop, this test would never return.
    #[tokio::test]
    async fn shutdown_and_join_returns_on_live_server() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let server = Server::start(ServerParams {
            socket: sock_path,
            socket_mode: 0o600,
            config: cfg_path,
        })
        .await
        .unwrap();
        // Tight timeout: a working shutdown is essentially instant;
        // 1s is plenty of headroom for slow CI. The bug being
        // regressed would manifest as an indefinite hang, so the
        // exact value just bounds the diagnostic latency.
        tokio::time::timeout(Duration::from_secs(1), server.shutdown_and_join())
            .await
            .expect("shutdown_and_join hung — regression of the _stop drop-order bug")
            .expect("listener task returned an error");
    }
}
