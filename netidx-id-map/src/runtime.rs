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
//! The daemon holds the parsed [`crate::file::IdMap`] behind an
//! `RwLock<Arc<IdMap>>`. Two paths trigger a re-read:
//!
//! - An `extended-notify` watch on the config file fires whenever
//!   the file is modified or replaced (atomic-write rename-into-place
//!   shows up as a Create event for the parent-dir watcher). This is
//!   the normal path — operators editing `id-map.json` directly or
//!   running `netidx conf id-map …` get reload-for-free.
//! - `SIGHUP` is kept as a manual override for the rare case where a
//!   filesystem doesn't deliver notifications (network mounts, some
//!   container setups) or the operator wants to force a reload after
//!   editing through some channel the watcher didn't catch.
//!
//! Either way, parse failures keep the last-known-good map in place
//! so a botched edit can't lock out every TLS identity.
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
use anyhow::{Context, Result};
use enumflags2::make_bitflags;
use extended_notify::{
    ArcPath, EventBatch, EventHandler, EventKind, Interest, Watched, Watcher,
    WatcherConfigBuilder,
};
use log::{debug, info, warn};
use parking_lot::RwLock;
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    signal::unix::{signal, SignalKind},
    sync::oneshot,
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

/// A running id-map daemon. Drop the server to shut it down (the
/// listener loop receives a oneshot and exits; in-flight requests
/// continue until their timeout). The file watcher and SIGHUP task
/// shut down implicitly when their handles drop.
pub struct Server {
    _stop: oneshot::Sender<()>,
    join: Option<JoinHandle<()>>,
    /// Shutdown signal + JoinHandle for the SIGHUP-reload task.
    /// Spawned alongside the listener so a malformed reload can't
    /// block accept(), but tracked here (rather than detached) so
    /// `Drop` / `shutdown_and_join` actually stop it — otherwise the
    /// task lives until runtime shutdown, holds an `Arc<RwLock<…>>`
    /// to the (now-orphaned) map across restarts, and races a fresh
    /// server's SIGHUP handler.
    _sighup_stop: oneshot::Sender<()>,
    sighup_join: Option<JoinHandle<()>>,
    map: Arc<RwLock<Arc<IdMap>>>,
    config_path: PathBuf,
    // The watcher's background task stays alive as long as either
    // `_watcher` or `_watched` holds a sender into its command
    // channel; `_watched` also tears down the specific path watch on
    // drop, which is what we want at Server shutdown. Both are held
    // here so they live exactly as long as the Server does.
    _watcher: Option<Watcher>,
    _watched: Option<Watched>,
}

impl Server {
    /// Start the daemon: bind the socket, parse the config, spawn the
    /// listener loop, install the config-file watcher, and arm SIGHUP.
    /// Returns when the listener is accepting connections.
    pub async fn start(params: ServerParams) -> Result<Server> {
        let initial = load_config(&params.config)
            .with_context(|| format!("loading {:?}", params.config))?;
        let map = Arc::new(RwLock::new(Arc::new(initial)));

        // Probe before unlinking: if something is already listening
        // on this socket, abort rather than silently steal it.
        // Activation supervision already enforces single-instance
        // per unit; this defends against direct `netidx id-map serve`
        // misuse and against a stale path racing a fresh start.
        if params.socket.exists() {
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
        let listener = UnixListener::bind(&params.socket)
            .with_context(|| format!("binding {:?}", params.socket))?;
        set_socket_mode(&params.socket, params.socket_mode).with_context(|| {
            format!("chmod {:o} on {:?}", params.socket_mode, params.socket)
        })?;

        let (stop_tx, stop_rx) = oneshot::channel();
        let map_clone = Arc::clone(&map);
        let config_path = params.config.clone();
        let join = tokio::spawn(run_loop(listener, map_clone, stop_rx));
        // SIGHUP runs in its own task so a malformed config reload
        // can't get stuck and block the accept loop. The JoinHandle
        // and a separate oneshot stop go into `Server` so shutdown
        // signals both tasks and awaits both — without this the
        // SIGHUP task would outlive the Server, hold an Arc to the
        // map that nobody reads, and race a fresh Server's handler.
        let (sighup_stop_tx, sighup_stop_rx) = oneshot::channel();
        let sighup_join = tokio::spawn(sighup_task(
            config_path.clone(),
            Arc::clone(&map),
            sighup_stop_rx,
        ));
        // Filesystem watcher for live reload. A failure to start the
        // watcher logs a warning but doesn't fail the daemon —
        // SIGHUP still works as a manual fallback, and the daemon
        // can serve queries against the initial map even if no
        // reloads ever fire. When the watcher does start, block
        // briefly waiting for its `Established` event so callers
        // (and tests) can rely on the daemon being reload-armed by
        // the time `start` returns.
        let (watcher, watched) =
            match start_config_watcher(config_path.clone(), Arc::clone(&map)) {
                Ok((w, h, ready_rx)) => {
                    match timeout(Duration::from_secs(2), ready_rx).await {
                        Ok(Ok(())) => {
                            debug!("id-map: config-file watcher established");
                        }
                        Ok(Err(_)) => warn!(
                            "id-map: watcher handler exited before establishment; \
                             reloads will not fire"
                        ),
                        Err(_) => warn!(
                            "id-map: config-file watcher did not establish within 2s; \
                             continuing — reloads may be delayed or missed"
                        ),
                    }
                    (Some(w), Some(h))
                }
                Err(e) => {
                    warn!(
                        "id-map: failed to install config-file watcher: {e:#}; \
                         continuing with SIGHUP-only reload"
                    );
                    (None, None)
                }
            };

        info!(
            "id-map daemon ready on {} (config {})",
            params.socket.display(),
            params.config.display(),
        );
        Ok(Server {
            _stop: stop_tx,
            join: Some(join),
            _sighup_stop: sighup_stop_tx,
            sighup_join: Some(sighup_join),
            map,
            config_path,
            _watcher: watcher,
            _watched: watched,
        })
    }

    /// Signal shutdown and block until the listener exits. The
    /// previous `join`-by-self API deadlocked on a live server
    /// because `self` still owned `_stop` while awaiting the listener
    /// task — the `oneshot::Receiver` in `run_loop` would never fire,
    /// so the task would never exit. This method destructures `Self`
    /// up front so `_stop` (and the watcher handles) drop *before* we
    /// start awaiting the listener.
    ///
    /// The only other way to stop the daemon is dropping the `Server`
    /// (same drop order via the struct's implicit `Drop`, just without
    /// the await). Both paths produce the same shutdown sequence; this
    /// one additionally lets the caller observe the listener-task
    /// result.
    pub async fn shutdown_and_join(self) -> Result<()> {
        // Exhaustive destructure — adding a new field to `Server`
        // forces this site to declare what its drop order should be,
        // rather than silently letting it ride along on the implicit
        // end-of-function drop (which is what caused the original
        // deadlock).
        let Self {
            _stop,
            mut join,
            _sighup_stop,
            mut sighup_join,
            _watcher,
            _watched,
            map: _,
            config_path: _,
        } = self;
        // Signal both tasks to exit before awaiting either, so the
        // joins can finish in any order.
        drop(_stop);
        drop(_sighup_stop);
        drop(_watcher);
        drop(_watched);
        if let Some(h) = join.take() {
            h.await.context("id-map listener task panicked")?;
        }
        if let Some(h) = sighup_join.take() {
            h.await.context("id-map SIGHUP task panicked")?;
        }
        Ok(())
    }

    /// Path to the config file the daemon was started with.
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// Snapshot of the currently-live map. Cloning the inner Arc is
    /// cheap — used in tests and for `netidx conf id-map show`.
    pub fn snapshot(&self) -> Arc<IdMap> {
        Arc::clone(&*self.map.read())
    }
}

async fn run_loop(
    listener: UnixListener,
    map: Arc<RwLock<Arc<IdMap>>>,
    stop: oneshot::Receiver<()>,
) {
    let inflight = Arc::new(AtomicUsize::new(0));
    tokio::pin!(stop);
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => {
                debug!("id-map listener shutdown");
                break;
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

async fn handle_one(
    mut client: UnixStream,
    map: Arc<RwLock<Arc<IdMap>>>,
) -> Result<()> {
    let mut buf = Vec::with_capacity(128);
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
    let snapshot = Arc::clone(&*map.read());
    let response = match Query::parse(line) {
        Query::Uid(u) => snapshot.format_id_line_for_uid(u),
        Query::Name(n) => snapshot.format_id_line_for_name(n),
    };
    debug!("id-map response: {}", response.trim_end());
    client.write_all(response.as_bytes()).await?;
    client.shutdown().await?;
    Ok(())
}

/// `extended-notify` event handler that reloads the id-map config
/// whenever the watched path fires a notification. We inspect each
/// event's `Interest` so that the synthetic `Established` event
/// (fired once when the watch is first armed) can resolve a oneshot
/// without triggering a redundant reload — Server::start awaits that
/// oneshot before returning, so the daemon is observably ready
/// instead of relying on a "probably long enough" sleep.
///
/// All other events are reloads. `atomic::write_atomic` (the writer
/// the `netidx conf id-map` tools use) writes to a tempfile then
/// renames over the target; extended-notify catches that via its
/// parent-dir watch + polling, so the rename-induced inode swap
/// doesn't lose us the watch.
#[derive(Clone)]
struct ReloadHandler {
    config_path: PathBuf,
    map: Arc<RwLock<Arc<IdMap>>>,
    // Wrapped in Arc<Mutex<>> because `EventHandler` requires `Clone`
    // (the watcher clones the handler internally) and oneshot senders
    // aren't `Clone`. The mutex lets the first clone-to-fire `take()`
    // the sender; subsequent Established events (after a watch is
    // torn down and re-armed, etc.) find it `None` and skip.
    established_tx: Arc<parking_lot::Mutex<Option<oneshot::Sender<()>>>>,
}

impl EventHandler for ReloadHandler {
    fn handle_event(
        &mut self,
        batch: EventBatch,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        // Classify events synchronously: did we just see the
        // synthetic Established event (signal readiness, skip
        // reload), or a real change (do the reload)? Returning Err
        // from this handler permanently stops the watcher — we never
        // want that — so always return Ok.
        let mut saw_change = false;
        for (_, event) in batch.iter() {
            if let EventKind::Event(Interest::Established) = event.event {
                if let Some(tx) = self.established_tx.lock().take() {
                    let _ = tx.send(());
                }
            } else {
                saw_change = true;
            }
        }
        let path = self.config_path.clone();
        let map = Arc::clone(&self.map);
        async move {
            if !saw_change {
                return Ok(());
            }
            info!(
                "id-map: detected change to {}, reloading",
                path.display()
            );
            match tokio::task::spawn_blocking(move || load_config(&path)).await {
                Ok(Ok(new)) => {
                    *map.write() = Arc::new(new);
                    info!("id-map: reload OK");
                }
                Ok(Err(e)) => warn!(
                    "id-map: reload failed, keeping last-known-good map: {e:#}"
                ),
                Err(e) => warn!("id-map: reload task panicked: {e:#}"),
            }
            Ok(())
        }
    }
}

/// Start the config-file watcher and return both ownership handles
/// plus a oneshot that resolves once the synthetic `Established`
/// event has fired (i.e. the underlying inotify watch is armed and
/// any subsequent edits will be observed). The caller is expected to
/// await the receiver with a timeout — that's the readiness contract
/// `Server::start` exposes.
fn start_config_watcher(
    config_path: PathBuf,
    map: Arc<RwLock<Arc<IdMap>>>,
) -> Result<(Watcher, Watched, oneshot::Receiver<()>)> {
    let (ready_tx, ready_rx) = oneshot::channel();
    let handler = ReloadHandler {
        config_path: config_path.clone(),
        map,
        established_tx: Arc::new(parking_lot::Mutex::new(Some(ready_tx))),
    };
    let watcher = WatcherConfigBuilder::default()
        .event_handler(handler)
        .build()
        .context("building config-file watcher")?
        .start()
        .context("starting config-file watcher")?;
    // Modify covers in-place writes; Create covers the rename-to side
    // of an atomic write (a fresh inode appearing at the path);
    // Delete covers `rm` followed by a fresh write. Established is
    // the synthetic "watch armed" event — the handler signals our
    // readiness oneshot on it and suppresses the reload.
    let interests =
        make_bitflags!(Interest::{Established | Modify | Create | Delete});
    let watched =
        watcher.add(ArcPath::from(config_path), interests).context("adding watch")?;
    Ok((watcher, watched, ready_rx))
}

async fn sighup_task(
    config_path: PathBuf,
    map: Arc<RwLock<Arc<IdMap>>>,
    stop: oneshot::Receiver<()>,
) {
    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            warn!("id-map: failed to install SIGHUP handler: {e}");
            return;
        }
    };
    tokio::pin!(stop);
    loop {
        // Biased: shutdown wins over a coincident SIGHUP, so a
        // race between operator-triggered reload and Server drop
        // doesn't leak one extra reload onto a dying map.
        tokio::select! {
            biased;
            _ = &mut stop => {
                debug!("id-map: SIGHUP task shutdown");
                break;
            }
            s = sighup.recv() => {
                if s.is_none() {
                    // Signal stream closed (e.g. runtime shutdown).
                    break;
                }
                info!(
                    "id-map: SIGHUP received, reloading {}",
                    config_path.display()
                );
                // Move the disk-read + parse off the runtime thread so a
                // slow disk doesn't stall accept(). On a single-threaded
                // runtime the previous `std::fs::read` blocked every other
                // task for the duration of the read.
                let path = config_path.clone();
                let result =
                    tokio::task::spawn_blocking(move || load_config(&path)).await;
                match result {
                    Ok(Ok(new)) => {
                        *map.write() = Arc::new(new);
                        info!("id-map: reload OK");
                    }
                    Ok(Err(e)) => warn!(
                        "id-map: reload failed, keeping last-known-good map: {e:#}"
                    ),
                    Err(e) => warn!("id-map: reload task panicked: {e:#}"),
                }
            }
        }
    }
}

fn load_config(path: &Path) -> Result<IdMap> {
    // Synchronous on purpose: callers from async contexts wrap us in
    // `spawn_blocking`; the synchronous startup path runs before the
    // runtime is doing anything else.
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading id-map config {path:?}"))?;
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
        let cfg = IdMap {
            default_uid: 65534,
            default_gid: 65534,
            groups,
            identities,
        };
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

    /// Edit the config on disk, then poll the daemon's snapshot until
    /// it reflects the new identity (or we hit the 10s ceiling). The
    /// watcher's default debounce timeout is 250ms, so a reload
    /// should arrive within a few hundred ms; the long ceiling is just
    /// to be CI-tolerant on slow filesystems.
    #[tokio::test]
    async fn config_watcher_reloads_on_modify() {
        use std::collections::BTreeMap;
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_sample(&cfg_path);
        let server = Server::start(ServerParams {
            socket: sock_path,
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        // Initial map has alice but not bob. By the time
        // `Server::start` has returned the watcher's `Established`
        // event has fired, so any subsequent edit is guaranteed to
        // be observed — no sleep needed.
        assert!(server.snapshot().lookup_by_name("alice.example.com").is_some());
        assert!(server.snapshot().lookup_by_name("bob.example.com").is_none());

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
        let cfg = IdMap {
            default_uid: 65534,
            default_gid: 65534,
            groups,
            identities,
        };
        let bytes = serde_json::to_vec_pretty(&cfg).unwrap();
        // Atomic write: temp + rename.
        let tmp = cfg_path.with_extension("json.tmp");
        std::fs::write(&tmp, &bytes).unwrap();
        std::fs::rename(&tmp, &cfg_path).unwrap();

        // Poll for up to ~10s.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if server.snapshot().lookup_by_name("bob.example.com").is_some() {
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "watcher did not reload within 10s; snapshot identities: {:?}",
                    server
                        .snapshot()
                        .identities
                        .keys()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>(),
                );
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
