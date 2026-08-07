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
//! Every `N`/`M`/`G` above is [`crate::file::PLACEHOLDER_ID`] and means
//! nothing — the resolver reads only the parenthesized names. The map holds no
//! numbers to put there; see the [`crate::file`] docs.
//!
//! The current [`crate::file::IdMap`] lives as a plain `Arc<IdMap>`
//! local to [`run_loop`]; each accepted connection is handed a clone of
//! it, and a reload just rebinds that local to a fresh `Arc`. There is
//! no shared lock — only `run_loop` ever mutates the binding, and it
//! does so between `select` branches. Two event sources, both handled
//! as extra arms of that one `select`, can trigger a re-read:
//!
//! - The config poll: every [`POLL_INTERVAL`] the loop compares the file's
//!   modification time against the one it recorded for what it is serving.
//!   This is the normal path — operators editing `id-map.json` directly or
//!   running `netidx admin id-map …` get reload-for-free. A poll rather than a
//!   filesystem watch for one small file; see [`POLL_INTERVAL`] for why.
//! - `SIGHUP` is kept as a manual override for an operator who doesn't want to
//!   wait out an interval.
//!
//! Either way, parse failures keep the last-known-good map in place so a
//! botched edit can't lock out every TLS identity. The only tasks this module
//! spawns are `run_loop` and one short-lived task per accepted connection.
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
use log::{debug, info, warn};
use poolshark::local::LPooled;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    signal::unix::{Signal, SignalKind, signal},
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

/// Max resolvers subscribed to cache invalidations at once.
///
/// One host runs one resolver, so this is only a bound on a local peer that
/// opens connections and never reads. Past it new subscribers are refused,
/// which costs them the invalidations and nothing else.
const MAX_CONTROL_SUBSCRIBERS: usize = 16;

/// How long one subscriber may take to accept an invalidation before it is
/// disconnected.
///
/// Publishing happens inside the main `select`, so every millisecond spent
/// waiting on a subscriber is a millisecond the daemon answers no queries and
/// notices no reload. A local socket takes a twenty byte line without waiting
/// unless its peer has stopped reading entirely, so a subscriber that misses
/// this is wedged rather than slow, and waiting on it longer would only spread
/// its problem to everyone else. Per subscriber rather than per round so that
/// one wedged subscriber cannot cost a healthy one its line; a round is
/// therefore bounded by [`MAX_CONTROL_SUBSCRIBERS`] times this.
///
/// Disconnecting costs the subscriber nothing it does not get back: the
/// resolver flushes its cache when it connects, so the reconnect delivers what
/// the dropped write was carrying.
const PUBLISH_TIMEOUT: Duration = Duration::from_millis(100);

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
    /// re-parsed whenever its modification time changes (checked every
    /// [`POLL_INTERVAL`]) or `SIGHUP` is received.
    pub config: PathBuf,
}

impl ServerParams {
    pub fn new(socket: PathBuf, config: PathBuf) -> Self {
        Self { socket, socket_mode: 0o600, config }
    }
}

/// A running id-map daemon. Drop the server to shut it down (the main
/// loop receives a oneshot and exits; in-flight requests continue
/// until their timeout).
pub struct Server {
    _stop: oneshot::Sender<()>,
    join: Option<JoinHandle<()>>,
}

impl Server {
    /// Start the daemon: bind the socket, parse the config, and spawn the
    /// main loop (which polls the config and arms SIGHUP). Returns when the
    /// listener is accepting connections.
    pub async fn start(params: ServerParams) -> Result<Server> {
        let (initial, stamp) = load_config(&params.config)
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

        // The invalidation socket is best-effort: without it a resolver falls
        // back to expiring its cache on the timeout, which is what every
        // resolver did before this existed. So a failure here is a warning,
        // never a refusal to start — the daemon's job is answering queries.
        let control_path = netidx_core::utils::id_map_control_socket(&params.socket);
        let control = match bind_control(&control_path, params.socket_mode).await {
            Ok(l) => Some(l),
            Err(e) => {
                warn!(
                    "id-map: not publishing cache invalidations on {}: {e:#} — \
                     resolvers will expire their caches on the timeout instead",
                    control_path.display()
                );
                None
            }
        };
        let (stop_tx, stop_rx) = oneshot::channel();
        let config_path = params.config.clone();
        let join =
            tokio::spawn(run_loop(listener, control, config_path, map, stamp, stop_rx));

        info!(
            "id-map daemon ready on {} (config {})",
            params.socket.display(),
            params.config.display(),
        );
        Ok(Server { _stop: stop_tx, join: Some(join) })
    }

    /// Signal shutdown and block until the main loop exits. The
    /// previous `join`-by-self API deadlocked on a live server
    /// because `self` still owned `_stop` while awaiting the loop
    /// task — the `oneshot::Receiver` in `run_loop` would never fire,
    /// so the task would never exit. This method destructures `Self`
    /// up front so `_stop` drops *before* we start awaiting the loop.
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
        let Self { _stop, mut join } = self;
        // Drop the stop sender before awaiting the join, so the main
        // loop's `select` sees the closed oneshot and breaks rather
        // than blocking here forever.
        drop(_stop);
        if let Some(h) = join.take() {
            h.await.context("id-map main loop task panicked")?;
        }
        Ok(())
    }
}

/// The daemon's single long-lived task. It owns the current map,
/// serves queries by handing each accepted connection a clone of it,
/// and folds both reload triggers — SIGHUP and the config poll — into
/// the same `select` so a reload is just a between-branches rebind of
/// the local `map`. The only other tasks are the per-connection
/// handlers it spawns.
async fn run_loop(
    listener: UnixListener,
    control: Option<UnixListener>,
    config_path: PathBuf,
    mut map: Arc<IdMap>,
    mut stamp: Option<SystemTime>,
    stop: oneshot::Receiver<()>,
) {
    let inflight = Arc::new(AtomicUsize::new(0));
    // Resolvers listening for "the map changed". Held right here in the loop
    // for the same reason the map is: only this loop touches them, so there is
    // nothing to lock. `generation` is for the logs at both ends — a resolver
    // flushes on any invalidation, so nothing depends on its ordering.
    let mut subscribers: Vec<UnixStream> = Vec::new();
    let mut generation: u64 = 0;
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
    // An interval, not a fresh `sleep` per iteration: this `select` also has a
    // busy arm (`accept`), and a timer rebuilt each time round would be
    // cancelled by every query, so a daemon anyone was actually using would
    // never get as far as re-reading its config.
    let mut poll = tokio::time::interval_at(
        tokio::time::Instant::now() + POLL_INTERVAL,
        POLL_INTERVAL,
    );
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                if let Some(m) = reload(&config_path, &mut map).await {
                    stamp = Some(m);
                    generation += 1;
                    publish_invalidation(&mut subscribers, generation).await;
                }
            }
            client = accept_control(&control) => {
                if subscribers.len() >= MAX_CONTROL_SUBSCRIBERS {
                    warn!(
                        "id-map: refusing an invalidation subscriber, already at \
                         {MAX_CONTROL_SUBSCRIBERS}"
                    );
                } else {
                    debug!("id-map: a resolver subscribed to cache invalidations");
                    subscribers.push(client);
                }
            }
            // Live reload from the config poll.
            _ = poll.tick() => {
                let latest = mtime(&config_path).await;
                if latest != stamp {
                    info!(
                        "id-map: detected change to {}, reloading",
                        config_path.display()
                    );
                    // From the descriptor the new map was read from, not from
                    // `latest`: a write between the stat and the read would
                    // otherwise be remembered as already applied. A failed
                    // reload records `latest` so a config that cannot be
                    // parsed is retried when it changes, not every tick.
                    let reloaded = reload(&config_path, &mut map).await;
                    // Only a reload that actually replaced the map is worth
                    // telling anyone about; a failed one kept the last-known-
                    // good map, so nothing a resolver has cached is stale.
                    if reloaded.is_some() {
                        generation += 1;
                        publish_invalidation(&mut subscribers, generation).await;
                    }
                    stamp = reloaded.or(latest);
                }
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
/// On success also returns the time of the descriptor the new map was read
/// from, for the caller to record.
/// Bind the invalidation socket.
///
/// No occupied-socket probe: the query socket's bind already established that
/// no other daemon holds this pair, so anything left here is ours from a
/// previous run. As there it is removed only if it is actually a socket —
/// a regular file or a symlink at this path is operator misconfiguration and
/// unlinking it would be the wrong repair.
async fn bind_control(path: &Path, mode: u32) -> Result<UnixListener> {
    use std::os::unix::fs::FileTypeExt;
    match tokio::fs::symlink_metadata(path).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("stat {path:?} before bind")),
        Ok(md) if md.file_type().is_socket() => {
            let _ = tokio::fs::remove_file(path).await;
        }
        Ok(md) => bail!(
            "{path:?} exists but is not a unix socket (file type: {:?})",
            md.file_type()
        ),
    }
    let listener =
        UnixListener::bind(path).with_context(|| format!("binding {path:?}"))?;
    set_socket_mode(path, mode).with_context(|| format!("chmod {mode:o} on {path:?}"))?;
    Ok(listener)
}

/// Accept an invalidation subscriber, or never resolve when there is no
/// invalidation socket.
///
/// Accept errors are retried in here rather than returned: this is one arm of
/// the main `select`, and returning would take a transient failure and turn it
/// into "no invalidations for the life of the process".
async fn accept_control(listener: &Option<UnixListener>) -> UnixStream {
    match listener {
        None => std::future::pending().await,
        Some(l) => loop {
            match l.accept().await {
                Ok((client, _)) => return client,
                Err(e) => {
                    warn!("id-map: invalidation accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        },
    }
}

/// Tell every subscribed resolver that the map has changed.
///
/// One line per reload, and nothing is replayed: a resolver that has gone away
/// fails the write and is dropped, and when it reconnects it flushes its cache
/// on connect anyway. So a missed invalidation costs a reconnect, not
/// correctness.
///
/// A subscriber that does not accept its line within [`PUBLISH_TIMEOUT`] is
/// dropped for the same reason and at the same cost. Without that the daemon
/// would stop serving queries for as long as its slowest subscriber took to
/// read — one wedged resolver would take the id-map down for everyone.
async fn publish_invalidation(subscribers: &mut Vec<UnixStream>, generation: u64) {
    if subscribers.is_empty() {
        return;
    }
    let line = compact_str::format_compact!("invalidate {generation}\n");
    let mut i = 0;
    while i < subscribers.len() {
        match timeout(PUBLISH_TIMEOUT, subscribers[i].write_all(line.as_bytes())).await {
            Ok(Ok(())) => i += 1,
            Ok(Err(e)) => {
                debug!("id-map: dropping an invalidation subscriber: {e}");
                subscribers.swap_remove(i);
            }
            Err(_) => {
                warn!("id-map: dropping an invalidation subscriber that stopped reading");
                subscribers.swap_remove(i);
            }
        }
    }
    debug!(
        "id-map: published invalidation {generation} to {} subscriber(s)",
        subscribers.len()
    );
}

async fn reload(config_path: &Path, map: &mut Arc<IdMap>) -> Option<SystemTime> {
    let path = config_path.to_path_buf();
    match tokio::task::spawn_blocking(move || load_config(&path)).await {
        Ok(Ok((new, mtime))) => {
            *map = Arc::new(new);
            info!("id-map: reload OK");
            mtime
        }
        Ok(Err(e)) => {
            warn!("id-map: reload failed, keeping last-known-good map: {e:#}");
            None
        }
        Err(e) => {
            warn!("id-map: reload task panicked: {e:#}");
            None
        }
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

/// How often the config's modification time is checked. The same interval the
/// resolver polls its own config on, and a netidx client its own.
///
/// A poll rather than a filesystem watch, for one small file. A watch has to
/// follow the path onto a fresh inode every time the file is replaced — and
/// every tool that writes it replaces it by renaming a new file over the top —
/// and when it stops doing that there is nothing to notice the silence. The
/// resolver's watch did exactly that in the lab, applying the first change
/// pushed to it and ignoring every one after.
///
/// The cost is up to 30 seconds of staleness after an enrollment, during which
/// a newly registered identity is not yet mapped. The resolver's own retry
/// covers that: an unmapped identity is refused, not mismapped.
#[cfg(not(test))]
const POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Short enough that the reload tests don't wait out a real interval.
#[cfg(test)]
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The config's modification time, or `None` if it isn't there — so deleting
/// it reads as a change, and recreating it reads as another.
async fn mtime(path: &Path) -> Option<SystemTime> {
    tokio::fs::metadata(path).await.ok()?.modified().ok()
}

/// Parse the config, and report the modification time of the descriptor it was
/// read from.
///
/// From the descriptor, not from the path: asking the path again afterwards
/// leaves a window for a write to land in between, so you hold one version and
/// remember another — and if what you remember matches the file, the poll never
/// reloads again. Every writer replaces this file by renaming a new one over
/// the top, so the inode behind an open descriptor is never modified underneath
/// us and the time it reports belongs to the bytes we parsed.
fn load_config(path: &Path) -> Result<(IdMap, Option<SystemTime>)> {
    use std::io::Read;
    // Synchronous on purpose: callers from async contexts wrap us in
    // `spawn_blocking`; the synchronous startup path runs before the
    // runtime is doing anything else.
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("opening id-map config {path:?}"))?;
    let mtime = file.metadata().ok().and_then(|md| md.modified().ok());
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("reading id-map config {path:?}"))?;
    Ok((file::parse_bytes(&bytes)?, mtime))
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
    use crate::file::{Identity, PLACEHOLDER_ID};
    use arcstr::ArcStr;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Write `map` to `path` the way every real writer of this file does: a
    /// sibling temp file renamed over the top, then a settle past one
    /// filesystem timestamp tick.
    ///
    /// The settle is not scaffolding, it is half of how this file is followed.
    /// Linux stamps inodes from a clock that only advances once per timer tick,
    /// so two writes closer together than that carry the same modification
    /// time — and the daemon, which notices a change by comparing that time
    /// against what it last read, never sees the second one. Ever: the stamp it
    /// holds already matches the file. `netidx-admin`'s atomic write is where
    /// production pays this; a test that skipped it would be exercising a
    /// writer that no netidx tool is, and failing on the daemon's behalf.
    pub(super) fn write_config(path: &Path, map: &IdMap) {
        let bytes = serde_json::to_vec_pretty(map).unwrap();
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &bytes).unwrap();
        std::fs::rename(&tmp, path).unwrap();
        std::thread::sleep(netidx_core::utils::FS_TIMESTAMP_SETTLE);
    }

    /// A map whose only group is `users`, with one identity per name in it.
    pub(super) fn map_of(names: &[&str]) -> IdMap {
        let identities = names
            .iter()
            .map(|name| {
                (
                    ArcStr::from(*name),
                    Identity { primary_group: ArcStr::from("users"), groups: vec![] },
                )
            })
            .collect();
        IdMap {
            default_group: None,
            groups: [ArcStr::from("users")].into_iter().collect(),
            identities,
        }
    }

    /// Whether the daemon knows this identity. Group membership is the whole
    /// answer now — an identity in the map resolves to its groups, one that
    /// isn't gets `nogroup`, and the numbers beside them are the same
    /// placeholder either way.
    fn is_known(line: &str) -> bool {
        assert!(!line.contains("nogroup") || !line.contains("(users)"));
        line.contains("(users)")
    }

    fn write_sample(path: &Path) {
        let mut identities = BTreeMap::new();
        identities.insert(
            ArcStr::from("alice.example.com"),
            Identity {
                primary_group: ArcStr::from("users"),
                groups: vec![ArcStr::from("wheel")],
            },
        );
        write_config(
            path,
            &IdMap {
                default_group: None,
                groups: ["users", "wheel"].into_iter().map(ArcStr::from).collect(),
                identities,
            },
        );
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
        let id = PLACEHOLDER_ID;
        assert!(s.contains(&format!("uid={id}(alice.example.com)")), "got {s:?}");
        assert!(s.contains(&format!("gid={id}(users)")), "got {s:?}");
        assert!(s.contains(&format!("groups={id}(users),{id}(wheel)")), "got {s:?}");
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
        // Nothing to reverse: the map holds no uids. The daemon answers with
        // the defaults rather than guessing, which is what makes `auth: Local`
        // with `id_map_type: Socket` fail closed instead of naming the wrong
        // identity. See the `file` module docs.
        let s = query(&sock_path, "1000").await;
        assert!(!s.contains("alice"), "a uid must not resolve to a name: {s:?}");
        assert!(s.contains("(nogroup)"), "got {s:?}");
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
        let id = PLACEHOLDER_ID;
        assert!(s.contains(&format!("uid={id}(ghost.example.com)")), "got {s:?}");
        assert!(s.contains("(nogroup)"), "got {s:?}");
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

    /// Edit the config on disk, then poll the daemon *through the socket*
    /// until it reflects the new identity (or we hit the 10s ceiling).
    /// Querying the live interface, rather than peeking at internal state, is
    /// the real end-to-end check that a reload reached the path that serves
    /// clients. A reload should arrive within a poll interval; the long
    /// ceiling is just to be CI-tolerant on slow filesystems.
    #[tokio::test]
    async fn config_watcher_reloads_on_modify() {
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
        // Initial map has alice but not bob.
        assert!(is_known(&query(&sock_path, "alice.example.com").await));
        assert!(!is_known(&query(&sock_path, "bob.example.com").await));

        write_config(&cfg_path, &map_of(&["alice.example.com", "bob.example.com"]));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let s = query(&sock_path, "bob.example.com").await;
            if is_known(&s) {
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!("daemon did not reload within 10s; last bob query: {s:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Regression: the daemon must keep picking up *repeated* replacements of
    /// its config, not just the first. Every `id-map.json` update (enrollment,
    /// `netidx admin id-map …`) renames a sibling temp file over the target — a
    /// fresh inode each time — and the daemon has to notice all of them, or the
    /// first enrollment registers and every one after it silently resolves to
    /// no group (denied). `config_watcher_reloads_on_modify` does a single
    /// replace, so it passed while an earlier version of this bug was live.
    #[tokio::test]
    async fn config_watcher_reloads_on_repeated_modify() {
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
        // Add identities one at a time, polling the live socket for each before
        // moving on, so every replacement has to be noticed on its own.
        let hosts: Vec<String> = ["bob", "carol", "dave", "erin", "frank"]
            .iter()
            .map(|name| format!("{name}.example.com"))
            .collect();
        let mut in_map = vec!["alice.example.com"];
        for (i, host) in hosts.iter().enumerate() {
            in_map.push(host.as_str());
            write_config(&cfg_path, &map_of(&in_map));

            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                let s = query(&sock_path, &host).await;
                if is_known(&s) {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "replacement #{} ({host}) was never observed — the daemon \
                         stopped noticing changes to its config. last query: {s:?}",
                        i + 1,
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
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

#[cfg(test)]
mod invalidation_tests {
    use super::tests::{map_of, write_config};
    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader};

    /// A reload is published to every subscribed resolver.
    ///
    /// This is what lets the resolver's group cache be long: it does not have
    /// to be short enough to notice a change, because it is told.
    #[tokio::test]
    async fn a_reload_is_published_to_subscribers() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_config(&cfg_path, &map_of(&["alice.example.com"]));
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let control = netidx_core::utils::id_map_control_socket(&sock_path);
        let mut lines =
            BufReader::new(UnixStream::connect(&control).await.unwrap()).lines();

        write_config(&cfg_path, &map_of(&["alice.example.com", "bob.example.com"]));
        let line = timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("an invalidation should arrive within a poll interval")
            .unwrap()
            .unwrap();
        assert!(line.starts_with("invalidate "), "got {line:?}");

        // Every reload, not just the first. The resolver's watch died after
        // one notification in an earlier iteration of a different watcher in
        // this codebase, and nothing noticed for months.
        write_config(&cfg_path, &map_of(&["alice.example.com"]));
        let second = timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("the second change must be published too")
            .unwrap()
            .unwrap();
        assert!(second.starts_with("invalidate "), "got {second:?}");
        assert_ne!(line, second, "the generation should move");
    }

    /// A reload that failed to parse kept the last-known-good map, so nothing
    /// a resolver holds has gone stale and there is nothing to announce.
    #[tokio::test]
    async fn a_failed_reload_is_not_published() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_config(&cfg_path, &map_of(&["alice.example.com"]));
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        let control = netidx_core::utils::id_map_control_socket(&sock_path);
        let mut lines =
            BufReader::new(UnixStream::connect(&control).await.unwrap()).lines();
        std::fs::write(&cfg_path, b"{ not json").unwrap();
        std::thread::sleep(netidx_core::utils::FS_TIMESTAMP_SETTLE);
        assert!(
            timeout(Duration::from_secs(2), lines.next_line()).await.is_err(),
            "a map that could not be parsed was never adopted, so nothing changed"
        );
        // And the daemon is still answering from the good map.
        let mut s = UnixStream::connect(&sock_path).await.unwrap();
        s.write_all(b"alice.example.com\n").await.unwrap();
        s.shutdown().await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        assert!(out.contains("(alice.example.com)"), "got {out:?}");
    }

    /// A subscriber that stops reading is disconnected, not waited on.
    ///
    /// Publishing runs in the daemon's one long lived task, so a write that
    /// waits on a wedged subscriber is a daemon that answers nothing — every
    /// query and every reload, for as long as that subscriber takes.
    #[tokio::test]
    async fn a_subscriber_that_stops_reading_is_dropped() {
        // Kept alive and never read, so the socket buffer fills and the write
        // that overflows it has nowhere to go.
        let (subscriber, _wedged) = UnixStream::pair().unwrap();
        let mut subscribers = vec![subscriber];
        let mut generation = 0;
        let start = std::time::Instant::now();
        while !subscribers.is_empty() {
            generation += 1;
            assert!(
                generation < 1_000_000,
                "the buffer never filled, so no write ever had to wait"
            );
            timeout(
                Duration::from_secs(5),
                publish_invalidation(&mut subscribers, generation),
            )
            .await
            .expect("a publish waited on a subscriber that will never read");
        }
        assert!(
            start.elapsed() >= PUBLISH_TIMEOUT,
            "dropped in {:?} — that was an error, not the timeout under test",
            start.elapsed()
        );
    }

    /// The daemon serves queries whether or not anyone is listening for
    /// invalidations — an old resolver never connects, and that is a supported
    /// configuration rather than a degraded one.
    #[tokio::test]
    async fn queries_work_with_no_subscriber() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("id-map.json");
        let sock_path = dir.path().join("id-map.sock");
        write_config(&cfg_path, &map_of(&["alice.example.com"]));
        let _server = Server::start(ServerParams {
            socket: sock_path.clone(),
            socket_mode: 0o600,
            config: cfg_path.clone(),
        })
        .await
        .unwrap();
        write_config(&cfg_path, &map_of(&["alice.example.com", "bob.example.com"]));
        tokio::time::sleep(Duration::from_millis(400)).await;
        let mut s = UnixStream::connect(&sock_path).await.unwrap();
        s.write_all(b"bob.example.com\n").await.unwrap();
        s.shutdown().await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        assert!(out.contains("(bob.example.com)"), "got {out:?}");
    }
}
