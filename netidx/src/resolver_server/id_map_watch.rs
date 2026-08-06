//! Holding a line open to the id-map daemon so a revoked group stops being
//! honoured promptly.
//!
//! The resolver caches each identity's group membership, because asking costs
//! a round trip and most identities' groups never change. That cache is what
//! stands between an administrator revoking someone's group and the resolver
//! enforcing it: until the entry expires, the old membership is still the one
//! being used, and neither the operator nor `netidx admin drift` can see that
//! — both can see the map arrive at the host, and arriving is not the same as
//! being enforced.
//!
//! So the daemon says when its map changed rather than the resolver guessing
//! how long to hold an answer. This module keeps a connection open to
//! [`id_map_control_socket`](netidx_core::utils::id_map_control_socket) and
//! flushes the cache on every line that arrives — and on every successful
//! connect, which is what covers the invalidations published while we were not
//! connected.
//!
//! Everything about it degrades to the plain timeout. A daemon too old to have
//! the socket, one that could not bind it, one that is restarting: the connect
//! fails, we retry quietly, and the cache expires the way it always did.

use super::secctx::SecCtx;
use futures::channel::oneshot;
use log::{debug, info, warn};
use std::{path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixStream,
    task,
};

/// How long to wait before reconnecting.
///
/// Flat, and unhurried. The socket's absence is the expected steady state for
/// an old daemon, so this is mostly the rate at which we quietly discover that
/// nothing has changed. Reconnecting fast would buy nothing — a reconnect
/// flushes the cache, so the cost of being late is bounded by the cache
/// timeout, which is the behaviour we are improving on rather than relying on.
const RECONNECT: Duration = Duration::from_secs(10);

/// Watch for invalidations until the returned sender is dropped or fired.
///
/// The caller keeps the sender alongside the connection stops it already
/// drains at shutdown, so this task ends with the server rather than outliving
/// it.
pub(super) fn spawn(socket: PathBuf, secctx: SecCtx) -> oneshot::Sender<()> {
    let (tx, rx) = oneshot::channel();
    task::spawn(async move {
        tokio::select! {
            _ = rx => debug!("id-map invalidation watch stopping"),
            _ = watch(socket, secctx) => (),
        }
    });
    tx
}

async fn watch(socket: PathBuf, secctx: SecCtx) {
    let mut connected_before = false;
    loop {
        match UnixStream::connect(&socket).await {
            Ok(stream) => {
                info!(
                    "id-map: subscribed to cache invalidations on {}",
                    socket.display()
                );
                connected_before = true;
                // On connect as well as on each message: while we were away
                // the map may have changed, and the daemon does not replay.
                secctx.flush_user_cache().await;
                let mut lines = BufReader::new(stream).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => {
                            debug!("id-map: {line} — flushing the group cache");
                            secctx.flush_user_cache().await;
                        }
                        Ok(None) => {
                            warn!("id-map: invalidation stream closed, reconnecting");
                            break;
                        }
                        Err(e) => {
                            warn!("id-map: invalidation stream failed: {e}");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                // Quiet on the first failures: an id-map daemon without this
                // socket is a supported configuration, not a fault, and the
                // resolver works exactly as it did before. Only say something
                // if we had it and lost it.
                if connected_before {
                    warn!(
                        "id-map: lost the invalidation socket {}: {e}",
                        socket.display()
                    );
                    connected_before = false;
                } else {
                    debug!(
                        "id-map: no invalidation socket at {} ({e}); the group cache \
                         will expire on its timeout",
                        socket.display()
                    );
                }
            }
        }
        tokio::time::sleep(RECONNECT).await;
    }
}
