//! Local control socket for the activation supervisor.
//!
//! A small length-prefixed-JSON protocol over a unix-domain socket
//! (`control.sock`, mode 0600) in the activation unit directory. The conf
//! server — running on the same host, as the same user (or root) — connects
//! to it to drive a unit's start / stop / restart / status on a role admin's
//! behalf. The conf plane carries the authentication and RBAC; this socket
//! is purely the local, owner-only trust boundary (kernel-enforced by the
//! 0600 mode, plus a `SO_PEERCRED` uid check).
//!
//! The message types and framing are cross-platform — a Windows conf client
//! carries them in the conf-plane wire protocol when it asks a unix conf
//! server to control a service. Only [`control`] itself (the local
//! unix-socket connect) is `#[cfg(unix)]`.

use anyhow::{bail, Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// File name of the control socket within the activation unit directory.
pub const SOCKET_FILE: &str = "control.sock";

/// Cap on a control message (these are tiny; the bound just stops a
/// misbehaving peer from forcing a huge allocation).
const MAX_MSG: usize = 1 << 20;

/// What to do to a unit. `Status` only reports; the others act, then report
/// the resulting state. An explicit `Restart` force-cycles the process
/// regardless of the unit's crash-restart policy (that policy governs
/// *crash* behavior, not an operator action); `Stop` latches the unit
/// stopped so it is not auto-restarted until an explicit `Start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlOp {
    Start,
    Stop,
    Restart,
    Status,
}

/// A unit's runtime state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnitState {
    /// Defined but never started (e.g. an OnAccess unit awaiting access).
    NotStarted,
    /// Running, with its pid if we can read it.
    Running { pid: Option<u32> },
    /// Explicitly stopped — will not auto-restart until started.
    Stopped,
    /// The process exited on its own and was not (yet) restarted.
    Died,
}

/// One unit's name + state, as reported by the supervisor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitStatus {
    pub unit: String,
    pub state: UnitState,
}

/// Apply `op` to each named unit. An empty `units` means *every* unit. Unit
/// names match the on-disk file basenames; a trailing `.unit` is optional
/// (`resolver` matches `resolver.unit`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlRequest {
    pub op: ControlOp,
    pub units: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    Ok { units: Vec<UnitStatus> },
    Err { reason: String },
}

/// The control socket path for a supervisor whose unit directory is
/// `units_dir` — `<units_dir>/control.sock`. It sits beside the `*.unit`
/// files (the loader only reads `*.unit`, so there is no collision), and
/// both the supervisor and the conf server resolve it the same way.
pub fn socket_path(units_dir: &Path) -> PathBuf {
    units_dir.join(SOCKET_FILE)
}

/// Connect to a local activation control socket and run one request.
#[cfg(unix)]
pub async fn control(socket: &Path, req: &ControlRequest) -> Result<ControlResponse> {
    let mut s = tokio::net::UnixStream::connect(socket).await.with_context(|| {
        format!("connecting to the activation control socket {}", socket.display())
    })?;
    write_msg(&mut s, req).await?;
    read_msg(&mut s).await
}

/// Write a length-prefixed (4-byte big-endian) JSON message and flush.
pub async fn write_msg<S, T>(s: &mut S, m: &T) -> Result<()>
where
    S: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    let body = serde_json::to_vec(m).context("serializing control message")?;
    if body.len() > MAX_MSG {
        bail!("control message too large ({} bytes)", body.len());
    }
    s.write_all(&(body.len() as u32).to_be_bytes()).await.context("writing length")?;
    s.write_all(&body).await.context("writing body")?;
    s.flush().await.context("flushing")?;
    Ok(())
}

/// Read a length-prefixed JSON message.
pub async fn read_msg<S, T>(s: &mut S) -> Result<T>
where
    S: AsyncReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len = [0u8; 4];
    s.read_exact(&mut len).await.context("reading length")?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_MSG {
        bail!("incoming control message too large ({len} bytes)");
    }
    let mut body = vec![0u8; len];
    s.read_exact(&mut body).await.context("reading body")?;
    serde_json::from_slice(&body).context("deserializing control message")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn framing_round_trips_request_and_response() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let req =
            ControlRequest { op: ControlOp::Restart, units: vec!["resolver".to_string()] };
        let resp = ControlResponse::Ok {
            units: vec![UnitStatus {
                unit: "resolver".to_string(),
                state: UnitState::Running { pid: Some(42) },
            }],
        };
        let (r2, p2) = (req.clone(), resp.clone());
        let h = tokio::spawn(async move {
            write_msg(&mut a, &r2).await.unwrap();
            let got: ControlResponse = read_msg(&mut a).await.unwrap();
            assert!(matches!(got, ControlResponse::Err { .. }));
            write_msg(&mut a, &p2).await.unwrap();
        });
        let got_req: ControlRequest = read_msg(&mut b).await.unwrap();
        assert_eq!(got_req.op, ControlOp::Restart);
        assert_eq!(got_req.units, vec!["resolver".to_string()]);
        write_msg(&mut b, &ControlResponse::Err { reason: "x".to_string() }).await.unwrap();
        let got_resp: ControlResponse = read_msg(&mut b).await.unwrap();
        match got_resp {
            ControlResponse::Ok { units } => {
                assert_eq!(units[0].unit, "resolver");
                assert_eq!(units[0].state, UnitState::Running { pid: Some(42) });
            }
            ControlResponse::Err { .. } => panic!("expected Ok"),
        }
        h.await.unwrap();
    }

    #[test]
    fn socket_path_is_under_the_unit_dir() {
        let p = socket_path(std::path::Path::new("/etc/netidx/activation"));
        assert_eq!(p, std::path::Path::new("/etc/netidx/activation/control.sock"));
    }
}
