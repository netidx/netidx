//! Wire protocol for the CA server: small request/response messages
//! exchanged over a TLS stream, length-prefixed JSON.
//!
//! This is a control path — one round trip per node join — not a data
//! path, so JSON framing (4-byte big-endian length + body) is plenty
//! and keeps the protocol dependency-light and human-debuggable. The
//! types are cross-platform: a Windows node speaks this to a unix CA.

use anyhow::{bail, Context, Result};
use serde_derive::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 1;

/// Conventional CA-server port (resolver is 4564).
pub const DEFAULT_PORT: u16 = 4565;

/// Reserved DNS SAN of the CA server's TLS *serving* certificate. The
/// join client requires the presented serving cert to carry exactly
/// this name and to be signed by the fingerprint-confirmed CA — that's
/// what distinguishes the daemon from any other node the same CA has
/// issued a cert to. Issuance policy must never grant this name to a
/// normal join.
pub const SERVING_SAN: &str = "netidx-ca-server";

/// Bodies larger than this are refused before allocation — CSRs and
/// certs are a few KB; this is a generous backstop against a hostile or
/// confused peer.
const MAX_MSG: u32 = 1 << 20; // 1 MiB

/// What kind of node is joining. Informational — the server logs it; it
/// doesn't change issuance policy (that's per-admin, by SAN).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeKind {
    Resolver,
    Publisher,
    Client,
    Workstation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHello {
    pub protocol_version: u32,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHello {
    pub protocol_version: u32,
}

/// A secret string that never appears in `Debug` output and is zeroized
/// on drop. Serializes as a bare string.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(pub String);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.0.zeroize();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignRequest {
    /// Which admin's password follows — selects the keyslot and its
    /// issuance policy.
    pub admin: String,
    pub password: Secret,
    pub csr_pem: String,
    /// The DNS name the leaf should carry as its single SAN (netidx
    /// pins identities to exactly one DNS SAN). The CA overrides
    /// whatever the CSR claims, so this is the authoritative request.
    pub requested_name: String,
    pub requested_validity_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SignResponse {
    Ok {
        signed_cert_pem: String,
        /// The full set of trusted CA certs the joining node should
        /// install as its trust anchor — a PEM bundle that always
        /// includes the issuing CA and may include additional
        /// (e.g. federated) CAs. The client installs this verbatim, so
        /// a join needs no manual file copying at all.
        trusted_pem: String,
    },
    Err {
        reason: String,
    },
}

/// Write a length-prefixed JSON message and flush.
pub async fn write_msg<S, T>(stream: &mut S, msg: &T) -> Result<()>
where
    S: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    let body = serde_json::to_vec(msg).context("serializing message")?;
    if body.len() as u64 > MAX_MSG as u64 {
        bail!("outgoing message too large ({} bytes)", body.len());
    }
    stream
        .write_all(&(body.len() as u32).to_be_bytes())
        .await
        .context("writing length prefix")?;
    stream.write_all(&body).await.context("writing message body")?;
    stream.flush().await.context("flushing message")?;
    Ok(())
}

/// Read a length-prefixed JSON message.
pub async fn read_msg<S, T>(stream: &mut S) -> Result<T>
where
    S: AsyncReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.context("reading length prefix")?;
    let len = u32::from_be_bytes(len);
    if len > MAX_MSG {
        bail!("incoming message length {len} exceeds maximum {MAX_MSG}");
    }
    let mut body = vec![0u8; len as usize];
    stream.read_exact(&mut body).await.context("reading message body")?;
    serde_json::from_slice(&body).context("deserializing message")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_is_redacted_but_serializes_plainly() {
        let s = Secret("hunter2".to_string());
        assert_eq!(format!("{s:?}"), "Secret(***)");
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"hunter2\"");
        let back: Secret = serde_json::from_str("\"hunter2\"").unwrap();
        assert_eq!(back.0, "hunter2");
    }

    #[tokio::test]
    async fn round_trips_over_a_pipe() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let req = SignRequest {
            admin: "alice".to_string(),
            password: Secret("pw".to_string()),
            csr_pem: "CSR".to_string(),
            requested_name: "resolver.example.com".to_string(),
            requested_validity_days: 365,
        };
        write_msg(&mut a, &req).await.unwrap();
        let got: SignRequest = read_msg(&mut b).await.unwrap();
        assert_eq!(got.admin, "alice");
        assert_eq!(got.password.0, "pw");
        assert_eq!(got.requested_name, "resolver.example.com");
    }
}
