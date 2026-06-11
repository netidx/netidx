//! Wire protocol for the conf server: small request/response messages
//! exchanged over a TLS stream, length-prefixed JSON.
//!
//! This is a control path — a handful of round trips per node setup —
//! not a data path, so JSON framing (4-byte big-endian length + body)
//! is plenty and keeps the protocol dependency-light and
//! human-debuggable. The types are cross-platform: a Windows node
//! speaks this to a unix conf server.
//!
//! A connection is: TLS accept, [`ClientHello`] / [`ServerHello`]
//! exchange, then exactly **one** [`Request`] and its response. One
//! request per connection keeps human think-time (fingerprint
//! confirmation, password entry) from ever holding a connection — and
//! the server's connection timeout — open.

use anyhow::{bail, Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 1;

/// Conventional conf-server port (resolver is 4564).
pub const DEFAULT_PORT: u16 = 4565;

/// Reserved DNS SAN of every conf server's TLS *serving* certificate.
/// Clients require the presented serving cert to carry exactly this
/// name and to be signed by the fingerprint-confirmed CA — that's what
/// distinguishes a conf-server daemon from any other node the same CA
/// has issued a cert to. Issuance policy must never grant this name to
/// a normal join; it is only issued locally on the CA host or via the
/// policy-gated [`Request::Enroll`].
pub const SERVING_SAN: &str = "netidx-conf-server";

/// Bodies larger than this are refused before allocation — CSRs and
/// certs are a few KB; this is a generous backstop against a hostile or
/// confused peer.
const MAX_MSG: u32 = 1 << 20; // 1 MiB

/// What kind of node is connecting. Informational — the server logs it;
/// it doesn't change issuance policy (that's per-admin, by SAN).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeKind {
    Resolver,
    Publisher,
    Client,
    Workstation,
    /// A peer conf server (server-to-server pushes).
    ConfServer,
}

/// A role this conf server's host performs. Claimed inside the
/// TLS-protected [`ServerHello`], so it's trustworthy once the chain is
/// pinned to the confirmed CA — unlike the mDNS beacon, which carries
/// the same list purely as a display hint.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    /// Holds the CA vault; answers [`Request::Sign`] and
    /// [`Request::Enroll`].
    Ca,
    /// A resolver server runs on this host.
    Resolver,
    /// An id-map daemon runs on this host; answers
    /// [`Request::AddIdentity`].
    IdMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHello {
    pub protocol_version: u32,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHello {
    pub protocol_version: u32,
    /// The TLS domain this network is rooted at (e.g. `ryu-oh.org`).
    pub domain: String,
    /// What this host does — see [`Role`].
    pub roles: Vec<Role>,
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

/// The one request a connection carries after the hello exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Ask for this host's local facts + known peers
    /// ([`GetInfoResponse`]). The *client* walks `peers` and
    /// aggregates — servers never fan out to answer this.
    GetInfo,
    /// Data-plane cert join (TLS networks), signed immediately — the
    /// admin's password rides in the request, so an admin must be
    /// present at the enrolling node. Answered with [`SignResponse`].
    Sign(SignRequest),
    /// Conf-server enrollment: issue the reserved [`SERVING_SAN`]
    /// serving cert to a new conf server. Requires an admin whose
    /// policy grants `may_enroll_servers`. Answered with
    /// [`SignResponse`].
    Enroll(EnrollRequest),
    /// CA → id-map push: register an identity on this host's id-map.
    /// Only accepted from a TLS client authenticated with a
    /// reserved-SAN serving cert. Answered with
    /// [`AddIdentityResponse`].
    AddIdentity(AddIdentityRequest),
    /// Queue a signing request for asynchronous admin approval — no
    /// credentials; this is how a node enrolls when no admin is
    /// present at it. Answered with [`EnqueueResponse`].
    Enqueue(EnqueueRequest),
    /// Check on a queued request. Answered with [`PollResponse`].
    Poll(PollRequest),
    /// List the pending queue (admin-authenticated). Answered with
    /// [`ListQueueResponse`].
    ListQueue(ListQueueRequest),
    /// Approve a queued request: sign its CSR and register its id-map
    /// groups (admin-authenticated; this is where the admin chooses
    /// the groups). Answered with [`ApproveResponse`]; the enrollee
    /// receives the cert via [`Request::Poll`].
    Approve(ApproveRequest),
    /// Deny a queued request (admin-authenticated). Answered with
    /// [`DenyResponse`].
    Deny(DenyRequest),
    /// Fetch the network's current CRL (no credentials — a CRL is
    /// public). Answered with [`GetCrlResponse`]. The renewal daemon
    /// pulls this and drops `crl.pem` beside each resolver's trusted
    /// bundle, where netidx's TLS acceptor enforces it.
    GetCrl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCrlResponse {
    /// `None` — no certificate has ever been revoked on this network.
    pub crl_pem: Option<String>,
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
    /// id-map groups to register the new identity with (first is
    /// primary) — chosen by the admin at enrollment time, validated
    /// against the allowed set in their policy. Empty ⇒ don't
    /// register this identity on the network's id-map hosts.
    #[serde(default)]
    pub id_map_groups: Vec<String>,
}

/// Conf-server enrollment ([`Request::Enroll`]): the CSR is signed with
/// the reserved [`SERVING_SAN`] regardless of what it claims. There is
/// no `requested_name` — the whole point is that the name is fixed and
/// privileged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrollRequest {
    pub admin: String,
    pub password: Secret,
    pub csr_pem: String,
    /// Where the new conf server will listen. The CA appends it to its
    /// own peer list (admin-authorized, so trusted), which makes the CA
    /// host the well-known starting point for peer walks.
    pub listen: SocketAddr,
}

/// Response to both [`Request::Sign`] and [`Request::Enroll`].
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
        /// Non-fatal follow-up failures (e.g. an id-map host that
        /// couldn't be reached for identity registration). The cert in
        /// this response is valid regardless; the client shows these to
        /// the operator.
        #[serde(default)]
        warnings: Vec<String>,
    },
    Err {
        reason: String,
    },
}

/// Register `san` on the receiving host's id-map. The uid is allocated
/// locally by the receiver — id-map perms are keyed on *names*; uids
/// are a per-host detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddIdentityRequest {
    /// The identity name — the DNS SAN the CA just issued.
    pub san: String,
    /// Primary group (created with an allocated gid if missing).
    pub primary_group: String,
    /// Secondary groups (each created if missing).
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AddIdentityResponse {
    Ok { uid: u32 },
    Err { reason: String },
}

/// Queue a CSR for asynchronous admin approval. Carries no
/// credentials — the requester proves nothing here; trust is
/// established out of band by the admin matching the request's
/// CSR-key fingerprint (shown on the enrolling node) before
/// approving. The id-map groups are chosen by the *admin* at
/// approval, not requested here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueRequest {
    pub kind: NodeKind,
    pub csr_pem: String,
    /// The DNS name the leaf should carry (validated against the
    /// approving admin's policy at approval time).
    pub requested_name: String,
    pub requested_validity_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EnqueueResponse {
    Ok { request_id: String },
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollRequest {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PollResponse {
    /// Still waiting for an admin.
    Pending,
    /// Approved and signed — same payload a synchronous sign returns.
    Signed {
        signed_cert_pem: String,
        trusted_pem: String,
        #[serde(default)]
        warnings: Vec<String>,
    },
    Denied {
        reason: String,
    },
    /// Never seen, expired, or already cleaned up.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListQueueRequest {
    pub admin: String,
    pub password: Secret,
}

/// One pending queue entry. The CSR rides along so the admin's CLI
/// computes the request fingerprint *locally* from the CSR's public
/// key — the value the enrollee reads out is never trusted from the
/// server's summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub id: String,
    pub kind: NodeKind,
    pub requested_name: String,
    pub requested_validity_days: u32,
    /// Seconds since the request was queued (server-computed; no
    /// clock-sync assumptions on the wire).
    pub age_secs: u64,
    /// The socket address the request arrived from — context for the
    /// admin, not a security signal.
    pub peer: String,
    pub csr_pem: String,
    /// The server verified this is a *renewal*: the request arrived on
    /// a connection authenticated by a live (unexpired, unrevoked)
    /// certificate for exactly this name. Cryptographic continuation —
    /// no glyph matching needed; safe to batch-approve (and what
    /// `autorenew` approves).
    #[serde(default)]
    pub verified_renewal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ListQueueResponse {
    Ok { requests: Vec<QueueEntry> },
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApproveRequest {
    pub admin: String,
    pub password: Secret,
    pub request_id: String,
    /// id-map groups for the new identity (first is primary), chosen
    /// by the admin here and validated against their policy's allowed
    /// set. Empty ⇒ no registration.
    pub id_map_groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApproveResponse {
    Ok {
        #[serde(default)]
        warnings: Vec<String>,
    },
    Err {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DenyRequest {
    pub admin: String,
    pub password: Secret,
    pub request_id: String,
    /// Shown to the waiting enrollee.
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DenyResponse {
    Ok,
    Err { reason: String },
}

/// How clients authenticate to a resolver — the data-plane auth, as
/// opposed to the conf plane, which is always TLS rooted at the CA.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InfoAuth {
    Anonymous,
    Krb5 { spn: String },
    Tls { name: String },
}

/// One resolver address with its data-plane auth.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolverAddr {
    pub addr: SocketAddr,
    pub auth: InfoAuth,
}

/// This host's local facts plus the conf servers it knows of. The
/// client aggregates across servers (mDNS-discovered ∪ peer-walk) to
/// build the network-wide picture; one reachable conf server is enough
/// to walk the rest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetInfoResponse {
    pub domain: String,
    /// Where to send [`Request::Sign`] / [`Request::Enroll`] — self if
    /// this host holds the CA, otherwise its configured location.
    pub ca_addr: Option<SocketAddr>,
    /// This host's resolver, if it runs one.
    pub resolver: Option<ResolverAddr>,
    /// Other conf servers this one knows of.
    pub peers: Vec<SocketAddr>,
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
        let req = Request::Sign(SignRequest {
            admin: "alice".to_string(),
            password: Secret("pw".to_string()),
            csr_pem: "CSR".to_string(),
            requested_name: "resolver.example.com".to_string(),
            requested_validity_days: 365,
            id_map_groups: vec!["users".to_string()],
        });
        write_msg(&mut a, &req).await.unwrap();
        let got: Request = read_msg(&mut b).await.unwrap();
        let Request::Sign(got) = got else { panic!("expected Sign") };
        assert_eq!(got.admin, "alice");
        assert_eq!(got.password.0, "pw");
        assert_eq!(got.requested_name, "resolver.example.com");
        assert_eq!(got.id_map_groups, vec!["users".to_string()]);
    }

    #[test]
    fn sign_request_without_groups_still_decodes() {
        // A request written before `id_map_groups` existed must decode
        // — the field is `#[serde(default)]`.
        let json = r#"{
            "admin": "alice",
            "password": "pw",
            "csr_pem": "CSR",
            "requested_name": "a.example.com",
            "requested_validity_days": 30
        }"#;
        let req: SignRequest = serde_json::from_str(json).unwrap();
        assert!(req.id_map_groups.is_empty());
    }

    #[tokio::test]
    async fn hello_and_info_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let hello = ServerHello {
            protocol_version: PROTOCOL_VERSION,
            domain: "ryu-oh.org".to_string(),
            roles: vec![Role::Ca, Role::Resolver, Role::IdMap],
        };
        write_msg(&mut a, &hello).await.unwrap();
        let got: ServerHello = read_msg(&mut b).await.unwrap();
        assert_eq!(got.domain, "ryu-oh.org");
        assert_eq!(got.roles, vec![Role::Ca, Role::Resolver, Role::IdMap]);
        let info = GetInfoResponse {
            domain: "ryu-oh.org".to_string(),
            ca_addr: Some("192.168.0.1:4565".parse().unwrap()),
            resolver: Some(ResolverAddr {
                addr: "192.168.0.1:4564".parse().unwrap(),
                auth: InfoAuth::Tls { name: "resolver.ryu-oh.org".to_string() },
            }),
            peers: vec!["192.168.0.2:4565".parse().unwrap()],
        };
        write_msg(&mut a, &info).await.unwrap();
        let got: GetInfoResponse = read_msg(&mut b).await.unwrap();
        assert_eq!(got.domain, "ryu-oh.org");
        assert_eq!(got.peers.len(), 1);
        assert_eq!(
            got.resolver.unwrap().auth,
            InfoAuth::Tls { name: "resolver.ryu-oh.org".to_string() }
        );
    }

    #[test]
    fn sign_response_without_warnings_still_decodes() {
        // A response written before `warnings` existed (or by a server
        // that omits empty fields) must decode — the field is
        // `#[serde(default)]`.
        let json = r#"{"Ok":{"signed_cert_pem":"CERT","trusted_pem":"CA"}}"#;
        let resp: SignResponse = serde_json::from_str(json).unwrap();
        let SignResponse::Ok { warnings, .. } = resp else { panic!("expected Ok") };
        assert!(warnings.is_empty());
    }
}
