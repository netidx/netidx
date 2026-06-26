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
use std::{net::SocketAddr, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 3;

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
    /// Revoke certificates by serial (admin-authenticated). The daemon
    /// owns the issuance index, so the `ca` CLI sends this rather than
    /// touching the CA files. Answered with [`RevokeResponse`].
    Revoke(RevokeRequest),
    /// List the issued certificates (admin-authenticated) — the revoke
    /// UI and inspection. Answered with [`ListIssuedResponse`].
    ListIssued(ListIssuedRequest),
    /// Request delegation of a namespace subtree to the child resolver
    /// described here — no credentials; the parent admin authorizes it
    /// (matching the out-of-band code) via `review-delegation`. Answered
    /// with [`DelegationResponse`].
    RequestDelegation(DelegationRequest),
    /// Check on a queued delegation request. Answered with
    /// [`DelegationPollResponse`].
    PollDelegation(PollRequest),
    /// List pending delegation requests (admin-authenticated). Answered
    /// with [`ListDelegationsResponse`].
    ListDelegations(ListDelegationsRequest),
    /// Approve a queued delegation: add the child to this resolver's
    /// `children` (propagated cluster-wide) and record the parent
    /// cluster's address(es) for the child to poll (admin-authenticated).
    /// Answered with [`ApproveDelegationResponse`].
    ApproveDelegation(ApproveDelegationRequest),
    /// Deny a queued delegation (admin-authenticated). Answered with
    /// [`DenyDelegationResponse`].
    DenyDelegation(DenyDelegationRequest),
    /// Server-to-server: apply a referral edit (add a child / set the
    /// parent) to this host's local resolver config — the receive side of
    /// cluster-wide delegation propagation. Peer-cert-gated like
    /// [`Request::AddIdentity`]. Answered with [`ApplyReferralEditResponse`].
    ApplyReferralEdit(ApplyReferralEditRequest),
    /// Server→CA push: register/update this conf server's facts (address,
    /// roles, resolver-cluster facts) in the CA's authoritative network
    /// map. Peer-cert-gated like [`Request::AddIdentity`]. Answered with
    /// [`RegisterResponse`].
    Register(RegisterRequest),
    /// Server→CA push: drop this conf server from the CA's map (on
    /// uninstall). Peer-cert-gated. Answered with [`RegisterResponse`].
    Deregister(DeregisterRequest),
    /// Cheap probe: return the served map's current version so a caching
    /// conf server can skip a full pull when unchanged. Answered with
    /// [`GetMapVersionResponse`].
    GetMapVersion,
    /// Fetch the full network map — the CA's authoritative copy, or a conf
    /// server's cache. One round trip to any conf server is the whole
    /// network. Answered with [`GetMapResponse`].
    GetMap,
    /// Admin-authenticated: drop a (dead) conf server from the CA's map,
    /// cascading to its resolver servers — for a machine that never ran
    /// `uninstall`. Answered with [`RemoveServerResponse`].
    RemoveServer(RemoveServerRequest),
    /// Read this resolver host's permissions file (no credentials — perms
    /// are readable within the trust domain, like the map). The client
    /// routes to a member of the cluster it wants. Answered with
    /// [`GetPermsResponse`].
    GetPerms,
    /// Admin-authenticated, sent to the **CA**: replace a target cluster's
    /// permissions file, validated and propagated cluster-wide. The CA
    /// authorizes the admin and pushes [`Request::ApplyPermsEdit`] to the
    /// target cluster's conf servers. Answered with [`EditPermsResponse`].
    EditPerms(EditPermsRequest),
    /// Server-to-server: apply a permissions edit to this host's local
    /// resolver perms — the receive side of cluster-wide perms propagation.
    /// Peer-cert-gated like [`Request::ApplyReferralEdit`]. Answered with
    /// [`ApplyPermsEditResponse`].
    ApplyPermsEdit(ApplyPermsEditRequest),
    /// Admin-authenticated, sent to the **CA**: mint a new **role** admin
    /// with the given scoped policy. Gated on the caller's
    /// `may_manage_admins` (or a signing slot), and the granted policy must
    /// be a subset of the caller's (no privilege escalation). Answered with
    /// [`AdminMgmtResponse`].
    AddRoleAdmin(AddRoleAdminRequest),
    /// Admin-authenticated, sent to the **CA**: replace a role admin's
    /// policy. Same gate + no-escalation subset rule as
    /// [`Request::AddRoleAdmin`]; never touches the reserved signing slots.
    /// Answered with [`AdminMgmtResponse`].
    SetAdminPolicy(SetAdminPolicyRequest),
    /// Admin-authenticated, sent to the **CA**: remove a role admin. Never
    /// the reserved signing slots, and never the last admin that can manage
    /// admins. Answered with [`AdminMgmtResponse`].
    RemoveAdmin(RemoveAdminRequest),
    /// Admin-authenticated, sent to the **CA**: list the admins, their tiers
    /// and policies (gated on `may_manage_admins` / a signing slot — the
    /// admin roster is not readable by a lower-tier role). Answered with
    /// [`AdminListResponse`].
    ListAdmins(ListAdminsRequest),
    /// Admin-authenticated, sent to the **CA**: restart / start / stop /
    /// status the activation units on the conf servers of the cluster serving
    /// `target_path`. Gated on the caller's `service_control_scopes` covering
    /// the path (or a signing slot). The CA fans out
    /// [`Request::ApplyServiceControl`] to the targeted members. Answered with
    /// [`ControlServiceResponse`].
    ControlService(ControlServiceRequest),
    /// Server-to-server: apply a service-control op to this host's local
    /// activation supervisor (via its control socket). Peer-cert-gated like
    /// [`Request::ApplyPermsEdit`]. Answered with
    /// [`ApplyServiceControlResponse`].
    ApplyServiceControl(ApplyServiceControlRequest),
    /// Mint a fresh recovery (off-box break-glass) password. Carries no
    /// credentials: it is **local-control-socket only** — the daemon refuses
    /// it over the network conf plane, because anyone who can reach the local
    /// socket already has on-box authority. The daemon rewraps the master key
    /// under a new recovery slot using its own in-process autorenew
    /// credential. Answered with [`RotateRecoveryResponse`] (the new
    /// password, shown once).
    RotateRecovery,
    /// Rotate the box's own autorenew signing credential and reseal its
    /// keytab, hot-swapping the in-process credential with no downtime.
    /// Carries no credentials: **local-control-socket only**, like
    /// [`Request::RotateRecovery`]. Answered with [`RotateAutorenewResponse`].
    RotateAutorenew,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeRequest {
    pub admin: String,
    pub password: Secret,
    /// Serial numbers to revoke (chosen from a [`ListIssuedResponse`]).
    pub serials: Vec<u64>,
    /// Recorded with each revocation and shown in the audit log.
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RevokeResponse {
    Ok {
        /// Non-fatal follow-ups (e.g. couldn't install the CRL beside a
        /// local resolver).
        #[serde(default)]
        warnings: Vec<String>,
    },
    Err {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListIssuedRequest {
    pub admin: String,
    pub password: Secret,
}

/// One issued certificate, for the admin revoke UI / inspection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedEntry {
    pub serial: u64,
    /// The DNS SAN the cert carries (empty for the rare no-DNS-SAN cert).
    pub name: String,
    /// SPKI fingerprint (grouped text) — the glyph shown at enrollment.
    pub spki_fp: String,
    pub not_after_unix: u64,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ListIssuedResponse {
    Ok { entries: Vec<IssuedEntry> },
    Err { reason: String },
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
    #[serde(with = "humantime_serde")]
    pub requested_validity: Duration,
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
    #[serde(with = "humantime_serde")]
    pub requested_validity: Duration,
    /// `Some` ⇒ this queues a **conf-server enrollment**: the cert is
    /// the reserved [`SERVING_SAN`] (whatever `requested_name` says)
    /// and the value is where the new conf server will listen — the CA
    /// records it as a peer at approval. Approval requires an admin
    /// whose policy grants `may_enroll_servers`; the request code
    /// ceremony is the same as any queued request.
    #[serde(default)]
    pub enroll_listen: Option<SocketAddr>,
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
    #[serde(with = "humantime_serde")]
    pub requested_validity: Duration,
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
    /// `Some` ⇒ a conf-server enrollment (see
    /// [`EnqueueRequest::enroll_listen`]): approval signs the reserved
    /// [`SERVING_SAN`] and requires `may_enroll_servers`; id-map groups
    /// don't apply.
    #[serde(default)]
    pub enroll_listen: Option<SocketAddr>,
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

// -- resolver hierarchy delegation -------------------------------------------

/// A delegation request: the child proposes to own `proposed_path` as a
/// subtree and submits its own resolver cluster's address(es). No
/// credentials — the parent admin authorizes by matching the out-of-band
/// request code (a fingerprint over exactly `(proposed_path, child)`)
/// before approving. The `child` list is the full child cluster so the
/// parent can refer down to any member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationRequest {
    pub proposed_path: String,
    pub child: Vec<ResolverAddr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DelegationResponse {
    Ok { request_id: String },
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DelegationPollResponse {
    /// Still waiting for the parent admin.
    Pending,
    /// Approved — the parent resolver cluster's address(es), to write
    /// into the child's `parent` referral.
    Approved { parent: Vec<ResolverAddr> },
    Denied { reason: String },
    /// Never seen, expired, or already cleaned up.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDelegationsRequest {
    pub admin: String,
    pub password: Secret,
}

/// One pending delegation request for the admin reviewer. The
/// `proposed_path` and `child` are exactly what the request code
/// fingerprints, so the admin's CLI recomputes the code locally — never
/// trusting a wire value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationEntry {
    pub id: String,
    pub proposed_path: String,
    pub child: Vec<ResolverAddr>,
    pub age_secs: u64,
    pub peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ListDelegationsResponse {
    Ok { requests: Vec<DelegationEntry> },
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApproveDelegationRequest {
    pub admin: String,
    pub password: Secret,
    pub request_id: String,
}

/// The per-peer outcome of propagating a delegation edit across the
/// resolver cluster. A non-`Ok` peer means the cluster is inconsistent
/// until re-synced — the reviewer surfaces it loudly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerResult {
    pub addr: SocketAddr,
    /// `None` ⇒ updated; `Some(err)` ⇒ failed (unreachable / rejected).
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApproveDelegationResponse {
    Ok { peers: Vec<PeerResult> },
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DenyDelegationRequest {
    pub admin: String,
    pub password: Secret,
    pub request_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DenyDelegationResponse {
    Ok,
    Err { reason: String },
}

/// A referral edit pushed server-to-server for cluster-wide consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReferralEdit {
    /// Add `child` as the resolver's child owning `path`.
    AddChild { path: String, child: Vec<ResolverAddr> },
    /// Set the resolver's `parent` referral: it attaches at `path` under
    /// the parent cluster `parent`.
    SetParent { path: String, parent: Vec<ResolverAddr> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyReferralEditRequest {
    pub edit: ReferralEdit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyReferralEditResponse {
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

/// One edge of the resolver hierarchy: a mount path and the cluster it
/// points at. A read-only fact for the network map — distinct from
/// [`ReferralEdit`], which *mutates* a referral during delegation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterEdge {
    pub path: String,
    pub addrs: Vec<ResolverAddr>,
}

/// A resolver cluster's facts, self-reported by one of its conf servers:
/// its advertisable members, where it attaches in the namespace, and its
/// hierarchy edges. This is the single-owner fact the CA folds into the
/// map — each cluster owns its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterFacts {
    /// The cluster's advertisable member resolver servers (`Local` dropped).
    pub members: Vec<ResolverAddr>,
    /// Where this cluster attaches — its parent-referral path, or `/` for
    /// the root cluster.
    pub base: String,
    /// The parent cluster this one attaches under, if any.
    pub parent: Option<ClusterEdge>,
    /// The child clusters delegated below this one.
    pub children: Vec<ClusterEdge>,
}

/// One conf server in the trust domain, as recorded in the CA's map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerEntry {
    /// The conf server's listen address.
    pub addr: SocketAddr,
    /// What this host does (see [`Role`]).
    pub roles: Vec<Role>,
    /// Its resolver cluster's facts, if it runs a resolver.
    pub cluster: Option<ClusterFacts>,
}

/// The CA-authoritative, versioned picture of the whole trust domain. The
/// CA builds it from conf-server [`Request::Register`] pushes — never by
/// walking — bumps `version` on every change, persists it, and serves it.
/// Every conf server caches a copy (version-checked) and serves it to
/// clients, so one round trip to any conf server is the whole network.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkMap {
    /// Monotonic, bumped by the CA on every change. Callers cheap-compare
    /// this (via [`Request::GetMapVersion`]) before pulling the full map.
    pub version: u64,
    /// Where the CA lives (the Sign/Enroll destination).
    pub ca_addr: Option<SocketAddr>,
    /// Every conf server in the trust domain.
    pub servers: Vec<ServerEntry>,
}

/// Server→CA: register/update this conf server's facts in the map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// This conf server's own listen address.
    pub addr: SocketAddr,
    pub roles: Vec<Role>,
    pub cluster: Option<ClusterFacts>,
}

/// Server→CA: drop this conf server from the map (on uninstall).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeregisterRequest {
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegisterResponse {
    Ok { version: u64 },
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetMapVersionResponse {
    Ok { version: u64 },
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetMapResponse {
    Ok { map: NetworkMap },
    Err { reason: String },
}

/// Admin-authenticated: drop a (dead) conf server from the CA's map,
/// cascading to its resolver servers. For the machine that never ran
/// `uninstall`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveServerRequest {
    pub admin: String,
    pub password: Secret,
    /// The listen address of the conf server to remove.
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoveServerResponse {
    Ok { version: u64 },
    Err { reason: String },
}

/// This host's permissions file, serialized (a resolver `PMap` as JSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GetPermsResponse {
    Ok { perms_json: String },
    Err { reason: String },
}

/// Admin → CA: replace the `target_path` cluster's permissions with
/// `perms_json` (a serialized resolver `PMap`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditPermsRequest {
    pub admin: String,
    pub password: Secret,
    /// The base path of the cluster whose perms to edit (e.g. `/eu`).
    pub target_path: String,
    pub perms_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EditPermsResponse {
    Ok { peers: Vec<PeerResult> },
    Err { reason: String },
}

/// Server → server: apply a permissions edit to the local resolver perms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyPermsEditRequest {
    pub perms_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyPermsEditResponse {
    Ok,
    Err { reason: String },
}

// -- remote admin management (over the conf plane) ----------------------------

/// Admin → CA: mint a new role admin `name` with `policy`. The server gates
/// on the caller's `may_manage_admins` (or a signing slot) and enforces that
/// `policy` is a subset of the caller's own (no escalation). `new_password`
/// is the password set on the minted slot (the managing admin conveys it to
/// the satellite); it rides the same TLS-to-the-pinned-CA channel as the
/// caller's own password.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddRoleAdminRequest {
    pub admin: String,
    pub password: Secret,
    pub name: String,
    pub new_password: Secret,
    pub policy: crate::ca_policy::Policy,
}

/// Admin → CA: replace role admin `target`'s policy with `policy` (same gate
/// + subset rule as [`AddRoleAdminRequest`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetAdminPolicyRequest {
    pub admin: String,
    pub password: Secret,
    pub target: String,
    pub policy: crate::ca_policy::Policy,
}

/// Admin → CA: remove role admin `target`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveAdminRequest {
    pub admin: String,
    pub password: Secret,
    pub target: String,
}

/// Admin → CA: list the admin roster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListAdminsRequest {
    pub admin: String,
    pub password: Secret,
}

/// Response to add/set/remove admin ops. These are CA-local (no cluster
/// propagation), so there is no peer-result list — just success or a safe
/// reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminMgmtResponse {
    Ok,
    Err { reason: String },
}

/// Response to [`Request::ListAdmins`]: the roster (each entry carries the
/// admin's name, tier, and full policy — including `may_manage_admins`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminListResponse {
    Ok { admins: Vec<crate::ca_policy::AdminInfo> },
    Err { reason: String },
}

// -- remote service control (over the conf plane) -----------------------------

/// One unit to act on, optionally pinned to a single cluster member by index
/// (`resolver:0`). `member: None` ⇒ every member of the cluster — `member:
/// Some(i)` ⇒ only the i-th member (the map's member order), which lets an
/// admin stagger restarts so a cluster isn't interrupted all at once.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnitTarget {
    pub unit: String,
    pub member: Option<u32>,
}

/// Admin → CA: control services on the cluster serving `target_path`. The op
/// (start/stop/restart/status) and the unit names are reused from the
/// activation control protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlServiceRequest {
    pub admin: String,
    pub password: Secret,
    pub target_path: String,
    pub targets: Vec<UnitTarget>,
    pub op: netidx_activation::control::ControlOp,
}

/// One cluster member's outcome: which member (index + address), an error if
/// it couldn't be reached / refused, and the per-unit statuses it reported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceControlResult {
    pub member: u32,
    pub addr: SocketAddr,
    pub error: Option<String>,
    pub units: Vec<netidx_activation::control::UnitStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlServiceResponse {
    Ok { results: Vec<ServiceControlResult> },
    Err { reason: String },
}

/// Server → server: apply a service-control op to this host's local
/// activation supervisor. `units` are the resolved unit names for THIS host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyServiceControlRequest {
    pub units: Vec<String>,
    pub op: netidx_activation::control::ControlOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyServiceControlResponse {
    Ok { units: Vec<netidx_activation::control::UnitStatus> },
    Err { reason: String },
}

// -- local control socket (CA box only) ---------------------------------------

/// Response to [`Request::RotateRecovery`]: the freshly minted recovery
/// password, in grouped display form (shown to the operator once, never
/// stored). On the wire it is a [`Secret`] so it is redacted in logs and
/// zeroized after use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RotateRecoveryResponse {
    Ok { recovery_password: Secret },
    Err { reason: String },
}

/// Response to [`Request::RotateAutorenew`]: success, optionally with a
/// warning (e.g. the keytab was resealed in plaintext because the existing
/// one was), or a safe failure reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RotateAutorenewResponse {
    Ok { warning: Option<String> },
    Err { reason: String },
}

/// Write a length-prefixed JSON message and flush.
pub async fn write_msg<S, T>(stream: &mut S, msg: &T) -> Result<()>
where
    S: AsyncWriteExt + Unpin,
    T: serde::Serialize,
{
    // Zeroize the serialized frame on drop: admin passwords and the recovery
    // password ride these messages as `Secret`, so the plaintext JSON buffer
    // is a credential too. Cheap on a control plane (not a data path).
    let body = zeroize::Zeroizing::new(serde_json::to_vec(msg).context("serializing message")?);
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
    // Zeroize the decoded frame on drop (it may hold a `Secret` password in
    // plaintext JSON) — see `write_msg`.
    let mut body = zeroize::Zeroizing::new(vec![0u8; len as usize]);
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
            requested_validity: std::time::Duration::from_secs(365 * 86400),
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

    #[tokio::test]
    async fn delegation_messages_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let req = Request::RequestDelegation(DelegationRequest {
            proposed_path: "/eu".to_string(),
            child: vec![ResolverAddr {
                addr: "10.0.0.2:4564".parse().unwrap(),
                auth: InfoAuth::Anonymous,
            }],
        });
        write_msg(&mut a, &req).await.unwrap();
        let got: Request = read_msg(&mut b).await.unwrap();
        let Request::RequestDelegation(got) = got else {
            panic!("expected RequestDelegation")
        };
        assert_eq!(got.proposed_path, "/eu");
        assert_eq!(got.child.len(), 1);

        let resp = DelegationPollResponse::Approved {
            parent: vec![ResolverAddr {
                addr: "10.0.0.1:4564".parse().unwrap(),
                auth: InfoAuth::Tls { name: "r.eu.example".to_string() },
            }],
        };
        write_msg(&mut a, &resp).await.unwrap();
        match read_msg::<_, DelegationPollResponse>(&mut b).await.unwrap() {
            DelegationPollResponse::Approved { parent } => {
                assert_eq!(parent[0].addr, "10.0.0.1:4564".parse().unwrap());
            }
            _ => panic!("expected Approved"),
        }

        let edit = Request::ApplyReferralEdit(ApplyReferralEditRequest {
            edit: ReferralEdit::AddChild {
                path: "/eu".to_string(),
                child: vec![ResolverAddr {
                    addr: "10.0.0.2:4564".parse().unwrap(),
                    auth: InfoAuth::Anonymous,
                }],
            },
        });
        write_msg(&mut a, &edit).await.unwrap();
        let Request::ApplyReferralEdit(got) = read_msg(&mut b).await.unwrap() else {
            panic!("expected ApplyReferralEdit")
        };
        match got.edit {
            ReferralEdit::AddChild { path, child } => {
                assert_eq!(path, "/eu");
                assert_eq!(child.len(), 1);
            }
            _ => panic!("expected AddChild"),
        }

        // The child-cluster direction: SetParent carries the parent's
        // address(es) + auth and must round-trip identically.
        let edit = Request::ApplyReferralEdit(ApplyReferralEditRequest {
            edit: ReferralEdit::SetParent {
                path: "/eu".to_string(),
                parent: vec![ResolverAddr {
                    addr: "10.0.0.1:4564".parse().unwrap(),
                    auth: InfoAuth::Krb5 { spn: "svc/r@EU".to_string() },
                }],
            },
        });
        write_msg(&mut a, &edit).await.unwrap();
        let Request::ApplyReferralEdit(got) = read_msg(&mut b).await.unwrap() else {
            panic!("expected ApplyReferralEdit")
        };
        match got.edit {
            ReferralEdit::SetParent { path, parent } => {
                assert_eq!(path, "/eu");
                assert_eq!(parent.len(), 1);
                assert_eq!(
                    parent[0].auth,
                    InfoAuth::Krb5 { spn: "svc/r@EU".to_string() }
                );
            }
            _ => panic!("expected SetParent"),
        }
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
            "requested_validity": "30days"
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

    #[tokio::test]
    async fn map_messages_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let req = Request::Register(RegisterRequest {
            addr: "10.0.0.2:4565".parse().unwrap(),
            roles: vec![Role::Resolver],
            cluster: Some(ClusterFacts {
                members: vec![ResolverAddr {
                    addr: "10.0.0.2:4564".parse().unwrap(),
                    auth: InfoAuth::Anonymous,
                }],
                base: "/eu".to_string(),
                parent: Some(ClusterEdge {
                    path: "/eu".to_string(),
                    addrs: vec![ResolverAddr {
                        addr: "10.0.0.1:4564".parse().unwrap(),
                        auth: InfoAuth::Anonymous,
                    }],
                }),
                children: vec![],
            }),
        });
        write_msg(&mut a, &req).await.unwrap();
        let got: Request = read_msg(&mut b).await.unwrap();
        let Request::Register(got) = got else { panic!("expected Register") };
        assert_eq!(got.addr, "10.0.0.2:4565".parse().unwrap());
        assert_eq!(got.cluster.unwrap().base, "/eu");

        let resp = GetMapResponse::Ok {
            map: NetworkMap {
                version: 7,
                ca_addr: Some("10.0.0.1:4565".parse().unwrap()),
                servers: vec![ServerEntry {
                    addr: "10.0.0.1:4565".parse().unwrap(),
                    roles: vec![Role::Ca, Role::Resolver],
                    cluster: None,
                }],
            },
        };
        write_msg(&mut a, &resp).await.unwrap();
        match read_msg::<_, GetMapResponse>(&mut b).await.unwrap() {
            GetMapResponse::Ok { map } => {
                assert_eq!(map.version, 7);
                assert_eq!(map.servers.len(), 1);
                assert_eq!(map.servers[0].roles, vec![Role::Ca, Role::Resolver]);
            }
            GetMapResponse::Err { reason } => panic!("err: {reason}"),
        }
    }

    #[test]
    fn cluster_facts_root_decodes() {
        // A root cluster reports base "/" and no parent/children.
        let json = r#"{"members":[{"addr":"10.0.0.1:4564","auth":"Anonymous"}],"base":"/","parent":null,"children":[]}"#;
        let cf: ClusterFacts = serde_json::from_str(json).unwrap();
        assert_eq!(cf.base, "/");
        assert!(cf.parent.is_none());
        assert!(cf.children.is_empty());
        assert_eq!(cf.members.len(), 1);
    }
}
