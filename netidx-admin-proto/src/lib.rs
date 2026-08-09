//! Wire protocol for the admin server: small request/response messages
//! exchanged over a TLS stream, length-prefixed [`Pack`] values.
//!
//! The types are cross-platform: a Windows node speaks this to a unix admin
//! server. The outer 4-byte big-endian length bounds allocation and isolates
//! one message.
//!
//! A connection is: TLS accept, [`ClientHello`] / [`ServerHello`]
//! exchange, then exactly **one** [`Request`] and its response. One
//! request per connection keeps human think-time (fingerprint
//! confirmation, password entry) from ever holding a connection — and
//! the server's connection timeout — open.
//!
//! # Protocol evolution
//!
//! This protocol has never shipped, so **no compatibility is maintained**.
//! There is no older peer to accommodate: change a message into whatever
//! shape is right and bump [`PROTOCOL_VERSION`], which peers compare for
//! exact equality before a request — and therefore before a credential — is
//! sent. A mismatch is refused at the hello with both versions named.
//!
//! That means no defaulted fields standing in for an encoding nobody ever
//! produced, no deprecated fields held in place, and no fixtures asserting
//! that last month's layout still decodes. Every field in a message is a
//! field the sender wrote, and a message that does not decode in full is an
//! error rather than a shape half-filled with defaults.
//!
//! Reconsider all of this at the first release: from then on there are peers
//! in the field, an epoch bump is an outage, and appending with defaults
//! becomes the cheap way to evolve rather than a lie about history.

use anyhow::{Context, Result, bail};
use enumflags2::{BitFlags, bitflags};
use netidx::resolver_server::config::{PMap, ReadGate};
use netidx_core::pack::Pack;
use netidx_derive::Pack;
use netidx_id_map::file::IdMap;
use serde_derive::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

pub mod fingerprint;
pub mod identity;
pub mod policy;
pub mod server_config;

pub const PROTOCOL_VERSION: u32 = 13;

/// Conventional admin-server port (resolver is 4564).
pub const DEFAULT_PORT: u16 = 4565;

/// Reserved DNS SAN of every admin server's TLS *serving* certificate.
/// Clients require the presented serving cert to carry exactly this
/// name and to be signed by the fingerprint-confirmed CA — that's what
/// distinguishes an admin-server daemon from any other node the same CA
/// has issued a cert to. Issuance policy must never grant this name to
/// a normal join; it is only issued locally on the CA host or via the
/// policy-gated [`Request::Enroll`].
pub const SERVING_SAN: &str = "netidx-admin-server";

pub const SERVER_ID_URI_PREFIX: &str = "urn:netidx:admin:server:";
pub const CA_ROLE_URI: &str = "urn:netidx:admin:role:ca";

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Pack,
)]
#[serde(transparent)]
pub struct AdminServerId(pub Uuid);

impl AdminServerId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn uri(self) -> String {
        format!("{SERVER_ID_URI_PREFIX}{}", self.0)
    }
}

impl Default for AdminServerId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AdminServerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for AdminServerId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Pack,
)]
#[serde(transparent)]
pub struct ResolverClusterId(pub Uuid);

impl ResolverClusterId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ResolverClusterId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ResolverClusterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Pack)]
#[serde(transparent)]
pub struct OperationId(pub Uuid);

impl OperationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for OperationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Bodies larger than this are refused before allocation — CSRs and
/// certs are a few KB; this is a generous backstop against a hostile or
/// confused peer.
const MAX_MSG: u32 = 1 << 20; // 1 MiB

/// What kind of node is connecting. Informational — the server logs it;
/// it doesn't change issuance policy (that's per-admin, by SAN).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Pack, PartialEq, Eq)]
pub enum NodeKind {
    #[pack(tag(0))]
    Resolver,
    #[pack(tag(1))]
    Publisher,
    #[pack(tag(2))]
    Client,
    #[pack(tag(3))]
    Workstation,
    /// A peer admin server (server-to-server pushes).
    #[pack(tag(4))]
    AdminServer,
}

/// A role this admin server's host performs. Claimed inside the
/// TLS-protected [`ServerHello`], so it's trustworthy once the chain is
/// pinned to the confirmed CA — unlike the mDNS beacon, which carries
/// the same list purely as a display hint.
#[bitflags]
#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Pack, PartialEq, Eq)]
pub enum Role {
    /// Holds the CA vault; answers [`Request::Sign`] and
    /// [`Request::Enroll`].
    #[pack(tag(0))]
    Ca = 1 << 0,
    /// A resolver server runs on this host.
    #[pack(tag(1))]
    Resolver = 1 << 1,
    /// An id-map daemon runs on this host; answers
    /// [`Request::ApplyIdMapEdit`].
    #[pack(tag(2))]
    IdMap = 1 << 2,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ClientHello {
    pub protocol_version: u32,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ServerHello {
    pub protocol_version: u32,
    /// The TLS domain this admin domain is rooted at (e.g. `ryu-oh.org`).
    pub domain: String,
    /// What this host does — see [`Role`].
    pub roles: BitFlags<Role>,
    pub server_id: AdminServerId,
    pub ca: bool,
}

/// A secret string that never appears in `Debug` output and is zeroized
/// on drop. Both Serde and Pack encode it as a string.
#[derive(Clone, Serialize, Deserialize, Pack)]
#[serde(transparent)]
pub struct Secret(pub String);

impl Secret {
    /// The secret as a string slice — for passing to RPC helpers that take
    /// `&str`. Prefer this over reaching into `.0` at call sites.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum AdminCredential {
    #[pack(tag(0))]
    Password { admin: String, password: Secret },
    #[pack(tag(1))]
    Session { token: Secret },
}

impl AdminCredential {
    pub fn password(admin: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Password { admin: admin.into(), password: Secret(password.into()) }
    }
}

/// The one request a connection carries after the hello exchange.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum Request {
    /// Ask for this host's local facts + known peers
    /// ([`GetInfoResponse`]). The *client* walks `peers` and
    /// aggregates — servers never fan out to answer this.
    #[pack(tag(0))]
    GetInfo,
    /// Authenticate a password once and mint an in-memory CA session.
    #[pack(tag(1))]
    Login(LoginRequest),
    /// Revoke one in-memory CA session.
    #[pack(tag(2))]
    Logout(LogoutRequest),
    /// Data-plane cert join (TLS admin domains), signed immediately — the
    /// admin's password rides in the request, so an admin must be
    /// present at the enrolling node. Answered with [`SignResponse`].
    #[pack(tag(3))]
    Sign(SignRequest),
    /// Admin-server enrollment: issue the reserved [`SERVING_SAN`]
    /// serving cert to a new admin server. Requires an admin whose
    /// policy covers the requested resolver cluster base and roles. Answered with
    /// [`SignResponse`].
    #[pack(tag(4))]
    Enroll(EnrollRequest),
    /// Server → server: apply one id-map operation to this host's local map.
    /// Only accepted from a TLS client authenticated with a reserved-SAN
    /// serving cert. Answered with [`ApplyIdMapEditResponse`].
    ///
    /// Both id-map writers land here: the CA's registration push after it
    /// signs an identity, and an operator's [`Request::EditIdMap`].
    #[pack(tag(5))]
    ApplyIdMapEdit(ApplyIdMapEditRequest),
    /// Queue a signing request for asynchronous admin approval — no
    /// credentials; this is how a node enrolls when no admin is
    /// present at it. Answered with [`EnqueueResponse`].
    #[pack(tag(6))]
    Enqueue(EnqueueRequest),
    /// Check on a queued request. Answered with [`PollResponse`].
    #[pack(tag(7))]
    Poll(PollRequest),
    /// List the pending queue (admin-authenticated). Answered with
    /// [`ListQueueResponse`].
    #[pack(tag(8))]
    ListQueue(ListQueueRequest),
    /// Approve a queued request: sign its CSR and register its id-map
    /// groups (admin-authenticated; this is where the admin chooses
    /// the groups). Answered with [`ApproveResponse`]; the enrollee
    /// receives the cert via [`Request::Poll`].
    #[pack(tag(9))]
    Approve(ApproveRequest),
    /// Deny a queued request (admin-authenticated). Answered with
    /// [`DenyResponse`].
    #[pack(tag(10))]
    Deny(DenyRequest),
    /// Fetch the admin domain's current CRL (no credentials — a CRL is
    /// public). Answered with [`GetCrlResponse`]. The renewal daemon
    /// pulls this and drops `crl.pem` beside each resolver's trusted
    /// bundle, where netidx's TLS acceptor enforces it.
    #[pack(tag(11))]
    GetCrl,
    /// Revoke certificates by serial (admin-authenticated). The daemon
    /// owns the issuance index, so the `ca` CLI sends this rather than
    /// touching the CA files. Answered with [`RevokeResponse`].
    #[pack(tag(12))]
    Revoke(RevokeRequest),
    /// List the issued certificates (admin-authenticated) — the revoke
    /// UI and inspection. Answered with [`ListIssuedResponse`].
    #[pack(tag(13))]
    ListIssued(ListIssuedRequest),
    /// Request delegation of a namespace subtree to the child resolver
    /// described here — no credentials; the parent admin authorizes it
    /// (matching the out-of-band code) via `review-delegation`. Answered
    /// with [`DelegationResponse`].
    #[pack(tag(14))]
    RequestDelegation(DelegationRequest),
    /// Check on a queued delegation request. Answered with
    /// [`DelegationPollResponse`].
    #[pack(tag(15))]
    PollDelegation(PollRequest),
    /// List pending delegation requests (admin-authenticated). Answered
    /// with [`ListDelegationsResponse`].
    #[pack(tag(16))]
    ListDelegations(ListDelegationsRequest),
    /// Approve a queued delegation: add the child to this resolver's
    /// `children` (propagated resolver cluster-wide) and record the parent
    /// resolver cluster's address(es) for the child to poll (admin-authenticated).
    /// Answered with [`ApproveDelegationResponse`].
    #[pack(tag(17))]
    ApproveDelegation(ApproveDelegationRequest),
    /// Deny a queued delegation (admin-authenticated). Answered with
    /// [`DenyDelegationResponse`].
    #[pack(tag(18))]
    DenyDelegation(DenyDelegationRequest),
    /// Server-to-server: apply a referral edit (add a child / set the
    /// parent) to this host's local resolver config — the receive side of
    /// resolver cluster-wide delegation propagation. Peer-cert-gated like
    /// [`Request::ApplyIdMapEdit`]. Answered with [`ApplyReferralEditResponse`].
    #[pack(tag(19))]
    ApplyReferralEdit(ApplyReferralEditRequest),
    /// Server→CA push: register/update this admin server's facts (address,
    /// roles, resolver cluster facts) in the CA's authoritative admin domain
    /// map. Peer-cert-gated like [`Request::ApplyIdMapEdit`]. Answered with
    /// [`RegisterResponse`].
    #[pack(tag(20))]
    Register(RegisterRequest),
    /// Server→CA push: mark this admin server's grant `Enrolled` (on
    /// uninstall), taking it out of routing without destroying its identity.
    /// Peer-cert-gated. Answered with [`RegisterResponse`].
    #[pack(tag(21))]
    Deregister,
    /// Cheap probe: return the served map's current version so a caching
    /// admin server can skip a full pull when unchanged. Answered with
    /// [`GetMapVersionResponse`].
    #[pack(tag(22))]
    GetMapVersion,
    /// Fetch the full admin domain map — the CA's authoritative copy, or an admin
    /// server's cache. One round trip to any admin server is the whole
    /// admin domain. Answered with [`GetMapResponse`].
    #[pack(tag(23))]
    GetMap,
    /// Admin-authenticated: permanently revoke and remove a dead admin-server
    /// identity, then reconcile referral topology on surviving resolvers. An
    /// idempotent repeat is the manual recovery path after partial fanout.
    /// Answered with [`RemoveServerResponse`].
    #[pack(tag(24))]
    RemoveServer(RemoveServerRequest),
    /// CA → node: read this resolver host's permissions file. This is
    /// an internal exact-target RPC; admin domain clients use [`Request::ReadPerms`]
    /// so credentials are verified at the CA first. Answered with
    /// [`GetPermsResponse`].
    #[pack(tag(25))]
    GetPerms,
    /// Admin-authenticated, sent to the **CA**: replace a target resolver cluster's
    /// permissions file, validated and propagated resolver cluster-wide. The CA
    /// authorizes the admin and pushes [`Request::ApplyPermsEdit`] to the
    /// target resolver cluster's admin servers. Answered with [`EditPermsResponse`].
    #[pack(tag(26))]
    EditPerms(EditPermsRequest),
    /// Server-to-server: apply a permissions edit to this host's local
    /// resolver perms — the receive side of resolver cluster-wide perms propagation.
    /// Peer-cert-gated like [`Request::ApplyReferralEdit`]. Answered with
    /// [`ApplyPermsEditResponse`].
    #[pack(tag(27))]
    ApplyPermsEdit(ApplyPermsEditRequest),
    /// Admin-authenticated, sent to the **CA**: mint a new **role** admin
    /// with the given scoped policy. Gated on the caller's
    /// `may_manage_admins` (or a signing slot), and the granted policy must
    /// be a subset of the caller's (no privilege escalation). Answered with
    /// [`AdminMgmtResponse`].
    #[pack(tag(28))]
    AddRoleAdmin(AddRoleAdminRequest),
    /// Admin-authenticated, sent to the **CA**: replace a role admin's
    /// policy. Same gate + no-escalation subset rule as
    /// [`Request::AddRoleAdmin`]; never touches the reserved signing slots.
    /// Answered with [`AdminMgmtResponse`].
    #[pack(tag(29))]
    SetAdminPolicy(SetAdminPolicyRequest),
    /// Admin-authenticated, sent to the **CA**: remove a role admin. Never
    /// the reserved signing slots, and never the last admin that can manage
    /// admins. Answered with [`AdminMgmtResponse`].
    #[pack(tag(30))]
    RemoveAdmin(RemoveAdminRequest),
    /// Admin-authenticated, sent to the **CA**: list the admins, their tiers
    /// and policies (gated on `may_manage_admins` / a signing slot — the
    /// admin roster is not readable by a lower-tier role). Answered with
    /// [`AdminListResponse`].
    #[pack(tag(31))]
    ListAdmins(ListAdminsRequest),
    /// Admin-authenticated, sent to the **CA**: restart / start / stop /
    /// status the activation units on **one** admin server (`target_server`).
    /// Gated on the caller's `service_control_scopes` covering that server's
    /// resolver cluster base (or a signing slot). The CA forwards a single
    /// [`Request::ApplyServiceControl`] to `target_server` (applying locally when
    /// it is the CA itself). Answered with [`ControlServiceResponse`].
    #[pack(tag(32))]
    ControlService(ControlServiceRequest),
    /// Server-to-server: apply a service-control op to this host's local
    /// activation supervisor (via its control socket). Peer-cert-gated like
    /// [`Request::ApplyPermsEdit`]. Answered with
    /// [`ApplyServiceControlResponse`].
    #[pack(tag(33))]
    ApplyServiceControl(ApplyServiceControlRequest),
    /// Mint a fresh recovery (off-box break-glass) password. Carries no
    /// credentials: it is **local-control-socket only** — the daemon refuses
    /// it over the network admin plane, because anyone who can reach the local
    /// socket already has on-box authority. The daemon rewraps the master key
    /// under a new recovery slot using its own in-process autorenew
    /// credential. Answered with [`RotateRecoveryResponse`] (the new
    /// password, shown once).
    #[pack(tag(34))]
    RotateRecovery,
    /// Rotate the box's own autorenew signing credential and reseal its
    /// keytab, hot-swapping the in-process credential with no downtime.
    /// Carries no credentials: **local-control-socket only**, like
    /// [`Request::RotateRecovery`]. Answered with [`RotateAutorenewResponse`].
    #[pack(tag(35))]
    RotateAutorenew,
    /// CA → node: atomically install the home CA's freshly signed CRL
    /// beside this node's admin and resolver trust bundles. The receiver
    /// verifies the CRL signature against its exact home CA before writing it.
    /// Answered with [`ApplyCrlResponse`].
    #[pack(tag(36))]
    ApplyCrl(ApplyCrlRequest),
    /// Admin-authenticated, sent to the **CA**: read the permissions
    /// of the active resolver cluster mounted at `target_path`. The CA
    /// authorizes the scope and reads one exact CA-owned server identity via
    /// [`Request::GetPerms`]. Local-control callers are authorized by the
    /// protected socket and confined to this host's own resolver cluster. Answered with
    /// [`ReadPermsResponse`].
    #[pack(tag(37))]
    ReadPerms(ReadPermsRequest),
    /// Local-control-only: capture a point-in-time-consistent recovery bundle
    /// from the running CA into `target`. The target path is interpreted
    /// on the CA host and is never accepted over the network plane.
    /// Answered with [`BackupResponse`].
    #[pack(tag(38))]
    Backup(BackupRequest),
    /// Admin-authenticated CA maintenance: fan the CA's current
    /// identity/address, authoritative map, and CRL out to every registered
    /// node. Idempotent; this is the explicit retry after a node missed startup
    /// reconciliation. Answered with [`ReconcileCaResponse`].
    #[pack(tag(39))]
    ReconcileCa(ReconcileCaRequest),
    /// CA → node: install a verified CA relocation and its
    /// accompanying authoritative state. The TLS peer's exact home-CA-issued
    /// CA identity is the authorization boundary. Answered with
    /// [`ApplyCaStateResponse`].
    #[pack(tag(40))]
    ApplyCaState(ApplyCaStateRequest),
    /// Local-control-only: emit a renewal CSR for an externally-signed
    /// CA using the live in-process CA key. The key never leaves
    /// the daemon and the CA remains online. Answered with
    /// [`ExternalCaCsrResponse`].
    #[pack(tag(41))]
    ExternalCaCsr,
    /// Local-control-only: install a renewed externally-signed CA
    /// certificate. The daemon requires the same CA key and the already-pinned
    /// external issuer, then hot-reloads its serving chain. Answered with
    /// [`ExternalCaInstallResponse`].
    #[pack(tag(42))]
    ExternalCaInstall(ExternalCaInstallRequest),
    /// Local-control-only: report the running CA's credential and external-PKI
    /// state without racing its durable state through an offline reader.
    /// Answered with [`CaStatusResponse`].
    #[pack(tag(43))]
    CaStatus,
    /// Admin-authenticated, sent to the **CA**: open or shut the read gate on
    /// **one** member (`target_server`). Scoped exactly like
    /// [`Request::ControlService`] — the caller's `service_control_scopes`
    /// must cover that server's resolver cluster base, or hold a signing
    /// slot. The CA forwards a single [`Request::ApplySetReadGate`] to the
    /// target. Answered with [`SetReadGateResponse`].
    #[pack(tag(44))]
    SetReadGate(SetReadGateRequest),
    /// Server-to-server: write the read gate into this host's own resolver
    /// config. Peer-cert-gated like [`Request::ApplyReferralEdit`]. Answered
    /// with [`ApplySetReadGateResponse`].
    #[pack(tag(45))]
    ApplySetReadGate(ApplySetReadGateRequest),
    /// CA → node: read this host's id-map. The internal exact-target RPC
    /// behind a reconcile — the CA has to know what a lagging host actually
    /// holds before it can work out what it is missing. Admin domain clients
    /// use [`Request::GetIdMap`] so credentials are verified at the CA first.
    /// Answered with [`GetLocalIdMapResponse`].
    #[pack(tag(48))]
    GetLocalIdMap,
    /// Admin-authenticated, sent to the **CA**: read the receiving host's
    /// id-map. Answered with [`GetIdMapResponse`].
    #[pack(tag(46))]
    GetIdMap(GetIdMapRequest),
    /// Admin-authenticated, sent to the **CA**: apply one id-map operation
    /// and propagate it to every id-map host. The CA authorizes the caller
    /// against its `id_map_groups` / `allowed_san` and pushes
    /// [`Request::ApplyIdMapEdit`] to each. Answered with
    /// [`EditIdMapResponse`].
    #[pack(tag(47))]
    EditIdMap(EditIdMapRequest),
    /// Sent to the **CA**: replace the caller's OWN password, proving the
    /// current one in the same request — the one admin request that carries no
    /// [`AdminCredential`], and so cannot be authorized by a session. The only
    /// request a `must_change` password may send, so it authenticates through
    /// a path that deliberately skips that gate — an admin locked out of
    /// everything else must still be able to get out. Answered with
    /// [`AdminMgmtResponse`].
    #[pack(tag(49))]
    ChangePassword(ChangePasswordRequest),
    /// Admin-authenticated, sent to the **CA**: replace a role admin's
    /// password with a one-time key and lock the slot to
    /// [`Request::ChangePassword`] until its holder chooses one. Gated on
    /// `may_manage_admins` plus the target's authority being within the
    /// caller's. Answered with [`AdminMgmtResponse`].
    #[pack(tag(50))]
    ResetPassword(ResetPasswordRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct BackupRequest {
    /// Absolute or caller-relative path on the CA host. The server
    /// refuses an existing target rather than overwriting backup material.
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum RpcResult<T: 'static> {
    #[pack(tag(0))]
    Ok(T),
    #[pack(tag(1))]
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct BackupOk {
    pub target: String,
    pub ca_fingerprint: String,
    pub ca: AdminServerId,
    pub map_version: u64,
    pub highest_serial: u64,
    pub files: u64,
    pub bytes: u64,
    pub manifest_sha256: String,
}

pub type BackupResponse = RpcResult<BackupOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ExternalCaInstallRequest {
    pub signed_cert_pem: String,
    pub root_pem: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ExternalCaCsrOk {
    pub common_name: String,
    pub csr_pem: String,
}

pub type ExternalCaCsrResponse = RpcResult<ExternalCaCsrOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ExternalCaInstallOk {
    pub ca_fingerprint: String,
}

pub type ExternalCaInstallResponse = RpcResult<ExternalCaInstallOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct CaStatus {
    pub autorenew_slot_present: bool,
    pub recovery_slot_present: bool,
    pub externally_signed: bool,
    pub cert_installed: bool,
    pub pending: Option<(String, String)>,
}

pub type CaStatusResponse = RpcResult<CaStatus>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ReconcileCaRequest {
    pub credential: AdminCredential,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct PropagationOk {
    pub operation_id: OperationId,
    pub peers: Vec<PeerResult>,
}

pub type ReconcileCaResponse = RpcResult<PropagationOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplyCaStateRequest {
    pub operation_id: OperationId,
    pub ca: AdminServerId,
    pub addr: SocketAddr,
    pub map: AdminDomainMap,
    pub crl_pem: String,
}

pub type ApplyCaStateResponse = RpcResult<()>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct LoginRequest {
    pub credential: AdminCredential,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct LoginOk {
    pub admin: String,
    pub token: Secret,
    pub issued_unix: u64,
    pub absolute_deadline_unix: u64,
    pub idle_timeout_secs: u64,
}

/// Response to [`Request::Login`]. Shaped like [`RpcResult<LoginOk>`], whose
/// tags it keeps, plus a third case that a `reason` string could not carry: a
/// frontend has to *route* on "change your password" (the TUI opens its
/// change-password screen) and sniffing an error message for that is exactly
/// the fragile coupling a variant exists to prevent.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum LoginResponse {
    #[pack(tag(0))]
    Ok(LoginOk),
    #[pack(tag(1))]
    Err { reason: String },
    /// The password verified but the slot is one-time: nothing is authorized
    /// until [`Request::ChangePassword`] replaces it. Sent only *after* the
    /// password checks out — answering it earlier would name which accounts
    /// are mid-reset to anyone who can guess a login.
    #[pack(tag(2))]
    PasswordChangeRequired { admin: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct LogoutRequest {
    pub credential: AdminCredential,
}

pub type LogoutResponse = RpcResult<()>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RevokeRequest {
    pub credential: AdminCredential,
    /// Serial numbers to revoke (chosen from a [`ListIssuedResponse`]).
    pub serials: Vec<u64>,
    /// Recorded with each revocation and shown in the audit log.
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RevokeOk {
    /// Non-fatal revocation/signing follow-ups.
    #[serde(default)]
    pub warnings: Vec<String>,
    /// The immediate CRL-distribution operation, when this response came
    /// from a protocol-v6 CA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<OperationId>,
    /// One deterministic result for every registered admin server that
    /// should enforce the new CRL.
    #[serde(default)]
    pub peers: Vec<PeerResult>,
}

pub type RevokeResponse = RpcResult<RevokeOk>;

/// CA → node immediate CRL distribution. The CA certificate
/// is the authorization gate; the CRL itself is independently signature
/// checked by the receiver before it replaces any local file.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplyCrlRequest {
    pub operation_id: OperationId,
    pub crl_pem: String,
}

pub type ApplyCrlResponse = RpcResult<()>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ListIssuedRequest {
    pub credential: AdminCredential,
}

/// One issued certificate, for the admin revoke UI / inspection.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct IssuedEntry {
    pub serial: u64,
    /// The DNS SAN the cert carries (empty for the rare no-DNS-SAN cert).
    pub name: String,
    /// SPKI fingerprint (grouped text) — the glyph shown at enrollment.
    pub spki_fp: String,
    pub not_after_unix: u64,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum ListIssuedResponse {
    #[pack(tag(0))]
    Entry { entry: IssuedEntry },
    #[pack(tag(1))]
    End,
    #[pack(tag(2))]
    Err { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct GetCrlResponse {
    /// `None` — no certificate has ever been revoked on this admin domain.
    pub crl_pem: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct SignRequest {
    /// The identity class being issued. Resolver service identities may have
    /// multiple live keys for their shared resolver cluster TLS name; user-like kinds
    /// retain the one-live-name rule.
    pub kind: NodeKind,
    /// Which admin's password follows — selects the keyslot and its
    /// issuance policy.
    pub credential: AdminCredential,
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
    /// register this identity on the admin domain's id-map hosts.
    #[serde(default)]
    pub id_map_groups: Vec<String>,
    /// Restore-time replacement of this exact still-live certificate serial.
    /// The approving administrator sees and authorizes the replacement; the
    /// CA issues the new key and immediately revokes this serial.
    #[serde(default)]
    pub replaces_serial: Option<u64>,
}

/// Admin-server enrollment ([`Request::Enroll`]): the CSR is signed with
/// the reserved [`SERVING_SAN`] regardless of what it claims. There is
/// no `requested_name` — the whole point is that the name is fixed and
/// privileged.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct EnrollRequest {
    pub credential: AdminCredential,
    pub csr_pem: String,
    /// Where the new admin server will listen. The CA appends it to its
    /// own peer list (admin-authorized, so trusted), which makes the CA
    /// host the well-known starting point for peer walks.
    pub listen: SocketAddr,
    pub roles: BitFlags<Role>,
    /// The resolver endpoint owned by this admin-server identity. It must be
    /// one of this host's locally configured `resolver_members`; stable
    /// ownership lets the CA form and split resolver clusters without treating those
    /// optional launch blocks as the authoritative roster.
    pub resolver_member: Option<ResolverAddr>,
    pub resolver_members: Vec<ResolverAddr>,
    pub cluster: ResolverClusterPlacement,
    /// Accepted only on the protected local control socket, for renewing the
    /// already-installed CA identity across a key rotation.
    pub renew_identity: Option<AdminServerId>,
    /// Restore-time replacement of a failed satellite. On approval the CA
    /// atomically grants the fresh identity, removes this old identity, and
    /// revokes all of its live serving certificates. Never accepted for the
    /// active CA.
    #[serde(default)]
    pub replaces: Option<AdminServerId>,
    /// This host's resolver config as it was installed, verbatim.
    ///
    /// It carries the half the CA cannot otherwise know: the bind address, the
    /// paths to this host's certificate and key, its pid file, its id-map
    /// socket, its tuning, and where its permissions file lives. Handing it
    /// over here — once, at enrollment, from the only party that knows it — is
    /// what lets the CA be authoritative for the whole document afterwards
    /// rather than patching topology into a file it does not understand and
    /// preserving the rest by matching addresses.
    ///
    /// Its topology fields are ignored; the CA renders those from the map.
    ///
    /// `None` from a host with no resolver.
    #[serde(default)]
    pub resolver_config: Option<netidx::resolver_server::config::file::Config>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack, PartialEq, Eq)]
pub enum ResolverClusterPlacement {
    #[pack(tag(0))]
    Create { base: String },
    #[pack(tag(1))]
    Join { cluster: ResolverClusterId },
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack, PartialEq, Eq)]
pub struct EnrollmentRequest {
    pub listen: SocketAddr,
    pub roles: BitFlags<Role>,
    pub resolver_member: Option<ResolverAddr>,
    pub resolver_members: Vec<ResolverAddr>,
    pub cluster: ResolverClusterPlacement,
    #[serde(default)]
    pub replaces: Option<AdminServerId>,
    /// This host's resolver config as it was installed, verbatim.
    ///
    /// It carries the half the CA cannot otherwise know: the bind address, the
    /// paths to this host's certificate and key, its pid file, its id-map
    /// socket, its tuning, and where its permissions file lives. Handing it
    /// over here — once, at enrollment, from the only party that knows it — is
    /// what lets the CA be authoritative for the whole document afterwards
    /// rather than patching topology into a file it does not understand and
    /// preserving the rest by matching addresses.
    ///
    /// Its topology fields are ignored; the CA renders those from the map.
    ///
    /// `None` from a host with no resolver.
    #[serde(default)]
    pub resolver_config: Option<netidx::resolver_server::config::file::Config>,
}

/// Response to both [`Request::Sign`] and [`Request::Enroll`].
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct SignOk {
    pub signed_cert_pem: String,
    /// The full set of trusted CA certs the joining node should
    /// install as its trust anchor — a PEM bundle that always
    /// includes the issuing CA and may include additional
    /// (e.g. federated) CAs. The client installs this verbatim, so
    /// a join needs no manual file copying at all.
    pub trusted_pem: String,
    /// Non-fatal follow-up failures (e.g. an id-map host that
    /// couldn't be reached for identity registration). The cert in
    /// this response is valid regardless; the client shows these to
    /// the operator.
    #[serde(default)]
    pub warnings: Vec<String>,
    /// Present when issuance triggered an id-map fanout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<OperationId>,
}

pub type SignResponse = RpcResult<SignOk>;

/// Queue a CSR for asynchronous admin approval. Carries no
/// credentials — the requester proves nothing here; trust is
/// established out of band by the admin matching the request's
/// CSR-key fingerprint (shown on the enrolling node) before
/// approving. The id-map groups are chosen by the *admin* at
/// approval, not requested here.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct EnqueueRequest {
    pub kind: NodeKind,
    pub csr_pem: String,
    /// The DNS name the leaf should carry (validated against the
    /// approving admin's policy at approval time).
    pub requested_name: String,
    #[serde(with = "humantime_serde")]
    pub requested_validity: Duration,
    /// `Some` ⇒ this queues a **admin-server enrollment**: the cert is
    /// the reserved [`SERVING_SAN`] (whatever `requested_name` says)
    /// and the value is where the new admin server will listen — the CA
    /// records it as a peer at approval. Approval requires an admin
    /// whose policy covers the requested resolver cluster base and roles; the request code
    /// ceremony is the same as any queued request.
    #[serde(default)]
    pub enrollment: Option<EnrollmentRequest>,
    /// Restore-time replacement of an exact still-live leaf certificate.
    #[serde(default)]
    pub replaces_serial: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct QueuedOk {
    pub request_id: String,
}

pub type EnqueueResponse = RpcResult<QueuedOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct PollRequest {
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum PollResponse {
    /// Still waiting for an admin.
    #[pack(tag(0))]
    Pending,
    /// Approved and signed — same payload a synchronous sign returns.
    #[pack(tag(1))]
    Signed(SignOk),
    #[pack(tag(2))]
    Denied { reason: String },
    /// Never seen, expired, or already cleaned up.
    #[pack(tag(3))]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ListQueueRequest {
    pub credential: AdminCredential,
}

/// One pending queue entry. The CSR rides along so the admin's CLI
/// computes the request fingerprint *locally* from the CSR's public
/// key — the value the enrollee reads out is never trusted from the
/// server's summary.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
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
    /// `Some` ⇒ an admin-server enrollment (see
    /// [`EnqueueRequest::enroll_listen`]): approval signs the reserved
    /// [`SERVING_SAN`] and requires scoped server-enrollment authority; id-map groups
    /// don't apply.
    #[serde(default)]
    pub enrollment: Option<EnrollmentRequest>,
    /// Authoritative base of the requested resolver cluster. For `Create` this
    /// repeats the requested base; for `Join` the CA resolves the stable resolver cluster
    /// ID through its map so approval UIs can show both identity and scope.
    #[serde(default)]
    pub cluster_base: Option<String>,
    /// Exact live leaf serial replaced when this restore request is approved.
    #[serde(default)]
    pub replaces_serial: Option<u64>,
}

pub type ListQueueResponse = RpcResult<Vec<QueueEntry>>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApproveRequest {
    pub credential: AdminCredential,
    pub request_id: String,
    /// id-map groups for the new identity (first is primary), chosen
    /// by the admin here and validated against their policy's allowed
    /// set. Empty ⇒ no registration.
    pub id_map_groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApproveOk {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<OperationId>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

pub type ApproveResponse = RpcResult<ApproveOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct DenyRequest {
    pub credential: AdminCredential,
    pub request_id: String,
    /// Shown to the waiting enrollee.
    pub reason: String,
}

pub type DenyResponse = RpcResult<()>;

// -- resolver hierarchy delegation -------------------------------------------

/// A delegation request names the intended parent and child by immutable
/// admin-server identities. The two sets may currently belong to distinct
/// resolver clusters (attach/rebase) or to one active peer resolver cluster (split). No
/// credentials are carried: approval is authorized by matching the
/// out-of-band request code over this complete proposal.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct DelegationRequest {
    pub proposed_path: String,
    pub parent_servers: Vec<AdminServerId>,
    pub child_servers: Vec<AdminServerId>,
}

pub type DelegationResponse = RpcResult<QueuedOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum DelegationPollResponse {
    /// Still waiting for the parent admin.
    #[pack(tag(0))]
    Pending,
    /// Approved — the parent resolver cluster's address(es), to write
    /// into the child's `parent` referral.
    #[pack(tag(1))]
    Approved { parent: Vec<ResolverAddr> },
    #[pack(tag(2))]
    Denied { reason: String },
    /// Never seen, expired, or already cleaned up.
    #[pack(tag(3))]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ListDelegationsRequest {
    pub credential: AdminCredential,
}

/// One pending or previously-approved delegation request for the admin
/// reviewer. Approved entries remain reviewable so an administrator can run
/// the idempotent reconciliation path after a partial propagation failure. The
/// `proposed_path`, `parent_servers`, and `child_servers` are exactly what the
/// request code fingerprints, so the admin's CLI recomputes the code locally
/// rather than trusting a wire value.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct DelegationEntry {
    pub id: String,
    pub proposed_path: String,
    pub parent_servers: Vec<AdminServerId>,
    pub child_servers: Vec<AdminServerId>,
    /// Current/final resolver cluster IDs resolved by the CA for display.
    pub parent: ResolverClusterId,
    pub child: ResolverClusterId,
    pub parent_base: String,
    pub child_base: String,
    pub parent_members: Vec<ResolverAddr>,
    pub child_members: Vec<ResolverAddr>,
    pub approved: bool,
    pub age_secs: u64,
    pub peer: String,
}

pub type ListDelegationsResponse = RpcResult<Vec<DelegationEntry>>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApproveDelegationRequest {
    pub credential: AdminCredential,
    pub request_id: String,
}

/// The per-peer outcome of propagating a delegation edit across the
/// resolver cluster. A non-`Ok` peer means the resolver cluster is inconsistent
/// until re-synced — the reviewer surfaces it loudly.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct PeerResult {
    pub server: AdminServerId,
    pub addr: SocketAddr,
    /// `None` ⇒ updated; `Some(err)` ⇒ failed (unreachable / rejected).
    pub error: Option<String>,
}

pub type ApproveDelegationResponse = RpcResult<PropagationOk>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct DenyDelegationRequest {
    pub credential: AdminCredential,
    pub request_id: String,
    pub reason: String,
}

pub type DenyDelegationResponse = RpcResult<()>;

/// A referral edit pushed server-to-server for resolver cluster-wide consistency.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum ReferralEdit {
    /// Replace the complete CA-managed topology portion of a resolver config.
    /// Existing member blocks are retained by address (preserving TLS paths,
    /// bind addresses, and tuning); only the selected blocks and referrals are
    /// rewritten. `local_member` is placed first so netidx-admin-managed units
    /// continue to run member index zero.
    #[pack(tag(0))]
    SetTopology {
        local_member: ResolverAddr,
        members: Vec<ResolverAddr>,
        parent: Option<ResolverClusterEdge>,
        children: Vec<ResolverClusterEdge>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplyReferralEditRequest {
    pub operation_id: OperationId,
    pub edit: ReferralEdit,
}

pub type ApplyReferralEditResponse = RpcResult<()>;

/// How clients authenticate to a resolver — the data-plane auth, as
/// opposed to the admin plane, which is always TLS rooted at the CA.
#[derive(Debug, Clone, Serialize, Deserialize, Pack, PartialEq, Eq)]
pub enum InfoAuth {
    #[pack(tag(0))]
    Anonymous,
    #[pack(tag(1))]
    Krb5 { spn: String },
    #[pack(tag(2))]
    Tls { name: String },
}

/// One resolver address with its data-plane auth.
#[derive(Debug, Clone, Serialize, Deserialize, Pack, PartialEq, Eq)]
pub struct ResolverAddr {
    pub addr: SocketAddr,
    pub auth: InfoAuth,
}

/// This host's local facts plus the admin servers it knows of. The
/// client aggregates across servers (mDNS-discovered ∪ peer-walk) to
/// build the admin domain-wide picture; one reachable admin server is enough
/// to walk the rest.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct GetInfoResponse {
    pub domain: String,
    /// Where to send [`Request::Sign`] / [`Request::Enroll`] — self if
    /// this host holds the CA, otherwise its configured location.
    pub ca_addr: Option<SocketAddr>,
    /// This host's resolver, if it runs one.
    pub resolver: Option<ResolverAddr>,
    /// Other admin servers this one knows of.
    pub peers: Vec<SocketAddr>,
}

/// One edge of the resolver hierarchy: a mount path and the resolver cluster it
/// points at. A read-only fact for the admin domain map — distinct from
/// [`ReferralEdit`], which *mutates* a referral during delegation.
#[derive(Debug, Clone, Serialize, Deserialize, Pack, PartialEq, Eq)]
pub struct ResolverClusterEdge {
    pub path: String,
    pub addrs: Vec<ResolverAddr>,
}

/// Resolver facts self-reported by one admin server: its locally configured
/// launch members, where its assigned resolver cluster attaches, and resolver hierarchy edges.
/// The CA derives the authoritative resolver cluster roster from enrolled server
/// ownership; `members` may be only this node or a convenient larger subset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub struct ResolverClusterFacts {
    /// This host's configured advertisable member blocks (`Local` dropped).
    pub members: Vec<ResolverAddr>,
    /// Where this resolver cluster attaches — its parent-referral path, or `/` for
    /// the root resolver cluster.
    pub base: String,
    /// The parent resolver cluster this one attaches under, if any.
    pub parent: Option<ResolverClusterEdge>,
    /// The child resolver clusters delegated below this one.
    pub children: Vec<ResolverClusterEdge>,
    /// Whether this host is currently refusing read clients, as its own
    /// resolver config says. Status, not topology: it is never checked
    /// against the grant and never decides anything.
    pub read_gated: ReadGate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub enum ServerState {
    #[pack(tag(0))]
    Enrolled,
    #[pack(tag(1))]
    Registered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub enum ResolverClusterState {
    #[pack(tag(0))]
    Pending,
    #[pack(tag(1))]
    Active,
}

/// One admin server grant in the CA-owned map. The immutable identity is the
/// key; `addr` is mutable routing data and never serves as identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub struct AdminServerEntry {
    pub id: AdminServerId,
    pub addr: SocketAddr,
    pub roles: BitFlags<Role>,
    /// Resolver endpoint owned by this identity. `None` only for a server
    /// without the Resolver role.
    #[serde(default)]
    pub resolver: Option<ResolverAddr>,
    pub cluster: Option<ResolverClusterId>,
    pub state: ServerState,
    /// The read gate this host last reported from its own resolver config,
    /// or `None` if it runs no resolver or has not reported since the CA
    /// started. Unlike every other field here it is not a grant — the CA
    /// records what the host said it is doing, not what the CA told it to
    /// do, so an operator can see a push that never landed.
    #[serde(default)]
    pub reported_read_gate: Option<ReadGate>,
    /// The id-map model version this host last applied, or `None` if it holds
    /// no id-map or has never applied anything. Like `reported_read_gate` this
    /// is what the host said, not what the CA told it — which is the point:
    /// a version below the CA's model is a host that missed a change, and it
    /// is the CA noticing that, rather than an operator, which repairs it.
    #[serde(default)]
    pub reported_id_map_version: Option<u64>,
    /// The perms model version this member's document is at. Below its
    /// cluster's model means it missed an edit — and, before the CA kept a
    /// model at all, meant it could be read back as the truth for the next
    /// one.
    #[serde(default)]
    pub reported_perms_version: Option<u64>,
    /// The version of the CA-rendered resolver config this host's file is at.
    /// `None` when it runs no resolver, or when the CA has rendered none for
    /// it yet.
    #[serde(default)]
    pub reported_config_version: Option<u64>,
    /// Whether the resolver facts this host last reported disagreed with the
    /// cluster the CA granted it — a stale document, a hand edit, or a config
    /// that would not load at all.
    ///
    /// Recorded rather than refused. The register response is the only thing
    /// that hands a drifting host a correct config, so rejecting the register
    /// would leave it drifting forever; and there is nothing to protect by
    /// rejecting, because a host's reported facts are never adopted into the
    /// map in the first place.
    #[serde(default)]
    pub reported_config_drift: bool,
    /// The version of the config the CA has rendered for this server, against
    /// which `reported_config_version` is compared.
    ///
    /// Kept here rather than only in the CA's store for the same reason
    /// [`ResolverClusterEntry::perms_version`] is: it makes "which servers are
    /// behind" answerable by anyone who can read the map, in one round trip
    /// and with no privileged call.
    #[serde(default)]
    pub config_version: Option<u64>,
}

impl AdminServerEntry {
    /// A server exactly as the CA granted it, having reported nothing back.
    ///
    /// Every `reported_*` field is what a host said about itself, and no host
    /// has said anything at the moment its grant is written. Constructing them
    /// here rather than at each call site keeps that a property of the type:
    /// the only way reported state gets into the map is [`register`], which is
    /// where the rules about what a host may claim live.
    pub fn granted(
        id: AdminServerId,
        addr: SocketAddr,
        roles: BitFlags<Role>,
        resolver: Option<ResolverAddr>,
        cluster: Option<ResolverClusterId>,
        state: ServerState,
    ) -> Self {
        Self {
            id,
            addr,
            roles,
            resolver,
            cluster,
            state,
            reported_read_gate: None,
            reported_id_map_version: None,
            reported_perms_version: None,
            reported_config_version: None,
            reported_config_drift: false,
            config_version: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub struct ResolverClusterEntry {
    pub id: ResolverClusterId,
    pub base: String,
    pub state: ResolverClusterState,
    pub members: Vec<ResolverAddr>,
    pub parent: Option<ResolverClusterId>,
    pub children: Vec<ResolverClusterId>,
    /// The perms version the CA has recorded for this cluster, if any.
    ///
    /// Here rather than only in the CA's model file so that "which members are
    /// behind" is answerable by anyone who can read the map, against the
    /// versions those members report in it — one round trip, no privileged
    /// call. `None` means no perms edit has ever been made for this cluster,
    /// which is not the same as every member being current.
    #[serde(default)]
    pub perms_version: Option<u64>,
}

/// The CA-authoritative, versioned picture of the whole admin domain. The
/// CA builds it from admin-server [`Request::Register`] pushes — never by
/// walking — bumps `version` on every change, persists it, and serves it.
/// Every admin server caches a copy (version-checked) and serves it to
/// clients, so one round trip to any admin server is the whole admin domain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Pack)]
pub struct AdminDomainMap {
    /// Monotonic, bumped by the CA on every change. Callers cheap-compare
    /// this (via [`Request::GetMapVersion`]) before pulling the full map.
    pub version: u64,
    /// The id-map version the CA has recorded, if any. Admin-domain-wide, for
    /// the same reason [`ResolverClusterEntry::perms_version`] is per cluster:
    /// so drift is a question the map answers.
    #[serde(default)]
    pub id_map_version: Option<u64>,
    pub ca: AdminServerId,
    pub admin_servers: Vec<AdminServerEntry>,
    pub resolver_clusters: Vec<ResolverClusterEntry>,
}

impl AdminDomainMap {
    /// The perms version the CA has recorded for `cluster`, if any.
    pub fn perms_version_for(&self, cluster: Option<ResolverClusterId>) -> Option<u64> {
        let cluster = cluster?;
        self.resolver_clusters
            .iter()
            .find(|c| c.id == cluster)
            .and_then(|c| c.perms_version)
    }

    pub fn empty(ca: AdminServerId) -> Self {
        Self {
            version: 0,
            id_map_version: None,
            ca,
            admin_servers: Vec::new(),
            resolver_clusters: Vec::new(),
        }
    }

    pub fn ca_entry(&self) -> Option<&AdminServerEntry> {
        self.admin_servers.iter().find(|s| s.id == self.ca)
    }
}

impl Default for AdminDomainMap {
    fn default() -> Self {
        Self::empty(AdminServerId(Uuid::nil()))
    }
}

/// Server→CA: register/update this admin server's facts in the map.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RegisterRequest {
    pub addr: SocketAddr,
    /// The resolver configuration is evidence checked against the CA grant;
    /// it is never copied wholesale into the authoritative map.
    pub resolver: Option<ResolverClusterFacts>,
    /// The id-map model version this host has applied. `None` from a host
    /// with no id-map role, or one that has never applied an edit.
    #[serde(default)]
    pub id_map_version: Option<u64>,
    /// The perms model version this host's resolver cluster document is at.
    /// One number suffices: a member belongs to exactly one cluster. `None`
    /// from a host with no resolver, or one whose cluster has never had an
    /// edit propagated to it.
    #[serde(default)]
    pub perms_version: Option<u64>,
    /// The version of the CA-rendered resolver config this host has written.
    /// `None` from a host with no resolver, or one installed before the CA
    /// rendered configs.
    #[serde(default)]
    pub config_version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct MapVersion {
    pub version: u64,
}

/// The CA's answer to a register: the map version, and whatever configuration
/// this server turned out to be behind on.
///
/// Attaching it here rather than pushing it is the whole shape of the admin
/// plane. A member already registers every 30s reporting the versions it
/// holds, so the CA already knows what it is missing; sending it back on the
/// same round trip means a member that was down for an edit repairs itself on
/// its own schedule, with the CA needing nothing from it but the poll it was
/// making anyway.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RegisterOk {
    pub map: MapVersion,
    pub updates: DesiredUpdate,
}

/// The parts of a server's desired configuration it does not yet have.
///
/// Each part carries its own version rather than sharing one. They have
/// genuinely different scopes — the id-map is admin-domain-wide, perms are
/// per resolver cluster — so one counter would have to be bumped by fanning
/// out over every affected server on every edit, and a member that applied one
/// part and failed the other could not describe itself.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Pack)]
pub struct DesiredUpdate {
    /// This server's resolver cluster's permissions.
    #[serde(default)]
    pub perms: Option<VersionedPerms>,
    /// The admin domain's id-map.
    #[serde(default)]
    pub id_map: Option<VersionedIdMap>,
    /// This server's whole resolver config, rendered by the CA from the block
    /// this host handed over at enrollment plus its cluster's current
    /// topology.
    #[serde(default)]
    pub config: Option<VersionedResolverConfig>,
}

impl DesiredUpdate {
    pub fn is_empty(&self) -> bool {
        let Self { perms, id_map, config } = self;
        perms.is_none() && id_map.is_none() && config.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct VersionedResolverConfig {
    pub version: u64,
    pub config: netidx::resolver_server::config::file::Config,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct VersionedPerms {
    pub version: u64,
    pub perms: PMap,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct VersionedIdMap {
    pub version: u64,
    pub id_map: IdMap,
}

pub type RegisterResponse = RpcResult<RegisterOk>;
/// Deregistration has no configuration to hand back — the server is leaving.
pub type DeregisterResponse = RpcResult<MapVersion>;
pub type GetMapVersionResponse = RpcResult<MapVersion>;

pub type GetMapResponse = RpcResult<AdminDomainMap>;

/// Admin-authenticated permanent removal of one immutable dead-server identity.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RemoveServerRequest {
    pub credential: AdminCredential,
    pub server: AdminServerId,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RemoveServerOk {
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<OperationId>,
    #[serde(default)]
    pub revoked: u64,
    #[serde(default)]
    pub removed: bool,
    #[serde(default)]
    pub affected_clusters: Vec<String>,
    #[serde(default)]
    pub peers: Vec<PeerResult>,
    /// Immediate CRL-distribution results. Separate from `peers`, which
    /// reports referral-topology reconciliation for the same operation.
    #[serde(default)]
    pub crl_peers: Vec<PeerResult>,
    /// Whether the departing member was told to stop answering read clients.
    ///
    /// Taking it out of everyone's configuration does not stop the clients
    /// that have not synced yet from asking it, and it has no reason to
    /// refuse them. `None` ⇒ it was already gone from the map; otherwise the
    /// result of telling it, so an operator who can see it failed knows to go
    /// and stop that host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gated: Option<PeerResult>,
}

pub type RemoveServerResponse = RpcResult<RemoveServerOk>;

/// This host's permissions file.
pub type GetPermsResponse = RpcResult<PMap>;

/// Admin → ca: read the permissions of the resolver cluster mounted exactly at
/// `target_path`.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ReadPermsRequest {
    pub credential: AdminCredential,
    pub target_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ReadPermsOk {
    pub server: AdminServerId,
    pub addr: SocketAddr,
    pub perms: PMap,
    /// The model version this document is at, to be handed back as
    /// [`EditPermsRequest::base_version`] by whoever edits it.
    ///
    /// `None` is not "unknown" — it is the CA holding no model for this
    /// cluster yet, which is a state an edit can legitimately be based on and
    /// which an edit from someone else can take away.
    #[serde(default)]
    pub version: Option<u64>,
}

pub type ReadPermsResponse = RpcResult<ReadPermsOk>;

/// Admin → CA: replace the `target_path` resolver cluster's permissions.
///
/// The document is a `PMap`, not the JSON of one. JSON is what an operator
/// edits and what the resolver reads off disk; between hosts there is no
/// reason to render a structure both ends already have. A frontend renders it
/// for `$EDITOR` and parses what comes back — that boundary is the only place
/// the text form belongs.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct EditPermsRequest {
    pub credential: AdminCredential,
    /// The base path of the resolver cluster whose perms to edit (e.g. `/eu`).
    pub target_path: String,
    pub perms: PMap,
    /// The version this document was edited from, as
    /// [`ReadPermsOk::version`] reported it.
    ///
    /// A whole document is submitted, so anything the editor did not see is
    /// erased by it. The CA therefore refuses the write unless this still
    /// names the current version, and answers [`EditOutcome::Stale`] with the
    /// document that is current so the caller can rebase.
    ///
    /// `None` means "based on the CA holding no model", which is only current
    /// while that remains true.
    #[serde(default)]
    pub base_version: Option<u64>,
}

/// What an edit records, and the version members will converge on.
///
/// Not a list of per-peer results: nothing is pushed, so there are no peers to
/// report on. An edit that returns is an edit the CA has recorded, and every
/// member reaches it on its own next register. Who has got there yet is a
/// question about the admin domain right now, answered by the versions members
/// report in the map — not by a snapshot taken during the edit, which is stale
/// by the time it prints.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RecordedOk {
    /// The model version this edit produced.
    pub version: u64,
    /// Whether it moved. An edit that asked for something already true leaves
    /// the version where it was, which is not an error and is what makes
    /// re-running a command safe. The CA reports it because the CA is the only
    /// party that can see it without another round trip.
    pub changed: bool,
}

/// What an edit did, or what stopped it.
///
/// A stale edit is not an error: nothing went wrong, the caller simply edited
/// a document someone else has since replaced. It has a typed answer — the
/// document that is current — where an error has only a reason, so the two are
/// different shapes rather than a string the caller would have to parse.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum EditOutcome {
    #[pack(tag(0))]
    Recorded(RecordedOk),
    /// Refused: `base_version` is no longer current, so applying this whole
    /// document would erase whatever changed in between. Carries what is
    /// current, so a caller whose intent survives a rebase can reapply it
    /// without another round trip.
    #[pack(tag(1))]
    Stale { current_version: Option<u64>, current: PMap },
}

pub type EditPermsResponse = RpcResult<EditOutcome>;

/// Server → server: apply a permissions edit to the local resolver perms.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplyPermsEditRequest {
    pub operation_id: OperationId,
    pub perms: PMap,
    /// The model version this document leaves the member at, recorded only on
    /// success. `None` means "this does not by itself make you current",
    /// reserved for a multi-step repair; a perms push is one step, so it
    /// always carries a version.
    pub version: Option<u64>,
}

pub type ApplyPermsEditResponse = RpcResult<()>;

/// One id-map mutation, as an operation.
///
/// This is the operator's verb — what `admin id-map add-user` means, and what
/// the CA applies to its model after it signs an identity. It carries names
/// only, because names are all an id-map holds: see the schema docs on
/// [`netidx_id_map::file::IdMap`].
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub enum IdMapEdit {
    /// Register (or update) `san` with its groups, creating any that are
    /// missing. This is what the CA pushes after it signs an identity, and
    /// what `admin id-map add-user` sends.
    #[pack(tag(0))]
    AddIdentity { san: String, primary_group: String, groups: Vec<String> },
    /// Drop `san` entirely. Its groups are left alone — they may have other
    /// members, and an empty group is not an error.
    #[pack(tag(1))]
    RemoveIdentity { san: String },
    /// Create `name` if absent, allocating a gid. Existing groups keep theirs.
    #[pack(tag(2))]
    AddGroup { name: String },
    /// Remove `name`. Refused by the applier while any identity still has it,
    /// so membership can't dangle.
    #[pack(tag(3))]
    RemoveGroup { name: String },
    /// Add `san` to `group`.
    #[pack(tag(4))]
    AddMember { san: String, group: String },
    /// Remove `san` from `group`. Refused for a primary group — that is what
    /// `RemoveIdentity` is for.
    #[pack(tag(5))]
    RemoveMember { san: String, group: String },
}

impl IdMapEdit {
    /// Every group name this edit names, for the policy check. An operation
    /// that names no group (`RemoveIdentity`) returns empty and is authorized
    /// by the identity's *existing* groups instead — the applier knows those,
    /// the wire message doesn't.
    pub fn groups(&self) -> Vec<&str> {
        match self {
            Self::AddIdentity { primary_group, groups, .. } => {
                let mut v = vec![primary_group.as_str()];
                v.extend(groups.iter().map(|g| g.as_str()));
                v
            }
            Self::AddGroup { name } | Self::RemoveGroup { name } => vec![name.as_str()],
            Self::AddMember { group, .. } | Self::RemoveMember { group, .. } => {
                vec![group.as_str()]
            }
            Self::RemoveIdentity { .. } => vec![],
        }
    }

    /// The identity this edit acts on, if any — checked against the caller's
    /// `allowed_san` so an admin cannot touch a name it could never sign.
    pub fn identity(&self) -> Option<&str> {
        match self {
            Self::AddIdentity { san, .. }
            | Self::RemoveIdentity { san }
            | Self::AddMember { san, .. }
            | Self::RemoveMember { san, .. } => Some(san.as_str()),
            Self::AddGroup { .. } | Self::RemoveGroup { .. } => None,
        }
    }
}

/// Admin → CA: read the receiving host's id-map.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct GetIdMapRequest {
    pub credential: AdminCredential,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct GetIdMapOk {
    pub server: AdminServerId,
    pub addr: SocketAddr,
    pub id_map: IdMap,
}

pub type GetIdMapResponse = RpcResult<GetIdMapOk>;

/// Admin → CA: record `edit` in the admin domain's id-map model. Every host
/// holding the id-map role converges on it at its next register.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct EditIdMapRequest {
    pub credential: AdminCredential,
    pub edit: IdMapEdit,
}

pub type EditIdMapResponse = RpcResult<RecordedOk>;

/// The receiving host's id-map.
pub type GetLocalIdMapResponse = RpcResult<IdMap>;

/// Server → server: apply `edit` to this host's local id-map.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplyIdMapEditRequest {
    pub operation_id: OperationId,
    pub edit: IdMapEdit,
    /// The model version this operation leaves the host at, recorded only on
    /// success — a host that fails to apply stays behind and is picked up by
    /// the next reconcile rather than silently claiming to be current.
    ///
    /// `None` means "this operation does not by itself make you current":
    /// one step of a multi-step repair, where claiming the version after any
    /// single step would let a host that failed halfway look caught up.
    pub version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplyIdMapEditOk {
    /// Whether this host's map changed.
    pub changed: bool,
}

pub type ApplyIdMapEditResponse = RpcResult<ApplyIdMapEditOk>;

// -- remote admin management (over the admin plane) ----------------------------

/// Admin → CA: mint a new role admin `name` with `policy`. The server gates
/// on the caller's `may_manage_admins` (or a signing slot) and enforces that
/// `policy` is a subset of the caller's own (no escalation). `new_password`
/// is the password set on the minted slot (the managing admin conveys it to
/// the satellite); it rides the same TLS-to-the-pinned-CA channel as the
/// caller's own password.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct AddRoleAdminRequest {
    pub credential: AdminCredential,
    pub name: String,
    pub new_password: Secret,
    pub policy: crate::policy::Policy,
    /// Mark the minted slot one-time: `new_password` authorizes nothing but
    /// [`Request::ChangePassword`] until its holder chooses one. Always
    /// `true` from every client there is — an admin who did not pick their
    /// own password should never hold a working one — but the CA reads the
    /// field rather than assuming it, so a client that means otherwise has
    /// to say so.
    pub must_change: bool,
}

/// Admin → CA: replace the **caller's own** password, proving the current one.
/// There is no target field by construction: the slot rekeyed is the one
/// `old_password` authenticates, so this request can never reach another
/// admin's slot. The one request a `must_change` password may send.
///
/// It carries a password rather than an [`AdminCredential`] deliberately, so
/// that no session can authorize it. A token proves only that someone held a
/// terminal when it was minted, hours earlier; a rekey mints a credential that
/// outlives every session and evicts them all, including the real holder's. An
/// inherited session would therefore be a complete, one-way account takeover,
/// which is exactly what a password prompt at the moment of change prevents.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ChangePasswordRequest {
    pub admin: String,
    pub old_password: Secret,
    pub new_password: Secret,
}

/// Admin → CA: replace role admin `target`'s password with a one-time
/// `new_password` the caller generated, and mark the slot `must_change`.
/// Gated on `may_manage_admins` *and* on `target`'s current authority being
/// within the caller's — unlike a rescope or a removal this hands the caller
/// a credential for the target, so a manager who could reset an admin broader
/// than itself could simply become it.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ResetPasswordRequest {
    pub credential: AdminCredential,
    pub target: String,
    pub new_password: Secret,
}

/// Admin → CA: replace role admin `target`'s policy with `policy` (same gate
/// + subset rule as [`AddRoleAdminRequest`]).
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct SetAdminPolicyRequest {
    pub credential: AdminCredential,
    pub target: String,
    pub policy: crate::policy::Policy,
}

/// Admin → CA: remove role admin `target`.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct RemoveAdminRequest {
    pub credential: AdminCredential,
    pub target: String,
}

/// Admin → CA: list the admin roster.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ListAdminsRequest {
    pub credential: AdminCredential,
}

/// Response to add/set/remove admin ops. These are CA-local (no resolver cluster
/// propagation), so there is no peer-result list — just success or a safe
/// reason.
pub type AdminMgmtResponse = RpcResult<()>;

/// Response to [`Request::ListAdmins`]: the roster (each entry carries the
/// admin's name, tier, and full policy — including `may_manage_admins`).
pub type AdminListResponse = RpcResult<Vec<crate::policy::AdminInfo>>;

// -- remote service control (over the admin plane) -----------------------------

/// Admin → CA: control services on **one** admin server (`target_server`, its
/// immutable CA-issued identity). Restart is deliberately per-server, not
/// resolver cluster-wide — an operator restarts one resolver at a time so readers never
/// see a gap. Authorized by the caller's `service_control_scopes` covering that
/// server's resolver cluster base (or a signing slot). `units` empty ⇒ every unit (for
/// [`netidx_activation::control::ControlOp::Status`]); the op + unit names come
/// from the activation control protocol.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ControlServiceRequest {
    pub credential: AdminCredential,
    pub target_server: AdminServerId,
    pub units: Vec<String>,
    pub op: netidx_activation::control::ControlOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ControlServiceOk {
    pub operation_id: OperationId,
    pub units: Vec<ServiceUnit>,
}

pub type ControlServiceResponse = RpcResult<ControlServiceOk>;

/// Server → server: apply a service-control op to this host's local
/// activation supervisor. `units` are the resolved unit names for THIS host.
/// Admin → CA: open or shut one member's read gate.
///
/// A resolver holds only what publishers have told it, so a member that has
/// just joined a cluster serves correct-looking empty answers until every
/// publisher has found it. The gate keeps subscribers away meanwhile — and is
/// equally how a decommissioned member stops answering without being stopped.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct SetReadGateRequest {
    pub credential: AdminCredential,
    pub target_server: AdminServerId,
    pub gate: ReadGate,
}

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct SetReadGateOk {
    pub operation_id: OperationId,
    pub gate: ReadGate,
}

pub type SetReadGateResponse = RpcResult<SetReadGateOk>;

/// CA → node: write the read gate into this host's own resolver config. The
/// resolver applies it live, so it takes effect without a restart, and it
/// survives one because it is in the config.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplySetReadGateRequest {
    pub operation_id: OperationId,
    pub gate: ReadGate,
}

pub type ApplySetReadGateResponse = RpcResult<()>;

#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ApplyServiceControlRequest {
    pub operation_id: OperationId,
    pub units: Vec<String>,
    pub op: netidx_activation::control::ControlOp,
}

pub type ApplyServiceControlResponse = RpcResult<Vec<ServiceUnit>>;

/// A resolver cluster member's unit as reported to the service panel: its live run
/// state plus, when a definition file exists on that member, the display
/// fields the panel shows (so the remote services view has the same
/// list + status + definition layout as the local one). The member fills
/// `definition` from its own unit directory — the operator never has it.
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ServiceUnit {
    pub unit: String,
    pub state: netidx_activation::control::UnitState,
    /// `None` when the supervisor reports a unit with no definition file.
    pub definition: Option<ServiceUnitDef>,
}

/// The display fields of a unit's definition, pre-formatted on the member
/// (matching what the local Services surface shows).
#[derive(Debug, Clone, Serialize, Deserialize, Pack)]
pub struct ServiceUnitDef {
    pub exe: String,
    pub args: Vec<String>,
    pub trigger: String,
    pub restart: String,
}

// -- local control socket (CA box only) ---------------------------------------

/// Response to [`Request::RotateRecovery`]: the freshly minted recovery
/// password, in grouped display form (shown to the operator once, never
/// stored). On the wire it is a [`Secret`] so it is redacted in logs and
/// zeroized after use.
pub type RotateRecoveryResponse = RpcResult<Secret>;

/// Response to [`Request::RotateAutorenew`]: success, optionally with a
/// warning (e.g. the keytab was resealed in plaintext because the existing
/// one was), or a safe failure reason.
pub type RotateAutorenewResponse = RpcResult<Option<String>>;

/// Write one length-prefixed Pack message without flushing.
pub async fn write_msg_unflushed<S, T>(stream: &mut S, msg: &T) -> Result<()>
where
    S: AsyncWrite + Unpin,
    T: Pack,
{
    let len = msg.encoded_len();
    if len as u64 > MAX_MSG as u64 {
        bail!("outgoing message too large ({len} bytes)");
    }
    // Passwords, bearer tokens, and recovery passwords ride these messages,
    // so the encoded plaintext is itself a credential until TLS consumes it.
    let mut body = zeroize::Zeroizing::new(Vec::with_capacity(len));
    msg.encode(&mut *body).context("encoding message")?;
    if body.len() != len {
        bail!("Pack encoded length mismatch: expected {len}, got {}", body.len());
    }
    stream
        .write_all(&(body.len() as u32).to_be_bytes())
        .await
        .context("writing length prefix")?;
    stream.write_all(&body).await.context("writing message body")?;
    Ok(())
}

/// Write one length-prefixed Pack message and flush.
pub async fn write_msg<S, T>(stream: &mut S, msg: &T) -> Result<()>
where
    S: AsyncWrite + Unpin,
    T: Pack,
{
    write_msg_unflushed(stream, msg).await?;
    stream.flush().await.context("flushing message")?;
    Ok(())
}

/// Read exactly one length-prefixed Pack message.
pub async fn read_msg<S, T>(stream: &mut S) -> Result<T>
where
    S: AsyncRead + Unpin,
    T: Pack,
{
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.context("reading length prefix")?;
    let len = u32::from_be_bytes(len);
    if len > MAX_MSG {
        bail!("incoming message length {len} exceeds maximum {MAX_MSG}");
    }
    // The frame may contain a plaintext credential — see `write_msg`.
    let mut body = zeroize::Zeroizing::new(vec![0u8; len as usize]);
    stream.read_exact(&mut body).await.context("reading message body")?;
    let mut body = body.as_slice();
    let msg = T::decode(&mut body).context("decoding message")?;
    if !body.is_empty() {
        bail!("message contains {} trailing bytes", body.len());
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode<T: Pack>(value: &T) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(value.encoded_len());
        value.encode(&mut bytes).unwrap();
        bytes
    }

    #[test]
    fn secret_is_redacted_but_round_trips() {
        let s = Secret("hunter2".to_string());
        assert_eq!(format!("{s:?}"), "Secret(***)");
        let mut encoded = Vec::new();
        s.encode(&mut encoded).unwrap();
        let back = Secret::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(back.0, "hunter2");
    }

    #[test]
    fn protocol_epoch_and_request_tags_have_frozen_pack_bytes() {
        assert_eq!(
            encode(&ClientHello {
                protocol_version: PROTOCOL_VERSION,
                kind: NodeKind::Client,
            }),
            vec![7, 0, 0, 0, 13, 2, 2]
        );
        assert_eq!(encode(&Request::GetMap), vec![2, 23]);
        assert_eq!(encode(&Request::Deregister), vec![2, 21]);
        let apply_crl = encode(&Request::ApplyCrl(ApplyCrlRequest {
            operation_id: OperationId::new(),
            crl_pem: "crl".to_string(),
        }));
        assert_eq!(apply_crl[1], 36);
        let read_perms = encode(&Request::ReadPerms(ReadPermsRequest {
            credential: AdminCredential::password("alice", "pw"),
            target_path: "/eu".to_string(),
        }));
        assert_eq!(read_perms[1], 37);
        let backup =
            encode(&Request::Backup(BackupRequest { target: "/backup/CA".into() }));
        assert_eq!(backup[1], 38);
        let reconcile = encode(&Request::ReconcileCa(ReconcileCaRequest {
            credential: AdminCredential::password("alice", "pw"),
        }));
        assert_eq!(reconcile[1], 39);
        let ca = AdminServerId::new();
        let apply = encode(&Request::ApplyCaState(ApplyCaStateRequest {
            operation_id: OperationId::new(),
            ca,
            addr: "127.0.0.1:4565".parse().unwrap(),
            map: AdminDomainMap::empty(ca),
            crl_pem: "crl".into(),
        }));
        assert_eq!(apply[1], 40);
        assert_eq!(encode(&Request::ExternalCaCsr), vec![2, 41]);
        let external = encode(&Request::ExternalCaInstall(ExternalCaInstallRequest {
            signed_cert_pem: "cert".into(),
            root_pem: Some("root".into()),
        }));
        assert_eq!(external[1], 42);
        assert_eq!(encode(&Request::CaStatus), vec![2, 43]);
    }

    #[tokio::test]
    async fn framing_rejects_oversized_trailing_truncated_and_malformed_messages() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(&(MAX_MSG + 1).to_be_bytes()).await.unwrap();
        let err = read_msg::<_, ClientHello>(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("exceeds maximum"));

        let hello =
            ClientHello { protocol_version: PROTOCOL_VERSION, kind: NodeKind::Client };
        let mut body = encode(&hello);
        body.push(0xff);
        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(&(body.len() as u32).to_be_bytes()).await.unwrap();
        writer.write_all(&body).await.unwrap();
        let err = read_msg::<_, ClientHello>(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("trailing bytes"));

        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(&4u32.to_be_bytes()).await.unwrap();
        writer.write_all(&[1]).await.unwrap();
        writer.shutdown().await.unwrap();
        assert!(read_msg::<_, ClientHello>(&mut reader).await.is_err());

        let (mut writer, mut reader) = tokio::io::duplex(64);
        writer.write_all(&1u32.to_be_bytes()).await.unwrap();
        writer.write_all(&[0]).await.unwrap();
        let err = read_msg::<_, ClientHello>(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("decoding message"));

        let oversized = "x".repeat(MAX_MSG as usize + 1);
        let (mut writer, _reader) = tokio::io::duplex(64);
        let err = write_msg(&mut writer, &oversized).await.unwrap_err();
        assert!(err.to_string().contains("outgoing message too large"));
    }

    #[tokio::test]
    async fn round_trips_over_a_pipe() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let req = Request::Sign(SignRequest {
            kind: NodeKind::Resolver,
            credential: AdminCredential::password("alice", "pw"),
            csr_pem: "CSR".to_string(),
            requested_name: "resolver.example.com".to_string(),
            requested_validity: std::time::Duration::from_secs(365 * 86400),
            id_map_groups: vec!["users".to_string()],
            replaces_serial: None,
        });
        write_msg(&mut a, &req).await.unwrap();
        let got: Request = read_msg(&mut b).await.unwrap();
        let Request::Sign(got) = got else { panic!("expected Sign") };
        assert!(matches!(
            got.credential,
            AdminCredential::Password { admin, password }
                if admin == "alice" && password.0 == "pw"
        ));
        assert_eq!(got.requested_name, "resolver.example.com");
        assert_eq!(got.kind, NodeKind::Resolver);
        assert_eq!(got.id_map_groups, vec!["users".to_string()]);

        let server = AdminServerId::new();
        let perms: PMap =
            serde_json::from_str(r#"{"/eu":{"alice":"swlpd","":"sl"},"/":{"bob":"!s"}}"#)
                .unwrap();
        let response = ReadPermsResponse::Ok(ReadPermsOk {
            server,
            addr: "127.0.0.1:4565".parse().unwrap(),
            perms: perms.clone(),
            version: Some(9),
        });
        write_msg(&mut a, &response).await.unwrap();
        match read_msg::<_, ReadPermsResponse>(&mut b).await.unwrap() {
            ReadPermsResponse::Ok(ReadPermsOk {
                server: got,
                addr,
                perms: got_perms,
                version,
            }) => {
                assert_eq!(got, server);
                assert_eq!(addr, "127.0.0.1:4565".parse().unwrap());
                assert_eq!(got_perms, perms);
                // The version rides with the document it describes: an editor
                // that got one without the other could not tell the CA what it
                // based on.
                assert_eq!(version, Some(9));
            }
            ReadPermsResponse::Err { reason } => panic!("unexpected error: {reason}"),
        }
        // An empty document is a real state — a resolver cluster with no
        // grants — and must not decode as an error or a truncation.
        write_msg(&mut a, &GetPermsResponse::Ok(PMap::default())).await.unwrap();
        match read_msg::<_, GetPermsResponse>(&mut b).await.unwrap() {
            GetPermsResponse::Ok(got) => assert_eq!(got, PMap::default()),
            GetPermsResponse::Err { reason } => panic!("unexpected error: {reason}"),
        }
    }

    #[tokio::test]
    async fn issued_entries_are_independent_frames_with_an_explicit_end() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        for serial in [41, 42] {
            write_msg_unflushed(
                &mut writer,
                &ListIssuedResponse::Entry {
                    entry: IssuedEntry {
                        serial,
                        name: format!("node-{serial}.example"),
                        spki_fp: format!("fp-{serial}"),
                        not_after_unix: 1234,
                        revoked: false,
                    },
                },
            )
            .await
            .unwrap();
        }
        write_msg(&mut writer, &ListIssuedResponse::End).await.unwrap();

        for serial in [41, 42] {
            let ListIssuedResponse::Entry { entry } =
                read_msg::<_, ListIssuedResponse>(&mut reader).await.unwrap()
            else {
                panic!("expected issued entry")
            };
            assert_eq!(entry.serial, serial);
        }
        assert!(matches!(
            read_msg::<_, ListIssuedResponse>(&mut reader).await.unwrap(),
            ListIssuedResponse::End
        ));
    }

    #[tokio::test]
    async fn issued_stream_can_exceed_the_single_frame_limit() {
        let response = ListIssuedResponse::Entry {
            entry: IssuedEntry {
                serial: 1,
                name: "n".repeat(1024),
                spki_fp: "fp".to_string(),
                not_after_unix: 1234,
                revoked: false,
            },
        };
        let frame_len = response.encoded_len() + 4;
        let count = MAX_MSG as usize / frame_len + 2;
        assert!(frame_len * count > MAX_MSG as usize);
        let (mut writer, mut reader) = tokio::io::duplex(8192);

        let send = async {
            for _ in 0..count {
                write_msg_unflushed(&mut writer, &response).await.unwrap();
            }
            write_msg(&mut writer, &ListIssuedResponse::End).await.unwrap();
        };
        let receive = async {
            for _ in 0..count {
                assert!(matches!(
                    read_msg::<_, ListIssuedResponse>(&mut reader).await.unwrap(),
                    ListIssuedResponse::Entry { .. }
                ));
            }
            assert!(matches!(
                read_msg::<_, ListIssuedResponse>(&mut reader).await.unwrap(),
                ListIssuedResponse::End
            ));
        };
        tokio::join!(send, receive);
    }

    #[tokio::test]
    async fn delegation_messages_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let parent_id = AdminServerId::new();
        let child_id = AdminServerId::new();
        let req = Request::RequestDelegation(DelegationRequest {
            proposed_path: "/ap".into(),
            parent_servers: vec![parent_id],
            child_servers: vec![child_id],
        });
        write_msg(&mut a, &req).await.unwrap();
        let got: Request = read_msg(&mut b).await.unwrap();
        let Request::RequestDelegation(got) = got else {
            panic!("expected RequestDelegation")
        };
        assert_eq!(got.proposed_path, "/ap");
        assert_eq!(got.parent_servers, vec![parent_id]);
        assert_eq!(got.child_servers, vec![child_id]);

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

        // A topology fanout carries the complete CA-owned roster and edges.
        let local_member = ResolverAddr {
            addr: "10.0.0.2:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        };
        let edit = Request::ApplyReferralEdit(ApplyReferralEditRequest {
            operation_id: OperationId::new(),
            edit: ReferralEdit::SetTopology {
                local_member: local_member.clone(),
                members: vec![local_member.clone()],
                parent: Some(ResolverClusterEdge {
                    path: "/eu".to_string(),
                    addrs: vec![ResolverAddr {
                        addr: "10.0.0.1:4564".parse().unwrap(),
                        auth: InfoAuth::Krb5 { spn: "svc/r@EU".to_string() },
                    }],
                }),
                children: vec![],
            },
        });
        write_msg(&mut a, &edit).await.unwrap();
        let Request::ApplyReferralEdit(got) = read_msg(&mut b).await.unwrap() else {
            panic!("expected ApplyReferralEdit")
        };
        match got.edit {
            ReferralEdit::SetTopology { local_member: got, parent, .. } => {
                assert_eq!(got, local_member);
                let parent = parent.unwrap();
                assert_eq!(parent.path, "/eu");
                assert_eq!(parent.addrs.len(), 1);
                assert_eq!(
                    parent.addrs[0].auth,
                    InfoAuth::Krb5 { spn: "svc/r@EU".to_string() }
                );
            }
        }
    }

    #[test]
    fn sign_request_without_groups_still_decodes() {
        // A request written before `id_map_groups` existed must decode
        // — the field is `#[serde(default)]`.
        let json = r#"{
            "kind": "Client",
            "credential": {"Password":{"admin":"alice","password":"pw"}},
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
            roles: Role::Ca | Role::Resolver | Role::IdMap,
            server_id: AdminServerId::new(),
            ca: true,
        };
        write_msg(&mut a, &hello).await.unwrap();
        let got: ServerHello = read_msg(&mut b).await.unwrap();
        assert_eq!(got.domain, "ryu-oh.org");
        assert_eq!(got.roles, Role::Ca | Role::Resolver | Role::IdMap);
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
        let SignResponse::Ok(SignOk { warnings, .. }) = resp else {
            panic!("expected Ok")
        };
        assert!(warnings.is_empty());
    }

    #[tokio::test]
    async fn map_messages_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let req = Request::Register(RegisterRequest {
            config_version: None,
            addr: "10.0.0.2:4565".parse().unwrap(),
            resolver: Some(ResolverClusterFacts {
                members: vec![ResolverAddr {
                    addr: "10.0.0.2:4564".parse().unwrap(),
                    auth: InfoAuth::Anonymous,
                }],
                base: "/eu".to_string(),
                parent: Some(ResolverClusterEdge {
                    path: "/eu".to_string(),
                    addrs: vec![ResolverAddr {
                        addr: "10.0.0.1:4564".parse().unwrap(),
                        auth: InfoAuth::Anonymous,
                    }],
                }),
                children: vec![],
                read_gated: ReadGate::No,
            }),
            id_map_version: Some(9),
            perms_version: Some(3),
        });
        write_msg(&mut a, &req).await.unwrap();
        let got: Request = read_msg(&mut b).await.unwrap();
        let Request::Register(got) = got else { panic!("expected Register") };
        assert_eq!(got.addr, "10.0.0.2:4565".parse().unwrap());
        assert_eq!(got.resolver.unwrap().base, "/eu");
        assert_eq!(got.id_map_version, Some(9));
        assert_eq!(got.perms_version, Some(3));

        let ca = AdminServerId::new();
        let resp = GetMapResponse::Ok(AdminDomainMap {
            id_map_version: None,
            version: 7,
            ca,
            admin_servers: vec![AdminServerEntry {
                id: ca,
                addr: "10.0.0.1:4565".parse().unwrap(),
                roles: Role::Ca | Role::Resolver,
                resolver: None,
                cluster: None,
                state: ServerState::Registered,
                reported_read_gate: None,
                reported_id_map_version: Some(9),
                reported_perms_version: None,
                reported_config_version: Some(4),
                reported_config_drift: true,
                config_version: Some(5),
            }],
            resolver_clusters: vec![],
        });
        write_msg(&mut a, &resp).await.unwrap();
        match read_msg::<_, GetMapResponse>(&mut b).await.unwrap() {
            GetMapResponse::Ok(map) => {
                assert_eq!(map.version, 7);
                assert_eq!(map.admin_servers.len(), 1);
                assert_eq!(map.admin_servers[0].roles, Role::Ca | Role::Resolver);
            }
            GetMapResponse::Err { reason } => panic!("err: {reason}"),
        }

        let operation_id = OperationId::new();
        let server = AdminServerId::new();
        let removal = RemoveServerResponse::Ok(RemoveServerOk {
            version: 8,
            operation_id: Some(operation_id),
            revoked: 2,
            removed: true,
            affected_clusters: vec!["/".to_string(), "/eu".to_string()],
            peers: vec![PeerResult {
                server,
                addr: "10.0.0.2:4565".parse().unwrap(),
                error: Some("offline".to_string()),
            }],
            crl_peers: vec![],
            gated: Some(PeerResult {
                server,
                addr: "10.0.0.2:4565".parse().unwrap(),
                error: None,
            }),
        });
        write_msg(&mut a, &removal).await.unwrap();
        match read_msg::<_, RemoveServerResponse>(&mut b).await.unwrap() {
            RemoveServerResponse::Ok(RemoveServerOk {
                version,
                operation_id: got_operation,
                revoked,
                removed,
                affected_clusters,
                peers,
                crl_peers,
                gated,
            }) => {
                assert_eq!(version, 8);
                assert_eq!(got_operation, Some(operation_id));
                assert_eq!(revoked, 2);
                assert!(removed);
                assert_eq!(affected_clusters, vec!["/", "/eu"]);
                assert_eq!(peers[0].server, server);
                assert_eq!(peers[0].error.as_deref(), Some("offline"));
                assert!(crl_peers.is_empty());
                let gated = gated.expect("gate result round trips");
                assert_eq!(gated.server, server);
                assert_eq!(gated.error, None);
            }
            RemoveServerResponse::Err { reason } => panic!("err: {reason}"),
        }

        let operation_id = OperationId::new();
        let peer =
            PeerResult { server, addr: "10.0.0.2:4565".parse().unwrap(), error: None };
        let response = RevokeResponse::Ok(RevokeOk {
            warnings: vec![],
            operation_id: Some(operation_id),
            peers: vec![peer],
        });
        write_msg(&mut a, &response).await.unwrap();
        match read_msg::<_, RevokeResponse>(&mut b).await.unwrap() {
            RevokeResponse::Ok(RevokeOk { operation_id: got, peers, .. }) => {
                assert_eq!(got, Some(operation_id));
                assert_eq!(peers[0].server, server);
            }
            RevokeResponse::Err { reason } => panic!("err: {reason}"),
        }
    }

    /// The id-map crosses the wire as itself, not as a JSON document. It used
    /// to be a `String`, which meant the CA parsed a host's map back out of
    /// text before it could diff it — a serializer, a parser, and a schema
    /// that only the two of them agreed on, for a structure both already had.
    #[tokio::test]
    async fn the_id_map_crosses_the_wire_as_itself() {
        use netidx_id_map::file::Identity;
        let mut map = IdMap::default();
        map.groups.insert("users".into());
        map.groups.insert("wheel".into());
        map.identities.insert(
            "alice.example.com".into(),
            Identity { primary_group: "users".into(), groups: vec!["wheel".into()] },
        );
        let (mut a, mut b) = tokio::io::duplex(4096);
        let server = AdminServerId::new();
        let addr: SocketAddr = "10.0.0.2:4565".parse().unwrap();
        write_msg(
            &mut a,
            &GetIdMapResponse::Ok(GetIdMapOk { server, addr, id_map: map.clone() }),
        )
        .await
        .unwrap();
        match read_msg::<_, GetIdMapResponse>(&mut b).await.unwrap() {
            GetIdMapResponse::Ok(ok) => {
                assert_eq!(ok.server, server);
                assert_eq!(ok.id_map, map);
            }
            GetIdMapResponse::Err { reason } => panic!("err: {reason}"),
        }
        write_msg(&mut a, &GetLocalIdMapResponse::Ok(map.clone())).await.unwrap();
        match read_msg::<_, GetLocalIdMapResponse>(&mut b).await.unwrap() {
            GetLocalIdMapResponse::Ok(got) => assert_eq!(got, map),
            GetLocalIdMapResponse::Err { reason } => panic!("err: {reason}"),
        }
        // An empty map is a real state — a host whose id-map daemon has never
        // registered anyone — and must not decode as an error or a truncation.
        let empty = IdMap::default();
        write_msg(&mut a, &GetLocalIdMapResponse::Ok(empty.clone())).await.unwrap();
        match read_msg::<_, GetLocalIdMapResponse>(&mut b).await.unwrap() {
            GetLocalIdMapResponse::Ok(got) => assert_eq!(got, empty),
            GetLocalIdMapResponse::Err { reason } => panic!("err: {reason}"),
        }
    }

    #[test]
    fn cluster_facts_root_decodes() {
        // A root resolver cluster reports base "/" and no parent/children.
        // `read_gated` is required, not defaulted: facts are always built from
        // a config the sender just read, so a missing gate means the sender is
        // not reporting one — and defaulting that to "open" would be the map
        // asserting something nobody said.
        let json = r#"{"members":[{"addr":"10.0.0.1:4564","auth":"Anonymous"}],"base":"/","parent":null,"children":[],"read_gated":"No"}"#;
        let cf: ResolverClusterFacts = serde_json::from_str(json).unwrap();
        assert_eq!(cf.base, "/");
        assert!(cf.parent.is_none());
        assert!(cf.children.is_empty());
        assert_eq!(cf.members.len(), 1);
        assert_eq!(cf.read_gated, ReadGate::No);
    }
}
