//! Admin-server client: discover what a netidx admin domain looks like, join
//! it (generate a key + CSR locally and request a signature over TLS),
//! and enroll new admin servers — verifying the admin domain's CA identity by
//! fingerprint before sending anything secret.
//!
//! Cross-platform — rcgen + rustls + sha2 + x509-parser, never openssl
//! — so a Windows node can talk to a unix admin server.
//!
//! ## Trust model
//!
//! Separate connections for inspection and for anything secret, so the
//! operator never types a password before seeing — and confirming — who
//! they're talking to, and so human think-time never holds a connection
//! (or the server's request timeout) open:
//!
//! 1. **Inspect** ([`fetch_identity`]): a TOFU handshake — a custom
//!    verifier accepts whatever cert the daemon presents but still
//!    validates the handshake signature, so the daemon proves it holds
//!    the serving key. From the presented chain (`[serving_leaf, …, CA]`)
//!    the client takes the CA cert, verifies the serving leaf is signed
//!    by it and carries the reserved [`SERVING_SAN`], exchanges hellos
//!    (the server's claimed domain + roles), and returns the CA's
//!    [`Fingerprint`]. The connection is then closed; nothing secret was
//!    sent.
//! 2. The operator compares the fingerprint out of band and, only if it
//!    matches, proceeds.
//! 3. **Everything else** ([`get_info`], [`request_cert`], [`enroll`]):
//!    connections that **pin** the authenticated CA cert to the confirmed
//!    fingerprint, aborting before sending anything secret if it changed.
//!    Later connections to the same endpoint resume the authenticated TLS
//!    session when possible; a full handshake is automatic when resumption
//!    is unavailable.

use crate::{
    admin_proto::{
        self, AddRoleAdminRequest, AdminDomainMap, AdminListResponse, AdminMgmtResponse,
        ApplyCaStateRequest, ApplyCaStateResponse, ApplyCrlRequest, ApplyCrlResponse,
        ApplyPermsEditRequest, ApplyPermsEditResponse, ApplyReferralEditRequest,
        ApplyReferralEditResponse, ApplyServiceControlRequest,
        ApplyServiceControlResponse, ApproveDelegationRequest, ApproveDelegationResponse,
        ApproveOk, ApproveRequest, ApproveResponse, ClientHello, ControlServiceOk,
        ControlServiceRequest, ControlServiceResponse, DelegationEntry,
        DelegationPollResponse, DelegationRequest, DelegationResponse,
        DenyDelegationRequest, DenyDelegationResponse, DenyRequest, DenyResponse,
        EditPermsRequest, EditPermsResponse, EnqueueRequest, EnqueueResponse,
        EnrollRequest, GetInfoResponse, GetMapResponse, GetMapVersionResponse,
        GetPermsResponse, IssuedEntry, ListAdminsRequest, ListDelegationsRequest,
        ListDelegationsResponse, ListIssuedRequest, ListIssuedResponse, ListQueueRequest,
        ListQueueResponse, LoginOk, MapVersion, NodeKind, PROTOCOL_VERSION, PeerResult,
        PollRequest, PollResponse, PropagationOk, QueueEntry, QueuedOk, ReadPermsOk,
        ReadPermsRequest, ReadPermsResponse, ReconcileCaRequest, ReconcileCaResponse,
        ReferralEdit, RegisterRequest, RegisterResponse, RemoveAdminRequest,
        RemoveServerOk, RemoveServerRequest, RemoveServerResponse, Request, ResolverAddr,
        RevokeOk, RevokeRequest, RevokeResponse, Role, SERVING_SAN, Secret, ServerHello,
        SetAdminPolicyRequest, SignOk, SignRequest, SignResponse,
    },
    fingerprint::Fingerprint,
    id_map::IdMap,
    tls_tofu::TofuVerifier,
};
use anyhow::{Context, Result, anyhow, bail};
use enumflags2::BitFlags;
use log::warn;
use parking_lot::Mutex;
use poolshark::local::LPooled;
use rustls::{ClientConfig, client::Resumption, crypto::CryptoProvider};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use zeroize::Zeroizing;

/// A black-holed candidate must not hold discovery, enrollment, renewal, or
/// administration on the operating system's multi-minute TCP timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TLS_ENDPOINTS: usize = 256;
const TLS_SESSIONS_PER_ENDPOINT: usize = 16;

static TLS_PROVIDER: LazyLock<Arc<CryptoProvider>> =
    LazyLock::new(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));

#[derive(Clone)]
struct TlsClient(Arc<TlsClientInner>);

struct TlsClientInner {
    template: ClientConfig,
    endpoints: Mutex<VecDeque<TlsEndpoint>>,
}

struct TlsEndpoint {
    addr: SocketAddr,
    config: Arc<ClientConfig>,
}

impl TlsClient {
    fn new(mut template: ClientConfig) -> Self {
        template.resumption = Resumption::disabled();
        template.enable_early_data = false;
        Self(Arc::new(TlsClientInner {
            template,
            endpoints: Mutex::new(VecDeque::new()),
        }))
    }

    fn connector(&self, addr: SocketAddr) -> TlsConnector {
        let TlsClientInner { template, endpoints } = &*self.0;
        let mut endpoints = endpoints.lock();
        if let Some(i) = endpoints.iter().position(|cached| cached.addr == addr) {
            let TlsEndpoint { addr, config } =
                endpoints.remove(i).expect("position came from iter");
            endpoints.push_back(TlsEndpoint { addr, config: config.clone() });
            return TlsConnector::from(config);
        }
        let mut config = template.clone();
        config.resumption = Resumption::in_memory_sessions(TLS_SESSIONS_PER_ENDPOINT);
        let config = Arc::new(config);
        if endpoints.len() == MAX_TLS_ENDPOINTS {
            endpoints.pop_front();
        }
        endpoints.push_back(TlsEndpoint { addr, config: config.clone() });
        TlsConnector::from(config)
    }
}

/// A reusable PKI client without a TLS client identity.
#[derive(Clone)]
pub struct PkiClient(TlsClient);

impl PkiClient {
    pub fn new(roots: rustls::RootCertStore) -> Result<Self> {
        let config = ClientConfig::builder_with_provider(TLS_PROVIDER.clone())
            .with_safe_default_protocol_versions()
            .context("selecting TLS versions")?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self(TlsClient::new(config)))
    }
}

/// A reusable PKI client that presents a TLS client identity.
#[derive(Clone)]
pub struct AuthenticatedPkiClient(TlsClient);

impl AuthenticatedPkiClient {
    pub fn new(
        roots: rustls::RootCertStore,
        cert_pem: &[u8],
        key: PrivateKeyDer<'static>,
    ) -> Result<Self> {
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
                .collect::<std::result::Result<_, _>>()
                .context("parsing client certificate chain")?;
        let config = ClientConfig::builder_with_provider(TLS_PROVIDER.clone())
            .with_safe_default_protocol_versions()
            .context("selecting TLS versions")?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .context("building client TLS config")?;
        Ok(Self(TlsClient::new(config)))
    }

    pub fn from_pem(
        roots: rustls::RootCertStore,
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<Self> {
        let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key_pem))
            .context("parsing client key")?
            .ok_or_else(|| anyhow!("no private key found in client key PEM"))?;
        Self::new(roots, cert_pem, key)
    }
}

trait HasTlsClient {
    fn tls_client(&self) -> &TlsClient;
}

impl HasTlsClient for PkiClient {
    fn tls_client(&self) -> &TlsClient {
        &self.0
    }
}

impl HasTlsClient for AuthenticatedPkiClient {
    fn tls_client(&self) -> &TlsClient {
        &self.0
    }
}

/// A freshly generated leaf identity awaiting signature.
pub use crate::csr::KeyAndCsr;

/// The result of a successful join or enrollment: the signed leaf, the
/// matching private key, and the full trusted-CA bundle to install.
/// Everything the node needs — no manual file copying.
pub struct Issued {
    pub cert_pem: String,
    pub private_key_pem: Zeroizing<String>,
    pub trusted_pem: String,
    /// Non-fatal follow-up failures reported by the server (e.g. an
    /// id-map host it couldn't reach). Show them to the operator.
    pub warnings: Vec<String>,
}

/// Generate a key and a CSR for `name` — a single DNS SAN equal to the CN,
/// which is the shape every enrollment uses.
pub fn generate_key_and_csr(name: &str) -> Result<KeyAndCsr> {
    use crate::csr::{SanEntry, Subject};
    crate::csr::generate_key_and_csr(
        &Subject::cn(name),
        &[SanEntry::Dns(name.to_string())],
    )
}

/// An admin server's verified identity, captured by [`fetch_identity`]:
/// the CA [`Fingerprint`] to show the operator out of band, the CA
/// certificate it's bound to, and the domain + roles the server claimed
/// inside TLS. Pass it to [`get_info`] / [`request_cert`] / [`enroll`]
/// to pin those connections to exactly the CA the operator confirmed.
#[derive(Clone)]
pub struct CaIdentity {
    /// SHA-256 of the CA cert, for out-of-band comparison (text +
    /// identicon).
    pub fingerprint: Fingerprint,
    /// The admin domain's TLS domain, as claimed in the server hello.
    pub domain: String,
    /// The roles the contacted host claimed in the server hello.
    pub roles: BitFlags<Role>,
    pub server_id: admin_proto::AdminServerId,
    pub ca: bool,
    /// The CA cert the fingerprint is of, kept so later connections can
    /// verify against the *confirmed* CA rather than a re-presented one.
    ca_der: CertificateDer<'static>,
    tls_client: TlsClient,
}

impl CaIdentity {
    /// The confirmed CA certificate as PEM — for building serving-cert
    /// chains (`[leaf, CA]`) after an [`enroll`].
    pub fn ca_pem(&self) -> String {
        der_to_pem(self.ca_der.as_ref())
    }

    pub fn ca_certificate(&self) -> CertificateDer<'static> {
        self.ca_der.clone()
    }
}

fn der_to_pem(der: &[u8]) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).expect("base64 is ascii"));
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

/// The certificate in `bundle` that actually signed `leaf_pem`, as PEM
/// — for rebuilding serving-style chains (`[leaf, CA]`) after a
/// renewal: the renewal response carries the bare leaf plus the trust
/// bundle, and a federated bundle may hold several CAs.
pub fn issuing_ca_pem(bundle: &str, leaf_pem: &str) -> Result<String> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let leaf_der = pem_to_der(leaf_pem, "CERTIFICATE")?;
    let (_, leaf) =
        X509Certificate::from_der(&leaf_der).map_err(|e| anyhow!("parsing leaf: {e}"))?;
    let mut rd = std::io::Cursor::new(bundle.as_bytes());
    for der in rustls_pemfile::certs(&mut rd).flatten() {
        if let Ok((_, ca)) = X509Certificate::from_der(der.as_ref())
            && leaf.verify_signature(Some(ca.public_key())).is_ok()
        {
            return Ok(der_to_pem(der.as_ref()));
        }
    }
    bail!("no certificate in the bundle signed this leaf")
}

/// Verify `crl_pem` is exactly one CRL, signed by one of `cas_der`.
///
/// A CRL that arrives over the network must be verified before it is
/// installed. The resolver's TLS acceptor rebuilds from the `crl.pem` beside
/// its trust bundle, and rustls refuses to build a config from a CRL it can't
/// use — so writing an unverified payload turns any peer that can answer
/// `GetCrl` into an authentication outage for the host that pulled it.
///
/// Unparseable entries in `cas_der` are skipped, not fatal: a federated bundle
/// is only as good as its worst entry, and one bad anchor must not stop a
/// legitimate CRL from being verified against a good one.
pub fn validate_crl_signed_by_any<'a>(
    crl_pem: &str,
    cas_der: impl IntoIterator<Item = &'a [u8]>,
) -> Result<()> {
    use x509_parser::prelude::{CertificateRevocationList, FromDer, X509Certificate};
    let crls = rustls_pemfile::crls(&mut std::io::Cursor::new(crl_pem.as_bytes()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("parsing CRL PEM")?;
    let [der] = crls.as_slice() else {
        bail!("expected exactly one CRL, got {}", crls.len());
    };
    let (remaining, crl) = CertificateRevocationList::from_der(der.as_ref())
        .map_err(|e| anyhow!("parsing CRL DER: {e}"))?;
    if !remaining.is_empty() {
        bail!("CRL DER contains trailing bytes");
    }
    let mut tried = 0;
    for ca_der in cas_der {
        let Ok((remaining, ca)) = X509Certificate::from_der(ca_der) else { continue };
        if !remaining.is_empty() {
            continue;
        }
        tried += 1;
        if crl.verify_signature(ca.public_key()).is_ok() {
            return Ok(());
        }
    }
    bail!("the CRL is not signed by any of the {tried} trusted certificate authorities")
}

/// [`validate_crl_signed_by_any`] against every CA in an installed trust
/// bundle — the rule [`verify_issued`] uses for leaves, applied to CRLs.
pub fn validate_crl_against_bundle(crl_pem: &str, bundle_pem: &str) -> Result<()> {
    let cas: LPooled<Vec<_>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(bundle_pem.as_bytes()))
            .flatten()
            .collect();
    validate_crl_signed_by_any(crl_pem, cas.iter().map(|der| der.as_ref()))
}

/// TOFU-handshake to the admin server at `addr`; return the live TLS
/// stream and the certificate chain it presented
/// (`[serving_leaf, …, CA]`). Accepts any cert — trust is established
/// out of band by fingerprint — but the handshake signature is still
/// verified, so the server proves it holds the key for the cert it
/// shows.
async fn connect_tofu(
    addr: SocketAddr,
    client: &TlsClient,
) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, Vec<CertificateDer<'static>>)> {
    let connector = client.connector(addr);
    let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .with_context(|| format!("timed out connecting to admin server {addr}"))?
        .with_context(|| format!("connecting to admin server {addr}"))?;
    let server_name = ServerName::try_from(SERVING_SAN).context("server name")?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake with admin server")?;
    let chain: Vec<CertificateDer<'static>> = {
        let (_io, conn) = tls.get_ref();
        conn.peer_certificates()
            .ok_or_else(|| anyhow!("admin server presented no certificate"))?
            .iter()
            .map(|c| c.clone().into_owned())
            .collect()
    };
    Ok((tls, chain))
}

/// Split a presented chain into `(serving_leaf, CA)`. The daemon's
/// serving chain is `[serving_leaf, …, CA]`; we need at least the leaf
/// and the CA.
fn split_chain<'a>(
    chain: &'a [CertificateDer<'static>],
) -> Result<(&'a CertificateDer<'static>, &'a CertificateDer<'static>)> {
    if chain.len() < 2 {
        bail!(
            "admin server did not present its CA certificate in the chain \
             (got {} cert(s)); the daemon's serving chain must be [leaf, …, ca]",
            chain.len()
        );
    }
    Ok((&chain[0], chain.last().unwrap()))
}

/// Exchange hellos on an established connection. Returns the server's
/// hello after checking the protocol version.
async fn exchange_hello(
    tls: &mut tokio_rustls::client::TlsStream<TcpStream>,
    kind: NodeKind,
) -> Result<ServerHello> {
    admin_proto::write_msg(
        tls,
        &ClientHello { protocol_version: PROTOCOL_VERSION, kind },
    )
    .await?;
    let hello: ServerHello = admin_proto::read_msg(tls).await?;
    if hello.protocol_version != PROTOCOL_VERSION {
        bail!(
            "admin server speaks protocol version {} but we speak {PROTOCOL_VERSION}",
            hello.protocol_version
        );
    }
    Ok(hello)
}

/// Connect to the admin server at `addr`, TOFU-handshake, exchange
/// hellos, and return its verified identity — **sending nothing
/// secret**. The connection is closed before returning, so the operator
/// can compare the fingerprint and enter credentials at their own pace
/// without holding a connection (and the server's request timeout)
/// open. The serving cert is verified to be bound to the returned CA,
/// so the fingerprint shown really is the authority backing this
/// daemon; the domain + roles come from inside the TLS session.
///
/// The operator confirms the returned [`CaIdentity`] out of band, then
/// hands it to [`get_info`] / [`request_cert`] / [`enroll`], which pin
/// their connections to it.
async fn fetch_identity_with(
    addr: SocketAddr,
    kind: NodeKind,
    tls_client: TlsClient,
) -> Result<CaIdentity> {
    let (mut tls, chain) = connect_tofu(addr, &tls_client).await?;
    let (serving_der, ca_der) = split_chain(&chain)?;
    let cert_identity = verify_serving_cert(serving_der, ca_der).context(
        "the admin server's serving certificate is not bound to the CA it presented",
    )?;
    let hello = exchange_hello(&mut tls, kind).await?;
    if hello.server_id != cert_identity.server_id || hello.ca != cert_identity.ca {
        bail!("admin server hello identity does not match its certificate");
    }
    drop(tls);
    Ok(CaIdentity {
        // The glyph is of the CA's *key* (SPKI), not the cert — stable
        // across same-key CA renewals.
        fingerprint: Fingerprint::of_cert_der(ca_der.as_ref())
            .context("fingerprinting the presented CA certificate")?,
        domain: hello.domain,
        roles: hello.roles,
        server_id: cert_identity.server_id,
        ca: cert_identity.ca,
        ca_der: ca_der.clone(),
        tls_client,
    })
}

pub async fn fetch_identity(addr: SocketAddr, kind: NodeKind) -> Result<CaIdentity> {
    let config = ClientConfig::builder_with_provider(TLS_PROVIDER.clone())
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TofuVerifier::new(
            TLS_PROVIDER.clone(),
        )))
        .with_no_client_auth();
    fetch_identity_with(addr, kind, TlsClient::new(config)).await
}

/// Connect, pin the presented CA to the identity the operator already
/// confirmed, verify the serving cert against it, and exchange hellos.
/// Every post-confirmation operation starts here; a server that swapped
/// its cert since [`fetch_identity`] is rejected before anything is
/// sent beyond the (secret-free) client hello.
async fn connect_pinned(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let (mut tls, chain) = connect_tofu(addr, &expected.tls_client).await?;
    let (serving_der, presented_ca) = split_chain(&chain)?;
    // An unparseable CA cert is treated as a mismatch — fail closed.
    let presented_fp = Fingerprint::of_cert_der(presented_ca.as_ref()).ok();
    if presented_fp != Some(expected.fingerprint) {
        bail!(
            "the admin server's identity changed since you confirmed it \
             (fingerprint mismatch); aborted before sending anything"
        );
    }
    // Everything downstream binds to the *confirmed* CA, not the
    // re-presented one (identical given the pin passed, but this makes
    // the trust anchor explicit).
    let cert_identity = verify_serving_cert(serving_der, &expected.ca_der).context(
        "the admin server's serving certificate failed verification against the \
         confirmed CA — this is not the admin domain you confirmed",
    )?;
    let hello = exchange_hello(&mut tls, kind).await?;
    if hello.server_id != cert_identity.server_id || hello.ca != cert_identity.ca {
        bail!("admin server hello identity does not match its certificate");
    }
    Ok(tls)
}

/// Resolve and verify the one active CA without sending a credential.
/// Bootstrap maps and peer lists are hints only; the returned connection is
/// pinned to the same home CA and requires the CA URI in the serving
/// certificate.
async fn connect_ca_pinned(
    bootstrap: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let map = get_map_pinned(bootstrap, kind, expected).await?;
    let ca = map
        .ca_entry()
        .filter(|s| s.state == admin_proto::ServerState::Registered)
        .context("the authoritative map contains no registered CA")?;
    let identity = fetch_identity_with(ca.addr, kind, expected.tls_client.clone())
        .await
        .context("verifying the active CA")?;
    if identity.fingerprint != expected.fingerprint
        || !identity.ca
        || identity.server_id != map.ca
    {
        bail!("the CA candidate is not the exact CA of the confirmed home CA");
    }
    connect_pinned(ca.addr, kind, &identity).await
}

pub struct LoginSession {
    pub admin: String,
    pub token: Secret,
    pub issued_unix: u64,
    pub absolute_deadline_unix: u64,
    pub idle_timeout_secs: u64,
}

fn admin_refusal(
    expected: &CaIdentity,
    credential: &admin_proto::AdminCredential,
    context: &str,
    reason: String,
) -> anyhow::Error {
    if matches!(credential, admin_proto::AdminCredential::Session { .. })
        && reason.contains("login required")
    {
        let _ = crate::session_cache::delete(&expected.fingerprint.text());
    }
    anyhow!("{context}: {reason}")
}

pub async fn login(
    bootstrap: SocketAddr,
    expected: &CaIdentity,
    admin: &str,
    password: &str,
) -> Result<LoginSession> {
    let mut tls = connect_ca_pinned(bootstrap, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Login(admin_proto::LoginRequest {
            credential: admin_proto::AdminCredential::password(admin, password),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, admin_proto::LoginResponse>(&mut tls).await? {
        admin_proto::LoginResponse::Ok(LoginOk {
            admin,
            token,
            issued_unix,
            absolute_deadline_unix,
            idle_timeout_secs,
        }) => Ok(LoginSession {
            admin,
            token,
            issued_unix,
            absolute_deadline_unix,
            idle_timeout_secs,
        }),
        admin_proto::LoginResponse::Err { reason } => bail!("login refused: {reason}"),
    }
}

pub async fn logout(
    bootstrap: SocketAddr,
    expected: &CaIdentity,
    token: &str,
) -> Result<()> {
    let mut tls = connect_ca_pinned(bootstrap, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Logout(admin_proto::LogoutRequest {
            credential: admin_proto::AdminCredential::Session {
                token: Secret(token.to_string()),
            },
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, admin_proto::LogoutResponse>(&mut tls).await? {
        admin_proto::LogoutResponse::Ok(()) => Ok(()),
        admin_proto::LogoutResponse::Err { reason } => bail!("logout refused: {reason}"),
    }
}

/// Fetch one admin server's local facts + known peers, pinned to the
/// confirmed identity. See [`aggregate`] for the admin domain-wide picture.
pub async fn get_info(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<GetInfoResponse> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(&mut tls, &Request::GetInfo).await?;
    admin_proto::read_msg(&mut tls).await
}

/// Fetch the whole admin domain map from one admin server, pinned to the
/// confirmed CA identity — one round trip is the entire admin domain. The
/// client-facing counterpart of [`get_map`] (which authenticates with a
/// serving cert for the server-to-server refresh path).
pub async fn get_map_pinned(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<AdminDomainMap> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(&mut tls, &Request::GetMap).await?;
    let hint = match admin_proto::read_msg::<_, GetMapResponse>(&mut tls).await? {
        GetMapResponse::Ok(map) => map,
        GetMapResponse::Err { reason } => bail!("map query refused: {reason}"),
    };
    if expected.ca && expected.server_id == hint.ca {
        return Ok(hint);
    }
    let ca = hint
        .ca_entry()
        .filter(|s| s.state == admin_proto::ServerState::Registered)
        .context("map hint contains no registered CA")?;
    let ca_id = ca.id;
    let ca_addr = ca.addr;
    let identity = fetch_identity_with(ca_addr, kind, expected.tls_client.clone())
        .await
        .context("verifying the CA named by the map hint")?;
    if identity.fingerprint != expected.fingerprint
        || !identity.ca
        || identity.server_id != ca_id
    {
        bail!("map candidate is not the exact CA of the confirmed home CA");
    }
    let mut tls = connect_pinned(ca_addr, kind, &identity).await?;
    admin_proto::write_msg(&mut tls, &Request::GetMap).await?;
    match admin_proto::read_msg::<_, GetMapResponse>(&mut tls).await? {
        GetMapResponse::Ok(map) if map.ca == ca_id => Ok(map),
        GetMapResponse::Ok(_) => {
            bail!("CA returned a map for another CA")
        }
        GetMapResponse::Err { reason } => bail!("CA map query refused: {reason}"),
    }
}

/// Server→server: apply a perms edit to a peer admin server (serving-cert
/// authed). Mirrors [`push_referral_edit`].
pub async fn push_perms_edit(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
    operation_id: admin_proto::OperationId,
    perms: &crate::perms::PMap,
    version: Option<u64>,
) -> Result<()> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ApplyPermsEdit(ApplyPermsEditRequest {
            operation_id,
            perms: perms.clone(),
            version,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ApplyPermsEditResponse>(&mut tls).await? {
        ApplyPermsEditResponse::Ok(()) => Ok(()),
        ApplyPermsEditResponse::Err { reason } => {
            bail!("peer refused the perms edit: {reason}")
        }
    }
}

/// Server→server: read a peer admin server's id-map (serving-cert authed).
///
/// The CA needs this to work out what a lagging host is actually missing,
/// rather than replaying its whole history at it. `Ok(None)` means the peer
/// does not hold the id-map role, same as [`push_id_map_edit`].
pub async fn fetch_id_map(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
) -> Result<Option<IdMap>> {
    let (mut tls, hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    if !hello.roles.contains(Role::IdMap) {
        return Ok(None);
    }
    admin_proto::write_msg(&mut tls, &Request::GetLocalIdMap).await?;
    match admin_proto::read_msg::<_, admin_proto::GetLocalIdMapResponse>(&mut tls).await?
    {
        admin_proto::GetLocalIdMapResponse::Ok(map) => Ok(Some(map)),
        admin_proto::GetLocalIdMapResponse::Err { reason } => {
            bail!("peer refused the id-map read: {reason}")
        }
    }
}

/// Server→server: apply one id-map operation to a peer admin server
/// (serving-cert authed).
///
/// `Ok(None)` means the peer does not hold the id-map role — checked from its
/// hello rather than by asking, so a server that was granted the role but
/// isn't running it is told apart from one that applied the edit. Otherwise
/// the peer reports whether its map changed, which only it can know.
pub async fn push_id_map_edit(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
    operation_id: admin_proto::OperationId,
    edit: &admin_proto::IdMapEdit,
    version: Option<u64>,
) -> Result<Option<admin_proto::ApplyIdMapEditOk>> {
    let (mut tls, hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    if !hello.roles.contains(Role::IdMap) {
        return Ok(None);
    }
    admin_proto::write_msg(
        &mut tls,
        &Request::ApplyIdMapEdit(admin_proto::ApplyIdMapEditRequest {
            operation_id,
            edit: edit.clone(),
            version,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, admin_proto::ApplyIdMapEditResponse>(&mut tls)
        .await?
    {
        admin_proto::ApplyIdMapEditResponse::Ok(ok) => Ok(Some(ok)),
        admin_proto::ApplyIdMapEditResponse::Err { reason } => {
            bail!("peer refused the id-map edit: {reason}")
        }
    }
}

/// CA → node: immediately install a freshly signed CRL on one exact
/// home-CA admin-server identity.
pub async fn push_crl(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
    operation_id: admin_proto::OperationId,
    crl_pem: &str,
) -> Result<()> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ApplyCrl(ApplyCrlRequest {
            operation_id,
            crl_pem: crl_pem.to_string(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ApplyCrlResponse>(&mut tls).await? {
        ApplyCrlResponse::Ok(()) => Ok(()),
        ApplyCrlResponse::Err { reason } => {
            bail!("peer refused the CRL update: {reason}")
        }
    }
}

/// CA → node: reconcile the exact CA address, authoritative
/// map, and CRL on one exact home-CA admin-server identity.
pub async fn push_ca_state(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
    request: ApplyCaStateRequest,
) -> Result<()> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    admin_proto::write_msg(&mut tls, &Request::ApplyCaState(request)).await?;
    match admin_proto::read_msg::<_, ApplyCaStateResponse>(&mut tls).await? {
        ApplyCaStateResponse::Ok(()) => Ok(()),
        ApplyCaStateResponse::Err { reason } => {
            bail!("peer refused ca-state reconciliation: {reason}")
        }
    }
}

/// CA → node: read one exact home CA's local permissions using
/// the CA serving certificate. This is the internal target-side half
/// of [`read_perms`]; ordinary clients cannot invoke `GetPerms`.
#[cfg_attr(not(unix), allow(dead_code))]
pub async fn pull_perms(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
) -> Result<crate::perms::PMap> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    admin_proto::write_msg(&mut tls, &Request::GetPerms).await?;
    match admin_proto::read_msg::<_, GetPermsResponse>(&mut tls).await? {
        GetPermsResponse::Ok(perms) => Ok(perms),
        GetPermsResponse::Err { reason } => bail!("perms read refused: {reason}"),
    }
}

/// Admin → ca: authenticate, authorize `target_path`, and have the
/// CA read one exact registered member of that resolver cluster.
pub async fn read_perms(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    target_path: &str,
) -> Result<crate::perms::PMap> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ReadPerms(ReadPermsRequest {
            credential: credential.clone(),
            target_path: target_path.to_string(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ReadPermsResponse>(&mut tls).await? {
        ReadPermsResponse::Ok(ReadPermsOk { perms, .. }) => Ok(perms),
        ReadPermsResponse::Err { reason } => Err(admin_refusal(
            expected,
            &credential,
            "the CA refused the perms read",
            reason,
        )),
    }
}

/// Admin → CA (pinned): edit a target resolver cluster's perms; returns the per-peer
/// propagation results so the caller can surface a partial failure.
pub async fn edit_perms(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    target_path: &str,
    perms: &crate::perms::PMap,
) -> Result<u64> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::EditPerms(EditPermsRequest {
            credential: credential.clone(),
            target_path: target_path.to_string(),
            perms: perms.clone(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, EditPermsResponse>(&mut tls).await? {
        EditPermsResponse::Ok(ok) => Ok(ok.version),
        EditPermsResponse::Err { reason } => Err(admin_refusal(
            expected,
            &credential,
            "the CA refused the perms edit",
            reason,
        )),
    }
}

/// Admin → CA (pinned): read the CA host's id-map.
pub async fn get_id_map(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
) -> Result<IdMap> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::GetIdMap(admin_proto::GetIdMapRequest {
            credential: credential.clone(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, admin_proto::GetIdMapResponse>(&mut tls).await? {
        admin_proto::GetIdMapResponse::Ok(ok) => Ok(ok.id_map),
        admin_proto::GetIdMapResponse::Err { reason } => Err(admin_refusal(
            expected,
            &credential,
            "the CA refused the id-map read",
            reason,
        )),
    }
}

/// Admin → CA (pinned): apply one id-map operation across the admin domain.
/// Returns whether anything changed alongside the per-peer results, so a
/// partial failure and a no-op are told apart.
pub async fn edit_id_map(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    edit: &admin_proto::IdMapEdit,
) -> Result<crate::ops::RecordedEdit> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::EditIdMap(admin_proto::EditIdMapRequest {
            credential: credential.clone(),
            edit: edit.clone(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, admin_proto::EditIdMapResponse>(&mut tls).await? {
        admin_proto::EditIdMapResponse::Ok(ok) => {
            Ok(crate::ops::RecordedEdit { version: ok.version, changed: ok.changed })
        }
        admin_proto::EditIdMapResponse::Err { reason } => Err(admin_refusal(
            expected,
            &credential,
            "the CA refused the id-map edit",
            reason,
        )),
    }
}

/// Admin → CA (pinned): mint a new role admin `name` with `policy` and
/// `new_password`. The server gates on the caller's management authority and
/// enforces `policy ⊆ caller`.
#[allow(clippy::too_many_arguments)]
pub async fn add_role_admin(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    name: &str,
    new_password: &str,
    policy: netidx_admin_proto::policy::Policy,
) -> Result<()> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::AddRoleAdmin(AddRoleAdminRequest {
            credential: credential.clone(),
            name: name.to_string(),
            new_password: admin_proto::Secret(new_password.to_string()),
            policy,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, AdminMgmtResponse>(&mut tls).await? {
        AdminMgmtResponse::Ok(()) => Ok(()),
        AdminMgmtResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "the CA refused", reason))
        }
    }
}

/// Admin → CA (pinned): replace role admin `target`'s policy.
pub async fn set_admin_policy(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    target: &str,
    policy: netidx_admin_proto::policy::Policy,
) -> Result<()> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::SetAdminPolicy(SetAdminPolicyRequest {
            credential: credential.clone(),
            target: target.to_string(),
            policy,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, AdminMgmtResponse>(&mut tls).await? {
        AdminMgmtResponse::Ok(()) => Ok(()),
        AdminMgmtResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "the CA refused", reason))
        }
    }
}

/// Admin → CA (pinned): remove role admin `target`.
pub async fn remove_admin(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    target: &str,
) -> Result<()> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::RemoveAdmin(RemoveAdminRequest {
            credential: credential.clone(),
            target: target.to_string(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, AdminMgmtResponse>(&mut tls).await? {
        AdminMgmtResponse::Ok(()) => Ok(()),
        AdminMgmtResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "the CA refused", reason))
        }
    }
}

/// Admin → CA (pinned): list the admin roster (tier + policy per admin).
pub async fn list_admins(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
) -> Result<Vec<netidx_admin_proto::policy::AdminInfo>> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ListAdmins(ListAdminsRequest { credential: credential.clone() }),
    )
    .await?;
    match admin_proto::read_msg::<_, AdminListResponse>(&mut tls).await? {
        AdminListResponse::Ok(admins) => Ok(admins),
        AdminListResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "the CA refused", reason))
        }
    }
}

/// Admin → CA (pinned): control services on the single admin server
/// `target_server`. Returns that server's per-unit statuses.
#[allow(clippy::too_many_arguments)]
pub async fn control_service(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    target_server: admin_proto::AdminServerId,
    units: Vec<String>,
    op: netidx_activation::control::ControlOp,
) -> Result<Vec<admin_proto::ServiceUnit>> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ControlService(ControlServiceRequest {
            credential: credential.clone(),
            target_server,
            units,
            op,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ControlServiceResponse>(&mut tls).await? {
        ControlServiceResponse::Ok(ControlServiceOk { units, .. }) => Ok(units),
        ControlServiceResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "the CA refused", reason))
        }
    }
}

/// Admin → CA: open or shut one member's read gate. Authorized by the same
/// `service_control_scopes` as [`control_service`], because it is the same
/// kind of act on the same target.
pub async fn set_read_gate(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    target_server: admin_proto::AdminServerId,
    gate: netidx::resolver_server::config::ReadGate,
) -> Result<()> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::SetReadGate(admin_proto::SetReadGateRequest {
            credential: credential.clone(),
            target_server,
            gate,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, admin_proto::SetReadGateResponse>(&mut tls).await? {
        admin_proto::SetReadGateResponse::Ok(_) => Ok(()),
        admin_proto::SetReadGateResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "the CA refused", reason))
        }
    }
}

/// Server → server: apply a service-control op to a peer admin server's local
/// activation supervisor (serving-cert authed). Mirrors [`push_perms_edit`].
pub async fn push_service_control(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
    operation_id: admin_proto::OperationId,
    units: Vec<String>,
    op: netidx_activation::control::ControlOp,
) -> Result<Vec<admin_proto::ServiceUnit>> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ApplyServiceControl(ApplyServiceControlRequest {
            operation_id,
            units,
            op,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ApplyServiceControlResponse>(&mut tls).await? {
        ApplyServiceControlResponse::Ok(units) => Ok(units),
        ApplyServiceControlResponse::Err { reason } => {
            bail!("peer refused service control: {reason}")
        }
    }
}

/// CA → node: write a read gate into one exact admin server's own resolver
/// config. Peer-cert-gated the same way as a service-control push.
pub async fn push_set_read_gate(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
    operation_id: admin_proto::OperationId,
    gate: netidx::resolver_server::config::ReadGate,
) -> Result<()> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ApplySetReadGate(admin_proto::ApplySetReadGateRequest {
            operation_id,
            gate,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, admin_proto::ApplySetReadGateResponse>(&mut tls)
        .await?
    {
        admin_proto::ApplySetReadGateResponse::Ok(()) => Ok(()),
        admin_proto::ApplySetReadGateResponse::Err { reason } => {
            bail!("peer refused the read gate: {reason}")
        }
    }
}

/// The ca-authoritative picture of the bootstrap server's resolver
/// resolver cluster, plus the admin servers reached while walking discovery hints.
/// Everything in it was served over connections pinned to the
/// operator-confirmed CA.
pub struct AdminDomainInfo {
    pub domain: String,
    /// Where Sign/Enroll requests go. The first CA location seen wins;
    /// a well-formed admin domain only has one.
    pub ca_addr: Option<SocketAddr>,
    /// The active members of the bootstrap server's CA-owned resolver cluster. These
    /// are replicas of one resolver cluster, never a flattening of the
    /// resolver hierarchy's parent and child clusters.
    pub resolvers: Vec<ResolverAddr>,
    /// Base of that one bootstrap resolver cluster. Installers use it when adding a
    /// replica to a non-root resolver cluster; it is ordinary admin metadata and never
    /// changes netidx's data-plane protocol.
    pub resolver_base: Option<String>,
    /// Referral topology for that one resolver cluster, derived from the authoritative
    /// map and restricted to active, registered routing targets.
    pub resolver_parent: Option<admin_proto::ResolverClusterEdge>,
    pub resolver_children: Vec<admin_proto::ResolverClusterEdge>,
    /// The admin servers actually reached.
    pub reached: Vec<SocketAddr>,
}

/// Upper bound on the peer walk — far above any plausible admin domain, just
/// a runaway backstop.
const MAX_WALK: usize = 64;

fn registered_members(
    map: &AdminDomainMap,
    cluster_id: admin_proto::ResolverClusterId,
) -> Vec<ResolverAddr> {
    let Some(cluster) = map.resolver_clusters.iter().find(|cluster| {
        cluster.id == cluster_id
            && cluster.state == admin_proto::ResolverClusterState::Active
    }) else {
        return Vec::new();
    };
    let mut members: Vec<_> = map
        .admin_servers
        .iter()
        .filter(|server| {
            server.cluster == Some(cluster_id)
                && server.state == admin_proto::ServerState::Registered
        })
        .filter_map(|server| server.resolver.clone())
        .filter(|resolver| cluster.members.contains(resolver))
        .collect();
    members.sort_by(|a, b| a.addr.cmp(&b.addr));
    members
}

/// One active resolver cluster's CA-authoritative resolver topology. This is also used
/// by strict installers whose explicit bootstrap server belongs to a
/// different resolver cluster than the one they are joining.
pub struct ResolverClusterTopology {
    pub members: Vec<ResolverAddr>,
    pub parent: Option<admin_proto::ResolverClusterEdge>,
    pub children: Vec<admin_proto::ResolverClusterEdge>,
}

fn cluster_topology(
    map: &AdminDomainMap,
    cluster_id: admin_proto::ResolverClusterId,
) -> Result<ResolverClusterTopology> {
    let cluster = map
        .resolver_clusters
        .iter()
        .find(|c| c.id == cluster_id)
        .filter(|c| c.state == admin_proto::ResolverClusterState::Active)
        .context("resolver cluster is not active")?;
    let members = registered_members(map, cluster_id);
    let parent = cluster.parent.and_then(|parent_id| {
        let addrs = registered_members(map, parent_id);
        (!addrs.is_empty()).then(|| admin_proto::ResolverClusterEdge {
            path: cluster.base.clone(),
            addrs,
        })
    });
    let mut children: Vec<_> = cluster
        .children
        .iter()
        .filter_map(|child_id| {
            let child = map.resolver_clusters.iter().find(|child| {
                child.id == *child_id
                    && child.state == admin_proto::ResolverClusterState::Active
            })?;
            let addrs = registered_members(map, *child_id);
            (!addrs.is_empty()).then(|| admin_proto::ResolverClusterEdge {
                path: child.base.clone(),
                addrs,
            })
        })
        .collect();
    children.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(ResolverClusterTopology { members, parent, children })
}

pub fn cluster_topology_by_base(
    map: &AdminDomainMap,
    base: &str,
) -> Result<ResolverClusterTopology> {
    let cluster = map
        .resolver_clusters
        .iter()
        .find(|cluster| {
            cluster.base == base
                && cluster.state == admin_proto::ResolverClusterState::Active
        })
        .with_context(|| {
            format!("the authoritative map has no active resolver cluster at {base}")
        })?;
    cluster_topology(map, cluster.id)
}

struct BootstrapSelection {
    ca: SocketAddr,
    resolver: Option<ResolverSelection>,
}

struct ResolverSelection {
    base: String,
    topology: ResolverClusterTopology,
}

fn bootstrap_cluster(
    map: &AdminDomainMap,
    server_id: admin_proto::AdminServerId,
) -> Result<BootstrapSelection> {
    let ca = map
        .ca_entry()
        .filter(|s| s.state == admin_proto::ServerState::Registered)
        .context("admin domain map contains no registered CA")?;
    let bootstrap = map
        .admin_servers
        .iter()
        .find(|s| s.id == server_id)
        .filter(|s| s.state == admin_proto::ServerState::Registered)
        .context(
            "the verified bootstrap server is not registered in the admin domain map",
        )?;
    // A dedicated CA has no resolver cluster of its own. It is still a
    // normal discovery seed, so map it to the one active root resolver cluster when
    // constructing client configuration. A satellite seed continues to select
    // its exact resolver cluster; the resolver hierarchy is never flattened.
    let bootstrap_cluster = bootstrap.cluster.or_else(|| {
        (bootstrap.id == map.ca)
            .then(|| {
                map.resolver_clusters.iter().find(|cluster| {
                    cluster.base == "/"
                        && cluster.parent.is_none()
                        && cluster.state == admin_proto::ResolverClusterState::Active
                })
            })
            .flatten()
            .map(|cluster| cluster.id)
    });
    let resolver = match bootstrap_cluster {
        None => None,
        Some(cluster_id) => {
            let cluster = map
                .resolver_clusters
                .iter()
                .find(|c| c.id == cluster_id)
                .context("the bootstrap server's resolver cluster is absent")?;
            let topology = cluster_topology(map, cluster_id)
                .context("the bootstrap server's resolver cluster is not active")?;
            Some(ResolverSelection { base: cluster.base.clone(), topology })
        }
    };
    Ok(BootstrapSelection { ca: ca.addr, resolver })
}

/// Try `seeds` and their advertised peers as candidate paths to the exact
/// CA. Stop as soon as one candidate yields the ca-authoritative
/// map: peer addresses are discovery hints, not a checklist that every client
/// must contact before it can use the admin domain. This matters across routed or
/// partitioned sites, where a perfectly usable bootstrap resolver cluster may advertise
/// admin servers that the joining client cannot reach directly.
///
/// Resolver membership comes only from the authoritative map. Parent/child
/// resolver clusters are referrals, not replicas, and must never be flattened into one
/// client address set.
pub async fn aggregate(
    seeds: &[SocketAddr],
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<AdminDomainInfo> {
    // Preserve candidate order: `confirm_seeds` records the identity of the
    // first reachable seed, and `AdminDomainInfo::reached[0]` becomes the durable
    // bootstrap hint in the install record. A LIFO walk could silently record
    // a different same-CA satellite from the tail of an mDNS result.
    let mut queue: VecDeque<SocketAddr> = seeds.iter().copied().collect();
    let mut visited: Vec<SocketAddr> = Vec::new();
    let mut info = AdminDomainInfo {
        domain: expected.domain.clone(),
        ca_addr: None,
        resolvers: Vec::new(),
        resolver_base: None,
        resolver_parent: None,
        resolver_children: Vec::new(),
        reached: Vec::new(),
    };
    let mut authoritative = None;
    while let Some(addr) = queue.pop_front() {
        if visited.contains(&addr) || visited.len() >= MAX_WALK {
            continue;
        }
        visited.push(addr);
        let resp = match get_info(addr, kind, expected).await {
            Ok(r) => r,
            Err(e) => {
                warn!("admin server {addr} could not be queried: {e:#}");
                continue;
            }
        };
        info.reached.push(addr);
        // Local facts and peers are discovery hints. If this candidate's map
        // points us to the verified CA, no other peer needs probing.
        // Otherwise its peers are fallback candidates for the next iteration.
        queue.extend(resp.peers);
        match get_map_pinned(addr, kind, expected).await {
            Ok(map) => {
                authoritative = Some(map);
                break;
            }
            Err(e) => {
                warn!("admin server {addr} did not yield a ca-authoritative map: {e:#}")
            }
        }
    }
    if info.reached.is_empty() {
        bail!("no admin server could be reached");
    }
    let map = authoritative.context(
        "reachable admin servers did not yield a ca-authoritative admin domain map",
    )?;
    let BootstrapSelection { ca, resolver } =
        bootstrap_cluster(&map, expected.server_id)?;
    info.ca_addr = Some(ca);
    if let Some(ResolverSelection {
        base,
        topology: ResolverClusterTopology { members, parent, children },
    }) = resolver
    {
        info.resolvers = members;
        info.resolver_base = Some(base);
        info.resolver_parent = parent;
        info.resolver_children = children;
    }
    Ok(info)
}

/// Submit a CSR for `name` to the admin domain's CA, authenticated as
/// `admin`/`password`, pinned to the identity the operator already
/// confirmed (`expected`, from [`fetch_identity`]). `id_map_groups`
/// (first = primary) registers the new identity on the admin domain's
/// id-map hosts — chosen here, at enrollment, by the authenticated
/// admin; the server validates the choice against that admin's
/// allowed set. Empty ⇒ no registration. Returns the signed cert +
/// key + trust bundle.
// One over clippy's threshold, but every argument is a distinct,
// required fact about the one sign being requested — a params struct
// would just move the same eight names one level down.
#[allow(clippy::too_many_arguments)]
pub async fn request_cert(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    admin: &str,
    password: Zeroizing<String>,
    validity: Duration,
    id_map_groups: Vec<String>,
    expected: &CaIdentity,
) -> Result<Issued> {
    request_cert_replacing(
        addr,
        kind,
        name,
        admin,
        password,
        validity,
        id_map_groups,
        None,
        expected,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn request_cert_replacing(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    admin: &str,
    password: Zeroizing<String>,
    validity: Duration,
    id_map_groups: Vec<String>,
    replaces_serial: Option<u64>,
    expected: &CaIdentity,
) -> Result<Issued> {
    // Local work first: our key + CSR.
    let kc = generate_key_and_csr(name)?;
    let req = Request::Sign(SignRequest {
        kind,
        credential: admin_proto::AdminCredential::password(admin, password.as_str()),
        csr_pem: kc.csr_pem.clone(),
        requested_name: name.to_string(),
        requested_validity: validity,
        id_map_groups,
        replaces_serial,
    });
    submit_csr(addr, kind, name, kc, req, expected).await
}

/// Enroll a new admin server: request the reserved [`SERVING_SAN`]
/// serving cert from the admin domain's CA, authenticated as
/// `admin`/`password` (whose policy must cover the requested resolver cluster and roles),
/// pinned to the confirmed identity. `listen` is where the new daemon
/// will serve — the CA records it as a peer. The returned leaf + the
/// confirmed CA ([`CaIdentity::ca_pem`]) form the new daemon's serving
/// chain.
pub async fn enroll(
    addr: SocketAddr,
    admin: &str,
    password: Zeroizing<String>,
    listen: SocketAddr,
    roles: BitFlags<Role>,
    resolver_member: ResolverAddr,
    resolver_members: Vec<ResolverAddr>,
    cluster: admin_proto::ResolverClusterPlacement,
    replaces: Option<admin_proto::AdminServerId>,
    expected: &CaIdentity,
) -> Result<Issued> {
    let kc = generate_key_and_csr(SERVING_SAN)?;
    let req = Request::Enroll(EnrollRequest {
        credential: admin_proto::AdminCredential::password(admin, password.as_str()),
        csr_pem: kc.csr_pem.clone(),
        listen,
        roles,
        resolver_member: Some(resolver_member),
        resolver_members,
        cluster,
        renew_identity: None,
        replaces,
    });
    submit_csr(addr, NodeKind::AdminServer, SERVING_SAN, kc, req, expected).await
}

/// Shared Sign/Enroll tail: send the prepared request over a pinned
/// connection and verify the response binds to what the operator
/// confirmed and to the key we generated.
async fn submit_csr(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    kc: KeyAndCsr,
    req: Request,
    expected: &CaIdentity,
) -> Result<Issued> {
    // Capture our public key now — we need it after the response to
    // confirm the CA signed *our* key, not a substituted one.
    let our_spki = csr_spki(&kc.csr_pem)?;
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(&mut tls, &req).await?;
    match admin_proto::read_msg::<_, SignResponse>(&mut tls).await? {
        SignResponse::Ok(SignOk { signed_cert_pem, trusted_pem, warnings, .. }) => {
            verify_issued(expected, name, &our_spki, &signed_cert_pem, &trusted_pem)?;
            Ok(Issued {
                cert_pem: signed_cert_pem,
                private_key_pem: kc.private_key_pem,
                trusted_pem,
                warnings,
            })
        }
        SignResponse::Err { reason } => bail!("admin server refused to sign: {reason}"),
    }
}

/// Verify a returned cert + bundle binds to what the operator
/// confirmed and to the key we generated. The confirmed CA must be
/// present in the trust bundle (or installing it would anchor trust in
/// something the operator never verified; additional CAs are allowed —
/// federated trust is intentional), and the leaf must be signed by
/// *the* confirmed CA, carry exactly the name we asked for, and
/// contain our own public key (so it isn't an unrelated/replayed cert
/// we couldn't even use).
fn verify_issued(
    expected: &CaIdentity,
    name: &str,
    our_spki: &[u8],
    signed_cert_pem: &str,
    trusted_pem: &str,
) -> Result<()> {
    if !bundle_contains(trusted_pem, &expected.fingerprint) {
        bail!(
            "the trust bundle returned by the server does not contain the \
             CA identity the operator confirmed"
        );
    }
    verify_issued_leaf(signed_cert_pem, &expected.ca_der, name, our_spki).context(
        "the certificate returned by the admin server failed verification \
         against the confirmed CA",
    )
}

/// A queued enrollment awaiting admin approval: the locally generated
/// key + CSR, the server's request id, and the fingerprint of *our*
/// CSR public key — the request code the operator reads to the admin
/// out of band (chat, phone). Both sides compute it independently from
/// the CSR, so the code never transits as a trusted value.
pub struct PendingEnrollment {
    pub request_id: String,
    /// Fingerprint of our CSR's SubjectPublicKeyInfo — show its text +
    /// identicon to the operator.
    pub fingerprint: Fingerprint,
    name: String,
    our_spki: Vec<u8>,
    kc: KeyAndCsr,
}

/// One [`poll`] outcome.
pub enum PollOutcome {
    /// Still waiting for an admin.
    Pending,
    /// Approved: the verified, installable result.
    Issued(Issued),
    Denied(String),
    /// The server no longer knows the request (expired / restarted
    /// past the TTL) — re-enqueue to try again.
    Expired,
}

/// Fingerprint of a CSR's public key (its SubjectPublicKeyInfo DER) —
/// the request code matched between enrollee and admin.
pub fn csr_fingerprint(csr_pem: &str) -> Result<Fingerprint> {
    Ok(Fingerprint::of_der(&csr_spki(csr_pem)?))
}

/// The SubjectPublicKeyInfo DER of a certificate's public key. The same key a
/// CSR carries, so `cert_fingerprint` == `csr_fingerprint` for a cert issued
/// from that CSR — but a cert always has one, even for a direct issuance
/// (`ca issue`) that never had a CSR.
pub(crate) fn cert_spki(cert_pem: &str) -> Result<Vec<u8>> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let der = pem_to_der(cert_pem, "CERTIFICATE")?;
    let (_, cert) = X509Certificate::from_der(&der)
        .map_err(|e| anyhow!("parsing certificate: {e}"))?;
    Ok(cert.public_key().raw.to_vec())
}

/// Fingerprint of a certificate's public key (its SubjectPublicKeyInfo DER) —
/// the per-key revoke glyph. Equal to [`csr_fingerprint`] of the CSR it was
/// issued from, and defined even for a CSR-less direct issuance.
pub fn cert_fingerprint(cert_pem: &str) -> Result<Fingerprint> {
    Ok(Fingerprint::of_der(&cert_spki(cert_pem)?))
}

/// Queue a signing request for asynchronous admin approval (no
/// credentials — the default enrollment path when no admin is present
/// at this node). Returns the pending enrollment to [`poll`] with; show
/// its `fingerprint` to the operator so the admin can match it.
pub async fn enqueue(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    validity: Duration,
    expected: &CaIdentity,
) -> Result<PendingEnrollment> {
    enqueue_inner(addr, kind, name, validity, None, None, expected).await
}

pub async fn enqueue_replacing(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    validity: Duration,
    replaces_serial: u64,
    expected: &CaIdentity,
) -> Result<PendingEnrollment> {
    enqueue_inner(addr, kind, name, validity, None, Some(replaces_serial), expected).await
}

/// Queue a **admin-server enrollment** for asynchronous admin approval:
/// the reserved [`SERVING_SAN`] serving cert, approvable only by an
/// admin whose policy covers the requested resolver cluster and roles. Same request-code
/// ceremony and [`poll`] loop as a queued sign; `listen` is where the
/// new admin server will serve (recorded as a peer at approval).
pub async fn enqueue_enroll(
    addr: SocketAddr,
    enrollment: admin_proto::EnrollmentRequest,
    expected: &CaIdentity,
) -> Result<PendingEnrollment> {
    // The validity is decided server-side at approval (the standard
    // serving-cert validity); the value here is a well-formedness
    // placeholder.
    enqueue_inner(
        addr,
        NodeKind::AdminServer,
        SERVING_SAN,
        Duration::from_secs(1),
        Some(enrollment),
        None,
        expected,
    )
    .await
}

async fn enqueue_inner(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    validity: Duration,
    enrollment: Option<admin_proto::EnrollmentRequest>,
    replaces_serial: Option<u64>,
    expected: &CaIdentity,
) -> Result<PendingEnrollment> {
    let kc = generate_key_and_csr(name)?;
    let our_spki = csr_spki(&kc.csr_pem)?;
    let fingerprint = Fingerprint::of_der(&our_spki);
    let mut tls = connect_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Enqueue(EnqueueRequest {
            kind,
            csr_pem: kc.csr_pem.clone(),
            requested_name: name.to_string(),
            requested_validity: validity,
            enrollment,
            replaces_serial,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, EnqueueResponse>(&mut tls).await? {
        EnqueueResponse::Ok(QueuedOk { request_id }) => Ok(PendingEnrollment {
            request_id,
            fingerprint,
            name: name.to_string(),
            our_spki,
            kc,
        }),
        EnqueueResponse::Err { reason } => {
            bail!("admin server refused to queue the request: {reason}")
        }
    }
}

/// Check on a queued enrollment — one short pinned connection per
/// poll, so hours of waiting never hold a socket (or the server's
/// connection timeout) open. A `Signed` response is verified exactly
/// like a synchronous sign before being returned as
/// [`PollOutcome::Issued`].
pub async fn poll(
    addr: SocketAddr,
    kind: NodeKind,
    pending: &PendingEnrollment,
    expected: &CaIdentity,
) -> Result<PollOutcome> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Poll(PollRequest { request_id: pending.request_id.clone() }),
    )
    .await?;
    match admin_proto::read_msg::<_, PollResponse>(&mut tls).await? {
        PollResponse::Pending => Ok(PollOutcome::Pending),
        PollResponse::Denied { reason } => Ok(PollOutcome::Denied(reason)),
        PollResponse::Unknown => Ok(PollOutcome::Expired),
        PollResponse::Signed(SignOk {
            signed_cert_pem, trusted_pem, warnings, ..
        }) => {
            verify_issued(
                expected,
                &pending.name,
                &pending.our_spki,
                &signed_cert_pem,
                &trusted_pem,
            )?;
            Ok(PollOutcome::Issued(Issued {
                cert_pem: signed_cert_pem,
                private_key_pem: pending.kc.private_key_pem.clone(),
                trusted_pem,
                warnings,
            }))
        }
    }
}

/// List the pending signing queue, authenticated as `admin` (pinned —
/// the admin's password only ever goes to the confirmed admin domain).
pub async fn list_queue(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    expected: &CaIdentity,
) -> Result<Vec<QueueEntry>> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ListQueue(ListQueueRequest { credential: credential.clone() }),
    )
    .await?;
    match admin_proto::read_msg::<_, ListQueueResponse>(&mut tls).await? {
        ListQueueResponse::Ok(requests) => Ok(requests),
        ListQueueResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "admin server refused", reason))
        }
    }
}

/// Approve a queued request: the server signs its CSR (under the
/// approving admin's policy) and registers `id_map_groups` — chosen
/// here, by the admin. Returns the push warnings; the enrollee
/// receives the cert via its own [`poll`].
pub async fn approve(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    request_id: &str,
    id_map_groups: Vec<String>,
    expected: &CaIdentity,
) -> Result<Vec<String>> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Approve(ApproveRequest {
            credential: credential.clone(),
            request_id: request_id.to_string(),
            id_map_groups,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ApproveResponse>(&mut tls).await? {
        ApproveResponse::Ok(ApproveOk { warnings, .. }) => Ok(warnings),
        ApproveResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "admin server refused", reason))
        }
    }
}

/// Fetch the admin domain's current CRL (pinned). `None` — nothing has ever
/// been revoked.
pub async fn get_crl(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<Option<String>> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(&mut tls, &Request::GetCrl).await?;
    let resp: admin_proto::GetCrlResponse = admin_proto::read_msg(&mut tls).await?;
    Ok(resp.crl_pem)
}

/// Deny a queued request with a reason shown to the waiting enrollee.
pub async fn deny(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    request_id: &str,
    reason: &str,
    expected: &CaIdentity,
) -> Result<()> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Deny(DenyRequest {
            credential: credential.clone(),
            request_id: request_id.to_string(),
            reason: reason.to_string(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, DenyResponse>(&mut tls).await? {
        DenyResponse::Ok(()) => Ok(()),
        DenyResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "admin server refused", reason))
        }
    }
}

// -- resolver hierarchy delegation -------------------------------------------

/// The request code for a delegation: a fingerprint over the proposed path
/// and immutable parent/child server-ID sets. Both sets are canonicalized so
/// selection order cannot change the out-of-band code.
pub fn delegation_code(
    proposed_path: &str,
    parent_servers: &[admin_proto::AdminServerId],
    child_servers: &[admin_proto::AdminServerId],
) -> Fingerprint {
    let mut parent_servers = parent_servers.to_vec();
    let mut child_servers = child_servers.to_vec();
    parent_servers.sort();
    parent_servers.dedup();
    child_servers.sort();
    child_servers.dedup();
    let canonical = serde_json::to_vec(&(proposed_path, parent_servers, child_servers))
        .unwrap_or_default();
    Fingerprint::of_der(&canonical)
}

/// Queue a delegation request with the parent's admin server (no
/// credentials — the parent admin authorizes by matching the code).
/// Returns the request id to [`poll_delegation`] with.
pub async fn request_delegation(
    addr: SocketAddr,
    proposed_path: &str,
    parent_servers: Vec<admin_proto::AdminServerId>,
    child_servers: Vec<admin_proto::AdminServerId>,
    expected: &CaIdentity,
) -> Result<String> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::RequestDelegation(DelegationRequest {
            proposed_path: proposed_path.to_string(),
            parent_servers,
            child_servers,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, DelegationResponse>(&mut tls).await? {
        DelegationResponse::Ok(QueuedOk { request_id }) => Ok(request_id),
        DelegationResponse::Err { reason } => {
            bail!("the parent refused the delegation request: {reason}")
        }
    }
}

/// Check on a queued delegation — one short pinned connection per poll.
pub async fn poll_delegation(
    addr: SocketAddr,
    request_id: &str,
    expected: &CaIdentity,
) -> Result<DelegationPollResponse> {
    // The durable request queue exists only at the CA. `addr` may be
    // an ordinary resolver used as a bootstrap hint, so resolve and verify the
    // CA on every poll just as request submission does.
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::PollDelegation(PollRequest { request_id: request_id.to_string() }),
    )
    .await?;
    admin_proto::read_msg(&mut tls).await
}

/// List the pending delegation queue, authenticated as `admin` (pinned).
pub async fn list_delegations(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    expected: &CaIdentity,
) -> Result<Vec<DelegationEntry>> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ListDelegations(ListDelegationsRequest {
            credential: credential.clone(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ListDelegationsResponse>(&mut tls).await? {
        ListDelegationsResponse::Ok(requests) => Ok(requests),
        ListDelegationsResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "admin server refused", reason))
        }
    }
}

/// Approve a queued delegation. Returns the per-peer resolver cluster-push results
/// (a non-empty `error` means that peer is out of sync — surface it).
pub async fn approve_delegation(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    request_id: &str,
    expected: &CaIdentity,
) -> Result<Vec<PeerResult>> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ApproveDelegation(ApproveDelegationRequest {
            credential: credential.clone(),
            request_id: request_id.to_string(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ApproveDelegationResponse>(&mut tls).await? {
        ApproveDelegationResponse::Ok(PropagationOk { peers, .. }) => Ok(peers),
        ApproveDelegationResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "admin server refused", reason))
        }
    }
}

/// Deny a queued delegation with a reason shown to the waiting child.
pub async fn deny_delegation(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    request_id: &str,
    reason: &str,
    expected: &CaIdentity,
) -> Result<()> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::DenyDelegation(DenyDelegationRequest {
            credential: credential.clone(),
            request_id: request_id.to_string(),
            reason: reason.to_string(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, DenyDelegationResponse>(&mut tls).await? {
        DenyDelegationResponse::Ok(()) => Ok(()),
        DenyDelegationResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "admin server refused", reason))
        }
    }
}

/// Server-to-server: push a referral edit to a peer admin server's local
/// resolver config (the resolver cluster-wide propagation push), authenticated by
/// our reserved-SAN serving cert. Mirrors [`push_id_map_edit`].
pub async fn push_referral_edit(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    target_id: admin_proto::AdminServerId,
    target_ca: bool,
    home_ca: CertificateDer<'static>,
    operation_id: admin_proto::OperationId,
    edit: &ReferralEdit,
) -> Result<()> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: Some(target_id), home_ca: &home_ca, ca: target_ca }),
    )
    .await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ApplyReferralEdit(ApplyReferralEditRequest {
            operation_id,
            edit: edit.clone(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, ApplyReferralEditResponse>(&mut tls).await? {
        ApplyReferralEditResponse::Ok(()) => Ok(()),
        ApplyReferralEditResponse::Err { reason } => {
            bail!("peer refused the referral edit: {reason}")
        }
    }
}

/// List every unexpired issued certificate the daemon holds
/// (admin-authenticated), including revoked certificates.
pub async fn list_issued(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    expected: &CaIdentity,
) -> Result<Vec<IssuedEntry>> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ListIssued(ListIssuedRequest { credential: credential.clone() }),
    )
    .await?;
    let mut entries = Vec::new();
    loop {
        match admin_proto::read_msg::<_, ListIssuedResponse>(&mut tls).await? {
            ListIssuedResponse::Entry { entry } => entries.push(entry),
            ListIssuedResponse::End => return Ok(entries),
            ListIssuedResponse::Err { reason } => {
                return Err(admin_refusal(
                    expected,
                    &credential,
                    "admin server refused",
                    reason,
                ));
            }
        }
    }
}

/// Revoke certificates by serial (admin-authenticated). The daemon
/// rewrites their records, re-signs the CRL, and immediately distributes it;
/// returns warnings, the operation ID, and per-node delivery results.
pub async fn revoke(
    addr: SocketAddr,
    credential: admin_proto::AdminCredential,
    serials: Vec<u64>,
    reason: &str,
    expected: &CaIdentity,
) -> Result<(Vec<String>, Option<admin_proto::OperationId>, Vec<PeerResult>)> {
    let mut tls = connect_ca_pinned(addr, NodeKind::Client, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Revoke(RevokeRequest {
            credential: credential.clone(),
            serials,
            reason: reason.to_string(),
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, RevokeResponse>(&mut tls).await? {
        RevokeResponse::Ok(RevokeOk { warnings, operation_id, peers }) => {
            Ok((warnings, operation_id, peers))
        }
        RevokeResponse::Err { reason } => {
            Err(admin_refusal(expected, &credential, "admin server refused", reason))
        }
    }
}

/// Push an identity registration to a peer admin server, authenticating
/// with *our* serving cert (server-to-server; the receiver requires the
/// reserved SAN). Unlike the operator-facing calls this does real PKI —
/// the caller is an admin server that has the CA bundle installed — so
/// there is no TOFU and no pinning: the client's roots are the trust anchors.
///
/// Returns `Ok(None)` when the peer's hello shows it has no id-map role
/// — not an error; the pusher fans out to every known peer and skips
/// the ones that can't register identities.
/// Extract the home CA from an admin certificate chain.
pub fn home_ca_from_chain(pem: &[u8]) -> Result<CertificateDer<'static>> {
    let certs = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("parsing admin certificate chain")?;
    if certs.len() < 2 {
        bail!("admin certificate chain does not include its home CA");
    }
    Ok(certs.last().expect("length checked").clone())
}

/// Server→CA: register/update this admin server's facts in the CA's admin domain
/// map, authenticated with the serving cert (peer-cert-gated). Returns the
/// CA's new map version.
pub async fn register(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    home_ca: CertificateDer<'static>,
    req: &RegisterRequest,
) -> Result<admin_proto::DesiredUpdate> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: None, home_ca: &home_ca, ca: true }),
    )
    .await?;
    admin_proto::write_msg(&mut tls, &Request::Register(req.clone())).await?;
    match admin_proto::read_msg::<_, RegisterResponse>(&mut tls).await? {
        RegisterResponse::Ok(ok) => Ok(ok.updates),
        RegisterResponse::Err { reason } => {
            bail!("the CA refused the registration: {reason}")
        }
    }
}

/// Server→CA: drop this admin server (`own_addr`) from the map on uninstall.
pub async fn deregister(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    home_ca: CertificateDer<'static>,
) -> Result<u64> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        NodeKind::AdminServer,
        Some(ExactTarget { id: None, home_ca: &home_ca, ca: true }),
    )
    .await?;
    admin_proto::write_msg(&mut tls, &Request::Deregister).await?;
    match admin_proto::read_msg::<_, admin_proto::DeregisterResponse>(&mut tls).await? {
        admin_proto::DeregisterResponse::Ok(MapVersion { version }) => Ok(version),
        admin_proto::DeregisterResponse::Err { reason } => {
            bail!("the CA refused the deregistration: {reason}")
        }
    }
}

/// Cheap probe: the served map's current version. No client cert needed —
/// the map is public within the admin domain (reads are pinned to the CA).
pub async fn get_map_version(
    client: &PkiClient,
    addr: SocketAddr,
    kind: NodeKind,
) -> Result<u64> {
    let (mut tls, _hello) = connect_pki(client, addr, kind).await?;
    admin_proto::write_msg(&mut tls, &Request::GetMapVersion).await?;
    match admin_proto::read_msg::<_, GetMapVersionResponse>(&mut tls).await? {
        GetMapVersionResponse::Ok(MapVersion { version }) => Ok(version),
        GetMapVersionResponse::Err { reason } => {
            bail!("map version query refused: {reason}")
        }
    }
}

pub async fn get_map_version_from_ca(
    client: &PkiClient,
    addr: SocketAddr,
    home_ca: CertificateDer<'static>,
    kind: NodeKind,
) -> Result<u64> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        kind,
        Some(ExactTarget { id: None, home_ca: &home_ca, ca: true }),
    )
    .await?;
    admin_proto::write_msg(&mut tls, &Request::GetMapVersion).await?;
    match admin_proto::read_msg::<_, GetMapVersionResponse>(&mut tls).await? {
        GetMapVersionResponse::Ok(MapVersion { version }) => Ok(version),
        GetMapVersionResponse::Err { reason } => {
            bail!("map version query refused: {reason}")
        }
    }
}

/// Fetch the whole admin domain map in one round trip — every resolver cluster, every
/// admin server's role, the CA location.
pub async fn get_map(
    client: &PkiClient,
    addr: SocketAddr,
    kind: NodeKind,
) -> Result<AdminDomainMap> {
    let (mut tls, _hello) = connect_pki(client, addr, kind).await?;
    admin_proto::write_msg(&mut tls, &Request::GetMap).await?;
    match admin_proto::read_msg::<_, GetMapResponse>(&mut tls).await? {
        GetMapResponse::Ok(map) => Ok(map),
        GetMapResponse::Err { reason } => bail!("map query refused: {reason}"),
    }
}

pub async fn get_map_from_ca(
    client: &PkiClient,
    addr: SocketAddr,
    home_ca: CertificateDer<'static>,
    kind: NodeKind,
) -> Result<AdminDomainMap> {
    let (mut tls, _hello) = connect_pki_target(
        client,
        addr,
        kind,
        Some(ExactTarget { id: None, home_ca: &home_ca, ca: true }),
    )
    .await?;
    admin_proto::write_msg(&mut tls, &Request::GetMap).await?;
    match admin_proto::read_msg::<_, GetMapResponse>(&mut tls).await? {
        GetMapResponse::Ok(map) => Ok(map),
        GetMapResponse::Err { reason } => bail!("map query refused: {reason}"),
    }
}

#[derive(Debug)]
pub struct RemoveServerOutcome {
    pub version: u64,
    pub operation_id: Option<admin_proto::OperationId>,
    pub revoked: u64,
    pub removed: bool,
    pub affected_clusters: Vec<String>,
    pub peers: Vec<admin_proto::PeerResult>,
    pub crl_peers: Vec<admin_proto::PeerResult>,
    /// Whether the departing member was told to stop answering read clients.
    /// `None` ⇒ it was already gone from the map.
    pub gated: Option<admin_proto::PeerResult>,
}

/// Permanently evict one immutable admin-server identity through the verified
/// CA. Unlike node-self deregistration, this revokes every live serving
/// certificate for the identity and drops its enrollment grant.
pub async fn remove_server(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
    server: admin_proto::AdminServerId,
) -> Result<RemoveServerOutcome> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::RemoveServer(RemoveServerRequest { credential, server }),
    )
    .await?;
    match admin_proto::read_msg::<_, RemoveServerResponse>(&mut tls).await? {
        RemoveServerResponse::Ok(RemoveServerOk {
            version,
            operation_id,
            revoked,
            removed,
            affected_clusters,
            peers,
            crl_peers,
            gated,
        }) => Ok(RemoveServerOutcome {
            version,
            operation_id,
            revoked,
            removed,
            affected_clusters,
            peers,
            crl_peers,
            gated,
        }),
        RemoveServerResponse::Err { reason } => {
            bail!("the CA refused server removal: {reason}")
        }
    }
}

/// Ask the verified CA to re-send its current address, map, and CRL to
/// every registered node. Idempotent and intended as the explicit retry after
/// a node was offline during CA recovery/relocation.
pub async fn reconcile_ca(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    credential: admin_proto::AdminCredential,
) -> Result<(admin_proto::OperationId, Vec<admin_proto::PeerResult>)> {
    let mut tls = connect_ca_pinned(addr, kind, expected).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::ReconcileCa(ReconcileCaRequest { credential }),
    )
    .await?;
    match admin_proto::read_msg::<_, ReconcileCaResponse>(&mut tls).await? {
        ReconcileCaResponse::Ok(PropagationOk { operation_id, peers }) => {
            Ok((operation_id, peers))
        }
        ReconcileCaResponse::Err { reason } => {
            bail!("the CA refused CA reconciliation: {reason}")
        }
    }
}

/// Connect to an admin server with real PKI using a reusable trust and
/// authentication context.
async fn connect_pki<C: HasTlsClient>(
    client: &C,
    addr: SocketAddr,
    kind: NodeKind,
) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, ServerHello)> {
    connect_pki_target(client, addr, kind, None).await
}

struct ExactTarget<'a> {
    id: Option<admin_proto::AdminServerId>,
    home_ca: &'a CertificateDer<'a>,
    ca: bool,
}

async fn connect_pki_target<C: HasTlsClient>(
    client: &C,
    addr: SocketAddr,
    kind: NodeKind,
    target: Option<ExactTarget<'_>>,
) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, ServerHello)> {
    let connector = client.tls_client().connector(addr);
    let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .with_context(|| format!("timed out connecting to admin server {addr}"))?
        .with_context(|| format!("connecting to admin server {addr}"))?;
    let server_name = ServerName::try_from(SERVING_SAN).context("server name")?;
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake with admin server")?;
    let hello = exchange_hello(&mut tls, kind).await?;
    let leaf = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first())
        .context("admin server presented no serving certificate")?;
    let identity = crate::tls::admin_cert_identity_from_der(leaf.as_ref())?;
    if identity.server_id != hello.server_id || identity.ca != hello.ca {
        bail!("admin server hello identity does not match its serving certificate");
    }
    if let Some(target) = target {
        if target.id.is_some_and(|id| identity.server_id != id)
            || identity.ca != target.ca
        {
            bail!(
                "admin target identity mismatch: expected {} (CA={}), got {} (CA={})",
                target.id.map(|id| id.to_string()).unwrap_or_else(|| "<CA>".into()),
                target.ca,
                identity.server_id,
                identity.ca
            );
        }
        use x509_parser::prelude::{FromDer, X509Certificate};
        let (_, leaf) = X509Certificate::from_der(leaf.as_ref())
            .map_err(|e| anyhow!("parsing target certificate: {e}"))?;
        let (_, ca) = X509Certificate::from_der(target.home_ca.as_ref())
            .map_err(|e| anyhow!("parsing home CA certificate: {e}"))?;
        leaf.verify_signature(Some(ca.public_key()))
            .map_err(|e| anyhow!("target is not issued by the exact home CA: {e}"))?;
    }
    Ok((tls, hello))
}

/// A queued renewal awaiting (possibly automatic) approval.
pub struct PendingRenewal {
    pub request_id: String,
    name: String,
    our_spki: Vec<u8>,
    kc: KeyAndCsr,
}

impl PendingRenewal {
    /// The requested name (single DNS SAN) of this renewal.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The SubjectPublicKeyInfo DER of the fresh key — what a later
    /// [`poll_renewal`] checks the issued leaf binds to.
    pub fn our_spki(&self) -> &[u8] {
        &self.our_spki
    }

    /// The fresh private key, PKCS#8 PEM. **Secret** — a caller that
    /// persists it must seal/encrypt it first.
    pub fn private_key_pem(&self) -> &str {
        &self.kc.private_key_pem
    }

    /// The fresh CSR PEM (not secret).
    pub fn csr_pem(&self) -> &str {
        &self.kc.csr_pem
    }

    /// Reconstruct a pending renewal persisted by an earlier cycle so
    /// [`poll_renewal`] can resume it — a renewal queued in one pass and
    /// approved later must still install, which means the fresh key it
    /// was signed against has to survive across passes (sealed on disk).
    pub fn resume(
        request_id: String,
        name: String,
        our_spki: Vec<u8>,
        private_key_pem: Zeroizing<String>,
        csr_pem: String,
    ) -> Self {
        PendingRenewal {
            request_id,
            name,
            our_spki,
            kc: KeyAndCsr { private_key_pem, csr_pem },
        }
    }
}

/// Queue a **verified renewal**: generate a fresh key + CSR for `name`
/// and enqueue it over a connection authenticated by the *current*
/// cert + key — the proof of possession that marks the request
/// renewable without a glyph. Fully unattended: the server is verified
/// with real PKI against the installed `roots`.
pub async fn enqueue_renewal(
    client: &AuthenticatedPkiClient,
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    validity: Duration,
) -> Result<PendingRenewal> {
    let kc = generate_key_and_csr(name)?;
    let our_spki = csr_spki(&kc.csr_pem)?;
    let (mut tls, _hello) = connect_pki(client, addr, kind).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Enqueue(EnqueueRequest {
            kind,
            csr_pem: kc.csr_pem.clone(),
            requested_name: name.to_string(),
            requested_validity: validity,
            enrollment: None,
            replaces_serial: None,
        }),
    )
    .await?;
    match admin_proto::read_msg::<_, EnqueueResponse>(&mut tls).await? {
        EnqueueResponse::Ok(QueuedOk { request_id }) => {
            Ok(PendingRenewal { request_id, name: name.to_string(), our_spki, kc })
        }
        EnqueueResponse::Err { reason } => {
            bail!("admin server refused to queue the renewal: {reason}")
        }
    }
}

/// Check on a queued renewal. On `Signed`, the returned leaf must
/// contain our fresh key and exact name, and be signed by a CA in the
/// **installed** trust bundle (`installed_pem`, the content of the
/// host's `trusted` file). The returned `trusted_pem` is carried back
/// for [`reconcile_trusted_bundle`] to fold in same-SPKI CA refreshes —
/// it is *not* itself a trust anchor for the leaf.
pub async fn poll_renewal(
    client: &PkiClient,
    addr: SocketAddr,
    kind: NodeKind,
    pending: &PendingRenewal,
    installed_pem: &str,
) -> Result<PollOutcome> {
    let (mut tls, _hello) = connect_pki(client, addr, kind).await?;
    admin_proto::write_msg(
        &mut tls,
        &Request::Poll(PollRequest { request_id: pending.request_id.clone() }),
    )
    .await?;
    match admin_proto::read_msg::<_, PollResponse>(&mut tls).await? {
        PollResponse::Pending => Ok(PollOutcome::Pending),
        PollResponse::Denied { reason } => Ok(PollOutcome::Denied(reason)),
        PollResponse::Unknown => Ok(PollOutcome::Expired),
        PollResponse::Signed(SignOk {
            signed_cert_pem, trusted_pem, warnings, ..
        }) => {
            verify_issued_any(
                installed_pem,
                &pending.name,
                &pending.our_spki,
                &signed_cert_pem,
            )?;
            Ok(PollOutcome::Issued(Issued {
                cert_pem: signed_cert_pem,
                private_key_pem: pending.kc.private_key_pem.clone(),
                trusted_pem,
                warnings,
            }))
        }
    }
}

/// [`get_info`] over real PKI (webpki against `roots`) — for unattended
/// callers that hold the trust bundle, e.g. the renewal daemon mapping
/// the admin domain to find the CA. Only genuine members of the admin domain can
/// answer; no human confirmation involved.
pub async fn get_info_pki(
    client: &PkiClient,
    addr: SocketAddr,
    kind: NodeKind,
) -> Result<GetInfoResponse> {
    let (mut tls, _hello) = connect_pki(client, addr, kind).await?;
    admin_proto::write_msg(&mut tls, &Request::GetInfo).await?;
    admin_proto::read_msg(&mut tls).await
}

/// [`get_crl`] over real PKI — the renewal daemon's CRL pull.
pub async fn get_crl_pki(
    client: &PkiClient,
    addr: SocketAddr,
    kind: NodeKind,
) -> Result<Option<String>> {
    let (mut tls, _hello) = connect_pki(client, addr, kind).await?;
    admin_proto::write_msg(&mut tls, &Request::GetCrl).await?;
    let resp: admin_proto::GetCrlResponse = admin_proto::read_msg(&mut tls).await?;
    Ok(resp.crl_pem)
}

/// Verify a leaf binds to our key + name and is signed by *some* CA in
/// `bundle` — the renewal-path counterpart of [`verify_issued`]. The
/// `bundle` here is the host's **installed** trust bundle, not anything
/// the peer returned, so a leaf is only accepted if one of the CAs we
/// already trust signed it (federated bundles carry several CAs; any of
/// them may have signed).
pub(crate) fn verify_issued_any(
    bundle: &str,
    name: &str,
    our_spki: &[u8],
    signed_cert_pem: &str,
) -> Result<()> {
    let mut rd = std::io::Cursor::new(bundle.as_bytes());
    let mut last_err = anyhow!("the installed trust bundle contains no certificates");
    for der in rustls_pemfile::certs(&mut rd).flatten() {
        match verify_issued_leaf(signed_cert_pem, &der, name, our_spki) {
            Ok(()) => return Ok(()),
            Err(e) => last_err = e,
        }
    }
    Err(last_err.context(
        "the renewed certificate failed verification against every CA in the \
         installed trust bundle",
    ))
}

/// Build the trust bundle to install after a renewal, anchored to what
/// we already trust. Starting from the installed roots, an installed CA
/// is replaced by a cert from the peer's returned bundle only when that
/// cert has the same public key (SPKI) *and* carries a valid signature
/// under that key — i.e. a genuine same-key CA-cert refresh (extended
/// validity). Certs whose key we don't already trust are ignored
/// (with a warning): introducing a new trust anchor is an out-of-band
/// admin action, never something an arbitrary renewal peer can do. An
/// installed root the peer omitted is kept.
pub fn reconcile_trusted_bundle(
    installed_pem: &str,
    returned_pem: &str,
) -> Result<String> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    // Installed roots, in order, keyed by SPKI fingerprint so a returned
    // refresh can replace the matching entry in place.
    let mut roots: Vec<(Fingerprint, Vec<u8>)> = Vec::new();
    for der in rustls_pemfile::certs(&mut std::io::Cursor::new(installed_pem.as_bytes()))
        .flatten()
    {
        let fp = Fingerprint::of_cert_der(der.as_ref())
            .context("fingerprinting an installed trust anchor")?;
        roots.push((fp, der.as_ref().to_vec()));
    }
    anyhow::ensure!(
        !roots.is_empty(),
        "the installed trust bundle contains no certificates"
    );
    for der in rustls_pemfile::certs(&mut std::io::Cursor::new(returned_pem.as_bytes()))
        .flatten()
    {
        let fp = match Fingerprint::of_cert_der(der.as_ref()) {
            Ok(fp) => fp,
            Err(e) => {
                warn!("ignoring an unparseable cert in the renewal trust bundle: {e:#}");
                continue;
            }
        };
        match roots.iter().position(|(known, _)| *known == fp) {
            None => warn!(
                "ignoring CA {} offered by the renewal response: it is not a \
                 trust anchor we already hold (distribute trust changes out of band)",
                fp.short()
            ),
            Some(idx) => {
                // Same pinned key — accept the refreshed cert only if it is
                // validly self-signed under that key (a self-signed CA
                // getting a new validity window), OR validly signed by the
                // SAME issuer that signed the cert currently in this slot
                // (the external root of an externally-signed intermediate).
                // The slot's public key never changes, so the glyph never
                // moves; and requiring the *installed cert's own issuer*
                // (not merely any held anchor) stops a federated peer that
                // controls a DIFFERENT anchor from overwriting this slot's
                // cert body with tampered constraints. A forged signature
                // fails both checks.
                let (_, cert) = X509Certificate::from_der(der.as_ref())
                    .map_err(|e| anyhow!("parsing a refreshed CA cert: {e}"))?;
                if refresh_is_authorized(&cert, &roots[idx].1, &roots) {
                    roots[idx].1 = der.as_ref().to_vec();
                } else {
                    warn!(
                        "ignoring same-key CA refresh for {}: not validly \
                         self-signed and not signed by the installed cert's issuer",
                        fp.short()
                    );
                }
            }
        }
    }
    let mut out = String::new();
    for (_, der) in &roots {
        out.push_str(&pem_encode_cert(der));
    }
    Ok(out)
}

/// Whether a returned CA cert whose SPKI already matches a pinned anchor
/// may replace it. Accept if it is validly self-signed under its own key
/// (a self-signed CA refreshing its validity window), or validly signed by
/// the **same issuer** that signed the cert currently installed in this
/// slot (`installed_der`) — the external root of an externally-signed
/// intermediate. Requiring the installed cert's own issuer, rather than any
/// held anchor, prevents a federated peer that controls a different trust
/// anchor from overwriting this slot with a same-key cert bearing tampered
/// constraints (a targeted DoS). The pinned public key never changes either
/// way, so the glyph cannot move and no new anchor can be introduced.
fn refresh_is_authorized(
    cert: &x509_parser::certificate::X509Certificate<'_>,
    installed_der: &[u8],
    roots: &[(Fingerprint, Vec<u8>)],
) -> bool {
    use x509_parser::prelude::{FromDer, X509Certificate};
    if cert.verify_signature(Some(cert.public_key())).is_ok() {
        return true;
    }
    // The issuer of the cert currently pinned in this slot.
    let installed = match X509Certificate::from_der(installed_der) {
        Ok((_, c)) => c,
        Err(_) => return false,
    };
    let issuer_raw = installed.issuer().as_raw();
    roots.iter().any(|(_, der)| {
        X509Certificate::from_der(der).ok().is_some_and(|(_, anchor)| {
            anchor.subject().as_raw() == issuer_raw
                && cert.verify_signature(Some(anchor.public_key())).is_ok()
        })
    })
}

/// PEM-encode a single DER certificate (no external pem dep — the rest
/// of this module already hand-rolls PEM via [`pem_to_der`]).
fn pem_encode_cert(der: &[u8]) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::with_capacity(b64.len() + 64);
    out.push_str("-----BEGIN CERTIFICATE-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap());
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

/// True if `fp` is the identity (SPKI) fingerprint of any certificate
/// in the PEM `bundle`.
fn bundle_contains(bundle: &str, fp: &Fingerprint) -> bool {
    let mut rd = std::io::Cursor::new(bundle.as_bytes());
    rustls_pemfile::certs(&mut rd).flatten().any(|der| {
        Fingerprint::of_cert_der(der.as_ref()).map(|f| f == *fp).unwrap_or(false)
    })
}

/// Verify the admin server's issued leaf binds to what the operator
/// confirmed: signed by the confirmed CA (`ca_der`), carrying exactly
/// the requested DNS SAN (`name`), and containing our own public key
/// (`our_spki`, the SubjectPublicKeyInfo DER from our CSR).
fn verify_issued_leaf(
    leaf_pem: &str,
    ca_der: &CertificateDer<'_>,
    name: &str,
    our_spki: &[u8],
) -> Result<()> {
    use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};
    let leaf_der = pem_to_der(leaf_pem, "CERTIFICATE")?;
    let (_, leaf) = X509Certificate::from_der(&leaf_der)
        .map_err(|e| anyhow!("parsing issued cert: {e}"))?;
    let (_, ca) = X509Certificate::from_der(ca_der.as_ref())
        .map_err(|e| anyhow!("parsing CA cert: {e}"))?;
    leaf.verify_signature(Some(ca.public_key()))
        .map_err(|e| anyhow!("issued cert is not signed by the confirmed CA: {e}"))?;
    // Exactly one DNS SAN, equal to the requested name (DNS is
    // case-insensitive).
    let dns: Vec<&str> = leaf
        .subject_alternative_name()
        .ok()
        .flatten()
        .map(|ext| {
            ext.value
                .general_names
                .iter()
                .filter_map(|gn| match gn {
                    GeneralName::DNSName(d) => Some(*d),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    if dns.len() != 1 || !dns[0].eq_ignore_ascii_case(name) {
        bail!("issued cert SAN {dns:?} does not match the requested name {name:?}");
    }
    if leaf.public_key().raw != our_spki {
        bail!("issued cert's public key does not match the CSR we sent");
    }
    Ok(())
}

/// Extract the SubjectPublicKeyInfo DER from a PEM-encoded CSR.
pub(crate) fn csr_spki(csr_pem: &str) -> Result<Vec<u8>> {
    use x509_parser::certification_request::X509CertificationRequest;
    use x509_parser::prelude::FromDer;
    let der = pem_to_der(csr_pem, "CERTIFICATE REQUEST")?;
    let (_, csr) = X509CertificationRequest::from_der(&der)
        .map_err(|e| anyhow!("parsing our CSR: {e}"))?;
    Ok(csr.certification_request_info.subject_pki.raw.to_vec())
}

/// Decode the first PEM block with the given `label` to DER.
fn pem_to_der(pem: &str, label: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let body = pem
        .split_once(&begin)
        .and_then(|(_, rest)| rest.split_once(&end))
        .map(|(b, _)| b)
        .ok_or_else(|| anyhow!("PEM block {label:?} not found"))?;
    let b64: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| anyhow!("decoding {label} base64: {e}"))
}

/// Verify the serving cert is signed by `ca_der`, carries the reserved
/// [`SERVING_SAN`], and is currently valid. Cross-platform via
/// x509-parser (ring) — no openssl, no webpki EKU requirements.
fn verify_serving_cert(
    serving_der: &CertificateDer<'_>,
    ca_der: &CertificateDer<'_>,
) -> Result<crate::tls::AdminCertIdentity> {
    use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};
    let (_, ca) =
        X509Certificate::from_der(ca_der.as_ref()).context("parsing CA cert")?;
    let (_, leaf) = X509Certificate::from_der(serving_der.as_ref())
        .context("parsing serving cert")?;
    leaf.verify_signature(Some(ca.public_key()))
        .map_err(|e| anyhow!("serving cert is not signed by the confirmed CA: {e}"))?;
    let mut has_san = false;
    if let Ok(Some(ext)) = leaf.subject_alternative_name() {
        for gn in &ext.value.general_names {
            if let GeneralName::DNSName(d) = gn
                && *d == SERVING_SAN
            {
                has_san = true;
            }
        }
    }
    if !has_san {
        bail!("serving cert does not carry the reserved SAN {SERVING_SAN:?}");
    }
    if !leaf.validity().is_valid() {
        bail!("serving cert is expired or not yet valid");
    }
    crate::tls::admin_cert_identity_from_der(serving_der.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestTls {
        config: Arc<rustls::ServerConfig>,
        roots: rustls::RootCertStore,
        client_cert_pem: Vec<u8>,
        client_key_pem: Vec<u8>,
        server_id: admin_proto::AdminServerId,
    }

    fn test_tls(
        server_id: admin_proto::AdminServerId,
        require_client_auth: bool,
    ) -> TestTls {
        use rcgen::{
            BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
            KeyPair, KeyUsagePurpose, SanType, string::Ia5String,
        };
        use rustls::{ServerConfig, server::WebPkiClientVerifier};
        use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params =
            CertificateParams::new(vec!["resumption-test-ca".into()]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages =
            vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::from_params(&ca_params, &ca_key);
        let ca_der = CertificateDer::from(ca_cert.der().to_vec());
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca_der.clone()).unwrap();

        let server_key = KeyPair::generate().unwrap();
        let mut server_params =
            CertificateParams::new(vec![admin_proto::SERVING_SAN.to_string()]).unwrap();
        server_params
            .subject_alt_names
            .push(SanType::URI(Ia5String::try_from(server_id.uri().as_str()).unwrap()));
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_cert = server_params.signed_by(&server_key, &issuer).unwrap();

        let client_key = KeyPair::generate().unwrap();
        let mut client_params =
            CertificateParams::new(vec!["resumption-test-client".into()]).unwrap();
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_cert = client_params.signed_by(&client_key, &issuer).unwrap();

        let builder = ServerConfig::builder_with_provider(TLS_PROVIDER.clone())
            .with_safe_default_protocol_versions()
            .unwrap();
        let builder = if require_client_auth {
            let verifier = WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots.clone()),
                TLS_PROVIDER.clone(),
            )
            .build()
            .unwrap();
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        let mut config = builder
            .with_single_cert(
                vec![CertificateDer::from(server_cert.der().to_vec()), ca_der.clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    server_key.serialize_der(),
                )),
            )
            .unwrap();
        config.session_storage = rustls::server::ServerSessionMemoryCache::new(32);
        config.send_tls13_tickets = 2;
        config.max_early_data_size = 0;

        let mut client_cert_pem = client_cert.pem();
        client_cert_pem.push_str(&ca_cert.pem());
        TestTls {
            config: Arc::new(config),
            roots,
            client_cert_pem: client_cert_pem.into_bytes(),
            client_key_pem: client_key.serialize_pem().into_bytes(),
            server_id,
        }
    }

    async fn handshake_server(
        config: Arc<rustls::ServerConfig>,
        server_id: admin_proto::AdminServerId,
        connections: usize,
    ) -> (
        SocketAddr,
        tokio::sync::mpsc::UnboundedReceiver<(rustls::HandshakeKind, bool)>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::{io::AsyncWriteExt, net::TcpListener};
        use tokio_rustls::TlsAcceptor;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = TlsAcceptor::from(config);
        let (handshakes_tx, handshakes_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            for _ in 0..connections {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut tls = acceptor.accept(tcp).await.unwrap();
                tls.flush().await.unwrap();
                let handshake = tls.get_ref().1.handshake_kind().unwrap();
                let client_authenticated =
                    tls.get_ref().1.peer_certificates().is_some_and(|c| !c.is_empty());
                handshakes_tx.send((handshake, client_authenticated)).unwrap();
                let hello =
                    admin_proto::read_msg::<_, ClientHello>(&mut tls).await.unwrap();
                assert_eq!(hello.protocol_version, PROTOCOL_VERSION);
                admin_proto::write_msg(
                    &mut tls,
                    &ServerHello {
                        protocol_version: PROTOCOL_VERSION,
                        domain: "resumption.test".into(),
                        roles: BitFlags::empty(),
                        server_id,
                        ca: false,
                    },
                )
                .await
                .unwrap();
                match admin_proto::read_msg::<_, Request>(&mut tls).await {
                    Ok(Request::GetMapVersion) => {
                        admin_proto::write_msg(
                            &mut tls,
                            &GetMapVersionResponse::Ok(MapVersion { version: 1 }),
                        )
                        .await
                        .unwrap();
                    }
                    Err(_) => {}
                    Ok(request) => panic!("unexpected test request {request:?}"),
                }
            }
        });
        (addr, handshakes_rx, task)
    }

    async fn complete_test_request(mut tls: tokio_rustls::client::TlsStream<TcpStream>) {
        admin_proto::write_msg(&mut tls, &Request::GetMapVersion).await.unwrap();
        assert!(matches!(
            admin_proto::read_msg::<_, GetMapVersionResponse>(&mut tls).await.unwrap(),
            GetMapVersionResponse::Ok(MapVersion { version: 1 })
        ));
    }

    #[tokio::test]
    async fn confirmed_identity_resumes_sessions_per_endpoint() {
        use rustls::HandshakeKind::{Full, Resumed};

        let material = test_tls(admin_proto::AdminServerId::new(), false);
        let (addr_a, mut handshakes_a, server_a) =
            handshake_server(material.config.clone(), material.server_id, 4).await;
        let (addr_b, mut handshakes_b, server_b) =
            handshake_server(material.config, material.server_id, 3).await;

        let identity_a = fetch_identity(addr_a, NodeKind::Client).await.unwrap();
        assert_eq!(handshakes_a.recv().await.unwrap(), (Full, false));
        complete_test_request(
            connect_pinned(addr_a, NodeKind::Client, &identity_a).await.unwrap(),
        )
        .await;
        assert_eq!(handshakes_a.recv().await.unwrap(), (Resumed, false));

        let identity_b =
            fetch_identity_with(addr_b, NodeKind::Client, identity_a.tls_client.clone())
                .await
                .unwrap();
        assert_eq!(handshakes_b.recv().await.unwrap(), (Full, false));
        complete_test_request(
            connect_pinned(addr_a, NodeKind::Client, &identity_a).await.unwrap(),
        )
        .await;
        assert_eq!(handshakes_a.recv().await.unwrap(), (Resumed, false));
        complete_test_request(
            connect_pinned(addr_b, NodeKind::Client, &identity_b).await.unwrap(),
        )
        .await;
        assert_eq!(handshakes_b.recv().await.unwrap(), (Resumed, false));
        complete_test_request(
            connect_pinned(addr_b, NodeKind::Client, &identity_b).await.unwrap(),
        )
        .await;
        assert_eq!(handshakes_b.recv().await.unwrap(), (Resumed, false));
        drop(fetch_identity(addr_a, NodeKind::Client).await.unwrap());
        assert_eq!(handshakes_a.recv().await.unwrap(), (Full, false));

        server_a.await.unwrap();
        server_b.await.unwrap();
    }

    #[tokio::test]
    async fn pki_clients_resume_and_new_handles_start_fresh() {
        use rustls::HandshakeKind::{Full, Resumed};

        let anonymous_material = test_tls(admin_proto::AdminServerId::new(), false);
        let (anonymous_addr, mut anonymous_handshakes, anonymous_server) =
            handshake_server(anonymous_material.config, anonymous_material.server_id, 3)
                .await;
        let client = PkiClient::new(anonymous_material.roots.clone()).unwrap();
        assert_eq!(
            get_map_version(&client, anonymous_addr, NodeKind::Client).await.unwrap(),
            1
        );
        assert_eq!(anonymous_handshakes.recv().await.unwrap(), (Full, false));
        assert_eq!(
            get_map_version(&client, anonymous_addr, NodeKind::Client).await.unwrap(),
            1
        );
        assert_eq!(anonymous_handshakes.recv().await.unwrap(), (Resumed, false));
        let replacement = PkiClient::new(anonymous_material.roots).unwrap();
        assert_eq!(
            get_map_version(&replacement, anonymous_addr, NodeKind::Client)
                .await
                .unwrap(),
            1
        );
        assert_eq!(anonymous_handshakes.recv().await.unwrap(), (Full, false));
        anonymous_server.await.unwrap();

        let authenticated_material = test_tls(admin_proto::AdminServerId::new(), true);
        let (authenticated_addr, mut authenticated_handshakes, authenticated_server) =
            handshake_server(
                authenticated_material.config,
                authenticated_material.server_id,
                2,
            )
            .await;
        let client = AuthenticatedPkiClient::from_pem(
            authenticated_material.roots,
            &authenticated_material.client_cert_pem,
            &authenticated_material.client_key_pem,
        )
        .unwrap();
        complete_test_request(
            connect_pki(&client, authenticated_addr, NodeKind::AdminServer)
                .await
                .unwrap()
                .0,
        )
        .await;
        assert_eq!(authenticated_handshakes.recv().await.unwrap(), (Full, true));
        complete_test_request(
            connect_pki(&client, authenticated_addr, NodeKind::AdminServer)
                .await
                .unwrap()
                .0,
        )
        .await;
        assert_eq!(authenticated_handshakes.recv().await.unwrap(), (Resumed, true));
        authenticated_server.await.unwrap();
    }

    /// A valid home-CA satellite that serves a hostile map naming itself as
    /// CA. It records every post-hello request, which lets the test
    /// prove CA resolution never exposes either credential form.
    async fn hostile_satellite(
        server_id: admin_proto::AdminServerId,
    ) -> (
        SocketAddr,
        tokio::sync::mpsc::UnboundedReceiver<&'static str>,
        tokio::task::JoinHandle<()>,
    ) {
        use admin_proto::{AdminServerEntry, ServerState};
        use tokio::net::TcpListener;
        use tokio_rustls::TlsAcceptor;

        let material = test_tls(server_id, false);
        let acceptor = TlsAcceptor::from(material.config);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hostile_map = AdminDomainMap {
            id_map_version: None,
            version: 1,
            ca: server_id,
            admin_servers: vec![AdminServerEntry {
                id: server_id,
                addr,
                // These are untrusted map claims. The serving certificate has
                // no CA role URI, which must win.
                roles: Role::Ca | Role::Resolver,
                resolver: None,
                cluster: None,
                state: ServerState::Registered,
                reported_read_gate: None,
                reported_id_map_version: None,
                reported_perms_version: None,
            }],
            resolver_clusters: vec![],
        };
        let (seen_tx, seen_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else { break };
                let Ok(mut tls) = acceptor.accept(tcp).await else { continue };
                let Ok(hello) = admin_proto::read_msg::<_, ClientHello>(&mut tls).await
                else {
                    continue;
                };
                let _ = admin_proto::write_msg(
                    &mut tls,
                    &ServerHello {
                        protocol_version: PROTOCOL_VERSION,
                        domain: "hostile.test".into(),
                        roles: Role::Resolver.into(),
                        server_id,
                        ca: false,
                    },
                )
                .await;
                if hello.protocol_version != PROTOCOL_VERSION {
                    continue;
                }
                // fetch_identity closes immediately after the hello. A real
                // request is present only on the map/credential connections.
                let Ok(request) = admin_proto::read_msg::<_, Request>(&mut tls).await
                else {
                    continue;
                };
                match request {
                    Request::GetMap => {
                        let _ = seen_tx.send("GetMap");
                        let _ = admin_proto::write_msg(
                            &mut tls,
                            &GetMapResponse::Ok(hostile_map.clone()),
                        )
                        .await;
                    }
                    Request::Login(_) => {
                        let _ = seen_tx.send("PASSWORD LEAKED");
                    }
                    Request::Logout(_) => {
                        let _ = seen_tx.send("SESSION LEAKED");
                    }
                    _ => {
                        let _ = seen_tx.send("unexpected request");
                    }
                }
            }
        });
        (addr, seen_rx, task)
    }

    #[tokio::test]
    async fn hostile_home_ca_satellite_never_receives_password_or_session() {
        let satellite = admin_proto::AdminServerId::new();
        let (addr, mut seen, server) = hostile_satellite(satellite).await;
        let identity = fetch_identity(addr, NodeKind::Client).await.unwrap();
        assert_eq!(identity.server_id, satellite);
        assert!(!identity.ca);

        let password = "sentinel-password-must-not-reach-satellite";
        let err = login(addr, &identity, "admin", password)
            .await
            .err()
            .expect("hostile satellite must be refused");
        assert!(
            format!("{err:#}").contains("not the exact CA"),
            "unexpected password-path error: {err:#}"
        );

        let token = "sentinel-session-must-not-reach-satellite";
        let err = logout(addr, &identity, token).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("not the exact CA"),
            "unexpected session-path error: {err:#}"
        );

        let mut requests = Vec::new();
        while let Ok(request) = seen.try_recv() {
            requests.push(request);
        }
        assert_eq!(requests, vec!["GetMap", "GetMap"]);
        server.abort();
    }

    #[test]
    fn generates_a_parseable_ecdsa_csr() {
        let kc = generate_key_and_csr("resolver.example.com").unwrap();
        assert!(kc.csr_pem.contains("BEGIN CERTIFICATE REQUEST"));
        assert!(kc.private_key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn bootstrap_cluster_does_not_flatten_the_resolver_hierarchy() {
        use admin_proto::{
            AdminServerEntry, ResolverClusterEntry, ResolverClusterId,
            ResolverClusterState, Role, ServerState,
        };
        let ca = admin_proto::AdminServerId::new();
        let root_server = admin_proto::AdminServerId::new();
        let satellite = admin_proto::AdminServerId::new();
        let waiting = admin_proto::AdminServerId::new();
        let root_cluster = ResolverClusterId::new();
        let child_cluster = ResolverClusterId::new();
        let root = ResolverAddr {
            addr: "192.168.50.11:4564".parse().unwrap(),
            auth: admin_proto::InfoAuth::Tls { name: "root.example".into() },
        };
        let child = ResolverAddr {
            addr: "192.168.60.15:4564".parse().unwrap(),
            auth: admin_proto::InfoAuth::Tls { name: "child.example".into() },
        };
        let waiting_root = ResolverAddr {
            addr: "192.168.50.12:4564".parse().unwrap(),
            auth: admin_proto::InfoAuth::Tls { name: "root.example".into() },
        };
        let map = AdminDomainMap {
            id_map_version: None,
            version: 1,
            ca,
            admin_servers: vec![
                AdminServerEntry {
                    id: ca,
                    addr: "192.168.50.11:4565".parse().unwrap(),
                    roles: Role::Ca.into(),
                    resolver: None,
                    cluster: None,
                    state: ServerState::Registered,
                    reported_read_gate: None,
                    reported_id_map_version: None,
                    reported_perms_version: None,
                },
                AdminServerEntry {
                    id: root_server,
                    addr: "192.168.50.10:4565".parse().unwrap(),
                    roles: Role::Resolver.into(),
                    resolver: Some(root.clone()),
                    cluster: Some(root_cluster),
                    state: ServerState::Registered,
                    reported_read_gate: None,
                    reported_id_map_version: None,
                    reported_perms_version: None,
                },
                AdminServerEntry {
                    id: waiting,
                    addr: "192.168.50.12:4565".parse().unwrap(),
                    roles: Role::Resolver.into(),
                    resolver: Some(waiting_root.clone()),
                    cluster: Some(root_cluster),
                    state: ServerState::Enrolled,
                    reported_read_gate: None,
                    reported_id_map_version: None,
                    reported_perms_version: None,
                },
                AdminServerEntry {
                    id: satellite,
                    addr: "192.168.60.15:4565".parse().unwrap(),
                    roles: Role::Resolver.into(),
                    resolver: Some(child.clone()),
                    cluster: Some(child_cluster),
                    state: ServerState::Registered,
                    reported_read_gate: None,
                    reported_id_map_version: None,
                    reported_perms_version: None,
                },
            ],
            resolver_clusters: vec![
                ResolverClusterEntry {
                    id: root_cluster,
                    base: "/".into(),
                    state: ResolverClusterState::Active,
                    members: vec![root.clone(), waiting_root],
                    parent: None,
                    children: vec![child_cluster],
                    perms_version: None,
                },
                ResolverClusterEntry {
                    id: child_cluster,
                    base: "/eu".into(),
                    state: ResolverClusterState::Active,
                    members: vec![child.clone()],
                    parent: Some(root_cluster),
                    children: vec![],
                    perms_version: None,
                },
            ],
        };

        let root_selection = bootstrap_cluster(&map, ca).unwrap();
        let child_selection = bootstrap_cluster(&map, satellite).unwrap();
        assert_eq!(root_selection.ca, "192.168.50.11:4565".parse().unwrap());
        assert_eq!(child_selection.ca, root_selection.ca);
        let ResolverSelection {
            base: root_base,
            topology:
                ResolverClusterTopology {
                    members: root_members,
                    parent: root_parent,
                    children: root_children,
                },
        } = root_selection.resolver.unwrap();
        let ResolverSelection {
            base: child_base,
            topology:
                ResolverClusterTopology {
                    members: child_members,
                    parent: child_parent,
                    children: child_children,
                },
        } = child_selection.resolver.unwrap();
        let child_by_base = cluster_topology_by_base(&map, "/eu").unwrap();
        assert_eq!(root_members, vec![root.clone()]);
        assert_eq!(child_members, vec![child.clone()]);
        assert_eq!(child_by_base.members, vec![child]);
        assert_eq!(root_base, "/");
        assert_eq!(child_base, "/eu");
        assert!(root_parent.is_none());
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].path, "/eu");
        assert_eq!(child_parent.unwrap().addrs, vec![root]);
        assert!(child_children.is_empty());
        assert_eq!(child_by_base.parent.unwrap().path, "/eu");
        assert!(child_by_base.children.is_empty());
    }

    /// A CA that can sign CRLs, plus an empty CRL signed by it.
    fn ca_with_crl(name: &str) -> (String, String) {
        use rcgen::{
            BasicConstraints, CertificateParams, CertificateRevocationListParams, IsCa,
            Issuer, KeyIdMethod, KeyPair, KeyUsagePurpose, SerialNumber,
        };
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.to_string()]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let cert = params.self_signed(&key).unwrap();
        let issuer = Issuer::from_params(&params, &key);
        let now = std::time::SystemTime::now();
        let crl = CertificateRevocationListParams {
            this_update: now.into(),
            next_update: (now + Duration::from_secs(86400)).into(),
            crl_number: SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: vec![],
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&issuer)
        .unwrap();
        (cert.pem(), crl.pem().unwrap())
    }

    /// A CRL pulled from the admin domain is only installable if one of the CAs we
    /// already trust signed it. Without this the renewd distribution path will
    /// write whatever any peer that answers GetCrl hands it, straight into the
    /// file the resolver's TLS acceptor rebuilds from.
    #[test]
    fn a_crl_must_be_signed_by_a_ca_in_the_installed_bundle() {
        let (home_pem, home_crl) = ca_with_crl("home-ca");
        let (foreign_pem, foreign_crl) = ca_with_crl("foreign-ca");
        let unrelated = self_signed("unrelated", &rcgen::KeyPair::generate().unwrap());

        validate_crl_against_bundle(&home_crl, &home_pem).unwrap();
        assert!(validate_crl_against_bundle(&foreign_crl, &home_pem).is_err());
        // A federated bundle accepts a CRL from any of its anchors.
        let federated = format!("{home_pem}{foreign_pem}");
        validate_crl_against_bundle(&home_crl, &federated).unwrap();
        validate_crl_against_bundle(&foreign_crl, &federated).unwrap();
        // An unparseable anchor is skipped, not fatal.
        let with_junk = format!("{unrelated}{home_pem}");
        validate_crl_against_bundle(&home_crl, &with_junk).unwrap();
        // Garbage and empty bundles are refused.
        assert!(validate_crl_against_bundle("not a crl", &home_pem).is_err());
        assert!(validate_crl_against_bundle(&home_crl, "").is_err());
        // Two CRLs in one payload is ambiguous; refuse it.
        assert!(
            validate_crl_against_bundle(&format!("{home_crl}{foreign_crl}"), &federated)
                .is_err()
        );
    }

    fn self_signed(name: &str, key: &rcgen::KeyPair) -> String {
        rcgen::CertificateParams::new(vec![name.to_string()])
            .unwrap()
            .self_signed(key)
            .unwrap()
            .pem()
    }

    /// The renewal trust-bundle reconciliation: installed roots are kept,
    /// a returned cert with the same key replaces its installed peer (a
    /// CA-cert refresh), and a returned cert whose key we don't already
    /// trust is dropped — a renewal peer can't introduce a new anchor.
    #[test]
    fn reconcile_keeps_roots_swaps_same_key_drops_unknown() {
        let key_a = rcgen::KeyPair::generate().unwrap();
        let key_b = rcgen::KeyPair::generate().unwrap();
        let key_c = rcgen::KeyPair::generate().unwrap();
        let a = self_signed("ca-a", &key_a);
        let b = self_signed("ca-b", &key_b);
        // Same key as A, different cert => same SPKI, a legitimate refresh.
        let a_refresh = self_signed("ca-a-renewed", &key_a);
        let c = self_signed("ca-c", &key_c);

        let installed = format!("{a}{b}");
        // The peer returns: a refresh of A, B unchanged, and an unknown CA C.
        let returned = format!("{a_refresh}{b}{c}");
        let out = reconcile_trusted_bundle(&installed, &returned).unwrap();

        let fp = |pem: &str| Fingerprint::of_cert_pem(pem.as_bytes()).unwrap();
        // A (by SPKI) and B survive; C is dropped; nothing extra is added.
        assert!(bundle_contains(&out, &fp(&a)));
        assert!(bundle_contains(&out, &fp(&b)));
        assert!(!bundle_contains(&out, &fp(&c)));
        let count = rustls_pemfile::certs(&mut std::io::Cursor::new(out.as_bytes()))
            .flatten()
            .count();
        assert_eq!(count, 2, "exactly the two installed roots remain");

        // The A slot now holds the refreshed cert's DER, not the old one.
        let a_fp = fp(&a);
        let der_of = |pem: &str| pem_to_der(pem, "CERTIFICATE").unwrap();
        let matched = rustls_pemfile::certs(&mut std::io::Cursor::new(out.as_bytes()))
            .flatten()
            .find(|d| Fingerprint::of_cert_der(d.as_ref()).unwrap() == a_fp)
            .unwrap();
        assert_eq!(matched.as_ref(), der_of(&a_refresh).as_slice(), "A was refreshed");
        assert_ne!(matched.as_ref(), der_of(&a).as_slice());
    }

    /// A returned bundle that shares no key with the installed roots
    /// changes nothing — the installed anchors are kept verbatim.
    #[test]
    fn reconcile_ignores_an_all_unknown_bundle() {
        let key_a = rcgen::KeyPair::generate().unwrap();
        let key_x = rcgen::KeyPair::generate().unwrap();
        let a = self_signed("ca-a", &key_a);
        let x = self_signed("attacker-ca", &key_x);
        let out = reconcile_trusted_bundle(&a, &x).unwrap();
        let fp = |pem: &str| Fingerprint::of_cert_pem(pem.as_bytes()).unwrap();
        assert!(bundle_contains(&out, &fp(&a)));
        assert!(!bundle_contains(&out, &fp(&x)));
    }

    fn ca_params(name: &str) -> rcgen::CertificateParams {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyUsagePurpose};
        let mut params = CertificateParams::new(vec![name.to_string()]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages =
            vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
        params
    }

    /// The relaxed rule: a same-SPKI refresh is accepted when it validly
    /// chains to a held anchor (an externally-signed intermediate's root),
    /// not only when it is self-signed; a forged same-SPKI cert that chains
    /// to neither is still rejected.
    #[test]
    fn reconcile_accepts_externally_signed_refresh_and_rejects_forgery() {
        use rcgen::{Issuer, KeyPair};
        let root_key = KeyPair::generate().unwrap();
        let root_params = ca_params("root");
        let root = root_params.self_signed(&root_key).unwrap().pem();
        let root_issuer = Issuer::from_params(&root_params, &root_key);
        let inter_key = KeyPair::generate().unwrap();
        let inter = ca_params("inter").signed_by(&inter_key, &root_issuer).unwrap().pem();
        let inter_refresh =
            ca_params("inter").signed_by(&inter_key, &root_issuer).unwrap().pem();
        let attacker_key = KeyPair::generate().unwrap();
        let attacker_params = ca_params("attacker");
        let attacker_issuer = Issuer::from_params(&attacker_params, &attacker_key);
        let forged =
            ca_params("inter").signed_by(&inter_key, &attacker_issuer).unwrap().pem();

        let fp = |pem: &str| Fingerprint::of_cert_pem(pem.as_bytes()).unwrap();
        let der_of = |pem: &str| pem_to_der(pem, "CERTIFICATE").unwrap();
        let inter_fp = fp(&inter);
        let installed = format!("{root}{inter}");
        let slot_der = |out: &str| {
            rustls_pemfile::certs(&mut std::io::Cursor::new(out.to_string().into_bytes()))
                .flatten()
                .find(|d| Fingerprint::of_cert_der(d.as_ref()).unwrap() == inter_fp)
                .unwrap()
                .as_ref()
                .to_vec()
        };

        // A legit externally-signed refresh (chains to the held root) wins.
        let out = reconcile_trusted_bundle(&installed, &format!("{root}{inter_refresh}"))
            .unwrap();
        assert_eq!(slot_der(&out), der_of(&inter_refresh), "intermediate refreshed");

        // A forged same-SPKI cert is ignored; the installed intermediate stays.
        let out2 =
            reconcile_trusted_bundle(&installed, &format!("{root}{forged}")).unwrap();
        assert_eq!(slot_der(&out2), der_of(&inter), "forged refresh ignored");
    }
}
