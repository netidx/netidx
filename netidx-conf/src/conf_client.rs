//! Conf-server client: discover what a netidx network looks like, join
//! it (generate a key + CSR locally and request a signature over TLS),
//! and enroll new conf servers — verifying the network's CA identity by
//! fingerprint before sending anything secret.
//!
//! Cross-platform — rcgen + rustls + sha2 + x509-parser, never openssl
//! — so a Windows node can talk to a unix conf server.
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
//!    the serving key. From the presented chain (`[serving_leaf, …, ca]`)
//!    the client takes the CA cert, verifies the serving leaf is signed
//!    by it and carries the reserved [`SERVING_SAN`], exchanges hellos
//!    (the server's claimed domain + roles), and returns the CA's
//!    [`Fingerprint`]. The connection is then closed; nothing secret was
//!    sent.
//! 2. The operator compares the fingerprint out of band and, only if it
//!    matches, proceeds.
//! 3. **Everything else** ([`get_info`], [`request_cert`], [`enroll`]):
//!    fresh TOFU handshakes that **pin** the presented CA cert to the
//!    confirmed fingerprint, aborting before sending anything secret if
//!    it changed. Combined with the handshake and serving-cert check,
//!    only a genuine conf server of the confirmed network (holding a
//!    CA-issued reserved-SAN serving cert) can reach the point where a
//!    password is sent.

use crate::{
    conf_proto::{
        self, AddIdentityRequest, AddIdentityResponse, AddRoleAdminRequest,
        AdminListResponse, AdminMgmtResponse, ApplyPermsEditRequest,
        ApplyPermsEditResponse, ApplyReferralEditRequest, ApplyReferralEditResponse,
        ApplyServiceControlRequest, ApplyServiceControlResponse,
        ApproveDelegationRequest, ApproveDelegationResponse, ApproveRequest,
        ApproveResponse, ClientHello, ControlServiceRequest, ControlServiceResponse,
        DelegationEntry, DelegationPollResponse, DelegationRequest, DelegationResponse,
        DenyDelegationRequest, DenyDelegationResponse, DenyRequest, DenyResponse,
        DeregisterRequest, EditPermsRequest, EditPermsResponse, EnqueueRequest,
        EnqueueResponse, EnrollRequest, GetInfoResponse, GetMapResponse,
        GetMapVersionResponse, GetPermsResponse, IssuedEntry, ListAdminsRequest,
        ListDelegationsRequest, ListDelegationsResponse, ListIssuedRequest,
        ListIssuedResponse, ListQueueRequest, ListQueueResponse, NetworkMap, NodeKind,
        PROTOCOL_VERSION, PeerResult, PollRequest, PollResponse, QueueEntry,
        ReferralEdit, RegisterRequest, RegisterResponse, RemoveAdminRequest, Request,
        ResolverAddr, RevokeRequest, RevokeResponse, Role, SERVING_SAN, Secret,
        ServerHello, SetAdminPolicyRequest, SignRequest, SignResponse,
    },
    fingerprint::Fingerprint,
    tls_tofu::TofuVerifier,
};
use anyhow::{Context, Result, anyhow, bail};
use log::warn;
use rustls::ClientConfig;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use zeroize::Zeroizing;

/// A freshly generated leaf identity awaiting signature.
pub struct KeyAndCsr {
    /// PKCS#8 PEM of the private key (ECDSA P-256). Zeroized on drop.
    pub private_key_pem: Zeroizing<String>,
    pub csr_pem: String,
}

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

/// Generate an ECDSA P-256 key and a CSR for `name` (a single DNS SAN
/// equal to the CN). rcgen's only practical keygen is ECDSA; the CA
/// accepts it (see `ca::check_pubkey_strength`) and netidx's TLS
/// runtime is algorithm-agnostic.
pub fn generate_key_and_csr(name: &str) -> Result<KeyAndCsr> {
    use rcgen::{CertificateParams, DnType, KeyPair};
    let key_pair = KeyPair::generate().context("generating key pair")?;
    let mut params =
        CertificateParams::new(vec![name.to_string()]).context("building CSR params")?;
    params.distinguished_name.push(DnType::CommonName, name);
    let csr = params.serialize_request(&key_pair).context("serializing CSR")?;
    Ok(KeyAndCsr {
        private_key_pem: Zeroizing::new(key_pair.serialize_pem()),
        csr_pem: csr.pem().context("encoding CSR PEM")?,
    })
}

/// A conf server's verified identity, captured by [`fetch_identity`]:
/// the CA [`Fingerprint`] to show the operator out of band, the CA
/// certificate it's bound to, and the domain + roles the server claimed
/// inside TLS. Pass it to [`get_info`] / [`request_cert`] / [`enroll`]
/// to pin those connections to exactly the CA the operator confirmed.
#[derive(Clone)]
pub struct CaIdentity {
    /// SHA-256 of the CA cert, for out-of-band comparison (text +
    /// identicon).
    pub fingerprint: Fingerprint,
    /// The network's TLS domain, as claimed in the server hello.
    pub domain: String,
    /// The roles the contacted host claimed in the server hello.
    pub roles: Vec<Role>,
    /// The CA cert the fingerprint is of, kept so later connections can
    /// verify against the *confirmed* CA rather than a re-presented one.
    ca_der: CertificateDer<'static>,
}

impl CaIdentity {
    /// The confirmed CA certificate as PEM — for building serving-cert
    /// chains (`[leaf, ca]`) after an [`enroll`].
    pub fn ca_pem(&self) -> String {
        der_to_pem(self.ca_der.as_ref())
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
/// — for rebuilding serving-style chains (`[leaf, ca]`) after a
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

/// TOFU-handshake to the conf server at `addr`; return the live TLS
/// stream and the certificate chain it presented
/// (`[serving_leaf, …, ca]`). Accepts any cert — trust is established
/// out of band by fingerprint — but the handshake signature is still
/// verified, so the server proves it holds the key for the cert it
/// shows.
async fn connect_tofu(
    addr: SocketAddr,
) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, Vec<CertificateDer<'static>>)> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TofuVerifier::new(provider)))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to conf server {addr}"))?;
    let server_name = ServerName::try_from(SERVING_SAN).context("server name")?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake with conf server")?;
    let chain: Vec<CertificateDer<'static>> = {
        let (_io, conn) = tls.get_ref();
        conn.peer_certificates()
            .ok_or_else(|| anyhow!("conf server presented no certificate"))?
            .iter()
            .map(|c| c.clone().into_owned())
            .collect()
    };
    Ok((tls, chain))
}

/// Split a presented chain into `(serving_leaf, ca)`. The daemon's
/// serving chain is `[serving_leaf, …, ca]`; we need at least the leaf
/// and the CA.
fn split_chain<'a>(
    chain: &'a [CertificateDer<'static>],
) -> Result<(&'a CertificateDer<'static>, &'a CertificateDer<'static>)> {
    if chain.len() < 2 {
        bail!(
            "conf server did not present its CA certificate in the chain \
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
    conf_proto::write_msg(tls, &ClientHello { protocol_version: PROTOCOL_VERSION, kind })
        .await?;
    let hello: ServerHello = conf_proto::read_msg(tls).await?;
    if hello.protocol_version != PROTOCOL_VERSION {
        bail!(
            "conf server speaks protocol version {} but we speak {PROTOCOL_VERSION}",
            hello.protocol_version
        );
    }
    Ok(hello)
}

/// Connect to the conf server at `addr`, TOFU-handshake, exchange
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
pub async fn fetch_identity(addr: SocketAddr, kind: NodeKind) -> Result<CaIdentity> {
    let (mut tls, chain) = connect_tofu(addr).await?;
    let (serving_der, ca_der) = split_chain(&chain)?;
    verify_serving_cert(serving_der, ca_der).context(
        "the conf server's serving certificate is not bound to the CA it presented",
    )?;
    let hello = exchange_hello(&mut tls, kind).await?;
    drop(tls);
    Ok(CaIdentity {
        // The glyph is of the CA's *key* (SPKI), not the cert — stable
        // across same-key CA renewals.
        fingerprint: Fingerprint::of_cert_der(ca_der.as_ref())
            .context("fingerprinting the presented CA certificate")?,
        domain: hello.domain,
        roles: hello.roles,
        ca_der: ca_der.clone(),
    })
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
    let (mut tls, chain) = connect_tofu(addr).await?;
    let (serving_der, presented_ca) = split_chain(&chain)?;
    // An unparseable CA cert is treated as a mismatch — fail closed.
    let presented_fp = Fingerprint::of_cert_der(presented_ca.as_ref()).ok();
    if presented_fp != Some(expected.fingerprint) {
        bail!(
            "the conf server's identity changed since you confirmed it \
             (fingerprint mismatch); aborted before sending anything"
        );
    }
    // Everything downstream binds to the *confirmed* CA, not the
    // re-presented one (identical given the pin passed, but this makes
    // the trust anchor explicit).
    verify_serving_cert(serving_der, &expected.ca_der).context(
        "the conf server's serving certificate failed verification against the \
         confirmed CA — this is not the network you confirmed",
    )?;
    exchange_hello(&mut tls, kind).await?;
    Ok(tls)
}

/// Fetch one conf server's local facts + known peers, pinned to the
/// confirmed identity. See [`aggregate`] for the network-wide picture.
pub async fn get_info(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<GetInfoResponse> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(&mut tls, &Request::GetInfo).await?;
    conf_proto::read_msg(&mut tls).await
}

/// Fetch the whole network map from one conf server, pinned to the
/// confirmed CA identity — one round trip is the entire network. The
/// client-facing counterpart of [`get_map`] (which authenticates with a
/// serving cert for the server-to-server refresh path).
pub async fn get_map_pinned(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<NetworkMap> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(&mut tls, &Request::GetMap).await?;
    match conf_proto::read_msg::<_, GetMapResponse>(&mut tls).await? {
        GetMapResponse::Ok { map } => Ok(map),
        GetMapResponse::Err { reason } => bail!("map query refused: {reason}"),
    }
}

/// Server→server: apply a perms edit to a peer conf server (serving-cert
/// authed). Mirrors [`push_referral_edit`].
pub async fn push_perms_edit(
    addr: SocketAddr,
    serving_cert_pem: &[u8],
    serving_key_pem: &[u8],
    roots: rustls::RootCertStore,
    perms_json: &str,
) -> Result<()> {
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(serving_key_pem))
        .context("parsing serving key")?
        .ok_or_else(|| anyhow!("no private key found in serving key PEM"))?;
    let (mut tls, _hello) =
        connect_pki(addr, roots, Some((serving_cert_pem, key)), NodeKind::ConfServer)
            .await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ApplyPermsEdit(ApplyPermsEditRequest {
            perms_json: perms_json.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, ApplyPermsEditResponse>(&mut tls).await? {
        ApplyPermsEditResponse::Ok => Ok(()),
        ApplyPermsEditResponse::Err { reason } => {
            bail!("peer refused the perms edit: {reason}")
        }
    }
}

/// Read a conf server's local perms, pinned to the confirmed CA (perms are
/// readable within the trust domain). The client routes to a member of the
/// cluster it wants.
pub async fn get_perms(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<String> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(&mut tls, &Request::GetPerms).await?;
    match conf_proto::read_msg::<_, GetPermsResponse>(&mut tls).await? {
        GetPermsResponse::Ok { perms_json } => Ok(perms_json),
        GetPermsResponse::Err { reason } => bail!("perms read refused: {reason}"),
    }
}

/// Admin → CA (pinned): edit a target cluster's perms; returns the per-peer
/// propagation results so the caller can surface a partial failure.
pub async fn edit_perms(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    admin: &str,
    password: &str,
    target_path: &str,
    perms_json: &str,
) -> Result<Vec<conf_proto::PeerResult>> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::EditPerms(EditPermsRequest {
            admin: admin.to_string(),
            password: conf_proto::Secret(password.to_string()),
            target_path: target_path.to_string(),
            perms_json: perms_json.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, EditPermsResponse>(&mut tls).await? {
        EditPermsResponse::Ok { peers } => Ok(peers),
        EditPermsResponse::Err { reason } => {
            bail!("the CA refused the perms edit: {reason}")
        }
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
    admin: &str,
    password: &str,
    name: &str,
    new_password: &str,
    policy: crate::ca_policy::Policy,
) -> Result<()> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::AddRoleAdmin(AddRoleAdminRequest {
            admin: admin.to_string(),
            password: conf_proto::Secret(password.to_string()),
            name: name.to_string(),
            new_password: conf_proto::Secret(new_password.to_string()),
            policy,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminMgmtResponse>(&mut tls).await? {
        AdminMgmtResponse::Ok => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Admin → CA (pinned): replace role admin `target`'s policy.
pub async fn set_admin_policy(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    admin: &str,
    password: &str,
    target: &str,
    policy: crate::ca_policy::Policy,
) -> Result<()> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::SetAdminPolicy(SetAdminPolicyRequest {
            admin: admin.to_string(),
            password: conf_proto::Secret(password.to_string()),
            target: target.to_string(),
            policy,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminMgmtResponse>(&mut tls).await? {
        AdminMgmtResponse::Ok => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Admin → CA (pinned): remove role admin `target`.
pub async fn remove_admin(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    admin: &str,
    password: &str,
    target: &str,
) -> Result<()> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::RemoveAdmin(RemoveAdminRequest {
            admin: admin.to_string(),
            password: conf_proto::Secret(password.to_string()),
            target: target.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminMgmtResponse>(&mut tls).await? {
        AdminMgmtResponse::Ok => Ok(()),
        AdminMgmtResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Admin → CA (pinned): list the admin roster (tier + policy per admin).
pub async fn list_admins(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    admin: &str,
    password: &str,
) -> Result<Vec<crate::ca_policy::AdminInfo>> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ListAdmins(ListAdminsRequest {
            admin: admin.to_string(),
            password: conf_proto::Secret(password.to_string()),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, AdminListResponse>(&mut tls).await? {
        AdminListResponse::Ok { admins } => Ok(admins),
        AdminListResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Admin → CA (pinned): control services on the cluster serving
/// `target_path`. Returns one result per targeted cluster member so the
/// caller can surface a partial failure or per-host status.
#[allow(clippy::too_many_arguments)]
pub async fn control_service(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
    admin: &str,
    password: &str,
    target_path: &str,
    targets: Vec<conf_proto::UnitTarget>,
    op: netidx_activation::control::ControlOp,
) -> Result<Vec<conf_proto::ServiceControlResult>> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ControlService(ControlServiceRequest {
            admin: admin.to_string(),
            password: conf_proto::Secret(password.to_string()),
            target_path: target_path.to_string(),
            targets,
            op,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, ControlServiceResponse>(&mut tls).await? {
        ControlServiceResponse::Ok { results } => Ok(results),
        ControlServiceResponse::Err { reason } => bail!("the CA refused: {reason}"),
    }
}

/// Server → server: apply a service-control op to a peer conf server's local
/// activation supervisor (serving-cert authed). Mirrors [`push_perms_edit`].
pub async fn push_service_control(
    addr: SocketAddr,
    serving_cert_pem: &[u8],
    serving_key_pem: &[u8],
    roots: rustls::RootCertStore,
    units: Vec<String>,
    op: netidx_activation::control::ControlOp,
) -> Result<Vec<netidx_activation::control::UnitStatus>> {
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(serving_key_pem))
        .context("parsing serving key")?
        .ok_or_else(|| anyhow!("no private key found in serving key PEM"))?;
    let (mut tls, _hello) =
        connect_pki(addr, roots, Some((serving_cert_pem, key)), NodeKind::ConfServer)
            .await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ApplyServiceControl(ApplyServiceControlRequest { units, op }),
    )
    .await?;
    match conf_proto::read_msg::<_, ApplyServiceControlResponse>(&mut tls).await? {
        ApplyServiceControlResponse::Ok { units } => Ok(units),
        ApplyServiceControlResponse::Err { reason } => {
            bail!("peer refused service control: {reason}")
        }
    }
}

/// The aggregated picture of a network, built by walking conf servers'
/// `peers` from one or more seeds. Everything in it was served over
/// connections pinned to the operator-confirmed CA.
pub struct NetworkInfo {
    pub domain: String,
    /// Where Sign/Enroll requests go. The first CA location seen wins;
    /// a well-formed network only has one.
    pub ca_addr: Option<SocketAddr>,
    /// Every resolver any reached conf server reported, deduped by
    /// address.
    pub resolvers: Vec<ResolverAddr>,
    /// The conf servers actually reached.
    pub reached: Vec<SocketAddr>,
}

/// Upper bound on the peer walk — far above any plausible network, just
/// a runaway backstop.
const MAX_WALK: usize = 64;

/// Walk the network from `seeds`: [`get_info`] each conf server, follow
/// `peers` (deduped, cycle-safe), and merge the results. Unreachable or
/// mismatching (different-CA) servers are skipped with a logged warning
/// — one live seed is enough to map the network.
pub async fn aggregate(
    seeds: &[SocketAddr],
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<NetworkInfo> {
    let mut queue: Vec<SocketAddr> = seeds.to_vec();
    let mut visited: Vec<SocketAddr> = Vec::new();
    let mut info = NetworkInfo {
        domain: expected.domain.clone(),
        ca_addr: None,
        resolvers: Vec::new(),
        reached: Vec::new(),
    };
    while let Some(addr) = queue.pop() {
        if visited.contains(&addr) || visited.len() >= MAX_WALK {
            continue;
        }
        visited.push(addr);
        let resp = match get_info(addr, kind, expected).await {
            Ok(r) => r,
            Err(e) => {
                warn!("conf server {addr} could not be queried: {e:#}");
                continue;
            }
        };
        info.reached.push(addr);
        // A server bound to 0.0.0.0 reports itself with an unspecified
        // IP — substitute the address we actually reached it at.
        let fixup = |a: SocketAddr| {
            if a.ip().is_unspecified() { SocketAddr::new(addr.ip(), a.port()) } else { a }
        };
        if info.ca_addr.is_none() {
            info.ca_addr = resp.ca_addr.map(fixup);
        }
        if let Some(r) = resp.resolver
            && !info.resolvers.iter().any(|x| x.addr == r.addr)
        {
            info.resolvers.push(r);
        }
        queue.extend(resp.peers);
    }
    if info.reached.is_empty() {
        bail!("no conf server could be reached");
    }
    Ok(info)
}

/// Submit a CSR for `name` to the network's CA, authenticated as
/// `admin`/`password`, pinned to the identity the operator already
/// confirmed (`expected`, from [`fetch_identity`]). `id_map_groups`
/// (first = primary) registers the new identity on the network's
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
    // Local work first: our key + CSR.
    let kc = generate_key_and_csr(name)?;
    let req = Request::Sign(SignRequest {
        admin: admin.to_string(),
        password: Secret(password.as_str().to_string()),
        csr_pem: kc.csr_pem.clone(),
        requested_name: name.to_string(),
        requested_validity: validity,
        id_map_groups,
    });
    submit_csr(addr, kind, name, kc, req, expected).await
}

/// Enroll a new conf server: request the reserved [`SERVING_SAN`]
/// serving cert from the network's CA, authenticated as
/// `admin`/`password` (whose policy must grant `may_enroll_servers`),
/// pinned to the confirmed identity. `listen` is where the new daemon
/// will serve — the CA records it as a peer. The returned leaf + the
/// confirmed CA ([`CaIdentity::ca_pem`]) form the new daemon's serving
/// chain.
pub async fn enroll(
    addr: SocketAddr,
    admin: &str,
    password: Zeroizing<String>,
    listen: SocketAddr,
    expected: &CaIdentity,
) -> Result<Issued> {
    let kc = generate_key_and_csr(SERVING_SAN)?;
    let req = Request::Enroll(EnrollRequest {
        admin: admin.to_string(),
        password: Secret(password.as_str().to_string()),
        csr_pem: kc.csr_pem.clone(),
        listen,
    });
    submit_csr(addr, NodeKind::ConfServer, SERVING_SAN, kc, req, expected).await
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
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(&mut tls, &req).await?;
    match conf_proto::read_msg::<_, SignResponse>(&mut tls).await? {
        SignResponse::Ok { signed_cert_pem, trusted_pem, warnings } => {
            verify_issued(expected, name, &our_spki, &signed_cert_pem, &trusted_pem)?;
            Ok(Issued {
                cert_pem: signed_cert_pem,
                private_key_pem: kc.private_key_pem,
                trusted_pem,
                warnings,
            })
        }
        SignResponse::Err { reason } => bail!("conf server refused to sign: {reason}"),
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
        "the certificate returned by the conf server failed verification \
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
    enqueue_inner(addr, kind, name, validity, None, expected).await
}

/// Queue a **conf-server enrollment** for asynchronous admin approval:
/// the reserved [`SERVING_SAN`] serving cert, approvable only by an
/// admin whose policy grants `may_enroll_servers`. Same request-code
/// ceremony and [`poll`] loop as a queued sign; `listen` is where the
/// new conf server will serve (recorded as a peer at approval).
pub async fn enqueue_enroll(
    addr: SocketAddr,
    listen: SocketAddr,
    expected: &CaIdentity,
) -> Result<PendingEnrollment> {
    // The validity is decided server-side at approval (the standard
    // serving-cert validity); the value here is a well-formedness
    // placeholder.
    enqueue_inner(
        addr,
        NodeKind::ConfServer,
        SERVING_SAN,
        Duration::from_secs(1),
        Some(listen),
        expected,
    )
    .await
}

async fn enqueue_inner(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    validity: Duration,
    enroll_listen: Option<SocketAddr>,
    expected: &CaIdentity,
) -> Result<PendingEnrollment> {
    let kc = generate_key_and_csr(name)?;
    let our_spki = csr_spki(&kc.csr_pem)?;
    let fingerprint = Fingerprint::of_der(&our_spki);
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::Enqueue(EnqueueRequest {
            kind,
            csr_pem: kc.csr_pem.clone(),
            requested_name: name.to_string(),
            requested_validity: validity,
            enroll_listen,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, EnqueueResponse>(&mut tls).await? {
        EnqueueResponse::Ok { request_id } => Ok(PendingEnrollment {
            request_id,
            fingerprint,
            name: name.to_string(),
            our_spki,
            kc,
        }),
        EnqueueResponse::Err { reason } => {
            bail!("conf server refused to queue the request: {reason}")
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
    conf_proto::write_msg(
        &mut tls,
        &Request::Poll(PollRequest { request_id: pending.request_id.clone() }),
    )
    .await?;
    match conf_proto::read_msg::<_, PollResponse>(&mut tls).await? {
        PollResponse::Pending => Ok(PollOutcome::Pending),
        PollResponse::Denied { reason } => Ok(PollOutcome::Denied(reason)),
        PollResponse::Unknown => Ok(PollOutcome::Expired),
        PollResponse::Signed { signed_cert_pem, trusted_pem, warnings } => {
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
/// the admin's password only ever goes to the confirmed network).
pub async fn list_queue(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    expected: &CaIdentity,
) -> Result<Vec<QueueEntry>> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ListQueue(ListQueueRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, ListQueueResponse>(&mut tls).await? {
        ListQueueResponse::Ok { requests } => Ok(requests),
        ListQueueResponse::Err { reason } => bail!("conf server refused: {reason}"),
    }
}

/// Approve a queued request: the server signs its CSR (under the
/// approving admin's policy) and registers `id_map_groups` — chosen
/// here, by the admin. Returns the push warnings; the enrollee
/// receives the cert via its own [`poll`].
pub async fn approve(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    request_id: &str,
    id_map_groups: Vec<String>,
    expected: &CaIdentity,
) -> Result<Vec<String>> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::Approve(ApproveRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
            request_id: request_id.to_string(),
            id_map_groups,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, ApproveResponse>(&mut tls).await? {
        ApproveResponse::Ok { warnings } => Ok(warnings),
        ApproveResponse::Err { reason } => bail!("conf server refused: {reason}"),
    }
}

/// Fetch the network's current CRL (pinned). `None` — nothing has ever
/// been revoked.
pub async fn get_crl(
    addr: SocketAddr,
    kind: NodeKind,
    expected: &CaIdentity,
) -> Result<Option<String>> {
    let mut tls = connect_pinned(addr, kind, expected).await?;
    conf_proto::write_msg(&mut tls, &Request::GetCrl).await?;
    let resp: conf_proto::GetCrlResponse = conf_proto::read_msg(&mut tls).await?;
    Ok(resp.crl_pem)
}

/// Deny a queued request with a reason shown to the waiting enrollee.
pub async fn deny(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    request_id: &str,
    reason: &str,
    expected: &CaIdentity,
) -> Result<()> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::Deny(DenyRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
            request_id: request_id.to_string(),
            reason: reason.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, DenyResponse>(&mut tls).await? {
        DenyResponse::Ok => Ok(()),
        DenyResponse::Err { reason } => bail!("conf server refused: {reason}"),
    }
}

// -- resolver hierarchy delegation -------------------------------------------

/// The request code for a delegation: a fingerprint over exactly
/// `(proposed_path, child)` — the value the child admin shows and the
/// parent admin recomputes from the queued request, matched out of band.
/// Child addresses are sorted so the code is order-independent (a cluster
/// child may list its members in any order).
pub fn delegation_code(proposed_path: &str, child: &[ResolverAddr]) -> Fingerprint {
    let mut sorted = child.to_vec();
    sorted.sort_by_key(|r| r.addr);
    // Canonical, byte-identical on both sides: a tuple's field order and a
    // struct's field order are fixed, and we sorted the vec.
    let canonical = serde_json::to_vec(&(proposed_path, &sorted)).unwrap_or_default();
    Fingerprint::of_der(&canonical)
}

/// Queue a delegation request with the parent's conf server (no
/// credentials — the parent admin authorizes by matching the code).
/// Returns the request id to [`poll_delegation`] with.
pub async fn request_delegation(
    addr: SocketAddr,
    proposed_path: &str,
    child: Vec<ResolverAddr>,
    expected: &CaIdentity,
) -> Result<String> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::RequestDelegation(DelegationRequest {
            proposed_path: proposed_path.to_string(),
            child,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, DelegationResponse>(&mut tls).await? {
        DelegationResponse::Ok { request_id } => Ok(request_id),
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
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::PollDelegation(PollRequest { request_id: request_id.to_string() }),
    )
    .await?;
    conf_proto::read_msg(&mut tls).await
}

/// List the pending delegation queue, authenticated as `admin` (pinned).
pub async fn list_delegations(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    expected: &CaIdentity,
) -> Result<Vec<DelegationEntry>> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ListDelegations(ListDelegationsRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, ListDelegationsResponse>(&mut tls).await? {
        ListDelegationsResponse::Ok { requests } => Ok(requests),
        ListDelegationsResponse::Err { reason } => bail!("conf server refused: {reason}"),
    }
}

/// Approve a queued delegation. Returns the per-peer cluster-push results
/// (a non-empty `error` means that peer is out of sync — surface it).
pub async fn approve_delegation(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    request_id: &str,
    expected: &CaIdentity,
) -> Result<Vec<PeerResult>> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ApproveDelegation(ApproveDelegationRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
            request_id: request_id.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, ApproveDelegationResponse>(&mut tls).await? {
        ApproveDelegationResponse::Ok { peers } => Ok(peers),
        ApproveDelegationResponse::Err { reason } => {
            bail!("conf server refused: {reason}")
        }
    }
}

/// Deny a queued delegation with a reason shown to the waiting child.
pub async fn deny_delegation(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    request_id: &str,
    reason: &str,
    expected: &CaIdentity,
) -> Result<()> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::DenyDelegation(DenyDelegationRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
            request_id: request_id.to_string(),
            reason: reason.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, DenyDelegationResponse>(&mut tls).await? {
        DenyDelegationResponse::Ok => Ok(()),
        DenyDelegationResponse::Err { reason } => bail!("conf server refused: {reason}"),
    }
}

/// Server-to-server: push a referral edit to a peer conf server's local
/// resolver config (the cluster-wide propagation push), authenticated by
/// our reserved-SAN serving cert. Mirrors [`push_identity`].
pub async fn push_referral_edit(
    addr: SocketAddr,
    client_cert_pem: &[u8],
    client_key_pem: &[u8],
    roots: rustls::RootCertStore,
    edit: &ReferralEdit,
) -> Result<()> {
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(client_key_pem))
        .context("parsing client key")?
        .ok_or_else(|| anyhow!("no private key found in client key PEM"))?;
    let (mut tls, _hello) =
        connect_pki(addr, roots, Some((client_cert_pem, key)), NodeKind::ConfServer)
            .await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ApplyReferralEdit(ApplyReferralEditRequest { edit: edit.clone() }),
    )
    .await?;
    match conf_proto::read_msg::<_, ApplyReferralEditResponse>(&mut tls).await? {
        ApplyReferralEditResponse::Ok => Ok(()),
        ApplyReferralEditResponse::Err { reason } => {
            bail!("peer refused the referral edit: {reason}")
        }
    }
}

/// List every issued certificate the daemon holds (admin-authenticated) —
/// live and revoked, with the metadata the revoke UI needs. The daemon
/// owns the index; this is the only way to read it.
pub async fn list_issued(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    expected: &CaIdentity,
) -> Result<Vec<IssuedEntry>> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::ListIssued(ListIssuedRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, ListIssuedResponse>(&mut tls).await? {
        ListIssuedResponse::Ok { entries } => Ok(entries),
        ListIssuedResponse::Err { reason } => bail!("conf server refused: {reason}"),
    }
}

/// Revoke certificates by serial (admin-authenticated). The daemon
/// rewrites their records and re-signs the CRL; returns any non-fatal
/// follow-up warnings.
pub async fn revoke(
    addr: SocketAddr,
    admin: &str,
    password: &str,
    serials: Vec<u64>,
    reason: &str,
    expected: &CaIdentity,
) -> Result<Vec<String>> {
    let mut tls = connect_pinned(addr, NodeKind::Client, expected).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::Revoke(RevokeRequest {
            admin: admin.to_string(),
            password: Secret(password.to_string()),
            serials,
            reason: reason.to_string(),
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, RevokeResponse>(&mut tls).await? {
        RevokeResponse::Ok { warnings } => Ok(warnings),
        RevokeResponse::Err { reason } => bail!("conf server refused: {reason}"),
    }
}

/// Push an identity registration to a peer conf server, authenticating
/// with *our* serving cert (server-to-server; the receiver requires the
/// reserved SAN). Unlike the operator-facing calls this does real PKI —
/// the caller is a conf server that has the CA bundle installed — so
/// there is no TOFU and no pinning: `roots` is the trust anchor.
///
/// Returns `Ok(None)` when the peer's hello shows it has no id-map role
/// — not an error; the pusher fans out to every known peer and skips
/// the ones that can't register identities.
pub async fn push_identity(
    addr: SocketAddr,
    client_cert_pem: &[u8],
    client_key_pem: &[u8],
    roots: rustls::RootCertStore,
    req: &AddIdentityRequest,
) -> Result<Option<u32>> {
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(client_key_pem))
        .context("parsing client key")?
        .ok_or_else(|| anyhow!("no private key found in client key PEM"))?;
    let (mut tls, hello) =
        connect_pki(addr, roots, Some((client_cert_pem, key)), NodeKind::ConfServer)
            .await?;
    if !hello.roles.contains(&Role::IdMap) {
        return Ok(None);
    }
    conf_proto::write_msg(&mut tls, &Request::AddIdentity(req.clone())).await?;
    match conf_proto::read_msg::<_, AddIdentityResponse>(&mut tls).await? {
        AddIdentityResponse::Ok { uid } => Ok(Some(uid)),
        AddIdentityResponse::Err { reason } => {
            bail!("conf server refused the identity: {reason}")
        }
    }
}

/// Server→CA: register/update this conf server's facts in the CA's network
/// map, authenticated with the serving cert (peer-cert-gated). Returns the
/// CA's new map version.
pub async fn register(
    addr: SocketAddr,
    serving_cert_pem: &[u8],
    serving_key_pem: &[u8],
    roots: rustls::RootCertStore,
    req: &RegisterRequest,
) -> Result<u64> {
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(serving_key_pem))
        .context("parsing serving key")?
        .ok_or_else(|| anyhow!("no private key found in serving key PEM"))?;
    let (mut tls, _hello) =
        connect_pki(addr, roots, Some((serving_cert_pem, key)), NodeKind::ConfServer)
            .await?;
    conf_proto::write_msg(&mut tls, &Request::Register(req.clone())).await?;
    match conf_proto::read_msg::<_, RegisterResponse>(&mut tls).await? {
        RegisterResponse::Ok { version } => Ok(version),
        RegisterResponse::Err { reason } => {
            bail!("the CA refused the registration: {reason}")
        }
    }
}

/// Server→CA: drop this conf server (`own_addr`) from the map on uninstall.
pub async fn deregister(
    addr: SocketAddr,
    serving_cert_pem: &[u8],
    serving_key_pem: &[u8],
    roots: rustls::RootCertStore,
    own_addr: SocketAddr,
) -> Result<u64> {
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(serving_key_pem))
        .context("parsing serving key")?
        .ok_or_else(|| anyhow!("no private key found in serving key PEM"))?;
    let (mut tls, _hello) =
        connect_pki(addr, roots, Some((serving_cert_pem, key)), NodeKind::ConfServer)
            .await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::Deregister(DeregisterRequest { addr: own_addr }),
    )
    .await?;
    match conf_proto::read_msg::<_, RegisterResponse>(&mut tls).await? {
        RegisterResponse::Ok { version } => Ok(version),
        RegisterResponse::Err { reason } => {
            bail!("the CA refused the deregistration: {reason}")
        }
    }
}

/// Cheap probe: the served map's current version. No client cert needed —
/// the map is public within the trust domain (reads are pinned to the CA).
pub async fn get_map_version(
    addr: SocketAddr,
    roots: rustls::RootCertStore,
    kind: NodeKind,
) -> Result<u64> {
    let (mut tls, _hello) = connect_pki(addr, roots, None, kind).await?;
    conf_proto::write_msg(&mut tls, &Request::GetMapVersion).await?;
    match conf_proto::read_msg::<_, GetMapVersionResponse>(&mut tls).await? {
        GetMapVersionResponse::Ok { version } => Ok(version),
        GetMapVersionResponse::Err { reason } => {
            bail!("map version query refused: {reason}")
        }
    }
}

/// Fetch the whole network map in one round trip — every cluster, every
/// conf server's role, the CA location.
pub async fn get_map(
    addr: SocketAddr,
    roots: rustls::RootCertStore,
    kind: NodeKind,
) -> Result<NetworkMap> {
    let (mut tls, _hello) = connect_pki(addr, roots, None, kind).await?;
    conf_proto::write_msg(&mut tls, &Request::GetMap).await?;
    match conf_proto::read_msg::<_, GetMapResponse>(&mut tls).await? {
        GetMapResponse::Ok { map } => Ok(map),
        GetMapResponse::Err { reason } => bail!("map query refused: {reason}"),
    }
}

/// Connect to a conf server with **real PKI** (webpki against `roots`,
/// `ServerName = SERVING_SAN`) — for callers that already hold the
/// trust bundle (peer conf servers, the renewal daemon). No TOFU, no
/// pinning, no human. Presents `client_identity` (cert chain + key
/// PEM) when given — that's what authenticates a verified renewal.
async fn connect_pki(
    addr: SocketAddr,
    roots: rustls::RootCertStore,
    client_identity: Option<(&[u8], PrivateKeyDer<'static>)>,
    kind: NodeKind,
) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, ServerHello)> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .with_root_certificates(roots);
    let config = match client_identity {
        Some((cert_pem, key)) => {
            let certs: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
                    .collect::<std::result::Result<_, _>>()
                    .context("parsing client certificate chain")?;
            builder
                .with_client_auth_cert(certs, key)
                .context("building client TLS config")?
        }
        None => builder.with_no_client_auth(),
    };
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to conf server {addr}"))?;
    let server_name = ServerName::try_from(SERVING_SAN).context("server name")?;
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake with conf server")?;
    let hello = exchange_hello(&mut tls, kind).await?;
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
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    validity: Duration,
    current_cert_pem: &[u8],
    current_key: PrivateKeyDer<'static>,
    roots: rustls::RootCertStore,
) -> Result<PendingRenewal> {
    let kc = generate_key_and_csr(name)?;
    let our_spki = csr_spki(&kc.csr_pem)?;
    let (mut tls, _hello) =
        connect_pki(addr, roots, Some((current_cert_pem, current_key)), kind).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::Enqueue(EnqueueRequest {
            kind,
            csr_pem: kc.csr_pem.clone(),
            requested_name: name.to_string(),
            requested_validity: validity,
            enroll_listen: None,
        }),
    )
    .await?;
    match conf_proto::read_msg::<_, EnqueueResponse>(&mut tls).await? {
        EnqueueResponse::Ok { request_id } => {
            Ok(PendingRenewal { request_id, name: name.to_string(), our_spki, kc })
        }
        EnqueueResponse::Err { reason } => {
            bail!("conf server refused to queue the renewal: {reason}")
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
    addr: SocketAddr,
    kind: NodeKind,
    pending: &PendingRenewal,
    installed_pem: &str,
    roots: rustls::RootCertStore,
) -> Result<PollOutcome> {
    let (mut tls, _hello) = connect_pki(addr, roots, None, kind).await?;
    conf_proto::write_msg(
        &mut tls,
        &Request::Poll(PollRequest { request_id: pending.request_id.clone() }),
    )
    .await?;
    match conf_proto::read_msg::<_, PollResponse>(&mut tls).await? {
        PollResponse::Pending => Ok(PollOutcome::Pending),
        PollResponse::Denied { reason } => Ok(PollOutcome::Denied(reason)),
        PollResponse::Unknown => Ok(PollOutcome::Expired),
        PollResponse::Signed { signed_cert_pem, trusted_pem, warnings } => {
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
/// the network to find the CA. Only genuine members of the network can
/// answer; no human confirmation involved.
pub async fn get_info_pki(
    addr: SocketAddr,
    kind: NodeKind,
    roots: rustls::RootCertStore,
) -> Result<GetInfoResponse> {
    let (mut tls, _hello) = connect_pki(addr, roots, None, kind).await?;
    conf_proto::write_msg(&mut tls, &Request::GetInfo).await?;
    conf_proto::read_msg(&mut tls).await
}

/// [`get_crl`] over real PKI — the renewal daemon's CRL pull.
pub async fn get_crl_pki(
    addr: SocketAddr,
    kind: NodeKind,
    roots: rustls::RootCertStore,
) -> Result<Option<String>> {
    let (mut tls, _hello) = connect_pki(addr, roots, None, kind).await?;
    conf_proto::write_msg(&mut tls, &Request::GetCrl).await?;
    let resp: conf_proto::GetCrlResponse = conf_proto::read_msg(&mut tls).await?;
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
        match roots.iter_mut().find(|(known, _)| *known == fp) {
            None => warn!(
                "ignoring CA {} offered by the renewal response: it is not a \
                 trust anchor we already hold (distribute trust changes out of band)",
                fp.short()
            ),
            Some((_, slot)) => {
                // Same key — accept the refreshed cert only if it is
                // validly self-signed under that key. A new validity
                // window is the point; tampered constraints over a
                // forged self-signature are not (the attacker lacks the
                // CA private key, so a bad self-signature can't pass).
                let (_, cert) = X509Certificate::from_der(der.as_ref())
                    .map_err(|e| anyhow!("parsing a refreshed CA cert: {e}"))?;
                match cert.verify_signature(Some(cert.public_key())) {
                    Ok(()) => *slot = der.as_ref().to_vec(),
                    Err(e) => warn!(
                        "ignoring same-key CA refresh for {}: not validly \
                         self-signed: {e}",
                        fp.short()
                    ),
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

/// Verify the conf server's issued leaf binds to what the operator
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
) -> Result<()> {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_a_parseable_ecdsa_csr() {
        let kc = generate_key_and_csr("resolver.example.com").unwrap();
        assert!(kc.csr_pem.contains("BEGIN CERTIFICATE REQUEST"));
        assert!(kc.private_key_pem.contains("BEGIN PRIVATE KEY"));
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
}
