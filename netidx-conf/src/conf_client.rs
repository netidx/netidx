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
        self, AddIdentityRequest, AddIdentityResponse, ApproveRequest, ApproveResponse,
        ClientHello, DenyRequest, DenyResponse, EnqueueRequest, EnqueueResponse,
        EnrollRequest, GetInfoResponse, ListQueueRequest, ListQueueResponse, NodeKind,
        PollRequest, PollResponse, QueueEntry, Request, ResolverAddr, Role, Secret,
        ServerHello, SignRequest, SignResponse, PROTOCOL_VERSION, SERVING_SAN,
    },
    fingerprint::Fingerprint,
    tls_tofu::TofuVerifier,
};
use anyhow::{anyhow, bail, Context, Result};
use log::warn;
use rustls::ClientConfig;
use rustls_pki_types::{CertificateDer, ServerName};
use std::{net::SocketAddr, sync::Arc};
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
        use base64::Engine;
        let b64 =
            base64::engine::general_purpose::STANDARD.encode(self.ca_der.as_ref());
        let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
        for chunk in b64.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(chunk).expect("base64 is ascii"));
            out.push('\n');
        }
        out.push_str("-----END CERTIFICATE-----\n");
        out
    }
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
            if a.ip().is_unspecified() {
                SocketAddr::new(addr.ip(), a.port())
            } else {
                a
            }
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
    validity_days: u32,
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
        requested_validity_days: validity_days,
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
    validity_days: u32,
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
            requested_validity_days: validity_days,
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
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(client_cert_pem))
            .collect::<std::result::Result<_, _>>()
            .context("parsing client certificate chain")?;
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(client_key_pem))
        .context("parsing client key")?
        .ok_or_else(|| anyhow!("no private key found in client key PEM"))?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .context("building client TLS config")?;
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to conf server {addr}"))?;
    let server_name = ServerName::try_from(SERVING_SAN).context("server name")?;
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake with conf server")?;
    let hello = exchange_hello(&mut tls, NodeKind::ConfServer).await?;
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
    let (_, leaf) =
        X509Certificate::from_der(&leaf_der).map_err(|e| anyhow!("parsing issued cert: {e}"))?;
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
fn csr_spki(csr_pem: &str) -> Result<Vec<u8>> {
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
    let (_, ca) = X509Certificate::from_der(ca_der.as_ref()).context("parsing CA cert")?;
    let (_, leaf) =
        X509Certificate::from_der(serving_der.as_ref()).context("parsing serving cert")?;
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
}
