//! CA server: turns a join client's `SignRequest` into a signed cert.
//!
//! The request-handling core ([`handle_sign_request`]) is pure of TLS
//! and directly testable: it unlocks the vault with the admin password,
//! enforces that admin's per-slot issuance [`Policy`], signs via the
//! existing [`Ca::sign_request`], and appends an audit line. The TLS
//! accept loop is a thin shell over it (added with the daemon CLI).
//!
//! Unix-only — the signer is openssl-backed.

use crate::{
    ca::{Ca, SanEntry},
    ca_proto::{
        self, ClientHello, ServerHello, SignRequest, SignResponse, PROTOCOL_VERSION,
    },
    ca_vault,
};
use anyhow::{anyhow, bail, Context, Result};
use globset::Glob;
use log::{debug, info, warn};
use rustls::ServerConfig as RustlsServerConfig;
use rustls_pki_types::CertificateDer;
use std::{
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::TlsAcceptor;

/// Max simultaneous connections. These are cheap (a TLS handshake and a
/// few small messages), so this can be generous — it just bounds socket
/// / task fan-out.
const MAX_CONNECTIONS: usize = 768;

/// Max simultaneous *signs*. Each sign runs an Argon2id derivation
/// (~64 MiB) to unlock the vault, so this — not `MAX_CONNECTIONS` — is
/// what bounds the memory a flood of (even wrong-password) requests can
/// pin: roughly `MAX_CONCURRENT_SIGNS × 64 MiB`. Tune for the CA box's
/// RAM (e.g. 64 ≈ 4 GiB). Signing runs on `spawn_blocking`, so this also
/// keeps Argon2/openssl off the async worker threads.
const MAX_CONCURRENT_SIGNS: usize = 64;

/// Upper bound on a single connection's whole lifetime (handshake +
/// request + sign + response). Without it a client that connects and
/// stalls holds a connection slot indefinitely.
const CONN_TIMEOUT: Duration = Duration::from_secs(30);

/// Inputs to run the CA server daemon.
pub struct ServeParams {
    pub ca_dir: PathBuf,
    pub listen: SocketAddr,
    /// The serving certificate **chain** in PEM, `[leaf, ca]` — the
    /// client reads the CA cert from the end of this chain.
    pub serving_cert_pem: Vec<u8>,
    /// The serving leaf's private key (PKCS#8 PEM).
    pub serving_key_pem: Vec<u8>,
}

/// Bind and run the CA server until the process is killed.
pub async fn serve(params: ServeParams) -> Result<()> {
    let acceptor = TlsAcceptor::from(Arc::new(build_server_config(
        &params.serving_cert_pem,
        &params.serving_key_pem,
    )?));
    let listener = TcpListener::bind(params.listen)
        .await
        .with_context(|| format!("binding CA server to {}", params.listen))?;
    info!("ca-server: listening on {}", params.listen);
    serve_on(listener, acceptor, Arc::new(params.ca_dir)).await
}

/// The accept loop, split out so tests can drive it on an ephemeral
/// port they bound themselves.
async fn serve_on(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    ca_dir: Arc<PathBuf>,
) -> Result<()> {
    let conns = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let signs = Arc::new(Semaphore::new(MAX_CONCURRENT_SIGNS));
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("ca-server: accept failed: {e:#}");
                continue;
            }
        };
        // Gate the connection count at the door, so we don't even spawn
        // a task for one we'd immediately have to drop.
        let conn_permit = match conns.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn!("ca-server: at connection limit, dropping {peer}");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let ca_dir = ca_dir.clone();
        let signs = signs.clone();
        tokio::spawn(async move {
            let _conn_permit = conn_permit; // released when the task ends
            // Bound the whole connection so a stalled peer can't hold a
            // connection slot (the handshake and every read are
            // otherwise deadline-less). The sign permit is *not* tied to
            // this cancellable future — see `handle_conn`.
            match tokio::time::timeout(
                CONN_TIMEOUT,
                handle_conn(&acceptor, tcp, &ca_dir, signs),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => debug!("ca-server: connection from {peer} ended: {e:#}"),
                Err(_) => debug!("ca-server: connection from {peer} timed out"),
            }
        });
    }
}

async fn handle_conn(
    acceptor: &TlsAcceptor,
    tcp: TcpStream,
    ca_dir: &Path,
    signs: Arc<Semaphore>,
) -> Result<()> {
    let mut tls = acceptor.accept(tcp).await.context("TLS handshake")?;
    let _hello: ClientHello = ca_proto::read_msg(&mut tls).await.context("reading ClientHello")?;
    ca_proto::write_msg(&mut tls, &ServerHello { protocol_version: PROTOCOL_VERSION })
        .await
        .context("writing ServerHello")?;
    let req: SignRequest = ca_proto::read_msg(&mut tls).await.context("reading SignRequest")?;
    // The sign is the expensive, blocking part (Argon2 vault unlock +
    // openssl). Run it on `spawn_blocking`, off the async worker
    // threads, bounded by the sign semaphore.
    //
    // The permit is moved *into* the blocking task and held for its
    // whole duration. A `spawn_blocking` task can't be cancelled, so if
    // this connection times out, the `JoinHandle` await below is dropped
    // while the task keeps running — releasing the permit there (not in
    // this cancellable future) is what keeps `MAX_CONCURRENT_SIGNS`
    // honest: otherwise a timeout would free the permit while the 64 MiB
    // Argon2 is still live, and repeated timeouts would exceed the bound.
    let permit =
        signs.acquire_owned().await.expect("sign semaphore is never closed");
    let ca_dir = ca_dir.to_path_buf();
    let resp = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handle_sign_request(&ca_dir, &req)
    })
    .await
    .context("CA signing task panicked")?;
    ca_proto::write_msg(&mut tls, &resp).await.context("writing SignResponse")?;
    Ok(())
}

fn build_server_config(cert_pem: &[u8], key_pem: &[u8]) -> Result<RustlsServerConfig> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
            .collect::<std::result::Result<_, _>>()
            .context("parsing serving certificate chain")?;
    if certs.is_empty() {
        bail!("serving certificate PEM contained no certificates");
    }
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key_pem))
        .context("parsing serving private key")?
        .ok_or_else(|| anyhow!("no private key found in serving key PEM"))?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    RustlsServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building TLS server config")
}

/// Handle a sign request against the CA rooted at `ca_dir`. Auth and
/// policy failures become a `SignResponse::Err` carrying a safe reason
/// for the client; only an internal fault (e.g. the CA cert can't be
/// read) maps to a generic error response — never a panic.
pub fn handle_sign_request(ca_dir: &Path, req: &SignRequest) -> SignResponse {
    match try_handle(ca_dir, req) {
        Ok(resp) => resp,
        Err(e) => SignResponse::Err { reason: format!("internal error: {e:#}") },
    }
}

fn try_handle(ca_dir: &Path, req: &SignRequest) -> Result<SignResponse> {
    // 1. Authenticate: the password must unlock a slot, and the named
    //    admin must be the one it unlocks.
    let unlocked = match ca_vault::unlock(ca_dir, &req.password.0) {
        Ok(u) => u,
        Err(_) => return Ok(reject("authentication failed")),
    };
    if req.admin != unlocked.admin {
        return Ok(reject("admin name does not match the password"));
    }

    // 2. Authorize: the requested name must match the admin's policy.
    let name = req.requested_name.trim();
    if name.is_empty() {
        return Ok(reject("requested name is empty"));
    }
    // The CA server's own serving name is reserved: the join trust model
    // hinges on *only* the genuine daemon holding a CA-signed cert with
    // it. Issuing it over the wire — even to an admin whose policy glob
    // (e.g. "*") happens to match — would let that admin stand up an
    // impostor daemon that passes the client's serving-cert check,
    // present the genuine CA fingerprint, and harvest other admins'
    // passwords. Refuse it unconditionally; the local setup path issues
    // the serving cert directly (via `Ca::sign_request`), bypassing this
    // handler.
    if name.eq_ignore_ascii_case(ca_proto::SERVING_SAN) {
        return Ok(reject("that name is reserved for the CA server and cannot be issued"));
    }
    if !name_permitted(name, &unlocked.policy.allowed_san)? {
        return Ok(reject(&format!(
            "name {name:?} is not permitted for admin {}",
            unlocked.admin
        )));
    }
    let validity = req.requested_validity_days.min(unlocked.policy.max_validity_days);
    if validity == 0 {
        return Ok(reject("validity_days must be > 0 and within policy"));
    }

    // 3. Sign. The CA is built transiently from the decrypted key; it
    //    is dropped (and the key zeroized) as soon as this returns.
    let cert_pem =
        std::fs::read(ca_dir.join("certificate.pem")).context("reading CA certificate")?;
    // Each request builds a transient `Ca`, so its per-instance
    // `serial_lock` doesn't serialize across handlers — concurrent signs
    // are kept from minting duplicate serials by the OS file lock inside
    // `next_serial` (see `ca::next_serial`).
    let ca = Ca::from_pem(ca_dir.to_path_buf(), &unlocked.ca_key_pem, &cert_pem)
        .context("loading CA from vault")?;
    let san = [SanEntry::Dns(name.to_string())];
    let signed = ca
        .sign_request(req.csr_pem.as_bytes(), &san, validity)
        .context("signing CSR")?;

    // 4. Audit (best-effort).
    audit(ca_dir, &unlocked.admin, name, validity);

    let trusted_pem = read_trusted_bundle(ca_dir, &cert_pem)?;
    Ok(SignResponse::Ok {
        signed_cert_pem: String::from_utf8(signed).context("signed cert not utf8")?,
        trusted_pem,
    })
}

/// The trust bundle handed to joining nodes. Defaults to the CA's own
/// cert; if the admin maintains a `trusted.pem` bundle in the CA dir
/// (e.g. for a federation of CAs), that is served instead. Either way
/// it must contain the issuing CA — the client checks the confirmed CA
/// fingerprint is present before installing it.
fn read_trusted_bundle(ca_dir: &Path, ca_cert_pem: &[u8]) -> Result<String> {
    let bundle = ca_dir.join("trusted.pem");
    let bytes = if bundle.exists() {
        std::fs::read(&bundle).with_context(|| format!("reading {}", bundle.display()))?
    } else {
        ca_cert_pem.to_vec()
    };
    String::from_utf8(bytes).context("trust bundle is not utf8")
}

/// True if `name` matches any of the admin's `allowed` glob patterns.
/// An empty pattern list denies everything (issuance scope is granted
/// explicitly).
fn name_permitted(name: &str, allowed: &[String]) -> Result<bool> {
    for pat in allowed {
        let glob = Glob::new(pat).with_context(|| format!("bad policy glob {pat:?}"))?;
        if glob.compile_matcher().is_match(name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reject(reason: &str) -> SignResponse {
    SignResponse::Err { reason: reason.to_string() }
}

fn audit(ca_dir: &Path, admin: &str, name: &str, validity: u32) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let line = format!("ts={ts} admin={admin} name={name} validity_days={validity}\n");
    let r = OpenOptions::new()
        .create(true)
        .append(true)
        .open(ca_dir.join("audit.log"))
        .and_then(|mut f| f.write_all(line.as_bytes()));
    if let Err(e) = r {
        // Audit is best-effort; a failed write must not fail issuance.
        eprintln!("ca-server: WARNING failed to append audit log: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ca::{Ca, CaParams, Subject, MIN_KEY_BITS},
        ca_join, ca_proto::{Secret, NodeKind, SERVING_SAN}, ca_vault, ca_vault::Policy,
        fingerprint::Fingerprint,
    };
    use zeroize::Zeroizing;

    fn policy() -> Policy {
        Policy { allowed_san: vec!["*.ryu-oh.org".to_string()], max_validity_days: 30 }
    }

    /// Issue the daemon's TLS serving cert from the (vault-protected)
    /// CA. Returns `([leaf_pem ++ ca_pem], serving_key_pem)`.
    fn issue_serving_cert(dir: &Path) -> (Vec<u8>, Vec<u8>) {
        let unlocked = ca_vault::unlock(dir, "apw").unwrap();
        let ca_cert = std::fs::read(dir.join("certificate.pem")).unwrap();
        let ca = Ca::from_pem(dir.to_path_buf(), &unlocked.ca_key_pem, &ca_cert).unwrap();
        let kc = ca_join::generate_key_and_csr(SERVING_SAN).unwrap();
        let leaf =
            ca.sign_request(kc.csr_pem.as_bytes(), &[SanEntry::Dns(SERVING_SAN.into())], 365)
                .unwrap();
        let mut chain = leaf;
        chain.extend_from_slice(&ca_cert);
        (chain, kc.private_key_pem.as_bytes().to_vec())
    }

    async fn spawn_server(dir: &Path) -> SocketAddr {
        let (cert, key) = issue_serving_cert(dir);
        let acceptor =
            TlsAcceptor::from(Arc::new(build_server_config(&cert, &key).unwrap()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ca_dir = Arc::new(dir.to_path_buf());
        tokio::spawn(serve_on(listener, acceptor, ca_dir));
        addr
    }

    /// Build a vault-protected CA in `dir`: a real (RSA) CA whose key is
    /// moved into a 1-admin vault, no `private.key` left behind.
    fn setup_ca(dir: &Path) {
        setup_ca_with_policy(dir, policy());
    }

    fn setup_ca_with_policy(dir: &Path, policy: Policy) {
        let params = CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn("Test CA".to_string()),
            san: vec![],
            key_bits: MIN_KEY_BITS,
            validity_days: 30,
        };
        Ca::init(&params, None).unwrap();
        let key = std::fs::read(dir.join("private.key")).unwrap();
        ca_vault::create(dir, &key, "alice", "apw", policy).unwrap();
        std::fs::remove_file(dir.join("private.key")).unwrap();
    }

    fn request(name: &str, admin: &str, pw: &str, days: u32) -> SignRequest {
        let kc = ca_join::generate_key_and_csr(name).unwrap();
        SignRequest {
            admin: admin.to_string(),
            password: Secret(pw.to_string()),
            csr_pem: kc.csr_pem,
            requested_name: name.to_string(),
            requested_validity_days: days,
        }
    }

    #[test]
    fn signs_a_permitted_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("resolver.ryu-oh.org", "alice", "apw", 30);
        match handle_sign_request(dir.path(), &req) {
            SignResponse::Ok { signed_cert_pem, trusted_pem } => {
                assert!(signed_cert_pem.contains("BEGIN CERTIFICATE"));
                assert!(trusted_pem.contains("BEGIN CERTIFICATE"));
                // The signed leaf parses as a real X.509 cert.
                openssl::x509::X509::from_pem(signed_cert_pem.as_bytes()).unwrap();
                // An audit line was written.
                let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
                assert!(log.contains("admin=alice"));
                assert!(log.contains("name=resolver.ryu-oh.org"));
            }
            SignResponse::Err { reason } => panic!("expected Ok, got: {reason}"),
        }
    }

    #[test]
    fn rejects_wrong_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("resolver.ryu-oh.org", "alice", "WRONG", 30);
        match handle_sign_request(dir.path(), &req) {
            SignResponse::Err { reason } => assert!(reason.contains("authentication")),
            SignResponse::Ok { .. } => panic!("wrong password was accepted"),
        }
    }

    #[test]
    fn rejects_name_outside_policy() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("evil.example.com", "alice", "apw", 30);
        match handle_sign_request(dir.path(), &req) {
            SignResponse::Err { reason } => assert!(reason.contains("not permitted")),
            SignResponse::Ok { .. } => panic!("out-of-policy name was signed"),
        }
    }

    #[test]
    fn caps_validity_to_policy() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // Ask for 9999 days; policy caps at 30. Should still succeed.
        let req = request("a.ryu-oh.org", "alice", "apw", 9999);
        let SignResponse::Ok { signed_cert_pem, .. } = handle_sign_request(dir.path(), &req)
        else {
            panic!("expected Ok");
        };
        let cert = openssl::x509::X509::from_pem(signed_cert_pem.as_bytes()).unwrap();
        // notAfter should be ~30 days out, well under the 9999 requested.
        let in_60_days = openssl::asn1::Asn1Time::days_from_now(60).unwrap();
        assert!(cert.not_after() < in_60_days);
    }

    #[test]
    fn refuses_to_issue_the_reserved_serving_name() {
        let dir = tempfile::tempdir().unwrap();
        // A wide-open "*" policy — which WOULD match the reserved name —
        // must still not be able to mint the CA server's own serving
        // cert (that would enable impersonating the daemon).
        setup_ca_with_policy(
            dir.path(),
            Policy { allowed_san: vec!["*".to_string()], max_validity_days: 30 },
        );
        let req = request(SERVING_SAN, "alice", "apw", 30);
        match handle_sign_request(dir.path(), &req) {
            SignResponse::Err { reason } => assert!(reason.contains("reserved")),
            SignResponse::Ok { .. } => panic!("issued the reserved serving name"),
        }
    }

    #[test]
    fn admin_name_must_match_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // Right password, wrong admin name.
        let req = request("a.ryu-oh.org", "bob", "apw", 30);
        match handle_sign_request(dir.path(), &req) {
            SignResponse::Err { reason } => assert!(reason.contains("does not match")),
            SignResponse::Ok { .. } => panic!("admin/password mismatch accepted"),
        }
    }

    #[tokio::test]
    async fn end_to_end_join_over_tls() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let addr = spawn_server(dir.path()).await;

        // Inspect first — the operator is shown the CA fingerprint.
        let identity = ca_join::fetch_ca_identity(addr).await.unwrap();
        assert!(!identity.fingerprint.text().is_empty());
        // Then sign, pinned to the confirmed identity.
        let issued = ca_join::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            &identity,
        )
        .await
        .unwrap();

        // The signed leaf parses and is for our name.
        let cert = openssl::x509::X509::from_pem(issued.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some("resolver.ryu-oh.org")));
        // The returned trust bundle is the CA cert (the default bundle).
        let ca_disk = std::fs::read(dir.path().join("certificate.pem")).unwrap();
        assert_eq!(issued.trusted_pem.as_bytes(), ca_disk.as_slice());
    }

    #[tokio::test]
    async fn mismatched_ca_identity_aborts_before_the_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let addr = spawn_server(dir.path()).await;
        // Inspect, then tamper the confirmed fingerprint to simulate a CA
        // that swapped its cert between inspection and signing. The pin in
        // `request_cert` must reject it before the password is sent.
        let mut identity = ca_join::fetch_ca_identity(addr).await.unwrap();
        identity.fingerprint = Fingerprint::of_der(b"not the real CA cert");
        let err = ca_join::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("fingerprint mismatch"));
    }

    #[tokio::test]
    async fn wrong_password_over_the_wire_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let addr = spawn_server(dir.path()).await;
        let identity = ca_join::fetch_ca_identity(addr).await.unwrap();
        let err = ca_join::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("WRONG".to_string()),
            30,
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("refused"));
    }
}
