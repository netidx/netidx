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
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::TlsAcceptor;

/// Concurrent in-flight handlers. Each `SignRequest` runs an Argon2id
/// derivation per slot, so this caps the memory/CPU an attacker can
/// induce by flooding wrong-password requests.
const MAX_CONCURRENT: usize = 4;

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
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("ca-server: accept failed: {e:#}");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let ca_dir = ca_dir.clone();
        let sem = sem.clone();
        tokio::spawn(async move {
            let permit = match sem.try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!("ca-server: too many concurrent requests, dropping {peer}");
                    return;
                }
            };
            let r = handle_conn(&acceptor, tcp, &ca_dir).await;
            drop(permit);
            if let Err(e) = r {
                debug!("ca-server: connection from {peer} ended: {e:#}");
            }
        });
    }
}

async fn handle_conn(
    acceptor: &TlsAcceptor,
    tcp: TcpStream,
    ca_dir: &Path,
) -> Result<()> {
    let mut tls = acceptor.accept(tcp).await.context("TLS handshake")?;
    let _hello: ClientHello = ca_proto::read_msg(&mut tls).await.context("reading ClientHello")?;
    ca_proto::write_msg(&mut tls, &ServerHello { protocol_version: PROTOCOL_VERSION })
        .await
        .context("writing ServerHello")?;
    let req: SignRequest = ca_proto::read_msg(&mut tls).await.context("reading SignRequest")?;
    let resp = handle_sign_request(ca_dir, &req);
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
        let params = CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn("Test CA".to_string()),
            san: vec![],
            key_bits: MIN_KEY_BITS,
            validity_days: 30,
        };
        Ca::init(&params, None).unwrap();
        let key = std::fs::read(dir.join("private.key")).unwrap();
        ca_vault::create(dir, &key, "alice", "apw", policy()).unwrap();
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

        let mut shown = false;
        let issued = ca_join::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            |fp| {
                // The operator is shown the CA fingerprint and confirms.
                assert!(!fp.text().is_empty());
                shown = true;
                Ok(true)
            },
        )
        .await
        .unwrap();

        assert!(shown, "confirm callback should have been invoked");
        // The signed leaf parses and is for our name.
        let cert = openssl::x509::X509::from_pem(issued.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some("resolver.ryu-oh.org")));
        // The returned trust bundle is the CA cert (the default bundle).
        let ca_disk = std::fs::read(dir.path().join("certificate.pem")).unwrap();
        assert_eq!(issued.trusted_pem.as_bytes(), ca_disk.as_slice());
    }

    #[tokio::test]
    async fn declining_the_fingerprint_aborts_before_the_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let addr = spawn_server(dir.path()).await;
        let err = ca_join::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            |_fp| Ok(false), // operator says "doesn't match"
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("not confirmed"));
    }

    #[tokio::test]
    async fn wrong_password_over_the_wire_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let addr = spawn_server(dir.path()).await;
        let err = ca_join::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("WRONG".to_string()),
            30,
            |_fp| Ok(true),
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("refused"));
    }
}
