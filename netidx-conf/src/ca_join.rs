//! Join client: generate a key + CSR locally and request a signature
//! from the CA server over TLS, verifying the CA's identity by
//! fingerprint before sending the admin password.
//!
//! Cross-platform — rcgen + rustls + sha2 + x509-parser, never openssl
//! — so a Windows node can join a unix CA.
//!
//! ## Trust model
//!
//! Two connections, so the operator never types a password before
//! seeing — and confirming — who they're talking to, and so human
//! think-time never holds a connection (or the server's request timeout)
//! open:
//!
//! 1. **Inspect** ([`fetch_ca_identity`]): a TOFU handshake — a custom
//!    verifier accepts whatever cert the daemon presents but still
//!    validates the handshake signature, so the daemon proves it holds
//!    the serving key. From the presented chain (`[serving_leaf, …, ca]`)
//!    the client takes the CA cert, verifies the serving leaf is signed
//!    by it and carries the reserved [`SERVING_SAN`], and returns its
//!    [`Fingerprint`]. The connection is then closed, sending nothing.
//! 2. The operator compares the fingerprint out of band and, only if it
//!    matches, enters their admin name + password.
//! 3. **Sign** ([`request_cert`]): a second TOFU handshake that **pins**
//!    the presented CA cert to the confirmed fingerprint, aborting before
//!    sending anything secret if it changed. Combined with the handshake
//!    and serving-cert check, only the genuine daemon (holding a
//!    CA-issued serving cert for the confirmed CA) can reach the point
//!    where the password is sent.

use crate::{
    ca_proto::{
        self, ClientHello, NodeKind, Secret, ServerHello, SignRequest, SignResponse,
        PROTOCOL_VERSION, SERVING_SAN,
    },
    fingerprint::Fingerprint,
    tls_tofu::TofuVerifier,
};
use anyhow::{anyhow, bail, Context, Result};
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

/// The result of a successful join: the signed leaf, the matching
/// private key, and the full trusted-CA bundle to install. Everything
/// the node needs — no manual file copying.
pub struct Issued {
    pub cert_pem: String,
    pub private_key_pem: Zeroizing<String>,
    pub trusted_pem: String,
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

/// A CA server's verified identity, captured by [`fetch_ca_identity`]:
/// the [`Fingerprint`] to show the operator out of band, plus the CA
/// certificate it's bound to. Pass it to [`request_cert`] to pin the
/// signing connection to exactly the CA the operator confirmed.
pub struct CaIdentity {
    /// SHA-256 of the CA cert, for out-of-band comparison (text +
    /// identicon).
    pub fingerprint: Fingerprint,
    /// The CA cert the fingerprint is of, kept so the signing connection
    /// can verify against the *confirmed* CA rather than a re-presented
    /// one.
    ca_der: CertificateDer<'static>,
}

/// TOFU-handshake to the CA server at `addr`; return the live TLS stream
/// and the certificate chain it presented (`[serving_leaf, …, ca]`).
/// Accepts any cert — trust is established out of band by fingerprint —
/// but the handshake signature is still verified, so the server proves it
/// holds the key for the cert it shows.
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
        .with_context(|| format!("connecting to CA server {addr}"))?;
    let server_name = ServerName::try_from(SERVING_SAN).context("server name")?;
    let tls = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake with CA server")?;
    let chain: Vec<CertificateDer<'static>> = {
        let (_io, conn) = tls.get_ref();
        conn.peer_certificates()
            .ok_or_else(|| anyhow!("CA server presented no certificate"))?
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
            "CA server did not present its CA certificate in the chain \
             (got {} cert(s)); the daemon's serving chain must be [leaf, …, ca]",
            chain.len()
        );
    }
    Ok((&chain[0], chain.last().unwrap()))
}

/// Connect to the CA server, TOFU-handshake, and return its verified
/// identity — **sending nothing**. The connection is closed before
/// returning, so the operator can compare the fingerprint and enter
/// credentials at their own pace without holding a connection (and the
/// server's request timeout) open. The serving cert is verified to be
/// bound to the returned CA, so the fingerprint shown really is the
/// authority backing this daemon.
///
/// The operator confirms the returned [`CaIdentity`] out of band, then
/// hands it to [`request_cert`], which pins the signing connection to it.
pub async fn fetch_ca_identity(addr: SocketAddr) -> Result<CaIdentity> {
    let (tls, chain) = connect_tofu(addr).await?;
    drop(tls); // we only needed the presented certificate; close now
    let (serving_der, ca_der) = split_chain(&chain)?;
    verify_serving_cert(serving_der, ca_der).context(
        "the CA server's serving certificate is not bound to the CA it presented",
    )?;
    Ok(CaIdentity {
        fingerprint: Fingerprint::of_der(ca_der.as_ref()),
        ca_der: ca_der.clone(),
    })
}

/// Submit a CSR for `name` to the CA server, authenticated as
/// `admin`/`password`, pinned to the CA identity the operator already
/// confirmed (`expected`, from [`fetch_ca_identity`]). Returns the signed
/// cert + key + trust bundle.
///
/// If the CA cert this (second) connection presents doesn't match
/// `expected` — a swap between inspection and signing — this aborts
/// **before** sending anything secret.
pub async fn request_cert(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    admin: &str,
    password: Zeroizing<String>,
    validity_days: u32,
    expected: &CaIdentity,
) -> Result<Issued> {
    // Local work first: our key + CSR.
    let kc = generate_key_and_csr(name)?;
    // Capture our public key now — the CSR is moved into the request
    // below, but we need it afterward to confirm the CA signed *our*
    // key, not a substituted one.
    let our_spki = csr_spki(&kc.csr_pem)?;

    let (mut tls, chain) = connect_tofu(addr).await?;
    let (serving_der, presented_ca) = split_chain(&chain)?;
    // Pin to the confirmed CA BEFORE anything secret is sent: a server
    // that swapped its cert since `fetch_ca_identity` is rejected here.
    if Fingerprint::of_der(presented_ca.as_ref()) != expected.fingerprint {
        bail!(
            "the CA server's identity changed since you confirmed it \
             (fingerprint mismatch); aborted before sending the password"
        );
    }
    // Everything downstream binds to the *confirmed* CA, not the
    // re-presented one (identical given the pin passed, but this makes
    // the trust anchor explicit).
    let ca_der = &expected.ca_der;
    verify_serving_cert(serving_der, ca_der).context(
        "the CA server's serving certificate failed verification against the \
         confirmed CA — this is not the CA you confirmed",
    )?;

    // Protocol.
    ca_proto::write_msg(
        &mut tls,
        &ClientHello { protocol_version: PROTOCOL_VERSION, kind },
    )
    .await?;
    let hello: ServerHello = ca_proto::read_msg(&mut tls).await?;
    if hello.protocol_version != PROTOCOL_VERSION {
        bail!(
            "CA server speaks protocol version {} but we speak {PROTOCOL_VERSION}",
            hello.protocol_version
        );
    }
    ca_proto::write_msg(
        &mut tls,
        &SignRequest {
            admin: admin.to_string(),
            password: Secret(password.as_str().to_string()),
            csr_pem: kc.csr_pem,
            requested_name: name.to_string(),
            requested_validity_days: validity_days,
        },
    )
    .await?;
    match ca_proto::read_msg::<_, SignResponse>(&mut tls).await? {
        SignResponse::Ok { signed_cert_pem, trusted_pem } => {
            // The confirmed CA must be present in the returned trust
            // bundle, or installing it would anchor trust in something
            // the operator never verified. (Additional CAs in the bundle
            // are allowed — federated trust is intentional.)
            if !bundle_contains(&trusted_pem, &expected.fingerprint) {
                bail!(
                    "the trust bundle returned by the server does not contain the \
                     CA identity the operator confirmed"
                );
            }
            // Bind the returned leaf to what the operator confirmed: a
            // bundle membership check alone doesn't — the leaf must be
            // signed by *the* confirmed CA (not merely some CA in the
            // bundle), carry exactly the name we asked for, and contain
            // our own public key (so it isn't an unrelated/replayed cert
            // we couldn't even use).
            verify_issued_leaf(&signed_cert_pem, ca_der, name, &our_spki).context(
                "the certificate returned by the CA server failed verification \
                 against the confirmed CA",
            )?;
            Ok(Issued {
                cert_pem: signed_cert_pem,
                private_key_pem: kc.private_key_pem,
                trusted_pem,
            })
        }
        SignResponse::Err { reason } => bail!("CA server refused to sign: {reason}"),
    }
}

/// True if `fp` is the fingerprint of any certificate in the PEM
/// `bundle`.
fn bundle_contains(bundle: &str, fp: &Fingerprint) -> bool {
    let mut rd = std::io::Cursor::new(bundle.as_bytes());
    rustls_pemfile::certs(&mut rd)
        .flatten()
        .any(|der| Fingerprint::of_der(der.as_ref()) == *fp)
}

/// Verify the CA server's issued leaf binds to what the operator
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
            if let GeneralName::DNSName(d) = gn {
                if *d == SERVING_SAN {
                    has_san = true;
                }
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
