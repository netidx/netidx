//! Join client: generate a key + CSR locally and request a signature
//! from the CA server over TLS, verifying the CA's identity by
//! fingerprint before sending the admin password.
//!
//! Cross-platform — rcgen + rustls + sha2 + x509-parser, never openssl
//! — so a Windows node can join a unix CA.
//!
//! ## Trust model
//!
//! The connection bootstraps with no prior trust (TOFU): a custom
//! verifier accepts whatever cert the daemon presents but still
//! validates the handshake signature, so the daemon proves it holds the
//! serving key. From the presented chain (`[serving_leaf, ca]`) the
//! client takes the CA cert, shows its [`Fingerprint`] to the operator,
//! and proceeds **only after the operator confirms it** out of band.
//! Then it verifies the serving leaf is signed by that confirmed CA and
//! carries the reserved [`SERVING_SAN`] — which, combined with the
//! handshake, means only the genuine daemon (holding a CA-issued
//! serving cert) can have reached this point. The admin password is
//! sent only after all of that.

use crate::{
    ca_proto::{
        self, ClientHello, NodeKind, Secret, ServerHello, SignRequest, SignResponse,
        PROTOCOL_VERSION, SERVING_SAN,
    },
    fingerprint::Fingerprint,
};
use anyhow::{anyhow, bail, Context, Result};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
    ClientConfig, DigitallySignedStruct, SignatureScheme,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
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

/// Connect to the CA server at `addr`, have the operator confirm the CA
/// identity (`confirm` is shown the CA-cert fingerprint and returns
/// whether it matched), then submit a CSR for `name` authenticated as
/// `admin`/`password`. Returns the signed cert + key + CA cert.
///
/// `confirm` is called **before** the password is sent — returning
/// `Ok(false)` aborts cleanly with nothing secret transmitted.
pub async fn request_cert<F>(
    addr: SocketAddr,
    kind: NodeKind,
    name: &str,
    admin: &str,
    password: Zeroizing<String>,
    validity_days: u32,
    confirm: F,
) -> Result<Issued>
where
    F: FnOnce(&Fingerprint) -> Result<bool>,
{
    // Local work first: our key + CSR.
    let kc = generate_key_and_csr(name)?;

    // TOFU handshake — accept any cert, but the handshake signature is
    // still checked, so the server proves it holds the serving key.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TofuVerifier { provider }))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let tcp = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to CA server {addr}"))?;
    let server_name = ServerName::try_from(SERVING_SAN).context("server name")?;
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake with CA server")?;

    // The presented chain is [serving_leaf, …, ca].
    let chain: Vec<CertificateDer<'static>> = {
        let (_io, conn) = tls.get_ref();
        conn.peer_certificates()
            .ok_or_else(|| anyhow!("CA server presented no certificate"))?
            .iter()
            .map(|c| c.clone().into_owned())
            .collect()
    };
    if chain.len() < 2 {
        bail!(
            "CA server did not present its CA certificate in the chain \
             (got {} cert(s)); the daemon's serving chain must be [leaf, ca]",
            chain.len()
        );
    }
    let serving_der = &chain[0];
    let ca_der = chain.last().unwrap();

    // Out-of-band CA identity check BEFORE anything secret is sent.
    let ca_fp = Fingerprint::of_der(ca_der.as_ref());
    if !confirm(&ca_fp)? {
        bail!("CA identity was not confirmed; aborted before sending the password");
    }

    // Bind the serving cert to the confirmed CA.
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
            // the operator never verified.
            if !bundle_contains(&trusted_pem, &ca_fp) {
                bail!(
                    "the trust bundle returned by the server does not contain the \
                     CA identity the operator confirmed"
                );
            }
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

/// Trust-on-first-use verifier: accepts any certificate chain (trust is
/// established out of band by fingerprint), but still validates the
/// handshake signature so the peer must hold the presented key.
#[derive(Debug)]
struct TofuVerifier {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for TofuVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
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
