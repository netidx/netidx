//! Trust-on-first-use rustls server-cert verifier, shared by the conf
//! client ([`crate::conf_client`]) and the resolver-name probe
//! ([`crate::resolver_probe`]).
//!
//! It accepts any certificate chain — trust is established out of band
//! (by fingerprint for the CA join; not at all for the name probe, whose
//! result is only a prefilled default and never a trust anchor) — but
//! still validates the handshake signature, so the peer must hold the
//! key for the cert it presents.
//!
//! It also **captures the peer's leaf cert** the moment it's verified.
//! That matters for the resolver probe: the resolver runs mutual TLS and
//! rejects the handshake once it sees we sent no client cert, so
//! `connect()` returns an error — but the server's `Certificate` message
//! (and thus this callback) arrives first, so the captured leaf is still
//! available after the failed handshake.

use parking_lot::Mutex;
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
    DigitallySignedStruct, SignatureScheme,
};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct TofuVerifier {
    provider: Arc<CryptoProvider>,
    captured_leaf: Mutex<Option<CertificateDer<'static>>>,
}

impl TofuVerifier {
    pub(crate) fn new(provider: Arc<CryptoProvider>) -> Self {
        TofuVerifier { provider, captured_leaf: Mutex::new(None) }
    }

    /// The leaf cert the peer presented during the handshake, captured
    /// even when the handshake ultimately fails (e.g. the peer required a
    /// client cert we didn't send). `None` until the server's cert has
    /// been seen.
    pub(crate) fn captured_leaf(&self) -> Option<CertificateDer<'static>> {
        self.captured_leaf.lock().clone()
    }
}

impl ServerCertVerifier for TofuVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        *self.captured_leaf.lock() = Some(end_entity.clone().into_owned());
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
