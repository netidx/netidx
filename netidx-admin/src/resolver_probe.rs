//! Discover a netidx resolver's TLS identity — the DNS SAN on the cert
//! it serves — by connecting and capturing that cert in a
//! trust-on-first-use handshake. The setup tooling uses the result to
//! prefill the "resolver TLS name" prompt instead of making the operator
//! type it by hand.
//!
//! ## How
//!
//! netidx TLS is STARTTLS-style: the client first exchanges a plaintext
//! protocol version and sends a [`ClientHello`], and only then does the
//! socket upgrade to TLS (see `resolver_client::read_client`). So this
//! replays that short plaintext preamble, then drives a TOFU handshake
//! (accept any cert) and reads the single DNS SAN off the leaf the server
//! presents.
//!
//! The resolver runs *mutual* TLS, so it rejects the handshake the moment
//! it sees we sent no client cert — `connect()` returns an error. That's
//! fine: the server sends its own `Certificate` before asking for ours,
//! so [`TofuVerifier`](crate::tls_tofu::TofuVerifier) has already captured
//! the leaf by then, and we read it from there rather than from the
//! (failed) connection.
//!
//! ## Why this is safe
//!
//! The name is **not** a trust anchor. A real client still verifies the
//! resolver's cert against its configured trusted-CA bundle at every
//! connect, so a wrong name — whether from a stale cert or a MITM'd probe
//! — fails closed (the connection just won't authenticate). We therefore
//! use the probe result only as a prefilled default the operator can
//! override, never as a security decision.

use crate::tls_tofu::TofuVerifier;
use anyhow::{Context, Result, anyhow, bail};
use netidx::{
    protocol::resolver::{AuthRead, ClientHello},
    read_raw, write_raw,
};
use rustls::ClientConfig;
use rustls_pki_types::ServerName;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpStream, time};
use tokio_rustls::TlsConnector;

/// The resolver wire-protocol version we speak; matches
/// `resolver_client::read_client`.
const PROTOCOL_VERSION: u64 = 3;

/// Total budget for the probe (TCP connect + preamble + handshake). A
/// hung or non-netidx peer must not stall the interactive setup flow.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Probe the resolver at `addr` and return the first DNS SAN on the cert
/// it presents — the value a client must use as its `Auth::Tls { name }`.
/// `Ok(None)` means the resolver answered but its cert had no usable DNS
/// SAN; `Err` means we couldn't reach or speak to it.
pub async fn probe_resolver_tls_name(addr: SocketAddr) -> Result<Option<String>> {
    match time::timeout(PROBE_TIMEOUT, probe_inner(addr)).await {
        Ok(res) => res,
        Err(_) => bail!("probing resolver {addr} timed out after {PROBE_TIMEOUT:?}"),
    }
}

async fn probe_inner(addr: SocketAddr) -> Result<Option<String>> {
    let mut tcp = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to resolver {addr}"))?;
    let _ = tcp.set_nodelay(true);
    // Plaintext preamble: version handshake, then announce we're a
    // read-only TLS client. After the hello the server upgrades to TLS.
    // Same length-prefixed framing the resolver client uses (it rejects
    // the encrypted-flag bit), reused straight from netidx.
    write_raw(&mut tcp, &PROTOCOL_VERSION).await.context("sending version")?;
    let server_version: u64 =
        read_raw::<u64, _, 1024>(&mut tcp).await.context("reading server version")?;
    if server_version != PROTOCOL_VERSION {
        bail!(
            "resolver {addr} speaks protocol version {server_version}, \
             expected {PROTOCOL_VERSION}"
        );
    }
    write_raw(&mut tcp, &ClientHello::ReadOnly(AuthRead::Tls))
        .await
        .context("sending TLS client hello")?;

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = Arc::new(TofuVerifier::new(provider.clone()));
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .dangerous()
        .with_custom_certificate_verifier(verifier.clone())
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    // The resolver serves a single cert regardless of SNI, so the name we
    // pass is irrelevant; use the IP, for which rustls omits the SNI
    // extension entirely.
    let server_name = ServerName::IpAddress(addr.ip().into());
    // The handshake is expected to fail (the resolver wants a client cert
    // we don't have), but the verifier captures the server's leaf before
    // that. Only surface the connect error if we got no cert at all.
    let connect_res = connector.connect(server_name, tcp).await;
    match verifier.captured_leaf() {
        Some(leaf) => Ok(crate::tls::first_dns_san_from_der(leaf.as_ref())),
        None => Err(connect_res.err().map(anyhow::Error::new).unwrap_or_else(|| {
            anyhow!(
                "resolver {addr} completed the handshake but presented no certificate"
            )
        }))
        .with_context(|| format!("TLS handshake with resolver {addr}")),
    }
}
