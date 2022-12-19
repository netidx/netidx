use anyhow::Result;
use std::sync::Arc;
use tokio::task;

fn load_certs(path: &str) -> Result<Vec<rustls::Certificate>> {
    use std::{fs, io::BufReader};
    Ok(rustls_pemfile::certs(&mut BufReader::new(fs::File::open(path)?))?
        .into_iter()
        .map(|v| rustls::Certificate(v))
        .collect())
}

fn load_private_key(path: &str) -> Result<rustls::PrivateKey> {
    use std::{fs, io::BufReader};
    let mut reader = BufReader::new(fs::File::open(path)?);
    while let Some(key) = rustls_pemfile::read_one(&mut reader)? {
        match key {
            rustls_pemfile::Item::RSAKey(key) => return Ok(rustls::PrivateKey(key)),
            rustls_pemfile::Item::PKCS8Key(key) => return Ok(rustls::PrivateKey(key)),
            rustls_pemfile::Item::ECKey(key) => return Ok(rustls::PrivateKey(key)),
            _ => (),
        }
    }
    // CR estokes: probably need to support encrypted keys.
    bail!("no keys found, encrypted keys not supported")
}

pub(crate) fn create_tls_connector(
    root_certificates: &str,
    certificate: &str,
    private_key: &str,
) -> Result<tokio_rustls::TlsConnector> {
    use crate::resolver_server::secctx::{load_certs, load_private_key};
    let mut root_store = rustls::RootCertStore::empty();
    for cert in load_certs(root_certificates)? {
        root_store.add(&cert)?;
    }
    let certs = load_certs(certificate)?;
    let private_key = load_private_key(private_key)?;
    let mut config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_single_cert(certs, private_key)?;
    config.session_storage = rustls::client::ClientSessionMemoryCache::new(256);
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

pub(crate) fn create_tls_acceptor(
    root_certificates: &str,
    certificate: &str,
    private_key: &str,
) -> Result<tokio_rustls::TlsAcceptor> {
    let client_auth = {
        let mut root_store = rustls::RootCertStore::empty();
        for cert in task::block_in_place(|| load_certs(root_certificates))? {
            root_store.add(&cert)?;
        }
        rustls::server::AllowAnyAnonymousOrAuthenticatedClient::new(root_store)
    };
    let certs = task::block_in_place(|| load_certs(certificate))?;
    let private_key = task::block_in_place(|| load_private_key(private_key))?;
    let mut config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, private_key)?;
    config.session_storage = rustls::server::ServerSessionMemoryCache::new(1024);
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}
