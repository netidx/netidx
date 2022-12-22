use anyhow::Result;
use parking_lot::Mutex;
use std::{sync::Arc, fmt};
use log::debug;

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
        debug!("creating tls client auth trust store");
        let mut root_store = rustls::RootCertStore::empty();
        debug!("loading CA certificates");
        for cert in load_certs(root_certificates)? {
            root_store.add(&cert)?;
        }
        rustls::server::AllowAnyAnonymousOrAuthenticatedClient::new(root_store)
    };
    debug!("loading server certificate");
    let certs = load_certs(certificate)?;
    debug!("loading server private key");
    let private_key = load_private_key(private_key)?;
    debug!("creating tls acceptor");
    let mut config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, private_key)?;
    config.session_storage = rustls::server::ServerSessionMemoryCache::new(1024);
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

#[derive(Clone)]
struct Cached<T>(Arc<Mutex<Option<T>>>);

impl<T> fmt::Debug for Cached<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachedTls")
    }
}

impl<T: Clone + 'static> Cached<T> {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    fn load(
        &self,
        root_certificates: &str,
        certificate: &str,
        private_key: &str,
        f: fn(&str, &str, &str) -> Result<T>,
    ) -> Result<T> {
        if let Some(con) = self.0.lock().as_ref() {
            return Ok(con.clone());
        }
        let con = f(root_certificates, certificate, private_key)?;
        *self.0.lock() = Some(con.clone());
        Ok(con)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CachedConnector(Cached<tokio_rustls::TlsConnector>);

impl CachedConnector {
    pub(crate) fn new() -> Self {
        Self(Cached::new())
    }

    pub(crate) fn load(
        &self,
        root_certificates: &str,
        certificate: &str,
        private_key: &str,
    ) -> Result<tokio_rustls::TlsConnector> {
        self.0.load(root_certificates, certificate, private_key, create_tls_connector)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CachedAcceptor(Cached<tokio_rustls::TlsAcceptor>);

impl CachedAcceptor {
    pub(crate) fn new() -> Self {
        Self(Cached::new())
    }

    pub(crate) fn load(
        &self,
        root_certificates: &str,
        certificate: &str,
        private_key: &str,
    ) -> Result<tokio_rustls::TlsAcceptor> {
        self.0.load(root_certificates, certificate, private_key, create_tls_acceptor)
    }
}
