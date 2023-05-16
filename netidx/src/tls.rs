use crate::config::{Tls, TlsIdentity};
use anyhow::Result;
use log::{debug, info, warn};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, Bound},
    fmt, mem,
    sync::Arc,
};

pub(crate) fn load_certs(path: &str) -> Result<Vec<rustls::Certificate>> {
    use std::{fs, io::BufReader};
    Ok(rustls_pemfile::certs(&mut BufReader::new(fs::File::open(path)?))?
        .into_iter()
        .map(|v| rustls::Certificate(v))
        .collect())
}

// TODO: second pass feature: verify that the crls file is signed by our CA
pub(crate) fn load_crls(crls_path: &str) -> Result<Vec<Vec<u8>>> {
    let crl_bytes = std::fs::read(crls_path)?;
    let (_, crl) = x509_parser::parse_x509_crl(&crl_bytes)?;
    let revoked = crl
        .tbs_cert_list
        .revoked_certificates
        .iter()
        .map(|r| r.raw_serial().to_vec())
        .collect::<Vec<_>>();
    Ok(revoked)
}

pub(crate) fn get_common_name(cert: &[u8]) -> Result<Option<String>> {
    let (_, cert) = x509_parser::parse_x509_certificate(&cert)?;
    let name = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok().map(String::from));
    Ok(name)
}

fn load_key_password(askpass: &str, path: &str) -> Result<String> {
    use keyring::Entry;
    use std::process::Command;
    let entry = Entry::new("netidx", path)?;
    info!("loading password for {} from the system keyring", path);
    match entry.get_password() {
        Ok(password) => Ok(password),
        Err(e) => {
            info!("failed to find password entry for netidx {}, error {}", path, e);
            let res = Command::new(askpass).arg(path).output()?;
            let password = String::from_utf8_lossy(&res.stdout);
            if let Err(e) = entry.set_password(&password) {
                warn!("failed to set password entry for netidx {}, error {}", path, e);
            }
            Ok(password.into_owned())
        }
    }
}

pub(crate) fn load_private_key(
    askpass: Option<&str>,
    path: &str,
) -> Result<rustls::PrivateKey> {
    use pkcs8::{
        der::{pem::PemLabel, zeroize::Zeroize},
        EncryptedPrivateKeyInfo, PrivateKeyInfo, SecretDocument,
    };
    debug!("reading key from {}", path);
    let key = std::fs::read_to_string(path)?;
    let (label, doc) = match SecretDocument::from_pem(&key) {
        Ok((label, doc)) => (label, doc),
        Err(e) => bail!("failed to load pem {}, error: {}", path, e),
    };
    debug!("key label is {}", label);
    if label == EncryptedPrivateKeyInfo::PEM_LABEL {
        let key = match EncryptedPrivateKeyInfo::try_from(doc.as_bytes()) {
            Ok(key) => key,
            Err(e) => bail!("failed to parse encrypted key {}", e),
        };
        debug!("decrypting key");
        let mut password = match askpass {
            Some(askpass) => load_key_password(askpass, path)?,
            None => bail!("loading encrypted private keys not supported"),
        };
        let key = match key.decrypt(&password) {
            Ok(key) => key,
            Err(e) => bail!("failed to decrypt key {}", e),
        };
        password.zeroize();
        Ok(rustls::PrivateKey(Vec::from(key.as_bytes())))
    } else if label == PrivateKeyInfo::PEM_LABEL {
        Ok(rustls::PrivateKey(Vec::from(doc.as_bytes())))
    } else {
        bail!("expected a key in pem format")
    }
}
struct CustomVerifier {
    webpki_verifier: rustls::client::WebPkiVerifier,
    revoked_certificate_serials: Vec<Vec<u8>>,
}

impl rustls::client::ServerCertVerifier for CustomVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::Certificate,
        intermediates: &[rustls::Certificate],
        server_name: &rustls::ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        // first, do normal verification
        rustls::client::WebPkiVerifier::verify_server_cert(
            &self.webpki_verifier,
            end_entity,
            intermediates,
            server_name,
            scts,
            ocsp_response,
            now,
        )?;
        // then see if any certificate appears on the revocation list
        x509_parser::parse_x509_certificate(&end_entity.0)
            .map_err(|e| {
                rustls::Error::General(format!(
                    "failed to parse end entity certificate {:?}: {}",
                    end_entity.0, e
                ))
            })
            .and_then(|(_, cert)| {
                let cert_raw_serial_lowercase =
                    cert.tbs_certificate.raw_serial().to_ascii_lowercase();
                if self
                    .revoked_certificate_serials
                    .iter()
                    .any(|r| r.to_ascii_lowercase() == cert_raw_serial_lowercase)
                {
                    Err(rustls::Error::General(format!(
                        "revoked certificate, serial number: {:?}",
                        cert_raw_serial_lowercase
                    ))
                    .into())
                } else {
                    Ok(())
                }
            })?;

        // check if any intermediates are revoked
        for certificate in intermediates {
            let (_, cert) =
                x509_parser::parse_x509_certificate(&certificate.0).map_err(|e| {
                    rustls::Error::General(format!(
                        "failed to parse intermediate certificate {:?}: {}",
                        &certificate.0, e
                    ))
                })?;
            let cert_raw_serial_lowercase =
                cert.tbs_certificate.raw_serial().to_ascii_lowercase();
            if self
                .revoked_certificate_serials
                .iter()
                .any(|r| r.to_ascii_lowercase() == cert_raw_serial_lowercase)
            {
                return Err(rustls::Error::General(
                    format!(
                        "revoked certificate, serial number: {:?}",
                        cert_raw_serial_lowercase
                    )
                    .into(),
                ));
            }
        }
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

pub(crate) fn create_tls_connector(
    askpass: Option<&str>,
    root_certificates: &str,
    certificate: &str,
    private_key: &str,
) -> Result<tokio_rustls::TlsConnector> {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in load_certs(root_certificates)? {
        root_store.add(&cert)?;
    }
    let certs = load_certs(certificate)?;
    let private_key = load_private_key(askpass, private_key)?;
    let revoked_certificate_serials = load_crls("crls.txt")?;
    let mut config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store.clone())
        .with_single_cert(certs, private_key)?;

    config.dangerous().set_certificate_verifier(Arc::new(CustomVerifier {
        revoked_certificate_serials,
        webpki_verifier: rustls::client::WebPkiVerifier::new(root_store, None),
    }));
    config.resumption = rustls::client::Resumption::in_memory_sessions(256);
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

pub(crate) fn create_tls_acceptor(
    askpass: Option<&str>,
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
        rustls::server::AllowAnyAuthenticatedClient::new(root_store).boxed()
    };
    debug!("loading server certificate");
    let certs = load_certs(certificate)?;
    debug!("loading server private key");
    let private_key = load_private_key(askpass, private_key)?;
    debug!("creating tls acceptor");
    let mut config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, private_key)?;
    config.session_storage = rustls::server::ServerSessionMemoryCache::new(1024);
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

pub(crate) fn get_match<'a: 'b, 'b, U>(
    m: &'a BTreeMap<String, U>,
    identity: &'b str,
) -> Option<&'a U> {
    m.range::<str, (Bound<&str>, Bound<&str>)>((
        Bound::Unbounded,
        Bound::Included(identity),
    ))
    .next_back()
    .and_then(|(k, v)| {
        if k == identity || (identity.starts_with(k)) {
            Some(v)
        } else {
            None
        }
    })
}

struct CachedInnerLocked<T> {
    tmp: String,
    cached: BTreeMap<String, T>,
}

struct CachedInner<T> {
    tls: Tls,
    t: Mutex<CachedInnerLocked<T>>,
}

#[derive(Clone)]
struct Cached<T>(Arc<CachedInner<T>>);

impl<T> fmt::Debug for Cached<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachedTls")
    }
}

impl<T: Clone + 'static> Cached<T> {
    fn new(tls: Tls) -> Self {
        Self(Arc::new(CachedInner {
            tls,
            t: Mutex::new(CachedInnerLocked {
                tmp: String::with_capacity(256),
                cached: BTreeMap::new(),
            }),
        }))
    }

    fn default_identity(&self) -> &TlsIdentity {
        self.0.tls.default_identity()
    }

    fn get_identity(&self, id: &str) -> Option<&TlsIdentity> {
        self.0.tls.identities.get(id)
    }

    fn load(
        &self,
        identity: &str,
        f: fn(Option<&str>, &str, &str, &str) -> Result<T>,
    ) -> Result<T> {
        let rev_identity = {
            let mut inner = self.0.t.lock();
            inner.tmp.clear();
            inner.tmp.push_str(&identity);
            Tls::reverse_domain_name(&mut inner.tmp);
            if let Some(v) = get_match(&inner.cached, &inner.tmp) {
                return Ok(v.clone());
            }
            mem::replace(&mut inner.tmp, String::new())
        };
        match get_match(&self.0.tls.identities, &rev_identity) {
            None => {
                self.0.t.lock().tmp = rev_identity;
                bail!("no plausable identity matches {}", identity)
            }
            Some(TlsIdentity { name: _, trusted, certificate, private_key }) => {
                let askpass = self.0.tls.askpass.as_ref().map(|s| s.as_str());
                let con = f(askpass, trusted, certificate, private_key)?;
                self.0.t.lock().cached.insert(rev_identity, con.clone());
                Ok(con)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CachedConnector(Cached<tokio_rustls::TlsConnector>);

impl CachedConnector {
    pub(crate) fn new(cfg: Tls) -> Self {
        Self(Cached::new(cfg))
    }

    pub(crate) fn load(&self, identity: &str) -> Result<tokio_rustls::TlsConnector> {
        self.0.load(identity, create_tls_connector)
    }

    pub(crate) fn default_identity(&self) -> &TlsIdentity {
        self.0.default_identity()
    }

    pub(crate) fn get_identity(&self, name: &str) -> Option<&TlsIdentity> {
        self.0.get_identity(name)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CachedAcceptor(Cached<tokio_rustls::TlsAcceptor>);

impl CachedAcceptor {
    pub(crate) fn new(tls: Tls) -> Self {
        Self(Cached::new(tls))
    }

    pub(crate) fn load(
        &self,
        identity: Option<&str>,
    ) -> Result<tokio_rustls::TlsAcceptor> {
        let identity = identity.unwrap_or_else(|| &self.0.default_identity().name);
        self.0.load(identity, create_tls_acceptor)
    }
}
