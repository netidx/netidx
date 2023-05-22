use crate::config::{Tls, TlsIdentity};
use anyhow::{Context, Result};
use log::{debug, info, warn};
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::{
    collections::{BTreeMap, Bound},
    fmt, mem,
    sync::Arc,
};
use x509_parser::prelude::GeneralName;

pub(crate) fn load_certs(path: &str) -> Result<Vec<rustls::Certificate>> {
    use std::{fs, io::BufReader};
    Ok(rustls_pemfile::certs(&mut BufReader::new(fs::File::open(path)?))
        .context("parsing cert pem")?
        .into_iter()
        .map(|v| rustls::Certificate(v))
        .collect())
}

#[derive(Debug)]
pub struct Names {
    /// this can be anything and will be used to identify the user via map-ids
    pub cn: String,
    /// This must be a hostname. There must be exactly 1
    /// subjectAltName on the certificate.
    pub alt_name: String,
}

pub(crate) fn get_names(cert: &[u8]) -> Result<Option<Names>> {
    let (_, cert) = x509_parser::parse_x509_certificate(&cert)?;
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok().map(String::from));
    let mut alt_names: SmallVec<[String; 4]> = cert
        .subject_alternative_name()?
        .map(|alt_name| &alt_name.value.general_names)
        .unwrap_or(&vec![])
        .into_iter()
        .filter_map(|gn| match gn {
            GeneralName::DNSName(d) => Some(String::from(*d)),
            GeneralName::DirectoryName(_)
            | GeneralName::EDIPartyName(_)
            | GeneralName::IPAddress(_)
            | GeneralName::OtherName(_, _)
            | GeneralName::RFC822Name(_)
            | GeneralName::RegisteredID(_)
            | GeneralName::URI(_)
            | GeneralName::X400Address(_) => None,
        })
        .collect();
    let alt_name = if alt_names.len() == 0 {
	bail!("certificate is missing subjectAltName")
    } else if alt_names.len() > 1 {
	bail!("certificate must have exactly 1 subjectAltName for use with netidx")
    } else {
	alt_names.pop().unwrap()
    };
    Ok(cn.map(|cn| Names { cn, alt_name }))
}

/// load the password for the key at the specified path, or else call
/// askpass to ask the user, or else fail.
pub fn load_key_password(askpass: Option<&str>, path: &str) -> Result<String> {
    use keyring::Entry;
    use std::process::Command;
    let entry = Entry::new("netidx", path)?;
    info!("loading password for {} from the system keyring", path);
    match entry.get_password() {
        Ok(password) => Ok(password),
        Err(e) => match askpass {
            None => bail!("password isn't in the keychain and no askpass specified"),
            Some(askpass) => {
                info!("failed to find password entry for netidx {}, error {}", path, e);
                let res = Command::new(askpass).arg(path).output()?;
                let password = String::from_utf8_lossy(&res.stdout);
                let password = password.trim_matches(|c| c == '\r' || c == '\n');
                if let Err(e) = entry.set_password(password) {
                    warn!(
                        "failed to set password entry for netidx {}, error {}",
                        path, e
                    );
                }
                Ok(String::from(password))
            }
        },
    }
}

/// Save the password for the specified key in the user's keychain.
pub fn save_password_for_key(path: &str, password: &str) -> Result<()> {
    use keyring::Entry;
    let entry = Entry::new("netidx", path)?;
    Ok(entry.set_password(password)?)
}

pub fn load_private_key(askpass: Option<&str>, path: &str) -> Result<rustls::PrivateKey> {
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
        let mut password = load_key_password(askpass, path)?;
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

pub(crate) fn create_tls_connector(
    askpass: Option<&str>,
    root_certificates: &str,
    certificate: &str,
    private_key: &str,
) -> Result<tokio_rustls::TlsConnector> {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in load_certs(root_certificates).context("loading root certs")? {
        root_store.add(&cert).context("adding root cert")?;
    }
    let certs = load_certs(certificate).context("loading user cert")?;
    let private_key =
        load_private_key(askpass, private_key).context("loading user private key")?;
    let mut config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_single_cert(certs, private_key)
        .context("building rustls client config")?;
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
