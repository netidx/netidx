//! TLS configuration and utilities.
use crate::config::{Tls, TlsIdentity};
use ahash::AHashMap;
use anyhow::{Context, Result};
use log::{debug, info, warn};
use parking_lot::Mutex;
use rustls_pki_types::{
    pem::{PemObject, SectionKind},
    PrivateKeyDer,
};
use smallvec::SmallVec;
use std::{
    collections::BTreeMap,
    fmt, mem,
    sync::{Arc, LazyLock},
};
use x509_parser::prelude::GeneralName;

pub(crate) fn load_certs(
    path: &str,
) -> Result<Vec<rustls_pki_types::CertificateDer<'static>>> {
    use std::{fs, io::BufReader};
    Ok(rustls_pemfile::certs(&mut BufReader::new(fs::File::open(path)?))
        .map(|r| r.map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?)
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
            | GeneralName::X400Address(_)
            | GeneralName::Invalid(_, _) => None,
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

static CACHED: LazyLock<Mutex<AHashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(AHashMap::default()));

/// pre cache the password for a private key.
///
/// If you obtained a certificate private key password via another
/// method as part of startup, you can provide it here and the system
/// keychain won't be consulted.
pub fn pre_cache_password(path: &str, password: &str) {
    CACHED.lock().insert(path.into(), password.into());
}

/// clear all cached passwords
pub fn clear_cached_passwords() {
    for (_, p) in CACHED.lock().drain() {
        let mut v = p.into_bytes();
        for i in 0..v.len() {
            v[i] = 0;
        }
    }
}

/// The TPM-sealed password sidecar for the key at `path` — `<path>.tpm`.
/// Written at issue time when the operator chose to seal; the password
/// inside only unseals on this machine's TPM, which is what makes the
/// (encrypted) key file useless off-host.
pub fn sealed_password_path(path: &str) -> String {
    format!("{path}.tpm")
}

/// load the password for the key at path
///
/// A TPM-sealed sidecar (`<path>.tpm`) is authoritative when present —
/// it's the daemon path: machine-bound, no keychain, no session, no
/// human. Failure to unseal it is a hard error rather than a fallthrough
/// to the keychain/askpass: a daemon hanging on a password prompt that
/// will never be answered is strictly worse than a clear failure, and
/// the fix is one command. Otherwise: the keychain, or else askpass, or
/// else fail.
pub fn load_key_password(askpass: Option<&str>, path: &str) -> Result<String> {
    use keyring::Entry;
    use std::process::Command;
    let mut cache = CACHED.lock();
    if let Some(pass) = cache.get(path) {
        return Ok(pass.into());
    }
    let sealed = sealed_password_path(path);
    if std::path::Path::new(&sealed).exists() {
        info!("unsealing password for {} from the TPM ({sealed})", path);
        let blob = std::fs::read(&sealed)
            .with_context(|| format!("reading sealed password {sealed}"))?;
        let secret = netidx_tpm::unseal(&blob).with_context(|| {
            format!(
                "unsealing {sealed} — if this host's TPM was cleared or the \
                 board was replaced, re-issue the key (netidx certificate \
                 issuance is one command; see `netidx conf ca`)"
            )
        })?;
        let password = String::from_utf8(secret.to_vec())
            .context("sealed password is not utf8")?;
        cache.insert(path.into(), password.clone());
        return Ok(password);
    }
    info!("loading password for {} from the system keyring", path);
    let entry = Entry::new("netidx", path)?;
    match entry.get_password() {
        Ok(password) => {
            cache.insert(path.into(), password.clone());
            Ok(password)
        }
        Err(e) => match askpass {
            None => {
                bail!("password isn't in the keychain and no askpass specified")
            }
            Some(askpass) => {
                info!(
                    "failed to find password entry for netidx {}, error {}",
                    path, e
                );
                let res = Command::new(askpass).arg(path).output()?;
                let password = String::from_utf8_lossy(&res.stdout);
                let password = password.trim_matches(|c| c == '\r' || c == '\n');
                if let Err(e) = entry.set_password(password) {
                    warn!(
                        "failed to set password entry for netidx {}, error {}",
                        path, e
                    );
                }
                let password = String::from(password);
                cache.insert(path.into(), password.clone());
                Ok(password)
            }
        },
    }
}

/// Save the password in the user's keychain.
pub fn save_password_for_key(path: &str, password: &str) -> Result<()> {
    use keyring::Entry;
    let entry = Entry::new("netidx", path)?;
    Ok(entry.set_password(password)?)
}

/// Decrypt an encrypted PKCS#8 private key PEM with `password`,
/// returning the plaintext PKCS#8 PEM. For loaders that don't go
/// through [`load_private_key`] (e.g. handing PEM bytes to rustls
/// directly); the returned value is key material — hold it briefly.
pub fn decrypt_private_key(
    enc_pem: &str,
    password: &str,
) -> Result<pkcs8::der::zeroize::Zeroizing<String>> {
    use pkcs8::{
        der::pem::PemLabel, EncryptedPrivateKeyInfo, LineEnding, PrivateKeyInfo,
        SecretDocument,
    };
    let (label, doc) = SecretDocument::from_pem(enc_pem)
        .map_err(|e| anyhow!("parsing private key pem: {e}"))?;
    if label != EncryptedPrivateKeyInfo::PEM_LABEL {
        bail!("expected an encrypted PKCS#8 private key, got {label:?}");
    }
    let enc = EncryptedPrivateKeyInfo::try_from(doc.as_bytes())
        .map_err(|e| anyhow!("parsing encrypted PKCS#8: {e}"))?;
    let dec = enc.decrypt(password).map_err(|e| anyhow!("decrypting key: {e}"))?;
    let pem = dec
        .to_pem(PrivateKeyInfo::PEM_LABEL, LineEnding::LF)
        .map_err(|e| anyhow!("encoding key pem: {e}"))?;
    Ok(pem)
}

/// Encrypt a plaintext PKCS#8 private key PEM under `password` (PBES2:
/// scrypt + AES-256-CBC — pure Rust, and what [`load_private_key`]
/// and openssl 3 both decrypt). The issue-time half of key protection;
/// pairs with either a typed password or a TPM-sealed one
/// ([`sealed_password_path`]).
pub fn encrypt_private_key(plain_pem: &str, password: &str) -> Result<String> {
    use pkcs8::{
        der::pem::PemLabel, rand_core::OsRng, EncryptedPrivateKeyInfo, LineEnding,
        PrivateKeyInfo, SecretDocument,
    };
    let (label, doc) = SecretDocument::from_pem(plain_pem)
        .map_err(|e| anyhow!("parsing private key pem: {e}"))?;
    if label != PrivateKeyInfo::PEM_LABEL {
        bail!("expected an unencrypted PKCS#8 private key, got {label:?}");
    }
    let pki = PrivateKeyInfo::try_from(doc.as_bytes())
        .map_err(|e| anyhow!("parsing PKCS#8 structure: {e}"))?;
    let enc = pki.encrypt(OsRng, password).map_err(|e| anyhow!("encrypting key: {e}"))?;
    let pem = enc
        .to_pem(EncryptedPrivateKeyInfo::PEM_LABEL, LineEnding::LF)
        .map_err(|e| anyhow!("encoding encrypted key pem: {e}"))?;
    Ok(pem.to_string())
}

/// load a private key
///
/// if askpass is Some then call `load_key_password` if the private key is encrypted
pub fn load_private_key(
    askpass: Option<&str>,
    path: &str,
) -> Result<PrivateKeyDer<'static>> {
    use pkcs8::{
        der::{pem::PemLabel, zeroize::Zeroize},
        EncryptedPrivateKeyInfo, PrivateKeyInfo, SecretDocument,
    };
    debug!("reading key from {}", path);
    let doc = std::fs::read_to_string(path)?;
    let (label, doc) = match SecretDocument::from_pem(&doc) {
        Ok((label, doc)) => (label, doc),
        Err(e) => bail!("failed to load pem {}, error: {}", path, e),
    };
    debug!("key label is {}", label);
    if label == EncryptedPrivateKeyInfo::PEM_LABEL {
        let doc = match EncryptedPrivateKeyInfo::try_from(doc.as_bytes()) {
            Ok(doc) => doc,
            Err(e) => bail!("failed to parse encrypted key {}", e),
        };
        debug!("decrypting key");
        let mut password = load_key_password(askpass, path)?;
        let doc = match doc.decrypt(&password) {
            Ok(doc) => doc,
            Err(e) => bail!("failed to decrypt key {}", e),
        };
        password.zeroize();
        let key =
            PrivateKeyDer::from_pem(SectionKind::PrivateKey, Vec::from(doc.as_bytes()))
                .ok_or_else(|| anyhow!("invalid key"))?;
        Ok(key)
    } else if label == PrivateKeyInfo::PEM_LABEL {
        let key =
            PrivateKeyDer::from_pem(SectionKind::PrivateKey, Vec::from(doc.as_bytes()))
                .ok_or_else(|| anyhow!("invalid key"))?;
        Ok(key)
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
        root_store.add(cert).context("adding root cert")?;
    }
    let certs = load_certs(certificate).context("loading user cert")?;
    let private_key =
        load_private_key(askpass, private_key).context("loading user private key")?;
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, private_key)
        .context("building rustls client config")?;
    config.resumption = rustls::client::Resumption::in_memory_sessions(256);
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// The CRL that applies to the trust bundle at `root_certificates`, by
/// convention `crl.pem` in the same directory. Distributed there by the
/// conf plane; absence simply means no revocation checking.
fn crl_path_for(root_certificates: &str) -> std::path::PathBuf {
    std::path::Path::new(root_certificates).with_file_name("crl.pem")
}

fn load_crls(
    root_certificates: &str,
) -> Result<Vec<rustls_pki_types::CertificateRevocationListDer<'static>>> {
    let path = crl_path_for(root_certificates);
    let pem = match std::fs::read(&path) {
        Ok(p) => p,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e).context(format!("reading CRL {path:?}")),
    };
    rustls_pemfile::crls(&mut std::io::Cursor::new(pem))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context(format!("parsing CRL {path:?}"))
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
            root_store.add(cert)?;
        }
        let builder =
            rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store));
        // A `crl.pem` beside the trust bundle turns on revocation
        // checking: a serial on the list is refused at the handshake.
        // Unknown status stays permitted — a federated bundle may
        // contain CAs whose CRLs we don't hold, and absence of a CRL
        // must not lock everyone out; presence on one must.
        let crls = load_crls(root_certificates)?;
        if crls.is_empty() {
            builder.build()?
        } else {
            debug!("loading certificate revocation list");
            builder.with_crls(crls).allow_unknown_revocation_status().build()?
        }
    };
    debug!("loading server certificate");
    let certs = load_certs(certificate)?;
    debug!("loading server private key");
    let private_key = load_private_key(askpass, private_key)?;
    debug!("creating tls acceptor");
    let mut config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(certs, private_key)?;
    config.session_storage = rustls::server::ServerSessionMemoryCache::new(1024);
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// A TLS acceptor that rebuilds itself when the CRL beside its trust
/// bundle changes (appears, disappears, or is rewritten), so a
/// revocation distributed by the conf plane takes effect on the next
/// accepted connection — no daemon restart. One `stat` per accept;
/// rebuilds are rare (revocations) and a failed rebuild keeps serving
/// with the previous acceptor rather than going dark.
///
/// Used by the resolver server — the enforcement choke point: a
/// revoked cert that can't authenticate to the resolver gets no
/// subscription tokens, so publishers never see it.
#[derive(Clone)]
pub(crate) struct CrlWatchingAcceptor(Arc<CrlWatchingInner>);

struct CrlWatchingInner {
    askpass: Option<String>,
    root_certificates: String,
    certificate: String,
    private_key: String,
    state: Mutex<(Option<std::time::SystemTime>, tokio_rustls::TlsAcceptor)>,
}

impl fmt::Debug for CrlWatchingAcceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CrlWatchingAcceptor")
    }
}

fn crl_mtime(root_certificates: &str) -> Option<std::time::SystemTime> {
    std::fs::metadata(crl_path_for(root_certificates)).and_then(|m| m.modified()).ok()
}

impl CrlWatchingAcceptor {
    pub(crate) fn new(
        askpass: Option<&str>,
        root_certificates: &str,
        certificate: &str,
        private_key: &str,
    ) -> Result<Self> {
        let acceptor =
            create_tls_acceptor(askpass, root_certificates, certificate, private_key)?;
        Ok(Self(Arc::new(CrlWatchingInner {
            askpass: askpass.map(String::from),
            root_certificates: String::from(root_certificates),
            certificate: String::from(certificate),
            private_key: String::from(private_key),
            state: Mutex::new((crl_mtime(root_certificates), acceptor)),
        })))
    }

    /// The current acceptor, rebuilt first if the CRL changed.
    pub(crate) fn acceptor(&self) -> tokio_rustls::TlsAcceptor {
        let t = &*self.0;
        let mtime = crl_mtime(&t.root_certificates);
        let mut state = t.state.lock();
        if mtime != state.0 {
            match create_tls_acceptor(
                t.askpass.as_deref(),
                &t.root_certificates,
                &t.certificate,
                &t.private_key,
            ) {
                Ok(acceptor) => {
                    info!("reloaded TLS acceptor (CRL changed)");
                    *state = (mtime, acceptor);
                }
                Err(e) => {
                    // Keep serving with the previous acceptor; don't
                    // re-attempt on every connection while the file is
                    // broken — wait for the next change.
                    warn!("failed to reload TLS acceptor after CRL change: {e:#}");
                    state.0 = mtime;
                }
            }
        }
        state.1.clone()
    }
}

pub(crate) fn get_match<'a: 'b, 'b, U>(
    m: &'a BTreeMap<String, U>,
    identity: &'b str,
) -> Option<&'a U> {
    m.iter().find_map(|(k, v)| {
        if k == identity || identity.starts_with(k) {
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
                bail!("no plausible identity matches {}", identity)
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_match() {
        let mut m = BTreeMap::new();
        m.insert("com.mydomain.".to_string(), 1);
        m.insert("com.mydomain.foo.".to_string(), 2);
        let r = get_match(&m, "com.mydomain.bar.").copied();
        assert_eq!(r, Some(1));
        let r = get_match(&m, "com.mydomain.qux.").copied();
        assert_eq!(r, Some(1));
    }
}
