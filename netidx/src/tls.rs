//! TLS configuration and utilities.
use crate::config::{Tls, TlsIdentity};
use ahash::AHashMap;
use anyhow::{Context, Result};
use log::{debug, info, warn};
use parking_lot::Mutex;
use rustls_pki_types::{
    PrivateKeyDer,
    pem::{PemObject, SectionKind},
};
use smallvec::SmallVec;
use std::{
    collections::BTreeMap,
    fmt,
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
        info!(
            "unsealing password for {} from the {} ({sealed})",
            path,
            netidx_tpm::MECHANISM
        );
        let blob = std::fs::read(&sealed)
            .with_context(|| format!("reading sealed password {sealed}"))?;
        let secret = netidx_tpm::unseal(&blob).with_context(|| {
            format!(
                "unsealing {sealed} — if this host's {} was cleared or the \
                 hardware was replaced, re-issue the key (netidx certificate \
                 issuance is one command; see `netidx conf ca`)",
                netidx_tpm::MECHANISM
            )
        })?;
        let password =
            String::from_utf8(secret.to_vec()).context("sealed password is not utf8")?;
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
        EncryptedPrivateKeyInfo, LineEnding, PrivateKeyInfo, SecretDocument,
        der::pem::PemLabel,
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
        EncryptedPrivateKeyInfo, LineEnding, PrivateKeyInfo, SecretDocument,
        der::pem::PemLabel, rand_core::OsRng,
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
        EncryptedPrivateKeyInfo, PrivateKeyInfo, SecretDocument,
        der::{pem::PemLabel, zeroize::Zeroize},
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
        let builder = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store));
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

/// A TLS acceptor that rebuilds itself when its serving cert, key, trust
/// bundle, or CRL changes on disk (see [`cert_set_mtimes`]), so a renewal or
/// revocation distributed by the conf plane takes effect on the next accepted
/// connection — no daemon restart. A few `stat`s per accept; rebuilds are
/// rare (renewals/revocations) and a failed rebuild keeps serving with the
/// previous acceptor rather than going dark.
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
    state: Mutex<(CertMtimes, tokio_rustls::TlsAcceptor)>,
}

impl fmt::Debug for CrlWatchingAcceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CrlWatchingAcceptor")
    }
}

/// mtimes of the four files a TLS config is built from: the leaf cert, its
/// key, the trust bundle, and the CRL beside the bundle. A leaf renewal (new
/// cert + key), a CA renewal (new trusted.pem), or a revocation (new crl.pem)
/// each move one of these — so a cached connector/acceptor is rebuilt when
/// any of them changes. Without this, a long-running process keeps
/// presenting (or serving) the cert it loaded at startup until it restarts,
/// which silently defeats certificate renewal.
type CertMtimes = [Option<std::time::SystemTime>; 4];

fn cert_set_mtimes(certificate: &str, private_key: &str, trusted: &str) -> CertMtimes {
    let m = |p: &std::path::Path| std::fs::metadata(p).and_then(|x| x.modified()).ok();
    [
        m(std::path::Path::new(certificate)),
        m(std::path::Path::new(private_key)),
        m(std::path::Path::new(trusted)),
        m(&crl_path_for(trusted)),
    ]
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
            state: Mutex::new((
                cert_set_mtimes(certificate, private_key, root_certificates),
                acceptor,
            )),
        })))
    }

    /// The current acceptor, rebuilt first if the cert set changed on disk.
    pub(crate) fn acceptor(&self) -> tokio_rustls::TlsAcceptor {
        let t = &*self.0;
        let mtimes =
            cert_set_mtimes(&t.certificate, &t.private_key, &t.root_certificates);
        let mut state = t.state.lock();
        if mtimes != state.0 {
            match create_tls_acceptor(
                t.askpass.as_deref(),
                &t.root_certificates,
                &t.certificate,
                &t.private_key,
            ) {
                Ok(acceptor) => {
                    info!("reloaded TLS acceptor (serving cert or CRL changed)");
                    *state = (mtimes, acceptor);
                }
                Err(e) => {
                    // Keep serving with the previous acceptor; don't
                    // re-attempt on every connection while the file is
                    // broken — wait for the next change.
                    warn!("failed to reload TLS acceptor after cert/CRL change: {e:#}");
                    state.0 = mtimes;
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
        if k == identity || identity.starts_with(k) { Some(v) } else { None }
    })
}

/// How many times a cold cert build retries when it has no cached config to
/// fall back to, and the delay between attempts. The product is the worst-case
/// added latency before a genuinely broken cert surfaces its error; it only
/// applies on the build-failure path (the steady state and the warm-fallback
/// path are untouched). 8 × 25ms = 200ms comfortably covers a renewal's
/// cert-then-key install window, which is sub-millisecond in practice.
const CERT_BUILD_RETRIES: usize = 8;
const CERT_BUILD_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_millis(25);

struct CachedInnerLocked<T> {
    cached: BTreeMap<String, (CertMtimes, T)>,
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
            t: Mutex::new(CachedInnerLocked { cached: BTreeMap::new() }),
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
        let mut rev = String::with_capacity(identity.len() + 1);
        rev.push_str(identity);
        Tls::reverse_domain_name(&mut rev);
        let TlsIdentity { name: _, trusted, certificate, private_key } =
            match get_match(&self.0.tls.identities, &rev) {
                None => bail!("no plausible identity matches {identity}"),
                Some(id) => id,
            };
        // Rebuild whenever the cert set on disk has moved since this
        // identity's connector/acceptor was cached — that's how a renewal
        // the conf plane installed actually reaches a long-running process,
        // rather than it presenting its startup cert forever.
        let mtimes = cert_set_mtimes(certificate, private_key, trusted);
        {
            let inner = self.0.t.lock();
            if let Some((m, v)) = get_match(&inner.cached, &rev)
                && *m == mtimes
            {
                return Ok(v.clone());
            }
        }
        let askpass = self.0.tls.askpass.as_deref();
        // A rebuild can fail transiently mid-renewal: the certificate and key
        // are two separate files, so a reader can momentarily observe a freshly
        // installed cert whose matching key hasn't landed yet (rustls reports
        // KeyMismatch). Two cases:
        //
        // - We have a previously cached config: serve it (a still-valid,
        //   unexpired cert) immediately; the next mtime change retries.
        // - Nothing cached — a fresh process, or a fresh `Subscriber`'s own
        //   connector, which starts with an empty cache: there is nothing to
        //   fall back to, so retry the build a few times. The matching key
        //   lands within milliseconds. Without this, a cold subscriber that
        //   happens to resolve during a renewal's install window fails outright
        //   while warm/persistent subscribers (already cached) sail through.
        let mut mtimes = mtimes;
        let mut tries = 0;
        loop {
            match f(askpass, trusted, certificate, private_key) {
                Ok(built) => {
                    self.0.t.lock().cached.insert(rev, (mtimes, built.clone()));
                    break Ok(built);
                }
                Err(e) => {
                    {
                        let inner = self.0.t.lock();
                        if let Some((_, v)) = get_match(&inner.cached, &rev) {
                            let v = v.clone();
                            drop(inner);
                            warn!("tls: cert reload failed, using previous cert: {e:#}");
                            break Ok(v);
                        }
                    }
                    tries += 1;
                    if tries >= CERT_BUILD_RETRIES {
                        break Err(e);
                    }
                    // load() always runs inside spawn_blocking, so a short
                    // blocking sleep here is fine and bridges the install skew.
                    std::thread::sleep(CERT_BUILD_RETRY_DELAY);
                    mtimes = cert_set_mtimes(certificate, private_key, trusted);
                }
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

    // `f` is a bare fn pointer (can't capture), so the "build" counts through
    // a module-level static. fetch_add returns the pre-increment value, so the
    // first build yields token 0. Used only by the test below.
    static BUILD_COUNT: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    fn counting_build(_: Option<&str>, _: &str, _: &str, _: &str) -> Result<usize> {
        Ok(BUILD_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }

    /// A renewed cert on disk must actually reach a long-running process:
    /// `Cached::load` rebuilds when any watched file's mtime moves, and
    /// serves the cached value otherwise. This is the regression guard for
    /// the class of bug where renewal silently did nothing until restart.
    #[test]
    fn cached_rebuilds_when_cert_set_changes() {
        use std::{
            fs,
            io::Write,
            sync::atomic::Ordering::SeqCst,
            time::{Duration, SystemTime},
        };
        let count = || BUILD_COUNT.load(SeqCst);
        BUILD_COUNT.store(0, SeqCst);
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        let trusted = dir.path().join("trusted.pem");
        for p in [&cert, &key, &trusted] {
            fs::File::create(p).unwrap().write_all(b"x").unwrap();
        }
        let t0 = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let set_mtime = |p: &std::path::Path, t: SystemTime| {
            fs::File::options().write(true).open(p).unwrap().set_modified(t).unwrap();
        };
        for p in [&cert, &key, &trusted] {
            set_mtime(p, t0);
        }
        // identities + cache are keyed by the reversed domain name.
        let mut ids = BTreeMap::new();
        ids.insert(
            "com.example.foo.".to_string(),
            TlsIdentity {
                trusted: trusted.to_str().unwrap().to_string(),
                name: "foo.example.com".to_string(),
                certificate: cert.to_str().unwrap().to_string(),
                private_key: key.to_str().unwrap().to_string(),
            },
        );
        let tls = Tls {
            default_identity: "com.example.foo.".to_string(),
            identities: ids,
            askpass: None,
        };
        let cached: Cached<usize> = Cached::new(tls);

        // cold load builds (token 0)
        assert_eq!(cached.load("foo.example.com", counting_build).unwrap(), 0);
        assert_eq!(count(), 1);
        // unchanged: cache hit, f not called
        assert_eq!(cached.load("foo.example.com", counting_build).unwrap(), 0);
        assert_eq!(count(), 1);
        // a renewal bumps the cert mtime: rebuild (token 1)
        set_mtime(&cert, t0 + Duration::from_secs(60));
        assert_eq!(cached.load("foo.example.com", counting_build).unwrap(), 1);
        assert_eq!(count(), 2);
        // unchanged again: cache hit
        assert_eq!(cached.load("foo.example.com", counting_build).unwrap(), 1);
        assert_eq!(count(), 2);
        // a CA renewal bumps the trust bundle: rebuild (token 2)
        set_mtime(&trusted, t0 + Duration::from_secs(120));
        assert_eq!(cached.load("foo.example.com", counting_build).unwrap(), 2);
        assert_eq!(count(), 3);
        // a key rotation bumps the key: rebuild (token 3)
        set_mtime(&key, t0 + Duration::from_secs(180));
        assert_eq!(cached.load("foo.example.com", counting_build).unwrap(), 3);
        assert_eq!(count(), 4);
    }
}
