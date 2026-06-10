//! Small CA management. Just enough to stand up a local CA, generate
//! private keys + CSRs with SubjectAltNames, and sign CSRs — a typed
//! replacement for the hand-rolled shell scripts in `cfg/tls/*/gen.sh`.
//!
//! The cert profiles match those scripts: SHA-512 signatures, RSA
//! 4096-bit keys, CA cert with `basicConstraints=CA:TRUE,critical` and
//! `keyUsage=cRLSign,digitalSignature,keyCertSign`; leaf cert with
//! `basicConstraints=CA:FALSE,critical` and
//! `keyUsage=nonRepudiation,digitalSignature,keyEncipherment`. Both
//! carry the SAN extension and Subject/Authority Key Identifier
//! extensions so the resulting certs validate through netidx's
//! existing `tls::load_certs` / `tls::get_names`.
//!
//! Deferred to design/netidx-conf-future.md: CRL generation,
//! revocation, encrypted CA
//! keys, hardware-token backing, `Ca::trust_into_*` config-wiring
//! helpers, CA inventory / index files, the `--tls-auto` template
//! integration.

use crate::atomic;
use anyhow::{Context, Result};
use openssl::{
    asn1::Asn1Time,
    bn::BigNum,
    hash::MessageDigest,
    pkey::{Id, PKey, Private, Public},
    rsa::Rsa,
    symm::Cipher,
    x509::{
        X509, X509Builder, X509NameBuilder, X509Req, X509ReqBuilder,
        extension::{
            AuthorityKeyIdentifier, BasicConstraints, KeyUsage,
            SubjectAlternativeName, SubjectKeyIdentifier,
        },
    },
};
use parking_lot::Mutex;
use std::{
    net::IpAddr,
    path::{Path, PathBuf},
};

/// A DistinguishedName-ish subject. CN is required; the rest are
/// optional and only emitted if `Some`.
#[derive(Debug, Clone)]
pub struct Subject {
    pub common_name: String,
    pub country: Option<String>,
    pub state: Option<String>,
    pub locality: Option<String>,
    pub organization: Option<String>,
}

impl Subject {
    pub fn cn<S: Into<String>>(cn: S) -> Self {
        Self {
            common_name: cn.into(),
            country: None,
            state: None,
            locality: None,
            organization: None,
        }
    }
}

/// One SubjectAltName entry. CSR generation and cert signing both
/// take a slice of these.
#[derive(Debug, Clone)]
pub enum SanEntry {
    Dns(String),
    Ip(IpAddr),
    Uri(String),
    Email(String),
}

/// Default key size — matches the shell scripts.
pub const DEFAULT_KEY_BITS: u32 = 4096;
/// Minimum RSA key size accepted by this module. RSA below 2048 is
/// broken; NIST deprecates 2048 itself after 2030 in favor of 3072+.
/// Operators who really want a weaker key should pre-generate it with
/// `openssl` directly.
pub const MIN_KEY_BITS: u32 = 2048;
/// Default CA validity — matches the shell scripts (20 years).
pub const DEFAULT_CA_VALIDITY_DAYS: u32 = 7300;
/// Default leaf validity — matches the shell scripts (2 years).
pub const DEFAULT_LEAF_VALIDITY_DAYS: u32 = 730;

fn check_key_bits(bits: u32) -> Result<()> {
    if bits < MIN_KEY_BITS {
        bail!("RSA key size {bits} is below MIN_KEY_BITS ({MIN_KEY_BITS})");
    }
    Ok(())
}

/// Algorithm-aware strength check for an externally-supplied CSR public
/// key. RSA must be ≥ [`MIN_KEY_BITS`]; elliptic-curve keys (P-256 and
/// up) and the Edwards curves are fixed-strength and accepted; anything
/// else is refused. A flat bit-count minimum would wrongly reject a
/// perfectly strong 256-bit EC key, so the CA server's ECDSA join
/// clients need this rather than [`check_key_bits`] alone.
fn check_pubkey_strength(pubkey: &PKey<Public>) -> Result<()> {
    match pubkey.id() {
        Id::RSA => check_key_bits(pubkey.bits()),
        Id::EC => {
            let bits = pubkey.bits();
            if bits < 256 {
                bail!("EC key size {bits} is too small (need P-256 or stronger)");
            }
            Ok(())
        }
        Id::ED25519 | Id::ED448 => Ok(()),
        other => bail!("unsupported CSR key algorithm: {other:?}"),
    }
}

/// Verify a SAN list will satisfy `netidx::tls::get_names`: exactly
/// one `SanEntry::Dns` entry, that entry non-empty; any number of
/// non-DNS entries. The netidx TLS validator filters general-names
/// down to `DNSName` and rejects 0 or 2+ — so a cert with two DNS
/// SANs would silently fail at config-load time even though openssl
/// would happily sign it.
///
/// Wildcards (`*.example.com`) and Punycode (`xn--…`) are accepted
/// here without further inspection: openssl signs them fine, and
/// netidx's reverse-domain matching can use either with the
/// operator's understanding. If you want stricter shape checks, do
/// them before calling.
///
/// Called from the "stand up a netidx identity" path (`Ca::issue`),
/// **not** from the lower-level `sign_request` — operators who want
/// a non-netidx cert (e.g. a multi-DNS web server cert) can drive
/// the latter directly.
fn check_san_for_netidx(san: &[SanEntry]) -> Result<()> {
    let mut dns_entries = san.iter().filter_map(|s| match s {
        SanEntry::Dns(d) => Some(d.as_str()),
        _ => None,
    });
    let only = match (dns_entries.next(), dns_entries.next()) {
        (None, _) => {
            bail!(
                "netidx requires exactly one DNS SAN entry on the leaf cert, found 0; \
                 use Ca::sign_request directly if you need a non-netidx SAN composition",
            )
        }
        (Some(_), Some(_)) => {
            bail!(
                "netidx requires exactly one DNS SAN entry on the leaf cert, found 2+; \
                 use Ca::sign_request directly if you need a non-netidx SAN composition",
            )
        }
        (Some(d), None) => d,
    };
    if only.is_empty() {
        bail!("DNS SAN value must not be empty");
    }
    Ok(())
}

/// Parameters for `Ca::init`.
#[derive(Debug, Clone)]
pub struct CaParams {
    /// Where to drop `private.key` (mode 0600) and
    /// `certificate.pem` (mode 0644).
    pub directory: PathBuf,
    pub subject: Subject,
    /// SAN on the CA cert. Defaults to a single DNS:`common_name`
    /// when empty (matching the shell scripts).
    pub san: Vec<SanEntry>,
    pub key_bits: u32,
    pub validity_days: u32,
}

/// A PEM-encoded private key and the matching CSR.
#[derive(Debug, Clone)]
pub struct KeyAndRequest {
    pub private_key_pem: Vec<u8>,
    pub csr_pem: Vec<u8>,
}

/// Parameters for `Ca::issue` — the one-shot "stand up a host
/// identity" path. Generates a fresh keypair, builds a CSR, signs it,
/// writes both files to `out_dir` with correct unix modes.
#[derive(Debug, Clone)]
pub struct IssueParams {
    pub subject: Subject,
    pub san: Vec<SanEntry>,
    pub key_bits: u32,
    pub validity_days: u32,
    pub out_dir: PathBuf,
    /// Encrypt the on-disk private key with this passphrase. `None`
    /// writes an unencrypted PKCS#8 key; `Some(s)` writes a PKCS#8
    /// key encrypted with AES-256-CBC. Same semantics as the
    /// password parameter on `Ca::init`. Callers that want to
    /// support encrypted leaf keys also need to plumb the password
    /// somewhere a netidx process can find it (e.g. the system
    /// keychain via `netidx::tls::save_password_for_key`).
    pub password: Option<String>,
}

/// Output of `Ca::issue`.
#[derive(Debug, Clone)]
pub struct IssuedFiles {
    pub directory: PathBuf,
    pub private_key: PathBuf,
    pub certificate: PathBuf,
}

/// A loaded local certificate authority.
///
/// The `serial_lock` serializes concurrent `issue` / `sign_request`
/// calls so that the on-disk serial counter is never read-modified-
/// written by two threads in parallel (which would silently emit two
/// certificates with the same X.509 serial, a CA contract violation).
#[derive(Debug)]
pub struct Ca {
    directory: PathBuf,
    cert: X509,
    pkey: PKey<Private>,
    serial_lock: Mutex<()>,
}

impl Ca {
    /// Create a brand-new CA: generate the private key, self-sign the
    /// root certificate, write `private.key` (0600) + `certificate.pem`
    /// (0644) into `params.directory`. Refuses if a key/cert already
    /// exist there; call `Ca::open` for that case.
    ///
    /// `password` controls encryption of the on-disk private key:
    /// `None` writes an unencrypted PKCS#8 key; `Some(s)` writes a
    /// PKCS#8 EncryptedPrivateKeyInfo (AES-256-CBC) using `s` as the
    /// passphrase, *including the empty string* — the engine never
    /// silently demotes `Some("")` to `None`. CLI front-ends that ask
    /// for a password and want "skip = no encryption" should pass
    /// `None` when the user enters nothing, not `Some("")`.
    pub fn init(params: &CaParams, password: Option<&str>) -> Result<Self> {
        let (key_path, cert_path) = Self::prepare_dir(params)?;
        let (cert, pkey) = Self::generate(params)?;
        let key_pem = match password {
            Some(p) => pkey
                .private_key_to_pem_pkcs8_passphrase(Cipher::aes_256_cbc(), p.as_bytes())
                .context("encrypting CA private key")?,
            None => pkey.private_key_to_pem_pkcs8().context("encoding CA private key")?,
        };
        let cert_pem = cert.to_pem().context("encoding CA cert")?;
        atomic::write_atomic(&key_path, &key_pem, 0o600)?;
        atomic::write_atomic(&cert_path, &cert_pem, 0o644)?;
        write_next_serial(&params.directory, 2)?;
        Ok(Self {
            directory: params.directory.clone(),
            cert,
            pkey,
            serial_lock: Mutex::new(()),
        })
    }

    /// Create a CA whose key goes into a [`crate::ca_vault`] rather than
    /// an on-disk `private.key`: writes `certificate.pem` + the serial
    /// counter, and **returns the unencrypted PKCS#8 key PEM** for the
    /// caller to seal into the vault. The key never touches disk in
    /// plaintext — that's the whole point of the vault — so callers must
    /// vault it promptly and let the `Zeroizing` wrapper wipe it.
    pub fn init_vaulted(
        params: &CaParams,
    ) -> Result<(Self, zeroize::Zeroizing<Vec<u8>>)> {
        let (_key_path, cert_path) = Self::prepare_dir(params)?;
        let (cert, pkey) = Self::generate(params)?;
        let key_pem = pkey.private_key_to_pem_pkcs8().context("encoding CA private key")?;
        let cert_pem = cert.to_pem().context("encoding CA cert")?;
        atomic::write_atomic(&cert_path, &cert_pem, 0o644)?;
        write_next_serial(&params.directory, 2)?;
        let ca = Self {
            directory: params.directory.clone(),
            cert,
            pkey,
            serial_lock: Mutex::new(()),
        };
        Ok((ca, zeroize::Zeroizing::new(key_pem)))
    }

    /// Create the CA directory and refuse if a CA (key, cert, or vault)
    /// already lives there. Returns the `(key, cert)` paths.
    fn prepare_dir(params: &CaParams) -> Result<(PathBuf, PathBuf)> {
        std::fs::create_dir_all(&params.directory)
            .with_context(|| format!("creating CA directory {:?}", params.directory))?;
        let key_path = params.directory.join("private.key");
        let cert_path = params.directory.join("certificate.pem");
        let vault_path = params.directory.join(crate::ca_vault::VAULT_FILE);
        if key_path.exists() || cert_path.exists() || vault_path.exists() {
            bail!(
                "CA appears to already exist at {:?}; use Ca::open instead",
                params.directory
            );
        }
        Ok((key_path, cert_path))
    }

    /// Generate the CA keypair + self-signed cert in memory — no disk
    /// writes, so callers choose how to persist the key.
    fn generate(params: &CaParams) -> Result<(X509, PKey<Private>)> {
        check_key_bits(params.key_bits)?;
        let rsa = Rsa::generate(params.key_bits)
            .context("generating CA RSA key")?;
        let pkey = PKey::from_rsa(rsa).context("wrapping CA key")?;

        let name = build_name(&params.subject)?;
        let mut cert = X509Builder::new().context("X509Builder::new")?;
        cert.set_version(2).context("set X509 version")?;
        let serial = BigNum::from_u32(1)?.to_asn1_integer()?;
        cert.set_serial_number(&serial)?;
        cert.set_subject_name(&name)?;
        // Self-signed: issuer == subject.
        cert.set_issuer_name(&name)?;
        cert.set_pubkey(&pkey)?;
        let not_before = Asn1Time::days_from_now(0)?;
        let not_after = Asn1Time::days_from_now(params.validity_days)?;
        cert.set_not_before(&not_before)?;
        cert.set_not_after(&not_after)?;

        let ctx = cert.x509v3_context(None, None);
        let bc = BasicConstraints::new().critical().ca().build()?;
        let ku = KeyUsage::new()
            .critical()
            .crl_sign()
            .digital_signature()
            .key_cert_sign()
            .build()?;
        let ski = SubjectKeyIdentifier::new().build(&ctx)?;
        // AKI is intentionally omitted on the self-signed CA cert.
        // The openssl-cli shell scripts (cfg/tls/ca/gen.sh) emit
        // `authorityKeyIdentifier=keyid:always,issuer:always` here
        // because the cli has access to "the cert currently being
        // constructed" in its v3 context. The openssl-rs
        // `X509v3Context` can only reference *existing* X509s, so
        // there's no clean way to build AKI without round-tripping
        // through a temporary cert. RFC 5280 §4.2.1.1 explicitly
        // permits omitting AKI on self-signed CA certs, and netidx's
        // TLS validators (`tls::load_certs` / `tls::get_names`)
        // don't inspect AKI on trust anchors. Leaf certs we sign
        // below DO carry AKI.
        let san_entries = if params.san.is_empty() {
            vec![SanEntry::Dns(params.subject.common_name.clone())]
        } else {
            params.san.clone()
        };
        let san = build_san(&san_entries, &ctx)?;
        drop(ctx);
        cert.append_extension(bc)?;
        cert.append_extension(ku)?;
        cert.append_extension(ski)?;
        cert.append_extension(san)?;
        cert.sign(&pkey, MessageDigest::sha512()).context("self-signing CA")?;
        let cert = cert.build();
        Ok((cert, pkey))
    }

    /// Load an existing CA from `directory/private.key` +
    /// `directory/certificate.pem`. The serial counter file
    /// (`directory/serial`) is created on first issuance if absent.
    ///
    /// `password` is required if and only if the on-disk key is
    /// PKCS#8-encrypted. `None` selects the unencrypted path; `Some(s)`
    /// selects the encrypted path with `s` as the passphrase
    /// (including the empty string, which is a real — if pointless —
    /// passphrase). Mismatch (encrypted key + None, or unencrypted key
    /// + Some) returns a clear error.
    pub fn open<P: AsRef<Path>>(
        directory: P,
        password: Option<&str>,
    ) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        let key_path = directory.join("private.key");
        let cert_path = directory.join("certificate.pem");
        let key_pem = std::fs::read(&key_path)
            .with_context(|| format!("reading {:?}", key_path))?;
        let cert_pem = std::fs::read(&cert_path)
            .with_context(|| format!("reading {:?}", cert_path))?;
        let pkey = parse_key_pem(&key_pem, password)
            .with_context(|| format!("parsing {:?}", key_path))?;
        let cert = X509::from_pem(&cert_pem)
            .with_context(|| format!("parsing {:?}", cert_path))?;
        Ok(Self { directory, cert, pkey, serial_lock: Mutex::new(()) })
    }

    /// Build a CA from an in-memory **unencrypted** PKCS#8 key PEM and
    /// cert PEM, with `directory` providing the serial counter. Used by
    /// the CA server: [`crate::ca_vault::unlock`] hands back the
    /// decrypted key per request, and this turns it into a transient
    /// signer without the key ever touching disk in plaintext.
    pub fn from_pem(
        directory: PathBuf,
        key_pem: &[u8],
        cert_pem: &[u8],
    ) -> Result<Self> {
        let pkey = parse_key_pem(key_pem, None).context("parsing CA key PEM")?;
        let cert = X509::from_pem(cert_pem).context("parsing CA cert PEM")?;
        Ok(Self { directory, cert, pkey, serial_lock: Mutex::new(()) })
    }

    /// The CA cert in PEM form (the trust anchor consumers pin).
    pub fn certificate_pem(&self) -> Result<Vec<u8>> {
        Ok(self.cert.to_pem()?)
    }

    /// Where this CA lives on disk.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Sign a CSR. Returns the signed leaf certificate as PEM bytes.
    /// The `san` argument overrides whatever the CSR claims — the CA
    /// is the sole authority on SAN content.
    pub fn sign_request(
        &self,
        csr_pem: &[u8],
        san: &[SanEntry],
        validity_days: u32,
    ) -> Result<Vec<u8>> {
        let req = X509Req::from_pem(csr_pem).context("parsing CSR")?;
        // Verify the CSR was signed by the key inside it (proof of
        // private-key possession).
        let req_pubkey = req.public_key().context("CSR public key")?;
        if !req.verify(&req_pubkey).context("verifying CSR signature")? {
            bail!("CSR signature does not match its embedded public key");
        }
        // Enforce key strength on externally-supplied CSRs too. The
        // entry-point check in `generate_csr` / `init` / `issue` guards
        // keys we generate (always RSA); this guards keys submitted to
        // us — notably by the CA server's join clients, which build
        // their CSRs with rcgen whose only practical keygen is ECDSA.
        // So the check is algorithm-aware: RSA ≥ MIN_KEY_BITS, EC
        // P-256+, Edwards curves accepted.
        check_pubkey_strength(&req_pubkey)?;

        let serial_n = {
            // Brief read-modify-write of the on-disk counter; held
            // long enough to keep two concurrent `issue` calls from
            // colliding on the same number.
            let _g = self.serial_lock.lock();
            next_serial(&self.directory)?
        };
        let mut cert = X509Builder::new()?;
        cert.set_version(2)?;
        let serial =
            BigNum::from_dec_str(&serial_n.to_string())?.to_asn1_integer()?;
        cert.set_serial_number(&serial)?;
        cert.set_subject_name(req.subject_name())?;
        cert.set_issuer_name(self.cert.subject_name())?;
        cert.set_pubkey(&req_pubkey)?;
        let not_before = Asn1Time::days_from_now(0)?;
        let not_after = Asn1Time::days_from_now(validity_days)?;
        cert.set_not_before(&not_before)?;
        cert.set_not_after(&not_after)?;

        let ctx = cert.x509v3_context(Some(&self.cert), None);
        let bc = BasicConstraints::new().critical().build()?; // CA:FALSE
        let ku = KeyUsage::new()
            .digital_signature()
            .non_repudiation()
            .key_encipherment()
            .build()?;
        let ski = SubjectKeyIdentifier::new().build(&ctx)?;
        let aki = AuthorityKeyIdentifier::new()
            .keyid(true)
            .issuer(true)
            .build(&ctx)?;
        let san_ext = build_san(san, &ctx)?;
        drop(ctx);
        cert.append_extension(bc)?;
        cert.append_extension(ku)?;
        cert.append_extension(ski)?;
        cert.append_extension(aki)?;
        cert.append_extension(san_ext)?;
        cert.sign(&self.pkey, MessageDigest::sha512()).context("signing leaf")?;
        let cert = cert.build();
        Ok(cert.to_pem()?)
    }

    /// One-shot: generate a fresh keypair + CSR, sign it, write
    /// `private.key` (0600) and `certificate.pem` (0644) into
    /// `params.out_dir`.
    pub fn issue(&self, params: &IssueParams) -> Result<IssuedFiles> {
        check_key_bits(params.key_bits)?;
        check_san_for_netidx(&params.san)?;
        std::fs::create_dir_all(&params.out_dir).with_context(|| {
            format!("creating identity directory {:?}", params.out_dir)
        })?;
        let kr = generate_csr(
            &params.subject,
            &params.san,
            params.key_bits,
            params.password.as_deref(),
        )?;
        let cert_pem = self.sign_request(
            &kr.csr_pem,
            &params.san,
            params.validity_days,
        )?;
        let key_path = params.out_dir.join("private.key");
        let cert_path = params.out_dir.join("certificate.pem");
        atomic::write_atomic(&key_path, &kr.private_key_pem, 0o600)?;
        atomic::write_atomic(&cert_path, &cert_pem, 0o644)?;
        Ok(IssuedFiles {
            directory: params.out_dir.clone(),
            private_key: key_path,
            certificate: cert_path,
        })
    }
}

/// Inspect a PEM-encoded CSR — the engine half of `netidx conf ca sign`'s
/// pre-flight confirmation. Returns the requested subject CN and the
/// embedded SAN list. The CA admin is expected to look at this before
/// signing, since `Ca::sign_request` deliberately ignores whatever SAN
/// the CSR claims (the CA is the sole authority on SAN content).
///
/// SAN extraction handles the common forms (DNS, IP, URI, email) plus
/// IPv4/IPv6 ipaddress fields. Other v3 extensions in the CSR are
/// ignored — only the SAN is decoded.
pub fn inspect_csr(csr_pem: &[u8]) -> Result<CsrSummary> {
    let req = X509Req::from_pem(csr_pem).context("parsing CSR")?;
    let pubkey = req.public_key().context("CSR public key")?;
    if !req.verify(&pubkey).context("verifying CSR signature")? {
        bail!("CSR signature does not match its embedded public key");
    }
    let mut cn: Option<String> = None;
    for entry in req.subject_name().entries_by_nid(openssl::nid::Nid::COMMONNAME) {
        if let Ok(s) = entry.data().as_utf8() {
            cn = Some(s.to_string());
            break;
        }
    }
    let mut san: Vec<SanEntry> = Vec::new();
    // The openssl crate exposes `subject_alt_names()` on X509 but not
    // on X509Req. Round-trip the CSR through a throwaway self-signed
    // X509 (its sole purpose is to expose the SAN extension via the
    // available API) and read SANs from that. The shim never leaves
    // this function.
    if let Some(names) = san_from_csr_via_shim(&req).ok().flatten() {
        for name in names.iter() {
            if let Some(d) = name.dnsname() {
                san.push(SanEntry::Dns(d.to_string()));
            } else if let Some(u) = name.uri() {
                san.push(SanEntry::Uri(u.to_string()));
            } else if let Some(e) = name.email() {
                san.push(SanEntry::Email(e.to_string()));
            } else if let Some(b) = name.ipaddress() {
                if let Some(ip) = parse_ip_octets(b) {
                    san.push(SanEntry::Ip(ip));
                }
            }
        }
    }
    Ok(CsrSummary {
        common_name: cn,
        key_bits: pubkey.bits(),
        san,
    })
}

/// Round-trip the CSR through a throwaway self-signed X509 so we can
/// use `subject_alt_names()`. Returns `None` when building the shim
/// fails for whatever reason — the caller treats absent SAN as "no
/// SAN reported," consistent with operators using `openssl req -text`
/// to verify.
fn san_from_csr_via_shim(
    req: &X509Req,
) -> Result<Option<openssl::stack::Stack<openssl::x509::GeneralName>>> {
    // Use the CSR's own public key to "sign" the shim — only the
    // structure matters here, the signature is never validated.
    // We can't actually sign with a public key, so generate a tiny
    // throwaway private key for the shim. The shim is dropped at
    // function end and never serialized.
    let throwaway_rsa = Rsa::generate(MIN_KEY_BITS).context("shim key")?;
    let throwaway = PKey::from_rsa(throwaway_rsa).context("shim wrap")?;
    let mut cert = X509Builder::new()?;
    cert.set_version(2)?;
    let serial = BigNum::from_u32(1)?.to_asn1_integer()?;
    cert.set_serial_number(&serial)?;
    cert.set_subject_name(req.subject_name())?;
    cert.set_issuer_name(req.subject_name())?;
    let pubkey = req.public_key()?;
    cert.set_pubkey(&pubkey)?;
    let nb = Asn1Time::days_from_now(0)?;
    let na = Asn1Time::days_from_now(1)?;
    cert.set_not_before(&nb)?;
    cert.set_not_after(&na)?;
    // Copy SAN extensions verbatim from the CSR into the shim cert.
    if let Ok(exts) = req.extensions() {
        for ext in exts.iter() {
            cert.append_extension2(ext)?;
        }
    }
    cert.sign(&throwaway, MessageDigest::sha512())?;
    let cert = cert.build();
    Ok(cert.subject_alt_names())
}

fn parse_ip_octets(b: &[u8]) -> Option<IpAddr> {
    match b.len() {
        4 => Some(IpAddr::V4(std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]))),
        16 => {
            let arr: [u8; 16] = b.try_into().ok()?;
            Some(IpAddr::V6(std::net::Ipv6Addr::from(arr)))
        }
        _ => None,
    }
}

/// What `inspect_csr` returns — the bits a CA admin wants to read
/// before deciding whether to sign.
#[derive(Debug, Clone)]
pub struct CsrSummary {
    pub common_name: Option<String>,
    pub key_bits: u32,
    pub san: Vec<SanEntry>,
}

/// Generate a private key + CSR with the given subject and SANs.
/// Useful when the CSR will be signed by a different CA than this
/// process owns (e.g. handed off to a remote conf-server). The CSR's
/// requested SAN is also embedded so a signing party can verify the
/// request before signing.
/// Generate a fresh RSA keypair and a matching CSR. The CSR is
/// always returned unencrypted (a CSR is a public document by
/// definition); `password` controls whether the *private key* is
/// returned as encrypted or unencrypted PKCS#8 PEM, with the same
/// semantics as `Ca::init`'s `password` parameter.
pub fn generate_csr(
    subject: &Subject,
    san: &[SanEntry],
    key_bits: u32,
    password: Option<&str>,
) -> Result<KeyAndRequest> {
    check_key_bits(key_bits)?;
    let rsa = Rsa::generate(key_bits).context("generating RSA key")?;
    let pkey = PKey::from_rsa(rsa).context("wrapping key")?;

    let mut req = X509ReqBuilder::new()?;
    req.set_version(0)?;
    let name = build_name(subject)?;
    req.set_subject_name(&name)?;
    req.set_pubkey(&pkey)?;

    if !san.is_empty() {
        let ctx = req.x509v3_context(None);
        let san_ext = build_san(san, &ctx)?;
        drop(ctx);
        let mut stack = openssl::stack::Stack::new()?;
        stack.push(san_ext)?;
        req.add_extensions(&stack)?;
    }

    req.sign(&pkey, MessageDigest::sha512()).context("signing CSR")?;
    let req = req.build();

    let private_key_pem = match password {
        Some(p) => pkey
            .private_key_to_pem_pkcs8_passphrase(
                Cipher::aes_256_cbc(),
                p.as_bytes(),
            )
            .context("encrypting private key")?,
        None => pkey
            .private_key_to_pem_pkcs8()
            .context("encoding private key")?,
    };

    Ok(KeyAndRequest { private_key_pem, csr_pem: req.to_pem()? })
}

fn build_name(s: &Subject) -> Result<openssl::x509::X509Name> {
    let mut name = X509NameBuilder::new()?;
    name.append_entry_by_text("CN", &s.common_name)?;
    if let Some(c) = &s.country {
        name.append_entry_by_text("C", c)?;
    }
    if let Some(st) = &s.state {
        name.append_entry_by_text("ST", st)?;
    }
    if let Some(l) = &s.locality {
        name.append_entry_by_text("L", l)?;
    }
    if let Some(o) = &s.organization {
        name.append_entry_by_text("O", o)?;
    }
    Ok(name.build())
}

fn build_san(
    san: &[SanEntry],
    ctx: &openssl::x509::X509v3Context<'_>,
) -> Result<openssl::x509::X509Extension> {
    let mut b = SubjectAlternativeName::new();
    for entry in san {
        match entry {
            SanEntry::Dns(s) => {
                b.dns(s);
            }
            SanEntry::Ip(ip) => {
                b.ip(&ip.to_string());
            }
            SanEntry::Uri(s) => {
                b.uri(s);
            }
            SanEntry::Email(s) => {
                b.email(s);
            }
        }
    }
    Ok(b.build(ctx)?)
}

/// Read a PEM-encoded private key, dispatching on whether the PEM is
/// encrypted (header `ENCRYPTED PRIVATE KEY`) and on whether a
/// password was supplied. Mismatch produces a clear error rather than
/// the cryptic openssl one.
fn parse_key_pem(pem: &[u8], password: Option<&str>) -> Result<PKey<Private>> {
    // Match the literal PEM header at a line boundary so we never
    // false-positive on the string appearing in a comment or in
    // unrelated PEM-armored data concatenated into the same file.
    const ENC_HEADER: &[u8] = b"-----BEGIN ENCRYPTED PRIVATE KEY-----";
    let encrypted = pem
        .split(|&b| b == b'\n')
        .any(|line| line.trim_ascii() == ENC_HEADER);
    match (encrypted, password) {
        (true, Some(pw)) => PKey::private_key_from_pem_passphrase(pem, pw.as_bytes())
            .map_err(|e| anyhow!("decrypting CA private key: {e}")),
        (true, None) => {
            bail!("CA private key is encrypted but no password was supplied")
        }
        (false, Some(_)) => {
            bail!("CA private key is not encrypted but a password was supplied")
        }
        (false, None) => PKey::private_key_from_pem(pem)
            .map_err(|e| anyhow!("parsing CA private key: {e}")),
    }
}

fn serial_path(dir: &Path) -> PathBuf {
    dir.join("serial")
}

fn next_serial(dir: &Path) -> Result<u64> {
    use fs3::FileExt;
    // Serialize the counter read-modify-write with an exclusive OS file
    // lock. The per-`Ca` `serial_lock` only covers one instance, but the
    // CA server builds a fresh `Ca` per request and a `ca issue` may run
    // alongside the daemon — without a shared (and inter-process) lock,
    // concurrent signers race the counter and mint duplicate X.509
    // serials. We lock a dedicated `serial.lock` rather than the serial
    // file itself, which `write_next_serial` replaces by atomic rename
    // (that would drop a lock held on the old inode). The lock releases
    // when `_lock` drops at end of scope (including on early return).
    let lock_path = dir.join("serial.lock");
    let _lock = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening serial lock {lock_path:?}"))?;
    _lock
        .lock_exclusive()
        .with_context(|| format!("locking serial counter {lock_path:?}"))?;
    let p = serial_path(dir);
    let cur = match std::fs::read_to_string(&p) {
        Ok(s) => s.trim().parse::<u64>().with_context(|| {
            format!("parsing serial counter at {p:?}")
        })?,
        // Serial file is missing: someone deleted it or the CA was
        // restored from a partial backup. Falling back to 2 here
        // would silently reuse a serial number that's already on a
        // previously-issued leaf. Instead, read the CA cert's serial
        // (issued certs use the next slot up) and start the counter
        // beyond whichever is higher: CA serial + 1, or any default.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let from_cert = ca_cert_serial(dir).unwrap_or(1);
            from_cert.saturating_add(1).max(2)
        }
        Err(e) => return Err(anyhow!("reading {p:?}: {e}")),
    };
    write_next_serial(dir, cur + 1)?;
    Ok(cur)
}

/// Read the CA cert's own X.509 serial number. Used to defensively
/// seed `next_serial` if the on-disk counter file is missing or
/// damaged. Returns `None` on any failure (the caller falls back to
/// the default).
fn ca_cert_serial(dir: &Path) -> Option<u64> {
    let cert_pem = std::fs::read(dir.join("certificate.pem")).ok()?;
    let cert = X509::from_pem(&cert_pem).ok()?;
    let bn = cert.serial_number().to_bn().ok()?;
    bn.to_dec_str().ok()?.parse::<u64>().ok()
}

fn write_next_serial(dir: &Path, next: u64) -> Result<()> {
    atomic::write_atomic(&serial_path(dir), next.to_string().as_bytes(), 0o644)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::stack::Stack;

    fn small_ca(dir: &Path) -> Ca {
        small_ca_pw(dir, None)
    }

    fn small_ca_pw(dir: &Path, password: Option<&str>) -> Ca {
        Ca::init(
            &CaParams {
                directory: dir.to_path_buf(),
                subject: Subject {
                    common_name: "test-ca".into(),
                    country: Some("US".into()),
                    state: None,
                    locality: None,
                    organization: Some("netidx-test".into()),
                },
                san: vec![SanEntry::Dns("test-ca".into())],
                key_bits: 2048, // smaller for test speed
                validity_days: 30,
            },
            password,
        )
        .unwrap()
    }

    /// Cross-check: `tls::extract_dns_san_from_pem` reads the SAN
    /// out of an openssl-issued cert — same direction the install
    /// tools rely on for deriving `our_name` from a user-supplied
    /// cert. If x509-parser and openssl ever disagree on SAN
    /// encoding the install tooling would silently pick the wrong
    /// name; this catches that.
    #[test]
    fn extract_dns_san_reads_openssl_issued_cert() {
        use crate::tls::extract_dns_san_from_pem;
        let dir = tempfile::tempdir().unwrap();
        let ca = small_ca(dir.path());
        let leaf_dir = tempfile::tempdir().unwrap();
        let issued = ca
            .issue(&IssueParams {
                subject: Subject::cn("ignored"),
                san: vec![SanEntry::Dns("alice.example.com".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: leaf_dir.path().to_path_buf(),
                password: None,
            })
            .unwrap();
        assert_eq!(
            extract_dns_san_from_pem(&issued.certificate).unwrap(),
            "alice.example.com",
        );
    }

    /// Cross-check: rustls-pemfile (the implementation in
    /// `tls::validate_pem_cert_file`) accepts the openssl-produced
    /// PEMs that `Ca::issue` and `Ca::init` generate. Catches any
    /// divergence in PEM dialect handling between the two libs.
    /// The shape-only tests (missing file, empty, garbage) live
    /// alongside the validator itself in `tls.rs` since they need
    /// no openssl-generated input.
    #[test]
    fn validate_pem_cert_file_accepts_openssl_certs() {
        use crate::tls::validate_pem_cert_file;
        let dir = tempfile::tempdir().unwrap();
        let ca = small_ca(dir.path());
        validate_pem_cert_file(&dir.path().join("certificate.pem")).unwrap();
        let leaf_dir = tempfile::tempdir().unwrap();
        let issued = ca
            .issue(&IssueParams {
                subject: Subject::cn("leaf"),
                san: vec![SanEntry::Dns("leaf".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: leaf_dir.path().to_path_buf(),
                password: None,
            })
            .unwrap();
        validate_pem_cert_file(&issued.certificate).unwrap();
    }

    #[test]
    fn init_and_open_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let ca = small_ca(dir.path());
        assert!(dir.path().join("private.key").exists());
        assert!(dir.path().join("certificate.pem").exists());
        assert!(dir.path().join("serial").exists());
        drop(ca);

        let reopened = Ca::open(dir.path(), None).unwrap();
        let pem = reopened.certificate_pem().unwrap();
        assert!(pem.starts_with(b"-----BEGIN CERTIFICATE-----"));
    }

    #[test]
    fn init_rejects_weak_key_bits() {
        let dir = tempfile::tempdir().unwrap();
        let err = Ca::init(
            &CaParams {
                directory: dir.path().to_path_buf(),
                subject: Subject::cn("weak"),
                san: vec![],
                key_bits: 1024, // below MIN_KEY_BITS
                validity_days: 30,
            },
            None,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("below MIN_KEY_BITS"));
    }

    #[test]
    fn generate_csr_rejects_weak_key_bits() {
        let err = generate_csr(&Subject::cn("h"), &[], 1024, None).unwrap_err();
        assert!(format!("{err:#}").contains("below MIN_KEY_BITS"));
    }

    #[test]
    fn issue_rejects_multi_dns_san() {
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = small_ca(ca_dir.path());
        let id_dir = tempfile::tempdir().unwrap();
        let err = ca
            .issue(&IssueParams {
                subject: Subject::cn("alice"),
                san: vec![
                    SanEntry::Dns("alice.example.com".into()),
                    SanEntry::Dns("alice-alt.example.com".into()),
                ],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_dir.path().to_path_buf(),
                password: None,
            })
            .unwrap_err();
        assert!(format!("{err:#}").contains("exactly one DNS SAN"));
    }

    #[test]
    fn issue_rejects_no_dns_san() {
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = small_ca(ca_dir.path());
        let id_dir = tempfile::tempdir().unwrap();
        let err = ca
            .issue(&IssueParams {
                subject: Subject::cn("alice"),
                san: vec![SanEntry::Ip("127.0.0.1".parse().unwrap())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_dir.path().to_path_buf(),
                password: None,
            })
            .unwrap_err();
        assert!(format!("{err:#}").contains("exactly one DNS SAN"));
    }

    #[test]
    fn init_refuses_existing_ca() {
        let dir = tempfile::tempdir().unwrap();
        let _ca = small_ca(dir.path());
        let err = Ca::init(
            &CaParams {
                directory: dir.path().to_path_buf(),
                subject: Subject::cn("test-ca"),
                san: vec![],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("already exist"));
    }

    #[test]
    fn encrypted_key_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        small_ca_pw(dir.path(), Some("hunter2"));

        // On-disk PEM is the ENCRYPTED PRIVATE KEY form.
        let pem = std::fs::read(dir.path().join("private.key")).unwrap();
        assert!(pem.starts_with(b"-----BEGIN ENCRYPTED PRIVATE KEY-----"));

        // Reopen with the right password — succeeds and the CA can sign.
        let ok = Ca::open(dir.path(), Some("hunter2")).unwrap();
        let leaf_pem = ok
            .sign_request(
                &generate_csr(
                    &Subject::cn("h.example.com"),
                    &[SanEntry::Dns("h.example.com".into())],
                    2048,
                    None,
                )
                .unwrap()
                .csr_pem,
                &[SanEntry::Dns("h.example.com".into())],
                30,
            )
            .unwrap();
        assert!(leaf_pem.starts_with(b"-----BEGIN CERTIFICATE-----"));

        // Wrong password — fails.
        let err = Ca::open(dir.path(), Some("nope")).unwrap_err();
        assert!(format!("{err:#}").to_lowercase().contains("decrypt"));

        // Encrypted key + no password — clear error.
        let err = Ca::open(dir.path(), None).unwrap_err();
        assert!(format!("{err:#}").contains("encrypted"));
    }

    #[test]
    fn unencrypted_key_rejects_password() {
        let dir = tempfile::tempdir().unwrap();
        small_ca(dir.path());
        let err = Ca::open(dir.path(), Some("oops")).unwrap_err();
        assert!(format!("{err:#}").contains("not encrypted"));
    }

    #[test]
    fn empty_string_is_a_real_passphrase() {
        // Some("") encrypts with the empty passphrase — pointless but
        // semantically distinct from None. The engine does not coerce.
        let dir = tempfile::tempdir().unwrap();
        small_ca_pw(dir.path(), Some(""));
        let pem = std::fs::read(dir.path().join("private.key")).unwrap();
        assert!(pem.starts_with(b"-----BEGIN ENCRYPTED PRIVATE KEY-----"));
        // Open with Some("") works.
        let _ = Ca::open(dir.path(), Some("")).unwrap();
        // Open with None fails because the key is encrypted.
        let err = Ca::open(dir.path(), None).unwrap_err();
        assert!(format!("{err:#}").contains("encrypted"));
    }

    #[test]
    fn ca_cert_has_ca_extensions() {
        let dir = tempfile::tempdir().unwrap();
        let ca = small_ca(dir.path());
        let pem = ca.certificate_pem().unwrap();
        let cert = X509::from_pem(&pem).unwrap();
        // Self-signed: issuer == subject.
        let subj = cert.subject_name();
        let issuer = cert.issuer_name();
        let subj_cn = subj
            .entries_by_nid(openssl::nid::Nid::COMMONNAME)
            .next()
            .unwrap()
            .data()
            .as_utf8()
            .unwrap()
            .to_string();
        let iss_cn = issuer
            .entries_by_nid(openssl::nid::Nid::COMMONNAME)
            .next()
            .unwrap()
            .data()
            .as_utf8()
            .unwrap()
            .to_string();
        assert_eq!(subj_cn, "test-ca");
        assert_eq!(iss_cn, "test-ca");
    }

    #[test]
    fn inspect_csr_round_trip() {
        let kr = generate_csr(
            &Subject::cn("alice.example.com"),
            &[
                SanEntry::Dns("alice.example.com".into()),
                SanEntry::Ip("10.0.0.1".parse().unwrap()),
            ],
            2048,
            None,
        )
        .unwrap();
        let summary = inspect_csr(&kr.csr_pem).unwrap();
        assert_eq!(summary.common_name.as_deref(), Some("alice.example.com"));
        assert_eq!(summary.key_bits, 2048);
        // SAN order isn't guaranteed; assert membership.
        assert!(
            summary
                .san
                .iter()
                .any(|s| matches!(s, SanEntry::Dns(d) if d == "alice.example.com")),
            "missing DNS SAN: {:?}",
            summary.san,
        );
        assert!(
            summary.san.iter().any(|s| matches!(
                s,
                SanEntry::Ip(ip) if ip == &"10.0.0.1".parse::<IpAddr>().unwrap()
            )),
            "missing IP SAN: {:?}",
            summary.san,
        );
    }

    #[test]
    fn inspect_csr_rejects_tampered_signature() {
        let kr = generate_csr(
            &Subject::cn("alice.example.com"),
            &[SanEntry::Dns("alice.example.com".into())],
            2048,
            None,
        )
        .unwrap();
        // Flip a byte in the middle of the PEM body — base64 changes
        // ripple into either an unparseable CSR or a verify failure;
        // both should bubble out as an error.
        let mut bad = kr.csr_pem.clone();
        let idx = bad.len() / 2;
        bad[idx] = bad[idx].wrapping_add(1);
        assert!(inspect_csr(&bad).is_err());
    }

    #[test]
    fn generate_csr_produces_parseable_csr() {
        let kr = generate_csr(
            &Subject::cn("host.example.com"),
            &[SanEntry::Dns("host.example.com".into())],
            2048,
            None,
        )
        .unwrap();
        assert!(kr.private_key_pem.starts_with(b"-----BEGIN PRIVATE KEY-----"));
        assert!(kr.csr_pem.starts_with(b"-----BEGIN CERTIFICATE REQUEST-----"));
        let req = X509Req::from_pem(&kr.csr_pem).unwrap();
        let pubkey = req.public_key().unwrap();
        assert!(req.verify(&pubkey).unwrap());
    }

    /// Round-trip an encrypted-leaf-key CSR: generating with
    /// `password = Some(p)` must produce an ENCRYPTED PRIVATE KEY
    /// PEM block, and parsing it back with the matching password
    /// (via `parse_key_pem`) must succeed. The CSR signature is
    /// independent of key encryption — verify both halves.
    #[test]
    fn generate_csr_with_password_encrypts_key() {
        let kr = generate_csr(
            &Subject::cn("host.example.com"),
            &[SanEntry::Dns("host.example.com".into())],
            2048,
            Some("hunter2"),
        )
        .unwrap();
        // PKCS#8 + AES marker — same shape `Ca::init` writes for
        // an encrypted CA key.
        assert!(
            kr.private_key_pem.starts_with(b"-----BEGIN ENCRYPTED PRIVATE KEY-----"),
            "encrypted key PEM header missing: {:?}",
            std::str::from_utf8(&kr.private_key_pem[..40]).unwrap_or("?"),
        );
        // Decrypt round-trip — the existing `parse_key_pem` is the
        // same code path netidx::tls::load_private_key takes for
        // encrypted keys at runtime, so this also exercises the
        // wire-format compatibility.
        assert!(parse_key_pem(&kr.private_key_pem, Some("hunter2")).is_ok());
        // Wrong password fails.
        assert!(parse_key_pem(&kr.private_key_pem, Some("nope")).is_err());
        // No password fails with the "encrypted but no password" error.
        let err = parse_key_pem(&kr.private_key_pem, None).unwrap_err();
        assert!(format!("{err:#}").contains("encrypted"));
        // CSR itself is still a valid CSR even with an encrypted key.
        let req = X509Req::from_pem(&kr.csr_pem).unwrap();
        let pubkey = req.public_key().unwrap();
        assert!(req.verify(&pubkey).unwrap());
    }

    #[test]
    fn sign_request_produces_valid_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let ca = small_ca(dir.path());
        let kr = generate_csr(
            &Subject::cn("host.example.com"),
            &[SanEntry::Dns("host.example.com".into())],
            2048,
            None,
        )
        .unwrap();
        let leaf_pem = ca
            .sign_request(
                &kr.csr_pem,
                &[SanEntry::Dns("host.example.com".into())],
                30,
            )
            .unwrap();
        let leaf = X509::from_pem(&leaf_pem).unwrap();

        // Verify the leaf was signed by the CA.
        let ca_cert = X509::from_pem(&ca.certificate_pem().unwrap()).unwrap();
        let ca_pubkey = ca_cert.public_key().unwrap();
        assert!(leaf.verify(&ca_pubkey).unwrap());

        // Verify it via an X509Store (the realistic path).
        let mut builder = openssl::x509::store::X509StoreBuilder::new().unwrap();
        builder.add_cert(ca_cert).unwrap();
        let store = builder.build();
        let chain: Stack<X509> = Stack::new().unwrap();
        let mut ctx = openssl::x509::X509StoreContext::new().unwrap();
        let ok = ctx
            .init(&store, &leaf, &chain, |c| c.verify_cert())
            .unwrap();
        assert!(ok, "leaf should verify against the CA store");
    }

    #[test]
    fn issue_round_trip() {
        let ca_dir = tempfile::tempdir().unwrap();
        let id_dir = tempfile::tempdir().unwrap();
        let ca = small_ca(ca_dir.path());

        let out = ca
            .issue(&IssueParams {
                subject: Subject::cn("alice.example.com"),
                san: vec![
                    SanEntry::Dns("alice.example.com".into()),
                    SanEntry::Ip("127.0.0.1".parse().unwrap()),
                ],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_dir.path().to_path_buf(),
                password: None,
            })
            .unwrap();

        assert!(out.private_key.exists());
        assert!(out.certificate.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| {
                std::fs::metadata(p).unwrap().permissions().mode() & 0o777
            };
            assert_eq!(mode(&out.private_key), 0o600);
            assert_eq!(mode(&out.certificate), 0o644);
        }

        // Issued cert validates against the CA.
        let leaf =
            X509::from_pem(&std::fs::read(&out.certificate).unwrap()).unwrap();
        let ca_cert =
            X509::from_pem(&std::fs::read(ca_dir.path().join("certificate.pem"))
                .unwrap())
            .unwrap();
        assert!(leaf.verify(&ca_cert.public_key().unwrap()).unwrap());
    }

    #[test]
    fn serials_are_monotonic() {
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = small_ca(ca_dir.path());

        let mut seen = std::collections::HashSet::new();
        for i in 0..3 {
            let id_dir = tempfile::tempdir().unwrap();
            let out = ca
                .issue(&IssueParams {
                    subject: Subject::cn(format!("host-{i}.example.com")),
                    san: vec![SanEntry::Dns(format!("host-{i}.example.com"))],
                    key_bits: 2048,
                    validity_days: 30,
                    out_dir: id_dir.path().to_path_buf(),
                    password: None,
                })
                .unwrap();
            let leaf =
                X509::from_pem(&std::fs::read(&out.certificate).unwrap()).unwrap();
            let serial = leaf.serial_number().to_bn().unwrap().to_dec_str().unwrap();
            assert!(seen.insert(serial.to_string()), "serial reused: {serial}");
        }
    }
}
