//! Minimal TLS identity file management.
//!
//! v1 has **no** CA, **no** cert issuance — just file shuttling. An
//! operator who already has a cert/key/trusted-CA bundle (from their
//! corporate PKI or from the `cfg/tls/*/gen.sh` shell scripts in this
//! repo) calls [`install_identity`] to copy the three files into the
//! canonical `~/.config/netidx/tls/<cn>/` layout with correct modes.
//! The templates then reference the installed paths from the generated
//! configs.
//!
//! Issuance, signing, CRLs, and the `--tls-auto` flow are
//! design/netidx-conf-future.md.

use crate::{atomic, paths};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// What an installed identity looks like on disk.
#[derive(Debug, Clone)]
pub struct InstalledIdentity {
    pub cn: String,
    pub directory: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub trusted: PathBuf,
}

/// Inputs for [`install_identity`].
#[derive(Debug, Clone)]
pub struct InstallIdentity<'a> {
    /// Common name. Used as the identity's display name and (with
    /// [`install_identity_for_user`]) as the subdirectory under
    /// `~/.config/netidx/tls/`.
    pub cn: &'a str,
    /// Directory to copy into. With [`install_identity_for_user`] this
    /// is computed automatically; with [`install_identity`] callers
    /// supply it explicitly.
    pub dest_dir: &'a Path,
    pub certificate_src: &'a Path,
    pub private_key_src: &'a Path,
    pub trusted_src: &'a Path,
}

/// Copy a (cert, key, trusted-CA) triple into `dest_dir`, atomically,
/// with correct unix modes (0600 on the private key, 0644 on cert and
/// CA). Returns the installed paths.
///
/// Idempotent: re-installing with identical inputs is a no-op of the
/// "writes the same bytes back" kind. Installing with different inputs
/// overwrites.
pub fn install_identity(p: &InstallIdentity<'_>) -> Result<InstalledIdentity> {
    ensure_valid_cn(p.cn)?;

    let dir = p.dest_dir.to_path_buf();
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating tls dir {dir:?}"))?;

    let cert_bytes = std::fs::read(p.certificate_src).with_context(|| {
        format!("reading certificate source {:?}", p.certificate_src)
    })?;
    let key_bytes = std::fs::read(p.private_key_src).with_context(|| {
        format!("reading private key source {:?}", p.private_key_src)
    })?;
    let ca_bytes = std::fs::read(p.trusted_src)
        .with_context(|| format!("reading trusted CA source {:?}", p.trusted_src))?;

    let [cert_dst, key_dst, ca_dst] = installed_files_in(&dir);

    atomic::write_atomic(&cert_dst, &cert_bytes, 0o644)?;
    atomic::write_atomic(&key_dst, &key_bytes, 0o600)?;
    atomic::write_atomic(&ca_dst, &ca_bytes, 0o644)?;
    // A TPM-sealed password sidecar (`<key>.tpm`, see
    // `netidx::tls::sealed_password_path`) travels with its key:
    // without it an encrypted key is undecryptable, so installing one
    // and not the other would produce an identity that looks installed
    // but can't load. Symmetrically, a key with no sidecar must clear
    // any stale one at the destination — at load time a leftover
    // sidecar is authoritative and would shadow the new key's real
    // password source.
    let tpm_src = sealed_sidecar(p.private_key_src);
    let tpm_dst = sealed_sidecar(&key_dst);
    if tpm_src.exists() {
        if tpm_src != tpm_dst {
            let blob = std::fs::read(&tpm_src)
                .with_context(|| format!("reading sealed password {tpm_src:?}"))?;
            atomic::write_atomic(&tpm_dst, &blob, 0o600)?;
        }
    } else if tpm_dst.exists() {
        std::fs::remove_file(&tpm_dst)
            .with_context(|| format!("removing stale sealed password {tpm_dst:?}"))?;
    }

    Ok(InstalledIdentity {
        cn: p.cn.to_string(),
        directory: dir,
        certificate: cert_dst,
        private_key: key_dst,
        trusted: ca_dst,
    })
}

/// The TPM-sealed password sidecar beside `key` — the same path
/// `netidx::tls::load_key_password` consults at load time.
pub fn sealed_sidecar(key: &Path) -> PathBuf {
    let mut s = key.as_os_str().to_os_string();
    s.push(".tpm");
    PathBuf::from(s)
}

/// Protect a freshly issued plaintext PKCS#8 key with a TPM-sealed
/// random password: encrypt the key (PBES2, pure Rust) and seal the
/// password. Returns `(encrypted_key_pem, sealed_password_blob)` —
/// write the blob to [`sealed_sidecar`] beside wherever the key lands.
/// Fails when no TPM is usable; callers decide the fallback.
pub fn seal_private_key(plain_pem: &str) -> Result<(String, Vec<u8>)> {
    let password = netidx_tpm::random_secret();
    let blob = netidx_tpm::seal(password.as_bytes())?;
    let encrypted = netidx::tls::encrypt_private_key(plain_pem, &password)
        .context("encrypting the private key under the sealed password")?;
    Ok((encrypted, blob))
}

/// How [`write_private_key_maybe_sealed`] protected the key.
pub enum KeyWrite {
    /// Encrypted, password sealed to this machine's TPM.
    Sealed,
    /// Written plaintext; the error says why sealing wasn't possible.
    Plain(anyhow::Error),
}

/// Write a daemon's private key at `path`, TPM-sealed when the host
/// has a usable TPM, plaintext otherwise — daemons can't type
/// passwords, so for them the choice is seal-or-nothing and setup must
/// not dead-end on a missing TPM. Callers print what happened from the
/// returned [`KeyWrite`].
pub fn write_private_key_maybe_sealed(path: &Path, plain_pem: &str) -> Result<KeyWrite> {
    match seal_private_key(plain_pem) {
        Ok((enc, blob)) => {
            atomic::write_atomic(path, enc.as_bytes(), 0o600)?;
            atomic::write_atomic(&sealed_sidecar(path), &blob, 0o600)?;
            Ok(KeyWrite::Sealed)
        }
        Err(e) => {
            atomic::write_atomic(path, plain_pem.as_bytes(), 0o600)?;
            // A stale sidecar beside a plaintext key would shadow it at
            // load time with a password that decrypts nothing.
            let sidecar = sealed_sidecar(path);
            if sidecar.exists() {
                std::fs::remove_file(&sidecar)
                    .with_context(|| format!("removing stale sidecar {sidecar:?}"))?;
            }
            Ok(KeyWrite::Plain(e))
        }
    }
}

/// The three on-disk files [`install_identity`] writes into
/// `dest_dir`, in the order `(certificate, private_key, trusted)`. The
/// single source of truth for these filenames — used by pre-write
/// existence checks (e.g. --force gating) and by the template layer
/// when emitting the `trusted`/`certificate`/`private_key` paths into
/// generated configs, so the install location and the config always
/// agree.
pub fn installed_files_in(dest_dir: &Path) -> [PathBuf; 3] {
    [
        dest_dir.join("certificate.pem"),
        dest_dir.join("private.key"),
        dest_dir.join("trusted.pem"),
    ]
}

/// Where this identity lives by convention: `${user_tls_dir}/<cn>`.
pub fn identity_dir(cn: &str) -> Result<PathBuf> {
    ensure_valid_cn(cn)?;
    let mut p = paths::user_tls_dir()?;
    p.push(cn);
    Ok(p)
}

/// Convenience: same as [`install_identity`] but resolves the
/// destination directory automatically to `${user_tls_dir}/<cn>/`.
pub fn install_identity_for_user(
    cn: &str,
    certificate_src: &Path,
    private_key_src: &Path,
    trusted_src: &Path,
) -> Result<InstalledIdentity> {
    let dest_dir = identity_dir(cn)?;
    install_identity(&InstallIdentity {
        cn,
        dest_dir: &dest_dir,
        certificate_src,
        private_key_src,
        trusted_src,
    })
}

/// Verify a file at `path` parses as one or more PEM-encoded X.509
/// certificates. Used by the conf-install tooling to confirm an
/// operator has placed signed cert / trusted-CA files before
/// proceeding with a TLS install — it deliberately stops short of
/// chain validation (the runtime TLS handshake is the authority on
/// that) so it can be called in a tight retry loop without false
/// negatives on intermediate CAs the operator hasn't yet attached.
///
/// Cross-platform: implemented with `rustls-pemfile` rather than
/// openssl so it works on Windows too. The previous openssl-backed
/// implementation lived in `ca.rs` which is unix-only.
pub fn validate_pem_cert_file(path: &Path) -> Result<()> {
    use std::io::BufReader;
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut reader = BufReader::new(f);
    // `rustls_pemfile::certs` returns an iterator of
    // `Result<CertificateDer, io::Error>` over every BEGIN
    // CERTIFICATE block in the file. Empty input → empty iterator,
    // so we count and bail if zero.
    let mut count = 0usize;
    for cert in rustls_pemfile::certs(&mut reader) {
        let _ = cert.with_context(|| {
            format!("parsing PEM X.509 from {}", path.display())
        })?;
        count += 1;
    }
    if count == 0 {
        bail!("{} contains no PEM certificates", path.display());
    }
    Ok(())
}

/// Read the first PEM-encoded X.509 cert from `path` and return its
/// first DNS SubjectAlternativeName entry. Used by the install tools
/// to derive `our_name` from a cert the operator brought along —
/// always more reliable than asking the operator to type the name
/// and risking it diverging from what's actually in the cert.
///
/// netidx's TLS runtime (`Tls::load` in netidx/src/config/mod.rs)
/// keys identities by the cert's `alt_name` SAN at load time, so
/// this helper extracts exactly that field — the on-disk name we
/// choose for install paths matches what the runtime will see on
/// the wire.
///
/// Errors if the file doesn't parse, the file is empty, or the
/// first cert carries no DNS SAN (other SAN forms — IP, URI, email
/// — are deliberately not accepted; netidx pins TLS identities by
/// DNS name).
pub fn extract_dns_san_from_pem(path: &Path) -> Result<String> {
    use std::io::BufReader;
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut reader = BufReader::new(f);
    let der = rustls_pemfile::certs(&mut reader)
        .next()
        .ok_or_else(|| anyhow!("{} contains no PEM certificates", path.display()))?
        .with_context(|| format!("parsing PEM in {}", path.display()))?;
    first_dns_san_from_der(der.as_ref()).ok_or_else(|| {
        anyhow!(
            "{} has no DNS SubjectAlternativeName entry (or is not a \
             parseable X.509 certificate)",
            path.display()
        )
    })
}

/// The first DNS SAN on a DER-encoded X.509 cert, or `None` if it can't
/// be parsed or carries no DNS SAN. Used by the resolver-name probe to
/// read the name off the cert a resolver presents during a TOFU
/// handshake — the same "pin TLS identities by DNS name" rule as
/// [`extract_dns_san_from_pem`], over raw DER instead of a PEM file.
pub fn first_dns_san_from_der(der: &[u8]) -> Option<String> {
    use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    let ext = cert.subject_alternative_name().ok().flatten()?;
    ext.value.general_names.iter().find_map(|gn| match gn {
        GeneralName::DNSName(dns) => Some(dns.to_string()),
        _ => None,
    })
}

/// Strip the leftmost DNS label off a SAN to get the *domain* that
/// `tls.identities` should be keyed by. In netidx's convention the
/// SAN is `<user>.<domain>` (e.g. `mazikeen.local`) and one identity
/// entry covers the whole domain (`local`), with the runtime matching
/// any host under that domain via the reverse-domain prefix match in
/// `tls::get_match`.
///
/// Errors when the SAN has no `.` separator — a single-label SAN
/// (e.g. `localhost`) has no domain to derive and the operator should
/// pick an identity key explicitly.
pub fn domain_from_san(san: &str) -> Result<&str> {
    match san.split_once('.') {
        Some((_user, domain)) if !domain.is_empty() => Ok(domain),
        _ => bail!(
            "cannot derive identity domain from SAN {san:?}: expected \
             `<user>.<domain>` form (e.g. `host.example.com`)"
        ),
    }
}

fn ensure_valid_cn(cn: &str) -> Result<()> {
    if cn.is_empty() {
        bail!("TLS identity name (cn) must not be empty");
    }
    if cn.contains('/') || cn.contains('\\') {
        bail!(
            "TLS identity name (cn) may not contain path separators: {cn:?}"
        );
    }
    if cn == "." || cn == ".." {
        bail!("TLS identity name (cn) must not be a relative-dir marker");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, bytes: &[u8]) {
        std::fs::write(path, bytes).unwrap();
    }

    /// rustls-pemfile happy path: a well-formed self-contained PEM
    /// block parses. We don't need openssl to generate the cert —
    /// any RFC-7468-shaped PEM with a CERTIFICATE label is accepted
    /// at the syntactic level (rustls-pemfile validates only the
    /// PEM framing, not the X.509 contents). For "real cert" cross-
    /// checking against openssl-generated PEMs, see the matching
    /// test in `ca.rs::validate_pem_cert_file_accepts_openssl_certs`.
    #[test]
    fn validate_pem_cert_file_accepts_pem_block() {
        let dir = tempfile::tempdir().unwrap();
        // Minimal valid-shape PEM: rustls-pemfile only checks the
        // BEGIN/END delimiters and base64 body — not the cert
        // semantics. A 1-byte DER body is fine for the parse test.
        // We're verifying the framing recogniser, not X.509.
        let pem = b"-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n";
        let p = dir.path().join("ok.pem");
        write(&p, pem);
        validate_pem_cert_file(&p).unwrap();
    }

    #[test]
    fn validate_pem_cert_file_rejects_missing_empty_and_junk() {
        let dir = tempfile::tempdir().unwrap();
        // Missing file.
        assert!(validate_pem_cert_file(&dir.path().join("nope.pem")).is_err());
        // Empty file → "contains no PEM certificates".
        let empty = dir.path().join("empty.pem");
        write(&empty, b"");
        assert!(validate_pem_cert_file(&empty).is_err());
        // Garbage with no BEGIN markers — same "no certificates" path.
        let junk = dir.path().join("junk.pem");
        write(&junk, b"definitely not a PEM file");
        assert!(validate_pem_cert_file(&junk).is_err());
    }

    #[test]
    fn extract_dns_san_rejects_missing_and_junk() {
        // Pure-shape failure cases. The "cert with a SAN" happy
        // path can't be tested here cheaply without openssl
        // (rustls-pemfile only validates framing, not contents) —
        // that test lives in `ca.rs` alongside the issuer.
        let dir = tempfile::tempdir().unwrap();
        assert!(extract_dns_san_from_pem(&dir.path().join("nope.pem")).is_err());
        let empty = dir.path().join("empty.pem");
        write(&empty, b"");
        assert!(extract_dns_san_from_pem(&empty).is_err());
        // Well-formed PEM with garbage DER inside — passes the
        // framing check, fails the X.509 parse.
        let bad_der = dir.path().join("bad-der.pem");
        write(
            &bad_der,
            b"-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n",
        );
        assert!(extract_dns_san_from_pem(&bad_der).is_err());
    }

    #[test]
    fn domain_from_san_splits_off_leftmost_label() {
        assert_eq!(domain_from_san("mazikeen.local").unwrap(), "local");
        assert_eq!(
            domain_from_san("host.subdomain.example.com").unwrap(),
            "subdomain.example.com"
        );
        // Single-label SAN has no domain → error
        assert!(domain_from_san("localhost").is_err());
        // Trailing-dot edge case: empty domain after split → error
        assert!(domain_from_san("user.").is_err());
        // Empty input
        assert!(domain_from_san("").is_err());
    }

    #[test]
    fn cn_validation() {
        assert!(ensure_valid_cn("alice.example.com").is_ok());
        assert!(ensure_valid_cn("").is_err());
        assert!(ensure_valid_cn("a/b").is_err());
        assert!(ensure_valid_cn("a\\b").is_err());
        assert!(ensure_valid_cn(".").is_err());
        assert!(ensure_valid_cn("..").is_err());
    }

    #[test]
    fn install_round_trip() {
        let src = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        let cert_src = src.path().join("cert.pem");
        let key_src = src.path().join("key.pem");
        let ca_src = src.path().join("ca.pem");
        write(&cert_src, b"-----BEGIN CERTIFICATE-----\n...");
        write(&key_src, b"-----BEGIN PRIVATE KEY-----\n...");
        write(&ca_src, b"-----BEGIN CERTIFICATE-----\n...ca...");

        let id = install_identity(&InstallIdentity {
            cn: "host.example.com",
            dest_dir: dest.path(),
            certificate_src: &cert_src,
            private_key_src: &key_src,
            trusted_src: &ca_src,
        })
        .unwrap();

        assert_eq!(id.cn, "host.example.com");
        assert!(id.certificate.exists());
        assert!(id.private_key.exists());
        assert!(id.trusted.exists());
        assert_eq!(id.directory, dest.path());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| {
                std::fs::metadata(p).unwrap().permissions().mode() & 0o777
            };
            assert_eq!(mode(&id.private_key), 0o600);
            assert_eq!(mode(&id.certificate), 0o644);
            assert_eq!(mode(&id.trusted), 0o644);
        }

        assert_eq!(
            std::fs::read(&id.certificate).unwrap(),
            std::fs::read(&cert_src).unwrap()
        );
        assert_eq!(
            std::fs::read(&id.private_key).unwrap(),
            std::fs::read(&key_src).unwrap()
        );
        assert_eq!(
            std::fs::read(&id.trusted).unwrap(),
            std::fs::read(&ca_src).unwrap()
        );
    }

    #[test]
    fn install_overwrites() {
        let src = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        let cert_src = src.path().join("cert.pem");
        let key_src = src.path().join("key.pem");
        let ca_src = src.path().join("ca.pem");
        write(&cert_src, b"first cert");
        write(&key_src, b"first key");
        write(&ca_src, b"first ca");

        let p = InstallIdentity {
            cn: "h",
            dest_dir: dest.path(),
            certificate_src: &cert_src,
            private_key_src: &key_src,
            trusted_src: &ca_src,
        };
        let id = install_identity(&p).unwrap();
        assert_eq!(std::fs::read(&id.certificate).unwrap(), b"first cert");

        write(&cert_src, b"second cert");
        let id2 = install_identity(&p).unwrap();
        assert_eq!(std::fs::read(&id2.certificate).unwrap(), b"second cert");
        assert_eq!(id.certificate, id2.certificate);
    }

    /// The key's DER must survive encrypt → decrypt unchanged — this
    /// is the pure-Rust PBES2 path every sealed or password-protected
    /// key takes (no TPM involved here; the password is the variable).
    #[test]
    fn encrypt_decrypt_round_trips_the_key() {
        let kc = crate::conf_client::generate_key_and_csr("x.example.com").unwrap();
        let enc =
            netidx::tls::encrypt_private_key(&kc.private_key_pem, "hunter2").unwrap();
        assert!(enc.contains("ENCRYPTED PRIVATE KEY"));
        // The wrong password must fail, not produce garbage.
        assert!(netidx::tls::decrypt_private_key(&enc, "wrong").is_err());
        let dec = netidx::tls::decrypt_private_key(&enc, "hunter2").unwrap();
        let der = |pem: &str| {
            rustls_pemfile::private_key(&mut std::io::Cursor::new(pem.as_bytes()))
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            der(&kc.private_key_pem).secret_der(),
            der(&dec).secret_der(),
            "decrypted key must equal the original",
        );
    }

    /// The whole daemon path on real hardware: seal a key, write it +
    /// sidecar the way the install flows do, and load it back through
    /// `netidx::tls::load_private_key` exactly as a starting resolver
    /// would — no keychain, no askpass, no human. Skips silently where
    /// no TPM is reachable.
    #[test]
    fn sealed_key_loads_through_netidx_tls() {
        if !netidx_tpm::available() {
            eprintln!("skipping: no usable TPM on this host");
            return;
        }
        let kc = crate::conf_client::generate_key_and_csr("x.example.com").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("private.key");
        let (enc, blob) = seal_private_key(&kc.private_key_pem).unwrap();
        std::fs::write(&key, enc.as_bytes()).unwrap();
        std::fs::write(sealed_sidecar(&key), &blob).unwrap();
        let loaded =
            netidx::tls::load_private_key(None, &key.to_string_lossy()).unwrap();
        let original =
            rustls_pemfile::private_key(&mut std::io::Cursor::new(
                kc.private_key_pem.as_bytes(),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(loaded.secret_der(), original.secret_der());
        // A corrupted sidecar must be a hard error, not a fallthrough
        // to a password prompt that would hang a daemon.
        let mut bad = blob.clone();
        let n = bad.len();
        bad[n / 2] ^= 0xff;
        std::fs::write(sealed_sidecar(&key), &bad).unwrap();
        // The password cache inside netidx::tls is keyed by path; use a
        // fresh path so the cached good password doesn't mask the
        // corruption.
        let key2 = dir.path().join("other.key");
        std::fs::write(&key2, enc.as_bytes()).unwrap();
        std::fs::write(sealed_sidecar(&key2), &bad).unwrap();
        assert!(netidx::tls::load_private_key(None, &key2.to_string_lossy()).is_err());
    }

    /// Sidecars travel with their keys through the identity installer —
    /// copied when the source has one, and a stale one at the
    /// destination cleared when it doesn't.
    #[test]
    fn install_carries_and_clears_sidecars() {
        let src = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        let cert_src = src.path().join("cert.pem");
        let key_src = src.path().join("key.pem");
        let ca_src = src.path().join("ca.pem");
        write(&cert_src, b"cert");
        write(&key_src, b"key");
        write(&ca_src, b"ca");
        write(&sealed_sidecar(&key_src), b"sealed blob");
        let p = InstallIdentity {
            cn: "h",
            dest_dir: dest.path(),
            certificate_src: &cert_src,
            private_key_src: &key_src,
            trusted_src: &ca_src,
        };
        let id = install_identity(&p).unwrap();
        let dst_sidecar = sealed_sidecar(&id.private_key);
        assert_eq!(std::fs::read(&dst_sidecar).unwrap(), b"sealed blob");
        // Re-install from a source with no sidecar: the stale one at
        // the destination must go — at load time it would shadow the
        // new key's real password source.
        std::fs::remove_file(sealed_sidecar(&key_src)).unwrap();
        install_identity(&p).unwrap();
        assert!(!dst_sidecar.exists());
    }
}
