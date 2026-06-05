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

    Ok(InstalledIdentity {
        cn: p.cn.to_string(),
        directory: dir,
        certificate: cert_dst,
        private_key: key_dst,
        trusted: ca_dst,
    })
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
    use x509_parser::prelude::FromDer;
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut reader = BufReader::new(f);
    let der = rustls_pemfile::certs(&mut reader)
        .next()
        .ok_or_else(|| anyhow!("{} contains no PEM certificates", path.display()))?
        .with_context(|| format!("parsing PEM in {}", path.display()))?;
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(der.as_ref())
        .with_context(|| format!("parsing DER X.509 in {}", path.display()))?;
    let ext = cert
        .subject_alternative_name()
        .with_context(|| format!("reading SAN extension from {}", path.display()))?
        .ok_or_else(|| {
            anyhow!("{} has no SubjectAlternativeName extension", path.display())
        })?;
    for name in &ext.value.general_names {
        if let x509_parser::extensions::GeneralName::DNSName(dns) = name {
            return Ok(dns.to_string());
        }
    }
    bail!("{} has no DNS SubjectAlternativeName entry", path.display())
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
}
