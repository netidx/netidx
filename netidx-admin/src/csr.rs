//! Certificate signing requests, without openssl.
//!
//! The subject and SAN types every CSR path shares, the `<kind>:<value>` SAN
//! parser the CLI takes, and an rcgen key + CSR generator. All of it is
//! portable: preparing a request is arithmetic over a name, so a Windows host
//! does it exactly like a unix one. Only *signing* needs the vault, and that
//! stays in [`crate::ca`].
//!
//! rcgen's only practical keygen is ECDSA P-256, which the CA accepts
//! (`ca::check_pubkey_strength`) and netidx's TLS runtime is agnostic to.

use anyhow::{Context, Result, anyhow, bail};
use std::{net::IpAddr, path::PathBuf};
use zeroize::Zeroizing;

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

/// A freshly generated keypair and the CSR that requests a certificate for it.
pub struct KeyAndCsr {
    /// PKCS#8 PEM of the private key (ECDSA P-256). Zeroized on drop.
    pub private_key_pem: Zeroizing<String>,
    pub csr_pem: String,
}

fn ia5(kind: &str, s: &str) -> Result<rcgen::string::Ia5String> {
    rcgen::string::Ia5String::try_from(s)
        .map_err(|_| anyhow!("SAN {kind}:{s:?} must be ASCII"))
}

/// Generate an ECDSA P-256 key and a CSR requesting `subject` with `san`.
///
/// The CSR is a public document and is always returned unencrypted; the
/// private key is returned as unencrypted PKCS#8 PEM, so a caller that wants it
/// protected at rest is responsible for that.
pub fn generate_key_and_csr(subject: &Subject, san: &[SanEntry]) -> Result<KeyAndCsr> {
    use rcgen::{CertificateParams, DnType, KeyPair, SanType};
    let key_pair = KeyPair::generate().context("generating key pair")?;
    let mut params = CertificateParams::default();
    let dn = &mut params.distinguished_name;
    dn.push(DnType::CommonName, subject.common_name.as_str());
    if let Some(v) = subject.country.as_deref() {
        dn.push(DnType::CountryName, v);
    }
    if let Some(v) = subject.state.as_deref() {
        dn.push(DnType::StateOrProvinceName, v);
    }
    if let Some(v) = subject.locality.as_deref() {
        dn.push(DnType::LocalityName, v);
    }
    if let Some(v) = subject.organization.as_deref() {
        dn.push(DnType::OrganizationName, v);
    }
    params.subject_alt_names = san
        .iter()
        .map(|s| {
            Ok(match s {
                SanEntry::Dns(d) => SanType::DnsName(ia5("dns", d)?),
                SanEntry::Ip(ip) => SanType::IpAddress(*ip),
                SanEntry::Uri(u) => SanType::URI(ia5("uri", u)?),
                SanEntry::Email(e) => SanType::Rfc822Name(ia5("email", e)?),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let csr = params.serialize_request(&key_pair).context("serializing CSR")?;
    Ok(KeyAndCsr {
        private_key_pem: Zeroizing::new(key_pair.serialize_pem()),
        csr_pem: csr.pem().context("encoding CSR PEM")?,
    })
}

/// Parse `--san` strings (`<kind>:<value>`), defaulting to `dns:<fallback_cn>`
/// when none are given.
pub fn parse_sans(raw: &[String], fallback_cn: &str) -> Result<Vec<SanEntry>> {
    if raw.is_empty() {
        return Ok(vec![SanEntry::Dns(fallback_cn.to_string())]);
    }
    raw.iter().map(|s| parse_san_one(s)).collect()
}

/// Parse a single `<kind>:<value>` SAN string.
pub fn parse_san_one(s: &str) -> Result<SanEntry> {
    let (kind, val) = s
        .split_once(':')
        .ok_or_else(|| anyhow!("SAN must be in the form <kind>:<value>: {s:?}"))?;
    if val.is_empty() {
        bail!("SAN value must not be empty: {s:?}");
    }
    Ok(match kind {
        "dns" => SanEntry::Dns(val.to_string()),
        "ip" => SanEntry::Ip(
            val.parse::<IpAddr>().map_err(|e| anyhow!("invalid SAN ip {val:?}: {e}"))?,
        ),
        "uri" => SanEntry::Uri(val.to_string()),
        "email" => SanEntry::Email(val.to_string()),
        other => bail!("unknown SAN kind {other:?}; expected dns / ip / uri / email"),
    })
}

/// Make a CN safe to embed in a filename. CNs are usually hostnames (already
/// safe), but the field is free-form text, so replace anything outside
/// `[A-Za-z0-9._-]` with `_`. The result is always a single path component — no
/// separators survive — so a defaulted output path can't traverse out of the
/// cwd. Empty input collapses to `_` so we never produce a bare extension.
pub fn sanitize_filename(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() { "_".to_string() } else { out }
}

/// Default `request` CSR path: `./<cn>.csr`.
pub fn default_csr_filename(cn: &str) -> PathBuf {
    PathBuf::from(format!("{}.csr", sanitize_filename(cn)))
}

/// Default `sign` cert path: `./<csr-cn>.pem`, or `./certificate.pem` when the
/// CSR carries no CN.
pub fn default_cert_filename(csr_cn: Option<&str>) -> PathBuf {
    let stem = match csr_cn {
        Some(cn) => sanitize_filename(cn),
        None => "certificate".to_string(),
    };
    PathBuf::from(format!("{stem}.pem"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_filename_keeps_safe_chars_replaces_rest() {
        assert_eq!(sanitize_filename("alice.example.com"), "alice.example.com");
        assert_eq!(sanitize_filename("host-1_test"), "host-1_test");
        assert_eq!(sanitize_filename("a/b"), "a_b");
        assert_eq!(sanitize_filename("../etc/passwd"), ".._etc_passwd");
        assert_eq!(sanitize_filename("with space"), "with_space");
        assert_eq!(sanitize_filename("weird*<>chars"), "weird___chars");
        assert_eq!(sanitize_filename(""), "_");
    }

    // The offline `request` → `sign` handoff depends on every SAN kind and
    // every optional subject field surviving into the PEM, so exercise the
    // whole surface rather than just the CN.
    #[test]
    fn generated_csr_carries_the_subject_and_every_san_kind() {
        let subject = Subject {
            common_name: "alice.example.com".to_string(),
            country: Some("US".to_string()),
            state: Some("NY".to_string()),
            locality: Some("New York".to_string()),
            organization: Some("Example".to_string()),
        };
        let san = parse_sans(
            &[
                "dns:alice.example.com".to_string(),
                "ip:10.0.0.1".to_string(),
                "uri:https://example.com/a".to_string(),
                "email:alice@example.com".to_string(),
            ],
            "alice.example.com",
        )
        .unwrap();
        let out = generate_key_and_csr(&subject, &san).unwrap();
        assert!(out.csr_pem.starts_with("-----BEGIN CERTIFICATE REQUEST-----"));
        assert!(out.private_key_pem.contains("PRIVATE KEY"));
        let der = pem_body(&out.csr_pem);
        let (_, csr) =
            x509_parser::certification_request::X509CertificationRequest::from_der(&der)
                .unwrap();
        let info = &csr.certification_request_info;
        let subject_str = info.subject.to_string();
        for want in ["CN=alice.example.com", "C=US", "ST=NY", "L=New York", "O=Example"] {
            assert!(subject_str.contains(want), "{want} missing from {subject_str}");
        }
        let mut names = Vec::new();
        for attr in info.attributes() {
            if let ParsedCriAttribute::ExtensionRequest(req) = attr.parsed_attribute() {
                for ext in &req.extensions {
                    if let ParsedExtension::SubjectAlternativeName(san) =
                        ext.parsed_extension()
                    {
                        names.extend(san.general_names.iter().map(fmt_name));
                    }
                }
            }
        }
        names.sort();
        assert_eq!(
            names,
            vec![
                "dns:alice.example.com".to_string(),
                "email:alice@example.com".to_string(),
                "ip:10.0.0.1".to_string(),
                "uri:https://example.com/a".to_string(),
            ]
        );
    }

    fn fmt_name(n: &GeneralName<'_>) -> String {
        match n {
            GeneralName::DNSName(d) => format!("dns:{d}"),
            GeneralName::RFC822Name(e) => format!("email:{e}"),
            GeneralName::URI(u) => format!("uri:{u}"),
            GeneralName::IPAddress(ip) => match ip {
                [a, b, c, d] => format!("ip:{a}.{b}.{c}.{d}"),
                other => format!("ip:{other:?}"),
            },
            other => format!("other:{other:?}"),
        }
    }

    // A non-ASCII SAN can't be encoded as IA5; refuse it here rather than
    // producing a CSR the CA will reject.
    #[test]
    fn non_ascii_san_is_refused() {
        let s = Subject::cn("a");
        let e = match generate_key_and_csr(&s, &[SanEntry::Dns("café.example".into())]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a non-ASCII SAN must not produce a CSR"),
        };
        assert!(e.contains("ASCII"), "{e}");
    }

    #[test]
    fn san_parsing_rejects_junk() {
        assert!(parse_san_one("bogus").is_err());
        assert!(parse_san_one("dns:").is_err());
        assert!(parse_san_one("ip:not-an-ip").is_err());
        assert!(parse_san_one("wat:x").is_err());
        assert!(matches!(parse_san_one("ip:10.0.0.1").unwrap(), SanEntry::Ip(_)));
    }

    fn pem_body(pem: &str) -> Vec<u8> {
        use base64::Engine;
        let b64: String =
            pem.lines().filter(|l| !l.starts_with("-----")).collect::<Vec<_>>().join("");
        base64::engine::general_purpose::STANDARD.decode(b64).unwrap()
    }

    use x509_parser::{
        cri_attributes::ParsedCriAttribute,
        extensions::{GeneralName, ParsedExtension},
        prelude::FromDer,
    };
}
