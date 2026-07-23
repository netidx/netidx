use crate::{AdminServerId, CONTROLLER_ROLE_URI, SERVER_ID_URI_PREFIX, SERVING_SAN};
use anyhow::{Context, Result, anyhow, bail};
use uuid::Uuid;
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminCertIdentity {
    pub server_id: AdminServerId,
    pub controller: bool,
}

pub fn admin_cert_identity_from_der(der: &[u8]) -> Result<AdminCertIdentity> {
    let (_, cert) = X509Certificate::from_der(der)
        .map_err(|e| anyhow!("parsing admin certificate: {e}"))?;
    let san = cert
        .subject_alternative_name()
        .context("reading admin certificate SAN")?
        .context("admin certificate has no SubjectAlternativeName")?;
    let dns: Vec<&str> = san
        .value
        .general_names
        .iter()
        .filter_map(|n| match n {
            GeneralName::DNSName(s) => Some(*s),
            _ => None,
        })
        .collect();
    if dns.len() != 1 || !dns[0].eq_ignore_ascii_case(SERVING_SAN) {
        bail!(
            "admin certificate must contain exactly one DNS SAN {:?}, found {:?}",
            SERVING_SAN,
            dns
        );
    }
    let mut server_id = None;
    let mut controller = false;
    for uri in san.value.general_names.iter().filter_map(|n| match n {
        GeneralName::URI(s) => Some(*s),
        _ => None,
    }) {
        if let Some(raw) = uri.strip_prefix(SERVER_ID_URI_PREFIX) {
            if server_id.is_some() {
                bail!("admin certificate contains duplicate server identity URIs");
            }
            let id = Uuid::parse_str(raw)
                .with_context(|| format!("invalid admin server identity URI {uri:?}"))?;
            server_id = Some(AdminServerId(id));
        } else if uri == CONTROLLER_ROLE_URI {
            if controller {
                bail!("admin certificate contains duplicate controller role URIs");
            }
            controller = true;
        }
    }
    Ok(AdminCertIdentity {
        server_id: server_id.context(
            "admin certificate has no protocol-v6 server identity URI (legacy certificates are refused)",
        )?,
        controller,
    })
}

pub fn admin_cert_identity_from_pem(pem: &[u8]) -> Result<AdminCertIdentity> {
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .next()
        .context("admin certificate PEM contains no certificate")?
        .context("parsing admin certificate PEM")?;
    admin_cert_identity_from_der(der.as_ref())
}
