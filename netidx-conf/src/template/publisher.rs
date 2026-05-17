//! Publisher-host template. No resolver, no activation units — just
//! a client config pointing at explicit addresses. (The on-disk file
//! is still `client.json` because that's the netidx config name; the
//! template is named for the typical host running it, which mostly
//! hosts publishers connecting to a remote resolver cluster.)
//!
//! Auth model:
//! - Each address in [`addrs`](PublisherParams::addrs) carries its own
//!   [`ReferralAuth`] (no cert paths there — those go in
//!   `tls_identities`). The client uses this auth scheme to talk to
//!   that specific address.
//! - [`default_auth`](PublisherParams::default_auth) is the top-level
//!   client preference (relevant when following referrals that don't
//!   pin auth).
//! - [`tls_identities`](PublisherParams::tls_identities) installs and
//!   registers TLS identities. Each entry's `server_pattern` becomes
//!   the key in `tls.identities` so netidx's reverse-domain match
//!   picks the right identity for each TLS server.

use super::*;
use crate::{client::ClientConfig, paths};
use anyhow::Result;
use std::{net::SocketAddr, path::PathBuf};

/// Parameters for [`publisher`].
///
/// Deliberately does **not** derive `Default` — same rationale as
/// [`WorkstationParams`](super::workstation::WorkstationParams):
/// struct-literal construction at call sites forces adding a new
/// field to be a compile error rather than a silent default.
#[derive(Debug, Clone)]
pub struct PublisherParams {
    /// Each address carries its own per-address auth via
    /// [`ReferralAuth`]. For TLS, the server's TLS name goes in
    /// `ReferralAuth::Tls(name)`; the cert/key/CA paths come from
    /// `tls_identities` instead.
    pub addrs: Vec<(SocketAddr, ReferralAuth)>,
    /// Override `default_auth` on the on-disk config. `None` ⇒ derive
    /// from `addrs` (Tls > Krb5 > Local > Anonymous). The field is
    /// `#[serde(default)]` in netidx's schema (defaulting to Krb5),
    /// so leaving it `None` here also produces a well-formed config.
    pub default_auth: Option<DefaultAuthMech>,
    /// TLS identities to install and register. Empty list for
    /// non-TLS deployments.
    pub tls_identities: Vec<TlsIdentitySpec>,
    /// Cluster base path. Default `/`.
    pub base: ArcStr,
    /// Output path. `None` ⇒ user default.
    pub config_path: Option<PathBuf>,
    /// Publisher's `default_bind_config` for the on-disk client
    /// config. `None` leaves the field unset, which means the
    /// publisher default (`BindCfg::Local` = 127.0.0.1) applies — that
    /// only works for resolvers also bound to loopback. For any
    /// non-loopback resolver the publisher MUST bind to a routable
    /// interface or the resolver rejects its registration (loopback
    /// vs. non-loopback mixing fails check_addrs). The CLI defaults
    /// this from `default_advertised_ip()`.
    pub default_bind_config: Option<String>,
}

pub fn publisher(p: &PublisherParams) -> Result<RenderedTemplate> {
    if p.addrs.is_empty() {
        bail!("publisher requires at least one address");
    }

    let default_auth = match &p.default_auth {
        Some(d) => d.clone(),
        None => derive_default_auth(p.addrs.iter().map(|(_, a)| a)),
    };
    if matches!(default_auth, DefaultAuthMech::Tls) && p.tls_identities.is_empty() {
        bail!(
            "default_auth=Tls requires at least one tls_identity (Config::from_file rejects otherwise)"
        );
    }

    let path = match &p.config_path {
        Some(p) => p.clone(),
        None => paths::user_client_config()?,
    };
    let base = if p.base.is_empty() {
        ArcStr::from("/")
    } else {
        p.base.clone()
    };

    let mut ccfg_builder = cfile::ConfigBuilder::default();
    ccfg_builder
        .addrs(
            p.addrs
                .iter()
                .map(|(a, ac)| (*a, ac.clone().into_client_file()))
                .collect(),
        )
        .base(base.as_str())
        .default_auth(default_auth);
    if let Some(bind) = &p.default_bind_config {
        ccfg_builder.default_bind_config(bind.clone());
    }
    if let Some(tls) = client_tls_section_from(&p.tls_identities)? {
        ccfg_builder.tls(tls);
    }
    let client_cfg = ClientConfig::from(ccfg_builder.build()?);

    let tls_install = p
        .tls_identities
        .iter()
        .map(|s| s.install_job())
        .collect::<Result<Vec<_>>>()?;

    Ok(RenderedTemplate {
        client_config: Some((path, client_cfg)),
        resolver_config: None,
        perms_file: None,
        id_map_file: None,
        units: BTreeMap::new(),
        units_dir: None,
        tls_install,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn cfg_path(out: &tempfile::TempDir) -> PathBuf {
        out.path().join("client.json")
    }

    fn addr(s: &str) -> SocketAddr {
        SocketAddr::from_str(s).unwrap()
    }

    #[test]
    fn requires_addrs() {
        let p = PublisherParams {
            addrs: vec![],
            default_auth: None,
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: None,
            default_bind_config: None,
        };
        assert!(publisher(&p).is_err());
    }

    #[test]
    fn anonymous() {
        let out = tempfile::tempdir().unwrap();
        let p = PublisherParams {
            addrs: vec![(addr("10.0.0.1:4564"), ReferralAuth::Anonymous)],
            default_auth: Some(DefaultAuthMech::Anonymous),
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: None,
        };
        publisher(&p).unwrap().apply().unwrap();
        let c = client::ClientConfig::load(cfg_path(&out)).unwrap();
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Anonymous));
        assert!(matches!(c.0.addrs[0].1, cfile::Auth::Anonymous));
    }

    #[test]
    fn local_auth_loopback_only() {
        let out = tempfile::tempdir().unwrap();
        let p = PublisherParams {
            addrs: vec![(
                addr("127.0.0.1:4564"),
                ReferralAuth::Local(ArcStr::from("/var/run/netidx.sock")),
            )],
            default_auth: Some(DefaultAuthMech::Local),
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: None,
        };
        publisher(&p).unwrap().apply().unwrap();
        let c = client::ClientConfig::load(cfg_path(&out)).unwrap();
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Local));
        assert!(matches!(c.0.addrs[0].1, cfile::Auth::Local(_)));
    }

    #[test]
    fn krb5() {
        let out = tempfile::tempdir().unwrap();
        let p = PublisherParams {
            addrs: vec![(
                addr("10.0.0.1:4564"),
                ReferralAuth::Krb5(ArcStr::from(
                    "host/resolver.example.com@REALM",
                )),
            )],
            default_auth: Some(DefaultAuthMech::Krb5),
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: None,
        };
        publisher(&p).unwrap().apply().unwrap();
        let c = client::ClientConfig::load(cfg_path(&out)).unwrap();
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Krb5));
        assert!(matches!(c.0.addrs[0].1, cfile::Auth::Krb5(_)));
    }

    #[test]
    fn tls_with_identity() {
        use crate::ca;
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = ca::Ca::init(
            &ca::CaParams {
                directory: ca_dir.path().to_path_buf(),
                subject: ca::Subject::cn("test-ca"),
                san: vec![],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued = ca
            .issue(&ca::IssueParams {
                subject: ca::Subject::cn("client"),
                san: vec![ca::SanEntry::Dns("client.example.com".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_src.path().to_path_buf(),
            })
            .unwrap();

        let out = tempfile::tempdir().unwrap();
        let p = PublisherParams {
            addrs: vec![(
                addr("10.0.0.1:4564"),
                ReferralAuth::Tls(ArcStr::from("resolver.example.com")),
            )],
            default_auth: Some(DefaultAuthMech::Tls),
            tls_identities: vec![TlsIdentitySpec {
                server_pattern: ArcStr::from("example.com"),
                our_name: ArcStr::from("client"),
                certificate: issued.certificate.clone(),
                private_key: issued.private_key.clone(),
                trusted: ca_dir.path().join("certificate.pem"),
                dest_dir: Some(out.path().join("installed-tls")),
            }],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: None,
        };
        let rt = publisher(&p).unwrap();
        assert_eq!(rt.tls_install.len(), 1);
        let (_, c) = rt.client_config.as_ref().unwrap();
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Tls));
        let tls = c.0.tls.as_ref().expect("tls section");
        assert!(tls.identities.contains_key("example.com"));

        // apply() exercises install-before-validate ordering.
        rt.apply().unwrap();
        assert!(out.path().join("installed-tls/certificate.pem").exists());
    }

    #[test]
    fn tls_default_without_identity_errors() {
        let out = tempfile::tempdir().unwrap();
        let p = PublisherParams {
            addrs: vec![(addr("10.0.0.1:4564"), ReferralAuth::Anonymous)],
            default_auth: Some(DefaultAuthMech::Tls),
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: None,
        };
        assert!(publisher(&p).is_err());
    }

    #[test]
    fn default_auth_derives_from_addrs_when_none() {
        let out = tempfile::tempdir().unwrap();
        // Krb5 on the only addr ⇒ derived default_auth: Krb5.
        let p = PublisherParams {
            addrs: vec![(
                addr("10.0.0.1:4564"),
                ReferralAuth::Krb5(ArcStr::from("svc")),
            )],
            default_auth: None,
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: None,
        };
        publisher(&p).unwrap().apply().unwrap();
        let c = client::ClientConfig::load(cfg_path(&out)).unwrap();
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Krb5));
    }

    /// `default_bind_config` round-trips through the generated client
    /// config. This is the publisher-side analogue of the resolver
    /// template's loopback-mixing fix.
    #[test]
    fn default_bind_config_round_trips() {
        let out = tempfile::tempdir().unwrap();
        let p = PublisherParams {
            addrs: vec![(addr("10.0.0.1:4564"), ReferralAuth::Anonymous)],
            default_auth: Some(DefaultAuthMech::Anonymous),
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: Some("10.0.0.5/32".to_string()),
        };
        publisher(&p).unwrap().apply().unwrap();
        let c = client::ClientConfig::load(cfg_path(&out)).unwrap();
        // Round-trip through the file representation.
        assert_eq!(c.0.default_bind_config.as_deref(), Some("10.0.0.5/32"));
    }

    #[test]
    fn mixed_auth_across_addrs() {
        // Multiple non-loopback addrs with different schemes — netidx
        // accepts this as long as loopback/non-loopback aren't mixed.
        let out = tempfile::tempdir().unwrap();
        let p = PublisherParams {
            addrs: vec![
                (addr("10.0.0.1:4564"), ReferralAuth::Anonymous),
                (
                    addr("10.0.0.2:4564"),
                    ReferralAuth::Krb5(ArcStr::from("svc")),
                ),
            ],
            default_auth: Some(DefaultAuthMech::Krb5),
            tls_identities: vec![],
            base: ArcStr::from("/"),
            config_path: Some(cfg_path(&out)),
            default_bind_config: None,
        };
        publisher(&p).unwrap().apply().unwrap();
        let c = client::ClientConfig::load(cfg_path(&out)).unwrap();
        assert_eq!(c.0.addrs.len(), 2);
    }
}
