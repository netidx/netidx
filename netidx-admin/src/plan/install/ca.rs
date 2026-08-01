//! First-class CA / certificate-authority role install.

use super::{DEFAULT_TLS_DOMAIN, InstallCommon, resolve_units_dir};

use crate::{
    answer::{Answerer, Field},
    config_lock::ConfigDirLock,
    fingerprint::Fingerprint,
    paths,
    plan::{
        ca_setup,
        service::{ServiceGate, ServiceNeed, offer},
    },
    provenance::{AdminDomainIdentity, InstallRecord, InstallRole},
    service::ServiceScope,
};
use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

/// Create the self-signed CA role used both by a dedicated CA
/// install and by the first resolver when the roles are co-located.
pub async fn create_self_signed_ca(
    ans: &mut dyn Answerer,
    config_lock: &ConfigDirLock,
    domain: String,
    listen: Option<SocketAddr>,
    listen_hint: Option<IpAddr>,
    units_dir: Option<PathBuf>,
    insecure_no_tpm: bool,
) -> Result<(crate::ca::Ca, ServiceNeed, AdminDomainIdentity)> {
    ca_setup::announce_founding_policy(ans, &domain);
    let mut opts = ca_setup::founding_ca_opts(
        paths::user_ca_dir()?,
        domain.clone(),
        insecure_no_tpm,
        Some(true),
        listen_hint,
        units_dir,
    );
    opts.listen = listen;
    let (ca, need) = ca_setup::create_vaulted_ca(ans, config_lock, opts).await?;
    let identity = AdminDomainIdentity::new(
        domain,
        &Fingerprint::of_cert_pem(&ca.certificate_pem()?)?,
    );
    Ok((ca, need, identity))
}

#[derive(Debug, Clone)]
pub struct CaInput {
    pub domain: Option<String>,
    pub listen: Option<SocketAddr>,
    pub units_dir: Option<PathBuf>,
    pub external_sign: Option<bool>,
    pub insecure_no_tpm: bool,
    pub common: InstallCommon,
}

impl CaInput {
    pub fn defaults(common: InstallCommon) -> Self {
        Self {
            domain: None,
            listen: None,
            units_dir: None,
            external_sign: None,
            insecure_no_tpm: false,
            common,
        }
    }
}

/// Install a CA without a resolver role. Resolver installation calls
/// the same CA-creation brain when co-locating the two roles.
/// What a CA install left behind. A CA is the one role whose install has a
/// second outcome: an externally-signed CA stops after emitting its CSR and
/// registers no service until the external PKI returns, and the operator needs
/// the path of that CSR. Reporting it here stops a frontend re-deriving it by
/// reopening the CA it just created.
pub struct CaInstalled {
    /// An OS service the frontend must register, if any.
    pub service: Option<ServiceScope>,
    /// The subordinate-CA CSR awaiting an external signature.
    pub pending_external: Option<PathBuf>,
}

impl CaInstalled {
    fn service(service: Option<ServiceScope>) -> CaInstalled {
        CaInstalled { service, pending_external: None }
    }
}

pub async fn run_ca(ans: &mut dyn Answerer, input: CaInput) -> Result<CaInstalled> {
    let record_path = paths::user_install_record()?;
    if !input.common.mode.is_dry_run() && tokio::fs::try_exists(&record_path).await? {
        let existing = InstallRecord::load_async(&record_path)
            .await
            .map(|r| r.role.as_str().to_string())
            .unwrap_or_else(|_| "existing".to_string());
        bail!(
            "refusing to install a CA over the {existing} install recorded at {}; \
             use a fresh dedicated machine, or let the first resolver compose the \
             CA role during its own install",
            record_path.display()
        );
    }
    let domain = ans
        .text(Field::AdminDomainName, input.domain, Some(DEFAULT_TLS_DOMAIN), false)
        .await?
        .unwrap_or_else(|| DEFAULT_TLS_DOMAIN.to_string());
    let units_dir = resolve_units_dir(input.common.no_units, input.units_dir.as_deref())?;
    let external_sign =
        ans.confirm(Field::ExternalSign, input.external_sign, false).await?;
    ans.announce(
        "CA / Certificate Authority",
        "This machine will be the admin domain's one active ca and \
         certificate authority. It does not need to run a resolver.",
    )
    .await?;
    if input.common.mode.is_dry_run() {
        ans.note(&format_compact!(
            "would create the CA for domain {domain:?} at {}{}",
            paths::user_ca_dir()?.display(),
            if external_sign {
                " and emit a subordinate-CA CSR for the external PKI"
            } else {
                ""
            }
        ));
        if external_sign {
            ans.note(
                "[dry-run] OS-service registration would be deferred until the signed \
                 subordinate-CA certificate is installed",
            );
            return Ok(CaInstalled::service(None));
        }
        return Ok(CaInstalled::service(
            offer(
                ans,
                crate::plan::service::ServiceNeed::at(ServiceScope::System),
                ServiceGate {
                    dry_run: true,
                    no_service: input.common.no_service,
                    with_service: input.common.with_service,
                },
            )
            .await?,
        ));
    }
    if external_sign {
        let config_lock = input
            .common
            .mode
            .config_lock()
            .context("CA apply mode has no config-directory lock")?;
        ca_setup::announce_founding_policy(ans, &domain);
        let mut opts = ca_setup::founding_ca_opts(
            paths::user_ca_dir()?,
            domain.clone(),
            input.insecure_no_tpm,
            Some(true),
            input.listen.map(|a| a.ip()),
            units_dir,
        );
        opts.listen = input.listen;
        let csr = ca_setup::create_vaulted_external_ca(ans, config_lock, opts).await?;
        let record = InstallRecord::new(InstallRole::Ca, "/", "admin-tls", None, None);
        record.save_default_async(config_lock).await?;
        ans.note(
            "CA key and recovery material installed; the CA remains \
             pending until the external PKI returns and you install its certificate",
        );
        return Ok(CaInstalled {
            service: None,
            pending_external: Some(std::fs::canonicalize(&csr).unwrap_or(csr)),
        });
    }
    let config_lock = input
        .common
        .mode
        .config_lock()
        .context("CA apply mode has no config-directory lock")?;
    let (_ca, need, identity) = create_self_signed_ca(
        ans,
        config_lock,
        domain,
        input.listen,
        input.listen.map(|a| a.ip()),
        units_dir,
        input.insecure_no_tpm,
    )
    .await?;
    let cfg_path = paths::discover_admin_server_config_async()
        .await
        .context("the CA was created but its admin-server config is missing")?;
    let cfg = crate::admin_server_config::load_async(&cfg_path).await?;
    InstallRecord::new(
        InstallRole::Ca,
        "/",
        "admin-tls",
        Some(identity),
        Some(cfg.listen),
    )
    .save_default_async(config_lock)
    .await?;
    Ok(CaInstalled::service(
        offer(
            ans,
            need,
            ServiceGate {
                dry_run: false,
                no_service: input.common.no_service,
                with_service: input.common.with_service,
            },
        )
        .await?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guided_ca_install_asks_about_external_signing() {
        let input = CaInput::defaults(InstallCommon {
            mode: crate::plan::install::InstallMode::DryRun,
            force: false,
            no_units: false,
            with_service: false,
            no_service: false,
        });
        assert!(input.external_sign.is_none());
        assert!(input.domain.is_none());
    }
}
