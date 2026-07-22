//! First-class controller / certificate-authority role install.

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
    provenance::{InstallRecord, InstallRole, NetworkIdentity},
    service::ServiceScope,
};
use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

/// Create the self-signed controller role used both by a dedicated controller
/// install and by the first resolver when the roles are co-located.
pub async fn create_self_signed_controller(
    ans: &mut dyn Answerer,
    config_lock: &ConfigDirLock,
    domain: String,
    listen: Option<SocketAddr>,
    listen_hint: Option<IpAddr>,
    units_dir: Option<PathBuf>,
    insecure_no_tpm: bool,
) -> Result<(crate::ca::Ca, ServiceNeed, NetworkIdentity)> {
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
    let identity =
        NetworkIdentity::new(domain, &Fingerprint::of_cert_pem(&ca.certificate_pem()?)?);
    Ok((ca, need, identity))
}

#[derive(Debug, Clone)]
pub struct ControllerInput {
    pub domain: Option<String>,
    pub listen: Option<SocketAddr>,
    pub units_dir: Option<PathBuf>,
    pub external_sign: Option<bool>,
    pub insecure_no_tpm: bool,
    pub common: InstallCommon,
}

impl ControllerInput {
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

/// Install a controller without a resolver role. Resolver installation calls
/// the same CA-creation brain when co-locating the two roles.
pub async fn run_controller(
    ans: &mut dyn Answerer,
    input: ControllerInput,
) -> Result<Option<ServiceScope>> {
    let record_path = paths::user_install_record()?;
    if !input.common.mode.is_dry_run() && tokio::fs::try_exists(&record_path).await? {
        let existing = InstallRecord::load_async(&record_path)
            .await
            .map(|r| r.role.as_str().to_string())
            .unwrap_or_else(|_| "existing".to_string());
        bail!(
            "refusing to install a controller over the {existing} install recorded at {}; \
             use a fresh dedicated machine, or let the first resolver compose the \
             controller role during its own install",
            record_path.display()
        );
    }
    let domain = ans
        .text(Field::NetworkDomain, input.domain, Some(DEFAULT_TLS_DOMAIN), false)
        .await?
        .unwrap_or_else(|| DEFAULT_TLS_DOMAIN.to_string());
    let units_dir = resolve_units_dir(input.common.no_units, input.units_dir.as_deref())?;
    let external_sign =
        ans.confirm(Field::ExternalSign, input.external_sign, false).await?;
    ans.announce(
        "Controller / Certificate Authority",
        "This machine will be the administrative network's one active controller and \
         certificate authority. It does not need to run a resolver.",
    )
    .await?;
    if input.common.mode.is_dry_run() {
        ans.note(&format_compact!(
            "would create the controller CA for domain {domain:?} at {}{}",
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
            return Ok(None);
        }
        return offer(
            ans,
            crate::plan::service::ServiceNeed::at(ServiceScope::System),
            ServiceGate {
                dry_run: true,
                no_service: input.common.no_service,
                with_service: input.common.with_service,
            },
        )
        .await;
    }
    if external_sign {
        let config_lock = input
            .common
            .mode
            .config_lock()
            .context("controller apply mode has no config-directory lock")?;
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
        ca_setup::create_vaulted_external_ca(ans, config_lock, opts).await?;
        let record =
            InstallRecord::new(InstallRole::Controller, "/", "admin-tls", None, None);
        record.save_default_async().await?;
        ans.note(
            "controller key and recovery material installed; the controller remains \
             pending until the external PKI returns and you install its certificate",
        );
        return Ok(None);
    }
    let config_lock = input
        .common
        .mode
        .config_lock()
        .context("controller apply mode has no config-directory lock")?;
    let (_ca, need, identity) = create_self_signed_controller(
        ans,
        config_lock,
        domain,
        input.listen,
        input.listen.map(|a| a.ip()),
        units_dir,
        input.insecure_no_tpm,
    )
    .await?;
    let cfg_path = paths::discover_admin_server_config_async().await.context(
        "the controller CA was created but its admin-server config is missing",
    )?;
    let cfg =
        crate::admin_server_config::AdminServerConfig::load_async(&cfg_path).await?;
    InstallRecord::new(
        InstallRole::Controller,
        "/",
        "admin-tls",
        Some(identity),
        Some(cfg.listen),
    )
    .save_default_async()
    .await?;
    offer(
        ans,
        need,
        ServiceGate {
            dry_run: false,
            no_service: input.common.no_service,
            with_service: input.common.with_service,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guided_controller_install_asks_about_external_signing() {
        let input = ControllerInput::defaults(InstallCommon {
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
