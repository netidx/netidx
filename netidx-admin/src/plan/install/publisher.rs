//! The publisher role install: a client config for a host that publishes.
//!
//! A publisher never mints a CA or an admin server — it either joins a
//! discovered network (enrolling a TLS cert over the admin plane) or is
//! configured against explicit resolver addresses. The one daemon it may run
//! is the certificate-renewal supervisor, for TLS setups.

use super::{
    InstallCommon, finish_with, install_renew_unit, network_provenance,
    prompt_resolver_port, prompt_resolver_tls_name, publisher_bind_shape,
    resolve_units_dir, suggest_client_san,
};
use crate::{
    admin_proto::NodeKind,
    answer::{Answerer, Field},
    plan::{
        AuthKind,
        enroll::{self, AdminServers, KeyProtArg},
        service::ServiceNeed,
    },
    provenance::{InstallRecord, InstallRole},
    service::ServiceScope,
    template::{self, ReferralAuth},
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

/// Typed inputs for [`run_publisher`] — the resolved form of the clap
/// `PublisherFlags` (the clap struct stays in the CLI frontend).
pub struct PublisherInput {
    /// Cluster addresses (empty ⇒ discover or prompt).
    pub addrs: Vec<SocketAddr>,
    /// Data-plane auth scheme (`None` ⇒ discover or prompt).
    pub auth: Option<AuthKind>,
    /// Enroll against this admin server (`--admin-server`) instead of mDNS
    /// discovery — the non-interactive join path. On a TLS network this
    /// enrolls a client certificate; the presented identity is confirmed via
    /// `--accept-glyph`. Takes precedence over `--addr`/`--auth`.
    pub admin_server: Option<SocketAddr>,
    /// Resolver's Kerberos SPN (krb5).
    pub spn: Option<String>,
    /// Resolver's local-auth socket path (local).
    pub socket: Option<PathBuf>,
    /// Server's TLS name (tls).
    pub tls_server_name: Option<String>,
    /// Override `default_auth` on the client config.
    pub default_auth: Option<AuthKind>,
    /// Namespace base path.
    pub base: String,
    /// Client config output path.
    pub config_path: Option<PathBuf>,
    /// `default_bind_config` string (`None` ⇒ suggest / prompt).
    pub bind: Option<String>,
    /// Where to drop the renewal-daemon unit (`None` ⇒ default dir).
    pub units_dir: Option<PathBuf>,
    /// Private-key protection choice.
    pub key_protection: Option<KeyProtArg>,
    /// Install-wide flags.
    pub common: InstallCommon,
}

/// Per-resolver referral auth for the manual (non-discovery) path: `auth` was
/// already resolved; the per-scheme sub-args are required once a scheme is
/// chosen.
async fn publisher_per_addr_auth(
    ans: &mut dyn Answerer,
    auth: AuthKind,
    socket: Option<PathBuf>,
    spn: Option<String>,
    tls_server_name: Option<String>,
    first_addr: Option<SocketAddr>,
) -> Result<ReferralAuth> {
    Ok(match auth {
        AuthKind::Anonymous => ReferralAuth::Anonymous,
        AuthKind::Local => {
            let path = ans
                .text(
                    Field::Socket,
                    socket.map(|p| p.to_string_lossy().into_owned()),
                    None,
                    true,
                )
                .await?
                .context("a local-auth socket path is required")?;
            ReferralAuth::Local(ArcStr::from(path.as_str()))
        }
        AuthKind::Krb5 => {
            let spn = ans
                .text(Field::Spn, spn, None, true)
                .await?
                .context("the resolver's Kerberos SPN is required")?;
            ReferralAuth::Krb5(ArcStr::from(spn.as_str()))
        }
        AuthKind::Tls => {
            let name = prompt_resolver_tls_name(
                ans,
                first_addr,
                Field::TlsName,
                tls_server_name,
            )
            .await?;
            ReferralAuth::Tls(ArcStr::from(name.as_str()))
        }
    })
}

/// Install a publisher client config, returning the OS-service scope the
/// frontend should register (or `None`).
pub async fn run_publisher(
    ans: &mut dyn Answerer,
    input: PublisherInput,
) -> Result<Option<ServiceScope>> {
    let PublisherInput {
        mut addrs,
        mut auth,
        admin_server,
        spn,
        socket,
        tls_server_name,
        default_auth,
        base,
        config_path,
        bind,
        units_dir,
        key_protection,
        common,
    } = input;

    let mut tls_identities = vec![];
    // Staging tempdirs for any admin-server-joined identity must outlive the
    // `finish_with` call (dropping a TempDir deletes its contents).
    let mut tls_staging: Vec<tempfile::TempDir> = Vec::new();
    // Ask the network before asking the human: with no `--addr` / `--auth`, a
    // discovered (glyph-confirmed) admin server yields every resolver address
    // with its auth — and, on TLS networks, our client cert.
    let probe = if let Some(addr) = admin_server {
        enroll::confirm_network_at(ans, addr, NodeKind::Publisher).await?
    } else if addrs.is_empty() && auth.is_none() {
        enroll::discover_network(ans, NodeKind::Publisher).await?
    } else {
        AdminServers::NotProbed
    };
    let resolved_addrs: Vec<(SocketAddr, ReferralAuth)> = match probe.have() {
        Some(net) => {
            let have_identity = !tls_identities.is_empty();
            enroll::network_addrs_and_identity(
                ans,
                net,
                NodeKind::Publisher,
                have_identity,
                key_protection,
                &mut tls_identities,
                &mut tls_staging,
            )
            .await?
        }
        None => {
            let kind: AuthKind = ans
                .choice(
                    Field::Auth,
                    auth.map(|k| k.as_str().to_string()),
                    &["anonymous", "local", "krb5", "tls"],
                    Some("tls"),
                )
                .await?
                .parse()?;
            auth = Some(kind);
            if addrs.is_empty() {
                let ip: IpAddr = ans
                    .text(Field::ResolverAddr, None, None, true)
                    .await?
                    .context("the cluster IP (resolver to connect to) is required")?
                    .trim()
                    .parse()
                    .context("invalid cluster IP")?;
                addrs.push(prompt_resolver_port(ans, ip, None).await?);
            }
            let per_addr_auth = publisher_per_addr_auth(
                ans,
                kind,
                socket,
                spn,
                tls_server_name,
                addrs.first().copied(),
            )
            .await?;
            if tls_identities.is_empty() && matches!(auth, Some(AuthKind::Tls)) {
                // `--auth tls` with no identity: enroll one over the admin
                // plane (or bail if no admin server is reachable), mirroring
                // the workstation/resolver UX. Suggest `<user>.<domain>` from
                // the resolver's TLS name.
                let suggested = match &per_addr_auth {
                    ReferralAuth::Tls(san) => suggest_client_san(san),
                    _ => None,
                };
                let si = enroll::prompt_tls_client_identity(
                    ans,
                    suggested.as_deref(),
                    key_protection,
                    &probe,
                )
                .await?;
                tls_identities.push(si.spec);
                tls_staging.extend(si.staging);
            }
            addrs.iter().map(|a| (*a, per_addr_auth.clone())).collect()
        }
    };
    let default_auth = default_auth.map(|k| k.default_mech());
    // Level-1 prompt: leaving bind unset lands the publisher on
    // `BindCfg::Local` = 127.0.0.1, which a non-loopback resolver rejects. The
    // suggestion is the discovered NIC's subnet (or the Elastic form on a
    // cloud VM). Only probe the environment when a default is actually needed.
    let bind: String = if let Some(b) = bind {
        b
    } else {
        let (suggestion, needs_hint) = publisher_bind_shape().await;
        if needs_hint {
            ans.warn(
                "detected a container environment with no NETIDX_PUBLIC_IP env \
                 var and no reachable cloud metadata service. The suggested bind \
                 contains a <PUBLIC_IP> placeholder you must replace with the \
                 externally-visible IP this publisher's traffic appears from (or \
                 set NETIDX_PUBLIC_IP / pass --bind).",
            );
        }
        let answer = ans
            .text(Field::Bind, None, suggestion.as_deref(), false)
            .await?
            .unwrap_or_default();
        if answer.trim().is_empty() {
            bail!(
                "a publisher bind (BindCfg) is required; pass --bind \
                 (e.g. 10.0.0.0/24, 10.0.0.5/32, local)"
            );
        }
        if answer.contains('<') {
            bail!(
                "publisher bind contains a placeholder ({answer:?}); set \
                 NETIDX_PUBLIC_IP in the environment or pass --bind with the \
                 public IP filled in"
            );
        }
        answer
    };
    let default_bind_config = Some(bind);
    // TLS publishers get the renewal daemon (certificates expire); everything
    // else stays service-free.
    let has_tls = !tls_identities.is_empty();
    let units_dir = if has_tls {
        resolve_units_dir(common.no_units, units_dir.as_deref())?
    } else {
        None
    };
    let need = if units_dir.is_some() {
        // A publisher is typically a headless host, so a system service that
        // starts at boot is the right default, like the resolver.
        ServiceNeed::at(ServiceScope::System)
    } else {
        ServiceNeed::NONE
    };
    let (network, admin_server) = network_provenance(&probe);
    // On the discovery/auto-import path `auth` is never set — the scheme comes
    // from the network's per-referral auths — so fall back to what we actually
    // configured rather than defaulting the record to "tls".
    let record_auth = auth
        .map(|k| k.as_str())
        .or_else(|| resolved_addrs.first().map(|(_, ra)| ra.scheme_str()))
        .unwrap_or("tls");
    let record = InstallRecord::new(
        InstallRole::Publisher,
        base.clone(),
        record_auth,
        network,
        admin_server,
    );
    let params = template::publisher::PublisherParams {
        addrs: resolved_addrs,
        default_auth,
        tls_identities,
        base: ArcStr::from(base),
        config_path,
        default_bind_config,
    };
    let rt = template::publisher(&params)?;
    finish_with(ans, rt, &common, need, record, async move |ans| match &units_dir {
        Some(d) => install_renew_unit(ans, d),
        None => Ok(()),
    })
    .await
}
