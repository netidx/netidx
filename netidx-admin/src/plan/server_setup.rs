//! The "stand up an admin server on this host" step, lifted out of the
//! `netidx-tools` CLI into the library.
//!
//! Shared by `ca init` and the resolver install flows: issue the daemon's
//! serving cert (a CA-issued leaf with the reserved [`SERVING_SAN`]), write
//! `admin-server.json`, and (when a units dir is given) drop the
//! `admin-server` activation unit. Every question and status goes through the
//! [`Answerer`] seam — no `prompt::`, no `println!` — so the strict CLI, the
//! TUI, and Atlas drive the identical flow. The daemon launcher itself
//! (`server run`) stays in the CLI shell.

use crate::{
    activation, admin_client,
    admin_proto::{self, AdminServerId, CONTROLLER_ROLE_URI, NodeKind, SERVING_SAN},
    admin_server_config::{AdminServerConfig, CaRole, Roles},
    answer::{Answerer, Field},
    atomic,
    ca::{self, Ca, SanEntry},
    config_lock::ConfigDirLock,
    offline_ca, paths,
    plan::service::ServiceNeed,
    service::ServiceScope,
    tls,
};
use anyhow::{Context, Result, anyhow};
use compact_str::format_compact;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

/// Inputs to [`setup_server`].
pub struct SetupArgs<'a> {
    pub ca_dir: &'a Path,
    pub ca: &'a Ca,
    pub config_lock: ConfigDirLock,
    /// The network's TLS domain — what discovery groups by.
    pub domain: &'a str,
    /// Explicit `--listen` (skips the prompt).
    pub listen: Option<SocketAddr>,
    /// IP to suggest when prompting (e.g. the resolver being created
    /// in the same flow). `None` ⇒ an existing resolver's IP, then the
    /// public IP.
    pub listen_hint: Option<IpAddr>,
    /// Where to drop the `admin-server` activation unit. `None` ⇒ don't
    /// write a unit (e.g. `--no-units`); the server can still be run
    /// manually.
    pub units_dir: Option<&'a Path>,
}

/// Stand up an admin server on the CA host: issue the daemon's serving
/// cert (a CA-issued leaf with the reserved [`SERVING_SAN`]), write
/// `admin-server.json` (ca role only — install flows add resolver /
/// id-map roles as they set those up), and (when `units_dir` is set)
/// drop the `admin-server` activation unit. Returns the [`ServiceNeed`]
/// the caller folds into its single end-of-process service offer — an
/// admin server is a network daemon, so it needs a **system**-scope
/// service when a unit was written, or `NONE` when `--no-units` left the
/// operator to run it.
pub async fn setup_server(
    ans: &mut dyn Answerer,
    a: SetupArgs<'_>,
) -> Result<ServiceNeed> {
    a.config_lock.require_descendant(a.ca_dir)?;
    let cfg_path = a.config_lock.require_contained(paths::user_admin_server_config()?)?;
    let server_dir = a.ca_dir.join("server");
    tokio::fs::create_dir_all(&server_dir)
        .await
        .with_context(|| format!("creating {}", server_dir.display()))?;
    // Generate the serving key + CSR (ECDSA) and have the CA sign it.
    // This is bootstrap — before the daemon owns the CA — so record the
    // issuance (allocating the serial from the store) the way the daemon
    // would, keeping the serving cert's serial unique and the daemon's
    // startup counter seeded past it.
    let server_id = AdminServerId::new();
    let kc = admin_client::generate_key_and_csr(SERVING_SAN)?;
    let leaf = offline_ca::sign_and_record(
        &a.config_lock,
        a.ca,
        NodeKind::AdminServer,
        kc.csr_pem.as_bytes(),
        &[
            SanEntry::Dns(SERVING_SAN.to_string()),
            SanEntry::Uri(server_id.uri()),
            SanEntry::Uri(CONTROLLER_ROLE_URI.to_string()),
        ],
        SERVING_SAN,
        ca::CaLifetimes::load_async(a.ca_dir)
            .await
            .map(|l| l.leaf_validity)
            .unwrap_or(ca::DEFAULT_LEAF_VALIDITY),
    )
    .await
    .context("signing the admin server's serving certificate")?;
    let ca_cert = tokio::fs::read(a.ca_dir.join("certificate.pem")).await?;
    let home_ca_fingerprint =
        crate::fingerprint::Fingerprint::of_cert_pem(&ca_cert)?.text();
    // Chain = [serving leaf, ca cert] so the client receives the CA.
    let mut chain = leaf;
    chain.extend_from_slice(&ca_cert);
    let serving_cert = server_dir.join("cert.pem");
    let serving_key = server_dir.join("key.pem");
    atomic::write_atomic_async(&serving_cert, &chain, 0o644).await?;
    let key_write = tokio::task::spawn_blocking({
        let serving_key = serving_key.clone();
        let private_key_pem = kc.private_key_pem;
        move || tls::write_private_key_maybe_sealed(&serving_key, &private_key_pem)
    })
    .await
    .context("serving-key protection task panicked")??;
    match key_write {
        tls::KeyWrite::Sealed => {
            ans.note(&format_compact!(
                "  serving key sealed to this machine's {}",
                netidx_tpm::MECHANISM
            ));
        }
        tls::KeyWrite::Plain(e) => {
            ans.warn(&format_compact!(
                "serving key is plaintext ({} sealing unavailable: {e:#})",
                netidx_tpm::MECHANISM
            ));
        }
    }

    // Ask for the listen IP and port. The IP defaults to the resolver
    // this admin server is being set up alongside (or an existing
    // resolver's listen address, or the machine's first public IP);
    // the port defaults to the conventional 4565. An explicit
    // `--listen` skips the prompt.
    let listen = match a.listen {
        Some(addr) => addr,
        None => {
            let default_ip = default_listen_ip(a.listen_hint).await;
            // A concrete default (the co-located resolver's advertised IP, an
            // existing resolver's, or a detected public IP) is itself a valid
            // non-interactive answer — the admin server address isn't a
            // separate decision the operator must restate. Only `0.0.0.0`
            // (nothing detectable) forces an explicit value.
            let ip_required = default_ip.is_unspecified();
            let ip_default = default_ip.to_string();
            let ip = ans
                .text(Field::AdminServerListenIp, None, Some(&ip_default), ip_required)
                .await?
                .context("admin server listen IP required")?
                .parse::<IpAddr>()?;
            let port_default = admin_proto::DEFAULT_PORT.to_string();
            let port = ans
                .text(Field::AdminServerListenPort, None, Some(&port_default), false)
                .await?
                .context("admin server listen port required")?
                .parse::<u16>()?;
            SocketAddr::new(ip, port)
        }
    };
    // The trust anchor for peer verification: the maintained federation
    // bundle when one exists, else the CA's own cert.
    let trusted = {
        let bundle = a.ca_dir.join("trusted.pem");
        if tokio::fs::try_exists(&bundle).await? {
            bundle
        } else {
            a.ca_dir.join("certificate.pem")
        }
    };
    let cfg = AdminServerConfig {
        domain: a.domain.to_string(),
        server_id,
        home_ca_fingerprint,
        listen,
        serving_cert,
        serving_key,
        trusted,
        roles: Roles {
            ca: Some(CaRole {
                dir: a.ca_dir.to_path_buf(),
                autorenew: None,
                session_absolute_lifetime: None,
                session_idle_timeout: None,
            }),
            resolver: None,
            id_map: None,
        },
        ca_addr: None,
        peers: Vec::new(),
        mdns: true,
        activation_units_dir: None,
    };
    cfg.save_async(&a.config_lock, &cfg_path).await?;

    ans.note(&format_compact!(
        "admin server configured:\n\
         \x20 config:   {}\n\
         \x20 listen:   {listen}\n\
         \x20 domain:   {}",
        cfg_path.display(),
        a.domain
    ));

    // Drop the activation unit so the supervisor runs the daemon — but
    // only when we have a units dir. With `--no-units` the operator
    // wires activation themselves, so there's no system service to
    // offer for it.
    let Some(units_dir) = a.units_dir else {
        ans.note(&format_compact!(
            "  (--no-units: no activation unit written; run it yourself with\n\
             \x20  netidx admin component server run -c {})",
            cfg_path.display()
        ));
        return Ok(ServiceNeed::NONE);
    };
    install_unit(ans, units_dir, &cfg_path).await?;
    // An admin server is a network daemon → system-scope service.
    Ok(ServiceNeed::at(ServiceScope::System))
}

/// Write the `admin-server` activation unit into `units_dir`, pointing
/// at the config at `cfg_path`. Shared by [`setup_server`] (the CA
/// host) and the resolver install's enrollment path (everyone else).
pub async fn install_unit(
    ans: &mut dyn Answerer,
    units_dir: &Path,
    cfg_path: &Path,
) -> Result<()> {
    use crate::template::services::admin_server::{self, AdminServerServiceParams};
    let netidx_binary = std::env::current_exe()
        .context("could not determine current netidx binary for the admin-server unit")?;
    let unit = admin_server::unit(&AdminServerServiceParams {
        netidx_binary,
        config: cfg_path.to_path_buf(),
    })?;
    let units_dir_owned = units_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let dir = activation::ActivationDir::open(Some(&units_dir_owned))?;
        dir.save("admin-server", &unit)
            .context("writing the admin-server activation unit")
    })
    .await
    .context("activation-unit write task panicked")??;
    ans.note(&format_compact!(
        "  unit:     {}",
        activation::unit_path_in(units_dir, "admin-server").display()
    ));
    Ok(())
}

/// Add or replace roles on this host's admin-server config — the
/// install flows call this after standing up the resolver / id-map so
/// the daemon advertises what actually runs here. A missing config is
/// an error: roles only make sense on a host that has one.
pub async fn update_roles(update: impl FnOnce(&mut Roles)) -> Result<PathBuf> {
    let cfg_path = paths::discover_admin_server_config_async().await?;
    let lock = ConfigDirLock::acquire_for_file_async(&cfg_path).await?;
    let mut cfg = AdminServerConfig::load_async(&cfg_path).await?;
    update(&mut cfg.roles);
    cfg.save_async(&lock, &cfg_path).await?;
    Ok(cfg_path)
}

/// Point this host's CA role at the autorenew slot's `keytab`, so the
/// running admin-server daemon approves verified renewals in-process. The
/// config must already exist and hold a CA role — autorenew is a CA-host
/// feature, and the keytab path is all the daemon needs to read the slot.
pub async fn set_ca_autorenew(
    config_lock: &ConfigDirLock,
    keytab: &Path,
) -> Result<PathBuf> {
    let keytab = config_lock.require_contained(keytab)?;
    let cfg_path = paths::discover_admin_server_config_async().await?;
    anyhow::ensure!(
        config_lock.contains(&cfg_path)?,
        "admin-server config {} is outside locked config directory {}",
        cfg_path.display(),
        config_lock.root().display()
    );
    let mut cfg = AdminServerConfig::load_async(&cfg_path).await?;
    let ca = cfg.roles.ca.as_mut().ok_or_else(|| {
        anyhow!("admin-server config {} has no CA role", cfg_path.display())
    })?;
    ca.autorenew = Some(keytab);
    cfg.save_async(config_lock, &cfg_path).await?;
    Ok(cfg_path)
}

/// IP to suggest for the admin server's listen address: the resolver
/// being created in this same flow (`hint`), else an existing
/// resolver's listen IP, else the machine's first public IP. The admin
/// server usually co-locates with a resolver, so its address is the
/// resolver's.
async fn default_listen_ip(hint: Option<IpAddr>) -> IpAddr {
    if let Some(ip) = hint {
        return ip;
    }
    if let Some(ip) = existing_resolver_listen_ip().await {
        return ip;
    }
    crate::plan::install::detected_advertised_ip()
        .await
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
}

/// The listen IP of the default resolver config, if one is present and
/// parseable.
async fn existing_resolver_listen_ip() -> Option<IpAddr> {
    let path = paths::discover_resolver_config().ok()?;
    crate::resolver::ResolverConfig::load_async(path)
        .await
        .ok()
        .and_then(|c| c.0.member_servers.first().map(|m| m.addr.ip()))
}
