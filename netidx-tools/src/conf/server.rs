//! `netidx conf component server …` — run and set up the conf-server daemon.
//!
//! The daemon itself lives in `netidx_conf::conf_server`; this module
//! is the CLI shell plus [`setup_server`], the shared "stand up a conf
//! server on this host" step used by `ca init` and the install flows.

use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand};
use netidx_conf::{
    atomic,
    ca::{self, Ca, SanEntry},
    conf_client,
    conf_proto::{self, NodeKind, SERVING_SAN},
    conf_server,
    conf_server_config::{CaRole, ConfServerConfig, Roles},
    netshape::NetShape,
    paths,
};
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

use super::{prompt, service};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// run the conf server (answers discovery/info queries and, on the
    /// CA host, signs CSRs received over TLS)
    Run(RunArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    /// Path to `conf-server.json`. Defaults to the standard search
    /// order (user config dir, then the system location).
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    /// Don't daemonize (run in the foreground).
    #[arg(short, long)]
    #[allow(dead_code)]
    pub foreground: bool,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Run(p) => run_server(p),
    }
}

fn run_server(p: RunArgs) -> Result<()> {
    env_logger::init();
    let cfg_path = match p.config {
        Some(c) => c,
        None => paths::discover_conf_server_config()?,
    };
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    rt.block_on(conf_server::serve(cfg_path))
}

/// Inputs to [`setup_server`].
pub(super) struct SetupArgs<'a> {
    pub ca_dir: &'a Path,
    pub ca: &'a Ca,
    /// The network's TLS domain — what discovery groups by.
    pub domain: &'a str,
    /// Explicit `--listen` (skips the prompt).
    pub listen: Option<SocketAddr>,
    /// IP to suggest when prompting (e.g. the resolver being created
    /// in the same flow). `None` ⇒ an existing resolver's IP, then the
    /// public IP.
    pub listen_hint: Option<IpAddr>,
    /// Where to drop the `conf-server` activation unit. `None` ⇒ don't
    /// write a unit (e.g. `--no-units`); the server can still be run
    /// manually.
    pub units_dir: Option<&'a Path>,
}

/// Stand up a conf server on the CA host: issue the daemon's serving
/// cert (a CA-issued leaf with the reserved [`SERVING_SAN`]), write
/// `conf-server.json` (ca role only — install flows add resolver /
/// id-map roles as they set those up), and (when `units_dir` is set)
/// drop the `conf-server` activation unit. Returns the
/// [`ServiceNeed`](service::ServiceNeed) the caller folds into its
/// single end-of-process service offer — a conf server is a network
/// daemon, so it needs a **system**-scope service when a unit was
/// written, or `NONE` when `--no-units` left the operator to run it.
pub(super) fn setup_server(a: SetupArgs) -> Result<service::ServiceNeed> {
    let server_dir = a.ca_dir.join("server");
    std::fs::create_dir_all(&server_dir)
        .with_context(|| format!("creating {}", server_dir.display()))?;
    // Generate the serving key + CSR (ECDSA) and have the CA sign it.
    // This is bootstrap — before the daemon owns the CA — so record the
    // issuance (allocating the serial from the store) the way the daemon
    // would, keeping the serving cert's serial unique and the daemon's
    // startup counter seeded past it.
    let kc = conf_client::generate_key_and_csr(SERVING_SAN)?;
    let leaf = super::ca::sign_and_record(
        a.ca,
        NodeKind::ConfServer,
        kc.csr_pem.as_bytes(),
        &[SanEntry::Dns(SERVING_SAN.to_string())],
        SERVING_SAN,
        ca::DEFAULT_LEAF_VALIDITY_DAYS,
    )
    .context("signing the conf server's serving certificate")?;
    let ca_cert = std::fs::read(a.ca_dir.join("certificate.pem"))?;
    // Chain = [serving leaf, ca cert] so the client receives the CA.
    let mut chain = leaf;
    chain.extend_from_slice(&ca_cert);
    let serving_cert = server_dir.join("cert.pem");
    let serving_key = server_dir.join("key.pem");
    atomic::write_atomic(&serving_cert, &chain, 0o644)?;
    match netidx_conf::tls::write_private_key_maybe_sealed(
        &serving_key,
        &kc.private_key_pem,
    )? {
        netidx_conf::tls::KeyWrite::Sealed => {
            println!("  serving key sealed to this machine's {}", netidx_tpm::MECHANISM);
        }
        netidx_conf::tls::KeyWrite::Plain(e) => {
            println!(
                "  note: serving key is plaintext ({} sealing unavailable: {e:#})",
                netidx_tpm::MECHANISM
            );
        }
    }

    // Ask for the listen IP and port. The IP defaults to the resolver
    // this conf server is being set up alongside (or an existing
    // resolver's listen address, or the machine's first public IP);
    // the port defaults to the conventional 4565. An explicit
    // `--listen` skips the prompt.
    let listen = match a.listen {
        Some(addr) => addr,
        None => {
            let ip_default = default_listen_ip(a.listen_hint);
            let ip = prompt::parsed_with_default::<IpAddr>(
                "conf server listen IP",
                None,
                &ip_default.to_string(),
            )?;
            let port = prompt::parsed_with_default::<u16>(
                "conf server listen port",
                None,
                &conf_proto::DEFAULT_PORT.to_string(),
            )?;
            SocketAddr::new(ip, port)
        }
    };
    // The trust anchor for peer verification: the maintained federation
    // bundle when one exists, else the CA's own cert.
    let trusted = {
        let bundle = a.ca_dir.join("trusted.pem");
        if bundle.is_file() { bundle } else { a.ca_dir.join("certificate.pem") }
    };
    let cfg = ConfServerConfig {
        domain: a.domain.to_string(),
        listen,
        serving_cert,
        serving_key,
        trusted,
        roles: Roles {
            ca: Some(CaRole { dir: a.ca_dir.to_path_buf(), autorenew: None }),
            resolver: None,
            id_map: None,
        },
        ca_addr: None,
        peers: Vec::new(),
        mdns: true,
        activation_units_dir: None,
    };
    let cfg_path = paths::user_conf_server_config()?;
    cfg.save(&cfg_path)?;

    println!();
    println!("conf server configured:");
    println!("  config:   {}", cfg_path.display());
    println!("  listen:   {listen}");
    println!("  domain:   {}", a.domain);

    // Drop the activation unit so the supervisor runs the daemon — but
    // only when we have a units dir. With `--no-units` the operator
    // wires activation themselves, so there's no system service to
    // offer for it.
    let Some(units_dir) = a.units_dir else {
        println!(
            "  (--no-units: no activation unit written; run it yourself with\n\
             \x20  netidx conf component server run -c {})",
            cfg_path.display()
        );
        return Ok(service::ServiceNeed::NONE);
    };
    install_unit(units_dir, &cfg_path)?;
    // A conf server is a network daemon → system-scope service.
    Ok(service::ServiceNeed::at(service::ScopeArg::System))
}

/// Write the `conf-server` activation unit into `units_dir`, pointing
/// at the config at `cfg_path`. Shared by [`setup_server`] (the CA
/// host) and the resolver install's enrollment path (everyone else).
pub(super) fn install_unit(units_dir: &Path, cfg_path: &Path) -> Result<()> {
    std::fs::create_dir_all(units_dir)
        .with_context(|| format!("creating activation dir {}", units_dir.display()))?;
    let netidx_binary = std::env::current_exe()
        .context("could not determine current netidx binary for the conf-server unit")?;
    let unit = netidx_conf::template::services::conf_server::unit(
        &netidx_conf::template::services::conf_server::ConfServerServiceParams {
            netidx_binary,
            config: cfg_path.to_path_buf(),
        },
    )?;
    let dir = netidx_conf::activation::ActivationDir::open(Some(units_dir))?;
    dir.save("conf-server", &unit).context("writing the conf-server activation unit")?;
    println!(
        "  unit:     {}",
        netidx_conf::activation::unit_path_in(units_dir, "conf-server").display()
    );
    Ok(())
}

/// Add or replace roles on this host's conf-server config — the
/// install flows call this after standing up the resolver / id-map so
/// the daemon advertises what actually runs here. A missing config is
/// an error: roles only make sense on a host that has one.
pub(super) fn update_roles(
    update: impl FnOnce(&mut Roles),
) -> Result<PathBuf> {
    let cfg_path = paths::discover_conf_server_config()?;
    let mut cfg = ConfServerConfig::load(&cfg_path)?;
    update(&mut cfg.roles);
    cfg.save(&cfg_path)?;
    Ok(cfg_path)
}

/// Point this host's CA role at the autorenew slot's `keytab`, so the
/// running conf-server daemon approves verified renewals in-process. The
/// config must already exist and hold a CA role — autorenew is a CA-host
/// feature, and the keytab path is all the daemon needs to read the slot.
pub(super) fn set_ca_autorenew(keytab: &Path) -> Result<PathBuf> {
    let cfg_path = paths::discover_conf_server_config()?;
    let mut cfg = ConfServerConfig::load(&cfg_path)?;
    let ca = cfg
        .roles
        .ca
        .as_mut()
        .ok_or_else(|| anyhow!("conf-server config {} has no CA role", cfg_path.display()))?;
    ca.autorenew = Some(keytab.to_path_buf());
    cfg.save(&cfg_path)?;
    Ok(cfg_path)
}

/// IP to suggest for the conf server's listen address: the resolver
/// being created in this same flow (`hint`), else an existing
/// resolver's listen IP, else the machine's first public IP. The conf
/// server usually co-locates with a resolver, so its address is the
/// resolver's.
fn default_listen_ip(hint: Option<IpAddr>) -> IpAddr {
    hint.or_else(existing_resolver_listen_ip)
        .unwrap_or_else(|| NetShape::detect().advertised_ip().into())
}

/// The listen IP of the default resolver config, if one is present and
/// parseable.
fn existing_resolver_listen_ip() -> Option<IpAddr> {
    netidx_conf::resolver::ResolverConfig::load_default()
        .ok()
        .and_then(|c| c.0.member_servers.first().map(|m| m.addr.ip()))
}
