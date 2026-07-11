use super::answer_cli::{FlagAnswerer, RemoteAuthFlags, parse_glyph};
use anyhow::{Context, Result, bail};
use clap::Args;
use netidx_admin::{
    admin_client,
    admin_proto::{NodeKind, ServerState},
    answer::Answerer,
    fingerprint::Fingerprint,
    paths,
    session_cache::{self, CachedSession},
};
use std::{net::SocketAddr, path::Path};
use zeroize::Zeroizing;

fn local_admin_server() -> Option<SocketAddr> {
    #[cfg(unix)]
    {
        let path = paths::discover_admin_server_config().ok()?;
        netidx_admin::admin_server_config::AdminServerConfig::load(&path)
            .ok()
            .map(|cfg| cfg.listen)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

async fn resolve_controller(
    answerer: &mut FlagAnswerer,
    bootstrap: SocketAddr,
    ca_dir: Option<&Path>,
) -> Result<(SocketAddr, admin_client::CaIdentity)> {
    let identity = admin_client::fetch_identity(bootstrap, NodeKind::Client)
        .await
        .with_context(|| format!("contacting admin server {bootstrap}"))?;
    let local_fp = ca_dir
        .map(Path::to_path_buf)
        .or_else(|| paths::user_ca_dir().ok())
        .and_then(|d| std::fs::read(d.join("certificate.pem")).ok())
        .and_then(|pem| Fingerprint::of_cert_pem(&pem).ok());
    match local_fp {
        Some(fp) if fp == identity.fingerprint => {
            answerer.note(&format!("verified {bootstrap} against the local CA"));
        }
        _ if !answerer.confirm_identity(&identity).await? => {
            bail!("the admin server's identity was not confirmed; nothing was sent")
        }
        _ => {}
    }
    let map =
        admin_client::get_map_pinned(bootstrap, NodeKind::Client, &identity).await?;
    let controller = map
        .controller_entry()
        .filter(|s| s.state == ServerState::Registered)
        .context("the authoritative map has no registered controller")?;
    let controller_addr = controller.addr;
    let controller_identity =
        admin_client::fetch_identity(controller_addr, NodeKind::Client).await?;
    if controller_identity.fingerprint != identity.fingerprint
        || !controller_identity.controller
        || controller_identity.server_id != map.controller
    {
        bail!("the map's controller candidate failed exact home-CA verification");
    }
    Ok((controller_addr, controller_identity))
}

pub(crate) fn login(flags: RemoteAuthFlags) -> Result<()> {
    let mut answerer =
        FlagAnswerer::single(None, parse_glyph(flags.accept_glyph.as_deref())?);
    let bootstrap = flags.server_addr()?.or_else(local_admin_server).context(
        "no admin server specified and none found on this host — pass --server <ip:port>",
    )?;
    let runtime = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let (server, identity) = runtime.block_on(resolve_controller(
        &mut answerer,
        bootstrap,
        flags.ca_dir.as_deref(),
    ))?;
    let admin = flags
        .admin
        .clone()
        .or_else(|| std::env::var("USER").ok())
        .or_else(|| std::env::var("USERNAME").ok())
        .context("an administrator name is required (pass --admin)")?;
    let password = match flags.password()? {
        Some(password) => password,
        None => Zeroizing::new(rpassword::prompt_password("Administrator password: ")?),
    };
    let logged =
        runtime.block_on(admin_client::login(server, &identity, &admin, &password))?;
    let fingerprint = identity.fingerprint.text();
    session_cache::store(CachedSession {
        ca_fingerprint: fingerprint.clone(),
        bootstrap: server,
        admin: logged.admin.clone(),
        token: logged.token,
        issued_unix: logged.issued_unix,
        absolute_deadline_unix: logged.absolute_deadline_unix,
        idle_timeout_secs: logged.idle_timeout_secs,
    })?;
    println!("logged in as {}", logged.admin);
    println!("network: {fingerprint}");
    println!("expires: @{}", logged.absolute_deadline_unix);
    Ok(())
}

#[derive(Args, Debug)]
pub(crate) struct LogoutArgs {
    /// Revoke and remove every cached network session.
    #[arg(long)]
    all: bool,
    /// Select the network by its CA glyph when more than one is cached.
    #[arg(long = "accept-glyph")]
    accept_glyph: Option<String>,
}

pub(crate) fn logout(args: LogoutArgs) -> Result<()> {
    let all = args.all;
    let sessions = session_cache::load_all()?;
    let selected: Vec<_> = if all {
        sessions
    } else if let Some(glyph) = args.accept_glyph {
        let normalized =
            netidx_admin::fingerprint::Fingerprint::parse_text(&glyph)?.text();
        sessions.into_iter().filter(|s| s.ca_fingerprint == normalized).collect()
    } else {
        if sessions.len() > 1 {
            bail!(
                "more than one network session is cached; pass --accept-glyph or --all"
            );
        }
        sessions
    };
    let runtime = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    for cached in selected {
        let revoke = runtime.block_on(async {
            let identity = admin_client::fetch_identity(
                cached.bootstrap,
                netidx_admin::admin_proto::NodeKind::Client,
            )
            .await?;
            if identity.fingerprint.text() != cached.ca_fingerprint {
                bail!("cached bootstrap now presents a different CA");
            }
            admin_client::logout(cached.bootstrap, &identity, cached.token.as_str()).await
        });
        if let Err(e) = revoke {
            eprintln!(
                "warning: could not revoke {} remotely: {e:#}",
                cached.ca_fingerprint
            );
        }
        session_cache::delete(&cached.ca_fingerprint)?;
        println!("logged out {}", cached.ca_fingerprint);
    }
    if all {
        session_cache::delete_all()?;
    }
    Ok(())
}
