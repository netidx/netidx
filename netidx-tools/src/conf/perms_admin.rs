//! `netidx conf perms show|edit --at <path>` — remote permissions
//! administration through the conf server, routed by the network map. No
//! SSH: an admin contacts a conf server (glyph-confirming its CA exactly as
//! delegation does), the map locates the cluster mounted at `<path>`, and
//! `show` reads that cluster's perms while `edit` opens them in `$EDITOR`,
//! validates, and hands the result to the CA — which authenticates the
//! admin and propagates the validated file to every cluster member.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin::{
    conf_client::{self, CaIdentity},
    conf_proto::{NetworkMap, NodeKind, PeerResult},
    perms,
};
use std::{collections::BTreeSet, net::SocketAddr};
use zeroize::Zeroizing;

use super::{
    ca::{collect_existing_password, env_user_name, local_conf_server_listen},
    editor, init, prompt,
};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// show the permissions of the cluster mounted at <path>
    Show(Flags),
    /// edit the permissions of the cluster mounted at <path> (admin)
    Edit(Flags),
}

#[derive(Args, Debug)]
pub(crate) struct Flags {
    /// A conf server to reach the network through: a hostname or IP, with
    /// or without a `:port` (the conf port defaults to 4565). Defaults to
    /// this host's own conf server, else prompted.
    #[arg(long = "server")]
    server: Option<String>,
    /// The hierarchy path whose cluster's perms to act on (e.g. `/eu`, or
    /// `/` for the root cluster). Prompted when omitted.
    #[arg(long = "at")]
    at: Option<String>,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Show(f) => show(f),
        Cmd::Edit(f) => edit(f),
    }
}

/// A glyph-confirmed conf server plus the network map fetched from it.
struct Bootstrap {
    rt: tokio::runtime::Runtime,
    addr: SocketAddr,
    id: CaIdentity,
    map: NetworkMap,
}

/// Reach a conf server (flag, else this host's own, else prompted),
/// confirm its CA glyph (the one human trust decision), and pull the map.
fn bootstrap(server: Option<String>) -> Result<Bootstrap> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let addr = match server {
        Some(s) => init::resolve_conf_server_addr(&s)?,
        None => match local_conf_server_listen() {
            Some(a) => a,
            None => prompt::required_with(
                "conf-server address (host or ip, optional :port, e.g. \
                 203.0.113.1:4565)",
                init::resolve_conf_server_addr,
            )?,
        },
    };
    let id = rt
        .block_on(conf_client::fetch_identity(addr, NodeKind::Client))
        .with_context(|| format!("contacting conf server {addr}"))?;
    init::show_network_identity(addr, &id);
    if !prompt::confirm("is this your network's conf server?", false)? {
        bail!("conf-server identity was not confirmed; nothing was sent");
    }
    let map = rt
        .block_on(conf_client::get_map_pinned(addr, NodeKind::Client, &id))
        .context("fetching the network map")?;
    Ok(Bootstrap { rt, addr, id, map })
}

/// The conf server of the cluster mounted exactly at `at` (the same
/// exact-base match the CA uses to route the edit).
fn route(map: &NetworkMap, at: &str) -> Result<SocketAddr> {
    let mut bases = BTreeSet::new();
    for s in &map.servers {
        if let Some(c) = &s.cluster {
            if c.base == at {
                return Ok(s.addr);
            }
            bases.insert(c.base.as_str());
        }
    }
    bail!(
        "no resolver cluster is mounted at {at:?} in the network map. Known \
         cluster bases: {}",
        bases.into_iter().collect::<Vec<_>>().join(", ")
    )
}

/// Run `f` against `target` with an identity pinned to the SAME CA the
/// operator already confirmed. The map could route us at an impostor, so a
/// target presenting a different CA cert is refused. Reuses the bootstrap
/// identity when the target is the bootstrap host (no re-confirm).
fn with_same_ca<T>(
    bs: &Bootstrap,
    target: SocketAddr,
    f: impl FnOnce(&CaIdentity) -> Result<T>,
) -> Result<T> {
    if target == bs.addr {
        return f(&bs.id);
    }
    let tid = bs
        .rt
        .block_on(conf_client::fetch_identity(target, NodeKind::Client))
        .with_context(|| format!("contacting conf server {target}"))?;
    if tid.fingerprint != bs.id.fingerprint {
        bail!(
            "the conf server at {target} presents a DIFFERENT CA than the one \
             you confirmed — refusing to trust where the map routed us."
        );
    }
    f(&tid)
}

fn show(f: Flags) -> Result<()> {
    let at = prompt::required_string(
        "the hierarchy path whose perms to show (e.g. /eu, or / for root)",
        f.at,
    )?;
    let bs = bootstrap(f.server)?;
    let target = route(&bs.map, &at)?;
    let perms_json = with_same_ca(&bs, target, |id| {
        bs.rt.block_on(conf_client::get_perms(target, NodeKind::Client, id))
    })?;
    println!("{}", pretty(&perms_json)?);
    Ok(())
}

fn edit(f: Flags) -> Result<()> {
    let at = prompt::required_string(
        "the hierarchy path whose perms to edit (e.g. /eu, or / for root)",
        f.at,
    )?;
    let bs = bootstrap(f.server)?;
    let ca_addr = bs.map.ca_addr.context(
        "the network map records no CA address — cannot route an authenticated edit",
    )?;
    // Seed the editor with the cluster's current perms.
    let target = route(&bs.map, &at)?;
    let current = with_same_ca(&bs, target, |id| {
        bs.rt.block_on(conf_client::get_perms(target, NodeKind::Client, id))
    })?;
    let edited = editor::edit_with_validation(&pretty(&current)?, validate)?;
    // Authenticate to the CA, which performs the edit and propagates it.
    let admin = match env_user_name() {
        Some(user) => prompt::string_with_default("admin name", None, &user)?,
        None => prompt::required_string("admin name", None)?,
    };
    let password = Zeroizing::new(collect_existing_password(&format!(
        "CA password for admin {admin:?}"
    ))?);
    let peers = with_same_ca(&bs, ca_addr, |id| {
        bs.rt.block_on(conf_client::edit_perms(
            ca_addr,
            NodeKind::Client,
            id,
            &admin,
            password.as_str(),
            &at,
            &edited,
        ))
    })?;
    report_peers(&peers, &at);
    Ok(())
}

/// Validate edited perms JSON in the editor loop: it must parse as a PMap
/// and every entry's bits must be valid. Returns the normalized JSON to
/// send. The CA re-validates the whole resolver config server-side; this
/// just gives a fast local re-edit on an obvious mistake.
fn validate(s: &str) -> Result<String> {
    let pmap: perms::PMap = serde_json::from_str(s).context("not valid perms JSON")?;
    for (path, entity, bits) in perms::iter(&pmap) {
        netidx::resolver_server::auth::Permissions::try_from(bits.as_str())
            .with_context(|| {
                format!("invalid permission bits {bits:?} for {entity} at {path}")
            })?;
    }
    serde_json::to_string(&pmap).context("serializing perms")
}

/// Pretty-print perms JSON for display / editor seeding.
fn pretty(perms_json: &str) -> Result<String> {
    let v: serde_json::Value =
        serde_json::from_str(perms_json).context("parsing perms JSON")?;
    serde_json::to_string_pretty(&v).context("formatting perms JSON")
}

fn report_peers(peers: &[PeerResult], at: &str) {
    let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
    if failed.is_empty() {
        println!("ok — perms at {at:?} updated on {} cluster member(s).", peers.len());
        println!("  restart the resolver server(s) to load the new perms.");
        return;
    }
    println!(
        "perms at {at:?}: {} of {} member(s) could NOT be updated:",
        failed.len(),
        peers.len()
    );
    for p in &failed {
        println!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("?"));
    }
    println!(
        "  the cluster is INCONSISTENT. The edit is idempotent — re-run \
         `perms edit --at {at}` once the member(s) are back to converge."
    );
}
