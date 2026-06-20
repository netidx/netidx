//! Lifecycle ops on an *already-installed* role: `status` (read-only)
//! and `update` (additive reconcile against the network). Shared by the
//! per-role command modules under [`super::roles`].
//!
//! The security-critical step is [`fetch_network_pinned`]: these ops
//! trust a conf server's picture of the network, so they first re-pin to
//! the **same** CA identity the operator glyph-confirmed at install
//! (stored in the [`InstallRecord`]). A conf server whose CA fingerprint
//! doesn't match the pin is refused before anything is read or changed.

use anyhow::{Context, Result};
use clap::Args;
use netidx_conf::{
    conf_client::{self, NetworkInfo},
    conf_proto::NodeKind,
    discovery, paths,
    provenance::{InstallRecord, InstallRole, NetworkIdentity},
    reconcile,
    resolver::ResolverConfig,
    template::{describe_member_auth, describe_ref_auth},
};
use std::{net::SocketAddr, time::Duration};

/// How long to browse mDNS for the install's conf server when the
/// recorded address doesn't answer.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Args, Debug)]
pub(crate) struct UpdateFlags {
    /// Show the reconcile plan without writing anything.
    #[arg(long = "dry-run")]
    pub dry_run: bool,
}

/// Load this host's install record, or bail with a friendly message.
fn require_record() -> Result<InstallRecord> {
    InstallRecord::load_default()?.context(
        "no install record (install.json) found — this host has no netidx \
         install managed by `netidx conf`, or the install predates the record",
    )
}

/// Require that the loaded record is for `want`; otherwise point the
/// operator at the right command.
fn require_role(rec: &InstallRecord, want: InstallRole) -> Result<()> {
    if rec.role != want {
        bail!(
            "this host is a {} install, not a {} — use `netidx conf {} …`",
            rec.role.as_str(),
            want.as_str(),
            rec.role.as_str(),
        )
    }
    Ok(())
}

/// Locate the install's conf server and return the network's current
/// facts, **pinned** to the CA fingerprint recorded at install. Tries
/// the recorded address first, then mDNS; every candidate is verified
/// against the pin before any info is trusted. Fails closed: a reachable
/// but wrong-fingerprint server is refused, not used.
fn fetch_network_pinned(
    net_id: &NetworkIdentity,
    conf_server: Option<SocketAddr>,
    kind: NodeKind,
) -> Result<NetworkInfo> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let mut candidates: Vec<SocketAddr> = Vec::new();
    if let Some(a) = conf_server {
        candidates.push(a);
    }
    for d in discovery::browse_or_empty(DISCOVERY_TIMEOUT) {
        candidates.extend(d.socket_addrs());
    }
    candidates.dedup();
    let mut saw_mismatch = false;
    for addr in &candidates {
        let id = match rt.block_on(conf_client::fetch_identity(*addr, kind)) {
            Ok(id) => id,
            // Unreachable / not a conf server — try the next candidate.
            Err(_) => continue,
        };
        // Fail closed on a malformed stored fingerprint (corrupt record).
        if net_id.matches(&id.fingerprint)? {
            return rt
                .block_on(conf_client::aggregate(&[*addr], kind, &id))
                .context("mapping the network (GetInfo)");
        }
        saw_mismatch = true;
    }
    if saw_mismatch {
        bail!(
            "reached a conf server, but its CA fingerprint did not match this \
             install's pinned network identity (network {:?}). Refusing to \
             trust it — if your network's CA legitimately changed, re-join.",
            net_id.domain,
        )
    }
    bail!(
        "could not reach any conf server for network {:?} (the recorded \
         address and mDNS both failed). Is the resolver / conf-server host up?",
        net_id.domain,
    )
}

/// The resolver config this host's role edits. The lifecycle ops assume
/// the canonical layout (the install wrote it there).
fn resolver_config_path() -> Result<std::path::PathBuf> {
    paths::discover_resolver_config()
        .context("no resolver config found at the standard locations")
}

// -- workstation --------------------------------------------------------------

pub(crate) fn workstation_status() -> Result<()> {
    let rec = require_record()?;
    require_role(&rec, InstallRole::Workstation)?;
    println!("workstation install (base {})", rec.base);

    let rpath = resolver_config_path()?;
    let rcfg = ResolverConfig::load(&rpath)?;
    let file = rcfg.as_file();
    for m in &file.member_servers {
        println!("  local resolver: {} ({})", m.addr, describe_member_auth(&m.auth));
    }
    match &file.parent {
        Some(parent) => {
            println!("  parent referral — {} peer(s):", parent.addrs.len());
            for (a, auth) in &parent.addrs {
                println!("    {a} ({})", describe_ref_auth(auth));
            }
        }
        None => println!("  parent: none (local-only)"),
    }

    match &rec.network {
        None => println!(
            "  network: local-only — run `netidx conf workstation join` to \
             attach to a network",
        ),
        Some(net_id) => {
            println!("  network: {:?}", net_id.domain);
            match fetch_network_pinned(net_id, rec.conf_server, NodeKind::Client) {
                Err(e) => println!("  sync: could not check ({e:#})"),
                Ok(info) => {
                    let plan = reconcile::reconcile_resolver_peers(&rpath, &info)?;
                    if plan.is_empty() {
                        println!("  sync: in sync ({} network resolver(s))", info.resolvers.len());
                    } else {
                        println!(
                            "  sync: behind by {} resolver peer(s) — run \
                             `netidx conf workstation update`:",
                            plan.changes.len(),
                        );
                        print!("{}", plan.describe());
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn workstation_update(flags: UpdateFlags) -> Result<()> {
    let rec = require_record()?;
    require_role(&rec, InstallRole::Workstation)?;
    let net_id = rec.network.as_ref().context(
        "this workstation is local-only — it hasn't joined a network, so there \
         is nothing to update. Run `netidx conf workstation join` first.",
    )?;
    let rpath = resolver_config_path()?;
    let info = fetch_network_pinned(net_id, rec.conf_server, NodeKind::Client)?;
    let plan = reconcile::reconcile_resolver_peers(&rpath, &info)?;
    if plan.is_empty() {
        println!("already in sync with network {:?} — nothing to do", net_id.domain);
        return Ok(());
    }
    println!("update plan for network {:?}:", net_id.domain);
    print!("{}", plan.describe());
    if flags.dry_run {
        println!("(dry-run: nothing written)");
        return Ok(());
    }
    plan.apply()?;
    println!("ok — restart the local resolver to serve the new peers");
    Ok(())
}
