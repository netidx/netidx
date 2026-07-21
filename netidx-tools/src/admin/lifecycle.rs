//! Lifecycle ops on an *already-installed* role: `status` (read-only)
//! and `update` (additive reconcile against the network). Shared by the
//! per-role command modules under [`super::roles`].
//!
//! The security-critical step is [`fetch_network_pinned`]: these ops
//! trust a admin server's picture of the network, so they first re-pin to
//! the **same** CA identity the operator glyph-confirmed at install
//! (stored in the [`InstallRecord`]). A admin server whose CA fingerprint
//! doesn't match the pin is refused before anything is read or changed.

use anyhow::{Context, Result};
use clap::Args;
use netidx_admin::{
    admin_client::{self, NetworkInfo},
    admin_proto::{NetworkMap, NodeKind},
    config_lock::ConfigDirLock,
    discovery, paths,
    provenance::{InstallRecord, InstallRole, NetworkIdentity},
    reconcile,
    resolver::ResolverConfig,
    template::{describe_member_auth, describe_ref_auth},
};
use std::{net::SocketAddr, time::Duration};

/// How long to browse mDNS for the install's admin server when the
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
         install managed by `netidx admin`, or the install predates the record",
    )
}

/// Require that the loaded record is for `want`; otherwise point the
/// operator at the right command.
fn require_role(rec: &InstallRecord, want: InstallRole) -> Result<()> {
    if rec.role != want {
        bail!(
            "this host is a {} install, not a {} — use `netidx admin {} …`",
            rec.role.as_str(),
            want.as_str(),
            rec.role.as_str(),
        )
    }
    Ok(())
}

/// Locate the install's admin server and return the network's current
/// facts, **pinned** to the CA fingerprint recorded at install. Tries
/// the recorded address first, then mDNS; every candidate is verified
/// against the pin before any info is trusted. Fails closed: a reachable
/// but wrong-fingerprint server is refused, not used.
fn fetch_network_pinned(
    net_id: &NetworkIdentity,
    admin_server: Option<SocketAddr>,
    kind: NodeKind,
) -> Result<NetworkInfo> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let mut candidates: Vec<SocketAddr> = Vec::new();
    if let Some(a) = admin_server {
        candidates.push(a);
    }
    for d in discovery::browse_or_empty(DISCOVERY_TIMEOUT) {
        candidates.extend(d.socket_addrs());
    }
    candidates.dedup();
    let mut saw_mismatch = false;
    for addr in &candidates {
        let id = match rt.block_on(admin_client::fetch_identity(*addr, kind)) {
            Ok(id) => id,
            // Unreachable / not a admin server — try the next candidate.
            Err(_) => continue,
        };
        // Fail closed on a malformed stored fingerprint (corrupt record).
        if net_id.matches(&id.fingerprint)? {
            return rt
                .block_on(admin_client::aggregate(&[*addr], kind, &id))
                .context("mapping the network (GetInfo)");
        }
        saw_mismatch = true;
    }
    if saw_mismatch {
        bail!(
            "reached a admin server, but its CA fingerprint did not match this \
             install's pinned network identity (network {:?}). Refusing to \
             trust it — if your network's CA legitimately changed, re-join.",
            net_id.domain,
        )
    }
    bail!(
        "could not reach any admin server for network {:?} (the recorded \
         address and mDNS both failed). Is the resolver / admin-server host up?",
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
            "  network: local-only — run `netidx admin workstation join` to \
             attach to a network",
        ),
        Some(net_id) => {
            println!("  network: {:?}", net_id.domain);
            match fetch_network_pinned(net_id, rec.admin_server, NodeKind::Client) {
                Err(e) => println!("  sync: could not check ({e:#})"),
                Ok(info) => {
                    let plan = reconcile::reconcile_resolver_peers(&rpath, &info)?;
                    if plan.is_empty() {
                        println!(
                            "  sync: in sync ({} network resolver(s))",
                            info.resolvers.len()
                        );
                    } else {
                        println!(
                            "  sync: behind by {} resolver peer(s) — run \
                             `netidx admin workstation update`:",
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
         is nothing to update. Run `netidx admin workstation join` first.",
    )?;
    let rpath = resolver_config_path()?;
    let info = fetch_network_pinned(net_id, rec.admin_server, NodeKind::Client)?;
    let mode = if flags.dry_run {
        UpdateMode::DryRun
    } else {
        UpdateMode::Apply(ConfigDirLock::acquire_for_file(&rpath)?)
    };
    let plan = reconcile::reconcile_resolver_peers(&rpath, &info)?;
    if plan.is_empty() {
        println!("already in sync with network {:?} — nothing to do", net_id.domain);
        return Ok(());
    }
    println!("update plan for network {:?}:", net_id.domain);
    run_update(plan, mode, "restart the local resolver to serve the new peers")
}

// -- shared helpers for the map-driven roles (resolver / publisher) ------------

/// The client config this host's role keeps in sync with its cluster.
fn client_config_path() -> Result<std::path::PathBuf> {
    paths::discover_client_config()
        .context("no client config found at the standard locations")
}

/// Like [`fetch_network_pinned`] but returns the CA-authoritative network
/// map in one round trip (no client-side walk). Same fail-closed pinning.
fn fetch_map_pinned(
    net_id: &NetworkIdentity,
    admin_server: Option<SocketAddr>,
    kind: NodeKind,
) -> Result<NetworkMap> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let mut candidates: Vec<SocketAddr> = Vec::new();
    if let Some(a) = admin_server {
        candidates.push(a);
    }
    for d in discovery::browse_or_empty(DISCOVERY_TIMEOUT) {
        candidates.extend(d.socket_addrs());
    }
    candidates.dedup();
    let mut saw_mismatch = false;
    for addr in &candidates {
        let id = match rt.block_on(admin_client::fetch_identity(*addr, kind)) {
            Ok(id) => id,
            Err(_) => continue,
        };
        if net_id.matches(&id.fingerprint)? {
            return rt
                .block_on(admin_client::get_map_pinned(*addr, kind, &id))
                .context("fetching the network map");
        }
        saw_mismatch = true;
    }
    if saw_mismatch {
        bail!(
            "reached a admin server, but its CA fingerprint did not match this \
             install's pinned network identity (network {:?}). Refusing to trust \
             it — if your network's CA legitimately changed, re-join.",
            net_id.domain,
        )
    }
    bail!(
        "could not reach any admin server for network {:?} (the recorded address \
         and mDNS both failed). Is the resolver / admin-server host up?",
        net_id.domain,
    )
}

/// Apply a reconcile plan (or just describe it) — the shared tail of every
/// map-driven `update`.
enum UpdateMode {
    DryRun,
    Apply(ConfigDirLock),
}

fn run_update(
    plan: reconcile::EditPlan,
    mode: UpdateMode,
    restart_hint: &str,
) -> Result<()> {
    if plan.is_empty() {
        println!("already in sync — nothing to do");
        return Ok(());
    }
    print!("{}", plan.describe());
    match mode {
        UpdateMode::DryRun => println!("(dry-run: nothing written)"),
        UpdateMode::Apply(config_lock) => {
            plan.apply(&config_lock)?;
            println!("ok — {restart_hint}");
        }
    }
    Ok(())
}

/// Print a reconcile result as a drift summary for `status`.
fn report_drift(what: &str, plan: Result<reconcile::EditPlan>) {
    match plan {
        Err(e) => println!("  {what}: could not check ({e:#})"),
        Ok(p) if p.is_empty() => println!("  {what}: in sync"),
        Ok(p) => {
            println!("  {what}: out of sync — run `update`:");
            print!("{}", p.describe());
        }
    }
}

// -- resolver -----------------------------------------------------------------

pub(crate) fn resolver_status() -> Result<()> {
    let rec = require_record()?;
    require_role(&rec, InstallRole::Resolver)?;
    println!("resolver install (base {})", rec.base);
    let rpath = resolver_config_path()?;
    let rcfg = ResolverConfig::load(&rpath)?;
    for m in &rcfg.as_file().member_servers {
        println!("  member: {} ({})", m.addr, describe_member_auth(&m.auth));
    }
    let has_parent = rcfg.as_file().parent.is_some();
    match &rec.network {
        None => println!("  network: local-only — not attached"),
        Some(net_id) => {
            println!("  network: {:?}", net_id.domain);
            match fetch_map_pinned(net_id, rec.admin_server, NodeKind::Resolver) {
                Err(e) => println!("  sync: could not check ({e:#})"),
                Ok(map) => {
                    if let Ok(cpath) = client_config_path() {
                        report_drift(
                            "client",
                            reconcile::reconcile_client_peers(&cpath, &map),
                        );
                    }
                    if has_parent {
                        report_drift(
                            "parent referral",
                            reconcile::reconcile_parent_peers(&rpath, &map),
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn resolver_update(flags: UpdateFlags) -> Result<()> {
    let rec = require_record()?;
    require_role(&rec, InstallRole::Resolver)?;
    let net_id = rec.network.as_ref().context(
        "this resolver is local-only — it hasn't joined a network, so there is \
         nothing to update",
    )?;
    let map = fetch_map_pinned(net_id, rec.admin_server, NodeKind::Resolver)?;
    let rpath = resolver_config_path()?;
    let mode = if flags.dry_run {
        UpdateMode::DryRun
    } else {
        UpdateMode::Apply(ConfigDirLock::acquire_for_file(&rpath)?)
    };
    // The client config (the resolvers this host talks to), if present.
    let mut plan = match client_config_path() {
        Ok(cpath) => reconcile::reconcile_client_peers(&cpath, &map)?,
        Err(_) => reconcile::EditPlan::default(),
    };
    // The parent referral, if this resolver is a child. NEVER member_servers.
    if ResolverConfig::load(&rpath)?.as_file().parent.is_some() {
        plan = plan.merge(reconcile::reconcile_parent_peers(&rpath, &map)?);
    }
    let hint = if plan.resolver_edit.is_some() {
        "no service was restarted. Restart this resolver manually at its place in the \
         cluster's rolling sequence; re-run client processes if their resolver addresses \
         changed"
    } else {
        "re-run client processes to use the new resolver addresses; no resolver service \
         restart is needed"
    };
    run_update(plan, mode, hint)
}

// -- publisher ----------------------------------------------------------------

pub(crate) fn publisher_status() -> Result<()> {
    let rec = require_record()?;
    require_role(&rec, InstallRole::Publisher)?;
    println!("publisher install (base {})", rec.base);
    match &rec.network {
        None => println!("  network: local-only — not attached"),
        Some(net_id) => {
            println!("  network: {:?}", net_id.domain);
            match fetch_map_pinned(net_id, rec.admin_server, NodeKind::Publisher) {
                Err(e) => println!("  sync: could not check ({e:#})"),
                Ok(map) => match client_config_path() {
                    Ok(cpath) => report_drift(
                        "client",
                        reconcile::reconcile_client_peers(&cpath, &map),
                    ),
                    Err(e) => println!("  client config: {e:#}"),
                },
            }
        }
    }
    Ok(())
}

pub(crate) fn publisher_update(flags: UpdateFlags) -> Result<()> {
    let rec = require_record()?;
    require_role(&rec, InstallRole::Publisher)?;
    let net_id = rec.network.as_ref().context(
        "this publisher is local-only — it hasn't joined a network, so there is \
         nothing to update",
    )?;
    let map = fetch_map_pinned(net_id, rec.admin_server, NodeKind::Publisher)?;
    let cpath = client_config_path()?;
    let mode = if flags.dry_run {
        UpdateMode::DryRun
    } else {
        UpdateMode::Apply(ConfigDirLock::acquire_for_file(&cpath)?)
    };
    let plan = reconcile::reconcile_client_peers(&cpath, &map)?;
    run_update(plan, mode, "re-run publishers to use the new resolvers")
}
