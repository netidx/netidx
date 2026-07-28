//! Lifecycle ops on an *already-installed* role: `status` (read-only)
//! and `update` (additive reconcile against the admin domain). Shared by the
//! per-role command modules under [`super::roles`].
//!
//! The security-critical step is [`fetch_admin_domain_pinned`]: these ops
//! trust an admin server's picture of the admin domain, so they first re-pin to
//! the **same** CA identity the operator glyph-confirmed at install
//! (stored in the [`InstallRecord`]). An admin server whose CA fingerprint
//! doesn't match the pin is refused before anything is read or changed.

use anyhow::{Context, Result};
use clap::Args;
use log::warn;
use netidx_admin_client::{
    config_lock::ConfigDirLock,
    discovery, paths,
    provenance::{AdminDomainIdentity, InstallRecord, InstallRole},
    reconcile,
    resolver::ResolverConfig,
    template::{describe_member_auth, describe_ref_auth},
    transport::{self, AdminDomainInfo, CaIdentity},
};
use netidx_admin_proto::{AdminDomainMap, NodeKind};
use std::net::SocketAddr;

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

/// The first admin server that proves it belongs to this install's
/// **pinned** admin domain, from [`discovery::admin_servers_blocking`].
/// Fails closed: a reachable but wrong-fingerprint server is refused, not
/// used.
fn pinned_admin_server(
    net_id: &AdminDomainIdentity,
    kind: NodeKind,
    rt: &tokio::runtime::Runtime,
) -> Result<(SocketAddr, CaIdentity)> {
    let mut saw_mismatch = false;
    for addr in discovery::admin_servers_blocking() {
        let id = match rt.block_on(transport::fetch_identity(addr, kind)) {
            Ok(id) => id,
            // Unreachable / not an admin server — try the next candidate.
            Err(_) => continue,
        };
        // Fail closed on a malformed stored fingerprint (corrupt record).
        if net_id.matches(&id.fingerprint)? {
            return Ok((addr, id));
        }
        saw_mismatch = true;
    }
    if saw_mismatch {
        bail!(
            "reached an admin server, but its CA fingerprint did not match this \
             install's pinned admin domain identity (admin domain {:?}). Refusing to \
             trust it — if your admin domain's CA legitimately changed, re-join.",
            net_id.domain,
        )
    }
    bail!(
        "could not reach any admin server for admin domain {:?} (the recorded \
         addresses and mDNS both failed). Is the resolver / admin-server host up?",
        net_id.domain,
    )
}

/// The admin domain's current facts, via [`pinned_admin_server`].
fn fetch_admin_domain_pinned(
    net_id: &AdminDomainIdentity,
    kind: NodeKind,
) -> Result<AdminDomainInfo> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let (addr, id) = pinned_admin_server(net_id, kind, &rt)?;
    rt.block_on(transport::aggregate(&[addr], kind, &id))
        .context("mapping the admin domain (GetInfo)")
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

    match &rec.admin_domain {
        None => println!(
            "  admin domain: local-only — run `netidx admin workstation join` to \
             attach to an admin domain",
        ),
        Some(net_id) => {
            println!("  admin domain: {:?}", net_id.domain);
            match fetch_admin_domain_pinned(net_id, NodeKind::Client) {
                Err(e) => println!("  sync: could not check ({e:#})"),
                Ok(info) => {
                    let plan = reconcile::reconcile_resolver_peers(&rpath, &info)?;
                    if plan.is_empty() {
                        println!(
                            "  sync: in sync ({} admin domain resolver(s))",
                            info.resolvers.len()
                        );
                    } else {
                        println!(
                            "  sync: {} pending change(s) — run \
                             `netidx admin workstation update`:",
                            plan.changes().len(),
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
    let mut rec = require_record()?;
    require_role(&rec, InstallRole::Workstation)?;
    let net_id = rec.admin_domain.clone().context(
        "this workstation is local-only — it hasn't joined an admin domain, so there \
         is nothing to update. Run `netidx admin workstation join` first.",
    )?;
    let rpath = resolver_config_path()?;
    let info = fetch_admin_domain_pinned(&net_id, NodeKind::Client)?;
    let mode = if flags.dry_run {
        UpdateMode::DryRun
    } else {
        UpdateMode::Apply(ConfigDirLock::acquire_for_file(&rpath)?)
    };
    let map = fetch_map_pinned(&net_id, NodeKind::Client)?;
    record_admin_servers(&mut rec, &map, &mode)?;
    let plan = reconcile::reconcile_resolver_peers(&rpath, &info)?;
    if plan.is_empty() {
        println!("already in sync with admin domain {:?} — nothing to do", net_id.domain);
        return Ok(());
    }
    println!("update plan for admin domain {:?}:", net_id.domain);
    run_update(plan, mode, "restart the local resolver to serve the new peers")
}

// -- shared helpers for the map-driven roles (resolver / publisher) ------------

/// The client config this host's role keeps in sync with its resolver cluster.
fn client_config_path() -> Result<std::path::PathBuf> {
    paths::discover_client_config()
        .context("no client config found at the standard locations")
}

/// Like [`fetch_admin_domain_pinned`] but returns the CA-authoritative admin domain
/// map in one round trip (no client-side walk). Same fail-closed pinning.
fn fetch_map_pinned(
    net_id: &AdminDomainIdentity,
    kind: NodeKind,
) -> Result<AdminDomainMap> {
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let (addr, id) = pinned_admin_server(net_id, kind, &rt)?;
    rt.block_on(transport::get_map_pinned(addr, kind, &id))
        .context("fetching the admin domain map")
}

/// Refresh the record's admin-server list from the authoritative map, so
/// this host keeps a way in when one of them moves. Best-effort: a host
/// that can still reach the admin domain must not fail an `update` because
/// its own bookkeeping couldn't be written.
fn record_admin_servers(
    rec: &mut InstallRecord,
    map: &AdminDomainMap,
    mode: &UpdateMode,
) -> Result<()> {
    let UpdateMode::Apply(config_lock) = mode else { return Ok(()) };
    if !rec.set_admin_servers(map.admin_servers.iter().map(|s| s.addr)) {
        return Ok(());
    }
    let path = paths::discover_install_record()?;
    if let Err(e) = rec.save(config_lock, &path) {
        warn!("could not record the admin server list: {e:#}");
    }
    Ok(())
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
    match &rec.admin_domain {
        None => println!("  admin domain: local-only — not attached"),
        Some(net_id) => {
            println!("  admin domain: {:?}", net_id.domain);
            match fetch_map_pinned(net_id, NodeKind::Resolver) {
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
    let mut rec = require_record()?;
    require_role(&rec, InstallRole::Resolver)?;
    let net_id = rec.admin_domain.clone().context(
        "this resolver is local-only — it hasn't joined an admin domain, so there is \
         nothing to update",
    )?;
    let map = fetch_map_pinned(&net_id, NodeKind::Resolver)?;
    let rpath = resolver_config_path()?;
    let mode = if flags.dry_run {
        UpdateMode::DryRun
    } else {
        UpdateMode::Apply(ConfigDirLock::acquire_for_file(&rpath)?)
    };
    record_admin_servers(&mut rec, &map, &mode)?;
    // The client config (the resolvers this host talks to), if present.
    let mut plan = match client_config_path() {
        Ok(cpath) => reconcile::reconcile_client_peers(&cpath, &map)?,
        Err(_) => reconcile::EditPlan::default(),
    };
    // The parent referral, if this resolver is a child. NEVER member_servers.
    if ResolverConfig::load(&rpath)?.as_file().parent.is_some() {
        plan = plan.merge(reconcile::reconcile_parent_peers(&rpath, &map)?);
    }
    let hint = if plan.changes_resolver_config() {
        "no service was restarted. Restart this resolver manually at its place in the \
         resolver cluster's rolling sequence; re-run client processes if their resolver addresses \
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
    match &rec.admin_domain {
        None => println!("  admin domain: local-only — not attached"),
        Some(net_id) => {
            println!("  admin domain: {:?}", net_id.domain);
            match fetch_map_pinned(net_id, NodeKind::Publisher) {
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
    let mut rec = require_record()?;
    require_role(&rec, InstallRole::Publisher)?;
    let net_id = rec.admin_domain.clone().context(
        "this publisher is local-only — it hasn't joined an admin domain, so there is \
         nothing to update",
    )?;
    let map = fetch_map_pinned(&net_id, NodeKind::Publisher)?;
    let cpath = client_config_path()?;
    let mode = if flags.dry_run {
        UpdateMode::DryRun
    } else {
        UpdateMode::Apply(ConfigDirLock::acquire_for_file(&cpath)?)
    };
    record_admin_servers(&mut rec, &map, &mode)?;
    let plan = reconcile::reconcile_client_peers(&cpath, &map)?;
    run_update(plan, mode, "re-run publishers to use the new resolvers")
}
