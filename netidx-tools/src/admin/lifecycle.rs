//! Lifecycle ops on an *already-installed* role: `status` (read-only) and
//! `update`. Shared by the per-role command modules under [`super::roles`].
//!
//! Both are thin printers over [`netidx_admin::sync`], which is the
//! one implementation — the same code the `admin-agent` runs on a timer.
//! `update` is therefore not a maintenance chore but a way to force the
//! refresh now; on a client the agent would get to it within the day.
//!
//! The security-critical step lives in the library: these ops trust an
//! admin server's picture of the admin domain, so they first re-pin to the
//! **same** CA identity the operator glyph-confirmed at install (stored in
//! the [`InstallRecord`]). An admin server whose CA fingerprint doesn't
//! match the pin is refused before anything is read or changed.

use anyhow::{Context, Result};
use clap::Args;
use netidx_admin::{
    provenance::InstallRole,
    resolver::ResolverConfig,
    sync::{self, SyncPlan},
    template::{describe_member_auth, describe_ref_auth},
};

#[derive(Args, Debug)]
pub(crate) struct UpdateFlags {
    /// Show the reconcile plan without writing anything.
    #[arg(long = "dry-run")]
    pub dry_run: bool,
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

/// The local configuration this host's role owns, printed before any admin
/// domain round trip so `status` says something useful even when offline.
fn print_local_config(role: InstallRole) -> Result<()> {
    if role == InstallRole::Publisher || role == InstallRole::Ca {
        return Ok(());
    }
    let path = netidx_admin::paths::discover_resolver_config()
        .context("no resolver config found at the standard locations")?;
    let cfg = ResolverConfig::load(&path)?;
    let file = cfg.as_file();
    let what = if role == InstallRole::Workstation { "local resolver" } else { "member" };
    for m in &file.member_servers {
        println!("  {what}: {} ({})", m.addr, describe_member_auth(&m.auth));
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
    Ok(())
}

pub(crate) fn status(role: InstallRole) -> Result<()> {
    let (rec, path) = sync::record_for(role, None)?;
    println!("{} install (base {})", role.as_str(), rec.base);
    print_local_config(role)?;
    let Some(net_id) = rec.admin_domain.clone() else {
        println!("  admin domain: local-only — not attached");
        return Ok(());
    };
    println!("  admin domain: {:?}", net_id.domain);
    match runtime()?.block_on(sync::plan(rec, path)) {
        Err(e) => println!("  sync: could not check ({e:#})"),
        Ok(plan) => {
            for addr in plan.admin_servers() {
                println!("  admin server: {addr}");
            }
            if plan.edits.is_empty() {
                println!("  sync: in sync");
            } else {
                println!(
                    "  sync: {} pending change(s) — run `netidx admin {} update`:",
                    plan.edits.changes().len(),
                    role.as_str(),
                );
                print!("{}", plan.edits.describe());
            }
        }
    }
    Ok(())
}

pub(crate) fn update(role: InstallRole, flags: UpdateFlags) -> Result<()> {
    let rt = runtime()?;
    let plan: SyncPlan = rt.block_on(sync::plan_for(role, None))?;
    let domain = plan
        .record()
        .admin_domain
        .as_ref()
        .map(|net| net.domain.clone())
        .unwrap_or_default();
    if plan.is_empty() {
        println!("already in sync with admin domain {domain:?} — nothing to do");
        return Ok(());
    }
    println!("update plan for admin domain {domain:?}:");
    if plan.admin_servers_changed {
        println!("  admin servers: {}", plan.admin_servers().len());
    }
    print!("{}", plan.edits.describe());
    if flags.dry_run {
        println!("(dry-run: nothing written)");
        return Ok(());
    }
    let hint = plan.restart_hint();
    rt.block_on(plan.apply_locked())?;
    println!("ok — {hint}");
    Ok(())
}
