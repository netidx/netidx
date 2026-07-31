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
    config_lock::ConfigDirLock,
    provenance::{InstallRecord, InstallRole},
    reconcile::EditPlan,
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

/// Load this host's install record and the path it came from, or bail with
/// a friendly message.
fn require_record(want: InstallRole) -> Result<(InstallRecord, std::path::PathBuf)> {
    let path = netidx_admin::paths::discover_install_record().ok();
    let rec = InstallRecord::load_default()?.context(
        "no install record (install.json) found — this host has no netidx \
         install managed by `netidx admin`, or the install predates the record",
    )?;
    if rec.role != want {
        bail!(
            "this host is a {} install, not a {} — use `netidx admin {} …`",
            rec.role.as_str(),
            want.as_str(),
            rec.role.as_str(),
        )
    }
    Ok((rec, path.context("locating this host's install record")?))
}

/// What the operator has to do for an applied plan to take effect. Nothing
/// re-reads its configuration at runtime, so every edit lands at the next
/// process start — which is why nothing here ever restarts a service.
pub(crate) fn restart_hint(role: InstallRole, plan: &EditPlan) -> &'static str {
    match role {
        InstallRole::Ca => "The CA requires no resolver restart.",
        InstallRole::Workstation => {
            "No service was restarted. Restart the local resolver to serve the new peers."
        }
        InstallRole::Resolver if plan.changes_resolver_config() => {
            "No service was restarted. Restart this resolver manually at its place in \
             the resolver cluster's rolling sequence; re-run client processes if their \
             resolver addresses changed."
        }
        InstallRole::Resolver => {
            "Re-run client processes to use the new resolver addresses; no resolver \
             service restart is needed."
        }
        InstallRole::Publisher => "Re-run publishers to use the new resolvers.",
    }
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
    let (rec, path) = require_record(role)?;
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
    let (rec, path) = require_record(role)?;
    let domain = rec
        .admin_domain
        .as_ref()
        .context(
            "this host is local-only — it hasn't joined an admin domain, so there is \
             nothing to update",
        )?
        .domain
        .clone();
    // Everything over the network happens here, before any lock is taken:
    // the config-directory lock never waits, so holding it across a round
    // trip would fail a concurrent command for no reason.
    let plan: SyncPlan = runtime()?.block_on(sync::plan(rec, path.clone()))?;
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
    let hint = restart_hint(role, &plan.edits);
    let lock = ConfigDirLock::acquire_for_file(&path)?;
    plan.apply(&lock)?;
    println!("ok — {hint}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_only_resolver_update_does_not_request_a_resolver_restart() {
        let hint = restart_hint(InstallRole::Resolver, &EditPlan::default());
        assert!(hint.contains("no resolver service restart is needed"));
        assert!(!hint.contains("Restart this resolver"));
    }
}
