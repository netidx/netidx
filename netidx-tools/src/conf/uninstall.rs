//! `netidx conf uninstall` — wholesale teardown of a netidx install.
//!
//! Mirrors the install side's sudo-escalation pattern: a system-scope
//! uninstall re-execs itself under `sudo` when not already root, then
//! the elevated child runs the engine. The CLI also shows the plan
//! and prompts the operator before deleting anything (unless
//! `--yes`).

use anyhow::Result;
use clap::Args;
use netidx_conf::{
    paths,
    provenance::InstallRecord,
    service::ServiceScope,
    uninstall::{self, UninstallParams, UninstallReport},
};
use std::path::PathBuf;

use super::{
    prompt,
    service::{self as svc_cli, ScopeArg},
};
// Only the unix sudo re-exec path builds commands or adds error
// context.
#[cfg(unix)]
use super::service::ELEVATED_ENV;
#[cfg(unix)]
use anyhow::Context;
#[cfg(unix)]
use std::process::Command;

#[derive(Args, Debug)]
pub(crate) struct Params {
    /// User or system scope. System-scope re-execs under sudo. The
    /// config root and the OS service are both per-scope. As a
    /// convenience, an unelevated `--scope user` run also probes for
    /// a matching system-scope install (which the resolver and
    /// publisher templates register via sudo) and offers to escalate
    /// + remove it.
    #[arg(long, default_value = "user")]
    pub scope: ScopeArg,
    /// Service name to disable + remove. Default "netidx".
    #[arg(long, default_value = "netidx")]
    pub service_name: String,
    /// For system-scope only: the user the templated systemd unit
    /// was instantiated as. Defaults like `service uninstall`
    /// (`$SUDO_USER` then current user).
    #[arg(long)]
    pub for_user: Option<String>,
    /// Override the config root. Default: the canonical user / system
    /// root for `--scope`.
    #[arg(long)]
    pub config_dir: Option<PathBuf>,
    /// Also delete the CA private key and issued certs. DANGEROUS —
    /// anything signed by this CA cannot be re-issued without
    /// bootstrapping a new chain of trust. Default: keep `ca/`.
    #[arg(long)]
    pub with_ca: bool,
    /// Skip the confirmation prompt.
    #[arg(short, long)]
    pub yes: bool,
    /// Report what would be done without doing it.
    #[arg(long)]
    pub dry_run: bool,
}

pub(crate) fn run(p: Params) -> Result<()> {
    let scope: ServiceScope = p.scope.into();
    // Elevate before doing anything else so the plan we print
    // reflects what root sees (e.g. /etc/netidx that the unelevated
    // process couldn't list).
    if scope == ServiceScope::System && !svc_cli::is_elevated()? {
        return escalate(&p);
    }
    do_primary_scope(&p, scope)?;
    // Templated installs (resolver, publisher) register a *system*-scope
    // service (`netidx@<user>.service`) even when the config they write is
    // user-scope (`~/.config/netidx`), so it never shows up in our
    // user-scope probe. A `--scope user` teardown still has to catch it,
    // or `install` then `uninstall` leaves the daemons running. As root we
    // remove it directly; unelevated, we offer to escalate.
    if scope == ServiceScope::User {
        if svc_cli::is_elevated()? {
            remove_system_scope_if_present(&p)?;
        } else {
            offer_system_scope(&p)?;
        }
    }
    Ok(())
}

/// Run the uninstall for the scope the operator explicitly asked
/// about (user or system). Self-contained — prints the plan, prompts,
/// executes. The system-scope cross-probe (only meaningful for an
/// unelevated `--scope user` run) happens separately in `run`.
fn do_primary_scope(p: &Params, scope: ServiceScope) -> Result<()> {
    // Surface what role this install is, when it left a provenance
    // marker — a confirmation of *what* is being torn down. Best-effort:
    // a hand-rolled config (no marker) just skips the line. The marker
    // itself is removed with the rest of the config root below.
    if let Some(rec) = load_install_record(p, scope) {
        match &rec.network {
            Some(net) => println!(
                "tearing down {} install (joined to network {:?})",
                rec.role.as_str(),
                net.domain,
            ),
            None => {
                println!("tearing down {} install (local-only)", rec.role.as_str())
            }
        }
    }
    let base = UninstallParams {
        scope,
        service_name: p.service_name.clone(),
        for_user: p.for_user.clone(),
        config_dir: p.config_dir.clone(),
        remove_ca: p.with_ca,
        dry_run: true,
    };
    let plan = uninstall::uninstall(&base)?;
    print_report(&plan, false);
    let root = config_root(p, scope);
    if p.dry_run {
        if let Some(root) = &root {
            deregister_conf_server(root, true);
        }
        return Ok(());
    }
    if plan.is_empty() {
        println!("(nothing to do)");
        return Ok(());
    }
    let ca_in_plan = plan_contains_ca(&plan);
    let confirm = if p.yes {
        true
    } else {
        let q = if ca_in_plan {
            "DESTRUCTIVE: this will remove the CA private key. \
             Anything signed by this CA cannot be re-issued. Proceed?"
        } else {
            "Proceed with uninstall?"
        };
        // Default `false`: an unattended `echo "" | netidx conf
        // uninstall` must not silently wipe an install.
        prompt::confirm(q, false)?
    };
    if !confirm {
        println!("aborted");
        return Ok(());
    }
    // Drop ourselves from the CA's map before deleting the certs we'd need
    // to authenticate the deregister.
    if let Some(root) = &root {
        deregister_conf_server(root, false);
    }
    let report = uninstall::uninstall(&UninstallParams { dry_run: false, ..base })?;
    print_report(&report, true);
    Ok(())
}

/// Probe `/etc/systemd/system/<name>@.service` + `/etc/netidx`
/// without escalating. If anything's there, show it and offer to
/// escalate. Best-effort: a probe failure (e.g. perms denied on
/// `/etc/netidx`) is logged at debug and the offer is silently
/// skipped — the operator can always re-run `--scope system`.
fn offer_system_scope(p: &Params) -> Result<()> {
    let probe = UninstallParams {
        scope: ServiceScope::System,
        service_name: p.service_name.clone(),
        // Engine resolves `None` to the invoking user via
        // `service::status`'s for_user fallback — matches what the
        // install side registered for this user.
        for_user: p.for_user.clone(),
        // Always probe the canonical /etc/netidx — don't inherit a
        // user-scope --config-dir override (it was for the user dir).
        config_dir: None,
        remove_ca: p.with_ca,
        dry_run: true,
    };
    let plan = match uninstall::uninstall(&probe) {
        Ok(plan) => plan,
        Err(e) => {
            log::debug!("system-scope probe failed (skipping offer): {e:#}");
            return Ok(());
        }
    };
    if plan.is_empty() {
        return Ok(());
    }
    println!();
    println!(
        "Detected a matching system-scope install (a templated install \
         registers its service at system scope even when the config is \
         user-scope):"
    );
    print_report(&plan, false);
    if p.dry_run {
        println!("(re-run with `--scope system` to remove it)");
        return Ok(());
    }
    if p.yes {
        // `--yes` was scoped to the explicit request; escalating
        // under sudo is a separate trust boundary that should not be
        // silent. Tell the operator how to opt in.
        println!("(re-run with `--scope system --yes` to remove it)");
        return Ok(());
    }
    let ca_in_plan = plan_contains_ca(&plan);
    let q = if ca_in_plan {
        "Also remove the system-scope install? \
         DESTRUCTIVE: will require sudo and will destroy the CA private key"
    } else {
        "Also remove the system-scope install? (will require sudo)"
    };
    if !prompt::confirm(q, false)? {
        return Ok(());
    }
    escalate_for_system_offer(p)?;
    Ok(())
}

/// Already-root counterpart to [`offer_system_scope`]: a `--scope user`
/// teardown run as root must still remove the system-scope service a
/// templated install (resolver, publisher) registered — but we can do it
/// directly, no escalation. Probe the system scope and, if anything's there, remove it
/// under the same `--dry-run` / `--yes` / confirm rules as the primary
/// scope. Silent when there's nothing to remove (the common single-scope
/// case), so it never adds noise to a plain user-scope teardown.
fn remove_system_scope_if_present(p: &Params) -> Result<()> {
    let base = UninstallParams {
        scope: ServiceScope::System,
        service_name: p.service_name.clone(),
        for_user: p.for_user.clone(),
        // Probe the canonical /etc/netidx — a user-scope `--config-dir`
        // override was for the user dir, not this.
        config_dir: None,
        remove_ca: p.with_ca,
        dry_run: true,
    };
    let plan = match uninstall::uninstall(&base) {
        Ok(plan) => plan,
        Err(e) => {
            log::debug!("system-scope probe failed (skipping): {e:#}");
            return Ok(());
        }
    };
    if plan.is_empty() {
        return Ok(());
    }
    println!();
    println!(
        "Detected a matching system-scope service (a templated install \
         registers its service at system scope even when the config is \
         user-scope):"
    );
    print_report(&plan, false);
    if p.dry_run {
        return Ok(());
    }
    if !p.yes && !prompt::confirm("Also remove the system-scope service?", true)? {
        println!("(left the system-scope service in place)");
        return Ok(());
    }
    let report = uninstall::uninstall(&UninstallParams { dry_run: false, ..base })?;
    print_report(&report, true);
    Ok(())
}

/// Load the install provenance marker for the config root this teardown
/// targets (honouring a `--config-dir` override). `None` when there's no
/// marker (hand-rolled config, or an install predating the record) or it
/// can't be read — reporting the role is a convenience, never a gate.
fn load_install_record(p: &Params, scope: ServiceScope) -> Option<InstallRecord> {
    let root = match &p.config_dir {
        Some(d) => d.clone(),
        None => match scope {
            ServiceScope::User => paths::user_config_root().ok()?,
            ServiceScope::System => paths::system_config_root(),
        },
    };
    let path = root.join("install.json");
    if !path.exists() {
        return None;
    }
    InstallRecord::load(&path).ok()
}

fn plan_contains_ca(r: &UninstallReport) -> bool {
    r.removed.iter().any(|p| p.file_name().and_then(|s| s.to_str()) == Some("ca"))
}

/// The config root this teardown targets (honouring `--config-dir`).
fn config_root(p: &Params, scope: ServiceScope) -> Option<PathBuf> {
    match &p.config_dir {
        Some(d) => Some(d.clone()),
        None => match scope {
            ServiceScope::User => paths::user_config_root().ok(),
            ServiceScope::System => Some(paths::system_config_root()),
        },
    }
}

/// Tell the CA to drop this conf server from the network map before we
/// delete its config + certs. Best-effort: a non-CA conf server registers
/// its facts with the CA, so on teardown it should deregister, or the CA
/// keeps a dead entry until `conf ca remove-server`. The CA host itself
/// owns the map and has nothing to deregister from. Runs while the serving
/// cert/key still exist (before the teardown removes them). Unix-only: the
/// conf-server daemon is, so only a unix host ever has one to deregister.
#[cfg(unix)]
fn deregister_conf_server(root: &std::path::Path, dry_run: bool) {
    use netidx_conf::{conf_client, conf_server, conf_server_config::ConfServerConfig};
    let cfg = match ConfServerConfig::load(&root.join("conf-server.json")) {
        Ok(c) => c,
        Err(_) => return, // no conf server here (workstation/publisher/hand-rolled)
    };
    if cfg.roles.ca.is_some() {
        return; // the CA owns the map; it doesn't deregister from itself
    }
    let Some(ca_addr) = cfg.ca_addr else { return };
    if dry_run {
        println!("conf server: would deregister {} from the CA at {ca_addr}", cfg.listen);
        return;
    }
    let result = (|| -> Result<()> {
        let cert = std::fs::read(&cfg.serving_cert).with_context(|| {
            format!("reading serving cert {}", cfg.serving_cert.display())
        })?;
        let key = std::fs::read(&cfg.serving_key).with_context(|| {
            format!("reading serving key {}", cfg.serving_key.display())
        })?;
        let trusted = std::fs::read(&cfg.trusted)
            .with_context(|| format!("reading trust bundle {}", cfg.trusted.display()))?;
        let roots = conf_server::load_roots(&trusted)?;
        let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
        rt.block_on(conf_client::deregister(ca_addr, &cert, &key, roots, cfg.listen))?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            println!("conf server: deregistered {} from the CA at {ca_addr}", cfg.listen)
        }
        Err(e) => eprintln!(
            "conf server: could not deregister from the CA at {ca_addr} ({e:#}); \
             the CA will keep this server in its map until `netidx conf ca remove-server`"
        ),
    }
}

#[cfg(not(unix))]
fn deregister_conf_server(_root: &std::path::Path, _dry_run: bool) {}

/// Print an uninstall report. `applied` distinguishes the dry-run
/// preview (`false` ⇒ "plan", subjunctive "would be uninstalled") from
/// the report of work actually done (`true` ⇒ "removed", past-tense
/// "uninstalled"). Folding the tense into one flag keeps the preview
/// and the result from ever disagreeing.
fn print_report(r: &UninstallReport, applied: bool) {
    if r.service_was_installed {
        if applied {
            println!("service: uninstalled");
        } else {
            println!("service: would be uninstalled");
        }
    } else {
        println!("service: not installed");
    }
    if let Some(e) = &r.service_error {
        println!("service uninstall reported error: {e}");
    }
    if r.removed.is_empty() && r.kept.is_empty() {
        return;
    }
    println!("{}:", if applied { "removed" } else { "plan" });
    for p in &r.removed {
        println!("  - {}", p.display());
    }
    if !r.kept.is_empty() {
        println!("kept:");
        for (p, why) in &r.kept {
            println!("  - {} ({})", p.display(), why.as_str());
        }
    }
}

#[cfg(unix)]
fn escalate(a: &Params) -> Result<()> {
    let for_user = svc_cli::resolve_for_user(a.for_user.clone())?;
    let exe = std::env::current_exe()
        .context("could not determine current binary for sudo re-exec")?;
    let mut cmd = Command::new(svc_cli::elevator());
    cmd.arg("--preserve-env=NETIDX_ELEVATED")
        .arg(&exe)
        .arg("conf")
        .arg("uninstall")
        .arg("--scope")
        .arg("system")
        .arg("--for-user")
        .arg(&for_user)
        .arg("--service-name")
        .arg(&a.service_name)
        .env(ELEVATED_ENV, "1");
    if let Some(dir) = &a.config_dir {
        cmd.arg("--config-dir").arg(dir);
    }
    if a.with_ca {
        cmd.arg("--with-ca");
    }
    if a.yes {
        cmd.arg("--yes");
    }
    if a.dry_run {
        cmd.arg("--dry-run");
    }
    let status = cmd.status().with_context(|| {
        format!("spawning `{}` for privilege escalation", svc_cli::elevator())
    })?;
    if !status.success() {
        bail!("escalation failed: {status}");
    }
    Ok(())
}

#[cfg(windows)]
fn escalate(_a: &Params) -> Result<()> {
    bail!(
        "system-scope uninstall on Windows requires an already-elevated shell. \
         Open an Administrator PowerShell / cmd and re-run this command."
    )
}

/// Escalate to do the system-scope teardown the operator just
/// confirmed via the cross-scope offer prompt. Distinct from
/// [`escalate`] because we always pass `--yes` (the operator
/// already confirmed) and never inherit the user-scope's
/// `--config-dir` or `--dry-run`.
#[cfg(unix)]
fn escalate_for_system_offer(p: &Params) -> Result<()> {
    let for_user = svc_cli::resolve_for_user(p.for_user.clone())?;
    let exe = std::env::current_exe()
        .context("could not determine current binary for sudo re-exec")?;
    let mut cmd = Command::new(svc_cli::elevator());
    cmd.arg("--preserve-env=NETIDX_ELEVATED")
        .arg(&exe)
        .arg("conf")
        .arg("uninstall")
        .arg("--scope")
        .arg("system")
        .arg("--for-user")
        .arg(&for_user)
        .arg("--service-name")
        .arg(&p.service_name)
        .arg("--yes")
        .env(ELEVATED_ENV, "1");
    if p.with_ca {
        cmd.arg("--with-ca");
    }
    let status = cmd.status().with_context(|| {
        format!("spawning `{}` for system-scope uninstall", svc_cli::elevator())
    })?;
    if !status.success() {
        bail!("system-scope escalation failed: {status}");
    }
    Ok(())
}

#[cfg(windows)]
fn escalate_for_system_offer(_p: &Params) -> Result<()> {
    bail!(
        "system-scope uninstall on Windows requires an already-elevated shell. \
         Open an Administrator PowerShell / cmd and re-run this command \
         with `--scope system`."
    )
}
