//! `netidx admin uninstall` — the CLI surface over
//! [`netidx_admin::uninstall`].
//!
//! The engine decides what to tear down, whether root is needed, and in what
//! order. This module turns flags into its input, prints the plan, gates it
//! behind `--yes`, and performs the one step the engine hands back: re-execing
//! under `sudo`, which needs a terminal for the password prompt.

use anyhow::{Context, Result};
use clap::Args;
use netidx_admin::{
    service::ServiceScope,
    uninstall::{
        self, Covers, Escalation, Next, Prepared, UninstallInput, UninstallReport,
    },
};
use std::path::PathBuf;

use super::{answer_cli, service::ScopeArg};
// Only the unix sudo re-exec path builds commands or adds error context.
#[cfg(unix)]
use super::service::{self as svc_cli, ELEVATED_ENV};
#[cfg(unix)]
use std::process::Command;

#[derive(Args, Debug)]
pub(crate) struct Params {
    /// User or system scope. Omit to auto-detect from the install record:
    /// a resolver / publisher writes user-scope config but registers a
    /// system-scope service, so a full teardown spans both — this resolves
    /// the primary scope and escalates (sudo, prompting for credentials) to
    /// remove the system-scope parts as needed. Pass `--scope` only to
    /// target one scope precisely. System scope re-execs under sudo.
    #[arg(long)]
    pub scope: Option<ScopeArg>,
    /// Service name to disable + remove. Default "netidx".
    #[arg(long)]
    pub service_name: Option<String>,
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

impl Params {
    fn input(&self) -> UninstallInput {
        UninstallInput {
            scope: self.scope.map(ServiceScope::from),
            service_name: self.service_name.clone(),
            for_user: self.for_user.clone(),
            config_dir: self.config_dir.clone(),
            remove_ca: self.with_ca,
            ..Default::default()
        }
    }
}

/// Whether this process has any way to become root from where it stands. Unix
/// re-execs under `sudo`/`su`; Windows has no equivalent — an elevated shell
/// is a separate session the operator has to start. Both frontends consult
/// this, so neither can decide differently about the same host.
#[cfg(unix)]
pub(crate) const CAN_ESCALATE: bool = true;
#[cfg(not(unix))]
pub(crate) const CAN_ESCALATE: bool = false;

pub(crate) fn run(p: Params) -> Result<()> {
    match uninstall::plan(&p.input())? {
        Next::Nothing(report) => {
            print_report(&report, false);
            println!("(nothing to do)");
            Ok(())
        }
        Next::Apply(prepared) => finish(&p, prepared),
        // Nothing here can become root, so the elevated step is not a step to
        // perform — it is a fact to report. When it covers only the system
        // service a user-scope install registered, the unprivileged half is
        // still ours to finish, and refusing to would leave the install fully
        // in place over a service we were never going to be able to remove.
        Next::Escalate(escalation)
            if !CAN_ESCALATE && escalation.covers == Covers::SystemServiceOnly =>
        {
            report_unremovable(&escalation);
            let input = UninstallInput { cross_scope: false, ..p.input() };
            match uninstall::plan(&input)? {
                Next::Nothing(report) => {
                    print_report(&report, false);
                    println!("(nothing to do at user scope)");
                    Ok(())
                }
                Next::Apply(prepared) => finish(&p, prepared),
                Next::Escalate(_) => {
                    anyhow::bail!("the user-scope teardown still reports needing root")
                }
            }
        }
        Next::Escalate(escalation) => {
            let covers = escalation.covers;
            elevate(&p, escalation)?;
            if covers == Covers::Everything {
                return Ok(());
            }
            // The elevated step removed only the system-scope service a
            // user-scope install registered; the rest needs no root.
            match uninstall::plan(&p.input())? {
                Next::Nothing(report) => {
                    print_report(&report, false);
                    Ok(())
                }
                Next::Apply(prepared) => finish(&p, prepared),
                Next::Escalate(_) => {
                    anyhow::bail!(
                        "the elevated step did not remove the system-scope service"
                    )
                }
            }
        }
    }
}

/// Name a system-scope remnant this process cannot remove, and say what would.
fn report_unremovable(escalation: &Escalation) {
    println!();
    println!(
        "Detected a system-scope install this process cannot remove without \
         administrator privileges:"
    );
    print_report(&escalation.plan, false);
    println!(
        "(open an elevated shell and run `netidx admin uninstall --scope system` \
         to remove it; continuing with the user-scope teardown)"
    );
}

/// Show what the engine resolved, gate it behind `--yes`, then apply.
fn finish(p: &Params, prepared: Prepared) -> Result<()> {
    // Surface what role this install is, when it left a provenance marker — a
    // confirmation of *what* is being torn down.
    if let Some(record) = prepared.record() {
        match &record.admin_domain {
            Some(net) => println!(
                "tearing down {} install (admin domain {:?})",
                record.role.as_str(),
                net.domain,
            ),
            None => {
                println!("tearing down {} install (local-only)", record.role.as_str())
            }
        }
    }
    print_report(prepared.plan(), false);
    if let Some(cross) = prepared.cross_scope_plan() {
        println!();
        println!(
            "Detected a matching system-scope service (a templated install \
             registers its service at system scope even when the config is \
             user-scope):"
        );
        print_report(cross, false);
    }
    if let Some(what) = prepared.deregistration() {
        println!(
            "admin server: would deregister {} from the CA at {}",
            what.server, what.ca
        );
    }
    if p.dry_run {
        return Ok(());
    }
    // Never destroy an install without explicit `--yes`; the plan above showed
    // exactly what would be removed.
    if !p.yes {
        if prepared.destroys_ca() {
            println!(
                "DESTRUCTIVE: this will remove the CA private key — anything \
                 signed by this CA cannot be re-issued."
            );
        }
        println!("re-run with --yes to apply.");
        return Ok(());
    }
    let mut ans =
        answer_cli::FlagAnswerer::install(None, false, None, false, None, false, None)?;
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let report = rt.block_on(uninstall::apply(&mut ans, prepared))?;
    print_report(&report, true);
    Ok(())
}

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

/// Show what the elevated step would do, then re-exec under sudo. Without
/// `--yes` we have shown the plan; point at the flag rather than escalating
/// unbidden. sudo's own password prompt is the credential check.
fn elevate(p: &Params, escalation: Escalation) -> Result<()> {
    if escalation.covers == Covers::SystemServiceOnly {
        println!();
        println!(
            "Detected a matching system-scope install (a templated install \
             registers its service at system scope even when the config is \
             user-scope):"
        );
        print_report(&escalation.plan, false);
        if p.dry_run {
            println!("(this would be removed too — sudo escalates for it)");
            return Ok(());
        }
        if !p.yes {
            if escalation.plan.ca_destroyed.is_some() {
                println!(
                    "(DESTRUCTIVE: also destroys the CA private key — re-run with \
                     `--yes` to remove it)"
                );
            } else {
                println!("(re-run with `--yes` to remove the system-scope service too)");
            }
            return Ok(());
        }
        println!(
            "removing the system-scope install (sudo may prompt for your password)…"
        );
    }
    reexec(p, &escalation)
}

#[cfg(unix)]
fn reexec(p: &Params, e: &Escalation) -> Result<()> {
    let exe = std::env::current_exe()
        .context("could not determine current binary for sudo re-exec")?;
    let mut cmd = Command::new(svc_cli::elevator());
    cmd.arg("--preserve-env=NETIDX_ELEVATED")
        .arg(&exe)
        .args(elevated_argv(e))
        .env(ELEVATED_ENV, "1");
    if p.yes {
        cmd.arg("--yes");
    }
    if p.dry_run {
        cmd.arg("--dry-run");
    }
    let status = cmd.status().with_context(|| {
        format!("spawning `{}` for privilege escalation", svc_cli::elevator())
    })?;
    if !status.success() {
        anyhow::bail!("escalation failed: {status}");
    }
    Ok(())
}

#[cfg(not(unix))]
fn reexec(_p: &Params, _e: &Escalation) -> Result<()> {
    anyhow::bail!(
        "system-scope uninstall on Windows requires an already-elevated shell. \
         Open an Administrator PowerShell / cmd and re-run this command."
    )
}

/// The `netidx admin uninstall` invocation that performs an [`Escalation`].
/// Both frontends spawn the elevated step, so both build their argument list
/// here rather than each spelling out the flags.
pub(crate) fn elevated_argv(e: &Escalation) -> Vec<String> {
    let mut args = vec![
        "admin".to_string(),
        "uninstall".to_string(),
        "--scope".to_string(),
        match e.scope {
            ServiceScope::User => "user".to_string(),
            ServiceScope::System => "system".to_string(),
        },
        "--for-user".to_string(),
        e.for_user.clone(),
        "--service-name".to_string(),
        e.service_name.clone(),
    ];
    if let Some(dir) = &e.config_dir {
        args.push("--config-dir".to_string());
        args.push(dir.display().to_string());
    }
    if e.remove_ca {
        args.push("--with-ca".to_string());
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    fn escalation(covers: Covers) -> Escalation {
        Escalation {
            scope: ServiceScope::System,
            service_name: "netidx".to_string(),
            for_user: "resolver".to_string(),
            config_dir: Some(PathBuf::from("/etc/netidx")),
            remove_ca: true,
            covers,
            plan: UninstallReport::default(),
        }
    }

    #[test]
    fn the_elevated_run_is_told_the_instance_user_and_the_directory() {
        let args = elevated_argv(&escalation(Covers::SystemServiceOnly));
        // Under `su` there is no $SUDO_USER for the child to infer the
        // templated unit's instance user from, so it must be explicit.
        assert!(args.windows(2).any(|a| a == ["--for-user", "resolver"]));
        assert!(args.windows(2).any(|a| a == ["--config-dir", "/etc/netidx"]));
        assert!(args.windows(2).any(|a| a == ["--scope", "system"]));
        assert!(args.iter().any(|a| a == "--with-ca"));
    }

    #[test]
    fn the_elevated_run_never_carries_consent_of_its_own() {
        // --yes and --dry-run are the caller's gate, appended by the spawner
        // per invocation; the argv the engine's plan implies must not smuggle
        // consent the operator did not give.
        let args = elevated_argv(&escalation(Covers::Everything));
        assert!(!args.iter().any(|a| a == "--yes" || a == "--dry-run"));
    }
}
