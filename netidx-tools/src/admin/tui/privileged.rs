//! Privileged follow-ups for the TUI.
//!
//! Installing or removing a **system-scope** OS service needs root. The strict
//! CLI re-execs itself under `sudo`; the TUI can't (that would replace the
//! process and kill the UI). Instead it **suspends** the ratatui terminal (drops
//! raw mode + the alternate screen), runs the privileged step as a child so its
//! password prompt reaches the operator on the normal terminal, then **resumes**.
//!
//! Privilege is gained with `sudo` when the operator has it, else by `su` to
//! root. The distinction matters: netidx is often run as a dedicated service
//! account (`netidx`, `resolver`, …) that is deliberately *not* in `sudoers`, so
//! `su` to root (with root's password) is the working path there.

use anyhow::{Context, Result, bail};
use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use netidx_admin::service::{self, ServiceParams, ServiceScope};
use std::{
    io::{Write, stdout},
    path::Path,
    process::Command,
};

/// Register the OS service at `scope`, returning a human summary line.
///
/// User scope is unprivileged and runs in-process (no terminal takeover); system
/// scope suspends the TUI and runs `netidx admin component service install` as
/// root.
pub(super) fn install_service(
    terminal: &mut ratatui::DefaultTerminal,
    scope: ServiceScope,
) -> Result<String> {
    match scope {
        ServiceScope::User => {
            let params = user_params()?;
            let installed = service::install(&params)?;
            Ok(format!("registered the user service ({})", installed.service_id))
        }
        ServiceScope::System => {
            let for_user = super::super::service::resolve_for_user(None)?;
            let exe = current_exe()?;
            let args = install_argv(&for_user, &exe);
            run_privileged(terminal, &exe, &args, true, "install the system service")?;
            Ok("registered the system service (netidx)".to_string())
        }
    }
}

/// Tear down an install whose config lives at `config_scope` in `config_dir`.
///
/// Runs the full `netidx admin uninstall` command so its cross-scope logic
/// applies — a resolver/publisher writes **user** config but registers a
/// **system** service, so a user-scope teardown must also drop that templated
/// service (which needs root; `needs_root` says so). A workstation (user config
/// + user service) needs no root. `--config-dir` is passed explicitly so it
/// still targets the right directory when run as root, and `--for-user` names
/// the system-service instance.
pub(super) fn uninstall(
    terminal: &mut ratatui::DefaultTerminal,
    config_scope: ServiceScope,
    config_dir: std::path::PathBuf,
    needs_root: bool,
    remove_ca: bool,
) -> Result<String> {
    let exe = current_exe()?;
    let mut args = vec![
        "admin".to_string(),
        "uninstall".to_string(),
        "--scope".to_string(),
        scope_flag(config_scope).to_string(),
        "--config-dir".to_string(),
        config_dir.display().to_string(),
        "--yes".to_string(),
    ];
    if needs_root {
        // Name the templated system service's instance user explicitly — under
        // `su` there is no $SUDO_USER for the child to infer it from.
        let for_user = super::super::service::resolve_for_user(None)?;
        args.push("--for-user".to_string());
        args.push(for_user);
    }
    if remove_ca {
        args.push("--with-ca".to_string());
    }
    run_privileged(terminal, &exe, &args, needs_root, "tear down the install")?;
    Ok("removed the install".to_string())
}

/// The `--scope` flag value for a scope.
fn scope_flag(scope: ServiceScope) -> &'static str {
    match scope {
        ServiceScope::User => "user",
        ServiceScope::System => "system",
    }
}

fn user_params() -> Result<ServiceParams> {
    Ok(ServiceParams {
        scope: ServiceScope::User,
        for_user: Some(super::super::service::resolve_for_user(None)?),
        binary: current_exe()?,
        service_name: ServiceParams::DEFAULT_NAME.to_string(),
        activation_dir: None,
    })
}

fn install_argv(for_user: &str, exe: &Path) -> Vec<String> {
    vec![
        "admin".to_string(),
        "component".to_string(),
        "service".to_string(),
        "install".to_string(),
        "--scope".to_string(),
        "system".to_string(),
        "--for-user".to_string(),
        for_user.to_string(),
        "--service-name".to_string(),
        ServiceParams::DEFAULT_NAME.to_string(),
        "--netidx-binary".to_string(),
        exe.display().to_string(),
    ]
}

fn current_exe() -> Result<std::path::PathBuf> {
    std::env::current_exe().context("could not determine the current netidx binary")
}

/// Suspend the TUI, run `<exe> <args>` (as root via sudo/su when `become_root`,
/// else as the current user), and resume.
#[cfg(unix)]
fn run_privileged(
    terminal: &mut ratatui::DefaultTerminal,
    exe: &Path,
    args: &[String],
    become_root: bool,
    what: &str,
) -> Result<()> {
    with_suspended(terminal, || {
        let esc = if become_root { detect_escalation() } else { Escalation::Direct };
        if become_root {
            println!("\nAdministrator privileges are needed to {what}.");
        }
        let status = match esc {
            Escalation::Direct => Command::new(exe).args(args).status(),
            Escalation::Sudo => Command::new("sudo").arg(exe).args(args).status(),
            Escalation::Su => {
                println!("You are not a sudoer here — switching to root (enter root's password):");
                let _ = stdout().flush();
                let mut line = sh_quote(&exe.display().to_string());
                for a in args {
                    line.push(' ');
                    line.push_str(&sh_quote(a));
                }
                Command::new("su").arg("-c").arg(&line).status()
            }
        };
        let status = status.context("spawning the privileged child")?;
        if !status.success() {
            bail!("the privileged step exited with {status}");
        }
        Ok(())
    })
}

#[cfg(not(unix))]
fn run_privileged(
    _terminal: &mut ratatui::DefaultTerminal,
    _exe: &Path,
    _args: &[String],
    _become_root: bool,
    what: &str,
) -> Result<()> {
    bail!(
        "to {what} on this platform, open an elevated (Administrator) shell and run \
         the equivalent `netidx admin` command"
    )
}

/// How this process can become root for a system-scope step.
#[cfg(unix)]
enum Escalation {
    /// Already root.
    Direct,
    /// The operator may `sudo`.
    Sudo,
    /// The operator has no sudo; `su` to root instead.
    Su,
}

/// Decide how to become root. Runs while the terminal is suspended, so the sudo
/// probe may prompt. Per the operator's guidance: use `sudo -l` to find out
/// whether we have sudo at all, and if not, `su` to root.
#[cfg(unix)]
fn detect_escalation() -> Escalation {
    if nix::unistd::geteuid().is_root() {
        return Escalation::Direct;
    }
    // Fast path: cached or passwordless sudo answers `-l` without a prompt.
    if quiet(Command::new("sudo").args(["-n", "-l"])) {
        return Escalation::Sudo;
    }
    // Otherwise ask sudo directly. `sudo -l` exits 0 iff the operator may run
    // sudo (prompting for their password once, which is fine — we're
    // suspended); a non-sudoer fails fast. Hide its listing on stdout but let
    // the password prompt (on the tty) through.
    let has_sudo = Command::new("sudo")
        .arg("-l")
        .stdout(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if has_sudo { Escalation::Sudo } else { Escalation::Su }
}

/// Run a command with its output discarded, returning whether it succeeded.
#[cfg(unix)]
fn quiet(cmd: &mut Command) -> bool {
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Single-quote a string for a POSIX shell (`su -c` runs its argument through a
/// shell, so each piece must be escaped).
#[cfg(unix)]
fn sh_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Drop raw mode + the alternate screen, run `f` (which spawns the privileged
/// child on the normal terminal), then restore, even if `f` failed.
fn with_suspended<T>(
    terminal: &mut ratatui::DefaultTerminal,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    disable_raw_mode().context("leaving raw mode")?;
    execute!(stdout(), LeaveAlternateScreen).context("leaving the alternate screen")?;
    let result = f();
    let resumed = (|| {
        enable_raw_mode().context("re-entering raw mode")?;
        execute!(stdout(), EnterAlternateScreen).context("re-entering the alternate screen")?;
        terminal.clear().context("clearing the terminal")?;
        Ok(())
    })();
    // The child's result takes priority; surface a resume failure only if the
    // child otherwise succeeded (a broken terminal we couldn't restore).
    match (result, resumed) {
        (Err(e), _) => Err(e),
        (Ok(_), Err(e)) => Err(e),
        (Ok(v), Ok(())) => Ok(v),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::sh_quote;

    #[test]
    fn sh_quote_wraps_and_escapes() {
        assert_eq!(sh_quote("plain"), "'plain'");
        assert_eq!(sh_quote("/usr/bin/netidx"), "'/usr/bin/netidx'");
        assert_eq!(sh_quote("a b"), "'a b'");
        // An embedded single quote closes, escapes, and reopens.
        assert_eq!(sh_quote("it's"), "'it'\\''s'");
    }
}
