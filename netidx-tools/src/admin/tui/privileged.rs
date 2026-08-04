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
    terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    },
};
use netidx_activation::runtime::default_units_dir;
use netidx_admin::service::{self, ServiceParams, ServiceScope, ServiceStatus};
#[cfg(unix)]
use std::{io::Write, process::Command};
use std::{io::stdout, path::Path};

/// Register the OS service at `scope`, returning a human summary line.
///
/// User scope is unprivileged and runs in-process (no terminal takeover); system
/// scope suspends the TUI and runs `netidx admin host service install` as
/// root.
pub(super) fn install_service(
    terminal: &mut ratatui::DefaultTerminal,
    request: &super::action::ServiceInstall,
) -> Result<String> {
    match request.scope {
        ServiceScope::User => {
            let params = user_params(&request.name)?;
            let installed = service::install(&params)?;
            Ok(format!("registered the user service ({})", installed.service_id))
        }
        ServiceScope::System => {
            let for_user = match &request.for_user {
                Some(for_user) => for_user.clone(),
                None => netidx_admin::service::resolve_for_user(None)?,
            };
            let exe = current_exe()?;
            let activation_dir = activation_dir()?;
            let args = install_argv(&for_user, &request.name, &exe, &activation_dir);
            let params = ServiceParams {
                scope: ServiceScope::System,
                for_user: Some(for_user),
                binary: exe.clone(),
                service_name: request.name.clone(),
                activation_dir: Some(activation_dir),
            };
            let ran =
                run_privileged(terminal, &exe, &args, true, "install the system service");
            // The privileged handoff (suspend → sudo/su → resume) is inherently
            // flaky: a lost tty or a bumpy terminal resume can report failure even
            // when the child already installed the unit. The unit's real state is
            // authoritative, and querying it needs no privilege — so consult it
            // before believing a reported failure.
            system_install_outcome(&request.name, ran, service::status(&params))
        }
    }
}

/// Reconcile the terminal-owning child's result with the service manager's
/// authoritative state. Only a running service is success. In particular, an
/// installed-but-inactive unit is the normal residue of `enable --now`
/// failing, not evidence that installation completed.
fn system_install_outcome(
    service_name: &str,
    ran: Result<()>,
    status: Result<ServiceStatus>,
) -> Result<String> {
    match status {
        Ok(ServiceStatus::Active) => {
            Ok(format!("registered the system service ({service_name})"))
        }
        Ok(ServiceStatus::Inactive) => {
            let state = anyhow::anyhow!(
                "the system service unit is installed but inactive; installation is incomplete"
            );
            match ran {
                Ok(()) => Err(state),
                Err(child) => Err(state.context(child)),
            }
        }
        Ok(ServiceStatus::NotInstalled) => {
            let state = anyhow::anyhow!(
                "the system service unit is not installed; installation is incomplete"
            );
            match ran {
                Ok(()) => Err(state),
                Err(child) => Err(state.context(child)),
            }
        }
        Err(status_err) => {
            let state = status_err.context(
                "could not verify that the system service is active; installation is incomplete",
            );
            match ran {
                Ok(()) => Err(state),
                Err(child) => Err(state.context(child)),
            }
        }
    }
}

/// Perform the one elevated step a teardown needs, as the engine described it.
///
/// The engine already decided *that* root is required, from an actual probe of
/// what is installed; this only becomes root and runs the same
/// `netidx admin uninstall` the strict CLI would — the argument list comes
/// from [`elevated_argv`](super::super::uninstall::elevated_argv), so the two
/// frontends cannot spawn different commands.
pub(super) fn uninstall(
    terminal: &mut ratatui::DefaultTerminal,
    escalation: &netidx_admin::uninstall::Escalation,
) -> Result<String> {
    let exe = current_exe()?;
    let mut args = super::super::uninstall::elevated_argv(escalation);
    // The operator confirmed in the TUI; the elevated child must not prompt.
    args.push("--yes".to_string());
    run_privileged(terminal, &exe, &args, true, "tear down the install")?;
    Ok("removed the system-scope install".to_string())
}

/// Suspend the TUI and drop the operator into their `$EDITOR` on `seed`,
/// validating (and re-editing on failure) via the shared editor loop, then
/// resume. Returns the normalized text. Used by the roster (policy JSON) and
/// perms (perms JSON) panels — the one place the TUI hands off to a full editor.
pub(super) fn edit_in_terminal(
    terminal: &mut ratatui::DefaultTerminal,
    seed: &str,
    validate: super::answer::EditValidator,
) -> Result<String> {
    with_suspended(terminal, || {
        super::super::editor::edit_with_validation(seed, |s| validate(s))
    })
}

fn user_params(service_name: &str) -> Result<ServiceParams> {
    Ok(ServiceParams {
        scope: ServiceScope::User,
        for_user: Some(netidx_admin::service::resolve_for_user(None)?),
        binary: current_exe()?,
        service_name: service_name.to_string(),
        activation_dir: Some(activation_dir()?),
    })
}

fn activation_dir() -> Result<std::path::PathBuf> {
    default_units_dir().context("no default activation unit directory was found")
}

fn install_argv(
    for_user: &str,
    service_name: &str,
    exe: &Path,
    activation_dir: &Path,
) -> Vec<String> {
    vec![
        "admin".to_string(),
        "host".to_string(),
        "service".to_string(),
        "install".to_string(),
        "--scope".to_string(),
        "system".to_string(),
        "--for-user".to_string(),
        for_user.to_string(),
        "--service-name".to_string(),
        service_name.to_string(),
        "--netidx-binary".to_string(),
        exe.display().to_string(),
        "--activation-dir".to_string(),
        activation_dir.display().to_string(),
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
                println!(
                    "You are not a sudoer here — switching to root (enter root's password):"
                );
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
        execute!(stdout(), EnterAlternateScreen)
            .context("re-entering the alternate screen")?;
        // Force a full repaint. `Terminal::clear` can't be trusted here: in
        // ratatui 0.30 it opens with a cursor-position DSR round-trip on stdin,
        // which is unreliable immediately after a child process and an
        // alternate-screen switch — if it misparses, the back buffer is never
        // reset and the next draw diffs against the pre-suspend frame, leaving
        // the child's output on screen. `resize` does the same clear + back-
        // buffer reset with no stdin round-trip, so the next draw repaints every
        // cell.
        let size = terminal.size().context("querying terminal size")?;
        terminal.resize(size.into()).context("repainting the terminal")?;
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
    use super::{install_argv, sh_quote, system_install_outcome};
    use anyhow::anyhow;
    use netidx_admin::service::ServiceStatus;
    use std::path::Path;

    #[test]
    fn sh_quote_wraps_and_escapes() {
        assert_eq!(sh_quote("plain"), "'plain'");
        assert_eq!(sh_quote("/usr/bin/netidx"), "'/usr/bin/netidx'");
        assert_eq!(sh_quote("a b"), "'a b'");
        // An embedded single quote closes, escapes, and reopens.
        assert_eq!(sh_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn privileged_service_install_pins_the_activation_directory() {
        let args = install_argv(
            "root",
            "netidx",
            Path::new("/usr/local/bin/netidx"),
            Path::new("/root/.config/netidx/activation"),
        );
        assert!(
            args.windows(2).any(|a| {
                a == ["--activation-dir", "/root/.config/netidx/activation"]
            })
        );
        assert!(args.windows(2).any(|a| a == ["--service-name", "netidx"]));
    }

    #[test]
    fn active_state_is_the_only_success_and_overrides_child_failure() {
        assert!(
            system_install_outcome("netidx", Ok(()), Ok(ServiceStatus::Active)).is_ok()
        );
        assert!(
            system_install_outcome(
                "netidx",
                Err(anyhow!("lost terminal")),
                Ok(ServiceStatus::Active)
            )
            .is_ok()
        );

        for state in [ServiceStatus::Inactive, ServiceStatus::NotInstalled] {
            assert!(system_install_outcome("netidx", Ok(()), Ok(state)).is_err());
            let err =
                system_install_outcome("netidx", Err(anyhow!("child failed")), Ok(state))
                    .unwrap_err();
            assert!(format!("{err:#}").contains("child failed"));
        }
    }

    #[test]
    fn unverifiable_status_is_failure_regardless_of_child_result() {
        let err = system_install_outcome("netidx", Ok(()), Err(anyhow!("status failed")))
            .unwrap_err();
        assert!(format!("{err:#}").contains("status failed"));

        let err = system_install_outcome(
            "netidx",
            Err(anyhow!("child failed")),
            Err(anyhow!("status failed")),
        )
        .unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("child failed") && err.contains("status failed"));
    }
}
