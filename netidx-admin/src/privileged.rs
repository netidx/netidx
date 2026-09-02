//! The privileged handoff a frontend performs on the operator's terminal.
//!
//! Installing or removing a **system-scope** OS service needs root. A
//! frontend that owns the terminal (a TUI) releases it, runs the step as a
//! child so the password prompt reaches the operator, and resumes. The
//! decision of *how* to become root — `sudo` when the operator has it, else
//! `su` to root (netidx is often run as a dedicated service account that is
//! deliberately not in `sudoers`) — is made by the child itself, inside the
//! released terminal, because probing `sudo` may prompt.

use crate::service::{self, ServiceParams, ServiceScope, ServiceStatus};
use anyhow::{Context, Result};
use std::path::Path;

/// Single-quote a string for a POSIX shell (`su -c` runs its argument through
/// a shell, so each piece must be escaped).
pub fn sh_quote(s: &str) -> String {
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

/// The shell that becomes root and execs `$@`: as is when already root,
/// under `sudo` when the operator may use it (a cached credential first,
/// else one prompt), else under `su` with root's password. `$1` is the
/// same command line quoted for `su -c`.
#[cfg(unix)]
const BECOME_ROOT: &str = r#"su_cmd="$1"; shift
if [ "$(id -u)" = 0 ]; then exec "$@"; fi
if sudo -n -l >/dev/null 2>&1; then exec sudo "$@"; fi
if sudo -l >/dev/null; then exec sudo "$@"; fi
echo "You are not a sudoer here - switching to root (enter root's password):"
exec su -c "$su_cmd""#;

/// The program and arguments a frontend runs on the released terminal to
/// perform `exe args…` as root.
#[cfg(unix)]
pub fn privileged_command(exe: &Path, args: &[String]) -> Result<(String, Vec<String>)> {
    let mut su_cmd = sh_quote(&exe.display().to_string());
    for a in args {
        su_cmd.push(' ');
        su_cmd.push_str(&sh_quote(a));
    }
    let mut argv = vec![
        "-c".to_string(),
        BECOME_ROOT.to_string(),
        "netidx-privileged".to_string(),
        su_cmd,
        exe.display().to_string(),
    ];
    argv.extend(args.iter().cloned());
    Ok(("/bin/sh".to_string(), argv))
}

#[cfg(not(unix))]
pub fn privileged_command(
    _exe: &Path,
    _args: &[String],
) -> Result<(String, Vec<String>)> {
    anyhow::bail!(
        "on this platform, open an elevated (Administrator) shell and run the \
         equivalent `netidx admin` command"
    )
}

/// The `netidx admin host service install` invocation that registers the
/// system-scope service.
pub fn install_service_argv(
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

/// Reconcile the terminal-owning child's result with the service manager's
/// authoritative state. Only a running service is success: an
/// installed-but-inactive unit is the normal residue of `enable --now`
/// failing, not evidence that installation completed. The handoff itself is
/// inherently flaky (a lost tty, a bumpy resume), so the unit's real state
/// is consulted before a reported failure is believed.
pub fn system_service_outcome(
    service_name: &str,
    ran: Result<()>,
    status: Result<ServiceStatus>,
) -> Result<String> {
    let incomplete = |state: anyhow::Error| match ran {
        Ok(()) => Err(state),
        Err(child) => Err(state.context(child)),
    };
    match status {
        Ok(ServiceStatus::Active) => {
            Ok(format!("registered the system service ({service_name})"))
        }
        Ok(ServiceStatus::Inactive) => incomplete(anyhow::anyhow!(
            "the system service unit is installed but inactive; installation is incomplete"
        )),
        Ok(ServiceStatus::NotInstalled) => incomplete(anyhow::anyhow!(
            "the system service unit is not installed; installation is incomplete"
        )),
        Err(status_err) => incomplete(status_err.context(
            "could not verify that the system service is active; installation is incomplete",
        )),
    }
}

/// Query the system-scope service registered for `for_user` and judge the
/// handoff by it: `ran` is what the child reported.
pub fn verify_system_service(
    service_name: &str,
    for_user: &str,
    ran: Result<()>,
) -> Result<String> {
    let params = ServiceParams {
        scope: ServiceScope::System,
        for_user: Some(for_user.to_string()),
        binary: std::path::PathBuf::new(),
        service_name: service_name.to_string(),
        activation_dir: None,
    };
    system_service_outcome(service_name, ran, service::status(&params))
}

/// The netidx binary this process runs as — what the privileged child
/// re-runs.
pub fn current_exe() -> Result<std::path::PathBuf> {
    std::env::current_exe().context("could not determine the current netidx binary")
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn sh_quote_wraps_and_escapes() {
        assert_eq!(sh_quote("plain"), "'plain'");
        assert_eq!(sh_quote("/usr/bin/netidx"), "'/usr/bin/netidx'");
        assert_eq!(sh_quote("a b"), "'a b'");
        assert_eq!(sh_quote("it's"), "'it'\\''s'");
    }

    #[test]
    fn privileged_service_install_pins_the_activation_directory() {
        let args = install_service_argv(
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

    #[cfg(unix)]
    #[test]
    fn the_privileged_child_carries_both_spellings_of_the_command() {
        let (program, argv) = privileged_command(
            Path::new("/usr/bin/netidx"),
            &["admin".to_string(), "it's".to_string()],
        )
        .unwrap();
        assert_eq!(program, "/bin/sh");
        assert_eq!(argv[0], "-c");
        // the su form is one quoted line; the sudo/exec form is positional
        assert_eq!(argv[3], "'/usr/bin/netidx' 'admin' 'it'\\''s'");
        assert_eq!(&argv[4..], ["/usr/bin/netidx", "admin", "it's"]);
    }

    #[test]
    fn active_state_is_the_only_success_and_overrides_child_failure() {
        assert!(
            system_service_outcome("netidx", Ok(()), Ok(ServiceStatus::Active)).is_ok()
        );
        assert!(
            system_service_outcome(
                "netidx",
                Err(anyhow!("lost terminal")),
                Ok(ServiceStatus::Active)
            )
            .is_ok()
        );
        for state in [ServiceStatus::Inactive, ServiceStatus::NotInstalled] {
            assert!(system_service_outcome("netidx", Ok(()), Ok(state)).is_err());
            let err =
                system_service_outcome("netidx", Err(anyhow!("child failed")), Ok(state))
                    .unwrap_err();
            assert!(format!("{err:#}").contains("child failed"));
        }
    }

    #[test]
    fn unverifiable_status_is_failure_regardless_of_child_result() {
        let err = system_service_outcome("netidx", Ok(()), Err(anyhow!("status failed")))
            .unwrap_err();
        assert!(format!("{err:#}").contains("status failed"));
        let err = system_service_outcome(
            "netidx",
            Err(anyhow!("child failed")),
            Err(anyhow!("status failed")),
        )
        .unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("child failed") && err.contains("status failed"));
    }
}
