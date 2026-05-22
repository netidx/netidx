//! systemd-specific install/uninstall/status for the netidx
//! activation service. Linux-only.
//!
//! The unit-file content lives in [`render_unit`] (pure, tested);
//! everything else (file writes, `systemctl` invocations) is shelled
//! out — there's no Rust crate worth pulling in for the small number
//! of `systemctl` commands we need.

use super::{
    InstalledService, ServiceParams, ServiceScope, ServiceStatus,
    ensure_valid_service_name, systemd_quote_exec_arg,
};
use anyhow::{Context, Result};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
};

/// Render the contents of a `.service` unit file. Pure — no I/O.
///
/// For [`ServiceScope::User`] this is a plain unit file the running
/// user installs to their own systemd config dir. For
/// [`ServiceScope::System`] this is a template (`%i`-substituted)
/// instantiated per service-user; the operator enables
/// `netidx@<user>.service`.
pub(super) fn render_unit(p: &ServiceParams) -> String {
    // Pipe both paths through `systemd_quote_exec_arg` so spaces in
    // `current_exe()` (common on macOS and corporate Linux installs
    // under `~/Documents/...`) and stray `%` chars (a systemd
    // specifier prefix that would otherwise be interpreted) don't
    // produce a broken ExecStart line. systemd parses ExecStart as
    // a whitespace-split token list with double-quote support, so
    // wrapping in `"..."` plus `%`-doubling is exactly the
    // contract the helper implements.
    let exe = systemd_quote_exec_arg(&p.binary.to_string_lossy());
    let mut exec_start = format!("{exe} activation -f");
    if let Some(dir) = &p.activation_dir {
        // `netidx activation` takes the unit directory via `--units`.
        exec_start.push_str(" --units ");
        exec_start
            .push_str(&systemd_quote_exec_arg(&dir.to_string_lossy()));
    }
    match p.scope {
        ServiceScope::User => format!(
            "[Unit]\n\
             Description=netidx activation supervisor\n\
             After=network.target\n\
             \n\
             [Service]\n\
             ExecStart={exec_start}\n\
             Restart=on-failure\n\
             RestartSec=2s\n\
             \n\
             [Install]\n\
             WantedBy=default.target\n",
        ),
        ServiceScope::System => format!(
            "[Unit]\n\
             Description=netidx activation supervisor for %i\n\
             After=network.target\n\
             \n\
             [Service]\n\
             # `%i` is the instance name — the user the service runs as.\n\
             # Operators enable this as `netidx@<user>.service`.\n\
             User=%i\n\
             ExecStart={exec_start}\n\
             Restart=on-failure\n\
             RestartSec=2s\n\
             \n\
             [Install]\n\
             WantedBy=multi-user.target\n",
        ),
    }
}

/// Path on disk where the unit file is written.
fn unit_path(p: &ServiceParams) -> Result<PathBuf> {
    match p.scope {
        ServiceScope::User => {
            // systemd looks at $XDG_CONFIG_HOME/systemd/user, falling
            // back to ~/.config/systemd/user. `dirs::config_dir`
            // already honours $XDG_CONFIG_HOME so we just use it.
            let mut p = dirs::config_dir().ok_or_else(|| {
                anyhow!("could not determine user config dir for systemd unit install")
            })?;
            p.push("systemd");
            p.push("user");
            Ok(p)
        }
        ServiceScope::System => Ok(PathBuf::from("/etc/systemd/system")),
    }
    .map(|dir| {
        let filename = match p.scope {
            ServiceScope::User => format!("{}.service", p.service_name),
            // Templated unit: `<name>@.service`. systemctl
            // instantiates per-user as `<name>@<user>.service`.
            ServiceScope::System => format!("{}@.service", p.service_name),
        };
        dir.join(filename)
    })
}

/// The handle systemctl uses to refer to this service, including
/// the instance suffix for system-scope templated units.
fn service_id(p: &ServiceParams, for_user: &str) -> String {
    match p.scope {
        ServiceScope::User => format!("{}.service", p.service_name),
        ServiceScope::System => format!("{}@{}.service", p.service_name, for_user),
    }
}

pub(super) fn install(p: &ServiceParams) -> Result<InstalledService> {
    ensure_valid_service_name(&p.service_name)?;
    let path = unit_path(p)?;
    let body = render_unit(p);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {parent:?}"))?;
    }
    std::fs::write(&path, body.as_bytes())
        .with_context(|| format!("writing unit file {path:?}"))?;
    // 0644 is the systemd-shipped convention; matches what `apt`
    // installs and what `systemctl edit` produces.
    set_mode(&path, 0o644)?;
    let for_user = resolve_for_user(p)?;
    let id = service_id(p, &for_user);
    run_systemctl(p.scope, &["daemon-reload"]).context("systemctl daemon-reload")?;
    run_systemctl(p.scope, &["enable", "--now", &id])
        .with_context(|| format!("systemctl enable --now {id}"))?;
    Ok(InstalledService { unit_path: path, service_id: id })
}

pub(super) fn uninstall(p: &ServiceParams) -> Result<()> {
    ensure_valid_service_name(&p.service_name)?;
    let for_user = resolve_for_user(p)?;
    let id = service_id(p, &for_user);
    let path = unit_path(p)?;
    // Best-effort disable + stop; the unit may already be stopped.
    let _ = run_systemctl(p.scope, &["disable", "--now", &id]);
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing unit file {path:?}"))?;
    }
    // daemon-reload after the file is gone so systemd forgets it.
    let _ = run_systemctl(p.scope, &["daemon-reload"]);
    Ok(())
}

pub(super) fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    ensure_valid_service_name(&p.service_name)?;
    let path = unit_path(p)?;
    if !path.exists() {
        return Ok(ServiceStatus::NotInstalled);
    }
    let for_user = resolve_for_user(p)?;
    let id = service_id(p, &for_user);
    // `systemctl is-active` exits 0 when active, non-zero otherwise.
    // Parsing exit code is more reliable than parsing stdout text.
    let mut cmd = systemctl_command(p.scope);
    cmd.arg("is-active").arg(&id);
    let status = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("running systemctl is-active")?;
    Ok(if status.success() {
        ServiceStatus::Active
    } else {
        ServiceStatus::Inactive
    })
}

fn resolve_for_user(p: &ServiceParams) -> Result<String> {
    if let Some(u) = &p.for_user {
        return Ok(u.clone());
    }
    // Fall back to the invoking user's name. The CLI normally fills
    // `for_user` in explicitly, so this branch is a safety net for
    // direct library use.
    let uid = nix::unistd::geteuid();
    let user = nix::unistd::User::from_uid(uid)
        .ok()
        .flatten()
        .ok_or_else(|| anyhow!("could not resolve current uid {uid} to a username"))?;
    Ok(user.name)
}

fn systemctl_command(scope: ServiceScope) -> Command {
    let mut c = Command::new("systemctl");
    if scope == ServiceScope::User {
        c.arg("--user");
    }
    c
}

fn run_systemctl(scope: ServiceScope, args: &[&str]) -> Result<()> {
    let mut cmd = systemctl_command(scope);
    cmd.args(args);
    let status =
        cmd.status().context("spawning systemctl (is it installed?)")?;
    if !status.success() {
        bail!("systemctl {args:?} failed: {status}");
    }
    Ok(())
}

#[cfg(unix)]
fn set_mode(path: &std::path::Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("chmod {mode:o} on {path:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(scope: ServiceScope) -> ServiceParams {
        ServiceParams {
            scope,
            for_user: Some("alice".into()),
            binary: PathBuf::from("/usr/local/bin/netidx"),
            service_name: "netidx".into(),
            activation_dir: None,
        }
    }

    #[test]
    fn user_unit_runs_as_invoking_user() {
        let body = render_unit(&params(ServiceScope::User));
        // No User= line on a user-scope unit — systemd inherits the
        // user from the session.
        assert!(!body.contains("User="), "got: {body}");
        assert!(body.contains("ExecStart=/usr/local/bin/netidx activation -f"));
        assert!(body.contains("Restart=on-failure"));
        // User services target `default.target`, system services
        // target `multi-user.target` — easy to flip by accident.
        assert!(body.contains("WantedBy=default.target"));
    }

    #[test]
    fn system_unit_is_templated_with_user_substitution() {
        let body = render_unit(&params(ServiceScope::System));
        // `%i` substitution is what makes the system unit reusable
        // across service accounts — `User=%i` becomes `User=alice`
        // for `netidx@alice.service`. Catch a literal-`alice` regression.
        assert!(body.contains("User=%i"));
        assert!(!body.contains("User=alice"));
        assert!(body.contains("WantedBy=multi-user.target"));
    }

    #[test]
    fn activation_dir_override_propagates_to_exec_start() {
        let mut p = params(ServiceScope::User);
        p.activation_dir = Some(PathBuf::from("/var/lib/netidx/units"));
        let body = render_unit(&p);
        assert!(
            body.contains("ExecStart=/usr/local/bin/netidx activation -f --units /var/lib/netidx/units"),
            "got: {body}",
        );
    }

    #[test]
    fn user_unit_path_lives_under_systemd_user_dir() {
        let p = params(ServiceScope::User);
        let path = unit_path(&p).unwrap();
        assert!(path.ends_with("systemd/user/netidx.service"), "got: {path:?}");
    }

    #[test]
    fn system_unit_path_is_templated_at_etc() {
        let p = params(ServiceScope::System);
        let path = unit_path(&p).unwrap();
        assert_eq!(path, PathBuf::from("/etc/systemd/system/netidx@.service"));
    }

    #[test]
    fn system_service_id_includes_user() {
        let p = params(ServiceScope::System);
        assert_eq!(service_id(&p, "alice"), "netidx@alice.service");
    }

    #[test]
    fn user_service_id_has_no_instance_suffix() {
        let p = params(ServiceScope::User);
        assert_eq!(service_id(&p, "alice"), "netidx.service");
    }

    /// `ExecStart=` must survive a binary path with spaces. Before
    /// the escape fix the line ended up as
    /// `ExecStart=/Applications/My App/netidx activation -f`, which
    /// systemd splits into 5 tokens, calls `/Applications/My` with
    /// the rest as args, and the unit fails at start with ENOENT.
    #[test]
    fn render_unit_quotes_paths_with_spaces() {
        let mut p = params(ServiceScope::User);
        p.binary = PathBuf::from("/Applications/My App/netidx");
        p.activation_dir = Some(PathBuf::from("/var/lib/netidx units"));
        let body = render_unit(&p);
        assert!(
            body.contains(
                "ExecStart=\"/Applications/My App/netidx\" activation -f \
                 --units \"/var/lib/netidx units\""
            ),
            "got: {body}",
        );
    }

    /// `%` is a systemd specifier prefix and must always be escaped
    /// to `%%` in ExecStart, regardless of quoting. A path like
    /// `/srv/100%backup/netidx` would otherwise have `%b` consumed
    /// by systemd as the boot-id specifier.
    #[test]
    fn render_unit_doubles_percent_in_paths() {
        let mut p = params(ServiceScope::User);
        p.binary = PathBuf::from("/srv/100%backup/netidx");
        let body = render_unit(&p);
        assert!(
            body.contains("ExecStart=/srv/100%%backup/netidx activation -f"),
            "got: {body}",
        );
    }

    #[test]
    fn install_uninstall_status_reject_invalid_service_name() {
        // The validator is called from all three entry points; we
        // can only directly exercise the error path because the
        // success path would actually try to talk to systemd.
        let mut p = params(ServiceScope::User);
        p.service_name = "../../etc/passwd".into();
        assert!(install(&p).is_err());
        assert!(uninstall(&p).is_err());
        assert!(status(&p).is_err());
    }
}
