//! systemd-specific install/uninstall/status for the netidx
//! activation service. Linux-only.
//!
//! The unit-file content lives in [`render_unit`] (pure, tested);
//! everything else (file writes, `systemctl` invocations) is shelled
//! out — there's no Rust crate worth pulling in for the small number
//! of `systemctl` commands we need.

use super::{InstalledService, ServiceParams, ServiceScope, ServiceStatus};
use anyhow::{Context, Result};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
};

/// Quote an argument for systemd's `ExecStart=` token list. systemd
/// parses ExecStart as a whitespace-split sequence of tokens with
/// double-quote support and treats `%` as a specifier prefix
/// regardless of quoting. Both rules need to hold for any path
/// containing spaces or `%`, which is realistic for `current_exe()`
/// on macOS / WSL / `~/Documents/...` installs.
///
/// Rules (cross-checked against `man systemd.service`):
/// - `%` always doubled to `%%` to escape the specifier syntax.
/// - If the resulting string contains any whitespace or quoting
///   metacharacter (`"`, `'`, `\`, `;`, `\n`), wrap the whole arg in
///   `"..."` and backslash-escape `"` and `\` inside the quotes.
#[cfg(target_os = "linux")]
pub(super) fn quote_exec_arg(s: &str) -> String {
    let percent_escaped: String =
        s.chars().flat_map(|c| if c == '%' { vec!['%', '%'] } else { vec![c] }).collect();
    let needs_quoting = percent_escaped
        .chars()
        .any(|c| c.is_whitespace() || matches!(c, '"' | '\'' | '\\' | ';'));
    if !needs_quoting {
        return percent_escaped;
    }
    let mut out = String::with_capacity(percent_escaped.len() + 2);
    out.push('"');
    for c in percent_escaped.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

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
    let exe = quote_exec_arg(&p.binary.to_string_lossy());
    let mut exec_start = format!("{exe} activation -f");
    if let Some(dir) = &p.activation_dir {
        // `netidx activation` takes the unit directory via `--units`.
        exec_start.push_str(" --units ");
        exec_start.push_str(&quote_exec_arg(&dir.to_string_lossy()));
    }
    match p.scope {
        ServiceScope::User => format!(
            r#"[Unit]
Description=netidx activation supervisor
After=network.target

[Service]
ExecStart={exec_start}
Restart=on-failure
RestartSec=2s

[Install]
WantedBy=default.target
"#
        ),
        ServiceScope::System => format!(
            r#"[Unit]
Description=netidx activation supervisor for %i
After=network.target

[Service]
# `%i` is the instance name — the user the service runs as.
# Operators enable this as `netidx@<user>.service`.
User=%i
ExecStart={exec_start}
Restart=on-failure
RestartSec=2s

[Install]
WantedBy=multi-user.target
"#
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
    let for_user = resolve_for_user(p)?;
    let id = service_id(p, &for_user);
    let path = unit_path(p)?;
    if !path.exists() {
        return Ok(());
    }

    // The command result alone is not authoritative: systemctl may report an
    // error after completing part (or all) of the requested transition. What
    // makes it safe to remove the definition is the independently verified end
    // state: no running supervisor and no boot-time enablement.
    let transition = run_systemctl(p.scope, &["disable", "--now", &id]);
    let active = systemctl_property(p.scope, &id, "ActiveState")?;
    let enabled = systemctl_enabled_state(p.scope, &id)?;
    verify_teardown(transition, &active, &enabled)?;
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing unit file {path:?}"))?;
    }
    // daemon-reload after the file is gone so systemd forgets it.
    run_systemctl(p.scope, &["daemon-reload"]).context("systemctl daemon-reload")?;
    Ok(())
}

pub(super) fn status(p: &ServiceParams) -> Result<ServiceStatus> {
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
    Ok(if status.success() { ServiceStatus::Active } else { ServiceStatus::Inactive })
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
    // Capture systemctl's output rather than inherit it: a caller may be a live
    // full-screen TUI, and systemctl's "Created symlink …" chatter on enable
    // would scribble on the alternate screen and desync its cell diff. Nothing
    // it prints on success is worth showing; on failure its stderr goes into the
    // error instead.
    let mut cmd = systemctl_command(scope);
    cmd.args(args).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped());
    let out = cmd.output().context("spawning systemctl (is it installed?)")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        match stderr.trim() {
            "" => bail!("systemctl {args:?} failed: {}", out.status),
            msg => bail!("systemctl {args:?} failed: {}: {msg}", out.status),
        }
    }
    Ok(())
}

/// Query one stable systemd property. Unlike `is-active`, `show` distinguishes
/// a real inactive/failed state from an inability to ask systemd at all.
fn systemctl_property(scope: ServiceScope, id: &str, property: &str) -> Result<String> {
    let mut cmd = systemctl_command(scope);
    let property_arg = format!("--property={property}");
    let out = cmd
        .args(["show", property_arg.as_str(), "--value", id])
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("querying systemd {property} for {id}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        bail!(
            "systemctl show {property} {id} failed: {}{}",
            out.status,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", stderr.trim())
            }
        );
    }
    let value = String::from_utf8(out.stdout)
        .with_context(|| format!("systemd returned non-UTF-8 {property} for {id}"))?;
    let value = value.trim();
    if value.is_empty() {
        bail!("systemd returned an empty {property} for {id}");
    }
    Ok(value.to_string())
}

/// `systemctl is-enabled` intentionally exits nonzero for the safe `disabled`
/// state, so classify its stdout and reserve command errors for missing/unknown
/// output.
fn systemctl_enabled_state(scope: ServiceScope, id: &str) -> Result<String> {
    let mut cmd = systemctl_command(scope);
    let out = cmd
        .args(["is-enabled", id])
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("querying whether systemd unit {id} is enabled"))?;
    let value = String::from_utf8(out.stdout)
        .with_context(|| format!("systemd returned non-UTF-8 enablement for {id}"))?;
    let value = value.trim();
    if value.is_empty() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        bail!(
            "systemctl is-enabled {id} returned no state: {}{}",
            out.status,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!(": {}", stderr.trim())
            }
        );
    }
    Ok(value.to_string())
}

fn verify_uninstalled_state(active: &str, enabled: &str) -> Result<()> {
    if !matches!(active, "inactive" | "failed") {
        bail!("refusing to remove the unit while systemd ActiveState is {active:?}");
    }
    if !matches!(enabled, "disabled" | "masked" | "masked-runtime") {
        bail!("refusing to remove the unit while systemd enablement is {enabled:?}");
    }
    Ok(())
}

fn verify_teardown(transition: Result<()>, active: &str, enabled: &str) -> Result<()> {
    match verify_uninstalled_state(active, enabled) {
        Ok(()) => Ok(()),
        Err(state_err) => match transition {
            Ok(()) => Err(state_err),
            Err(command_err) => Err(state_err.context(command_err)),
        },
    }
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
    fn teardown_requires_inactive_and_disabled_end_state() {
        for active in ["inactive", "failed"] {
            for enabled in ["disabled", "masked", "masked-runtime"] {
                verify_uninstalled_state(active, enabled).unwrap();
            }
        }
        for active in ["active", "activating", "deactivating", "reloading"] {
            assert!(verify_uninstalled_state(active, "disabled").is_err());
        }
        for enabled in ["enabled", "enabled-runtime", "linked", "static", "unknown"] {
            assert!(verify_uninstalled_state("inactive", enabled).is_err());
        }

        // A command may return failure after reaching the requested state; the
        // independently observed final state wins. Conversely, command success
        // never excuses an unsafe final state.
        verify_teardown(
            Err(anyhow!("transient systemctl error")),
            "inactive",
            "disabled",
        )
        .unwrap();
        assert!(verify_teardown(Ok(()), "active", "disabled").is_err());
        let err = verify_teardown(Err(anyhow!("disable failed")), "active", "enabled")
            .unwrap_err();
        let err = format!("{err:#}");
        assert!(err.contains("disable failed") && err.contains("ActiveState"));
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
}
