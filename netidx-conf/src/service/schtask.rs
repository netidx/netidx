//! Windows install/uninstall/status for the netidx activation service.
//!
//! Windows is workstation-only and these users typically have **no local
//! Administrator rights**, so there is no Windows Service / SCM here. The
//! supervisor runs as the logged-in user, started by a **per-user logon
//! Scheduled Task** — the Windows analog of `systemctl --user enable`: no
//! elevation, runs in the user's session, stops at logoff.
//!
//! The task definition is built as XML ([`render_task_xml`], pure and
//! tested) and registered with `schtasks /Create /XML`, which cleanly
//! separates the command from its arguments (no `/TR` quoting hazard) and
//! lets us mark the task `Hidden`. The supervisor additionally hides its
//! own console window (see `netidx-tools` `activation::hide_console_window`)
//! so nothing flashes at logon.

use super::{InstalledService, ServiceParams, ServiceScope, ServiceStatus};
use anyhow::{Context, Result, bail};
use std::process::{Command, Stdio};

/// Escape a string for inclusion in XML text/attribute content.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// The principal the task runs as: `DOMAIN\user`. Prefer an explicit
/// `for_user` (already domain-qualified if it contains a backslash),
/// otherwise compose it from the environment.
fn principal(p: &ServiceParams) -> Result<String> {
    if let Some(u) = &p.for_user {
        if u.contains('\\') {
            return Ok(u.clone());
        }
    }
    let user = p
        .for_user
        .clone()
        .or_else(|| std::env::var("USERNAME").ok())
        .filter(|s| !s.is_empty())
        .context("could not determine the current Windows user (%USERNAME%)")?;
    let domain = std::env::var("USERDOMAIN")
        .ok()
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .filter(|s| !s.is_empty());
    Ok(match domain {
        Some(d) => format!("{d}\\{user}"),
        None => user,
    })
}

/// Render the Scheduled Task XML. `principal` is `DOMAIN\user`; `command`
/// is the absolute exe path; `arguments` is the full argument string
/// (already internally quoted as needed). Pure — no I/O.
fn render_task_xml(principal: &str, command: &str, arguments: &str) -> String {
    let principal = xml_escape(principal);
    let command = xml_escape(command);
    let arguments = xml_escape(arguments);
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>netidx activation supervisor</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{principal}</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{principal}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>true</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{command}</Command>
      <Arguments>{arguments}</Arguments>
    </Exec>
  </Actions>
</Task>
"#
    )
}

/// The argument string the task runs: `activation` plus `--units "<dir>"`
/// when an explicit activation directory is given (otherwise the
/// supervisor's own per-user default applies).
fn task_arguments(p: &ServiceParams) -> String {
    match &p.activation_dir {
        Some(dir) => format!("activation --units \"{}\"", dir.display()),
        None => "activation".to_string(),
    }
}

/// Run `schtasks` with `args`, returning its captured output on success
/// or an error including stderr.
fn schtasks(args: &[&str]) -> Result<std::process::Output> {
    let out = Command::new("schtasks")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .context("running schtasks.exe")?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        bail!("schtasks {:?} failed ({}): {}", args, out.status, stderr.trim());
    }
    Ok(out)
}

pub(super) fn install(p: &ServiceParams) -> Result<InstalledService> {
    if p.scope == ServiceScope::System {
        bail!(
            "system-scope services are not supported on Windows (netidx is \
             workstation-only); install at user scope, which registers a \
             per-user logon Scheduled Task"
        );
    }
    let principal = principal(p)?;
    let command = p.binary.to_string_lossy().into_owned();
    let arguments = task_arguments(p);
    let xml = render_task_xml(&principal, &command, &arguments);

    // schtasks /XML reads a UTF-16 file; write UTF-16LE with a BOM to match
    // the declared encoding.
    let xml_path = std::env::temp_dir()
        .join(format!("netidx-task-{}-{}.xml", p.service_name, std::process::id()));
    let mut bytes = Vec::with_capacity(xml.len() * 2 + 2);
    bytes.extend_from_slice(&[0xFF, 0xFE]); // UTF-16LE BOM
    for u in xml.encode_utf16() {
        bytes.extend_from_slice(&u.to_le_bytes());
    }
    std::fs::write(&xml_path, &bytes)
        .with_context(|| format!("writing task XML to {}", xml_path.display()))?;

    let xml_str = xml_path.to_string_lossy().into_owned();
    let create =
        schtasks(&["/Create", "/TN", &p.service_name, "/XML", &xml_str, "/F"]);
    // Best-effort cleanup of the temp file regardless of the result.
    let _ = std::fs::remove_file(&xml_path);
    create?;

    // Start it now so the operator doesn't have to log out and back in —
    // the `enable --now` analog. Best-effort: a failure to start now does
    // not undo the (successful) registration.
    let _ = schtasks(&["/Run", "/TN", &p.service_name]);

    Ok(InstalledService {
        unit_path: xml_path,
        service_id: p.service_name.clone(),
    })
}

pub(super) fn uninstall(p: &ServiceParams) -> Result<()> {
    // Stop a running instance first (best-effort), then delete the task.
    let _ = schtasks(&["/End", "/TN", &p.service_name]);
    schtasks(&["/Delete", "/TN", &p.service_name, "/F"]).map(|_| ())
}

pub(super) fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    // A query that fails means the task isn't registered. We can't cheaply
    // distinguish "registered, supervisor not currently running" from
    // "registered and running" for a logon task, so a registered task
    // reports Active.
    let out = Command::new("schtasks")
        .args(["/Query", "/TN", &p.service_name])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("running schtasks.exe")?;
    Ok(if out.success() {
        ServiceStatus::Active
    } else {
        ServiceStatus::NotInstalled
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_escapes_specials() {
        assert_eq!(xml_escape(r#"a&b<c>d"e'f"#), "a&amp;b&lt;c&gt;d&quot;e&apos;f");
    }

    #[test]
    fn task_xml_contains_command_args_and_principal() {
        let xml = render_task_xml(
            r"WS\alice",
            r"C:\Program Files\netidx\netidx.exe",
            r#"activation --units "C:\Users\alice\AppData\Roaming\netidx\activation""#,
        );
        assert!(xml.contains("<LogonTrigger>"));
        assert!(xml.contains("<UserId>WS\\alice</UserId>"));
        assert!(xml.contains("<Hidden>true</Hidden>"));
        assert!(xml.contains(r"<Command>C:\Program Files\netidx\netidx.exe</Command>"));
        // Inner quotes in the args are XML-escaped.
        assert!(xml.contains("activation --units &quot;C:\\Users\\alice"));
        assert!(xml.contains("InteractiveToken"));
    }
}
