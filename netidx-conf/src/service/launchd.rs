//! launchd-specific install/uninstall/status for the netidx
//! activation service. macOS-only.
//!
//! The plist content lives in [`render_plist`] (pure, tested);
//! everything else (file writes, `launchctl` invocations) is shelled
//! out.

use super::{InstalledService, ServiceParams, ServiceScope, ServiceStatus};
use anyhow::{Context, Result};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

/// Launchd label — what shows up in `launchctl list`. Conventionally
/// reverse-DNS; `com.netidx.<name>` keeps multiple parallel installs
/// distinguishable.
fn label(p: &ServiceParams) -> String {
    format!("com.netidx.{}", p.service_name)
}

/// Render the plist XML for the service. Pure — no I/O.
///
/// User-scope plists go in `~/Library/LaunchAgents` and run as the
/// logged-in user. System-scope plists go in
/// `/Library/LaunchDaemons` and need a `UserName` key set to the
/// service account, otherwise they'd run as root.
pub(super) fn render_plist(p: &ServiceParams) -> String {
    let label = label(p);
    let exe = p.binary.to_string_lossy();
    let user_name_block = match (p.scope, p.for_user.as_deref()) {
        (ServiceScope::System, Some(u)) => {
            format!("\n  <key>UserName</key><string>{u}</string>")
        }
        // User scope: launchd runs the LaunchAgent as the session
        // user, so an explicit UserName key is redundant (and wrong
        // if it disagrees with the session). System scope without
        // for_user falls through to root — surprising but consistent
        // with the engine contract that `for_user` is what governs.
        _ => String::new(),
    };
    let mut args = format!(
        "    <string>{exe}</string>\n    <string>activation</string>\n    <string>-f</string>"
    );
    if let Some(dir) = &p.activation_dir {
        args.push_str(&format!(
            "\n    <string>--units</string>\n    <string>{}</string>",
            dir.to_string_lossy(),
        ));
    }
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20\x20<key>Label</key><string>{label}</string>\n\
         \x20\x20<key>ProgramArguments</key>\n\
         \x20\x20<array>\n\
         {args}\n\
         \x20\x20</array>\n\
         \x20\x20<key>RunAtLoad</key><true/>\n\
         \x20\x20<key>KeepAlive</key><true/>{user_name_block}\n\
         </dict>\n\
         </plist>\n"
    )
}

fn plist_path(p: &ServiceParams) -> Result<PathBuf> {
    let filename = format!("{}.plist", label(p));
    match p.scope {
        ServiceScope::User => {
            let mut h = dirs::home_dir().ok_or_else(|| {
                anyhow!("could not determine home dir for LaunchAgents install")
            })?;
            h.push("Library");
            h.push("LaunchAgents");
            Ok(h.join(filename))
        }
        ServiceScope::System => {
            Ok(PathBuf::from("/Library/LaunchDaemons").join(filename))
        }
    }
}

pub(super) fn install(p: &ServiceParams) -> Result<InstalledService> {
    let path = plist_path(p)?;
    let body = render_plist(p);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {parent:?}"))?;
    }
    std::fs::write(&path, body.as_bytes())
        .with_context(|| format!("writing plist {path:?}"))?;
    set_mode(&path, 0o644)?;
    bootstrap(p, &path)?;
    Ok(InstalledService { unit_path: path, service_id: label(p) })
}

pub(super) fn uninstall(p: &ServiceParams) -> Result<()> {
    let path = plist_path(p)?;
    // `launchctl bootout` removes the loaded job. If the service was
    // never bootstrapped, bootout errors — best-effort, ignore.
    let _ = bootout(p, &path);
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("removing plist {path:?}"))?;
    }
    Ok(())
}

pub(super) fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    let path = plist_path(p)?;
    if !path.exists() {
        return Ok(ServiceStatus::NotInstalled);
    }
    // `launchctl print <domain>/<label>` exits 0 if the job is loaded
    // (active), non-zero otherwise. The domain is `gui/<uid>` for user
    // services and `system` for daemons.
    let domain = service_domain(p)?;
    let lbl = label(p);
    let target = format!("{domain}/{lbl}");
    let status = Command::new("launchctl")
        .arg("print")
        .arg(&target)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("running launchctl print")?;
    Ok(if status.success() {
        ServiceStatus::Active
    } else {
        ServiceStatus::Inactive
    })
}

fn bootstrap(p: &ServiceParams, plist: &Path) -> Result<()> {
    let domain = service_domain(p)?;
    let status = Command::new("launchctl")
        .arg("bootstrap")
        .arg(&domain)
        .arg(plist)
        .status()
        .context("spawning launchctl bootstrap (is launchctl on $PATH?)")?;
    if !status.success() {
        bail!("launchctl bootstrap {domain} {plist:?} failed: {status}");
    }
    Ok(())
}

fn bootout(p: &ServiceParams, plist: &Path) -> Result<()> {
    let domain = service_domain(p)?;
    let status = Command::new("launchctl")
        .arg("bootout")
        .arg(&domain)
        .arg(plist)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("running launchctl bootout")?;
    if !status.success() {
        bail!("launchctl bootout failed: {status}");
    }
    Ok(())
}

fn service_domain(p: &ServiceParams) -> Result<String> {
    Ok(match p.scope {
        ServiceScope::User => {
            let uid = nix::unistd::geteuid();
            format!("gui/{}", uid)
        }
        ServiceScope::System => "system".to_string(),
    })
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
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
    fn label_uses_reverse_dns() {
        assert_eq!(label(&params(ServiceScope::User)), "com.netidx.netidx");
    }

    #[test]
    fn user_plist_omits_username_key() {
        let body = render_plist(&params(ServiceScope::User));
        // LaunchAgents inherit the session user; an explicit UserName
        // key would conflict with launchd's own semantics.
        assert!(!body.contains("<key>UserName</key>"), "got: {body}");
        assert!(body.contains("<string>activation</string>"));
        assert!(body.contains("<string>-f</string>"));
        assert!(body.contains("<key>Label</key><string>com.netidx.netidx</string>"));
    }

    #[test]
    fn system_plist_sets_username() {
        let body = render_plist(&params(ServiceScope::System));
        // System LaunchDaemons run as root unless UserName is set —
        // the service-account semantics depend on this key.
        assert!(
            body.contains("<key>UserName</key><string>alice</string>"),
            "got: {body}",
        );
    }

    #[test]
    fn activation_dir_emits_units_args() {
        let mut p = params(ServiceScope::User);
        p.activation_dir = Some(PathBuf::from("/var/lib/netidx/units"));
        let body = render_plist(&p);
        assert!(body.contains("<string>--units</string>"));
        assert!(body.contains("<string>/var/lib/netidx/units</string>"));
    }

    #[test]
    fn user_plist_path_lives_under_library_launchagents() {
        let p = params(ServiceScope::User);
        let path = plist_path(&p).unwrap();
        assert!(
            path.ends_with("Library/LaunchAgents/com.netidx.netidx.plist"),
            "got: {path:?}",
        );
    }

    #[test]
    fn system_plist_path_is_library_launchdaemons() {
        let p = params(ServiceScope::System);
        assert_eq!(
            plist_path(&p).unwrap(),
            PathBuf::from("/Library/LaunchDaemons/com.netidx.netidx.plist"),
        );
    }
}
