//! launchd-specific install/uninstall/status for the netidx
//! activation service. macOS-only.
//!
//! The plist content lives in [`render_plist`] (pure, tested);
//! everything else (file writes, `launchctl` invocations) is shelled
//! out.

use super::{
    ensure_valid_service_name, InstalledService, ServiceParams, ServiceScope,
    ServiceStatus,
};
use anyhow::{Context, Result};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

/// XML-escape the five entity-reference characters. Used by the
/// launchd plist renderer to safely interpolate operator-supplied
/// paths into `<string>...</string>` content; without this a path
/// like `~/bin/foo & bar/netidx` produces invalid XML and
/// `launchctl bootstrap` rejects the plist outright.
// `xml_escape` is only consumed by the launchd backend (macOS) plus
// the unit tests below. The `cfg_attr` keeps the unused-fn warning
// off on non-mac builds while still letting the test module link to
// it under `cfg(test)`.
pub(super) fn quote_exec_arg(s: &str) -> String {
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
    // Every interpolated value goes through `xml_escape`. The plist
    // is XML and a bare `&`, `<`, `>`, `"`, or `'` anywhere inside a
    // `<string>...</string>` element produces ill-formed XML that
    // launchctl bootstrap will outright reject. `current_exe()` and
    // operator-supplied `--activation-dir` can both contain `&` and
    // `'` in real installs (e.g. `~/Library/Application Support`
    // gets quoted oddly on some shells; corporate paths often carry
    // `&` from project codenames).
    let label = quote_exec_arg(&label(p));
    let exe = quote_exec_arg(&p.binary.to_string_lossy());
    let user_name_block = match (p.scope, p.for_user.as_deref()) {
        (ServiceScope::System, Some(u)) => {
            format!("\n  <key>UserName</key><string>{}</string>", quote_exec_arg(u),)
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
            quote_exec_arg(&dir.to_string_lossy()),
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
    ensure_valid_service_name(&p.service_name)?;
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
    ensure_valid_service_name(&p.service_name)?;
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
    ensure_valid_service_name(&p.service_name)?;
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
    Ok(if status.success() { ServiceStatus::Active } else { ServiceStatus::Inactive })
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

    /// Paths containing XML-entity bytes (`&`, `<`, `>`, `"`, `'`)
    /// have to be escaped or `launchctl bootstrap` rejects the plist
    /// outright. The regression these tests guard is "operator
    /// installs from a path containing `&` and the daemon never
    /// boots; the failure is opaque because `launchctl` just says
    /// `Bootstrap failed: 5: Input/output error`".
    #[test]
    fn render_plist_xml_escapes_paths_with_entities() {
        let mut p = params(ServiceScope::User);
        p.binary = PathBuf::from("/Applications/My & Tools/netidx");
        p.activation_dir = Some(PathBuf::from("/var/lib/<netidx>/units"));
        let body = render_plist(&p);
        // Body must not contain the raw entity-bytes inside an
        // interpolated string.
        assert!(!body.contains("My & Tools"), "raw `&` leaked into plist: {body}",);
        assert!(!body.contains("<netidx>/units"), "raw `<` leaked into plist: {body}",);
        // And it MUST contain the escaped forms.
        assert!(
            body.contains("<string>/Applications/My &amp; Tools/netidx</string>"),
            "escaped binary path missing: {body}",
        );
        assert!(
            body.contains("<string>/var/lib/&lt;netidx&gt;/units</string>"),
            "escaped activation_dir missing: {body}",
        );
    }

    /// Even the `UserName` value gets escaped — `for_user` is
    /// operator-supplied and shouldn't be a syntactic foothold even
    /// for an attacker who already has shell access (defence in
    /// depth, the validator already rejects most of these names).
    #[test]
    fn render_plist_xml_escapes_for_user() {
        let mut p = params(ServiceScope::System);
        p.for_user = Some("a&b".into());
        let body = render_plist(&p);
        assert!(
            body.contains("<string>a&amp;b</string>"),
            "for_user wasn't escaped: {body}",
        );
    }

    #[test]
    fn install_uninstall_status_reject_invalid_service_name() {
        let mut p = params(ServiceScope::User);
        p.service_name = "../escape".into();
        assert!(install(&p).is_err());
        assert!(uninstall(&p).is_err());
        assert!(status(&p).is_err());
    }

    #[test]
    fn xml_escape_converts_the_five_entities() {
        assert_eq!(
            platform::quote_exec_arg("a & b < c > d \" e ' f"),
            "a &amp; b &lt; c &gt; d &quot; e &apos; f",
        );
        // Idempotency / round-trip safety on ASCII strings.
        assert_eq!(platform::quote_exec_arg("/usr/bin/netidx"), "/usr/bin/netidx");
        // Empty in, empty out.
        assert_eq!(platform::quote_exec_arg(""), "");
    }
}
