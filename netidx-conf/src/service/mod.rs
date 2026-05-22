//! OS service install for `netidx activation`.
//!
//! The activation supervisor is great at running netidx daemons, but
//! it needs something to *run it* at boot. This module writes the
//! native OS init-system unit file and registers it with the local
//! service manager. The CLI (`netidx conf service install`) layers
//! interactive prompting and `sudo` escalation on top.
//!
//! Per-platform plans:
//!
//! - **Linux (systemd).** User scope writes
//!   `${XDG_CONFIG_HOME:-~/.config}/systemd/user/<name>.service` and
//!   runs `systemctl --user daemon-reload && enable --now`. System
//!   scope writes a templated `/etc/systemd/system/<name>@.service`
//!   with `User=%i`, then `systemctl daemon-reload && enable --now
//!   <name>@<for_user>`. The `@` form lets one unit file serve any
//!   number of unprivileged service accounts.
//!
//! - **macOS (launchd).** User scope writes
//!   `~/Library/LaunchAgents/<label>.plist` and bootstraps it with
//!   `launchctl bootstrap gui/<uid>`. System scope writes
//!   `/Library/LaunchDaemons/<label>.plist` with the `UserName` key
//!   set to `for_user`, and `launchctl bootstrap system`.
//!
//! - **Windows (SCM).** Stub: the install function returns an error
//!   pointing the operator at the manual `sc.exe` recipe. A real
//!   implementation needs to handle "log on as a service" rights for
//!   per-user accounts (and the password capture that goes with it),
//!   which is a different shape than systemd / launchd.
//!
//! The unit-file *content* is pure (it's just a formatted string)
//! and is what's covered by tests; the actual file write and the
//! `systemctl` / `launchctl` invocation are not — they need a real
//! init system to be meaningful, which a `cargo test` run doesn't
//! have.

use anyhow::Result;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
mod systemd;
#[cfg(target_os = "macos")]
mod launchd;
#[cfg(target_os = "windows")]
mod scm;

/// Whether the service runs at user scope (per-login) or system
/// scope (boot-triggered, root-owned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceScope {
    User,
    System,
}

/// Inputs for [`install`] / [`uninstall`] / [`status`]. The CLI
/// layer is responsible for resolving `for_user` and absolutising
/// `binary` before calling in.
#[derive(Debug, Clone)]
pub struct ServiceParams {
    /// User or system scope.
    pub scope: ServiceScope,
    /// Username the service should run as. For [`ServiceScope::User`]
    /// this is informational and the OS picks the user from the
    /// session; for [`ServiceScope::System`] it's the value that
    /// goes into `User=` (systemd) / `UserName` (launchd) /
    /// service-account fields. Defaults to the invoking user at the
    /// CLI layer.
    pub for_user: Option<String>,
    /// Absolute path to the `netidx` binary the service should
    /// `ExecStart`. Always supplied by the CLI from
    /// [`std::env::current_exe`] so the installed service runs the
    /// same binary the operator invoked.
    pub binary: PathBuf,
    /// Service / unit name. Defaults to `"netidx"`; overrideable so
    /// operators running multiple netidx setups on one host (a
    /// development scenario) can install distinct services.
    ///
    /// Validated by [`ensure_valid_service_name`] from every install /
    /// uninstall / status entry point — path separators, `.`/`..`,
    /// systemd specifier `%`, and the five XML entity bytes are all
    /// rejected before any code that interpolates this value into a
    /// filename or plist body runs.
    pub service_name: String,
    /// Override the activation directory the supervisor reads its
    /// unit files from. `None` ⇒ the activation supervisor's own
    /// default (which is `${XDG_CONFIG_HOME}/netidx/activation/` for
    /// the user that runs the service).
    pub activation_dir: Option<PathBuf>,
}

impl ServiceParams {
    /// Default service name — fixed so tooling and operators agree
    /// on what to look for.
    pub const DEFAULT_NAME: &'static str = "netidx";
}

/// Where an installed service's unit file lives.
#[derive(Debug, Clone)]
pub struct InstalledService {
    pub unit_path: PathBuf,
    /// The full name systemctl / launchctl / sc.exe address the
    /// service by — e.g. `netidx@alice` for a system-scope systemd
    /// install.
    pub service_id: String,
}

/// Whether the service is currently active. Platform-mapped onto the
/// underlying status query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    /// Unit file is installed and the service is running.
    Active,
    /// Unit file is installed but the service isn't running.
    Inactive,
    /// No unit file installed for this name + scope.
    NotInstalled,
}

#[cfg(target_os = "linux")]
pub fn install(p: &ServiceParams) -> Result<InstalledService> {
    systemd::install(p)
}
#[cfg(target_os = "linux")]
pub fn uninstall(p: &ServiceParams) -> Result<()> {
    systemd::uninstall(p)
}
#[cfg(target_os = "linux")]
pub fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    systemd::status(p)
}

#[cfg(target_os = "macos")]
pub fn install(p: &ServiceParams) -> Result<InstalledService> {
    launchd::install(p)
}
#[cfg(target_os = "macos")]
pub fn uninstall(p: &ServiceParams) -> Result<()> {
    launchd::uninstall(p)
}
#[cfg(target_os = "macos")]
pub fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    launchd::status(p)
}

#[cfg(target_os = "windows")]
pub fn install(p: &ServiceParams) -> Result<InstalledService> {
    scm::install(p)
}
#[cfg(target_os = "windows")]
pub fn uninstall(p: &ServiceParams) -> Result<()> {
    scm::uninstall(p)
}
#[cfg(target_os = "windows")]
pub fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    scm::status(p)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub fn install(_p: &ServiceParams) -> Result<InstalledService> {
    anyhow::bail!(
        "OS service install is only implemented on Linux, macOS, and Windows; \
         run `netidx activation` from your platform's init system manually"
    )
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub fn uninstall(_p: &ServiceParams) -> Result<()> {
    anyhow::bail!("OS service uninstall is not implemented on this platform")
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub fn status(_p: &ServiceParams) -> Result<ServiceStatus> {
    Ok(ServiceStatus::NotInstalled)
}

// ---- shared helpers --------------------------------------------------------

/// Reject service names that would compromise the renderers below.
/// `service_name` ends up in the systemd unit filename (`<name>.service`
/// or `<name>@.service`) and the launchd plist label
/// (`com.netidx.<name>`). A system-scope install runs as root, so an
/// unvalidated name is a directory-traversal / XML-injection foothold.
///
/// Reuses the shape from [`crate::tls::ensure_valid_cn`] and
/// `crate::activation::ensure_valid_basename`, plus the extra characters
/// codex flagged (systemd specifier `%`, XML entity bytes).
fn ensure_valid_service_name(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("service_name must not be empty");
    }
    if name == "." || name == ".." {
        anyhow::bail!("service_name must not be a relative-dir marker");
    }
    if name.contains('/') || name.contains('\\') {
        anyhow::bail!(
            "service_name may not contain path separators: {name:?}"
        );
    }
    // `%` is the systemd specifier-escape prefix (e.g. `%i`). `&<>'"`
    // need entity-escaping inside XML, which launchd would otherwise
    // see and reject. Whitespace is rejected because both backends
    // splice the name into single-token contexts (filename, label).
    for c in name.chars() {
        if matches!(c, '%' | '&' | '<' | '>' | '\'' | '"')
            || c.is_whitespace()
        {
            anyhow::bail!(
                "service_name contains reserved character {c:?}: {name:?}"
            );
        }
    }
    Ok(())
}

/// XML-escape the five entity-reference characters. Used by the
/// launchd plist renderer to safely interpolate operator-supplied
/// paths into `<string>...</string>` content; without this a path
/// like `~/bin/foo & bar/netidx` produces invalid XML and
/// `launchctl bootstrap` rejects the plist outright.
// `xml_escape` is only consumed by the launchd backend (macOS) plus
// the unit tests below. The `cfg_attr` keeps the unused-fn warning
// off on non-mac builds while still letting the test module link to
// it under `cfg(test)`.
#[cfg_attr(not(any(target_os = "macos", test)), allow(dead_code))]
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
fn systemd_quote_exec_arg(s: &str) -> String {
    let percent_escaped: String = s
        .chars()
        .flat_map(|c| {
            if c == '%' {
                vec!['%', '%']
            } else {
                vec![c]
            }
        })
        .collect();
    let needs_quoting = percent_escaped.chars().any(|c| {
        c.is_whitespace() || matches!(c, '"' | '\'' | '\\' | ';')
    });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_valid_service_name_accepts_typical_names() {
        for ok in ["netidx", "netidx-dev", "netidx_alt", "netidx2"] {
            assert!(
                ensure_valid_service_name(ok).is_ok(),
                "should accept {ok:?}",
            );
        }
    }

    #[test]
    fn ensure_valid_service_name_rejects_dangerous_input() {
        // Path traversal — would let a root install write outside
        // /etc/systemd/system or /Library/LaunchDaemons.
        for bad in ["", ".", "..", "a/b", "a\\b", "../../etc/passwd"] {
            assert!(
                ensure_valid_service_name(bad).is_err(),
                "should reject {bad:?}",
            );
        }
        // systemd specifier + XML entities + whitespace — the
        // injection vectors codex pointed at for the two renderers.
        for bad in ["%i", "n&e", "n<e", "n>e", "n'e", "n\"e", "n e", "n\te"] {
            assert!(
                ensure_valid_service_name(bad).is_err(),
                "should reject {bad:?}",
            );
        }
        // `.` in the middle is also rejected via the "is just `.`"
        // check above only matching exact `.` / `..` — confirm the
        // permissive case still works.
        assert!(
            ensure_valid_service_name("mycompany-netidx").is_ok(),
            "hyphenated names are valid",
        );
    }

    #[test]
    fn xml_escape_converts_the_five_entities() {
        assert_eq!(
            xml_escape("a & b < c > d \" e ' f"),
            "a &amp; b &lt; c &gt; d &quot; e &apos; f",
        );
        // Idempotency / round-trip safety on ASCII strings.
        assert_eq!(xml_escape("/usr/bin/netidx"), "/usr/bin/netidx");
        // Empty in, empty out.
        assert_eq!(xml_escape(""), "");
    }

    #[test]
    fn systemd_quote_exec_arg_handles_plain_paths() {
        // Bare path: passes through unchanged.
        assert_eq!(systemd_quote_exec_arg("/usr/bin/netidx"), "/usr/bin/netidx");
    }

    #[test]
    fn systemd_quote_exec_arg_doubles_percent() {
        // `%` is a systemd specifier prefix — always escaped,
        // regardless of whether quoting kicks in.
        assert_eq!(systemd_quote_exec_arg("a%b"), "a%%b");
        // Combined with quoting.
        assert_eq!(
            systemd_quote_exec_arg("a % b"),
            "\"a %% b\"",
        );
    }

    #[test]
    fn systemd_quote_exec_arg_quotes_spaces() {
        assert_eq!(
            systemd_quote_exec_arg("/home/user with space/netidx"),
            "\"/home/user with space/netidx\"",
        );
    }

    #[test]
    fn systemd_quote_exec_arg_escapes_quotes_and_backslashes() {
        assert_eq!(systemd_quote_exec_arg("a\"b"), "\"a\\\"b\"");
        assert_eq!(systemd_quote_exec_arg("a\\b"), "\"a\\\\b\"");
        // Even without spaces, a `"` or `\` triggers quoting because
        // those would otherwise need their own escape outside quotes.
    }
}
