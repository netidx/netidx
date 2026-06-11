//! Canonical config-path discovery.
//!
//! The client helpers delegate to netidx
//! (`netidx::config::file::Config::{user_platform_default_path,
//! default_path}`) so there is a single source of truth for the client
//! config location and search order. netidx has no resolver-config
//! discovery, so the resolver helpers mirror the same scheme here. The
//! `system_*` helpers do not check existence; the `user_*` helpers
//! return the platform user-config directory location (also without
//! existence checks — callers are responsible).

use anyhow::Result;
use std::path::PathBuf;

/// `${dirs::config_dir}/netidx`. No existence check. Root of the
/// user's netidx config tree — every other `user_*` path lives
/// under here.
pub fn user_config_root() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    Ok(p)
}

/// `/etc/netidx` on unix, `C:\netidx` on windows. No existence check.
/// Root of the system-scope netidx config tree.
pub fn system_config_root() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from("C:\\netidx")
    } else {
        PathBuf::from("/etc/netidx")
    }
}

/// `${dirs::config_dir}/netidx/client.json`. No existence check.
///
/// Delegates to netidx so there is a single source of truth for the
/// client config location.
pub fn user_client_config() -> Result<PathBuf> {
    netidx::config::file::Config::user_platform_default_path()
}

/// `/etc/netidx/client.json` on unix, `C:\netidx\client.json` on windows.
pub fn system_client_config() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from("C:\\netidx\\client.json")
    } else {
        PathBuf::from("/etc/netidx/client.json")
    }
}

/// `${dirs::config_dir}/netidx/resolver.json`. No existence check.
pub fn user_resolver_config() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("resolver.json");
    Ok(p)
}

/// `/etc/netidx/resolver.json` on unix, `C:\netidx\resolver.json` on windows.
pub fn system_resolver_config() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from("C:\\netidx\\resolver.json")
    } else {
        PathBuf::from("/etc/netidx/resolver.json")
    }
}

/// `${dirs::config_dir}/netidx/perms.json`. No existence check.
pub fn user_perms_file() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("perms.json");
    Ok(p)
}

/// `${dirs::config_dir}/netidx/activation/`. No existence check.
pub fn user_activation_dir() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("activation");
    Ok(p)
}

/// `${dirs::config_dir}/netidx/tls/`. No existence check.
pub fn user_tls_dir() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("tls");
    Ok(p)
}

/// `${dirs::config_dir}/netidx/ca/`. No existence check.
///
/// There is one CA per netidx install. Operators who genuinely want
/// multiple CAs on the same machine (rare — most users want a single
/// local CA per workstation/server) can pass `--dir` to point at a
/// non-default location.
pub fn user_ca_dir() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("ca");
    Ok(p)
}

/// `${dirs::config_dir}/netidx/conf-server.json`. No existence check.
pub fn user_conf_server_config() -> Result<PathBuf> {
    let mut p = user_config_root()?;
    p.push("conf-server.json");
    Ok(p)
}

/// `/etc/netidx/conf-server.json` on unix, `C:\netidx\conf-server.json`
/// on windows.
pub fn system_conf_server_config() -> PathBuf {
    let mut p = system_config_root();
    p.push("conf-server.json");
    p
}

/// Find the first existing conf-server config in standard order:
/// `${dirs::config_dir}/netidx/conf-server.json` then the system path.
/// Errors if none exists.
pub fn discover_conf_server_config() -> Result<PathBuf> {
    if let Ok(p) = user_conf_server_config()
        && p.is_file()
    {
        return Ok(p);
    }
    let sys = system_conf_server_config();
    if sys.is_file() {
        return Ok(sys);
    }
    bail!("no conf-server config found in any standard location")
}

/// Find the first existing client config in the standard search order:
/// `$NETIDX_CFG`, then `${dirs::config_dir}/netidx/client.json`, then
/// `${HOME}/.config/netidx/client.json`, then the system path. Errors
/// if none exists.
///
/// Delegates to netidx so the search order stays in lockstep with the
/// library's own client config discovery.
pub fn discover_client_config() -> Result<PathBuf> {
    netidx::config::file::Config::default_path()
}

/// Find the first existing resolver-server config in standard order:
/// `${dirs::config_dir}/netidx/resolver.json` then the system path.
/// Errors if none exists.
pub fn discover_resolver_config() -> Result<PathBuf> {
    if let Ok(p) = user_resolver_config()
        && p.is_file()
    {
        return Ok(p);
    }
    let sys = system_resolver_config();
    if sys.is_file() {
        return Ok(sys);
    }
    bail!("no resolver config found in any standard location")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_paths_are_under_netidx() {
        let p = user_config_root().unwrap();
        assert!(p.ends_with("netidx"));

        let p = user_client_config().unwrap();
        assert!(p.ends_with("netidx/client.json"));

        let p = user_resolver_config().unwrap();
        assert!(p.ends_with("netidx/resolver.json"));

        let p = user_perms_file().unwrap();
        assert!(p.ends_with("netidx/perms.json"));

        let p = user_activation_dir().unwrap();
        assert!(p.ends_with("netidx/activation"));

        let p = user_tls_dir().unwrap();
        assert!(p.ends_with("netidx/tls"));
    }

    #[test]
    fn system_paths_are_well_known() {
        let p = system_client_config();
        if cfg!(windows) {
            assert_eq!(p.to_str().unwrap(), "C:\\netidx\\client.json");
        } else {
            assert_eq!(p.to_str().unwrap(), "/etc/netidx/client.json");
        }
        let p = system_resolver_config();
        if cfg!(windows) {
            assert_eq!(p.to_str().unwrap(), "C:\\netidx\\resolver.json");
        } else {
            assert_eq!(p.to_str().unwrap(), "/etc/netidx/resolver.json");
        }
        let p = system_config_root();
        if cfg!(windows) {
            assert_eq!(p.to_str().unwrap(), "C:\\netidx");
        } else {
            assert_eq!(p.to_str().unwrap(), "/etc/netidx");
        }
    }
}
