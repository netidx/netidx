//! mDNS/DNS-SD advertisement + browsing for admin servers.
//!
//! Admin servers register `_netidx-admin._tcp.local.` with a TXT record
//! carrying the admin domain's domain name, the host's roles, and a short CA
//! fingerprint. **The beacon is a hint, never trusted**: browsing
//! yields candidate addresses and labels for grouping in the setup UI;
//! every security-relevant fact (domain, roles, the CA itself) is
//! re-established over TLS pinned to the operator-confirmed
//! fingerprint. Anyone on the LAN can broadcast anything here; it buys
//! them nothing.

use anyhow::{Context, Result};
use enumflags2::BitFlags;
use log::{debug, warn};
use mdns_sd::{Receiver, ResolvedService, ServiceDaemon, ServiceEvent, ServiceInfo};
use netidx_admin_proto::Role;
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};

pub const SERVICE_TYPE: &str = "_netidx-admin._tcp.local.";

/// TXT schema version — bump if the key set changes incompatibly.
const TXT_VERSION: &str = "1";

fn role_to_str(r: Role) -> &'static str {
    match r {
        Role::Ca => "ca",
        Role::Resolver => "resolver",
        Role::IdMap => "id-map",
    }
}

fn role_from_str(s: &str) -> Option<Role> {
    match s {
        "ca" => Some(Role::Ca),
        "resolver" => Some(Role::Resolver),
        "id-map" => Some(Role::IdMap),
        _ => None,
    }
}

/// Encode roles for the TXT record: `"ca,resolver,id-map"`.
fn roles_to_txt(roles: BitFlags<Role>) -> String {
    let mut out = String::new();
    for role in roles {
        if !out.is_empty() {
            out.push(',');
        }
        out.push_str(role_to_str(role));
    }
    out
}

/// Decode a TXT roles value. Unknown role names are skipped — a newer
/// server advertising a role we don't know about shouldn't make the
/// whole record unreadable.
fn roles_from_txt(s: &str) -> BitFlags<Role> {
    s.split(',').filter_map(|r| role_from_str(r.trim())).collect()
}

/// A live advertisement. Dropping it unregisters the service and shuts
/// the responder down.
pub struct Advertisement {
    daemon: ServiceDaemon,
    fullname: String,
}

impl Drop for Advertisement {
    fn drop(&mut self) {
        if let Err(e) = self.daemon.unregister(&self.fullname) {
            debug!("mdns unregister failed: {e}");
        }
        if let Err(e) = self.daemon.shutdown() {
            debug!("mdns shutdown failed: {e}");
        }
    }
}

/// Advertise an admin server at `listen`. When the listen IP is concrete
/// it is advertised directly; an unspecified IP (0.0.0.0 / ::) lets the
/// responder advertise every interface address automatically.
pub fn advertise(
    listen: SocketAddr,
    domain: &str,
    roles: BitFlags<Role>,
    fp_short: &str,
) -> Result<Advertisement> {
    let daemon = ServiceDaemon::new().context("starting mDNS responder")?;
    // Instance names must be unique on the LAN. The concrete listen
    // address is unique by definition; for a bind-all listen fall back
    // to fingerprint + pid (two daemons of the same admin domain on one
    // host would differ by pid).
    let instance = if listen.ip().is_unspecified() {
        format!("netidx-admin-{}-{}", fp_short.to_lowercase(), std::process::id())
    } else {
        format!(
            "netidx-admin-{}-{}",
            listen.ip().to_string().replace([':', '.'], "-"),
            listen.port()
        )
    };
    let host_name = format!("{instance}.local.");
    let props = [
        ("v", TXT_VERSION),
        ("domain", domain),
        ("roles", &roles_to_txt(roles)),
        ("fp", fp_short),
    ];
    let info = if listen.ip().is_unspecified() {
        ServiceInfo::new(
            SERVICE_TYPE,
            &instance,
            &host_name,
            (),
            listen.port(),
            &props[..],
        )
        .context("building mDNS service info")?
        .enable_addr_auto()
    } else {
        ServiceInfo::new(
            SERVICE_TYPE,
            &instance,
            &host_name,
            listen.ip(),
            listen.port(),
            &props[..],
        )
        .context("building mDNS service info")?
    };
    let fullname = info.get_fullname().to_string();
    daemon.register(info).context("registering mDNS service")?;
    Ok(Advertisement { daemon, fullname })
}

/// An admin server seen on the local network. Everything here is
/// unauthenticated hint material.
#[derive(Debug, Clone)]
pub struct Discovered {
    pub addrs: Vec<IpAddr>,
    pub port: u16,
    pub domain: String,
    pub roles: BitFlags<Role>,
    pub fp_short: String,
}

impl Discovered {
    /// Candidate socket addresses for this admin server.
    pub fn socket_addrs(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.addrs.iter().map(|ip| SocketAddr::new(*ip, self.port))
    }
}

/// Parse a resolved service beacon into `(fullname, Discovered)`, or `None` if
/// it is malformed (no domain / no addresses).
fn parse_resolved(info: &ResolvedService) -> Option<(String, Discovered)> {
    let domain = info.get_property_val_str("domain").unwrap_or_default().to_string();
    let roles = roles_from_txt(info.get_property_val_str("roles").unwrap_or_default());
    let fp_short = info.get_property_val_str("fp").unwrap_or_default().to_string();
    let addrs: Vec<IpAddr> =
        info.get_addresses().iter().map(|a| a.to_ip_addr()).collect();
    if domain.is_empty() || addrs.is_empty() {
        debug!("ignoring malformed admin-server record {}", info.get_fullname());
        return None;
    }
    Some((
        info.get_fullname().to_string(),
        Discovered { addrs, port: info.get_port(), domain, roles, fp_short },
    ))
}

/// Drain resolve/remove events into `found` until `deadline`. When
/// `stop_on_first`, return as soon as a valid service resolves. Keyed by service
/// fullname so re-resolutions overwrite instead of duplicating. The `recv`
/// timeout is bounded by `deadline`, so a timeout there means the deadline is
/// reached (the daemon is held alive by the caller, so the channel won't drop).
fn collect_until(
    receiver: &Receiver<ServiceEvent>,
    found: &mut BTreeMap<String, Discovered>,
    deadline: Instant,
    stop_on_first: bool,
) {
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        match receiver.recv_timeout(deadline - now) {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                if let Some((name, d)) = parse_resolved(&info) {
                    found.insert(name, d);
                    if stop_on_first {
                        break;
                    }
                }
            }
            Ok(ServiceEvent::ServiceRemoved(_, fullname)) => {
                found.remove(&fullname);
            }
            Ok(_) => (),
            Err(_) => break, // deadline reached (or channel closed)
        }
    }
}

/// Browse for admin servers up to `timeout`, blocking the calling thread.
///
/// `settle` = `None` waits the whole window and returns every distinct service
/// resolved (possibly from multiple admin domains — group by `domain` + confirm
/// fingerprints before trusting anything). This is for the scripted enumerator.
///
/// `settle` = `Some(d)` is the interactive-join mode: collect for at least `d`
/// (so slow-to-answer admin domains — a VM, a second office — still make the list),
/// then early-exit if we found anything; if nothing answered within `d`, keep
/// waiting up to `timeout`, returning on the first admin domain to appear.
fn browse_inner(timeout: Duration, settle: Option<Duration>) -> Result<Vec<Discovered>> {
    let daemon = ServiceDaemon::new().context("starting mDNS browser")?;
    let receiver = daemon.browse(SERVICE_TYPE).context("browsing for admin servers")?;
    let mut found: BTreeMap<String, Discovered> = BTreeMap::new();
    let start = Instant::now();
    let deadline = start + timeout;
    match settle {
        None => collect_until(&receiver, &mut found, deadline, false),
        Some(d) => {
            let settle_deadline = (start + d).min(deadline);
            collect_until(&receiver, &mut found, settle_deadline, false);
            if found.is_empty() {
                collect_until(&receiver, &mut found, deadline, true);
            }
        }
    }
    if let Err(e) = daemon.stop_browse(SERVICE_TYPE) {
        debug!("mdns stop_browse failed: {e}");
    }
    if let Err(e) = daemon.shutdown() {
        debug!("mdns shutdown failed: {e}");
    }
    Ok(found.into_values().collect())
}

/// Browse for admin servers for the full `timeout`, blocking the calling thread.
/// Returns every distinct service resolved in the window — possibly from
/// multiple admin domains (group by `domain` + confirm fingerprints before trusting
/// anything).
pub fn browse_blocking(timeout: Duration) -> Result<Vec<Discovered>> {
    browse_inner(timeout, None)
}

/// Async wrapper for [`browse_blocking`] — runs it on the blocking
/// pool so an admin server can browse mid-request without stalling the
/// runtime.
pub async fn browse(timeout: Duration) -> Result<Vec<Discovered>> {
    tokio::task::spawn_blocking(move || browse_blocking(timeout))
        .await
        .context("mDNS browse task panicked")?
}

/// Like [`browse_blocking`], but logs and returns empty on failure —
/// for prompt-layer callers where discovery is best-effort and a
/// missing/blocked mDNS stack must not break manual setup.
pub fn browse_or_empty(timeout: Duration) -> Vec<Discovered> {
    match browse_blocking(timeout) {
        Ok(found) => found,
        Err(e) => {
            warn!("mDNS browse failed (manual setup still available): {e:#}");
            Vec::new()
        }
    }
}

/// Like [`browse_or_empty`], but the interactive-join browse: collect for at
/// least `settle`, then early-exit if anything answered, waiting up to `timeout`
/// for the first admin domain if nothing did. See [`browse_inner`].
pub fn browse_first_or_empty(timeout: Duration, settle: Duration) -> Vec<Discovered> {
    match browse_inner(timeout, Some(settle)) {
        Ok(found) => found,
        Err(e) => {
            warn!("mDNS browse failed (manual setup still available): {e:#}");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_round_trip_txt() {
        let all = Role::Ca | Role::Resolver | Role::IdMap;
        let txt = roles_to_txt(all);
        assert_eq!(txt, "ca,resolver,id-map");
        assert_eq!(roles_from_txt(&txt), all);
        assert_eq!(roles_from_txt(""), BitFlags::empty());
        // Unknown roles from a newer version are skipped, not fatal.
        assert_eq!(roles_from_txt("ca,flux-capacitor"), Role::Ca);
        // Whitespace tolerance.
        assert_eq!(roles_from_txt("ca, resolver"), Role::Ca | Role::Resolver);
    }

    /// Live loopback advertise/browse. Ignored by default: multicast is
    /// unreliable in CI sandboxes. Run with
    /// `cargo test -p netidx-admin discovery -- --ignored` on a real
    /// machine.
    #[test]
    #[ignore]
    fn advertise_and_browse_loopback() {
        let listen: SocketAddr = "0.0.0.0:14565".parse().unwrap();
        let _ad =
            advertise(listen, "test.example.com", Role::Ca | Role::Resolver, "ABCDEFGH")
                .unwrap();
        let found = browse_blocking(Duration::from_secs(3)).unwrap();
        let ours: Vec<_> =
            found.iter().filter(|d| d.domain == "test.example.com").collect();
        assert!(!ours.is_empty(), "did not find our own advertisement");
        assert_eq!(ours[0].port, 14565);
        assert_eq!(ours[0].roles, Role::Ca | Role::Resolver);
        assert_eq!(ours[0].fp_short, "ABCDEFGH");
    }
}
