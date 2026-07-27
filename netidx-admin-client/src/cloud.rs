//! Deployment-environment IP discovery for the `admin install`
//! tooling: cloud-metadata probes, env-var overrides, and container
//! detection. Internal to [`crate::netshape`].
//!
//! Public-cloud VMs (AWS / GCP / Azure) almost always bind to a
//! private RFC1918 address locally while networking equipment NATs
//! traffic from a public IP onto that private address. netidx has a
//! dedicated `BindCfg::Elastic` form for this scenario
//! (`<public-ip>@<private-subnet>/<prefix>`): the publisher binds to
//! the private interface but registers the public IP with the
//! resolver.
//!
//! For *containers* the same NAT shape applies but there is no
//! link-local metadata service to ask: from inside a Docker bridge
//! container or a Kubernetes pod, the host's address is invisible.
//! The conventional fix is to inject the public IP via env var at
//! deploy time — [`env_public_ip`] reads `NETIDX_PUBLIC_IP` for
//! exactly that. When the env var isn't set and the cloud probe
//! comes up empty, [`detect_container`] lets the caller fall back to
//! a placeholder suggestion that the operator must fill in by hand
//! rather than silently emitting a bogus subnet-only bind.

use std::{net::Ipv4Addr, time::Duration};

/// Per-request timeout. Kept short because non-cloud hosts pay this
/// in full on the prompt path before the suggestion appears.
const TIMEOUT: Duration = Duration::from_millis(400);

/// Race AWS / GCP / Azure metadata endpoints; return the first
/// public IPv4 we can confirm, else `None`. Synchronous wrapper —
/// constructs a single-threaded tokio runtime for the duration of
/// the detection. Safe to call from any sync context.
pub(crate) fn detect_public_ip() -> Option<Ipv4Addr> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .ok()?;
    rt.block_on(detect_public_ip_async())
}

async fn detect_public_ip_async() -> Option<Ipv4Addr> {
    use futures::future::FutureExt;
    let mut futs =
        vec![detect_aws().boxed(), detect_gcp().boxed(), detect_azure().boxed()];
    while !futs.is_empty() {
        let (result, _, remaining) = futures::future::select_all(futs).await;
        futs = remaining;
        if let Some(ip) = result {
            return Some(ip);
        }
    }
    None
}

fn client() -> Option<reqwest::Client> {
    reqwest::Client::builder().timeout(TIMEOUT).build().ok()
}

/// AWS EC2 IMDSv2 (the modern, mandatory-in-many-accounts variant).
/// Two-step: PUT for a session token, then GET with the token in
/// the request header.
async fn detect_aws() -> Option<Ipv4Addr> {
    let client = client()?;
    let token = client
        .put("http://169.254.169.254/latest/api/token")
        .header("X-aws-ec2-metadata-token-ttl-seconds", "60")
        .send()
        .await
        .ok()?;
    if !token.status().is_success() {
        return None;
    }
    let token = token.text().await.ok()?;
    let resp = client
        .get("http://169.254.169.254/latest/meta-data/public-ipv4")
        .header("X-aws-ec2-metadata-token", token)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    parse_ipv4(resp.text().await.ok()?.as_str())
}

/// GCP Compute Engine. Uses 169.254.169.254 directly (rather than
/// `metadata.google.internal`) so non-GCP hosts that happen to
/// resolve that name to something else can't confuse us.
async fn detect_gcp() -> Option<Ipv4Addr> {
    let client = client()?;
    let resp = client
        .get("http://169.254.169.254/computeMetadata/v1/instance/admin domain-interfaces/0/access-configs/0/external-ip")
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    parse_ipv4(resp.text().await.ok()?.as_str())
}

/// Azure Instance Metadata Service.
async fn detect_azure() -> Option<Ipv4Addr> {
    let client = client()?;
    let resp = client
        .get("http://169.254.169.254/metadata/instance/admin domain/interface/0/ipv4/ipAddress/0/publicIpAddress?api-version=2021-02-01&format=text")
        .header("Metadata", "true")
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    parse_ipv4(resp.text().await.ok()?.as_str())
}

/// Explicit operator override: `NETIDX_PUBLIC_IP` lets a Dockerfile,
/// docker-compose file, or Kubernetes manifest inject the public IP
/// once at deploy time. Takes precedence over cloud-metadata probes
/// (which would fail in a container anyway, but it's a clearer
/// contract this way: explicit env var > inferred metadata).
///
/// Strictly parses to a non-empty, non-loopback, non-private public
/// IPv4 — same rules as the metadata-service responses — so a stray
/// `NETIDX_PUBLIC_IP=` or `NETIDX_PUBLIC_IP=10.0.0.5` in the
/// environment doesn't quietly poison the suggested config.
pub(crate) fn env_public_ip() -> Option<Ipv4Addr> {
    let raw = std::env::var("NETIDX_PUBLIC_IP").ok()?;
    parse_ipv4(&raw)
}

/// True when we appear to be running inside a container. Used by the
/// network-shape detector to decide whether falling back to a bare
/// subnet bind is safe: in a container with no public-IP hint, the
/// "right" suggestion is `<PUBLIC_IP>@<private-subnet>/<prefix>`
/// with the public part as a placeholder the operator must fill in,
/// not the private subnet alone (which would land traffic on the
/// container-internal address and fail to register externally).
///
/// Linux-only: outside Linux this returns false. Docker on macOS /
/// Windows runs in a Linux VM, so the in-container check still hits
/// `/.dockerenv` correctly from inside the workload container.
pub(crate) fn detect_container() -> bool {
    detect_container_at("/.dockerenv", "/run/.containerenv", "/proc/1/cgroup")
}

/// Pure version of [`detect_container`] for unit testing — paths are
/// injected. Returns true when either of the marker files exists or
/// the cgroup file mentions a known container runtime.
fn detect_container_at(
    dockerenv: &str,
    podman_containerenv: &str,
    pid1_cgroup: &str,
) -> bool {
    use std::path::Path;
    if Path::new(dockerenv).exists() || Path::new(podman_containerenv).exists() {
        return true;
    }
    if let Ok(cgroup) = std::fs::read_to_string(pid1_cgroup) {
        const MARKERS: &[&str] = &["docker", "containerd", "kubepods", "lxc", "podman"];
        if MARKERS.iter().any(|m| cgroup.contains(m)) {
            return true;
        }
    }
    false
}

/// Strict parse: only accept a non-empty, non-loopback, non-private
/// public IPv4. A blank body, RFC1918 address, or 0.0.0.0 means "no
/// public IP attached" and we don't want to advertise that to clients.
fn parse_ipv4(s: &str) -> Option<Ipv4Addr> {
    let ip: Ipv4Addr = s.trim().parse().ok()?;
    if ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
    {
        return None;
    }
    Some(ip)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ipv4_accepts_public() {
        assert_eq!(parse_ipv4("54.32.224.1"), Some(Ipv4Addr::new(54, 32, 224, 1)));
        assert_eq!(parse_ipv4("  8.8.8.8\n"), Some(Ipv4Addr::new(8, 8, 8, 8)));
    }

    #[test]
    fn detect_container_finds_dockerenv() {
        let dir = tempfile::tempdir().unwrap();
        let dockerenv = dir.path().join(".dockerenv");
        std::fs::write(&dockerenv, b"").unwrap();
        // The other paths don't exist; the docker marker alone is enough.
        assert!(detect_container_at(
            dockerenv.to_str().unwrap(),
            dir.path().join("does-not-exist").to_str().unwrap(),
            dir.path().join("also-missing").to_str().unwrap(),
        ));
    }

    #[test]
    fn detect_container_finds_podman_marker() {
        let dir = tempfile::tempdir().unwrap();
        let podman = dir.path().join("containerenv");
        std::fs::write(&podman, b"").unwrap();
        assert!(detect_container_at(
            dir.path().join("no-docker").to_str().unwrap(),
            podman.to_str().unwrap(),
            dir.path().join("no-cgroup").to_str().unwrap(),
        ));
    }

    #[test]
    fn detect_container_finds_kubepods_in_cgroup() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup = dir.path().join("cgroup");
        std::fs::write(
            &cgroup,
            b"0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1234.slice\n",
        )
        .unwrap();
        assert!(detect_container_at(
            dir.path().join("no-docker").to_str().unwrap(),
            dir.path().join("no-podman").to_str().unwrap(),
            cgroup.to_str().unwrap(),
        ));
    }

    #[test]
    fn detect_container_returns_false_on_bare_host() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup = dir.path().join("cgroup");
        // Typical /proc/1/cgroup on a host running systemd: no
        // container runtime markers anywhere in the slice path.
        std::fs::write(&cgroup, b"0::/init.scope\n").unwrap();
        assert!(!detect_container_at(
            dir.path().join("no-docker").to_str().unwrap(),
            dir.path().join("no-podman").to_str().unwrap(),
            cgroup.to_str().unwrap(),
        ));
    }

    #[test]
    fn parse_ipv4_rejects_non_public() {
        // Azure returns an empty body when the NIC has no public IP.
        assert_eq!(parse_ipv4(""), None);
        assert_eq!(parse_ipv4("\n"), None);
        // GCP responds with `0.0.0.0` when there's no external-ip on
        // the access config.
        assert_eq!(parse_ipv4("0.0.0.0"), None);
        assert_eq!(parse_ipv4("127.0.0.1"), None);
        // We never want to suggest an RFC1918 as a "public" IP even
        // if the metadata service happens to echo one back.
        assert_eq!(parse_ipv4("10.0.0.5"), None);
        assert_eq!(parse_ipv4("192.168.1.1"), None);
        // Garbage / HTML error pages.
        assert_eq!(parse_ipv4("not an ip"), None);
        assert_eq!(parse_ipv4("<html>error</html>"), None);
    }
}
