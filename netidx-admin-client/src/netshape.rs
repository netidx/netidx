//! Deployment-environment network-shape detection for the
//! `admin install` flow. Decides the `--listen` / `--bind` defaults a
//! frontend should suggest for the resolver and publisher templates,
//! by enumerating local interfaces and — only when the routable NIC is
//! RFC1918 — racing the cloud-metadata services to discover a NAT'd
//! public IP (see [`crate::cloud`]).
//!
//! The interesting logic ([`NetShape::from_interfaces`]) is a pure
//! function with every I/O-bound input injected, so it's unit-tested
//! exhaustively; [`NetShape::detect`] is the thin impure entry point.

use crate::cloud;
use std::net::Ipv4Addr;

/// How a candidate interface address ranks as the resolver's
/// advertised address. Loopback isn't a variant — it's the
/// fallback when nothing classifies, so [`classify`] returns `None`
/// for it (and for anything else that can't be advertised:
/// link-local, multicast, broadcast, unspecified).
#[derive(Debug, PartialEq, Eq)]
enum AddrClass {
    Public,
    Private,
}

/// Classify an IPv4 interface address for use as the resolver's
/// advertised address. `None` means "don't suggest this one" —
/// loopback (the final fallback, handled by [`pick_advertised_ip`]),
/// link-local, multicast, broadcast, and unspecified all return
/// `None`.
///
/// IPv4 only: netidx's IPv6 support isn't well exercised, so the
/// tooling doesn't *volunteer* a v6 address as the default. An
/// operator who needs one can still type it at the `--listen`
/// prompt — we just don't suggest it.
fn classify(v4: Ipv4Addr) -> Option<AddrClass> {
    if v4.is_loopback()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_multicast()
        || v4.is_unspecified()
    {
        None
    } else if v4.is_private() {
        Some(AddrClass::Private)
    } else {
        // Not provably-public (the stable stdlib has no `is_global`),
        // but a non-private, non-special v4 address on a real
        // interface is public in practice.
        Some(AddrClass::Public)
    }
}

/// Pick the best routable v4 interface for advertising: first public,
/// else first private, else `None`. Returns the interface's
/// `(ip, netmask)` so callers can derive both an exact `--listen`
/// address and a subnet-shaped `BindCfg::Match`. IPv6 candidates are
/// skipped entirely (see [`classify`]).
fn pick_advertised_v4_interface(
    ifaces: &[if_addrs::Interface],
) -> Option<(Ipv4Addr, Ipv4Addr)> {
    let mut first_private: Option<(Ipv4Addr, Ipv4Addr)> = None;
    for i in ifaces {
        let if_addrs::IfAddr::V4(v4) = &i.addr else { continue };
        match classify(v4.ip) {
            Some(AddrClass::Public) => return Some((v4.ip, v4.netmask)),
            Some(AddrClass::Private) => {
                first_private.get_or_insert((v4.ip, v4.netmask));
            }
            None => {}
        }
    }
    first_private
}

/// Network shape of the host running `admin install`, used to drive
/// the `--listen` / `--bind` defaults for the resolver and publisher
/// templates. Computed once per CLI invocation; the resolver and
/// publisher prompts both consume it so the suggested address(es)
/// stay consistent across the two flows.
#[derive(Debug, PartialEq, Eq)]
pub enum NetShape {
    /// At least one routable public IPv4 interface — advertise it
    /// directly, no NAT trickery.
    Public { ip: Ipv4Addr, netmask: Ipv4Addr },
    /// Cloud VM: the local NIC is private, but the cloud metadata
    /// service told us the public IP that NAT routes onto it.
    /// Publishers need `BindCfg::Elastic` (`<public>@<private>/<n>`);
    /// the resolver needs `addr=<public>`, `bind_addr=<private>`.
    CloudElastic { public: Ipv4Addr, private: Ipv4Addr, netmask: Ipv4Addr },
    /// Container with no public-IP hint: we have a private NIC but
    /// no `NETIDX_PUBLIC_IP` env var, no reachable cloud metadata,
    /// and `/.dockerenv` / cgroup markers tell us we're in a
    /// container. The bind suggestion is a `<PUBLIC_IP>` placeholder
    /// the operator must fill in — silently emitting just the
    /// container subnet would land traffic on the bridge-internal IP
    /// and fail to register externally.
    ContainerPrivate { private: Ipv4Addr, netmask: Ipv4Addr },
    /// Private-only host (LAN / dev box). Suggest the private IP
    /// directly; no NAT involved.
    Private { ip: Ipv4Addr, netmask: Ipv4Addr },
    /// Nothing routable; suggest loopback so the operator at least
    /// gets a valid string at the prompt.
    Loopback,
}

impl NetShape {
    /// Enumerate the local interfaces and, if the only routable v4
    /// is RFC1918, race the cloud metadata endpoints to see if a
    /// public IP NATs onto it. Best-effort: any failure walks back
    /// to the underlying private/loopback shape.
    pub fn detect() -> Self {
        let ifaces = match if_addrs::get_if_addrs() {
            Ok(i) => i,
            Err(_) => return NetShape::Loopback,
        };
        Self::from_interfaces(
            &ifaces,
            cloud::env_public_ip(),
            cloud::detect_public_ip,
            cloud::detect_container(),
        )
    }

    /// Pure version of [`Self::detect`] for unit testing — every
    /// I/O-bound input is supplied by the caller. Decision order:
    /// 1. `NETIDX_PUBLIC_IP` (explicit operator override)
    /// 2. Discovered NIC is public → advertise directly
    /// 3. Cloud metadata returns a public IP → CloudElastic
    /// 4. We're in a container → ContainerPrivate (needs hint)
    /// 5. Otherwise → bare Private
    fn from_interfaces(
        ifaces: &[if_addrs::Interface],
        env_public_ip: Option<Ipv4Addr>,
        detect_cloud: impl FnOnce() -> Option<Ipv4Addr>,
        in_container: bool,
    ) -> Self {
        let Some((ip, netmask)) = pick_advertised_v4_interface(ifaces) else {
            return NetShape::Loopback;
        };
        // Explicit env override wins outright: the operator is telling
        // us what the outside world sees, and that's authoritative.
        // When the override happens to match the NIC IP we collapse
        // to Public — emitting `54.32.224.1@54.32.224.0/24` is valid
        // but uglier than the bare subnet form.
        if let Some(public) = env_public_ip {
            return if public == ip {
                NetShape::Public { ip, netmask }
            } else {
                NetShape::CloudElastic { public, private: ip, netmask }
            };
        }
        if !ip.is_private() {
            return NetShape::Public { ip, netmask };
        }
        // Private NIC — only now is a metadata roundtrip worth it.
        if let Some(public) = detect_cloud() {
            return NetShape::CloudElastic { public, private: ip, netmask };
        }
        // No cloud metadata, but in a container — the operator must
        // supply the public IP (NETIDX_PUBLIC_IP or --bind/--listen).
        if in_container {
            return NetShape::ContainerPrivate { private: ip, netmask };
        }
        NetShape::Private { ip, netmask }
    }

    /// Suggested `<ip>` for the resolver's `--listen` (also the
    /// publisher's `--addr` if it were prompted): the address clients
    /// will actually connect to. For `ContainerPrivate` we don't
    /// *know* the external address — the private IP is the only
    /// concrete answer we have, and the CLI warns the operator
    /// alongside.
    pub fn advertised_ip(&self) -> Ipv4Addr {
        match self {
            NetShape::Public { ip, .. } => *ip,
            NetShape::CloudElastic { public, .. } => *public,
            NetShape::ContainerPrivate { private, .. } => *private,
            NetShape::Private { ip, .. } => *ip,
            NetShape::Loopback => Ipv4Addr::LOCALHOST,
        }
    }

    /// Suggested `bind_addr` for the resolver. `None` when the
    /// publisher default (=== `listen.ip()`) is already correct;
    /// `Some` only in the cloud-elastic case where the resolver
    /// advertises one IP and binds to a different one locally.
    pub fn resolver_bind_override(&self) -> Option<Ipv4Addr> {
        match self {
            NetShape::CloudElastic { private, .. } => Some(*private),
            NetShape::Public { .. }
            | NetShape::ContainerPrivate { .. }
            | NetShape::Private { .. }
            | NetShape::Loopback => None,
        }
    }

    /// True when the suggestion is incomplete — the operator must
    /// supply something the host can't infer. The CLI prints a
    /// guidance line before the prompt in that case so the
    /// `<PUBLIC_IP>` placeholder isn't a mystery.
    pub fn needs_operator_hint(&self) -> bool {
        matches!(self, NetShape::ContainerPrivate { .. })
    }

    /// Suggested `BindCfg` string for the publisher template's
    /// `--bind` prompt. Falls back to `local` when nothing routable
    /// was found; emits a `<PUBLIC_IP>` placeholder in the container
    /// case so the operator notices the missing piece rather than
    /// shipping a config that binds to the container-internal IP.
    pub fn publisher_bind_suggestion(&self) -> String {
        match self {
            NetShape::Public { ip, netmask } | NetShape::Private { ip, netmask } => {
                let prefix = u32::from(*netmask).count_ones();
                let network = Ipv4Addr::from(u32::from(*ip) & u32::from(*netmask));
                format!("{network}/{prefix}")
            }
            NetShape::CloudElastic { public, private, netmask } => {
                // BindCfg::Elastic form: `<public>@<private-subnet>/<prefix>`.
                // Publisher binds to any local NIC on the private
                // subnet but advertises the public IP to the resolver.
                let prefix = u32::from(*netmask).count_ones();
                let network = Ipv4Addr::from(u32::from(*private) & u32::from(*netmask));
                format!("{public}@{network}/{prefix}")
            }
            NetShape::ContainerPrivate { private, netmask } => {
                let prefix = u32::from(*netmask).count_ones();
                let network = Ipv4Addr::from(u32::from(*private) & u32::from(*netmask));
                format!("<PUBLIC_IP>@{network}/{prefix}")
            }
            NetShape::Loopback => "local".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    #[test]
    fn classify_v4() {
        // Special / non-advertisable ranges → None.
        assert_eq!(classify(v4("127.0.0.1")), None); // loopback
        assert_eq!(classify(v4("169.254.3.4")), None); // link-local
        assert_eq!(classify(v4("0.0.0.0")), None); // unspecified
        assert_eq!(classify(v4("255.255.255.255")), None); // broadcast
        assert_eq!(classify(v4("224.0.0.1")), None); // multicast
        // RFC1918 private ranges.
        assert_eq!(classify(v4("10.0.0.5")), Some(AddrClass::Private));
        assert_eq!(classify(v4("172.16.9.9")), Some(AddrClass::Private));
        assert_eq!(classify(v4("192.168.1.42")), Some(AddrClass::Private));
        // Everything else on a real interface is public in practice.
        assert_eq!(classify(v4("8.8.8.8")), Some(AddrClass::Public));
        assert_eq!(classify(v4("203.0.113.7")), Some(AddrClass::Public));
    }

    fn iface(ip: &str, netmask: &str) -> if_addrs::Interface {
        if_addrs::Interface {
            name: "test".to_string(),
            addr: if_addrs::IfAddr::V4(if_addrs::Ifv4Addr {
                ip: v4(ip),
                netmask: v4(netmask),
                prefixlen: u32::from(v4(netmask)).count_ones() as u8,
                broadcast: None,
            }),
            index: None,
            oper_status: if_addrs::IfOperStatus::Up,
            is_p2p: false,
            #[cfg(windows)]
            adapter_name: "test".to_string(),
        }
    }

    #[test]
    fn pick_advertised_interface_returns_subnet() {
        // Public wins; netmask carried through unchanged.
        let ifs =
            [iface("192.168.1.5", "255.255.255.0"), iface("8.8.8.8", "255.255.255.252")];
        assert_eq!(
            pick_advertised_v4_interface(&ifs),
            Some((v4("8.8.8.8"), v4("255.255.255.252"))),
        );
        // No public → first private with its own netmask.
        let ifs = [iface("127.0.0.1", "255.0.0.0"), iface("10.0.0.5", "255.255.0.0")];
        assert_eq!(
            pick_advertised_v4_interface(&ifs),
            Some((v4("10.0.0.5"), v4("255.255.0.0"))),
        );
        // Nothing routable → None (the caller picks a fallback string).
        let ifs = [iface("127.0.0.1", "255.0.0.0")];
        assert_eq!(pick_advertised_v4_interface(&ifs), None);
    }

    #[test]
    fn netshape_public_skips_cloud_probe() {
        // Directly-attached public NIC → no need to ask metadata.
        // The cloud probe must NOT be called (panics if it is).
        let ifs = [iface("8.8.8.8", "255.255.255.0")];
        let shape =
            NetShape::from_interfaces(&ifs, None, || panic!("cloud probe ran"), false);
        assert_eq!(
            shape,
            NetShape::Public { ip: v4("8.8.8.8"), netmask: v4("255.255.255.0") },
        );
    }

    #[test]
    fn netshape_private_with_cloud_metadata_is_elastic() {
        let ifs = [iface("10.0.0.5", "255.255.255.0")];
        let shape =
            NetShape::from_interfaces(&ifs, None, || Some(v4("54.32.224.1")), false);
        assert_eq!(
            shape,
            NetShape::CloudElastic {
                public: v4("54.32.224.1"),
                private: v4("10.0.0.5"),
                netmask: v4("255.255.255.0"),
            },
        );
        assert_eq!(shape.advertised_ip(), v4("54.32.224.1"));
        assert_eq!(shape.resolver_bind_override(), Some(v4("10.0.0.5")));
        // Publisher BindCfg::Elastic — masked subnet on the right
        // side, public IP on the left.
        assert_eq!(shape.publisher_bind_suggestion(), "54.32.224.1@10.0.0.0/24",);
    }

    #[test]
    fn netshape_private_without_cloud_falls_through() {
        // Private NIC, no metadata service → ordinary private host.
        let ifs = [iface("192.168.1.5", "255.255.255.0")];
        let shape = NetShape::from_interfaces(&ifs, None, || None, false);
        assert_eq!(
            shape,
            NetShape::Private { ip: v4("192.168.1.5"), netmask: v4("255.255.255.0") },
        );
        assert_eq!(shape.advertised_ip(), v4("192.168.1.5"));
        assert_eq!(shape.resolver_bind_override(), None);
        assert_eq!(shape.publisher_bind_suggestion(), "192.168.1.0/24");
    }

    #[test]
    fn netshape_loopback_when_nothing_routable() {
        let ifs = [iface("127.0.0.1", "255.0.0.0")];
        // Cloud probe MUST NOT run when there's no routable NIC to
        // pair a public IP with — gating on "private NIC found" is
        // the cheap-out-on-non-cloud-hosts optimization.
        let shape =
            NetShape::from_interfaces(&ifs, None, || panic!("cloud probe ran"), false);
        assert_eq!(shape, NetShape::Loopback);
        assert_eq!(shape.advertised_ip(), Ipv4Addr::LOCALHOST);
        assert_eq!(shape.resolver_bind_override(), None);
        assert_eq!(shape.publisher_bind_suggestion(), "local");
    }

    /// Container with a private bridge IP, no cloud, no env hint.
    /// The publisher suggestion is the `<PUBLIC_IP>` placeholder so
    /// the operator notices the missing piece rather than silently
    /// shipping a bind that only routes container-internal traffic.
    #[test]
    fn netshape_container_private_emits_placeholder_suggestion() {
        // Typical Docker bridge: 172.17.0.0/16.
        let ifs = [iface("172.17.0.2", "255.255.0.0")];
        let shape = NetShape::from_interfaces(
            &ifs,
            None,
            || None,
            /* in_container = */ true,
        );
        assert_eq!(
            shape,
            NetShape::ContainerPrivate {
                private: v4("172.17.0.2"),
                netmask: v4("255.255.0.0"),
            },
        );
        assert!(shape.needs_operator_hint());
        assert_eq!(shape.publisher_bind_suggestion(), "<PUBLIC_IP>@172.17.0.0/16",);
        // Resolver bind override stays None — the resolver template
        // doesn't get a special elastic hint here; the CLI warns
        // instead so the operator knows to override --listen.
        assert_eq!(shape.resolver_bind_override(), None);
    }

    /// `NETIDX_PUBLIC_IP` overrides everything else: cloud probe
    /// doesn't run (panic if it does) and the container path is
    /// bypassed.
    #[test]
    fn netshape_env_override_short_circuits_detection() {
        let ifs = [iface("172.17.0.2", "255.255.0.0")];
        let shape = NetShape::from_interfaces(
            &ifs,
            Some(v4("54.32.224.1")),
            || panic!("cloud probe ran despite env override"),
            true, // in_container — should still defer to env var
        );
        assert_eq!(
            shape,
            NetShape::CloudElastic {
                public: v4("54.32.224.1"),
                private: v4("172.17.0.2"),
                netmask: v4("255.255.0.0"),
            },
        );
        assert!(!shape.needs_operator_hint());
    }

    /// When `NETIDX_PUBLIC_IP` happens to equal the discovered NIC
    /// (e.g. the operator set it from `curl ifconfig.me` on a host
    /// with a directly-attached public IP), collapse to `Public`
    /// rather than emit a redundant `54.32.224.1@54.32.224.0/24`.
    #[test]
    fn netshape_env_matching_nic_collapses_to_public() {
        let ifs = [iface("54.32.224.1", "255.255.255.0")];
        let shape = NetShape::from_interfaces(
            &ifs,
            Some(v4("54.32.224.1")),
            || panic!("cloud probe ran"),
            false,
        );
        assert_eq!(
            shape,
            NetShape::Public { ip: v4("54.32.224.1"), netmask: v4("255.255.255.0") },
        );
    }
}
