//! The install planner: the decision logic behind `admin <role> install`,
//! driven through the [`crate::answer::Answerer`] seam so the strict CLI, the
//! TUI, and Atlas all share it.
//!
//! This module is being grown incrementally out of the old
//! `netidx-tools/src/admin/init.rs` cascade. It starts with the foundational
//! types and pure decision helpers; the prompt-driven cascade moves in on top
//! of the `Answerer` seam.

use anyhow::{Context, Result, bail};
use netidx::config::DefaultAuthMech;
use netidx_admin_proto::DEFAULT_PORT;
use std::{net::SocketAddr, str::FromStr};

/// The child half of resolver-hierarchy delegation (`delegate_under_parent`).
pub mod delegation;
/// Trust domain discovery + certificate enrollment — the keystone subgraph every
/// role install shares, driven through the [`crate::answer::Answerer`] seam.
pub mod enroll;
/// The role install cascades (`resolver` / `workstation` / `publisher`) and
/// the shared tail they run.
pub mod install;
/// The OS-service setup decision seam (`ServiceNeed` / `offer`).
pub mod service;

/// The data-plane authentication scheme a trust domain uses: how subscribers prove
/// who they are to publishers and resolvers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthKind {
    /// No authentication (labs / dev).
    Anonymous,
    /// Unix peer credentials over a local socket.
    Local,
    /// Kerberos v5.
    Krb5,
    /// Certificates issued by this trust domain's CA.
    Tls,
}

impl FromStr for AuthKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "anonymous" => Ok(Self::Anonymous),
            "local" => Ok(Self::Local),
            "krb5" => Ok(Self::Krb5),
            "tls" => Ok(Self::Tls),
            _ => bail!("auth must be one of anonymous|local|krb5|tls"),
        }
    }
}

impl AuthKind {
    /// The `DefaultAuthMech` a client config records for this scheme.
    pub fn default_mech(self) -> DefaultAuthMech {
        match self {
            Self::Anonymous => DefaultAuthMech::Anonymous,
            Self::Local => DefaultAuthMech::Local,
            Self::Krb5 => DefaultAuthMech::Krb5,
            Self::Tls => DefaultAuthMech::Tls,
        }
    }

    /// The scheme's canonical lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Local => "local",
            Self::Krb5 => "krb5",
            Self::Tls => "tls",
        }
    }
}

/// What a resolver install should do about the admin plane (the admin server,
/// and on non-TLS trust domains the admin-plane CA that anchors it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminPlane {
    /// Set it up. Announce what's happening; don't ask.
    Mandatory,
    /// Default-yes question.
    Ask,
    /// Never offer (host-local auth), or the operator opted out.
    Skip,
}

/// Decide how a resolver install treats the admin plane for a given auth
/// scheme.
///
/// TLS already creates the CA on a fresh trust domain (it signs the data plane),
/// and krb5/anonymous still need the admin plane's TLS trust root for
/// discovery, enrollment, and renewal — declining it on a TLS or krb5 trust domain
/// produces a trust domain where certificate renewal and zero-touch installs can
/// never work, so neither is offered as a question. Anonymous trust domains may
/// genuinely not want the machinery (lab/dev setups), so they're asked. Local
/// auth is host-local by definition: nothing to discover, nothing to enroll.
///
/// The same rule covers joining an existing trust domain: enrolling an admin server
/// queues for remote approval like any other request (the admin's
/// scoped enrollment authorization runs at approval), so no admin needs to be at
/// this keyboard and there is no reason for the join side of the matrix to
/// differ.
pub fn admin_plane_decision(kind: AuthKind, no_admin_server: bool) -> AdminPlane {
    if no_admin_server {
        return AdminPlane::Skip;
    }
    match kind {
        AuthKind::Tls | AuthKind::Krb5 => AdminPlane::Mandatory,
        AuthKind::Anonymous => AdminPlane::Ask,
        AuthKind::Local => AdminPlane::Skip,
    }
}

/// Resolve an operator-typed admin-server address into seed socket addresses.
/// Accepts a hostname or an IP, with or without a `:port`; when the port is
/// omitted it defaults to the conventional admin-server port. A hostname may
/// resolve to several addresses — all are returned, which the peer walk in the
/// join path tries in turn.
pub fn resolve_admin_server_seeds(input: &str) -> Result<Vec<SocketAddr>> {
    use std::net::ToSocketAddrs;
    let s = input.trim();
    if s.is_empty() {
        bail!("empty address");
    }
    // An explicit port (`ip:port`, `host:port`, `[ipv6]:port`) resolves
    // directly. Without one, std's `&str` resolver errors for lack of a
    // port; fall back to attaching the default admin-server port to the
    // bare host / ip.
    let seeds: Vec<SocketAddr> = match s.to_socket_addrs() {
        Ok(addrs) => addrs.collect(),
        Err(_) => (s, DEFAULT_PORT)
            .to_socket_addrs()
            .with_context(|| format!("could not resolve admin server address {s:?}"))?
            .collect(),
    };
    if seeds.is_empty() {
        bail!("{s:?} resolved to no addresses");
    }
    Ok(seeds)
}

/// The first seed [`resolve_admin_server_seeds`] yields. For the commands that
/// contact one admin server directly (`add-parent`, `review-delegation`,
/// remote `perms`) rather than peer-walking a set of discovery seeds.
pub fn resolve_admin_server_addr(input: &str) -> Result<SocketAddr> {
    Ok(resolve_admin_server_seeds(input)?
        .into_iter()
        .next()
        .expect("resolve_admin_server_seeds never returns an empty vec"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // IP literals so the resolution is deterministic and needs no DNS; the
    // hostname path is the same `ToSocketAddrs` call, just with a name on the
    // left.
    #[test]
    fn admin_server_seeds_default_and_explicit_port() {
        let dflt = DEFAULT_PORT;
        // bare ip → default admin port
        assert_eq!(
            resolve_admin_server_seeds("1.2.3.4").unwrap(),
            vec![SocketAddr::from(([1, 2, 3, 4], dflt))],
        );
        // explicit port wins
        assert_eq!(
            resolve_admin_server_seeds("1.2.3.4:9999").unwrap(),
            vec![SocketAddr::from(([1, 2, 3, 4], 9999))],
        );
        // surrounding whitespace is trimmed
        assert_eq!(
            resolve_admin_server_seeds("  1.2.3.4  ").unwrap(),
            vec![SocketAddr::from(([1, 2, 3, 4], dflt))],
        );
        // bare ipv6 → default port; bracketed ipv6 carries an explicit port
        assert_eq!(
            resolve_admin_server_seeds("::1").unwrap(),
            vec![SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, dflt))],
        );
        assert_eq!(
            resolve_admin_server_seeds("[::1]:9999").unwrap(),
            vec![SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 9999))],
        );
        // empty / blank is rejected (the prompt treats blank as "none" before
        // reaching here, but the parser must not accept it either)
        assert!(resolve_admin_server_seeds("").is_err());
        assert!(resolve_admin_server_seeds("   ").is_err());
        // the single-addr wrapper applies the same defaulting
        assert_eq!(
            resolve_admin_server_addr("1.2.3.4").unwrap(),
            SocketAddr::from(([1, 2, 3, 4], dflt)),
        );
        assert!(resolve_admin_server_addr("").is_err());
    }

    // The install-profile matrix, exhaustively. This test and
    // design/admin-server.md (Install profiles) mirror `admin_plane_decision`;
    // change all three together.
    #[test]
    fn the_admin_plane_matrix() {
        use AdminPlane::*;
        use AuthKind::*;
        // Declining the admin plane on a TLS or krb5 trust domain breaks renewal +
        // zero-touch installs forever, so neither is a question — fresh
        // trust domain or joining one (enrollment queues for remote approval, so no
        // admin is needed at this keyboard). Anonymous trust domains may not want
        // the machinery; local auth has nothing to discover.
        for (kind, want) in
            [(Tls, Mandatory), (Krb5, Mandatory), (Anonymous, Ask), (Local, Skip)]
        {
            assert_eq!(admin_plane_decision(kind, false), want, "{kind:?}");
        }
        // The expert opt-out beats everything.
        for kind in [Tls, Krb5, Anonymous, Local] {
            assert_eq!(admin_plane_decision(kind, true), Skip);
        }
    }
}
