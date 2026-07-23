//! Remote-admin operations behind the [`Answerer`] seam.
//!
//! Every operator-facing admin action was, historically, one interactive
//! ceremony in the CLI: list a queue, pick an item, eyeball a code, act. Those
//! are relocated here and restructured into **query + action** pairs so the
//! strict CLI, the TUI, and Atlas share one implementation and the library
//! never touches a terminal.
//!
//! The redesign: a *query* returns structured data (a list); an *action* takes
//! the item's **code** — the grouped base32 fingerprint an operator reads out
//! of band — as its argument. Passing the code both *selects* the item and
//! *asserts* it: [`find_by_code`] re-derives the code from fresh data and
//! refuses on mismatch or ambiguity, which is the non-interactive form of the
//! old "does this code match? y/n" gesture and mirrors
//! `ca fingerprint <addr>` → `--accept-glyph <code>`.
//!
//! Codes are **always recomputed locally** from the fresh query result (never
//! trusted from the wire): an action re-runs the query, then `find_by_code`, so
//! the assertion is against current state. The opaque per-request id the RPC
//! needs stays a private field of the query row — callers address a request
//! only by its code.

use crate::{
    admin_proto::{AdminCredential, NodeKind, Secret},
    admin_server_config,
    answer::{Answerer, Field},
    fingerprint::Fingerprint,
    paths,
    plan::enroll::current_username,
    transport::{self, CaIdentity},
};
use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

pub mod delegation;
pub mod perms;
pub mod queue;
pub mod revoke;
pub mod roster;
pub mod servers;
pub mod service;

/// A pinned, authenticated remote-admin session: the admin server to talk to,
/// its confirmed identity (later connections verify against *this* CA, not a
/// re-presented one), and the admin credentials authorizing the operation.
pub struct AdminSession {
    /// The admin server this session talks to.
    pub server: SocketAddr,
    /// The confirmed CA identity (glyph verified or local-cert matched).
    pub identity: CaIdentity,
    /// The role-admin name authorizing the operation.
    pub admin: String,
    pub credential: AdminCredential,
}

/// This host's own admin-server listen address, read from its
/// `admin-server.json`, loopback-adjusted when it binds all interfaces. `None`
/// when this host runs no admin server — the caller then requires `--server`.
pub fn local_admin_server_listen() -> Option<SocketAddr> {
    let path = paths::discover_admin_server_config().ok()?;
    let cfg = admin_server_config::load(&path).ok()?;
    let mut addr = cfg.listen;
    if addr.ip().is_unspecified() {
        addr.set_ip(IpAddr::V4(Ipv4Addr::LOCALHOST));
    }
    Some(addr)
}

/// Resolve which admin server to talk to and establish its identity — no auth
/// yet. `server = None` means *this host's own admin server*. The identity is
/// **auto-verified** against the local CA certificate when present (the CA-host
/// case — no glyph to confirm); otherwise it is routed through
/// [`Answerer::confirm_identity`] (an interactive glyph confirm, or the strict
/// answerer's `--accept-glyph` comparison). The library never mDNS-discovers:
/// an interactive frontend resolves a discovered network to a concrete
/// `Some(addr)` before calling in.
async fn resolve_identity(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<&Path>,
) -> Result<(SocketAddr, CaIdentity)> {
    let server = server.or_else(local_admin_server_listen).context(
        "no admin server specified and none found on this host — pass --server <ip:port>",
    )?;
    let identity = transport::fetch_identity(server, NodeKind::Client)
        .await
        .with_context(|| format!("contacting admin server {server}"))?;
    // On the CA host (or any box holding the CA dir) the local CA cert is the
    // trust anchor — verify against it rather than asking the operator to
    // confirm their own glyph.
    let local_fp = ca_dir
        .map(Path::to_path_buf)
        .or_else(|| paths::user_ca_dir().ok())
        .and_then(|d| std::fs::read(d.join("certificate.pem")).ok())
        .and_then(|pem| Fingerprint::of_cert_pem(&pem).ok());
    match local_fp {
        Some(fp) if fp == identity.fingerprint => {
            ans.note(&format_compact!("verified {server} against the local CA"));
        }
        _ => {
            if !ans.confirm_identity(&identity).await? {
                bail!("the admin server's identity was not confirmed; nothing was sent");
            }
        }
    }
    Ok((server, identity))
}

/// Where an admin-roster (or other management) op runs: this host's own admin
/// server over its local control socket, or a pinned remote admin server.
///
/// The two are authenticated completely differently and must not be conflated:
/// **Local** talks to the daemon over its `0600` + `SO_PEERCRED` control
/// socket, which trusts the connecting superuser with no glyph and no password;
/// **Remote** is a pinned TLS session that glyph-confirms the CA and
/// authenticates a named admin with a password. A `--server` pointing at *this*
/// host's own CA is still `Remote` (it fetches the identity and auto-verifies
/// against the local cert) — only the absence of `--server` selects `Local`.
pub enum AdminTarget {
    /// This host's admin server, over its local control socket.
    #[cfg(unix)]
    Local { cfg_path: PathBuf },
    /// A pinned, authenticated remote admin session.
    Remote { session: AdminSession },
}

/// Resolve which admin server a management op runs against: `Some(addr)` opens a
/// pinned [`AdminSession`] ([`AdminTarget::Remote`]); `None` selects this host's
/// own admin server over its local control socket ([`AdminTarget::Local`]),
/// erroring if this host runs none.
pub async fn resolve_admin_target(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
) -> Result<AdminTarget> {
    match server {
        Some(addr) => {
            let session =
                open_admin_session(ans, Some(addr), ca_dir, admin, password).await?;
            Ok(AdminTarget::Remote { session })
        }
        None => {
            #[cfg(unix)]
            {
                let cfg_path = paths::discover_admin_server_config().context(
                    "no --server given and no local admin server on this host — this box \
                     must run the admin server to manage its roster locally, or pass \
                     --server <ip:port> to manage a remote CA",
                )?;
                Ok(AdminTarget::Local { cfg_path })
            }
            #[cfg(not(unix))]
            {
                bail!("a remote admin server address is required on this platform")
            }
        }
    }
}

/// The one shared remote preamble: resolve the admin server + confirm its
/// identity, then collect the admin name (defaulting to the current OS user)
/// and password. Every authenticated remote query and action starts here.
pub async fn open_admin_session(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
) -> Result<AdminSession> {
    let (server, controller_identity) =
        resolve_controller(ans, server, ca_dir.as_deref()).await?;
    if password.is_none()
        && !ans.has_explicit_secret(Field::AdminPassword)
        && let Some(cached) =
            crate::session_cache::load(&controller_identity.fingerprint.text())?
    {
        return Ok(AdminSession {
            server,
            identity: controller_identity,
            admin: cached.admin,
            credential: AdminCredential::Session { token: cached.token },
        });
    }
    password_session(ans, server, controller_identity, admin, password).await
}

/// Open a controller-verified password session even if a reusable login cache
/// exists. This is the `admin login` path: an explicit login replaces the
/// selected cached session instead of accidentally reusing it.
pub async fn open_admin_password_session(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
) -> Result<AdminSession> {
    let (server, controller_identity) =
        resolve_controller(ans, server, ca_dir.as_deref()).await?;
    password_session(ans, server, controller_identity, admin, password).await
}

async fn resolve_controller(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<&Path>,
) -> Result<(SocketAddr, CaIdentity)> {
    let (server, identity) = resolve_identity(ans, server, ca_dir).await?;
    // Resolve the authoritative controller while we still hold no secret.
    let map = transport::get_map_pinned(server, NodeKind::Client, &identity).await?;
    let controller = map
        .controller_entry()
        .filter(|s| s.state == netidx_admin_proto::ServerState::Registered)
        .context("the authoritative map has no registered controller")?;
    let server = controller.addr;
    let controller_identity = transport::fetch_identity(server, NodeKind::Client).await?;
    if controller_identity.fingerprint != identity.fingerprint
        || !controller_identity.controller
        || controller_identity.server_id != map.controller
    {
        bail!("the map's controller candidate failed exact home-CA verification");
    }
    Ok((server, controller_identity))
}

async fn password_session(
    ans: &mut dyn Answerer,
    server: SocketAddr,
    controller_identity: CaIdentity,
    admin: Option<String>,
    password: Option<Secret>,
) -> Result<AdminSession> {
    let admin = ans
        .text(Field::AdminName, admin, current_username().as_deref(), true)
        .await?
        .context("an admin name is required")?;
    let password = ans.secret(Field::AdminPassword, password).await?;
    Ok(AdminSession {
        server,
        identity: controller_identity,
        admin: admin.clone(),
        credential: AdminCredential::Password { admin, password },
    })
}

/// Select exactly one item from `items` by its **full** security code.
///
/// `code` is the grouped base32 fingerprint an operator read out of band and
/// must be the FULL form: all 52 base32 chars = the full 256-bit fingerprint.
/// Matching is whitespace- and case-insensitive. A short prefix is deliberately
/// **refused for actions**: a query command may show a short code for
/// readability, but acting on a request (approve / deny / delegate) asserts all
/// 256 bits — so a request that has since left the queue (TTL expiry, already
/// actioned) cannot be impersonated by a cheaper prefix collision. Refuses:
/// - a code that is not exactly a full fingerprint (`use the full code`),
/// - a non-base32 character,
/// - a code that matches no item (`no request matches`),
/// - (defensively) a code that matches more than one item.
///
/// `of` derives the code for a row; a row whose code cannot be derived (e.g. an
/// unparseable CSR) yields `None` and never matches. Pure and IO-free — the
/// security assertion is unit-testable and shared by the queue and delegation
/// action groups.
pub fn find_by_code<'a, T>(
    items: &'a [T],
    code: &str,
    of: impl Fn(&T) -> Option<Fingerprint>,
) -> Result<&'a T> {
    let norm: String = code
        .chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(char::to_uppercase)
        .collect();
    if norm.is_empty() {
        bail!("a code is required to select a request");
    }
    if let Some(bad) =
        norm.chars().find(|c| !c.is_ascii_uppercase() && !('2'..='7').contains(c))
    {
        bail!("invalid code character {bad:?} — codes are base32 (A–Z, 2–7)");
    }
    if norm.len() != 52 {
        bail!(
            "code {code:?} is not a full request code — copy all 52 base32 \
             characters shown by the query command (a short prefix is not \
             accepted for this action)"
        );
    }
    let mut found: Option<&T> = None;
    for it in items {
        let Some(fp) = of(it) else { continue };
        let text: String = fp.text().chars().filter(|c| *c != ' ').collect();
        if text == norm {
            if found.is_some() {
                bail!(
                    "code {code:?} matches more than one request (duplicate \
                     fingerprints) — refusing to act ambiguously"
                );
            }
            found = Some(it);
        }
    }
    found.with_context(|| format!("no request matches code {code:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(seed: &str) -> Fingerprint {
        // A distinct fingerprint per seed char, via a full 52-char base32 code.
        Fingerprint::parse_text(&seed.repeat(52)).unwrap()
    }

    #[test]
    fn find_by_code_full_exact_match() {
        let items = [fp("A"), fp("B")];
        let code = items[0].text();
        let got = find_by_code(&items, &code, |f| Some(*f)).unwrap();
        assert_eq!(*got, items[0]);
    }

    #[test]
    fn find_by_code_short_prefix_refused() {
        // A short prefix that would once have matched is now refused: an action
        // must assert the full 256-bit code, never a cheaper prefix.
        let items = [fp("A"), fp("B")];
        let e = find_by_code(&items, "AAAAA", |f| Some(*f)).unwrap_err();
        assert!(format!("{e:#}").contains("full request code"));
    }

    #[test]
    fn find_by_code_duplicate_full_code_refused() {
        // Two rows with the *same* full fingerprint — the defensive ambiguity
        // guard still refuses rather than acting on an arbitrary one.
        let items = [fp("A"), fp("A")];
        let code = items[0].text();
        let e = find_by_code(&items, &code, |f| Some(*f)).unwrap_err();
        assert!(format!("{e:#}").contains("more than one request"));
    }

    #[test]
    fn find_by_code_no_match() {
        let items = [fp("A"), fp("B")];
        let code = fp("Z").text(); // not in the set
        let e = find_by_code(&items, &code, |f| Some(*f)).unwrap_err();
        assert!(format!("{e:#}").contains("no request matches"));
    }

    #[test]
    fn find_by_code_rejects_non_base32() {
        let items = [fp("A")];
        let e = find_by_code(&items, "!!oops", |f| Some(*f)).unwrap_err();
        assert!(format!("{e:#}").contains("base32"));
    }

    #[test]
    fn find_by_code_skips_underivable_rows() {
        // A row whose code cannot be derived never matches.
        let items = [fp("A")];
        let code = items[0].text();
        let e = find_by_code(&items, &code, |_| None).unwrap_err();
        assert!(format!("{e:#}").contains("no request matches"));
    }
}
