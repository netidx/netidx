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
    admin_client::{self, CaIdentity},
    admin_proto::{NodeKind, Secret},
    admin_server_config::AdminServerConfig,
    answer::{Answerer, Field},
    fingerprint::Fingerprint,
    paths,
    plan::enroll::current_username,
};
use anyhow::{Context, Result, bail};
use compact_str::format_compact;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

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
    /// The admin's password.
    pub password: Secret,
}

/// This host's own admin-server listen address, read from its
/// `admin-server.json`, loopback-adjusted when it binds all interfaces. `None`
/// when this host runs no admin server — the caller then requires `--server`.
pub fn local_admin_server_listen() -> Option<SocketAddr> {
    let path = paths::discover_admin_server_config().ok()?;
    let cfg = AdminServerConfig::load(&path).ok()?;
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
    let identity = admin_client::fetch_identity(server, NodeKind::Client)
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
    let (server, identity) = resolve_identity(ans, server, ca_dir.as_deref()).await?;
    let admin = ans
        .text(Field::AdminName, admin, current_username().as_deref(), true)
        .await?
        .context("an admin name is required")?;
    let password = ans.secret(Field::AdminPassword, password).await?;
    Ok(AdminSession { server, identity, admin, password })
}

/// Select exactly one item from `items` by its security **code**.
///
/// `code` is the grouped base32 fingerprint an operator read out of band. It
/// may be the **full** form (52 base32 chars = the full 256-bit fingerprint, an
/// exact match — the security-load-bearing form) or an unambiguous base32
/// **prefix** (a convenience, e.g. the 8-char short code). Matching is
/// whitespace- and case-insensitive. Refuses:
/// - a code that matches no item (`no request matches`),
/// - a prefix that matches more than one (`ambiguous` — use the full code),
/// - a non-base32 character, or a code longer than a full fingerprint.
///
/// `of` derives the code for a row; a row whose code cannot be derived (e.g. an
/// unparseable CSR) yields `None` and never matches. Pure and IO-free — the
/// security assertion is unit-testable and shared by the queue, delegation, and
/// revocation groups.
pub fn find_by_code<'a, T>(
    items: &'a [T],
    code: &str,
    of: impl Fn(&T) -> Option<Fingerprint>,
) -> Result<&'a T> {
    let norm: String =
        code.chars().filter(|c| !c.is_whitespace()).flat_map(char::to_uppercase).collect();
    if norm.is_empty() {
        bail!("a code is required to select a request");
    }
    if let Some(bad) =
        norm.chars().find(|c| !c.is_ascii_uppercase() && !('2'..='7').contains(c))
    {
        bail!("invalid code character {bad:?} — codes are base32 (A–Z, 2–7)");
    }
    if norm.len() > 52 {
        bail!("code {code:?} is longer than a full request code");
    }
    let full = norm.len() == 52;
    let mut found: Option<&T> = None;
    for it in items {
        let Some(fp) = of(it) else { continue };
        let text: String = fp.text().chars().filter(|c| *c != ' ').collect();
        let hit = if full { text == norm } else { text.starts_with(&norm) };
        if hit {
            if found.is_some() {
                bail!(
                    "code {code:?} is ambiguous — it matches more than one request; \
                     use the full code shown by the query command"
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
    fn find_by_code_unambiguous_prefix() {
        let items = [fp("A"), fp("B")];
        // fp("A") is 32 zero bytes → text is all 'A's; "AAAAA" matches only it.
        let got = find_by_code(&items, "AAAAA", |f| Some(*f)).unwrap();
        assert_eq!(*got, items[0]);
    }

    #[test]
    fn find_by_code_ambiguous_prefix_refused() {
        // Two identical fingerprints share every prefix.
        let items = [fp("A"), fp("A")];
        let e = find_by_code(&items, "AAAAA", |f| Some(*f)).unwrap_err();
        assert!(format!("{e:#}").contains("ambiguous"));
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
