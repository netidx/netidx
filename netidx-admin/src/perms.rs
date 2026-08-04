//! `PMap` (resolver permissions file) load / edit / save.
//!
//! The on-disk format is a JSON object mapping a netidx path to an
//! object mapping an entity name (a netidx user / group identity) to a
//! permission-bit string drawn from the alphabet `!swlpd`:
//!
//! ```json
//! {
//!   "/local": {
//!     "alice": "swlpd",
//!     "domain users": "!swlpd"
//!   }
//! }
//! ```
//!
//! Mirrored from `cfg/perms.json` in this repo.

use crate::atomic;
use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::resolver_server::auth::Permissions;
pub use netidx::resolver_server::config::PMap;
use std::{collections::HashMap, path::Path};

/// Read a perms file from `path`. Thin wrapper over
/// [`netidx::resolver_server::config::load_perms`] so the read/parse
/// implementation lives once in netidx; kept here to pair with
/// [`save_perms`], which is tooling-only (netidx never writes perms).
pub fn load_perms<P: AsRef<Path>>(path: P) -> Result<PMap> {
    netidx::resolver_server::config::load_perms(path)
}

/// Atomically save `p` to `path` at mode 0o644.
pub fn save_perms<P: AsRef<Path>>(path: P, p: &PMap) -> Result<()> {
    atomic::write_atomic_pretty_json(path.as_ref(), p)
}

pub async fn load_perms_async<P: AsRef<Path>>(path: P) -> Result<PMap> {
    let path = path.as_ref();
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("reading perms file {path:?}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing perms file {path:?}"))
}

pub async fn save_perms_async<P: AsRef<Path>>(path: P, p: &PMap) -> Result<()> {
    atomic::write_atomic_pretty_json_async(path.as_ref(), p).await
}

/// Insert or replace a (path, entity, perm-string) entry. Validates the
/// perm string before mutating. Empty `entity` (`""`) is the conventional
/// anonymous identity in the file format.
pub fn add_entry(p: &mut PMap, path: &str, entity: &str, perms: &str) -> Result<()> {
    Permissions::try_from(perms)?;
    let path = ArcStr::from(path);
    let entity = ArcStr::from(entity);
    let perms = ArcStr::from(perms);
    p.0.entry(path).or_default().insert(entity, perms);
    Ok(())
}

/// Parse perms JSON and check every entry's permission bits.
///
/// `load_perms` and `validate_for_path` keep bits as opaque strings, so without
/// this an edit with unparseable bits would be written and only fail when the
/// resolver next loaded it. One rule, so the CLI, the TUI, and the daemon
/// refuse the same input with the same message.
pub fn validate(edited: &str) -> Result<PMap> {
    let pmap: PMap = serde_json::from_str(edited).context("not valid perms JSON")?;
    for (path, entity, bits) in iter(&pmap) {
        Permissions::try_from(bits.as_str()).with_context(|| {
            format!("invalid permission bits {bits:?} for {entity} at {path}")
        })?;
    }
    Ok(pmap)
}

/// [`validate`], then canonical JSON to send — what an editor loop wants, so
/// the CA receives a normalized document rather than whatever was typed.
pub fn normalize(edited: &str) -> Result<String> {
    serde_json::to_string(&validate(edited)?).context("serializing perms")
}

/// Pretty-print perms JSON, for display or for seeding an editor.
pub fn pretty(perms_json: &str) -> Result<String> {
    let v: serde_json::Value =
        serde_json::from_str(perms_json).context("parsing perms JSON")?;
    serde_json::to_string_pretty(&v).context("formatting perms JSON")
}

/// Look up the permission string for `(path, entity)`, if present.
pub fn lookup<'a>(p: &'a PMap, path: &str, entity: &str) -> Option<&'a ArcStr> {
    p.0.get(path).and_then(|tbl| tbl.get(entity))
}

/// Iterate `(path, entity, perm)` triples in arbitrary order.
pub fn iter(p: &PMap) -> impl Iterator<Item = (&ArcStr, &ArcStr, &ArcStr)> {
    p.0.iter().flat_map(|(path, tbl)| {
        tbl.iter().map(move |(entity, perms)| (path, entity, perms))
    })
}

/// Convenience: an empty perms map.
pub fn empty() -> PMap {
    PMap(HashMap::new())
}

/// Whether the resolver a seed is being written for can resolve an
/// identity's group memberships.
///
/// `IdMapType::DoNotMap` reports every identity as belonging to no group at
/// all (`os::unix::Mapper::groups` returns the identity itself and an empty
/// group list), so a perms entry naming a group is inert by construction
/// there. Seeding one would write a rule that can never match — the operator
/// reads a shared grant in their perms file and gets permission denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Groups {
    /// Group names in perms resolve — an id-map daemon, `/bin/id` / nsswitch,
    /// or unix uids behind local auth.
    Resolve,
    /// They do not: perms are keyed on the raw identity string.
    DoNotResolve,
}

/// Sensible starter perms for a freshly-installed resolver. The
/// `users`-group rule anchors at `base` — the resolver's own
/// subtree — so the seed says "members of `users` can subscribe /
/// write / list anywhere this resolver controls". It is omitted
/// entirely when `groups` is [`Groups::DoNotResolve`]. The per-user
/// playground lives at `<base>/users/$[user]` so it's always under a
/// real directory: netidx's PMap loader (see
/// `auth.rs::PMap::from_file`) rejects a `$[user]` entry whose
/// dirname is root, so anchoring it directly at base would break
/// the root-resolver case.
///
/// - `<base>` → members of the `users` group get `swl` (subscribe,
///   write, list) — read everyone's published values and write to
///   existing paths, but *not* `p` (publish) so they can't drop new
///   paths into someone else's subtree.
/// - `<base>/users/$[user]` → the authenticated user gets `swlpd`
///   (full subscribe / write / list / publish / publish-default
///   rights) under their own subtree. `$[user]` is the resolver's
///   built-in variable, substituted at evaluation time with the
///   connecting principal's name — so user `alice` gets full control
///   of `<base>/users/alice/**` without an explicit per-user entry.
///
/// Together these give a "shared read-write tree under the
/// resolver's base, plus a per-user playground under
/// `<base>/users`" out of the box. Operators who want a different
/// default can override `perms_seed` on the template or edit the
/// emitted `perms.json` directly.
pub fn default_seed(base: &str, groups: Groups) -> PMap {
    use netidx::path::Path;
    let mut p = empty();
    // Normalise base to a canonical netidx path. `""` or unset bases
    // collapse to `/`, and `Path::append` handles redundant
    // separators when we build the `<base>/users/$[user]` path
    // below.
    let base_path = if base.is_empty() { Path::root() } else { Path::from_str(base) };
    // Group-wide read+write at the resolver's base. Not a `$[group]`
    // dynamic entry — that form requires the group name to appear in
    // the basename, and we want a fixed reference to the literal
    // group `users`. So this is just a normal entry whose entity
    // happens to be a group name; the resolver matches it when the
    // connecting principal is a member of that group.
    if groups == Groups::Resolve {
        add_entry(&mut p, base_path.as_ref(), "users", "swl")
            .expect("static seed must validate");
    }
    // Per-user playground under `<base>/users/$[user]`. The intermediate
    // `users` segment is mandatory: the PMap loader unwraps
    // `Path::dirname(...)` on `$[user]` paths, and dirname of a
    // path directly under root is `None` — so anchoring this
    // entry at e.g. `/$[user]` would panic the resolver at startup.
    let user_subtree = base_path.append("users").append("$[user]");
    add_entry(&mut p, user_subtree.as_ref(), "$[user]", "swlpd")
        .expect("static seed must validate");
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One rule for edited perms, so the CLI, the TUI and the daemon refuse the
    /// same input. Each used to check the bits itself; `x` is not in the
    /// `!swlpd` alphabet and must be caught before anything is written, because
    /// nothing downstream looks at the bits until a resolver loads the file.
    #[test]
    fn bits_outside_the_alphabet_are_refused_and_the_entry_is_named() {
        let e = validate(r#"{"/eu": {"alice": "swlpdx"}}"#).unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains("swlpdx"), "names the bits: {msg}");
        assert!(msg.contains("alice") && msg.contains("/eu"), "names where: {msg}");
        assert!(validate("not json at all").is_err());
    }

    /// `normalize` is `validate` plus canonical JSON, so what reaches the CA is
    /// a normalized document rather than whatever the operator's editor left.
    #[test]
    fn normalize_returns_canonical_json_for_a_valid_map() {
        let out = normalize("{\n  \"/eu\"  :  {\"alice\":\"swlpd\"}\n}").unwrap();
        assert_eq!(out, r#"{"/eu":{"alice":"swlpd"}}"#);
        assert!(normalize(r#"{"/eu": {"alice": "nope"}}"#).is_err());
    }

    #[test]
    fn default_seed_root_base_anchors_at_root() {
        let p = default_seed("/", Groups::Resolve);
        // Users group `swl` at the resolver's base (`/` here).
        assert_eq!(lookup(&p, "/", "users").map(|s| s.as_str()), Some("swl"));
        // Per-user dynamic entry under `<base>/users/$[user]` —
        // `/users/$[user]` for a root resolver. The intermediate
        // `users` segment exists because PMap::from_file rejects
        // a `$[user]` entry whose dirname is root.
        assert_eq!(
            lookup(&p, "/users/$[user]", "$[user]").map(|s| s.as_str()),
            Some("swlpd"),
        );
    }

    /// A resolver that does not map ids reports every identity as belonging
    /// to no group, so the shared `users` grant could never match — the seed
    /// wrote a rule that read like access and denied it. The per-user
    /// playground stays: `$[user]` is the raw identity, which is exactly what
    /// this mode keys on.
    #[test]
    fn without_id_mapping_the_seed_has_no_group_it_could_mean() {
        let p = default_seed("/", Groups::DoNotResolve);
        assert!(
            lookup(&p, "/", "users").is_none(),
            "a group grant is inert without id mapping and must not be seeded"
        );
        assert_eq!(
            lookup(&p, "/users/$[user]", "$[user]").map(|s| s.as_str()),
            Some("swlpd"),
        );
    }

    #[test]
    fn default_seed_child_base_anchors_under_base() {
        // A workstation-style resolver attached at /local should
        // anchor the seed entries at /local and /local/users/$[user]
        // — not the old root-level /users paths, which sit in a
        // different subtree and are useless to this resolver.
        let p = default_seed("/local", Groups::Resolve);
        assert_eq!(lookup(&p, "/local", "users").map(|s| s.as_str()), Some("swl"),);
        assert_eq!(
            lookup(&p, "/local/users/$[user]", "$[user]").map(|s| s.as_str()),
            Some("swlpd"),
        );
        // The bare root-level entries shouldn't appear — guards
        // against accidental regressions to the hard-coded layout.
        assert!(lookup(&p, "/users", "users").is_none());
    }

    #[test]
    fn default_seed_empty_base_collapses_to_root() {
        // Defensive: an empty `base` arg should be treated as `/`
        // so callers don't have to special-case it.
        let p = default_seed("", Groups::Resolve);
        assert_eq!(lookup(&p, "/", "users").map(|s| s.as_str()), Some("swl"));
        assert_eq!(
            lookup(&p, "/users/$[user]", "$[user]").map(|s| s.as_str()),
            Some("swlpd"),
        );
    }

    #[test]
    fn default_seed_validates_through_resolver_loader() {
        // The dynamic-entry shape (`$[user]` ending, `$[user]`
        // entity) has strict rules in PMap::from_file. Catch any
        // future drift in `default_seed` here rather than at first
        // resolver-start.
        use netidx::resolver_server::config;
        let p = default_seed("/", Groups::Resolve);
        let file = config::file::ConfigBuilder::default()
            .member_servers(vec![
                config::file::MemberServerBuilder::default()
                    .addr("127.0.0.1:4564".parse().unwrap())
                    .bind_addr("127.0.0.1".parse().unwrap())
                    .auth(config::file::Auth::Anonymous)
                    .build()
                    .unwrap(),
            ])
            .perms(p)
            .build()
            .unwrap();
        config::Config::from_file(file).expect("default_seed must pass PMap::from_file");
    }

    #[test]
    fn add_round_trip() {
        let mut p = empty();
        add_entry(&mut p, "/foo", "alice", "swlpd").unwrap();
        add_entry(&mut p, "/foo", "bob", "sl").unwrap();
        add_entry(&mut p, "/bar", "alice", "p").unwrap();

        assert_eq!(lookup(&p, "/foo", "alice").map(|s| s.as_str()), Some("swlpd"));
        assert_eq!(lookup(&p, "/foo", "bob").map(|s| s.as_str()), Some("sl"));
        assert_eq!(lookup(&p, "/bar", "alice").map(|s| s.as_str()), Some("p"));
    }

    #[test]
    fn add_validates_before_mutating() {
        let mut p = empty();
        add_entry(&mut p, "/foo", "alice", "swxlpd").unwrap_err();
        // Map must be untouched.
        assert!(p.0.get("/foo").is_none());
    }

    #[test]
    fn load_save_round_trip() {
        let mut p = empty();
        add_entry(&mut p, "/foo", "alice", "swlpd").unwrap();
        add_entry(&mut p, "/foo", "", "sl").unwrap(); // anonymous

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("perms.json");
        save_perms(&path, &p).unwrap();

        let back = load_perms(&path).unwrap();
        assert_eq!(lookup(&back, "/foo", "alice").map(|s| s.as_str()), Some("swlpd"));
        assert_eq!(lookup(&back, "/foo", "").map(|s| s.as_str()), Some("sl"));
    }

    #[test]
    fn parses_repo_example() {
        let json = r#"{
            "/": {
                "root@YOUR-KRB5-REALM": "swlpd",
                "domain users": "!swlpd"
            },
            "/example/path": {
                "domain users": "sl",
                "publisher@YOUR-KRB5-REALM": "p"
            },
            "/anon": {
                "": "swlpd"
            }
        }"#;
        let p: PMap = serde_json::from_str(json).unwrap();
        assert_eq!(
            lookup(&p, "/", "root@YOUR-KRB5-REALM").map(|s| s.as_str()),
            Some("swlpd")
        );
        assert_eq!(
            lookup(&p, "/example/path", "domain users").map(|s| s.as_str()),
            Some("sl")
        );
        assert_eq!(lookup(&p, "/anon", "").map(|s| s.as_str()), Some("swlpd"));
    }
}
