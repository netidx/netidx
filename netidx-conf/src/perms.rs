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
use anyhow::Result;
use arcstr::ArcStr;
pub use netidx::resolver_server::config::PMap;
use std::{collections::HashMap, path::Path};

/// Parse and validate a permission-bit string. The alphabet is
/// `!swlpd`, with `!` only allowed as the first character (where it
/// marks the entry as a *deny* mask). Mirrors the parser in
/// `netidx::resolver_server::auth::Permissions::try_from`, which we
/// can't import (the `auth` module is private to the netidx crate).
pub fn validate_perm_bits(s: &str) -> Result<()> {
    for (i, c) in s.chars().enumerate() {
        match c {
            '!' if i == 0 => {}
            '!' => bail!("'!' may only be used as the first character"),
            's' | 'w' | 'l' | 'p' | 'd' => {}
            other => bail!(
                "unrecognized permission bit {other}, valid bits are !swlpd"
            ),
        }
    }
    Ok(())
}

/// Read a perms file from `path`.
pub fn load_perms<P: AsRef<Path>>(path: P) -> Result<PMap> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)
        .map_err(|e| anyhow!("reading perms {:?}: {e}", path))?;
    let p: PMap = serde_json::from_slice(&bytes)
        .map_err(|e| anyhow!("parsing perms {:?}: {e}", path))?;
    Ok(p)
}

/// Atomically save `p` to `path` at mode 0o644.
pub fn save_perms<P: AsRef<Path>>(path: P, p: &PMap) -> Result<()> {
    atomic::write_atomic_pretty_json(path.as_ref(), p)
}

/// Insert or replace a (path, entity, perm-string) entry. Validates the
/// perm string before mutating. Empty `entity` (`""`) is the conventional
/// anonymous identity in the file format.
pub fn add_entry(
    p: &mut PMap,
    path: &str,
    entity: &str,
    perms: &str,
) -> Result<()> {
    validate_perm_bits(perms)?;
    let path = ArcStr::from(path);
    let entity = ArcStr::from(entity);
    let perms = ArcStr::from(perms);
    p.0.entry(path).or_default().insert(entity, perms);
    Ok(())
}

/// Remove an entity's entry from a path. If removing the last entity
/// under `path`, also removes the path entry. No-op when the entry
/// doesn't exist.
pub fn remove_entry(p: &mut PMap, path: &str, entity: &str) {
    use std::collections::hash_map::Entry;
    if let Entry::Occupied(mut e) = p.0.entry(ArcStr::from(path)) {
        e.get_mut().remove(entity);
        if e.get().is_empty() {
            e.remove();
        }
    }
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

/// Iterate entries belonging to a single path.
pub fn iter_path<'a>(
    p: &'a PMap,
    path: &str,
) -> Box<dyn Iterator<Item = (&'a ArcStr, &'a ArcStr)> + 'a> {
    match p.0.get(path) {
        Some(tbl) => Box::new(tbl.iter()) as Box<dyn Iterator<Item = _>>,
        None => Box::new(std::iter::empty::<(&'a ArcStr, &'a ArcStr)>()),
    }
}

/// Convenience: an empty perms map.
pub fn empty() -> PMap {
    PMap(HashMap::new())
}

/// Sensible starter perms for a freshly-installed resolver:
///
/// - `/users/$[user]` → the authenticated user gets `swlpd` (full
///   subscribe / write / list / publish / publish-default rights)
///   under their own subtree. `$[user]` is the resolver's built-in
///   variable, substituted at evaluation time with the connecting
///   principal's name — so user `alice` gets full control of
///   `/users/alice/**` without an explicit per-user entry.
/// - `/users` → members of the `users` group get `swl` (subscribe,
///   write, list) — i.e. read everyone's published values and write
///   to existing paths, but *not* `p` (publish) so they can't drop
///   new paths into someone else's subtree.
///
/// Together these give a "shared playground under `/users` with
/// per-user write-protected directories" out of the box. Operators
/// who want a different default can override `perms_seed` on the
/// template or edit the emitted `perms.json` directly.
pub fn default_seed() -> PMap {
    let mut p = empty();
    // Full control of own subtree via the $[user] dynamic entry. The
    // resolver's PMap loader requires that the only key inside a
    // `…/$[user]` entry is `$[user]` itself; see `PMap::from_file`
    // in netidx/src/resolver_server/auth.rs.
    add_entry(&mut p, "/users/$[user]", "$[user]", "swlpd")
        .expect("static seed must validate");
    // Group-wide read+write at the /users root. Not a `$[group]`
    // dynamic entry — that form requires the group name to appear in
    // the basename, and we want a fixed reference to the literal
    // group `users`. So this is just a normal entry whose entity
    // happens to be a group name; the resolver matches it when the
    // connecting principal is a member of that group.
    add_entry(&mut p, "/users", "users", "swl")
        .expect("static seed must validate");
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_canonical_alphabet() {
        for s in ["s", "w", "l", "p", "d", "swlpd", "!swlpd", "sw", "lpd"] {
            assert!(validate_perm_bits(s).is_ok(), "should accept {s}");
        }
    }

    #[test]
    fn parser_rejects_unknown_bits() {
        for s in ["x", "swx", "S", "1"] {
            assert!(validate_perm_bits(s).is_err(), "should reject {s}");
        }
    }

    #[test]
    fn parser_rejects_misplaced_bang() {
        assert!(validate_perm_bits("s!w").is_err());
        assert!(validate_perm_bits("sw!").is_err());
    }

    #[test]
    fn default_seed_has_user_dynamic_and_users_group() {
        let p = default_seed();
        // Per-user dynamic entry: full rights for $[user] under
        // /users/$[user]. Looking up by the literal entity string
        // `$[user]` exercises the same shape the resolver's
        // PMap::from_file parses.
        assert_eq!(
            lookup(&p, "/users/$[user]", "$[user]").map(|s| s.as_str()),
            Some("swlpd"),
        );
        // Read+write for the users group at /users — no `p`/`d`,
        // since publishing under someone else's prefix isn't part of
        // the "playground" contract.
        assert_eq!(
            lookup(&p, "/users", "users").map(|s| s.as_str()),
            Some("swl"),
        );
    }

    #[test]
    fn default_seed_validates_through_resolver_loader() {
        // The dynamic-entry shape (`$[user]` ending, `$[user]`
        // entity) has strict rules in PMap::from_file. Catch any
        // future drift in `default_seed` here rather than at first
        // resolver-start.
        use netidx::resolver_server::config;
        let p = default_seed();
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
        config::Config::from_file(file)
            .expect("default_seed must pass PMap::from_file");
    }

    #[test]
    fn add_remove_round_trip() {
        let mut p = empty();
        add_entry(&mut p, "/foo", "alice", "swlpd").unwrap();
        add_entry(&mut p, "/foo", "bob", "sl").unwrap();
        add_entry(&mut p, "/bar", "alice", "p").unwrap();

        assert_eq!(lookup(&p, "/foo", "alice").map(|s| s.as_str()), Some("swlpd"));
        assert_eq!(lookup(&p, "/foo", "bob").map(|s| s.as_str()), Some("sl"));
        assert_eq!(lookup(&p, "/bar", "alice").map(|s| s.as_str()), Some("p"));

        remove_entry(&mut p, "/foo", "alice");
        assert!(lookup(&p, "/foo", "alice").is_none());
        assert!(lookup(&p, "/foo", "bob").is_some());

        // Removing the last entity under a path drops the path key too.
        remove_entry(&mut p, "/foo", "bob");
        assert!(p.0.get("/foo").is_none());
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
