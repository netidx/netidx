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
use netidx::resolver_server::config::PMap;
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
