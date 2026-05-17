//! Engine-side wrapper for the id-map JSON file.
//!
//! Re-exports [`netidx_id_map::file`] and adds the load / save /
//! editor helpers the CLI calls. Editor helpers operate on the
//! in-memory `IdMap`; the CLI then writes back via [`save`].
//!
//! Path discovery mirrors the other configs: the canonical user
//! location is `${dirs::config_dir}/netidx/id-map.json` and the
//! corresponding socket is `${dirs::config_dir}/netidx/id-map.sock`.

use crate::atomic;
use anyhow::{Context, Result};
use arcstr::ArcStr;
use std::path::{Path, PathBuf};

pub use netidx_id_map::file::{
    Group, GroupBuilder, IdMap, IdMapBuilder, Identity, IdentityBuilder, Query,
    parse_bytes,
};

/// Canonical user path for the id-map JSON
/// (`${dirs::config_dir}/netidx/id-map.json`). No existence check.
pub fn user_id_map_path() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("id-map.json");
    Ok(p)
}

/// Canonical user path for the id-map socket
/// (`${dirs::config_dir}/netidx/id-map.sock`). No existence check.
pub fn user_id_map_socket() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("id-map.sock");
    Ok(p)
}

/// Load an id-map config from disk. Validates structurally before
/// returning — missing-group references surface here, not at the
/// first lookup.
pub fn load<P: AsRef<Path>>(path: P) -> Result<IdMap> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading id-map {path:?}"))?;
    parse_bytes(&bytes)
}

/// Load from the user default path.
pub fn load_default() -> Result<IdMap> {
    load(user_id_map_path()?)
}

/// Validate, then atomically save at mode 0600.
///
/// The id-map JSON is the policy file mapping netidx names to unix
/// uids — anyone who can read it knows the full who-becomes-whom
/// table. We treat it like a credential rather than a config: same
/// mode (0o600) as the daemon's default socket so the read surface
/// is the same for both.
pub fn save<P: AsRef<Path>>(path: P, map: &IdMap) -> Result<()> {
    map.validate().context("id-map structural validation")?;
    let bytes = serde_json::to_vec_pretty(map).context("serialize id-map JSON")?;
    atomic::write_atomic(path.as_ref(), &bytes, 0o600)
}

/// Save to the user default path.
pub fn save_default(map: &IdMap) -> Result<()> {
    save(user_id_map_path()?, map)
}

/// An empty starter map. Useful for `netidx conf id-map init` (which
/// writes this if the file doesn't exist yet).
pub fn empty() -> IdMap {
    IdMap::default()
}

// ---- editor helpers ----------------------------------------------------

/// Insert or update a group. Returns the previous gid for `name`,
/// if any. The group becomes immediately available as a
/// primary/secondary target for identities.
pub fn upsert_group(map: &mut IdMap, name: &str, gid: u32) -> Option<u32> {
    let prev = map.groups.insert(ArcStr::from(name), Group { gid });
    prev.map(|g| g.gid)
}

/// Remove a group. Fails if any identity still references it (we
/// can't dangle a primary or secondary group reference without
/// breaking validation).
pub fn remove_group(map: &mut IdMap, name: &str) -> Result<()> {
    let dependents: Vec<&str> = map
        .identities
        .iter()
        .filter_map(|(ident, rec)| {
            if rec.primary_group.as_str() == name
                || rec.groups.iter().any(|g| g.as_str() == name)
            {
                Some(ident.as_str())
            } else {
                None
            }
        })
        .collect();
    if !dependents.is_empty() {
        bail!(
            "cannot remove group {name:?}: still referenced by identities {dependents:?}"
        );
    }
    if map.groups.remove(name).is_none() {
        bail!("no such group {name:?}");
    }
    Ok(())
}

/// Insert or update an identity. The primary group must already exist
/// in the groups table; secondary groups likewise.
pub fn upsert_identity(
    map: &mut IdMap,
    name: &str,
    uid: u32,
    primary_group: &str,
    groups: &[&str],
) -> Result<Option<Identity>> {
    if !map.groups.contains_key(primary_group) {
        bail!("primary_group {primary_group:?} is not in the groups table");
    }
    for g in groups {
        if !map.groups.contains_key(*g) {
            bail!("group {g:?} is not in the groups table");
        }
    }
    let ident = Identity {
        uid,
        primary_group: ArcStr::from(primary_group),
        groups: groups.iter().map(|g| ArcStr::from(*g)).collect(),
    };
    Ok(map.identities.insert(ArcStr::from(name), ident))
}

/// Remove an identity. Returns the prior record if one existed.
pub fn remove_identity(map: &mut IdMap, name: &str) -> Option<Identity> {
    map.identities.remove(name)
}

/// Add `group` to `name`'s secondary group list. No-op if already
/// present. Fails if either is unknown.
pub fn add_group_member(map: &mut IdMap, name: &str, group: &str) -> Result<()> {
    if !map.groups.contains_key(group) {
        bail!("no such group {group:?}");
    }
    let ident = map
        .identities
        .get_mut(name)
        .ok_or_else(|| anyhow!("no such identity {name:?}"))?;
    if ident.primary_group.as_str() == group {
        return Ok(());
    }
    if ident.groups.iter().any(|g| g.as_str() == group) {
        return Ok(());
    }
    ident.groups.push(ArcStr::from(group));
    Ok(())
}

/// Drop `group` from `name`'s secondary group list. No-op if absent.
/// Refuses to remove the primary group — change that via
/// [`upsert_identity`] instead.
pub fn remove_group_member(
    map: &mut IdMap,
    name: &str,
    group: &str,
) -> Result<()> {
    let ident = map
        .identities
        .get_mut(name)
        .ok_or_else(|| anyhow!("no such identity {name:?}"))?;
    if ident.primary_group.as_str() == group {
        bail!(
            "cannot remove {group:?} from {name:?} via remove_group_member: it is the primary group"
        );
    }
    ident.groups.retain(|g| g.as_str() != group);
    Ok(())
}

/// Set the defaults returned for unknown queries.
pub fn set_defaults(map: &mut IdMap, default_uid: u32, default_gid: u32) {
    map.default_uid = default_uid;
    map.default_gid = default_gid;
}

/// Parse an octal unix mode string. Accepts `"600"`, `"0600"`, or
/// `"0o600"`. The whole input after stripping at most one `0o`/`0O`
/// prefix is interpreted in base 8 — no leading-zero stripping. That
/// matters: stripping leading zeros would turn `"00"` into `""` and
/// then `0`, silently producing mode 0 (no access) for a socket.
///
/// Rejects modes outside the 0o777 range so the caller can't
/// accidentally pass setuid/setgid/sticky bits to `set_permissions`.
pub fn parse_octal_mode(s: &str) -> Result<u32> {
    let body = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")).unwrap_or(s);
    if body.is_empty() {
        bail!("--socket-mode is empty");
    }
    let n = u32::from_str_radix(body, 8)
        .with_context(|| format!("--socket-mode {s:?} is not an octal mode"))?;
    if n > 0o777 {
        bail!("--socket-mode {s:?} has bits outside the 0o777 range");
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed() -> IdMap {
        let mut m = empty();
        upsert_group(&mut m, "users", 100);
        upsert_group(&mut m, "wheel", 10);
        upsert_identity(&mut m, "alice.example.com", 1000, "users", &["wheel"])
            .unwrap();
        m
    }

    #[test]
    fn upsert_then_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("id-map.json");
        save(&p, &seed()).unwrap();
        let back = load(&p).unwrap();
        assert_eq!(back, seed());
        // Mode must be 0o600 — see `save` docstring.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "id-map JSON must be 0o600");
        }
    }

    #[test]
    fn remove_group_blocks_when_in_use() {
        let mut m = seed();
        assert!(remove_group(&mut m, "users").is_err());
        // wheel is a secondary — same rule applies.
        assert!(remove_group(&mut m, "wheel").is_err());
    }

    #[test]
    fn remove_group_succeeds_when_unused() {
        let mut m = seed();
        upsert_group(&mut m, "spare", 200);
        remove_group(&mut m, "spare").unwrap();
        assert!(!m.groups.contains_key("spare"));
    }

    #[test]
    fn upsert_identity_validates_groups() {
        let mut m = seed();
        // Unknown primary.
        assert!(
            upsert_identity(&mut m, "bob.example.com", 1001, "ghost", &[]).is_err()
        );
        // Unknown secondary.
        assert!(
            upsert_identity(
                &mut m,
                "bob.example.com",
                1001,
                "users",
                &["ghost"]
            )
            .is_err()
        );
    }

    #[test]
    fn add_group_member_is_idempotent() {
        let mut m = seed();
        // Already in wheel — second add is a no-op.
        add_group_member(&mut m, "alice.example.com", "wheel").unwrap();
        let ident = m.lookup_by_name("alice.example.com").unwrap();
        assert_eq!(ident.groups.len(), 1);
    }

    #[test]
    fn remove_group_member_refuses_primary() {
        let mut m = seed();
        let err = remove_group_member(&mut m, "alice.example.com", "users")
            .unwrap_err();
        assert!(format!("{err:#}").contains("primary group"));
    }

    #[test]
    fn parse_octal_mode_accepts_common_forms() {
        assert_eq!(parse_octal_mode("600").unwrap(), 0o600);
        assert_eq!(parse_octal_mode("0600").unwrap(), 0o600);
        assert_eq!(parse_octal_mode("0o600").unwrap(), 0o600);
        assert_eq!(parse_octal_mode("0O600").unwrap(), 0o600);
        assert_eq!(parse_octal_mode("660").unwrap(), 0o660);
        assert_eq!(parse_octal_mode("777").unwrap(), 0o777);
        assert_eq!(parse_octal_mode("0").unwrap(), 0);
    }

    #[test]
    fn parse_octal_mode_rejects_garbage() {
        // Non-octal digit.
        assert!(parse_octal_mode("9").is_err());
        // Setuid bit, outside 0o777.
        assert!(parse_octal_mode("1000").is_err());
        // Non-numeric.
        assert!(parse_octal_mode("hello").is_err());
        // Empty.
        assert!(parse_octal_mode("").is_err());
        assert!(parse_octal_mode("0o").is_err());
        // Regression: this used to be silently parsed as mode 0 because
        // the old impl stripped all leading zeros, leaving an empty
        // string that fell through to "0".
        assert_eq!(
            parse_octal_mode("00").unwrap(),
            0,
            "explicit `00` is mode 0; the only way to get mode 0 (no access)"
        );
        assert_eq!(parse_octal_mode("0o0").unwrap(), 0);
    }

    #[test]
    fn save_rejects_invalid_map() {
        // Construct a map that fails validate(): identity references a
        // non-existent group. save() must catch this before writing.
        let mut m = IdMap::default();
        m.identities.insert(
            ArcStr::from("alice"),
            Identity {
                uid: 1000,
                primary_group: ArcStr::from("ghost"),
                groups: vec![],
            },
        );
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("id-map.json");
        assert!(save(&p, &m).is_err());
        assert!(!p.exists(), "no file should be written on validation failure");
    }
}
