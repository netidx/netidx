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
    check_name_chars, parse_bytes,
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
    let bytes =
        std::fs::read(path).with_context(|| format!("reading id-map {path:?}"))?;
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

/// Starter map with a single `users` group at gid 100 — matches the
/// conventional Linux `/etc/group` line for `users` and gives the
/// `ca sign` flow a sensible default group to assign new identities
/// to without forcing the operator to define one up front. The
/// `$default_gid` for unknown queries stays 65534 (nobody) so an
/// unrecognised principal doesn't accidentally land in `users`.
pub fn empty() -> IdMap {
    let mut m = IdMap::default();
    m.groups.insert(ArcStr::from("users"), Group { gid: 100 });
    m
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
pub fn remove_group_member(map: &mut IdMap, name: &str, group: &str) -> Result<()> {
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

/// Next free uid: `max(existing uids) + 1`, clamped to start at 1000.
/// Deterministic and stable, and clear of low system uids. uids are
/// local to each id-map — perms are keyed on *names* — so per-host
/// allocation needs no cross-host coordination.
pub fn next_uid(map: &IdMap) -> u32 {
    match map.identities.values().map(|i| i.uid).max() {
        Some(n) if n >= 1000 => n + 1,
        _ => 1000,
    }
}

/// Next free gid: same scheme as [`next_uid`], over the groups table.
pub fn next_gid(map: &IdMap) -> u32 {
    match map.groups.values().map(|g| g.gid).max() {
        Some(n) if n >= 1000 => n + 1,
        _ => 1000,
    }
}

/// Zero-touch registration: ensure every named group exists (creating
/// missing ones with allocated gids), then upsert `name` — an existing
/// identity keeps its uid (idempotent re-registration), a new one gets
/// [`next_uid`]. Returns the identity's uid.
///
/// Names are checked against the id-map delimiter rules up front so a
/// network caller gets a clear refusal instead of a save-time
/// validation failure.
pub fn register_identity(
    map: &mut IdMap,
    name: &str,
    primary_group: &str,
    groups: &[&str],
) -> Result<u32> {
    check_name_chars("identity name", name)?;
    if name.parse::<u32>().is_ok() {
        bail!("identity name {name:?} parses as a u32; it would be unreachable by name");
    }
    check_name_chars("group name", primary_group)?;
    for g in groups {
        check_name_chars("group name", g)?;
    }
    for g in std::iter::once(primary_group).chain(groups.iter().copied()) {
        if !map.groups.contains_key(g) {
            let gid = next_gid(map);
            upsert_group(map, g, gid);
        }
    }
    let uid = match map.identities.get(name) {
        Some(existing) => existing.uid,
        None => next_uid(map),
    };
    upsert_identity(map, name, uid, primary_group, groups)?;
    Ok(uid)
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
        upsert_identity(&mut m, "alice.example.com", 1000, "users", &["wheel"]).unwrap();
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
        assert!(upsert_identity(&mut m, "bob.example.com", 1001, "ghost", &[]).is_err());
        // Unknown secondary.
        assert!(
            upsert_identity(&mut m, "bob.example.com", 1001, "users", &["ghost"])
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
        let err = remove_group_member(&mut m, "alice.example.com", "users").unwrap_err();
        assert!(format!("{err:#}").contains("primary group"));
    }

    #[test]
    fn next_uid_and_gid_allocate_from_1000() {
        let m = IdMap::default();
        assert_eq!(next_uid(&m), 1000);
        assert_eq!(next_gid(&m), 1000);
        let m = seed(); // alice has uid 1000; users gid 100, wheel gid 10
        assert_eq!(next_uid(&m), 1001);
        assert_eq!(next_gid(&m), 1000, "low system gids don't shift the base");
    }

    #[test]
    fn register_identity_is_zero_touch_and_idempotent() {
        let mut m = IdMap::default();
        // Fresh map: groups don't exist yet — they're created with
        // allocated gids; the identity gets the first free uid.
        let uid =
            register_identity(&mut m, "eric.ryu-oh.org", "users", &["wheel"]).unwrap();
        assert_eq!(uid, 1000);
        assert!(m.groups.contains_key("users"));
        assert!(m.groups.contains_key("wheel"));
        assert_ne!(m.groups["users"].gid, m.groups["wheel"].gid);
        m.validate().unwrap();
        // Re-registration (a node re-joining) keeps the uid.
        let again = register_identity(&mut m, "eric.ryu-oh.org", "users", &[]).unwrap();
        assert_eq!(again, uid);
        // A second identity gets the next uid.
        let bob = register_identity(&mut m, "bob.ryu-oh.org", "users", &[]).unwrap();
        assert_eq!(bob, 1001);
    }

    #[test]
    fn register_identity_rejects_hostile_names() {
        let mut m = IdMap::default();
        // Delimiter injection and numeric names are refused up front —
        // these arrive over the network.
        assert!(register_identity(&mut m, "a) gid=0(root", "users", &[]).is_err());
        assert!(register_identity(&mut m, "1000", "users", &[]).is_err());
        assert!(register_identity(&mut m, "a.example.com", "ev(il", &[]).is_err());
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
            Identity { uid: 1000, primary_group: ArcStr::from("ghost"), groups: vec![] },
        );
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("id-map.json");
        assert!(save(&p, &m).is_err());
        assert!(!p.exists(), "no file should be written on validation failure");
    }
}
