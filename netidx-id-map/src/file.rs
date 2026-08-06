//! On-disk JSON schema for the id-map daemon.
//!
//! The schema is deliberately small: a flat `identities` table keyed by netidx
//! name (typically the TLS SubjectAltName DNS entry), a `groups` set, and a
//! `$default_group` fallback for queries that don't match any identity. The
//! leading `$` avoids colliding with any real identity or group name in JSON.
//!
//! **There are no uids or gids here, on purpose.** The resolver never reads
//! one: `Mapper::parse_output` walks the `/bin/id`-style line this daemon emits
//! pulling out the parenthesized *names* and discards every number, so what a
//! query answers with is a primary group name and a set of group names. The
//! daemon emits [`PLACEHOLDER_ID`] wherever the format demands a number.
//!
//! The one caller that ever wanted a real number is `Mapper::user(uid)`, the
//! local-auth peer-credentials shim, and this daemon is never configured on
//! that path — `IdMapMode` in `netidx-admin` is documented as meaningful only
//! for TLS and Kerberos. Storing numbers for it would be worse than useless:
//! they were allocated from 1000 with no relation to the host's `/etc/passwd`,
//! so reversing a real kernel uid through them could name the wrong identity.
//! With no table to consult, a numeric query falls through to the defaults and
//! that misconfiguration fails closed instead.
//!
//! The runtime daemon parses this once at startup, holds it in memory, and
//! answers queries from the resolver over a unix socket.
//!
//! These types are also what the admin plane moves between hosts, so they
//! implement `Pack` as well as serde. JSON is the on-disk form because an
//! operator reads and hand-edits the file; Pack is the wire form because
//! nothing on the wire is a document. Adding a field means appending it and
//! marking it `#[pack(default)]`, the same rule the admin protocol follows.

use anyhow::{Context, Result};
use arcstr::ArcStr;
use derive_builder::Builder;
use netidx_derive::Pack;
use std::collections::{BTreeMap, BTreeSet};

/// The number this daemon writes wherever the `/bin/id` line format demands
/// one. It means nothing — see the module docs.
///
/// A constant rather than something derived per identity: a number that varied
/// would invite a reader to depend on it. The nobody id is the right constant
/// because if anything ever does read one, that is the least-privileged answer
/// it could get.
pub const PLACEHOLDER_ID: u32 = 65534;

/// One identity row in the map. Maps a netidx name (the TLS
/// SubjectAltName, usually) to its group membership.
#[derive(Debug, Clone, Serialize, Deserialize, Builder, Pack, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Name of the primary group (must exist in the top-level
    /// `groups` set or validation fails).
    pub primary_group: ArcStr,
    /// Additional groups this identity belongs to. Each must also
    /// exist in the top-level `groups` set.
    #[serde(default)]
    #[builder(default)]
    pub groups: Vec<ArcStr>,
}

/// The full id-map file. Hand-edited as JSON; loaded and saved
/// atomically by the engine layer.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Builder, Pack, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IdMap {
    /// The group an identity that isn't in the map belongs to, if any.
    ///
    /// `None` — the default — means it belongs to no group at all, and the
    /// answer names the conventional `nogroup`, which no perms entry should
    /// match. Setting it grants every unknown identity that group, so it is a
    /// deliberate act.
    #[serde(default, rename = "$default_group")]
    #[builder(default)]
    pub default_group: Option<ArcStr>,
    /// Every group that exists.
    #[serde(default)]
    #[builder(default)]
    pub groups: BTreeSet<ArcStr>,
    /// Identity table: netidx name → identity record.
    #[serde(default)]
    #[builder(default)]
    pub identities: BTreeMap<ArcStr, Identity>,
}

/// Reject any name that would break the resolver's `/bin/id`-style
/// parser. The parser tokenises on `(`, `)`, `,`, and `=` and trims
/// whitespace at field boundaries, so any of those embedded in a
/// name would alias the field structure on the wire. We're strict
/// here on purpose: a legitimate name (TLS SubjectAltName DNS entry,
/// Kerberos principal) never needs these characters anyway.
///
/// Public so the runtime can apply the same rule to query strings
/// arriving on the socket — without that, an attacker who can mint
/// a TLS cert with a delimited SAN could craft a query that, after
/// being echoed into the `uid=N(<query>)` field of the
/// unknown-name fallback line, lets the resolver's parser read an
/// injected `gid=...(root)` token and assign the principal to an
/// arbitrary unix group.
pub fn check_name_chars(role: &str, s: &str) -> Result<()> {
    check_delimiter_chars(role, s)
}

fn check_delimiter_chars(role: &str, s: &str) -> Result<()> {
    if s.is_empty() {
        bail!("{role} must not be empty");
    }
    for c in s.chars() {
        if matches!(c, '(' | ')' | ',' | '=' | '\n' | '\r' | '\t' | ' ') {
            bail!(
                "{role} {s:?} contains forbidden character {c:?} \
                 (resolver `/bin/id`-style parser uses it as a delimiter)"
            );
        }
    }
    Ok(())
}

impl IdMap {
    /// Structural validation. Three invariants are enforced:
    ///
    /// 1. Every group referenced by an identity (primary or secondary), and
    ///    `$default_group` if set, must exist in the `groups` set.
    /// 2. No identity or group name may contain a character that the
    ///    resolver's `/bin/id`-style parser uses as a delimiter — `(`,
    ///    `)`, `,`, `=`, or any whitespace. The resolver's
    ///    `Mapper::parse_output` (`netidx/src/os/unix.rs`) extracts
    ///    parenthesised tokens byte-for-byte; a group named e.g.
    ///    `wheel) gid=0(root` would inject extra entries into the
    ///    response and let the named identity claim arbitrary unix
    ///    groups on the resolver.
    /// 3. No identity name may parse as a u32. The wire protocol
    ///    distinguishes uid-vs-name queries via `Query::parse`, so an
    ///    identity named `"1000"` is unreachable by name — a query
    ///    for `"1000"` always goes through the uid path. Catching
    ///    this at save time prevents silent misrouting.
    ///
    /// There used to be a fourth, that no two identities share a uid. It went
    /// with the uids: see the module docs.
    ///
    /// Run at load + before save; the daemon also runs it before
    /// swapping a reloaded map into the live slot.
    pub fn validate(&self) -> Result<()> {
        for name in self.identities.keys() {
            check_delimiter_chars("identity name", name.as_str())?;
            if name.parse::<u32>().is_ok() {
                bail!(
                    "identity name {:?} parses as a u32; the socket wire \
                     protocol would treat a query for it as a uid lookup, \
                     making the row unreachable by name",
                    name.as_str(),
                );
            }
        }
        for name in &self.groups {
            check_delimiter_chars("group name", name.as_str())?;
        }
        if let Some(g) = &self.default_group
            && !self.groups.contains(g)
        {
            bail!("$default_group {:?} is not in the groups set", g.as_str());
        }
        for (name, ident) in &self.identities {
            check_delimiter_chars(
                "identity primary_group",
                ident.primary_group.as_str(),
            )?;
            for g in &ident.groups {
                check_delimiter_chars("identity secondary group", g.as_str())?;
            }
            if !self.groups.contains(&ident.primary_group) {
                bail!(
                    "identity {:?}: primary_group {:?} is not in the groups set",
                    name.as_str(),
                    ident.primary_group.as_str(),
                );
            }
            for g in &ident.groups {
                if !self.groups.contains(g) {
                    bail!(
                        "identity {:?}: group {:?} is not in the groups set",
                        name.as_str(),
                        g.as_str(),
                    );
                }
            }
        }
        Ok(())
    }

    /// Look up an identity by name. Returns `None` for queries that
    /// don't match.
    pub fn lookup_by_name(&self, name: &str) -> Option<&Identity> {
        self.identities.get(name)
    }

    /// Format the `/bin/id`-style response for a name query. The
    /// resolver's existing parser only reads the parenthesized values
    /// after each `<key>=` so the line is deliberately simple.
    ///
    /// Falls back to `$default_group` when the name is unknown, mirroring how
    /// `/bin/id` would respond for a non-existent user.
    pub fn format_id_line_for_name(&self, query: &str) -> String {
        match self.lookup_by_name(query) {
            Some(ident) => self.format_id_line(query, ident),
            None => self.format_default_id_line(query),
        }
    }

    /// Format the `/bin/id`-style response for a uid query.
    ///
    /// Always the defaults: this map holds no uids, so there is nothing to
    /// reverse. The only caller that asks is the resolver's local-auth
    /// peer-credentials shim, which this daemon is not meant to serve — see the
    /// module docs. Answering with the defaults rather than a guess is what
    /// makes that misconfiguration fail closed.
    pub fn format_id_line_for_uid(&self, uid: u32) -> String {
        // The query echoed back as the name token, as for any unknown name.
        // The resolver reads only the parsed token, so this is honest about
        // having found nothing.
        self.format_default_id_line(&uid.to_string())
    }

    /// Defaults-only line. Names `$default_group` when one is set, so perms
    /// keyed on that group still match — otherwise emits the literal
    /// `(nogroup)` token, which the resolver treats as a distinct group with
    /// no perms.
    fn format_default_id_line(&self, name_token: &str) -> String {
        let group = self.default_group.as_deref().unwrap_or("nogroup");
        format!(
            "uid={id}({name_token}) gid={id}({group}) groups={id}({group})\n",
            id = PLACEHOLDER_ID,
        )
    }

    fn format_id_line(&self, name: &str, ident: &Identity) -> String {
        let id = PLACEHOLDER_ID;
        let mut out = format!(
            "uid={id}({name}) gid={id}({pg})",
            pg = ident.primary_group.as_str(),
        );
        // The resolver parses "groups=" by scanning parenthesized
        // tokens — the primary group must appear here too so the
        // membership set is complete (matching /bin/id's behavior).
        let mut groups_field =
            format!(" groups={id}({pg})", pg = ident.primary_group.as_str());
        for g in &ident.groups {
            groups_field.push_str(&format!(",{id}({name})", name = g.as_str()));
        }
        out.push_str(&groups_field);
        out.push('\n');
        out
    }
}

/// Decide whether a query line is a numeric uid or a name. The
/// existing socket protocol distinguishes via `parse::<u32>()` — we
/// mirror that exactly so the daemon answers identically to a remote
/// `/bin/id` invocation.
pub enum Query<'a> {
    Uid(u32),
    Name(&'a str),
}

impl<'a> Query<'a> {
    pub fn parse(s: &'a str) -> Self {
        match s.parse::<u32>() {
            Ok(u) => Query::Uid(u),
            Err(_) => Query::Name(s),
        }
    }
}

/// Parse an in-memory JSON byte slice into an `IdMap` and run
/// structural validation. Used by both the daemon and the engine
/// layer.
pub fn parse_bytes(bytes: &[u8]) -> Result<IdMap> {
    let cfg: IdMap = serde_json::from_slice(bytes).context("parsing id-map JSON")?;
    cfg.validate().context("id-map structural validation")?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(primary: &str, groups: &[&str]) -> Identity {
        Identity {
            primary_group: ArcStr::from(primary),
            groups: groups.iter().map(|g| ArcStr::from(*g)).collect(),
        }
    }

    fn sample() -> IdMap {
        let groups = ["users", "wheel"].into_iter().map(ArcStr::from).collect();
        let mut identities = BTreeMap::new();
        identities.insert(ArcStr::from("alice.example.com"), ident("users", &["wheel"]));
        IdMap { default_group: None, groups, identities }
    }

    #[test]
    fn validate_accepts_well_formed() {
        sample().validate().unwrap();
    }

    #[test]
    fn validate_rejects_missing_primary() {
        let mut m = sample();
        m.identities.get_mut("alice.example.com").unwrap().primary_group =
            ArcStr::from("nope");
        assert!(m.validate().is_err());
    }

    #[test]
    fn validate_rejects_close_paren_in_identity_name() {
        // The attack: a name containing `)` could close the resolver's
        // `uid=N(name)` field early and inject extra tokens. The
        // resolver's parser is byte-faithful; we have to reject at
        // save time.
        let mut m = sample();
        m.identities.insert(ArcStr::from("alice) gid=0(root"), ident("users", &[]));
        let err = m.validate().unwrap_err();
        assert!(format!("{err:#}").contains("forbidden character"));
    }

    #[test]
    fn validate_rejects_comma_in_group_name() {
        let mut m = sample();
        m.groups.insert(ArcStr::from("wheel,root"));
        let err = m.validate().unwrap_err();
        assert!(format!("{err:#}").contains("forbidden character"));
    }

    #[test]
    fn validate_rejects_paren_in_primary_group_reference() {
        let mut m = sample();
        // Have to register the bogus name so the missing-group check
        // doesn't fire first; the delimiter check runs ahead of it.
        m.identities.insert(ArcStr::from("bob.example.com"), ident("ev(il", &[]));
        let err = m.validate().unwrap_err();
        assert!(format!("{err:#}").contains("forbidden character"));
    }

    #[test]
    fn validate_rejects_numeric_identity_names() {
        // `Query::parse` treats a u32-parseable line as a uid. An
        // identity named `"1000"` would never be reached via name
        // lookup — every query for it would route through the uid
        // path. Catch at save time.
        let mut m = sample();
        m.identities.insert(ArcStr::from("1000"), ident("users", &[]));
        let err = m.validate().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("parses as a u32"),
            "expected numeric-name error, got: {msg}",
        );
    }

    #[test]
    fn validate_rejects_a_default_group_that_does_not_exist() {
        let mut m = sample();
        m.default_group = Some(ArcStr::from("dragons"));
        let err = m.validate().unwrap_err();
        assert!(format!("{err:#}").contains("$default_group"));
        m.default_group = Some(ArcStr::from("users"));
        m.validate().unwrap();
    }

    #[test]
    fn validate_rejects_empty_names() {
        let mut m = sample();
        m.identities.insert(ArcStr::from(""), ident("users", &[]));
        assert!(m.validate().is_err());
    }

    #[test]
    fn validate_rejects_missing_secondary() {
        let mut m = sample();
        m.identities
            .get_mut("alice.example.com")
            .unwrap()
            .groups
            .push(ArcStr::from("dragons"));
        assert!(m.validate().is_err());
    }

    #[test]
    fn format_id_line_matches_bin_id_shape() {
        // Format must be parseable by the resolver's existing
        // `Mapper::parse_output` (the parser looks for `gid=` and
        // `groups=` and reads parenthesized names after each).
        let m = sample();
        let s = m.format_id_line_for_name("alice.example.com");
        assert!(s.contains("uid=65534(alice.example.com)"));
        assert!(s.contains("gid=65534(users)"));
        // Primary group appears first in `groups=`, then secondaries.
        assert!(s.contains("groups=65534(users),65534(wheel)"));
        assert!(s.ends_with('\n'));
    }

    /// The names are the whole answer, and they must survive the resolver's
    /// parser unchanged now that every number beside them is the same
    /// constant.
    ///
    /// `extract` is `Mapper::parse_output` (`netidx/src/os/unix.rs:146`)
    /// reproduced faithfully, including the part that surprises: it scans from
    /// the key to the *end of the line*, so asking for `gid=` also sweeps up
    /// everything in `groups=`. The resolver takes element 0 and discards the
    /// rest, which is why the primary group has to come first.
    #[test]
    fn the_names_are_what_a_reader_gets_back() {
        let extract = |line: &str, key: &str| -> Vec<String> {
            let mut out = Vec::new();
            let mut s = &line[line.find(key).expect("key present")..];
            while let Some(op) = s.find('(') {
                let cp = s.find(')').expect("balanced");
                out.push(s[op + 1..cp].to_string());
                s = &s[cp + 1..];
            }
            out
        };
        let m = sample();
        let s = m.format_id_line_for_name("alice.example.com");
        assert_eq!(extract(&s, "gid=")[0], "users", "the primary group comes first");
        assert_eq!(extract(&s, "groups="), vec!["users", "wheel"]);
        // Unknown identities land in no group at all, so nothing keyed on a
        // group name can match them.
        let s = m.format_id_line_for_name("ghost.example.com");
        assert_eq!(extract(&s, "gid=")[0], "nogroup");
    }

    /// A uid query has nothing to reverse — this map holds no uids — so it
    /// answers with the defaults rather than guessing at an identity. That is
    /// what makes `auth: Local` with `id_map_type: Socket` fail closed.
    #[test]
    fn a_uid_query_never_names_an_identity() {
        let m = sample();
        let s = m.format_id_line_for_uid(1000);
        assert!(!s.contains("alice"), "a uid must not resolve to a name: {s}");
        assert!(s.contains("uid=65534(1000)"));
        assert!(s.contains("gid=65534(nogroup)"));
    }

    #[test]
    fn unknown_name_falls_back_to_defaults() {
        // No `$default_group`, so the fallback emits the literal `nogroup`
        // token, which the resolver treats as a group no perms entry matches.
        let m = sample();
        let s = m.format_id_line_for_name("nobody.example.com");
        assert!(s.contains("uid=65534(nobody.example.com)"));
        assert!(s.contains("gid=65534(nogroup)"));
    }

    #[test]
    fn unknown_name_uses_the_default_group_when_one_is_set() {
        // Setting `$default_group` grants every unknown identity that group,
        // so the fallback line has to name it — otherwise perms keyed on it
        // silently don't match.
        let mut m = sample();
        m.default_group = Some(ArcStr::from("users"));
        let s = m.format_id_line_for_name("nobody.example.com");
        assert!(s.contains("gid=65534(users)"), "got: {s}");
        assert!(s.contains("groups=65534(users)"));
    }

    #[test]
    fn json_round_trip() {
        let m = sample();
        let bytes = serde_json::to_vec_pretty(&m).unwrap();
        let back = parse_bytes(&bytes).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn pack_round_trip() {
        use netidx_core::pack::Pack;
        let m = sample();
        let mut buf = bytes::BytesMut::new();
        m.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), m.encoded_len());
        assert_eq!(IdMap::decode(&mut buf).unwrap(), m);
        assert!(buf.is_empty());
    }

    /// `$default_group` is easy to lose in a wire change — a map that decodes
    /// with `groups` and `identities` right but the default dropped looks
    /// correct until an unknown principal shows up and silently gets nothing.
    #[test]
    fn pack_carries_the_default_group() {
        use netidx_core::pack::Pack;
        let mut m = sample();
        m.default_group = Some(ArcStr::from("users"));
        let mut buf = bytes::BytesMut::new();
        m.encode(&mut buf).unwrap();
        let back = IdMap::decode(&mut buf).unwrap();
        assert_eq!(back.default_group.as_deref(), Some("users"));
    }

    #[test]
    fn query_parse() {
        match Query::parse("1000") {
            Query::Uid(u) => assert_eq!(u, 1000),
            _ => panic!("expected Uid"),
        }
        match Query::parse("alice.example.com") {
            Query::Name(n) => assert_eq!(n, "alice.example.com"),
            _ => panic!("expected Name"),
        }
    }

    #[test]
    fn default_keys_use_dollar_prefix() {
        // The `$default_group` rename is load-bearing — without the leading
        // `$` it would collide with a real group or identity name in tooling.
        // Catch a rename regression.
        let mut m = IdMap::default();
        m.default_group = Some(ArcStr::from("users"));
        let json = serde_json::to_value(&m).unwrap();
        assert!(json.get("$default_group").is_some());
        assert!(json.get("default_group").is_none());
    }
}
