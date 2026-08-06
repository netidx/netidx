//! On-disk JSON schema for the id-map daemon.
//!
//! The schema is deliberately small: a flat `identities` table keyed
//! by netidx name (typically the TLS SubjectAltName DNS entry), a
//! `groups` table mapping group name → gid, and two scalar fallbacks
//! (`$default_uid` / `$default_gid`) for queries that don't match any
//! identity. The leading `$` on the default keys avoids colliding
//! with any real identity / group name in JSON.
//!
//! The runtime daemon parses this once at startup, holds it in
//! memory, and answers queries from the resolver over a unix socket.
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
use std::collections::BTreeMap;

/// A group definition: just the numeric gid for now. Wrapping in a
/// struct gives us room to add per-group attributes later (description,
/// admin contacts, etc.) without breaking on-disk back-compat — JSON's
/// `deny_unknown_fields` is strict enough to catch typos but liberal
/// enough to let us add optional fields.
#[derive(Debug, Clone, Serialize, Deserialize, Builder, Pack, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub gid: u32,
}

/// One identity row in the map. Maps a netidx name (the TLS
/// SubjectAltName, usually) to a unix uid + group membership.
#[derive(Debug, Clone, Serialize, Deserialize, Builder, Pack, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub uid: u32,
    /// Name of the primary group (must exist in the top-level
    /// `groups` table or validation fails).
    pub primary_group: ArcStr,
    /// Additional groups this identity belongs to. Each must also
    /// exist in the top-level `groups` table.
    #[serde(default)]
    #[builder(default)]
    pub groups: Vec<ArcStr>,
}

/// The full id-map file. Hand-edited as JSON; loaded and saved
/// atomically by the engine layer.
#[derive(Debug, Clone, Serialize, Deserialize, Builder, Pack, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IdMap {
    /// Uid returned for queries that don't match any identity.
    /// Defaults to the nobody-uid (65534 on Linux) when omitted.
    #[serde(default = "default_default_uid", rename = "$default_uid")]
    #[builder(default = "default_default_uid()")]
    pub default_uid: u32,
    /// Gid returned for queries that don't match any identity.
    #[serde(default = "default_default_gid", rename = "$default_gid")]
    #[builder(default = "default_default_gid()")]
    pub default_gid: u32,
    /// Group table: group name → gid.
    #[serde(default)]
    #[builder(default)]
    pub groups: BTreeMap<ArcStr, Group>,
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

fn default_default_uid() -> u32 {
    65534
}

fn default_default_gid() -> u32 {
    65534
}

impl Default for IdMap {
    fn default() -> Self {
        Self {
            default_uid: default_default_uid(),
            default_gid: default_default_gid(),
            groups: BTreeMap::new(),
            identities: BTreeMap::new(),
        }
    }
}

impl IdMap {
    /// Structural validation. Four invariants are enforced:
    ///
    /// 1. Every group referenced by an identity (primary or secondary)
    ///    must exist in the `groups` table.
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
    /// 4. No two identities may share a uid. Reverse lookup via
    ///    `lookup_by_uid` returns the first BTreeMap match; duplicate
    ///    uids make the reverse mapping order-dependent. The
    ///    resolver's TLS path doesn't query by uid, but the
    ///    local-auth peer-credentials shim
    ///    (`netidx/src/os/unix.rs::Mapper::user`) does, so a
    ///    socket-mode local-auth deployment would silently pick one
    ///    of the colliding names.
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
        for name in self.groups.keys() {
            check_delimiter_chars("group name", name.as_str())?;
        }
        let mut seen_uids: BTreeMap<u32, &str> = BTreeMap::new();
        for (name, ident) in &self.identities {
            if let Some(prev) = seen_uids.insert(ident.uid, name.as_str()) {
                bail!(
                    "uid {} is shared by identities {:?} and {:?}; reverse \
                     lookups by uid would be order-dependent",
                    ident.uid,
                    prev,
                    name.as_str(),
                );
            }
            check_delimiter_chars(
                "identity primary_group",
                ident.primary_group.as_str(),
            )?;
            for g in &ident.groups {
                check_delimiter_chars("identity secondary group", g.as_str())?;
            }
            if !self.groups.contains_key(&ident.primary_group) {
                bail!(
                    "identity {:?}: primary_group {:?} is not in the groups table",
                    name.as_str(),
                    ident.primary_group.as_str(),
                );
            }
            for g in &ident.groups {
                if !self.groups.contains_key(g) {
                    bail!(
                        "identity {:?}: group {:?} is not in the groups table",
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

    /// Reverse lookup by uid. Returns the first matching name —
    /// duplicate uids are caller error.
    pub fn lookup_by_uid(&self, uid: u32) -> Option<(&ArcStr, &Identity)> {
        self.identities.iter().find(|(_, i)| i.uid == uid)
    }

    /// Format the `/bin/id`-style response for a name query. The
    /// resolver's existing parser only reads the parenthesized values
    /// after each `<key>=` so the line is deliberately simple.
    ///
    /// Falls back to the `$default_uid` / `$default_gid` defaults
    /// when the name is unknown, mirroring how `/bin/id` would
    /// respond for a non-existent user.
    pub fn format_id_line_for_name(&self, query: &str) -> String {
        match self.lookup_by_name(query) {
            Some(ident) => self.format_id_line(query, ident),
            None => self.format_default_id_line(query),
        }
    }

    /// Format the `/bin/id`-style response for a uid query.
    pub fn format_id_line_for_uid(&self, uid: u32) -> String {
        match self.lookup_by_uid(uid) {
            Some((name, ident)) => self.format_id_line(name.as_str(), ident),
            // Fall back as if the query were the literal uid string —
            // the resolver only uses the parsed name token anyway, and
            // this gives us a single defaults-rendering path.
            None => {
                let uid_str = uid.to_string();
                self.format_default_id_line(&uid_str)
            }
        }
    }

    /// Defaults-only line. Uses the real group name for `$default_gid`
    /// when it is registered in the `groups` table (so perms keyed on
    /// that group still match) — otherwise emits the literal
    /// `(nogroup)` token, which the resolver will treat as a distinct
    /// group with no perms.
    fn format_default_id_line(&self, name_token: &str) -> String {
        let (gid_name, gid) = self
            .groups
            .iter()
            .find(|(_, g)| g.gid == self.default_gid)
            .map(|(n, g)| (n.as_str(), g.gid))
            .unwrap_or(("nogroup", self.default_gid));
        format!(
            "uid={uid}({name_token}) gid={gid}({gid_name}) groups={gid}({gid_name})\n",
            uid = self.default_uid,
        )
    }

    fn format_id_line(&self, name: &str, ident: &Identity) -> String {
        let primary_gid = self
            .groups
            .get(&ident.primary_group)
            .map(|g| g.gid)
            .unwrap_or(self.default_gid);
        let mut out = format!(
            "uid={uid}({name}) gid={gid}({pg})",
            uid = ident.uid,
            name = name,
            gid = primary_gid,
            pg = ident.primary_group.as_str(),
        );
        // The resolver parses "groups=" by scanning parenthesized
        // tokens — the primary group must appear here too so the
        // membership set is complete (matching /bin/id's behavior).
        let mut groups_field =
            format!(" groups={primary_gid}({pg})", pg = ident.primary_group.as_str());
        for g in &ident.groups {
            let gid = self.groups.get(g).map(|x| x.gid).unwrap_or(self.default_gid);
            groups_field.push_str(&format!(",{gid}({name})", name = g.as_str()));
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

    fn sample() -> IdMap {
        let mut groups = BTreeMap::new();
        groups.insert(ArcStr::from("users"), Group { gid: 100 });
        groups.insert(ArcStr::from("wheel"), Group { gid: 10 });
        let mut identities = BTreeMap::new();
        identities.insert(
            ArcStr::from("alice.example.com"),
            Identity {
                uid: 1000,
                primary_group: ArcStr::from("users"),
                groups: vec![ArcStr::from("wheel")],
            },
        );
        IdMap { default_uid: 65534, default_gid: 65534, groups, identities }
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
        m.identities.insert(
            ArcStr::from("alice) gid=0(root"),
            Identity { uid: 1, primary_group: ArcStr::from("users"), groups: vec![] },
        );
        let err = m.validate().unwrap_err();
        assert!(format!("{err:#}").contains("forbidden character"));
    }

    #[test]
    fn validate_rejects_comma_in_group_name() {
        let mut m = sample();
        m.groups.insert(ArcStr::from("wheel,root"), Group { gid: 1 });
        let err = m.validate().unwrap_err();
        assert!(format!("{err:#}").contains("forbidden character"));
    }

    #[test]
    fn validate_rejects_paren_in_primary_group_reference() {
        let mut m = sample();
        // Have to register the bogus name so the missing-group check
        // doesn't fire first; the delimiter check runs ahead of it.
        m.identities.insert(
            ArcStr::from("bob.example.com"),
            Identity { uid: 2, primary_group: ArcStr::from("ev(il"), groups: vec![] },
        );
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
        m.identities.insert(
            ArcStr::from("1000"),
            Identity { uid: 2000, primary_group: ArcStr::from("users"), groups: vec![] },
        );
        let err = m.validate().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("parses as a u32"),
            "expected numeric-name error, got: {msg}",
        );
    }

    #[test]
    fn validate_rejects_duplicate_uids() {
        let mut m = sample();
        // alice.example.com already has uid 1000; add a second row
        // claiming the same uid.
        m.identities.insert(
            ArcStr::from("bob.example.com"),
            Identity { uid: 1000, primary_group: ArcStr::from("users"), groups: vec![] },
        );
        let err = m.validate().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("uid 1000 is shared"),
            "expected duplicate-uid error, got: {msg}",
        );
    }

    #[test]
    fn validate_rejects_empty_names() {
        let mut m = sample();
        m.identities.insert(
            ArcStr::from(""),
            Identity { uid: 1, primary_group: ArcStr::from("users"), groups: vec![] },
        );
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
        assert!(s.contains("uid=1000(alice.example.com)"));
        assert!(s.contains("gid=100(users)"));
        // Primary group appears first in `groups=`, then secondaries.
        assert!(s.contains("groups=100(users),10(wheel)"));
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn format_id_line_for_uid_round_trips() {
        let m = sample();
        let s = m.format_id_line_for_uid(1000);
        assert!(s.contains("uid=1000(alice.example.com)"));
    }

    #[test]
    fn unknown_name_falls_back_to_defaults() {
        // Defaults point at gid 65534 which is NOT in the groups
        // table, so the fallback emits the literal `nogroup` token.
        let m = sample();
        let s = m.format_id_line_for_name("nobody.example.com");
        assert!(s.contains("uid=65534(nobody.example.com)"));
        assert!(s.contains("gid=65534(nogroup)"));
    }

    #[test]
    fn unknown_name_uses_real_group_name_when_default_gid_registered() {
        // Operator set $default_gid = 100, which IS in the groups
        // table as "users". The fallback line should say
        // `gid=100(users)`, not `gid=100(nogroup)` — otherwise perms
        // keyed on the real group name silently don't match.
        let mut m = sample();
        m.default_gid = 100; // matches the "users" gid in sample()
        let s = m.format_id_line_for_name("nobody.example.com");
        assert!(
            s.contains("gid=100(users)"),
            "fallback should use the real group name; got: {s}"
        );
        assert!(s.contains("groups=100(users)"));
    }

    #[test]
    fn unknown_uid_falls_back_to_defaults_with_group_name() {
        let mut m = sample();
        m.default_gid = 100;
        let s = m.format_id_line_for_uid(99999);
        assert!(s.contains("uid=65534(99999)"));
        assert!(s.contains("gid=100(users)"));
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

    /// The defaults are two of the four fields and are easy to lose in a
    /// wire change — a map that decodes with `groups` and `identities` right
    /// but `$default_uid` reset to nobody looks correct until an unknown
    /// principal shows up.
    #[test]
    fn pack_carries_the_defaults() {
        use netidx_core::pack::Pack;
        let mut m = sample();
        m.default_uid = 1;
        m.default_gid = 2;
        let mut buf = bytes::BytesMut::new();
        m.encode(&mut buf).unwrap();
        let back = IdMap::decode(&mut buf).unwrap();
        assert_eq!((back.default_uid, back.default_gid), (1, 2));
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
        // The `$default_uid` / `$default_gid` rename is load-bearing —
        // without the leading `$` they'd collide with potential group
        // or identity names in tooling. Catch a rename regression.
        let json = serde_json::to_value(&IdMap::default()).unwrap();
        assert!(json.get("$default_uid").is_some());
        assert!(json.get("$default_gid").is_some());
        assert!(json.get("default_uid").is_none());
    }
}
