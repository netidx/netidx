//! What each [`IdMapEdit`] means, and — the part that decides whether a
//! partial propagation can be repaired — which of them are allowed to find
//! nothing to do.

use crate::{
    admin_proto::IdMapEdit,
    id_map::{IdMap, apply_edit, empty},
};

fn seeded() -> IdMap {
    let mut map = empty();
    apply_edit(
        &mut map,
        &IdMapEdit::AddIdentity {
            san: "alice.example.com".into(),
            primary_group: "users".into(),
            groups: vec!["ops".into()],
        },
    )
    .unwrap();
    map
}

fn groups_of(map: &IdMap, san: &str) -> Vec<String> {
    map.identities
        .get(san)
        .map(|i| i.groups.iter().map(|g| g.to_string()).collect())
        .unwrap_or_default()
}

#[test]
fn add_identity_creates_its_groups_and_is_idempotent() {
    let mut map = empty();
    let edit = IdMapEdit::AddIdentity {
        san: "alice.example.com".into(),
        primary_group: "users".into(),
        groups: vec!["ops".into()],
    };
    assert!(apply_edit(&mut map, &edit).unwrap());
    assert!(map.groups.contains("users"));
    assert!(map.groups.contains("ops"));
    let before = map.identities.get("alice.example.com").unwrap().clone();
    // Re-applying the identical registration changes nothing.
    assert!(!apply_edit(&mut map, &edit).unwrap());
    assert_eq!(map.identities.get("alice.example.com").unwrap(), &before);
}

/// A registration that only adds a group is still a change, even though the
/// identity record it names may already be exactly right.
#[test]
fn add_identity_reports_a_new_group_as_a_change() {
    let mut map = seeded();
    let changed = apply_edit(
        &mut map,
        &IdMapEdit::AddIdentity {
            san: "alice.example.com".into(),
            primary_group: "users".into(),
            groups: vec!["ops".into(), "oncall".into()],
        },
    )
    .unwrap();
    assert!(changed);
    assert!(map.groups.contains("oncall"));
}

/// The whole point of reporting `changed` instead of erroring: re-running an
/// edit that already landed must be a safe no-op, because that is how a
/// resolver cluster left half-updated by a failed push gets repaired.
#[test]
fn removals_that_find_nothing_report_no_change_rather_than_failing() {
    let mut map = seeded();
    assert!(
        !apply_edit(&mut map, &IdMapEdit::RemoveIdentity { san: "nobody".into() })
            .unwrap()
    );
    assert!(
        !apply_edit(&mut map, &IdMapEdit::RemoveGroup { name: "nosuch".into() }).unwrap()
    );
    assert!(
        !apply_edit(
            &mut map,
            &IdMapEdit::RemoveMember { san: "nobody".into(), group: "ops".into() }
        )
        .unwrap()
    );
    assert!(
        !apply_edit(
            &mut map,
            &IdMapEdit::RemoveMember {
                san: "alice.example.com".into(),
                group: "notamember".into()
            }
        )
        .unwrap()
    );
    // And the second application of a removal that DID do something.
    let remove = IdMapEdit::RemoveIdentity { san: "alice.example.com".into() };
    assert!(apply_edit(&mut map, &remove).unwrap());
    assert!(!apply_edit(&mut map, &remove).unwrap());
}

#[test]
fn group_lifecycle() {
    let mut map = empty();
    let add = IdMapEdit::AddGroup { name: "wheel".into() };
    assert!(apply_edit(&mut map, &add).unwrap());
    // Already there — no change.
    assert!(!apply_edit(&mut map, &add).unwrap());
    assert!(map.groups.contains("wheel"));
    assert!(
        apply_edit(&mut map, &IdMapEdit::RemoveGroup { name: "wheel".into() }).unwrap()
    );
    assert!(!map.groups.contains("wheel"));
}

/// Removing a group out from under its members would leave the identities
/// pointing at nothing, so it fails. This is the distinction the applier
/// draws: "already true" is fine, "would corrupt the map" is not.
#[test]
fn a_group_in_use_cannot_be_removed() {
    let mut map = seeded();
    let err =
        apply_edit(&mut map, &IdMapEdit::RemoveGroup { name: "ops".into() }).unwrap_err();
    assert!(
        format!("{err:#}").contains("alice.example.com"),
        "the error should name who still holds it: {err:#}"
    );
    assert!(map.groups.contains("ops"));
}

#[test]
fn membership_add_and_remove() {
    let mut map = seeded();
    apply_edit(&mut map, &IdMapEdit::AddGroup { name: "oncall".into() }).unwrap();
    let add =
        IdMapEdit::AddMember { san: "alice.example.com".into(), group: "oncall".into() };
    assert!(apply_edit(&mut map, &add).unwrap());
    assert!(groups_of(&map, "alice.example.com").contains(&"oncall".to_string()));
    assert!(!apply_edit(&mut map, &add).unwrap());
    let remove = IdMapEdit::RemoveMember {
        san: "alice.example.com".into(),
        group: "oncall".into(),
    };
    assert!(apply_edit(&mut map, &remove).unwrap());
    assert!(!groups_of(&map, "alice.example.com").contains(&"oncall".to_string()));
}

/// Already a member via the *primary* group is not a change either — the
/// identity is in that group, which is what the operator asked for.
#[test]
fn adding_a_member_to_its_primary_group_is_not_a_change() {
    let mut map = seeded();
    assert!(
        !apply_edit(
            &mut map,
            &IdMapEdit::AddMember {
                san: "alice.example.com".into(),
                group: "users".into()
            }
        )
        .unwrap()
    );
}

/// Dropping the primary group would leave the identity without one, so it is
/// refused rather than silently reported as no-change.
#[test]
fn the_primary_group_cannot_be_removed_as_a_membership() {
    let mut map = seeded();
    let err = apply_edit(
        &mut map,
        &IdMapEdit::RemoveMember {
            san: "alice.example.com".into(),
            group: "users".into(),
        },
    )
    .unwrap_err();
    assert!(format!("{err:#}").contains("primary group"), "unexpected: {err:#}");
}

/// An edit naming an identity that isn't there can't be satisfied — that is a
/// mistake, not a state that is already true.
#[test]
fn adding_a_member_requires_the_identity_and_the_group_to_exist() {
    let mut map = seeded();
    assert!(
        apply_edit(
            &mut map,
            &IdMapEdit::AddMember { san: "ghost".into(), group: "ops".into() }
        )
        .is_err()
    );
    assert!(
        apply_edit(
            &mut map,
            &IdMapEdit::AddMember {
                san: "alice.example.com".into(),
                group: "nosuch".into()
            }
        )
        .is_err()
    );
}

/// The authorization inputs the CA reads off an edit. `groups` is what the
/// caller's `id_map_groups` must cover and `identity` what its `allowed_san`
/// must cover, so a variant that reported the wrong thing would silently
/// widen or narrow every id-map grant.
#[test]
fn an_edit_reports_the_names_it_is_authorized_against() {
    let add = IdMapEdit::AddIdentity {
        san: "alice.example.com".into(),
        primary_group: "users".into(),
        groups: vec!["ops".into()],
    };
    assert_eq!(add.groups(), vec!["users", "ops"]);
    assert_eq!(add.identity(), Some("alice.example.com"));

    // Removing an identity names no group: it is authorized by the identity
    // scope, and by the groups the applier finds on it.
    let rm = IdMapEdit::RemoveIdentity { san: "alice.example.com".into() };
    assert!(rm.groups().is_empty());
    assert_eq!(rm.identity(), Some("alice.example.com"));

    // Group operations name no identity.
    let g = IdMapEdit::AddGroup { name: "wheel".into() };
    assert_eq!(g.groups(), vec!["wheel"]);
    assert_eq!(g.identity(), None);

    let m =
        IdMapEdit::AddMember { san: "alice.example.com".into(), group: "wheel".into() };
    assert_eq!(m.groups(), vec!["wheel"]);
    assert_eq!(m.identity(), Some("alice.example.com"));
}
