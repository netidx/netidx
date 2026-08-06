//! The admin domain's authoritative id-map *shape*, held by the CA.
//!
//! Every id-map host holds its own map file. This is what they are all
//! supposed to agree about: which identities exist, which groups exist, and
//! who belongs to what. That is the whole content of an id-map — it holds no
//! numbers at all, because the resolver reads only names out of the answer.
//! See the schema docs on [`netidx_id_map::file::IdMap`].
//!
//! The CA writes this in the same step that authorizes an edit, so it records
//! what the admin domain is supposed to look like whether or not every host
//! was reachable at the time. A host that missed an edit is then just a host
//! behind the model's version, which is a thing that can be noticed and
//! repaired — see `design/id-map-convergence.md`.

use crate::{
    admin_proto::IdMapEdit,
    id_map::{IdMap, apply_edit, empty},
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// The shape of the admin domain's id-map, and how many changes have been
/// applied to it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdMapModel {
    /// Bumped once per edit that changed the shape. A host reports the
    /// version it last applied; anything lower is behind.
    ///
    /// Starts at 0, which also means "this model has never been written to"
    /// — no host can be behind version 0, so an empty model never triggers a
    /// reconcile. That matters: a CA whose model file is missing must not
    /// conclude that every host should be emptied.
    pub version: u64,
    /// The shape itself.
    ///
    /// Held as a real [`IdMap`] so that [`apply_edit`] — the single applier
    /// every host uses — decides what each operation means here exactly as it
    /// means there. A second implementation of the same semantics is precisely
    /// how a model comes to disagree with the hosts it claims to describe.
    shape: IdMap,
}

impl Default for IdMapModel {
    fn default() -> Self {
        Self { version: 0, shape: empty() }
    }
}

impl IdMapModel {
    /// Whether this model has ever been written to. A model at version 0
    /// describes nothing and must never be used to prune a host.
    pub fn established(&self) -> bool {
        self.version > 0
    }

    /// The shape, for diffing a host against.
    pub fn shape(&self) -> &IdMap {
        &self.shape
    }

    /// Apply `edit`, bumping the version if it changed anything.
    ///
    /// Returns whether it changed the model. A no-op edit does not advance the
    /// version — otherwise re-running a command to converge a lagging host
    /// would move the target every time and no host could ever catch up.
    ///
    /// Errors on an edit the shape rejects (a group still in use, a member
    /// added to an identity that isn't there). Because the CA applies this
    /// before it pushes anything, that check runs *once, centrally*, instead
    /// of being discovered separately by each host.
    pub fn apply(&mut self, edit: &IdMapEdit) -> Result<bool> {
        let changed = apply_edit(&mut self.shape, edit)?;
        if changed {
            self.version += 1;
        }
        Ok(changed)
    }

    /// The operations that bring `host` to this model — empty when it already
    /// agrees.
    ///
    /// Compares names and membership, which is everything a map holds.
    ///
    /// **Order is the whole difficulty.** Each step has to be applicable when
    /// it runs, against the applier's own invariants — a group cannot be
    /// removed while an identity still holds it, and an identity cannot name a
    /// group that does not exist yet. So: create groups, then settle
    /// identities, then drop memberships, then identities, then groups. Any
    /// other order produces a batch the host is right to refuse.
    ///
    /// Returns nothing for a model that is not [`established`](Self::established).
    /// A CA that has lost its model knows nothing, and "knows nothing" must
    /// never be read as "everything should be deleted".
    pub fn diff(&self, host: &IdMap) -> Vec<IdMapEdit> {
        if !self.established() {
            return Vec::new();
        }
        let mut out = Vec::new();
        // 1. Groups the host is missing, before anything can name them.
        for name in self.shape.groups.iter() {
            if !host.groups.contains(name) {
                out.push(IdMapEdit::AddGroup { name: name.to_string() });
            }
        }
        // 2. Identities the host is missing or holds differently.
        //    AddIdentity is an upsert that creates any group it names, so it
        //    settles a wrong primary group and any missing membership at once.
        for (san, want) in &self.shape.identities {
            let matches = host.identities.get(san).is_some_and(|have| {
                have.primary_group == want.primary_group && have.groups == want.groups
            });
            if !matches {
                out.push(IdMapEdit::AddIdentity {
                    san: san.to_string(),
                    primary_group: want.primary_group.to_string(),
                    groups: want.groups.iter().map(|g| g.to_string()).collect(),
                });
            }
        }
        // 3. Memberships the host has that the model does not — the removals
        //    step 2 cannot express, since AddIdentity only adds.
        for (san, have) in &host.identities {
            let Some(want) = self.shape.identities.get(san) else { continue };
            for group in &have.groups {
                if !want.groups.contains(group) {
                    out.push(IdMapEdit::RemoveMember {
                        san: san.to_string(),
                        group: group.to_string(),
                    });
                }
            }
        }
        // 4. Identities the model does not have, before the groups they hold.
        for san in host.identities.keys() {
            if !self.shape.identities.contains_key(san) {
                out.push(IdMapEdit::RemoveIdentity { san: san.to_string() });
            }
        }
        // 5. Groups the model does not have, once nothing references them.
        for name in host.groups.iter() {
            if !self.shape.groups.contains(name) {
                out.push(IdMapEdit::RemoveGroup { name: name.to_string() });
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(model: &mut IdMapModel, edit: IdMapEdit) -> bool {
        model.apply(&edit).unwrap()
    }

    /// The property the whole design rests on: applying a sequence of edits to
    /// the model produces the same shape as applying them to a host's map. If
    /// these can differ, the model describes something no host is.
    #[test]
    fn the_model_is_what_the_same_edits_produce_on_a_host() {
        let edits = [
            IdMapEdit::AddGroup { name: "wheel".into() },
            IdMapEdit::AddIdentity {
                san: "alice.example.com".into(),
                primary_group: "users".into(),
                groups: vec!["ops".into()],
            },
            IdMapEdit::AddIdentity {
                san: "bob.example.com".into(),
                primary_group: "users".into(),
                groups: vec![],
            },
            IdMapEdit::AddMember { san: "bob.example.com".into(), group: "wheel".into() },
            IdMapEdit::RemoveMember {
                san: "alice.example.com".into(),
                group: "ops".into(),
            },
            IdMapEdit::RemoveGroup { name: "ops".into() },
            IdMapEdit::RemoveIdentity { san: "bob.example.com".into() },
        ];
        let mut model = IdMapModel::default();
        let mut host = empty();
        for e in &edits {
            model.apply(e).unwrap();
            apply_edit(&mut host, e).unwrap();
        }
        // Names and membership must agree — that is all a map holds.
        assert_eq!(
            model.shape().groups.iter().collect::<Vec<_>>(),
            host.groups.iter().collect::<Vec<_>>()
        );
        for (name, ident) in &host.identities {
            let m = model.shape().identities.get(name).expect("identity in model");
            assert_eq!(m.primary_group, ident.primary_group);
            assert_eq!(m.groups, ident.groups);
        }
        assert_eq!(model.shape().identities.len(), host.identities.len());
    }

    #[test]
    fn only_a_real_change_advances_the_version() {
        let mut model = IdMapModel::default();
        assert!(!model.established());
        assert!(edit(&mut model, IdMapEdit::AddGroup { name: "wheel".into() }));
        assert_eq!(model.version, 1);
        assert!(model.established());
        // Already there.
        assert!(!edit(&mut model, IdMapEdit::AddGroup { name: "wheel".into() }));
        assert_eq!(model.version, 1);
        // Nothing to remove.
        assert!(!edit(&mut model, IdMapEdit::RemoveIdentity { san: "ghost".into() }));
        assert_eq!(model.version, 1);
    }

    /// The model starts from the same [`empty`] every host does, seeded
    /// `users` group and all. That is load-bearing for the reconciler: a model
    /// that started from a *truly* empty map would see `users` on every host,
    /// conclude it was not supposed to be there, and prune the one group the
    /// installers grant permissions to.
    #[test]
    fn the_model_and_a_host_start_from_the_same_base() {
        let model = IdMapModel::default();
        let host = empty();
        assert!(host.groups.contains("users"), "the base map seeds `users`");
        assert_eq!(
            model.shape().groups.iter().collect::<Vec<_>>(),
            host.groups.iter().collect::<Vec<_>>()
        );
        assert!(model.shape().identities.is_empty());
    }

    /// The CA refuses centrally what every host would otherwise refuse
    /// separately — and refuses it before anything is pushed.
    #[test]
    fn an_impossible_edit_is_refused_before_it_is_propagated() {
        let mut model = IdMapModel::default();
        edit(
            &mut model,
            IdMapEdit::AddIdentity {
                san: "alice.example.com".into(),
                primary_group: "users".into(),
                groups: vec![],
            },
        );
        let before = model.clone();
        assert!(model.apply(&IdMapEdit::RemoveGroup { name: "users".into() }).is_err());
        assert_eq!(model, before, "a refused edit must not have moved the model");
    }

    #[test]
    fn round_trips_through_json() {
        let mut model = IdMapModel::default();
        edit(
            &mut model,
            IdMapEdit::AddIdentity {
                san: "alice.example.com".into(),
                primary_group: "users".into(),
                groups: vec!["ops".into()],
            },
        );
        let json = serde_json::to_string(&model).unwrap();
        let back: IdMapModel = serde_json::from_str(&json).unwrap();
        assert_eq!(model, back);
    }

    /// A diff is only correct if the host can actually apply it in the order
    /// given — every step has to satisfy the applier's invariants when it
    /// runs. Applying the diff and re-diffing is the strongest statement of
    /// that: it must converge, and it must converge in one pass.
    fn assert_converges(model: &IdMapModel, host: &mut IdMap) -> Vec<IdMapEdit> {
        let edits = model.diff(host);
        for e in &edits {
            apply_edit(host, e)
                .unwrap_or_else(|err| panic!("the host refused {e:?}: {err:#}"));
        }
        assert!(
            model.diff(host).is_empty(),
            "one pass must be enough; still to do: {:?}",
            model.diff(host)
        );
        edits
    }

    fn model_of(edits: &[IdMapEdit]) -> IdMapModel {
        let mut m = IdMapModel::default();
        for e in edits {
            m.apply(e).unwrap();
        }
        m
    }

    #[test]
    fn a_host_that_agrees_needs_nothing() {
        let edits = [IdMapEdit::AddIdentity {
            san: "alice.example.com".into(),
            primary_group: "users".into(),
            groups: vec!["ops".into()],
        }];
        let model = model_of(&edits);
        let mut host = empty();
        for e in &edits {
            apply_edit(&mut host, e).unwrap();
        }
        assert!(model.diff(&host).is_empty(), "no needless pushes");
    }

    /// The case that prompted all of this: a host that was down for a batch of
    /// edits, including the removals that are the unsafe direction to miss.
    #[test]
    fn a_host_that_missed_everything_catches_up_in_one_pass() {
        let model = model_of(&[
            IdMapEdit::AddGroup { name: "oncall".into() },
            IdMapEdit::AddIdentity {
                san: "alice.example.com".into(),
                primary_group: "users".into(),
                groups: vec!["oncall".into()],
            },
            IdMapEdit::AddIdentity {
                san: "carol.example.com".into(),
                primary_group: "users".into(),
                groups: vec![],
            },
        ]);
        // The host is at an older state: it has an identity since removed, a
        // membership since revoked, and a group since dropped.
        let mut host = empty();
        for e in [
            IdMapEdit::AddGroup { name: "legacy".into() },
            IdMapEdit::AddIdentity {
                san: "alice.example.com".into(),
                primary_group: "users".into(),
                groups: vec!["legacy".into()],
            },
            IdMapEdit::AddIdentity {
                san: "bob.example.com".into(),
                primary_group: "users".into(),
                groups: vec!["legacy".into()],
            },
        ] {
            apply_edit(&mut host, &e).unwrap();
        }
        assert_converges(&model, &mut host);
        // The revoked membership is gone, not merely absent from the others.
        assert!(
            !host
                .identities
                .get("alice.example.com")
                .unwrap()
                .groups
                .iter()
                .any(|g| g.as_str() == "legacy")
        );
        assert!(
            host.identities
                .get("alice.example.com")
                .unwrap()
                .groups
                .iter()
                .any(|g| g.as_str() == "oncall")
        );
        assert!(!host.identities.contains_key("bob.example.com"));
        assert!(!host.groups.contains("legacy"));
        assert!(host.identities.contains_key("carol.example.com"));
    }

    /// The ordering trap: a group can only be dropped once nothing holds it,
    /// and the identity holding it is itself being dropped in the same pass.
    #[test]
    fn a_group_is_dropped_after_the_identity_that_held_it() {
        let model = model_of(&[IdMapEdit::AddGroup { name: "keep".into() }]);
        let mut host = empty();
        for e in [
            IdMapEdit::AddGroup { name: "keep".into() },
            IdMapEdit::AddIdentity {
                san: "gone.example.com".into(),
                primary_group: "doomed".into(),
                groups: vec![],
            },
        ] {
            apply_edit(&mut host, &e).unwrap();
        }
        let edits = assert_converges(&model, &mut host);
        let rm_ident = edits
            .iter()
            .position(|e| matches!(e, IdMapEdit::RemoveIdentity { .. }))
            .expect("the identity is removed");
        let rm_group = edits
            .iter()
            .position(
                |e| matches!(e, IdMapEdit::RemoveGroup { name } if name == "doomed"),
            )
            .expect("its group is removed");
        assert!(rm_ident < rm_group, "the holder has to go first: {edits:?}");
        assert!(!host.groups.contains("doomed"));
    }

    /// The mirror trap: an identity cannot name a group that does not exist
    /// yet, so the group has to be created first.
    #[test]
    fn a_group_is_created_before_the_identity_that_names_it() {
        let model = model_of(&[IdMapEdit::AddIdentity {
            san: "alice.example.com".into(),
            primary_group: "newgroup".into(),
            groups: vec![],
        }]);
        let mut host = empty();
        let edits = assert_converges(&model, &mut host);
        let add_group = edits
            .iter()
            .position(|e| matches!(e, IdMapEdit::AddGroup { name } if name == "newgroup"))
            .expect("the group is created");
        let add_ident = edits
            .iter()
            .position(|e| matches!(e, IdMapEdit::AddIdentity { .. }))
            .expect("the identity is created");
        assert!(add_group < add_ident, "the group has to exist first: {edits:?}");
    }

    /// A host whose identity has the right groups but the wrong primary is
    /// corrected, not left alone — the primary group is what an unmatched
    /// permission entry falls back to.
    #[test]
    fn a_wrong_primary_group_is_corrected() {
        let model = model_of(&[IdMapEdit::AddIdentity {
            san: "alice.example.com".into(),
            primary_group: "users".into(),
            groups: vec![],
        }]);
        let mut host = empty();
        apply_edit(
            &mut host,
            &IdMapEdit::AddIdentity {
                san: "alice.example.com".into(),
                primary_group: "wrong".into(),
                groups: vec![],
            },
        )
        .unwrap();
        assert_converges(&model, &mut host);
        assert_eq!(
            host.identities.get("alice.example.com").unwrap().primary_group.as_str(),
            "users"
        );
    }

    /// A model that has never been written to knows nothing. Diffing against
    /// it must produce nothing — the alternative is a CA that lost its model
    /// file emptying every id-map in the admin domain.
    #[test]
    fn an_unestablished_model_never_proposes_anything() {
        let model = IdMapModel::default();
        assert!(!model.established());
        let mut host = empty();
        apply_edit(
            &mut host,
            &IdMapEdit::AddIdentity {
                san: "alice.example.com".into(),
                primary_group: "users".into(),
                groups: vec![],
            },
        )
        .unwrap();
        assert!(model.diff(&host).is_empty());
    }
}
