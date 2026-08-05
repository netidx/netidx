//! The admin domain's authoritative id-map *shape*, held by the CA.
//!
//! Every id-map host holds its own map file. This is what they are all
//! supposed to agree about: which identities exist, which groups exist, and
//! who belongs to what. It is deliberately not what they agree about
//! *numerically* — a uid is allocated locally by each host, because the
//! resolver keys permissions on names and the number is a per-host detail.
//! That is why an edit propagates as an operation rather than as a document,
//! and it is why this model has no uids to propagate.
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
    ///
    /// The uid and gid numbers in it are an artifact of reusing the type. They
    /// are never read, never compared, and never propagated.
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

    /// The shape, for diffing a host against. uids in it are meaningless.
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
        // Names and membership must agree. uids are per-host and are not part
        // of the comparison — the model's are meaningless.
        assert_eq!(
            model.shape().groups.keys().collect::<Vec<_>>(),
            host.groups.keys().collect::<Vec<_>>()
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
        assert!(host.groups.contains_key("users"), "the base map seeds `users`");
        assert_eq!(
            model.shape().groups.keys().collect::<Vec<_>>(),
            host.groups.keys().collect::<Vec<_>>()
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
}
