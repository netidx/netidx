//! The CA's authoritative copy of each resolver cluster's permissions.
//!
//! Perms are the one piece of propagated state the CA kept no copy of: a read
//! walked the cluster's members and returned whatever the first one to answer
//! held. That makes a member which missed an edit a candidate *source of
//! truth*, not merely a straggler — the next edit can be built on its stale
//! document and propagated over everyone else's correct one.
//!
//! So the CA holds it. Unlike an id-map there is nothing per-host about a
//! perms document — every member of a cluster must hold exactly the same one
//! — which is why this needs no diff: a member that is behind is simply sent
//! the document. See `design/perms-convergence.md`.

use crate::{admin_proto::ResolverClusterId, perms::PMap};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One cluster's authoritative permissions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterPerms {
    /// Bumped once per edit that changed the document. A member reports the
    /// version it holds; anything lower is behind.
    ///
    /// Starts at 0, meaning "never established". No member can be behind
    /// version 0, so a CA that has lost this file leaves every member alone
    /// rather than pushing an empty document at a working cluster.
    pub version: u64,
    /// The document itself. A structure rather than its JSON, so comparing two
    /// of them compares their content — whitespace was never a difference, and
    /// when this was text it took a normalization pass to say so.
    pub perms: PMap,
}

/// Every cluster's perms, by cluster. One file: clusters are few, and one
/// atomic write keeps the set consistent with itself.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PermsModel(pub BTreeMap<ResolverClusterId, ClusterPerms>);

impl PermsModel {
    /// What `cluster` is supposed to hold, if the CA has ever been told.
    pub fn get(&self, cluster: ResolverClusterId) -> Option<&ClusterPerms> {
        self.0.get(&cluster).filter(|p| p.version > 0)
    }

    /// Record `perms` as `cluster`'s permissions, returning the version it is
    /// now at.
    ///
    /// Re-recording the same permissions does not advance the version —
    /// otherwise every re-run of a command to converge a lagging member would
    /// move the target it is chasing.
    pub fn set(&mut self, cluster: ResolverClusterId, perms: &PMap) -> Result<u64> {
        crate::perms::check(perms)?;
        let entry = self
            .0
            .entry(cluster)
            .or_insert(ClusterPerms { version: 0, perms: crate::perms::empty() });
        if entry.version > 0 && &entry.perms == perms {
            return Ok(entry.version);
        }
        entry.perms = perms.clone();
        entry.version += 1;
        Ok(entry.version)
    }

    /// Whether a member reporting `reported` is behind `cluster`'s document.
    ///
    /// `None` — a member that has never been stamped — is behind anything
    /// established. A cluster the CA has no document for leaves its members
    /// alone: not knowing is not the same as knowing they are empty.
    pub fn behind(&self, cluster: ResolverClusterId, reported: Option<u64>) -> bool {
        self.get(cluster).is_some_and(|p| reported.is_none_or(|have| have < p.version))
    }

    /// Drop a cluster the admin domain no longer has. Without this the model
    /// would keep a document for a cluster nobody can be a member of, which
    /// is harmless but accumulates and would confuse anyone reading the file.
    pub fn forget(&mut self, cluster: ResolverClusterId) -> bool {
        self.0.remove(&cluster).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a() -> PMap {
        crate::perms::parse(r#"{"/eu":{"alice":"swlpd"}}"#).unwrap()
    }

    fn b() -> PMap {
        crate::perms::parse(r#"{"/eu":{"alice":"sl"}}"#).unwrap()
    }

    fn cluster() -> ResolverClusterId {
        ResolverClusterId::new()
    }

    #[test]
    fn a_cluster_is_unknown_until_it_is_set() {
        let id = cluster();
        let mut model = PermsModel::default();
        assert!(model.get(id).is_none());
        assert_eq!(model.set(id, &a()).unwrap(), 1);
        assert_eq!(model.get(id).unwrap().version, 1);
    }

    /// Re-propagating the same permissions must not advance the version. A
    /// member converging on the model is chasing that number; moving it on
    /// every retry would mean it could never arrive.
    ///
    /// Formatting cannot enter into it any more — the model holds a structure,
    /// so a document that differs only in whitespace is not a different
    /// document by construction rather than by a normalization pass.
    #[test]
    fn the_same_document_does_not_advance_the_version() {
        let id = cluster();
        let mut model = PermsModel::default();
        assert_eq!(model.set(id, &a()).unwrap(), 1);
        assert_eq!(model.set(id, &a()).unwrap(), 1);
        let spaced = crate::perms::parse(r#"{ "/eu" : { "alice" : "swlpd" } }"#).unwrap();
        assert_eq!(model.set(id, &spaced).unwrap(), 1);
        // A real change does advance it.
        assert_eq!(model.set(id, &b()).unwrap(), 2);
    }

    /// Each cluster is versioned on its own. An edit to one must not make
    /// every member of another look behind.
    #[test]
    fn clusters_are_versioned_independently() {
        let (eu, ap) = (cluster(), cluster());
        let mut model = PermsModel::default();
        model.set(eu, &a()).unwrap();
        model.set(eu, &b()).unwrap();
        model.set(ap, &a()).unwrap();
        assert_eq!(model.get(eu).unwrap().version, 2);
        assert_eq!(model.get(ap).unwrap().version, 1);
    }

    /// Bad bits are refused even though they arrive as a structure. `PMap`
    /// keeps them as opaque strings, so the type does not make this
    /// unrepresentable and the check still has to run.
    #[test]
    fn invalid_permissions_are_refused_rather_than_recorded() {
        let id = cluster();
        let mut model = PermsModel::default();
        let mut bad = crate::perms::empty();
        bad.0
            .entry(arcstr::ArcStr::from("/eu"))
            .or_default()
            .insert(arcstr::ArcStr::from("alice"), arcstr::ArcStr::from("swx"));
        assert!(model.set(id, &bad).is_err());
        assert!(model.get(id).is_none(), "a refused edit must not establish it");
    }

    #[test]
    fn a_forgotten_cluster_is_unknown_again() {
        let id = cluster();
        let mut model = PermsModel::default();
        model.set(id, &a()).unwrap();
        assert!(model.forget(id));
        assert!(model.get(id).is_none());
        assert!(!model.forget(id));
    }

    #[test]
    fn who_is_behind() {
        let (eu, ap) = (cluster(), cluster());
        let mut model = PermsModel::default();
        // Nothing established: nobody is behind, whatever they report.
        assert!(!model.behind(eu, None));
        assert!(!model.behind(eu, Some(7)));
        model.set(eu, &a()).unwrap();
        // Never stamped is behind anything established.
        assert!(model.behind(eu, None));
        assert!(!model.behind(eu, Some(1)));
        model.set(eu, &b()).unwrap();
        assert!(model.behind(eu, Some(1)));
        assert!(!model.behind(eu, Some(2)));
        // A member reporting a version from the future is not behind — it is
        // wrong, but pushing an older document at it would make that worse.
        assert!(!model.behind(eu, Some(99)));
        // Another cluster's edits do not make this one's members behind.
        assert!(!model.behind(ap, Some(1)));
    }

    #[test]
    fn round_trips_through_json() {
        let id = cluster();
        let mut model = PermsModel::default();
        model.set(id, &a()).unwrap();
        let json = serde_json::to_string(&model).unwrap();
        assert_eq!(serde_json::from_str::<PermsModel>(&json).unwrap(), model);
    }
}
