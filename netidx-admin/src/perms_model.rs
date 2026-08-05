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

use crate::admin_proto::ResolverClusterId;
use anyhow::{Context, Result};
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
    /// Canonical perms JSON, as [`crate::perms::normalize`] produces it.
    /// Canonical so that comparing two documents compares their content
    /// rather than their whitespace.
    pub doc: String,
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

    /// Record `doc` as `cluster`'s permissions, returning the version it is
    /// now at.
    ///
    /// The document is normalized first, so a re-propagation of the same
    /// permissions written differently is recognised as no change and does
    /// not advance the version — otherwise every re-run of a command to
    /// converge a lagging member would move the target it is chasing.
    pub fn set(&mut self, cluster: ResolverClusterId, doc: &str) -> Result<u64> {
        let doc = crate::perms::normalize(doc)
            .context("normalizing permissions for the model")?;
        let entry = self
            .0
            .entry(cluster)
            .or_insert(ClusterPerms { version: 0, doc: String::new() });
        if entry.version > 0 && entry.doc == doc {
            return Ok(entry.version);
        }
        entry.doc = doc;
        entry.version += 1;
        Ok(entry.version)
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

    const A: &str = r#"{"/eu":{"alice":"swlpd"}}"#;
    const B: &str = r#"{"/eu":{"alice":"sl"}}"#;

    fn cluster() -> ResolverClusterId {
        ResolverClusterId::new()
    }

    #[test]
    fn a_cluster_is_unknown_until_it_is_set() {
        let id = cluster();
        let mut model = PermsModel::default();
        assert!(model.get(id).is_none());
        assert_eq!(model.set(id, A).unwrap(), 1);
        assert_eq!(model.get(id).unwrap().version, 1);
    }

    /// Re-propagating the same permissions must not advance the version. A
    /// member converging on the model is chasing that number; moving it on
    /// every retry would mean it could never arrive.
    #[test]
    fn the_same_document_does_not_advance_the_version() {
        let id = cluster();
        let mut model = PermsModel::default();
        assert_eq!(model.set(id, A).unwrap(), 1);
        assert_eq!(model.set(id, A).unwrap(), 1);
        // Same content, different formatting.
        let spaced = r#"{ "/eu" : { "alice" : "swlpd" } }"#;
        assert_eq!(model.set(id, spaced).unwrap(), 1);
        // A real change does advance it.
        assert_eq!(model.set(id, B).unwrap(), 2);
    }

    /// Each cluster is versioned on its own. An edit to one must not make
    /// every member of another look behind.
    #[test]
    fn clusters_are_versioned_independently() {
        let (eu, ap) = (cluster(), cluster());
        let mut model = PermsModel::default();
        model.set(eu, A).unwrap();
        model.set(eu, B).unwrap();
        model.set(ap, A).unwrap();
        assert_eq!(model.get(eu).unwrap().version, 2);
        assert_eq!(model.get(ap).unwrap().version, 1);
    }

    #[test]
    fn invalid_permissions_are_refused_rather_than_recorded() {
        let id = cluster();
        let mut model = PermsModel::default();
        assert!(model.set(id, r#"{"/eu":{"alice":"swx"}}"#).is_err());
        assert!(model.get(id).is_none(), "a refused edit must not establish it");
        assert!(model.set(id, "not json").is_err());
    }

    #[test]
    fn a_forgotten_cluster_is_unknown_again() {
        let id = cluster();
        let mut model = PermsModel::default();
        model.set(id, A).unwrap();
        assert!(model.forget(id));
        assert!(model.get(id).is_none());
        assert!(!model.forget(id));
    }

    #[test]
    fn round_trips_through_json() {
        let id = cluster();
        let mut model = PermsModel::default();
        model.set(id, A).unwrap();
        let json = serde_json::to_string(&model).unwrap();
        assert_eq!(serde_json::from_str::<PermsModel>(&json).unwrap(), model);
    }
}
