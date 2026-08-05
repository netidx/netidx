//! Reporting the outcome of an edit that had to reach several hosts.
//!
//! Shared by `admin perms` and `admin id-map`, which both propagate: a
//! partial failure must read as a partial failure and not as success, and the
//! operator needs to be told what to re-run. The wording is a frontend
//! concern; the predicate behind it — which hosts took the edit — comes from
//! the library.

use netidx_admin_proto::PeerResult;

/// What kind of state was propagated, for the operator-facing wording. The
/// two differ in what a partial failure *means* on the ground: perms are
/// loaded by the resolver at start, an id-map by the id-mapper daemon.
pub(crate) enum Propagated<'a> {
    /// Permissions for the resolver cluster mounted at this path.
    Perms { at: &'a str },
    /// The admin domain's id-map.
    IdMap,
}

impl Propagated<'_> {
    fn what(&self) -> String {
        match self {
            Propagated::Perms { at } => format!("perms at {at:?}"),
            Propagated::IdMap => "the id-map".to_string(),
        }
    }

    fn hosts(&self) -> &'static str {
        match self {
            Propagated::Perms { .. } => "resolver cluster member",
            Propagated::IdMap => "id-map host",
        }
    }

    /// Nothing, for either. Both daemons poll the files they were just handed
    /// and reload on their own, so there is no operator step after a
    /// successful propagation — the resolver polls its config and its
    /// `include_permissions` mtimes, and the id-mapper polls its map. Saying
    /// otherwise sends operators to restart a service that did not need it,
    /// which on a resolver means a needless gap in service.
    fn after(&self) -> Option<&'static str> {
        match self {
            Propagated::Perms { .. } | Propagated::IdMap => None,
        }
    }
}

/// Print which hosts took the edit, and — when some didn't — say plainly that
/// they now disagree and how to converge them.
pub(crate) fn report_peers(
    peers: &[PeerResult],
    what: Propagated<'_>,
    retry: std::fmt::Arguments<'_>,
) {
    let failed: Vec<_> = peers.iter().filter(|p| p.error.is_some()).collect();
    if failed.is_empty() {
        println!("ok — {} updated on {} {}(s).", what.what(), peers.len(), what.hosts());
        if let Some(line) = what.after() {
            println!("{line}");
        }
        return;
    }
    println!(
        "{}: {} of {} {}(s) could NOT be updated:",
        what.what(),
        failed.len(),
        peers.len(),
        what.hosts()
    );
    for p in &failed {
        println!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("?"));
    }
    println!(
        "  the hosts now DISAGREE. The edit is idempotent — re-run \
         `{retry}` once they are back to converge."
    );
}
