//! The service-setup **decision** seam: whether a completed install should
//! register the activation supervisor as an OS service, and at what scope.
//!
//! This is the composable half — a top-level install that stands up more than
//! one supervised daemon (a resolver that also runs an admin server) folds
//! each sub-step's [`ServiceNeed`] together and makes a *single* offer at the
//! end. The offer itself is a pure decision through the [`Answerer`] seam; the
//! privileged doing (unit files, sudo re-exec) stays in the CLI frontend,
//! which acts on the [`ServiceScope`] this returns.

use crate::{
    answer::{Answerer, Field},
    service::ServiceScope,
};
use anyhow::Result;
use compact_str::format_compact;

/// Whether a setup process needs the activation supervisor installed as an OS
/// service, and at what scope. Setup steps that drop activation units needing
/// unattended supervision contribute a scope; a top-level flow
/// [`merge`](ServiceNeed::merge)s the needs of all its sub-steps and offers a
/// *single* service install at the end. `None` ⇒ nothing to supervise (e.g. a
/// client-only publisher).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServiceNeed(Option<ServiceScope>);

impl ServiceNeed {
    /// No service needed.
    pub const NONE: ServiceNeed = ServiceNeed(None);

    /// A service is needed at `scope`.
    pub fn at(scope: ServiceScope) -> Self {
        ServiceNeed(Some(scope))
    }

    /// Combine two needs. System outranks User outranks nothing — a process
    /// that needs *any* system-scope daemon needs a system service.
    ///
    /// The composition seam: a flow that stands up more than one daemon (e.g.
    /// a resolver install that also sets up a CA server) folds each sub-step's
    /// need together with this and offers once.
    pub fn merge(self, other: ServiceNeed) -> ServiceNeed {
        fn rank(n: ServiceNeed) -> u8 {
            match n.0 {
                None => 0,
                Some(ServiceScope::User) => 1,
                Some(ServiceScope::System) => 2,
            }
        }
        if rank(other) > rank(self) { other } else { self }
    }

    /// The scope a service is needed at, if any.
    pub fn scope(self) -> Option<ServiceScope> {
        self.0
    }
}

/// Gates on the single service-setup offer, lifted from a flow's install flags.
#[derive(Debug, Clone, Copy)]
pub struct ServiceGate {
    /// `--dry-run`: describe, change nothing.
    pub dry_run: bool,
    /// `--no-service`: skip the offer silently.
    pub no_service: bool,
    /// `--with-service`: register without asking.
    pub with_service: bool,
}

/// **The** end-of-install decision for OS-service setup. A flow merges the
/// [`ServiceNeed`]s of its setup steps and calls this once. Returns the scope
/// the frontend should install at, or `None` when nothing should be installed.
/// The privileged install itself is the frontend's job — this only decides.
///
/// - need is `NONE` ⇒ nothing to do.
/// - `--dry-run` ⇒ note what would be offered, decide nothing.
/// - `--no-service` ⇒ skip.
/// - otherwise ⇒ `confirm` (default yes; `--with-service` pre-answers it).
pub async fn offer(
    ans: &mut dyn Answerer,
    need: ServiceNeed,
    gate: ServiceGate,
) -> Result<Option<ServiceScope>> {
    let Some(scope) = need.scope() else { return Ok(None) };
    let label = match scope {
        ServiceScope::User => "user-scope (no sudo)",
        ServiceScope::System => "system-scope (sudo required)",
    };
    if gate.dry_run {
        ans.note(&format_compact!(
            "[dry-run] would offer to install netidx as a {label} OS service"
        ));
        return Ok(None);
    }
    if gate.no_service {
        return Ok(None);
    }
    let install_now =
        ans.confirm(Field::Service, gate.with_service.then_some(true), true).await?;
    if install_now {
        Ok(Some(scope))
    } else {
        ans.note(
            "skipping OS-service registration (run `netidx admin component \
             service install` later to register it)",
        );
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_need_merge_ranks_system_over_user_over_none() {
        use ServiceNeed as N;
        let sys = N::at(ServiceScope::System);
        let usr = N::at(ServiceScope::User);
        // System wins regardless of order.
        assert_eq!(sys.merge(usr), sys);
        assert_eq!(usr.merge(sys), sys);
        // User beats nothing.
        assert_eq!(N::NONE.merge(usr), usr);
        assert_eq!(usr.merge(N::NONE), usr);
        // Nothing merges to nothing.
        assert_eq!(N::NONE.merge(N::NONE), N::NONE);
        // Idempotent.
        assert_eq!(sys.merge(sys), sys);
    }
}
