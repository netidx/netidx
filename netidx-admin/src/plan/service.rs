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
    /// a resolver install that also sets up a CA) folds each sub-step's
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
    /// `--with-service`: register without asking. Redundant for a scripted
    /// caller, which already gets that; it suppresses the prompt for an
    /// interactive one.
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
/// - otherwise ⇒ register. An interactive frontend is asked first, with
///   default yes; `--with-service` skips that prompt.
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
    // Registering is the default. This point is only reached because the
    // install produced units that need a supervisor, `--no-service` above is
    // the explicit opt-out, and the interactive path has always defaulted to
    // yes — so demanding a flag of a scripted caller asked for a decision
    // whose answer was already made by installing at all.
    let install_now = if gate.with_service || !ans.interactive() {
        true
    } else {
        ans.confirm(Field::Service, None, true).await?
    };
    if install_now {
        Ok(Some(scope))
    } else {
        ans.note(
            "skipping OS-service registration (run `netidx admin host \
             service install` later to register it)",
        );
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::answer::OneTimeSecret;

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

    struct Frontend {
        interactive: bool,
        asked: bool,
    }

    #[async_trait::async_trait]
    impl Answerer for Frontend {
        fn interactive(&self) -> bool {
            self.interactive
        }
        async fn confirm(
            &mut self,
            _f: Field,
            _p: Option<bool>,
            default: bool,
        ) -> Result<bool> {
            self.asked = true;
            Ok(default)
        }
        async fn text(
            &mut self,
            _f: Field,
            _p: Option<String>,
            _d: Option<&str>,
            _r: bool,
        ) -> Result<Option<String>> {
            unreachable!()
        }
        async fn choice(
            &mut self,
            _f: Field,
            _p: Option<String>,
            _c: &[&str],
            _d: Option<&str>,
        ) -> Result<String> {
            unreachable!()
        }
        async fn select_admin_domain(
            &mut self,
            _d: &[crate::answer::AdminDomainOption],
        ) -> Result<crate::answer::AdminDomainChoice> {
            unreachable!()
        }
        async fn secret(
            &mut self,
            _f: Field,
            _p: Option<crate::admin_proto::Secret>,
        ) -> Result<crate::admin_proto::Secret> {
            unreachable!()
        }
        async fn announce(&mut self, _t: &str, _b: &str) -> Result<()> {
            Ok(())
        }
        async fn announce_identity(
            &mut self,
            _b: &str,
            _c: &crate::fingerprint::Fingerprint,
        ) -> Result<()> {
            Ok(())
        }
        async fn confirm_identity(
            &mut self,
            _i: &crate::transport::CaIdentity,
        ) -> Result<bool> {
            unreachable!()
        }
        fn show_verification_code(
            &mut self,
            _p: &str,
            _c: &crate::fingerprint::Fingerprint,
        ) {
        }
        fn clear_verification_code(&mut self) {}
        fn progress(&mut self, _p: crate::answer::Progress) {}
        fn note(&mut self, _m: &str) {}
        fn warn(&mut self, _m: &str) {}
        async fn show_one_time_secret(
            &mut self,
            _secret: OneTimeSecret,
            _p: &str,
        ) -> Result<()> {
            Ok(())
        }
    }

    fn gate(no_service: bool, with_service: bool) -> ServiceGate {
        ServiceGate { dry_run: false, no_service, with_service }
    }

    /// An install that produced units wants a supervisor. A scripted caller
    /// used to have to say so with a flag, and got a hard error otherwise —
    /// after the install had already enrolled a certificate and written every
    /// config file.
    #[tokio::test]
    async fn registering_is_the_default_and_only_a_person_is_asked() {
        let need = ServiceNeed::at(ServiceScope::System);

        let mut scripted = Frontend { interactive: false, asked: false };
        let scope = offer(&mut scripted, need, gate(false, false)).await.unwrap();
        assert_eq!(scope, Some(ServiceScope::System));
        assert!(!scripted.asked, "a scripted caller must not be asked");

        // --no-service is still the way out, and still asks nobody.
        let mut scripted = Frontend { interactive: false, asked: false };
        assert_eq!(offer(&mut scripted, need, gate(true, false)).await.unwrap(), None);
        assert!(!scripted.asked);

        // A person is asked, and the default is yes.
        let mut person = Frontend { interactive: true, asked: false };
        let scope = offer(&mut person, need, gate(false, false)).await.unwrap();
        assert_eq!(scope, Some(ServiceScope::System));
        assert!(person.asked, "an interactive frontend decides for itself");

        // --with-service skips that prompt.
        let mut person = Frontend { interactive: true, asked: false };
        assert_eq!(
            offer(&mut person, need, gate(false, true)).await.unwrap(),
            Some(ServiceScope::System)
        );
        assert!(!person.asked);

        // Nothing to supervise stays nothing to do.
        let mut scripted = Frontend { interactive: false, asked: false };
        assert_eq!(
            offer(&mut scripted, ServiceNeed::NONE, gate(false, false)).await.unwrap(),
            None
        );
    }
}
