//! Reporting what an edit recorded.
//!
//! Shared by `admin perms` and `admin id-map`. Neither pushes, so there are no
//! per-host results to print: an edit that returns is an edit the CA has
//! recorded, and every host reaches it on the register it already makes. Who
//! has got there *yet* is a different question with a different answer every
//! few seconds, which is why it is `admin drift` and not a line in this output.
//!
//! The wording is a frontend concern; the facts behind it — the version, and
//! whether it moved — come from the library.

use netidx_admin::ops::RecordedEdit;

/// What kind of state was recorded, for the operator-facing wording.
pub(crate) enum Recorded<'a> {
    /// Permissions for the resolver cluster mounted at this path.
    Perms { at: &'a str },
    /// The admin domain's id-map.
    IdMap,
}

impl Recorded<'_> {
    fn what(&self) -> String {
        match self {
            Recorded::Perms { at } => format!("perms at {at:?}"),
            Recorded::IdMap => "the id-map".to_string(),
        }
    }

    /// Who will pick it up, so the version means something to read.
    fn who(&self) -> &'static str {
        match self {
            Recorded::Perms { .. } => "every member of the resolver cluster",
            Recorded::IdMap => "every id-map host",
        }
    }
}

/// Print the recorded version, and say plainly that convergence is pending
/// rather than implying it has already happened.
pub(crate) fn report_recorded(edit: &RecordedEdit, what: Recorded<'_>) {
    if !edit.changed {
        println!("no change — {} already at version {}.", what.what(), edit.version);
        return;
    }
    println!("ok — {} recorded as version {}.", what.what(), edit.version);
    println!(
        "  {} converges on it within a poll interval; `netidx admin drift` \
         says who has.",
        what.who()
    );
}
