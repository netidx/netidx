//! The CA's RBAC policy model — pure data, shared between the unix-only
//! keyslot vault ([`crate::ca_vault`]) that stores and enforces it and the
//! cross-platform conf-plane wire protocol ([`crate::conf_proto`]) that
//! carries it. A Windows admin client speaks these types to a unix CA, so
//! the types must be cross-platform even though the vault is `#[cfg(unix)]`.

use serde_derive::{Deserialize, Serialize};
use std::time::Duration;

/// The `max_validity` for the system-managed signing slots (autorenew,
/// recovery) and the local superuser. Deliberately huge: the real bound on
/// any leaf is the CA-remaining clamp in `Ca::sign_request` (a leaf can never
/// outlive its issuer), so this only needs to be wide enough never to clip a
/// legitimate renewal of a long-lived leaf — which a hardcoded 730 days did.
const SIGNING_SLOT_MAX_VALIDITY: Duration =
    Duration::from_secs(100 * 365 * 86400);

/// An admin's capabilities, stored in their slot and returned by
/// `authenticate` so the server can authorize a request against exactly
/// the admin who proved their password. In the server-signs model the
/// SERVER holds the only signing key (the autorenew credential); this
/// policy decides what it will sign / edit / manage *on a role admin's
/// behalf* — a role admin's password never unlocks the key itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    /// The admin's **server-signing scope**: glob patterns every requested
    /// SAN must match for the server to sign it on this admin's behalf.
    /// Empty ⇒ may not cause any issuance (granted explicitly; no
    /// accidental allow-everything).
    pub allowed_san: Vec<String>,
    /// The longest leaf validity the server will sign on this admin's
    /// behalf. A requested validity is capped to this (and then clamped to
    /// the CA's own remaining lifetime). Stored human-readably
    /// (`"730days"`, `"10m"`) in the vault and on the wire.
    #[serde(with = "humantime_serde")]
    pub max_validity: Duration,
    /// id-map groups this admin **may assign** when signing — the
    /// allowed set bounding the groups chosen at enrollment time in
    /// the `SignRequest`. Empty ⇒ this admin's signs never register
    /// identities.
    #[serde(default)]
    pub id_map_groups: Vec<String>,
    /// Whether this admin may enroll new conf servers — i.e. authorize
    /// issuance of the reserved serving SAN. Granted explicitly; a
    /// rogue enrollee can impersonate the conf plane, so this is more
    /// privileged than any `allowed_san` glob.
    #[serde(default)]
    pub may_enroll_servers: bool,
    /// Netidx hierarchy paths under which this admin may edit permissions
    /// (the remote perms edit). A target path is in scope when it equals
    /// or descends from one of these (`/` ⇒ the whole tree). Empty ⇒ no
    /// perms-edit authority. Unlike issuance, this needs no CA key, so a
    /// `Role` keyslot can carry it.
    #[serde(default)]
    pub perms_edit_scopes: Vec<String>,
    /// Whether this admin may mint / rescope / revoke **role** admins (the
    /// `ca admin` ops, local or over the conf plane). It never confers MK
    /// access — a managing admin directs the server, which uses the
    /// autorenew credential as the MK proof — and may only grant
    /// capabilities ⊆ its own (no escalation; enforced server-side).
    /// Granted explicitly.
    #[serde(default)]
    pub may_manage_admins: bool,
    /// Netidx hierarchy paths under which this admin may control services
    /// (restart / start / stop / status the activation units on the conf
    /// servers of the cluster serving that path). Same path-scoping as
    /// [`perms_edit_scopes`](Self::perms_edit_scopes) but a separate grant —
    /// editing perms and restarting services are distinct authorities.
    /// Needs no CA key, so a `Role` keyslot can carry it.
    #[serde(default)]
    pub service_control_scopes: Vec<String>,
}

/// What a keyslot's `wrap` field protects, and so what authority the slot
/// confers. The cryptographic boundary of the RBAC model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotKind {
    /// The slot wraps the master key, so its password can recover the CA
    /// private key and sign certificates. The original (and default for
    /// pre-RBAC vaults, whose slots all wrap MK).
    Signing,
    /// The slot wraps a random verifier — never the master key — so its
    /// password authenticates and yields the slot's scoped [`Policy`] but
    /// can NEVER recover the CA private key. A satellite admin's keyslot.
    Role,
}

/// One admin keyslot's public facts (no secrets), for `ca admin list` —
/// locally and over the wire (`ListAdmins`), hence `Serialize`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminInfo {
    pub admin: String,
    pub kind: SlotKind,
    pub policy: Policy,
}

/// The minimal policy carried by the box's `autorenew` signing slot. It
/// holds the master key, so its authority is the signing tier — not this
/// policy, which is deliberately empty (no issuance glob, no admin
/// management): the slot exists to let the daemon sign verified renewals and
/// unlock the key, nothing a glob would widen.
///
/// `max_validity` is the one field that must be wide enough: the autorenew
/// sweep auto-approves *renewals*, each re-requesting its cert's original
/// validity, so the cap has to admit any leaf this CA could have issued —
/// see [`SIGNING_SLOT_MAX_VALIDITY`]. (A hardcoded 730d here silently clipped
/// any longer-lived leaf's renewal.)
pub fn autorenew_policy() -> Policy {
    Policy {
        allowed_san: vec![],
        max_validity: SIGNING_SLOT_MAX_VALIDITY,
        id_map_groups: vec![],
        may_enroll_servers: false,
        perms_edit_scopes: vec![],
        may_manage_admins: false,
        service_control_scopes: vec![],
    }
}

/// The policy on the `recovery` (off-box break-glass) signing slot — the
/// same minimal shape as [`autorenew_policy`]. Its power is the signing
/// tier (it can unlock the key to mint/rotate admins), not the glob.
pub fn recovery_policy() -> Policy {
    autorenew_policy()
}

/// The policy attached to a synthetic local-control-socket superuser. A
/// local request authorizes as a signing slot — the tier is what every
/// admin-management gate checks — so these fields are mostly moot; they are
/// set to full authority so any code that *reads* the policy (rather than
/// the tier) also sees a superuser.
pub fn superuser_policy() -> Policy {
    Policy {
        allowed_san: vec!["*".to_string()],
        max_validity: SIGNING_SLOT_MAX_VALIDITY,
        id_map_groups: vec![],
        may_enroll_servers: true,
        perms_edit_scopes: vec!["/".to_string()],
        may_manage_admins: true,
        service_control_scopes: vec!["/".to_string()],
    }
}
