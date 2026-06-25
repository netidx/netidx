//! The CA's RBAC policy model — pure data, shared between the unix-only
//! keyslot vault ([`crate::ca_vault`]) that stores and enforces it and the
//! cross-platform conf-plane wire protocol ([`crate::conf_proto`]) that
//! carries it. A Windows admin client speaks these types to a unix CA, so
//! the types must be cross-platform even though the vault is `#[cfg(unix)]`.

use serde_derive::{Deserialize, Serialize};

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
    pub max_validity_days: u32,
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
