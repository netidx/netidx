//! LUKS-style keyslot vault for the CA private key.
//!
//! The CA key is never encrypted directly with an admin password.
//! Instead, exactly like LUKS:
//!
//! - A random 32-byte **master key (MK)** encrypts the CA key
//!   (AES-256-GCM) into `key_enc`.
//! - MK is then **wrapped** once per admin — each wrap keyed by an
//!   Argon2id derivation of that admin's password — and stored as a
//!   **slot**.
//!
//! This buys what LUKS slots buy: multiple admins each with their own
//! password, and revoking an admin by deleting their slot *without*
//! re-encrypting the CA key or redistributing anything. Each slot also
//! carries that admin's issuance [`Policy`], so authentication and
//! authorization scope are one record.
//!
//! Nothing here is openssl-coupled (`aes-gcm` + `argon2`, pure Rust) —
//! the vault just encrypts opaque bytes. The caller hands in / gets
//! back the CA key as an *unencrypted* PKCS#8 PEM (encrypted only by
//! MK while at rest), which the `ca` module turns into a signer.
//!
//! On-disk: `<ca-dir>/vault.json`, mode 0600.

use crate::{atomic, password::fold_if_one_time};
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce, aead::Aead};
use anyhow::{Context, Result, anyhow, bail};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

#[cfg(test)]
use netidx_admin_proto::policy::RECOVERY_ADMIN;
use netidx_admin_proto::policy::{AdminInfo, Policy, SlotKind, is_reserved_admin};

/// A long random password for a **signing** slot (the box-held `autorenew`
/// credential), kept in a `Zeroizing` buffer that wipes on drop. Unlike a
/// recovery password this is never read by a human — it lives only in the
/// sealed keytab and in process memory — so it is plain hex with no
/// confusable-character handling. Any signing-slot password unlocks MK, so
/// treat it like the CA key.
pub fn random_signing_password() -> Zeroizing<String> {
    Zeroizing::new(format!("{}{}", crate::ca_store::new_id(), crate::ca_store::new_id()))
}

/// File name of the vault within a CA directory. Its presence (rather
/// than a bare `private.key`) marks a CA as vault-protected.
pub const VAULT_FILE: &str = "vault.json";
const VAULT_VERSION: u32 = 1;

/// The off-box break-glass signing credential the operator stores in a
/// safe — the only key-recovery credential that ever leaves the box.
/// Minted once at init via [`create`]; rotated on-box via
/// `admin ca recovery rotate`. With [`crate::admin_server::AUTORENEW_ADMIN`]
/// these are the only two signing (master-key-holding) slots.
// Argon2id cost. Per the design doc: 64 MiB / t=3 / p=4 in production;
// the params are stored *per slot*, so `unlock` always uses whatever a
// slot was created with — which lets the test build derive cheaply
// without weakening the real format.
#[cfg(not(test))]
const KDF_COST: (u32, u32, u32) = (65536, 3, 4);
#[cfg(test)]
const KDF_COST: (u32, u32, u32) = (32, 1, 1);

/// Pre-RBAC slots have no `kind` field but all wrap MK, so they are
/// signing slots — the only back-compatible default.
fn default_slot_kind() -> SlotKind {
    SlotKind::Signing
}

/// The result of a successful [`unlock`]: the admin identified by the
/// password, their policy, and the decrypted CA key. The key bytes are
/// zeroized on drop — hold the `Unlocked` only as long as needed to
/// sign one request. Only a [`SlotKind::Signing`] slot can produce this.
pub struct Unlocked {
    pub slot_id: uuid::Uuid,
    pub credential_revision: u64,
    pub admin: String,
    pub policy: Policy,
    pub ca_key_pem: Zeroizing<Vec<u8>>,
}

/// The result of a successful [`authenticate`]: who the password belongs
/// to, what they may do, and which keyslot tier they hold — but NO CA
/// key. Every non-signing admin op authorizes against this.
#[derive(Clone)]
pub struct Authenticated {
    pub slot_id: uuid::Uuid,
    pub credential_revision: u64,
    pub admin: String,
    pub policy: Policy,
    pub kind: SlotKind,
    /// The password that produced this is a one-time key: it proves who the
    /// caller is and nothing else. Carried here rather than re-read at each
    /// gate so the single authentication funnel can refuse on it — see
    /// `admin_server::auth::authenticate`.
    pub must_change: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct AeadBlob {
    nonce: String, // base64, 12 bytes
    ct: String,    // base64, ciphertext || tag
}

#[derive(Clone, Serialize, Deserialize)]
struct Kdf {
    #[serde(rename = "type")]
    kind: String, // "argon2id"
    salt: String, // base64, 16 bytes
    m_cost_kib: u32,
    t_cost: u32,
    p_cost: u32,
}

#[derive(Clone, Serialize, Deserialize)]
struct Slot {
    #[serde(default = "uuid::Uuid::new_v4")]
    id: uuid::Uuid,
    #[serde(default)]
    credential_revision: u64,
    admin: String,
    kdf: Kdf,
    // AES-256-GCM(KEK, secret). For a `Signing` slot the secret is the
    // master key (so its password recovers the CA key); for a `Role` slot
    // it is a random verifier (the GCM tag proves the password, but the
    // plaintext is useless — it cannot decrypt `key_enc`).
    wrap: AeadBlob,
    #[serde(default = "default_slot_kind")]
    kind: SlotKind,
    policy: Policy,
    /// This slot's password was generated, not chosen — by `add-role` or by
    /// `reset-password` — and authorizes only its own replacement. Cleared
    /// when its holder sets one. A `must_change` slot always holds a
    /// [`gen_crockford_password`] value, which is what lets `authenticate`
    /// fold the typed form (see [`normalize_crockford_password`]).
    #[serde(default)]
    must_change: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct VaultFile {
    version: u32,
    key_enc: AeadBlob, // AES-256-GCM(MK, ca_key_pkcs8_pem)
    slots: Vec<Slot>,
}

/// A CA directory's keyslot vault (`<ca-dir>/<VAULT_FILE>`). Reached
/// through [`crate::ca_store::CaDir`], which retains the config-directory guard;
/// read methods take `&self`, write methods `&mut self`, so the borrow
/// checker enforces exclusion rather than a remembered global lock.
pub struct CAVault {
    dir: PathBuf,
    vault: Option<VaultFile>,
}

pub struct VaultSnapshot {
    vault: VaultFile,
}

pub struct PreparedRoleSlot {
    slot: Slot,
}

/// A role slot re-wrapped under a new password, ready to install. Carries the
/// revision it was prepared against so the commit refuses a slot that moved
/// while the (deliberately slow) KDF ran.
pub struct PreparedRoleRekey {
    previous: (uuid::Uuid, u64),
    replacement: Slot,
}

impl PreparedRoleRekey {
    /// Whether this rekey was derived against exactly that slot at that
    /// revision. Change-password uses it to prove the rekey it is about to
    /// install belongs to the caller's own slot and not to whoever else was
    /// preparing one at the same moment.
    pub fn targets(&self, slot_id: uuid::Uuid, credential_revision: u64) -> bool {
        self.previous == (slot_id, credential_revision)
    }
}

pub struct PreparedSigningReplacement {
    authorizer: (uuid::Uuid, u64),
    target: String,
    previous: Option<(uuid::Uuid, u64)>,
    replacement: Slot,
}

pub struct PreparedSigningRekey {
    previous: (uuid::Uuid, u64),
    replacement: Slot,
}

pub struct SigningRekeyRollback {
    installed_revision: u64,
    previous: Slot,
}

impl VaultSnapshot {
    pub fn unlock(&self, password: &str) -> Result<Unlocked> {
        unlock(&self.vault, password)
    }

    pub fn authenticate(&self, admin: &str, password: &str) -> Result<Authenticated> {
        authenticate(&self.vault, admin, password)
    }

    pub fn prepare_role_slot(
        &self,
        new_admin: &str,
        new_password: &str,
        policy: Policy,
        must_change: bool,
    ) -> Result<PreparedRoleSlot> {
        prepare_role_slot(&self.vault, new_admin, new_password, policy, must_change)
    }

    pub fn prepare_role_rekey(
        &self,
        target_admin: &str,
        new_password: &str,
        must_change: bool,
    ) -> Result<PreparedRoleRekey> {
        prepare_role_rekey(&self.vault, target_admin, new_password, must_change)
    }

    pub fn prepare_signing_replacement(
        &self,
        existing_password: &str,
        target_admin: &str,
        new_password: &str,
        policy: Policy,
    ) -> Result<PreparedSigningReplacement> {
        let (authorizer, mk) = recover_mk(&self.vault, existing_password)?;
        let authorizer = &self.vault.slots[authorizer];
        let previous = self
            .vault
            .slots
            .iter()
            .find(|slot| slot.admin == target_admin)
            .map(|slot| (slot.id, slot.credential_revision));
        let replacement =
            make_slot(&mk, SlotKind::Signing, target_admin, new_password, policy)?;
        Ok(PreparedSigningReplacement {
            authorizer: (authorizer.id, authorizer.credential_revision),
            target: target_admin.to_string(),
            previous,
            replacement,
        })
    }

    pub fn prepare_signing_rekey(
        &self,
        target_admin: &str,
        old_password: &str,
        new_password: &str,
    ) -> Result<PreparedSigningRekey> {
        let (idx, mk) = recover_mk(&self.vault, old_password)?;
        let previous = &self.vault.slots[idx];
        if previous.admin != target_admin {
            bail!(
                "that password unlocks {:?}, not {target_admin:?}; refusing to rekey a \
                 different slot",
                previous.admin
            );
        }
        let mut replacement = make_slot(
            &mk,
            SlotKind::Signing,
            target_admin,
            new_password,
            previous.policy.clone(),
        )?;
        replacement.id = previous.id;
        replacement.credential_revision = previous
            .credential_revision
            .checked_add(1)
            .context("the signing-slot credential revision is exhausted")?;
        Ok(PreparedSigningRekey {
            previous: (previous.id, previous.credential_revision),
            replacement,
        })
    }
}

impl CAVault {
    #[cfg(test)]
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self { dir, vault: None }
    }

    pub(crate) async fn open(dir: PathBuf) -> Result<Self> {
        let path = dir.join(VAULT_FILE);
        let vault = match read_vault(&path).await {
            Ok(vault) => Some(vault),
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                None
            }
            Err(e) => return Err(e),
        };
        Ok(Self { dir, vault })
    }

    fn vault_path(&self) -> PathBuf {
        self.dir.join(VAULT_FILE)
    }

    pub fn snapshot(&self) -> Result<VaultSnapshot> {
        Ok(VaultSnapshot { vault: self.current()?.clone() })
    }

    /// True if `dir` holds a vault-protected CA. A pre-open check (used to
    /// decide whether to init), so it takes the path directly rather than a
    /// constructed (locked) vault.
    pub fn exists(dir: &Path) -> bool {
        dir.join(VAULT_FILE).exists()
    }

    pub async fn exists_async(dir: &Path) -> bool {
        tokio::fs::try_exists(dir.join(VAULT_FILE)).await.unwrap_or(false)
    }

    fn current(&self) -> Result<&VaultFile> {
        self.vault
            .as_ref()
            .with_context(|| format!("no vault at {}", self.vault_path().display()))
    }

    async fn commit(&mut self, vault: VaultFile) -> Result<()> {
        write_vault(&self.vault_path(), &vault).await?;
        self.vault = Some(vault);
        Ok(())
    }

    /// Create a new vault: generate a master key, encrypt `ca_key_pem` (an
    /// unencrypted PKCS#8 PEM) under it, and write the first admin's slot.
    /// Refuses to clobber an existing vault.
    pub async fn create(
        &mut self,
        ca_key_pem: &[u8],
        admin: &str,
        password: &str,
        policy: Policy,
    ) -> Result<()> {
        if self.vault.is_some() {
            bail!("a vault already exists at {}", self.vault_path().display());
        }
        let ca_key_pem = Zeroizing::new(ca_key_pem.to_vec());
        let admin = admin.to_string();
        let password = Zeroizing::new(password.to_string());
        let vault = tokio::task::spawn_blocking(move || {
            let mut mk = Zeroizing::new([0u8; 32]);
            rand::rng().fill_bytes(&mut mk[..]);
            let key_enc = aead_seal(&mk, &ca_key_pem)?;
            let slot = make_slot(&mk, SlotKind::Signing, &admin, &password, policy)?;
            Ok::<_, anyhow::Error>(VaultFile {
                version: VAULT_VERSION,
                key_enc,
                slots: vec![slot],
            })
        })
        .await
        .context("vault creation task panicked")??;
        self.commit(vault).await
    }

    /// Recover the CA key (and the unlocking admin's identity + policy)
    /// using `password`. A wrong password — one that no slot accepts — is a
    /// clean error, not a panic; the GCM tag on each slot's wrap is the
    /// password check.
    pub fn unlock(&self, password: &str) -> Result<Unlocked> {
        unlock(self.current()?, password)
    }

    pub(crate) async fn unlock_async(&self, password: &str) -> Result<Unlocked> {
        let snapshot = self.snapshot()?;
        let password = Zeroizing::new(password.to_string());
        tokio::task::spawn_blocking(move || snapshot.unlock(&password))
            .await
            .context("vault unlock task panicked")?
    }

    /// Authenticate `admin` by `password` against any keyslot (signing or
    /// role) and return their policy and tier — but never the CA key. The
    /// password proof is the GCM tag on the slot's `wrap`; we don't care what
    /// it wraps, only that it opens. This is the auth primitive for every
    /// admin op that doesn't sign a certificate (perms edits, map admin), so
    /// a role keyslot is a first-class admin for those ops without ever
    /// touching the CA private key.
    pub fn authenticate(&self, admin: &str, password: &str) -> Result<Authenticated> {
        authenticate(self.current()?, admin, password)
    }

    /// Add a **signing** slot — one that wraps MK and so can recover the CA
    /// key. DANGER: a new signing slot is a new MK-holder, exactly the thing
    /// the two-slot model minimises; never call this from a wire-exposed
    /// handler. `existing_password` must unlock an existing signing slot (the
    /// bootstrap authority). In the server-signs model the only routine caller
    /// is autorenew setup (re-minting the box's `autorenew` slot, authorized
    /// by the `recovery` password); init mints `recovery` via [`create`](Self::create).
    pub async fn add_signing_slot(
        &mut self,
        existing_password: &str,
        new_admin: &str,
        new_password: &str,
        policy: Policy,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        let existing_password = Zeroizing::new(existing_password.to_string());
        let new_admin = new_admin.to_string();
        let new_password = Zeroizing::new(new_password.to_string());
        let vault = tokio::task::spawn_blocking(move || {
            if vault.slots.iter().any(|s| s.admin == new_admin) {
                bail!("an admin named {new_admin:?} already exists");
            }
            let (_slot, mk) = recover_mk(&vault, &existing_password)?;
            let slot =
                make_slot(&mk, SlotKind::Signing, &new_admin, &new_password, policy)?;
            vault.slots.push(slot);
            Ok::<_, anyhow::Error>(vault)
        })
        .await
        .context("signing-slot creation task panicked")??;
        self.commit(vault).await
    }

    /// Atomically re-key a signing slot: recover the master key via a *different*
    /// authorizing slot (`existing_password` — e.g. the box's autorenew
    /// credential), drop any current slot named `admin`, and add a fresh one
    /// under `new_password`, in a **single** `write_vault`. Unlike
    /// [`remove_slot`](Self::remove_slot) + [`add_signing_slot`](Self::add_signing_slot),
    /// there is no intermediate on-disk state with the slot missing — a failed
    /// write leaves the old slot intact — so rotating the sole `recovery` slot
    /// can't leave the CA with no recovery credential. `admin` need not already
    /// exist (then this is a plain add).
    pub async fn replace_signing_slot(
        &mut self,
        existing_password: &str,
        admin: &str,
        new_password: &str,
        policy: Policy,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        let existing_password = Zeroizing::new(existing_password.to_string());
        let admin = admin.to_string();
        let new_password = Zeroizing::new(new_password.to_string());
        let vault = tokio::task::spawn_blocking(move || {
            let (_slot, mk) = recover_mk(&vault, &existing_password)?;
            let slot = make_slot(&mk, SlotKind::Signing, &admin, &new_password, policy)?;
            vault.slots.retain(|s| s.admin != admin);
            vault.slots.push(slot);
            Ok::<_, anyhow::Error>(vault)
        })
        .await
        .context("signing-slot replacement task panicked")??;
        self.commit(vault).await
    }

    /// Add a **role** slot: a keyslot that authenticates and carries `policy`
    /// but does NOT wrap the master key, so it can never recover the CA
    /// private key. It wraps a throwaway random verifier purely so its
    /// password has a GCM target to prove against; MK is never touched, so
    /// creating one needs no MK and no signing authority. This is a low-level
    /// primitive — **authority to call it lives in the caller** (the local CLI
    /// holds the vault file; the wire handler checks `may_manage_admins` + the
    /// no-escalation subset rule). The hard-wired `SlotKind::Role` is the
    /// guard that this path can never mint a new MK-holder.
    pub async fn add_role_slot(
        &mut self,
        new_admin: &str,
        new_password: &str,
        policy: Policy,
        must_change: bool,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        let snapshot = vault.clone();
        let new_admin = new_admin.to_string();
        let new_password = Zeroizing::new(new_password.to_string());
        let prepared = tokio::task::spawn_blocking(move || {
            prepare_role_slot(&snapshot, &new_admin, &new_password, policy, must_change)
        })
        .await
        .context("role-slot creation task panicked")??;
        vault.slots.push(prepared.slot);
        self.commit(vault).await
    }

    pub async fn add_prepared_role_slot(
        &mut self,
        prepared: PreparedRoleSlot,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        validate_new_role_slot(&vault, &prepared.slot.admin)?;
        vault.slots.push(prepared.slot);
        self.commit(vault).await
    }

    /// Install a [`PreparedRoleRekey`], replacing the target slot in place.
    /// [`ensure_slot_revision`] is the whole point: the KDF runs off the write
    /// lock, so between prepare and commit the slot could have been reset by
    /// someone else, removed, or re-added under the same name. Any of those
    /// refuses rather than silently reinstating a password the vault has
    /// already moved past.
    pub async fn install_role_rekey(
        &mut self,
        prepared: PreparedRoleRekey,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        let idx = ensure_slot_revision(&vault, prepared.previous)?;
        if vault.slots[idx].kind != SlotKind::Role {
            bail!(
                "the target slot became a signing keyslot while its rekey was prepared"
            );
        }
        vault.slots[idx] = prepared.replacement;
        self.commit(vault).await
    }

    pub async fn install_signing_replacement(
        &mut self,
        prepared: PreparedSigningReplacement,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        ensure_slot_revision(&vault, prepared.authorizer)?;
        let current = vault
            .slots
            .iter()
            .find(|slot| slot.admin == prepared.target)
            .map(|slot| (slot.id, slot.credential_revision));
        if current != prepared.previous {
            bail!("the target signing slot changed while its replacement was prepared");
        }
        vault.slots.retain(|slot| slot.admin != prepared.target);
        vault.slots.push(prepared.replacement);
        self.commit(vault).await
    }

    pub async fn install_signing_rekey(
        &mut self,
        prepared: PreparedSigningRekey,
    ) -> Result<SigningRekeyRollback> {
        let mut vault = self.current()?.clone();
        let idx = ensure_slot_revision(&vault, prepared.previous)?;
        let previous = vault.slots[idx].clone();
        let installed_revision = prepared.replacement.credential_revision;
        vault.slots[idx] = prepared.replacement;
        self.commit(vault).await?;
        Ok(SigningRekeyRollback { installed_revision, previous })
    }

    pub async fn rollback_signing_rekey(
        &mut self,
        rollback: SigningRekeyRollback,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        let idx = vault
            .slots
            .iter()
            .position(|slot| {
                slot.id == rollback.previous.id
                    && slot.credential_revision == rollback.installed_revision
            })
            .context("the signing slot changed again before rollback")?;
        vault.slots[idx] = rollback.previous;
        self.commit(vault).await
    }

    /// Remove (revoke) an admin's slot. A low-level primitive — authority
    /// lives in the caller (local FS / wire `may_manage_admins`). Refuses to
    /// remove the last remaining **signing** slot unless `force`: that is the
    /// only MK-holder, and removing it orphans the CA key forever (no password
    /// could ever unlock it again). Removing a role slot never trips the
    /// guard.
    pub async fn remove_slot(&mut self, target_admin: &str, force: bool) -> Result<()> {
        let mut vault = self.current()?.clone();
        let idx = vault
            .slots
            .iter()
            .position(|s| s.admin == target_admin)
            .ok_or_else(|| anyhow!("no admin named {target_admin:?}"))?;
        let signing = vault.slots.iter().filter(|s| s.kind == SlotKind::Signing).count();
        if vault.slots[idx].kind == SlotKind::Signing && signing == 1 && !force {
            bail!(
                "refusing to remove {target_admin:?}: it is the only signing keyslot, so \
                 removing it would orphan the CA key permanently (no password could unlock \
                 it again). Pass force only to intentionally retire this CA."
            );
        }
        vault.slots.remove(idx);
        self.commit(vault).await
    }

    /// Replace the [`Policy`] on `target_admin`'s slot. A low-level primitive
    /// — authority lives in the caller (local FS / wire `may_manage_admins` +
    /// subset check). It rewrites **only** the (plaintext) `policy` field; the
    /// slot's `kind` and `wrap` are untouched, so it can never promote a Role
    /// slot to Signing (a Role admin can be rescoped but never handed MK).
    pub async fn set_policy(&mut self, target_admin: &str, policy: Policy) -> Result<()> {
        let mut vault = self.current()?.clone();
        let idx = vault
            .slots
            .iter()
            .position(|s| s.admin == target_admin)
            .ok_or_else(|| anyhow!("no admin named {target_admin:?}"))?;
        vault.slots[idx].policy = policy;
        self.commit(vault).await
    }

    /// List the admins, their tiers, and their policies (no secrets).
    pub fn list_admins(&self) -> Result<Vec<AdminInfo>> {
        Ok(self
            .current()?
            .slots
            .iter()
            .map(|s| AdminInfo {
                slot_id: s.id,
                admin: s.admin.clone(),
                kind: s.kind,
                policy: s.policy.clone(),
                must_change: s.must_change,
            })
            .collect())
    }

    /// The names of every MK-wrapping (`Signing`) slot. The server-only model
    /// keeps this to exactly `{recovery, autorenew}`; anything else is a
    /// backup-crackable extra key-holder. Used by init/recovery + as an
    /// invariant check.
    pub fn signing_slot_names(&self) -> Result<Vec<String>> {
        Ok(self
            .current()?
            .slots
            .iter()
            .filter(|s| s.kind == SlotKind::Signing)
            .map(|s| s.admin.clone())
            .collect())
    }

    /// One slot's tier + policy (no secrets), by admin name.
    pub fn slot_policy(&self, admin: &str) -> Result<(SlotKind, Policy)> {
        self.current()?
            .slots
            .iter()
            .find(|s| s.admin == admin)
            .map(|s| (s.kind, s.policy.clone()))
            .ok_or_else(|| anyhow!("no admin named {admin:?}"))
    }

    pub fn resolve_session_slot(
        &self,
        slot_id: uuid::Uuid,
        credential_revision: u64,
    ) -> Result<Authenticated> {
        let slot = self
            .current()?
            .slots
            .iter()
            .find(|s| s.id == slot_id)
            .ok_or_else(|| anyhow!("the administrator slot no longer exists"))?;
        if slot.credential_revision != credential_revision {
            bail!("the administrator password was rotated");
        }
        Ok(Authenticated {
            slot_id: slot.id,
            credential_revision: slot.credential_revision,
            admin: slot.admin.clone(),
            policy: slot.policy.clone(),
            kind: slot.kind,
            must_change: slot.must_change,
        })
    }

    /// Re-wrap a **signing** slot's master key under a fresh password, in
    /// place — the slot keeps its name, tier, and policy, and no slot is
    /// added or removed. `old_password` must unlock the target signing slot:
    /// holding the slot's current password IS the authority to rotate it.
    ///
    /// This is how the box rotates its OWN `autorenew` credential live, with
    /// no second signing slot to authorize a remove-then-re-add — the daemon
    /// holds only the autorenew password, recovers MK with it, and re-wraps
    /// the same slot under a new password. Both MK-holders are preserved, so
    /// the `recovery` backstop is untouched. Returns an error (and leaves the
    /// vault unchanged) if the password unlocks a *different* slot, so a
    /// caller can't rekey the wrong credential by mistake.
    pub async fn rekey_signing_slot(
        &mut self,
        target_admin: &str,
        old_password: &str,
        new_password: &str,
    ) -> Result<()> {
        let mut vault = self.current()?.clone();
        let target_admin = target_admin.to_string();
        let old_password = Zeroizing::new(old_password.to_string());
        let new_password = Zeroizing::new(new_password.to_string());
        let vault = tokio::task::spawn_blocking(move || {
            let (idx, mk) = recover_mk(&vault, &old_password)?;
            if vault.slots[idx].admin != target_admin {
                bail!(
                    "that password unlocks {:?}, not {target_admin:?}; refusing to rekey a \
                     different slot",
                    vault.slots[idx].admin
                );
            }
            let policy = vault.slots[idx].policy.clone();
            let old_id = vault.slots[idx].id;
            let revision = vault.slots[idx]
                .credential_revision
                .checked_add(1)
                .context("the signing-slot credential revision is exhausted")?;
            let mut replacement = make_slot(
                &mk,
                SlotKind::Signing,
                &target_admin,
                &new_password,
                policy,
            )?;
            replacement.id = old_id;
            replacement.credential_revision = revision;
            vault.slots[idx] = replacement;
            Ok::<_, anyhow::Error>(vault)
        })
        .await
        .context("signing-slot rekey task panicked")??;
        self.commit(vault).await
    }
}

// -- internals ----------------------------------------------------------------

fn validate_new_role_slot(vault: &VaultFile, new_admin: &str) -> Result<()> {
    if is_reserved_admin(new_admin) {
        bail!(
            "{new_admin:?} is a reserved signing-slot name (recovery / autorenew); \
             choose another name for a role admin"
        );
    }
    if vault.slots.iter().any(|slot| slot.admin == new_admin) {
        bail!("an admin named {new_admin:?} already exists");
    }
    Ok(())
}

fn prepare_role_slot(
    vault: &VaultFile,
    new_admin: &str,
    new_password: &str,
    policy: Policy,
    must_change: bool,
) -> Result<PreparedRoleSlot> {
    validate_new_role_slot(vault, new_admin)?;
    let mut verifier = Zeroizing::new([0u8; 32]);
    rand::rng().fill_bytes(&mut verifier[..]);
    let new_password = fold_if_one_time(new_password, must_change);
    let mut slot =
        make_slot(&verifier, SlotKind::Role, new_admin, &new_password, policy)?;
    slot.must_change = must_change;
    Ok(PreparedRoleSlot { slot })
}

/// Re-wrap role slot `target_admin` under `new_password`, in place. The slot
/// keeps its id, name, tier, and policy, and its `credential_revision` goes
/// up by one — which is what evicts every session issued against the old
/// password (see [`CAVault::resolve_session_slot`]).
///
/// A role slot's `wrap` holds a throwaway verifier, so replacing it needs
/// neither MK nor the old password: a fresh random verifier under the new
/// KDF is as good as the one it replaces. **All** authority to call this
/// therefore lives in the caller — self-service change-password proves the
/// old password by authenticating first, and reset-password proves
/// `may_manage_admins` plus the target being within the caller's own scope.
fn prepare_role_rekey(
    vault: &VaultFile,
    target_admin: &str,
    new_password: &str,
    must_change: bool,
) -> Result<PreparedRoleRekey> {
    let previous = vault
        .slots
        .iter()
        .find(|slot| slot.admin == target_admin)
        .ok_or_else(|| anyhow!("no admin named {target_admin:?}"))?;
    if previous.kind != SlotKind::Role {
        bail!(
            "{target_admin:?} is a signing keyslot; its password is rotated with \
             `netidx admin ca recovery rotate` or `netidx admin ca auto-approve --rotate`"
        );
    }
    let mut verifier = Zeroizing::new([0u8; 32]);
    rand::rng().fill_bytes(&mut verifier[..]);
    let new_password = fold_if_one_time(new_password, must_change);
    let mut replacement = make_slot(
        &verifier,
        SlotKind::Role,
        target_admin,
        &new_password,
        previous.policy.clone(),
    )?;
    replacement.id = previous.id;
    replacement.credential_revision = previous
        .credential_revision
        .checked_add(1)
        .context("the role-slot credential revision is exhausted")?;
    replacement.must_change = must_change;
    Ok(PreparedRoleRekey {
        previous: (previous.id, previous.credential_revision),
        replacement,
    })
}

fn ensure_slot_revision(vault: &VaultFile, expected: (uuid::Uuid, u64)) -> Result<usize> {
    vault
        .slots
        .iter()
        .position(|slot| slot.id == expected.0 && slot.credential_revision == expected.1)
        .context("the authorizing signing slot changed while the operation was prepared")
}

fn unlock(vault: &VaultFile, password: &str) -> Result<Unlocked> {
    let (slot, mk) = recover_mk(vault, password)?;
    let ca_key_pem = match aead_try_open(&mk, &vault.key_enc)? {
        Some(pt) => pt,
        None => bail!("vault: master key does not decrypt the CA key (corrupt vault)"),
    };
    Ok(Unlocked {
        slot_id: vault.slots[slot].id,
        credential_revision: vault.slots[slot].credential_revision,
        admin: vault.slots[slot].admin.clone(),
        policy: vault.slots[slot].policy.clone(),
        ca_key_pem,
    })
}

/// Verify `password` against the slot named `admin`. Costs exactly one KDF
/// whether or not a slot of that name exists: an early return on a
/// name miss is a timing oracle that enumerates the admin roster. The dummy
/// derivation is affordable only because password attempts are rate-limited
/// per source and run under the bounded signing semaphore — do not call this
/// from a path that isn't.
fn authenticate(vault: &VaultFile, admin: &str, password: &str) -> Result<Authenticated> {
    let mut named = false;
    for slot in &vault.slots {
        if slot.admin != admin {
            continue;
        }
        named = true;
        // A `must_change` slot always holds a generated Crockford password, so
        // fold the typed form back to canonical: the operator was shown it in
        // quads and may retype it that way, or in lower case. Deterministic,
        // one derivation, no second attempt — and it can never reach a
        // human-chosen password, which lives only on slots without the flag.
        let typed = fold_if_one_time(password, slot.must_change);
        let salt = b64d(&slot.kdf.salt).context("vault: slot salt")?;
        let kek = derive_kek(
            typed.as_bytes(),
            &salt,
            slot.kdf.m_cost_kib,
            slot.kdf.t_cost,
            slot.kdf.p_cost,
        )?;
        if aead_try_open(&kek, &slot.wrap)?.is_some() {
            return Ok(Authenticated {
                slot_id: slot.id,
                credential_revision: slot.credential_revision,
                admin: slot.admin.clone(),
                policy: slot.policy.clone(),
                kind: slot.kind,
                must_change: slot.must_change,
            });
        }
    }
    if !named {
        let (m, t, p) = KDF_COST;
        let _ = derive_kek(password.as_bytes(), &DUMMY_SALT, m, t, p)?;
    }
    bail!("authentication failed")
}

/// Salt for the no-such-admin derivation in [`authenticate`]. A constant is
/// fine: its only job is to make the work happen, and nothing is compared
/// against the result.
const DUMMY_SALT: [u8; 16] = [0x6e; 16];

/// Try every **signing** slot; return the index of the slot `password`
/// unlocks plus the recovered master key. Role slots are skipped — their
/// `wrap` holds a verifier, not MK, so they can never produce the key.
/// This is the cryptographic enforcement of the keyslot tiering: there is
/// no code path by which a role password reaches MK.
fn recover_mk(vault: &VaultFile, password: &str) -> Result<(usize, Zeroizing<[u8; 32]>)> {
    for (i, slot) in vault.slots.iter().enumerate() {
        if slot.kind != SlotKind::Signing {
            continue;
        }
        let salt = b64d(&slot.kdf.salt).context("vault: slot salt")?;
        let kek = derive_kek(
            password.as_bytes(),
            &salt,
            slot.kdf.m_cost_kib,
            slot.kdf.t_cost,
            slot.kdf.p_cost,
        )?;
        if let Some(mk_vec) = aead_try_open(&kek, &slot.wrap)? {
            if mk_vec.len() != 32 {
                bail!("vault: corrupt master-key wrap (len {})", mk_vec.len());
            }
            let mut mk = Zeroizing::new([0u8; 32]);
            mk.copy_from_slice(&mk_vec);
            return Ok((i, mk));
        }
    }
    bail!("no signing keyslot accepts that password")
}

/// Build a slot wrapping `secret` (MK for a signing slot, a random
/// verifier for a role slot) under a fresh Argon2id KDF of `password`.
///
/// Always `must_change: false`. The two role constructors set it after,
/// which is what keeps a signing slot from ever carrying it: no signing path
/// mentions the field, so none can set it. A one-time master key would be a
/// slot whose holder must unlock the CA to replace the credential that
/// unlocks the CA.
fn make_slot(
    secret: &[u8; 32],
    kind: SlotKind,
    admin: &str,
    password: &str,
    policy: Policy,
) -> Result<Slot> {
    if admin.is_empty() {
        bail!("admin name must not be empty");
    }
    let (m, t, p) = KDF_COST;
    let mut salt = [0u8; 16];
    rand::rng().fill_bytes(&mut salt);
    let kek = derive_kek(password.as_bytes(), &salt, m, t, p)?;
    let wrap = aead_seal(&kek, secret)?;
    Ok(Slot {
        id: uuid::Uuid::new_v4(),
        credential_revision: 0,
        admin: admin.to_string(),
        kdf: Kdf {
            kind: "argon2id".to_string(),
            salt: b64e(&salt),
            m_cost_kib: m,
            t_cost: t,
            p_cost: p,
        },
        wrap,
        kind,
        policy,
        must_change: false,
    })
}

// Counts key derivations so tests can assert the *work* an operation does,
// not just its result — the property that closes a timing oracle. Per-thread
// so tests running in parallel don't see each other's derivations.
#[cfg(test)]
thread_local! {
    static KDF_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn kdf_calls() -> usize {
    KDF_CALLS.with(std::cell::Cell::get)
}

fn derive_kek(
    password: &[u8],
    salt: &[u8],
    m: u32,
    t: u32,
    p: u32,
) -> Result<Zeroizing<[u8; 32]>> {
    #[cfg(test)]
    KDF_CALLS.with(|calls| calls.set(calls.get() + 1));
    let params = Params::new(m, t, p, Some(32))
        .map_err(|e| anyhow!("argon2 params (m={m}, t={t}, p={p}): {e}"))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut kek = Zeroizing::new([0u8; 32]);
    argon
        .hash_password_into(password, salt, &mut kek[..])
        .map_err(|e| anyhow!("argon2 derive: {e}"))?;
    Ok(kek)
}

/// AES-256-GCM seal with a fresh random 96-bit nonce.
fn aead_seal(key: &[u8; 32], plaintext: &[u8]) -> Result<AeadBlob> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let mut nonce = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| anyhow!("aes-gcm encrypt failed"))?;
    Ok(AeadBlob { nonce: b64e(&nonce), ct: b64e(&ct) })
}

/// AES-256-GCM open. `Ok(None)` is a tag mismatch (wrong key); `Err` is
/// structural corruption (bad base64 / nonce length).
fn aead_try_open(key: &[u8; 32], blob: &AeadBlob) -> Result<Option<Zeroizing<Vec<u8>>>> {
    let nonce = b64d(&blob.nonce).context("vault: nonce")?;
    if nonce.len() != 12 {
        bail!("vault: bad nonce length {}", nonce.len());
    }
    let ct = b64d(&blob.ct).context("vault: ciphertext")?;
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    match cipher.decrypt(Nonce::from_slice(&nonce), ct.as_ref()) {
        Ok(pt) => Ok(Some(Zeroizing::new(pt))),
        Err(_) => Ok(None),
    }
}

fn b64e(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

fn b64d(s: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| anyhow!("base64: {e}"))
}

async fn write_vault(path: &Path, vault: &VaultFile) -> Result<()> {
    let json = serde_json::to_vec_pretty(vault).context("serializing vault")?;
    atomic::write_atomic_async(path, &json, 0o600)
        .await
        .with_context(|| format!("writing vault {}", path.display()))
}

async fn read_vault(path: &Path) -> Result<VaultFile> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("reading vault {}", path.display()))?;
    let vault: VaultFile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing {}", path.display()))?;
    if vault.version != VAULT_VERSION {
        bail!("unsupported vault version {} (expected {VAULT_VERSION})", vault.version);
    }
    Ok(vault)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::password::{
        gen_crockford_password, group_crockford_password, normalize_crockford_password,
    };

    fn pol(san: &str) -> Policy {
        Policy {
            allowed_san: vec![san.to_string()],
            max_validity: std::time::Duration::from_secs(365 * 86400),
            id_map_groups: vec!["users".to_string()],
            server_enroll_scopes: vec![],
            server_enroll_roles: Default::default(),
            perms_edit_scopes: vec![],
            may_manage_admins: false,
            service_control_scopes: vec![],
        }
    }

    /// A role policy: no issuance, just a perms-edit scope.
    fn role_pol(scope: &str) -> Policy {
        Policy {
            allowed_san: vec![],
            max_validity: std::time::Duration::from_secs(0 * 86400),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: Default::default(),
            perms_edit_scopes: vec![scope.to_string()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        }
    }

    const KEY: &[u8] =
        b"-----BEGIN PRIVATE KEY-----\nMOCKKEYBYTES\n-----END PRIVATE KEY-----\n";

    #[tokio::test]
    async fn create_unlock_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "alice", "hunter2", pol("*.a.example")).await.unwrap();
        assert!(CAVault::exists(dir.path()));
        let u = v.unlock("hunter2").unwrap();
        assert_eq!(u.admin, "alice");
        assert_eq!(u.policy, pol("*.a.example"));
        assert_eq!(&u.ca_key_pem[..], KEY);
    }

    #[tokio::test]
    async fn wrong_password_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "alice", "hunter2", pol("*.a")).await.unwrap();
        assert!(v.unlock("wrong").is_err());
        // And it doesn't leak the key on failure (nothing to assert
        // beyond the error, but exercise the path).
        assert!(v.unlock("").is_err());
    }

    /// An unknown admin name must cost the same key derivation a wrong
    /// password does. Returning early on a name miss answers in microseconds
    /// while a real attempt spends a full Argon2 — a free oracle for
    /// enumerating the admin roster. Asserted on work done, not wall clock.
    #[tokio::test]
    async fn authentication_costs_one_kdf_whether_or_not_the_admin_exists() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "alice", "hunter2", pol("*.a")).await.unwrap();

        let before = kdf_calls();
        assert!(v.authenticate("alice", "wrong").is_err());
        let wrong_password = kdf_calls() - before;

        let before = kdf_calls();
        assert!(v.authenticate("nobody", "wrong").is_err());
        let unknown_admin = kdf_calls() - before;

        assert_eq!(wrong_password, 1);
        assert_eq!(
            unknown_admin, wrong_password,
            "an unknown admin name must not be cheaper to test than a wrong password"
        );
    }

    #[tokio::test]
    async fn create_refuses_to_clobber() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "alice", "pw", pol("*")).await.unwrap();
        assert!(v.create(KEY, "bob", "pw2", pol("*")).await.is_err());
    }

    #[tokio::test]
    async fn the_two_signing_slots_each_unlock() {
        // The server-only model keeps two signing slots: recovery + the
        // box's autorenew. Both wrap MK and unlock; both yield the same key.
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_signing_slot("rpw", "autorenew", "apw", pol("*.b")).await.unwrap();

        let r = v.unlock("rpw").unwrap();
        assert_eq!(r.admin, "recovery");
        let a = v.unlock("apw").unwrap();
        assert_eq!(a.admin, "autorenew");
        assert_eq!(&r.ca_key_pem[..], &a.ca_key_pem[..]);
        assert_eq!(v.signing_slot_names().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn replace_signing_slot_atomically_rekeys() {
        // Rotate the recovery slot, authorized by the *autorenew* slot: the old
        // recovery password stops working and the new one unlocks the same MK,
        // in a single write (the old slot is never momentarily absent).
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "old-rpw", pol("*.a")).await.unwrap();
        v.add_signing_slot("old-rpw", "autorenew", "apw", pol("*.b")).await.unwrap();
        v.replace_signing_slot("apw", "recovery", "new-rpw", pol("*.a")).await.unwrap();
        assert!(v.unlock("old-rpw").is_err(), "old recovery password must stop working");
        let r = v.unlock("new-rpw").unwrap();
        assert_eq!(r.admin, "recovery");
        // The autorenew slot is untouched; still exactly two slots, one MK.
        let a = v.unlock("apw").unwrap();
        assert_eq!(a.admin, "autorenew");
        assert_eq!(v.signing_slot_names().unwrap().len(), 2);
        assert_eq!(&r.ca_key_pem[..], &a.ca_key_pem[..]);
    }

    #[tokio::test]
    async fn replace_signing_slot_needs_signing_authority_and_leaves_slot_intact() {
        // Re-keying recovers MK first, so it needs a *signing* authority — a
        // role password or a wrong one can't do it, and the failed attempt
        // leaves the existing recovery slot untouched (no destructive window).
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_role_slot("eve", "epw", role_pol("/eu"), false).await.unwrap();
        assert!(v.replace_signing_slot("epw", "recovery", "x", pol("*")).await.is_err());
        assert!(
            v.replace_signing_slot("wrong", "recovery", "x", pol("*")).await.is_err()
        );
        assert!(v.unlock("rpw").is_ok(), "recovery slot must survive a failed re-key");
    }

    #[tokio::test]
    async fn snapshot_authentication_is_revalidated_against_the_live_slot() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_role_slot("eve", "old", role_pol("/eu"), false).await.unwrap();
        let snapshot = v.snapshot().unwrap();
        let authenticated = snapshot.authenticate("eve", "old").unwrap();

        v.remove_slot("eve", false).await.unwrap();
        v.add_role_slot("eve", "new", role_pol("/us"), false).await.unwrap();

        assert!(snapshot.authenticate("eve", "old").is_ok());
        assert!(
            v.resolve_session_slot(
                authenticated.slot_id,
                authenticated.credential_revision,
            )
            .is_err()
        );
        assert!(v.authenticate("eve", "old").is_err());
        assert_eq!(v.authenticate("eve", "new").unwrap().policy, role_pol("/us"));
    }

    #[tokio::test]
    async fn prepared_role_slot_rechecks_name_at_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        let prepared = v
            .snapshot()
            .unwrap()
            .prepare_role_slot("eve", "first", role_pol("/eu"), false)
            .unwrap();

        v.add_role_slot("eve", "second", role_pol("/us"), false).await.unwrap();

        assert!(v.add_prepared_role_slot(prepared).await.is_err());
        assert!(v.authenticate("eve", "first").is_err());
        assert!(v.authenticate("eve", "second").is_ok());
    }

    #[tokio::test]
    async fn prepared_signing_replacement_rechecks_authorizer_and_target() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_signing_slot("rpw", "autorenew", "apw", pol("*.b")).await.unwrap();
        let stale_authorizer = v
            .snapshot()
            .unwrap()
            .prepare_signing_replacement("apw", "recovery", "replacement", pol("*.a"))
            .unwrap();
        v.rekey_signing_slot("autorenew", "apw", "next-apw").await.unwrap();
        assert!(v.install_signing_replacement(stale_authorizer).await.is_err());
        assert!(v.unlock("rpw").is_ok());

        let stale_target = v
            .snapshot()
            .unwrap()
            .prepare_signing_replacement("next-apw", "recovery", "stale", pol("*.a"))
            .unwrap();
        v.replace_signing_slot("next-apw", "recovery", "current", pol("*.a"))
            .await
            .unwrap();
        assert!(v.install_signing_replacement(stale_target).await.is_err());
        assert!(v.unlock("current").is_ok());
        assert!(v.unlock("stale").is_err());
    }

    #[tokio::test]
    async fn prepared_signing_rekey_can_be_committed_and_rolled_back() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "old", pol("*.a")).await.unwrap();
        let prepared = v
            .snapshot()
            .unwrap()
            .prepare_signing_rekey("recovery", "old", "new")
            .unwrap();

        let rollback = v.install_signing_rekey(prepared).await.unwrap();
        assert!(v.unlock("old").is_err());
        assert!(v.unlock("new").is_ok());

        v.rollback_signing_rekey(rollback).await.unwrap();
        assert!(v.unlock("old").is_ok());
        assert!(v.unlock("new").is_err());
    }

    #[tokio::test]
    async fn add_signing_slot_requires_signing_authority_and_unique_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        // A password unlocking no signing slot can't mint another MK-holder.
        assert!(
            v.add_signing_slot("nope", "autorenew", "apw", pol("*.b")).await.is_err()
        );
        // A role password is not signing authority either.
        v.add_role_slot("eve", "epw", role_pol("/eu"), false).await.unwrap();
        assert!(v.add_signing_slot("epw", "autorenew", "apw", pol("*")).await.is_err());
        // Duplicate name rejected.
        assert!(v.add_signing_slot("rpw", "recovery", "x", pol("*")).await.is_err());
    }

    #[tokio::test]
    async fn role_slot_management_needs_no_password_and_cannot_reach_mk() {
        // The vault primitives carry no authority of their own (the server /
        // local FS gates them); they only ever touch non-MK plaintext.
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_role_slot("eve", "epw", role_pol("/eu"), false).await.unwrap();

        // A role authenticates and gets its scoped policy, but NEVER unlocks.
        let a = v.authenticate("eve", "epw").unwrap();
        assert_eq!(a.kind, SlotKind::Role);
        assert_eq!(a.policy, role_pol("/eu"));
        assert!(v.unlock("epw").is_err());

        // Rescoping a role — even to broad issuance authority — never hands
        // it MK: it stays a Role slot and its password still can't unlock.
        v.set_policy("eve", pol("*")).await.unwrap();
        assert_eq!(v.slot_policy("eve").unwrap().0, SlotKind::Role);
        assert!(v.unlock("epw").is_err());

        // remove_slot drops a role with no password; the signing slot stays.
        v.remove_slot("eve", false).await.unwrap();
        assert!(v.authenticate("eve", "epw").is_err());
        assert!(v.unlock("rpw").is_ok());
    }

    #[tokio::test]
    async fn remove_slot_guards_the_last_signing_slot() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_signing_slot("rpw", "autorenew", "apw", pol("*.b")).await.unwrap();
        v.add_role_slot("eve", "epw", role_pol("/eu"), false).await.unwrap();

        // Removing a role never trips the guard.
        v.remove_slot("eve", false).await.unwrap();
        // Two signing slots: removing one is fine (one MK-holder remains).
        v.remove_slot("autorenew", false).await.unwrap();
        // Now `recovery` is the only signing slot: refused without force
        // (removing it would orphan the CA key forever), allowed with it.
        assert!(v.remove_slot("recovery", false).await.is_err());
        v.remove_slot("recovery", true).await.unwrap();
        assert!(v.unlock("rpw").is_err());
    }

    #[tokio::test]
    async fn list_and_signing_slot_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_role_slot("eve", "epw", role_pol("/eu"), false).await.unwrap();
        let mut admins = v.list_admins().unwrap();
        admins.sort_by(|a, b| a.admin.cmp(&b.admin));
        let kinds: Vec<_> = admins.iter().map(|a| (a.admin.clone(), a.kind)).collect();
        assert_eq!(
            kinds,
            vec![
                ("eve".to_string(), SlotKind::Role),
                ("recovery".to_string(), SlotKind::Signing),
            ]
        );
        // Only the signing slot is an MK-holder.
        assert_eq!(v.signing_slot_names().unwrap(), vec!["recovery".to_string()]);
    }

    #[tokio::test]
    async fn reserved_names_cannot_be_role_admins() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        // A role admin may never take a reserved signing-slot name (any case).
        assert!(v.add_role_slot("recovery", "x", role_pol("/"), false).await.is_err());
        assert!(v.add_role_slot("AutoRenew", "x", role_pol("/"), false).await.is_err());
        assert!(is_reserved_admin("recovery") && is_reserved_admin("AUTORENEW"));
        assert!(!is_reserved_admin("eve"));
    }

    #[tokio::test]
    async fn a_minted_recovery_password_actually_unlocks() {
        // The canonical (ungrouped) password is the slot password; a copy
        // re-typed in grouped/confusable form normalizes back and unlocks.
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        let pw = gen_crockford_password();
        v.create(KEY, RECOVERY_ADMIN, &pw, pol("*.a")).await.unwrap();
        assert!(v.unlock(&pw).is_ok());
        let retyped = normalize_crockford_password(&group_crockford_password(&pw));
        assert!(v.unlock(&retyped).is_ok());
    }

    #[tokio::test]
    async fn pre_rbac_vault_without_kind_loads_as_signing() {
        let dir = tempfile::tempdir().unwrap();
        let mut vault = CAVault::new(dir.path().to_path_buf());
        vault.create(KEY, "alice", "apw", pol("*.a")).await.unwrap();
        // Simulate a vault written before slot tiering: drop the `kind`
        // field from every slot. The serde default must read it as Signing
        // — pre-RBAC slots all wrap MK, so they really are signing slots.
        let path = vault.vault_path();
        let mut v: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        for slot in v["slots"].as_array_mut().unwrap() {
            slot.as_object_mut().unwrap().remove("kind");
        }
        std::fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();

        let vault = CAVault::open(dir.path().to_path_buf()).await.unwrap();

        assert_eq!(vault.list_admins().unwrap()[0].kind, SlotKind::Signing);
        assert_eq!(vault.unlock("apw").unwrap().admin, "alice");
        assert_eq!(vault.authenticate("alice", "apw").unwrap().kind, SlotKind::Signing);
    }

    #[tokio::test]
    async fn tampering_with_the_key_ciphertext_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let mut vault = CAVault::new(dir.path().to_path_buf());
        vault.create(KEY, "alice", "apw", pol("*")).await.unwrap();
        // Flip a byte in key_enc.ct and confirm unlock fails rather than
        // returning garbage.
        let path = vault.vault_path();
        let mut v: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let ct = v["key_enc"]["ct"].as_str().unwrap().to_string();
        let mut bytes = base64::engine::general_purpose::STANDARD.decode(&ct).unwrap();
        bytes[0] ^= 0xff;
        v["key_enc"]["ct"] = serde_json::Value::String(b64e(&bytes));
        std::fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
        // The password still unlocks a slot (MK recovered), but the CA
        // key AEAD now fails its tag → clean error, not garbage.
        let vault = CAVault::open(dir.path().to_path_buf()).await.unwrap();
        assert!(vault.unlock("apw").is_err());
    }

    /// A rekey keeps the slot's identity and policy and replaces only the
    /// credential — and bumps `credential_revision`, which is what evicts
    /// every session issued against the old password.
    #[tokio::test]
    async fn a_role_rekey_replaces_the_credential_and_evicts_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_role_slot("eve", "old", role_pol("/eu"), false).await.unwrap();
        let before = v.authenticate("eve", "old").unwrap();
        assert!(!before.must_change);

        let prepared =
            v.snapshot().unwrap().prepare_role_rekey("eve", "new", false).unwrap();
        v.install_role_rekey(prepared).await.unwrap();

        assert!(v.authenticate("eve", "old").is_err(), "the old password must be dead");
        let after = v.authenticate("eve", "new").unwrap();
        // Same slot, same policy — this is a credential change, not a new admin.
        assert_eq!(after.slot_id, before.slot_id);
        assert_eq!(after.policy, role_pol("/eu"));
        assert_eq!(after.kind, SlotKind::Role);
        // …and every session minted against the old revision stops resolving.
        assert_eq!(after.credential_revision, before.credential_revision + 1);
        assert!(
            v.resolve_session_slot(before.slot_id, before.credential_revision).is_err()
        );
    }

    /// A prepared rekey is derived off the write lock, so it names the slot
    /// revision it was built against and refuses to install over anything
    /// else. Two administrators resetting the same admin at once must not
    /// leave the loser's password installed after the winner's.
    #[tokio::test]
    async fn a_stale_role_rekey_refuses_to_install() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v.add_role_slot("eve", "old", role_pol("/eu"), false).await.unwrap();
        let stale =
            v.snapshot().unwrap().prepare_role_rekey("eve", "mine", false).unwrap();

        let winner =
            v.snapshot().unwrap().prepare_role_rekey("eve", "theirs", false).unwrap();
        v.install_role_rekey(winner).await.unwrap();

        assert!(v.install_role_rekey(stale).await.is_err());
        assert!(v.authenticate("eve", "mine").is_err());
        assert!(v.authenticate("eve", "theirs").is_ok());
    }

    /// Reset installs a generated password and marks the slot one-time; the
    /// admin's own change clears the mark. `must_change` reaching
    /// `Authenticated` is what lets the server's single authentication funnel
    /// refuse everything else in between.
    #[tokio::test]
    async fn must_change_is_set_by_a_reset_and_cleared_by_a_change() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        let key = gen_crockford_password();
        v.add_role_slot("eve", &key, role_pol("/eu"), true).await.unwrap();
        assert!(v.authenticate("eve", &key).unwrap().must_change);
        assert!(
            v.list_admins().unwrap().iter().any(|a| a.admin == "eve" && a.must_change)
        );

        let prepared =
            v.snapshot().unwrap().prepare_role_rekey("eve", "chosen-pw", false).unwrap();
        v.install_role_rekey(prepared).await.unwrap();
        assert!(!v.authenticate("eve", "chosen-pw").unwrap().must_change);
        assert!(
            v.list_admins().unwrap().iter().any(|a| a.admin == "eve" && !a.must_change)
        );
    }

    /// A one-time key is shown in quads and uppercase; an operator may retype
    /// it that way, or in lower case, or with the substitutions Crockford
    /// exists to absorb. Only a `must_change` slot folds the input — once a
    /// human has chosen a password, it is matched exactly, so two passwords
    /// differing only in `O` versus `0` stay distinct.
    #[tokio::test]
    async fn a_one_time_key_authenticates_as_typed_but_a_chosen_password_does_not_fold() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        let key = gen_crockford_password();
        v.add_role_slot("eve", &key, role_pol("/eu"), true).await.unwrap();

        // As shown (quads), lower-cased, and with the confusable characters
        // an operator reading it aloud would produce.
        let shown = group_crockford_password(&key);
        assert!(v.authenticate("eve", &shown).is_ok());
        assert!(v.authenticate("eve", &shown.to_lowercase()).is_ok());
        assert!(
            v.authenticate("eve", &shown.replace('0', "O").replace('1', "l")).is_ok()
        );

        // The fold happens on the *store* side too, so a client that sent the
        // key in the grouped form it displayed still installs a usable
        // credential. Before that, such a slot could never be authenticated:
        // the stored derivation was of the grouped text and every attempt was
        // folded to canonical before comparison.
        let mut v3 = CAVault::new(tempfile::tempdir().unwrap().keep());
        v3.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        let grouped = group_crockford_password(&gen_crockford_password());
        v3.add_role_slot("eve", &grouped, role_pol("/eu"), true).await.unwrap();
        assert!(v3.authenticate("eve", &grouped).is_ok());
        assert!(
            v3.authenticate("eve", &normalize_crockford_password(&grouped)).is_ok(),
            "canonical and as-displayed must be the same credential"
        );

        // A chosen password is matched exactly: folding it would collapse
        // distinct passwords onto each other.
        let mut v2 = CAVault::new(tempfile::tempdir().unwrap().keep());
        v2.create(KEY, "recovery", "rpw", pol("*.a")).await.unwrap();
        v2.add_role_slot("eve", "pOlicy1", role_pol("/eu"), false).await.unwrap();
        assert!(v2.authenticate("eve", "pOlicy1").is_ok());
        assert!(v2.authenticate("eve", "p0licy1").is_err());
        assert!(v2.authenticate("eve", "policy1").is_err());
    }

    /// `recovery` and `autorenew` wrap the master key. A one-time signing slot
    /// would be a credential whose holder must unlock the CA in order to
    /// replace the credential that unlocks the CA, so the role rekey refuses
    /// them outright — the rotate ops exist for that.
    #[tokio::test]
    async fn a_signing_slot_cannot_be_role_rekeyed() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, RECOVERY_ADMIN, "rpw", pol("*.a")).await.unwrap();
        // Matched rather than `unwrap_err`: `PreparedRoleRekey` deliberately
        // has no `Debug`, since it carries a slot's key material.
        let e = match v.snapshot().unwrap().prepare_role_rekey(RECOVERY_ADMIN, "x", true)
        {
            Ok(_) => panic!("a signing slot must not be role-rekeyable"),
            Err(e) => e,
        };
        assert!(format!("{e:#}").contains("signing keyslot"), "{e:#}");
        assert!(v.unlock("rpw").is_ok(), "the signing slot must be untouched");
    }
}
