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

use crate::atomic;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce, aead::Aead};
use anyhow::{Context, Result, anyhow, bail};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

// The policy model lives in the cross-platform `ca_policy` module (a
// Windows admin client carries these types over the conf plane). Re-export
// so existing `ca_vault::Policy` / `SlotKind` / `AdminInfo` paths in unix
// code keep working and the vault body can name them unqualified.
pub use crate::ca_policy::{AdminInfo, Policy, SlotKind};

/// File name of the vault within a CA directory. Its presence (rather
/// than a bare `private.key`) marks a CA as vault-protected.
pub const VAULT_FILE: &str = "vault.json";
const VAULT_VERSION: u32 = 1;

/// The off-box break-glass signing credential the operator stores in a
/// safe — the only key-recovery credential that ever leaves the box.
/// Minted once at init via [`create`]; rotated on-box via
/// `conf ca recovery rotate`. With [`crate::conf_server::AUTORENEW_ADMIN`]
/// these are the only two signing (master-key-holding) slots.
pub const RECOVERY_ADMIN: &str = "recovery";

/// True if `name` is one of the two reserved signing-slot names
/// (`recovery`, `autorenew`). A role admin may never take either: they name
/// the master-key holders, and a role admin shadowing one would muddy who
/// authorizes what.
pub fn is_reserved_admin(name: &str) -> bool {
    name.eq_ignore_ascii_case(RECOVERY_ADMIN)
        || name.eq_ignore_ascii_case(crate::conf_server::AUTORENEW_ADMIN)
}

/// Crockford base32 alphabet (digits + uppercase, excluding I L O U — the
/// characters easiest to confuse written down or read aloud). The recovery
/// password is rendered in this alphabet so it survives a trip through a
/// safe and a human's handwriting.
const CROCKFORD32: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Bytes of entropy in a recovery password — 160 bits (a multiple of 5, so
/// it renders to exactly 32 base32 chars with no padding).
const RECOVERY_ENTROPY_BYTES: usize = 20;

/// A long random password for a **signing** slot (the box-held `autorenew`
/// credential), kept in a `Zeroizing` buffer that wipes on drop. Unlike a
/// recovery password this is never read by a human — it lives only in the
/// sealed keytab and in process memory — so it is plain hex with no
/// confusable-character handling. Any signing-slot password unlocks MK, so
/// treat it like the CA key.
pub fn random_signing_password() -> Zeroizing<String> {
    Zeroizing::new(format!("{}{}", crate::ca_store::new_id(), crate::ca_store::new_id()))
}

/// Generate a fresh recovery-slot password: 160 bits rendered as 32
/// Crockford base32 characters. This canonical string is the actual slot
/// password; [`group_recovery_password`] renders it in quads for the
/// operator to copy, and [`normalize_recovery_password`] folds a re-typed
/// copy back to it. Never persisted — printed exactly once at init/rotate.
pub fn gen_recovery_password() -> Zeroizing<String> {
    let mut bytes = Zeroizing::new([0u8; RECOVERY_ENTROPY_BYTES]);
    rand::rng().fill_bytes(&mut bytes[..]);
    let mut out = String::with_capacity(32);
    let (mut acc, mut bits) = (0u16, 0u32);
    for &b in bytes.iter() {
        acc = (acc << 8) | b as u16;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(CROCKFORD32[((acc >> bits) & 0x1f) as usize] as char);
        }
    }
    // 160 bits / 5 == 32 chars exactly; no leftover bits to pad.
    Zeroizing::new(out)
}

/// Render a recovery password in 4-character quads separated by spaces
/// (e.g. `45QD 567D 8H2K …`) for the boxed one-time display. Grouping only
/// aids transcription; [`normalize_recovery_password`] strips it back out.
/// The result is `Zeroizing` (it holds the full secret) — the same care
/// [`gen_recovery_password`] takes, kept across this hop.
pub fn group_recovery_password(pw: &str) -> Zeroizing<String> {
    let mut out = String::with_capacity(pw.len() + pw.len() / 4);
    for (i, c) in pw.chars().enumerate() {
        if i > 0 && i % 4 == 0 {
            out.push(' ');
        }
        out.push(c);
    }
    Zeroizing::new(out)
}

/// Fold an operator-typed recovery password back to the canonical form
/// [`gen_recovery_password`] produced: drop whitespace and hyphens,
/// uppercase, and apply Crockford's digit substitutions (O→0, I/L→1) so a
/// transcription that confused those characters still unlocks. The result is
/// `Zeroizing` — it is the secret that goes to `unlock`.
pub fn normalize_recovery_password(typed: &str) -> Zeroizing<String> {
    Zeroizing::new(
        typed
            .chars()
            .filter_map(|c| match c.to_ascii_uppercase() {
                ' ' | '-' | '\t' | '\n' | '\r' => None,
                'O' => Some('0'),
                'I' | 'L' => Some('1'),
                c => Some(c),
            })
            .collect(),
    )
}

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
    pub admin: String,
    pub policy: Policy,
    pub ca_key_pem: Zeroizing<Vec<u8>>,
}

/// The result of a successful [`authenticate`]: who the password belongs
/// to, what they may do, and which keyslot tier they hold — but NO CA
/// key. Every non-signing admin op authorizes against this.
pub struct Authenticated {
    pub admin: String,
    pub policy: Policy,
    pub kind: SlotKind,
}

#[derive(Serialize, Deserialize)]
struct AeadBlob {
    nonce: String, // base64, 12 bytes
    ct: String,    // base64, ciphertext || tag
}

#[derive(Serialize, Deserialize)]
struct Kdf {
    #[serde(rename = "type")]
    kind: String, // "argon2id"
    salt: String, // base64, 16 bytes
    m_cost_kib: u32,
    t_cost: u32,
    p_cost: u32,
}

#[derive(Serialize, Deserialize)]
struct Slot {
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
}

#[derive(Serialize, Deserialize)]
struct VaultFile {
    version: u32,
    key_enc: AeadBlob, // AES-256-GCM(MK, ca_key_pkcs8_pem)
    slots: Vec<Slot>,
}

/// A CA directory's keyslot vault (`<ca-dir>/<VAULT_FILE>`). Reached
/// through [`crate::ca_store::CaDir`], which holds the dir's exclusive lock;
/// read methods take `&self`, write methods `&mut self`, so the borrow
/// checker enforces exclusion rather than a remembered global lock.
pub struct CAVault {
    dir: PathBuf,
}

impl CAVault {
    /// A file-backed handle to the vault at `dir`. `pub(crate)` on purpose:
    /// the only way to reach a vault from outside this crate is through
    /// [`crate::ca_store::CaDir`], which holds the dir's exclusive flock. That
    /// makes the daemon (and `ca init` / offline issuance, which open a
    /// `CaDir` themselves) the only writers — no CLI can touch the vault
    /// behind the daemon's back. The vault is stateless (every method re-reads
    /// the file), so the handle itself takes no lock; the flock lives on the
    /// `CaDir` that owns it.
    pub(crate) fn new(dir: PathBuf) -> Self {
        CAVault { dir }
    }

    fn vault_path(&self) -> PathBuf {
        self.dir.join(VAULT_FILE)
    }

    /// True if `dir` holds a vault-protected CA. A pre-open check (used to
    /// decide whether to init), so it takes the path directly rather than a
    /// constructed (locked) vault.
    pub fn exists(dir: &Path) -> bool {
        dir.join(VAULT_FILE).exists()
    }

    /// Create a new vault: generate a master key, encrypt `ca_key_pem` (an
    /// unencrypted PKCS#8 PEM) under it, and write the first admin's slot.
    /// Refuses to clobber an existing vault.
    pub fn create(
        &mut self,
        ca_key_pem: &[u8],
        admin: &str,
        password: &str,
        policy: Policy,
    ) -> Result<()> {
        let path = self.vault_path();
        if path.exists() {
            bail!("a vault already exists at {}", path.display());
        }
        let mut mk = Zeroizing::new([0u8; 32]);
        rand::rng().fill_bytes(&mut mk[..]);
        let key_enc = aead_seal(&mk, ca_key_pem)?;
        let slot = make_slot(&mk, SlotKind::Signing, admin, password, policy)?;
        write_vault(
            &path,
            &VaultFile { version: VAULT_VERSION, key_enc, slots: vec![slot] },
        )
    }

    /// Recover the CA key (and the unlocking admin's identity + policy)
    /// using `password`. A wrong password — one that no slot accepts — is a
    /// clean error, not a panic; the GCM tag on each slot's wrap is the
    /// password check.
    pub fn unlock(&self, password: &str) -> Result<Unlocked> {
        let vault = read_vault(&self.vault_path())?;
        let (slot, mk) = recover_mk(&vault, password)?;
        let ca_key_pem = match aead_try_open(&mk, &vault.key_enc)? {
            Some(pt) => pt,
            None => {
                bail!("vault: master key does not decrypt the CA key (corrupt vault)")
            }
        };
        Ok(Unlocked {
            admin: vault.slots[slot].admin.clone(),
            policy: vault.slots[slot].policy.clone(),
            ca_key_pem,
        })
    }

    /// Authenticate `admin` by `password` against any keyslot (signing or
    /// role) and return their policy and tier — but never the CA key. The
    /// password proof is the GCM tag on the slot's `wrap`; we don't care what
    /// it wraps, only that it opens. This is the auth primitive for every
    /// admin op that doesn't sign a certificate (perms edits, map admin), so
    /// a role keyslot is a first-class admin for those ops without ever
    /// touching the CA private key.
    pub fn authenticate(&self, admin: &str, password: &str) -> Result<Authenticated> {
        let vault = read_vault(&self.vault_path())?;
        for slot in &vault.slots {
            if slot.admin != admin {
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
            if aead_try_open(&kek, &slot.wrap)?.is_some() {
                return Ok(Authenticated {
                    admin: slot.admin.clone(),
                    policy: slot.policy.clone(),
                    kind: slot.kind,
                });
            }
        }
        bail!("authentication failed")
    }

    /// Add a **signing** slot — one that wraps MK and so can recover the CA
    /// key. DANGER: a new signing slot is a new MK-holder, exactly the thing
    /// the two-slot model minimises; never call this from a wire-exposed
    /// handler. `existing_password` must unlock an existing signing slot (the
    /// bootstrap authority). In the server-signs model the only routine caller
    /// is autorenew setup (re-minting the box's `autorenew` slot, authorized
    /// by the `recovery` password); init mints `recovery` via [`create`](Self::create).
    pub fn add_signing_slot(
        &mut self,
        existing_password: &str,
        new_admin: &str,
        new_password: &str,
        policy: Policy,
    ) -> Result<()> {
        let path = self.vault_path();
        let mut vault = read_vault(&path)?;
        if vault.slots.iter().any(|s| s.admin == new_admin) {
            bail!("an admin named {new_admin:?} already exists");
        }
        let (_slot, mk) = recover_mk(&vault, existing_password)?;
        let slot = make_slot(&mk, SlotKind::Signing, new_admin, new_password, policy)?;
        vault.slots.push(slot);
        write_vault(&path, &vault)
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
    pub fn add_role_slot(
        &mut self,
        new_admin: &str,
        new_password: &str,
        policy: Policy,
    ) -> Result<()> {
        if is_reserved_admin(new_admin) {
            bail!(
                "{new_admin:?} is a reserved signing-slot name (recovery / autorenew); \
                 choose another name for a role admin"
            );
        }
        let path = self.vault_path();
        let mut vault = read_vault(&path)?;
        if vault.slots.iter().any(|s| s.admin == new_admin) {
            bail!("an admin named {new_admin:?} already exists");
        }
        let mut verifier = Zeroizing::new([0u8; 32]);
        rand::rng().fill_bytes(&mut verifier[..]);
        let slot = make_slot(&verifier, SlotKind::Role, new_admin, new_password, policy)?;
        vault.slots.push(slot);
        write_vault(&path, &vault)
    }

    /// Remove (revoke) an admin's slot. A low-level primitive — authority
    /// lives in the caller (local FS / wire `may_manage_admins`). Refuses to
    /// remove the last remaining **signing** slot unless `force`: that is the
    /// only MK-holder, and removing it orphans the CA key forever (no password
    /// could ever unlock it again). Removing a role slot never trips the
    /// guard.
    pub fn remove_slot(&mut self, target_admin: &str, force: bool) -> Result<()> {
        let path = self.vault_path();
        let mut vault = read_vault(&path)?;
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
        write_vault(&path, &vault)
    }

    /// Replace the [`Policy`] on `target_admin`'s slot. A low-level primitive
    /// — authority lives in the caller (local FS / wire `may_manage_admins` +
    /// subset check). It rewrites **only** the (plaintext) `policy` field; the
    /// slot's `kind` and `wrap` are untouched, so it can never promote a Role
    /// slot to Signing (a Role admin can be rescoped but never handed MK).
    pub fn set_policy(&mut self, target_admin: &str, policy: Policy) -> Result<()> {
        let path = self.vault_path();
        let mut vault = read_vault(&path)?;
        let idx = vault
            .slots
            .iter()
            .position(|s| s.admin == target_admin)
            .ok_or_else(|| anyhow!("no admin named {target_admin:?}"))?;
        vault.slots[idx].policy = policy;
        write_vault(&path, &vault)
    }

    /// List the admins, their tiers, and their policies (no secrets).
    pub fn list_admins(&self) -> Result<Vec<AdminInfo>> {
        let vault = read_vault(&self.vault_path())?;
        Ok(vault
            .slots
            .into_iter()
            .map(|s| AdminInfo { admin: s.admin, kind: s.kind, policy: s.policy })
            .collect())
    }

    /// The names of every MK-wrapping (`Signing`) slot. The server-only model
    /// keeps this to exactly `{recovery, autorenew}`; anything else is a
    /// backup-crackable extra key-holder. Used by init/recovery + as an
    /// invariant check.
    pub fn signing_slot_names(&self) -> Result<Vec<String>> {
        let vault = read_vault(&self.vault_path())?;
        Ok(vault
            .slots
            .iter()
            .filter(|s| s.kind == SlotKind::Signing)
            .map(|s| s.admin.clone())
            .collect())
    }

    /// One slot's tier + policy (no secrets), by admin name.
    pub fn slot_policy(&self, admin: &str) -> Result<(SlotKind, Policy)> {
        let vault = read_vault(&self.vault_path())?;
        vault
            .slots
            .iter()
            .find(|s| s.admin == admin)
            .map(|s| (s.kind, s.policy.clone()))
            .ok_or_else(|| anyhow!("no admin named {admin:?}"))
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
    pub fn rekey_signing_slot(
        &mut self,
        target_admin: &str,
        old_password: &str,
        new_password: &str,
    ) -> Result<()> {
        let path = self.vault_path();
        let mut vault = read_vault(&path)?;
        let (idx, mk) = recover_mk(&vault, old_password)?;
        if vault.slots[idx].admin != target_admin {
            bail!(
                "that password unlocks {:?}, not {target_admin:?}; refusing to rekey a \
                 different slot",
                vault.slots[idx].admin
            );
        }
        let policy = vault.slots[idx].policy.clone();
        vault.slots[idx] =
            make_slot(&mk, SlotKind::Signing, target_admin, new_password, policy)?;
        write_vault(&path, &vault)
    }
}

// -- internals ----------------------------------------------------------------

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
    })
}

fn derive_kek(
    password: &[u8],
    salt: &[u8],
    m: u32,
    t: u32,
    p: u32,
) -> Result<Zeroizing<[u8; 32]>> {
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

fn write_vault(path: &Path, vault: &VaultFile) -> Result<()> {
    let json = serde_json::to_vec_pretty(vault).context("serializing vault")?;
    atomic::write_atomic(path, &json, 0o600)
        .with_context(|| format!("writing vault {}", path.display()))
}

fn read_vault(path: &Path) -> Result<VaultFile> {
    let bytes = std::fs::read(path)
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

    fn pol(san: &str) -> Policy {
        Policy {
            allowed_san: vec![san.to_string()],
            max_validity: std::time::Duration::from_secs(365 * 86400),
            id_map_groups: vec!["users".to_string()],
            may_enroll_servers: false,
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
            may_enroll_servers: false,
            perms_edit_scopes: vec![scope.to_string()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        }
    }

    const KEY: &[u8] =
        b"-----BEGIN PRIVATE KEY-----\nMOCKKEYBYTES\n-----END PRIVATE KEY-----\n";

    #[test]
    fn create_unlock_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "alice", "hunter2", pol("*.a.example")).unwrap();
        assert!(CAVault::exists(dir.path()));
        let u = v.unlock("hunter2").unwrap();
        assert_eq!(u.admin, "alice");
        assert_eq!(u.policy, pol("*.a.example"));
        assert_eq!(&u.ca_key_pem[..], KEY);
    }

    #[test]
    fn wrong_password_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "alice", "hunter2", pol("*.a")).unwrap();
        assert!(v.unlock("wrong").is_err());
        // And it doesn't leak the key on failure (nothing to assert
        // beyond the error, but exercise the path).
        assert!(v.unlock("").is_err());
    }

    #[test]
    fn create_refuses_to_clobber() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "alice", "pw", pol("*")).unwrap();
        assert!(v.create(KEY, "bob", "pw2", pol("*")).is_err());
    }

    #[test]
    fn the_two_signing_slots_each_unlock() {
        // The server-only model keeps two signing slots: recovery + the
        // box's autorenew. Both wrap MK and unlock; both yield the same key.
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).unwrap();
        v.add_signing_slot("rpw", "autorenew", "apw", pol("*.b")).unwrap();

        let r = v.unlock("rpw").unwrap();
        assert_eq!(r.admin, "recovery");
        let a = v.unlock("apw").unwrap();
        assert_eq!(a.admin, "autorenew");
        assert_eq!(&r.ca_key_pem[..], &a.ca_key_pem[..]);
        assert_eq!(v.signing_slot_names().unwrap().len(), 2);
    }

    #[test]
    fn add_signing_slot_requires_signing_authority_and_unique_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).unwrap();
        // A password unlocking no signing slot can't mint another MK-holder.
        assert!(v.add_signing_slot("nope", "autorenew", "apw", pol("*.b")).is_err());
        // A role password is not signing authority either.
        v.add_role_slot("eve", "epw", role_pol("/eu")).unwrap();
        assert!(v.add_signing_slot("epw", "autorenew", "apw", pol("*")).is_err());
        // Duplicate name rejected.
        assert!(v.add_signing_slot("rpw", "recovery", "x", pol("*")).is_err());
    }

    #[test]
    fn role_slot_management_needs_no_password_and_cannot_reach_mk() {
        // The vault primitives carry no authority of their own (the server /
        // local FS gates them); they only ever touch non-MK plaintext.
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).unwrap();
        v.add_role_slot("eve", "epw", role_pol("/eu")).unwrap();

        // A role authenticates and gets its scoped policy, but NEVER unlocks.
        let a = v.authenticate("eve", "epw").unwrap();
        assert_eq!(a.kind, SlotKind::Role);
        assert_eq!(a.policy, role_pol("/eu"));
        assert!(v.unlock("epw").is_err());

        // Rescoping a role — even to broad issuance authority — never hands
        // it MK: it stays a Role slot and its password still can't unlock.
        v.set_policy("eve", pol("*")).unwrap();
        assert_eq!(v.slot_policy("eve").unwrap().0, SlotKind::Role);
        assert!(v.unlock("epw").is_err());

        // remove_slot drops a role with no password; the signing slot stays.
        v.remove_slot("eve", false).unwrap();
        assert!(v.authenticate("eve", "epw").is_err());
        assert!(v.unlock("rpw").is_ok());
    }

    #[test]
    fn remove_slot_guards_the_last_signing_slot() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).unwrap();
        v.add_signing_slot("rpw", "autorenew", "apw", pol("*.b")).unwrap();
        v.add_role_slot("eve", "epw", role_pol("/eu")).unwrap();

        // Removing a role never trips the guard.
        v.remove_slot("eve", false).unwrap();
        // Two signing slots: removing one is fine (one MK-holder remains).
        v.remove_slot("autorenew", false).unwrap();
        // Now `recovery` is the only signing slot: refused without force
        // (removing it would orphan the CA key forever), allowed with it.
        assert!(v.remove_slot("recovery", false).is_err());
        v.remove_slot("recovery", true).unwrap();
        assert!(v.unlock("rpw").is_err());
    }

    #[test]
    fn list_and_signing_slot_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).unwrap();
        v.add_role_slot("eve", "epw", role_pol("/eu")).unwrap();
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

    #[test]
    fn reserved_names_cannot_be_role_admins() {
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        v.create(KEY, "recovery", "rpw", pol("*.a")).unwrap();
        // A role admin may never take a reserved signing-slot name (any case).
        assert!(v.add_role_slot("recovery", "x", role_pol("/")).is_err());
        assert!(v.add_role_slot("AutoRenew", "x", role_pol("/")).is_err());
        assert!(is_reserved_admin("recovery") && is_reserved_admin("AUTORENEW"));
        assert!(!is_reserved_admin("eve"));
    }

    #[test]
    fn recovery_password_roundtrips_through_display_and_renentry() {
        let pw = gen_recovery_password();
        // 160 bits of Crockford base32 == 32 chars from the alphabet.
        assert_eq!(pw.len(), 32);
        assert!(pw.chars().all(|c| CROCKFORD32.contains(&(c as u8))));
        // Grouped for display: 8 quads separated by 7 spaces.
        let shown = group_recovery_password(&pw);
        assert_eq!(shown.split(' ').count(), 8);
        assert!(shown.split(' ').all(|q| q.len() == 4));
        // Re-typing the grouped form (or with confusable chars) folds back.
        assert_eq!(*normalize_recovery_password(&shown), *pw);
        // Two fresh passwords differ (RNG is actually consulted).
        assert_ne!(*gen_recovery_password(), *pw);
        // Crockford leniency: O→0, I/L→1, lowercase, stray hyphens.
        assert_eq!(normalize_recovery_password("o0-iI lL-ab").as_str(), "001111AB");
    }

    #[test]
    fn a_minted_recovery_password_actually_unlocks() {
        // The canonical (ungrouped) password is the slot password; a copy
        // re-typed in grouped/confusable form normalizes back and unlocks.
        let dir = tempfile::tempdir().unwrap();
        let mut v = CAVault::new(dir.path().to_path_buf());
        let pw = gen_recovery_password();
        v.create(KEY, RECOVERY_ADMIN, &pw, pol("*.a")).unwrap();
        assert!(v.unlock(&pw).is_ok());
        let retyped = normalize_recovery_password(&group_recovery_password(&pw));
        assert!(v.unlock(&retyped).is_ok());
    }

    #[test]
    fn pre_rbac_vault_without_kind_loads_as_signing() {
        let dir = tempfile::tempdir().unwrap();
        let mut vault = CAVault::new(dir.path().to_path_buf());
        vault.create(KEY, "alice", "apw", pol("*.a")).unwrap();
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

        assert_eq!(vault.list_admins().unwrap()[0].kind, SlotKind::Signing);
        assert_eq!(vault.unlock("apw").unwrap().admin, "alice");
        assert_eq!(vault.authenticate("alice", "apw").unwrap().kind, SlotKind::Signing);
    }

    #[test]
    fn tampering_with_the_key_ciphertext_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let mut vault = CAVault::new(dir.path().to_path_buf());
        vault.create(KEY, "alice", "apw", pol("*")).unwrap();
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
        assert!(vault.unlock("apw").is_err());
    }
}
