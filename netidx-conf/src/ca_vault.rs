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
use aes_gcm::{aead::Aead, Aes256Gcm, Key, KeyInit, Nonce};
use anyhow::{anyhow, bail, Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

/// File name of the vault within a CA directory. Its presence (rather
/// than a bare `private.key`) marks a CA as vault-protected.
pub const VAULT_FILE: &str = "vault.json";
const VAULT_VERSION: u32 = 1;

// Argon2id cost. Per the design doc: 64 MiB / t=3 / p=4 in production;
// the params are stored *per slot*, so `unlock` always uses whatever a
// slot was created with — which lets the test build derive cheaply
// without weakening the real format.
#[cfg(not(test))]
const KDF_COST: (u32, u32, u32) = (65536, 3, 4);
#[cfg(test)]
const KDF_COST: (u32, u32, u32) = (32, 1, 1);

/// Per-admin issuance policy, stored in that admin's slot. Recovered by
/// [`unlock`] so the server can authorize a request against exactly the
/// admin who unlocked it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    /// Glob patterns every requested SAN must match. Empty ⇒ deny all
    /// (issuance scope must be granted explicitly; no accidental
    /// allow-everything).
    pub allowed_san: Vec<String>,
    pub max_validity_days: u32,
}

/// The result of a successful [`unlock`]: the admin identified by the
/// password, their policy, and the decrypted CA key. The key bytes are
/// zeroized on drop — hold the `Unlocked` only as long as needed to
/// sign one request.
pub struct Unlocked {
    pub admin: String,
    pub policy: Policy,
    pub ca_key_pem: Zeroizing<Vec<u8>>,
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
    wrap: AeadBlob, // AES-256-GCM(KEK, MK)
    policy: Policy,
}

#[derive(Serialize, Deserialize)]
struct VaultFile {
    version: u32,
    key_enc: AeadBlob, // AES-256-GCM(MK, ca_key_pkcs8_pem)
    slots: Vec<Slot>,
}

fn vault_path(ca_dir: &Path) -> PathBuf {
    ca_dir.join(VAULT_FILE)
}

/// True if `ca_dir` holds a vault-protected CA.
pub fn exists(ca_dir: &Path) -> bool {
    vault_path(ca_dir).exists()
}

/// Create a new vault in `ca_dir`: generate a master key, encrypt
/// `ca_key_pem` (an unencrypted PKCS#8 PEM) under it, and write the
/// first admin's slot. Refuses to clobber an existing vault.
pub fn create(
    ca_dir: &Path,
    ca_key_pem: &[u8],
    admin: &str,
    password: &str,
    policy: Policy,
) -> Result<()> {
    let path = vault_path(ca_dir);
    if path.exists() {
        bail!("a vault already exists at {}", path.display());
    }
    let mut mk = Zeroizing::new([0u8; 32]);
    rand::rng().fill_bytes(&mut mk[..]);
    let key_enc = aead_seal(&mk, ca_key_pem)?;
    let slot = make_slot(&mk, admin, password, policy)?;
    write_vault(&path, &VaultFile { version: VAULT_VERSION, key_enc, slots: vec![slot] })
}

/// Recover the CA key (and the unlocking admin's identity + policy)
/// from `ca_dir` using `password`. A wrong password — one that no slot
/// accepts — is a clean error, not a panic; the GCM tag on each slot's
/// wrap is the password check.
pub fn unlock(ca_dir: &Path, password: &str) -> Result<Unlocked> {
    let vault = read_vault(&vault_path(ca_dir))?;
    let (slot, mk) = recover_mk(&vault, password)?;
    let ca_key_pem = match aead_try_open(&mk, &vault.key_enc)? {
        Some(pt) => pt,
        None => bail!("vault: master key does not decrypt the CA key (corrupt vault)"),
    };
    Ok(Unlocked {
        admin: vault.slots[slot].admin.clone(),
        policy: vault.slots[slot].policy.clone(),
        ca_key_pem,
    })
}

/// Add an admin slot. `existing_password` must unlock an existing slot
/// (proof of authority); the new admin gets `new_password` and
/// `policy`. The CA key is untouched.
pub fn add_admin(
    ca_dir: &Path,
    existing_password: &str,
    new_admin: &str,
    new_password: &str,
    policy: Policy,
) -> Result<()> {
    let path = vault_path(ca_dir);
    let mut vault = read_vault(&path)?;
    if vault.slots.iter().any(|s| s.admin == new_admin) {
        bail!("an admin named {new_admin:?} already exists");
    }
    let (_slot, mk) = recover_mk(&vault, existing_password)?;
    let slot = make_slot(&mk, new_admin, new_password, policy)?;
    vault.slots.push(slot);
    write_vault(&path, &vault)
}

/// Remove (revoke) an admin's slot. `authorizing_password` must unlock
/// some slot. Refuses to remove the last remaining slot unless `force`
/// (that would lock the CA permanently).
pub fn remove_admin(
    ca_dir: &Path,
    authorizing_password: &str,
    target_admin: &str,
    force: bool,
) -> Result<()> {
    let path = vault_path(ca_dir);
    let mut vault = read_vault(&path)?;
    let idx = vault
        .slots
        .iter()
        .position(|s| s.admin == target_admin)
        .ok_or_else(|| anyhow!("no admin named {target_admin:?}"))?;
    if vault.slots.len() == 1 && !force {
        bail!(
            "refusing to remove the last admin {target_admin:?}: that would lock \
             the CA permanently. Pass --force if you really mean to."
        );
    }
    // Authority: the caller must know some valid admin password.
    recover_mk(&vault, authorizing_password)
        .context("authorizing password does not unlock any slot")?;
    vault.slots.remove(idx);
    write_vault(&path, &vault)
}

/// List the admins and their policies (no secrets).
pub fn list_admins(ca_dir: &Path) -> Result<Vec<(String, Policy)>> {
    let vault = read_vault(&vault_path(ca_dir))?;
    Ok(vault.slots.into_iter().map(|s| (s.admin, s.policy)).collect())
}

// -- internals ----------------------------------------------------------------

/// Try every slot; return the index of the slot `password` unlocks plus
/// the recovered master key.
fn recover_mk(vault: &VaultFile, password: &str) -> Result<(usize, Zeroizing<[u8; 32]>)> {
    for (i, slot) in vault.slots.iter().enumerate() {
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
    bail!("no key slot accepts that password")
}

fn make_slot(mk: &[u8; 32], admin: &str, password: &str, policy: Policy) -> Result<Slot> {
    if admin.is_empty() {
        bail!("admin name must not be empty");
    }
    let (m, t, p) = KDF_COST;
    let mut salt = [0u8; 16];
    rand::rng().fill_bytes(&mut salt);
    let kek = derive_kek(password.as_bytes(), &salt, m, t, p)?;
    let wrap = aead_seal(&kek, mk)?;
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
    let vault: VaultFile =
        serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?;
    if vault.version != VAULT_VERSION {
        bail!("unsupported vault version {} (expected {VAULT_VERSION})", vault.version);
    }
    Ok(vault)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pol(san: &str) -> Policy {
        Policy { allowed_san: vec![san.to_string()], max_validity_days: 365 }
    }

    const KEY: &[u8] = b"-----BEGIN PRIVATE KEY-----\nMOCKKEYBYTES\n-----END PRIVATE KEY-----\n";

    #[test]
    fn create_unlock_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "hunter2", pol("*.a.example")).unwrap();
        assert!(exists(dir.path()));
        let u = unlock(dir.path(), "hunter2").unwrap();
        assert_eq!(u.admin, "alice");
        assert_eq!(u.policy, pol("*.a.example"));
        assert_eq!(&u.ca_key_pem[..], KEY);
    }

    #[test]
    fn wrong_password_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "hunter2", pol("*.a")).unwrap();
        assert!(unlock(dir.path(), "wrong").is_err());
        // And it doesn't leak the key on failure (nothing to assert
        // beyond the error, but exercise the path).
        assert!(unlock(dir.path(), "").is_err());
    }

    #[test]
    fn create_refuses_to_clobber() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "pw", pol("*")).unwrap();
        assert!(create(dir.path(), KEY, "bob", "pw2", pol("*")).is_err());
    }

    #[test]
    fn multiple_admins_each_unlock_with_their_own_policy() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "apw", pol("*.a")).unwrap();
        add_admin(dir.path(), "apw", "bob", "bpw", pol("*.b")).unwrap();

        let a = unlock(dir.path(), "apw").unwrap();
        assert_eq!(a.admin, "alice");
        assert_eq!(a.policy, pol("*.a"));
        let b = unlock(dir.path(), "bpw").unwrap();
        assert_eq!(b.admin, "bob");
        assert_eq!(b.policy, pol("*.b"));
        // Both recover the same CA key.
        assert_eq!(&a.ca_key_pem[..], &b.ca_key_pem[..]);
    }

    #[test]
    fn add_admin_requires_an_existing_password_and_unique_name() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "apw", pol("*.a")).unwrap();
        // Wrong existing password can't add.
        assert!(add_admin(dir.path(), "nope", "bob", "bpw", pol("*.b")).is_err());
        // Duplicate name rejected.
        assert!(add_admin(dir.path(), "apw", "alice", "x", pol("*")).is_err());
    }

    #[test]
    fn remove_admin_revokes_and_guards_last_slot() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "apw", pol("*.a")).unwrap();
        add_admin(dir.path(), "apw", "bob", "bpw", pol("*.b")).unwrap();

        // Authority is required.
        assert!(remove_admin(dir.path(), "nope", "bob", false).is_err());
        // Revoke bob using alice's password.
        remove_admin(dir.path(), "apw", "bob", false).unwrap();
        assert!(unlock(dir.path(), "bpw").is_err());
        assert!(unlock(dir.path(), "apw").is_ok());

        // Can't remove the last slot without force.
        assert!(remove_admin(dir.path(), "apw", "alice", false).is_err());
        remove_admin(dir.path(), "apw", "alice", true).unwrap();
        assert!(unlock(dir.path(), "apw").is_err());
    }

    #[test]
    fn list_admins_reports_names_and_policies() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "apw", pol("*.a")).unwrap();
        add_admin(dir.path(), "apw", "bob", "bpw", pol("*.b")).unwrap();
        let mut admins = list_admins(dir.path()).unwrap();
        admins.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(admins, vec![
            ("alice".to_string(), pol("*.a")),
            ("bob".to_string(), pol("*.b")),
        ]);
    }

    #[test]
    fn tampering_with_the_key_ciphertext_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), KEY, "alice", "apw", pol("*")).unwrap();
        // Flip a byte in key_enc.ct and confirm unlock fails rather than
        // returning garbage.
        let path = vault_path(dir.path());
        let mut v: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let ct = v["key_enc"]["ct"].as_str().unwrap().to_string();
        let mut bytes = base64::engine::general_purpose::STANDARD.decode(&ct).unwrap();
        bytes[0] ^= 0xff;
        v["key_enc"]["ct"] =
            serde_json::Value::String(b64e(&bytes));
        std::fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
        // The password still unlocks a slot (MK recovered), but the CA
        // key AEAD now fails its tag → clean error, not garbage.
        assert!(unlock(dir.path(), "apw").is_err());
    }
}
