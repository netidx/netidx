//! Seal a secret to this host's TPM 2.0.
//!
//! netidx uses this to bind credentials to a machine: TLS private-key
//! passwords (`<key>.tpm` sidecars read by `netidx::tls`) and the CA
//! autorenew keytab. A sealed blob written to disk is inert anywhere
//! else — stolen disks, leaked backups, and decommissioned drives
//! recover nothing. It does NOT defend against an attacker with code
//! execution on the live host (they can ask the TPM to unseal, just as
//! they could have read a plaintext secret) — that boundary is
//! unchanged.
//!
//! Mechanism: the secret is encrypted under a fresh AES-256-GCM key,
//! and that key — 32 bytes, comfortably inside the 128 a TPM 2.0
//! guarantees per sealed object — is wrapped in a TPM `KeyedHash`
//! sealed-data object under the standard ECC P-256 storage root key
//! (SRK) template on the owner hierarchy. The ciphertext rides in the
//! same blob. Sealing the key rather than the secret is what lets
//! [`seal`] take a secret of any size: a TLS key password is 64 bytes,
//! a cached administrator session is ~300, and only one of those would
//! fit in a sealed object.
//!
//! The SRK is re-derived by `CreatePrimary` on every operation — same
//! seed + same template ⇒ same key — so nothing is persisted in the TPM
//! and no NV/owner-auth bookkeeping exists to rot. The sealed object
//! carries an empty auth value: the protection is the TPM binding, not
//! a second password (which would just move the problem to "where do we
//! store that").
//!
//! Deliberately **no PCR binding**: a PCR-bound blob silently stops
//! unsealing after a firmware update, and a daemon that silently stops
//! is an outage on a delay timer. The recovery story for any unseal
//! failure (TPM cleared, board swapped) is re-issue/re-seal — netidx
//! certificate issuance is one command, so keys are disposable.
//!
//! The command marshalling is pure Rust ([`tpm2-protocol`]) and
//! compiles everywhere; only the *transport* is per-platform: the
//! linux kernel resource manager (`/dev/tpmrm0`, note the device node
//! is conventionally root:tss) and Windows TPM Base Services
//! (`Tbsip_Submit_Command` takes the same raw frames).
//!
//! macOS has no TPM; there the same [`seal`]/[`unseal`] contract is
//! kept by the **Secure Enclave** instead, and in the same shape —
//! hardware holds a key, the key holds the secret. Each seal generates
//! a fresh transient SE P-256 key, ECIES-encrypts the secret under it,
//! and stores the key's SEP-wrapped private material *inside the blob*
//! (the CTK token object id — what CryptoKit calls
//! `dataRepresentation`). Nothing touches the keychain, so no
//! code-signing entitlement is needed, and there is no system state to
//! lose: the blob is self-contained and only this machine's enclave can
//! unwrap it. No user-presence gate, accessible after first unlock — a
//! daemon that can't start unattended is an outage on a delay timer.
//!
//! On platforms with neither, [`available`] is `false` and
//! [`seal`]/[`unseal`] return honest errors; callers fall back to
//! their plaintext path.

use anyhow::Result;

/// Marker prefixed to TPM-sealed files. Contains a NUL so it can
/// never collide with a plaintext secret (which is printable text).
pub const MAGIC: &[u8] = b"#netidx-tpm-sealed-v1\0";

/// Marker prefixed to Secure-Enclave-sealed files (macOS). Same NUL
/// trick. A distinct magic means a blob carried to the wrong platform
/// fails with "sealed elsewhere — re-issue", not a parse error.
pub const SE_MAGIC: &[u8] = b"#netidx-se-sealed-v1\0";

/// Does `data` carry a sealed payload (vs a plaintext secret),
/// whichever platform sealed it?
pub fn is_sealed(data: &[u8]) -> bool {
    data.starts_with(MAGIC) || data.starts_with(SE_MAGIC)
}

/// What does the sealing on this platform — for user-facing messages
/// ("serving key sealed to this machine's {MECHANISM}").
#[cfg(target_os = "macos")]
pub const MECHANISM: &str = "Secure Enclave";
#[cfg(not(target_os = "macos"))]
pub const MECHANISM: &str = "TPM";

/// Bytes of entropy in a [`random_secret`] — 256 bits, sized like the
/// private key whose password it becomes.
const RANDOM_SECRET_BYTES: usize = 32;

/// A long random secret (256 bits, lowercase hex — printable, so it
/// composes with anything that expects a password string). Used as the
/// generated password when a key is sealed rather than typed.
///
/// Every buffer it passes through is wiped, and the output string is
/// preallocated to its exact length: a realloc midway would strand half
/// the secret in an old allocation, where `Zeroizing` never reaches it.
/// The password protects a TLS private key and is meant to exist only
/// inside a sealed blob, so a copy left in freed heap for a core dump to
/// find is the whole of what it was supposed to prevent.
pub fn random_secret() -> zeroize::Zeroizing<String> {
    use rand::Rng;
    use std::fmt::Write;
    let mut bytes = zeroize::Zeroizing::new([0u8; RANDOM_SECRET_BYTES]);
    rand::rng().fill_bytes(&mut bytes[..]);
    let mut out = zeroize::Zeroizing::new(String::with_capacity(2 * RANDOM_SECRET_BYTES));
    for b in bytes.iter() {
        let _ = write!(&mut *out, "{b:02x}");
    }
    out
}

/// One marshalled TPM command in, one complete raw response frame out.
/// The only piece of the TPM path that touches a platform API. Not
/// defined on macOS — the Secure Enclave doesn't speak TPM2 frames;
/// its platform module implements seal/unseal directly.
#[cfg(not(target_os = "macos"))]
pub trait Transport {
    fn exchange(&mut self, command: &[u8]) -> Result<Vec<u8>>;
}

#[cfg(not(target_os = "macos"))]
mod ops {
    use super::Transport;
    use anyhow::{Context, Result, anyhow, bail};
    use tpm2_protocol::{
        TpmMarshal, TpmResult, TpmSized, TpmWriter,
        basic::TpmHandle,
        constant::TPM_MAX_COMMAND_SIZE,
        data::{
            Tpm2bAuth, Tpm2bData, Tpm2bDigest, Tpm2bNonce, Tpm2bPublic,
            Tpm2bSensitiveCreate, Tpm2bSensitiveData, TpmAlgId, TpmCc, TpmEccCurve,
            TpmRc, TpmRcBase, TpmRh, TpmSt, TpmaObject, TpmaSession, TpmlPcrSelection,
            TpmsAuthCommand, TpmsEccParms, TpmsEccPoint, TpmsKeyedhashParms,
            TpmsSensitiveCreate, TpmtEccScheme, TpmtKdfScheme, TpmtKeyedhashScheme,
            TpmtPublic, TpmtSymDefObject, TpmuAsymScheme, TpmuKdfScheme,
            TpmuKeyedhashScheme, TpmuPublicId, TpmuPublicParms, TpmuSymKeyBits,
            TpmuSymMode,
        },
        frame::{
            TpmCreateCommand, TpmCreatePrimaryCommand, TpmFlushContextCommand, TpmFrame,
            TpmMarshalBody, TpmResponse, TpmUnsealCommand, tpm_marshal_command,
        },
    };
    use zeroize::{Zeroize, Zeroizing};

    /// The empty-password authorization session (TPM_RS_PW). We assume
    /// an empty owner-hierarchy auth: how every mainstream linux
    /// distro ships, and how modern Windows provisions TPM 2.0 (it
    /// randomizes only lockoutAuth). A site that sets an owner
    /// password gets a clear TPM_RC_BAD_AUTH and falls back to its
    /// plaintext path.
    fn pw_session() -> TpmsAuthCommand {
        TpmsAuthCommand {
            session_handle: (TpmRh::Pw as u32).into(),
            nonce: Tpm2bNonce::default(),
            session_attributes: TpmaSession::CONTINUE_SESSION,
            hmac: Tpm2bAuth::default(),
        }
    }

    /// The TCG-standard ECC P-256 storage key template (the "SRK"
    /// shape used by tpm2-tools, systemd-creds, clevis). `CreatePrimary`
    /// with this template deterministically re-derives the same key on
    /// the same TPM, so we never persist anything. ECC, not RSA: the
    /// derivation runs on every seal/unseal and RSA primary generation
    /// takes seconds on discrete TPMs.
    fn srk_template() -> TpmtPublic {
        TpmtPublic {
            object_type: TpmAlgId::Ecc,
            name_alg: TpmAlgId::Sha256,
            object_attributes: TpmaObject::FIXED_TPM
                | TpmaObject::FIXED_PARENT
                | TpmaObject::SENSITIVE_DATA_ORIGIN
                | TpmaObject::USER_WITH_AUTH
                | TpmaObject::RESTRICTED
                | TpmaObject::DECRYPT
                | TpmaObject::NO_DA,
            auth_policy: Tpm2bDigest::default(),
            parameters: TpmuPublicParms::Ecc(TpmsEccParms {
                symmetric: TpmtSymDefObject {
                    algorithm: TpmAlgId::Aes,
                    key_bits: TpmuSymKeyBits::Aes(128u16.into()),
                    mode: TpmuSymMode::Aes(TpmAlgId::Cfb),
                },
                scheme: TpmtEccScheme {
                    scheme: TpmAlgId::Null,
                    details: TpmuAsymScheme::Null,
                },
                curve_id: TpmEccCurve::NistP256,
                kdf: TpmtKdfScheme {
                    scheme: TpmAlgId::Null,
                    details: TpmuKdfScheme::Null,
                },
            }),
            unique: TpmuPublicId::Ecc(TpmsEccPoint::default()),
        }
    }

    /// Template for the sealed-data object itself: a `KeyedHash` blob
    /// with a Null scheme (pure storage, can't sign or HMAC), bound to
    /// this TPM and parent, gated by (empty) user auth, exempt from
    /// dictionary-attack lockout — an empty password can't be
    /// brute-forced, and DA lockout would let any local process wedge
    /// the daemon by spamming bad auths.
    fn sealed_template() -> TpmtPublic {
        TpmtPublic {
            object_type: TpmAlgId::KeyedHash,
            name_alg: TpmAlgId::Sha256,
            object_attributes: TpmaObject::FIXED_TPM
                | TpmaObject::FIXED_PARENT
                | TpmaObject::USER_WITH_AUTH
                | TpmaObject::NO_DA,
            auth_policy: Tpm2bDigest::default(),
            parameters: TpmuPublicParms::KeyedHash(TpmsKeyedhashParms {
                scheme: TpmtKeyedhashScheme {
                    scheme: TpmAlgId::Null,
                    details: TpmuKeyedhashScheme::Null,
                },
            }),
            unique: TpmuPublicId::KeyedHash(Tpm2bDigest::default()),
        }
    }

    /// `TPM2_Load` with the private/public areas kept as the raw
    /// `TPM2B` wire bytes the `Create` response produced. Round-tripping
    /// the bytes verbatim (instead of parsing `TpmtPublic` and
    /// re-marshalling it) means the object's name — a hash of its
    /// public area — can't be perturbed by a parse/re-marshal asymmetry.
    #[derive(Debug)]
    struct RawLoadCommand<'a> {
        parent: TpmHandle,
        in_private: &'a [u8],
        in_public: &'a [u8],
    }

    impl TpmSized for RawLoadCommand<'_> {
        const SIZE: usize = TPM_MAX_COMMAND_SIZE;
        fn len(&self) -> usize {
            self.parent.len() + self.in_private.len() + self.in_public.len()
        }
    }

    impl TpmMarshal for RawLoadCommand<'_> {
        fn marshal(&self, writer: &mut TpmWriter) -> TpmResult<()> {
            self.marshal_handles(writer)?;
            self.marshal_parameters(writer)
        }
    }

    impl TpmMarshalBody for RawLoadCommand<'_> {
        fn marshal_handles(&self, writer: &mut TpmWriter) -> TpmResult<()> {
            self.parent.marshal(writer)
        }
        fn marshal_parameters(&self, writer: &mut TpmWriter) -> TpmResult<()> {
            writer.write_bytes(self.in_private)?;
            writer.write_bytes(self.in_public)
        }
    }

    impl TpmFrame for RawLoadCommand<'_> {
        fn cc(&self) -> TpmCc {
            TpmCc::Load
        }
        fn handles(&self) -> usize {
            1
        }
    }

    /// Marshal `cmd`, exchange it over the transport, and fail on a
    /// non-success TPM return code.
    fn transmit(
        dev: &mut dyn Transport,
        cmd: &impl TpmFrame,
        sessions: &[TpmsAuthCommand],
    ) -> Result<Vec<u8>> {
        let cc = cmd.cc();
        let mut buf = vec![0u8; TPM_MAX_COMMAND_SIZE];
        let len = {
            let mut writer = TpmWriter::new(&mut buf);
            let tag =
                if sessions.is_empty() { TpmSt::NoSessions } else { TpmSt::Sessions };
            tpm_marshal_command(cmd, tag, sessions, &mut writer)
                .map_err(|e| anyhow!("marshalling {cc}: {e}"))?;
            writer.len()
        };
        let resp = dev
            .exchange(&buf[..len])
            .with_context(|| format!("exchanging {cc} with the TPM"))?;
        buf.zeroize();
        let frame = TpmResponse::cast(&resp)
            .map_err(|e| anyhow!("parsing {cc} response: {e}"))?;
        let rc = frame.rc().map_err(|e| anyhow!("parsing {cc} return code: {e}"))?;
        if !matches!(rc, TpmRc::Fmt0(TpmRcBase::Success)) {
            bail!("TPM {cc} failed: {rc}");
        }
        Ok(resp)
    }

    /// Split a successful response's body into (handle area, parameter
    /// area), skipping the parameter-size word present when the
    /// command carried sessions.
    fn response_areas(resp: &[u8], nhandles: usize) -> Result<(&[u8], &[u8])> {
        let frame =
            TpmResponse::cast(resp).map_err(|e| anyhow!("response reparse: {e}"))?;
        let body = frame.body();
        let hlen = nhandles * 4;
        if body.len() < hlen {
            bail!("TPM response too short for {nhandles} handle(s)");
        }
        let (handles, rest) = body.split_at(hlen);
        let tag = frame.tag().map_err(|e| anyhow!("response tag: {e}"))?;
        if tag != TpmSt::Sessions {
            return Ok((handles, rest));
        }
        if rest.len() < 4 {
            bail!("TPM response truncated at the parameter size");
        }
        let plen = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
        let rest = &rest[4..];
        if rest.len() < plen {
            bail!("TPM response truncated inside the parameter area");
        }
        Ok((handles, &rest[..plen]))
    }

    /// Take one size-prefixed `TPM2B` off the front of `buf`, returning
    /// (the whole TPM2B including its size prefix, the rest).
    fn take_tpm2b(buf: &[u8]) -> Result<(&[u8], &[u8])> {
        if buf.len() < 2 {
            bail!("truncated TPM2B size prefix");
        }
        let size = u16::from_be_bytes(buf[..2].try_into().unwrap()) as usize;
        if buf.len() < 2 + size {
            bail!("truncated TPM2B payload");
        }
        Ok(buf.split_at(2 + size))
    }

    fn empty_sensitive() -> Tpm2bSensitiveCreate {
        Tpm2bSensitiveCreate {
            inner: TpmsSensitiveCreate {
                user_auth: Tpm2bAuth::default(),
                data: Tpm2bSensitiveData::default(),
            },
        }
    }

    /// Re-derive the SRK on the owner hierarchy; returns its transient
    /// handle (flush it when done).
    fn create_primary(dev: &mut dyn Transport) -> Result<TpmHandle> {
        let cmd = TpmCreatePrimaryCommand {
            handles: [(TpmRh::Owner as u32).into()],
            in_sensitive: empty_sensitive(),
            in_public: Tpm2bPublic { inner: srk_template() },
            outside_info: Tpm2bData::default(),
            creation_pcr: TpmlPcrSelection::default(),
        };
        let resp = transmit(dev, &cmd, &[pw_session()])?;
        let (handles, _) = response_areas(&resp, 1)?;
        Ok(u32::from_be_bytes(handles[..4].try_into().unwrap()).into())
    }

    fn flush(dev: &mut dyn Transport, handle: TpmHandle) {
        // Best-effort: the handle is transient, so a failed flush costs
        // a TPM object slot until the next reboot, nothing more.
        let cmd = TpmFlushContextCommand { flush_handle: handle, handles: [] };
        let _ = transmit(dev, &cmd, &[]);
    }

    /// A TPM sealed object holds a guaranteed 128 bytes (MAX_SYM_DATA)
    /// — plenty for a password, nowhere near a cached administrator
    /// session. So the secret is never what the TPM holds: it is
    /// encrypted under a fresh AES-256-GCM key and the *key* is sealed,
    /// with the ciphertext beside it in the same blob. The machine
    /// binding is exactly what it was, since the key is inert without an
    /// unseal on this host, and size stops being anyone's problem.
    const ENVELOPE_KEY_BYTES: usize = 32;
    const ENVELOPE_NONCE_BYTES: usize = 12;

    /// `MAGIC || out_private || out_public || nonce || ciphertext`. The
    /// two TPM areas are TPM2Bs and so self-delimiting, which is what
    /// lets the envelope sit after them and still be parsed.
    pub fn seal(dev: &mut dyn Transport, secret: &[u8]) -> Result<Vec<u8>> {
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce, aead::Aead};
        use rand::Rng;
        let mut key = Zeroizing::new([0u8; ENVELOPE_KEY_BYTES]);
        rand::rng().fill_bytes(&mut key[..]);
        // A fresh key per seal, so a fresh random nonce cannot repeat
        // under one key however many secrets this host seals.
        let mut nonce = [0u8; ENVELOPE_NONCE_BYTES];
        rand::rng().fill_bytes(&mut nonce);
        let ciphertext = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key[..]))
            .encrypt(Nonce::from_slice(&nonce), secret)
            .map_err(|_| anyhow!("encrypting the enveloped secret failed"))?;
        let mut blob = seal_key(dev, &key[..])?;
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ciphertext);
        Ok(blob)
    }

    fn seal_key(dev: &mut dyn Transport, secret: &[u8]) -> Result<Vec<u8>> {
        let primary = create_primary(dev)?;
        let result = (|| {
            let cmd = TpmCreateCommand {
                handles: [primary],
                in_sensitive: Tpm2bSensitiveCreate {
                    inner: TpmsSensitiveCreate {
                        user_auth: Tpm2bAuth::default(),
                        data: Tpm2bSensitiveData::try_from(secret)
                            .map_err(|e| anyhow!("sealed payload: {e}"))?,
                    },
                },
                in_public: Tpm2bPublic { inner: sealed_template() },
                outside_info: Tpm2bData::default(),
                creation_pcr: TpmlPcrSelection::default(),
            };
            let resp = transmit(dev, &cmd, &[pw_session()])?;
            let (_, params) = response_areas(&resp, 0)?;
            let (private, rest) =
                take_tpm2b(params).context("TPM2_Create out_private")?;
            let (public, _) = take_tpm2b(rest).context("TPM2_Create out_public")?;
            let mut blob =
                Vec::with_capacity(super::MAGIC.len() + private.len() + public.len());
            blob.extend_from_slice(super::MAGIC);
            blob.extend_from_slice(private);
            blob.extend_from_slice(public);
            Ok(blob)
        })();
        flush(dev, primary);
        result
    }

    pub fn unseal(dev: &mut dyn Transport, blob: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce, aead::Aead};
        let Some(body) = blob.strip_prefix(super::MAGIC) else {
            if blob.starts_with(super::SE_MAGIC) {
                bail!(
                    "this blob was sealed by a Mac's Secure Enclave, which only \
                     that machine can open — re-issue the credential on this host"
                );
            }
            bail!("not a netidx TPM-sealed blob (bad magic)");
        };
        let (key, rest) = unseal_key(dev, body)?;
        // `Key::from_slice` panics on a wrong length, and what the TPM
        // hands back is only as long as whatever was sealed.
        if key.len() != ENVELOPE_KEY_BYTES {
            bail!(
                "the sealed envelope key is {} bytes, not {ENVELOPE_KEY_BYTES}",
                key.len()
            );
        }
        if rest.len() < ENVELOPE_NONCE_BYTES {
            bail!("sealed blob truncated inside the envelope");
        }
        let (nonce, ciphertext) = rest.split_at(ENVELOPE_NONCE_BYTES);
        let plaintext = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key))
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| {
                anyhow!(
                    "the enveloped secret failed authentication — the blob is corrupt"
                )
            })?;
        Ok(Zeroizing::new(plaintext))
    }

    /// Load and unseal the sealed object at the head of `body`,
    /// returning the envelope key with the nonce + ciphertext that
    /// follow it.
    fn unseal_key<'a>(
        dev: &mut dyn Transport,
        body: &'a [u8],
    ) -> Result<(Zeroizing<Vec<u8>>, &'a [u8])> {
        let (private, rest) = take_tpm2b(body).context("sealed blob private area")?;
        let (public, rest) = take_tpm2b(rest).context("sealed blob public area")?;
        let primary = create_primary(dev)?;
        let result = (|| {
            let cmd = RawLoadCommand {
                parent: primary,
                in_private: private,
                in_public: public,
            };
            let resp = transmit(dev, &cmd, &[pw_session()])?;
            let (handles, _) = response_areas(&resp, 1)?;
            let loaded: TpmHandle =
                u32::from_be_bytes(handles[..4].try_into().unwrap()).into();
            let result = (|| {
                let cmd = TpmUnsealCommand { handles: [loaded] };
                let mut resp = transmit(dev, &cmd, &[pw_session()])?;
                let secret = {
                    let (_, params) = response_areas(&resp, 0)?;
                    let (out, _) = take_tpm2b(params).context("TPM2_Unseal out_data")?;
                    Zeroizing::new(out[2..].to_vec())
                };
                resp.zeroize();
                Ok(secret)
            })();
            flush(dev, loaded);
            result
        })();
        flush(dev, primary);
        result.map(|secret| (secret, rest))
    }
}

/// Seal `secret` to this host's TPM over the platform transport (or
/// the Secure Enclave on macOS).
#[cfg(not(target_os = "macos"))]
pub fn seal(secret: &[u8]) -> Result<Vec<u8>> {
    let mut dev = platform::transport()?;
    ops::seal(&mut dev, secret)
}
#[cfg(target_os = "macos")]
pub fn seal(secret: &[u8]) -> Result<Vec<u8>> {
    platform::seal(secret)
}

/// Unseal a blob produced by [`seal`] on this same machine.
#[cfg(not(target_os = "macos"))]
pub fn unseal(blob: &[u8]) -> Result<zeroize::Zeroizing<Vec<u8>>> {
    let mut dev = platform::transport()?;
    ops::unseal(&mut dev, blob)
}
#[cfg(target_os = "macos")]
pub fn unseal(blob: &[u8]) -> Result<zeroize::Zeroizing<Vec<u8>>> {
    platform::unseal(blob)
}

/// Is sealing usable here (TPM present and accessible, or a working
/// Secure Enclave on macOS)?
#[cfg(not(target_os = "macos"))]
pub fn available() -> bool {
    platform::transport().is_ok()
}
#[cfg(target_os = "macos")]
pub fn available() -> bool {
    platform::available()
}

#[cfg(target_os = "linux")]
mod platform {
    use super::Transport;
    use anyhow::{Context, Result, bail};
    use std::{
        fs::{File, OpenOptions},
        io::{Read, Write},
    };
    use tpm2_protocol::constant::TPM_MAX_COMMAND_SIZE;

    const DEVICE: &str = "/dev/tpmrm0";

    /// The kernel TPM resource manager: write one whole command, read
    /// back one whole response.
    pub struct LinuxDevice(File);

    pub fn transport() -> Result<LinuxDevice> {
        let file = OpenOptions::new().read(true).write(true).open(DEVICE).with_context(
            || {
                format!(
                    "opening {DEVICE} (no TPM 2.0, or this user lacks access — \
                     the device node is conventionally root:tss)"
                )
            },
        )?;
        Ok(LinuxDevice(file))
    }

    impl Transport for LinuxDevice {
        fn exchange(&mut self, command: &[u8]) -> Result<Vec<u8>> {
            self.0.write_all(command).context("writing to the TPM")?;
            // The resource manager hands back the whole response in one
            // read, but loop on the self-described size to be safe.
            let mut resp: Vec<u8> = Vec::with_capacity(4096);
            let mut chunk = [0u8; 4096];
            loop {
                let n = self.0.read(&mut chunk).context("reading the TPM response")?;
                if n == 0 {
                    bail!("TPM closed the connection mid-response");
                }
                resp.extend_from_slice(&chunk[..n]);
                if resp.len() >= 10 {
                    let total =
                        u32::from_be_bytes(resp[2..6].try_into().unwrap()) as usize;
                    if !(10..=TPM_MAX_COMMAND_SIZE).contains(&total) {
                        bail!("TPM response declares an absurd size {total}");
                    }
                    if resp.len() >= total {
                        break;
                    }
                }
            }
            Ok(resp)
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::Transport;
    use anyhow::{Error, Result, anyhow, bail};
    use std::ffi::c_void;
    use tpm2_protocol::constant::TPM_MAX_COMMAND_SIZE;
    use windows::Win32::System::TpmBaseServices::{
        TBS_COMMAND_LOCALITY_ZERO, TBS_COMMAND_PRIORITY_NORMAL, TBS_CONTEXT_PARAMS,
        TBS_CONTEXT_PARAMS2, TBS_CONTEXT_PARAMS2_0, TBS_CONTEXT_VERSION_TWO, TBS_SUCCESS,
        Tbsi_Context_Create, Tbsip_Context_Close, Tbsip_Submit_Command,
    };

    /// TPM Base Services — the Windows TPM resource manager.
    /// `Tbsip_Submit_Command` exchanges the same raw TPM2 frames the
    /// linux device does; TBS virtualizes transient handles per
    /// context just like tpmrm0.
    pub struct TbsDevice(*mut c_void);

    pub fn transport() -> Result<TbsDevice> {
        let params = TBS_CONTEXT_PARAMS2 {
            version: TBS_CONTEXT_VERSION_TWO,
            // includeTpm20 is bit 2 of the flags word. We never speak
            // TPM 1.2 — on a 1.2-only machine context creation fails
            // and the caller takes its plaintext path.
            Anonymous: TBS_CONTEXT_PARAMS2_0 { asUINT32: 1 << 2 },
        };
        let mut ctx: *mut c_void = std::ptr::null_mut();
        let rc = unsafe {
            // The API takes the v1 struct type; v2 extends it in place
            // (the version field is how TBS tells them apart).
            Tbsi_Context_Create(
                &params as *const TBS_CONTEXT_PARAMS2 as *const TBS_CONTEXT_PARAMS,
                &mut ctx,
            )
        };
        if rc != TBS_SUCCESS || ctx.is_null() {
            return Err(tbs_error("opening a TBS context", rc));
        }
        Ok(TbsDevice(ctx))
    }

    impl Drop for TbsDevice {
        fn drop(&mut self) {
            unsafe { Tbsip_Context_Close(self.0) };
        }
    }

    impl Transport for TbsDevice {
        fn exchange(&mut self, command: &[u8]) -> Result<Vec<u8>> {
            let mut resp = vec![0u8; TPM_MAX_COMMAND_SIZE];
            // In: buffer capacity. Out: actual response length.
            let mut len = resp.len() as u32;
            let rc = unsafe {
                Tbsip_Submit_Command(
                    self.0,
                    // User mode may only use locality zero.
                    TBS_COMMAND_LOCALITY_ZERO,
                    TBS_COMMAND_PRIORITY_NORMAL,
                    command,
                    resp.as_mut_ptr(),
                    &mut len,
                )
            };
            if rc != TBS_SUCCESS {
                return Err(tbs_error("submitting the TPM command", rc));
            }
            let len = len as usize;
            if len > resp.len() {
                bail!("TBS declared a response longer than the buffer it filled");
            }
            resp.truncate(len);
            Ok(resp)
        }
    }

    /// Name the TBS facility errors an operator can act on; anything
    /// else is reported as the raw HRESULT (they're all in tbs.h).
    fn tbs_error(doing: &str, rc: u32) -> Error {
        let detail = match rc {
            0x8028400F => "no TPM 2.0 on this machine (TBS_E_TPM_NOT_FOUND)",
            0x80284008 => {
                "the TPM Base Services service is not running (TBS_E_SERVICE_NOT_RUNNING)"
            }
            0x8028400B => {
                "TPM Base Services is still starting (TBS_E_SERVICE_START_PENDING)"
            }
            0x80284010 => "TPM Base Services is disabled (TBS_E_SERVICE_DISABLED)",
            0x80284012 => "access to the TPM was denied (TBS_E_ACCESS_DENIED)",
            0x80280400 => {
                "the command is on the TBS blocked-commands list (TPM_E_COMMAND_BLOCKED)"
            }
            rc => return anyhow!("{doing}: TBS error {rc:#010x}"),
        };
        anyhow!("{doing}: {detail}")
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use anyhow::{Result, anyhow, bail};
    use core_foundation::{
        base::TCFType, data::CFData, dictionary::CFDictionary, string::CFString,
    };
    use security_framework::{
        access_control::{ProtectionMode, SecAccessControl},
        key::{Algorithm, GenerateKeyOptions, KeyType, SecKey, Token},
    };
    use security_framework_sys::{
        item::{
            kSecAttrKeyClass, kSecAttrKeyClassPrivate, kSecAttrKeyType,
            kSecAttrKeyTypeECSECPrimeRandom, kSecAttrTokenID,
            kSecAttrTokenIDSecureEnclave,
        },
        key::{SecKeyCopyAttributes, SecKeyCreateWithData},
    };
    use zeroize::Zeroizing;

    /// Apple's recommended ECIES for SE keys (ephemeral ECDH +
    /// X9.63-SHA256 KDF + AES-GCM) — what CryptoKit uses underneath.
    /// Encryption needs only the public half; the enclave is touched
    /// at decrypt.
    const ALGORITHM: Algorithm =
        Algorithm::ECIESEncryptionCofactorVariableIVX963SHA256AESGCM;

    /// kSecAccessControlPrivateKeyUsage — mandatory for Secure
    /// Enclave keys; the access control governs private-key use.
    /// security-framework takes raw flags and doesn't re-export the
    /// constant (SecAccessControlCreateFlags in Apple's headers).
    const PRIVATE_KEY_USAGE: usize = 1 << 30;

    /// CryptoTokenKit's token-object-id attribute: the SEP-wrapped
    /// private key material — exactly what CryptoKit exposes as an SE
    /// key's `dataRepresentation`. Not in the public headers, but it
    /// is the persistence format of every CryptoKit Secure Enclave
    /// key in every shipped app, so it is frozen ABI in practice.
    /// Verified empirically on this design's bring-up: a key
    /// reconstituted via `{kSecAttrTokenID: SE, "toid": blob}` has
    /// the same public key and decrypts what the original sealed.
    /// (Beware: `SecKeyCreateWithData` WITHOUT the toid attribute
    /// silently generates a fresh key instead of failing.)
    const TOID: &str = "toid";

    /// A fresh transient SE key — never persisted anywhere; its
    /// wrapped private material travels inside the sealed blob, just
    /// like a TPM sealed object's private area. AfterFirstUnlock +
    /// ThisDeviceOnly, and deliberately NO user-presence gate — same
    /// reasoning as no PCR binding on the TPM path: the threat model
    /// is offline theft, not live-host compromise, and a daemon stuck
    /// on a biometric prompt nobody will answer is an outage on a
    /// delay timer.
    fn fresh_key() -> Result<SecKey> {
        let access = SecAccessControl::create_with_protection(
            Some(ProtectionMode::AccessibleAfterFirstUnlockThisDeviceOnly),
            PRIVATE_KEY_USAGE,
        )
        .map_err(|e| anyhow!("creating the sealing key's access control: {e}"))?;
        let mut opts = GenerateKeyOptions::default();
        opts.set_key_type(KeyType::ec());
        opts.set_size_in_bits(256);
        opts.set_token(Token::SecureEnclave);
        // NB: no set_location → kSecAttrIsPermanent unset → the key
        // exists only in this process and the blob we serialize.
        opts.set_access_control(access);
        SecKey::new(&opts).map_err(|e| {
            anyhow!("generating a Secure Enclave key (no enclave on this machine?): {e}")
        })
    }

    /// Pull the SEP-wrapped key material out of a transient SE key.
    fn extract_toid(key: &SecKey) -> Result<Vec<u8>> {
        unsafe {
            let attrs: CFDictionary = CFDictionary::wrap_under_create_rule(
                SecKeyCopyAttributes(key.as_concrete_TypeRef()) as _,
            );
            let toid_key = CFString::from_static_string(TOID);
            let v = attrs
                .find(toid_key.as_concrete_TypeRef() as *const _)
                .ok_or_else(|| anyhow!("SE key attributes carry no token object id"))?;
            Ok(CFData::wrap_under_get_rule(*v as _).bytes().to_vec())
        }
    }

    /// Load the SEP-wrapped key material back into a usable key. Only
    /// the enclave that wrapped it can — that is the machine binding.
    fn reconstitute(toid: &[u8]) -> Result<SecKey> {
        unsafe {
            let attrs = CFDictionary::from_CFType_pairs(&[
                (
                    CFString::wrap_under_get_rule(kSecAttrTokenID).as_CFType(),
                    CFString::wrap_under_get_rule(kSecAttrTokenIDSecureEnclave)
                        .as_CFType(),
                ),
                (
                    CFString::wrap_under_get_rule(kSecAttrKeyType).as_CFType(),
                    CFString::wrap_under_get_rule(kSecAttrKeyTypeECSECPrimeRandom)
                        .as_CFType(),
                ),
                (
                    CFString::wrap_under_get_rule(kSecAttrKeyClass).as_CFType(),
                    CFString::wrap_under_get_rule(kSecAttrKeyClassPrivate).as_CFType(),
                ),
                (
                    CFString::from_static_string(TOID).as_CFType(),
                    CFData::from_buffer(toid).as_CFType(),
                ),
            ]);
            let mut error = std::ptr::null_mut();
            let key = SecKeyCreateWithData(
                CFData::from_buffer(&[]).as_concrete_TypeRef(),
                attrs.as_concrete_TypeRef() as _,
                &mut error,
            );
            if key.is_null() {
                let e = core_foundation::base::CFType::wrap_under_create_rule(error as _);
                bail!(
                    "the Secure Enclave could not load the sealed key — sealed on \
                     another machine, or this Mac's enclave was reset? re-issue the \
                     credential on this host ({e:?})"
                );
            }
            Ok(SecKey::wrap_under_create_rule(key))
        }
    }

    /// Blob layout after [`super::SE_MAGIC`]: u16 BE toid length,
    /// the toid, then the ECIES ciphertext.
    pub fn seal(secret: &[u8]) -> Result<Vec<u8>> {
        // No size check: ECIES encrypts a secret of any length, and the
        // TPM path no longer imposes one either (it envelopes what does
        // not fit), so there is nothing left to mirror. This layout is
        // unchanged — every SE blob was already, in effect, an envelope.
        let key = fresh_key()?;
        let toid = extract_toid(&key)?;
        let toid_len = u16::try_from(toid.len())
            .map_err(|_| anyhow!("absurd token object id size {}", toid.len()))?;
        let public = key
            .public_key()
            .ok_or_else(|| anyhow!("the sealing key has no public half"))?;
        let ct = public
            .encrypt_data(ALGORITHM, secret)
            .map_err(|e| anyhow!("Secure Enclave encrypt: {e}"))?;
        let mut blob =
            Vec::with_capacity(super::SE_MAGIC.len() + 2 + toid.len() + ct.len());
        blob.extend_from_slice(super::SE_MAGIC);
        blob.extend_from_slice(&toid_len.to_be_bytes());
        blob.extend_from_slice(&toid);
        blob.extend_from_slice(&ct);
        Ok(blob)
    }

    pub fn unseal(blob: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        let Some(body) = blob.strip_prefix(super::SE_MAGIC) else {
            if blob.starts_with(super::MAGIC) {
                bail!(
                    "this blob was sealed by a TPM, which only that machine can \
                     open — re-issue the credential on this host"
                );
            }
            bail!("not a netidx sealed blob (bad magic)");
        };
        if body.len() < 2 {
            bail!("truncated sealed blob");
        }
        let (len, rest) = body.split_at(2);
        let toid_len = u16::from_be_bytes(len.try_into().unwrap()) as usize;
        if rest.len() < toid_len {
            bail!("sealed blob truncated inside the wrapped key");
        }
        let (toid, ct) = rest.split_at(toid_len);
        let key = reconstitute(toid)?;
        let secret = key
            .decrypt_data(ALGORITHM, ct)
            .map_err(|e| anyhow!("Secure Enclave decrypt: {e}"))?;
        Ok(Zeroizing::new(secret))
    }

    /// Generating a transient key IS the probe — it leaves nothing
    /// behind, and a Mac without a Secure Enclave (or with one the OS
    /// can't reach) fails right here.
    pub fn available() -> bool {
        fresh_key().is_ok()
    }
}

#[cfg(not(any(target_os = "linux", windows, target_os = "macos")))]
mod platform {
    use super::Transport;
    use anyhow::{Result, bail};

    pub struct NoDevice;

    pub fn transport() -> Result<NoDevice> {
        bail!(
            "no sealing mechanism for this platform yet (supported: linux, windows, macos)"
        )
    }

    impl Transport for NoDevice {
        fn exchange(&mut self, _command: &[u8]) -> Result<Vec<u8>> {
            bail!("no TPM transport on this platform")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_detection() {
        let mut sealed = MAGIC.to_vec();
        sealed.extend_from_slice(b"\x00\x10whatever");
        assert!(is_sealed(&sealed));
        // A plaintext password can never collide: MAGIC contains NUL.
        assert!(!is_sealed(b"6fb1a5ad9c8e42d1b2c3"));
        assert!(!is_sealed(b""));
        // A prefix of the magic is not the magic.
        assert!(!is_sealed(&MAGIC[..MAGIC.len() - 1]));
    }

    #[test]
    fn random_secrets_are_long_and_distinct() {
        // The size is the security claim; the length is also what keeps the
        // output string's preallocation exact, so that no realloc can leave
        // half the secret behind in a freed buffer.
        assert_eq!(RANDOM_SECRET_BYTES * 8, 256);
        let a = random_secret();
        let b = random_secret();
        assert_eq!(a.len(), 2 * RANDOM_SECRET_BYTES);
        assert_ne!(*a, *b);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    /// Real hardware round-trip. Runs only where a TPM is reachable
    /// (e.g. the dev box); elsewhere it's a silent pass so CI without
    /// hardware stays green while real machines actually exercise the
    /// command stack.
    #[test]
    fn seal_unseal_round_trip() {
        if !available() {
            eprintln!("skipping: no usable TPM on this host");
            return;
        }
        let secret = b"correct horse battery staple";
        let blob = seal(secret).expect("seal");
        assert!(is_sealed(&blob));
        let out = unseal(&blob).expect("unseal");
        assert_eq!(&*out, secret);
        // Corrupting the payload must fail loudly, not produce data.
        // The last byte sits in the TPM public area (perturbs the
        // object name) or the SE blob's GCM tag — both must reject.
        let mut bad = blob.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        assert!(unseal(&bad).is_err());
    }

    /// A secret far past what one TPM sealed object holds — the case
    /// the envelope exists for. A cached administrator session is ~300
    /// bytes against a 128-byte sealed object, so before it,
    /// `netidx admin login` could not persist anything on any machine
    /// with a TPM.
    #[test]
    fn seal_unseal_round_trips_a_secret_larger_than_a_sealed_object() {
        if !available() {
            eprintln!("skipping: no usable TPM on this host");
            return;
        }
        let secret: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let blob = seal(&secret).expect("seal");
        assert!(is_sealed(&blob));
        assert_eq!(&*unseal(&blob).expect("unseal"), &secret[..]);

        // The plaintext must not be sitting in the blob next to the key.
        assert!(
            blob.windows(64).all(|w| w != &secret[..64]),
            "the envelope shipped its secret in the clear"
        );

        // Every byte of the envelope is authenticated: flipping one in
        // the ciphertext, the nonce, or the tag must fail rather than
        // return something.
        for at in [blob.len() - 1, blob.len() / 2] {
            let mut bad = blob.clone();
            bad[at] ^= 0xff;
            assert!(unseal(&bad).is_err(), "corruption at {at} was not caught");
        }
    }
}
