//! Seal a small secret to this host's TPM 2.0.
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
//! Mechanism: the secret is wrapped in a TPM `KeyedHash` sealed-data
//! object under the standard ECC P-256 storage root key (SRK) template
//! on the owner hierarchy. The SRK is re-derived by `CreatePrimary` on
//! every operation — same seed + same template ⇒ same key — so nothing
//! is persisted in the TPM and no NV/owner-auth bookkeeping exists to
//! rot. The sealed object carries an empty auth value: the protection
//! is the TPM binding, not a second password (which would just move
//! the problem to "where do we store that").
//!
//! Deliberately **no PCR binding**: a PCR-bound blob silently stops
//! unsealing after a firmware update, and a daemon that silently stops
//! is an outage on a delay timer. The recovery story for any unseal
//! failure (TPM cleared, board swapped) is re-issue/re-seal — netidx
//! certificate issuance is one command, so keys are disposable.
//!
//! The command marshalling is pure Rust ([`tpm2-protocol`]) and
//! compiles everywhere; only the *transport* is per-platform. Today
//! that's the linux kernel resource manager (`/dev/tpmrm0`, note the
//! device node is conventionally root:tss); a Windows TBS transport
//! ([`Tbsip_Submit_Command`] takes the same raw buffers) slots in
//! behind [`Transport`] without touching anything else. On platforms
//! with no transport, [`available`] is `false` and [`seal`]/[`unseal`]
//! return honest errors; callers fall back to their plaintext path.

use anyhow::Result;

/// Marker prefixed to sealed files. Contains a NUL so it can never
/// collide with a plaintext secret (which is printable text).
pub const MAGIC: &[u8] = b"#netidx-tpm-sealed-v1\0";

/// Does `data` carry a TPM-sealed payload (vs a plaintext secret)?
pub fn is_sealed(data: &[u8]) -> bool {
    data.starts_with(MAGIC)
}

/// A long random secret (256 bits, lowercase hex — printable, so it
/// composes with anything that expects a password string). Used as the
/// generated password when a key is sealed rather than typed.
pub fn random_secret() -> zeroize::Zeroizing<String> {
    use rand::Rng;
    use std::fmt::Write;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let mut s = String::with_capacity(64);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    zeroize::Zeroizing::new(s)
}

/// One marshalled TPM command in, one complete raw response frame out.
/// The only piece of this crate that touches a platform API.
pub trait Transport {
    fn exchange(&mut self, command: &[u8]) -> Result<Vec<u8>>;
}

mod ops {
    use super::Transport;
    use anyhow::{anyhow, bail, Context, Result};
    use tpm2_protocol::{
        basic::TpmHandle,
        constant::TPM_MAX_COMMAND_SIZE,
        data::{
            Tpm2bAuth, Tpm2bData, Tpm2bDigest, Tpm2bNonce, Tpm2bPublic, Tpm2bSensitiveCreate,
            Tpm2bSensitiveData, TpmAlgId, TpmCc, TpmEccCurve, TpmRc, TpmRcBase, TpmRh, TpmSt,
            TpmaObject, TpmaSession, TpmlPcrSelection, TpmsAuthCommand, TpmsEccParms,
            TpmsEccPoint, TpmsKeyedhashParms, TpmsSensitiveCreate, TpmtEccScheme, TpmtKdfScheme,
            TpmtKeyedhashScheme, TpmtPublic, TpmtSymDefObject, TpmuAsymScheme, TpmuKdfScheme,
            TpmuKeyedhashScheme, TpmuPublicId, TpmuPublicParms, TpmuSymKeyBits, TpmuSymMode,
        },
        frame::{
            tpm_marshal_command, TpmCreateCommand, TpmCreatePrimaryCommand,
            TpmFlushContextCommand, TpmFrame, TpmMarshalBody, TpmResponse, TpmUnsealCommand,
        },
        TpmMarshal, TpmResult, TpmSized, TpmWriter,
    };
    use zeroize::{Zeroize, Zeroizing};

    /// Sealed-data capacity every TPM 2.0 guarantees (MAX_SYM_DATA).
    /// The protocol types allow more, but a portable seal must not.
    pub const MAX_SEAL_BYTES: usize = 128;

    /// The empty-password authorization session (TPM_RS_PW). We assume
    /// an empty owner-hierarchy auth, which is how every mainstream
    /// distro ships; a site that sets an owner password gets a clear
    /// TPM_RC_BAD_AUTH and falls back to its plaintext path.
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
                kdf: TpmtKdfScheme { scheme: TpmAlgId::Null, details: TpmuKdfScheme::Null },
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
            let tag = if sessions.is_empty() { TpmSt::NoSessions } else { TpmSt::Sessions };
            tpm_marshal_command(cmd, tag, sessions, &mut writer)
                .map_err(|e| anyhow!("marshalling {cc}: {e}"))?;
            writer.len()
        };
        let resp = dev
            .exchange(&buf[..len])
            .with_context(|| format!("exchanging {cc} with the TPM"))?;
        buf.zeroize();
        let frame = TpmResponse::cast(&resp).map_err(|e| anyhow!("parsing {cc} response: {e}"))?;
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
        let frame = TpmResponse::cast(resp).map_err(|e| anyhow!("response reparse: {e}"))?;
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

    pub fn seal(dev: &mut dyn Transport, secret: &[u8]) -> Result<Vec<u8>> {
        if secret.len() > MAX_SEAL_BYTES {
            bail!(
                "cannot TPM-seal {} bytes; every TPM 2.0 guarantees only \
                 {MAX_SEAL_BYTES} bytes of sealed data",
                secret.len()
            );
        }
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
            let (private, rest) = take_tpm2b(params).context("TPM2_Create out_private")?;
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
        let Some(body) = blob.strip_prefix(super::MAGIC) else {
            bail!("not a netidx TPM-sealed blob (bad magic)");
        };
        let (private, rest) = take_tpm2b(body).context("sealed blob private area")?;
        let (public, rest) = take_tpm2b(rest).context("sealed blob public area")?;
        if !rest.is_empty() {
            bail!("trailing garbage after the sealed blob");
        }
        let primary = create_primary(dev)?;
        let result = (|| {
            let cmd = RawLoadCommand { parent: primary, in_private: private, in_public: public };
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
        result
    }
}

pub use ops::MAX_SEAL_BYTES;

/// Seal `secret` to this host's TPM over the platform transport.
pub fn seal(secret: &[u8]) -> Result<Vec<u8>> {
    let mut dev = platform::transport()?;
    ops::seal(&mut dev, secret)
}

/// Unseal a blob produced by [`seal`] on this same machine.
pub fn unseal(blob: &[u8]) -> Result<zeroize::Zeroizing<Vec<u8>>> {
    let mut dev = platform::transport()?;
    ops::unseal(&mut dev, blob)
}

/// Is a TPM usable here (device present and accessible)?
pub fn available() -> bool {
    platform::transport().is_ok()
}

#[cfg(target_os = "linux")]
mod platform {
    use super::Transport;
    use anyhow::{bail, Context, Result};
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
        let file =
            OpenOptions::new().read(true).write(true).open(DEVICE).with_context(|| {
                format!(
                    "opening {DEVICE} (no TPM 2.0, or this user lacks access — \
                     the device node is conventionally root:tss)"
                )
            })?;
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

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::Transport;
    use anyhow::{bail, Result};

    pub struct NoDevice;

    pub fn transport() -> Result<NoDevice> {
        bail!("TPM sealing is not supported on this platform yet (linux only)")
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
        let a = random_secret();
        let b = random_secret();
        assert_eq!(a.len(), 64);
        assert_ne!(*a, *b);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
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
        // Corrupting the private area must fail loudly, not produce data.
        let mut bad = blob.clone();
        let last_private_byte = MAGIC.len() + 10;
        bad[last_private_byte] ^= 0xff;
        assert!(unseal(&bad).is_err());
    }
}
