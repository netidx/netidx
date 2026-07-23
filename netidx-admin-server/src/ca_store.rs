//! The CA's request store — one self-contained JSON record per request,
//! one atomic write per state transition. The daemon is the sole owner
//! (the installation-wide config-directory guard, taken at startup) and its state
//! loop serializes mutations. Each fact that must be atomic lives in exactly
//! one file.
//!
//! Three directories keep the active working set away from history:
//!   - `queue/<id>.json`  — Pending requests (the active queue).
//!   - `issued/<id>.json` — Signed records: the queue outcome **and** the
//!     issuance index **and** the id-map groups **and** the revocation/push
//!     state, all in one record. Expired terminal records are compacted.
//!   - `denied/<id>.json` — Denied requests, pruned with the queue by TTL.
//!
//! A terminal transition *moves* the record out of `queue/` (write the
//! `issued/`/`denied/` file — the atomic commit — then remove the `queue/`
//! file). `status` checks `issued/`→`denied/`→`queue/`, so a stale `queue/`
//! file left by a crash between the two steps loses to the committed
//! terminal record and is swept by [`prune`].
//!
//! Request ids are random hex, validated on every lookup (an id is a
//! wire-supplied string that becomes a file name): anything but
//! `[0-9a-f]{32}` is rejected (no path traversal).

use crate::{
    admin_proto::{EnrollmentRequest, NodeKind},
    atomic,
    config_lock::ConfigDirLock,
};
use anyhow::{Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

/// Pending/denied records older than this are pruned. Signed outcomes are kept
/// for at least this long even when their certificate has already expired.
pub const TTL: Duration = Duration::from_secs(24 * 3600);

/// Cap on pending requests — a bound on unauthenticated disk writes,
/// far above any real enrollment burst.
pub const MAX_PENDING: usize = 64;

pub fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedReq {
    pub id: String,
    pub kind: NodeKind,
    pub csr_pem: String,
    pub requested_name: String,
    #[serde(with = "humantime_serde")]
    pub requested_validity: Duration,
    /// Unix seconds when the request was queued.
    pub received_unix: u64,
    /// Socket address the request arrived from (display context).
    pub peer: String,
    /// `Some(serial)` ⇒ the enqueue connection was authenticated by the
    /// live, in-index certificate of serial `serial` for exactly
    /// `requested_name` (and its presented key matched that record) — a
    /// proof-of-possession renewal. The serial is re-checked still-live
    /// by the state owner at approval, so a revocation between enqueue
    /// and approval cannot be outrun. `None` ⇒ an ordinary request.
    #[serde(default)]
    pub renewal_of: Option<u64>,
    /// `Some` ⇒ a admin-server enrollment.
    #[serde(default)]
    pub enrollment: Option<EnrollmentRequest>,
    /// Exact live certificate this approved restore request replaces.
    #[serde(default)]
    pub replaces_serial: Option<u64>,
}

impl QueuedReq {
    /// A fresh request: random id, stamped now.
    pub fn new(
        kind: NodeKind,
        csr_pem: String,
        requested_name: String,
        requested_validity: Duration,
        peer: String,
        renewal_of: Option<u64>,
        enrollment: Option<EnrollmentRequest>,
    ) -> Self {
        QueuedReq {
            id: new_id(),
            kind,
            csr_pem,
            requested_name,
            requested_validity,
            received_unix: now_unix(),
            peer,
            renewal_of,
            enrollment,
            replaces_serial: None,
        }
    }

    /// A verified renewal: it continues an identity already approved once,
    /// proven by possession of its live key at enqueue.
    pub fn is_verified_renewal(&self) -> bool {
        self.renewal_of.is_some()
    }

    pub fn age_secs(&self) -> u64 {
        now_unix().saturating_sub(self.received_unix)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Revocation {
    pub serial: u64,
    pub revoked_unix: u64,
    pub reason: String,
}

/// A signed (issued) record — the permanent issuance entry. Carries the
/// originating request, the cert, the id-map groups, and the
/// revocation/push state, so "request `id` is Signed as serial `S`, here
/// is its cert and groups" is one atomic file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedRecord {
    pub req: QueuedReq,
    pub serial: u64,
    /// The DNS SAN actually signed (== `req.requested_name`, or the
    /// reserved serving name for a admin-server enrollment).
    pub name: String,
    /// SPKI fingerprint of the leaf public key (revoke UI glyph).
    pub spki_fp: String,
    pub cert_pem: String,
    #[serde(default)]
    pub groups: Vec<String>,
    pub not_after_unix: u64,
    pub issued_unix: u64,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub revoked: Option<Revocation>,
    /// Whether the id-map registration for this identity has been pushed.
    #[serde(default)]
    pub push_done: bool,
}

impl IssuedRecord {
    pub fn live(&self, now_unix: u64) -> bool {
        self.revoked.is_none() && self.not_after_unix > now_unix
    }

    /// A one-line human description for operator-facing messages: serial,
    /// issue and expiry dates, and the key glyph — enough to recognise a
    /// pre-existing certificate and find it in `ca issued` to revoke. Matches
    /// the fields `ca issued` prints (glyph == `spki_fp`).
    pub fn describe(&self) -> String {
        format!(
            "serial {}, issued {}, expires {}, glyph {}",
            self.serial,
            fmt_day(self.issued_unix),
            fmt_day(self.not_after_unix),
            self.spki_fp,
        )
    }
}

/// A unix timestamp as a UTC `YYYY-MM-DD` day, or `@<secs>` if it falls
/// outside the representable range. Uses only date accessors, so it needs no
/// `time` formatting feature.
fn fmt_day(unix: u64) -> String {
    match time::OffsetDateTime::from_unix_timestamp(unix as i64) {
        Ok(d) => format!("{:04}-{:02}-{:02}", d.year(), u8::from(d.month()), d.day()),
        Err(_) => format!("@{unix}"),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeniedRecord {
    req: QueuedReq,
    reason: String,
}

/// The poll outcome derived from a Signed record + the current trust
/// bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedOutcome {
    pub signed_cert_pem: String,
    pub trusted_pem: String,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeniedOutcome {
    pub reason: String,
}

/// The state of a request id.
pub enum Status {
    Pending(QueuedReq),
    Signed(SignedOutcome),
    Denied(DeniedOutcome),
    Unknown,
}

/// How long a signed CRL remains valid (`nextUpdate`). Generous, because
/// re-signing needs an admin password (the vault has no signing capability
/// at rest) — but every admin session re-signs opportunistically (see
/// [`CAStore::refresh_crl_if_stale`]), so a CRL only approaches this age on
/// a network where no admin has signed, approved, or revoked anything for
/// months.
pub const CRL_VALIDITY: Duration = Duration::from_secs(90 * 24 * 3600);

/// Re-sign the CRL when less than this much of its validity remains.
pub const CRL_REFRESH: Duration = Duration::from_secs(30 * 24 * 3600);

/// A CA directory owned by an exclusively guarded netidx installation.
pub struct CaDir {
    pub store: CAStore,
    pub vault: crate::ca_vault::CAVault,
    pub autorenew_pw: Option<Zeroizing<String>>,
    /// The CA's configured lifetime policy (default leaf validity, CA renewal
    /// threshold), read from `lifetimes.json` at open. Defaults when absent,
    /// so a CA predating the file keeps today's behaviour. A daemon picks up
    /// edits on restart.
    pub lifetimes: crate::ca::CaLifetimes,
    pub sessions: crate::session::SessionStore,
    /// The CA directory path — a lockless accessor for the netmap, the CA
    /// cert, and other files that are neither the store nor the vault.
    dir: PathBuf,
    _config_lock: ConfigDirLock,
}

impl CaDir {
    pub async fn open(lock: ConfigDirLock, dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = lock.require_contained(dir.into())?;
        Self::open_inner(lock, dir).await
    }

    async fn open_inner(lock: ConfigDirLock, dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating {}", dir.display()))?;
        let lifetimes = crate::ca::CaLifetimes::load_async(&dir).await?;
        Ok(CaDir {
            store: CAStore::open(dir.clone()).await?,
            vault: crate::ca_vault::CAVault::open(dir.clone()).await?,
            autorenew_pw: None,
            lifetimes,
            sessions: crate::session::SessionStore::default(),
            dir,
            _config_lock: lock,
        })
    }

    /// The CA directory path — for the netmap, the CA cert, and other
    /// files in the dir that are neither the request store nor the vault.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn config_lock(&self) -> ConfigDirLock {
        self._config_lock.clone()
    }
}

/// Generate a fresh random request id (16 bytes, lowercase hex).
pub fn new_id() -> String {
    use rand::Rng;
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// True iff `id` has exactly the shape [`new_id`] produces. Everything
/// that touches the filesystem goes through this — ids arrive over the
/// wire.
pub fn valid_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The `notAfter` of the first (leaf) certificate in a signed PEM, as
/// unix seconds. The record's liveness has to reflect the cert the CA
/// actually signed: [`crate::ca::Ca::sign_request`] clamps a leaf to the
/// CA's remaining lifetime, so the *requested* validity can overstate it —
/// and an overstated `not_after_unix` would keep an already-expired cert
/// "live" (blocking its replacement) and pinned in the CRL too long.
fn cert_not_after_unix(cert_pem: &str) -> Result<u64> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem.as_bytes()))
        .next()
        .ok_or_else(|| anyhow::anyhow!("no certificate in signed PEM"))?
        .context("parsing signed certificate PEM")?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| anyhow::anyhow!("parsing signed certificate: {e}"))?;
    Ok(cert.validity().not_after.timestamp() as u64)
}

/// The `nextUpdate` of the CRL at `path`, unix seconds. `Ok(None)` if
/// there is no CRL.
async fn crl_next_update_at(path: &Path) -> Result<Option<u64>> {
    use x509_parser::prelude::{CertificateRevocationList, FromDer};
    let pem = match tokio::fs::read(path).await {
        Ok(p) => p,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let der = rustls_pemfile::crls(&mut std::io::Cursor::new(&pem))
        .next()
        .ok_or_else(|| anyhow::anyhow!("no CRL in {}", path.display()))?
        .context("parsing CRL PEM")?;
    let (_, crl) = CertificateRevocationList::from_der(der.as_ref())
        .map_err(|e| anyhow::anyhow!("parsing CRL: {e}"))?;
    Ok(crl.next_update().map(|t| t.timestamp() as u64))
}

/// The CA's request store + published CRL, rooted at one directory. Reach
/// it through [`CaDir`], which retains the config-directory guard; read
/// methods take `&self`, write methods `&mut self`.
pub struct CAStore {
    dir: PathBuf,
    /// In-memory next-serial counter, seeded from disk at [`open`](Self::open)
    /// and bumped by [`alloc_serial`](Self::alloc_serial). The committed
    /// record's serial is the persistent source; a fresh open re-seeds past
    /// it.
    serial_counter: u64,
}

pub(crate) struct IssuedRecords {
    entries: Option<tokio::fs::ReadDir>,
}

impl IssuedRecords {
    pub(crate) async fn next(&mut self) -> Result<Option<IssuedRecord>> {
        let Some(entries) = self.entries.as_mut() else { return Ok(None) };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
            if !valid_id(id) {
                continue;
            }
            let Ok(bytes) = tokio::fs::read(&path).await else { continue };
            let Ok(record) = serde_json::from_slice::<IssuedRecord>(&bytes) else {
                continue;
            };
            if record.req.id == id {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }
}

impl CAStore {
    /// Open the store rooted at `dir`, seeding the in-memory serial counter
    /// from disk.
    async fn open(dir: PathBuf) -> Result<Self> {
        let mut s = CAStore { dir, serial_counter: 0 };
        s.serial_counter = s.next_serial().await?;
        s.prune().await?;
        s.compact_issued(now_unix()).await?;
        Ok(s)
    }

    /// Allocate the next serial, bumping the in-memory counter.
    pub fn alloc_serial(&mut self) -> u64 {
        let s = self.serial_counter;
        self.serial_counter += 1;
        s
    }

    /// The CA directory this store is rooted at.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn queue_dir(&self) -> PathBuf {
        self.dir.join("queue")
    }
    fn issued_dir(&self) -> PathBuf {
        self.dir.join("issued")
    }
    fn denied_dir(&self) -> PathBuf {
        self.dir.join("denied")
    }
    fn queue_path(&self, id: &str) -> PathBuf {
        self.queue_dir().join(format!("{id}.json"))
    }
    fn issued_path(&self, id: &str) -> PathBuf {
        self.issued_dir().join(format!("{id}.json"))
    }
    fn denied_path(&self, id: &str) -> PathBuf {
        self.denied_dir().join(format!("{id}.json"))
    }

    /// The trust bundle handed to joining nodes: the admin's `trusted.pem`
    /// federation bundle if present, else the CA's own cert.
    pub async fn read_trusted_bundle(&self) -> Result<String> {
        let bundle = self.dir.join("trusted.pem");
        let path = if tokio::fs::try_exists(&bundle).await? {
            bundle
        } else {
            self.dir.join("certificate.pem")
        };
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        String::from_utf8(bytes).context("trust bundle is not utf8")
    }

    /// Read the issued record for `id`, if it exists (the request was
    /// signed). The daemon uses this on a Signed poll to decide whether the
    /// id-map push still needs to run (`groups` non-empty and `!push_done`).
    pub async fn read_issued(&self, id: &str) -> Result<Option<IssuedRecord>> {
        if !valid_id(id) {
            return Ok(None);
        }
        match tokio::fs::read(self.issued_path(id)).await {
            Ok(b) => {
                Ok(Some(serde_json::from_slice(&b).context("parsing issued record")?))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading issued record {id}")),
        }
    }

    async fn outcome_of(&self, rec: &IssuedRecord) -> Result<SignedOutcome> {
        Ok(SignedOutcome {
            signed_cert_pem: rec.cert_pem.clone(),
            trusted_pem: self.read_trusted_bundle().await?,
            warnings: rec.warnings.clone(),
        })
    }

    /// Start an incremental scan of `issued/`. Unparseable files are skipped
    /// (an operator might hand-edit in an emergency).
    pub(crate) async fn issued_records(&self) -> Result<IssuedRecords> {
        let dir = self.issued_dir();
        let entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => Some(entries),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(e).with_context(|| format!("listing {}", dir.display()));
            }
        };
        Ok(IssuedRecords { entries })
    }

    /// All issued records (a scan of `issued/`).
    async fn all_issued(&self) -> Result<Vec<IssuedRecord>> {
        let mut entries = self.issued_records().await?;
        let mut out = Vec::new();
        while let Some(record) = entries.next().await? {
            out.push(record);
        }
        Ok(out)
    }

    /// Add a request to the active queue. Prunes first; refuses at
    /// [`MAX_PENDING`].
    pub async fn enqueue(&mut self, req: &QueuedReq) -> Result<()> {
        anyhow::ensure!(valid_id(&req.id), "malformed request id");
        let dir = self.queue_dir();
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating {}", dir.display()))?;
        self.prune().await?;
        anyhow::ensure!(
            self.pending().await?.len() < MAX_PENDING,
            "the signing queue is full ({MAX_PENDING} pending requests)"
        );
        let bytes =
            serde_json::to_vec_pretty(req).context("serializing queued request")?;
        atomic::write_atomic_async(&self.queue_path(&req.id), &bytes, 0o644).await
    }

    /// Every pending request (active, not expired, not already terminal),
    /// oldest first.
    pub async fn pending(&self) -> Result<Vec<QueuedReq>> {
        let dir = self.queue_dir();
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(e).with_context(|| format!("listing {}", dir.display()));
            }
        };
        let now = now_unix();
        let mut out: Vec<QueuedReq> = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
            if !valid_id(id) {
                continue;
            }
            let Ok(bytes) = tokio::fs::read(&path).await else { continue };
            let Ok(req) = serde_json::from_slice::<QueuedReq>(&bytes) else { continue };
            let expired = now.saturating_sub(req.received_unix) > TTL.as_secs();
            let terminal = tokio::fs::try_exists(self.issued_path(id)).await?
                || tokio::fs::try_exists(self.denied_path(id)).await?;
            if !expired && !terminal {
                out.push(req);
            }
        }
        out.sort_by_key(|r| r.received_unix);
        Ok(out)
    }

    /// The state of request `id` — terminal records win over a stale queue
    /// entry.
    pub async fn status(&self, id: &str) -> Result<Status> {
        if !valid_id(id) {
            return Ok(Status::Unknown);
        }
        if let Some(rec) = self.read_issued(id).await? {
            return Ok(Status::Signed(self.outcome_of(&rec).await?));
        }
        if let Ok(bytes) = tokio::fs::read(self.denied_path(id)).await {
            let d: DeniedRecord =
                serde_json::from_slice(&bytes).context("parsing denied record")?;
            return Ok(Status::Denied(DeniedOutcome { reason: d.reason }));
        }
        match tokio::fs::read(self.queue_path(id)).await {
            Ok(bytes) => {
                let req: QueuedReq =
                    serde_json::from_slice(&bytes).context("parsing queued request")?;
                if now_unix().saturating_sub(req.received_unix) > TTL.as_secs() {
                    Ok(Status::Unknown)
                } else {
                    Ok(Status::Pending(req))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Status::Unknown),
            Err(e) => Err(e).context("reading queued request"),
        }
    }

    /// Commit an issuance: write the `issued/` record (the atomic commit),
    /// then remove the `queue/` entry (cleanup). The single write is the
    /// transaction; a crash before it leaves the request Pending, after it
    /// leaves a complete Signed record.
    pub async fn commit_signed(&mut self, rec: &IssuedRecord) -> Result<()> {
        anyhow::ensure!(valid_id(&rec.req.id), "malformed request id");
        let dir = self.issued_dir();
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating {}", dir.display()))?;
        let bytes =
            serde_json::to_vec_pretty(rec).context("serializing issued record")?;
        atomic::write_atomic_async(&self.issued_path(&rec.req.id), &bytes, 0o644).await?;
        let _ = tokio::fs::remove_file(self.queue_path(&rec.req.id)).await;
        Ok(())
    }

    /// Record a denial and move it out of the active queue.
    pub async fn deny(&mut self, req: &QueuedReq, reason: &str) -> Result<()> {
        anyhow::ensure!(valid_id(&req.id), "malformed request id");
        let dir = self.denied_dir();
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("creating {}", dir.display()))?;
        let rec = DeniedRecord { req: req.clone(), reason: reason.to_string() };
        let bytes =
            serde_json::to_vec_pretty(&rec).context("serializing denied record")?;
        atomic::write_atomic_async(&self.denied_path(&req.id), &bytes, 0o644).await?;
        let _ = tokio::fs::remove_file(self.queue_path(&req.id)).await;
        Ok(())
    }

    /// Every live (unexpired, unrevoked) issuance for `name` — the one-live
    /// check and revoke-by-name source. DNS names compare case-insensitively.
    pub async fn live_for_name(&self, name: &str) -> Result<Vec<IssuedRecord>> {
        let now = now_unix();
        Ok(self
            .all_issued()
            .await?
            .into_iter()
            .filter(|r| r.live(now) && r.name.eq_ignore_ascii_case(name))
            .collect())
    }

    /// Every revoked-but-unexpired issuance — the CRL set.
    pub async fn revoked_unexpired(&self) -> Result<Vec<IssuedRecord>> {
        let now = now_unix();
        Ok(self
            .all_issued()
            .await?
            .into_iter()
            .filter(|r| r.revoked.is_some() && r.not_after_unix > now)
            .collect())
    }

    /// Every Signed record (for the admin `list` / revoke UI).
    pub async fn list_signed(&self) -> Result<Vec<IssuedRecord>> {
        self.all_issued().await
    }

    /// Mark serial `serial` revoked (rewrites its one `issued/` record).
    /// Returns true if it was live and is now revoked, false if not found or
    /// already revoked.
    pub async fn revoke(&mut self, serial: u64, rev: Revocation) -> Result<bool> {
        for mut rec in self.all_issued().await? {
            if rec.serial == serial {
                if !rec.live(now_unix()) {
                    return Ok(false);
                }
                rec.revoked = Some(rev);
                let bytes = serde_json::to_vec_pretty(&rec)
                    .context("serializing issued record")?;
                atomic::write_atomic_async(&self.issued_path(&rec.req.id), &bytes, 0o644)
                    .await?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Record that the id-map registration for this issuance has been
    /// pushed (rewrites its `issued/` record).
    pub async fn set_push_done(&mut self, id: &str) -> Result<()> {
        if let Some(mut rec) = self.read_issued(id).await?
            && !rec.push_done
        {
            rec.push_done = true;
            let bytes =
                serde_json::to_vec_pretty(&rec).context("serializing issued record")?;
            atomic::write_atomic_async(&self.issued_path(id), &bytes, 0o644).await?;
        }
        Ok(())
    }

    /// Live issuances whose id-map groups were never confirmed pushed — the
    /// startup/poll id-map recovery set.
    pub async fn pending_pushes(&self) -> Result<Vec<IssuedRecord>> {
        let now = now_unix();
        Ok(self
            .all_issued()
            .await?
            .into_iter()
            .filter(|r| r.live(now) && !r.groups.is_empty() && !r.push_done)
            .collect())
    }

    /// The highest serial ever issued (to seed the in-memory counter at
    /// startup). `None` if nothing has been issued.
    pub async fn max_serial(&self) -> Result<Option<u64>> {
        Ok(self.all_issued().await?.into_iter().map(|r| r.serial).max())
    }

    /// The next X.509 serial to mint: one past every serial the store knows
    /// and the CA cert's own serial. The daemon seeds its in-memory counter
    /// from this once at startup; offline bootstrap issuance (before the
    /// daemon owns the CA) allocates from it per issuance — and records the
    /// result, so the next allocation and the daemon both move past it.
    pub async fn next_serial(&self) -> Result<u64> {
        let max_issued = self.max_serial().await?.unwrap_or(0);
        let ca_cert = crate::ca::ca_cert_serial_async(&self.dir).await.unwrap_or(1);
        Ok(max_issued.max(ca_cert) + 1)
    }

    /// Build and commit the `Signed` record for an issuance — the single
    /// source of "this CA issued serial `S` as `name`, here is its cert and
    /// id-map groups." Shared by the daemon's issuance path and offline
    /// bootstrap issuance so every issued cert lands in the one index.
    pub async fn commit_issuance(
        &mut self,
        req: &QueuedReq,
        serial: u64,
        name: &str,
        cert_pem: &str,
        groups: &[String],
    ) -> Result<()> {
        let now = now_unix();
        // Fingerprint the issued cert's public key, not the CSR's: the two keys
        // are identical for a CSR-based issuance, but a direct `ca issue` has no
        // CSR, and an empty-CSR fingerprint would leave those certs glyph-less
        // (unrevokable by `--assert-glyph`).
        let spki_fp = crate::transport::cert_fingerprint(cert_pem)
            .map(|f| f.text())
            .unwrap_or_default();
        let record = IssuedRecord {
            req: req.clone(),
            serial,
            name: name.to_string(),
            spki_fp,
            cert_pem: cert_pem.to_string(),
            groups: groups.to_vec(),
            not_after_unix: cert_not_after_unix(cert_pem)?,
            issued_unix: now,
            warnings: Vec::new(),
            revoked: None,
            push_done: groups.is_empty(),
        };
        self.commit_signed(&record).await
    }

    /// Remove expired `queue/`/`denied/` entries and any queue entry shadowed
    /// by a terminal record.
    pub async fn prune(&mut self) -> Result<()> {
        let now = now_unix();
        if let Ok(mut entries) = tokio::fs::read_dir(self.queue_dir()).await {
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                if !valid_id(id) {
                    continue;
                }
                let shadowed = tokio::fs::try_exists(self.issued_path(id)).await?
                    || tokio::fs::try_exists(self.denied_path(id)).await?;
                let expired = match tokio::fs::read(&path).await {
                    Ok(b) => match serde_json::from_slice::<QueuedReq>(&b) {
                        Ok(req) => now.saturating_sub(req.received_unix) > TTL.as_secs(),
                        Err(_) => true,
                    },
                    Err(_) => true,
                };
                if shadowed || expired {
                    let _ = tokio::fs::remove_file(&path).await;
                }
            }
        }
        if let Ok(mut entries) = tokio::fs::read_dir(self.denied_dir()).await {
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                if !valid_id(id) {
                    continue;
                }
                let expired = match tokio::fs::read(&path).await {
                    Ok(b) => match serde_json::from_slice::<DeniedRecord>(&b) {
                        Ok(rec) => {
                            now.saturating_sub(rec.req.received_unix) > TTL.as_secs()
                        }
                        Err(_) => true,
                    },
                    Err(_) => true,
                };
                if expired {
                    let _ = tokio::fs::remove_file(&path).await;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn compact_issued(&mut self, now: u64) -> Result<()> {
        let records = self.all_issued().await?;
        let Some(max_serial) = records.iter().map(|record| record.serial).max() else {
            return Ok(());
        };
        let mut live_names = HashSet::new();
        let mut latest_group_serial = HashMap::new();
        for record in &records {
            let name = record.name.to_ascii_lowercase();
            if record.live(now) {
                live_names.insert(name.clone());
            }
            if !record.groups.is_empty() {
                latest_group_serial
                    .entry(name)
                    .and_modify(|serial: &mut u64| *serial = (*serial).max(record.serial))
                    .or_insert(record.serial);
            }
        }
        for record in records {
            if record.not_after_unix > now
                || now.saturating_sub(record.issued_unix) <= TTL.as_secs()
                || record.serial == max_serial
            {
                continue;
            }
            let name = record.name.to_ascii_lowercase();
            let holds_live_groups = live_names.contains(&name)
                && latest_group_serial.get(&name) == Some(&record.serial);
            if !holds_live_groups {
                match tokio::fs::remove_file(self.issued_path(&record.req.id)).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!("removing expired issued record {}", record.req.id)
                        });
                    }
                }
            }
        }
        Ok(())
    }

    // -- published CRL (`<ca-dir>/crl.pem`) ----------------------------------
    //
    // Folded in from the former `ca_index` module: the CRL is just the
    // revoked-but-unexpired set rendered and signed. `&mut self` on the
    // writers and the server's mutable-state guard give scan→sign→rename one serialized
    // mutation, so one writer can't drop a just-revoked serial by racing.

    /// Canonical CRL location: `<ca-dir>/crl.pem`. The admin server serves it
    /// (`GetCrl`); the renewal daemon copies it beside each resolver's
    /// trusted bundle, where netidx's TLS acceptor picks it up.
    pub fn crl_path(&self) -> PathBuf {
        self.dir.join("crl.pem")
    }

    /// Build and atomically store a CRL signed by the CA: every
    /// revoked-but-unexpired serial from the store, `nextUpdate` =
    /// now + [`CRL_VALIDITY`], CRL number = now (monotonic enough — one
    /// CRL per second per CA). `ca_key_pem` is the vault-decrypted CA key;
    /// the CA cert is read from the dir.
    pub async fn write_crl(&mut self, ca_key_pem: &[u8]) -> Result<()> {
        let ca_cert_pem = tokio::fs::read_to_string(self.dir.join("certificate.pem"))
            .await
            .context("reading CA certificate")?;
        let revoked = self.revoked_unexpired().await?;
        let ca_key_pem = ca_key_pem.to_vec();
        let pem = tokio::task::spawn_blocking(move || {
            build_crl(&ca_key_pem, &ca_cert_pem, revoked)
        })
        .await
        .context("CRL signing task panicked")??;
        crate::atomic::write_atomic_async(&self.crl_path(), pem.as_bytes(), 0o644).await
    }

    /// The `nextUpdate` of this store's published CRL, unix seconds.
    /// `Ok(None)` if there is no CRL yet.
    pub async fn crl_next_update(&self) -> Result<Option<u64>> {
        crl_next_update_at(&self.crl_path()).await
    }

    /// Create the CRL if it is missing, or re-sign it when it is nearing its
    /// `nextUpdate`.
    /// Called opportunistically wherever the vault is already unlocked (an
    /// admin password is the only thing that can sign) — best-effort; a
    /// failure is logged by the caller, never fatal.
    pub async fn refresh_crl_if_stale(&mut self, ca_key_pem: &[u8]) -> Result<bool> {
        match self.crl_next_update().await? {
            None => {
                self.write_crl(ca_key_pem).await?;
                Ok(true)
            }
            Some(next_update) => {
                if next_update.saturating_sub(now_unix()) < CRL_REFRESH.as_secs() {
                    self.write_crl(ca_key_pem).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }
}

fn build_crl(
    ca_key_pem: &[u8],
    ca_cert_pem: &str,
    revoked: Vec<IssuedRecord>,
) -> Result<String> {
    use rcgen::{
        CertificateRevocationListParams, Issuer, KeyIdMethod, KeyPair, RevokedCertParams,
        SerialNumber,
    };
    use time::OffsetDateTime;
    // Normalize the CA key to PKCS#8 through openssl — the vault may
    // hold PKCS#1 ("BEGIN RSA PRIVATE KEY") from older generations, and
    // rcgen's ring backend only reads PKCS#8.
    let pkey = openssl::pkey::PKey::private_key_from_pem(ca_key_pem)
        .context("parsing CA key")?;
    let pkcs8 = pkey.private_key_to_pem_pkcs8().context("normalizing CA key")?;
    let key =
        KeyPair::from_pem(std::str::from_utf8(&pkcs8).context("CA key pem not utf8")?)
            .context("loading CA key for CRL signing")?;
    let issuer =
        Issuer::from_ca_cert_pem(ca_cert_pem, key).context("loading CRL issuer")?;
    let now = now_unix();
    let ts = |unix: u64| {
        OffsetDateTime::from_unix_timestamp(unix as i64).context("timestamp out of range")
    };
    let revoked_certs = revoked
        .into_iter()
        .map(|s| {
            let r = s.revoked.expect("revoked_unexpired returns revoked certs");
            Ok(RevokedCertParams {
                serial_number: SerialNumber::from(s.serial),
                revocation_time: ts(r.revoked_unix)?,
                reason_code: None,
                invalidity_date: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let crl = CertificateRevocationListParams {
        this_update: ts(now)?,
        next_update: ts(now + CRL_VALIDITY.as_secs())?,
        crl_number: SerialNumber::from(now),
        issuing_distribution_point: None,
        revoked_certs,
        key_identifier_method: KeyIdMethod::Sha256,
    }
    .signed_by(&issuer)
    .context("signing the CRL")?;
    crl.pem().context("encoding the CRL")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_lock::ConfigDirLock;

    async fn open(dir: &Path) -> CaDir {
        let lock = ConfigDirLock::acquire_for_ca_dir(dir).await.unwrap();
        CaDir::open(lock, dir).await.unwrap()
    }

    fn req(name: &str) -> QueuedReq {
        QueuedReq::new(
            NodeKind::Workstation,
            "CSR".to_string(),
            name.to_string(),
            std::time::Duration::from_secs(30 * 86400),
            "10.0.0.7:51000".to_string(),
            None,
            None,
        )
    }

    fn issued(req: QueuedReq, serial: u64, name: &str, not_after: u64) -> IssuedRecord {
        IssuedRecord {
            req,
            serial,
            name: name.to_string(),
            spki_fp: format!("fp{serial}"),
            cert_pem: format!("CERT{serial}"),
            groups: vec![],
            not_after_unix: not_after,
            issued_unix: now_unix(),
            warnings: vec![],
            revoked: None,
            push_done: false,
        }
    }

    #[test]
    fn ids_are_validated() {
        assert!(valid_id(&new_id()));
        assert_ne!(new_id(), new_id());
        assert!(!valid_id("../../etc/passwd"));
        assert!(!valid_id(""));
        assert!(!valid_id("ABCDEF00112233445566778899aabbcc"));
        assert!(!valid_id("0123456789abcdef"));
    }

    #[tokio::test]
    async fn enqueue_approve_moves_out_of_queue() {
        let dir = tempfile::tempdir().unwrap();
        // Need a CA cert for the trust bundle in the Signed outcome.
        std::fs::write(dir.path().join("certificate.pem"), b"CA-CERT").unwrap();
        let mut ca = open(dir.path()).await;
        let r = req("alice.example.com");
        ca.store.enqueue(&r).await.unwrap();
        assert_eq!(ca.store.pending().await.unwrap().len(), 1);
        assert!(matches!(ca.store.status(&r.id).await.unwrap(), Status::Pending(_)));

        ca.store
            .commit_signed(&issued(r.clone(), 5, "alice.example.com", now_unix() + 1000))
            .await
            .unwrap();
        // Moved out of the active queue, status now Signed with the cert + bundle.
        assert!(ca.store.pending().await.unwrap().is_empty());
        match ca.store.status(&r.id).await.unwrap() {
            Status::Signed(o) => {
                assert_eq!(o.signed_cert_pem, "CERT5");
                assert_eq!(o.trusted_pem, "CA-CERT");
            }
            _ => panic!("expected Signed"),
        }
        assert!(!ca.store.queue_path(&r.id).exists());
        assert!(ca.store.issued_path(&r.id).exists());
    }

    #[tokio::test]
    async fn deny_moves_out_and_status_is_denied() {
        let dir = tempfile::tempdir().unwrap();
        let mut ca = open(dir.path()).await;
        let r = req("bob.example.com");
        ca.store.enqueue(&r).await.unwrap();
        ca.store.deny(&r, "ask your manager").await.unwrap();
        assert!(ca.store.pending().await.unwrap().is_empty());
        match ca.store.status(&r.id).await.unwrap() {
            Status::Denied(d) => assert_eq!(d.reason, "ask your manager"),
            _ => panic!("expected Denied"),
        }
        assert!(!ca.store.queue_path(&r.id).exists());
    }

    #[tokio::test]
    async fn deny_creates_no_certificate() {
        // Denying a queued request must never leave an issued/live cert behind:
        // the enqueue one-live check would then refuse a fresh re-enrollment of
        // the same name. A denial only records a DeniedRecord; the issuance
        // index stays empty, so a later request for the same name is free to
        // queue.
        let dir = tempfile::tempdir().unwrap();
        let mut ca = open(dir.path()).await;
        let r = req("eric.ryu-oh.org");
        ca.store.enqueue(&r).await.unwrap();
        ca.store.deny(&r, "not this time").await.unwrap();
        assert!(
            ca.store.live_for_name("eric.ryu-oh.org").await.unwrap().is_empty(),
            "a denied request must not create a live certificate"
        );
        assert!(!ca.store.issued_path(&r.id).exists());
        assert!(ca.store.list_signed().await.unwrap().is_empty());
    }

    #[test]
    fn describe_names_serial_dates_and_glyph() {
        // The operator-facing one-line description must carry the serial, an
        // ISO day for issue + expiry, and the glyph — so a re-enroll refusal
        // makes clear it's a pre-existing cert findable in `ca issued`.
        let rec = issued(req("eric.ryu-oh.org"), 42, "eric.ryu-oh.org", 1_800_000_000);
        let d = rec.describe();
        assert!(d.contains("serial 42"), "{d}");
        assert!(d.contains("expires 2027-01-15"), "{d}");
        assert!(d.contains("glyph fp42"), "{d}");
    }

    #[tokio::test]
    async fn one_live_and_revoke_and_crl() {
        let dir = tempfile::tempdir().unwrap();
        let mut ca = open(dir.path()).await;
        let now = now_unix();
        let a = req("eric.ryu-oh.org");
        ca.store
            .commit_signed(&issued(a.clone(), 2, "eric.ryu-oh.org", now + 1000))
            .await
            .unwrap();
        let b = req("bob.ryu-oh.org");
        ca.store
            .commit_signed(&issued(b, 3, "bob.ryu-oh.org", now + 1000))
            .await
            .unwrap();
        let old = req("old.ryu-oh.org");
        ca.store
            .commit_signed(&issued(old, 4, "old.ryu-oh.org", now.saturating_sub(10)))
            .await
            .unwrap();

        assert_eq!(ca.store.live_for_name("ERIC.RYU-OH.ORG").await.unwrap().len(), 1);
        assert!(ca.store.live_for_name("old.ryu-oh.org").await.unwrap().is_empty());
        assert!(ca.store.revoked_unexpired().await.unwrap().is_empty());
        assert!(
            !ca.store
                .revoke(
                    4,
                    Revocation { serial: 4, revoked_unix: now, reason: "x".into() }
                )
                .await
                .unwrap()
        );

        assert!(
            ca.store
                .revoke(
                    2,
                    Revocation {
                        serial: 2,
                        revoked_unix: now,
                        reason: "laptop stolen".into()
                    }
                )
                .await
                .unwrap()
        );
        assert!(ca.store.live_for_name("eric.ryu-oh.org").await.unwrap().is_empty());
        let crl = ca.store.revoked_unexpired().await.unwrap();
        assert_eq!(crl.len(), 1);
        assert_eq!(crl[0].serial, 2);
        // Revoking an unknown / already-revoked serial is a no-op false.
        assert!(
            !ca.store
                .revoke(
                    2,
                    Revocation { serial: 2, revoked_unix: now, reason: "x".into() }
                )
                .await
                .unwrap()
        );
        assert!(
            !ca.store
                .revoke(
                    999,
                    Revocation { serial: 999, revoked_unix: now, reason: "x".into() }
                )
                .await
                .unwrap()
        );

        assert_eq!(ca.store.max_serial().await.unwrap(), Some(4));
    }

    #[tokio::test]
    async fn refresh_creates_a_missing_empty_crl() {
        use crate::ca::{Ca, CaParams, Subject};

        let dir = tempfile::tempdir().unwrap();
        Ca::init(
            &CaParams {
                directory: dir.path().to_path_buf(),
                subject: Subject::cn("test-ca"),
                san: vec![],
                key_bits: 2048,
                validity: Duration::from_secs(30 * 86400),
            },
            None,
        )
        .unwrap();
        let key = std::fs::read(dir.path().join("private.key")).unwrap();
        let mut ca = open(dir.path()).await;

        assert!(!ca.store.crl_path().exists());
        assert!(ca.store.refresh_crl_if_stale(&key).await.unwrap());
        assert!(ca.store.crl_path().exists());
        assert!(ca.store.crl_next_update().await.unwrap().is_some());
        assert!(!ca.store.refresh_crl_if_stale(&key).await.unwrap());
    }

    #[tokio::test]
    async fn push_done_recovery_set() {
        let dir = tempfile::tempdir().unwrap();
        let mut ca = open(dir.path()).await;
        let now = now_unix();
        let r = req("u.example.com");
        let mut rec = issued(r.clone(), 7, "u.example.com", now + 1000);
        rec.groups = vec!["users".into()];
        ca.store.commit_signed(&rec).await.unwrap();
        // Has groups, not pushed → in the recovery set.
        assert_eq!(ca.store.pending_pushes().await.unwrap().len(), 1);
        ca.store.set_push_done(&r.id).await.unwrap();
        assert!(ca.store.pending_pushes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn prune_keeps_issued_clears_old_queue_and_denied() {
        let dir = tempfile::tempdir().unwrap();
        let mut ca = open(dir.path()).await;
        let old = now_unix() - TTL.as_secs() - 10;
        let mut stale = req("stale.example.com");
        stale.received_unix = old;
        std::fs::create_dir_all(ca.store.queue_dir()).unwrap();
        atomic::write_atomic(
            &ca.store.queue_path(&stale.id),
            &serde_json::to_vec_pretty(&stale).unwrap(),
            0o644,
        )
        .unwrap();
        // A live issued record survives prune.
        let live = req("live.example.com");
        ca.store
            .commit_signed(&issued(
                live.clone(),
                9,
                "live.example.com",
                now_unix() + 1000,
            ))
            .await
            .unwrap();
        ca.store.prune().await.unwrap();
        assert!(!ca.store.queue_path(&stale.id).exists());
        assert!(ca.store.issued_path(&live.id).exists());
    }

    #[tokio::test]
    async fn compaction_preserves_only_live_state_recent_outcomes_and_serial_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut ca = open(dir.path()).await;
        let now = now_unix();
        let old = now.saturating_sub(TTL.as_secs() + 10);

        let commit = |serial, name: &str, groups: &[&str], expires, issued_at| {
            let mut record = issued(req(name), serial, name, expires);
            record.groups = groups.iter().map(|group| (*group).to_string()).collect();
            record.issued_unix = issued_at;
            record
        };
        let obsolete = commit(1, "old.example", &[], now - 1, old);
        let old_groups = commit(2, "alice.example", &["old"], now - 1, old);
        let current_groups = commit(3, "ALICE.example", &["users"], now - 1, old);
        let renewal = commit(4, "alice.example", &[], now + 1000, old);
        let recent = commit(5, "recent.example", &[], now - 1, now);
        let watermark = commit(6, "watermark.example", &[], now - 1, old);
        for record in
            [&obsolete, &old_groups, &current_groups, &renewal, &recent, &watermark]
        {
            ca.store.commit_signed(record).await.unwrap();
        }

        ca.store.compact_issued(now).await.unwrap();
        assert!(!ca.store.issued_path(&obsolete.req.id).exists());
        assert!(!ca.store.issued_path(&old_groups.req.id).exists());
        assert!(ca.store.issued_path(&current_groups.req.id).exists());
        assert!(ca.store.issued_path(&renewal.req.id).exists());
        assert!(ca.store.issued_path(&recent.req.id).exists());
        assert!(ca.store.issued_path(&watermark.req.id).exists());

        ca.store.compact_issued(now + TTL.as_secs() + 2000).await.unwrap();
        let records = ca.store.list_signed().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].serial, 6);
        assert_eq!(ca.store.next_serial().await.unwrap(), 7);
    }

    #[tokio::test]
    async fn queue_cap_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let mut ca = open(dir.path()).await;
        for i in 0..MAX_PENDING {
            ca.store.enqueue(&req(&format!("n{i}.example.com"))).await.unwrap();
        }
        let err = ca.store.enqueue(&req("overflow.example.com")).await.unwrap_err();
        assert!(format!("{err:#}").contains("full"));
    }
}
