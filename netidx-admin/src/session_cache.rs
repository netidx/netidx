//! TPM/Secure-Enclave sealed per-admin domain administrator session cache.

use crate::{admin_proto::Secret, atomic, fingerprint::Fingerprint};
use anyhow::{Context, Result, bail};
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

const VERSION: u32 = 1;

#[derive(Clone, Serialize, Deserialize)]
pub struct CachedSession {
    pub ca_fingerprint: String,
    pub bootstrap: SocketAddr,
    pub admin: String,
    pub token: Secret,
    pub issued_unix: u64,
    pub absolute_deadline_unix: u64,
    pub idle_timeout_secs: u64,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    version: u32,
    session: CachedSession,
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Process-local fallback used by the TUI when the platform has no sealing
/// facility. One-shot commands deliberately do not populate this: they keep
/// using password authentication when a persistent cache cannot be protected.
fn memory() -> &'static Mutex<BTreeMap<String, CachedSession>> {
    static MEMORY: OnceLock<Mutex<BTreeMap<String, CachedSession>>> = OnceLock::new();
    MEMORY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

pub fn remember(mut session: CachedSession) -> Result<()> {
    let (_, normalized) = normalized(&session.ca_fingerprint)?;
    session.ca_fingerprint = normalized.clone();
    memory().lock().expect("session memory poisoned").insert(normalized, session);
    Ok(())
}

fn root() -> Result<PathBuf> {
    let mut root = if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
        PathBuf::from(runtime)
    } else {
        dirs::data_local_dir().context(
            "neither a per-user runtime directory nor local data directory is available",
        )?
    };
    root.push("netidx");
    root.push("admin-sessions");
    Ok(root)
}

fn normalized(fp: &str) -> Result<(Fingerprint, String)> {
    let fp = Fingerprint::parse_text(fp)?;
    Ok((fp, fp.text()))
}

fn path_for(fp: &Fingerprint) -> Result<PathBuf> {
    let mut path = root()?;
    let mut name = String::with_capacity(64);
    for byte in fp.bytes() {
        use std::fmt::Write;
        let _ = write!(name, "{byte:02x}");
    }
    path.push(format!("{name}.sealed"));
    Ok(path)
}

pub fn store(mut session: CachedSession) -> Result<()> {
    if !netidx_tpm::available() {
        bail!(
            "{} sealing is unavailable; refusing to persist an administrator session",
            netidx_tpm::MECHANISM
        );
    }
    let (fp, text) = normalized(&session.ca_fingerprint)?;
    session.ca_fingerprint = text.clone();
    let plaintext = serde_json::to_vec(&Payload { version: VERSION, session })
        .context("encoding administrator session")?;
    let sealed = netidx_tpm::seal(&plaintext)
        .context("sealing administrator session to this platform")?;
    let path = path_for(&fp)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    atomic::write_atomic(&path, &sealed, 0o600)
        .with_context(|| format!("writing {}", path.display()))?;
    memory().lock().expect("session memory poisoned").remove(&text);
    Ok(())
}

/// A cached session speaks for exactly one admin: the one it was minted for.
/// The cache is keyed by admin domain, so asking it for a *different* admin is a
/// question it cannot answer — and answering with whoever is cached would
/// silently run the command as them. It declines instead, and leaves the
/// session it holds alone: naming someone else is no reason to end a login.
fn answers_for(session: &CachedSession, admin: Option<&str>) -> bool {
    admin.is_none_or(|admin| admin == session.admin)
}

/// The session cached for `ca_fingerprint`, or `None` if there is none, it has
/// expired, or it belongs to an admin other than `admin`. `admin` is `None`
/// when the caller named nobody and means "whoever I am logged in as".
pub fn load(ca_fingerprint: &str, admin: Option<&str>) -> Result<Option<CachedSession>> {
    let (fp, normalized) = normalized(ca_fingerprint)?;
    {
        let mut memory = memory().lock().expect("session memory poisoned");
        if let Some(session) = memory.get(&normalized) {
            if now() < session.absolute_deadline_unix {
                return Ok(answers_for(session, admin).then(|| session.clone()));
            }
            memory.remove(&normalized);
        }
    }
    let path = path_for(&fp)?;
    let blob = match std::fs::read(&path) {
        Ok(blob) => blob,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let plaintext = netidx_tpm::unseal(&blob)
        .with_context(|| format!("unsealing {}", path.display()))?;
    let payload: Payload = serde_json::from_slice(&plaintext)
        .with_context(|| format!("decoding {}", path.display()))?;
    if payload.version != VERSION || payload.session.ca_fingerprint != normalized {
        bail!(
            "sealed administrator session has the wrong version or admin domain identity"
        );
    }
    if now() >= payload.session.absolute_deadline_unix {
        let _ = std::fs::remove_file(&path);
        return Ok(None);
    }
    let answers = answers_for(&payload.session, admin);
    Ok(answers.then_some(payload.session))
}

pub fn delete(ca_fingerprint: &str) -> Result<()> {
    let (fp, normalized) = normalized(ca_fingerprint)?;
    memory().lock().expect("session memory poisoned").remove(&normalized);
    let path = path_for(&fp)?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

pub fn load_all() -> Result<Vec<CachedSession>> {
    let mut sessions: BTreeMap<String, CachedSession> = memory()
        .lock()
        .expect("session memory poisoned")
        .iter()
        .filter(|(_, session)| now() < session.absolute_deadline_unix)
        .map(|(fp, session)| (fp.clone(), session.clone()))
        .collect();
    let root = root()?;
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(sessions.into_values().collect());
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", root.display())),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sealed") {
            continue;
        }
        let Ok(blob) = std::fs::read(&path) else { continue };
        let Ok(plain) = netidx_tpm::unseal(&blob) else { continue };
        let Ok(payload) = serde_json::from_slice::<Payload>(&plain) else { continue };
        if payload.version == VERSION && now() < payload.session.absolute_deadline_unix {
            sessions.insert(payload.session.ca_fingerprint.clone(), payload.session);
        }
    }
    Ok(sessions.into_values().collect())
}

pub fn delete_all() -> Result<()> {
    memory().lock().expect("session memory poisoned").clear();
    let root = root()?;
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", root.display())),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("sealed") {
            std::fs::remove_file(&path)
                .with_context(|| format!("removing {}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cached(deadline: u64) -> CachedSession {
        CachedSession {
            ca_fingerprint: Fingerprint::of_der(uuid::Uuid::new_v4().as_bytes()).text(),
            bootstrap: "127.0.0.1:4565".parse().unwrap(),
            admin: "alice".into(),
            token: Secret("opaque-token".into()),
            issued_unix: now(),
            absolute_deadline_unix: deadline,
            idle_timeout_secs: 1800,
        }
    }

    #[test]
    fn process_memory_cache_round_trip_and_expiry() {
        let valid = cached(now().saturating_add(60));
        remember(valid.clone()).unwrap();
        assert_eq!(
            load(&valid.ca_fingerprint, None).unwrap().unwrap().token.as_str(),
            valid.token.as_str()
        );
        delete(&valid.ca_fingerprint).unwrap();

        let expired = cached(now());
        remember(expired.clone()).unwrap();
        assert!(load(&expired.ca_fingerprint, None).unwrap().is_none());
    }

    /// The cache is keyed by admin domain, not by admin, so one login's token is
    /// what every later command for that CA finds. Asking for someone else
    /// must not be answered with it: the command would run as the cached
    /// admin under the other one's name — and for `change-password` that
    /// meant rekeying a slot nobody asked about.
    #[test]
    fn a_cached_session_does_not_answer_for_another_admin() {
        let session = cached(now().saturating_add(60));
        remember(session.clone()).unwrap();
        assert_eq!(session.admin, "alice");

        assert!(load(&session.ca_fingerprint, Some("bob")).unwrap().is_none());
        // Declining is not forgetting: alice is still logged in, both for the
        // command that names her and for the one that names nobody.
        assert!(load(&session.ca_fingerprint, Some("alice")).unwrap().is_some());
        assert!(load(&session.ca_fingerprint, None).unwrap().is_some());
        delete(&session.ca_fingerprint).unwrap();
    }

    /// The whole persistent path on real hardware, which is where this
    /// broke: a session is ~300 bytes and a TPM seals 128 in one object,
    /// so `store` failed outright and `netidx admin login` could not
    /// persist anything on any machine with a TPM. Nothing above this
    /// layer noticed, because the only frontend that keeps a session
    /// without one — the TUI — falls back to process memory.
    ///
    /// Runs only where a TPM is reachable; the fingerprint is random, so
    /// it can never collide with a real cached session, and it is
    /// deleted either way.
    #[test]
    fn a_real_session_survives_sealing_to_this_machine() {
        if !netidx_tpm::available() {
            eprintln!("skipping: no usable sealing mechanism on this host");
            return;
        }
        let session = cached(now().saturating_add(3600));
        let fp = session.ca_fingerprint.clone();
        let outcome = (|| -> Result<()> {
            assert!(
                serde_json::to_vec(&Payload {
                    version: VERSION,
                    session: session.clone()
                })?
                .len()
                    > netidx_tpm::MAX_SEAL_BYTES,
                "this test is pointless if a session fits in one sealed object"
            );
            store(session.clone())?;
            let back = load(&fp, Some(&session.admin))?.context("nothing was cached")?;
            assert_eq!(back.token.as_str(), session.token.as_str());
            assert_eq!(back.admin, session.admin);
            assert_eq!(back.bootstrap, session.bootstrap);
            assert_eq!(back.absolute_deadline_unix, session.absolute_deadline_unix);
            // On disk it is sealed, not merely encoded.
            let raw = std::fs::read(path_for(&normalized(&fp)?.0)?)?;
            assert!(netidx_tpm::is_sealed(&raw));
            assert!(
                !raw.windows(session.token.0.len())
                    .any(|w| w == session.token.0.as_bytes())
            );
            Ok(())
        })();
        let _ = delete(&fp);
        outcome.unwrap();
    }

    #[test]
    fn persistent_cache_refuses_unsealed_storage() {
        if netidx_tpm::available() {
            return;
        }
        let session = cached(now().saturating_add(60));
        let error = store(session).unwrap_err().to_string();
        assert!(error.contains("unavailable"));
    }
}
