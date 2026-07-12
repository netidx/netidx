use crate::resolver_server::config::{Config, IdMap, MemberServer};
use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use std::{
    ffi::c_void,
    iter,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
};
use tokio::process::Command;
use windows::{
    Win32::{
        Foundation::{HANDLE, HLOCAL, LocalFree},
        NetworkManagement::NetManagement::{
            LG_INCLUDE_INDIRECT, LOCALGROUP_USERS_INFO_0, MAX_PREFERRED_LENGTH,
            NetApiBufferFree, NetUserGetLocalGroups,
        },
        Security::{
            Authorization::ConvertSidToStringSidW, GetLengthSid, GetTokenInformation,
            LookupAccountSidW, PSID, RevertToSelf, SID_NAME_USE, TOKEN_QUERY, TOKEN_USER,
            TokenUser,
        },
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    },
    core::{PCWSTR, PWSTR},
};

/// A NUL-terminated UTF-16 buffer for the wide Win32 APIs.
fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(iter::once(0)).collect()
}

fn as_handle(o: &OwnedHandle) -> HANDLE {
    HANDLE(o.as_raw_handle() as *mut c_void)
}

/// Restores the calling thread's own token on drop, so an impersonation
/// is undone on every exit path including a panic.
struct RevertGuard;

impl Drop for RevertGuard {
    fn drop(&mut self) {
        let _ = unsafe { RevertToSelf() };
    }
}

/// Resolve a `PSID` to a human-readable `DOMAIN\username`. The SID buffer
/// must stay alive for the duration of the call.
fn lookup_account_sid(sid: PSID) -> Result<ArcStr> {
    let mut name_len = 0u32;
    let mut dom_len = 0u32;
    let mut sid_use = SID_NAME_USE(0);
    // Size query — expected to fail with the required lengths.
    let _ = unsafe {
        LookupAccountSidW(
            PCWSTR::null(),
            sid,
            None,
            &mut name_len,
            None,
            &mut dom_len,
            &mut sid_use,
        )
    };
    if name_len == 0 {
        bail!("LookupAccountSidW could not size the account name");
    }
    let mut name = vec![0u16; name_len as usize];
    let mut dom = vec![0u16; dom_len as usize];
    unsafe {
        LookupAccountSidW(
            PCWSTR::null(),
            sid,
            Some(PWSTR(name.as_mut_ptr())),
            &mut name_len,
            Some(PWSTR(dom.as_mut_ptr())),
            &mut dom_len,
            &mut sid_use,
        )
    }
    .context("LookupAccountSidW")?;
    let name = String::from_utf16_lossy(&name[..name_len as usize]);
    let dom = String::from_utf16_lossy(&dom[..dom_len as usize]);
    Ok(if dom.is_empty() {
        ArcStr::from(name)
    } else {
        ArcStr::from(format!("{dom}\\{name}"))
    })
}

/// Read the user SID out of a token and run `f` on it. The `TOKEN_USER`
/// buffer (which the SID points into) is kept alive across the call.
fn with_token_user_sid<T>(token: HANDLE, f: impl FnOnce(PSID) -> Result<T>) -> Result<T> {
    let mut len = 0u32;
    let _ = unsafe { GetTokenInformation(token, TokenUser, None, 0, &mut len) };
    if len == 0 {
        bail!("GetTokenInformation(TokenUser) could not size the buffer");
    }
    let mut buf = vec![0u8; len as usize];
    unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            Some(buf.as_mut_ptr() as *mut c_void),
            len,
            &mut len,
        )
    }
    .context("GetTokenInformation(TokenUser)")?;
    // SAFETY: the buffer holds a TOKEN_USER whose User.Sid points within it.
    let tu = unsafe { &*(buf.as_ptr() as *const TOKEN_USER) };
    f(tu.User.Sid)
}

/// Copy a token's user SID into an owned, self-contained byte buffer so
/// it stays valid after the thread reverts from an impersonation (the
/// SID otherwise points into a buffer, and the lookup must run as the
/// server, not the impersonated client).
fn copy_token_user_sid(token: HANDLE) -> Result<Vec<u8>> {
    with_token_user_sid(token, |sid| {
        let len = unsafe { GetLengthSid(sid) } as usize;
        if len == 0 {
            bail!("GetLengthSid returned 0");
        }
        // SAFETY: `sid` is a valid SID of `len` contiguous bytes.
        let bytes = unsafe { std::slice::from_raw_parts(sid.0 as *const u8, len) };
        Ok(bytes.to_vec())
    })
}

/// The current process user's SID as a string (`S-1-5-…`), used to make
/// the local-auth pipe name unique per user so concurrent RDS/Citrix
/// sessions on one host don't collide.
pub fn current_user_sid_string() -> Result<String> {
    let mut token = HANDLE::default();
    unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) }
        .context("OpenProcessToken")?;
    // SAFETY: OpenProcessToken succeeded, so `token` is a valid owned handle.
    let token = unsafe { OwnedHandle::from_raw_handle(token.0 as RawHandle) };
    with_token_user_sid(as_handle(&token), |sid| {
        let mut s = PWSTR::null();
        unsafe { ConvertSidToStringSidW(sid, &mut s) }
            .context("ConvertSidToStringSidW")?;
        let out = unsafe { s.to_string() }.unwrap_or_default();
        unsafe {
            let _ = LocalFree(Some(HLOCAL(s.0 as *mut c_void)));
        }
        if out.is_empty() { bail!("empty SID string") } else { Ok(out) }
    })
}

/// Derive the local-auth named-pipe name from the configured `Auth::Local`
/// string. An explicit `\\.\pipe\…` is used verbatim (the operator owns
/// any cross-user collision); any other string is a logical seed that the
/// server and client both expand to `\\.\pipe\netidx-local-<seed>-<sid>`,
/// scoping the pipe to the current user.
fn pipe_name(seed: &str) -> Result<String> {
    if seed.starts_with(r"\\.\pipe\") || seed.starts_with(r"\\?\pipe\") {
        return Ok(seed.to_string());
    }
    let mut sanitized: String = seed
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    sanitized.truncate(64);
    let sid = current_user_sid_string()?;
    Ok(format!(r"\\.\pipe\netidx-local-{sanitized}-{sid}"))
}

/// Local group names the user belongs to (direct and indirect), via
/// NetUserGetLocalGroups against the local machine. These come back as
/// the friendly names an operator writes in a perms file
/// (`Administrators`, `Users`, …).
fn local_groups(user: &str) -> Result<Vec<ArcStr>> {
    // For a local-machine query, the SAM account name (the part after any
    // `DOMAIN\`) is what NetUserGetLocalGroups expects.
    let sam = user.rsplit('\\').next().unwrap_or(user);
    let wuser = wide(sam);
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut read = 0u32;
    let mut total = 0u32;
    let rc = unsafe {
        NetUserGetLocalGroups(
            PCWSTR::null(),
            PCWSTR(wuser.as_ptr()),
            0,
            LG_INCLUDE_INDIRECT,
            &mut buf,
            MAX_PREFERRED_LENGTH,
            &mut read,
            &mut total,
        )
    };
    if rc != 0 {
        bail!("NetUserGetLocalGroups for {user:?} failed with code {rc}");
    }
    let mut groups = Vec::with_capacity(read as usize);
    if !buf.is_null() {
        // SAFETY: on success buf points at `read` LOCALGROUP_USERS_INFO_0.
        let entries = unsafe {
            std::slice::from_raw_parts(
                buf as *const LOCALGROUP_USERS_INFO_0,
                read as usize,
            )
        };
        for e in entries {
            let name = unsafe { e.lgrui0_name.to_string() }.unwrap_or_default();
            if !name.is_empty() {
                groups.push(ArcStr::from(name));
            }
        }
        unsafe {
            NetApiBufferFree(Some(buf as *const c_void));
        }
    }
    Ok(groups)
}

// On Windows there is no separate id-map daemon: the access token carries
// group membership, and a SID resolves to a name directly. `Native` reads
// local-group membership via NetUserGetLocalGroups; `Command` runs an
// `id`-style command; `DoNotMap` reports no groups.
#[derive(Clone)]
pub(crate) enum Mapper {
    DoNotMap,
    Command(ArcStr),
    Native,
}

impl Mapper {
    pub(crate) async fn new(_cfg: &Config, member: &MemberServer) -> Result<Mapper> {
        match &member.id_map {
            IdMap::DoNotMap => Ok(Mapper::DoNotMap),
            IdMap::Command(cmd) => Ok(Mapper::Command(ArcStr::from(cmd))),
            IdMap::Socket(_) => bail!("id-map sockets are not supported on windows"),
            IdMap::PlatformDefault => Ok(Mapper::Native),
        }
    }

    pub(crate) async fn groups(&self, user: &str) -> Result<(ArcStr, Vec<ArcStr>)> {
        let parse = |s: &str| {
            let mut primary = Mapper::parse_output(s, "gid=")?;
            let groups = Mapper::parse_output(s, "groups=")?;
            let primary = if primary.is_empty() {
                bail!("missing primary group")
            } else {
                primary.swap_remove(0)
            };
            Ok((primary, groups))
        };
        match &self {
            Mapper::DoNotMap => Ok((user.into(), vec![])),
            Mapper::Command(cmd) => {
                let out = Command::new(&**cmd).arg(user).output().await?;
                parse(String::from_utf8_lossy(&out.stdout).as_ref())
            }
            Mapper::Native => {
                let u = user.to_string();
                let groups =
                    tokio::task::spawn_blocking(move || local_groups(&u)).await??;
                // Windows has no unix-style primary group; use the user as a
                // (harmless) extra entity so the non-empty-primary contract
                // holds.
                Ok((ArcStr::from(user), groups))
            }
        }
    }

    fn parse_output(out: &str, key: &str) -> Result<Vec<ArcStr>> {
        let mut groups = Vec::new();
        match out.find(key) {
            None => Ok(Vec::new()),
            Some(i) => {
                let mut s = &out[i..];
                while let Some(i_op) = s.find('(') {
                    match s.find(')') {
                        None => {
                            return Err(anyhow!(
                                "invalid id command output, expected ')'"
                            ));
                        }
                        Some(i_cp) => {
                            groups.push(ArcStr::from(&s[i_op + 1..i_cp]));
                            s = &s[i_cp + 1..];
                        }
                    }
                }
                Ok(groups)
            }
        }
    }
}

pub(crate) mod local_auth {
    use super::{
        RevertGuard, as_handle, copy_token_user_sid, lookup_account_sid, pipe_name,
    };
    use crate::{
        os::local_auth::Credential,
        resolver_server::config::{Config, MemberServer},
    };
    use ahash::AHashMap;
    use anyhow::{Context, Result, bail};
    use bytes::{Bytes, BytesMut};
    use futures::{channel::oneshot, prelude::*, select_biased};
    use log::{debug, warn};
    use netidx_core::utils::{make_sha3_token, pack};
    use netidx_netproto::resolver::HashMethod;
    use parking_lot::Mutex;
    use rand::{RngExt, rng};
    use std::{
        collections::hash_map::Entry,
        ffi::c_void,
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions},
        task::spawn,
        time::{interval, sleep, timeout},
    };
    use windows::Win32::{
        Foundation::HANDLE,
        Security::{PSID, TOKEN_QUERY},
        System::{
            Pipes::ImpersonateNamedPipeClient,
            Threading::{GetCurrentThread, OpenThreadToken},
        },
    };

    /// Identify the pipe's connecting client as `DOMAIN\username`. Fully
    /// synchronous: the impersonation is reverted (via [`RevertGuard`])
    /// before returning, so it never straddles an `.await`.
    ///
    /// The client's user SID is copied out *while impersonating*, then the
    /// thread reverts before resolving it to a name — `LookupAccountSidW`
    /// makes an LSA lookup the client's identification-level token isn't
    /// permitted to perform, so the resolution must run as the server's own
    /// identity.
    fn peer_user(pipe: &NamedPipeServer) -> Result<arcstr::ArcStr> {
        let h = HANDLE(pipe.as_raw_handle() as *mut c_void);
        let sid = {
            unsafe { ImpersonateNamedPipeClient(h) }
                .context("ImpersonateNamedPipeClient")?;
            let _revert = RevertGuard;
            let mut token = HANDLE::default();
            unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, true, &mut token) }
                .context("OpenThreadToken")?;
            // SAFETY: OpenThreadToken succeeded, so `token` is a valid owned handle.
            let token = unsafe { OwnedHandle::from_raw_handle(token.0 as RawHandle) };
            copy_token_user_sid(as_handle(&token))?
            // `_revert` drops here → RevertToSelf before the lookup below.
        };
        lookup_account_sid(PSID(sid.as_ptr() as *mut c_void))
    }

    pub(crate) struct AuthServer {
        secret: u128,
        issued: Arc<Mutex<AHashMap<u128, Instant>>>,
        _stop: oneshot::Sender<()>,
    }

    impl AuthServer {
        async fn process_request(
            mut client: NamedPipeServer,
            secret: u128,
            issued: Arc<Mutex<AHashMap<u128, Instant>>>,
        ) -> Result<()> {
            let user = peer_user(&client)?;
            debug!("local auth: peer is {user}");
            let salt = loop {
                let ts = Instant::now();
                let salt = rng().random::<u128>();
                let mut issued = issued.lock();
                if let Entry::Vacant(e) = issued.entry(salt) {
                    e.insert(ts);
                    break salt;
                }
            };
            let token = make_sha3_token([
                &salt.to_be_bytes()[..],
                user.as_bytes(),
                &secret.to_be_bytes()[..],
            ]);
            let c = Credential { hash_method: HashMethod::Sha3_512, salt, user, token };
            let mut msg = pack(&c)?;
            client.write_all_buf(&mut msg).await?;
            client.flush().await?;
            // Dropping `client` closes the pipe; the client side reads EOF.
            Ok(())
        }

        async fn run(
            name: String,
            mut server: NamedPipeServer,
            secret: u128,
            issued: Arc<Mutex<AHashMap<u128, Instant>>>,
            stop: oneshot::Receiver<()>,
        ) {
            let open = Arc::new(AtomicUsize::new(0));
            let mut stop = stop.fuse();
            let mut gc = interval(Duration::from_secs(60));
            loop {
                select_biased! {
                    _ = stop => break,
                    _ = gc.tick().fuse() => issued.lock().retain(|_, ts| {
                        ts.elapsed() < Duration::from_secs(60)
                    }),
                    r = server.connect().fuse() => match r {
                        Err(e) => {
                            warn!("local auth: pipe connect failed: {e}");
                            sleep(Duration::from_millis(100)).await;
                        }
                        Ok(()) => {
                            // Pre-create the next instance, then hand off the
                            // connected one. (No await between connect and the
                            // swap, so this is cancellation-atomic.)
                            let next = match ServerOptions::new()
                                .reject_remote_clients(true)
                                .create(&name)
                            {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!("local auth: could not create next pipe instance: {e}");
                                    let _ = server.disconnect();
                                    continue;
                                }
                            };
                            let client = std::mem::replace(&mut server, next);
                            if open.load(Ordering::Relaxed) >= 32 {
                                continue;
                            }
                            open.fetch_add(1, Ordering::Relaxed);
                            let issued = issued.clone();
                            let open = Arc::clone(&open);
                            spawn(async move {
                                match timeout(
                                    Duration::from_secs(10),
                                    Self::process_request(client, secret, issued),
                                )
                                .await
                                {
                                    Ok(Ok(())) => (),
                                    Err(_) => warn!("local auth: request timed out"),
                                    Ok(Err(e)) => warn!("local auth: process request: {e}"),
                                }
                                open.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                    },
                }
            }
        }

        pub(crate) async fn start(
            socket_path: &str,
            _cfg: &Config,
            _member: &MemberServer,
        ) -> Result<AuthServer> {
            let name = pipe_name(socket_path)?;
            // Default pipe security (creator + SYSTEM + Administrators full
            // control) plus the per-user SID in the name confines this to the
            // owner's own processes — the workstation's single-owner model.
            // It is safe regardless of who connects: `peer_user` binds each
            // issued credential to the *caller's* identity, so a credential is
            // never usable as anyone else. (A permissive DACL matching unix's
            // 0o777, to let other local users authenticate as themselves, is a
            // multi-user follow-up.)
            let server = ServerOptions::new()
                .first_pipe_instance(true)
                .reject_remote_clients(true)
                .create(&name)
                .with_context(|| format!("creating local auth pipe {name}"))?;
            debug!("local auth pipe listening at {name}");
            let issued = Arc::new(Mutex::new(AHashMap::default()));
            let secret = rng().random::<u128>();
            let (tx, rx) = oneshot::channel();
            spawn(Self::run(name, server, secret, issued.clone(), rx));
            Ok(AuthServer { secret, _stop: tx, issued })
        }

        pub(crate) fn validate(&self, cred: &Credential) -> bool {
            if cred.hash_method != HashMethod::Sha3_512 {
                false
            } else {
                let token = make_sha3_token([
                    &cred.salt.to_be_bytes()[..],
                    cred.user.as_bytes(),
                    &self.secret.to_be_bytes()[..],
                ]);
                token == cred.token && self.issued.lock().remove(&cred.salt).is_some()
            }
        }
    }

    pub(crate) struct AuthClient;

    impl AuthClient {
        async fn token_once(name: &str) -> Result<Bytes> {
            const TOKEN_MAX: usize = 4 * 1024;
            let mut pipe = ClientOptions::new().open(name)?;
            let mut buf = BytesMut::new();
            loop {
                let n = pipe.read_buf(&mut buf).await?;
                if buf.len() > TOKEN_MAX {
                    bail!("token is too large")
                }
                if n == 0 {
                    break;
                }
            }
            if buf.is_empty() { bail!("empty token") } else { Ok(buf.freeze()) }
        }

        pub(crate) async fn token(path: &str) -> Result<Bytes> {
            let name = pipe_name(path)?;
            let mut tries = 0;
            loop {
                match Self::token_once(&name).await {
                    Ok(buf) => return Ok(buf),
                    Err(e) => {
                        // The pipe may not exist yet (the supervisor can start
                        // the resolver and a client in parallel) or be busy;
                        // retry a few times with a short random backoff.
                        if tries >= 5 {
                            return Err(e).with_context(|| {
                                format!("getting a local token from {name}")
                            });
                        } else {
                            let delay =
                                Duration::from_millis(rng().random_range(50..400));
                            sleep(delay).await
                        }
                    }
                }
                tries += 1;
            }
        }
    }
}
