use crate::resolver_server::config::{Config, IdMap, MemberServer};
use anyhow::{anyhow, Result};
use arcstr::ArcStr;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    process::Command,
    time::sleep,
};

/// How many times to retry a connect to the id-map socket before
/// giving up. The activation supervisor can start the resolver and
/// the id-map daemon in parallel; the resolver's first TLS handshake
/// may arrive before the daemon's `bind()` returns. A short retry
/// window papers over the boot race without pessimising the steady
/// state — once the daemon is up, every subsequent connect succeeds
/// on the first try.
const SOCKET_CONNECT_TRIES: u32 = 25;
const SOCKET_CONNECT_BACKOFF: Duration = Duration::from_millis(200);

async fn connect_with_retry(path: &str) -> Result<UnixStream> {
    let mut last_err: Option<std::io::Error> = None;
    for attempt in 0..SOCKET_CONNECT_TRIES {
        match UnixStream::connect(path).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < SOCKET_CONNECT_TRIES {
                    sleep(SOCKET_CONNECT_BACKOFF).await;
                }
            }
        }
    }
    Err(last_err
        .map(|e| anyhow!(e))
        .unwrap_or_else(|| anyhow!("connect failed"))
        .context(format!("connecting to id-map socket {path:?}")))
}

// Unix group membership is a little complex, it can come from a
// lot of places, and it's not entirely standardized at the api
// level, it seems libc provides getgrouplist on most platforms,
// but unfortunatly Apple doesn't implement it. Luckily the 'id'
// command is specified in POSIX.
#[derive(Clone)]
pub(crate) enum Mapper {
    DoNotMap,
    Socket(ArcStr),
    Command(ArcStr),
}

impl Mapper {
    pub(crate) async fn new(_cfg: &Config, member: &MemberServer) -> Result<Mapper> {
        match &member.id_map {
            IdMap::DoNotMap => Ok(Mapper::DoNotMap),
            IdMap::Command(cmd) => Ok(Mapper::Command(ArcStr::from(cmd))),
            IdMap::Socket(path) => Ok(Mapper::Socket(ArcStr::from(path))),
            IdMap::PlatformDefault => {
                let out = Command::new("sh").arg("-c").arg("which id").output().await?;
                let buf = String::from_utf8_lossy(&out.stdout);
                let path = buf
                    .lines()
                    .next()
                    .ok_or_else(|| anyhow!("can't find the id command"))?;
                Ok(Mapper::Command(ArcStr::from(path)))
            }
        }
    }

    pub(crate) async fn groups(&self, user: &str) -> Result<(ArcStr, Vec<ArcStr>)> {
        let parse = |s: &str| {
            let mut primary = Mapper::parse_output(&s, "gid=")?;
            let groups = Mapper::parse_output(&s, "groups=")?;
            let primary = if primary.is_empty() {
                // No `gid=` in the mapper output ⇒ the identity resolved
                // to no unix primary group. The usual cause is a krb5
                // principal (or TLS SAN) that doesn't correspond to a
                // local user — name it and point at the fix rather than
                // failing with a bare "missing primary group".
                bail!(
                    "no unix primary group for {user:?} — the authenticated \
                     identity does not map to a local user. Install the netidx \
                     id-mapper and register this identity, or configure your \
                     system IdM (sssd/FreeIPA/AD) so `id {user:?}` resolves it."
                )
            } else {
                primary.swap_remove(0)
            };
            Ok((primary, groups))
        };
        match &self {
            Mapper::DoNotMap => Ok((user.into(), vec![])),
            Mapper::Command(cmd) => {
                let out = Command::new(&**cmd).arg(user).output().await?;
                // `id <user>` exits non-zero for an unknown user and
                // writes nothing useful to stdout, so without this check
                // the parse below fails with the opaque "no primary
                // group" rather than the actual "no such user" reason.
                if !out.status.success() {
                    let err = String::from_utf8_lossy(&out.stderr);
                    bail!(
                        "could not map identity {user:?} to unix ids: \
                         `{cmd}` failed ({}): {}",
                        out.status,
                        err.trim()
                    )
                }
                parse(String::from_utf8_lossy(&out.stdout).as_ref())
            }
            Mapper::Socket(path) => {
                let mut sock = connect_with_retry(&**path).await?;
                sock.write_all(format!("{}\n", user).as_bytes()).await?;
                let mut reply = vec![];
                sock.read_to_end(&mut reply).await?;
                parse(String::from_utf8_lossy(&reply).as_ref())
            }
        }
    }

    pub(crate) async fn user(&self, user: u32) -> Result<ArcStr> {
        let parse = |s: &str| {
            let mut user = Mapper::parse_output(s, "uid=")?;
            if user.is_empty() {
                bail!("user not found")
            } else {
                Ok(user.swap_remove(0))
            }
        };
        match &self {
            Mapper::DoNotMap => Ok(ArcStr::from(format!("{}", user))),
            Mapper::Command(cmd) => {
                let out = Command::new(&**cmd).arg(user.to_string()).output().await?;
                parse(String::from_utf8_lossy(&out.stdout).as_ref())
            }
            Mapper::Socket(path) => {
                let mut sock = connect_with_retry(&**path).await?;
                sock.write_all(format!("{}\n", user).as_bytes()).await?;
                let mut reply = vec![];
                sock.read_to_end(&mut reply).await?;
                parse(String::from_utf8_lossy(&reply).as_ref())
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
                            ))
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
    use super::Mapper;
    use crate::{
        os::local_auth::Credential,
        resolver_server::config::{Config, MemberServer},
    };
    use ahash::AHashMap;
    use anyhow::Result;
    use bytes::{Bytes, BytesMut};
    use futures::{channel::oneshot, prelude::*, select_biased};
    use log::{debug, warn};
    use netidx_core::utils::{make_sha3_token, pack};
    use netidx_netproto::resolver::HashMethod;
    use parking_lot::Mutex;
    use rand::{rng, RngExt};
    use std::{
        collections::hash_map::Entry,
        fs::Permissions,
        os::unix::fs::PermissionsExt,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };
    use tokio::{
        fs,
        io::{AsyncReadExt, AsyncWriteExt},
        net::{UnixListener, UnixStream},
        task::spawn,
        time::{interval, sleep, timeout},
    };

    pub(crate) struct AuthServer {
        secret: u128,
        issued: Arc<Mutex<AHashMap<u128, Instant>>>,
        _stop: oneshot::Sender<()>,
    }

    impl AuthServer {
        async fn process_request(
            mapper: Mapper,
            mut client: UnixStream,
            secret: u128,
            issued: Arc<Mutex<AHashMap<u128, Instant>>>,
        ) -> Result<()> {
            let cred = client.peer_cred()?;
            debug!("got peer credentials {:?}", cred);
            let user = mapper.user(cred.uid()).await?;
            debug!("got user {}", user);
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
            Ok(())
        }

        async fn run(
            mapper: Mapper,
            listener: UnixListener,
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
                    r = listener.accept().fuse() => match r {
                        Err(e) => {
                            warn!("accept: {}", e);
                            sleep(Duration::from_millis(100)).await
                        }
                        Ok((client, _addr)) => {
                            debug!("accepted client");
                            if open.load(Ordering::Relaxed) >= 32 {
                                continue;
                            } else {
                                open.fetch_add(1, Ordering::Relaxed);
                                let mapper = mapper.clone();
                                let issued = issued.clone();
                                let open = Arc::clone(&open);
                                spawn(async move {
                                    match timeout(
                                        Duration::from_secs(10),
                                        Self::process_request(mapper, client, secret, issued),
                                    )
                                        .await
                                    {
                                        Ok(Ok(())) => (),
                                        Err(_) => warn!("auth request timed out"),
                                        Ok(Err(e)) => warn!("process request: {}", e),
                                    }
                                    open.fetch_sub(1, Ordering::Relaxed);
                                });
                            }
                        }
                    },
                }
            }
        }

        pub(crate) async fn start(
            socket_path: &str,
            cfg: &Config,
            member: &MemberServer,
        ) -> Result<AuthServer> {
            let _ = fs::remove_file(socket_path).await;
            let listener = UnixListener::bind(socket_path)?;
            fs::set_permissions(socket_path, Permissions::from_mode(0o777)).await?;
            let mapper = Mapper::new(cfg, member).await?;
            let issued = Arc::new(Mutex::new(AHashMap::default()));
            let secret = rng().random::<u128>();
            let (tx, rx) = oneshot::channel();
            spawn(Self::run(mapper, listener, secret, issued.clone(), rx));
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
        async fn token_once(path: &str) -> Result<Bytes> {
            const TOKEN_MAX: usize = 4 * 1024;
            debug!("asking for a local token from {}", path);
            let mut soc = UnixStream::connect(path).await?;
            let mut buf = BytesMut::new();
            loop {
                let n = soc.read_buf(&mut buf).await?;
                debug!("read {} bytes from the token", n);
                if buf.len() > TOKEN_MAX {
                    bail!("token is too large")
                }
                if n == 0 {
                    break;
                }
            }
            if buf.len() == 0 {
                bail!("empty token")
            } else {
                Ok(buf.freeze())
            }
        }

        pub(crate) async fn token(path: &str) -> Result<Bytes> {
            let mut tries = 0;
            loop {
                match Self::token_once(path).await {
                    Ok(buf) => return Ok(buf),
                    Err(e) => {
                        if tries >= 2 {
                            return Err(e);
                        } else {
                            let delay = Duration::from_secs(rng().random_range(0..3));
                            sleep(delay).await
                        }
                    }
                }
                tries += 1;
            }
        }
    }
}
