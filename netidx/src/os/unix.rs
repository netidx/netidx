use anyhow::{anyhow, Result};
use arcstr::ArcStr;
use std::process::Command;
use tokio::task;

// Unix group membership is a little complex, it can come from a
// lot of places, and it's not entirely standardized at the api
// level, it seems libc provides getgrouplist on most platforms,
// but unfortunatly Apple doesn't implement it. Luckily the 'id'
// command is specified in POSIX.
#[derive(Clone)]
pub(crate) struct Mapper(ArcStr);

impl Mapper {
    pub(crate) fn new() -> Result<Mapper> {
        task::block_in_place(|| {
            let out = Command::new("sh").arg("-c").arg("which id").output()?;
            let buf = String::from_utf8_lossy(&out.stdout);
            let path =
                buf.lines().next().ok_or_else(|| anyhow!("can't find the id command"))?;
            Ok(Mapper(ArcStr::from(path)))
        })
    }

    pub(crate) fn groups(&self, user: &str) -> Result<Vec<String>> {
        task::block_in_place(|| {
            let out = Command::new(&*self.0).arg(user).output()?;
            Mapper::parse_output(&String::from_utf8_lossy(&out.stdout), "groups=")
        })
    }

    pub(crate) fn user(&self, user: u32) -> Result<String> {
        task::block_in_place(|| {
            let out = Command::new(&*self.0).arg(user.to_string()).output()?;
            let mut user =
                Mapper::parse_output(&String::from_utf8_lossy(&out.stdout), "user=")?;
            if user.is_empty() {
                bail!("user not found")
            } else {
                Ok(user.pop().unwrap())
            }
        })
    }

    fn parse_output(out: &str, key: &str) -> Result<Vec<String>> {
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
                            groups.push(String::from(&s[i_op + 1..i_cp]));
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
    use crate::os::local_auth::Credential;
    use anyhow::Result;
    use bytes::{Bytes, BytesMut};
    use futures::{channel::oneshot, prelude::*, select_biased};
    use fxhash::{FxBuildHasher, FxHashMap};
    use log::{debug, warn};
    use netidx_core::utils::{make_sha3_token, pack};
    use parking_lot::Mutex;
    use rand::{thread_rng, Rng};
    use std::{
        collections::{hash_map::Entry, HashMap},
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
        issued: Arc<Mutex<FxHashMap<u64, Instant>>>,
        _stop: oneshot::Sender<()>,
    }

    impl AuthServer {
        async fn process_request(
            mapper: Mapper,
            mut client: UnixStream,
            secret: u128,
            issued: Arc<Mutex<FxHashMap<u64, Instant>>>,
        ) -> Result<()> {
            let cred = client.peer_cred()?;
            let user = mapper.user(cred.uid())?;
            let salt: u64 = loop {
                let ts = Instant::now();
                let salt: u64 = thread_rng().gen();
                let mut issued = issued.lock();
                if let Entry::Vacant(e) = issued.entry(salt) {
                    e.insert(ts);
                    break salt;
                }
            };
            let token =
                make_sha3_token(Some(salt), &[user.as_bytes(), &secret.to_be_bytes()]);
            let mut msg = pack(&Credential { salt, user, token })?;
            client.write_all_buf(&mut msg).await?;
            Ok(())
        }

        async fn run(
            mapper: Mapper,
            listener: UnixListener,
            secret: u128,
            issued: Arc<Mutex<FxHashMap<u64, Instant>>>,
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
                        Ok((client, addr)) => {
                            debug!("accepted client {:?}", addr);
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

        pub(crate) async fn start(socket_path: &str) -> Result<AuthServer> {
            let _ = fs::remove_file(socket_path).await;
            let listener = UnixListener::bind(socket_path)?;
            fs::set_permissions(socket_path, Permissions::from_mode(0o777)).await?;
            let mapper = Mapper::new()?;
            let issued =
                Arc::new(Mutex::new(HashMap::with_hasher(FxBuildHasher::default())));
            let secret: u128 = thread_rng().gen();
            let (tx, rx) = oneshot::channel();
            spawn(Self::run(mapper, listener, secret, issued.clone(), rx));
            Ok(AuthServer { secret, _stop: tx, issued })
        }

        pub(crate) fn validate(&self, cred: &Credential) -> bool {
            let token = make_sha3_token(
                Some(cred.salt),
                &[cred.user.as_bytes(), &self.secret.to_be_bytes()],
            );
            token == cred.token && self.issued.lock().remove(&cred.salt).is_some()
        }
    }

    pub(crate) struct AuthClient;

    impl AuthClient {
        async fn token_once(path: &str) -> Result<Bytes> {
            debug!("asking for a local token from {}", path);
            let mut soc = UnixStream::connect(path).await?;
            let mut buf = BytesMut::new();
            loop {
                let n = soc.read_buf(&mut buf).await?;
                debug!("read {} bytes from the token", n);
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
                            let delay = Duration::from_secs(thread_rng().gen_range(0..3));
                            sleep(delay).await
                        }
                    }
                }
                tries += 1;
            }
        }
    }
}
