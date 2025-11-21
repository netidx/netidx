use crate::{config::Config, publisher::DesiredAuth, resolver_client::ResolverRead};
use anyhow::Result;
use poolshark::local::LPooled;
use rand::random_range;
use std::{
    env, future,
    io::{stdin, ErrorKind},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    os::fd::{AsRawFd, FromRawFd},
    process::Command,
    str::FromStr,
    time::Duration,
};
use tokio::{net::TcpListener, time};

const DEFAULT_BASE_PORT: u16 = 59200;
const DEFAULT_PORT_RANGE: u16 = 16;
const TIMEOUT: Duration = Duration::from_millis(250);
const VAR_RESOLVER_BASE: &str = "NETIDX_LOCAL_ONLY_RESOLVER_BASE";
const VAR_RESOLVER_RANGE: &str = "NETIDX_LOCAL_ONLY_RESOLVER_RANGE";
const VAR_WE_ARE_RESOLVER: &str = "NETIDX_LOCAL_ONLY_WE_ARE_THE_RESOLVER";

fn get_env_as<T: FromStr>(name: &str, default: T) -> T {
    match env::var(name) {
        Err(_) => default,
        Ok(s) => match s.parse::<T>() {
            Ok(v) => v,
            Err(_) => default,
        },
    }
}

fn client_cfg_for_addr(addr: SocketAddr) -> Result<Config> {
    use crate::config::{self, file, DefaultAuthMech};
    let cfg = file::ConfigBuilder::default()
        .addrs(vec![(addr, file::Auth::Anonymous)])
        .default_auth(DefaultAuthMech::Anonymous)
        .default_bind_config("local")
        .build()?;
    config::Config::from_file(cfg)
}

enum PortStatus {
    ResolverFound(Config),
    PortAvailable(TcpListener),
    PortSquatted,
}

async fn check_port(port: u16) -> Result<PortStatus> {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    match TcpListener::bind(addr).await {
        Ok(l) => Ok(PortStatus::PortAvailable(l)),
        Err(e) => match e.kind() {
            ErrorKind::AddrInUse => {
                let cfg = client_cfg_for_addr(addr)?;
                let res = ResolverRead::new(cfg.clone(), DesiredAuth::Anonymous);
                // test if it's really a resolver server by listing /
                match time::timeout(TIMEOUT, res.list("/".into())).await {
                    Ok(Ok(_)) => Ok(PortStatus::ResolverFound(cfg)),
                    Ok(Err(_)) | Err(_) => Ok(PortStatus::PortSquatted),
                }
            }
            _ => bail!(e),
        },
    }
}

pub(super) async fn child_run_local_resolver() -> Result<()> {
    if !get_env_as::<bool>(VAR_WE_ARE_RESOLVER, false) {
        return Ok(());
    }
    let fd = stdin().as_raw_fd();
    let listener = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    use crate::resolver_server::{
        self,
        config::{self, file},
    };
    let listener = TcpListener::from_std(listener)?;
    let addr = listener.local_addr()?;
    let cfg = file::ConfigBuilder::default()
        .member_servers(vec![file::MemberServerBuilder::default()
            .auth(file::Auth::Anonymous)
            .addr(addr)
            .bind_addr("127.0.0.1".parse()?)
            .build()?])
        .build()?;
    let cfg = config::Config::from_file(cfg)?;
    resolver_server::Server::new_local_only(cfg, listener).await?;
    future::pending::<Result<()>>().await
}

#[cfg(windows)]
fn start_local_resolver(l: TcpListener) -> Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::process::Stdio;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::SetHandleInformation;
    use windows::Win32::System::Threading::{
        CREATE_NO_WINDOW, DETACHED_PROCESS, HANDLE_FLAG_INHERIT,
    };
    let raw_handle = l.as_raw_socket().as_raw_handle();
    // Make the handle inheritable
    unsafe {
        let handle = HANDLE(raw_handle);
        SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
    }
    Command::new(env::current_exe()?)
        .creation_flags(CREATE_NO_WINDOW | DETACHED_PROCESS)
        .stdin(Stdio::from_raw_handle(raw_handle))
        .spawn()?;
    Ok(())
}

#[cfg(unix)]
fn start_local_resolver(l: TcpListener) -> Result<()> {
    use daemonize::{Daemonize, Outcome};
    use std::{os::unix::process::CommandExt, process::Stdio};
    let raw_fd = l.as_raw_fd();
    let current_exe = env::current_exe()?;
    match Daemonize::new().execute() {
        Outcome::Parent(r) => {
            r?;
            Ok(())
        }
        Outcome::Child(_) => {
            let err = Command::new(current_exe)
                .env(VAR_WE_ARE_RESOLVER, "true")
                .stdin(unsafe { Stdio::from_raw_fd(raw_fd) })
                .exec();
            panic!("exec failed {err}")
        }
    }
}

pub(super) async fn find_or_start_resolver() -> Result<Config> {
    let base = get_env_as::<u16>(VAR_RESOLVER_BASE, DEFAULT_BASE_PORT);
    let range = get_env_as::<u16>(VAR_RESOLVER_RANGE, DEFAULT_PORT_RANGE);
    let mut backoff = true;
    let mut we_are_leader = false;
    let mut second_pass = false;
    loop {
        let mut candidates: LPooled<Vec<TcpListener>> = LPooled::take();
        let mut errors: LPooled<Vec<anyhow::Error>> = LPooled::take();
        for port in base..base + range {
            match check_port(port).await {
                Ok(PortStatus::ResolverFound(cfg)) => return Ok(cfg), // we found an existing resolver
                Ok(PortStatus::PortAvailable(l)) => candidates.push(l), // we might be the leader
                Ok(PortStatus::PortSquatted) => {
                    // spread out potential simultaneous starts
                    time::sleep(Duration::from_millis(random_range(1..100))).await
                }
                Err(e) => errors.push(e),
            }
        }
        // if we are the leader, then we started a server, which means we should
        // not have arrived here. This means the server failed to start.
        if we_are_leader {
            // we have already been here, and the resolver failed to start
            bail!("starting the resolver failed")
        }
        if !candidates.is_empty() {
            if backoff {
                drop(candidates);
                time::sleep(Duration::from_millis(random_range(1..200))).await;
                backoff = false;
                continue;
            }
            start_local_resolver(candidates.remove(0))?;
            we_are_leader = true;
            continue;
        }
        // If we find all the ports squatted try one more time before failing
        if !second_pass {
            second_pass = true;
            time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        bail!("couldn't find or start a local resolver {errors:?}")
    }
}
