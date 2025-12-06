use crate::config::Config;
use anyhow::Result;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
use std::{
    env,
    fs::{File, OpenOptions},
    future,
    io::{stdin, ErrorKind},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    process::Command,
    str::FromStr,
};
use tempdir::TempDir;
use tokio::net::TcpListener;

const DEFAULT_PORT: u16 = 59200;
const VAR_RESOLVER_PORT: &str = "NETIDX_LOCAL_ONLY_RESOLVER_PORT";
const VAR_WE_ARE_RESOLVER: &str = "NETIDX_LOCAL_ONLY_WE_ARE_THE_RESOLVER";
const VAR_RESOLVER_TMPDIR: &str = "NETIDX_LOCAL_ONLY_RESOLVER_TEMP";

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

pub(super) fn maybe_run_local_resolver() -> Result<()> {
    if !get_env_as::<bool>(VAR_WE_ARE_RESOLVER, false) {
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(
        async move {
            let _ = ctrlc::set_handler(|| {
                if let Ok(tempdir) = env::var(VAR_RESOLVER_TMPDIR) {
                    let _ = std::fs::remove_dir_all(tempdir);
                }
                std::process::exit(0)
            });
            #[cfg(unix)]
            let listener = {
                use std::os::fd::{AsRawFd, FromRawFd};
                let fd = stdin().as_raw_fd();
                unsafe { std::net::TcpListener::from_raw_fd(fd) }
            };
            #[cfg(windows)]
            let listener = {
                use std::os::windows::io::FromRawSocket;
                let fd = stdin().as_raw_handle() as u64;
                unsafe { std::net::TcpListener::from_raw_socket(fd) }
            };
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
            let _server = resolver_server::Server::new_local_only(cfg, listener).await?;
            future::pending::<Result<()>>().await
        },
    )
}

fn open_out(tmp: &PathBuf, name: &str) -> Result<File> {
    Ok(OpenOptions::new().write(true).create(true).truncate(true).open(tmp.join(name))?)
}

#[cfg(windows)]
fn start_local_resolver(l: TcpListener) -> Result<()> {
    use std::{
        os::{
            raw::c_void,
            windows::{
                io::{AsRawSocket, FromRawHandle},
                process::CommandExt,
            },
        },
        process::Stdio,
    };
    use windows::Win32::Foundation::{SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT};
    use windows::Win32::System::Threading::{CREATE_NO_WINDOW, DETACHED_PROCESS};
    let raw_handle = l.as_raw_socket() as *mut c_void;
    // Make the handle inheritable
    unsafe {
        let handle = HANDLE(raw_handle);
        SetHandleInformation(handle, HANDLE_FLAG_INHERIT.0, HANDLE_FLAG_INHERIT)?;
    }
    let exe = env::current_exe()?;
    let td = TempDir::new("netidx_resolver")?.into_path();
    let stdout = Stdio::from(open_out(&td, "stdout")?);
    let stderr = Stdio::from(open_out(&td, "stderr")?);
    Command::new(exe)
        .creation_flags(CREATE_NO_WINDOW.0 | DETACHED_PROCESS.0)
        .stdin(unsafe { Stdio::from_raw_handle(raw_handle) })
        .env(VAR_WE_ARE_RESOLVER, "true")
        .env(VAR_RESOLVER_TMPDIR, &td)
        .stdout(stdout)
        .stderr(stderr)
        .spawn()?;
    Ok(())
}

#[cfg(unix)]
fn start_local_resolver(l: TcpListener) -> Result<PathBuf> {
    use daemonize::{Daemonize, Outcome, Stdio};
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::process::CommandExt,
    };
    let raw_fd = l.as_raw_fd();
    let current_exe = env::current_exe()?;
    let td = TempDir::new("netidx_resolver")?.into_path();
    let stdout = Stdio::from(open_out(&td, "stdout")?);
    let stderr = Stdio::from(open_out(&td, "stderr")?);
    match Daemonize::new()
        .stdout(stdout)
        .stderr(stderr)
        .pid_file(td.join("pid"))
        .execute()
    {
        Outcome::Parent(r) => {
            r?;
            Ok(td)
        }
        Outcome::Child(_) => {
            let err = Command::new(current_exe)
                .env(VAR_WE_ARE_RESOLVER, "true")
                .env(VAR_RESOLVER_TMPDIR, &td)
                .stdin(unsafe { std::process::Stdio::from_raw_fd(raw_fd) })
                .exec();
            let _ = std::fs::remove_dir_all(td);
            panic!("exec failed {err}")
        }
    }
}

pub(super) async fn find_or_start_resolver() -> Result<Config> {
    let port = get_env_as::<u16>(VAR_RESOLVER_PORT, DEFAULT_PORT);
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    match TcpListener::bind(addr).await {
        Ok(l) => {
            start_local_resolver(l)?;
            client_cfg_for_addr(addr)
        }
        Err(e) => match e.kind() {
            ErrorKind::AddrInUse => client_cfg_for_addr(addr),
            _ => bail!(e),
        },
    }
}
