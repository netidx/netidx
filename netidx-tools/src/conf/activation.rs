use anyhow::{Context, Result, anyhow, bail};
use arcstr::ArcStr;
use netidx::path::Path as NetidxPath;
use netidx_activation::control::{
    ControlOp, ControlRequest, ControlResponse, UnitState, UnitStatus,
};
use netidx_conf::{
    activation::{
        self, ActivationDir, Environment, ProcessCfgBuilder, Restart, Trigger,
        UnitBuilder,
    },
    client::ClientConfig,
    conf_client,
    conf_proto::{self, NodeKind},
    id_map as id_map_engine,
    template::services::{
        container::{self, ContainerServiceParams},
        id_map::{self as id_map_template, IdMapServiceParams},
    },
};

use super::prompt;
use clap::{Args, Subcommand};
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf};

// One-shot CLI argument value on the stack; boxing the big variant
// would trade nothing for an allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// list installed activation units
    List {
        /// activation directory
        #[arg(short, long)]
        dir: Option<PathBuf>,
    },
    /// add or replace an activation unit
    Add {
        #[command(subcommand)]
        cmd: AddCmd,
    },
    /// remove an activation unit
    Remove {
        #[arg(short, long)]
        dir: Option<PathBuf>,
        /// Unit basename to remove. Prompted when omitted.
        name: Option<String>,
    },
    /// restart units (locally, or remotely with `--server`)
    Restart(ServiceCtlArgs),
    /// start units
    Start(ServiceCtlArgs),
    /// stop units (they will not auto-restart until started)
    Stop(ServiceCtlArgs),
    /// report unit status
    Status(ServiceCtlArgs),
}

#[derive(Args, Debug)]
pub(crate) struct ServiceCtlArgs {
    /// Drive a REMOTE CA over the conf plane (RBAC-gated, routed by the
    /// network map). Without it, this host's local activation supervisor is
    /// controlled directly (on-box, filesystem authority).
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// The resolver-cluster path whose services to control — the RBAC scope.
    /// Required with `--server`; ignored locally. e.g. `/eu`.
    #[arg(long)]
    pub path: Option<String>,
    /// Units to act on (empty ⇒ every unit). With `--server`, a unit may be
    /// pinned to one cluster member by index — `resolver:0` restarts the
    /// resolver on the first member, `resolver:1` the second, so an admin
    /// can stagger restarts. Locally, just unit names.
    pub units: Vec<String>,
    /// Activation directory (local mode). Default: the user activation dir.
    #[arg(long)]
    pub dir: Option<PathBuf>,
    /// CA dir, to verify the conf server's identity (remote mode).
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum AddCmd {
    /// drop a hand-rolled unit (any exe + args + trigger)
    Generic(GenericAddArgs),
    /// drop a netidx container service unit
    Container(ContainerAddArgs),
    /// drop an id-mapper daemon service unit
    IdMap(IdMapAddArgs),
}

#[derive(Args, Debug)]
pub(crate) struct GenericAddArgs {
    #[arg(short, long)]
    pub dir: Option<PathBuf>,
    /// Unit basename (no `.unit` suffix). Prompted when omitted.
    pub name: Option<String>,
    /// Path to the executable. Prompted when omitted.
    #[arg(long)]
    pub exe: Option<String>,
    /// Argument passed after the exe. Repeatable.
    #[arg(long = "arg", num_args = 1)]
    pub args: Vec<String>,
    /// Netidx path that, when subscribed, triggers this unit.
    /// Repeatable. If any are given, the trigger is `OnAccess`
    /// rather than `OnStart`.
    #[arg(long, num_args = 1)]
    pub on_access: Vec<String>,
    /// Restart policy: `no`, `yes`, or `rate-limited:<seconds>`
    /// (default `rate-limited:1.0`).
    #[arg(long)]
    pub restart: Option<String>,
    #[arg(long)]
    pub working_dir: Option<PathBuf>,
    #[arg(long)]
    pub uid: Option<u32>,
    #[arg(long)]
    pub gid: Option<u32>,
    #[arg(long)]
    pub stdin: Option<PathBuf>,
    #[arg(long)]
    pub stdout: Option<PathBuf>,
    #[arg(long)]
    pub stderr: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct IdMapAddArgs {
    /// Where to drop the unit file. `None` ⇒ the user activation dir.
    #[arg(short, long)]
    pub dir: Option<PathBuf>,
    /// Unit basename. Default `id-map`.
    #[arg(long, default_value = "id-map")]
    pub name: String,
    /// Override the netidx binary the unit launches. Default: the
    /// currently-running binary (via `std::env::current_exe`).
    #[arg(long)]
    pub netidx_binary: Option<PathBuf>,
    /// Unix socket the daemon will bind. Default:
    /// `${dirs::config_dir}/netidx/id-map.sock`.
    #[arg(short, long)]
    pub socket: Option<PathBuf>,
    /// JSON config the daemon will load. Default:
    /// `${dirs::config_dir}/netidx/id-map.json`.
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    /// File mode applied to the bound socket (octal). Default 600.
    #[arg(long)]
    pub socket_mode: Option<String>,
    /// Restart policy: `no`, `yes`, or `rate-limited:<seconds>`.
    #[arg(long)]
    pub restart: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct ContainerAddArgs {
    /// Where to drop the unit file. `None` ⇒ the user activation dir.
    #[arg(short, long)]
    pub dir: Option<PathBuf>,
    /// Unit basename. Default `container`.
    #[arg(long, default_value = "container")]
    pub name: String,
    /// Where the container should publish its API. Default:
    /// `<client.base>/container/api` from the user client config
    /// (so a workstation gets `/local/container/api`), or
    /// `/container/api` when no client config can be loaded.
    #[arg(long)]
    pub api_path: Option<String>,
    /// Override the on-disk db directory. `None` ⇒ container default
    /// (`$XDG_DATA_HOME/netidx/container/db`).
    #[arg(long)]
    pub db: Option<PathBuf>,
    /// Pass `--compress` to the container.
    #[arg(long)]
    pub compress: bool,
    /// Pass `--bind <cfg>` to the container.
    #[arg(long)]
    pub bind: Option<String>,
    /// Override the binary the unit launches. Default:
    /// the currently-running `netidx` binary (via `std::env::current_exe`).
    /// The `netidx` subcommand for the container is appended
    /// automatically.
    #[arg(long)]
    pub netidx_binary: Option<PathBuf>,
    /// Restart policy: `no`, `yes`, or `rate-limited:<seconds>`.
    #[arg(long)]
    pub restart: Option<String>,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::List { dir } => list(dir),
        Cmd::Add { cmd: AddCmd::Generic(args) } => add_generic(args),
        Cmd::Add { cmd: AddCmd::Container(args) } => add_container(args),
        Cmd::Add { cmd: AddCmd::IdMap(args) } => add_id_map(args),
        Cmd::Remove { dir, name } => {
            let name = prompt::required_string("unit name", name)?;
            remove(dir, name)
        }
        Cmd::Restart(a) => service_control(ControlOp::Restart, a),
        Cmd::Start(a) => service_control(ControlOp::Start, a),
        Cmd::Stop(a) => service_control(ControlOp::Stop, a),
        Cmd::Status(a) => service_control(ControlOp::Status, a),
    }
}

/// Restart / start / stop / status units — over the conf plane (`--server`,
/// RBAC-gated, cluster+member targeting) or against the local activation
/// supervisor's control socket (on-box).
fn service_control(op: ControlOp, a: ServiceCtlArgs) -> Result<()> {
    match a.server {
        Some(server) => {
            let path = a.path.ok_or_else(|| {
                anyhow!("--path <resolver-cluster-path> is required with --server")
            })?;
            let targets = parse_unit_targets(&a.units)?;
            let (rt, identity, admin, password) =
                super::ca::remote_admin_preamble(server, a.ca_dir.clone())?;
            let results = rt.block_on(conf_client::control_service(
                server,
                NodeKind::Client,
                &identity,
                &admin,
                password.as_str(),
                &path,
                targets,
                op,
            ))?;
            print_service_results(&results);
            Ok(())
        }
        None => {
            // Local: talk straight to this host's activation control socket.
            let dir = a
                .dir
                .or_else(netidx_activation::runtime::default_units_dir)
                .ok_or_else(|| anyhow!("no activation directory found on this host"))?;
            let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
            let req = ControlRequest { op, units: a.units };
            match rt.block_on(netidx_activation::control::control(&dir, &req))? {
                ControlResponse::Ok { units } => {
                    print_unit_statuses(&units);
                    Ok(())
                }
                ControlResponse::Err { reason } => bail!("{reason}"),
            }
        }
    }
}

/// Parse `unit[:member]` tokens into [`conf_proto::UnitTarget`]s. A trailing
/// `:<n>` pins the unit to cluster member `n`; otherwise it hits every member.
fn parse_unit_targets(toks: &[String]) -> Result<Vec<conf_proto::UnitTarget>> {
    toks.iter()
        .map(|t| match t.rsplit_once(':') {
            Some((unit, idx)) => {
                let member = idx
                    .parse::<u32>()
                    .with_context(|| format!("invalid member index in {t:?}"))?;
                Ok(conf_proto::UnitTarget {
                    unit: unit.to_string(),
                    member: Some(member),
                })
            }
            None => Ok(conf_proto::UnitTarget { unit: t.clone(), member: None }),
        })
        .collect()
}

fn fmt_state(s: &UnitState) -> String {
    match s {
        UnitState::NotStarted => "not started".to_string(),
        UnitState::Running { pid: Some(p) } => format!("running (pid {p})"),
        UnitState::Running { pid: None } => "running".to_string(),
        UnitState::Stopped => "stopped".to_string(),
        UnitState::Died => "died".to_string(),
    }
}

fn print_unit_statuses(units: &[UnitStatus]) {
    if units.is_empty() {
        println!("  (no units)");
    }
    for u in units {
        println!("  {}: {}", u.unit, fmt_state(&u.state));
    }
}

fn print_service_results(results: &[conf_proto::ServiceControlResult]) {
    if results.is_empty() {
        println!("(no cluster members matched)");
    }
    for r in results {
        match &r.error {
            Some(e) => println!("member {} ({}): ERROR {e}", r.member, r.addr),
            None => {
                println!("member {} ({}):", r.member, r.addr);
                print_unit_statuses(&r.units);
            }
        }
    }
}

fn list(dir: Option<PathBuf>) -> Result<()> {
    let ad = ActivationDir::open(dir.as_deref())?;
    let units = ad.list()?;
    if units.is_empty() {
        println!("# no units in {:?}", ad.dir());
        return Ok(());
    }
    for (name, unit) in &units {
        let trigger = match &unit.trigger {
            Trigger::OnStart => "OnStart".to_string(),
            Trigger::OnAccess(paths) => {
                let v: Vec<_> = paths.iter().map(|p| p.as_ref()).collect();
                format!("OnAccess({})", v.join(", "))
            }
        };
        println!("{name}  {trigger}  exe={}", unit.process.exe);
    }
    // Structural validation across the set (matches what the daemon
    // would catch at startup) — surface conflicts at list time too.
    activation::validate(&units)?;
    Ok(())
}

fn add_generic(a: GenericAddArgs) -> Result<()> {
    let name = prompt::required_string("unit basename", a.name)?;
    let exe = prompt::required_string("path to the executable", a.exe)?;
    let trigger = if a.on_access.is_empty() {
        Trigger::OnStart
    } else {
        let mut set = BTreeSet::new();
        for p in a.on_access {
            set.insert(NetidxPath::from(p));
        }
        Trigger::OnAccess(set)
    };
    let mut pb = ProcessCfgBuilder::default();
    pb.exe(exe);
    if !a.args.is_empty() {
        pb.args(a.args);
    }
    if let Some(d) = a.working_dir {
        pb.working_directory(d);
    }
    if let Some(u) = a.uid {
        pb.uid(u);
    }
    if let Some(g) = a.gid {
        pb.gid(g);
    }
    if let Some(p) = a.stdin {
        pb.stdin(p);
    }
    if let Some(p) = a.stdout {
        pb.stdout(p);
    }
    if let Some(p) = a.stderr {
        pb.stderr(p);
    }
    if let Some(r) = a.restart {
        pb.restart(parse_restart(&r)?);
    }
    pb.environment(Environment::default());
    let process = pb.build()?;
    let unit = UnitBuilder::default().trigger(trigger).process(process).build()?;
    install_unit(a.dir, &name, unit)
}

fn add_container(a: ContainerAddArgs) -> Result<()> {
    let netidx_binary = match a.netidx_binary {
        Some(p) => p,
        None => std::env::current_exe().context(
            "could not determine current netidx binary path; pass --netidx-binary",
        )?,
    };
    let api_path = match a.api_path {
        Some(p) => ArcStr::from(p),
        None => default_container_api_path(),
    };
    let mut unit = container::unit(&ContainerServiceParams {
        netidx_binary,
        api_path,
        db: a.db,
        compress: a.compress,
        bind: a.bind.map(ArcStr::from),
    })?;
    if let Some(r) = &a.restart {
        unit.process.restart = parse_restart(r)?;
    }
    install_unit(a.dir, &a.name, unit)
}

/// Compute the default container API path. The rule: take the client
/// config's `base` and append `/container/api`. Falls back to
/// `/container/api` when no client config is loadable (e.g. brand-new
/// install pre-`netidx conf install`) **or** when `base` is malformed
/// (the JSON deserialiser accepts any string for `base` but a netidx
/// path must start with `/`; treating "local" as a base would emit
/// "local/container/api" which is not a valid netidx path). A
/// `/local`-base workstation lands at `/local/container/api`; a
/// root-base install at `/container/api`.
fn default_container_api_path() -> ArcStr {
    let raw_base = ClientConfig::load_default().ok().map(|c| c.0.base);
    ArcStr::from(api_path_for_base(raw_base.as_deref()).as_str())
}

/// Pure helper for `default_container_api_path` — separated so it can
/// be tested without touching the user's real client config. Returns
/// `<base>/container/api` for a well-formed `/foo` base; falls back
/// to `/container/api` otherwise.
fn api_path_for_base(raw_base: Option<&str>) -> String {
    let base = match raw_base {
        Some(b) => {
            let trimmed = b.trim_end_matches('/');
            // Refuse anything that isn't a valid netidx-style absolute
            // path. The `base` field is `String` in the schema, so the
            // load step accepts shapes the runtime would later reject.
            if !trimmed.starts_with('/') || trimmed.is_empty() {
                "/".to_string()
            } else {
                trimmed.to_string()
            }
        }
        None => "/".to_string(),
    };
    if base == "/" {
        "/container/api".to_string()
    } else {
        format!("{base}/container/api")
    }
}

fn add_id_map(a: IdMapAddArgs) -> Result<()> {
    let netidx_binary = match a.netidx_binary {
        Some(p) => p,
        None => std::env::current_exe().context(
            "could not determine current netidx binary path; pass --netidx-binary",
        )?,
    };
    let socket = match a.socket {
        Some(p) => p,
        None => id_map_engine::user_id_map_socket()?,
    };
    let config = match a.config {
        Some(p) => p,
        None => id_map_engine::user_id_map_path()?,
    };
    let socket_mode = match a.socket_mode.as_deref() {
        Some(s) => Some(id_map_engine::parse_octal_mode(s)?),
        None => None,
    };
    let mut unit = id_map_template::unit(&IdMapServiceParams {
        netidx_binary,
        socket,
        config,
        socket_mode,
    })?;
    if let Some(r) = &a.restart {
        unit.process.restart = parse_restart(r)?;
    }
    install_unit(a.dir, &a.name, unit)
}

fn install_unit(
    dir: Option<PathBuf>,
    name: &str,
    unit: netidx_conf::activation::Unit,
) -> Result<()> {
    let ad = ActivationDir::open(dir.as_deref())?;
    let mut units = ad.list()?;
    units.insert(name.to_string(), unit.clone());
    activation::validate(&units)?;
    ad.save(name, &unit)?;
    println!("wrote {}/{}.unit", ad.dir().display(), name);
    Ok(())
}

fn remove(dir: Option<PathBuf>, name: String) -> Result<()> {
    let ad = ActivationDir::open(dir.as_deref())?;
    ad.remove(&name)?;
    println!("removed {}/{}.unit", ad.dir().display(), name);
    Ok(())
}

fn parse_restart(s: &str) -> Result<Restart> {
    match s {
        "no" => Ok(Restart::No),
        "yes" => Ok(Restart::Yes),
        other => {
            if let Some(rest) = other.strip_prefix("rate-limited:") {
                let secs: f64 = rest
                    .parse()
                    .map_err(|e| anyhow!("invalid rate-limit seconds {rest:?}: {e}"))?;
                if !secs.is_finite() || secs <= 0.0 {
                    bail!("rate-limit seconds must be finite and positive, got {secs}");
                }
                Ok(Restart::RateLimited(secs))
            } else {
                bail!(
                    "unknown restart policy {other:?}; expected `no`, `yes`, or `rate-limited:<seconds>`"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_path_for_base_branches() {
        // No client config → fallback.
        assert_eq!(api_path_for_base(None), "/container/api");
        // Root base → fallback.
        assert_eq!(api_path_for_base(Some("/")), "/container/api");
        // Trailing slash trimmed.
        assert_eq!(api_path_for_base(Some("/local/")), "/local/container/api");
        // Nested base.
        assert_eq!(api_path_for_base(Some("/sites/east")), "/sites/east/container/api");
        // Malformed: missing leading slash → fallback (the runtime
        // would reject this; emitting "local/container/api" would be
        // a footgun).
        assert_eq!(api_path_for_base(Some("local")), "/container/api");
        // Malformed: empty after trim.
        assert_eq!(api_path_for_base(Some("")), "/container/api");
        assert_eq!(api_path_for_base(Some("//")), "/container/api");
    }

    #[test]
    fn unit_target_parsing() {
        let t = parse_unit_targets(&[
            "resolver".to_string(),
            "resolver:0".to_string(),
            "id-map:12".to_string(),
        ])
        .unwrap();
        assert_eq!(t[0].unit, "resolver");
        assert_eq!(t[0].member, None);
        assert_eq!(t[1].unit, "resolver");
        assert_eq!(t[1].member, Some(0));
        assert_eq!(t[2].unit, "id-map");
        assert_eq!(t[2].member, Some(12));
        // A non-numeric index is a clear error, not a silent unit name.
        assert!(parse_unit_targets(&["resolver:abc".to_string()]).is_err());
    }

    #[test]
    fn parse_restart_round_trip() {
        assert!(matches!(parse_restart("no").unwrap(), Restart::No));
        assert!(matches!(parse_restart("yes").unwrap(), Restart::Yes));
        match parse_restart("rate-limited:2.5").unwrap() {
            Restart::RateLimited(s) => assert_eq!(s, 2.5),
            _ => panic!("expected RateLimited"),
        }
        assert!(parse_restart("rate-limited:nope").is_err());
        assert!(parse_restart("rate-limited:-1").is_err());
        assert!(parse_restart("rate-limited:0").is_err());
        assert!(parse_restart("bogus").is_err());
    }

    #[test]
    fn add_id_map_writes_expected_unit() {
        let dir = tempfile::tempdir().unwrap();
        add_id_map(IdMapAddArgs {
            dir: Some(dir.path().to_path_buf()),
            name: "id-map".into(),
            netidx_binary: Some(PathBuf::from("/usr/local/bin/netidx")),
            socket: Some(PathBuf::from("/run/netidx/id-map.sock")),
            config: Some(PathBuf::from("/etc/netidx/id-map.json")),
            socket_mode: Some("660".into()),
            restart: None,
        })
        .unwrap();
        let path = dir.path().join("id-map.unit");
        assert!(path.exists());
        let bytes = std::fs::read(&path).unwrap();
        let u: netidx_conf::activation::Unit = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(u.process.exe, "/usr/local/bin/netidx");
        // `-f` is mandatory under the activation supervisor; see the
        // regression note on `template::services::id_map::unit`.
        assert_eq!(
            u.process.args,
            vec![
                "id-map",
                "serve",
                "-f",
                "--socket",
                "/run/netidx/id-map.sock",
                "--config",
                "/etc/netidx/id-map.json",
                "--socket-mode",
                "660",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn add_container_writes_expected_unit() {
        let dir = tempfile::tempdir().unwrap();
        add_container(ContainerAddArgs {
            dir: Some(dir.path().to_path_buf()),
            name: "container".into(),
            api_path: Some("/foo/container/api".into()),
            db: Some(PathBuf::from("/tmp/db")),
            compress: true,
            bind: Some("local".into()),
            netidx_binary: Some(PathBuf::from("/usr/local/bin/netidx")),
            restart: None,
        })
        .unwrap();
        let path = dir.path().join("container.unit");
        assert!(path.exists());
        let bytes = std::fs::read(&path).unwrap();
        let u: netidx_conf::activation::Unit = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(u.process.exe, "/usr/local/bin/netidx");
        assert_eq!(
            u.process.args,
            vec![
                "container",
                "--api-path",
                "/foo/container/api",
                "--db",
                "/tmp/db",
                "--compress",
                "--bind",
                "local",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );
    }
}
