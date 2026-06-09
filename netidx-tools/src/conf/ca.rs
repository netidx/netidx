use anyhow::{anyhow, Context, Result};
use netidx_conf::{
    atomic,
    ca::{self, Ca, CaParams, IssueParams, IssuedFiles, SanEntry, Subject},
    ca_join, ca_proto, ca_server, ca_vault,
    fingerprint::{ColorMode, Fingerprint},
    paths, tls,
};
use clap::{Args, Subcommand};
use serde_derive::{Deserialize, Serialize};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};
use zeroize::Zeroizing;

use super::{prompt, service};

/// CA-server daemon config (`<ca-dir>/server.json`): everything `ca
/// serve` needs. Issuance policy lives per-admin in the vault, so this
/// is transport-only.
#[derive(Debug, Serialize, Deserialize)]
struct CaServerConfig {
    ca_dir: PathBuf,
    listen: SocketAddr,
    serving_cert: PathBuf,
    serving_key: PathBuf,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// create a new local CA (keyslot vault; can run as a server)
    Init(InitParams),
    /// issue a leaf certificate from a CA
    Issue(IssueArgs),
    /// generate a private key + CSR locally, to be signed by a CA elsewhere
    Request(RequestArgs),
    /// sign an externally-supplied CSR with a local CA
    Sign(SignArgs),
    /// list local CAs
    List,
    /// run the CA server (signs CSRs received over TLS)
    Serve(ServeArgs),
    /// manage CA admin keyslots (add / revoke / list)
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
    /// show the CA's fingerprint + identicon for out-of-band verification
    Fingerprint(FingerprintArgs),
    /// request a certificate from a CA server and install it
    Join(JoinArgs),
}

#[derive(Subcommand, Debug)]
pub(crate) enum AdminCmd {
    /// add an admin keyslot (a new password that can sign)
    Add(AdminAddArgs),
    /// revoke an admin keyslot
    Remove(AdminRemoveArgs),
    /// list admin keyslots and their issuance policy
    List(AdminScopeArgs),
}

#[derive(Args, Debug)]
pub(crate) struct AdminScopeArgs {
    /// Override the CA directory. Defaults to `${basedir}/ca/`.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct AdminAddArgs {
    /// Name of the new admin. Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// SAN glob this admin may issue (repeatable). Prompted when omitted.
    #[arg(long = "allow-san", num_args = 1)]
    pub allow_san: Vec<String>,
    /// Max validity (days) this admin may issue. Default 730.
    #[arg(long, default_value = "730")]
    pub max_validity_days: u32,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct AdminRemoveArgs {
    /// Name of the admin to revoke. Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// Allow removing the last admin (locks the CA permanently).
    #[arg(long)]
    pub force: bool,
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct FingerprintArgs {
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct ServeArgs {
    /// Path to the server config (`<ca-dir>/server.json`). Defaults to
    /// the default CA dir's `server.json`.
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    /// Don't daemonize (run in the foreground).
    #[arg(short, long)]
    #[allow(dead_code)]
    pub foreground: bool,
}

#[derive(Args, Debug)]
pub(crate) struct JoinArgs {
    /// CA server address (`ip:port`). Prompted when omitted.
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// The TLS identity name to request (one DNS SAN). Prompted when omitted.
    #[arg(long)]
    pub name: Option<String>,
    /// The admin name to authenticate as. Prompted when omitted.
    #[arg(long)]
    pub admin: Option<String>,
    /// Validity (days) to request. Default 730 (capped by server policy).
    #[arg(long, default_value = "730")]
    pub validity_days: u32,
}

#[derive(Args, Debug)]
pub(crate) struct InitParams {
    /// Common Name on the CA cert. Prompted for when stdin is a TTY
    /// and this flag is omitted.
    #[arg(long)]
    pub cn: Option<String>,
    #[arg(long)]
    pub country: Option<String>,
    #[arg(long)]
    pub state: Option<String>,
    #[arg(long)]
    pub locality: Option<String>,
    #[arg(short = 'O', long)]
    pub organization: Option<String>,
    /// SubjectAltName entry. Repeatable. Form: `dns:<name>`,
    /// `ip:<addr>`, `uri:<uri>`, or `email:<addr>`. Defaults to a
    /// single `dns:<cn>` if not given.
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    #[arg(long, default_value = "4096")]
    pub key_bits: u32,
    #[arg(long, default_value = "7300")]
    pub validity_days: u32,
    /// The first admin's name (keyslot label). Defaults to the current
    /// unix user; prompted only if that can't be determined.
    #[arg(long)]
    pub admin: Option<String>,
    /// SAN glob the first admin may issue (repeatable). Prompted when
    /// omitted — e.g. `*.example.com`.
    #[arg(long = "allow-san", num_args = 1)]
    pub allow_san: Vec<String>,
    /// Max validity (days) the first admin may issue. Default 730.
    #[arg(long, default_value = "730")]
    pub max_validity_days: u32,
    /// Set up the CA server (issue a serving cert + write server.json)
    /// without prompting. By default `ca init` asks.
    #[arg(long)]
    pub with_server: bool,
    /// Skip the CA-server setup entirely (offline CA only).
    #[arg(long, conflicts_with = "with_server")]
    pub no_server: bool,
    /// Address the CA server should listen on when set up. Default
    /// `0.0.0.0:<ca-port>`.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Where to drop the CA server's activation unit. Defaults to the
    /// user activation dir (same place the resolver/id-map units go).
    #[arg(long = "units-dir")]
    pub units_dir: Option<PathBuf>,
    /// After setting up the CA server, also register netidx as an OS
    /// service without prompting. Mutually exclusive with
    /// `--no-service`.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    pub with_service: bool,
    /// Skip the OS-service prompt after setting up the CA server.
    #[arg(long = "no-service")]
    pub no_service: bool,
    /// Override the directory the CA is created in. Defaults to
    /// `${basedir}/ca/` — one CA per netidx install.
    #[arg(long)]
    pub dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct IssueArgs {
    /// Common Name for the issued cert. Prompted when omitted.
    #[arg(long)]
    pub cn: Option<String>,
    #[arg(long)]
    pub country: Option<String>,
    #[arg(long)]
    pub state: Option<String>,
    #[arg(long)]
    pub locality: Option<String>,
    #[arg(short = 'O', long)]
    pub organization: Option<String>,
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    #[arg(long, default_value = "4096")]
    pub key_bits: u32,
    #[arg(long, default_value = "730")]
    pub validity_days: u32,
    #[arg(long)]
    pub no_password: bool,
    /// Override the CA's directory. Defaults to `${basedir}/ca/`.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    /// Where to write the issued `private.key` + `certificate.pem`.
    /// Prompted when omitted.
    #[arg(short, long = "out")]
    pub out_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct RequestArgs {
    /// Common Name for the requested cert. Prompted when omitted.
    #[arg(long)]
    pub cn: Option<String>,
    #[arg(long)]
    pub country: Option<String>,
    #[arg(long)]
    pub state: Option<String>,
    #[arg(long)]
    pub locality: Option<String>,
    #[arg(short = 'O', long)]
    pub organization: Option<String>,
    /// SubjectAltName entry. Repeatable. Form: `dns:<name>`,
    /// `ip:<addr>`, `uri:<uri>`, or `email:<addr>`. Defaults to a
    /// single `dns:<cn>` if not given.
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    #[arg(long, default_value = "4096")]
    pub key_bits: u32,
    /// Output path for the generated private key (mode 0600).
    /// Defaults to `./private.key`; the default path refuses to
    /// overwrite an existing file (an explicit `--out-key` does not).
    #[arg(long)]
    pub out_key: Option<PathBuf>,
    /// Output path for the generated CSR (mode 0644). Defaults to
    /// `./<cn>.csr`.
    #[arg(long)]
    pub out_csr: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub(crate) struct SignArgs {
    /// Path to the CSR (PEM-encoded) to sign. Prompted when omitted.
    pub csr_path: Option<PathBuf>,
    /// SubjectAltName entry to embed in the signed cert. Repeatable.
    /// The CA is authoritative — these override whatever the CSR
    /// claims. One of `--san` or `--accept-csr-san` must be passed:
    /// the CLI deliberately does not silently inherit SAN from the
    /// CSR, since an absent-minded admin signing whatever was
    /// requested is the most likely failure mode of a CA tool.
    #[arg(long, num_args = 1)]
    pub san: Vec<String>,
    /// Accept the CSR's embedded SAN as-is. The summary is still
    /// printed before signing; this flag just makes the
    /// inherit-from-CSR decision explicit rather than implicit.
    #[arg(long)]
    pub accept_csr_san: bool,
    #[arg(long, default_value = "730")]
    pub validity_days: u32,
    #[arg(long)]
    pub no_password: bool,
    /// Override the CA's directory. Defaults to `${basedir}/ca/`.
    #[arg(long)]
    pub ca_dir: Option<PathBuf>,
    /// Where to write the signed certificate (mode 0644). Defaults
    /// to `./<csr-cn>.pem` (or `./certificate.pem` if the CSR has no
    /// CN).
    #[arg(short, long)]
    pub out: Option<PathBuf>,
    /// Skip the post-sign id-map registration prompt. The default on
    /// a TTY (when a local id-map exists) is to prompt for groups
    /// and add the identity to the map; this flag suppresses that
    /// entirely. Non-TTY callers already skip the prompt by default,
    /// so this is mostly useful for interactive sessions where you
    /// want to handle id-map registration separately (or not at
    /// all).
    #[arg(long)]
    pub no_id_map: bool,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Init(p) => init(p),
        Cmd::Issue(p) => issue(p),
        Cmd::Request(p) => request(p),
        Cmd::Sign(p) => sign(p),
        Cmd::List => list(),
        Cmd::Serve(p) => serve(p),
        Cmd::Admin { cmd } => admin(cmd),
        Cmd::Fingerprint(p) => fingerprint(p),
        Cmd::Join(p) => join(p),
    }
}

fn ca_dir_for(override_: Option<PathBuf>) -> Result<PathBuf> {
    match override_ {
        Some(p) => Ok(p),
        None => paths::user_ca_dir(),
    }
}

fn init(p: InitParams) -> Result<()> {
    let directory = ca_dir_for(p.dir)?;
    let cn = prompt::required_string("CA common name", p.cn)?;
    let san = parse_sans(&p.san, &cn)?;
    // The first admin: name + password + issuance policy.
    let admin = match p.admin {
        Some(a) => a,
        None => default_admin_name()?,
    };
    let password = collect_required_password(&format!(
        "set a CA password for admin {admin:?} (this signs certs)"
    ))?;
    let policy = prompt_policy(&p.allow_san, p.max_validity_days, &cn)?;

    // Generate the CA with its key returned (never written to disk in
    // plaintext) and seal it into the vault under the first admin.
    let (ca, key_pem) = Ca::init_vaulted(&CaParams {
        directory: directory.clone(),
        subject: Subject {
            common_name: cn.clone(),
            country: p.country,
            state: p.state,
            locality: p.locality,
            organization: p.organization,
        },
        san,
        key_bits: p.key_bits,
        validity_days: p.validity_days,
    })?;
    ca_vault::create(&directory, &key_pem, &admin, &password, policy)
        .context("sealing CA key into the vault")?;

    println!("initialized CA at {}", directory.display());
    println!("  admin {admin:?} can sign; the CA key is encrypted at rest (keyslot vault)");
    println!();
    show_ca_identity(&directory)?;
    println!();
    println!(
        "Share the fingerprint/identicon above with anyone joining, so they can\n\
         verify they're talking to the real CA before sending a password."
    );

    // Optionally set up the server (serving cert + server.json + unit).
    let set_up_server = if p.no_server {
        false
    } else if p.with_server {
        true
    } else {
        prompt::confirm(
            "set up the CA server (so nodes can request certs over the network)?",
            true,
        )?
    };
    let need = if set_up_server {
        setup_server(&directory, &ca, p.listen, p.units_dir.clone())?
    } else {
        service::ServiceNeed::NONE
    };

    // Single end-of-process hook: if we installed a server unit, offer
    // to register the activation supervisor as a system service. This
    // is the SAME entry point the `conf install` templates use; when a
    // future flow sets up a CA *and* a resolver, both contribute a
    // `ServiceNeed` and the offer still fires exactly once.
    service::offer(need, service::ServiceGate {
        dry_run: false,
        no_service: p.no_service,
        with_service: p.with_service,
    })
}

/// Issue the daemon's serving cert (a CA-issued leaf with the reserved
/// `SERVING_SAN`), write `server.json`, and drop the `ca` activation
/// unit. Returns the [`ServiceNeed`](service::ServiceNeed) the caller
/// folds into its single end-of-process service offer — a CA server is
/// a network daemon, so it needs a **system**-scope service.
///
/// This is the reusable seam: `ca init` calls it standalone, and a
/// future resolver flow that also stands up a CA can call it and merge
/// the returned need with its own.
fn setup_server(
    ca_dir: &std::path::Path,
    ca: &Ca,
    listen: Option<SocketAddr>,
    units_dir: Option<PathBuf>,
) -> Result<service::ServiceNeed> {
    let server_dir = ca_dir.join("server");
    std::fs::create_dir_all(&server_dir)
        .with_context(|| format!("creating {}", server_dir.display()))?;
    // Generate the serving key + CSR (ECDSA) and have the CA sign it.
    let kc = ca_join::generate_key_and_csr(ca_proto::SERVING_SAN)?;
    let leaf = ca
        .sign_request(
            kc.csr_pem.as_bytes(),
            &[SanEntry::Dns(ca_proto::SERVING_SAN.to_string())],
            ca::DEFAULT_LEAF_VALIDITY_DAYS,
        )
        .context("signing the CA server's serving certificate")?;
    let ca_cert = std::fs::read(ca_dir.join("certificate.pem"))?;
    // Chain = [serving leaf, ca cert] so the client receives the CA.
    let mut chain = leaf;
    chain.extend_from_slice(&ca_cert);
    let serving_cert = server_dir.join("cert.pem");
    let serving_key = server_dir.join("key.pem");
    atomic::write_atomic(&serving_cert, &chain, 0o644)?;
    atomic::write_atomic(&serving_key, kc.private_key_pem.as_bytes(), 0o600)?;

    let listen = listen
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], ca_proto::DEFAULT_PORT)));
    let cfg = CaServerConfig {
        ca_dir: ca_dir.to_path_buf(),
        listen,
        serving_cert,
        serving_key,
    };
    let cfg_path = ca_dir.join("server.json");
    atomic::write_atomic(
        &cfg_path,
        serde_json::to_vec_pretty(&cfg).context("serializing server.json")?.as_slice(),
        0o644,
    )?;

    // Drop the activation unit so the supervisor runs `ca serve`.
    let units_dir = match units_dir {
        Some(d) => d,
        None => paths::user_activation_dir()?,
    };
    std::fs::create_dir_all(&units_dir)
        .with_context(|| format!("creating activation dir {}", units_dir.display()))?;
    let netidx_binary = std::env::current_exe()
        .context("could not determine current netidx binary for the CA server unit")?;
    let unit = netidx_conf::template::services::ca_server::unit(
        &netidx_conf::template::services::ca_server::CaServerServiceParams {
            netidx_binary,
            config: cfg_path.clone(),
        },
    )?;
    let dir = netidx_conf::activation::ActivationDir::open(Some(&units_dir))?;
    dir.save("ca", &unit).context("writing the ca activation unit")?;

    println!();
    println!("CA server configured:");
    println!("  config:   {}", cfg_path.display());
    println!("  listen:   {listen}");
    println!("  unit:     {}", netidx_conf::activation::unit_path_in(&units_dir, "ca").display());
    // A CA server is a network daemon → system-scope service.
    Ok(service::ServiceNeed::at(service::ScopeArg::System))
}

// -- ca serve -----------------------------------------------------------------

fn serve(p: ServeArgs) -> Result<()> {
    env_logger::init();
    let cfg_path = match p.config {
        Some(c) => c,
        None => paths::user_ca_dir()?.join("server.json"),
    };
    let bytes = std::fs::read(&cfg_path)
        .with_context(|| format!("reading CA server config {}", cfg_path.display()))?;
    let cfg: CaServerConfig = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing {}", cfg_path.display()))?;
    let serving_cert_pem = std::fs::read(&cfg.serving_cert)
        .with_context(|| format!("reading serving cert {}", cfg.serving_cert.display()))?;
    let serving_key_pem = std::fs::read(&cfg.serving_key)
        .with_context(|| format!("reading serving key {}", cfg.serving_key.display()))?;
    let params = ca_server::ServeParams {
        ca_dir: cfg.ca_dir,
        listen: cfg.listen,
        serving_cert_pem,
        serving_key_pem,
    };
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    rt.block_on(ca_server::serve(params))
}

// -- ca admin -----------------------------------------------------------------

fn admin(cmd: AdminCmd) -> Result<()> {
    match cmd {
        AdminCmd::Add(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let name = prompt::required_string("new admin name", a.name)?;
            let policy = prompt_policy(&a.allow_san, a.max_validity_days, "")?;
            let existing =
                collect_existing_password("an existing admin password (to authorize)")?;
            let new_pw =
                collect_required_password(&format!("password for new admin {name:?}"))?;
            ca_vault::add_admin(&dir, &existing, &name, &new_pw, policy)?;
            println!("added admin {name:?}");
            Ok(())
        }
        AdminCmd::Remove(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let name = prompt::required_string("admin to revoke", a.name)?;
            let auth = collect_existing_password("an admin password (to authorize)")?;
            ca_vault::remove_admin(&dir, &auth, &name, a.force)?;
            println!("revoked admin {name:?}");
            Ok(())
        }
        AdminCmd::List(a) => {
            let dir = ca_dir_for(a.ca_dir)?;
            let admins = ca_vault::list_admins(&dir)?;
            if admins.is_empty() {
                println!("(no admins — this CA is not vault-protected)");
            }
            for (name, pol) in admins {
                println!(
                    "{name}: allowed_san={:?} max_validity_days={}",
                    pol.allowed_san, pol.max_validity_days
                );
            }
            Ok(())
        }
    }
}

// -- ca fingerprint -----------------------------------------------------------

fn fingerprint(p: FingerprintArgs) -> Result<()> {
    let dir = ca_dir_for(p.ca_dir)?;
    show_ca_identity(&dir)
}

// -- ca join (the client) -----------------------------------------------------

fn join(p: JoinArgs) -> Result<()> {
    let server: SocketAddr = match p.server {
        Some(s) => s,
        None => prompt::required_parsed("CA server address (ip:port)", None)?,
    };
    let name = prompt::required_string("TLS identity name to request", p.name)?;
    let admin = prompt::required_string("admin name", p.admin)?;
    let password = Zeroizing::new(collect_existing_password(&format!(
        "CA password for admin {admin:?}"
    ))?);
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let issued = rt.block_on(ca_join::request_cert(
        server,
        ca_proto::NodeKind::Client,
        &name,
        &admin,
        password,
        p.validity_days,
        |fp| {
            println!("The CA presented this identity:");
            println!("  SHA256  {}", fp.text());
            println!("{}", fp.identicon(ColorMode::detect()));
            prompt::confirm("does this match what your CA admin gave you?", false)
        },
    ))?;
    let dir = tls::identity_dir(&name)?;
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    atomic::write_atomic(&dir.join("certificate.pem"), issued.cert_pem.as_bytes(), 0o644)?;
    atomic::write_atomic(
        &dir.join("private.key"),
        issued.private_key_pem.as_bytes(),
        0o600,
    )?;
    atomic::write_atomic(&dir.join("trusted.pem"), issued.trusted_pem.as_bytes(), 0o644)?;
    println!("installed identity {name:?} in {}", dir.display());
    Ok(())
}

// -- shared helpers -----------------------------------------------------------

fn show_ca_identity(ca_dir: &std::path::Path) -> Result<()> {
    let cert = std::fs::read(ca_dir.join("certificate.pem"))
        .with_context(|| format!("reading CA cert in {}", ca_dir.display()))?;
    let fp = Fingerprint::of_pem(&cert)?;
    println!("CA fingerprint:");
    println!("  SHA256  {}", fp.text());
    println!("{}", fp.identicon(ColorMode::detect()));
    Ok(())
}

fn default_admin_name() -> Result<String> {
    for var in ["USER", "LOGNAME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.is_empty() {
                return Ok(v);
            }
        }
    }
    prompt::required_string("admin name", None)
}

fn prompt_policy(
    allow_san: &[String],
    max_validity_days: u32,
    cn: &str,
) -> Result<ca_vault::Policy> {
    let allowed_san = if !allow_san.is_empty() {
        allow_san.to_vec()
    } else {
        let suggestion = match cn.split_once('.') {
            Some((_, domain)) if !domain.is_empty() => format!("*.{domain}"),
            _ => "*".to_string(),
        };
        let entry = prompt::string_with_default(
            "SAN names this admin may issue (glob, e.g. *.example.com)",
            None,
            &suggestion,
        )?;
        vec![entry]
    };
    Ok(ca_vault::Policy { allowed_san, max_validity_days })
}

/// Prompt twice for a new password (confirmed, non-empty). Bails on a
/// non-TTY — a vaulted CA must have a real password.
fn collect_required_password(label: &str) -> Result<String> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Err(anyhow!(
            "{label}: a password is required but stdin is not a TTY"
        ));
    }
    loop {
        let pw = rpassword::prompt_password(format!("{label}: "))?;
        if pw.is_empty() {
            eprintln!("password must not be empty");
            continue;
        }
        let again = rpassword::prompt_password("again: ")?;
        if again != pw {
            eprintln!("passwords did not match; try again");
            continue;
        }
        return Ok(pw);
    }
}

/// Prompt once for an existing password (no confirmation).
fn collect_existing_password(label: &str) -> Result<String> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Err(anyhow!("{label}: stdin is not a TTY"));
    }
    Ok(rpassword::prompt_password(format!("{label}: "))?)
}

fn issue(p: IssueArgs) -> Result<()> {
    let directory = ca_dir_for(p.ca_dir)?;
    let cn = prompt::required_string("certificate common name", p.cn)?;
    let out_dir = prompt::required_path("output directory for key + cert", p.out_dir)?;
    let password = collect_password(p.no_password, false)?;
    let ca = Ca::open(&directory, password.as_deref())
        .with_context(|| format!("opening CA at {}", directory.display()))?;
    let san = parse_sans(&p.san, &cn)?;
    let issued = ca.issue(&IssueParams {
        subject: Subject {
            common_name: cn.clone(),
            country: p.country,
            state: p.state,
            locality: p.locality,
            organization: p.organization,
        },
        san,
        key_bits: p.key_bits,
        validity_days: p.validity_days,
        out_dir,
        // Leaf key encryption is wired through the install flow
        // (`netidx conf init`), where the engine knows how to plumb
        // an askpass entry into the emitted client config. The bare
        // `ca issue` CLI deliberately stays unencrypted: callers
        // here are doing manual cert issuance and don't necessarily
        // have a netidx config to receive the askpass.
        password: None,
    })?;
    println!("issued cert:");
    println!("  cn:          {}", cn);
    println!("  private key: {}", issued.private_key.display());
    println!("  certificate: {}", issued.certificate.display());
    Ok(())
}

fn request(p: RequestArgs) -> Result<()> {
    let cn = prompt::required_string("requested certificate common name", p.cn)?;
    // `--out-key` default is `./private.key`, but the *default* path
    // refuses to clobber: re-running `request` in the same dir would
    // otherwise silently destroy a key the operator may not have used
    // yet. An explicit `--out-key` overwrites freely — that's the
    // operator's call.
    let out_key = match p.out_key {
        Some(path) => path,
        None => {
            let default = PathBuf::from("private.key");
            if default.exists() {
                bail!(
                    "./private.key already exists — refusing to overwrite a \
                     private key. Pass --out-key <path>, or move the existing \
                     file."
                );
            }
            default
        }
    };
    // The CSR and (later) the signed cert are cheap to regenerate, so
    // their CWD defaults overwrite freely.
    let out_csr = p.out_csr.unwrap_or_else(|| default_csr_filename(&cn));
    let san = parse_sans(&p.san, &cn)?;
    let kr = ca::generate_csr(
        &Subject {
            common_name: cn.clone(),
            country: p.country,
            state: p.state,
            locality: p.locality,
            organization: p.organization,
        },
        &san,
        p.key_bits,
        // Bare `ca request` CLI doesn't encrypt the key — same
        // rationale as the `ca issue` CLI: encrypted leaf keys are
        // wired through `netidx conf init`, which knows how to set
        // the matching `tls.askpass` in the emitted config.
        None,
    )?;
    atomic::write_atomic(&out_key, &kr.private_key_pem, 0o600)
        .with_context(|| format!("writing private key to {:?}", out_key))?;
    atomic::write_atomic(&out_csr, &kr.csr_pem, 0o644)
        .with_context(|| format!("writing CSR to {:?}", out_csr))?;
    println!("wrote private key (0600): {}", out_key.display());
    println!("wrote CSR        (0644): {}", out_csr.display());
    println!();
    println!("# Next step: hand the CSR to a CA admin who runs");
    println!("#   netidx conf ca sign {} --out <cert.pem>", out_csr.display());
    Ok(())
}

fn sign(mut p: SignArgs) -> Result<()> {
    let csr_path =
        prompt::required_path("path to the CSR to sign", p.csr_path.take())?;
    let directory = ca_dir_for(p.ca_dir.take())?;
    let password = collect_password(p.no_password, false)?;
    let ca = Ca::open(&directory, password.as_deref())
        .with_context(|| format!("opening CA at {}", directory.display()))?;
    let csr_pem = std::fs::read(&csr_path)
        .with_context(|| format!("reading CSR {}", csr_path.display()))?;
    let summary = ca::inspect_csr(&csr_pem).context("inspecting CSR")?;
    println!("CSR summary:");
    println!("  path:        {}", csr_path.display());
    println!("  cn:          {}", summary.common_name.as_deref().unwrap_or("(none)"));
    println!("  key bits:    {}", summary.key_bits);
    if summary.san.is_empty() {
        println!("  san:         (none in CSR)");
    } else {
        println!("  san:");
        for entry in &summary.san {
            println!("    - {}", san_display(entry));
        }
    }
    // `--out` defaults to `./<csr-cn>.pem` once we've read the CN out
    // of the CSR (falling back to `./certificate.pem` for a CN-less
    // CSR). Certs are cheap to regenerate, so the default overwrites
    // freely.
    let out = p
        .out
        .take()
        .unwrap_or_else(|| default_cert_filename(summary.common_name.as_deref()));
    let san = resolve_sign_san(&p, &summary)?;
    println!("  signing SAN:");
    for entry in &san {
        println!("    - {}", san_display(entry));
    }
    let cert_pem = ca.sign_request(&csr_pem, &san, p.validity_days)?;
    atomic::write_atomic(&out, &cert_pem, 0o644)
        .with_context(|| format!("writing certificate to {:?}", out))?;
    println!("\nsigned cert (0644): {}", out.display());
    maybe_register_in_id_map(&summary, &san, p.no_id_map)?;
    Ok(())
}

/// After signing a cert, optionally register the new identity in
/// the local id-map (the file the resolver consults to map TLS SANs
/// to unix uid + groups). Silently skipped when:
/// - `--no-id-map` was passed, or
/// - no local id-map exists at the canonical user path, or
/// - the CSR carries no usable identity name (no SAN DNS entry and
///   no CN), or
/// - stdin is not a TTY (scripts use `netidx conf id-map set-user`
///   for explicit non-interactive registration; we don't want a
///   level-1 prompt to silently write a wrong UID).
///
/// On a TTY with an id-map present, prompts for groups (level-1,
/// default `users`) and uid (level-1, default = max(existing) + 1
/// starting from 1000). First group in the list is the primary;
/// the rest become secondary memberships. Refuses any group not
/// already in the map — there's no "create group on the fly" path
/// here because that would let a typo silently introduce a
/// privilege-bearing group.
fn maybe_register_in_id_map(
    summary: &ca::CsrSummary,
    san: &[SanEntry],
    no_id_map: bool,
) -> Result<()> {
    use netidx_conf::id_map;
    if no_id_map {
        return Ok(());
    }
    if !prompt::stdin_is_tty() {
        return Ok(());
    }
    // The identity NAME in the id-map is what the resolver sees on
    // the wire: the cert's SAN alt-name, i.e. the first DNS SAN.
    // Fall back to the CN if no DNS SAN (unlikely; netidx rejects
    // such certs anyway, but the prompt path shouldn't crash).
    let identity_name = san
        .iter()
        .find_map(|s| if let SanEntry::Dns(d) = s { Some(d.clone()) } else { None })
        .or_else(|| summary.common_name.clone());
    let identity_name = match identity_name {
        Some(n) => n,
        None => {
            println!("(no DNS SAN / CN — skipping id-map registration)");
            return Ok(());
        }
    };
    let map_path = id_map::user_id_map_path()?;
    let mut map = match id_map::load(&map_path) {
        Ok(m) => m,
        Err(_) => {
            // Most common cause: no map yet. Tell the operator
            // exactly what's missing so they can `id-map init` if
            // they want one, but don't fail the sign.
            println!(
                "(no local id-map at {} — skipping registration; \
                 create one with `netidx conf id-map init`)",
                map_path.display(),
            );
            return Ok(());
        }
    };
    if !prompt::confirm(
        &format!("register identity {identity_name:?} in the local id-map?"),
        true,
    )? {
        return Ok(());
    }
    // List groups so the operator knows what's valid; sorted for
    // readable output and stable across runs.
    let mut group_names: Vec<&str> =
        map.groups.keys().map(|k| k.as_str()).collect();
    group_names.sort_unstable();
    println!("available groups: {}", group_names.join(", "));
    let groups_str = prompt::string_with_default(
        "groups (comma-separated; first is primary)",
        None,
        "users",
    )?;
    let groups: Vec<&str> =
        groups_str.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    if groups.is_empty() {
        bail!(
            "no groups specified — at least the primary group is required"
        );
    }
    let (primary, secondary): (&str, &[&str]) = (groups[0], &groups[1..]);
    let uid: u32 = prompt::parsed_with_default(
        "uid",
        None,
        &next_uid_suggestion(&map).to_string(),
    )?;
    let prev = id_map::upsert_identity(&mut map, &identity_name, uid, primary, secondary)?;
    id_map::save(&map_path, &map)?;
    match prev {
        Some(old) => println!(
            "updated id-map: {identity_name} (was uid={} primary={})",
            old.uid,
            old.primary_group.as_str(),
        ),
        None => println!("added to id-map: {identity_name} uid={uid} primary={primary}"),
    }
    Ok(())
}

/// Suggested next uid: `max(existing uids) + 1`, clamped to start at
/// 1000. Picking from a deterministic base keeps the suggestion
/// stable and avoids colliding with low system uids.
fn next_uid_suggestion(map: &netidx_conf::id_map::IdMap) -> u32 {
    let max = map.identities.values().map(|i| i.uid).max();
    match max {
        Some(n) if n >= 1000 => n + 1,
        _ => 1000,
    }
}

/// Decide which SAN to embed in the signed cert.
///
/// - `--san …` (one or more) → use those as-is, override the CSR.
/// - `--accept-csr-san` → use whatever the CSR carries (no prompt).
/// - Both flags → error: the explicit choice makes the implicit
///   acceptance redundant, and combining them would quietly hide
///   whether `--san` came from the operator's intent or from an
///   earlier shell-history copy of the CSR's contents.
/// - Neither flag → level-1 prompt: the CSR summary (incl. its SAN)
///   was already printed by `sign`; ask the admin whether to accept
///   that SAN as-is, defaulting to yes. A non-TTY caller also takes
///   the default — scripts that want to be explicit can still pass
///   `--san` or `--accept-csr-san`. The bare "neither flag" case
///   used to bail and tell the operator to re-run with one of the
///   flags; that was a UX wart for the common interactive case.
fn resolve_sign_san(p: &SignArgs, summary: &ca::CsrSummary) -> Result<Vec<SanEntry>> {
    match (p.san.is_empty(), p.accept_csr_san) {
        (false, false) => p.san.iter().map(|s| parse_san_one(s)).collect(),
        (true, true) => {
            if summary.san.is_empty() {
                bail!(
                    "--accept-csr-san was set but the CSR carries no SAN; pass \
                     --san <kind>:<value> to specify one"
                );
            }
            Ok(summary.san.clone())
        }
        (false, true) => bail!(
            "pass either --san <kind>:<value> (one or more) or --accept-csr-san, \
             not both"
        ),
        (true, false) => {
            if summary.san.is_empty() {
                bail!(
                    "CSR carries no SAN to inherit; pass --san <kind>:<value> \
                     (one or more) to specify one"
                );
            }
            if prompt::confirm("use the CSR's SAN as the signed cert's SAN?", true)? {
                Ok(summary.san.clone())
            } else {
                bail!(
                    "rejected — re-run with --san <kind>:<value> (one or \
                     more) to override the CSR's SAN"
                );
            }
        }
    }
}

fn san_display(s: &SanEntry) -> String {
    match s {
        SanEntry::Dns(d) => format!("dns:{d}"),
        SanEntry::Ip(ip) => format!("ip:{ip}"),
        SanEntry::Uri(u) => format!("uri:{u}"),
        SanEntry::Email(e) => format!("email:{e}"),
    }
}

/// Make a CN safe to embed in a filename. CNs are usually hostnames
/// (already safe), but the field is free-form text, so replace
/// anything outside `[A-Za-z0-9._-]` with `_`. The result is always
/// a single path component — no separators survive — so a defaulted
/// output path can't traverse out of the cwd. Empty input collapses
/// to `_` so we never produce a bare extension like `.csr`.
pub(super) fn sanitize_filename(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() { "_".to_string() } else { out }
}

/// Default `request` CSR path: `./<cn>.csr`.
pub(super) fn default_csr_filename(cn: &str) -> PathBuf {
    PathBuf::from(format!("{}.csr", sanitize_filename(cn)))
}

/// Default `sign` cert path: `./<csr-cn>.pem`, or `./certificate.pem`
/// when the CSR carries no CN.
fn default_cert_filename(csr_cn: Option<&str>) -> PathBuf {
    let stem = match csr_cn {
        Some(cn) => sanitize_filename(cn),
        None => "certificate".to_string(),
    };
    PathBuf::from(format!("{stem}.pem"))
}

fn list() -> Result<()> {
    let dir = match paths::user_ca_dir() {
        Ok(p) => p,
        Err(_) => {
            println!("# no user config dir on this platform");
            return Ok(());
        }
    };
    if !dir.join("certificate.pem").is_file() {
        println!("# no CA at {} — run `netidx conf ca init` first", dir.display());
        return Ok(());
    }
    println!("CA at {}", dir.display());
    if dir.join("private.key").is_file() {
        println!("  private key: present");
    } else {
        println!("  private key: MISSING — CA cannot sign");
    }
    Ok(())
}

// -- generate-flow helpers ---------------------------------------------------
//
// Used by `netidx conf install resolver --auth tls` to offer a
// "just generate the resolver certificate" path: for a small org the
// resolver host is commonly the CA host too, and making that one-step
// is the whole point.

/// True if the default CA location holds a usable CA — both the cert
/// and the private key. (A cert with no key is a trust anchor we
/// imported, not a CA we can sign with.)
pub(super) fn default_ca_present() -> bool {
    match paths::user_ca_dir() {
        Ok(dir) => {
            dir.join("certificate.pem").is_file()
                && dir.join("private.key").is_file()
        }
        Err(_) => false,
    }
}

/// Open the CA at the default location. Tries an unencrypted open
/// first and only prompts for a password if the key turns out to be
/// encrypted — so the common unencrypted-CA case needs no prompt at
/// all. A non-TTY caller facing an encrypted key bails rather than
/// hanging.
pub(super) fn open_default_ca() -> Result<Ca> {
    let dir = paths::user_ca_dir()?;
    match Ca::open(&dir, None) {
        Ok(ca) => Ok(ca),
        // `Ca::open` reports the encrypted-key-without-password case
        // with a message containing "encrypted"; treat that as
        // "prompt and retry" and everything else as a hard failure.
        Err(e) if format!("{e:#}").contains("encrypted") => {
            if !prompt::stdin_is_tty() {
                bail!(
                    "the CA at {} has an encrypted private key and stdin is \
                     not a TTY; cannot prompt for the password",
                    dir.display(),
                );
            }
            let pw = rpassword::prompt_password("CA password: ")
                .context("reading CA password")?;
            Ca::open(&dir, Some(&pw))
                .with_context(|| format!("opening CA at {}", dir.display()))
        }
        Err(e) => {
            Err(e).with_context(|| format!("opening CA at {}", dir.display()))
        }
    }
}

/// Create a new CA at the default location, prompting for the common
/// name and an optional password (blank = unencrypted, exactly as
/// `ca init`).
pub(super) fn create_default_ca() -> Result<Ca> {
    let dir = paths::user_ca_dir()?;
    let cn = prompt::required_string("CA common name", None)?;
    let password = collect_password(false, true)?;
    let ca = Ca::init(
        &CaParams {
            directory: dir.clone(),
            subject: Subject::cn(cn.clone()),
            san: vec![SanEntry::Dns(cn)],
            key_bits: ca::DEFAULT_KEY_BITS,
            validity_days: ca::DEFAULT_CA_VALIDITY_DAYS,
        },
        password.as_deref(),
    )
    .with_context(|| format!("creating CA at {}", dir.display()))?;
    println!("created a new local CA at {}", dir.display());
    if password.is_some() {
        println!("  (private key is encrypted; password required to sign)");
    }
    Ok(ca)
}

/// Issue an identity (CN = SAN-DNS = `name`) from `ca` into `out_dir`.
/// Returns the issued file paths. The caller chooses `out_dir`: the
/// install flow issues into a staging dir and lets `apply()` copy the
/// result into the canonical location, so nothing under the config
/// tree is touched until the apply phase.
///
/// `password = Some(p)` encrypts the on-disk private key with `p`
/// (PKCS#8 + AES-256-CBC). `None` writes an unencrypted key.
/// Encrypted-key callers in the install flow also wire an
/// `askpass` entry into the emitted client config so netidx can
/// decrypt the key at startup.
pub(super) fn issue_identity(
    ca: &Ca,
    name: &str,
    out_dir: PathBuf,
    password: Option<&str>,
) -> Result<IssuedFiles> {
    issue_identity_into(ca, name, out_dir, ca::DEFAULT_KEY_BITS, password)
}

/// Inner form of [`issue_identity`] with the destination directory
/// and key size as parameters — lets tests issue into a tempdir with
/// a fast key.
fn issue_identity_into(
    ca: &Ca,
    name: &str,
    out_dir: PathBuf,
    key_bits: u32,
    password: Option<&str>,
) -> Result<IssuedFiles> {
    ca.issue(&IssueParams {
        subject: Subject::cn(name),
        // Exactly one DNS SAN, matching the CN — that's what the
        // netidx TLS validator requires of a member-server cert.
        san: vec![SanEntry::Dns(name.to_string())],
        key_bits,
        validity_days: ca::DEFAULT_LEAF_VALIDITY_DAYS,
        out_dir,
        password: password.map(|s| s.to_string()),
    })
    .with_context(|| format!("issuing certificate for {name}"))
}

fn parse_sans(raw: &[String], fallback_cn: &str) -> Result<Vec<SanEntry>> {
    if raw.is_empty() {
        return Ok(vec![SanEntry::Dns(fallback_cn.to_string())]);
    }
    raw.iter().map(|s| parse_san_one(s)).collect()
}

fn parse_san_one(s: &str) -> Result<SanEntry> {
    let (kind, val) = s
        .split_once(':')
        .ok_or_else(|| anyhow!("SAN must be in the form <kind>:<value>: {s:?}"))?;
    if val.is_empty() {
        bail!("SAN value must not be empty: {s:?}");
    }
    Ok(match kind {
        "dns" => SanEntry::Dns(val.to_string()),
        "ip" => SanEntry::Ip(
            val.parse::<IpAddr>()
                .map_err(|e| anyhow!("invalid SAN ip {val:?}: {e}"))?,
        ),
        "uri" => SanEntry::Uri(val.to_string()),
        "email" => SanEntry::Email(val.to_string()),
        other => bail!("unknown SAN kind {other:?}; expected dns / ip / uri / email"),
    })
}

/// Collect a CA password.
///
/// - `--no-password` ⇒ `None` (the on-disk key is unencrypted). This
///   is the scripted / non-interactive path: operators who want to
///   drive the CLI without a TTY just pass `--no-password`.
/// - else: prompt with `rpassword::prompt_password` (masked echo). On
///   `init` (`confirm: true`) the prompt fires twice to catch typos.
///
/// The engine never sees a passphrase from any source other than the
/// terminal. There is no environment-variable path — that would
/// expose the secret to anything that can read `/proc/<pid>/environ`,
/// which is a footgun. Use `--no-password` for scripts.
///
/// `pub(super)` so the standalone-resolver init flow can reuse it
/// when it creates a CA on the operator's behalf.
pub(super) fn collect_password(
    no_password: bool,
    confirm: bool,
) -> Result<Option<String>> {
    if no_password {
        return Ok(None);
    }
    if !prompt::stdin_is_tty() {
        bail!(
            "stdin is not a TTY and no password was supplied. Pass --no-password to write an unencrypted key, or run with a TTY attached to be prompted."
        );
    }
    let pw = rpassword::prompt_password("CA password (blank for no encryption): ")?;
    if pw.is_empty() {
        return Ok(None);
    }
    if confirm {
        let again = rpassword::prompt_password("CA password (again): ")?;
        if again != pw {
            bail!("passwords did not match");
        }
    }
    Ok(Some(pw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn san_parser() {
        assert!(matches!(
            parse_san_one("dns:example.com").unwrap(),
            SanEntry::Dns(s) if s == "example.com"
        ));
        assert!(matches!(
            parse_san_one("ip:127.0.0.1").unwrap(),
            SanEntry::Ip(ip) if ip == "127.0.0.1".parse::<IpAddr>().unwrap()
        ));
        assert!(parse_san_one("uri:https://x").is_ok());
        assert!(parse_san_one("email:a@b").is_ok());
        assert!(parse_san_one("bogus").is_err());
        assert!(parse_san_one("bogus:x").is_err());
        assert!(parse_san_one("ip:not-an-ip").is_err());
        // Empty values are rejected for every kind.
        for kind in ["dns", "ip", "uri", "email"] {
            assert!(
                parse_san_one(&format!("{kind}:")).is_err(),
                "empty {kind}: should be rejected",
            );
        }
    }

    #[test]
    fn san_defaults_to_dns_cn() {
        let v = parse_sans(&[], "host.example.com").unwrap();
        assert_eq!(v.len(), 1);
        assert!(matches!(&v[0], SanEntry::Dns(s) if s == "host.example.com"));
    }

    #[test]
    fn request_then_sign_round_trip() {
        // The full client/admin handoff: client generates key+CSR
        // locally; admin uses `Ca::sign_request` to mint a cert.
        let scratch = tempfile::tempdir().unwrap();
        let key_path = scratch.path().join("client.key");
        let csr_path = scratch.path().join("client.csr");
        // Use 2048 for test speed — production is 4096.
        request(RequestArgs {
            cn: Some("client.example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:client.example.com".into()],
            key_bits: 2048,
            out_key: Some(key_path.clone()),
            out_csr: Some(csr_path.clone()),
        })
        .unwrap();
        assert!(key_path.exists());
        assert!(csr_path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&key_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "key must be 0600");
            let mode = std::fs::metadata(&csr_path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o644, "csr must be 0644");
        }

        // Admin side: stand up a tiny CA and sign the CSR.
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("test-ca"),
                san: vec![SanEntry::Dns("test-ca".into())],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let cert_path = scratch.path().join("client.pem");
        sign(SignArgs {
            csr_path: Some(csr_path.clone()),
            san: vec![],
            // Explicit accept: the round trip flow simulates the admin
            // who has looked at the CSR and is happy to sign as-is.
            accept_csr_san: true,
            validity_days: 30,
            no_password: true,
            ca_dir: Some(ca_dir.clone()),
            out: Some(cert_path.clone()),
            no_id_map: true,
        })
        .unwrap();
        assert!(cert_path.exists());
        // Confirm we actually wrote a PEM-encoded leaf cert; the
        // engine-side `Ca::sign_request` test already verifies that
        // signed certs chain back to the CA.
        let bytes = std::fs::read(&cert_path).unwrap();
        assert!(bytes.starts_with(b"-----BEGIN CERTIFICATE-----"));
    }

    #[test]
    fn sign_without_flags_accepts_csr_san_by_default() {
        // With neither --san nor --accept-csr-san, `sign` now drops
        // through a level-1 prompt (default Y). In test builds
        // `prompt::stdin_is_tty()` is pinned to `false`, so
        // `prompt::confirm` returns the default, which means signing
        // succeeds and the resulting cert carries the CSR's SAN. The
        // interactive path is "type 'n' to reject and bail" — covered
        // by smoke-testing the built binary.
        let scratch = tempfile::tempdir().unwrap();
        let csr_path = scratch.path().join("client.csr");
        let key_path = scratch.path().join("client.key");
        request(RequestArgs {
            cn: Some("x.example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:x.example.com".into()],
            key_bits: 2048,
            out_key: Some(key_path),
            out_csr: Some(csr_path.clone()),
        })
        .unwrap();
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("strict-ca"),
                san: vec![SanEntry::Dns("strict-ca".into())],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let out_cert = scratch.path().join("out.pem");
        sign(SignArgs {
            csr_path: Some(csr_path),
            san: vec![],
            accept_csr_san: false,
            validity_days: 30,
            no_password: true,
            ca_dir: Some(ca_dir),
            out: Some(out_cert.clone()),
            no_id_map: true,
        })
        .unwrap();
        // The (true, false) branch went through the prompt-default-Y
        // path: same code as `accept_csr_san=true`, so it would have
        // bailed pre-change with "must pass either --san or
        // --accept-csr-san". Output cert is a real PEM X.509.
        netidx_conf::tls::validate_pem_cert_file(&out_cert).unwrap();
    }

    #[test]
    fn sign_without_flags_bails_when_csr_has_no_san() {
        // The "no SAN to inherit" branch — there's nothing to default
        // to, so the confirm-prompt path is skipped and we bail with
        // a clear "pass --san …" message regardless of TTY.
        let scratch = tempfile::tempdir().unwrap();
        // Build a CSR with no SAN by going through generate_csr directly
        // (request() always wires up dns:<cn> by default).
        let kr = ca::generate_csr(
            &Subject::cn("no-san"),
            &[],
            2048,
            None,
        )
        .unwrap();
        let csr_path = scratch.path().join("no-san.csr");
        std::fs::write(&csr_path, &kr.csr_pem).unwrap();
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("strict-ca"),
                san: vec![SanEntry::Dns("strict-ca".into())],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let err = sign(SignArgs {
            csr_path: Some(csr_path),
            san: vec![],
            accept_csr_san: false,
            validity_days: 30,
            no_password: true,
            ca_dir: Some(ca_dir),
            out: Some(scratch.path().join("out.pem")),
            no_id_map: true,
        })
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("--san"), "error should mention --san: {msg}");
    }

    #[test]
    fn sign_refuses_both_san_and_accept_flag() {
        let scratch = tempfile::tempdir().unwrap();
        let csr_path = scratch.path().join("client.csr");
        let key_path = scratch.path().join("client.key");
        request(RequestArgs {
            cn: Some("x.example.com".into()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:x.example.com".into()],
            key_bits: 2048,
            out_key: Some(key_path),
            out_csr: Some(csr_path.clone()),
        })
        .unwrap();
        let ca_dir = scratch.path().join("ca");
        Ca::init(
            &CaParams {
                directory: ca_dir.clone(),
                subject: Subject::cn("conflict-ca"),
                san: vec![SanEntry::Dns("conflict-ca".into())],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let err = sign(SignArgs {
            csr_path: Some(csr_path),
            san: vec!["dns:x.example.com".into()],
            accept_csr_san: true,
            validity_days: 30,
            no_password: true,
            ca_dir: Some(ca_dir),
            out: Some(scratch.path().join("out.pem")),
            no_id_map: true,
        })
        .unwrap_err();
        assert!(format!("{err:#}").contains("not both"));
    }

    #[test]
    fn sanitize_filename_keeps_safe_chars_replaces_rest() {
        // Typical hostnames pass through untouched.
        assert_eq!(sanitize_filename("alice.example.com"), "alice.example.com");
        assert_eq!(sanitize_filename("host-1_test"), "host-1_test");
        // Separators and spaces become `_` — no path component can
        // escape the cwd.
        assert_eq!(sanitize_filename("a/b"), "a_b");
        assert_eq!(sanitize_filename("../etc/passwd"), ".._etc_passwd");
        assert_eq!(sanitize_filename("with space"), "with_space");
        assert_eq!(sanitize_filename("weird*<>chars"), "weird___chars");
        // Empty collapses to `_` so we never produce a bare extension.
        assert_eq!(sanitize_filename(""), "_");
    }

    #[test]
    fn default_filenames() {
        assert_eq!(
            default_csr_filename("alice.example.com"),
            PathBuf::from("alice.example.com.csr"),
        );
        assert_eq!(
            default_cert_filename(Some("alice.example.com")),
            PathBuf::from("alice.example.com.pem"),
        );
        // CN-less CSR falls back to a fixed name.
        assert_eq!(
            default_cert_filename(None),
            PathBuf::from("certificate.pem"),
        );
        // Slashes in the CN can't produce a traversing path.
        assert_eq!(
            default_csr_filename("../sneaky"),
            PathBuf::from(".._sneaky.csr"),
        );
    }

    #[test]
    fn issue_identity_into_round_trip() {
        // Stand up a tiny CA, issue an identity from it into a temp
        // dir, and confirm the files land. 2048-bit keys keep the
        // test fast; the real `issue_identity` uses the 4096 default.
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = Ca::init(
            &CaParams {
                directory: ca_dir.path().to_path_buf(),
                subject: Subject::cn("test-ca"),
                san: vec![SanEntry::Dns("test-ca".into())],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let out = tempfile::tempdir().unwrap();
        let issued = issue_identity_into(
            &ca,
            "resolver.example.com",
            out.path().to_path_buf(),
            2048,
            None,
        )
        .unwrap();
        assert!(issued.certificate.exists());
        assert!(issued.private_key.exists());
        let cert = std::fs::read(&issued.certificate).unwrap();
        assert!(cert.starts_with(b"-----BEGIN CERTIFICATE-----"));
        let key = std::fs::read(&issued.private_key).unwrap();
        assert!(key.starts_with(b"-----BEGIN PRIVATE KEY-----"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                std::fs::metadata(&issued.private_key).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "issued key must be 0600");
        }
    }

    #[test]
    fn prompt_required_uses_provided_value() {
        // Sanity: when a value is provided, no prompt fires (so the
        // test is safe to run in CI where stdin is not a TTY).
        assert_eq!(
            prompt::required_string("ignored", Some("hello".to_string())).unwrap(),
            "hello"
        );
    }

    #[test]
    fn prompt_required_fails_without_tty() {
        // In test builds `prompt::stdin_is_tty()` is pinned to
        // `false`, so an omitted required arg must bail rather than
        // hang. We exercise the non-TTY branch by passing `None`.
        let r = prompt::required_string("test prompt", None);
        assert!(r.is_err());
        let msg = format!("{:#}", r.unwrap_err());
        assert!(
            msg.contains("not a TTY"),
            "should report non-TTY context: {msg}"
        );
    }
}
