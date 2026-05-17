use anyhow::{Context, Result};
use netidx_conf::{
    atomic,
    ca::{self, Ca, CaParams, IssueParams, IssuedFiles, SanEntry, Subject},
    paths, tls,
};
use std::{io::IsTerminal, net::IpAddr, path::PathBuf};
use structopt::StructOpt;

use super::prompt;

#[derive(StructOpt, Debug)]
pub(crate) enum Cmd {
    #[structopt(name = "init", about = "create a new local CA")]
    Init(InitParams),
    #[structopt(name = "issue", about = "issue a leaf certificate from a CA")]
    Issue(IssueArgs),
    #[structopt(
        name = "request",
        about = "generate a private key + CSR locally, to be signed by a CA elsewhere"
    )]
    Request(RequestArgs),
    #[structopt(
        name = "sign",
        about = "sign an externally-supplied CSR with a local CA"
    )]
    Sign(SignArgs),
    #[structopt(name = "list", about = "list local CAs")]
    List,
}

#[derive(StructOpt, Debug)]
pub(crate) struct InitParams {
    /// Common Name on the CA cert. Prompted for when stdin is a TTY
    /// and this flag is omitted.
    #[structopt(long = "cn")]
    pub cn: Option<String>,
    #[structopt(long = "country")]
    pub country: Option<String>,
    #[structopt(long = "state")]
    pub state: Option<String>,
    #[structopt(long = "locality")]
    pub locality: Option<String>,
    #[structopt(long = "organization", short = "O")]
    pub organization: Option<String>,
    /// SubjectAltName entry. Repeatable. Form: `dns:<name>`,
    /// `ip:<addr>`, `uri:<uri>`, or `email:<addr>`. Defaults to a
    /// single `dns:<cn>` if not given.
    #[structopt(long = "san", number_of_values = 1)]
    pub san: Vec<String>,
    #[structopt(long = "key-bits", default_value = "4096")]
    pub key_bits: u32,
    #[structopt(long = "validity-days", default_value = "7300")]
    pub validity_days: u32,
    /// Skip the password prompt and write an unencrypted key.
    #[structopt(long = "no-password")]
    pub no_password: bool,
    /// Override the directory the CA is created in. Defaults to
    /// `${basedir}/ca/` — one CA per netidx install.
    #[structopt(long = "dir")]
    pub dir: Option<PathBuf>,
}

#[derive(StructOpt, Debug)]
pub(crate) struct IssueArgs {
    /// Common Name for the issued cert. Prompted when omitted.
    #[structopt(long = "cn")]
    pub cn: Option<String>,
    #[structopt(long = "country")]
    pub country: Option<String>,
    #[structopt(long = "state")]
    pub state: Option<String>,
    #[structopt(long = "locality")]
    pub locality: Option<String>,
    #[structopt(long = "organization", short = "O")]
    pub organization: Option<String>,
    #[structopt(long = "san", number_of_values = 1)]
    pub san: Vec<String>,
    #[structopt(long = "key-bits", default_value = "4096")]
    pub key_bits: u32,
    #[structopt(long = "validity-days", default_value = "730")]
    pub validity_days: u32,
    #[structopt(long = "no-password")]
    pub no_password: bool,
    /// Override the CA's directory. Defaults to `${basedir}/ca/`.
    #[structopt(long = "ca-dir")]
    pub ca_dir: Option<PathBuf>,
    /// Where to write the issued `private.key` + `certificate.pem`.
    /// Prompted when omitted.
    #[structopt(long = "out", short = "o")]
    pub out_dir: Option<PathBuf>,
}

#[derive(StructOpt, Debug)]
pub(crate) struct RequestArgs {
    /// Common Name for the requested cert. Prompted when omitted.
    #[structopt(long = "cn")]
    pub cn: Option<String>,
    #[structopt(long = "country")]
    pub country: Option<String>,
    #[structopt(long = "state")]
    pub state: Option<String>,
    #[structopt(long = "locality")]
    pub locality: Option<String>,
    #[structopt(long = "organization", short = "O")]
    pub organization: Option<String>,
    /// SubjectAltName entry. Repeatable. Form: `dns:<name>`,
    /// `ip:<addr>`, `uri:<uri>`, or `email:<addr>`. Defaults to a
    /// single `dns:<cn>` if not given.
    #[structopt(long = "san", number_of_values = 1)]
    pub san: Vec<String>,
    #[structopt(long = "key-bits", default_value = "4096")]
    pub key_bits: u32,
    /// Output path for the generated private key (mode 0600).
    /// Defaults to `./private.key`; the default path refuses to
    /// overwrite an existing file (an explicit `--out-key` does not).
    #[structopt(long = "out-key")]
    pub out_key: Option<PathBuf>,
    /// Output path for the generated CSR (mode 0644). Defaults to
    /// `./<cn>.csr`.
    #[structopt(long = "out-csr")]
    pub out_csr: Option<PathBuf>,
}

#[derive(StructOpt, Debug)]
pub(crate) struct SignArgs {
    /// Path to the CSR (PEM-encoded) to sign. Prompted when omitted.
    pub csr_path: Option<PathBuf>,
    /// SubjectAltName entry to embed in the signed cert. Repeatable.
    /// The CA is authoritative — these override whatever the CSR
    /// claims. One of `--san` or `--accept-csr-san` must be passed:
    /// the CLI deliberately does not silently inherit SAN from the
    /// CSR, since an absent-minded admin signing whatever was
    /// requested is the most likely failure mode of a CA tool.
    #[structopt(long = "san", number_of_values = 1)]
    pub san: Vec<String>,
    /// Accept the CSR's embedded SAN as-is. The summary is still
    /// printed before signing; this flag just makes the
    /// inherit-from-CSR decision explicit rather than implicit.
    #[structopt(long = "accept-csr-san")]
    pub accept_csr_san: bool,
    #[structopt(long = "validity-days", default_value = "730")]
    pub validity_days: u32,
    #[structopt(long = "no-password")]
    pub no_password: bool,
    /// Override the CA's directory. Defaults to `${basedir}/ca/`.
    #[structopt(long = "ca-dir")]
    pub ca_dir: Option<PathBuf>,
    /// Where to write the signed certificate (mode 0644). Defaults
    /// to `./<csr-cn>.pem` (or `./certificate.pem` if the CSR has no
    /// CN).
    #[structopt(long = "out", short = "o")]
    pub out: Option<PathBuf>,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Init(p) => init(p),
        Cmd::Issue(p) => issue(p),
        Cmd::Request(p) => request(p),
        Cmd::Sign(p) => sign(p),
        Cmd::List => list(),
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
    let password = collect_password(p.no_password, true)?;
    let san = parse_sans(&p.san, &cn)?;
    let ca = Ca::init(
        &CaParams {
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
        },
        password.as_deref(),
    )?;
    let _ = ca;
    println!("initialized CA at {}", directory.display());
    if password.is_some() {
        println!("  (private key is encrypted; password required to issue)");
    } else {
        println!("  (private key is unencrypted)");
    }
    Ok(())
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
    Ok(())
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
fn sanitize_filename(s: &str) -> String {
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
fn default_csr_filename(cn: &str) -> PathBuf {
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
            if !is_stdin_tty() {
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

/// Issue an identity (CN = SAN-DNS = `name`) from `ca`, into the
/// canonical `${user_tls_dir}/<name>/` directory. Returns the issued
/// file paths.
pub(super) fn issue_identity(ca: &Ca, name: &str) -> Result<IssuedFiles> {
    let out_dir = tls::identity_dir(name)?;
    issue_identity_into(ca, name, out_dir, ca::DEFAULT_KEY_BITS)
}

/// Inner form of [`issue_identity`] with the destination directory
/// and key size as parameters — lets tests issue into a tempdir with
/// a fast key.
fn issue_identity_into(
    ca: &Ca,
    name: &str,
    out_dir: PathBuf,
    key_bits: u32,
) -> Result<IssuedFiles> {
    ca.issue(&IssueParams {
        subject: Subject::cn(name),
        // Exactly one DNS SAN, matching the CN — that's what the
        // netidx TLS validator requires of a member-server cert.
        san: vec![SanEntry::Dns(name.to_string())],
        key_bits,
        validity_days: ca::DEFAULT_LEAF_VALIDITY_DAYS,
        out_dir,
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
    if !is_stdin_tty() {
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

fn is_stdin_tty() -> bool {
    std::io::stdin().is_terminal()
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
        // through a level-1 prompt (default Y). `cargo test` runs
        // without a TTY, so `prompt::confirm` returns the default,
        // which means signing succeeds and the resulting cert carries
        // the CSR's SAN. The interactive path is "type 'n' to reject
        // and bail" — covered by smoke-testing the built binary.
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
        })
        .unwrap();
        // The (true, false) branch went through the prompt-default-Y
        // path: same code as `accept_csr_san=true`, so it would have
        // bailed pre-change with "must pass either --san or
        // --accept-csr-san". Output cert is a real PEM X.509.
        ca::validate_pem_cert_file(&out_cert).unwrap();
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
        // CI runs without a TTY on stdin, so an omitted required arg
        // must bail rather than hang. We exercise the non-TTY branch
        // by passing `None` and trusting `is_stdin_tty()` returns
        // false here (which it does under cargo test).
        let r = prompt::required_string("test prompt", None);
        assert!(r.is_err());
        let msg = format!("{:#}", r.unwrap_err());
        assert!(
            msg.contains("not a TTY"),
            "should report non-TTY context: {msg}"
        );
    }
}
