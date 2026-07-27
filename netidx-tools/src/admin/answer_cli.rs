//! The strict-CLI [`Answerer`]: every decision value comes from a flag or
//! errors naming the flag. Never prompts, never discovers. Secrets come from
//! `--password-file` / `--password-stdin` (never argv). The glyph confirm
//! compares the presented CA against a required `--accept-glyph` fingerprint
//! the operator obtained out of band.

use anyhow::{Context, Result, bail};
use netidx_admin_client::{
    answer::{Answerer, Field, Progress, TrustDomainChoice, TrustDomainOption},
    transport::CaIdentity,
};
use netidx_admin_proto::{Secret, fingerprint::Fingerprint};
use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

/// Read a password from `--password-file` / `--password-stdin` (never argv) and
/// parse an out-of-band `--accept-glyph` fingerprint into a [`FlagAnswerer`].
/// Shared by install (`CommonFlags`) and remote-admin (`RemoteAuthFlags`).
pub(crate) fn make_flag_answerer(
    password_file: Option<&Path>,
    password_stdin: bool,
    accept_glyph: Option<&str>,
) -> Result<FlagAnswerer> {
    let password = read_secret(
        password_file,
        password_stdin,
        ("--password-file", "--password-stdin"),
    )?;
    Ok(FlagAnswerer::single(password, parse_glyph(accept_glyph)?))
}

/// The strict answerer for offline CA-vault ops (`ca sign` / `ca issue`): the
/// only secret is the CA **recovery** password, read from
/// `--recovery-password-file` / `--recovery-password-stdin` (never argv), and
/// there is no admin server to glyph-confirm. The flag names are threaded into
/// the answerer so a missing-secret error cites the right flags.
pub(crate) fn make_offline_answerer(
    recovery_password_file: Option<&Path>,
    recovery_password_stdin: bool,
) -> Result<FlagAnswerer> {
    let flags = ("--recovery-password-file", "--recovery-password-stdin");
    let password = read_secret(recovery_password_file, recovery_password_stdin, flags)?;
    Ok(FlagAnswerer::offline(password))
}

/// Read a password once from a `--*-file` path or stdin (never argv), trimming a
/// single trailing newline. `flags` is only used to name the source in errors.
fn read_secret(
    file: Option<&Path>,
    stdin: bool,
    flags: (&'static str, &'static str),
) -> Result<Option<Zeroizing<String>>> {
    if stdin {
        use std::io::Read;
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .with_context(|| format!("reading {}", flags.1))?;
        Ok(Some(Zeroizing::new(s.trim_end_matches(['\n', '\r']).to_string())))
    } else if let Some(p) = file {
        let s = std::fs::read_to_string(p)
            .with_context(|| format!("reading {} {}", flags.0, p.display()))?;
        Ok(Some(Zeroizing::new(s.trim_end_matches(['\n', '\r']).to_string())))
    } else {
        Ok(None)
    }
}

/// The shared clap flags every remote-admin query/action carries: which admin
/// server, the authorizing admin + password, and the out-of-band CA glyph.
#[derive(clap::Args, Debug)]
pub(crate) struct RemoteAuthFlags {
    /// The admin server to run this against (`ip:port`, or a host resolved with
    /// the default admin port). Defaults to this host's own admin server.
    #[arg(long = "server")]
    pub server: Option<String>,
    /// The CA role-admin name authorizing this operation.
    #[arg(long = "admin")]
    pub admin: Option<String>,
    /// Read the admin password from a file (never on the command line).
    #[arg(long = "password-file")]
    pub password_file: Option<PathBuf>,
    /// Read the admin password from stdin.
    #[arg(long = "password-stdin", conflicts_with = "password_file")]
    pub password_stdin: bool,
    /// The admin server's CA fingerprint, obtained out of band (view it with
    /// `netidx admin ca fingerprint <ip:port>`). Required off the CA host;
    /// auto-verified against the local CA cert on it.
    #[arg(long = "accept-glyph")]
    pub accept_glyph: Option<String>,
    /// Override the CA directory used to auto-verify the server's identity.
    #[arg(long = "ca-dir")]
    pub ca_dir: Option<PathBuf>,
}

impl RemoteAuthFlags {
    /// The strict answerer these flags drive.
    pub(crate) fn answerer(&self) -> Result<FlagAnswerer> {
        make_flag_answerer(
            self.password_file.as_deref(),
            self.password_stdin,
            self.accept_glyph.as_deref(),
        )
    }

    /// The explicit admin server, resolved from `--server` (host → `ip:port`
    /// with the default admin port), or `None` for this host's own.
    pub(crate) fn server_addr(&self) -> Result<Option<SocketAddr>> {
        self.server.as_deref().map(super::init::resolve_admin_server_addr).transpose()
    }

    /// Read the explicitly supplied password after the caller has verified its
    /// controller target. Keeping this separate from [`Self::answerer`] lets
    /// `admin login` honor the no-credentials-before-controller rule even for
    /// password files and stdin.
    pub(crate) fn password(&self) -> Result<Option<Zeroizing<String>>> {
        read_secret(
            self.password_file.as_deref(),
            self.password_stdin,
            ("--password-file", "--password-stdin"),
        )
    }
}

/// One purpose's secret: its value (read once from a `--*-file` / `--*-stdin`
/// pair, never argv) plus the flag names a missing-secret error should cite.
/// Distinct slots keep an install's key / admin / recovery passwords from
/// silently collapsing to one shared secret (an install that creates a CA reads
/// both a founding-admin password AND, under `--key-protection password`, a leaf
/// key password — from the same file, before the split).
struct SecretSlot {
    value: Option<Zeroizing<String>>,
    flags: (&'static str, &'static str),
}

impl SecretSlot {
    /// A slot with no supplied value; a `secret()` call on it errors, naming
    /// `flags`. For a purpose this command doesn't accept a secret for.
    fn none(flags: (&'static str, &'static str)) -> Self {
        SecretSlot { value: None, flags }
    }

    /// Read a slot's value from its file/stdin flags (never argv).
    fn read(
        file: Option<&Path>,
        stdin: bool,
        flags: (&'static str, &'static str),
    ) -> Result<Self> {
        Ok(SecretSlot { value: read_secret(file, stdin, flags)?, flags })
    }

    /// The value for a `secret()` call: an inline `provided` wins; else the slot
    /// value; else an error naming the slot's flags (never prompts).
    fn resolve(&self, field: Field, provided: Option<Secret>) -> Result<Secret> {
        if let Some(s) = provided {
            return Ok(s);
        }
        match &self.value {
            Some(pw) => Ok(Secret(pw.to_string())),
            None => bail!(
                "{} is required — supply it with {} <path> or {} (never on the \
                 command line)",
                field.flag(),
                self.flags.0,
                self.flags.1,
            ),
        }
    }
}

// The distinct install secret flags — a password-protected leaf key, the
// founding superuser, and a CA recovery unlock each get their own file.
const KEY_FLAGS: (&str, &str) = ("--key-password-file", "--key-password-stdin");
const ADMIN_FLAGS: (&str, &str) = ("--admin-password-file", "--admin-password-stdin");
const RECOVERY_FLAGS: (&str, &str) =
    ("--recovery-password-file", "--recovery-password-stdin");
// The single-secret flag for commands that take exactly one password (remote
// admin, `ca init`, `component tls join`) — no collapse is possible with one.
const PASSWORD_FLAGS: (&str, &str) = ("--password-file", "--password-stdin");

/// The strict-CLI answerer: three purpose-scoped secret slots plus the CA glyph
/// the operator obtained out of band (a presented identity must match it — there
/// is no interactive glyph confirm here).
pub(crate) struct FlagAnswerer {
    key: SecretSlot,
    admin: SecretSlot,
    recovery: SecretSlot,
    accept_glyph: Option<Fingerprint>,
}

impl FlagAnswerer {
    /// One password answers any `secret()` this command makes (cited as
    /// `--password-file`). For commands with a single distinct secret — remote
    /// admin ops, `ca init` (the founding superuser), `component tls join` —
    /// where no collapse is possible because only one secret is ever needed.
    pub(crate) fn single(
        password: Option<Zeroizing<String>>,
        accept_glyph: Option<Fingerprint>,
    ) -> Self {
        FlagAnswerer {
            key: SecretSlot { value: password.clone(), flags: PASSWORD_FLAGS },
            admin: SecretSlot { value: password.clone(), flags: PASSWORD_FLAGS },
            recovery: SecretSlot { value: password, flags: PASSWORD_FLAGS },
            accept_glyph,
        }
    }

    /// The offline CA-vault answerer: the single secret is the recovery password
    /// (cited as `--recovery-password-file`), which also unlocks a legacy
    /// encrypted key.
    pub(crate) fn offline(recovery: Option<Zeroizing<String>>) -> Self {
        FlagAnswerer {
            key: SecretSlot { value: recovery.clone(), flags: RECOVERY_FLAGS },
            admin: SecretSlot::none(ADMIN_FLAGS),
            recovery: SecretSlot { value: recovery, flags: RECOVERY_FLAGS },
            accept_glyph: None,
        }
    }

    /// A full install answerer: distinct key / admin / recovery secrets read
    /// from their own flags, so they can never collapse to one shared file.
    pub(crate) fn install(
        key_file: Option<&Path>,
        key_stdin: bool,
        admin_file: Option<&Path>,
        admin_stdin: bool,
        recovery_file: Option<&Path>,
        recovery_stdin: bool,
        accept_glyph: Option<Fingerprint>,
    ) -> Result<Self> {
        Ok(FlagAnswerer {
            key: SecretSlot::read(key_file, key_stdin, KEY_FLAGS)?,
            admin: SecretSlot::read(admin_file, admin_stdin, ADMIN_FLAGS)?,
            recovery: SecretSlot::read(recovery_file, recovery_stdin, RECOVERY_FLAGS)?,
            accept_glyph,
        })
    }
}

/// Parse an out-of-band `--accept-glyph` fingerprint (if present).
pub(crate) fn parse_glyph(accept_glyph: Option<&str>) -> Result<Option<Fingerprint>> {
    match accept_glyph {
        Some(s) => {
            Ok(Some(Fingerprint::parse_text(s).context("parsing --accept-glyph")?))
        }
        None => Ok(None),
    }
}

#[allow(dead_code)]
fn missing(field: Field) -> anyhow::Error {
    anyhow::anyhow!(
        "{} is required in non-interactive mode (run `netidx admin` with no \
         subcommand for the interactive setup)",
        field.flag()
    )
}

#[async_trait::async_trait]
impl Answerer for FlagAnswerer {
    fn interactive(&self) -> bool {
        false
    }

    fn has_explicit_secret(&self, field: Field) -> bool {
        match field {
            Field::KeyPassword => self.key.value.is_some(),
            Field::AdminPassword | Field::AdminPasswordConfirm => {
                self.admin.value.is_some()
            }
            Field::RecoveryPassword => self.recovery.value.is_some(),
            _ => false,
        }
    }

    async fn text(
        &mut self,
        field: Field,
        provided: Option<String>,
        default: Option<&str>,
        required: bool,
    ) -> Result<Option<String>> {
        match provided {
            Some(v) => Ok(Some(v)),
            // A required decision must be passed explicitly.
            None if required => Err(missing(field)),
            // A genuinely optional value takes its default (or none): "not
            // passing it" is itself a valid, reproducible choice.
            None => Ok(default.map(|s| s.to_string())),
        }
    }

    async fn choice(
        &mut self,
        field: Field,
        provided: Option<String>,
        choices: &[&str],
        _default: Option<&str>,
    ) -> Result<String> {
        match provided {
            Some(v) if choices.contains(&v.as_str()) => Ok(v),
            Some(v) => {
                bail!("{} must be one of {:?} (got {v:?})", field.flag(), choices)
            }
            None => Err(missing(field)),
        }
    }

    async fn confirm(
        &mut self,
        field: Field,
        provided: Option<bool>,
        _default: bool,
    ) -> Result<bool> {
        provided.ok_or_else(|| missing(field))
    }

    async fn select_trust_domain(
        &mut self,
        _networks: &[TrustDomainOption],
    ) -> Result<TrustDomainChoice> {
        // Discovery is interactive-only; the strict CLI takes an explicit
        // --admin-server instead and never reaches this.
        Err(missing(Field::SelectTrustDomain))
    }

    async fn secret(&mut self, field: Field, provided: Option<Secret>) -> Result<Secret> {
        let slot = match field {
            Field::KeyPassword => &self.key,
            Field::AdminPassword | Field::AdminPasswordConfirm => &self.admin,
            Field::RecoveryPassword => &self.recovery,
            other => {
                bail!("internal error: secret() requested for non-secret field {other:?}")
            }
        };
        slot.resolve(field, provided)
    }

    async fn announce(&mut self, _title: &str, _body: &str) -> Result<()> {
        // Section headers are an interactive nicety; scripted runs skip them.
        Ok(())
    }

    async fn announce_identity(&mut self, body: &str, code: &Fingerprint) -> Result<()> {
        // The glyph is out-of-band data, not just a header — print the body and
        // the code text so a scripted run still surfaces the CA identity.
        let _ = writeln!(std::io::stderr(), "{body}\nCA identity code: {}", code.text());
        Ok(())
    }

    async fn confirm_identity(&mut self, identity: &CaIdentity) -> Result<bool> {
        match &self.accept_glyph {
            Some(expected) => Ok(&identity.fingerprint == expected),
            None => bail!(
                "the admin server for network {:?} presented CA fingerprint:\n  \
                 {}\nnon-interactively you must confirm it out of band and pass \
                 --accept-glyph <fingerprint>",
                identity.domain,
                identity.fingerprint.text(),
            ),
        }
    }

    fn show_verification_code(&mut self, purpose: &str, code: &Fingerprint) {
        let _ = writeln!(std::io::stderr(), "{purpose} code: {}", code.text());
    }

    fn clear_verification_code(&mut self) {}

    fn progress(&mut self, p: Progress) {
        let _ = writeln!(std::io::stderr(), "… {}", p.message);
    }

    fn note(&mut self, m: &str) {
        let _ = writeln!(std::io::stderr(), "{m}");
    }

    fn warn(&mut self, m: &str) {
        let _ = writeln!(std::io::stderr(), "warning: {m}");
    }

    async fn show_recovery_password(&mut self, password: &str) -> Result<()> {
        // The one-time, never-stored CA break-glass secret. Boxed on stdout
        // (the operator must copy it) with the store-it-in-a-safe warning.
        let bar = "─".repeat(password.chars().count() + 2);
        let mut out = std::io::stdout();
        writeln!(out)?;
        writeln!(out, "┌{bar}┐")?;
        writeln!(out, "│ {password} │")?;
        writeln!(out, "└{bar}┘")?;
        writeln!(
            out,
            "This is the CA RECOVERY PASSWORD. Write it down and lock it in a safe.\n\
             It is shown ONCE and never stored. It is the only OFF-box credential\n\
             that can unlock the CA key — to mint a new admin or rotate the box's\n\
             own credential. If you lose it AND this machine, the CA is unrecoverable;\n\
             while the machine lives you can mint a fresh one with\n\
             `netidx admin ca recovery rotate`.\n"
        )?;
        out.flush().context("flushing the CA recovery password")?;
        Ok(())
    }
}
