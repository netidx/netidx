//! The strict-CLI [`Answerer`]: every decision value comes from a flag or
//! errors naming the flag. Never prompts, never discovers. Secrets come from
//! `--password-file` / `--password-stdin` (never argv). The glyph confirm
//! compares the presented CA against a required `--accept-glyph` fingerprint
//! the operator obtained out of band.

use anyhow::{Context, Result, bail};
use netidx_admin::{
    admin_client::CaIdentity,
    admin_proto::Secret,
    answer::{Answerer, Field, Progress},
    fingerprint::Fingerprint,
};
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
    let password = if password_stdin {
        use std::io::Read;
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s).context("reading --password-stdin")?;
        Some(Zeroizing::new(s.trim_end_matches(['\n', '\r']).to_string()))
    } else if let Some(p) = password_file {
        let s = std::fs::read_to_string(p)
            .with_context(|| format!("reading --password-file {}", p.display()))?;
        Some(Zeroizing::new(s.trim_end_matches(['\n', '\r']).to_string()))
    } else {
        None
    };
    let accept_glyph = match accept_glyph {
        Some(s) => Some(Fingerprint::parse_text(s).context("parsing --accept-glyph")?),
        None => None,
    };
    Ok(FlagAnswerer::new(password, accept_glyph))
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
}

// Wired into the subcommand handlers as they convert to the Answerer.
#[allow(dead_code)]
pub(crate) struct FlagAnswerer {
    /// A password read once from `--password-file` / `--password-stdin`, handed
    /// to any `secret()` call that has no inline value.
    password: Option<Zeroizing<String>>,
    /// The CA fingerprint the operator obtained out of band; a presented
    /// identity must match it (there is no interactive glyph confirm here).
    accept_glyph: Option<Fingerprint>,
}

#[allow(dead_code)]
impl FlagAnswerer {
    pub(crate) fn new(
        password: Option<Zeroizing<String>>,
        accept_glyph: Option<Fingerprint>,
    ) -> Self {
        FlagAnswerer { password, accept_glyph }
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

    async fn secret(&mut self, field: Field, provided: Option<Secret>) -> Result<Secret> {
        if let Some(s) = provided {
            return Ok(s);
        }
        match &self.password {
            Some(pw) => Ok(Secret(pw.to_string())),
            None => bail!(
                "{} is required — supply it with --password-file <path> or \
                 --password-stdin (never on the command line)",
                field.flag()
            ),
        }
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

    fn progress(&mut self, p: Progress) {
        let _ = writeln!(std::io::stderr(), "… {}", p.message);
    }

    fn note(&mut self, m: &str) {
        let _ = writeln!(std::io::stderr(), "{m}");
    }

    fn warn(&mut self, m: &str) {
        let _ = writeln!(std::io::stderr(), "warning: {m}");
    }

    fn show_recovery_password(&mut self, password: &str) {
        // The one-time, never-stored CA break-glass secret. Boxed on stdout
        // (the operator must copy it) with the store-it-in-a-safe warning.
        let bar = "─".repeat(password.chars().count() + 2);
        let mut out = std::io::stdout();
        let _ = writeln!(out);
        let _ = writeln!(out, "┌{bar}┐");
        let _ = writeln!(out, "│ {password} │");
        let _ = writeln!(out, "└{bar}┘");
        let _ = writeln!(
            out,
            "This is the CA RECOVERY PASSWORD. Write it down and lock it in a safe.\n\
             It is shown ONCE and never stored. It is the only OFF-box credential\n\
             that can unlock the CA key — to mint a new admin or rotate the box's\n\
             own credential. If you lose it AND this machine, the CA is unrecoverable;\n\
             while the machine lives you can mint a fresh one with\n\
             `netidx admin ca recovery rotate`.\n"
        );
    }
}
