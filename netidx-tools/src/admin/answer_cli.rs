//! The strict-CLI [`Answerer`]: every decision value comes from a flag or
//! errors naming the flag. Never prompts, never discovers. Secrets come from
//! `--password-file` / `--password-stdin` (never argv). The glyph confirm
//! compares the presented CA against a required `--accept-glyph` fingerprint
//! the operator obtained out of band.

use anyhow::{Result, bail};
use netidx_admin::{
    admin_client::CaIdentity,
    admin_proto::Secret,
    answer::{Answerer, Field, Progress},
    fingerprint::Fingerprint,
};
use std::io::Write;
use zeroize::Zeroizing;

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
}
