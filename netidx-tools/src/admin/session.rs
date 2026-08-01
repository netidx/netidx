use super::answer_cli::{FlagAnswerer, RemoteAuthFlags, parse_glyph};
use anyhow::{Context, Result};
use clap::Args;
use netidx_admin::{
    ops::{self, AdminSession, LogoutOutcome, LogoutSelection, Retention},
    plan::enroll::current_username,
};
use netidx_admin_proto::{AdminCredential, Secret, fingerprint::Fingerprint};
use zeroize::Zeroizing;

pub(crate) fn login(flags: RemoteAuthFlags) -> Result<()> {
    let mut ans = FlagAnswerer::single(None, parse_glyph(flags.accept_glyph.as_deref())?);
    let runtime = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    let (server, identity) = runtime.block_on(ops::resolve_ca(
        &mut ans,
        flags.server_addr()?,
        flags.ca_dir.as_deref(),
    ))?;
    // Only now that the CA is confirmed: a rejected glyph must not have cost the
    // operator a typed password.
    let admin = flags
        .admin
        .clone()
        .or_else(current_username)
        .context("an administrator name is required (pass --admin)")?;
    let password = match flags.password()? {
        Some(password) => password,
        None => Zeroizing::new(rpassword::prompt_password("Administrator password: ")?),
    };
    let session = AdminSession {
        server,
        identity,
        admin: admin.clone(),
        credential: AdminCredential::Password {
            admin,
            password: Secret(password.to_string()),
        },
    };
    // A one-shot command has nowhere to keep a token it cannot seal, so it
    // refuses rather than silently falling back to password auth next time.
    let logged = runtime
        .block_on(ops::cache_session(&session, Retention::Sealed))?
        .context("a password login always mints a token")?;
    println!("logged in as {}", logged.admin);
    println!("admin domain: {}", logged.ca_fingerprint);
    println!("expires: @{}", logged.absolute_deadline_unix);
    Ok(())
}

#[derive(Args, Debug)]
pub(crate) struct LogoutArgs {
    /// Revoke and remove every cached admin domain session.
    #[arg(long)]
    all: bool,
    /// Select the admin domain by its CA glyph when more than one is cached.
    #[arg(long = "accept-glyph")]
    accept_glyph: Option<String>,
}

pub(crate) fn logout(args: LogoutArgs) -> Result<()> {
    let select = match (args.all, args.accept_glyph.as_deref()) {
        (true, _) => LogoutSelection::All,
        (false, Some(glyph)) => LogoutSelection::Ca(Fingerprint::parse_text(glyph)?),
        (false, None) => LogoutSelection::TheOnlyOne,
    };
    let runtime = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    for out in runtime.block_on(ops::logout(select))? {
        match out.outcome {
            LogoutOutcome::Revoked => {}
            LogoutOutcome::NotRevoked(e) => eprintln!(
                "warning: could not revoke {} remotely: {e}",
                out.ca_fingerprint
            ),
            LogoutOutcome::NotCached => {
                println!("no session was cached for {}", out.ca_fingerprint);
                continue;
            }
        }
        println!("logged out {}", out.ca_fingerprint);
    }
    Ok(())
}
