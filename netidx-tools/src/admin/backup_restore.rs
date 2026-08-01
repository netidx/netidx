//! `netidx admin backup` / `restore` — the CLI surface over
//! [`netidx_admin::plan::bundle`].
//!
//! The ceremony lives in the engine; this module turns flags into its inputs,
//! performs the one privileged step the engine hands back (registering the OS
//! service, which needs a terminal for the sudo prompt), and reports.

use super::{
    answer_cli,
    service::{self as service_cli, ScopeArg},
};
use anyhow::{Context, Result};
use clap::Args;
use netidx_admin::{
    install_bundle::BundleScope,
    plan::{
        bundle::{self, BackupInput, Next, RestoreInput},
        enroll::KeyProtArg,
    },
    service::ServiceScope,
};
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

#[derive(Args, Debug)]
pub(crate) struct BackupArgs {
    /// New backup bundle directory. Existing paths are never overwritten.
    pub target: PathBuf,
    /// Select the user or system install when both exist.
    #[arg(long)]
    pub scope: Option<ScopeArg>,
    /// Override the managed configuration root.
    #[arg(long = "config-dir")]
    pub config_dir: Option<PathBuf>,
    /// OS service name to record (default: netidx).
    #[arg(long, default_value = "netidx")]
    pub service_name: String,
    /// Account used by a system-scope service.
    #[arg(long)]
    pub for_user: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct RestoreArgs {
    /// Bundle created by `netidx admin backup`.
    pub bundle: PathBuf,
    /// Override the destination configuration root.
    #[arg(long = "config-dir")]
    pub config_dir: Option<PathBuf>,
    /// Override the restored admin-server listen address.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Override the advertised address of a resolver co-located with the
    /// restored CA.
    #[arg(long = "resolver-listen")]
    pub resolver_listen: Option<SocketAddr>,
    /// Override the local bind IP of a resolver co-located with the restored
    /// CA. When only --resolver-listen changes, its IP is the default.
    #[arg(long = "resolver-bind")]
    pub resolver_bind: Option<IpAddr>,
    /// Explicitly attest that the old CA cannot run. Required for a
    /// CA restore because two copies of the same CA identity
    /// would violate the admin domain's single-writer boundary.
    #[arg(long = "old-ca-fenced")]
    pub old_ca_fenced: bool,
    /// Read the CA recovery password from a file.
    #[arg(long = "recovery-password-file")]
    pub recovery_password_file: Option<PathBuf>,
    /// Read the CA recovery password from stdin.
    #[arg(long = "recovery-password-stdin", conflicts_with = "recovery_password_file")]
    pub recovery_password_stdin: bool,
    /// Fresh externally-signed intermediate CA certificate, required when the
    /// bundled CA certificate has expired.
    #[arg(long = "external-cert")]
    pub external_cert: Option<PathBuf>,
    /// External root certificate when it is not appended to --external-cert.
    #[arg(long = "external-root")]
    pub external_root: Option<PathBuf>,
    /// Protection for freshly enrolled non-ca TLS keys.
    #[arg(long = "key-protection")]
    pub key_protection: Option<KeyProtArg>,
    /// Password for `--key-protection password`.
    #[arg(long = "key-password-file")]
    pub key_password_file: Option<PathBuf>,
    /// Read the key password from stdin.
    #[arg(long = "key-password-stdin", conflicts_with = "key_password_file")]
    pub key_password_stdin: bool,
    /// Override the CA/bootstrap address recorded in the bundle.
    #[arg(long = "admin-server")]
    pub admin_server: Option<String>,
    /// Restore the recorded OS service even if the source install had none.
    #[arg(long = "with-service", conflicts_with = "no_service")]
    pub with_service: bool,
    /// Restore configs and credentials but do not register an OS service.
    #[arg(long = "no-service")]
    pub no_service: bool,
    /// Override the service name stored in the bundle.
    #[arg(long = "service-name")]
    pub service_name: Option<String>,
    /// Override the account used by a restored system service.
    #[arg(long = "for-user")]
    pub for_user: Option<String>,
    /// Permit plaintext replacement CA credentials when no TPM or
    /// Secure Enclave is usable. Test installations only.
    #[arg(long = "insecure-no-tpm")]
    pub insecure_no_tpm: bool,
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("starting tokio runtime")
}

impl RestoreArgs {
    fn input(&self) -> RestoreInput {
        RestoreInput {
            bundle: self.bundle.clone(),
            config_dir: self.config_dir.clone(),
            listen: self.listen,
            resolver_listen: self.resolver_listen,
            resolver_bind: self.resolver_bind,
            old_ca_fenced: self.old_ca_fenced,
            external_cert: self.external_cert.clone(),
            external_root: self.external_root.clone(),
            key_protection: self.key_protection,
            admin_server: self.admin_server.clone(),
            with_service: self.with_service,
            no_service: self.no_service,
            service_name: self.service_name.clone(),
            for_user: self.for_user.clone(),
            insecure_no_tpm: self.insecure_no_tpm,
        }
    }

    /// The answerer for a phase. `glyph` pins identity confirmation to the CA
    /// the bundle recorded; the CA-recovery phase has no identity to confirm.
    fn answerer(
        &self,
        glyph: Option<netidx_admin_proto::fingerprint::Fingerprint>,
    ) -> Result<answer_cli::FlagAnswerer> {
        answer_cli::FlagAnswerer::install(
            self.key_password_file.as_deref(),
            self.key_password_stdin,
            None,
            false,
            self.recovery_password_file.as_deref(),
            self.recovery_password_stdin,
            glyph,
        )
    }
}

pub(crate) fn backup(a: BackupArgs) -> Result<()> {
    let target = if a.target.is_absolute() {
        a.target.clone()
    } else {
        std::env::current_dir()?.join(&a.target)
    };
    // Backup asks nothing: no key, no password, no identity to confirm.
    let mut ans =
        answer_cli::FlagAnswerer::install(None, false, None, false, None, false, None)?;
    let out = runtime()?.block_on(bundle::backup(
        &mut ans,
        BackupInput {
            target,
            scope: a.scope.map(|s| match s {
                ScopeArg::User => BundleScope::User,
                ScopeArg::System => BundleScope::System,
            }),
            config_dir: a.config_dir,
            service_name: a.service_name,
            for_user: a.for_user,
        },
    ))?;
    println!("created {} backup at {}", out.role.as_str(), out.target.display());
    println!("  components: {:?}", out.components);
    println!("  files:      {} ({} bytes)", out.files, out.bytes);
    println!("  re-enroll:  {} machine credential(s)", out.identities_to_reenroll);
    println!("  manifest SHA-256: {}", out.manifest_sha256);
    Ok(())
}

/// Register the restored OS service. Privileged and terminal-owning, which is
/// why the engine hands it back rather than doing it.
fn install_service(
    manifest: &netidx_admin::install_bundle::Manifest,
    a: &RestoreArgs,
    scope: ServiceScope,
) -> Result<()> {
    let (name, for_user) =
        bundle::restored_service(manifest, a.service_name.clone(), a.for_user.clone());
    service_cli::install_restored(scope.into(), name, for_user)
}

pub(crate) fn restore(a: RestoreArgs) -> Result<()> {
    let rt = runtime()?;
    let input = a.input();
    let mut ans = a.answerer(None)?;
    let staged = rt.block_on(bundle::restore_stage(&mut ans, &input))?;
    if let Next::ServiceThenFinish(scope) = staged.next {
        install_service(&staged.manifest, &a, scope)?;
    }
    let glyph = staged
        .manifest
        .install
        .admin_domain
        .as_ref()
        .map(|n| {
            netidx_admin_proto::fingerprint::Fingerprint::parse_text(&n.ca_fingerprint)
        })
        .transpose()?;
    let manifest = staged.manifest.clone();
    let mut ans = a.answerer(glyph)?;
    let out = rt.block_on(bundle::restore_finish(&mut ans, staged))?;
    if let Some(scope) = out.service_needed {
        install_service(&manifest, &a, scope)?;
    }
    if let Some(operation_id) = out.reconciled {
        println!("  resolver hierarchy reconciled (operation {operation_id})");
    }
    println!("restore complete: {} is installed and ready", out.role.as_str());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Parser, Subcommand};

    #[derive(Debug, Parser)]
    struct Cli {
        #[command(subcommand)]
        command: Command,
    }

    #[derive(Debug, Subcommand)]
    enum Command {
        Backup(BackupArgs),
        Restore(RestoreArgs),
    }

    #[test]
    fn backup_and_restore_are_top_level_commands() {
        let parsed =
            Cli::try_parse_from(["admin", "backup", "/srv/netidx-backup"]).unwrap();
        assert!(matches!(parsed.command, Command::Backup(_)));
        let parsed = Cli::try_parse_from([
            "admin",
            "restore",
            "/srv/netidx-backup",
            "--old-ca-fenced",
            "--recovery-password-stdin",
            "--listen",
            "10.1.0.4:5565",
            "--resolver-listen",
            "203.0.113.4:5564",
            "--resolver-bind",
            "10.1.0.4",
        ])
        .unwrap();
        let Command::Restore(args) = parsed.command else { panic!("restore") };
        assert!(args.old_ca_fenced);
        assert!(args.recovery_password_stdin);
        assert_eq!(args.listen, Some("10.1.0.4:5565".parse().unwrap()));
        assert_eq!(args.resolver_listen, Some("203.0.113.4:5564".parse().unwrap()));
        assert_eq!(args.resolver_bind, Some("10.1.0.4".parse().unwrap()));
    }
}
