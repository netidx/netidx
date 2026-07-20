//! The workstation role install: a local resolver + matching client, with an
//! optional parent referral up to a network-wide resolver.
//!
//! A workstation never mints a CA; when it joins a network it enrolls a client
//! cert over the admin plane. `run_workstation_join` graduates an existing
//! local-only workstation onto a network without a reinstall.

use super::{
    InstallCommon, finish_with, install_renew_unit, network_provenance,
    prompt_ip_or_addr, prompt_resolver_tls_name, resolve_netidx_binary,
    resolve_units_dir, suggest_client_san,
};
use crate::{
    admin_proto::NodeKind,
    answer::{Answerer, Field},
    paths,
    plan::{
        AuthKind,
        enroll::{self, AdminServers, KeyProtArg, StagedIdentity},
        service::ServiceNeed,
    },
    provenance::{InstallRecord, InstallRole, NetworkIdentity},
    service::ServiceScope,
    template::{self, ParentRef, ReferralAuth, TlsIdentitySpec},
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use std::{net::SocketAddr, path::PathBuf};

/// Typed inputs for [`run_workstation`]. Frontends begin with
/// [`WorkstationInput::defaults`] and apply only their explicit overrides; the
/// planner resolves environment-derived values such as the Local-auth owner.
#[cfg(any(unix, windows))]
pub struct WorkstationInput {
    /// A parent referral fully built from `--parent-*` flags. `Some` means the
    /// operator specified a parent explicitly (skip discovery); `None` means
    /// run the discovery / prompt cascade.
    pub explicit_parent: Option<ParentRef>,
    /// Enroll against this admin server (`--admin-server`) instead of mDNS
    /// discovery — the non-interactive join path. On a TLS network this
    /// enrolls a client certificate; the presented identity is confirmed via
    /// `--accept-glyph`. Ignored when `explicit_parent` is set.
    pub admin_server: Option<SocketAddr>,
    /// `default_auth` on the client config.
    pub default_auth: Option<AuthKind>,
    /// Namespace base path (`/local` by convention).
    pub base: String,
    /// Local resolver listen port.
    pub listen_port: Option<u16>,
    /// Local-auth unix socket path.
    pub local_socket: Option<PathBuf>,
    /// Client config output path.
    pub client_config_path: Option<PathBuf>,
    /// Resolver config output path.
    pub resolver_config_path: Option<PathBuf>,
    /// Activation units dir (`None` ⇒ default; `no_units` in `common` wins).
    pub units_dir: Option<PathBuf>,
    /// The binary the activation units run (`None` ⇒ current exe).
    pub netidx_binary: Option<PathBuf>,
    /// Private-key protection choice.
    pub key_protection: Option<KeyProtArg>,
    /// Emit the default `container` activation unit.
    pub with_container: bool,
    /// Explicit perms-file owner. When permissions are enabled, `None` resolves
    /// to the current Local-auth identity in the shared planner.
    pub owner: Option<ArcStr>,
    /// Whether to write the auto-seeded perms file.
    pub with_perms_file: bool,
    /// Perms file output path.
    pub perms_path: Option<PathBuf>,
    /// Install-wide flags.
    pub common: InstallCommon,
}

impl WorkstationInput {
    /// Safe frontend-independent defaults for a guided workstation install.
    /// Expert frontends apply explicit overrides after constructing this value.
    pub fn defaults(common: InstallCommon) -> Self {
        Self {
            explicit_parent: None,
            admin_server: None,
            default_auth: None,
            base: "/local".to_string(),
            listen_port: None,
            local_socket: None,
            client_config_path: None,
            resolver_config_path: None,
            units_dir: None,
            netidx_binary: None,
            key_protection: None,
            with_container: true,
            owner: None,
            with_perms_file: true,
            perms_path: None,
            common,
        }
    }

    fn resolve_owner(mut self) -> Result<Self> {
        if self.with_perms_file && self.owner.is_none() {
            self.owner = Some(crate::local_identity::current_user().context(
                "resolving the workstation permissions owner. Supply an explicit \
                 owner, or explicitly disable permissions generation",
            )?);
        }
        Ok(self)
    }
}

/// Typed inputs for [`run_workstation_join`].
pub struct WorkstationJoinInput {
    /// Show the join plan without writing anything.
    pub dry_run: bool,
    /// Private-key protection for an enrolled client cert (TLS networks).
    pub key_protection: Option<KeyProtArg>,
    /// The network's admin server, named explicitly (`--admin-server`) —
    /// selects + glyph-confirms the network directly, so `join` works under the
    /// strict answerer (which disables mDNS discovery). `None` ⇒ discover.
    pub admin_server: Option<SocketAddr>,
}

/// Install a workstation (local resolver + client), returning the OS-service
/// scope to register (a user-scope service — a workstation runs in the
/// operator's session).
#[cfg(any(unix, windows))]
pub async fn run_workstation(
    ans: &mut dyn Answerer,
    input: WorkstationInput,
) -> Result<Option<ServiceScope>> {
    // Resolve before discovery or enrollment, so an unnameable Local-auth user
    // fails before any network or disk changes.
    let input = input.resolve_owner()?;
    let WorkstationInput {
        explicit_parent,
        admin_server,
        default_auth,
        base,
        listen_port,
        local_socket,
        client_config_path,
        resolver_config_path,
        units_dir,
        netidx_binary,
        key_protection,
        with_container,
        owner,
        with_perms_file,
        perms_path,
        common,
    } = input;

    let mut tls_identities = vec![];
    // Staging tempdirs for any admin-server-joined identity must outlive the
    // `finish_with` (apply) call.
    let mut tls_staging: Vec<tempfile::TempDir> = Vec::new();
    // Provenance for the install record: set when we join a discovered network.
    let mut net_prov: (Option<NetworkIdentity>, Option<SocketAddr>) = (None, None);
    // `--parent-path` defaults to the workstation's own base.
    let parent = match explicit_parent {
        Some(p) => Some(p),
        None => {
            // Ask the network before asking the human: a discovered
            // (glyph-confirmed) admin server answers everything the prompt
            // cascade would have. `--admin-server` names it explicitly (the
            // non-interactive path, where mDNS discovery is disabled).
            let probe = match admin_server {
                Some(addr) => {
                    enroll::confirm_network_at(ans, addr, NodeKind::Workstation).await?
                }
                None => enroll::discover_network(ans, NodeKind::Workstation).await?,
            };
            net_prov = network_provenance(&probe);
            match probe.have() {
                Some(net) => {
                    let have_identity = !tls_identities.is_empty();
                    let addrs = enroll::network_addrs_and_identity(
                        ans,
                        net,
                        NodeKind::Workstation,
                        have_identity,
                        common.dry_run,
                        key_protection,
                        &mut tls_identities,
                        &mut tls_staging,
                    )
                    .await?;
                    Some(ParentRef {
                        path: ArcStr::from(base.as_str()),
                        ttl: None,
                        addrs,
                    })
                }
                None => {
                    match prompt_parent_referral(
                        ans,
                        &base,
                        key_protection,
                        &probe,
                        common.dry_run,
                    )
                    .await?
                    {
                        None => None,
                        Some((parent_ref, maybe_ident)) => {
                            if let Some(si) = maybe_ident {
                                tls_identities.push(si.spec);
                                tls_staging.extend(si.staging);
                            }
                            Some(parent_ref)
                        }
                    }
                }
            }
        }
    };
    let units_dir = resolve_units_dir(common.no_units, units_dir.as_deref())?;
    let has_tls = !tls_identities.is_empty();
    let post_apply_units_dir = units_dir.clone();
    let params = template::workstation::WorkstationParams {
        parent,
        tls_identities,
        default_auth: default_auth.map(|k| k.default_mech()),
        base: ArcStr::from(base.as_str()),
        listen_port,
        local_socket,
        client_config_path,
        resolver_config_path,
        owner,
        perms_seed: None,
        with_perms_file,
        perms_path,
        units_dir,
        netidx_binary: resolve_netidx_binary(netidx_binary)?,
        with_container,
    };
    let rt = template::workstation(&params)?;
    let (network, admin_server) = net_prov;
    // The workstation's own resolver is local-auth; the network it refers up
    // to (if any) carries its auth inside the parent referral.
    let record = InstallRecord::new(
        InstallRole::Workstation,
        base,
        "local",
        network,
        admin_server,
    );
    finish_with(
        ans,
        rt,
        &common,
        // A workstation runs in the operator's session → user-scope service.
        ServiceNeed::at(ServiceScope::User),
        record,
        // TLS identities expire: install the renewal daemon alongside.
        async move |ans| match (&post_apply_units_dir, has_tls) {
            (Some(d), true) => install_renew_unit(ans, d),
            _ => Ok(()),
        },
    )
    .await
}

/// Graduate a local-only workstation onto a network: discover + glyph-confirm
/// it, enroll a client cert if it's TLS, attach the local resolver via a
/// parent referral, and record the joined (pinned) network — without a
/// reinstall.
pub async fn run_workstation_join(
    ans: &mut dyn Answerer,
    input: WorkstationJoinInput,
) -> Result<()> {
    let mut rec = InstallRecord::load_default_async().await?.context(
        "no install record found — `workstation join` operates on an existing \
         workstation install",
    )?;
    if rec.role != InstallRole::Workstation {
        bail!(
            "this host is a {} install, not a workstation — `join` is a \
             workstation operation",
            rec.role.as_str(),
        );
    }
    if let Some(net) = &rec.network {
        bail!(
            "this workstation has already joined network {:?}. Re-joining a \
             different network isn't supported yet (uninstall + reinstall to \
             switch).",
            net.domain,
        );
    }
    let rpath = paths::discover_resolver_config()
        .context("no resolver config found — is this a workstation install?")?;
    let cpath = paths::discover_client_config()
        .context("no client config found — is this a workstation install?")?;
    // An explicit `--admin-server` names the network directly (and glyph-confirms
    // it via `--accept-glyph`); otherwise discover it (interactive only — the
    // strict answerer disables discovery, so strict `join` needs `--admin-server`).
    let probe = match input.admin_server {
        Some(addr) => {
            enroll::confirm_network_at(ans, addr, NodeKind::Workstation).await?
        }
        None => enroll::discover_network(ans, NodeKind::Workstation).await?,
    };
    let net = probe.have().context(
        "no network was selected to join — pass --admin-server <addr> (with \
         --accept-glyph) to name it explicitly, or run interactively to discover it",
    )?;
    let mut tls_identities: Vec<TlsIdentitySpec> = Vec::new();
    let mut tls_staging: Vec<tempfile::TempDir> = Vec::new();
    let addrs = enroll::network_addrs_and_identity(
        ans,
        net,
        NodeKind::Workstation,
        false,
        input.dry_run,
        input.key_protection,
        &mut tls_identities,
        &mut tls_staging,
    )
    .await?;
    let parent = ParentRef { path: ArcStr::from(rec.base.as_str()), ttl: None, addrs };
    // Capture the confirmed identity for the marker before applying.
    let network =
        NetworkIdentity::new(net.identity.domain.clone(), &net.identity.fingerprint);
    let admin_server = net.info.reached.first().copied();
    let rt = template::attach_to_network(&rpath, &cpath, parent, tls_identities)?;
    ans.note(&rt.describe());
    if input.dry_run {
        return Ok(());
    }
    rt.apply().context("applying the join")?;
    ans.note("ok");
    rec.network = Some(network);
    rec.admin_server = admin_server;
    rec.save_default_async().await.context("updating the install record")?;
    ans.note(&format_compact!(
        "joined network {:?} — restart the local resolver to use it",
        rec.network.as_ref().expect("just set").domain,
    ));
    Ok(())
}

/// Interactive cascade for the workstation's optional parent referral.
/// `Ok(None)` when the operator leaves the address blank; otherwise
/// `Ok(Some((ref, identity)))`, the optional identity being the TLS cert to
/// add when parent auth is TLS (enrolled over the admin plane, or a hard bail
/// if no admin server is reachable).
#[cfg(any(unix, windows))]
async fn prompt_parent_referral(
    ans: &mut dyn Answerer,
    default_path: &str,
    kp: Option<KeyProtArg>,
    probe: &AdminServers,
    dry_run: bool,
) -> Result<Option<(ParentRef, Option<StagedIdentity>)>> {
    let addr = match prompt_ip_or_addr(ans, Field::ParentAddr, None, false).await? {
        Some(addr) => addr,
        // Blank ⇒ no parent (a standalone workstation).
        None => return Ok(None),
    };
    let kind: AuthKind = ans
        .choice(
            Field::ParentAuth,
            None,
            &["anonymous", "local", "krb5", "tls"],
            Some("tls"),
        )
        .await?
        .parse()?;
    let (auth, identity) = match kind {
        AuthKind::Anonymous => (ReferralAuth::Anonymous, None),
        AuthKind::Local => {
            let socket = ans
                .text(Field::ParentSocket, None, None, true)
                .await?
                .context("a parent local-auth socket path is required")?;
            (ReferralAuth::Local(ArcStr::from(socket.as_str())), None)
        }
        AuthKind::Krb5 => {
            let spn = ans
                .text(Field::ParentSpn, None, None, true)
                .await?
                .context("the parent resolver's Kerberos SPN is required")?;
            (ReferralAuth::Krb5(ArcStr::from(spn.as_str())), None)
        }
        AuthKind::Tls => {
            let server_name =
                prompt_resolver_tls_name(ans, Some(addr), Field::ParentTlsName, None)
                    .await?;
            // identity is required for TLS — enrolled over the admin plane (or
            // a hard bail if no admin server is reachable). Suggest our SAN as
            // `<user>.<domain>`.
            let suggested = suggest_client_san(&server_name);
            let staged = enroll::prompt_tls_client_identity(
                ans,
                suggested.as_deref(),
                kp,
                probe,
                dry_run,
            )
            .await?;
            (ReferralAuth::Tls(ArcStr::from(server_name.as_str())), Some(staged))
        }
    };
    Ok(Some((
        ParentRef {
            path: ArcStr::from(default_path),
            ttl: None,
            addrs: vec![(addr, auth)],
        },
        identity,
    )))
}

#[cfg(test)]
mod input_tests {
    use super::*;

    fn common() -> InstallCommon {
        InstallCommon {
            dry_run: true,
            force: false,
            no_units: false,
            with_service: false,
            no_service: false,
        }
    }

    #[test]
    fn defaults_enable_a_usable_workstation() {
        let input = WorkstationInput::defaults(common());
        assert_eq!(input.base, "/local");
        assert!(input.with_container);
        assert!(input.with_perms_file);
        assert!(input.owner.is_none(), "owner is resolved after overrides");
    }

    #[test]
    fn enabled_permissions_resolve_the_exact_local_identity() {
        let input = WorkstationInput::defaults(common()).resolve_owner().unwrap();
        assert_eq!(input.owner.unwrap(), crate::local_identity::current_user().unwrap());
    }

    #[test]
    fn explicit_permission_opt_out_does_not_resolve_an_owner() {
        let mut input = WorkstationInput::defaults(common());
        input.with_perms_file = false;
        assert!(input.resolve_owner().unwrap().owner.is_none());
    }

    #[test]
    fn explicit_owner_is_preserved() {
        let mut input = WorkstationInput::defaults(common());
        input.owner = Some(ArcStr::from("chosen-user"));
        assert_eq!(input.resolve_owner().unwrap().owner.unwrap(), "chosen-user");
    }
}
