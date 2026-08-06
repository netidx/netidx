//! Resolver-server template. A single network-facing resolver,
//! with the operator's chosen auth scheme on the one member,
//! optional parent referral, optional perms seed, and a single
//! activation unit that runs `netidx resolver-server`.
//!
//! For v1 the TLS path here uses the resolver's default `id_map_type:
//! Command` (`/bin/id`). The id-map daemon and its socket-mode
//! wiring are design/netidx-admin-future.md.

use super::*;
use crate::{
    client::ClientConfig, id_map as id_map_engine, paths, resolver::ResolverConfig,
    tls as tlsmod,
};
use anyhow::Result;
use netidx::resolver_server::config::{ReadGate, file::IdMapType};
use std::{net::SocketAddr, path::PathBuf};

/// How the resolver maps an authenticated identity (a TLS cert SAN, or
/// a kerberos principal with realm) to the group set permission checks are
/// keyed on. Only meaningful for [`AuthChoice::Tls`] / [`AuthChoice::Krb5`]
/// — anonymous has no user, and local goes through `Mapper::user(uid)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdMapMode {
    /// Run the netidx id-mapper daemon and look identities up in the
    /// id-map config (`id_map_type: Socket`). The only coherent choice
    /// for TLS (cert SANs have no `/bin/id` translation), and the choice
    /// for a krb5 site with no system IdM that wants real uid/gid perms.
    Netidx,
    /// Map through the platform's `id` / nsswitch (`id_map_type: Command`
    /// with no command ⇒ `PlatformDefault`). Correct for a krb5 site
    /// whose system IdM (FreeIPA, AD, sssd) resolves full principals like
    /// `user@REALM`. Fails for TLS SANs and for krb5 without such an IdM.
    Platform,
    /// Don't map at all (`id_map_type: DoNotMap`): permissions are keyed
    /// on the raw identity string (the full krb5 principal, or the cert
    /// SAN). No daemon, no system IdM — the simplest working choice for a
    /// small krb5 setup.
    None,
}

/// Parameters for [`resolver`].
#[derive(Debug, Clone)]
pub struct ResolverParams {
    /// Auth this resolver will expose to its clients. TLS expects
    /// cert/key/trusted paths via [`AuthChoice::Tls`]; the template
    /// emits a [`TlsCopyJob`] that copies them to the canonical
    /// location before the resolver config references them.
    pub auth: AuthChoice,
    /// Resolver cluster base path (default `/`).
    pub base: ArcStr,
    /// `(addr, bind_addr)` for the single member server. If the second
    /// element is `None`, defaults to the address part of `addr`.
    pub listen: SocketAddr,
    pub bind: Option<std::net::IpAddr>,
    pub parent: Option<ParentRef>,
    /// Whether this member starts out refusing read clients. Set when joining
    /// a cluster that is already serving; see
    /// [`netidx::resolver_server::config::ReadGate`].
    pub read_gated: ReadGate,
    /// Initial perms map. The engine emits this as a separate file
    /// (referenced from the resolver config's `include_permissions`)
    /// and **leaves the resolver config's inline `perms` empty** —
    /// that's the layout the SIGHUP / file-watch reload path is built
    /// around.
    ///
    /// `None` does **not** mean "no perms file". When the field is
    /// `None` AND [`Self::with_perms_file`] is `true` (the default),
    /// the template auto-seeds via
    /// [`crate::perms::default_seed`] — full rights for the
    /// authenticated user under `/users/$[user]`, and read+write for the
    /// `users` group at the base wherever group names resolve at all
    /// ([`IdMapMode::None`] has no groups, so that entry is omitted and a
    /// warning says so). That gives a fresh install a working "per-user
    /// playground" without forcing the operator to hand-author a perms map
    /// up front.
    ///
    /// To skip emitting a perms file entirely, set `with_perms_file`
    /// to `false`.
    pub perms_seed: Option<PMap>,
    /// Set to `false` to skip emitting a perms file entirely (and
    /// drop the associated `include_permissions` reference from the
    /// resolver config). Default `true`. Useful for advanced layouts
    /// where perms come from somewhere else.
    pub with_perms_file: bool,
    pub perms_path: Option<PathBuf>,
    /// Output paths. `None` ⇒ standard user-config locations.
    pub resolver_config_path: Option<PathBuf>,
    /// Where to drop activation units. `None` ⇒ skip units entirely.
    /// The CLI fills in [`paths::user_activation_dir`] when the operator
    /// did not pass `--no-units`; this engine layer does not invent a
    /// default so "skip" stays distinct from "use the canonical dir".
    pub units_dir: Option<PathBuf>,
    /// Absolute path to the `netidx` binary the activation unit
    /// will exec. **Must be absolute** — `netidx-activation` does
    /// not search `$PATH`. Template-render errors out if it isn't.
    /// The CLI fills this from `std::env::current_exe()` when the
    /// operator doesn't pass `--netidx-binary`.
    pub netidx_binary: PathBuf,
    /// How to map authenticated identities to groups (see
    /// [`IdMapMode`]). [`IdMapMode::Netidx`] installs the id-mapper
    /// daemon — the resolver member gets `id_map_type: Socket`, an
    /// `id-map.unit` is emitted alongside `resolver.unit`, and a starter
    /// id-map JSON is written if absent. [`IdMapMode::Platform`] uses the
    /// member's default `id_map_type: Command` (`/bin/id`).
    /// [`IdMapMode::None`] sets `id_map_type: DoNotMap`. The latter two
    /// emit no daemon.
    ///
    /// Note for Krb5: the resolver passes the **full principal with
    /// realm** (e.g. `eric@RYU-OH.ORG`) as the lookup key, so id-map
    /// entries (Netidx) — or perms entries (None) — must use that exact
    /// form.
    pub id_map: IdMapMode,
    /// Where to put the id-map JSON. `None` ⇒
    /// `crate::id_map::user_id_map_path()`.
    pub id_map_path: Option<PathBuf>,
    /// Where the id-map daemon will bind its unix socket. `None` ⇒
    /// `crate::id_map::user_id_map_socket()`.
    pub id_map_socket: Option<PathBuf>,
    /// Also emit a client config pointing at this resolver — useful
    /// for inspecting the resolver from the same host. For
    /// `AuthChoice::Tls` the client reuses the resolver's installed
    /// identity (same cert), so commands like `netidx resolver list`
    /// work out of the box from the resolver host. The operator can
    /// override later; this just removes one manual step.
    pub with_local_client: bool,
    /// Where to write the client config. `None` ⇒
    /// `paths::user_client_config()`. Ignored when
    /// `with_local_client` is `false`.
    pub client_config_path: Option<PathBuf>,
    /// Override the local client's `default_bind_config` (a `BindCfg`
    /// string — `<ip>/<prefix>`, `<advertise>@<subnet>/<prefix>`, or
    /// `local`). The template uses this verbatim when set.
    ///
    /// Needed for the cloud-elastic case where `listen` is the public
    /// IP and `bind` is the private NIC IP: the template can't derive
    /// the Elastic form (`<public>@<private-subnet>/<prefix>`) from
    /// `listen`/`bind` alone because it lacks the private-subnet
    /// netmask. The CLI carries that via `NetShape`, computes the
    /// right `BindCfg`, and passes it through here.
    ///
    /// When `None`, the template falls back to `<bind-ip>/32` (or
    /// `<bind-ip>/128`) of `bind.unwrap_or(listen.ip())`. Skipped
    /// entirely for loopback addresses since the publisher default
    /// (`BindCfg::Local`) already matches. Ignored when
    /// `with_local_client` is `false`.
    pub local_client_bind: Option<String>,
}

pub fn resolver(p: &ResolverParams) -> Result<RenderedTemplate> {
    let resolver_cfg_path = match &p.resolver_config_path {
        Some(p) => p.clone(),
        None => paths::user_resolver_config()?,
    };
    // `None` means "skip units entirely". The CLI resolves the default
    // activation dir before calling in when the operator didn't pass
    // `--no-units`.
    let units_dir = p.units_dir.clone();
    let netidx_binary = p.netidx_binary.clone();
    if !netidx_binary.is_absolute() {
        bail!(
            "netidx_binary must be an absolute path (got {:?}); netidx-activation \
             does not search $PATH",
            netidx_binary,
        );
    }
    let bind_addr = p.bind.unwrap_or(p.listen.ip());

    let config_dir = resolver_cfg_path
        .parent()
        .context("resolver config path has no parent directory")?;
    let tls_job = resolver_tls_copy_job(&p.auth, config_dir)?;
    let tls_dest = tls_job
        .as_ref()
        .map(|j| j.dest_dir.clone())
        .unwrap_or_else(|| PathBuf::from("/dev/null"));

    // -- Id-map wiring (TLS / Krb5) -----------------------------------------
    // TLS and Krb5 both hand the resolver an opaque name string
    // (cert SAN, or kerberos principal with realm) that has to be
    // turned into a unix uid/gid set. Point those at the local
    // id-mapper daemon over a unix socket. Anonymous has no user;
    // Local goes through `Mapper::user(uid)` against `/bin/id` and
    // doesn't benefit from the daemon.
    let id_map_active = matches!(p.id_map, IdMapMode::Netidx)
        && matches!(p.auth, AuthChoice::Tls { .. } | AuthChoice::Krb5 { .. });
    let mut warnings = Vec::new();
    // A TLS resolver that maps through `/bin/id` (Platform) resolves no
    // SAN-shaped name — every identity maps to nobody and perms deny all
    // non-anonymous operations. `DoNotMap` (None) keys perms on the SAN
    // directly and is coherent; `Netidx` translates it. So only
    // Platform is a trap for TLS.
    if matches!(p.auth, AuthChoice::Tls { .. }) && matches!(p.id_map, IdMapMode::Platform)
    {
        warnings.push(arcstr::literal!(
            "TLS auth with platform id-mapping: certificate identities \
             (e.g. user.domain) have no /bin/id translation, so they map to \
             no unix user and perms will deny every non-anonymous operation. \
             Use the netidx id-mapper, or DoNotMap to key perms on the SAN."
        ));
    }
    let id_map_socket_path = if id_map_active {
        Some(match &p.id_map_socket {
            Some(p) => p.clone(),
            None => id_map_engine::user_id_map_socket()?,
        })
    } else {
        None
    };
    let id_map_config_path = if id_map_active {
        Some(match &p.id_map_path {
            Some(p) => p.clone(),
            None => id_map_engine::user_id_map_path()?,
        })
    } else {
        None
    };

    // -- Member server ------------------------------------------------------
    let mut member_builder = rfile::MemberServerBuilder::default();
    member_builder
        .addr(p.listen)
        .bind_addr(bind_addr)
        .auth(resolver_auth_from(&p.auth, &tls_dest))
        .read_gated(p.read_gated);
    // Perms keyed on the raw identity string — no daemon, no `/bin/id`, and
    // so no group memberships either. Decided once: the member config and the
    // perms seed below must not disagree about whether groups exist.
    let do_not_map = id_map_socket_path.is_none() && matches!(p.id_map, IdMapMode::None);
    if let Some(sock) = &id_map_socket_path {
        member_builder
            .id_map_type(IdMapType::Socket)
            .id_map_command(ArcStr::from(sock.to_string_lossy().as_ref()));
    } else if do_not_map {
        member_builder.id_map_type(IdMapType::DoNotMap);
    }
    // else: the builder default (`id_map_type: Command`, no command ⇒
    // PlatformDefault) covers IdMapMode::Platform.
    let member = member_builder.build()?;

    // -- Perms file (separate from the main config) -------------------------
    // Resolve the perms path up front so we can wire it into the
    // resolver's `include_permissions` before building the config —
    // otherwise the seed file would sit on disk inert.
    //
    // Default behaviour: emit a perms file unless `with_perms_file`
    // is explicitly false. When no seed was supplied, auto-seed via
    // `perms::default_seed` so a fresh install boots with a
    // per-user-playground layout under the resolver's base, rather
    // than an empty perms map that denies everything.
    //
    // Exception: anonymous auth performs no access control — the
    // resolver allows every operation regardless of perms
    // (`resolver_server::config`: "For Anonymous all operations on the
    // server are always allowed"). A perms file would be completely
    // inert and only invite the misreading that the wide-open resolver
    // is somehow restricted, so we don't write one. (Local auth *does*
    // enforce perms, via uid→user/group mapping, so it keeps the seed.)
    //
    // For auth modes that carry a stable identity for the resolver
    // itself (TLS cert SAN, Krb5 SPN), grant that identity full
    // rights at the base — without it the resolver can't
    // subscribe / publish under its own tree (e.g. the local-client
    // config we emit below, or future self-published resolver cluster state).
    let anonymous = matches!(p.auth, AuthChoice::Anonymous);
    let perms_file = if p.with_perms_file && !anonymous {
        let path = match &p.perms_path {
            Some(p) => p.clone(),
            None => paths::user_perms_file()?,
        };
        // A delegated child's runtime authority root is its parent referral
        // path, not `/`: `resolver_server::Config::root()` returns the
        // parent's path, and the perms validator rejects any entry outside
        // that root. Seed under the same base so a delegated resolver boots
        // instead of crash-looping ("permission entry for parent: /sat1,
        // entry: /"). A non-delegated resolver keeps its own base (or `/`).
        let base_str = match &p.parent {
            Some(parent) if !parent.path.is_empty() => parent.path.as_str(),
            _ if p.base.is_empty() => "/",
            _ => p.base.as_str(),
        };
        let groups = if do_not_map {
            crate::perms::Groups::DoNotResolve
        } else {
            crate::perms::Groups::Resolve
        };
        let mut seed = p
            .perms_seed
            .clone()
            .unwrap_or_else(|| crate::perms::default_seed(base_str, groups));
        if let Some(entity) = resolver_self_entity(&p.auth) {
            crate::perms::add_entry(&mut seed, base_str, entity, "swlpd").with_context(
                || format!("seeding resolver self perms ({base_str} → {entity} → swlpd)"),
            )?;
        }
        Some((path, seed))
    } else {
        None
    };
    // The seed's shared grant is a group entry, and without id mapping there
    // are no groups to be a member of. Leaving it out is right, but silence
    // would leave the operator wondering why identities that authenticate
    // fine can reach nothing but their own subtree.
    if do_not_map && p.perms_seed.is_none() && perms_file.is_some() {
        warnings.push(arcstr::literal!(
            "id-map mode `none` keys permissions on the raw identity string, \
             so there are no groups — the usual shared `users` grant was not \
             seeded and each identity can reach only its own subtree under \
             `users/`. Grant wider access by naming identities directly with \
             `netidx admin perms edit`"
        ));
    }
    // Don't silently drop an explicitly-supplied seed on the anonymous
    // path — say why it wasn't written.
    if anonymous && p.perms_seed.is_some() {
        warnings.push(arcstr::literal!(
            "anonymous auth enforces no permissions (the resolver allows \
             every operation), so the supplied perms seed was not written; \
             perms apply only under local, krb5, or tls auth"
        ));
    }

    let mut rcfg_builder = rfile::ConfigBuilder::default();
    rcfg_builder.member_servers(vec![member]);
    if !p.base.is_empty() && p.base.as_str() != "/" {
        // file::Config has no `base` field for the resolver — the
        // base is implied by the children/parent layout. Skip.
        // (Left in for clarity; v1 doesn't need to override base.)
    }
    if let Some(parent) = &p.parent {
        rcfg_builder.parent(parent_into_file(parent.clone()));
    }
    if let Some((path, _)) = &perms_file {
        // Point the resolver at the seed perms file. Apply() writes the
        // perms file before re-validating the resolver config, so the
        // include path will exist when `Config::from_file` opens it.
        rcfg_builder
            .include_permissions(vec![ArcStr::from(path.to_string_lossy().as_ref())]);
    }
    let resolver_cfg = ResolverConfig::from(rcfg_builder.build()?);

    // -- Activation unit ----------------------------------------------------
    let mut units = BTreeMap::new();
    let resolver_args = vec![
        "resolver-server".to_string(),
        "-c".to_string(),
        resolver_cfg_path.to_string_lossy().into_owned(),
        "-f".to_string(),
        // Delay serving reads until publishers have had a chance to
        // re-register (~2× the writer TTL) so a restarted member never
        // serves an incomplete view — this is what makes a careful
        // one-at-a-time rolling restart of a resolver cluster invisible to readers.
        "--delay-reads".to_string(),
    ];
    units.insert(
        "resolver".to_string(),
        netidx_activation::file::UnitBuilder::default()
            .process(
                netidx_activation::file::ProcessCfgBuilder::default()
                    .exe(netidx_binary.to_string_lossy().into_owned())
                    .args(resolver_args)
                    // Resolver-server resolver clusters are rolled manually, one member
                    // at a time. A failed start must stay failed: an automatic
                    // retry could come up later, outside the operator's rollout
                    // sequence, and overlap another member's planned restart.
                    .restart(netidx_activation::file::Restart::No)
                    .build()?,
            )
            .build()?,
    );

    // -- Id-map unit + starter JSON (TLS only) ------------------------------
    let id_map_file = if id_map_active {
        let socket = id_map_socket_path.as_ref().expect("set when active");
        let config = id_map_config_path.as_ref().expect("set when active");
        let unit = services::id_map::unit(&services::id_map::IdMapServiceParams {
            netidx_binary: netidx_binary.clone(),
            socket: socket.clone(),
            config: config.clone(),
            socket_mode: None,
        })?;
        units.insert("id-map".to_string(), unit);
        Some((config.clone(), id_map_engine::empty()))
    } else {
        None
    };

    // -- Local client config (optional) ------------------------------------
    // For TLS we reuse the resolver's installed identity: the client
    // and server speak mTLS with the same cert. That's only sensible
    // for the "check from the same host" use case the operator opted
    // into; for cross-host inspection they want their own identity.
    let client_config = if p.with_local_client {
        let path = match &p.client_config_path {
            Some(p) => p.clone(),
            None => paths::user_client_config()?,
        };
        let cfg = build_local_client_config(p, &tls_dest)?;
        Some((path, cfg))
    } else {
        None
    };

    Ok(RenderedTemplate {
        client_config,
        resolver_config: Some((resolver_cfg_path, resolver_cfg)),
        perms_file,
        id_map_file,
        units,
        units_dir,
        tls_install: tls_job.into_iter().collect(),
        warnings,
    })
}

/// Build the client config that pairs with this resolver. The addr
/// list is a single entry pointing at `p.listen`; the auth carries
/// through from `p.auth`; for TLS the client identity is the same
/// cert the resolver itself presents (so mTLS validates as the
/// resolver's own name).
fn build_local_client_config(
    p: &ResolverParams,
    tls_dest: &Path,
) -> Result<ClientConfig> {
    let base = if p.base.is_empty() { "/" } else { p.base.as_str() };
    let mut ccfg = cfile::ConfigBuilder::default();
    ccfg.addrs(vec![(p.listen, auth_choice_to_cfile_auth(&p.auth))])
        .base(base)
        .default_auth(auth_choice_to_default_mech(&p.auth));
    // Publishers default to `BindCfg::Local` (127.0.0.1), which the
    // resolver rejects whenever it itself is bound to a non-loopback
    // address (mixing loopback with non-loopback breaks check_addrs).
    // Match the publisher's bind to the resolver's own interface so
    // `netidx publisher`/`netidx-activation` Just Works locally.
    if let Some(bind) = default_local_client_bind_cfg(p) {
        ccfg.default_bind_config(bind);
    }
    if let AuthChoice::Tls { name, askpass, .. } = &p.auth {
        // Reuse the resolver's installed identity. The cert is
        // installed by the tls_install copy job; we just point the client section at
        // those installed paths — no second copy job is needed.
        let [certificate, private_key, trusted] = tlsmod::installed_files_in(tls_dest);
        let identity = cfile::TlsIdentity {
            trusted: trusted.to_string_lossy().into_owned(),
            certificate: certificate.to_string_lossy().into_owned(),
            private_key: private_key.to_string_lossy().into_owned(),
        };
        // Key the entry in `client.tls.identities` by the *domain*
        // part of the cert SAN, not the full SAN. netidx keys
        // identities by domain (one entry per administrative trust
        // domain) — the runtime matches any host under that domain
        // via the reverse-domain prefix lookup in `tls::get_match`.
        // The install dir stays at the full SAN so two hosts in the
        // same domain don't clobber each other's cert files.
        //
        // Single-label SANs (LAN hostnames like `resolver`, k8s
        // service names, mDNS short names) have no domain to peel
        // off. The runtime is fine with a single-label key — both
        // sides of the match get reverse-domain-name'd and the
        // exact-match arm of `tls::get_match` handles it — so we
        // fall back to the full SAN as the identity key rather than
        // dead-ending the install. Operators who want a different
        // identity layout can edit the emitted client.json.
        let identity_key = tlsmod::domain_from_san(name.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|_| name.to_string());
        let mut identities = BTreeMap::new();
        identities.insert(identity_key.clone(), identity);
        ccfg.tls(cfile::Tls {
            default_identity: Some(identity_key),
            identities,
            askpass: askpass.as_ref().map(|p| p.to_string_lossy().into_owned()),
        });
    }
    Ok(ClientConfig::from(ccfg.build()?))
}

/// The stable on-the-wire identity string that the resolver itself
/// presents when acting as a client (TLS handshake SAN; Krb5 client
/// principal of the SPN). `None` for auth modes that don't carry one
/// (Anonymous has no identity; Local is peer-credentials based and
/// has no fixed name for the resolver process). Used to seed
/// resolver-side perms for the resolver's own tree.
pub(super) fn resolver_self_entity(a: &AuthChoice) -> Option<&str> {
    match a {
        AuthChoice::Tls { name, .. } => Some(name.as_str()),
        AuthChoice::Krb5 { spn } => Some(spn.as_str()),
        AuthChoice::Anonymous | AuthChoice::Local { .. } => None,
    }
}

fn auth_choice_to_cfile_auth(a: &AuthChoice) -> cfile::Auth {
    match a {
        AuthChoice::Anonymous => cfile::Auth::Anonymous,
        AuthChoice::Local { path } => {
            cfile::Auth::Local(ArcStr::from(path.to_string_lossy().as_ref()))
        }
        AuthChoice::Krb5 { spn } => cfile::Auth::Krb5(spn.clone()),
        AuthChoice::Tls { name, .. } => cfile::Auth::Tls(name.clone()),
    }
}

fn auth_choice_to_default_mech(a: &AuthChoice) -> DefaultAuthMech {
    match a {
        AuthChoice::Anonymous => DefaultAuthMech::Anonymous,
        AuthChoice::Local { .. } => DefaultAuthMech::Local,
        AuthChoice::Krb5 { .. } => DefaultAuthMech::Krb5,
        AuthChoice::Tls { .. } => DefaultAuthMech::Tls,
    }
}

/// Pick a sensible `default_bind_config` for a client that lives on
/// the same host as this resolver. The publisher default is
/// `BindCfg::Local` (127.0.0.1); a non-loopback resolver rejects that.
///
/// Precedence:
///   1. `p.local_client_bind` is an explicit override (used verbatim).
///      The CLI fills this from `NetShape` in cloud-elastic deployments
///      so the local client gets the Elastic form
///      (`<public>@<private-subnet>/<prefix>`) the publisher needs to
///      bind to the private NIC while advertising the public IP.
///   2. Otherwise emit `<bind-ip>/32` (v4) or `<bind-ip>/128` (v6) of
///      `p.bind.unwrap_or(p.listen.ip())`. Using `bind` (not `listen`)
///      matters when the two differ: the local publisher must bind to
///      a real local interface, and the public listen IP is NAT'd onto
///      the private NIC, not assigned to it.
///   3. Loopback bind ⇒ leave unset; publisher's `BindCfg::Local`
///      default already matches.
fn default_local_client_bind_cfg(p: &ResolverParams) -> Option<String> {
    if let Some(override_) = p.local_client_bind.as_ref() {
        return Some(override_.clone());
    }
    let bind = p.bind.unwrap_or_else(|| p.listen.ip());
    let ip = if bind.is_unspecified() { p.listen.ip() } else { bind };
    if ip.is_loopback() {
        return None;
    }
    Some(match ip {
        std::net::IpAddr::V4(_) => format!("{ip}/32"),
        std::net::IpAddr::V6(_) => format!("{ip}/128"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::perms;

    fn anon_params(out: &tempfile::TempDir) -> ResolverParams {
        ResolverParams {
            read_gated: ReadGate::No,
            auth: AuthChoice::Anonymous,
            base: ArcStr::from("/"),
            listen: "127.0.0.1:4564".parse().unwrap(),
            bind: None,
            parent: None,
            perms_seed: None,
            // Most existing tests opt out of the perms file — the
            // auto-seed behaviour is exercised by its own test
            // (`auto_seeds_default_perms_when_enabled`). Tests that
            // explicitly need a perms file flip this back on.
            with_perms_file: false,
            perms_path: None,
            resolver_config_path: Some(out.path().join("resolver.json")),
            units_dir: Some(out.path().join("activation")),
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            id_map: IdMapMode::Platform,
            id_map_path: None,
            id_map_socket: None,
            // Most existing tests don't write a client.json; they
            // assert on the resolver side only. Opt out here and let
            // the local-client tests below opt in explicitly.
            with_local_client: false,
            client_config_path: None,
            local_client_bind: None,
        }
    }

    #[test]
    fn anonymous_resolver_validates() {
        let out = tempfile::tempdir().unwrap();
        let rt = resolver(&anon_params(&out)).unwrap();
        rt.apply_test(out.path()).unwrap();
        assert!(out.path().join("resolver.json").exists());
        assert!(out.path().join("activation/resolver.unit").exists());
    }

    /// The resolver unit must run with `--delay-reads` by default so a
    /// one-at-a-time rolling restart never serves readers an incomplete view.
    #[test]
    fn resolver_unit_delays_reads_by_default() {
        let out = tempfile::tempdir().unwrap();
        let rt = resolver(&anon_params(&out)).unwrap();
        let unit = rt.units.get("resolver").expect("resolver unit emitted");
        assert!(
            unit.process.args.iter().any(|a| a == "--delay-reads"),
            "resolver unit args must include --delay-reads: {:?}",
            unit.process.args,
        );
        assert!(
            matches!(unit.process.restart, netidx_activation::file::Restart::No),
            "resolver unit must not restart automatically: {}",
            unit.process.restart,
        );
    }

    #[test]
    fn local_client_anonymous() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.with_local_client = true;
        let client_path = out.path().join("client.json");
        p.client_config_path = Some(client_path.clone());
        let rt = resolver(&p).unwrap();
        // The client config slot is filled at template-render time.
        let (cp, c) = rt.client_config.as_ref().unwrap();
        assert_eq!(cp, &client_path);
        // Points at the resolver's listen addr, with matching auth.
        assert_eq!(c.0.addrs.len(), 1);
        assert_eq!(c.0.addrs[0].0, p.listen);
        assert!(matches!(c.0.addrs[0].1, cfile::Auth::Anonymous));
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Anonymous));
        // Anonymous → no tls section.
        assert!(c.0.tls.is_none());
        // Loopback listen → no default_bind_config override needed
        // (the publisher's BindCfg::Local default already matches).
        assert!(c.0.default_bind_config.is_none());
        rt.apply_test(out.path()).unwrap();
        assert!(client_path.exists());
    }

    /// When the resolver listens on a non-loopback address, the local
    /// client must override `default_bind_config` to that same interface
    /// — otherwise publishers run on the same host bind to 127.0.0.1
    /// and the resolver rejects their registrations as loopback while
    /// it's bound non-loopback.
    #[test]
    fn local_client_default_bind_matches_non_loopback_listen() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        // Use an address that doesn't have to exist on the test host —
        // we only inspect the rendered config, not start a publisher.
        p.listen = "10.0.0.1:4564".parse().unwrap();
        p.with_local_client = true;
        let client_path = out.path().join("client.json");
        p.client_config_path = Some(client_path.clone());
        let rt = resolver(&p).unwrap();
        let (_, c) = rt.client_config.as_ref().unwrap();
        assert_eq!(c.0.default_bind_config.as_deref(), Some("10.0.0.1/32"));
    }

    #[test]
    fn local_client_default_bind_uses_listen_for_wildcard_resolver_bind() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.listen = "10.0.0.1:4564".parse().unwrap();
        p.bind = Some("0.0.0.0".parse().unwrap());
        p.with_local_client = true;
        p.client_config_path = Some(out.path().join("client.json"));

        let rt = resolver(&p).unwrap();
        let (_, c) = rt.client_config.as_ref().unwrap();
        assert_eq!(c.0.default_bind_config.as_deref(), Some("10.0.0.1/32"));
    }

    /// Cloud-elastic shape: resolver listens on a public IP but binds
    /// to a private NIC. The local client must bind to the private NIC
    /// (the public IP is NAT'd onto it, not assigned). When the CLI
    /// supplies the elastic `BindCfg` string via `local_client_bind`,
    /// the template emits it verbatim; without that override the
    /// fallback uses `bind` (not `listen`) so we never emit a
    /// public-IP/32 that can't actually be bound.
    #[test]
    fn local_client_bind_handles_cloud_elastic() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.listen = "54.32.224.1:4564".parse().unwrap();
        p.bind = Some("10.0.0.5".parse().unwrap());
        p.with_local_client = true;
        p.client_config_path = Some(out.path().join("client.json"));

        // With explicit override: template uses it verbatim.
        p.local_client_bind = Some("54.32.224.1@10.0.0.0/24".to_string());
        let rt = resolver(&p).unwrap();
        let (_, c) = rt.client_config.as_ref().unwrap();
        assert_eq!(c.0.default_bind_config.as_deref(), Some("54.32.224.1@10.0.0.0/24"),);

        // Without override: fall back to bind/32, NOT listen/32. The
        // previous code used listen here and produced an unbindable
        // public-IP/32 in the cloud-elastic case.
        p.local_client_bind = None;
        let rt = resolver(&p).unwrap();
        let (_, c) = rt.client_config.as_ref().unwrap();
        assert_eq!(c.0.default_bind_config.as_deref(), Some("10.0.0.5/32"));
    }

    /// Stand up a TLS-auth resolver template that also wants a local
    /// client. Verifies the client config points at the resolver's
    /// installed identity (same cert, key, and trust anchor).
    #[test]
    fn local_client_tls_reuses_resolver_identity() {
        let ca_dir = tempfile::tempdir().unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued = crate::template::test_identity(
            ca_dir.path(),
            id_src.path(),
            "resolver.example.com",
        );

        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Tls {
            name: ArcStr::from("resolver.example.com"),
            certificate: issued.certificate.clone(),
            private_key: issued.private_key.clone(),
            trusted: ca_dir.path().join("certificate.pem"),
            askpass: None,
        };
        p.with_local_client = true;
        let client_path = out.path().join("client.json");
        p.client_config_path = Some(client_path.clone());
        let rt = resolver(&p).unwrap();

        let (_, c) = rt.client_config.as_ref().unwrap();
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Tls));
        // The single addr carries the resolver's TLS name (the full
        // SAN — this is what the client expects to see in the
        // server's cert at handshake time).
        match &c.0.addrs[0].1 {
            cfile::Auth::Tls(name) => {
                assert_eq!(name.as_str(), "resolver.example.com")
            }
            other => panic!("expected Tls auth, got {other:?}"),
        }
        // The TLS section must reference the *installed* identity
        // dir — same dir the resolver itself reads from. We don't
        // hard-code the absolute path (it depends on the user's
        // config dir) but we check the leaf filenames.
        let tls = c.0.tls.as_ref().expect("client tls section");
        // The identity is keyed by the *domain* part of the cert
        // SAN (`example.com`), not the full SAN (`resolver.example.com`).
        // netidx keys identities per admin domain and
        // matches hosts under that domain via reverse-domain prefix
        // lookup at handshake time.
        let identity = tls
            .identities
            .get("example.com")
            .expect("identity 'example.com' (domain part of resolver.example.com)");
        assert!(identity.certificate.ends_with("certificate.pem"));
        assert!(identity.private_key.ends_with("private.key"));
        assert!(identity.trusted.ends_with("trusted.pem"));
        assert_eq!(tls.default_identity.as_deref(), Some("example.com"));
        // The cert install path *is* what the resolver-side rfile::Auth
        // points at; this is the "same cert" guarantee in code form.
        let resolver_auth =
            &rt.resolver_config.as_ref().unwrap().1.0.member_servers[0].auth;
        match resolver_auth {
            rfile::Auth::Tls { certificate, .. } => {
                assert_eq!(certificate.as_str(), identity.certificate.as_str());
            }
            other => panic!("expected resolver Tls auth, got {other:?}"),
        }
    }

    #[test]
    fn no_local_client_when_opted_out() {
        let out = tempfile::tempdir().unwrap();
        let p = anon_params(&out); // with_local_client = false by default in fixture
        let rt = resolver(&p).unwrap();
        assert!(rt.client_config.is_none());
    }

    /// When `with_perms_file = true` and `perms_seed = None`, the
    /// template must auto-seed `default_seed()` rather than emit an
    /// empty perms map. A fresh install needs at least the
    /// per-user-playground rule to be usable without manual edits.
    #[test]
    fn auto_seeds_default_perms_when_enabled() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        // Anonymous no longer emits a perms file (perms are inert under
        // it). Use local auth, which enforces perms and — like
        // anonymous — has no resolver self-entity, so the seed is
        // exactly `default_seed` with no extra base entry to account for.
        p.auth = AuthChoice::Local { path: out.path().join("auth.sock") };
        p.with_perms_file = true;
        p.perms_path = Some(out.path().join("perms.json"));
        let rt = resolver(&p).unwrap();
        // The slot in RenderedTemplate carries the auto-seeded map.
        let (path, seeded) = rt.perms_file.as_ref().expect("perms_file emitted");
        assert_eq!(path, &out.path().join("perms.json"));
        // PMap doesn't impl PartialEq, so compare flat (path, entity,
        // perm) triples — same entries, ignoring iteration order.
        let collect = |m: &netidx::resolver_server::config::PMap| {
            let mut v: Vec<(String, String, String)> = crate::perms::iter(m)
                .map(|(p, e, r)| (p.to_string(), e.to_string(), r.to_string()))
                .collect();
            v.sort();
            v
        };
        // `anon_params` uses base "/", so auto-seed should anchor at
        // root — that's what we compare against.
        assert_eq!(
            collect(seeded),
            collect(&crate::perms::default_seed("/", crate::perms::Groups::Resolve))
        );
        // Validate the on-disk shape after apply — must parse back
        // into the same map.
        rt.apply_test(out.path()).unwrap();
        let loaded = crate::perms::load_perms(out.path().join("perms.json")).unwrap();
        assert_eq!(
            collect(&loaded),
            collect(&crate::perms::default_seed("/", crate::perms::Groups::Resolve))
        );
        // And the round-trip-through-resolver-validation step
        // accepts it (the $[user] dynamic entry shape can trip up
        // PMap::from_file if the seed is malformed).
        netidx::resolver_server::config::Config::load(out.path().join("resolver.json"))
            .expect("resolver config including auto-seed perms must validate");
    }

    #[test]
    fn perms_seed_emits_separate_file() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        // Local auth, not anonymous: anonymous discards a supplied seed
        // (perms are inert under it); local enforces it.
        p.auth = AuthChoice::Local { path: out.path().join("auth.sock") };
        let mut seed = perms::empty();
        perms::add_entry(&mut seed, "/", "alice", "swlpd").unwrap();
        p.perms_seed = Some(seed);
        p.with_perms_file = true;
        p.perms_path = Some(out.path().join("perms.json"));

        let rt = resolver(&p).unwrap();
        // The resolver config must reference the seed file via
        // `include_permissions`; otherwise the seed sits on disk inert.
        let (_, r) = rt.resolver_config.as_ref().unwrap();
        let perms_path_str = out.path().join("perms.json").to_string_lossy().into_owned();
        assert!(
            r.0.include_permissions.iter().any(|p| p.as_str() == perms_path_str.as_str()),
            "perms_path was not wired into include_permissions: {:?}",
            r.0.include_permissions,
        );
        rt.apply_test(out.path()).unwrap();
        assert!(out.path().join("perms.json").exists());

        let loaded = perms::load_perms(out.path().join("perms.json")).unwrap();
        assert_eq!(
            perms::lookup(&loaded, "/", "alice").map(|s| s.as_str()),
            Some("swlpd"),
        );

        // Round-trip through `Config::from_file` (via `Config::load`)
        // and confirm the merged live PMap actually contains the seed.
        let live = netidx::resolver_server::config::Config::load(
            out.path().join("resolver.json"),
        )
        .unwrap();
        assert_eq!(
            live.perms().0.get("/").and_then(|m| m.get("alice")).map(|s| s.as_str()),
            Some("swlpd"),
        );
    }

    #[test]
    fn tls_resolver_emits_install_job() {
        // Stand up a real CA-issued identity so the round-trip
        // validate path through Config::from_file is meaningful.
        let ca_dir = tempfile::tempdir().unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued =
            crate::template::test_identity(ca_dir.path(), id_src.path(), "resolver");

        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Tls {
            name: ArcStr::from("resolver"),
            certificate: issued.certificate.clone(),
            private_key: issued.private_key.clone(),
            trusted: ca_dir.path().join("certificate.pem"),
            askpass: None,
        };
        let rt = resolver(&p).unwrap();
        assert_eq!(rt.tls_install.len(), 1);
        assert_eq!(rt.tls_install[0].cn, "resolver");
        let (_, r) = rt.resolver_config.as_ref().unwrap();
        assert!(matches!(r.0.member_servers[0].auth, rfile::Auth::Tls { .. }));
        // TLS without the id-mapper is the incoherent expert combo —
        // the plan must say so (and `describe()` must surface it).
        assert_eq!(rt.warnings.len(), 1);
        assert!(rt.describe().contains("warning:"));
    }

    /// Stand up a TLS-auth resolver template that requests id-map
    /// installation. Verifies (a) the resolver config points at the
    /// socket, (b) an `id-map.unit` is emitted, and (c) a starter
    /// `id-map.json` lands on disk after `apply()`.
    #[test]
    fn tls_resolver_with_id_map_emits_unit_and_starter() {
        let ca_dir = tempfile::tempdir().unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued =
            crate::template::test_identity(ca_dir.path(), id_src.path(), "resolver");

        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Tls {
            name: ArcStr::from("resolver"),
            certificate: issued.certificate.clone(),
            private_key: issued.private_key.clone(),
            trusted: ca_dir.path().join("certificate.pem"),
            askpass: None,
        };
        p.id_map = IdMapMode::Netidx;
        let id_map_socket = out.path().join("id-map.sock");
        let id_map_path = out.path().join("id-map.json");
        p.id_map_socket = Some(id_map_socket.clone());
        p.id_map_path = Some(id_map_path.clone());
        let rt = resolver(&p).unwrap();

        // Resolver config must point at the daemon's socket.
        let (_, r) = rt.resolver_config.as_ref().unwrap();
        let member = &r.0.member_servers[0];
        assert!(matches!(member.id_map_type, IdMapType::Socket));
        assert_eq!(
            member.id_map_command.as_ref().map(|s| s.as_str()),
            Some(id_map_socket.to_string_lossy().as_ref()),
        );

        // The bundle must include both unit files and a starter JSON.
        assert!(rt.units.contains_key("resolver"));
        assert!(rt.units.contains_key("id-map"));
        let (file_path, starter) = rt.id_map_file.as_ref().unwrap();
        assert_eq!(file_path, &id_map_path);
        assert!(starter.identities.is_empty());
        // TLS *with* the id-mapper is the coherent profile — no warning.
        assert!(rt.warnings.is_empty());

        rt.apply_test(out.path()).unwrap();
        // Starter is on disk, both unit files dropped.
        assert!(id_map_path.exists());
        assert!(out.path().join("activation/id-map.unit").exists());
        assert!(out.path().join("activation/resolver.unit").exists());
    }

    /// Re-running `apply()` after the operator has populated the
    /// id-map JSON must not flatten their edits back to empty.
    #[test]
    fn id_map_starter_does_not_clobber_existing_file() {
        let ca_dir = tempfile::tempdir().unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued =
            crate::template::test_identity(ca_dir.path(), id_src.path(), "resolver");

        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Tls {
            name: ArcStr::from("resolver"),
            certificate: issued.certificate.clone(),
            private_key: issued.private_key.clone(),
            trusted: ca_dir.path().join("certificate.pem"),
            askpass: None,
        };
        p.id_map = IdMapMode::Netidx;
        let id_map_path = out.path().join("id-map.json");
        p.id_map_socket = Some(out.path().join("id-map.sock"));
        p.id_map_path = Some(id_map_path.clone());

        // Apply once — starter on disk.
        resolver(&p).unwrap().apply_test(out.path()).unwrap();

        // Operator populates the map.
        let mut populated = id_map_engine::empty();
        id_map_engine::upsert_group(&mut populated, "users");
        id_map_engine::upsert_identity(
            &mut populated,
            "resolver.example.com",
            "users",
            &[],
        )
        .unwrap();
        id_map_engine::save(&id_map_path, &populated).unwrap();

        // Apply again — populated state must survive.
        resolver(&p).unwrap().apply_test(out.path()).unwrap();
        let after = id_map_engine::load(&id_map_path).unwrap();
        assert!(after.lookup_by_name("resolver.example.com").is_some());
        assert!(after.groups.contains("users"));
    }

    /// Anonymous and Local auth must NOT get id-map auto-installation
    /// even with `id_map = Netidx`. Anonymous has no user; Local
    /// goes through `/bin/id` against the peer's uid and doesn't need
    /// the daemon.
    #[test]
    fn id_map_not_installed_for_anonymous_or_local() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.id_map = IdMapMode::Netidx;
        let rt = resolver(&p).unwrap();
        assert!(rt.id_map_file.is_none());
        assert!(!rt.units.contains_key("id-map"));

        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Local { path: PathBuf::from("/tmp/netidx-local.sock") };
        p.id_map = IdMapMode::Netidx;
        let rt = resolver(&p).unwrap();
        assert!(rt.id_map_file.is_none());
        assert!(!rt.units.contains_key("id-map"));
    }

    /// Krb5 with `id_map = Netidx` must install the daemon: the
    /// resolver hands the daemon the full principal (e.g.
    /// `eric@RYU-OH.ORG`) as the lookup key, and gets back the
    /// uid/gid set the perms map keys on.
    #[test]
    fn krb5_resolver_with_id_map_emits_unit_and_starter() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Krb5 { spn: ArcStr::from("netidx/resolver@RYU-OH.ORG") };
        p.id_map = IdMapMode::Netidx;
        let id_map_socket = out.path().join("id-map.sock");
        let id_map_path = out.path().join("id-map.json");
        p.id_map_socket = Some(id_map_socket.clone());
        p.id_map_path = Some(id_map_path.clone());
        let rt = resolver(&p).unwrap();

        let (_, r) = rt.resolver_config.as_ref().unwrap();
        let member = &r.0.member_servers[0];
        assert!(matches!(member.id_map_type, IdMapType::Socket));
        assert_eq!(
            member.id_map_command.as_ref().map(|s| s.as_str()),
            Some(id_map_socket.to_string_lossy().as_ref()),
        );

        assert!(rt.units.contains_key("resolver"));
        assert!(rt.units.contains_key("id-map"));
        let (file_path, starter) = rt.id_map_file.as_ref().unwrap();
        assert_eq!(file_path, &id_map_path);
        assert!(starter.identities.is_empty());

        rt.apply_test(out.path()).unwrap();
        assert!(id_map_path.exists());
        assert!(out.path().join("activation/id-map.unit").exists());
        assert!(out.path().join("activation/resolver.unit").exists());
    }

    /// Krb5 with `Platform` (a site IdM resolves full principals) is the
    /// default profile — `id_map_type: Command`, no daemon, no warning.
    #[test]
    fn krb5_platform_is_coherent() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Krb5 { spn: ArcStr::from("netidx/resolver@RYU-OH.ORG") };
        p.id_map = IdMapMode::Platform;
        let rt = resolver(&p).unwrap();
        assert!(rt.warnings.is_empty());
        let (_, r) = rt.resolver_config.as_ref().unwrap();
        assert!(matches!(r.0.member_servers[0].id_map_type, IdMapType::Command));
        assert!(!rt.units.contains_key("id-map"));
        assert!(rt.id_map_file.is_none());
    }

    /// Krb5 with `None` (no IdM) ⇒ `id_map_type: DoNotMap`: perms keyed
    /// on the raw principal, no daemon, no warning.
    #[test]
    fn krb5_none_uses_donotmap_no_daemon() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Krb5 { spn: ArcStr::from("netidx/resolver@RYU-OH.ORG") };
        p.id_map = IdMapMode::None;
        let rt = resolver(&p).unwrap();
        assert!(rt.warnings.is_empty());
        let (_, r) = rt.resolver_config.as_ref().unwrap();
        assert!(matches!(r.0.member_servers[0].id_map_type, IdMapType::DoNotMap));
        assert!(!rt.units.contains_key("id-map"));
        assert!(rt.id_map_file.is_none());
    }

    /// Krb5 resolver must grant its own SPN full rights at the base —
    /// same shape as the TLS branch, just keyed by SPN instead of
    /// cert SAN. Without it the resolver can't subscribe / publish
    /// under its own tree once resolver cluster / self-published state lands.
    #[test]
    fn krb5_resolver_seeds_self_spn_in_perms() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        let spn = ArcStr::from("netidx/resolver@RYU-OH.ORG");
        p.auth = AuthChoice::Krb5 { spn: spn.clone() };
        p.with_perms_file = true;
        p.perms_path = Some(out.path().join("perms.json"));
        let rt = resolver(&p).unwrap();
        let (_, seeded) = rt.perms_file.as_ref().expect("perms_file emitted");
        // Base "/" with the SPN as the entity and swlpd as the perm.
        assert_eq!(
            crate::perms::lookup(seeded, "/", spn.as_str()).map(|s| s.as_str()),
            Some("swlpd"),
            "krb5 SPN must appear at base with full rights",
        );
        // Round-trips through Config validation (the $[user] template
        // entry from the default seed can trip up PMap::from_file if
        // the file shape is wrong).
        rt.apply_test(out.path()).unwrap();
        netidx::resolver_server::config::Config::load(out.path().join("resolver.json"))
            .expect("resolver config including krb5-SPN perms must validate");
    }

    /// Anonymous auth enforces no permissions (the resolver allows every
    /// operation regardless of perms), so a perms file would be inert and
    /// misleading — the template must write none, and must not wire
    /// `include_permissions`, even with `with_perms_file` on.
    #[test]
    fn anonymous_resolver_emits_no_perms_file() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.with_perms_file = true;
        p.perms_path = Some(out.path().join("perms.json"));
        let rt = resolver(&p).unwrap();
        assert!(rt.perms_file.is_none(), "anonymous must not emit a perms file");
        let (_, r) = rt.resolver_config.as_ref().unwrap();
        assert!(
            r.0.include_permissions.is_empty(),
            "anonymous must not wire include_permissions: {:?}",
            r.0.include_permissions,
        );
        // Nothing lands on disk after apply either.
        rt.apply_test(out.path()).unwrap();
        assert!(!out.path().join("perms.json").exists());
    }

    /// An explicitly-supplied perms seed on the anonymous path is
    /// dropped (perms are inert under anonymous), but loudly — the
    /// operator gets a warning rather than a silent no-op.
    #[test]
    fn anonymous_resolver_warns_when_dropping_explicit_seed() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        let mut seed = perms::empty();
        perms::add_entry(&mut seed, "/", "alice", "swlpd").unwrap();
        p.perms_seed = Some(seed);
        p.with_perms_file = true;
        p.perms_path = Some(out.path().join("perms.json"));
        let rt = resolver(&p).unwrap();
        assert!(rt.perms_file.is_none());
        assert!(
            rt.warnings.iter().any(|w| w.contains("perms seed was not written")),
            "expected a dropped-seed warning, got {:?}",
            rt.warnings,
        );
    }
}
