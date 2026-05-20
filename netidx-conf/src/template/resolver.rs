//! Resolver-server template. A single network-facing resolver,
//! with the operator's chosen auth scheme on the one member,
//! optional parent referral, optional perms seed, and a single
//! activation unit that runs `netidx resolver-server`.
//!
//! For v1 the TLS path here uses the resolver's default `id_map_type:
//! Command` (`/bin/id`). The id-map daemon and its socket-mode
//! wiring are FUTURE.md.

use super::*;
use crate::{
    client::ClientConfig, id_map as id_map_engine, paths,
    resolver::ResolverConfig, tls as tlsmod,
};
use anyhow::Result;
use netidx::resolver_server::config::file::IdMapType;
use std::{net::SocketAddr, path::PathBuf};

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
    /// authenticated user under `/users/$[user]`, read+write for the
    /// `users` group under `/users`. That gives a fresh install a
    /// working "per-user playground" without forcing the operator to
    /// hand-author a perms map up front.
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
    /// Auto-install the id-mapper daemon. Only meaningful for
    /// `AuthChoice::Tls` — Anonymous / Local / Krb5 either don't need
    /// it or have their own group-lookup path. When the auth is TLS
    /// and this is `true`:
    /// - The resolver config's member auth is set to
    ///   `id_map_type: Socket` + `id_map_command: <socket path>`.
    /// - An `id-map.unit` activation unit is emitted alongside
    ///   `resolver.unit`.
    /// - A starter id-map JSON file is written if `id_map_path` does
    ///   not already exist (so `apply()` is idempotent on re-runs).
    pub with_id_map: bool,
    /// Where to put the id-map JSON. `None` ⇒
    /// `netidx_conf::id_map::user_id_map_path()`.
    pub id_map_path: Option<PathBuf>,
    /// Where the id-map daemon will bind its unix socket. `None` ⇒
    /// `netidx_conf::id_map::user_id_map_socket()`.
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

    let tls_job = resolver_tls_copy_job(&p.auth)?;
    let tls_dest = tls_job
        .as_ref()
        .map(|j| j.dest_dir.clone())
        .unwrap_or_else(|| PathBuf::from("/dev/null"));

    // -- Id-map wiring (TLS only) -------------------------------------------
    // For TLS-auth resolvers we point the member at the local
    // id-mapper daemon over a unix socket — that's the only auth
    // mode where uid/group lookups go through a non-local path.
    // Anonymous / Local / Krb5 keep the resolver's existing
    // `Command` default.
    let id_map_active = p.with_id_map && matches!(p.auth, AuthChoice::Tls { .. });
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
        .auth(resolver_auth_from(&p.auth, &tls_dest));
    if let Some(sock) = &id_map_socket_path {
        member_builder
            .id_map_type(IdMapType::Socket)
            .id_map_command(ArcStr::from(sock.to_string_lossy().as_ref()));
    }
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
    // For TLS auth we additionally grant the resolver's own cert
    // identity full rights at the base — without it the resolver
    // can't subscribe / publish under its own tree (e.g. for the
    // local-client config we emit below to actually reach the data
    // it's publishing). The entity is the cert SAN, which is what
    // the resolver matches against on the wire.
    let perms_file = if p.with_perms_file {
        let path = match &p.perms_path {
            Some(p) => p.clone(),
            None => paths::user_perms_file()?,
        };
        let base_str = if p.base.is_empty() { "/" } else { p.base.as_str() };
        let mut seed = p
            .perms_seed
            .clone()
            .unwrap_or_else(|| crate::perms::default_seed(base_str));
        if let AuthChoice::Tls { name, .. } = &p.auth {
            crate::perms::add_entry(&mut seed, base_str, name.as_str(), "swlpd")
                .with_context(|| {
                    format!(
                        "seeding resolver TLS-cert perms ({base_str} → {name} → swlpd)"
                    )
                })?;
        }
        Some((path, seed))
    } else {
        None
    };

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
        rcfg_builder.include_permissions(vec![ArcStr::from(
            path.to_string_lossy().as_ref(),
        )]);
    }
    let resolver_cfg = ResolverConfig::from(rcfg_builder.build()?);

    // -- Activation unit ----------------------------------------------------
    let mut units = BTreeMap::new();
    let resolver_args = vec![
        "resolver-server".to_string(),
        "-c".to_string(),
        resolver_cfg_path.to_string_lossy().into_owned(),
        "-f".to_string(),
    ];
    units.insert(
        "resolver".to_string(),
        netidx_activation::file::UnitBuilder::default()
            .process(
                netidx_activation::file::ProcessCfgBuilder::default()
                    .exe(netidx_binary.to_string_lossy().into_owned())
                    .args(resolver_args)
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
        let cfg = build_local_client_config(p)?;
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
    })
}

/// Build the client config that pairs with this resolver. The addr
/// list is a single entry pointing at `p.listen`; the auth carries
/// through from `p.auth`; for TLS the client identity is the same
/// cert the resolver itself presents (so mTLS validates as the
/// resolver's own name).
fn build_local_client_config(p: &ResolverParams) -> Result<ClientConfig> {
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
    if let Some(bind) = default_bind_cfg_for_listen(p.listen) {
        ccfg.default_bind_config(bind);
    }
    if let AuthChoice::Tls { name, askpass, .. } = &p.auth {
        // Reuse the resolver's installed identity. The cert is
        // installed at `identity_dir(<resolver-tls-name>)` by the
        // tls_install copy job; we just point the client section at
        // those installed paths — no second copy job is needed.
        let dest = tlsmod::identity_dir(name.as_str())?;
        let identity = cfile::TlsIdentity {
            trusted: dest.join("trusted.pem").to_string_lossy().into_owned(),
            certificate: dest.join("certificate.pem").to_string_lossy().into_owned(),
            private_key: dest.join("private.key").to_string_lossy().into_owned(),
        };
        // Key the entry in `client.tls.identities` by the *domain*
        // part of the cert SAN, not the full SAN. netidx keys
        // identities by domain (one entry per administrative trust
        // domain) — the runtime matches any host under that domain
        // via the reverse-domain prefix lookup in `tls::get_match`.
        // The install dir stays at the full SAN so two hosts in the
        // same domain don't clobber each other's cert files.
        let domain = tlsmod::domain_from_san(name.as_str()).with_context(|| {
            format!(
                "deriving identity domain from resolver cert SAN {:?}",
                name
            )
        })?;
        let mut identities = BTreeMap::new();
        identities.insert(domain.to_string(), identity);
        ccfg.tls(cfile::Tls {
            default_identity: Some(domain.to_string()),
            identities,
            askpass: askpass
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
        });
    }
    Ok(ClientConfig::from(ccfg.build()?))
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
/// the same host as a resolver bound to `listen`. The publisher
/// default is `BindCfg::Local` (127.0.0.1); a non-loopback resolver
/// rejects that, so we emit a `<ip>/32` (v4) or `<ip>/128` (v6)
/// `BindCfg::Match` pointing at the resolver's own interface. For a
/// loopback listen address the publisher default is already correct,
/// so we leave the field unset.
fn default_bind_cfg_for_listen(listen: SocketAddr) -> Option<String> {
    let ip = listen.ip();
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
            with_id_map: false,
            id_map_path: None,
            id_map_socket: None,
            // Most existing tests don't write a client.json; they
            // assert on the resolver side only. Opt out here and let
            // the local-client tests below opt in explicitly.
            with_local_client: false,
            client_config_path: None,
        }
    }

    #[test]
    fn anonymous_resolver_validates() {
        let out = tempfile::tempdir().unwrap();
        let rt = resolver(&anon_params(&out)).unwrap();
        rt.apply().unwrap();
        assert!(out.path().join("resolver.json").exists());
        assert!(out.path().join("activation/resolver.unit").exists());
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
        rt.apply().unwrap();
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

    /// Stand up a TLS-auth resolver template that also wants a local
    /// client. Verifies the client config points at the resolver's
    /// installed identity (same cert, key, and trust anchor).
    #[test]
    // `ca` module is unix-only (depends on openssl).
    #[cfg(unix)]
    fn local_client_tls_reuses_resolver_identity() {
        use crate::ca;
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = ca::Ca::init(
            &ca::CaParams {
                directory: ca_dir.path().to_path_buf(),
                subject: ca::Subject::cn("test-ca"),
                san: vec![],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued = ca
            .issue(&ca::IssueParams {
                subject: ca::Subject::cn("resolver.example.com"),
                san: vec![ca::SanEntry::Dns("resolver.example.com".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_src.path().to_path_buf(),
                password: None,
            })
            .unwrap();

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
        // netidx keys identities per administrative trust domain and
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
        let resolver_auth = &rt.resolver_config.as_ref().unwrap().1.0.member_servers[0].auth;
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
        assert_eq!(collect(seeded), collect(&crate::perms::default_seed("/")));
        // Validate the on-disk shape after apply — must parse back
        // into the same map.
        rt.apply().unwrap();
        let loaded =
            crate::perms::load_perms(out.path().join("perms.json")).unwrap();
        assert_eq!(collect(&loaded), collect(&crate::perms::default_seed("/")));
        // And the round-trip-through-resolver-validation step
        // accepts it (the $[user] dynamic entry shape can trip up
        // PMap::from_file if the seed is malformed).
        netidx::resolver_server::config::Config::load(
            out.path().join("resolver.json"),
        )
        .expect("resolver config including auto-seed perms must validate");
    }

    #[test]
    fn perms_seed_emits_separate_file() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
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
            r.0.include_permissions
                .iter()
                .any(|p| p.as_str() == perms_path_str.as_str()),
            "perms_path was not wired into include_permissions: {:?}",
            r.0.include_permissions,
        );
        rt.apply().unwrap();
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
            live.perms()
                .0
                .get("/")
                .and_then(|m| m.get("alice"))
                .map(|s| s.as_str()),
            Some("swlpd"),
        );
    }

    #[test]
    // `ca` module is unix-only (depends on openssl).
    #[cfg(unix)]
    fn tls_resolver_emits_install_job() {
        // Stand up a real CA-issued identity so the round-trip
        // validate path through Config::from_file is meaningful.
        use crate::ca;
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = ca::Ca::init(
            &ca::CaParams {
                directory: ca_dir.path().to_path_buf(),
                subject: ca::Subject::cn("test-ca"),
                san: vec![],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued = ca
            .issue(&ca::IssueParams {
                subject: ca::Subject::cn("resolver"),
                san: vec![ca::SanEntry::Dns("resolver".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_src.path().to_path_buf(),
                password: None,
            })
            .unwrap();

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
    }

    /// Stand up a TLS-auth resolver template that requests id-map
    /// installation. Verifies (a) the resolver config points at the
    /// socket, (b) an `id-map.unit` is emitted, and (c) a starter
    /// `id-map.json` lands on disk after `apply()`.
    #[test]
    // `ca` module is unix-only (depends on openssl).
    #[cfg(unix)]
    fn tls_resolver_with_id_map_emits_unit_and_starter() {
        use crate::ca;
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = ca::Ca::init(
            &ca::CaParams {
                directory: ca_dir.path().to_path_buf(),
                subject: ca::Subject::cn("test-ca"),
                san: vec![],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued = ca
            .issue(&ca::IssueParams {
                subject: ca::Subject::cn("resolver"),
                san: vec![ca::SanEntry::Dns("resolver".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_src.path().to_path_buf(),
                password: None,
            })
            .unwrap();

        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Tls {
            name: ArcStr::from("resolver"),
            certificate: issued.certificate.clone(),
            private_key: issued.private_key.clone(),
            trusted: ca_dir.path().join("certificate.pem"),
            askpass: None,
        };
        p.with_id_map = true;
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

        rt.apply().unwrap();
        // Starter is on disk, both unit files dropped.
        assert!(id_map_path.exists());
        assert!(out.path().join("activation/id-map.unit").exists());
        assert!(out.path().join("activation/resolver.unit").exists());
    }

    /// Re-running `apply()` after the operator has populated the
    /// id-map JSON must not flatten their edits back to empty.
    #[test]
    // `ca` module is unix-only (depends on openssl).
    #[cfg(unix)]
    fn id_map_starter_does_not_clobber_existing_file() {
        use crate::ca;
        let ca_dir = tempfile::tempdir().unwrap();
        let ca = ca::Ca::init(
            &ca::CaParams {
                directory: ca_dir.path().to_path_buf(),
                subject: ca::Subject::cn("test-ca"),
                san: vec![],
                key_bits: 2048,
                validity_days: 30,
            },
            None,
        )
        .unwrap();
        let id_src = tempfile::tempdir().unwrap();
        let issued = ca
            .issue(&ca::IssueParams {
                subject: ca::Subject::cn("resolver"),
                san: vec![ca::SanEntry::Dns("resolver".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_src.path().to_path_buf(),
                password: None,
            })
            .unwrap();

        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.auth = AuthChoice::Tls {
            name: ArcStr::from("resolver"),
            certificate: issued.certificate.clone(),
            private_key: issued.private_key.clone(),
            trusted: ca_dir.path().join("certificate.pem"),
            askpass: None,
        };
        p.with_id_map = true;
        let id_map_path = out.path().join("id-map.json");
        p.id_map_socket = Some(out.path().join("id-map.sock"));
        p.id_map_path = Some(id_map_path.clone());

        // Apply once — starter on disk.
        resolver(&p).unwrap().apply().unwrap();

        // Operator populates the map.
        let mut populated = id_map_engine::empty();
        id_map_engine::upsert_group(&mut populated, "users", 100);
        id_map_engine::upsert_identity(
            &mut populated,
            "resolver.example.com",
            1000,
            "users",
            &[],
        )
        .unwrap();
        id_map_engine::save(&id_map_path, &populated).unwrap();

        // Apply again — populated state must survive.
        resolver(&p).unwrap().apply().unwrap();
        let after = id_map_engine::load(&id_map_path).unwrap();
        assert!(after.lookup_by_name("resolver.example.com").is_some());
        assert!(after.groups.contains_key("users"));
    }

    /// Non-TLS auth must NOT get id-map auto-installation even with
    /// `with_id_map = true`. The id-map daemon only buys you anything
    /// for TLS auth.
    #[test]
    fn id_map_not_installed_for_non_tls_auth() {
        let out = tempfile::tempdir().unwrap();
        let mut p = anon_params(&out);
        p.with_id_map = true;
        let rt = resolver(&p).unwrap();
        assert!(rt.id_map_file.is_none());
        assert!(!rt.units.contains_key("id-map"));
    }
}
