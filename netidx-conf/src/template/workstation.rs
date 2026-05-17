//! Workstation template: local Local-auth resolver under `/local` plus
//! a matching client.
//!
//! Auth model:
//! - The **local resolver** is always Local-auth (peer credentials over
//!   a unix socket). There is no audience for it beyond the user that
//!   ran `init`; TLS/Krb5 there would be theater.
//! - The **parent referral** carries the auth scheme used to talk to
//!   the upstream resolver (per-address in `parent.addrs[i].1`). The
//!   parent's auth — Anonymous, Local, Krb5, or Tls — is the source
//!   of truth for "what auth do I use to follow the parent referral?".
//! - The **client config's `default_auth`** is what publishers on this
//!   workstation accept from incoming subscribers. Defaults to
//!   [`Local`](DefaultAuthMech::Local) because the typical workstation
//!   only exposes publishers to subscribers on the same machine.
//!   Operators who host network publishers override via the
//!   [`default_auth`](WorkstationParams::default_auth) field. The
//!   choice here does *not* affect outbound subscribe behavior —
//!   referrals from the local resolver carry their own per-address
//!   auth.
//! - TLS identities for following TLS referrals are specified via
//!   [`tls_identities`](WorkstationParams::tls_identities). Empty list
//!   for non-TLS upstreams.

use super::*;
use crate::{client::ClientConfig, paths, resolver::ResolverConfig};
use anyhow::Result;
use std::{net::SocketAddr, path::PathBuf};

/// Parameters for [`workstation`].
///
/// Deliberately does **not** derive `Default`. With struct-literal
/// construction at every call site, adding a field forces a compile
/// error there; allowing `..Default::default()` would let new fields
/// slip in silently with a possibly-wrong default. Tests and callers
/// build via the explicit struct-literal form below.
#[derive(Debug, Clone)]
pub struct WorkstationParams {
    /// Parent referral. The parent's per-address auth in
    /// `parent.addrs[i].1: ReferralAuth` is the source of truth for
    /// upstream auth. `None` ⇒ local-only workstation, no upstream.
    pub parent: Option<ParentRef>,
    /// TLS identities to install. Each spec produces one entry in
    /// `client.tls.identities` keyed by its `server_pattern`. Empty
    /// list is fine for non-TLS upstream.
    pub tls_identities: Vec<TlsIdentitySpec>,
    /// `default_auth` for publishers on this workstation. `None` ⇒
    /// [`Local`](DefaultAuthMech::Local). Override when this
    /// workstation hosts publishers that need to accept network
    /// subscribers under Krb5 / Tls.
    pub default_auth: Option<DefaultAuthMech>,
    /// Base path of the local resolver cluster. Default `/local`.
    pub base: ArcStr,
    /// Local resolver TCP port. Default 59200 (matches
    /// `netidx::config::local_only::DEFAULT_PORT`).
    pub listen_port: Option<u16>,
    /// Local-auth socket path. Default
    /// `${dirs::config_dir}/netidx/auth.sock`.
    pub local_socket: Option<PathBuf>,
    /// Output paths. `None` ⇒ standard user-config locations.
    pub client_config_path: Option<PathBuf>,
    pub resolver_config_path: Option<PathBuf>,
    /// Where to drop activation units. `None` ⇒ skip units entirely.
    pub units_dir: Option<PathBuf>,
    /// Absolute path to the `netidx` binary the activation unit
    /// will exec. **Must be absolute** — `netidx-activation` does
    /// not search `$PATH`, so a bare or relative path would fail at
    /// supervisor start. Template-render errors out if it isn't.
    /// The CLI fills this from `std::env::current_exe()` when the
    /// operator doesn't pass `--netidx-binary`.
    pub netidx_binary: PathBuf,
    /// Whether to also emit a `container` activation unit alongside
    /// the resolver. Defaults to `true` — a workstation that runs a
    /// local resolver usually wants a container service as well, and
    /// the marginal cost of an unused unit file is negligible.
    /// Operators who don't want the container service set this
    /// `false`; the CLI exposes it as `--no-container`.
    ///
    /// Ignored when `units_dir` is `None` (which already means "skip
    /// units entirely"), so the two flags compose without needing a
    /// tri-state.
    pub with_container: bool,
}

/// Default port for the local resolver — matches
/// `netidx::config::local_only::DEFAULT_PORT`. Clients of the local
/// resolver need a known address, so this *must* be deterministic.
pub const DEFAULT_LISTEN_PORT: u16 = 59200;

/// Render the workstation template.
pub fn workstation(p: &WorkstationParams) -> Result<RenderedTemplate> {
    let listen_port = p.listen_port.unwrap_or(DEFAULT_LISTEN_PORT);
    let local_sock_path = match &p.local_socket {
        Some(p) => p.clone(),
        None => default_auth_sock()?,
    };
    let resolver_cfg_path = match &p.resolver_config_path {
        Some(p) => p.clone(),
        None => paths::user_resolver_config()?,
    };
    let client_cfg_path = match &p.client_config_path {
        Some(p) => p.clone(),
        None => paths::user_client_config()?,
    };
    // `None` means "skip units entirely" — matches the field doc above.
    // The CLI is responsible for resolving the default activation dir when
    // the operator didn't pass `--no-units`.
    let units_dir = p.units_dir.clone();
    let netidx_binary = p.netidx_binary.clone();
    if !netidx_binary.is_absolute() {
        bail!(
            "netidx_binary must be an absolute path (got {:?}); netidx-activation \
             does not search $PATH",
            netidx_binary,
        );
    }

    let base = if p.base.is_empty() {
        ArcStr::from("/local")
    } else {
        p.base.clone()
    };

    let listen_addr: SocketAddr = SocketAddr::from((
        std::net::Ipv4Addr::LOCALHOST,
        listen_port,
    ));

    // -- Resolver config: one member at 127.0.0.1:<port>, Auth::Local -------
    let local_auth_arc = ArcStr::from(local_sock_path.to_string_lossy().as_ref());
    let resolver_member = rfile::MemberServerBuilder::default()
        .addr(listen_addr)
        .bind_addr(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .auth(rfile::Auth::Local(local_auth_arc.clone()))
        .build()?;

    let mut rcfg_builder = rfile::ConfigBuilder::default();
    rcfg_builder.member_servers(vec![resolver_member]);
    if let Some(parent) = &p.parent {
        // Reject the silent footgun: a TLS-auth parent address with
        // no `tls_identities` means we'd generate a config that
        // validates but fails at runtime when the client tries to
        // follow the referral and finds no identity to present.
        let needs_identity =
            parent.addrs.iter().any(|(_, a)| matches!(a, ReferralAuth::Tls(_)));
        if needs_identity && p.tls_identities.is_empty() {
            bail!(
                "parent referral uses TLS auth but no tls_identities were supplied; \
                 the workstation client would have no cert to present upstream"
            );
        }
        rcfg_builder.parent(parent_into_file(parent.clone()));
    }
    let resolver_cfg = ResolverConfig::from(rcfg_builder.build()?);

    // -- Client config: addrs at the local resolver via Local auth -----------
    //    default_auth: derived from parent.addrs (or override).
    //    tls section: built from tls_identities (one entry per spec).
    let local_addr_auth = cfile::Auth::Local(local_auth_arc);
    let mut ccfg_builder = cfile::ConfigBuilder::default();
    ccfg_builder
        .addrs(vec![(listen_addr, local_addr_auth)])
        .base(base.as_str());
    let default_auth =
        p.default_auth.clone().unwrap_or(DefaultAuthMech::Local);
    if matches!(default_auth, DefaultAuthMech::Tls) && p.tls_identities.is_empty()
    {
        bail!(
            "default_auth=Tls requires at least one tls_identity (the netidx config validator rejects otherwise)"
        );
    }
    ccfg_builder.default_auth(default_auth);
    if let Some(tls) = client_tls_section_from(&p.tls_identities)? {
        ccfg_builder.tls(tls);
    }
    let client_cfg = ClientConfig::from(ccfg_builder.build()?);

    // -- Activation unit: run the resolver-server ---------------------------
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

    // -- Activation unit: run the container service -------------------------
    // `with_container` defaults to true. The api path lives under the
    // workstation's base so a default workstation (base = `/local`)
    // ends up at `/local/container/api`.
    if p.with_container {
        let api_path = if base.as_str() == "/" {
            ArcStr::from("/container/api")
        } else {
            let trimmed = base.trim_end_matches('/');
            compact_str::format_compact!("{trimmed}/container/api").as_str().into()
        };
        let unit = services::container::unit(&services::container::ContainerServiceParams {
            netidx_binary: netidx_binary.clone(),
            api_path,
            db: None,
            compress: false,
            bind: None,
        })?;
        units.insert("container".to_string(), unit);
    }

    // -- TLS install jobs (one per identity) --------------------------------
    let tls_install = p
        .tls_identities
        .iter()
        .map(|s| s.install_job())
        .collect::<Result<Vec<_>>>()?;

    Ok(RenderedTemplate {
        client_config: Some((client_cfg_path, client_cfg)),
        resolver_config: Some((resolver_cfg_path, resolver_cfg)),
        perms_file: None,
        id_map_file: None,
        units,
        units_dir,
        tls_install,
    })
}

fn default_auth_sock() -> Result<PathBuf> {
    let mut p = dirs::config_dir().ok_or_else(|| {
        anyhow!("user config dir could not be determined for this platform")
    })?;
    p.push("netidx");
    p.push("auth.sock");
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn base_params(out: &tempfile::TempDir) -> WorkstationParams {
        WorkstationParams {
            parent: None,
            tls_identities: vec![],
            default_auth: None,
            base: ArcStr::from("/local"),
            listen_port: None,
            local_socket: Some(out.path().join("auth.sock")),
            client_config_path: Some(out.path().join("client.json")),
            resolver_config_path: Some(out.path().join("resolver.json")),
            units_dir: Some(out.path().join("activation")),
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            with_container: true,
        }
    }

    fn parent_addr() -> SocketAddr {
        SocketAddr::from_str("10.0.0.1:5000").unwrap()
    }

    #[test]
    fn container_unit_is_emitted_by_default() {
        let out = tempfile::tempdir().unwrap();
        let rt = workstation(&base_params(&out)).unwrap();
        rt.apply().unwrap();
        let unit_path = out.path().join("activation/container.unit");
        assert!(unit_path.exists(), "container unit missing at {unit_path:?}");
        // The unit must point at the workstation's binary and pass
        // an api-path scoped to the base.
        let bytes = std::fs::read(&unit_path).unwrap();
        let unit: netidx_activation::file::Unit =
            serde_json::from_slice(&bytes).unwrap();
        assert_eq!(unit.process.exe, "/usr/local/bin/netidx");
        let args = unit.process.args.clone();
        assert!(args.contains(&"container".to_string()));
        let api_idx = args.iter().position(|s| s == "--api-path").unwrap();
        assert_eq!(args[api_idx + 1], "/local/container/api");
        // The resolver unit should also still be there.
        assert!(out.path().join("activation/resolver.unit").exists());
    }

    #[test]
    fn with_container_false_skips_container_unit() {
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.with_container = false;
        let rt = workstation(&p).unwrap();
        rt.apply().unwrap();
        // Resolver is still emitted; container is not.
        assert!(out.path().join("activation/resolver.unit").exists());
        assert!(!out.path().join("activation/container.unit").exists());
    }

    #[test]
    fn container_api_path_follows_workstation_base() {
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.base = ArcStr::from("/sites/east");
        let rt = workstation(&p).unwrap();
        rt.apply().unwrap();
        let bytes =
            std::fs::read(out.path().join("activation/container.unit")).unwrap();
        let unit: netidx_activation::file::Unit =
            serde_json::from_slice(&bytes).unwrap();
        let args = unit.process.args;
        let api_idx = args.iter().position(|s| s == "--api-path").unwrap();
        assert_eq!(args[api_idx + 1], "/sites/east/container/api");
    }

    #[test]
    fn rejects_relative_netidx_binary() {
        // netidx-activation doesn't search $PATH — a bare `"netidx"`
        // (the old default) would produce a unit that fails at
        // supervisor start. The engine catches this at render time.
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.netidx_binary = PathBuf::from("netidx");
        let err = workstation(&p).unwrap_err();
        assert!(
            format!("{err:#}").contains("absolute"),
            "expected absolute-path error, got: {err:#}"
        );
    }

    #[test]
    fn units_dir_none_skips_unit_writes() {
        // Engine contract: `units_dir: None` means "skip", not "use the
        // canonical default". Regression for the CLI's `--no-units`
        // flag: when the CLI passes `None`, the template must not
        // synthesize a default activation dir.
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.units_dir = None;
        let rt = workstation(&p).unwrap();
        assert!(rt.units_dir.is_none());
        rt.apply().unwrap();
        // The canonical user activation dir should not have been
        // touched. We only assert the in-tempdir path because the
        // user-home dir is shared global state in CI.
        assert!(!out.path().join("activation").exists());
    }

    #[test]
    fn local_only_no_parent() {
        let out = tempfile::tempdir().unwrap();
        let rt = workstation(&base_params(&out)).unwrap();
        rt.apply().unwrap();

        let r = ResolverConfig::load(out.path().join("resolver.json"))
            .unwrap();
        assert!(r.0.parent.is_none());
        assert!(matches!(r.0.member_servers[0].auth, rfile::Auth::Local(_)));

        let c = client::ClientConfig::load(out.path().join("client.json")).unwrap();
        // No parent ⇒ local-only ⇒ default_auth: Local.
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Local));
        assert!(c.0.tls.is_none());
    }

    #[test]
    fn anonymous_upstream() {
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.parent = Some(ParentRef {
            path: ArcStr::from("/"),
            ttl: None,
            addrs: vec![(parent_addr(), ReferralAuth::Anonymous)],
        });
        let rt = workstation(&p).unwrap();
        rt.apply().unwrap();

        let c = client::ClientConfig::load(out.path().join("client.json")).unwrap();
        // Workstation default_auth is Local regardless of upstream;
        // it's the auth our local publishers accept, not what we use
        // to follow referrals.
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Local));
        assert!(c.0.tls.is_none());

        let r = ResolverConfig::load(out.path().join("resolver.json"))
            .unwrap();
        let parent = r.0.parent.as_ref().unwrap();
        // But the parent referral correctly carries the upstream auth.
        assert!(matches!(parent.addrs[0].1, rfile::RefAuth::Anonymous));
    }

    #[test]
    fn krb5_upstream() {
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.parent = Some(ParentRef {
            path: ArcStr::from("/"),
            ttl: None,
            addrs: vec![(
                parent_addr(),
                ReferralAuth::Krb5(ArcStr::from(
                    "host/resolver.example.com@REALM",
                )),
            )],
        });
        let rt = workstation(&p).unwrap();
        rt.apply().unwrap();

        let c = client::ClientConfig::load(out.path().join("client.json")).unwrap();
        // Default_auth is Local; upstream is followed via the
        // per-referral Krb5 auth, which the runtime picks
        // automatically.
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Local));
        assert!(c.0.tls.is_none());

        let r = ResolverConfig::load(out.path().join("resolver.json"))
            .unwrap();
        let parent = r.0.parent.as_ref().unwrap();
        assert!(matches!(parent.addrs[0].1, rfile::RefAuth::Krb5(_)));
    }

    #[test]
    fn tls_upstream_with_identity() {
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
                subject: ca::Subject::cn("workstation"),
                san: vec![ca::SanEntry::Dns("workstation.example.com".into())],
                key_bits: 2048,
                validity_days: 30,
                out_dir: id_src.path().to_path_buf(),
            })
            .unwrap();

        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.parent = Some(ParentRef {
            path: ArcStr::from("/"),
            ttl: None,
            addrs: vec![(
                parent_addr(),
                ReferralAuth::Tls(ArcStr::from("resolver.example.com")),
            )],
        });
        // Install the TLS identity into a temp dir so `apply()` can
        // actually validate against on-disk files (Config::from_file
        // opens the cert paths).
        let install_dest = out.path().join("installed-tls");
        p.tls_identities = vec![TlsIdentitySpec {
            // Key the identity under the parent's domain so the
            // reverse-domain match in netidx finds it.
            server_pattern: ArcStr::from("example.com"),
            our_name: ArcStr::from("workstation"),
            certificate: issued.certificate.clone(),
            private_key: issued.private_key.clone(),
            trusted: ca_dir.path().join("certificate.pem"),
            dest_dir: Some(install_dest.clone()),
        }];

        let rt = workstation(&p).unwrap();
        assert_eq!(rt.tls_install.len(), 1);
        assert_eq!(rt.tls_install[0].cn, "workstation");

        let (_, c) = rt.client_config.as_ref().unwrap();
        // Still Local — the TLS identity is for outbound referral
        // following, not for what we accept from local subscribers.
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Local));
        let tls = c.0.tls.as_ref().expect("client tls section");
        // Key is the SERVER pattern, not our SAN.
        assert!(tls.identities.contains_key("example.com"));
        assert_eq!(tls.default_identity.as_deref(), Some("example.com"));

        // Exercises the apply() ordering: TLS install must happen
        // before config validation, since validation opens the cert
        // paths from disk.
        rt.apply().unwrap();
        assert!(install_dest.join("certificate.pem").exists());
        assert!(install_dest.join("private.key").exists());
        assert!(install_dest.join("trusted.pem").exists());
    }

    #[test]
    fn tls_parent_without_identity_errors() {
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.parent = Some(ParentRef {
            path: ArcStr::from("/"),
            ttl: None,
            addrs: vec![(
                parent_addr(),
                ReferralAuth::Tls(ArcStr::from("resolver.example.com")),
            )],
        });
        // No tls_identities — should bail at template time, not
        // silently produce a config that fails at runtime.
        let err = workstation(&p).unwrap_err();
        assert!(format!("{err:#}").contains("no tls_identities"));
    }

    #[test]
    fn explicit_default_auth_override_wins() {
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        // A workstation that also hosts a network-facing publisher
        // wants Krb5 as the publisher's accepted scheme.
        p.default_auth = Some(DefaultAuthMech::Krb5);
        let rt = workstation(&p).unwrap();
        let (_, c) = rt.client_config.as_ref().unwrap();
        assert!(matches!(c.0.default_auth, DefaultAuthMech::Krb5));
    }

    #[test]
    fn tls_default_auth_requires_identity() {
        let out = tempfile::tempdir().unwrap();
        let mut p = base_params(&out);
        p.default_auth = Some(DefaultAuthMech::Tls);
        // tls_identities is empty → engine rejects.
        assert!(workstation(&p).is_err());
    }
}
