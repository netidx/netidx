//! The resolver role install: a network-facing resolver server, its data-plane
//! auth (anonymous / krb5 / tls), the id-mapper daemon, and — on a fresh TLS or
//! krb5/anonymous network — the admin-plane CA and this host's admin server.
//!
//! This is the richest of the install cascades: it can mint a CA, enroll from a
//! discovered one, delegate under a WAN parent, and stand up an admin server
//! after apply. Every question, note, and warning goes through the [`Answerer`]
//! seam — no `prompt::`, no `println!`, no per-call tokio runtimes — so the
//! strict CLI, the TUI, and Atlas drive the identical flow.

use super::{
    DEFAULT_RESOLVER_NAME, InstallCommon, detect_resolver_shape, finish_with,
    install_renew_unit, prompt_ip_or_addr, prompt_resolver_own_tls_name,
    resolve_netidx_binary, resolve_units_dir, trust_domain_provenance,
    warn_incomplete_resolver_address,
};

use crate::{
    admin_proto::{InfoAuth, NodeKind, ResolverClusterEdge},
    answer::{Answerer, Field},
    paths,
    plan::{
        AuthKind,
        enroll::{self, AdminServers, DiscoveredTrustDomain, KeyProtArg},
        service::ServiceNeed,
    },
    provenance::{InstallRecord, InstallRole, TrustDomainIdentity},
    service::ServiceScope,
    template::{self, AuthChoice, ParentRef, ReferralAuth, resolver::IdMapMode},
    transport,
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use compact_str::format_compact;
use std::{
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

// The unix-only tail: minting / opening a CA, standing up an admin server, and
// delegating under a WAN parent all depend on unix-only modules.
#[cfg(unix)]
use super::DEFAULT_TLS_DOMAIN;
#[cfg(unix)]
use crate::{
    admin_proto::{ResolverAddr, Role},
    admin_server_config::{AdminServerConfig, IdMapRole, ResolverRole, Roles},
    atomic,
    config_lock::ConfigDirLock,
    fingerprint::Fingerprint,
    offline_ca,
    plan::{
        AdminPlane, admin_plane_decision, ca_setup, delegation, enroll::KeyProtection,
        server_setup,
    },
    tls,
};
#[cfg(unix)]
use zeroize::Zeroizing;

/// Typed inputs for [`run_resolver`] — the resolved form of the clap
/// `ResolverFlags` (the clap struct stays in the CLI frontend). The tools
/// adapter builds [`Self::explicit_parent`] from the `--parent-*` flags when
/// any is set; otherwise it's `None` and the discovery / prompt cascade runs.
pub struct ResolverInput {
    /// Data-plane auth scheme (`None` ⇒ discover or prompt).
    pub auth: Option<AuthKind>,
    /// Kerberos SPN (krb5).
    pub spn: Option<String>,
    /// The resolver's own full TLS SAN (tls).
    pub tls_name: Option<String>,
    /// The resolver's advertised address (`None` ⇒ detect + prompt).
    pub listen: Option<SocketAddr>,
    /// Bind-address override (`None` ⇒ same as `listen`'s ip, or prompted on
    /// a cloud-elastic host).
    pub bind: Option<IpAddr>,
    /// Namespace base path (default `/`).
    pub base: String,
    /// Path to a seed perms.json (`None` ⇒ auto-seed).
    pub perms_seed: Option<PathBuf>,
    /// Where to write the perms file.
    pub perms_path: Option<PathBuf>,
    /// Skip emitting a perms file entirely.
    pub no_perms: bool,
    /// A parent referral fully built from `--parent-*` flags. `Some` ⇒ the
    /// operator specified a parent explicitly; `None` ⇒ no parent (the WAN
    /// delegation path uses `parent_admin_server` instead).
    pub explicit_parent: Option<ParentRef>,
    /// Resolver config output path.
    pub resolver_config_path: Option<PathBuf>,
    /// Activation units dir (`None` ⇒ default; `no_units` in `common` wins).
    pub units_dir: Option<PathBuf>,
    /// The binary the activation units run (`None` ⇒ current exe).
    pub netidx_binary: Option<PathBuf>,
    /// Skip auto-installing the id-mapper daemon.
    pub no_id_map: bool,
    /// For `--auth krb5`, how to map principals to unix ids
    /// (`platform` | `netidx` | `none`). `None` ⇒ prompt (interactive) or a
    /// required-value error (strict). Ignored for other auth schemes.
    pub id_map_mode: Option<String>,
    /// Skip admin-server setup entirely (expert).
    pub no_admin_server: bool,
    /// Explicitly set up an admin server. Only meaningful for an anonymous data
    /// plane, where the admin server is optional (`admin_plane_decision` ⇒
    /// `Ask`): strict mode has no TTY to answer the prompt, so this supplies the
    /// "yes". TLS/krb5 make it mandatory and Local skips it, so it's ignored
    /// there.
    pub with_admin_server: bool,
    /// Proceed even without a usable TPM / Secure Enclave (test CAs only).
    pub insecure_no_tpm: bool,
    /// Set this resolver up as a CHILD of an existing network: the parent's
    /// admin-server address. Unix-only.
    pub parent_admin_server: Option<SocketAddr>,
    /// The subtree this resolver will own under the WAN parent (e.g. `/eu`).
    pub delegate_subtree: Option<String>,
    /// Private-key protection choice.
    pub key_protection: Option<KeyProtArg>,
    /// Override the id-map socket path.
    pub id_map_socket: Option<PathBuf>,
    /// Override the id-map JSON path.
    pub id_map_path: Option<PathBuf>,
    /// Skip writing a client.json pointing at this resolver.
    pub no_client: bool,
    /// Override the client config path.
    pub client_config_path: Option<PathBuf>,
    /// Install-wide flags.
    pub common: InstallCommon,
}

/// Install a standalone resolver, returning the OS-service scope the frontend
/// should register (system scope — a resolver is a network-facing daemon), or
/// `None`.
pub async fn run_resolver(
    ans: &mut dyn Answerer,
    mut input: ResolverInput,
) -> Result<Option<ServiceScope>> {
    // Ask the network before asking the human: a second (or third…)
    // resolver discovers the existing network and imports its settings
    // — auth scheme, domain, where the CA is, and which one cluster it belongs
    // to. Resolver member blocks are local launch choices, not peer links, so
    // a joining replica can keep a one-member local config.
    // The probe outcome rides through the whole install: once the
    // operator has said "no admin server", nothing downstream offers a
    // network join again.
    let probe = if let Some(parent) = input.parent_admin_server {
        if input.common.mode.is_dry_run() {
            // dry-run can't run the live confirm; the parent match below
            // bails on dry-run with a clear message.
            AdminServers::NotProbed
        } else {
            // An explicit WAN parent (no mDNS): confirm it and pin the
            // network. This makes `probe.have()` Some, so the install
            // ENROLLS this resolver's cert from the parent's CA and the
            // "create a local CA" branches become unreachable — a satellite
            // shares the one trust domain, it never mints its own.
            enroll::confirm_trust_domain_at(ans, parent, NodeKind::Resolver).await?
        }
    } else if input.auth.is_none() && !input.common.mode.is_dry_run() {
        enroll::discover_trust_domain(ans, NodeKind::Resolver).await?
    } else {
        AdminServers::NotProbed
    };
    if input.parent_admin_server.is_none()
        && input.base == "/"
        && let Some(base) = probe.have().and_then(|net| net.info.resolver_base.as_ref())
    {
        input.base = base.clone();
    }
    // Interactive delegation offer: if we joined an EXISTING network that runs a
    // resolver, offer to become a delegated SUBTREE of it (its own /path,
    // referred up to the parent) instead of a plain peer member of the root
    // cluster. Gated on `Role::Resolver` — there must be an upstream resolver to
    // delegate FROM, else the option is meaningless. The strict CLI drives this
    // with --parent-admin-server/--delegate-subtree, so it only fires
    // interactively and when neither is already set. Setting these three inputs
    // routes the rest of the install down the existing delegated-child path.
    #[cfg(unix)]
    if ans.interactive()
        && input.parent_admin_server.is_none()
        && input.delegate_subtree.is_none()
        && let Some(net) = probe.have()
        && net.identity.roles.contains(Role::Resolver)
        && let Some(parent_addr) = net.info.reached.first().copied()
    {
        if let Some(subtree) = ans
            .text(Field::DelegateSubtree, None, None, false)
            .await?
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        {
            input.parent_admin_server = Some(parent_addr);
            input.delegate_subtree = Some(subtree.clone());
            input.base = subtree;
        }
    }
    // A delegated child (`--parent-admin-server`) keeps the data-plane auth
    // the operator chose — a /eu subtree may run krb5 under a TLS parent —
    // so only the trust-domain/CA decision comes from the parent, never its
    // auth. A plain discovered peer still imports its cluster's scheme. The
    // auth *choice* itself is deferred until after the control plane (below):
    // founding a new cluster sets up the CA first, then asks how the data
    // plane authenticates.
    let imported_auth = if input.parent_admin_server.is_some() {
        None
    } else {
        probe.have().and_then(trust_domain_auth_kind)
    };
    // Founding a brand-new cluster stands up the control plane (CA + admin
    // server) up front — before the data-plane auth is chosen (see the control
    // plane block below, after this host's address is resolved).
    #[cfg(unix)]
    let founding_new_cluster = probe.have().is_none()
        && input.parent_admin_server.is_none()
        && !input.common.mode.is_dry_run()
        && !ca_setup::default_ca_present().await;
    // Frame the whole "new cluster" install up front, before any machine or CA
    // questions, so they have context: administering netidx is always
    // authenticated over TLS by the CA no matter how the data plane
    // authenticates, so the control plane comes first and the data-plane auth
    // choice is deferred. `--no-admin-server` is the expert escape to a
    // control-plane-less resolver (never chosen interactively).
    #[cfg(unix)]
    let founded_admin_plane = founding_new_cluster && !input.no_admin_server;
    #[cfg(unix)]
    if founded_admin_plane {
        ans.announce(
            "New admin cluster",
            "Founding a new admin cluster requires a certificate authority. We \
             will set that up now.",
        )
        .await?;
    }
    // Shape detection (incl. cloud-metadata probe) only fires when we actually
    // need a default — i.e. when `--listen` or `--bind` weren't given
    // explicitly. Computed once, up front, and shared by the admin server's
    // listen prompt (control plane) and the resolver's (data plane).
    let shape: Option<super::ResolverShape> =
        if input.listen.is_none() || input.bind.is_none() {
            Some(detect_resolver_shape().await)
        } else {
            None
        };
    // This host's advertised IP, shared by the admin server (control plane) and
    // the resolver (data plane): an explicit `--listen` pins it, else shape
    // detection suggests it. Each component's own listen prompt defaults to it,
    // so the admin server's address is asked (and defaulted) up front, before
    // the resolver's.
    let machine_ip: Option<IpAddr> = input
        .listen
        .map(|l| l.ip())
        .or_else(|| shape.as_ref().and_then(|s| s.advertised_ip));
    // Resolve the activation units dir up front: if the flow stands up a CA
    // server, its `ca.unit` must land in the *same* dir as the resolver / id-map
    // units so the one supervisor runs them all.
    let units_dir = resolve_units_dir(input.common.no_units, input.units_dir.as_deref())?;
    // ── Control plane ── For a new cluster the CA and this host's admin server
    // are set up FIRST, before the data-plane auth is even chosen. Administering
    // netidx — this admin tool, discovery, enrollment, and certificate renewal —
    // is always authenticated over TLS by the CA, no matter how the *data* plane
    // authenticates; the control plane is not optional and does not depend on the
    // data-plane choice, so asking auth first only invites the "I picked
    // anonymous, why a CA?" confusion. `--no-admin-server` is the expert escape
    // to a control-plane-less resolver (never chosen interactively). The domain
    // is a control-plane fact (the CA's `ca.<domain>` CN) and flows into the data
    // plane below so a founding TLS resolver's own name shares it.
    // The cluster this resolver belongs to, recorded in install.json so its
    // status view can show the cluster glyph: a founding resolver records the
    // cluster it just created (its own domain + CA fingerprint); a joining one
    // pins the discovered network's identity via `trust_domain_provenance` below.
    #[allow(unused_mut)]
    let mut founding_identity: Option<TrustDomainIdentity> = None;
    #[cfg(unix)]
    let control_plane_domain: Option<String> = if founded_admin_plane {
        // The network domain — the CA's `ca.<domain>` CN, and the domain the
        // founding TLS resolver's own SAN reuses. Default it from an explicit
        // `--tls-name` when present (strict TLS derives the domain from the
        // resolver's SAN, so the CA and the cert agree); else the conventional
        // default.
        let domain_default = input
            .tls_name
            .as_deref()
            .and_then(|n| tls::domain_from_san(n).ok())
            .map(|d| d.to_string())
            .unwrap_or_else(|| DEFAULT_TLS_DOMAIN.to_string());
        let domain = ans
            .text(Field::TrustDomainName, None, Some(&domain_default), false)
            .await?
            .unwrap_or(domain_default);
        // The founding controller/CA + this host's admin server + the superuser admin. The
        // returned `ServiceNeed` is intentionally dropped: this install ends with
        // one system-service offer, and the admin-server unit lands in the shared
        // units dir. `Some(true)` — a new cluster always stands up its admin
        // server (the `--no-admin-server` escape is handled by the gate above).
        let (_ca, _need, identity) = super::controller::create_self_signed_controller(
            ans,
            input
                .common
                .mode
                .config_lock()
                .context("resolver apply mode has no config-directory lock")?,
            domain.clone(),
            None,
            machine_ip,
            units_dir.clone(),
            input.insecure_no_tpm,
        )
        .await?;
        // Record the cluster this host just founded, so it reads back like a
        // joined cluster everywhere downstream (status glyph, saved-cluster list).
        founding_identity = Some(identity);
        ans.announce(
            "Resolver server",
            "Admin cluster setup complete, now installing the resolver server.",
        )
        .await?;
        Some(domain)
    } else {
        None
    };
    #[cfg(not(unix))]
    let control_plane_domain: Option<String> = None;
    // ── Data plane ── the auth scheme (imported when joining a network, else
    // chosen now that the control plane exists) and the resolver's own identity
    // for it. On the founding TLS path the CA was created above, so
    // `resolver_self_auth` only issues this resolver's certificate from it.
    let kind: AuthKind = match imported_auth {
        Some(k) => {
            ans.note(&format_compact!(
                "importing auth scheme from the cluster: {}",
                k.as_str()
            ));
            k
        }
        None => ans
            .choice(
                Field::Auth,
                input.auth.map(|k| k.as_str().to_string()),
                &["anonymous", "krb5", "tls"],
                Some("tls"),
            )
            .await?
            .parse()?,
    };
    input.auth = Some(kind);
    // `_tls_staging` holds the TLS-issuance staging tempdir (Some only on the
    // local-CA-issue path). It must outlive `finish_with` below so the issued
    // cert/key survive until `apply()` copies them into place.
    let ResolvedAuth { choice: auth, staging: _tls_staging, netidx_ca } =
        match probe.have() {
            Some(net) => resolver_auth_from_trust_domain(ans, &input, net, kind).await?,
            None => {
                resolver_self_auth(
                    ans,
                    &input,
                    machine_ip,
                    units_dir.as_deref(),
                    &probe,
                    control_plane_domain.as_deref(),
                )
                .await?
            }
        };
    // ── This host's resolver address (data plane) ── asked only now, after the
    // control plane's admin-server address and the auth choice. Its IP defaults
    // to the same `machine_ip` the admin server took, so on a single-homed host
    // the operator just confirms it.
    let listen: SocketAddr = if let Some(l) = input.listen {
        l
    } else {
        let s = shape.as_ref().expect("shape detected when --listen/--bind absent");
        warn_incomplete_resolver_address(ans, s);
        // Accept either a bare IP (then ask the port) or a full host:port. The
        // IP is the one thing the operator has to know; the port defaults.
        let default_ip = machine_ip.map(|ip| ip.to_string());
        prompt_ip_or_addr(ans, Field::Listen, default_ip.as_deref(), default_ip.is_none())
            .await?
            .context("an advertised address is required (pass --listen)")?
    };
    // Bind: silent in the normal case (defaults to listen.ip()), but level-1
    // prompted in the cloud-elastic case where the resolver advertises a public
    // IP while binding to a private NIC.
    let bind = if let Some(b) = input.bind {
        Some(b)
    } else {
        let s = shape.as_ref().expect("shape detected when --listen/--bind absent");
        match s.bind_override {
            Some(private) => {
                let default = private.to_string();
                let b: IpAddr = ans
                    .text(Field::Bind, None, Some(&default), false)
                    .await?
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.parse::<IpAddr>())
                    .transpose()
                    .context("invalid bind address")?
                    .unwrap_or(private);
                Some(b)
            }
            None => None,
        }
    };
    // Local-client bind override: when the resolver listens on a public IP while
    // binding to a private NIC (cloud-elastic), the local client's publisher
    // must advertise the public IP but bind the private subnet. Only kicks in
    // when shape was actually detected.
    let local_client_bind =
        shape.as_ref().and_then(|s| s.elastic_local_client_bind.clone());
    let perms_seed = match &input.perms_seed {
        Some(p) => Some(crate::perms::load_perms(p)?),
        None => None,
    };
    let id_map =
        resolve_id_map_choice(ans, &auth, input.no_id_map, input.id_map_mode.clone())
            .await?;
    let no_admin_server = input.no_admin_server;
    let with_admin_server = input.with_admin_server;
    // The admin-server step after apply() needs the *actual* config paths this
    // install produces — resolve the template's defaults the same way it will.
    let resolver_config_actual = match &input.resolver_config_path {
        Some(p) => p.clone(),
        None => crate::resolver::default_save_path()?,
    };
    // Only the netidx id-mapper writes an id-map.json the post-apply step needs
    // to know about; Platform / None have no such file.
    let id_map_actual = if matches!(id_map, IdMapMode::Netidx) {
        Some(match &input.id_map_path {
            Some(p) => p.clone(),
            None => crate::id_map::user_id_map_path()?,
        })
    } else {
        None
    };
    let post_apply_units_dir = units_dir.clone();
    // Build the install record before the post-apply closure moves `probe`. A
    // founding resolver records the cluster it just created; a joining one pins
    // the discovered network's identity and a reachable admin-server address.
    let (network, admin_server) = match founding_identity {
        Some(id) => (Some(id), None),
        None => trust_domain_provenance(&probe),
    };
    let record = InstallRecord::new(
        InstallRole::Resolver,
        input.base.clone(),
        input.auth.map(|k| k.as_str()).unwrap_or("tls"),
        network,
        admin_server,
    );
    // Install-time child: collect the delegation request now, but do not send it
    // until after apply() has written the resolver config and the post-apply
    // step has enrolled this host's admin server. The CA owns cluster identity,
    // so delegation must reference that already-enrolled pending cluster; the
    // previous order sent the request first and was correctly rejected by the
    // controller because no child cluster existed yet.
    #[cfg(unix)]
    let mut install_delegation = None;
    // `--parent-admin-server` is also the strict/non-mDNS way to name the
    // existing network we are joining.  It only means "delegated child" when
    // a subtree was supplied; without one this resolver is a peer in the
    // cluster at `input.base`, exactly like the interactive blank-subtree
    // choice above.
    let delegated_child = input.delegate_subtree.is_some();
    // An explicit bootstrap can be a controller in some other cluster (the
    // common strict-CLI case is adding EU-B through HQ-A). Discovery facts are
    // intentionally scoped to that bootstrap server's own cluster, so select
    // the requested peer cluster from the verified controller map before
    // rendering referrals. Otherwise HQ-A's `/eu` child would become a
    // nonsensical self-child on a resolver whose own base is `/eu`.
    let authoritative_peer_topology = if !delegated_child
        && input.parent_admin_server.is_some()
        && probe.have().and_then(|net| net.info.resolver_base.as_deref())
            != Some(input.base.as_str())
    {
        let net =
            probe.have().context("the explicit bootstrap network was not verified")?;
        let controller = net
            .info
            .ca_addr
            .context("the verified network reported no controller address")?;
        let map =
            transport::get_map_pinned(controller, NodeKind::Resolver, &net.identity)
                .await
                .context("fetching the authoritative map for peer-cluster topology")?;
        Some(transport::cluster_topology_by_base(&map, &input.base)?)
    } else {
        None
    };
    let mut parent = match input.parent_admin_server {
        None => input.explicit_parent.take(),
        Some(parent_conf) if delegated_child => {
            #[cfg(unix)]
            {
                if input.common.mode.is_dry_run() {
                    // delegate_under_parent runs the real ceremony — it
                    // enqueues a request on the parent and blocks until a
                    // remote admin approves (which mutates the parent
                    // cluster). That is not a no-op, so it cannot honor
                    // --dry-run's "write nothing" contract.
                    bail!(
                        "--dry-run can't preview an install-time delegation: \
                         --parent-admin-server runs a live, interactive approval \
                         ceremony with the parent admin (it enqueues a request on \
                         the parent and blocks until they approve). Re-run without \
                         --dry-run, or drop --parent-admin-server to preview a \
                         standalone install."
                    );
                }
                let child_auth = authchoice_to_info(&auth)?;
                let child = vec![ResolverAddr { addr: listen, auth: child_auth }];
                let subtree = ans
                    .text(
                        Field::DelegateSubtree,
                        input.delegate_subtree.clone(),
                        None,
                        true,
                    )
                    .await?
                    .context(
                        "a subtree this resolver will own under the parent is required",
                    )?;
                // The probe already glyph-confirmed this parent (it had to,
                // to enroll our cert from its CA), so pass that identity in —
                // the operator confirms the parent's glyph exactly once.
                install_delegation = Some((
                    parent_conf,
                    subtree,
                    child,
                    probe.have().map(|n| n.identity.clone()),
                ));
                None
            }
            #[cfg(not(unix))]
            {
                let _ = parent_conf;
                bail!("delegation (--parent-admin-server) is unix-only")
            }
        }
        Some(_) => input.explicit_parent.take(),
    };
    if parent.is_none()
        && !delegated_child
        && let Some(edge) = authoritative_peer_topology
            .as_ref()
            .and_then(|topology| topology.parent.as_ref())
            .or_else(|| probe.have().and_then(|net| net.info.resolver_parent.as_ref()))
    {
        parent = Some(edge_to_parent_ref(edge));
    }
    let joining_children = if !delegated_child {
        authoritative_peer_topology
            .as_ref()
            .map(|topology| topology.children.clone())
            .or_else(|| probe.have().map(|net| net.info.resolver_children.clone()))
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let params = template::resolver::ResolverParams {
        auth,
        base: ArcStr::from(input.base),
        listen,
        bind,
        parent,
        perms_seed,
        with_perms_file: !input.no_perms,
        perms_path: input.perms_path,
        resolver_config_path: input.resolver_config_path,
        units_dir,
        netidx_binary: resolve_netidx_binary(input.netidx_binary)?,
        id_map,
        id_map_path: input.id_map_path,
        id_map_socket: input.id_map_socket,
        with_local_client: !input.no_client,
        client_config_path: input.client_config_path,
        local_client_bind,
    };
    // A delegated resolver's runtime base is encoded by its parent referral,
    // which is deliberately written only after the CA approves delegation.
    // Preserve the requested base separately so the preceding server-enrollment
    // grant creates the pending cluster at that intended path rather than
    // misreading the temporarily parentless config as the root cluster.
    #[cfg(unix)]
    let resolver_base = params.base.to_string();
    let mut rt = template::resolver(&params)?;
    if !joining_children.is_empty() {
        let (_, resolver) = rt
            .resolver_config
            .as_mut()
            .context("the resolver install did not render a resolver config")?;
        resolver.as_file_mut().children =
            joining_children.into_iter().map(edge_into_file).collect();
    }
    // A standalone resolver is a network-facing daemon — system-scope is what
    // makes it boot-triggered and visible to the OS.
    finish_with(
        ans,
        rt,
        &input.common,
        ServiceNeed::at(ServiceScope::System),
        record,
        // Admin-server step, after the configs it points at exist: a
        // discovered network ⇒ enroll a new admin server here; a fresh
        // network ⇒ add this host's roles to the config the CA setup wrote.
        // Then the renewal daemon, on any host with certificates our CA can
        // renew (a netidx-CA-issued resolver identity, or a admin-server
        // serving cert).
        async move |ans, config_lock| {
            #[cfg(unix)]
            let admin_server_ready = post_apply_admin_server(
                ans,
                probe.have(),
                kind,
                no_admin_server,
                with_admin_server,
                listen,
                resolver_base,
                post_apply_units_dir.as_deref(),
                resolver_config_actual.clone(),
                id_map_actual,
                config_lock,
            )
            .await?;
            #[cfg(unix)]
            if let Some((parent_conf, subtree, child, confirmed)) = install_delegation {
                if !admin_server_ready {
                    bail!(
                        "the child admin server was not enrolled, so its pending \
                         CA-owned cluster cannot be delegated"
                    );
                }
                let parent_addrs = delegation::delegate_under_parent(
                    ans,
                    parent_conf,
                    &subtree,
                    &child,
                    None,
                    confirmed.as_ref(),
                )
                .await?;
                let parent_ref = ParentRef {
                    path: ArcStr::from(subtree.as_str()),
                    ttl: None,
                    addrs: parent_addrs
                        .into_iter()
                        .map(|r| (r.addr, delegation::info_to_referral_auth(&r.auth)))
                        .collect(),
                };
                let update =
                    template::set_parent_referral(&resolver_config_actual, parent_ref)?;
                ans.note(&update.describe());
                update
                    .apply(config_lock)
                    .context("writing the approved parent referral")?;
            }
            #[cfg(not(unix))]
            {
                let _ = (&probe, kind, no_admin_server, with_admin_server, listen);
                let _ = (resolver_config_actual, id_map_actual);
            }
            if let Some(d) = post_apply_units_dir.as_deref()
                && (netidx_ca || paths::discover_admin_server_config().is_ok())
            {
                install_renew_unit(ans, d)?;
            }
            Ok(())
        },
    )
    .await
}

/// Decide how the resolver maps authenticated identities to unix uid/gid (see
/// [`IdMapMode`]). `no_id_map` forces `Platform` (the historical "no daemon,
/// /bin/id" behaviour). TLS always installs the netidx id-mapper — cert SANs
/// have no `/bin/id` translation, so any other choice denies every non-anonymous
/// operation. Krb5 is a three-way question: platform (a site IdM resolves full
/// principals), netidx (map them yourself), or none (perms keyed on the raw
/// principal). Anonymous and Local don't use the daemon.
async fn resolve_id_map_choice(
    ans: &mut dyn Answerer,
    auth: &AuthChoice,
    no_id_map: bool,
    id_map_mode: Option<String>,
) -> Result<IdMapMode> {
    if no_id_map {
        return Ok(IdMapMode::Platform);
    }
    match auth {
        AuthChoice::Tls { .. } => {
            ans.note(
                "installing the netidx id-mapper daemon (maps TLS cert identities \
                 to unix uids; skip with --no-id-map)",
            );
            Ok(IdMapMode::Netidx)
        }
        AuthChoice::Krb5 { .. } => {
            let choice = ans
                .choice(
                    Field::IdMapMode,
                    id_map_mode,
                    &["platform", "netidx", "none"],
                    Some("platform"),
                )
                .await?;
            Ok(match choice.as_str() {
                "netidx" => IdMapMode::Netidx,
                "none" => IdMapMode::None,
                _ => IdMapMode::Platform,
            })
        }
        AuthChoice::Anonymous | AuthChoice::Local { .. } => Ok(IdMapMode::Platform),
    }
}

/// The resolver's resolved auth identity. `staging` is the TLS-issuance staging
/// guard (see [`resolver_tls_generate`]); the caller must keep it alive until
/// `apply()` has run. `netidx_ca` ⇒ the TLS identity chains to this network's
/// netidx CA (issued locally or network-joined), so the renewal daemon can
/// renew it; false for external-PKI identities and for all non-TLS schemes.
struct ResolvedAuth {
    choice: AuthChoice,
    staging: Option<tempfile::TempDir>,
    netidx_ca: bool,
}

impl ResolvedAuth {
    fn external(choice: AuthChoice) -> Self {
        ResolvedAuth { choice, staging: None, netidx_ca: false }
    }
}

/// A best-effort default krb5 SPN for this resolver, `netidx/<fqdn>@<REALM>` —
/// realm from `/etc/krb5.conf`'s `default_realm`. `None` when the realm can't
/// be read: without it there's no useful default, so the operator must supply
/// the whole SPN.
///
/// Kerberos service principals are FQDN-based by convention
/// (`service/host.domain@REALM`). The local hostname is usually the short form,
/// so qualify it: keep it as-is if it already has a domain, otherwise borrow the
/// realm's domain (the krb5 convention is realm == the upper-cased DNS domain).
async fn default_krb5_spn() -> Option<String> {
    let krb5 = tokio::fs::read_to_string("/etc/krb5.conf").await.ok()?;
    let realm = krb5.lines().find_map(|l| {
        l.trim()
            .strip_prefix("default_realm")
            .and_then(|r| r.trim_start().strip_prefix('='))
            .map(|v| v.trim().to_string())
    })?;
    let host = enroll::current_hostname().unwrap_or_else(|| "resolver".to_string());
    let fqdn = if host.contains('.') {
        host
    } else {
        format!("{host}.{}", realm.to_lowercase())
    };
    Some(format!("netidx/{fqdn}@{realm}"))
}

/// Resolve the resolver's own auth choice. `input.auth` was resolved (with a
/// level-1 prompt) upstream in [`run_resolver`]; treat it as guaranteed-Some.
/// The per-scheme sub-args are level-2 prompts — once the operator has chosen a
/// scheme, the things that scheme needs are not optional.
async fn resolver_self_auth(
    ans: &mut dyn Answerer,
    input: &ResolverInput,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
    probe: &AdminServers,
    control_plane_domain: Option<&str>,
) -> Result<ResolvedAuth> {
    let auth = input.auth.expect("auth resolved before resolver_self_auth");
    match auth {
        AuthKind::Anonymous => Ok(ResolvedAuth::external(AuthChoice::Anonymous)),
        AuthKind::Local => bail!(
            "the resolver template does not support local auth: local (unix-socket) \
             auth only authenticates clients on the same machine, so it cannot serve \
             a network. For a single-machine setup use `netidx admin workstation \
             install`; for a network resolver choose anonymous, krb5, or tls."
        ),
        AuthKind::Krb5 => {
            let spn = match default_krb5_spn().await {
                Some(def) => {
                    let answer = ans
                        .text(Field::Spn, input.spn.clone(), Some(&def), false)
                        .await?;
                    answer.unwrap_or(def)
                }
                None => ans
                    .text(Field::Spn, input.spn.clone(), None, true)
                    .await?
                    .context("a kerberos SPN is required")?,
            };
            Ok(ResolvedAuth::external(AuthChoice::Krb5 {
                spn: ArcStr::from(spn.as_str()),
            }))
        }
        AuthKind::Tls => {
            resolver_tls_auth(
                ans,
                input,
                default_ca_ip,
                units_dir,
                probe,
                control_plane_domain,
            )
            .await
        }
    }
}

/// Resolve the resolver's TLS identity via the netidx CA — the only in-wizard
/// path. On unix this host either enrolls from a discovered admin server or
/// creates the network's CA and issues its own cert (both inside
/// [`resolver_tls_generate`]). On non-unix, where creating a CA needs openssl,
/// it can only enroll over the admin plane.
async fn resolver_tls_auth(
    ans: &mut dyn Answerer,
    input: &ResolverInput,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
    probe: &AdminServers,
    control_plane_domain: Option<&str>,
) -> Result<ResolvedAuth> {
    // When the control plane was just founded, its domain is fixed (the CA
    // issues `*.<domain>`), so the resolver's own TLS name reuses it and we
    // prompt only for the leftmost label instead of re-asking the domain.
    let name =
        prompt_resolver_own_tls_name(ans, input.tls_name.clone(), control_plane_domain)
            .await?;
    #[cfg(unix)]
    let res =
        resolver_tls_generate(ans, input, &name, default_ca_ip, units_dir, probe).await;
    #[cfg(not(unix))]
    let res = {
        // Creating a CA needs openssl (unix only), so a non-unix resolver can
        // only *enroll* over the admin plane. No admin server ⇒ nothing the
        // wizard can do: point at self-manage.
        let _ = (default_ca_ip, units_dir);
        match enroll::maybe_join_ca_server(
            ans,
            probe,
            NodeKind::Resolver,
            Some(name.as_str()),
            input.key_protection,
        )
        .await?
        {
            Some((j, staging)) => Ok(ResolvedAuth {
                choice: enroll::joined_to_auth(j),
                staging: Some(staging),
                netidx_ca: true,
            }),
            None => bail!(
                "a TLS resolver identity requires a reachable admin server to \
                 enroll against (creating a CA is unix-only). To run TLS without \
                 a admin server, configure the resolver's TLS identity by hand."
            ),
        }
    };
    res
}

/// Issue a resolver certificate from the local CA, creating the CA first if
/// there isn't one. Returns an [`AuthChoice::Tls`] pointing at the issued files,
/// plus an optional staging-dir guard.
///
/// The local-CA-issue path issues into a **staging tempdir** rather than the
/// canonical install location, and returns that `TempDir` so the caller can keep
/// it alive until `apply()` has copied the files into place.
///
/// Under `--dry-run` this issues nothing — it reports what it would do and
/// returns the *intended* canonical paths (and no staging dir).
#[cfg(unix)]
async fn resolver_tls_generate(
    ans: &mut dyn Answerer,
    input: &ResolverInput,
    name: &str,
    default_ca_ip: Option<IpAddr>,
    units_dir: Option<&Path>,
    probe: &AdminServers,
) -> Result<ResolvedAuth> {
    // First the network path: a admin server signs our CSR on the spot.
    // Whether this asks anything is decided by `probe`. The issued files are
    // written to a staging tempdir; we hand it back so the caller can hold it
    // across the template install.
    if !input.common.mode.is_dry_run()
        && let Some((j, staging)) = enroll::maybe_join_ca_server(
            ans,
            probe,
            NodeKind::Resolver,
            Some(name),
            input.key_protection,
        )
        .await?
    {
        return Ok(ResolvedAuth {
            choice: enroll::joined_to_auth(j),
            staging: Some(staging),
            netidx_ca: true,
        });
    }
    let ca_dir = paths::user_ca_dir()?;
    let ca_cert = ca_dir.join("certificate.pem");
    // `identity_dir` also validates `name` (no path separators) — do it up
    // front. It is the *canonical* install location, even though issuance
    // itself writes to the staging dir below.
    let identity_dir = tls::identity_dir(name)?;

    if input.common.mode.is_dry_run() {
        if ca_setup::default_ca_present().await {
            ans.note(&format_compact!(
                "[dry-run] would issue a resolver certificate {name:?} from the \
                 local CA at {}",
                ca_dir.display(),
            ));
        } else {
            ans.note(&format_compact!(
                "[dry-run] would create a new local CA at {}, then issue a resolver \
                 certificate {name:?} from it",
                ca_dir.display(),
            ));
        }
        return Ok(ResolvedAuth {
            choice: AuthChoice::Tls {
                name: ArcStr::from(name),
                certificate: identity_dir.join("certificate.pem"),
                private_key: identity_dir.join("private.key"),
                trusted: ca_cert,
                askpass: None,
            },
            staging: None,
            netidx_ca: true,
        });
    }

    let config_lock = input
        .common
        .mode
        .config_lock()
        .context("resolver apply mode has no config-directory lock")?;
    let ca = if ca_setup::default_ca_present().await {
        ans.note(&format_compact!("issuing from the local CA at {}", ca_dir.display()));
        offline_ca::open_default_ca(ans, config_lock).await?
    } else {
        // No CA — this is the first resolver of a new TLS network, so the CA
        // is created right here: it signs the data plane *and* anchors the
        // admin plane. Belt-and-suspenders: a resolver told about a parent
        // admin server must enroll from that network's CA, never mint its own.
        // The probe (confirm_trust_domain_at) already routes such installs to the
        // enroll path, so reaching here with a parent set would be a bug.
        if input.parent_admin_server.is_some() {
            bail!(
                "about to create a local CA while --parent-admin-server is set; a \
                 delegated child must enroll from the parent's CA, not create its \
                 own trust domain (internal: the probe should have prevented this)"
            );
        }
        ans.note(&format_compact!(
            "no CA found at {} — creating the cluster's CA (it signs this \
             resolver's certificate and anchors discovery, enrollment, and \
             renewal)",
            ca_dir.display()
        ));
        // We already know the domain from the resolver's TLS name (e.g.
        // `resolver.ryu-oh.org` → `ryu-oh.org`), so name the CA `ca.<domain>`
        // and pass the domain through so the first admin's policy defaults to
        // `*.<domain>`.
        let domain = tls::domain_from_san(name)
            .map(|d| d.to_string())
            .unwrap_or_else(|_| name.to_string());
        ca_setup::announce_founding_policy(ans, &domain);
        let setup_server =
            match admin_plane_decision(AuthKind::Tls, input.no_admin_server) {
                AdminPlane::Mandatory => Some(true),
                AdminPlane::Skip => Some(false),
                // TLS is never a question — see the matrix.
                AdminPlane::Ask => unreachable!("tls admin plane is not Ask"),
            };
        // The CA co-locates with this resolver — suggest its IP for the admin
        // server's listen address. The returned `ServiceNeed` is intentionally
        // dropped: this install always ends with a single system-service offer.
        let (created, _need) = ca_setup::create_vaulted_ca(
            ans,
            config_lock,
            ca_setup::founding_ca_opts(
                ca_dir.clone(),
                domain,
                input.insecure_no_tpm,
                setup_server,
                default_ca_ip,
                units_dir.map(|p| p.to_path_buf()),
            ),
        )
        .await?;
        created
    };
    // Local-CA-issue path: choose how the leaf key is protected at rest. The
    // askpass goes into the emitted client config as the password case's
    // fallback.
    let key_path = identity_dir.join("private.key");
    let protection =
        enroll::choose_key_protection(ans, input.key_protection, &key_path, name).await?;
    // Issue into a staging dir, not the canonical location: `apply()` is the
    // only thing that should write under the config tree.
    let staging = tempfile::TempDir::new().context("creating tls staging dir")?;
    ans.note(&format_compact!(
        "issuing resolver certificate {name:?} (this may take a moment)…"
    ));
    let issued = ca_setup::issue_identity(
        config_lock,
        &ca,
        name,
        staging.path().to_path_buf(),
        protection.password(),
        &["users".to_string()],
    )
    .await?;
    // The sealed password rides beside the staged key; apply()'s identity
    // install copies sidecars with their keys.
    protection.write_sidecar(&issued.private_key)?;
    // Show the *installed* paths apply() will create, not the transient staging
    // paths the files currently sit in.
    ans.note(&format_compact!(
        "issued resolver certificate:\n\
         \x20 name:        {name}\n\
         \x20 certificate: {}\n\
         \x20 private key: {}\n\
         \x20 trusted CA:  {}",
        identity_dir.join("certificate.pem").display(),
        identity_dir.join("private.key").display(),
        ca_cert.display(),
    ));
    match &protection {
        KeyProtection::Sealed { .. } => ans.note(&format_compact!(
            "  private key is encrypted; the password is sealed to this machine's \
             {} beside it.",
            netidx_tpm::MECHANISM
        )),
        KeyProtection::Password { .. } => {
            ans.note("  private key is encrypted; password saved to the system keychain.")
        }
        KeyProtection::None => (),
    }
    Ok(ResolvedAuth {
        choice: AuthChoice::Tls {
            name: ArcStr::from(name),
            certificate: issued.certificate,
            private_key: issued.private_key,
            trusted: ca_cert,
            askpass: protection.askpass(),
        },
        staging: Some(staging),
        netidx_ca: true,
    })
}

/// The auth scheme a discovered network's resolvers use (the first resolver's).
/// `None` when the network reported no resolvers; the caller falls back to
/// prompting.
fn trust_domain_auth_kind(net: &DiscoveredTrustDomain) -> Option<AuthKind> {
    net.info.resolvers.first().map(|r| match &r.auth {
        InfoAuth::Anonymous => AuthKind::Anonymous,
        InfoAuth::Krb5 { .. } => AuthKind::Krb5,
        InfoAuth::Tls { .. } => AuthKind::Tls,
    })
}

fn info_to_template_referral(auth: &InfoAuth) -> ReferralAuth {
    match auth {
        InfoAuth::Anonymous => ReferralAuth::Anonymous,
        InfoAuth::Krb5 { spn } => ReferralAuth::Krb5(ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => ReferralAuth::Tls(ArcStr::from(name.as_str())),
    }
}

fn edge_to_parent_ref(edge: &ResolverClusterEdge) -> ParentRef {
    ParentRef {
        path: ArcStr::from(edge.path.as_str()),
        ttl: None,
        addrs: edge
            .addrs
            .iter()
            .map(|resolver| (resolver.addr, info_to_template_referral(&resolver.auth)))
            .collect(),
    }
}

fn edge_into_file(
    edge: ResolverClusterEdge,
) -> netidx::resolver_server::config::file::Referral {
    use netidx::resolver_server::config::file::{RefAuth, Referral};
    Referral {
        path: ArcStr::from(edge.path),
        ttl: None,
        addrs: edge
            .addrs
            .into_iter()
            .map(|resolver| {
                let auth = match resolver.auth {
                    InfoAuth::Anonymous => RefAuth::Anonymous,
                    InfoAuth::Krb5 { spn } => RefAuth::Krb5(ArcStr::from(spn)),
                    InfoAuth::Tls { name } => RefAuth::Tls(ArcStr::from(name)),
                };
                (resolver.addr, auth)
            })
            .collect(),
    }
}

/// Resolve this resolver's own auth by importing from a confirmed network: TLS
/// ⇒ request our resolver cert from the network's CA (suggested
/// `resolver.<domain>`, identity already glyph-confirmed); krb5 ⇒ prompt for
/// this host's SPN (a peer's is shown as the shape to follow); anonymous ⇒
/// anonymous.
async fn resolver_auth_from_trust_domain(
    ans: &mut dyn Answerer,
    input: &ResolverInput,
    net: &DiscoveredTrustDomain,
    kind: AuthKind,
) -> Result<ResolvedAuth> {
    match kind {
        AuthKind::Anonymous => Ok(ResolvedAuth::external(AuthChoice::Anonymous)),
        AuthKind::Local => bail!(
            "a resolver joined to a discovered cluster cannot use local auth (it is host-local \
             by definition)"
        ),
        AuthKind::Krb5 => {
            if let Some(example) = net.info.resolvers.iter().find_map(|r| match &r.auth {
                InfoAuth::Krb5 { spn } => Some(spn.as_str()),
                InfoAuth::Anonymous | InfoAuth::Tls { .. } => None,
            }) {
                ans.note(&format_compact!(
                    "note: an existing resolver on this cluster uses SPN {example:?}"
                ));
            }
            let spn = ans
                .text(Field::Spn, input.spn.clone(), None, true)
                .await?
                .context("a kerberos SPN for this resolver is required")?;
            Ok(ResolvedAuth::external(AuthChoice::Krb5 {
                spn: ArcStr::from(spn.as_str()),
            }))
        }
        AuthKind::Tls => {
            let Some(ca_addr) = net.info.ca_addr else {
                bail!(
                    "cluster {:?} uses TLS but none of its admin servers reported a \
                     CA — cannot obtain the resolver certificate",
                    net.identity.domain,
                )
            };
            let suggested = match &input.tls_name {
                Some(n) => n.clone(),
                None => format!("{DEFAULT_RESOLVER_NAME}.{}", net.identity.domain),
            };
            let (j, staging) = enroll::join_trust_domain(
                ans,
                ca_addr,
                NodeKind::Resolver,
                Some(&suggested),
                input.key_protection,
                &net.identity,
            )
            .await?;
            Ok(ResolvedAuth {
                choice: enroll::joined_to_auth(j),
                staging: Some(staging),
                netidx_ca: true,
            })
        }
    }
}

/// Map this resolver's chosen data-plane auth to the `InfoAuth` the delegation
/// handshake exchanges (the child's address carries it). Local auth is
/// host-local and can't serve a delegated network subtree.
#[cfg(unix)]
fn authchoice_to_info(a: &AuthChoice) -> Result<InfoAuth> {
    match a {
        AuthChoice::Anonymous => Ok(InfoAuth::Anonymous),
        AuthChoice::Krb5 { spn } => Ok(InfoAuth::Krb5 { spn: spn.to_string() }),
        AuthChoice::Tls { name, .. } => Ok(InfoAuth::Tls { name: name.to_string() }),
        AuthChoice::Local { .. } => bail!(
            "a local-auth resolver can't be delegated a network subtree (its auth \
             is host-local)"
        ),
    }
}

/// The resolver install's post-apply admin-server step. Three cases: (1) joining
/// an existing network whose CA we do NOT hold ⇒ [`enroll_admin_server`]. (2) A
/// fresh network we just created, OR a "discovered" network whose CA *this host
/// already holds* ⇒ the ca-role `admin-server.json` already exists; merge this
/// host's resolver / id-map roles into it, preserving the `ca` role. (3) No
/// config at all ⇒ the operator declined a admin server — nothing to do.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
async fn post_apply_admin_server(
    ans: &mut dyn Answerer,
    discovered: Option<&DiscoveredTrustDomain>,
    kind: AuthKind,
    no_admin_server: bool,
    with_admin_server: bool,
    resolver_listen: SocketAddr,
    resolver_base: String,
    units_dir: Option<&Path>,
    resolver_config: PathBuf,
    id_map: Option<PathBuf>,
    config_lock: &ConfigDirLock,
) -> Result<bool> {
    match discovered {
        // A "discovered" network whose CA this host already holds is our OWN
        // network: it already serves the admin plane with the `ca` role, so it
        // must MERGE the new resolver/id-map roles into that config — never
        // enroll a fresh admin server, whose join-shape config drops the `ca`
        // role and silently disables signing.
        Some(net) if host_holds_ca(net).await => {
            match admin_plane_decision(kind, no_admin_server) {
                // Honor an explicit `--no-admin-server` (and Local auth) — don't
                // advertise this resolver — even though the admin server itself
                // keeps running here (it's the CA).
                AdminPlane::Skip => Ok(false),
                AdminPlane::Mandatory | AdminPlane::Ask => {
                    merge_resolver_roles(ans, resolver_config, id_map, config_lock)
                        .await?;
                    Ok(paths::discover_admin_server_config_async().await.is_ok())
                }
            }
        }
        Some(net) => {
            enroll_admin_server(
                ans,
                net,
                kind,
                no_admin_server,
                with_admin_server,
                resolver_listen,
                resolver_base,
                units_dir,
                resolver_config,
                id_map,
                None,
                None,
                config_lock,
            )
            .await
        }
        None => {
            merge_resolver_roles(ans, resolver_config, id_map, config_lock).await?;
            Ok(paths::discover_admin_server_config_async().await.is_ok())
        }
    }
}

/// True when this host already holds the CA for the just-discovered network —
/// i.e. the network is our own. We compare the local CA cert's fingerprint
/// against the discovered identity so we only short-circuit for genuinely our
/// own CA, never a different network that merely happens to be reachable.
#[cfg(unix)]
async fn host_holds_ca(net: &DiscoveredTrustDomain) -> bool {
    if !ca_setup::default_ca_present().await {
        return false;
    }
    let Ok(ca_dir) = paths::user_ca_dir() else {
        return false;
    };
    let Ok(pem) = tokio::fs::read(ca_dir.join("certificate.pem")).await else {
        return false;
    };
    matches!(Fingerprint::of_cert_pem(&pem), Ok(fp) if fp == net.identity.fingerprint)
}

/// Merge this host's resolver / id-map roles into the existing
/// `admin-server.json`, preserving every other role (notably `ca`). No existing
/// config ⇒ the operator declined a admin server here, so there's nothing to do.
#[cfg(unix)]
async fn merge_resolver_roles(
    ans: &mut dyn Answerer,
    resolver_config: PathBuf,
    id_map: Option<PathBuf>,
    config_lock: &ConfigDirLock,
) -> Result<()> {
    if paths::discover_admin_server_config_async().await.is_err() {
        return Ok(());
    }
    let path = server_setup::update_roles(config_lock, |roles| {
        roles.resolver = Some(ResolverRole { config: resolver_config });
        if let Some(map) = id_map {
            roles.id_map = Some(IdMapRole { map });
        }
    })
    .await?;
    ans.note(&format_compact!("updated admin-server roles in {}", path.display()));
    Ok(())
}

/// Enroll a admin server on this (non-CA) host: the network's CA signs our
/// reserved-SAN serving cert (admin-authorized, policy-gated), we install the
/// serving identity + `admin-server.json` with this host's roles, and drop the
/// activation unit.
///
/// An admin at this machine authorizes synchronously with their password;
/// otherwise the enrollment **queues** for remote approval. A denied or expired
/// enrollment is a note, not a failure — the resolver this install produced
/// works; it just isn't advertised to discovery from this host.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
pub async fn enroll_admin_server(
    ans: &mut dyn Answerer,
    net: &DiscoveredTrustDomain,
    kind: AuthKind,
    no_admin_server: bool,
    with_admin_server: bool,
    resolver_listen: SocketAddr,
    resolver_base: String,
    units_dir: Option<&Path>,
    resolver_config: PathBuf,
    id_map: Option<PathBuf>,
    listen_override: Option<SocketAddr>,
    replaces: Option<netidx_admin_proto::AdminServerId>,
    config_lock: &ConfigDirLock,
) -> Result<bool> {
    let Some(ca_addr) = net.info.ca_addr else {
        ans.note(&format_compact!(
            "note: cluster {:?} reported no CA; skipping admin-server setup on this \
             host",
            net.identity.domain,
        ));
        return Ok(false);
    };
    match admin_plane_decision(kind, no_admin_server) {
        AdminPlane::Skip => return Ok(false),
        AdminPlane::Mandatory => ans.note(
            "enrolling a admin server on this host — it advertises this resolver to \
             future installs and renews its certificates. (expert opt-out: \
             --no-admin-server)",
        ),
        AdminPlane::Ask => {
            if !ans
                .confirm(Field::SetupAdminServer, with_admin_server.then_some(true), true)
                .await?
            {
                ans.note(
                    "note: skipped — discovery only sees hosts running a admin \
                     server, so future installs won't learn about this resolver \
                     from this host",
                );
                return Ok(false);
            }
        }
    }
    let ip_default = listen_override.unwrap_or(resolver_listen).ip().to_string();
    let ip: IpAddr = ans
        .text(Field::AdminServerListenIp, None, Some(&ip_default), false)
        .await?
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<IpAddr>())
        .transpose()
        .context("invalid admin server listen IP")?
        .unwrap_or_else(|| resolver_listen.ip());
    let port_default = listen_override
        .map(|listen| listen.port())
        .unwrap_or(netidx_admin_proto::DEFAULT_PORT)
        .to_string();
    let port: u16 = ans
        .text(Field::AdminServerListenPort, None, Some(&port_default), false)
        .await?
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u16>())
        .transpose()
        .context("invalid admin server listen port")?
        .unwrap_or(netidx_admin_proto::DEFAULT_PORT);
    let listen = SocketAddr::new(ip, port);
    let resolver = crate::resolver::ResolverConfig::load_async(&resolver_config).await?;
    let resolver_members = resolver.resolver_addrs();
    let resolver_member = resolver_members
        .iter()
        .find(|member| member.addr == resolver_listen)
        .cloned()
        .context("the local resolver listen address is absent from resolver members")?;
    let roles = if id_map.is_some() {
        Role::Resolver | Role::IdMap
    } else {
        Role::Resolver.into()
    };
    let map = transport::get_map_pinned(ca_addr, NodeKind::AdminServer, &net.identity)
        .await
        .context("fetching the authoritative map for enrollment")?;
    let base = resolver_base;
    let cluster = map
        .resolver_clusters
        .iter()
        .find(|c| c.base == base)
        .map(|c| netidx_admin_proto::ResolverClusterPlacement::Join { cluster: c.id })
        .unwrap_or(netidx_admin_proto::ResolverClusterPlacement::Create {
            base: base.clone(),
        });
    let enrollment = netidx_admin_proto::EnrollmentRequest {
        listen,
        roles,
        resolver_member: Some(resolver_member.clone()),
        resolver_members: resolver_members.clone(),
        cluster: cluster.clone(),
        replaces,
    };
    let cluster_description = match &cluster {
        netidx_admin_proto::ResolverClusterPlacement::Create { base } => {
            format!("create pending cluster at {base}")
        }
        netidx_admin_proto::ResolverClusterPlacement::Join { cluster } => {
            format!("join cluster {cluster} at {base}")
        }
    };
    ans.note(&format!(
        "admin-server enrollment request:\n  listen: {listen}\n  roles: {:?}\n  \
         cluster: {cluster_description}\n  resolver members: {:?}",
        roles, resolver_members
    ));
    // A non-interactive install has no admin standing by to type a password, so
    // it always takes the queued (remote-approval) path — mirroring the cert
    // enrollment in `enroll.rs`. Without this gate a strict install that stands
    // up its own admin server would hard-error here, *after* apply() already
    // wrote the config, leaving a half-finished install.
    let admin_here =
        ans.interactive() && ans.confirm(Field::AdminHere, None, false).await?;
    let issued = if admin_here {
        let admin = ans
            .text(Field::AdminName, None, None, true)
            .await?
            .context("a CA admin name is required")?;
        let mut secret = ans.secret(Field::AdminPassword, None).await?;
        let password = Zeroizing::new(std::mem::take(&mut secret.0));
        transport::enroll(
            ca_addr,
            &admin,
            password,
            listen,
            roles,
            resolver_member,
            resolver_members,
            cluster,
            replaces,
            &net.identity,
        )
        .await?
    } else {
        let pending =
            transport::enqueue_enroll(ca_addr, enrollment, &net.identity).await?;
        match enroll::await_issuance(
            ans,
            ca_addr,
            NodeKind::AdminServer,
            &pending,
            &net.identity,
        )
        .await?
        {
            transport::PollOutcome::Issued(issued) => issued,
            transport::PollOutcome::Denied(reason) => {
                ans.warn(&format_compact!(
                    "the CA admin denied the enrollment ({reason}); this resolver \
                     works, but won't be advertised to future installs from this host"
                ));
                return Ok(false);
            }
            transport::PollOutcome::Expired => {
                ans.warn(
                    "the enrollment request expired before an admin approved it; \
                     this resolver works, but won't be advertised to future installs \
                     from this host. This does not invalidate the resolver install; \
                     the optional admin-server component can be enrolled and \
                     configured separately later.",
                );
                return Ok(false);
            }
            transport::PollOutcome::Pending => {
                unreachable!("await_issuance never returns Pending")
            }
        }
    };
    for w in &issued.warnings {
        ans.warn(w);
    }
    // Serving identity: chain = [issued leaf, confirmed CA] so clients receive
    // the CA cert at the end of the chain, exactly like the CA host's own admin
    // server.
    let cfg_path = paths::user_admin_server_config()?;
    config_lock.require_contained(&cfg_path)?;
    let dir = paths::user_config_root()?.join("admin-server");
    config_lock.require_contained(&dir)?;
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("creating {}", dir.display()))?;
    let mut chain = issued.cert_pem.clone().into_bytes();
    chain.extend_from_slice(net.identity.ca_pem().as_bytes());
    let serving_cert = dir.join("cert.pem");
    let serving_key = dir.join("key.pem");
    let trusted = dir.join("trusted.pem");
    atomic::write_atomic_async(&serving_cert, &chain, 0o644).await?;
    let key_write = tokio::task::spawn_blocking({
        let serving_key = serving_key.clone();
        let private_key_pem = issued.private_key_pem.clone();
        move || tls::write_private_key_maybe_sealed(&serving_key, &private_key_pem)
    })
    .await
    .context("serving-key protection task panicked")??;
    match key_write {
        tls::KeyWrite::Sealed => ans.note(&format_compact!(
            "  serving key sealed to this machine's {}",
            netidx_tpm::MECHANISM
        )),
        tls::KeyWrite::Plain(e) => ans.warn(&format_compact!(
            "serving key is plaintext ({} sealing unavailable: {e:#})",
            netidx_tpm::MECHANISM
        )),
    }
    atomic::write_atomic_async(&trusted, issued.trusted_pem.as_bytes(), 0o644).await?;
    let cfg = AdminServerConfig {
        domain: net.identity.domain.clone(),
        server_id: tls::admin_cert_identity_from_pem(issued.cert_pem.as_bytes())?
            .server_id,
        home_ca_fingerprint: net.identity.fingerprint.text(),
        listen,
        serving_cert,
        serving_key,
        trusted,
        roles: Roles {
            ca: None,
            resolver: Some(ResolverRole { config: resolver_config }),
            id_map: id_map.map(|map| IdMapRole { map }),
        },
        ca_addr: Some(ca_addr),
        peers: net.info.reached.clone(),
        mdns: true,
        activation_units_dir: None,
    };
    crate::admin_server_config::save_async(config_lock, &cfg_path, &cfg).await?;
    ans.note(&format_compact!(
        "admin server configured:\n\
         \x20 config:   {}\n\
         \x20 listen:   {listen}\n\
         \x20 domain:   {}",
        cfg_path.display(),
        net.identity.domain
    ));
    if let Some(units_dir) = units_dir {
        server_setup::install_unit(ans, units_dir, &cfg_path).await?;
    } else {
        ans.note(&format_compact!(
            "  (--no-units: no activation unit written; run it yourself with\n\
             \x20  netidx admin component server run -c {})",
            cfg_path.display()
        ));
    }
    Ok(true)
}
