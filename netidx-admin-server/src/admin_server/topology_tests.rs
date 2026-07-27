use super::*;
use crate::{
    admin_proto::AdminServerEntry,
    admin_server::{password_limiter::PasswordLimiter, test_support::signed_empty_crl},
    fingerprint::Fingerprint,
};
use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;

#[tokio::test]
async fn topology_fanout_writes_the_split_config_without_service_control() {
    use netidx::resolver_server::config::file as rfile;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("ap1.json");
    let config_lock = ConfigDirLock::acquire(dir.path()).unwrap();
    let member = |addr: &str| {
        let addr = addr.parse::<SocketAddr>().unwrap();
        rfile::MemberServerBuilder::default()
            .addr(addr)
            .bind_addr(addr.ip())
            .auth(rfile::Auth::Anonymous)
            .build()
            .unwrap()
    };
    let cfg = rfile::ConfigBuilder::default()
        .member_servers(vec![
            member("10.0.60.1:4564"),
            member("10.0.60.2:4564"),
            member("10.0.0.1:4564"),
            member("10.0.0.2:4564"),
        ])
        .build()
        .unwrap();
    std::fs::write(&p, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
    let resolver = |addr: &str| ResolverAddr {
        addr: addr.parse().unwrap(),
        auth: InfoAuth::Anonymous,
    };
    let edit = ReferralEdit::SetTopology {
        local_member: resolver("10.0.60.1:4564"),
        members: vec![resolver("10.0.60.2:4564"), resolver("10.0.60.1:4564")],
        parent: Some(admin_proto::ResolverClusterEdge {
            path: "/ap".into(),
            addrs: vec![resolver("10.0.0.1:4564"), resolver("10.0.0.2:4564")],
        }),
        children: vec![],
    };

    apply_referral_edit_local(&config_lock, &p, &edit).await.unwrap();
    let rc = crate::resolver::ResolverConfig::load(&p).unwrap();
    assert_eq!(
        rc.as_file().member_servers.iter().map(|m| m.addr).collect::<Vec<_>>(),
        vec![
            "10.0.60.1:4564".parse::<SocketAddr>().unwrap(),
            "10.0.60.2:4564".parse::<SocketAddr>().unwrap(),
        ]
    );
    let parent = rc.as_file().parent.as_ref().unwrap();
    assert_eq!(&*parent.path, "/ap");
    assert_eq!(
        parent.addrs.iter().map(|(addr, _)| *addr).collect::<Vec<_>>(),
        vec![
            "10.0.0.1:4564".parse::<SocketAddr>().unwrap(),
            "10.0.0.2:4564".parse::<SocketAddr>().unwrap(),
        ]
    );

    // A local-only member menu is equally valid. The authoritative AP2
    // member stays in the CA map/referrals but is not synthesized into
    // this host's resolver.json.
    let local_only = dir.path().join("ap-local-only.json");
    let cfg = rfile::ConfigBuilder::default()
        .member_servers(vec![member("10.0.60.1:4564")])
        .build()
        .unwrap();
    std::fs::write(&local_only, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
    apply_referral_edit_local(&config_lock, &local_only, &edit).await.unwrap();
    let rc = crate::resolver::ResolverConfig::load(&local_only).unwrap();
    assert_eq!(rc.as_file().member_servers.len(), 1);
    assert_eq!(rc.as_file().member_servers[0].addr, resolver("10.0.60.1:4564").addr);

    let wrong_member = ResolverAddr {
        addr: "10.0.60.1:4564".parse().unwrap(),
        auth: InfoAuth::Tls { name: "wrong.example".into() },
    };
    let wrong_auth = ReferralEdit::SetTopology {
        local_member: wrong_member.clone(),
        members: vec![wrong_member],
        parent: None,
        children: vec![],
    };
    assert!(apply_referral_edit_local(&config_lock, &p, &wrong_auth).await.is_err());
}

#[test]
fn controller_reconciliation_fanout_covers_the_complete_hierarchy() {
    let controller = admin_proto::AdminServerId::new();
    let satellite = admin_proto::AdminServerId::new();
    let root = admin_proto::ResolverClusterId::new();
    let child = admin_proto::ResolverClusterId::new();
    let resolver = |addr: &str| ResolverAddr {
        addr: addr.parse().unwrap(),
        auth: InfoAuth::Anonymous,
    };
    let root_member = resolver("10.1.0.1:4564");
    let child_member = resolver("10.2.0.1:4564");
    let map = TrustDomainMap {
        version: 9,
        controller,
        admin_servers: vec![
            AdminServerEntry {
                id: controller,
                addr: "10.1.0.1:4565".parse().unwrap(),
                roles: Role::Ca | Role::Resolver,
                resolver: Some(root_member.clone()),
                cluster: Some(root),
                state: admin_proto::ServerState::Registered,
            },
            AdminServerEntry {
                id: satellite,
                addr: "10.2.0.1:4565".parse().unwrap(),
                roles: Role::Resolver.into(),
                resolver: Some(child_member.clone()),
                cluster: Some(child),
                state: admin_proto::ServerState::Registered,
            },
        ],
        resolver_clusters: vec![
            admin_proto::ResolverClusterEntry {
                id: root,
                base: "/".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![root_member.clone()],
                parent: None,
                children: vec![child],
            },
            admin_proto::ResolverClusterEntry {
                id: child,
                base: "/eu".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![child_member.clone()],
                parent: Some(root),
                children: vec![],
            },
        ],
    };

    let fanout = topology_fanout(&map, map.resolver_clusters.iter());
    assert_eq!(fanout.targets.len(), 2);
    let (_, _, ReferralEdit::SetTopology { parent, children, .. }) =
        fanout.targets.iter().find(|(server, _, _)| *server == controller).unwrap();
    assert!(parent.is_none());
    assert_eq!(children[0].path, "/eu");
    assert_eq!(children[0].addrs, vec![child_member]);
    let (_, _, ReferralEdit::SetTopology { parent, children, .. }) =
        fanout.targets.iter().find(|(server, _, _)| *server == satellite).unwrap();
    assert!(children.is_empty());
    assert_eq!(parent.as_ref().unwrap().path, "/eu");
    assert_eq!(parent.as_ref().unwrap().addrs, vec![root_member]);
}

#[test]
fn registration_fanout_updates_its_cluster_and_both_adjacent_levels() {
    let controller = admin_proto::AdminServerId::new();
    let joining = admin_proto::AdminServerId::new();
    let peer = admin_proto::AdminServerId::new();
    let grandchild_server = admin_proto::AdminServerId::new();
    let sibling_server = admin_proto::AdminServerId::new();
    let root = admin_proto::ResolverClusterId::new();
    let child = admin_proto::ResolverClusterId::new();
    let grandchild = admin_proto::ResolverClusterId::new();
    let sibling = admin_proto::ResolverClusterId::new();
    let resolver = |addr: &str| ResolverAddr {
        addr: addr.parse().unwrap(),
        auth: InfoAuth::Anonymous,
    };
    let server = |id, admin_addr: &str, member: ResolverAddr, cluster| AdminServerEntry {
        id,
        addr: admin_addr.parse().unwrap(),
        roles: Role::Resolver.into(),
        resolver: Some(member),
        cluster: Some(cluster),
        state: admin_proto::ServerState::Registered,
    };
    let root_member = resolver("10.1.0.1:4564");
    let joining_member = resolver("10.2.0.1:4564");
    let peer_member = resolver("10.2.0.2:4564");
    let grandchild_member = resolver("10.3.0.1:4564");
    let sibling_member = resolver("10.4.0.1:4564");
    let map = TrustDomainMap {
        version: 12,
        controller,
        admin_servers: vec![
            server(controller, "10.1.0.1:4565", root_member.clone(), root),
            server(joining, "10.2.0.1:4565", joining_member.clone(), child),
            server(peer, "10.2.0.2:4565", peer_member.clone(), child),
            server(
                grandchild_server,
                "10.3.0.1:4565",
                grandchild_member.clone(),
                grandchild,
            ),
            server(sibling_server, "10.4.0.1:4565", sibling_member.clone(), sibling),
        ],
        resolver_clusters: vec![
            admin_proto::ResolverClusterEntry {
                id: root,
                base: "/".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![root_member],
                parent: None,
                children: vec![child, sibling],
            },
            admin_proto::ResolverClusterEntry {
                id: child,
                base: "/eu".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![joining_member, peer_member],
                parent: Some(root),
                children: vec![grandchild],
            },
            admin_proto::ResolverClusterEntry {
                id: grandchild,
                base: "/eu/fr".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![grandchild_member],
                parent: Some(child),
                children: vec![],
            },
            admin_proto::ResolverClusterEntry {
                id: sibling,
                base: "/us".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![sibling_member],
                parent: Some(root),
                children: vec![],
            },
        ],
    };

    let mut targets: Vec<_> = registration_topology_fanout(&map, joining)
        .unwrap()
        .targets
        .into_iter()
        .map(|(server, _, _)| server)
        .collect();
    targets.sort();
    let mut expected = vec![controller, joining, peer, grandchild_server];
    expected.sort();
    assert_eq!(targets, expected);
}

#[tokio::test]
async fn controller_state_relocation_persists_route_map_and_crl_without_rollback() {
    use crate::admin_server_config::Roles;
    let home = tempfile::tempdir().unwrap();
    let (crl, home_ca) = signed_empty_crl(home.path(), "home-ca").await;
    let root = tempfile::tempdir().unwrap();
    let cfg_path = root.path().join("admin-server.json");
    let trusted = root.path().join("trusted.pem");
    std::fs::write(&trusted, std::fs::read(home.path().join("certificate.pem")).unwrap())
        .unwrap();
    let controller = admin_proto::AdminServerId::new();
    let node = admin_proto::AdminServerId::new();
    let old_addr = "10.0.0.1:4565".parse().unwrap();
    let new_addr = "10.0.0.2:14565".parse().unwrap();
    let cfg = AdminServerConfig {
        domain: "example.com".into(),
        server_id: node,
        home_ca_fingerprint: Fingerprint::of_cert_der(&home_ca).unwrap().text(),
        listen: "10.1.0.2:4565".parse().unwrap(),
        serving_cert: root.path().join("unused-cert.pem"),
        serving_key: root.path().join("unused-key.pem"),
        trusted: trusted.clone(),
        roles: Roles::default(),
        ca_addr: Some(old_addr),
        peers: vec![],
        mdns: false,
        activation_units_dir: None,
    };
    let config_lock = ConfigDirLock::acquire(root.path()).unwrap();
    crate::admin_server_config::save(&config_lock, &cfg_path, &cfg).unwrap();
    drop(config_lock);
    let entry = |id, addr, roles| AdminServerEntry {
        id,
        addr,
        roles,
        resolver: None,
        cluster: None,
        state: admin_proto::ServerState::Registered,
    };
    let mut old_map = TrustDomainMap::empty(controller);
    old_map.version = 4;
    old_map.admin_servers.push(entry(controller, old_addr, Role::Ca.into()));
    old_map.admin_servers.push(entry(node, cfg.listen, Role::Resolver.into()));
    let mut new_map = old_map.clone();
    new_map.version = 5;
    new_map.admin_servers.iter_mut().find(|s| s.id == controller).unwrap().addr =
        new_addr;
    let state = Server::from_state(
        ConfigDirLock::acquire(root.path()).unwrap(),
        None,
        MutableState {
            cfg,
            map: old_map,
            ca: None,
            password_limiter: PasswordLimiter::default(),
        },
        Some(cfg_path.clone()),
        vec![],
        vec![],
        RootCertStore::empty(),
        CertificateDer::from(home_ca),
    )
    .unwrap();
    let req = ApplyControllerStateRequest {
        operation_id: admin_proto::OperationId::new(),
        controller,
        addr: new_addr,
        map: new_map.clone(),
        crl_pem: crl.clone(),
    };
    assert!(matches!(
        handle_apply_controller_state(&state, &req).await,
        ApplyControllerStateResponse::Ok(())
    ));
    let persisted = crate::admin_server_config::load_for_recovery(&cfg_path).unwrap();
    assert_eq!(persisted.ca_addr, Some(new_addr));
    assert_eq!(state.read(move |state| state.map.clone()).await, new_map);
    assert_eq!(std::fs::read_to_string(root.path().join("crl.pem")).unwrap(), crl);

    let mut stale = req.clone();
    stale.map.version = 3;
    stale.addr = old_addr;
    stale.map.admin_servers.iter_mut().find(|s| s.id == controller).unwrap().addr =
        old_addr;
    assert!(matches!(
        handle_apply_controller_state(&state, &stale).await,
        ApplyControllerStateResponse::Err { .. }
    ));
    assert_eq!(
        crate::admin_server_config::load_for_recovery(&cfg_path).unwrap().ca_addr,
        Some(new_addr)
    );
}

/// Deciding a delegation needs authority over the parent resolver cluster's base, not
/// just over the proposed child path. `trust_domain::delegate` only requires the
/// child to sit under the parent's base, so without this check an admin scoped
/// to `/eu` could approve a delegation of `/eu/x` parented at the ROOT resolver cluster
/// and rewrite the root resolvers' referrals.
#[test]
fn deciding_a_delegation_requires_authority_over_the_parent_cluster() {
    use netidx_admin_proto::policy::SlotKind;
    let root_srv = admin_proto::AdminServerId::new();
    let eu_srv = admin_proto::AdminServerId::new();
    let root = admin_proto::ResolverClusterId::new();
    let eu = admin_proto::ResolverClusterId::new();
    let resolver = |addr: &str| ResolverAddr {
        addr: addr.parse().unwrap(),
        auth: InfoAuth::Anonymous,
    };
    let root_member = resolver("10.1.0.1:4564");
    let eu_member = resolver("10.2.0.1:4564");
    let server = |id, addr: &str, member: &ResolverAddr, cluster| AdminServerEntry {
        id,
        addr: addr.parse().unwrap(),
        roles: Role::Resolver.into(),
        resolver: Some(member.clone()),
        cluster: Some(cluster),
        state: admin_proto::ServerState::Registered,
    };
    let map = TrustDomainMap {
        version: 1,
        controller: root_srv,
        admin_servers: vec![
            server(root_srv, "10.1.0.1:4565", &root_member, root),
            server(eu_srv, "10.2.0.1:4565", &eu_member, eu),
        ],
        resolver_clusters: vec![
            admin_proto::ResolverClusterEntry {
                id: root,
                base: "/".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![root_member],
                parent: None,
                children: vec![eu],
            },
            admin_proto::ResolverClusterEntry {
                id: eu,
                base: "/eu".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![eu_member],
                parent: Some(root),
                children: vec![],
            },
        ],
    };
    let scoped = |scope: &str| crate::ca_vault::Authenticated {
        slot_id: uuid::Uuid::new_v4(),
        credential_revision: 0,
        admin: "eu-ops".to_string(),
        policy: netidx_admin_proto::policy::Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(86400),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: Default::default(),
            perms_edit_scopes: vec![scope.to_string()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        },
        kind: SlotKind::Role,
    };
    let child_srv = admin_proto::AdminServerId::new();
    let under_root = delegation_store::PendingDelegation::new(
        "/eu/x".to_string(),
        vec![root_srv],
        vec![child_srv],
        "peer".to_string(),
    );
    let under_eu = delegation_store::PendingDelegation::new(
        "/eu/x".to_string(),
        vec![eu_srv],
        vec![child_srv],
        "peer".to_string(),
    );

    // The child path is inside /eu either way; only the parent differs.
    assert!(decide_delegation_authority(&scoped("/eu"), &map, &under_root).is_err());
    assert!(decide_delegation_authority(&scoped("/eu"), &map, &under_eu).is_ok());
    // The root admin may decide both.
    assert!(decide_delegation_authority(&scoped("/"), &map, &under_root).is_ok());
    assert!(decide_delegation_authority(&scoped("/"), &map, &under_eu).is_ok());
}
