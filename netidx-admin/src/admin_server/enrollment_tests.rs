use super::*;
use crate::admin_proto::{InfoAuth, ResolverAddr};
use enumflags2::BitFlags;
use std::time::Duration;

#[test]
fn ca_identity_is_renewal_only_and_preserved() {
    let ca = admin_proto::AdminServerId::new();
    let map = AdminDomainMap::empty(ca);
    let identity = enrollment_cert_identity(true, Some(ca), Some(&map)).unwrap();
    assert_eq!(identity.server_id, ca);
    assert!(identity.ca);
    assert!(enrollment_cert_identity(true, None, Some(&map)).is_err());
    assert!(
        enrollment_cert_identity(
            true,
            Some(admin_proto::AdminServerId::new()),
            Some(&map),
        )
        .is_err()
    );
    let satellite = enrollment_cert_identity(false, Some(ca), Some(&map)).unwrap();
    assert!(!satellite.ca);
    assert_ne!(satellite.server_id, ca);
}

#[test]
fn restore_enrollment_atomically_replaces_only_the_same_cluster_satellite() {
    let ca = admin_proto::AdminServerId::new();
    let old = admin_proto::AdminServerId::new();
    let fresh = admin_proto::AdminServerId::new();
    let mut map = AdminDomainMap::empty(ca);
    let member = ResolverAddr {
        addr: "10.0.0.10:4564".parse().unwrap(),
        auth: InfoAuth::Anonymous,
    };
    let initial = admin_proto::EnrollmentRequest {
        resolver_config: None,
        listen: "10.0.0.10:4565".parse().unwrap(),
        roles: Role::Resolver.into(),
        resolver_member: Some(member.clone()),
        resolver_members: vec![member.clone()],
        cluster: admin_proto::ResolverClusterPlacement::Create { base: "/eu".into() },
        replaces: None,
    };
    let cluster = stage_enrollment(&mut map, old, &initial).unwrap();
    let mut replacement = initial.clone();
    replacement.cluster = admin_proto::ResolverClusterPlacement::Join { cluster };
    replacement.replaces = Some(old);
    assert_eq!(stage_enrollment(&mut map, fresh, &replacement).unwrap(), cluster);
    assert!(map.admin_servers.iter().all(|server| server.id != old));
    assert!(map.admin_servers.iter().any(|server| server.id == fresh));
    assert!(map.resolver_clusters.iter().any(|entry| entry.id == cluster));

    replacement.replaces = Some(ca);
    assert!(
        stage_enrollment(&mut map, admin_proto::AdminServerId::new(), &replacement,)
            .is_err()
    );
}

#[test]
fn enrollment_policy_enforces_scope_roles_and_invariants() {
    let role_admin = ca_vault::Authenticated {
        slot_id: uuid::Uuid::new_v4(),
        credential_revision: 0,
        admin: "eu-ops".into(),
        policy: netidx_admin_proto::policy::Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(60),
            id_map_groups: vec![],
            server_enroll_scopes: vec!["/eu".into()],
            server_enroll_roles: Role::Resolver.into(),
            perms_edit_scopes: vec![],
            may_manage_admins: false,
            service_control_scopes: vec![],
        },
        kind: netidx_admin_proto::policy::SlotKind::Role,
    };
    let request = |base: &str, roles: BitFlags<Role>| admin_proto::EnrollmentRequest {
        resolver_config: None,
        listen: "127.0.0.1:4565".parse().unwrap(),
        roles,
        resolver_member: Some(ResolverAddr {
            addr: "127.0.0.1:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }),
        resolver_members: vec![ResolverAddr {
            addr: "127.0.0.1:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }],
        cluster: admin_proto::ResolverClusterPlacement::Create { base: base.into() },
        replaces: None,
    };
    assert!(
        authorize_enrollment(
            &role_admin,
            &request("/eu/one", Role::Resolver.into()),
            None,
        )
        .is_ok()
    );
    assert!(
        authorize_enrollment(&role_admin, &request("/us", Role::Resolver.into()), None,)
            .is_err()
    );
    assert!(
        authorize_enrollment(
            &role_admin,
            &request("/eu", Role::Resolver | Role::IdMap),
            None,
        )
        .is_err()
    );

    let signing = ca_vault::Authenticated {
        kind: netidx_admin_proto::policy::SlotKind::Signing,
        ..role_admin
    };
    assert!(
        authorize_enrollment(&signing, &request("/", Role::Ca | Role::Resolver), None,)
            .is_err()
    );
    assert!(
        authorize_enrollment(&signing, &request("/", BitFlags::empty()), None).is_err()
    );
}

/// Two members of one cluster, each with its own TLS identity. Both grants
/// must end up in the one document the cluster shares.
///
/// This is the shape that was wrong: the installer wrote each host a perms
/// file granting only itself, so members of one cluster authorized differently
/// depending on which one a subscriber reached — and the first CA edit
/// propagated one host's document over the others, silently revoking one
/// member's grant while extending another's across the whole cluster.
#[tokio::test]
async fn every_member_of_a_cluster_is_granted_in_the_one_document() {
    let dir = tempfile::tempdir().unwrap();
    let lock =
        crate::config_lock::ConfigDirLock::acquire_for_ca_dir(dir.path()).await.unwrap();
    let ca_store = ca_store::CaDir::open(lock, dir.path()).await.unwrap();
    let ca = admin_proto::AdminServerId::new();
    let mut map = AdminDomainMap::empty(ca);

    let enroll = |addr: &str, name: &str, cluster| {
        let member = ResolverAddr {
            addr: format!("{addr}:4564").parse().unwrap(),
            auth: InfoAuth::Tls { name: name.to_string() },
        };
        admin_proto::EnrollmentRequest {
            resolver_config: None,
            listen: format!("{addr}:4565").parse().unwrap(),
            roles: Role::Resolver.into(),
            resolver_member: Some(member.clone()),
            resolver_members: vec![member],
            cluster,
            replaces: None,
        }
    };

    let first = enroll(
        "10.0.0.10",
        "a.example.com",
        admin_proto::ResolverClusterPlacement::Create { base: "/eu".into() },
    );
    let id_a = admin_proto::AdminServerId::new();
    let cluster = stage_enrollment(&mut map, id_a, &first).unwrap();
    grant_member_self_perms(&ca_store, &mut map, cluster, &first).await.unwrap();

    let second = enroll(
        "10.0.0.11",
        "b.example.com",
        admin_proto::ResolverClusterPlacement::Join { cluster },
    );
    let id_b = admin_proto::AdminServerId::new();
    stage_enrollment(&mut map, id_b, &second).unwrap();
    grant_member_self_perms(&ca_store, &mut map, cluster, &second).await.unwrap();

    let model = ca_store.store.perms_model().await.unwrap();
    let perms = &model.get(cluster).expect("the cluster has a document").perms;
    assert_eq!(
        crate::perms::lookup(perms, "/eu", "a.example.com").map(|b| b.as_str()),
        Some("swlpd"),
        "the first member keeps its grant when the second joins"
    );
    assert_eq!(
        crate::perms::lookup(perms, "/eu", "b.example.com").map(|b| b.as_str()),
        Some("swlpd"),
    );
    // And the cluster-wide seed came with the first member, so the document is
    // complete from the moment the cluster exists rather than from the first
    // time someone edits it.
    assert!(crate::perms::lookup(perms, "/eu", "users").is_some());
    // The version moved once per member, so each is something a member can
    // converge on.
    assert_eq!(model.get(cluster).unwrap().version, 2);

    // An anonymous member has no identity to grant, and must not fail the
    // enrollment over it.
    let anon_member = ResolverAddr {
        addr: "10.0.0.12:4564".parse().unwrap(),
        auth: InfoAuth::Anonymous,
    };
    let anon = admin_proto::EnrollmentRequest {
        resolver_member: Some(anon_member.clone()),
        resolver_members: vec![anon_member],
        ..enroll(
            "10.0.0.12",
            "unused",
            admin_proto::ResolverClusterPlacement::Join { cluster },
        )
    };
    grant_member_self_perms(&ca_store, &mut map, cluster, &anon).await.unwrap();
    assert_eq!(
        ca_store.store.perms_model().await.unwrap().get(cluster).unwrap().version,
        2,
        "nothing to grant means nothing recorded, so no member is made to look behind"
    );
}
