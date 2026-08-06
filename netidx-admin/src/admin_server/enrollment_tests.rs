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
