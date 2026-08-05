use super::*;
use crate::admin_proto::{
    AdminDomainMap, AdminServerEntry, ApplyCaStateRequest, ApplyCrlRequest,
    ApplyIdMapEditRequest, ApplyPermsEditRequest, ApplyReferralEditRequest,
    ApplyServiceControlRequest, EditIdMapRequest, ExternalCaInstallRequest,
    GetIdMapRequest, IdMapEdit, InfoAuth, LoginRequest, LogoutRequest, ReadPermsRequest,
    ReferralEdit, RegisterRequest, ResolverAddr, Role,
};

fn assert_public(req: &Request) {
    assert!(matches!(request_requirements(req), RequestRequirements::Public));
}

fn assert_ca(req: &Request) {
    assert!(matches!(request_requirements(req), RequestRequirements::CaOnly));
}

fn assert_node(req: &Request) {
    assert!(matches!(request_requirements(req), RequestRequirements::NodeSelf));
}

fn assert_admin(req: &Request, server_key: ServerKeyRequirement) {
    match request_requirements(req) {
        RequestRequirements::Admin { server_key: actual, .. } => {
            assert_eq!(actual, server_key)
        }
        actual => panic!("expected admin requirements, got {actual:?}"),
    }
}

fn assert_local(req: &Request, server_key: ServerKeyRequirement) {
    match request_requirements(req) {
        RequestRequirements::LocalOnly { server_key: actual } => {
            assert_eq!(actual, server_key)
        }
        actual => panic!("expected local requirements, got {actual:?}"),
    }
}

#[test]
fn centralized_requirements_protect_all_mutations() {
    use ServerKeyRequirement::{NotNeeded, Required};

    let operation_id = admin_proto::OperationId::new();
    assert_public(&Request::GetInfo);
    assert_node(&Request::Register(RegisterRequest {
        addr: "127.0.0.1:4565".parse().unwrap(),
        resolver: None,
        id_map_version: None,
        perms_version: None,
    }));
    assert_ca(&Request::ApplyPermsEdit(ApplyPermsEditRequest {
        operation_id,
        perms_json: "{}".into(),
        version: Some(1),
    }));
    assert_ca(&Request::GetPerms);
    assert_admin(
        &Request::ReadPerms(ReadPermsRequest {
            credential: admin_proto::AdminCredential::password("alice", "pw"),
            target_path: "/eu".into(),
        }),
        NotNeeded,
    );
    // The id-map trio, gated exactly like its perms counterparts: the
    // server-to-server apply is CA-only, the operator-facing pair is
    // admin-authenticated and needs no server key (no signing involved).
    assert_ca(&Request::ApplyIdMapEdit(ApplyIdMapEditRequest {
        operation_id,
        edit: IdMapEdit::AddGroup { name: "users".into() },
        version: Some(1),
    }));
    // The same message carries the CA's post-sign registration push.
    assert_ca(&Request::ApplyIdMapEdit(ApplyIdMapEditRequest {
        operation_id,
        edit: IdMapEdit::AddIdentity {
            san: "alice.example".into(),
            primary_group: "users".into(),
            groups: vec![],
        },
        version: Some(2),
    }));
    assert_admin(
        &Request::GetIdMap(GetIdMapRequest {
            credential: admin_proto::AdminCredential::password("alice", "pw"),
        }),
        NotNeeded,
    );
    assert_admin(
        &Request::EditIdMap(EditIdMapRequest {
            credential: admin_proto::AdminCredential::password("alice", "pw"),
            edit: IdMapEdit::RemoveIdentity { san: "bob.example".into() },
        }),
        NotNeeded,
    );
    assert_ca(&Request::ApplyCrl(ApplyCrlRequest {
        operation_id,
        crl_pem: "crl".into(),
    }));
    let ca = admin_proto::AdminServerId::new();
    let mut map = AdminDomainMap::empty(ca);
    map.admin_servers.push(AdminServerEntry {
        id: ca,
        addr: "127.0.0.1:4565".parse().unwrap(),
        roles: Role::Ca.into(),
        resolver: None,
        cluster: None,
        state: admin_proto::ServerState::Registered,
        reported_read_gate: None,
        reported_id_map_version: None,
        reported_perms_version: None,
    });
    assert_ca(&Request::ApplyCaState(ApplyCaStateRequest {
        operation_id,
        ca,
        addr: "127.0.0.1:4565".parse().unwrap(),
        map,
        crl_pem: "crl".into(),
    }));
    assert_local(
        &Request::Backup(admin_proto::BackupRequest { target: "/backup".into() }),
        Required,
    );
    assert_admin(
        &Request::ReconcileCa(admin_proto::ReconcileCaRequest {
            credential: admin_proto::AdminCredential::password("admin", "pw"),
        }),
        Required,
    );
    assert_ca(&Request::ApplyReferralEdit(ApplyReferralEditRequest {
        operation_id,
        edit: ReferralEdit::SetTopology {
            local_member: ResolverAddr {
                addr: "127.0.0.1:4564".parse().unwrap(),
                auth: InfoAuth::Anonymous,
            },
            members: vec![],
            parent: None,
            children: vec![],
        },
    }));
    assert_ca(&Request::ApplyServiceControl(ApplyServiceControlRequest {
        operation_id,
        units: vec![],
        op: netidx_activation::control::ControlOp::Status,
    }));
    assert_local(&Request::RotateRecovery, NotNeeded);
    assert_local(&Request::ExternalCaCsr, Required);
    assert_local(
        &Request::ExternalCaInstall(ExternalCaInstallRequest {
            signed_cert_pem: "certificate".into(),
            root_pem: None,
        }),
        Required,
    );
    assert_local(&Request::CaStatus, NotNeeded);

    let password = admin_proto::AdminCredential::password("alice", "pw");
    assert_admin(
        &Request::Login(LoginRequest { credential: password.clone() }),
        NotNeeded,
    );
    let logout = Request::Logout(LogoutRequest { credential: password });
    let requirements = request_requirements(&logout);
    assert!(matches!(requirements, RequestRequirements::Logout));
    assert!(requirements.admin_credential().is_none());
    assert!(!requirements.needs_server_unlock());
}

#[test]
fn origin_requirements_are_enforced_independently_of_credentials() {
    assert!(
        authorize_request_origin(RequestRequirements::CaOnly, false, true, false,)
            .is_err()
    );
    assert!(
        authorize_request_origin(RequestRequirements::CaOnly, false, true, true,).is_ok()
    );
    assert!(
        authorize_request_origin(RequestRequirements::CaOnly, false, false, false,)
            .is_err()
    );
    assert!(
        authorize_request_origin(RequestRequirements::NodeSelf, false, true, false)
            .is_ok()
    );
    assert!(
        authorize_request_origin(RequestRequirements::NodeSelf, false, false, false)
            .is_err()
    );
    assert!(
        authorize_request_origin(
            RequestRequirements::LocalOnly {
                server_key: ServerKeyRequirement::NotNeeded,
            },
            false,
            true,
            true,
        )
        .is_err()
    );
    assert!(
        authorize_request_origin(
            RequestRequirements::LocalOnly {
                server_key: ServerKeyRequirement::NotNeeded,
            },
            true,
            false,
            false,
        )
        .is_ok()
    );
}

/// The local control socket sends an *empty* credential on purpose — the
/// `SO_PEERCRED` check at accept is the authorization. Letting that count as
/// a failed credential blocked the server unlock, which broke the only path
/// that can re-mint a CA host's serving certificate once it has expired.
/// A remote caller must still be blocked, or an unauthenticated client gets
/// to spend a 64 MiB Argon2 at will.
#[test]
fn a_local_request_may_unlock_despite_an_empty_credential() {
    let bad = PreparedAdminAuthentication::Password(Err("authentication failed".into()));
    let good =
        PreparedAdminAuthentication::Password(Ok(super::super::auth::local_superuser()));

    assert!(
        !unlock_blocked_by_credential(true, Some(&bad)),
        "the local socket's empty credential must not block the unlock"
    );
    assert!(
        unlock_blocked_by_credential(false, Some(&bad)),
        "a remote caller with a bad credential must still be blocked"
    );
    assert!(!unlock_blocked_by_credential(false, Some(&good)));
    assert!(!unlock_blocked_by_credential(true, Some(&good)));
    // No credential required by this request kind: nothing to block on.
    assert!(!unlock_blocked_by_credential(false, None));
    assert!(!unlock_blocked_by_credential(true, None));
}
