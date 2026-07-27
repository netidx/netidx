use super::*;
use crate::admin_proto::{
    AddIdentityRequest, AdminServerEntry, ApplyControllerStateRequest, ApplyCrlRequest,
    ApplyPermsEditRequest, ApplyReferralEditRequest, ApplyServiceControlRequest,
    ExternalCaInstallRequest, InfoAuth, LoginRequest, LogoutRequest, ReadPermsRequest,
    ReferralEdit, RegisterRequest, ResolverAddr, Role, TrustDomainMap,
};

fn assert_public(req: &Request) {
    assert!(matches!(request_requirements(req), RequestRequirements::Public));
}

fn assert_controller(req: &Request) {
    assert!(matches!(request_requirements(req), RequestRequirements::ControllerOnly));
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
    }));
    assert_controller(&Request::AddIdentity(AddIdentityRequest {
        operation_id,
        san: "alice.example".into(),
        primary_group: "users".into(),
        groups: vec![],
    }));
    assert_controller(&Request::ApplyPermsEdit(ApplyPermsEditRequest {
        operation_id,
        perms_json: "{}".into(),
    }));
    assert_controller(&Request::GetPerms);
    assert_admin(
        &Request::ReadPerms(ReadPermsRequest {
            credential: admin_proto::AdminCredential::password("alice", "pw"),
            target_path: "/eu".into(),
        }),
        NotNeeded,
    );
    assert_controller(&Request::ApplyCrl(ApplyCrlRequest {
        operation_id,
        crl_pem: "crl".into(),
    }));
    let controller = admin_proto::AdminServerId::new();
    let mut map = TrustDomainMap::empty(controller);
    map.admin_servers.push(AdminServerEntry {
        id: controller,
        addr: "127.0.0.1:4565".parse().unwrap(),
        roles: Role::Ca.into(),
        resolver: None,
        cluster: None,
        state: admin_proto::ServerState::Registered,
    });
    assert_controller(&Request::ApplyControllerState(ApplyControllerStateRequest {
        operation_id,
        controller,
        addr: "127.0.0.1:4565".parse().unwrap(),
        map,
        crl_pem: "crl".into(),
    }));
    assert_local(
        &Request::Backup(admin_proto::BackupRequest { target: "/backup".into() }),
        Required,
    );
    assert_admin(
        &Request::ReconcileController(admin_proto::ReconcileControllerRequest {
            credential: admin_proto::AdminCredential::password("admin", "pw"),
        }),
        Required,
    );
    assert_controller(&Request::ApplyReferralEdit(ApplyReferralEditRequest {
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
    assert_controller(&Request::ApplyServiceControl(ApplyServiceControlRequest {
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
        authorize_request_origin(
            RequestRequirements::ControllerOnly,
            false,
            true,
            false,
        )
        .is_err()
    );
    assert!(
        authorize_request_origin(RequestRequirements::ControllerOnly, false, true, true,)
            .is_ok()
    );
    assert!(
        authorize_request_origin(
            RequestRequirements::ControllerOnly,
            false,
            false,
            false,
        )
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
