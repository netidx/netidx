use super::*;
use crate::admin_proto::{
    AddIdentityRequest, ApplyControllerStateRequest, ApplyCrlRequest,
    ApplyPermsEditRequest, ApplyReferralEditRequest, ApplyServiceControlRequest,
    ExternalCaInstallRequest, InfoAuth, NetworkMap, ReadPermsRequest, ReferralEdit,
    RegisterRequest, ResolverAddr, Role, ServerEntry,
};

#[test]
fn centralized_authorization_matrix_protects_all_mutations() {
    let operation_id = admin_proto::OperationId::new();
    assert_eq!(request_authorization(&Request::GetInfo), RequestAuthorization::Public);
    assert_eq!(
        request_authorization(&Request::Register(RegisterRequest {
            addr: "127.0.0.1:4565".parse().unwrap(),
            resolver: None,
        })),
        RequestAuthorization::NodeSelf,
    );
    assert_eq!(
        request_authorization(&Request::AddIdentity(AddIdentityRequest {
            operation_id,
            san: "alice.example".into(),
            primary_group: "users".into(),
            groups: vec![],
        })),
        RequestAuthorization::ControllerOnly,
    );
    assert_eq!(
        request_authorization(&Request::ApplyPermsEdit(ApplyPermsEditRequest {
            operation_id,
            perms_json: "{}".into(),
        })),
        RequestAuthorization::ControllerOnly,
    );
    assert_eq!(
        request_authorization(&Request::GetPerms),
        RequestAuthorization::ControllerOnly,
    );
    assert_eq!(
        request_authorization(&Request::ReadPerms(ReadPermsRequest {
            credential: admin_proto::AdminCredential::password("alice", "pw"),
            target_path: "/eu".into(),
        })),
        RequestAuthorization::AdminAuthenticated,
    );
    assert_eq!(
        request_authorization(&Request::ApplyCrl(ApplyCrlRequest {
            operation_id,
            crl_pem: "crl".into(),
        })),
        RequestAuthorization::ControllerOnly,
    );
    let controller = admin_proto::AdminServerId::new();
    let mut map = NetworkMap::empty(controller);
    map.servers.push(ServerEntry {
        id: controller,
        addr: "127.0.0.1:4565".parse().unwrap(),
        roles: Role::Ca.into(),
        resolver: None,
        cluster: None,
        state: admin_proto::ServerState::Registered,
    });
    assert_eq!(
        request_authorization(&Request::ApplyControllerState(
            ApplyControllerStateRequest {
                operation_id,
                controller,
                addr: "127.0.0.1:4565".parse().unwrap(),
                map,
                crl_pem: "crl".into(),
            }
        )),
        RequestAuthorization::ControllerOnly,
    );
    assert_eq!(
        request_authorization(&Request::Backup(admin_proto::BackupRequest {
            target: "/backup".into(),
        })),
        RequestAuthorization::LocalOnly,
    );
    assert_eq!(
        request_authorization(&Request::ReconcileController(
            admin_proto::ReconcileControllerRequest {
                credential: admin_proto::AdminCredential::password("admin", "pw"),
            }
        )),
        RequestAuthorization::AdminAuthenticated,
    );
    assert_eq!(
        request_authorization(&Request::ApplyReferralEdit(ApplyReferralEditRequest {
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
        })),
        RequestAuthorization::ControllerOnly,
    );
    assert_eq!(
        request_authorization(&Request::ApplyServiceControl(
            ApplyServiceControlRequest {
                operation_id,
                units: vec![],
                op: netidx_activation::control::ControlOp::Status,
            },
        )),
        RequestAuthorization::ControllerOnly,
    );
    assert_eq!(
        request_authorization(&Request::RotateRecovery),
        RequestAuthorization::LocalOnly,
    );
    assert_eq!(
        request_authorization(&Request::ExternalCaCsr),
        RequestAuthorization::LocalOnly,
    );
    assert_eq!(
        request_authorization(&Request::ExternalCaInstall(ExternalCaInstallRequest {
            signed_cert_pem: "certificate".into(),
            root_pem: None,
        },)),
        RequestAuthorization::LocalOnly,
    );
    assert_eq!(
        request_authorization(&Request::CaStatus),
        RequestAuthorization::LocalOnly,
    );

    // A home-CA ordinary node may self-register, but it cannot invoke any
    // controller mutation. A foreign co-trusted certificate is represented
    // by both peer flags being false and receives neither authority.
    assert!(
        authorize_request_class(
            RequestAuthorization::ControllerOnly,
            false,
            true,
            false,
        )
        .is_err()
    );
    assert!(
        authorize_request_class(RequestAuthorization::ControllerOnly, false, true, true,)
            .is_ok()
    );
    assert!(
        authorize_request_class(
            RequestAuthorization::ControllerOnly,
            false,
            false,
            false,
        )
        .is_err()
    );
    assert!(
        authorize_request_class(RequestAuthorization::NodeSelf, false, true, false,)
            .is_ok()
    );
    assert!(
        authorize_request_class(RequestAuthorization::NodeSelf, false, false, false,)
            .is_err()
    );
    assert!(
        authorize_request_class(RequestAuthorization::LocalOnly, false, true, true)
            .is_err(),
        "even the controller certificate cannot invoke a local-only backup"
    );
    assert!(
        authorize_request_class(RequestAuthorization::LocalOnly, true, false, false)
            .is_ok()
    );
}
