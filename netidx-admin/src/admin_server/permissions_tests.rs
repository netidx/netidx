use super::*;
use enumflags2::BitFlags;

#[test]
fn perms_reads_and_edits_share_the_same_scope_authorization() {
    let role = ca_vault::Authenticated {
        slot_id: uuid::Uuid::new_v4(),
        credential_revision: 0,
        admin: "eu-ops".into(),
        policy: ca_vault::Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(60),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: BitFlags::empty(),
            perms_edit_scopes: vec!["/eu".into()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        },
        kind: ca_vault::SlotKind::Role,
    };
    for operation in ["read", "edit"] {
        assert!(authorize_perms_scope(&role, "/eu", operation).is_ok());
        assert!(authorize_perms_scope(&role, "/eu/ap", operation).is_ok());
        assert!(authorize_perms_scope(&role, "/", operation).is_err());
        assert!(authorize_perms_scope(&role, "/us", operation).is_err());
    }
    let signing = ca_vault::Authenticated { kind: ca_vault::SlotKind::Signing, ..role };
    assert!(authorize_perms_scope(&signing, "/", "read").is_ok());
    assert!(authorize_perms_scope(&signing, "/us", "edit").is_ok());
}
