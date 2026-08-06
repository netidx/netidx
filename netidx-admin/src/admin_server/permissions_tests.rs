use super::super::test_support::test_server;
use super::*;
use crate::{ca_store, config_lock::ConfigDirLock};
use enumflags2::BitFlags;

/// A local perms read on the CA host must answer from the CA's model, not from
/// this host's perms file.
///
/// Every `perms set` / `perms remove` is a read-modify-write, and the local
/// file trails the model by up to a poll interval. Reading the file meant two
/// edits inside that window silently dropped the first — the operator saw "ok"
/// twice and lost a grant. Proven in the lab: `set /lost carol swl` then
/// `set /lost dave swl` seconds apart left only dave.
#[tokio::test]
async fn a_local_read_on_the_ca_host_comes_from_the_model_not_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let lock = ConfigDirLock::acquire_for_ca_dir(dir.path()).await.unwrap();
    let ca = ca_store::CaDir::open(lock, dir.path()).await.unwrap();
    let state = test_server(Some(ca));
    // The model must be reachable only through this host's own grant, so give
    // it one — the same shape a registered resolver has.
    let cluster = admin_proto::ResolverClusterId::new();
    let (id, addr) = state.read(|state| (state.cfg.server_id, state.cfg.listen)).await;
    state
        .write(move |state| {
            state.map.admin_servers.push(admin_proto::AdminServerEntry {
                cluster: Some(cluster),
                ..admin_proto::AdminServerEntry::granted(
                    id,
                    addr,
                    admin_proto::Role::Ca | admin_proto::Role::Resolver,
                    None,
                    None,
                    admin_proto::ServerState::Registered,
                )
            });
            state.map.resolver_clusters.push(admin_proto::ResolverClusterEntry {
                id: cluster,
                base: "/".into(),
                state: admin_proto::ResolverClusterState::Active,
                members: vec![],
                parent: None,
                children: vec![],
                perms_version: None,
            });
        })
        .await;
    let mut model = crate::perms_model::PermsModel::default();
    let mut perms = crate::perms::empty();
    crate::perms::add_entry(&mut perms, "/lost", "carol", "swl").unwrap();
    model.set(cluster, &perms).unwrap();
    state
        .read_async(async move |state| {
            state.ca.as_ref().unwrap().store.save_perms_model(&model).await
        })
        .await
        .unwrap();
    // The real entry point, on the path an on-box `netidx admin perms` takes:
    // the local control socket, whose SO_PEERCRED superuser needs no
    // credential.
    let req = admin_proto::ReadPermsRequest {
        credential: admin_proto::AdminCredential::Password {
            admin: String::new(),
            password: admin_proto::Secret(String::new()),
        },
        target_path: "/".to_string(),
    };
    let got = match handle_read_perms(
        &state,
        &req,
        &PreparedAdminAuthentication::Password(Err(String::new())),
        true,
    )
    .await
    {
        admin_proto::ReadPermsResponse::Ok(ok) => ok.perms,
        admin_proto::ReadPermsResponse::Err { reason } => panic!("{reason}"),
    };
    assert_eq!(
        crate::perms::lookup(&got, "/lost", "carol").map(|b| b.as_str()),
        Some("swl"),
        "the read must see what the CA recorded, not what this host's file holds"
    );
}

#[test]
fn perms_reads_and_edits_share_the_same_scope_authorization() {
    let role = ca_vault::Authenticated {
        slot_id: uuid::Uuid::new_v4(),
        credential_revision: 0,
        admin: "eu-ops".into(),
        policy: netidx_admin_proto::policy::Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(60),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: BitFlags::empty(),
            perms_edit_scopes: vec!["/eu".into()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        },
        kind: netidx_admin_proto::policy::SlotKind::Role,
    };
    for operation in ["read", "edit"] {
        assert!(authorize_perms_scope(&role, "/eu", operation).is_ok());
        assert!(authorize_perms_scope(&role, "/eu/ap", operation).is_ok());
        assert!(authorize_perms_scope(&role, "/", operation).is_err());
        assert!(authorize_perms_scope(&role, "/us", operation).is_err());
    }
    let signing = ca_vault::Authenticated {
        kind: netidx_admin_proto::policy::SlotKind::Signing,
        ..role
    };
    assert!(authorize_perms_scope(&signing, "/", "read").is_ok());
    assert!(authorize_perms_scope(&signing, "/us", "edit").is_ok());
}
