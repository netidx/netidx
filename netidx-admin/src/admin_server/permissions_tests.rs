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

/// An edit submits the whole document, so it erases anything the editor did
/// not see. The CA therefore refuses one whose base version is no longer
/// current, and hands back what is.
///
/// Without this, two admins editing the same cluster — or one editor and one
/// `perms set` — silently drop each other's work, and both are told "ok".
#[tokio::test]
async fn an_edit_from_a_stale_version_is_refused_and_given_the_current_one() {
    let dir = tempfile::tempdir().unwrap();
    let lock = ConfigDirLock::acquire_for_ca_dir(dir.path()).await.unwrap();
    let ca = ca_store::CaDir::open(lock, dir.path()).await.unwrap();
    let state = test_server(Some(ca));
    let cluster = admin_proto::ResolverClusterId::new();

    let with = |path: &str, entity: &str| {
        let mut p = crate::perms::empty();
        crate::perms::add_entry(&mut p, path, entity, "swl").unwrap();
        p
    };

    // Nothing recorded yet, so an edit based on "no model" is the current one.
    let first =
        record_in_model(&state, cluster, &with("/a", "alice"), None).await.unwrap();
    let admin_proto::EditOutcome::Recorded(first) = first else {
        panic!("the first edit had a current base")
    };
    assert_eq!(first.version, 1);

    // A second editor that also read "no model" — it started before the first
    // landed. Its document would erase alice, so it is refused.
    let stale = record_in_model(&state, cluster, &with("/b", "bob"), None).await.unwrap();
    let admin_proto::EditOutcome::Stale { current_version, current } = stale else {
        panic!("an edit based on a version that has moved must not be recorded")
    };
    assert_eq!(current_version, Some(1));
    assert_eq!(
        crate::perms::lookup(&current, "/a", "alice").map(|b| b.as_str()),
        Some("swl"),
        "the refusal carries what is current, so the caller can rebase on it"
    );

    // Rebased onto what it was given, the same intent records.
    let mut rebased = current;
    crate::perms::add_entry(&mut rebased, "/b", "bob", "swl").unwrap();
    let ok = record_in_model(&state, cluster, &rebased, current_version).await.unwrap();
    let admin_proto::EditOutcome::Recorded(ok) = ok else {
        panic!("a rebased edit is current again")
    };
    assert_eq!(ok.version, 2);
    assert!(ok.changed);

    // And alice survived, which is the whole point.
    let after = state
        .read_async(async move |state| {
            state.ca.as_ref().unwrap().store.perms_model().await.unwrap()
        })
        .await
        .get(cluster)
        .unwrap()
        .perms
        .clone();
    assert!(crate::perms::lookup(&after, "/a", "alice").is_some());
    assert!(crate::perms::lookup(&after, "/b", "bob").is_some());
}

#[test]
fn perms_reads_and_edits_share_the_same_scope_authorization() {
    let role = ca_vault::Authenticated {
        slot_id: uuid::Uuid::new_v4(),
        credential_revision: 0,
        must_change: false,
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
