use super::*;
use crate::admin_server::test_support::signed_empty_crl;
use netidx_admin_proto::policy::SlotKind;

#[tokio::test]
async fn immediate_crl_install_is_signed_atomic_and_all_or_nothing_on_validation() {
    let home = tempfile::tempdir().unwrap();
    let foreign = tempfile::tempdir().unwrap();
    let (home_crl, home_ca) = signed_empty_crl(home.path(), "home-ca").await;
    let (foreign_crl, _) = signed_empty_crl(foreign.path(), "foreign-ca").await;
    let root = tempfile::tempdir().unwrap();
    let config_lock = ConfigDirLock::acquire(root.path()).unwrap();
    let destinations = BTreeSet::from([
        root.path().join("admin/crl.pem"),
        root.path().join("resolver/crl.pem"),
    ]);

    apply_crl_to_destinations(&config_lock, &home_crl, &home_ca, destinations.clone())
        .await
        .unwrap();
    for path in &destinations {
        assert_eq!(std::fs::read_to_string(path).unwrap(), home_crl);
    }

    let error = apply_crl_to_destinations(
        &config_lock,
        &foreign_crl,
        &home_ca,
        destinations.clone(),
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("does not verify"));
    for path in &destinations {
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            home_crl,
            "signature validation happens before any destination is replaced"
        );
    }
}

#[tokio::test]
async fn immediate_crl_partial_results_are_target_identifying_and_sorted() {
    let failed = admin_proto::AdminServerId::new();
    let ok = admin_proto::AdminServerId::new();
    let failed_addr = "127.0.0.1:41001".parse().unwrap();
    let ok_addr = "127.0.0.1:41002".parse().unwrap();
    let results = collect_peer_results(
        vec![(failed, failed_addr), (ok, ok_addr)],
        |server, _addr| async move {
            if server == failed {
                bail!("satellite link down")
            }
            Ok(())
        },
    )
    .await;

    assert_eq!(results.len(), 2);
    assert!(results.windows(2).all(|pair| pair[0].server < pair[1].server));
    let failed_result = results.iter().find(|result| result.server == failed).unwrap();
    assert_eq!(failed_result.addr, failed_addr);
    assert!(failed_result.error.as_deref().unwrap().contains("satellite link down"));
    let ok_result = results.iter().find(|result| result.server == ok).unwrap();
    assert_eq!(ok_result.addr, ok_addr);
    assert!(ok_result.error.is_none());
}

fn scoped_admin(kind: SlotKind, allowed_san: &[&str]) -> crate::ca_vault::Authenticated {
    crate::ca_vault::Authenticated {
        slot_id: uuid::Uuid::new_v4(),
        credential_revision: 0,
        admin: "eu-ops".to_string(),
        policy: netidx_admin_proto::policy::Policy {
            allowed_san: allowed_san.iter().map(|s| s.to_string()).collect(),
            max_validity: Duration::from_secs(86400),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: Default::default(),
            perms_edit_scopes: vec![],
            may_manage_admins: false,
            service_control_scopes: vec![],
        },
        kind,
    }
}

fn issued_for(serial: u64, name: &str) -> ca_store::IssuedRecord {
    ca_store::IssuedRecord {
        req: ca_store::QueuedReq::new(
            admin_proto::NodeKind::Client,
            String::new(),
            name.to_string(),
            Duration::from_secs(60),
            "test".to_string(),
            None,
            None,
        ),
        serial,
        name: name.to_string(),
        spki_fp: String::new(),
        cert_pem: String::new(),
        groups: vec![],
        not_after_unix: ca_store::now_unix() + 3600,
        issued_unix: ca_store::now_unix(),
        warnings: vec![],
        revoked: None,
        push_done: true,
    }
}

/// A serial this CA has no record of must be REFUSED, not passed through.
/// The scope of an unknown serial cannot be evaluated, so there is nothing to
/// authorize against — defaulting to authorized in an authorization loop is
/// the wrong shape even where the subsequent write happens to be a no-op.
#[test]
fn revoke_authority_denies_serials_it_cannot_evaluate() {
    let map = admin_proto::NetworkMap::empty(admin_proto::AdminServerId::new());
    let scoped = scoped_admin(SlotKind::Role, &["*.eu.example"]);

    assert!(revoke_authority(&scoped, &map, 7, None).is_err());
    assert!(
        revoke_authority(&scoped, &map, 7, Some(&issued_for(7, "a.eu.example"))).is_ok()
    );
    assert!(
        revoke_authority(&scoped, &map, 8, Some(&issued_for(8, "a.ap.example"))).is_err()
    );
    // A serving cert whose identity can't be resolved to a cluster is refused.
    assert!(
        revoke_authority(&scoped, &map, 9, Some(&issued_for(9, SERVING_SAN))).is_err()
    );

    // A broad admin needs no record: it is not scope-bound at all.
    let broad = scoped_admin(SlotKind::Signing, &[]);
    assert!(revoke_authority(&broad, &map, 7, None).is_ok());
}
