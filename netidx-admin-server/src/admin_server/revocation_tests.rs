use super::*;
use crate::admin_server::test_support::signed_empty_crl;

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
