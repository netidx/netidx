//! The first live integration coverage of the admin plane: found a
//! real domain with the harness, then drive the same `ops` functions
//! every frontend uses against the running daemon — TLS transport,
//! identity confirmation, password auth, and the query surface.
#![cfg(all(unix, feature = "testing"))]

use anyhow::Result;
use netidx_admin::{
    ops,
    testing::{SetupAnswerer, TestAdminDomain},
};

#[tokio::test(flavor = "multi_thread")]
async fn founding_serving_and_ops_round_trip() -> Result<()> {
    let d = TestAdminDomain::start().await?;
    assert!(!d.recovery_password.is_empty());
    // Authenticate exactly as a frontend's connect does: the answerer
    // confirms the CA identity and supplies the superuser's password.
    let mut ans = SetupAnswerer::new(&d.password);
    let target = ops::resolve_admin_target(
        &mut ans,
        Some(d.listen),
        None,
        Some(d.admin.clone()),
        None,
    )
    .await?;
    // The roster holds the founding superuser, with roster authority.
    let admins = ops::roster::list_admins(&target).await?;
    let root = admins
        .iter()
        .find(|a| a.admin == d.admin)
        .expect("the founding superuser is on the roster");
    assert!(root.policy.may_manage_admins);
    assert!(!root.must_change);
    // A fresh domain has an empty enrollment queue.
    let rows = ops::queue::list_queue(
        &mut ans,
        Some(d.listen),
        None,
        Some(d.admin.clone()),
        None,
    )
    .await?;
    assert!(rows.is_empty(), "fresh queue not empty: {} rows", rows.len());
    // And nothing pending in delegations.
    let dels = ops::delegation::list_pending_delegations(
        &mut ans,
        Some(d.listen),
        None,
        Some(d.admin.clone()),
        None,
    )
    .await?;
    assert!(dels.is_empty());
    // The server list shows this host's own admin server.
    let servers = ops::servers::list_servers(&mut ans, Some(d.listen), None).await?;
    assert_eq!(servers.len(), 1);
    assert!(servers[0].ca);
    // NB a second auth as root with a WRONG password would still
    // succeed here: the first success cached a session, and a
    // non-explicit password never gets asked while the cache holds —
    // the designed `has_explicit_secret` behavior, not a hole. The
    // wrong-password test needs an admin with no cached session:
    // mint one, whose one-time password also exercises the
    // PasswordChangeRequired routing every frontend depends on.
    let one_time = ops::roster::add_role_admin(
        &target,
        "alice",
        netidx_admin_proto::policy::Policy {
            allowed_san: vec![],
            max_validity: std::time::Duration::from_secs(86400),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: Default::default(),
            perms_edit_scopes: vec![],
            may_manage_admins: false,
            service_control_scopes: vec![],
        },
    )
    .await?;
    let admins = ops::roster::list_admins(&target).await?;
    let alice = admins.iter().find(|a| a.admin == "alice").expect("alice minted");
    assert!(alice.must_change, "a minted admin holds a one-time key");
    // A password session is a credential holder — opening one does not
    // contact the server, so a wrong password "succeeds" here and is
    // refused at the first authenticated use (which is why connect
    // flows verify via cache_session before declaring victory).
    let mut bad = SetupAnswerer::new("not-the-password");
    let bad_target = ops::resolve_admin_target(
        &mut bad,
        Some(d.listen),
        None,
        Some("alice".to_string()),
        None,
    )
    .await?;
    assert!(
        ops::roster::list_admins(&bad_target).await.is_err(),
        "a wrong password was accepted by an authenticated op"
    );
    match &bad_target {
        ops::AdminTarget::Remote { session } => assert!(
            ops::cache_session(session, ops::Retention::ProcessLifetime).await.is_err(),
            "a wrong password minted a bearer token"
        ),
        _ => unreachable!(),
    }
    // The one-time password verifies but authorizes only its own
    // replacement — the typed error frontends route on, surfaced at
    // the same verification step connect uses.
    let mut ot = SetupAnswerer::new(one_time.as_str());
    let ot_target = ops::resolve_admin_target(
        &mut ot,
        Some(d.listen),
        None,
        Some("alice".to_string()),
        None,
    )
    .await?;
    let e = match &ot_target {
        ops::AdminTarget::Remote { session } => {
            match ops::cache_session(session, ops::Retention::ProcessLifetime).await {
                Ok(_) => panic!("a one-time key minted a normal session token"),
                Err(e) => e,
            }
        }
        _ => unreachable!(),
    };
    assert!(
        ops::password_change_required(&e).is_some(),
        "expected PasswordChangeRequired, got: {e:#}"
    );
    Ok(())
}
