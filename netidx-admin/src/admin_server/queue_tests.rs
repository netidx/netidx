use super::*;
use crate::{
    admin_proto::{InfoAuth, Role},
    ca::{SanEntry, Subject, generate_csr},
    config_lock::ConfigDirLock,
};

async fn empty_ca(dir: &std::path::Path) -> ca_store::CaDir {
    let lock = ConfigDirLock::acquire_for_ca_dir(dir).await.unwrap();
    ca_store::CaDir::open(lock, dir).await.unwrap()
}

fn enqueue_request(name: &str, csr_pem: &str) -> EnqueueRequest {
    EnqueueRequest {
        kind: admin_proto::NodeKind::Workstation,
        csr_pem: csr_pem.to_string(),
        requested_name: name.to_string(),
        requested_validity: Duration::from_secs(3600),
        enrollment: None,
        replaces_serial: None,
    }
}

/// Enqueue is unauthenticated and its name checks scan and parse every record
/// in `issued/`. A full queue must be refused before any of that, or a flood of
/// cheap connections turns into unbounded disk I/O on the CA.
#[tokio::test]
async fn a_full_queue_is_refused_before_the_issuance_index_is_scanned() {
    let dir = tempfile::tempdir().unwrap();
    let mut ca = empty_ca(dir.path()).await;
    let csr = generate_csr(
        &Subject::cn("host.example.com"),
        &[SanEntry::Dns("host.example.com".to_string())],
        crate::ca::MIN_KEY_BITS,
        None,
    )
    .unwrap();
    let csr_pem = String::from_utf8(csr.csr_pem).unwrap();
    let peer: SocketAddr = "10.0.0.7:51000".parse().unwrap();

    // A request against an empty queue is accepted, and does consult the index.
    let before = ca_store::issued_scans();
    let resp = handle_enqueue(
        &mut ca,
        &enqueue_request("host.example.com", &csr_pem),
        peer,
        None,
    )
    .await;
    assert!(matches!(resp, EnqueueResponse::Ok(_)), "{resp:?}");
    assert!(ca_store::issued_scans() > before);

    // Fill the queue to its cap.
    for i in 1..ca_store::MAX_PENDING {
        let name = format!("n{i}.example.com");
        let resp =
            handle_enqueue(&mut ca, &enqueue_request(&name, &csr_pem), peer, None).await;
        assert!(matches!(resp, EnqueueResponse::Ok(_)), "{resp:?}");
    }

    // Now every further request must be refused without touching `issued/`.
    let before = ca_store::issued_scans();
    let resp = handle_enqueue(
        &mut ca,
        &enqueue_request("overflow.example.com", &csr_pem),
        peer,
        None,
    )
    .await;
    match resp {
        EnqueueResponse::Err { reason } => assert!(reason.contains("full"), "{reason}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(
        ca_store::issued_scans(),
        before,
        "a full queue must be refused before the issuance index is scanned"
    );
}

#[tokio::test]
async fn a_failed_grant_on_approve_restores_the_queue() {
    use super::super::test_support::{signing_ca, test_server};
    let mut fixture = signing_ca().await;
    let ca_dir = fixture.dir.path().to_path_buf();
    let csr = generate_csr(
        &Subject::cn(SERVING_SAN),
        &[SanEntry::Dns(SERVING_SAN.to_string())],
        crate::ca::MIN_KEY_BITS,
        None,
    )
    .unwrap();
    let csr_pem = String::from_utf8(csr.csr_pem).unwrap();
    let member = crate::admin_proto::ResolverAddr {
        addr: "10.0.0.10:4564".parse().unwrap(),
        auth: InfoAuth::Anonymous,
    };
    let queued = ca_store::QueuedReq::new(
        admin_proto::NodeKind::AdminServer,
        csr_pem,
        SERVING_SAN.to_string(),
        Duration::from_secs(3600),
        "10.0.0.10:1".into(),
        None,
        Some(admin_proto::EnrollmentRequest {
            resolver_config: None,
            listen: "10.0.0.10:4565".parse().unwrap(),
            roles: Role::Resolver.into(),
            resolver_member: Some(member.clone()),
            resolver_members: vec![member],
            cluster: admin_proto::ResolverClusterPlacement::Create { base: "/eu".into() },
            replaces: None,
        }),
    );
    let request_id = queued.id.clone();
    fixture.ca.store.enqueue(&queued).await.unwrap();
    // CaDir moved into the server after enqueue — reopen via the server.
    let state = test_server(Some(fixture.ca));
    std::fs::create_dir(ca_dir.join("admin-domain.json")).unwrap();
    let resp = handle_approve_request(
        &state,
        &ApproveRequest {
            credential: fixture.credential.clone(),
            request_id: request_id.clone(),
            id_map_groups: Vec::new(),
        },
        &fixture.authentication,
        &fixture.server_unlock,
    )
    .await;
    match resp {
        ApproveResponse::Err { reason } => {
            assert!(reason.contains("recording enrollment grant"), "{reason}")
        }
        other => panic!("expected a grant error, got {other:?}"),
    }
    let status = state
        .read_async(async move |state| {
            state.ca.as_ref().unwrap().store.status(&request_id).await
        })
        .await
        .unwrap();
    assert!(
        matches!(status, ca_store::Status::Pending(_)),
        "approve must put the request back in the queue"
    );
}
