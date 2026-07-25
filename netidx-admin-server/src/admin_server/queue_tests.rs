use super::*;
use crate::{
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
