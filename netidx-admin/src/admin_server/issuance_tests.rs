use super::*;

#[test]
fn resolver_replicas_may_share_a_tls_name_with_distinct_keys() {
    assert!(!one_live_name(NodeKind::Resolver));
    assert!(one_live_name(NodeKind::Client));
    assert!(one_live_name(NodeKind::Publisher));
    assert!(one_live_name(NodeKind::Workstation));
}

#[test]
fn id_map_reconciliation_uses_latest_groups_for_every_live_name() {
    let now = 10_000;
    let record = |serial: u64, name: &str, groups: &[&str], not_after_unix| {
        ca_store::IssuedRecord {
            req: ca_store::QueuedReq::new(
                NodeKind::Client,
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
            groups: groups.iter().map(|group| (*group).to_string()).collect(),
            not_after_unix,
            issued_unix: serial,
            warnings: Vec::new(),
            revoked: None,
            push_done: true,
        }
    };
    let mut records = vec![
        record(1, "alice.example", &["users"], now - 1),
        record(2, "alice.example", &[], now + 100),
        record(3, "bob.example", &["old"], now + 100),
        record(4, "bob.example", &["users"], now + 100),
        record(5, "expired.example", &["users"], now - 1),
    ];
    records[1].req.renewal_of = Some(1);

    let selected: Vec<_> = records
        .iter()
        .enumerate()
        .filter(|(index, _)| reconcile_identity_at(&records, *index, now))
        .map(|(_, record)| (record.name.as_str(), record.groups[0].as_str()))
        .collect();
    assert_eq!(selected, vec![("alice.example", "users"), ("bob.example", "users")]);
}
