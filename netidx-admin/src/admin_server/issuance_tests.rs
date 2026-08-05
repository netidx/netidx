use super::*;

#[test]
fn resolver_replicas_may_share_a_tls_name_with_distinct_keys() {
    assert!(!one_live_name(NodeKind::Resolver));
    assert!(one_live_name(NodeKind::Client));
    assert!(one_live_name(NodeKind::Publisher));
    assert!(one_live_name(NodeKind::Workstation));
}
