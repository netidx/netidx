use super::*;
use crate::admin_proto::{Role, ServerEntry};

#[test]
fn service_control_routing_uses_identity_across_address_reuse() {
    let controller = admin_proto::AdminServerId::new();
    let selected = admin_proto::AdminServerId::new();
    let replacement = admin_proto::AdminServerId::new();
    let old_addr = "10.0.0.10:4565".parse().unwrap();
    let new_addr = "10.0.0.20:4565".parse().unwrap();
    let entry = |id, addr| ServerEntry {
        id,
        addr,
        roles: Role::Resolver.into(),
        resolver: None,
        cluster: None,
        state: admin_proto::ServerState::Registered,
    };
    let mut map = NetworkMap::empty(controller);
    // The selected identity moved, and a different identity reused its old
    // address. Routing by address would now hit `replacement`.
    map.servers.push(entry(selected, new_addr));
    map.servers.push(entry(replacement, old_addr));

    assert_eq!(registered_server_addr(&map, selected), Some(new_addr));
    assert_eq!(registered_server_addr(&map, replacement), Some(old_addr));
    assert_eq!(registered_server_addr(&map, admin_proto::AdminServerId::new()), None);

    map.servers[0].state = admin_proto::ServerState::Enrolled;
    assert_eq!(registered_server_addr(&map, selected), None);
}
