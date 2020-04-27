use crate::{
    path::Path,
    utils::{pack, Chars, Pack},
};
use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use std::fmt::Debug;

fn check<T: Pack + Debug + PartialEq + Eq>(t: T) {
    let mut bytes = pack(&t).expect("encode failed");
    assert_eq!(t.len(), BytesMut::len(&bytes));
    let u = T::decode(&mut bytes).expect("decode failed");
    assert_eq!(t, u)
}

fn chars() -> impl Strategy<Value = Chars> {
    any::<String>().prop_map(Chars::from)
}

fn bytes() -> impl Strategy<Value = Bytes> {
    any::<Vec<u8>>().prop_map(Bytes::from)
}

fn path() -> impl Strategy<Value = Path> {
    chars().prop_map(Path::from)
}

mod resolver {
    use super::*;
    use crate::protocol::resolver::{
        ClientAuthRead, ClientAuthWrite, ClientHello, ClientHelloWrite, CtxId, FromRead,
        FromWrite, PermissionToken, Resolved, ResolverId, ServerAuthWrite,
        ServerHelloRead, ServerHelloWrite, ToRead, ToWrite,
    };
    use fxhash::FxBuildHasher;
    use proptest::{collection, option};
    use std::{collections::HashMap, net::SocketAddr};

    fn client_auth_read() -> impl Strategy<Value = ClientAuthRead> {
        prop_oneof![
            Just(ClientAuthRead::Anonymous),
            any::<u64>().prop_map(|i| ClientAuthRead::Reuse(CtxId::mk(i))),
            bytes().prop_map(ClientAuthRead::Initiate)
        ]
    }

    fn client_auth_write() -> impl Strategy<Value = ClientAuthWrite> {
        prop_oneof![
            Just(ClientAuthWrite::Anonymous),
            Just(ClientAuthWrite::Reuse),
            (option::of(chars()), bytes())
                .prop_map(|(spn, token)| ClientAuthWrite::Initiate { spn, token })
        ]
    }

    fn client_hello_write() -> impl Strategy<Value = ClientHelloWrite> {
        (any::<SocketAddr>(), client_auth_write())
            .prop_map(|(write_addr, auth)| ClientHelloWrite { write_addr, auth })
    }

    fn client_hello() -> impl Strategy<Value = ClientHello> {
        prop_oneof![
            client_auth_read().prop_map(ClientHello::ReadOnly),
            client_hello_write().prop_map(ClientHello::WriteOnly)
        ]
    }

    fn server_hello_read() -> impl Strategy<Value = ServerHelloRead> {
        prop_oneof![
            Just(ServerHelloRead::Anonymous),
            Just(ServerHelloRead::Reused),
            (bytes(), any::<u64>())
                .prop_map(|(tok, id)| ServerHelloRead::Accepted(tok, CtxId::mk(id)))
        ]
    }

    fn server_auth_write() -> impl Strategy<Value = ServerAuthWrite> {
        prop_oneof![
            Just(ServerAuthWrite::Anonymous),
            Just(ServerAuthWrite::Reused),
            bytes().prop_map(ServerAuthWrite::Accepted)
        ]
    }

    fn server_hello_write() -> impl Strategy<Value = ServerHelloWrite> {
        (any::<bool>(), any::<u64>(), server_auth_write()).prop_map(
            |(ttl_expired, resolver_id, auth)| ServerHelloWrite {
                ttl_expired,
                auth,
                resolver_id: ResolverId::mk(resolver_id),
            },
        )
    }

    fn to_read() -> impl Strategy<Value = ToRead> {
        prop_oneof![
            collection::vec(path(), (0, 100)).prop_map(ToRead::Resolve),
            path().prop_map(ToRead::List)
        ]
    }

    fn resolved() -> impl Strategy<Value = Resolved> {
        let krb5_spns = collection::hash_map(any::<SocketAddr>(), chars(), (0, 100))
            .prop_map(|h| {
                let mut hm =
                    HashMap::with_capacity_and_hasher(h.len(), FxBuildHasher::default());
                hm.extend(h.into_iter());
                hm
            });
        let resolver = any::<u64>().prop_map(ResolverId::mk);
        let addr = collection::vec((any::<SocketAddr>(), bytes()), (0, 10));
        let addrs = collection::vec(addr, (0, 100));
        (krb5_spns, resolver, addrs).prop_map(|(krb5_spns, resolver, addrs)| Resolved {
            krb5_spns,
            resolver,
            addrs,
        })
    }

    fn from_read() -> impl Strategy<Value = FromRead> {
        prop_oneof![
            resolved().prop_map(FromRead::Resolved),
            collection::vec(path(), (0, 1000)).prop_map(FromRead::List),
            chars().prop_map(FromRead::Error)
        ]
    }

    fn permission_token() -> impl Strategy<Value = PermissionToken> {
        (path(), any::<u64>()).prop_map(|(path, ts)| PermissionToken(path, ts))
    }

    fn to_write() -> impl Strategy<Value = ToWrite> {
        prop_oneof![
            collection::vec(path(), (0, 1000)).prop_map(ToWrite::Publish),
            collection::vec(path(), (0, 1000)).prop_map(ToWrite::Unpublish),
            Just(ToWrite::Clear),
            Just(ToWrite::Heartbeat)
        ]
    }

    fn from_write() -> impl Strategy<Value = FromWrite> {
        prop_oneof![
            Just(FromWrite::Published),
            Just(FromWrite::Unpublished),
            chars().prop_map(FromWrite::Error)
        ]
    }

    proptest! {
        #[test]
        fn test_client_hello(a in client_hello()) {
            check(a)
        }

        #[test]
        fn test_server_hello_read(a in server_hello_read()) {
            check(a)
        }

        #[test]
        fn test_server_hello_write(a in server_hello_write()) {
            check(a)
        }

        #[test]
        fn test_permission_token(a in permission_token()) {
            check(a)
        }

        #[test]
        fn test_to_read(a in to_read()) {
            check(a)
        }

        #[test]
        fn test_from_read(a in from_read()) {
            check(a)
        }

        #[test]
        fn test_to_write(a in to_write()) {
            check(a)
        }

        #[test]
        fn test_from_write(a in from_write()) {
            check(a)
        }
    }
}
