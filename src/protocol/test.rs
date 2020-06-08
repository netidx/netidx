use crate::{chars::Chars, pack::Pack, path::Path, utils::pack};
use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use std::fmt::Debug;

fn check<T: Pack + Debug + PartialEq>(t: T) {
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
    use crate::protocol::resolver::v1::{
        ClientAuthRead, ClientAuthWrite, ClientHello, ClientHelloWrite, CtxId, FromRead,
        FromWrite, ReadyForOwnershipCheck, Referral, Resolved, Secret,
        ServerAuthWrite, ServerHelloRead, ServerHelloWrite, ToRead, ToWrite,
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
        (any::<u64>(), any::<bool>(), any::<SocketAddr>(), server_auth_write()).prop_map(
            |(ttl, ttl_expired, resolver_id, auth)| ServerHelloWrite {
                ttl,
                ttl_expired,
                auth,
                resolver_id,
            },
        )
    }

    fn to_read() -> impl Strategy<Value = ToRead> {
        prop_oneof![path().prop_map(ToRead::Resolve), path().prop_map(ToRead::List)]
    }

    fn krb5_spns() -> impl Strategy<Value = HashMap<SocketAddr, Chars, FxBuildHasher>> {
        collection::hash_map(any::<SocketAddr>(), chars(), (0, 100)).prop_map(|h| {
            let mut hm =
                HashMap::with_capacity_and_hasher(h.len(), FxBuildHasher::default());
            hm.extend(h.into_iter());
            hm
        })
    }

    fn resolved() -> impl Strategy<Value = Resolved> {
        let resolver = any::<SocketAddr>();
        let addrs = collection::vec((any::<SocketAddr>(), bytes()), (0, 10));
        let timestamp = any::<u64>();
        let permissions = any::<u32>();
        (krb5_spns(), resolver, addrs, timestamp, permissions).prop_map(
            |(krb5_spns, resolver, addrs, timestamp, permissions)| Resolved {
                krb5_spns,
                resolver,
                addrs,
                timestamp,
                permissions,
            },
        )
    }

    fn referral() -> impl Strategy<Value = Referral> {
        (path(), any::<u64>(), collection::vec(any::<SocketAddr>(), (0, 10)), krb5_spns())
            .prop_map(|(path, ttl, addrs, krb5_spns)| Referral {
                path,
                ttl,
                addrs,
                krb5_spns,
            })
    }

    fn from_read() -> impl Strategy<Value = FromRead> {
        prop_oneof![
            resolved().prop_map(FromRead::Resolved),
            collection::vec(path(), (0, 1000)).prop_map(FromRead::List),
            referral().prop_map(FromRead::Referral),
            Just(FromRead::Denied),
            chars().prop_map(FromRead::Error)
        ]
    }

    fn secret() -> impl Strategy<Value = Secret> {
        any::<u128>().prop_map(Secret)
    }

    fn ready_for_ownership_check() -> impl Strategy<Value = ReadyForOwnershipCheck> {
        any::<u8>().prop_map(|_| ReadyForOwnershipCheck)
    }

    fn to_write() -> impl Strategy<Value = ToWrite> {
        prop_oneof![
            path().prop_map(ToWrite::Publish),
            path().prop_map(ToWrite::Unpublish),
            Just(ToWrite::Clear),
            Just(ToWrite::Heartbeat)
        ]
    }

    fn from_write() -> impl Strategy<Value = FromWrite> {
        prop_oneof![
            Just(FromWrite::Published),
            Just(FromWrite::Unpublished),
            referral().prop_map(FromWrite::Referral),
            Just(FromWrite::Denied),
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

        #[test]
        fn test_secret(a in secret()) {
            check(a)
        }

        #[test]
        fn test_read_for_ownership_check(a in ready_for_ownership_check()) {
            check(a)
        }
    }
}

mod publisher {
    use super::*;
    use crate::protocol::publisher::v1::{From, Hello, Id, To, Value};
    use std::net::SocketAddr;

    fn hello() -> impl Strategy<Value = Hello> {
        prop_oneof![
            Just(Hello::Anonymous),
            bytes().prop_map(Hello::Token),
            (any::<SocketAddr>(), bytes())
                .prop_map(|(i, b)| Hello::ResolverAuthenticate(i, b))
        ]
    }

    fn to() -> impl Strategy<Value = To> {
        prop_oneof![
            (path(), any::<SocketAddr>(), any::<u64>(), any::<u32>(), bytes()).prop_map(
                |(path, resolver, timestamp, permissions, token)| To::Subscribe {
                    path,
                    resolver,
                    timestamp,
                    permissions,
                    token
                }
            ),
            any::<u64>().prop_map(|i| To::Unsubscribe(Id::mk(i)))
        ]
    }

    fn value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<u32>().prop_map(Value::U32),
            any::<u32>().prop_map(Value::V32),
            any::<i32>().prop_map(Value::I32),
            any::<i32>().prop_map(Value::Z32),
            any::<u64>().prop_map(Value::U64),
            any::<u64>().prop_map(Value::V64),
            any::<i64>().prop_map(Value::I64),
            any::<i64>().prop_map(Value::Z64),
            any::<f32>().prop_map(Value::F32),
            any::<f64>().prop_map(Value::F64),
            chars().prop_map(Value::String),
            bytes().prop_map(Value::Bytes),
            Just(Value::True),
            Just(Value::False),
        ]
    }

    fn from() -> impl Strategy<Value = From> {
        prop_oneof![
            path().prop_map(From::NoSuchValue),
            path().prop_map(From::Denied),
            any::<u64>().prop_map(|i| From::Unsubscribed(Id::mk(i))),
            (path(), any::<u64>(), value()).prop_map(|(p, i, v)| From::Subscribed(
                p,
                Id::mk(i),
                v
            )),
            (any::<u64>(), value()).prop_map(|(i, v)| From::Update(Id::mk(i), v)),
            Just(From::Heartbeat)
        ]
    }

    proptest! {
        #[test]
        fn test_hello(a in hello()) {
            check(a)
        }

        #[test]
        fn test_to(a in to()) {
            check(a)
        }

        #[test]
        fn test_from(a in from()) {
            check(a)
        }
    }
}
