use bytes::{Bytes, BytesMut};
use netidx_core::{
    chars::Chars,
    pack::{Pack, Z64},
    path::Path,
    pool::Pooled,
    utils::pack,
};
use proptest::{prelude::*, string::string_regex};
use rust_decimal::Decimal;
use std::{fmt::Debug, sync::Arc};

fn check<T: Pack + Debug + PartialEq>(t: T) {
    let mut bytes = pack(&t).expect("encode failed");
    let actual_len = BytesMut::len(&bytes);
    let u = T::decode(&mut bytes).expect("decode failed");
    assert_eq!(t.encoded_len(), actual_len);
    assert_eq!(t, u)
}

fn chars() -> impl Strategy<Value = Chars> {
    any::<String>().prop_map(Chars::from)
}

fn chars_regex(rex: &str) -> impl Strategy<Value = Chars> {
    string_regex(rex).unwrap().prop_map(Chars::from)
}

fn bytes() -> impl Strategy<Value = Bytes> {
    any::<Vec<u8>>().prop_map(Bytes::from)
}

fn path() -> impl Strategy<Value = Path> {
    chars().prop_map(Path::from)
}

mod resolver {
    use super::*;
    use crate::{
        glob::{Glob, GlobSet},
        resolver::{
            Auth, AuthChallenge, AuthRead, AuthWrite, ClientHello, ClientHelloWrite,
            FromRead, FromWrite, GetChangeNr, HashMethod, ListMatching, Publisher,
            PublisherId, PublisherRef, ReadyForOwnershipCheck, Referral, Resolved,
            Secret, ServerHelloWrite, Table, TargetAuth, ToRead, ToWrite, UserInfo,
        },
    };
    use netidx_core::pack::PackError;
    use proptest::collection;
    use std::net::SocketAddr;

    fn fuzz(b: Bytes) {
        type Result<T> = std::result::Result<T, PackError>;
        let _: Result<Auth> = Pack::decode(&mut &*b);
        let _: Result<AuthChallenge> = Pack::decode(&mut &*b);
        let _: Result<AuthRead> = Pack::decode(&mut &*b);
        let _: Result<AuthWrite> = Pack::decode(&mut &*b);
        let _: Result<ClientHello> = Pack::decode(&mut &*b);
        let _: Result<ClientHelloWrite> = Pack::decode(&mut &*b);
        let _: Result<FromRead> = Pack::decode(&mut &*b);
        let _: Result<FromWrite> = Pack::decode(&mut &*b);
        let _: Result<GetChangeNr> = Pack::decode(&mut &*b);
        let _: Result<HashMethod> = Pack::decode(&mut &*b);
        let _: Result<ListMatching> = Pack::decode(&mut &*b);
        let _: Result<Publisher> = Pack::decode(&mut &*b);
        let _: Result<PublisherId> = Pack::decode(&mut &*b);
        let _: Result<PublisherRef> = Pack::decode(&mut &*b);
        let _: Result<ReadyForOwnershipCheck> = Pack::decode(&mut &*b);
        let _: Result<Referral> = Pack::decode(&mut &*b);
        let _: Result<Resolved> = Pack::decode(&mut &*b);
        let _: Result<Secret> = Pack::decode(&mut &*b);
        let _: Result<ServerHelloWrite> = Pack::decode(&mut &*b);
        let _: Result<Table> = Pack::decode(&mut &*b);
        let _: Result<TargetAuth> = Pack::decode(&mut &*b);
        let _: Result<ToRead> = Pack::decode(&mut &*b);
        let _: Result<ToWrite> = Pack::decode(&mut &*b);
    }

    fn auth_challenge() -> impl Strategy<Value = AuthChallenge> {
        (hash_method(), any::<u128>())
            .prop_map(|(hash_method, challenge)| AuthChallenge { hash_method, challenge })
    }

    fn auth_read() -> impl Strategy<Value = AuthRead> {
        prop_oneof![
            Just(AuthRead::Anonymous),
            Just(AuthRead::Krb5),
            Just(AuthRead::Local),
            Just(AuthRead::Tls),
        ]
    }

    fn auth_write() -> impl Strategy<Value = AuthWrite> {
        prop_oneof![
            Just(AuthWrite::Anonymous),
            Just(AuthWrite::Reuse),
            Just(AuthWrite::Local),
            chars().prop_map(|name| AuthWrite::Tls { name }),
            chars().prop_map(|spn| AuthWrite::Krb5 { spn })
        ]
    }

    fn target_auth() -> impl Strategy<Value = TargetAuth> {
        prop_oneof![
            Just(TargetAuth::Anonymous),
            Just(TargetAuth::Local),
            chars().prop_map(|name| TargetAuth::Tls { name }),
            chars().prop_map(|spn| TargetAuth::Krb5 { spn }),
        ]
    }

    fn client_hello_write() -> impl Strategy<Value = ClientHelloWrite> {
        (any::<SocketAddr>(), auth_write())
            .prop_map(|(write_addr, auth)| ClientHelloWrite { write_addr, auth })
    }

    fn client_hello() -> impl Strategy<Value = ClientHello> {
        prop_oneof![
            auth_read().prop_map(ClientHello::ReadOnly),
            client_hello_write().prop_map(ClientHello::WriteOnly)
        ]
    }

    fn server_hello_write() -> impl Strategy<Value = ServerHelloWrite> {
        (any::<u64>(), any::<bool>(), any::<SocketAddr>(), auth_write()).prop_map(
            |(ttl, ttl_expired, resolver_id, auth)| ServerHelloWrite {
                ttl,
                ttl_expired,
                auth,
                resolver_id,
            },
        )
    }

    fn glob() -> impl Strategy<Value = Glob> {
        chars_regex("/[a-zA-Z0-9*/]+").prop_map(|c| Glob::new(c).unwrap())
    }

    fn globset() -> impl Strategy<Value = GlobSet> {
        let published_only = any::<bool>();
        let globs = collection::vec(glob(), (0, 100));
        (published_only, globs).prop_map(|(published_only, globs)| {
            GlobSet::new(published_only, globs).unwrap()
        })
    }

    fn to_read() -> impl Strategy<Value = ToRead> {
        prop_oneof![
            path().prop_map(ToRead::Resolve),
            path().prop_map(ToRead::List),
            path().prop_map(ToRead::Table),
            globset().prop_map(ToRead::ListMatching),
            path().prop_map(ToRead::GetChangeNr),
        ]
    }

    fn publisher_id() -> impl Strategy<Value = PublisherId> {
        any::<u64>().prop_map(PublisherId::mk)
    }

    fn hash_method() -> impl Strategy<Value = HashMethod> {
        prop_oneof![Just(HashMethod::Sha3_512)]
    }

    fn publisher() -> impl Strategy<Value = Publisher> {
        let resolver = any::<SocketAddr>();
        let id = publisher_id();
        let addr = any::<SocketAddr>();
        let hash_method = hash_method();
        let target_auth = target_auth();
        (resolver, id, addr, hash_method, target_auth).prop_map(
            |(resolver, id, addr, hash_method, target_auth)| Publisher {
                resolver,
                id,
                addr,
                hash_method,
                target_auth,
            },
        )
    }

    fn publisher_ref() -> impl Strategy<Value = PublisherRef> {
        (publisher_id(), bytes()).prop_map(|(id, token)| PublisherRef { id, token })
    }

    fn resolved() -> impl Strategy<Value = Resolved> {
        let resolver = any::<SocketAddr>();
        let publishers =
            collection::vec(publisher_ref(), (0, 10)).prop_map(Pooled::orphan);
        let timestamp = any::<u64>();
        let flags = any::<u32>();
        let permissions = any::<u32>();
        (resolver, publishers, timestamp, flags, permissions).prop_map(
            |(resolver, publishers, timestamp, flags, permissions)| Resolved {
                resolver,
                publishers,
                timestamp,
                flags,
                permissions,
            },
        )
    }

    fn auth() -> impl Strategy<Value = Auth> {
        prop_oneof![
            Just(Auth::Anonymous),
            chars().prop_map(|path| Auth::Local { path }),
            chars().prop_map(|spn| Auth::Krb5 { spn }),
        ]
    }

    fn referral() -> impl Strategy<Value = Referral> {
        (
            path(),
            any::<Option<u16>>(),
            collection::vec((any::<SocketAddr>(), auth()), (0, 10)),
        )
            .prop_map(|(path, ttl, addrs)| Referral {
                path,
                ttl,
                addrs: Pooled::orphan(addrs),
            })
    }

    fn table() -> impl Strategy<Value = Table> {
        (
            collection::vec(path(), (0, 1000)),
            collection::vec((path(), any::<u64>().prop_map(Z64)), (0, 1000)),
        )
            .prop_map(|(rows, cols)| Table {
                rows: Pooled::orphan(rows),
                cols: Pooled::orphan(cols),
            })
    }

    fn list_matching() -> impl Strategy<Value = ListMatching> {
        let matched = collection::vec(
            collection::vec(path(), (0, 10)).prop_map(Pooled::orphan),
            (0, 100),
        )
        .prop_map(Pooled::orphan);
        let referrals = collection::vec(referral(), (0, 100)).prop_map(Pooled::orphan);
        (matched, referrals)
            .prop_map(|(matched, referrals)| ListMatching { matched, referrals })
    }

    fn get_change_nr() -> impl Strategy<Value = GetChangeNr> {
        let change_number = any::<u64>().prop_map(|v| Z64(v));
        let resolver = any::<SocketAddr>();
        let referrals = collection::vec(referral(), (0, 100));
        (change_number, resolver, referrals).prop_map(
            |(change_number, resolver, referrals)| GetChangeNr {
                change_number,
                resolver,
                referrals: Pooled::orphan(referrals),
            },
        )
    }

    fn from_read() -> impl Strategy<Value = FromRead> {
        prop_oneof![
            publisher().prop_map(FromRead::Publisher),
            resolved().prop_map(FromRead::Resolved),
            collection::vec(path(), (0, 1000))
                .prop_map(|v| FromRead::List(Pooled::orphan(v))),
            list_matching().prop_map(FromRead::ListMatching),
            get_change_nr().prop_map(FromRead::GetChangeNr),
            table().prop_map(FromRead::Table),
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

    fn user_info() -> impl Strategy<Value = UserInfo> {
        (chars(), chars(), collection::vec(chars(), (0, 20))).prop_map(
            |(name, primary_group, groups)| UserInfo { name, primary_group, groups },
        )
    }

    fn to_write() -> impl Strategy<Value = ToWrite> {
        prop_oneof![
            path().prop_map(ToWrite::Publish),
            path().prop_map(ToWrite::PublishDefault),
            path().prop_map(ToWrite::Unpublish),
            Just(ToWrite::Clear),
            Just(ToWrite::Heartbeat),
            (path(), any::<u32>())
                .prop_map(|(path, flags)| ToWrite::PublishWithFlags(path, flags)),
            (path(), any::<u32>())
                .prop_map(|(path, flags)| ToWrite::PublishDefaultWithFlags(path, flags)),
            path().prop_map(ToWrite::UnpublishDefault),
            chars().prop_map(ToWrite::GetUserInfo),
            Just(ToWrite::Unknown),
        ]
    }

    fn from_write() -> impl Strategy<Value = FromWrite> {
        prop_oneof![
            Just(FromWrite::Published),
            Just(FromWrite::Unpublished),
            referral().prop_map(FromWrite::Referral),
            Just(FromWrite::Denied),
            chars().prop_map(FromWrite::Error),
            user_info().prop_map(FromWrite::UserInfo)
        ]
    }

    proptest! {
        #[test]
        fn test_fuzz(b in bytes()) {
            fuzz(b)
        }

        #[test]
        fn test_auth_challenge(a in auth_challenge()) {
            check(a)
        }

        #[test]
        fn test_client_hello(a in client_hello()) {
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
    use crate::{
        publisher::{From, Hello, Id, To},
        value::Value,
    };
    use chrono::prelude::*;
    use netidx_core::pack::PackError;
    use proptest::collection;
    use std::{net::SocketAddr, time::Duration};

    fn fuzz(b: Bytes) {
        type Result<T> = std::result::Result<T, PackError>;
        let _: Result<From> = Pack::decode(&mut &*b);
        let _: Result<To> = Pack::decode(&mut &*b);
        let _: Result<Hello> = Pack::decode(&mut &*b);
        let _: Result<Id> = Pack::decode(&mut &*b);
        let _: Result<Value> = Pack::decode(&mut &*b);
    }

    fn hello() -> impl Strategy<Value = Hello> {
        prop_oneof![
            Just(Hello::Anonymous),
            Just(Hello::Krb5),
            Just(Hello::Local),
            any::<SocketAddr>().prop_map(Hello::ResolverAuthenticate)
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
            any::<u64>().prop_map(|i| To::Unsubscribe(Id::mk(i))),
            (any::<u64>(), value(), any::<bool>()).prop_map(|(i, v, r)| To::Write(
                Id::mk(i),
                r,
                v
            ))
        ]
    }

    fn datetime() -> impl Strategy<Value = DateTime<Utc>> {
        (
            DateTime::<Utc>::MIN_UTC.timestamp()..DateTime::<Utc>::MAX_UTC.timestamp(),
            0..1_000_000_000u32,
        )
            .prop_map(|(s, ns)| Utc.timestamp_opt(s, ns).unwrap())
    }

    fn duration() -> impl Strategy<Value = Duration> {
        (any::<u64>(), 0..1_000_000_000u32).prop_map(|(s, ns)| Duration::new(s, ns))
    }

    fn value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
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
            any::<[u8; 16]>().prop_map(|a| Value::Decimal(Decimal::deserialize(a))),
            datetime().prop_map(Value::DateTime),
            duration().prop_map(Value::Duration),
            chars().prop_map(Value::String),
            bytes().prop_map(Value::Bytes),
            Just(Value::True),
            Just(Value::False),
            Just(Value::Null),
            Just(Value::Ok),
            chars().prop_map(Value::Error),
        ];
        leaf.prop_recursive(10, 1000, 100, |inner| {
            prop_oneof![collection::vec(inner.clone(), 0..100)
                .prop_map(|e| Value::Array(Arc::from(e)))]
        })
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
            Just(From::Heartbeat),
            (any::<u64>(), value()).prop_map(|(i, v)| From::WriteResult(Id::mk(i), v))
        ]
    }

    fn vequiv(v0: &Value, v1: &Value) -> bool {
        match (v0, v1) {
            (Value::Duration(d0), Value::Duration(d1)) => {
                let f0 = d0.as_secs_f64();
                let f1 = d1.as_secs_f64();
                f0 == f1 || (f0 != 0. && f1 != 0. && ((f0 - f1).abs() / f0) < 1e-8)
            }
            (Value::F32(v0), Value::F32(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-7,
            (Value::F64(v0), Value::F64(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-8,
            (Value::Array(e0), Value::Array(e1)) => {
                e0.len() == e1.len()
                    && e0.iter().zip(e1.iter()).all(|(v0, v1)| vequiv(v0, v1))
            }
            (v0, v1) => v0 == v1,
        }
    }

    fn round_trip(v: Value) {
        let s = format!("{}", v);
        let v_ = s.parse::<Value>().unwrap();
        assert!(vequiv(&v, &v_))
    }

    proptest! {
        #[test]
        fn test_fuzz(b in bytes()) {
            fuzz(b)
        }

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

        #[test]
        fn test_value_roundtrip(v in value()) {
            round_trip(v)
        }
    }
}
