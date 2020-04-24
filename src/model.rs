use crate::protocol::shared;
use bytes::Bytes;
use std::net;

pub fn protobuf_socketaddr_to_std(p: shared::SocketAddr) -> Option<net::SocketAddr> {
    p.addr.map(|addr| match addr {
        Some(shared::SocketAddr_oneof_addr::V4(addr)) => {
            let octets = addr.octets.to_be_bytes();
            let ip = net::Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]);
            Some(net::SocketAddr::V4(net::SocketAddrV4::new(
                ip,
                addr.port as u16,
            )))
        }
        Some(shared::SocketAddr_oneof_addr::V6(addr)) => {
            if addr.octets.len() != 16 {
                None
            } else {
                let a = addr.octets.get_u16();
                let b = addr.octets.get_u16();
                let c = addr.octets.get_u16();
                let d = addr.octets.get_u16();
                let e = addr.octets.get_u16();
                let f = addr.octets.get_u16();
                let g = addr.octets.get_u16();
                let h = addr.octets.get_u16();
                let ip = net::Ipv6Addr::new(a, b, c, d, e, f, g, h);
                let a =
                    net::SocketAddrV6::new(ip, addr.port, addr.flowinfo, addr.scope_id);
                Some(net::SocketAddr::V6(a))
            }
        }
    })
}

pub fn std_socketaddr_to_protobuf(a: net::SocketAddr) -> shared::SocketAddr {
    match a {
        net::SocketAddr::V4(v4) => shared::SocketAddr {
            addr: shared::SocketAddr_oneof_addr::V4(shared::SocketAddr_SocketAddrV4 {
                octets: u32::from_be_bytes(v4.ip().octets()),
                port: v4.port() as u32,
                ..shared::SocketAddr_SocketAddrV4::default()
            }),
            ..shared::SocketAddr::default()
        },
        net::SocketAddr::V6(v6) => shared::SocketAddr {
            addr: shared::SocketAddr_oneof_addr::V6(shared::SocketAddr_SocketAddrV6 {
                octets: Bytes::from(&v6.ip().octets()[..]),
                port: v6.port() as u16,
                flowinfo: v6.flowinfo(),
                scope_id: v6.scope_id(),
                ..shared::SocketAddr_SocketAddrV6::default()
            }),
            ..shared::SocketAddr::default()
        },
    }
}

pub mod resolver {
    use crate::{path::Path, protocol};
    use anyhow::{anyhow, Result};
    use bytes::Bytes;
    use fxhash::FxBuildHasher;
    use protobuf::Chars;
    use std::{collections::HashMap, net::SocketAddr};
    use super::{protobuf_socketaddr_to_std, std_socketaddr_to_protobuf};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct CtxId(u64);

    impl CtxId {
        pub fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            CtxId(NEXT.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ResolverId(u64);

    #[derive(Clone, Debug)]
    pub enum ClientAuthRead {
        Anonymous,
        Reuse(CtxId),
        Initiate(Bytes),
    }

    #[derive(Clone, Debug)]
    pub enum ClientAuthWrite {
        Anonymous,
        Reuse,
        Initiate { spn: Option<Chars>, token: Bytes },
    }

    #[derive(Clone, Debug)]
    pub struct ClientHelloWrite {
        pub write_addr: SocketAddr,
        pub auth: ClientAuthWrite,
    }

    #[derive(Clone, Debug)]
    pub enum ClientHello {
        /// Instruct the resolver server that this connection will not
        /// publish paths.
        ReadOnly(ClientAuthRead),
        /// Instruct the resolver server that this connection will
        /// only publish paths. All published paths will use the
        /// specified address `write_addr`, and the publisher must
        /// send a heartbeat at least every `ttl` seconds or the
        /// resolver server will purge all paths published by
        /// `write_addr`.
        WriteOnly(ClientHelloWrite),
    }

    impl ClientHello {
        pub fn from_protobuf(msg: protocol::resolver::ClientHello) -> Result<Self> {
            use protocol::resolver::{
                ClientHello_Read, ClientHello_Read_Initiate, ClientHello_Read_Reuse,
                ClientHello_Read_oneof_auth, ClientHello_Write, ClientHello_oneof_hello,
            };
            match msg.hello {
                None => return Err(anyhow!("hello is required")),
                Some(hello) => match hello {
                    ClientHello_oneof_hello::ReadOnly(hello) => match hello.auth {
                        None => return Err(anyhow!("auth is required")),
                        Some(ClientHello_Read_oneof_auth::Anonymous(_)) => {
                            Ok(ClientHello::ReadOnly(ClientAuthRead::Anonymous))
                        }
                        Some(ClientHello_Read_oneof_auth::Reuse(r)) => {
                            Ok(ClientHello::ReadOnly(ClientAuthRead::Reuse(CtxId::from(
                                r.session_id,
                            ))))
                        }
                        Some(ClientHello_Read_oneof_auth::Initiate(i)) => {
                            Ok(ClientHello::ReadOnly(ClientAuthRead::Initiate(i.token)))
                        }
                    },
                    ClientHello_oneof_hello::WriteOnly(hello) => {
                        let write_addr = hello
                            .write_addr
                            .take()
                            .and_then(protobuf_socketaddr_to_std)
                            .ok_or_else(|| {
                                anyhow!("protocol error, write addr required")
                            })?;
                        
                    }
                },
            }
        }
    }

    #[derive(Clone, Debug)]
    pub enum ServerHelloRead {
        Anonymous,
        Reused,
        Accepted(Bytes, CtxId),
    }

    #[derive(Clone, Debug)]
    pub enum ServerAuthWrite {
        Anonymous,
        Reused,
        Accepted(Bytes),
    }

    #[derive(Clone, Debug)]
    pub struct ServerHelloWrite {
        pub ttl_expired: bool,
        pub resolver_id: ResolverId,
        pub auth: ServerAuthWrite,
    }

    #[derive(Clone, Debug)]
    pub enum ToRead {
        /// Resolve the list of paths to addresses/ports
        Resolve(Vec<Path>),
        /// List the paths published under the specified root path
        List(Path),
    }

    #[derive(Clone, Debug)]
    pub struct Resolved {
        pub krb5_spns: HashMap<SocketAddr, Chars, FxBuildHasher>,
        pub resolver: ResolverId,
        pub addrs: Vec<Vec<(SocketAddr, Bytes)>>,
    }

    #[derive(Clone, Debug)]
    pub enum FromRead {
        Resolved(Resolved),
        List(Vec<Path>),
        Error(Chars),
    }

    /// This is the format of the Vec<u8> passed back with each
    /// Resolved msg, however it is encrypted with the publisher's
    /// resolver security context. This allows the subscriber to prove
    /// to the publisher that the resolver authorized it to subscribe
    /// to the specified path (because the subsciber can't decrypt or
    /// fabricate the token without the session key shared by the
    /// resolver server and the publisher).
    #[derive(Clone, Debug)]
    pub struct PermissionToken(pub Chars, pub u64);

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ToWrite {
        /// Publish the list of paths
        Publish(Vec<Path>),
        /// Stop publishing the list of paths
        Unpublish(Vec<Path>),
        /// Clear all values you've published
        Clear,
        /// Tell the resolver that we are still alive
        Heartbeat,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum FromWrite {
        Published,
        Unpublished,
        Error(Chars),
    }
}

/// The protocol between the publisher and the subscriber. Messages in
/// this protocol are structured as,
///
/// hello from the client        `[u32, publisher::Hello]`
/// hello from the server        `[u32, publisher::Hello]`
/// messages to the publisher:   `[u32, publisher::To]`
/// messages from the publisher: `[u32, publisher::From, optional Bytes]`
///
/// The `To` and `From` messages are encoded with msgpack. The initial
/// u32 is the total message length, and is encoded in network byte
/// order. The optional payload, if present, has a user specified
/// encoding, and will not be interpreted at this layer.
pub mod publisher {
    use super::resolver::ResolverId;
    use crate::path::Path;

    #[derive(
        Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
    )]
    pub struct Id(u64);

    impl Id {
        pub fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            Id(NEXT.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum Hello {
        /// No authentication will be provided. The publisher may drop
        /// the connection at this point, if it chooses to allow this
        /// then it will return Anonymous.
        Anonymous,
        /// An authentication token, if the token is valid then the
        /// publisher will send a token back to authenticate itself to
        /// the subscriber.
        Token(Vec<u8>),
        /// In order to prevent denial of service, spoofing, etc,
        /// authenticated publishers must prove that they are actually
        /// listening on the socket they claim to be listening on. To
        /// facilitate this, after a new security context has been
        /// created the resolver server will encrypt a random number
        /// with it, connect to the write address specified by the
        /// publisher, and send the encrypted token. The publisher
        /// must decrypt the token using it's end of the security
        /// context, add 1 to the number, encrypt it again and send it
        /// back. If that round trip succeeds then the new security
        /// context will replace any old one, if it fails the new
        /// context will be thrown away and the old one will continue
        /// to be associated with the write address.
        ResolverAuthenticate(ResolverId, Vec<u8>),
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum To {
        /// Subscribe to the specified value, if it is not available
        /// the result will be NoSuchValue. The optional security
        /// token is a proof from the resolver server that this
        /// subscription is permitted. In the case of an anonymous
        /// connection this proof will be empty.
        Subscribe {
            path: Path,
            resolver: ResolverId,
            token: Vec<u8>,
        },
        /// Unsubscribe from the specified value, this will always result
        /// in an Unsubscibed message even if you weren't ever subscribed
        /// to the value, or it doesn't exist.
        Unsubscribe(Id),
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum From {
        /// The requested subscription to Path cannot be completed because
        /// it doesn't exist
        NoSuchValue(Path),
        /// Permission to subscribe to the specified path is denied.
        Denied(Path),
        /// You have been unsubscriped from Path. This can be the result
        /// of an Unsubscribe message, or it may be sent unsolicited, in
        /// the case the value is no longer published, or the publisher is
        /// in the process of shutting down.
        Unsubscribed(Id),
        /// You are now subscribed to Path with subscription id `Id`, and
        /// The next message contains the first value for Id. All further
        /// communications about this subscription will only refer to the
        /// Id.
        Subscribed(Path, Id),
        /// The next message contains an updated value for Id.
        Message(Id),
        /// Indicates that the publisher is idle, but still
        /// functioning correctly.
        Heartbeat,
    }
}
