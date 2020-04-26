use crate::utils::{Chars, Pack, PackError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    collections::HashMap,
    error, fmt,
    hash::{BuildHasher, Hash},
    mem, net, result,
};

type Error = PackError;
pub type Result<T> = result::Result<T, Error>;

impl Pack for net::SocketAddr {
    fn len(&self) -> usize {
        match self {
            net::SocketAddr::V4(_) => 7,
            net::SocketAddr::V6(_) => 27,
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            net::SocketAddr::V4(v4) => {
                buf.put_u8(0);
                buf.put_u32(u32::from_be_bytes(v4.ip().octets()));
                buf.put_u16(v4.port());
            }
            net::SocketAddr::V6(v6) => {
                buf.put_u8(1);
                for s in &v6.ip().segments() {
                    buf.put_u16(*s);
                }
                buf.put_u16(v6.port());
                buf.put_u32(v6.flowinfo());
                buf.put_u32(v6.scope_id());
            }
        }
        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => {
                let ip = net::Ipv4Addr::from(u32::to_be_bytes(buf.get_u32()));
                let port = buf.get_u16();
                Ok(net::SocketAddr::V4(net::SocketAddrV4::new(ip, port)))
            }
            1 => {
                let mut segments = [0u16; 8];
                for i in 0..8 {
                    segments[i] = buf.get_u16();
                }
                let port = buf.get_u16();
                let flowinfo = buf.get_u32();
                let scope_id = buf.get_u32();
                let ip = net::Ipv6Addr::from(segments);
                let v6 = net::SocketAddrV6::new(ip, port, flowinfo, scope_id);
                Ok(net::SocketAddr::V6(v6))
            }
            _ => return Err(Error::UnknownTag),
        }
    }
}

impl Pack for Bytes {
    fn len(&self) -> usize {
        Bytes::len(self) + mem::size_of::<u32>()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u32(Bytes::len(self) as u32);
        Ok(buf.extend_from_slice(&*self))
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        let len = buf.get_u32();
        Ok(buf.split_to(len as usize).freeze())
    }
}

impl Pack for u64 {
    fn len(&self) -> usize {
        mem::size_of::<u64>()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        Ok(buf.put_u64(*self))
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        Ok(buf.get_u64())
    }
}

impl<T: Pack> Pack for Vec<T> {
    fn len(&self) -> usize {
        self.iter()
            .fold(mem::size_of::<u32>(), |len, t| len + <T as Pack>::len(t))
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u32(Vec::len(self) as u32);
        for t in self {
            <T as Pack>::encode(t, buf)?
        }
        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        let elts = buf.get_u32() as usize;
        let mut data = Vec::with_capacity(elts);
        for _ in 0..elts {
            data.push(<T as Pack>::decode(buf)?);
        }
        Ok(data)
    }
}

impl<K, V, R> Pack for HashMap<K, V, R>
where
    K: Pack + Hash + Eq,
    V: Pack + Hash + Eq,
    R: Default + BuildHasher,
{
    fn len(&self) -> usize {
        self.iter().fold(mem::size_of::<u32>(), |len, (k, v)| {
            len + <K as Pack>::len(k) + <V as Pack>::len(v)
        })
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u32(HashMap::len(self) as u32);
        for (k, v) in self {
            <K as Pack>::encode(k, buf)?;
            <V as Pack>::encode(v, buf)?;
        }
        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        let elts = buf.get_u32() as usize;
        let mut data = HashMap::with_capacity_and_hasher(elts, R::default());
        for _ in 0..elts {
            let k = <K as Pack>::decode(buf)?;
            let v = <V as Pack>::decode(buf)?;
            data.insert(k, v);
        }
        Ok(data)
    }
}

impl<T: Pack> Pack for Option<T> {
    fn len(&self) -> usize {
        1 + match self {
            None => 0,
            Some(v) => <T as Pack>::len(v),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            None => Ok(buf.put_u8(0)),
            Some(v) => {
                buf.put_u8(1);
                <T as Pack>::encode(v, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(None),
            1 => Ok(Some(<T as Pack>::decode(buf)?)),
            _ => return Err(Error::UnknownTag),
        }
    }
}

pub mod resolver {
    use super::*;
    use crate::{path::Path, utils::{Chars, Pack}};
    use bytes::Bytes;
    use fxhash::FxBuildHasher;
    use std::{collections::HashMap, net::SocketAddr};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct CtxId(u64);

    impl CtxId {
        pub fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            CtxId(NEXT.fetch_add(1, Ordering::Relaxed))
        }
    }

    impl Pack for CtxId {
        fn len(&self) -> usize {
            <u64 as Pack>::len(&self.0)
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            <u64 as Pack>::encode(&self.0, buf)
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            Ok(CtxId(<u64 as Pack>::decode(buf)?))
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ResolverId(u64);

    impl Pack for ResolverId {
        fn len(&self) -> usize {
            <u64 as Pack>::len(&self.0)
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            self.0.encode(buf)
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            Ok(ResolverId(u64::decode(buf)?))
        }
    }

    #[derive(Clone, Debug)]
    pub enum ClientAuthRead {
        Anonymous,
        Reuse(CtxId),
        Initiate(Bytes),
    }

    impl Pack for ClientAuthRead {
        fn len(&self) -> usize {
            1 + match self {
                ClientAuthRead::Anonymous => 0,
                ClientAuthRead::Reuse(ref i) => Pack::len(i),
                ClientAuthRead::Initiate(ref b) => Pack::len(b),
            }
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            match self {
                ClientAuthRead::Anonymous => Ok(buf.put_u8(0)),
                ClientAuthRead::Reuse(ref id) => {
                    buf.put_u8(1);
                    Ok(<CtxId as Pack>::encode(id, buf)?)
                }
                ClientAuthRead::Initiate(ref tok) => {
                    buf.put_u8(2);
                    Ok(<Bytes as Pack>::encode(tok, buf)?)
                }
            }
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            match buf.get_u8() {
                0 => Ok(ClientAuthRead::Anonymous),
                1 => Ok(ClientAuthRead::Reuse(<CtxId as Pack>::decode(buf)?)),
                2 => Ok(ClientAuthRead::Initiate(<Bytes as Pack>::decode(buf)?)),
                _ => return Err(Error::UnknownTag),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub enum ClientAuthWrite {
        Anonymous,
        Reuse,
        Initiate { spn: Option<Chars>, token: Bytes },
    }

    impl Pack for ClientAuthWrite {
        fn len(&self) -> usize {
            1 + match self {
                ClientAuthWrite::Anonymous => 0,
                ClientAuthWrite::Reuse => 0,
                ClientAuthWrite::Initiate { spn, token } => {
                    <Option<Chars> as Pack>::len(spn) + <Bytes as Pack>::len(token)
                }
            }
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            match self {
                ClientAuthWrite::Anonymous => Ok(buf.put_u8(0)),
                ClientAuthWrite::Reuse => Ok(buf.put_u8(1)),
                ClientAuthWrite::Initiate { spn, token } => {
                    buf.put_u8(2);
                    <Option<Chars> as Pack>::encode(spn, buf)?;
                    Ok(<Bytes as Pack>::encode(token, buf)?)
                }
            }
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            match buf.get_u8() {
                0 => Ok(ClientAuthWrite::Anonymous),
                1 => Ok(ClientAuthWrite::Reuse),
                2 => {
                    let spn = <Option<Chars> as Pack>::decode(buf)?;
                    let token = <Bytes as Pack>::decode(buf)?;
                    Ok(ClientAuthWrite::Initiate { spn, token })
                }
                _ => Err(Error::UnknownTag),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct ClientHelloWrite {
        pub write_addr: SocketAddr,
        pub auth: ClientAuthWrite,
    }

    impl Pack for ClientHelloWrite {
        fn len(&self) -> usize {
            <SocketAddr as Pack>::len(&self.write_addr) + ClientAuthWrite::len(&self.auth)
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            <SocketAddr as Pack>::encode(&self.write_addr, buf)?;
            Ok(ClientAuthWrite::encode(&self.auth, buf)?)
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            let write_addr = <SocketAddr as Pack>::decode(buf)?;
            let auth = ClientAuthWrite::decode(buf)?;
            Ok(ClientHelloWrite { write_addr, auth })
        }
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

    impl Pack for ClientHello {
        fn len(&self) -> usize {
            1 + match self {
                ClientHello::ReadOnly(r) => ClientAuthRead::len(r),
                ClientHello::WriteOnly(r) => ClientHelloWrite::len(r),
            }
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            match self {
                ClientHello::ReadOnly(r) => {
                    buf.put_u8(0);
                    Ok(<ClientAuthRead as Pack>::encode(r, buf)?)
                }
                ClientHello::WriteOnly(r) => {
                    buf.put_u8(1);
                    Ok(<ClientHelloWrite as Pack>::encode(r, buf)?)
                }
            }
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            match buf.get_u8() {
                0 => Ok(ClientHello::ReadOnly(ClientAuthRead::decode(buf)?)),
                1 => Ok(ClientHello::WriteOnly(ClientHelloWrite::decode(buf)?)),
                _ => Err(Error::UnknownTag),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub enum ServerHelloRead {
        Anonymous,
        Reused,
        Accepted(Bytes, CtxId),
    }

    impl Pack for ServerHelloRead {
        fn len(&self) -> usize {
            1 + match self {
                ServerHelloRead::Anonymous => 0,
                ServerHelloRead::Reused => 0,
                ServerHelloRead::Accepted(tok, id) => {
                    <Bytes as Pack>::len(tok) + CtxId::len(id)
                }
            }
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            match self {
                ServerHelloRead::Anonymous => Ok(buf.put_u8(0)),
                ServerHelloRead::Reused => Ok(buf.put_u8(1)),
                ServerHelloRead::Accepted(tok, id) => {
                    buf.put_u8(2);
                    <Bytes as Pack>::encode(tok, buf)?;
                    Ok(CtxId::encode(id, buf)?)
                }
            }
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            match buf.get_u8() {
                0 => Ok(ServerHelloRead::Anonymous),
                1 => Ok(ServerHelloRead::Reused),
                2 => {
                    let tok = <Bytes as Pack>::decode(buf)?;
                    let id = CtxId::decode(buf)?;
                    Ok(ServerHelloRead::Accepted(tok, id))
                }
                _ => Err(Error::UnknownTag),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub enum ServerAuthWrite {
        Anonymous,
        Reused,
        Accepted(Bytes),
    }

    impl Pack for ServerAuthWrite {
        fn len(&self) -> usize {
            1 + match self {
                ServerAuthWrite::Anonymous => 0,
                ServerAuthWrite::Reused => 0,
                ServerAuthWrite::Accepted(b) => <Bytes as Pack>::len(b),
            }
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            match self {
                ServerAuthWrite::Anonymous => Ok(buf.put_u8(0)),
                ServerAuthWrite::Reused => Ok(buf.put_u8(1)),
                ServerAuthWrite::Accepted(b) => {
                    buf.put_u8(2);
                    Ok(<Bytes as Pack>::encode(b, buf)?)
                }
            }
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            match buf.get_u8() {
                0 => Ok(ServerAuthWrite::Anonymous),
                1 => Ok(ServerAuthWrite::Reused),
                2 => {
                    let tok = <Bytes as Pack>::decode(buf)?;
                    Ok(ServerAuthWrite::Accepted(tok))
                }
                _ => Err(Error::UnknownTag),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct ServerHelloWrite {
        pub ttl_expired: bool,
        pub resolver_id: ResolverId,
        pub auth: ServerAuthWrite,
    }

    impl Pack for ServerHelloWrite {
        fn len(&self) -> usize {
            1 + ResolverId::len(&self.resolver_id) + ServerAuthWrite::len(&self.auth)
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            buf.put_u8(if self.ttl_expired { 1 } else { 0 });
            ResolverId::encode(&self.resolver_id, buf)?;
            Ok(ServerAuthWrite::encode(&self.auth, buf)?)
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            let ttl_expired = match buf.get_u8() {
                0 => false,
                1 => true,
                _ => return Err(Error::UnknownTag),
            };
            let resolver_id = ResolverId::decode(buf)?;
            let auth = ServerAuthWrite::decode(buf)?;
            Ok(ServerHelloWrite {
                ttl_expired,
                resolver_id,
                auth,
            })
        }
    }

    #[derive(Clone, Debug)]
    pub enum ToRead {
        /// Resolve the list of paths to addresses/ports
        Resolve(Vec<Path>),
        /// List the paths published under the specified root path
        List(Path),
    }

    impl Pack for ToRead {
        fn len(&self) -> usize {
            1 + match self {
                ToRead::Resolve(paths) => <Vec<Path> as Pack>::len(paths),
                ToRead::List(path) => <Path as Pack>::len(path),
            }
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<()> {
            todo!()
        }

        fn decode(buf: &mut BytesMut) -> Result<Self> {
            todo!()
        }
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

    #[derive(Clone, Debug)]
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

    #[derive(Clone, Debug)]
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
    use super::*;
    use crate::path::Path;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Id(u64);

    impl Id {
        pub fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            Id(NEXT.fetch_add(1, Ordering::Relaxed))
        }
    }

    #[derive(Debug, Clone)]
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

    #[derive(Debug, Clone)]
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

    #[derive(Debug, Clone)]
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
