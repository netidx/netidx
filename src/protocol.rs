/// Messages to and from the resolver server consist of the following basic structure,
///
/// hello from the client: `[u32, resolver::ClientHello]`
/// hello from the server: `[u32, resolver::ServerHello]`
/// to the server:         `[u32, resolver::To]`
/// from the server:       `[u32, resolver::From]`
///
/// The `To`, `From`, `ClientHello`, and `ServerHello` components are
/// serialized with msgpack. The initial u32 is the total message
/// length, is serialized directly in network byte order, and is added
/// automatically by the channel library.
///
/// Upon estabilishing a connection to the resolver server the client
/// must send a `ClientHello` message, the server will respond with a
/// `ServerHello` message. Once this handshake process is complete the
/// client may send `To` messages. For each `To` message the server
/// will respond with a single `From` message in the order of receipt
/// of the `To` message. The client may have as many `To` messages "in
/// flight" as desired, the server will always respond to them in
/// order.
pub mod resolver {
    use crate::path::Path;
    use std::net::SocketAddr;

    pub enum Authentication<'a> {
        Anonymous,
        Token(&'a [u8]),
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ClientHello<'a> {
        /// Instruct the resolver server that this connection will not
        /// publish paths.
        ReadOnly(Authentication<'a>),
        /// Instruct the resolver server that this connection will
        /// only publish paths. All published paths will use the
        /// specified address `write_addr`, and the publisher must
        /// send a heartbeat at least every `ttl` seconds or the
        /// resolver server will purge all paths published by
        /// `write_addr`.
        WriteOnly { ttl: u64, write_addr: SocketAddr, auth: Authentication<'a> },
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct ServerHello<'a> {
        /// If `ttl_expired` is true, the resolver has previously
        /// purged everything published by this publisher, if desired
        /// it should be republished.
        pub ttl_expired: bool,
        pub auth: Authentication<'a>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum To<'a> {
        /// Resolve the list of paths to addresses/ports
        Resolve(&'a [&'a str]),
        /// List the paths published under the specified root path
        List(&'a str),
        /// Publish the list of paths
        Publish(&'a [&'a str]),
        /// Stop publishing the list of paths
        Unpublish(&'a [&'a str]),
        /// Clear all paths published by our ip/port
        Clear,
        /// Tell the resolver that we are still alive
        Heartbeat,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum From<'a> {
        Resolved(&'a [&'a [SocketAddr]]),
        List(&'a [&'a str]),
        Published,
        Unpublished,
        Error(&'a str),
    }
}

/// The protocol between the publisher and the subscriber. Messages in
/// this protocol are structured as,
///
/// client hello to publisher:   `[u32, resolver::Authentication]`
/// publisher hello to client:   `[u32, resolver::Authentication]`
/// messages to the publisher:   `[u32, publisher::To]`
/// messages from the publisher: `[u32, publisher::From, optional Bytes]`
///
/// The `To` and `From` messages are encoded with msgpack. The initial
/// u32 is the total message length, and is encoded in network byte
/// order. The optional payload, if present, has a user specified
/// encoding, and will not be interpreted at this layer.
pub mod publisher {
    use crate::path::Path;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum To<'a> {
        /// Subscribe to the specified value, if it is not available the
        /// result will be NoSuchValue
        Subscribe(&'a str),
        /// Unsubscribe from the specified value, this will always result
        /// in an Unsubscibed message even if you weren't ever subscribed
        /// to the value, or it doesn't exist.
        Unsubscribe(Id),
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum From<'a> {
        /// The requested subscription to Path cannot be completed because
        /// it doesn't exist
        NoSuchValue(&'a str),
        /// You have been unsubscriped from Path. This can be the result
        /// of an Unsubscribe message, or it may be sent unsolicited, in
        /// the case the value is no longer published, or the publisher is
        /// in the process of shutting down.
        Unsubscribed(Id),
        /// You are now subscribed to Path with subscription id `Id`, and
        /// The next message contains the first value for Id. All further
        /// communications about this subscription will only refer to the
        /// Id.
        Subscribed(&'a str, Id),
        /// The next message contains an updated value for Id.
        Message(Id),
        /// Indicates that the publisher is idle, but still
        /// functioning correctly.
        Heartbeat,
    }

    #[derive(
        Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
    )]
    pub struct Id(u64);

    impl Id {
        pub fn zero() -> Self {
            Id(0)
        }

        pub fn take(&mut self) -> Self {
            let new = *self;
            *self = Id(self.0 + 1);
            new
        }
    }
}
