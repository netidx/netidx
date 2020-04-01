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
        *self = Id(u64::wrapping_add(self.0, 1));
        new
    }
}

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
    use super::Id;

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ClientAuth {
        Anonymous,
        Reuse(Option<Id>),
        Token(Vec<u8>),
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ServerAuth {
        Anonymous,
        Reused,
        Accepted(Vec<u8>, Option<Id>),
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ClientHello {
        /// Instruct the resolver server that this connection will not
        /// publish paths.
        ReadOnly(ClientAuth),
        /// Instruct the resolver server that this connection will
        /// only publish paths. All published paths will use the
        /// specified address `write_addr`, and the publisher must
        /// send a heartbeat at least every `ttl` seconds or the
        /// resolver server will purge all paths published by
        /// `write_addr`.
        WriteOnly { ttl: u64, write_addr: SocketAddr, auth: ClientAuth },
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ServerHello {
        ReadOnly(ServerAuth),
        WriteOnly { ttl_expired: bool, auth: ServerAuth }
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum To {
        /// Resolve the list of paths to addresses/ports
        Resolve(Vec<Path>),
        /// List the paths published under the specified root path
        List(Path),
        /// Publish the list of paths
        Publish(Vec<Path>),
        /// Stop publishing the list of paths
        Unpublish(Vec<Path>),
        /// Tell the resolver that we are still alive
        Heartbeat,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum From {
        Resolved(Vec<Vec<(SocketAddr, Vec<u8>)>>),
        List(Vec<Path>),
        Published,
        Unpublished,
        Error(String),
    }
}

/// The protocol between the publisher and the subscriber. Messages in
/// this protocol are structured as,
///
/// messages to the publisher:   `[u32, publisher::To]`
/// messages from the publisher: `[u32, publisher::From, optional Bytes]`
///
/// The `To` and `From` messages are encoded with msgpack. The initial
/// u32 is the total message length, and is encoded in network byte
/// order. The optional payload, if present, has a user specified
/// encoding, and will not be interpreted at this layer.
pub mod publisher {
    use crate::path::Path;
    use super::Id;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum Hello {
        /// No authentication will be provided. The publisher may drop
        /// the connection at this point, if it chooses to allow this
        /// then it will return Anonymous.
        Anonymous,
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
        Authenticate(Vec<u8>),
        /// An authentication token, if the token is valid then the
        /// publisher will send a token back to authenticate itself to
        /// the subscriber.
        Token(Vec<u8>),
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum To {
        /// Subscribe to the specified value, if it is not available
        /// the result will be NoSuchValue. The optional security
        /// token is a proof from the resolver server that this
        /// subscription is permitted. In the case of an anonymous
        /// connection this proof will be omitted.
        Subscribe(Path, Option<Vec<u8>>),
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
        Denied(Path)
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
