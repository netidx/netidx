use crate::{resolver::UserInfo, value::Value};
use bytes::Bytes;
use netidx_core::path::Path;
use netidx_derive::Pack;
use std::net::SocketAddr;

atomic_id!(Id);

atomic_id!(WriteId);

impl Default for WriteId {
    fn default() -> Self {
        WriteId::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Pack)]
pub enum Hello {
    /// No authentication will be provided. The publisher may drop
    /// the connection at this point, if it chooses to allow this
    /// then it will return Anonymous.
    Anonymous,
    /// Authenticate using kerberos 5, following the hello, the
    /// subscriber and publisher will exchange tokens to complete the
    /// authentication.
    Krb5(#[pack(default)] Option<UserInfo>),
    /// Authenticate using a local unix socket, only valid for
    /// publishers on the same machine as the subscriber.
    Local(#[pack(default)] Option<UserInfo>),
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
    ResolverAuthenticate(SocketAddr),
    /// Authenticate using transport layer security. In this case both
    /// the server AND the client must have certificates that are
    /// signed by a CA they mutually trust.
    Tls(#[pack(default)] Option<UserInfo>),
}

#[derive(Debug, Clone, PartialEq, Pack)]
pub enum To {
    /// Subscribe to the specified value, if it is not available
    /// the result will be NoSuchValue. The optional security
    /// token is a proof from the resolver server that this
    /// subscription is permitted. In the case of an anonymous
    /// connection this proof will be empty.
    Subscribe {
        path: Path,
        resolver: SocketAddr,
        timestamp: u64,
        permissions: u32,
        token: Bytes,
    },
    /// Unsubscribe from the specified value, this will always result
    /// in an Unsubscribed message even if you weren't ever subscribed
    /// to the value, or it doesn't exist.
    Unsubscribe(Id),
    /// Send a write to the specified value.
    Write(Id, bool, Value, #[pack(default)] WriteId),
}

#[derive(Debug, Clone, PartialEq, Pack)]
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
    Subscribed(Path, Id, Value),
    /// A value update to Id
    Update(Id, Value),
    /// Indicates that the publisher is idle, but still
    /// functioning correctly.
    Heartbeat,
    /// Indicates the result of a write request
    WriteResult(Id, Value, #[pack(default)] WriteId),
}
