pub mod resolver {
    use crate::path::Path;
    use std::net::SocketAddr;

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ClientHello {
        ReadOnly,
        WriteOnly { ttl: u64, write_addr: SocketAddr },
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct ServerHello {
        pub ttl_expired: bool,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum ToResolver {
        Resolve(Vec<Path>),
        List(Path),
        Publish(Vec<Path>),
        Unpublish(Vec<Path>),
        Clear,
        Heartbeat,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum FromResolver {
        Resolved(Vec<Vec<SocketAddr>>),
        List(Vec<Path>),
        Published,
        Unpublished,
        Error(String),
    }
}

pub mod publisher {
    use crate::path::Path;

    /// This is the set of protocol messages that may be sent to the publisher
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum ToPublisher {
        /// Subscribe to the specified value, if it is not available the
        /// result will be NoSuchValue
        Subscribe(Path),
        /// Unsubscribe from the specified value, this will always result
        /// in an Unsubscibed message even if you weren't ever subscribed
        /// to the value, or it doesn't exist.
        Unsubscribe(Id),
    }

    /// This is the set of protocol messages that may come from the publisher
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum FromPublisher {
        /// The requested subscription to Path cannot be completed because
        /// it doesn't exist
        NoSuchValue(Path),
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

