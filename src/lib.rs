//! Share values between programs using human readable names,
//! publish/subscribe semantics, and a simple JSON message format.
//!
//! json_pubsub consists of three essential parts, a hierarchical
//! namespace of values (maintained by resolver_server), a publisher,
//! to provide values, and a subscriber to consume values. Published
//! values always have a current value. Multiple subscribers may
//! subscribe to a given value at different times, and each one will
//! be immediately given the current value. When a value is updated,
//! every subscriber receives the new value. For example,
//!
//! ```
//! use resolver::Resolver;
//! use resolver_server::Server;
//! use subscriber::Subscriber;
//! use publisher::{Publisher, BindCfg};
//! use path::Path;
//! use futures::prelude::*;
//! use tokio;
//!
//! let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234);
//! let server = await!(Server::new(addr)).unwrap();
//! let resolver = await!(Resolver::new(addr)).unwrap();
//! let publisher = Publisher::new(resolver.clone(), BindCfg::Any).unwrap();
//! let subscriber0 = Subscriber::new(resolver.clone());
//! let subscriber1 = Subscriber::new(resolver.clone());
//!
//! let path = Path::from("/n");
//! let p = await!(publisher.clone().publish::<i64>(path.clone(), 4)).unwrap();
//! let s0 = await!(subscriber0.clone().subscribe::<i64>(path.clone())).unwrap();
//! let s1 = await!(subscriber1.clone().subscribe::<i64>(path)).unwrap();
//!
//! // note you can call get on a subscription as many times as you like
//! assert_eq!(s0.get().unwrap(), 4);
//! assert_eq!(s1.get().unwrap(), 4);
//!
//! // register to be notified via a future when the value is updated,
//! // do the registration before actually updating to avoid a race.
//! let s0_next = s0.next();
//! let s1_next = s1.next();
//!
//! p.update(5).unwrap();
//! await!(p.clone().flush()).unwrap();
//!
//! await!(s0_next).unwrap();
//! assert_eq!(s0.get().unwrap(), 5)
//! await!(s1_next).unwrap();
//! assert_eq!(s1.get().unwrap(), 5)
//! ```
//!
//! We can also use a subscription as an ordered lossless stream, just
//! like a tcp socket. Using the above example as a basis,
//!
//! ```
//! // the 10000 is the maximum number of incoming values to queue before pushing back
//! // on the publisher.
//! #[async]
//! for v in s0.updates(10000) {
//!     process_value(v)
//! }
//! ```
//!
//! The subscriber will get every value the publisher sends with
//! `update`, and it will get them in the order publisher called
//! `update`. As with the previous example, every subscriber that is
//! subscribed to the value will see the same stream, in the same
//! order.
//!
//! The role of the `resolver_server` is rendezvous, it stores that
//! `publisher` has a value called "/n", and tells subscribers who ask
//! for that value where to find it (in the form of a
//! `SocketAddr`). Once the subscriber has asked for "/n" and been
//! told the `SocketAddr` of the publisher that has it, the resolver
//! server is out of the picture, the subscriber makes a direct
//! connection to that publisher, requests the value, and the
//! publisher sends the value and any future updates directly to all
//! it's subscribers. Since resolver server does nothing but store a
//! database of `Path -> SocketAddr` mappings, it is quite
//! lightweight.
//!
//! The transport is JSON, facilitated by serde, so any type that
//! supports Serialize can be sent. If a subscriber doesn't know what
//! type a publisher will be publishing, it can just elect to receive
//! `serde_json::Value`, or even look at the raw JSON string.
//!
//! The primary reason to choose JSON as the transport is easy interop
//! with other languages, and especially scripting languages. Besides
//! the fact that JSON is a very widely supported format, it allows
//! the wire protocol to be line oriented (the message delimiter is
//! '\n') which greatly simplifies message handling code.
//!
//! The performance implications of JSON transport are not as bad as
//! one might imagine, because `serde_json` is capable of producing
//! gigabits per second of JSON on a single 8550U cpu core.

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate failure;

mod resolver_store;
//pub mod error;
pub mod path;
mod utils;
pub mod resolver;
pub mod resolver_server;
//pub mod publisher;
//pub mod subscriber;

//#[cfg(test)]
//mod tests;
