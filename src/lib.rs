//! Share values between programs using human readable names,
//! publish/subscribe semantics, and a simple msgpack message format.
//!
//! json_pubsub consists of three essential parts, a hierarchical
//! namespace of values maintained in the resolver server, a
//! publisher, and a subscriber. Published values always have a
//! current value. Multiple subscribers may subscribe to a given value
//! at different times, and each one will be immediately given the
//! current value. When a value is updated, every subscriber receives
//! the new value. For example,
//!
//!
//! We can also use a subscription as an ordered lossless stream, just
//! like a tcp socket. Using the above example as a basis,
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
//! The default transport is messagepack, facilitated by serde, so any
//! type that supports Serialize can be sent. If a subscriber doesn't
//! know what type a publisher will be publishing, it can just elect
//! to receive `rmpv::Value`, or even just the raw bytes.
#![recursion_limit="1024"]
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate pin_utils;
#[macro_use] extern crate bitflags;

#[macro_use] pub mod utils;
mod os;
pub mod chars;
mod resolver_store;
mod auth;
mod secstore;
pub mod path;
pub mod protocol;
pub mod config;
pub mod channel;
pub mod resolver;
pub mod resolver_server;
pub mod publisher;
pub mod subscriber;
#[cfg(test)] mod test;
