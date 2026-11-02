use std::time::Duration;

pub mod client;
pub mod server;

#[cfg(test)]
pub mod test;

/// How long each step of a channel's handshake may take, on either side.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
