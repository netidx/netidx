use std::{
    net::SocketAddr,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Resolver {
    pub addr: SocketAddr,
}
