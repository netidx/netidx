use std::{
    net::SocketAddr,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Auth {
    Anonymous,
    Krb5 {principal: String}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resolver {
    pub addr: SocketAddr,
    pub auth: Auth,
    pub max_connections: usize,
}
