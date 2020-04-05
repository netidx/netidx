use crate::path::Path;
use std::{
    net::SocketAddr,
    collections::HashMap,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PMapFile(HashMap<Path, HashMap<Option<String>, String>>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Auth {
    Anonymous,
    Krb5 {principal: String, permissions: PMapFile }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resolver {
    pub addr: SocketAddr,
    pub auth: Auth,
    pub max_connections: usize,
}
