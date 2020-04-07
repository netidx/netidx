use crate::path::Path;
use std::{
    net::SocketAddr,
    collections::HashMap,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PMapFile(pub HashMap<Path, HashMap<Option<String>, String>>);

#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Krb5 {principal: Option<String>, permissions: PMapFile }
}

#[derive(Debug, Clone)]
pub struct Resolver {
    pub addr: SocketAddr,
    pub max_connections: usize,
    pub auth: Auth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthFile {
    Anonymous,
    Krb5 {principal: Option<String>, permissions: String}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolverFile {
    pub addr: SocketAddr,
    pub max_connections: usize,
    pub auth: AuthFile,
}
