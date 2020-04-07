pub mod resolver_server {
    use crate::path::Path;
    use std::{
        net::SocketAddr,
        collections::HashMap,
        path::Path as FsPath,
        convert::AsRef,
        result::Result,
    };
    use failure::Error;
    use serde_json::from_str;
    use tokio::fs::read_to_string;

    mod file {
        use super::Kind;
        use crate::path::Path;
        use std::net::SocketAddr;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) enum Auth {
            Anonymous,
            Krb5 { principal: String, permissions: String }
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Config {
            pub(super) kind: Kind,
            pub(super) max_connections: usize,
            pub(super) auth: Auth,
        }
    }

    type Permissions = String;
    type Entity = String;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PMap(pub HashMap<Path, HashMap<Option<Entity>, Permissions>>);

    #[derive(Debug, Clone)]
    pub enum Auth {
        Anonymous,
        Krb5 {principal: String, permissions: PMap }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Kind {
        Master {addr: SocketAddr},
        Replica {addr: SocketAddr, master: SocketAddr}
    }
    
    #[derive(Debug, Clone)]
    pub struct Config {
        pub kind: Kind,
        pub max_connections: usize,
        pub auth: Auth,
    }

    impl Config {
        async fn load<P: AsRef<FsPath>>(file: P) -> Result<Config, Error> {
            let cfg: file::Config = from_str(&read_to_string(file).await?)?;
            let auth = match cfg.auth {
                file::Auth::Anonymous => Auth::Anonymous,
                file::Auth::Krb5 {principal, permissions} => {
                    let permissions: PMap =
                        from_str(&read_to_string(&permissions).await?)?;
                    Auth::Krb5 {principal, permissions}
                }
            };
            Ok(Config {
                kind: cfg.kind,
                max_connections: cfg.max_connections,
                auth
            })
        }
    }
}

pub mod resolver {
    use failure::Error;
    use std::{
        net::SocketAddr,
        convert::AsRef,
        path::Path,
        result::Result
    };
    use serde_json::from_str;
    use tokio::fs::read_to_string;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Auth {
        Anonymous,
        Krb5 {principal: String}
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Config {
        pub master: SocketAddr,
        pub replicas: Vec<SocketAddr>,
        pub auth: Auth
    }

    impl Config {
        async fn load<P: AsRef<Path>>(file: P) -> Result<Config, Error> {
            Ok(from_str(&read_to_string(file).await?)?)
        }
    }
}
