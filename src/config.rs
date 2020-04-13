pub mod resolver_server {
    use crate::{path::Path, protocol::resolver::ResolverId};
    use failure::Error;
    use serde_json::from_str;
    use std::{
        collections::HashMap, convert::AsRef, net::SocketAddr, path::Path as FsPath,
        result::Result,
    };
    use tokio::fs::read_to_string;

    mod file {
        use crate::protocol::resolver::ResolverId;
        use std::net::SocketAddr;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) enum Auth {
            Anonymous,
            Krb5 {
                principal: String,
                permissions: String,
            },
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Config {
            pub(super) id: ResolverId,
            pub(super) addr: SocketAddr,
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
        Krb5 {
            principal: String,
            permissions: PMap,
        },
    }

    #[derive(Debug, Clone)]
    pub struct Config {
        pub id: ResolverId,
        pub addr: SocketAddr,
        pub max_connections: usize,
        pub auth: Auth,
    }

    impl Config {
        async fn load<P: AsRef<FsPath>>(file: P) -> Result<Config, Error> {
            let cfg: file::Config = from_str(&read_to_string(file).await?)?;
            let auth = match cfg.auth {
                file::Auth::Anonymous => Auth::Anonymous,
                file::Auth::Krb5 {
                    principal,
                    permissions,
                } => {
                    let permissions: PMap =
                        from_str(&read_to_string(&permissions).await?)?;
                    Auth::Krb5 {
                        principal,
                        permissions,
                    }
                }
            };
            Ok(Config {
                id: cfg.id,
                addr: cfg.addr,
                max_connections: cfg.max_connections,
                auth,
            })
        }
    }
}

pub mod resolver {
    use failure::Error;
    use serde_json::from_str;
    use std::{
        convert::AsRef, net::SocketAddr, path::Path, result::Result,
    };
    use tokio::fs::read_to_string;
    use crate::protocol::resolver::ResolverId;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Auth {
        Anonymous,
        Krb5 { principal: String },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Config {
        pub servers: Vec<(ResolverId, SocketAddr)>,
        pub auth: Auth,
    }

    impl Config {
        async fn load<P: AsRef<Path>>(file: P) -> Result<Config, Error> {
            Ok(from_str(&read_to_string(file).await?)?)
        }
    }
}
