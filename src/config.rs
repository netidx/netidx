pub mod resolver_server {
    use crate::protocol::resolver::v1::ResolverId;
    use anyhow::Result;
    use serde_json::from_str;
    use std::{
        collections::HashMap, convert::AsRef, fs::read_to_string, net::SocketAddr,
        path::Path as FsPath, time::Duration,
    };

    mod file {
        use super::ResolverId;
        use std::net::SocketAddr;

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) enum Auth {
            Anonymous,
            Krb5 { spn: String, permissions: String },
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub(super) struct Config {
            pub(super) pid_file: String,
            pub(super) id: ResolverId,
            pub(super) addr: SocketAddr,
            pub(super) max_connections: usize,
            pub(super) reader_ttl: u64,
            pub(super) writer_ttl: u64,
            pub(super) hello_timeout: u64,
            pub(super) auth: Auth,
        }
    }

    type Permissions = String;
    type Entity = String;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct PMap(pub HashMap<String, HashMap<Entity, Permissions>>);

    #[derive(Debug, Clone)]
    pub enum Auth {
        Anonymous,
        Krb5 { spn: String, permissions: PMap },
    }

    #[derive(Debug, Clone)]
    pub struct Config {
        pub pid_file: String,
        pub id: ResolverId,
        pub addr: SocketAddr,
        pub max_connections: usize,
        pub reader_ttl: Duration,
        pub writer_ttl: Duration,
        pub hello_timeout: Duration,
        pub auth: Auth,
    }

    impl Config {
        pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
            let cfg: file::Config = from_str(&read_to_string(file)?)?;
            let auth = match cfg.auth {
                file::Auth::Anonymous => Auth::Anonymous,
                file::Auth::Krb5 { spn, permissions } => {
                    let permissions: PMap = from_str(&read_to_string(&permissions)?)?;
                    Auth::Krb5 { spn, permissions }
                }
            };
            Ok(Config {
                pid_file: cfg.pid_file,
                id: cfg.id,
                addr: cfg.addr,
                max_connections: cfg.max_connections,
                reader_ttl: Duration::from_secs(cfg.reader_ttl),
                writer_ttl: Duration::from_secs(cfg.writer_ttl),
                hello_timeout: Duration::from_secs(cfg.hello_timeout),
                auth,
            })
        }
    }
}

pub mod resolver {
    use crate::protocol::resolver::v1::ResolverId;
    use anyhow::Result;
    use serde_json::from_str;
    use std::{collections::HashMap, convert::AsRef, net::SocketAddr, path::Path};
    use tokio::fs::read_to_string;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Auth {
        Anonymous,
        Krb5 { target_spns: HashMap<ResolverId, String> },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Config {
        pub servers: Vec<(ResolverId, SocketAddr)>,
        pub auth: Auth,
    }

    impl Config {
        pub async fn load<P: AsRef<Path>>(file: P) -> Result<Config> {
            Ok(from_str(&read_to_string(file).await?)?)
        }
    }
}
