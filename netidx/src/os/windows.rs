use crate::resolver_server::config::{Config, MemberServer};
use anyhow::{bail, Result};

pub(crate) struct Mapper;

impl Mapper {
    pub(crate) fn new(_cfg: &Config, _member: &MemberServer) -> Result<Mapper> {
        Ok(Mapper)
    }

    pub(crate) fn groups(&mut self, _user: &str) -> Result<Vec<String>> {
        bail!("group listing is not implemented on windows")
    }
}

pub(crate) mod local_auth {
    use super::super::local_auth::Credential;
    use crate::resolver_server::config::{Config, MemberServer};
    use anyhow::Result;
    use bytes::Bytes;

    #[derive(Clone)]
    pub(crate) struct AuthServer;

    impl AuthServer {
        pub(crate) async fn start(
            _socket_path: &str,
            _cfg: &Config,
            _member: &MemberServer,
        ) -> Result<AuthServer> {
            bail!("local auth not implemented on windows")
        }

        pub(crate) fn validate(&self, _cred: &Credential) -> bool {
            false
        }
    }

    pub(crate) struct AuthClient;

    impl AuthClient {
        pub(crate) async fn token(_path: &str) -> Result<Bytes> {
            bail!("local auth not implemented on windows")
        }
    }
}
