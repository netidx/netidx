use anyhow::{bail, Result};

pub(crate) struct Mapper;

impl apper {
    pub(crate) fn new() -> Result<Mapper> {
        Ok(Mapper)
    }

    pub(crate) fn groups(&mut self, _user: &str) -> Result<Vec<String>> {
        bail!("group listing is not implemented on windows")
    }


    pub(crate) fn user(&self, user: u32) -> Result<String> {
        bail!("user listing is not implemented on windows")
    }
}

pub(crate) mod local_auth {
    use super::super::local_auth::Credential;
    use anyhow::Result;
    use bytes::Bytes;

    pub(crate) struct AuthServer;

    impl AuthServer {
        pub(crate) async fn start(_socket_path: &str) -> Result<AuthServer> {
            bail!("local auth not implemented on windows")
        }

        pub(crate) fn validate(&self, _cred: &Credential) -> bool {
            false
        }
    }

    pub(crate) struct AuthClient;

    impl AuthClient {
        pub(crate) fn new(_path: String) -> Result<Bytes> {
            bail!("local auth not implemented on windows")
        }

        pub(crate) async fn token(&self) -> Result<Bytes> {
            bail!("local auth not implemented on windows")
        }
    }
}
