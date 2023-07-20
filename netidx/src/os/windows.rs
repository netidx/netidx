use crate::resolver_server::config::{Config, IdMap, MemberServer};
use anyhow::bail;
use anyhow::{anyhow, Result};
use arcstr::ArcStr;
use tokio::process::Command;

// Unix group membership is a little complex, it can come from a
// lot of places, and it's not entirely standardized at the api
// level, it seems libc provides getgrouplist on most platforms,
// but unfortunatly Apple doesn't implement it. Luckily the 'id'
// command is specified in POSIX.
#[derive(Clone)]
pub(crate) enum Mapper {
    DoNotMap,
    Command(ArcStr),
}

impl Mapper {
    pub(crate) async fn new(_cfg: &Config, member: &MemberServer) -> Result<Mapper> {
        match &member.id_map {
            IdMap::DoNotMap => Ok(Mapper::DoNotMap),
            IdMap::Command(cmd) => Ok(Mapper::Command(ArcStr::from(cmd))),
            IdMap::Socket(_) => bail!("id-map sockets are not supported on windows"),
            IdMap::PlatformDefault => Ok(Mapper::DoNotMap),
        }
    }

    pub(crate) async fn groups(&self, user: &str) -> Result<(ArcStr, Vec<ArcStr>)> {
        let parse = |s: &str| {
            let mut primary = Mapper::parse_output(&s, "gid=")?;
            let groups = Mapper::parse_output(&s, "groups=")?;
            let primary = if primary.is_empty() {
                bail!("missing primary group")
            } else {
                primary.swap_remove(0)
            };
            Ok((primary, groups))
        };
        match &self {
            Mapper::DoNotMap => Ok((user.into(), vec![])),
            Mapper::Command(cmd) => {
                let out = Command::new(&**cmd).arg(user).output().await?;
                parse(String::from_utf8_lossy(&out.stdout).as_ref())
            },
        }
    }

    fn parse_output(out: &str, key: &str) -> Result<Vec<ArcStr>> {
        let mut groups = Vec::new();
        match out.find(key) {
            None => Ok(Vec::new()),
            Some(i) => {
                let mut s = &out[i..];
                while let Some(i_op) = s.find('(') {
                    match s.find(')') {
                        None => {
                            return Err(anyhow!(
                                "invalid id command output, expected ')'"
                            ))
                        }
                        Some(i_cp) => {
                            groups.push(ArcStr::from(&s[i_op + 1..i_cp]));
                            s = &s[i_cp + 1..];
                        }
                    }
                }
                Ok(groups)
            }
        }
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
