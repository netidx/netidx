use crate::{
    auth::{PMap, UserDb, UserInfo},
    config,
    os::{
        local_auth::{AuthServer, Credential},
        Mapper,
    },
};
use anyhow::{bail, Result};
use netidx_core::pack::Pack;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct LocalSecStore {
    pmap: Arc<PMap>,
    server: AuthServer,
    users: Arc<Mutex<UserDb>>,
}

impl LocalSecStore {
    pub(crate) async fn new(
        socket_path: &str,
        pmap: config::server::PMap,
        cfg: &Arc<config::server::Config>,
    ) -> Result<Self> {
        let mut users = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(pmap, &mut users, cfg.root(), &cfg.children)?;
        Ok(LocalSecStore {
            server: AuthServer::start(socket_path).await?,
            users: Arc::new(Mutex::new(users)),
            pmap: Arc::new(pmap),
        })
    }

    pub(crate) fn validate(&self, mut token: &[u8]) -> Result<Arc<UserInfo>> {
        if token.len() < 10 {
            bail!("token short")
        }
        let cred = <Credential as Pack>::decode(&mut token)?;
        if !self.server.validate(&cred) {
            bail!("invalid token")
        }
        Ok(self.users.lock().ifo(Some(&*cred.user))?)
    }
}
