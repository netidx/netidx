use crate::{
    auth::{PMap, UserDb, UserInfo},
    channel::K5CtxWrap,
    chars::Chars,
    config,
    os::{
        local_auth::{AuthServer, Credential},
        Mapper,
    },
    utils,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use cross_krb5::{AcceptFlags, K5Ctx, ServerCtx};
use fxhash::FxBuildHasher;
use netidx_core::pack::Pack;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::task;

pub(crate) mod local {
    use super::*;
    pub(crate) struct Store {
        pmap: PMap,
        server: AuthServer,
        users: Mutex<UserDb>,
    }

    impl Store {
        pub(crate) async fn new(
            socket_path: &str,
            pmap: config::server::PMap,
            cfg: &Arc<config::server::Config>,
        ) -> Result<Self> {
            let mut users = UserDb::new(Mapper::new()?);
            let pmap = PMap::from_file(pmap, &mut users, cfg.root(), &cfg.children)?;
            Ok(Store {
                server: AuthServer::start(socket_path).await?,
                users: Mutex::new(users),
                pmap,
            })
        }

        pub(crate) fn pmap(&self) -> &PMap {
            &self.pmap
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
}

pub(crate) mod k5 {
    use super::*;
    pub(crate) struct Inner {
        ctxts: HashMap<SocketAddr, (Chars, u128, K5CtxWrap<ServerCtx>), FxBuildHasher>,
        userdb: UserDb,
    }

    impl Inner {
        pub(crate) fn get(
            &self,
            id: &SocketAddr,
        ) -> Option<&(Chars, u128, K5CtxWrap<ServerCtx>)> {
            self.ctxts.get(id).and_then(|r| {
                match task::block_in_place(|| r.2.lock().ttl()) {
                    Ok(ttl) if ttl.as_secs() > 0 => Some(r),
                    _ => None,
                }
            })
        }

        fn ifo(&mut self, user: Option<&str>) -> Result<Arc<UserInfo>> {
            self.userdb.ifo(user)
        }
    }

    pub(crate) struct Store {
        spn: Chars,
        pmap: PMap,
        pub(crate) store: RwLock<Inner>,
    }

    impl Store {
        pub(crate) fn new(
            spn: Chars,
            pmap: config::server::PMap,
            cfg: &Arc<config::server::Config>,
        ) -> Result<Self> {
            let mut userdb = UserDb::new(Mapper::new()?);
            let pmap = PMap::from_file(pmap, &mut userdb, cfg.root(), &cfg.children)?;
            let store = RwLock::new(Inner { ctxts: HashMap::default(), userdb });
            Ok(Store { spn, pmap, store })
        }

        pub(crate) fn pmap(&self) -> &PMap {
            &self.pmap
        }

        pub(crate) fn get(&self, id: &SocketAddr) -> Option<K5CtxWrap<ServerCtx>> {
            let inner = self.store.read();
            inner.get(id).map(|(_, _, c)| c.clone())
        }

        pub(crate) fn create(
            &self,
            tok: &[u8],
        ) -> Result<(K5CtxWrap<ServerCtx>, u128, Bytes)> {
            let spn = Some(&*self.spn);
            let (ctx, tok) = task::block_in_place(|| {
                ServerCtx::accept(AcceptFlags::empty(), spn, tok)
            })?;
            let secret = rand::thread_rng().gen::<u128>();
            Ok((K5CtxWrap::new(ctx), secret, utils::bytes(&*tok)))
        }

        pub(crate) fn store(
            &self,
            addr: SocketAddr,
            spn: Chars,
            secret: u128,
            ctx: K5CtxWrap<ServerCtx>,
        ) {
            let mut inner = self.store.write();
            inner.ctxts.insert(addr, (spn, secret, ctx));
        }

        pub(crate) fn remove(&self, addr: &SocketAddr) {
            let mut inner = self.store.write();
            inner.ctxts.remove(addr);
        }

        pub(crate) fn ifo(&self, user: Option<&str>) -> Result<Arc<UserInfo>> {
            let mut inner = self.store.write();
            Ok(inner.ifo(user)?)
        }
    }
}

#[derive(Clone)]
pub(crate) enum SecCtx {
    Anonymous,
    Krb5(Arc<k5::Store>),
    Local(Arc<local::Store>),
}
