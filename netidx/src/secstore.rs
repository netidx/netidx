use crate::{
    auth::{PMap, UserDb, UserInfo},
    chars::Chars,
    config,
    os::{self, Krb5Ctx, Mapper, ServerCtx},
};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use rand::Rng;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

pub(crate) struct SecStoreInner {
    ctxts: HashMap<SocketAddr, (Chars, u128, ServerCtx), FxBuildHasher>,
    userdb: UserDb,
}

impl SecStoreInner {
    pub(crate) fn get(&self, id: &SocketAddr) -> Option<&(Chars, u128, ServerCtx)> {
        self.ctxts.get(id).and_then(|r| match r.2.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(r),
            _ => None,
        })
    }

    fn ifo(&mut self, user: Option<&str>) -> Result<Arc<UserInfo>> {
        self.userdb.ifo(user)
    }
}

#[derive(Clone)]
pub(crate) struct SecStore {
    spn: Arc<String>,
    pmap: Arc<PMap>,
    pub(crate) store: Arc<RwLock<SecStoreInner>>,
}

impl SecStore {
    pub(crate) fn new(
        spn: String,
        pmap: config::PMap,
        cfg: &Arc<config::Config>,
    ) -> Result<Self> {
        let mut userdb = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(pmap, &mut userdb, cfg.root(), &cfg.children)?;
        Ok(SecStore {
            spn: Arc::new(spn),
            pmap: Arc::new(pmap),
            store: Arc::new(RwLock::new(SecStoreInner {
                ctxts: HashMap::with_hasher(FxBuildHasher::default()),
                userdb,
            })),
        })
    }

    pub(crate) fn pmap(&self) -> &PMap {
        &*self.pmap
    }

    pub(crate) fn get(&self, id: &SocketAddr) -> Option<ServerCtx> {
        let inner = self.store.read();
        inner.get(id).map(|(_, _, c)| c.clone())
    }

    pub(crate) fn create(&self, tok: &[u8]) -> Result<(ServerCtx, u128, Bytes)> {
        let ctx = os::create_server_ctx(Some(self.spn.as_str()))?;
        let secret = rand::thread_rng().gen::<u128>();
        let tok = ctx.step(Some(tok))?.map(|b| Bytes::copy_from_slice(&*b)).ok_or_else(
            || anyhow!("step didn't generate a mutual authentication token"),
        )?;
        Ok((ctx, secret, tok))
    }

    pub(crate) fn store(
        &self,
        addr: SocketAddr,
        spn: Chars,
        secret: u128,
        ctx: ServerCtx,
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
