use crate::{
    auth::{PMap, UserDb, UserInfo},
    channel::K5CtxWrap,
    chars::Chars,
    config,
    os::Mapper,
    utils,
};
use anyhow::Result;
use bytes::Bytes;
use cross_krb5::{AcceptFlags, K5Ctx, ServerCtx};
use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use rand::Rng;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::task;

pub(crate) struct K5SecStoreInner {
    ctxts: HashMap<SocketAddr, (Chars, u128, K5CtxWrap<ServerCtx>), FxBuildHasher>,
    userdb: UserDb,
}

impl K5SecStoreInner {
    pub(crate) fn get(
        &self,
        id: &SocketAddr,
    ) -> Option<&(Chars, u128, K5CtxWrap<ServerCtx>)> {
        self.ctxts.get(id).and_then(|r| match task::block_in_place(|| r.2.lock().ttl()) {
            Ok(ttl) if ttl.as_secs() > 0 => Some(r),
            _ => None,
        })
    }

    fn ifo(&mut self, user: Option<&str>) -> Result<Arc<UserInfo>> {
        self.userdb.ifo(user)
    }
}

pub(crate) struct K5SecStore {
    spn: Chars,
    pmap: PMap,
    pub(crate) store: RwLock<K5SecStoreInner>,
}

impl K5SecStore {
    pub(crate) fn new(
        spn: Chars,
        pmap: config::server::PMap,
        cfg: &Arc<config::server::Config>,
    ) -> Result<Self> {
        let mut userdb = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(pmap, &mut userdb, cfg.root(), &cfg.children)?;
        let store = RwLock::new(K5SecStoreInner { ctxts: HashMap::default(), userdb });
        Ok(K5SecStore { spn, pmap, store })
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
        let (ctx, tok) =
            task::block_in_place(|| ServerCtx::accept(AcceptFlags::empty(), spn, tok))?;
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
