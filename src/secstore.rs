use crate::{
    config::PMapFile,
    auth::{
        sysgmapper::Mapper,
        syskrb5::{sys_krb5, ServerCtx},
        Krb5, Krb5Ctx, PMap, UserDb, UserInfo,
    },
    protocol::resolver::CtxId,
};
use fxhash::FxBuildHasher;
use smallvec::SmallVec;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use failure::Error;
use parking_lot::RwLock;
use arc_swap::{ArcSwap, Guard};

pub(crate) struct SecStoreInner {
    read_ctxts: HashMap<CtxId, ServerCtx, FxBuildHasher>,
    write_ctxts: HashMap<SocketAddr, ServerCtx, FxBuildHasher>,
    userdb: UserDb<Mapper>,
}

impl SecStoreInner {
    fn get_read(&self, id: &CtxId) -> Option<ServerCtx> {
        self.read_ctxts.get(id).and_then(|ctx| match ctx.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(ctx.clone()),
            _ => None,
        })
    }

    fn get_write(&self, id: &SocketAddr) -> Option<ServerCtx> {
        self.write_ctxts.get(id).and_then(|ctx| match ctx.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(ctx.clone()),
            _ => None,
        })
    }

    pub(crate) fn get_write_ref(&self, id: &SocketAddr) -> Option<&ServerCtx> {
        self.write_ctxts.get(id)
    }

    fn delete_read(&mut self, id: &CtxId) {
        self.read_ctxts.remove(id);
    }

    fn delete_write(&mut self, id: &SocketAddr) {
        self.write_ctxts.remove(id);
    }

    fn gc(&mut self) -> Result<(), Error> {
        let mut read_delete = SmallVec::<[CtxId; 64]>::new();
        let mut write_delete = SmallVec::<[SocketAddr; 64]>::new();
        for (id, ctx) in self.read_ctxts.iter() {
            if ctx.ttl()?.as_secs() == 0 {
                read_delete.push(*id);
            }
        }
        for (id, ctx) in self.write_ctxts.iter() {
            if ctx.ttl()?.as_secs() == 0 {
                write_delete.push(*id);
            }
        }
        for id in read_delete.into_iter() {
            self.read_ctxts.remove(&id);
        }
        for id in write_delete.into_iter() {
            self.write_ctxts.remove(&id);
        }
        Ok(())
    }

    fn ifo(&mut self, user: Option<&str>) -> Result<Arc<UserInfo>, Error> {
        self.userdb.ifo(user)
    }
}

#[derive(Clone)]
pub(crate) struct SecStore {
    principal: Arc<String>,
    pmap: ArcSwap<PMap>,
    pub(crate) store: Arc<RwLock<SecStoreInner>>,
}

impl SecStore {
    pub(crate) fn new(principal: String, pmap: PMapFile) -> Result<Self, Error> {
        let mut userdb = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(pmap, &mut userdb)?;
        Ok(SecStore {
            principal: Arc::new(principal),
            pmap: ArcSwap::from(Arc::new(pmap)),
            store: Arc::new(RwLock::new(SecStoreInner {
                read_ctxts: HashMap::with_hasher(FxBuildHasher::default()),
                write_ctxts: HashMap::with_hasher(FxBuildHasher::default()),
                userdb,
            })),
        })
    }

    pub(crate) fn pmap(&self) -> Guard<'static, Arc<PMap>> {
        self.pmap.load()
    }

    pub(crate) fn update_pmap(&self, pmap: PMap) {
        self.pmap.swap(Arc::new(pmap));
    }

    pub(crate) fn get_read(&self, id: &CtxId) -> Option<ServerCtx> {
        let inner = self.store.read();
        inner.get_read(id)
    }

    pub(crate) fn get_write(&self, id: &SocketAddr) -> Option<ServerCtx> {
        let inner = self.store.read();
        inner.get_write(id)
    }

    pub(crate) fn delete_read(&self, id: &CtxId) {
        let mut inner = self.store.write();
        inner.delete_read(id);
    }

    pub(crate) fn delete_write(&self, id: &SocketAddr) {
        let mut inner = self.store.write();
        inner.delete_write(id);
    }

    pub(crate) fn create(&self, tok: &[u8]) -> Result<(ServerCtx, Vec<u8>), Error> {
        let ctx = sys_krb5.create_server_ctx(self.principal.as_bytes())?;
        let tok = ctx
            .step(Some(tok))?
            .map(|b| Vec::from(&*b))
            .ok_or_else(|| {
                format_err!("step didn't generate a mutual authentication token")
            })?;
        Ok((ctx, tok))
    }

    pub(crate) fn store_read(&self, ctx: ServerCtx) -> CtxId {
        let id = CtxId::new();
        let mut inner = self.store.write();
        inner.read_ctxts.insert(id, ctx);
        id
    }

    pub(crate) fn store_write(&self, addr: SocketAddr, ctx: ServerCtx) {
        let mut inner = self.store.write();
        inner.write_ctxts.insert(addr, ctx);
    }

    pub(crate) fn gc(&self) -> Result<(), Error> {
        let mut inner = self.store.write();
        Ok(inner.gc()?)
    }

    pub(crate) fn ifo(&self, user: Option<&str>) -> Result<Arc<UserInfo>, Error> {
        let mut inner = self.store.write();
        Ok(inner.ifo(user)?)
    }
}
