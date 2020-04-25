use crate::{
    auth::{
        sysgmapper::Mapper,
        syskrb5::{ServerCtx, SYS_KRB5},
        Krb5, Krb5Ctx, PMap, UserDb, UserInfo,
    },
    config,
};
use protobuf::Chars;
use anyhow::{anyhow, Result};
use arc_swap::{ArcSwap, Guard};
use bytes::Bytes;
use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct CtxId(u64);

impl CtxId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        CtxId(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for CtxId {
    fn from(i: u64) -> Self {
        CtxId(i)
    }
}

pub(crate) struct SecStoreInner {
    read_ctxts: HashMap<CtxId, ServerCtx, FxBuildHasher>,
    write_ctxts: HashMap<SocketAddr, (Chars, ServerCtx), FxBuildHasher>,
    userdb: UserDb<Mapper>,
}

impl SecStoreInner {
    fn get_read(&self, id: &CtxId) -> Option<ServerCtx> {
        self.read_ctxts.get(id).and_then(|ctx| match ctx.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(ctx.clone()),
            _ => None,
        })
    }

    pub(crate) fn get_write(&self, id: &SocketAddr) -> Option<&(Chars, ServerCtx)> {
        self.write_ctxts.get(id).and_then(|r| match r.1.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(r),
            _ => None,
        })
    }

    fn delete_read(&mut self, id: &CtxId) {
        self.read_ctxts.remove(id);
    }

    fn delete_write(&mut self, id: &SocketAddr) {
        self.write_ctxts.remove(id);
    }

    fn gc(&mut self) -> Result<()> {
        let mut read_delete = SmallVec::<[CtxId; 64]>::new();
        let mut write_delete = SmallVec::<[SocketAddr; 64]>::new();
        for (id, ctx) in self.read_ctxts.iter() {
            if ctx.ttl()?.as_secs() == 0 {
                read_delete.push(*id);
            }
        }
        for (id, (_, ctx)) in self.write_ctxts.iter() {
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

    fn ifo(&mut self, user: Option<&str>) -> Result<Arc<UserInfo>> {
        self.userdb.ifo(user)
    }
}

#[derive(Clone)]
pub(crate) struct SecStore {
    spn: Arc<String>,
    pmap: ArcSwap<PMap>,
    pub(crate) store: Arc<RwLock<SecStoreInner>>,
}

impl SecStore {
    pub(crate) fn new(spn: String, pmap: config::resolver_server::PMap) -> Result<Self> {
        let mut userdb = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(pmap, &mut userdb)?;
        Ok(SecStore {
            spn: Arc::new(spn),
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
        inner.get_write(id).map(|(_, c)| c.clone())
    }

    pub(crate) fn delete_read(&self, id: &CtxId) {
        let mut inner = self.store.write();
        inner.delete_read(id);
    }

    pub(crate) fn delete_write(&self, id: &SocketAddr) {
        let mut inner = self.store.write();
        inner.delete_write(id);
    }

    pub(crate) fn create(&self, tok: &[u8]) -> Result<(ServerCtx, Bytes)> {
        let ctx = SYS_KRB5.create_server_ctx(Some(self.spn.as_bytes()))?;
        let tok = ctx
            .step(Some(tok))?
            .map(|b| Bytes::copy_from_slice(&*b))
            .ok_or_else(|| {
                anyhow!("step didn't generate a mutual authentication token")
            })?;
        Ok((ctx, tok))
    }

    pub(crate) fn store_read(&self, ctx: ServerCtx) -> CtxId {
        let id = CtxId::new();
        let mut inner = self.store.write();
        inner.read_ctxts.insert(id, ctx);
        id
    }

    pub(crate) fn store_write(&self, addr: SocketAddr, spn: Chars, ctx: ServerCtx) {
        let mut inner = self.store.write();
        inner.write_ctxts.insert(addr, (spn, ctx));
    }

    pub(crate) fn gc(&self) -> Result<()> {
        let mut inner = self.store.write();
        Ok(inner.gc()?)
    }

    pub(crate) fn ifo(&self, user: Option<&str>) -> Result<Arc<UserInfo>> {
        let mut inner = self.store.write();
        Ok(inner.ifo(user)?)
    }
}
