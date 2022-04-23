use crate::{
    auth::{PMap, UserDb},
    channel::K5CtxWrap,
    chars::Chars,
    config,
    os::{
        local_auth::{AuthServer, Credential},
        Mapper,
    },
    protocol::resolver::{Publisher, PublisherId},
};
use anyhow::{bail, Result};
use cross_krb5::{K5Ctx, ServerCtx};
use fxhash::FxHashMap;
use netidx_core::pack::Pack;
use parking_lot::{RwLock, RwLockReadGuard};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::task;

pub(crate) mod local {
    use super::*;

    pub(crate) struct Authenticator(AuthServer);

    impl Authenticator {
        pub(crate) async fn new(socket_path: &str) -> Result<Self> {
            Ok(Authenticator(AuthServer::start(socket_path).await?))
        }

        pub(crate) fn authenticate(&self, mut token: &[u8]) -> Result<Credential> {
            if token.len() < 10 {
                bail!("token short")
            }
            let cred = <Credential as Pack>::decode(&mut token)?;
            if !self.0.validate(&cred) {
                bail!("invalid token")
            }
            Ok(cred)
        }
    }
}

trait SecDataCommon {
    fn publisher(&self) -> &Publisher;
    fn secret(&self) -> u128;
}

pub(crate) struct SecCtxData<S: 'static> {
    pub(crate) users: UserDb,
    by_id: FxHashMap<PublisherId, S>,
    by_addr: FxHashMap<SocketAddr, PublisherId>,
    pub(crate) pmap: PMap,
}

impl<S: 'static + SecDataCommon> SecCtxData<S> {
    pub(crate) fn new(cfg: &Arc<config::server::Config>) -> Result<Self> {
        let mut users = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(&cfg.perms, &mut users, cfg.root(), &cfg.children)?;
        Ok(Self { users, pmap, by_id: HashMap::default(), by_addr: HashMap::default() })
    }

    pub(crate) fn remove_by_id(&mut self, id: &PublisherId) {
        if let Some(s) = self.by_id.remove(&id) {
            self.by_addr.remove(&s.publisher().addr);
        }
    }

    pub(crate) fn remove_by_addr(&mut self, addr: &SocketAddr) {
        if let Some(id) = self.by_addr.remove(addr) {
            self.by_id.remove(&id);
        }
    }

    pub(crate) fn insert(&mut self, data: S) {
        let addr = data.publisher().addr;
        let id = data.publisher().id;
        self.remove_by_id(&id);
        self.remove_by_addr(&addr);
        self.by_addr.insert(addr, id);
        self.by_id.insert(id, data);
    }

    pub(crate) fn get_secret_by_id(&self, id: &PublisherId) -> Option<u128> {
        self.by_id.get(id).map(|d| d.secret())
    }
}

pub(crate) struct K5SecData {
    pub(crate) spn: Chars,
    pub(crate) secret: u128,
    pub(crate) ctx: K5CtxWrap<ServerCtx>,
    pub(crate) publisher: Arc<Publisher>,
}

impl SecDataCommon for K5SecData {
    fn publisher(&self) -> &Publisher {
        &self.publisher
    }

    fn secret(&self) -> u128 {
        self.secret
    }
}

impl SecCtxData<K5SecData> {
    pub(crate) fn get(&self, id: &SocketAddr) -> Option<&K5SecData> {
        self.by_addr.get(id).and_then(|id| {
            self.by_id.get(id).and_then(|r| {
                match task::block_in_place(|| r.ctx.lock().ttl()) {
                    Ok(ttl) if ttl.as_secs() > 0 => Some(r),
                    _ => None,
                }
            })
        })
    }

    pub(crate) fn get_ctx(&self, id: &SocketAddr) -> Option<K5CtxWrap<ServerCtx>> {
        self.get(id).map(|d| d.ctx.clone())
    }
}

pub(crate) struct LocalSecData {
    pub(crate) user: Chars,
    pub(crate) secret: u128,
    pub(crate) publisher: Arc<Publisher>,
}

impl SecDataCommon for LocalSecData {
    fn publisher(&self) -> &Publisher {
        &self.publisher
    }

    fn secret(&self) -> u128 {
        self.secret
    }
}

impl SecCtxData<LocalSecData> {
    pub(crate) fn get(&self, id: &SocketAddr) -> Option<&LocalSecData> {
        self.by_addr.get(id).and_then(|id| self.by_id.get(id))
    }
}

pub(crate) enum SecCtxDataReadGuard<'a> {
    Anonymous,
    Krb5(RwLockReadGuard<'a, SecCtxData<K5SecData>>),
    Local(RwLockReadGuard<'a, SecCtxData<LocalSecData>>),
}

impl<'a> SecCtxDataReadGuard<'a> {
    pub(crate) fn pmap(&'a self) -> Option<&'a PMap> {
        match self {
            SecCtxDataReadGuard::Anonymous => None,
            SecCtxDataReadGuard::Krb5(r) => Some(&r.pmap),
            SecCtxDataReadGuard::Local(r) => Some(&r.pmap),
        }
    }
}

#[derive(Clone)]
pub(crate) enum SecCtx {
    Anonymous,
    Krb5(Arc<(Chars, RwLock<SecCtxData<K5SecData>>)>),
    Local(Arc<(local::Authenticator, RwLock<SecCtxData<LocalSecData>>)>),
}

impl SecCtx {
    pub(crate) async fn new(
        cfg: &Arc<config::server::Config>,
        id: &SocketAddr,
    ) -> Result<Self> {
        let t = match &cfg.auth {
            config::Auth::Anonymous => SecCtx::Anonymous,
            config::Auth::Local { path } => {
                let auth = local::Authenticator::new(&path).await?;
                let store = RwLock::new(SecCtxData::new(cfg)?);
                SecCtx::Local(Arc::new((auth, store)))
            }
            config::Auth::Krb5 { spn } => {
                let store = RwLock::new(SecCtxData::new(cfg)?);
                SecCtx::Krb5(Arc::new((spn.clone(), store)))
            }
        };
        Ok(t)
    }

    pub(crate) fn read(&self) -> SecCtxDataReadGuard {
        match self {
            SecCtx::Anonymous => SecCtxDataReadGuard::Anonymous,
            SecCtx::Krb5(a) => SecCtxDataReadGuard::Krb5(a.1.read()),
            SecCtx::Local(a) => SecCtxDataReadGuard::Local(a.1.read()),
        }
    }
}
