use crate::{
    auth::{PMap, UserDb},
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
use cross_krb5::{AcceptFlags, K5Ctx, ServerCtx, Step};
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

pub(crate) mod k5 {
    use super::*;

    pub(crate) struct Authenticator(Chars);

    impl Authenticator {
        pub(crate) fn new(spn: Chars) -> Self {
            Authenticator(spn)
        }

        pub(crate) fn authenticate(
            &self,
            tok: &[u8],
        ) -> Result<(K5CtxWrap<ServerCtx>, Bytes)> {
            let (ctx, tok) = task::block_in_place(|| {
                let ctx = ServerCtx::new(AcceptFlags::empty(), Some(&*self.0))?;
                match ctx.step(tok)? {
                    Step::Finished((ctx, Some(tok))) => Ok((ctx, tok)),
                    Step::Finished((_, None)) => bail!("expected finish with token"),
                    Step::Continue((_, _)) => {
                        bail!("context did not finish, needs more tokens")
                    }
                }
            })?;
            Ok((K5CtxWrap::new(ctx), utils::bytes(&*tok)))
        }
    }
}

pub(crate) struct SecCtxData<S: 'static> {
    pub(crate) users: UserDb,
    secrets: FxHashMap<SocketAddr, S>,
    pub(crate) pmap: PMap,
}

impl<S: 'static> SecCtxData<S> {
    pub(crate) fn new(cfg: &Arc<config::server::Config>) -> Result<Self> {
        let mut users = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(&cfg.perms, &mut users, cfg.root(), &cfg.children)?;
        Ok(Self { users, pmap, secrets: HashMap::default() })
    }

    pub(crate) fn remove(&mut self, id: &SocketAddr) {
        self.secrets.remove(id);
    }
}

impl SecCtxData<(Chars, u128, K5CtxWrap<ServerCtx>)> {
    pub(crate) fn get_k5_full(
        &self,
        id: &SocketAddr,
    ) -> Option<&(Chars, u128, K5CtxWrap<ServerCtx>)> {
        self.secrets.get(id).and_then(|r| {
            match task::block_in_place(|| r.2.lock().ttl()) {
                Ok(ttl) if ttl.as_secs() > 0 => Some(r),
                _ => None,
            }
        })
    }

    pub(crate) fn get_k5_ctx(&self, id: &SocketAddr) -> Option<K5CtxWrap<ServerCtx>> {
        self.get_k5_full(id).map(|(_, _, ctx)| ctx.clone())
    }

    pub(crate) fn insert_k5(
        &mut self,
        write_addr: SocketAddr,
        spn: Chars,
        secret: u128,
        k5ctx: K5CtxWrap<ServerCtx>,
    ) {
        self.secrets.insert(write_addr, (spn, secret, k5ctx));
    }
}

impl SecCtxData<u128> {
    pub(crate) fn get_local(&self, id: &SocketAddr) -> Option<u128> {
        self.secrets.get(id).map(|s| *s)
    }

    pub(crate) fn insert_local(&mut self, write_addr: SocketAddr, secret: u128) {
        self.secrets.insert(write_addr, secret);
    }
}

pub(crate) enum SecCtxDataReadGuard<'a> {
    Anonymous,
    Krb5(RwLockReadGuard<'a, SecCtxData<(Chars, u128, K5CtxWrap<ServerCtx>)>>),
    Local(RwLockReadGuard<'a, SecCtxData<u128>>),
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
    Krb5(
        Arc<(k5::Authenticator, RwLock<SecCtxData<(Chars, u128, K5CtxWrap<ServerCtx>)>>)>,
    ),
    Local(Arc<(local::Authenticator, RwLock<SecCtxData<u128>>)>),
}

impl SecCtx {
    pub(crate) async fn new(
        cfg: &Arc<config::server::Config>,
        id: &SocketAddr,
    ) -> Result<Self> {
        let t = match &cfg.auth {
            config::Auth::Anonymous => SecCtx::Anonymous,
            config::Auth::Local(path) => {
                let auth = local::Authenticator::new(&path).await?;
                let store = RwLock::new(SecCtxData::new(cfg)?);
                SecCtx::Local(Arc::new((auth, store)))
            }
            config::Auth::Krb5(spns) => {
                let auth = k5::Authenticator::new(spns[id].clone());
                let store = RwLock::new(SecCtxData::new(cfg)?);
                SecCtx::Krb5(Arc::new((auth, store)))
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
