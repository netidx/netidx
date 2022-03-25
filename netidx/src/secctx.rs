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
use fxhash::FxHashMap;
use netidx_core::pack::Pack;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use rand::Rng;
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
            if !self.server.validate(&cred) {
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
            let spn = Some(&*self.spn);
            let (ctx, tok) = task::block_in_place(|| {
                ServerCtx::accept(AcceptFlags::empty(), spn, tok)
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

impl SecCtxData<S: 'static> {
    pub(crate) fn new(
        pmap: config::server::PMap,
        cfg: &Arc<config::server::Config>,
    ) -> Result<Self> {
        let mut users = UserDb::new(Mapper::new()?);
        let pmap = PMap::from_file(pmap, &mut users, cfg.root(), &cfg.children)?;
        Ok(Self { users, pmap, secrets: HashMap::default() })
    }
}

impl SecCtxData<(Chars, u128, K5CtxWrap<ServerCtx>)> {
    pub(crate) fn get_k5(
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
}

impl SecCtxData<u128> {
    pub(crate) fn get_local(&self, id: &SocketAddr) -> Option<u128> {
        self.secrets.get(id).map(|s| *s)
    }
}

pub(crate) enum SecCtxDataReadGuard<'a> {
    Anonymous,
    Krb5(RwLockReadGuard<'a, SecCtxData<(Chars, u128, K5CtxWrap<ServerCtx>)>>),
    Local(RwLockReadGuard<'a, SecCtxData<u128>>),
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
    pub(crate) fn data(&self) -> SecCtxDataReadGuard {
        match self {
            SecCtx::Anonymous => SecCtxDataReadGuard::Anonymous,
            SecCtx::Krb5(a) => SecCtxDataReadGuard::Krb5(a.1.read()),
            SecCtx::Local(a) => SecCtxDataReadGuard::Local(a.1.read()),
        }
    }
}
