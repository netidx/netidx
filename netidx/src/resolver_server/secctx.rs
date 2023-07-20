use super::{
    auth::{PMap, UserDb},
    config::{Auth, Config, MemberServer},
};
use crate::{
    channel::K5CtxWrap,
    chars::Chars,
    os::{
        local_auth::{AuthServer, Credential},
        Mapper,
    },
    protocol::resolver::PublisherId,
    tls,
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use cross_krb5::{K5Ctx, ServerCtx};
use fxhash::FxHashMap;
use log::debug;
use netidx_core::pack::Pack;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::{RwLock, RwLockReadGuard},
    task,
};

pub(super) struct LocalAuth(AuthServer);

impl LocalAuth {
    pub(super) async fn new(
        socket_path: &str,
        cfg: &Config,
        member: &MemberServer,
    ) -> Result<Self> {
        Ok(Self(AuthServer::start(socket_path, cfg, member).await?))
    }

    pub(super) fn authenticate(&self, mut token: &[u8]) -> Result<Credential> {
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

pub(super) trait SecDataCommon {
    fn secret(&self) -> u128;
}

pub(super) struct SecCtxData<S: 'static> {
    pub(super) users: UserDb,
    pub(super) pmap: PMap,
    data: FxHashMap<PublisherId, S>,
}

impl<S: 'static + SecDataCommon> SecCtxData<S> {
    pub(super) async fn new(cfg: &Config, member: &MemberServer) -> Result<Self> {
        let mut users =
            UserDb::new(member.id_map_timeout, Mapper::new(cfg, member).await?);
        let pmap = PMap::from_file(&cfg.perms, &mut users, cfg.root(), &cfg.children)?;
        Ok(Self { users, pmap, data: HashMap::default() })
    }

    pub(super) fn remove(&mut self, id: &PublisherId) {
        self.data.remove(&id);
    }

    pub(super) fn insert(&mut self, id: PublisherId, data: S) {
        self.remove(&id);
        self.data.insert(id, data);
    }

    pub(super) fn secret(&self, id: &PublisherId) -> Option<u128> {
        self.data.get(id).map(|d| d.secret())
    }
}

#[derive(Debug, Clone)]
pub(super) struct K5SecData {
    pub(super) secret: u128,
    pub(super) ctx: K5CtxWrap<ServerCtx>,
}

impl SecDataCommon for K5SecData {
    fn secret(&self) -> u128 {
        self.secret
    }
}

impl SecCtxData<K5SecData> {
    pub(super) fn get(&self, id: &PublisherId) -> Option<&K5SecData> {
        self.data.get(id).and_then(|r| {
            match task::block_in_place(|| r.ctx.lock().ttl()) {
                Ok(ttl) if ttl.as_secs() > 0 => Some(r),
                _ => None,
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct LocalSecData {
    pub(super) user: ArcStr,
    pub(super) secret: u128,
}

impl SecDataCommon for LocalSecData {
    fn secret(&self) -> u128 {
        self.secret
    }
}

impl SecCtxData<LocalSecData> {
    pub(super) fn get(&self, id: &PublisherId) -> Option<&LocalSecData> {
        self.data.get(id)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TlsSecData(pub(super) u128);

impl SecDataCommon for TlsSecData {
    fn secret(&self) -> u128 {
        self.0
    }
}

impl SecCtxData<TlsSecData> {
    pub(super) fn get(&self, id: &PublisherId) -> Option<&TlsSecData> {
        self.data.get(id)
    }
}

pub(super) enum SecCtxDataReadGuard<'a> {
    Anonymous,
    Krb5(RwLockReadGuard<'a, SecCtxData<K5SecData>>),
    Local(RwLockReadGuard<'a, SecCtxData<LocalSecData>>),
    Tls(RwLockReadGuard<'a, SecCtxData<TlsSecData>>),
}

impl<'a> SecCtxDataReadGuard<'a> {
    pub(super) fn pmap(&'a self) -> Option<&'a PMap> {
        match self {
            SecCtxDataReadGuard::Anonymous => None,
            SecCtxDataReadGuard::Krb5(r) => Some(&r.pmap),
            SecCtxDataReadGuard::Local(r) => Some(&r.pmap),
            SecCtxDataReadGuard::Tls(r) => Some(&r.pmap),
        }
    }
}

#[derive(Clone)]
pub(super) enum SecCtx {
    Anonymous,
    Krb5(Arc<(Chars, RwLock<SecCtxData<K5SecData>>)>),
    Local(Arc<(LocalAuth, RwLock<SecCtxData<LocalSecData>>)>),
    Tls(Arc<(tokio_rustls::TlsAcceptor, RwLock<SecCtxData<TlsSecData>>)>),
}

impl SecCtx {
    pub(super) async fn new(cfg: &Config, member: &MemberServer) -> Result<Self> {
        let t = match &member.auth {
            Auth::Anonymous => SecCtx::Anonymous,
            Auth::Local { path } => {
                debug!("starting local authenticator process");
                let auth = LocalAuth::new(&path, cfg, member).await?;
                let store = RwLock::new(SecCtxData::new(cfg, member).await?);
                SecCtx::Local(Arc::new((auth, store)))
            }
            Auth::Krb5 { spn } => {
                debug!("creating kerberos context with spn {}", spn);
                let store = RwLock::new(SecCtxData::new(cfg, member).await?);
                SecCtx::Krb5(Arc::new((spn.clone(), store)))
            }
            Auth::Tls { name: _, trusted, certificate, private_key } => {
                debug!("creating tls acceptor");
                let auth =
                    tls::create_tls_acceptor(None, trusted, certificate, private_key)?;
                let store = RwLock::new(SecCtxData::new(cfg, member).await?);
                SecCtx::Tls(Arc::new((auth, store)))
            }
        };
        Ok(t)
    }

    pub(super) async fn read(&self) -> SecCtxDataReadGuard {
        match self {
            SecCtx::Anonymous => SecCtxDataReadGuard::Anonymous,
            SecCtx::Krb5(a) => SecCtxDataReadGuard::Krb5(a.1.read().await),
            SecCtx::Local(a) => SecCtxDataReadGuard::Local(a.1.read().await),
            SecCtx::Tls(a) => SecCtxDataReadGuard::Tls(a.1.read().await),
        }
    }

    pub(super) async fn remove(&self, id: &PublisherId) {
        match self {
            SecCtx::Krb5(a) => a.1.write().await.remove(id),
            SecCtx::Local(a) => a.1.write().await.remove(id),
            SecCtx::Tls(a) => a.1.write().await.remove(id),
            SecCtx::Anonymous => (),
        }
    }
}
