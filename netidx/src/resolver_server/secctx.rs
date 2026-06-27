use super::{
    auth::{ANONYMOUS, PMap, UserDb, UserInfo},
    config::{self, Auth, Config, MemberServer},
};
use crate::{
    channel::K5CtxWrap,
    os::{
        Mapper,
        local_auth::{AuthServer, Credential},
    },
    path::Path,
    protocol::resolver::{PublisherId, Referral},
    tls,
};
use anyhow::{Result, bail};
use arcstr::ArcStr;
use chrono::Utc;
use cross_krb5::{K5Ctx, ServerCtx};
use log::{debug, warn};
use netidx_core::pack::Pack;
use nohash::IntMap;
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{RwLock, RwLockReadGuard},
    time,
};

/// Upper bound on a single id-map lookup. The mapper normally answers a local
/// socket in well under a millisecond; this only bites when the id-map daemon
/// is hung. We never hold the store lock across the lookup, so a stall here
/// slows only the one resolve that triggered it, not every other resolve.
const ID_MAP_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Map an authenticated identity to its uid/groups for authorization.
///
/// Consults the id-map mapper only on a cold/expired cache, and crucially does
/// so WITHOUT holding the store lock — a slow or hung id-map daemon must not be
/// able to wedge every other resolve behind it. On a mapper error or timeout we
/// serve the last-known-good mapping for this identity if we have one: the cache
/// already serves mappings for up to `id_map_timeout`, so a little extra
/// staleness during an outage is consistent with that trust model and far
/// better than denying an identity we successfully mapped moments ago. Only a
/// never-before-seen identity (no cached value at all) surfaces the error.
pub(super) async fn user_info<S: 'static>(
    store: &RwLock<SecCtxData<S>>,
    resolver: SocketAddr,
    user: Option<&str>,
) -> Result<Arc<UserInfo>> {
    let user = match user {
        None => return Ok(ANONYMOUS.clone()),
        Some(user) => user,
    };
    let mapper = {
        let r = store.read().await;
        if let Some(fresh) = r.users.fresh(user, Utc::now()) {
            return Ok(fresh);
        }
        r.users.mapper()
    };
    match time::timeout(ID_MAP_CALL_TIMEOUT, mapper.groups(user)).await {
        Ok(Ok((primary, groups))) => {
            Ok(store.write().await.users.record(resolver, user, primary, groups))
        }
        failed => {
            if let Some(stale) = store.read().await.users.last_known(user) {
                warn!(
                    "id-map lookup for {user:?} failed, serving last-known-good mapping"
                );
                return Ok(stale);
            }
            match failed {
                Ok(Err(e)) => Err(e.context(format!("mapping identity {user:?}"))),
                Ok(Ok(_)) => unreachable!(),
                Err(_) => {
                    bail!("id-map lookup for {user:?} timed out, no cached mapping")
                }
            }
        }
    }
}

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
    /// Cluster root path captured at construction. Used as the
    /// `root` argument to `PMap::from_file` on perms reload so the
    /// reload validates against the running structure rather than a
    /// possibly-edited parent/children section the operator has
    /// changed without restarting.
    pub(super) root: Path,
    /// Cluster children captured at construction; same rationale as
    /// `root`.
    pub(super) children: BTreeMap<Path, Referral>,
    data: IntMap<PublisherId, S>,
}

impl<S: 'static + SecDataCommon> SecCtxData<S> {
    pub(super) async fn new(cfg: &Config, member: &MemberServer) -> Result<Self> {
        let mut users =
            UserDb::new(member.id_map_timeout, Mapper::new(cfg, member).await?);
        let pmap = PMap::from_file(&cfg.perms, &mut users, cfg.root(), &cfg.children)?;
        Ok(Self {
            users,
            pmap,
            root: Path::from(ArcStr::from(cfg.root())),
            children: cfg.children.clone(),
            data: HashMap::default(),
        })
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
        self.data.get(id).and_then(|r| match r.ctx.lock().ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(r),
            _ => None,
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
    Krb5(Arc<(ArcStr, RwLock<SecCtxData<K5SecData>>)>),
    Local(Arc<(LocalAuth, RwLock<SecCtxData<LocalSecData>>)>),
    Tls(Arc<(tls::CrlWatchingAcceptor, RwLock<SecCtxData<TlsSecData>>)>),
}

impl SecCtx {
    /// Replace the in-memory `PMap` with one rebuilt from `new_perms`.
    pub(crate) async fn reload_pmap(&self, new_perms: &config::PMap) -> Result<()> {
        async fn swap_one<S: 'static>(
            store: &RwLock<SecCtxData<S>>,
            new_perms: &config::PMap,
        ) -> Result<()> {
            let mut w = store.write().await;
            let SecCtxData { users, root, children, .. } = &mut *w;
            let rebuilt = PMap::from_file(new_perms, users, &*root, children)?;
            w.pmap = rebuilt;
            Ok(())
        }
        match self {
            SecCtx::Anonymous => Ok(()),
            SecCtx::Krb5(a) => swap_one(&a.1, new_perms).await,
            SecCtx::Local(a) => swap_one(&a.1, new_perms).await,
            SecCtx::Tls(a) => swap_one(&a.1, new_perms).await,
        }
    }

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
                // CRL-watching: a `crl.pem` dropped beside the trusted
                // bundle (by the conf plane) takes effect on the next
                // accept — the resolver is the revocation choke point.
                let auth = tls::CrlWatchingAcceptor::new(
                    None,
                    trusted,
                    certificate,
                    private_key,
                )?;
                let store = RwLock::new(SecCtxData::new(cfg, member).await?);
                SecCtx::Tls(Arc::new((auth, store)))
            }
        };
        Ok(t)
    }

    pub(super) async fn read<'a>(&'a self) -> SecCtxDataReadGuard<'a> {
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
