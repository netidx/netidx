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
};
use anyhow::{bail, Result};
use cross_krb5::{K5Ctx, ServerCtx};
use fxhash::FxHashMap;
use netidx_core::pack::Pack;
use parking_lot::{RwLock, RwLockReadGuard};
use std::{collections::HashMap, sync::Arc};
use tokio::task;

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

pub(crate) fn load_certs(path: &str) -> Result<Vec<rustls::Certificate>> {
    use std::{fs, io::BufReader};
    Ok(rustls_pemfile::certs(&mut BufReader::new(fs::File::open(path)?))?
        .into_iter()
        .map(|v| rustls::Certificate(v))
        .collect())
}

pub(crate) fn load_private_key(path: &str) -> Result<rustls::PrivateKey> {
    use std::{fs, io::BufReader};
    let mut reader = BufReader::new(fs::File::open(path)?);
    while let Some(key) = rustls_pemfile::read_one(&mut reader)? {
        match key {
            rustls_pemfile::Item::RSAKey(key) => return Ok(rustls::PrivateKey(key)),
            rustls_pemfile::Item::PKCS8Key(key) => return Ok(rustls::PrivateKey(key)),
            rustls_pemfile::Item::ECKey(key) => return Ok(rustls::PrivateKey(key)),
            _ => (),
        }
    }
    // CR estokes: probably need to support encrypted keys.
    bail!("no keys found, encrypted keys not supported")
}

pub(super) struct TlsAuth(pub(super) tokio_rustls::TlsAcceptor);

impl TlsAuth {
    pub(super) fn new(
        root_certificates: &Chars,
        certificate: &Chars,
        private_key: &Chars,
    ) -> Result<TlsAuth> {
        let client_auth = {
            let mut root_store = rustls::RootCertStore::empty();
            for cert in task::block_in_place(|| load_certs(root_certificates))? {
                root_store.add(&cert)?;
            }
            rustls::server::AllowAnyAnonymousOrAuthenticatedClient::new(root_store)
        };
        let certs = task::block_in_place(|| load_certs(certificate))?;
        let private_key = task::block_in_place(|| load_private_key(private_key))?;
        let mut config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(client_auth)
            .with_single_cert(certs, private_key)?;
        config.session_storage = rustls::server::ServerSessionMemoryCache::new(1024);
        Ok(TlsAuth(tokio_rustls::TlsAcceptor::from(Arc::new(config))))
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
    pub(super) fn new(cfg: &Config, member: &MemberServer) -> Result<Self> {
        let mut users = UserDb::new(Mapper::new(cfg, member)?);
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
    pub(super) user: Chars,
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
    Tls(Arc<(TlsAuth, RwLock<SecCtxData<TlsSecData>>)>),
}

impl SecCtx {
    pub(super) async fn new(cfg: &Config, member: &MemberServer) -> Result<Self> {
        let t = match &member.auth {
            Auth::Anonymous => SecCtx::Anonymous,
            Auth::Local { path } => {
                let auth = LocalAuth::new(&path, cfg, member).await?;
                let store = RwLock::new(SecCtxData::new(cfg, member)?);
                SecCtx::Local(Arc::new((auth, store)))
            }
            Auth::Krb5 { spn } => {
                let store = RwLock::new(SecCtxData::new(cfg, member)?);
                SecCtx::Krb5(Arc::new((spn.clone(), store)))
            }
            Auth::Tls { root_certificates, certificate, private_key } => {
                let auth = TlsAuth::new(root_certificates, certificate, private_key)?;
                let store = RwLock::new(SecCtxData::new(cfg, member)?);
                SecCtx::Tls(Arc::new((auth, store)))
            }
        };
        Ok(t)
    }

    pub(super) fn read(&self) -> SecCtxDataReadGuard {
        match self {
            SecCtx::Anonymous => SecCtxDataReadGuard::Anonymous,
            SecCtx::Krb5(a) => SecCtxDataReadGuard::Krb5(a.1.read()),
            SecCtx::Local(a) => SecCtxDataReadGuard::Local(a.1.read()),
            SecCtx::Tls(a) => SecCtxDataReadGuard::Tls(a.1.read()),
        }
    }

    pub(super) fn remove(&self, id: &PublisherId) {
        match self {
            SecCtx::Krb5(a) => a.1.write().remove(id),
            SecCtx::Local(a) => a.1.write().remove(id),
            SecCtx::Tls(a) => a.1.write().remove(id),
            SecCtx::Anonymous => (),
        }
    }
}
