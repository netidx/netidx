use crate::{
    channel,
    path::Path,
    pool::{Pool, Pooled},
    protocol::resolver::{
        FromRead, FromWrite, Publisher, PublisherId, Resolved, ToRead, ToWrite,
    },
    utils,
};
use anyhow::Result;
use cross_krb5::{ClientCtx, InitiateFlags, Step};
use futures::channel::oneshot;
use fxhash::FxHashMap;
use netidx_core::pack::BoundedBytes;
use std::{fmt::Debug, str::FromStr, time::Duration};
use tokio::{net::TcpStream, task};
use lltimer as time;

pub(super) const HELLO_TO: Duration = Duration::from_secs(15);

lazy_static! {
    pub(super) static ref PUBLISHERPOOL: Pool<FxHashMap<PublisherId, Publisher>> =
        Pool::new(1000, 100);
    pub(super) static ref RAWTOREADPOOL: Pool<Vec<ToRead>> = Pool::new(100, 10_000);
    pub(super) static ref RAWFROMREADPOOL: Pool<Vec<FromRead>> = Pool::new(100, 10_000);
    pub(super) static ref TOREADPOOL: Pool<Vec<(usize, ToRead)>> = Pool::new(100, 10_000);
    pub(super) static ref FROMREADPOOL: Pool<Vec<(usize, FromRead)>> =
        Pool::new(100, 10_000);
    pub(super) static ref RAWTOWRITEPOOL: Pool<Vec<ToWrite>> = Pool::new(100, 10_000);
    pub(super) static ref RAWFROMWRITEPOOL: Pool<Vec<FromWrite>> = Pool::new(100, 10_000);
    pub(super) static ref TOWRITEPOOL: Pool<Vec<(usize, ToWrite)>> =
        Pool::new(100, 10_000);
    pub(super) static ref FROMWRITEPOOL: Pool<Vec<(usize, FromWrite)>> =
        Pool::new(100, 10_000);
    pub(super) static ref RESOLVEDPOOL: Pool<Vec<Resolved>> = Pool::new(100, 10_000);
    pub(super) static ref LISTPOOL: Pool<Vec<Pooled<Vec<Path>>>> = Pool::new(100, 10_000);
    pub(super) static ref PATHPOOL: Pool<Vec<Path>> = Pool::new(100, 100);
}

/// `DesiredAuth` instructs publishers and subscribers what authentication mechanism
/// they should try to use. To use the default specified in the configuration you
/// can call `Config::default_auth`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DesiredAuth {
    /// Don't use any authentication, authorization, or encryption.
    Anonymous,
    /// Use Kerberos for authentication and encryption. upn is the user principal
    /// name you wish to use to talk to the resolver server and the publishers
    /// (in the case of a subscriber). You should have a TGT for this user. If
    /// upn is `None` then the current user will be used.
    ///
    /// spn is required for publishers and ignored for subscribers. It is the
    /// service principal name that the publisher will publish as. The resolver
    /// will tell clients of the publisher to obtain a service ticket for
    /// the spn you specify here. Make sure you have a keytab for it available.
    Krb5 { upn: Option<String>, spn: Option<String> },
    /// Use local authentication. Your bind address must be `local` in order
    /// to use this mechanism.
    Local,
    /// Use Transport Layer Security for authentication and encryption.
    /// You must have one or more identities configured in your client
    /// configuration. If identity is `None` the default identity will be
    /// used. Otherwise identity should be the name of an identity in
    /// the configuration.
    Tls { identity: Option<String> },
}

impl FromStr for DesiredAuth {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<DesiredAuth, Self::Err> {
        match s {
            "anonymous" => Ok(DesiredAuth::Anonymous),
            "local" => Ok(DesiredAuth::Local),
            "krb5" => Ok(DesiredAuth::Krb5 { upn: None, spn: None }),
            "tls" => Ok(DesiredAuth::Tls { identity: None }),
            _ => bail!("expected, anonymous, local, krb5, or tls"),
        }
    }
}

pub(super) type Response<F> =
    (Pooled<FxHashMap<PublisherId, Publisher>>, Pooled<Vec<(usize, F)>>);

pub(super) type ResponseChan<F> = oneshot::Receiver<Response<F>>;

pub(crate) async fn krb5_authentication(
    principal: Option<&str>,
    target_principal: &str,
    con: &mut TcpStream,
) -> Result<ClientCtx> {
    async fn send(con: &mut TcpStream, token: &[u8]) -> Result<()> {
        let token = BoundedBytes::<L>(utils::bytes(&*token));
        Ok(time::timeout(HELLO_TO, channel::write_raw(con, &token)).await??)
    }
    const L: usize = 1 * 1024 * 1024;
    let (mut ctx, token) = task::spawn_blocking({
        let principal = principal.map(String::from);
        let target_principal = String::from(target_principal);
        move || {
            ClientCtx::new(
                InitiateFlags::empty(),
                principal.as_ref().map(|s| s.as_str()),
                &target_principal,
                None,
            )
        }
    })
    .await??;
    send(con, &*token).await?;
    loop {
        let token: BoundedBytes<L> =
            time::timeout(HELLO_TO, channel::read_raw(con)).await??;
        match task::spawn_blocking(move || ctx.step(&*token)).await?? {
            Step::Continue((nctx, token)) => {
                ctx = nctx;
                send(con, &*token).await?
            }
            Step::Finished((ctx, token)) => {
                if let Some(token) = token {
                    send(con, &*token).await?
                }
                break Ok(ctx);
            }
        }
    }
}
