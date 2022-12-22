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
use tokio::{net::TcpStream, task, time};

pub(super) const HELLO_TO: Duration = Duration::from_secs(15);

lazy_static! {
    pub(super) static ref PUBLISHERPOOL: Pool<FxHashMap<PublisherId, Publisher>> =
        Pool::new(1000, 1000);
    pub(super) static ref RAWTOREADPOOL: Pool<Vec<ToRead>> = Pool::new(1000, 10000);
    pub(super) static ref RAWFROMREADPOOL: Pool<Vec<FromRead>> = Pool::new(1000, 10000);
    pub(super) static ref TOREADPOOL: Pool<Vec<(usize, ToRead)>> = Pool::new(1000, 10000);
    pub(super) static ref FROMREADPOOL: Pool<Vec<(usize, FromRead)>> =
        Pool::new(1000, 10000);
    pub(super) static ref RAWTOWRITEPOOL: Pool<Vec<ToWrite>> = Pool::new(1000, 10000);
    pub(super) static ref RAWFROMWRITEPOOL: Pool<Vec<FromWrite>> = Pool::new(1000, 10000);
    pub(super) static ref TOWRITEPOOL: Pool<Vec<(usize, ToWrite)>> =
        Pool::new(1000, 10000);
    pub(super) static ref FROMWRITEPOOL: Pool<Vec<(usize, FromWrite)>> =
        Pool::new(1000, 10000);
    pub(super) static ref RESOLVEDPOOL: Pool<Vec<Resolved>> = Pool::new(1000, 10000);
    pub(super) static ref LISTPOOL: Pool<Vec<Pooled<Vec<Path>>>> = Pool::new(1000, 10000);
    pub(super) static ref PATHPOOL: Pool<Vec<Path>> = Pool::new(100, 100);
}

#[derive(Debug, Clone)]
pub enum DesiredAuth {
    Anonymous,
    Krb5 {
        upn: Option<String>,
        spn: Option<String>,
    },
    Local,
    Tls {
        name: Option<String>,
        certificate: String,
        private_key: String,
    },
}

impl FromStr for DesiredAuth {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<DesiredAuth, Self::Err> {
        match s {
            "anonymous" => Ok(DesiredAuth::Anonymous),
            "local" => Ok(DesiredAuth::Local),
            "krb5" => Ok(DesiredAuth::Krb5 { upn: None, spn: None }),
            _ => bail!("expected, anonymous, local, or krb5"),
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
    let (mut ctx, token) = task::block_in_place(|| {
        ClientCtx::new(InitiateFlags::empty(), principal, target_principal, None)
    })?;
    send(con, &*token).await?;
    loop {
        let token: BoundedBytes<L> =
            time::timeout(HELLO_TO, channel::read_raw(con)).await??;
        match task::block_in_place(|| ctx.step(&*token))? {
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
