use anyhow::{Context, Result};
use netidx::{
    publisher::PublisherBuilder, resolver_client::DesiredAuth, subscriber::Subscriber,
};

use crate::publisher;

pub(super) async fn run(
    cfg: netidx::config::Config,
    auth: DesiredAuth,
    pcfg: publisher::Params,
    proxy: netidx_wsproxy::config::Config,
) -> Result<()> {
    let publisher = PublisherBuilder::new(cfg.clone())
        .desired_auth(auth.clone())
        .bind_cfg(pcfg.bind)
        .build()
        .await
        .context("creating publisher")?;
    let subscriber = Subscriber::new(cfg, auth).context("creating subscriber")?;
    netidx_wsproxy::run(proxy, publisher, subscriber).await.context("ws proxy")
}
