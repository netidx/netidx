use crate::publisher;
use anyhow::{Context, Result};
use netidx::{
    publisher::PublisherBuilder, resolver_client::DesiredAuth, subscriber::Subscriber,
};
use std::time::Duration;

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
    let timeout = pcfg.timeout.map(Duration::from_secs);
    netidx_wsproxy::run(proxy, publisher, subscriber, timeout).await.context("ws proxy")
}
