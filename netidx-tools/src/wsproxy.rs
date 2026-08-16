use anyhow::{Context, Result};
use netidx::{
    publisher::PublisherBuilder, resolver_client::DesiredAuth, subscriber::Subscriber,
};
use std::time::Duration;

use crate::publisher;

#[tokio::main]
pub(super) async fn run(
    cfg: netidx::config::Config,
    auth: DesiredAuth,
    pcfg: publisher::Params,
    proxy: netidx_wsproxy::config::Config,
) -> Result<()> {
    env_logger::init();
    let timeout = pcfg.timeout.map(Duration::from_secs);
    let bind = pcfg.bind;
    let make = move || {
        let cfg = cfg.clone();
        let auth = auth.clone();
        async move {
            let publisher = PublisherBuilder::new(cfg.clone())
                .desired_auth(auth.clone())
                .bind_cfg(bind)
                .build()
                .await
                .context("creating publisher")?;
            let subscriber = Subscriber::new(cfg, auth).context("creating subscriber")?;
            crate::log_errors::publisher(&publisher);
            crate::log_errors::subscriber(&subscriber);
            Ok((publisher, subscriber))
        }
    };
    netidx_wsproxy::run(proxy, make, timeout).await.context("ws proxy")
}
