use netidx::{
    publisher::PublisherBuilder, resolver_client::DesiredAuth, subscriber::Subscriber,
};
use tokio::runtime::Runtime;

use crate::publisher;

pub(super) fn run(
    cfg: netidx::config::Config,
    auth: DesiredAuth,
    pcfg: publisher::Params,
    proxy: netidx_wsproxy::config::Config,
) {
    let rt = Runtime::new().expect("failed to create runtime");
    rt.block_on(async move {
        let mut builder = PublisherBuilder::new();
        builder.config(cfg.clone()).desired_auth(auth.clone());
        if let Some(bind) = pcfg.bind {
            builder.bind_cfg(bind);
        }
        let publisher = builder.build().await.expect("creating publisher");
        let subscriber = Subscriber::new(cfg, auth).expect("creating subscriber");
        netidx_wsproxy::run(proxy, publisher, subscriber).await.expect("ws proxy died")
    });
}
