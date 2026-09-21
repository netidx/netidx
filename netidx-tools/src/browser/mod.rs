use crate::{gx, publisher};
use anyhow::{Context, Result};
use arcstr::literal;
use graphix_package_core::NetConfig;
use graphix_shell::ShellBuilder;
use netidx::{
    config::Config,
    publisher::{DesiredAuth, PublisherBuilder},
    subscriber::Subscriber,
};

#[tokio::main]
pub async fn run(
    cfg: Config,
    auth: DesiredAuth,
    params: publisher::Params,
) -> Result<()> {
    let publisher = PublisherBuilder::new(cfg.clone())
        .desired_auth(auth.clone())
        .bind_cfg(params.bind)
        .build()
        .await
        .context("creating publisher")?;
    let subscriber = Subscriber::new(cfg, auth).context("create subscriber")?;
    crate::log_errors::publisher(&publisher);
    let net_config = NetConfig::Ready { publisher, subscriber };
    let shell = ShellBuilder::default().setup_context(Box::new(move |ctx| {
        ctx.libstate.set(net_config);
    }));
    gx::run_tui(shell, literal!(include_str!("browser.gx"))).await
}
