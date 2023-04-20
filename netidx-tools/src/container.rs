use anyhow::{Result, Context};
use netidx::{config::Config, publisher::DesiredAuth};
use netidx_container::Container;
pub(super) use netidx_container::Params;
use tokio::signal::ctrl_c;

pub(crate) async fn run(cfg: Config, auth: DesiredAuth, params: Params) -> Result<()> {
    let _c =
        Container::start(cfg, auth, params).await.context("container init failed")?;
    ctrl_c().await.context("ctrl-c handler failed")?;
    Ok(())
}
