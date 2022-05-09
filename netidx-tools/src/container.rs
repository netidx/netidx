use netidx::{config::Config, publisher::DesiredAuth};
use netidx_container::Container;
pub(super) use netidx_container::Params;
use tokio::{runtime::Runtime, signal::ctrl_c};

pub fn run(cfg: Config, auth: DesiredAuth, params: Params) {
    Runtime::new().expect("failed to create runtime").block_on(async move {
        let _c =
            Container::start(cfg, auth, params).await.expect("container init failed");
        ctrl_c().await.expect("ctrl-c handler failed");
    })
}
