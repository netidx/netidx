use netidx::{config::Config, publisher::DesiredAuth};
use netidx_container::ContainerHandle;
pub(super) use netidx_container::Params;
use tokio::{runtime::Runtime, signal::ctrl_c};

pub fn run(cfg: Config, auth: DesiredAuth, params: Params) {
    Runtime::new().expect("failed to create runtime").block_on(async move {
        match ContainerHandle::start(cfg, auth, params) {
            Ok(_h) => {
                ctrl_c().await.expect("ctrl-c handler failed");
            }
            Err(_) => ()
        }
    })
}
