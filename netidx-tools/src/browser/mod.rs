use crate::publisher;
use anyhow::{Context, Result};
use arcstr::literal;
use graphix_compiler::expr::Source;
use graphix_rt::NoExt;
use graphix_shell::{Mode, ShellBuilder};
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
    let (mt, rx) = graphix_package::MainThreadHandle::new();
    std::thread::spawn(move || {
        while let Ok(_) = rx.recv() {
            panic!("The main thread is not available")
        }
    });
    let publisher = PublisherBuilder::new(cfg.clone())
        .desired_auth(auth.clone())
        .bind_cfg(params.bind)
        .build()
        .await
        .context("creating publisher")?;
    let subscriber = Subscriber::new(cfg, auth).context("create subscriber")?;
    ShellBuilder::<NoExt>::default()
        .mode(Mode::Script(Source::Internal(literal!(include_str!("browser.gx")))))
        .publisher(publisher)
        .subscriber(subscriber)
        .no_init(true)
        .build()?
        .run(mt)
        .await
}
