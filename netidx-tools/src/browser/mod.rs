use crate::publisher;
use anyhow::{Context, Result};
use arcstr::literal;
use graphix_rt::NoExt;
use graphix_shell::{Mode, ShellBuilder};
use netidx::{
    config::Config,
    publisher::{DesiredAuth, PublisherBuilder},
    subscriber::Subscriber,
};

pub async fn run(
    cfg: Config,
    auth: DesiredAuth,
    params: publisher::Params,
) -> Result<()> {
    /*
    let resolver = ModuleResolver::VFS(FxHashMap::from_iter([(
        Path::from("/browser"),
        literal!(include_str!("browser.gx")),
    )]));
    */
    let publisher = PublisherBuilder::new(cfg.clone())
        .desired_auth(auth.clone())
        .bind_cfg(params.bind)
        .build()
        .await
        .context("creating publisher")?;
    let subscriber = Subscriber::new(cfg, auth).context("create subscriber")?;
    ShellBuilder::<NoExt>::default()
        .mode(Mode::Static(literal!(include_str!("browser.gx"))))
        .publisher(publisher)
        .subscriber(subscriber)
        .no_init(true)
        .build()?
        .run()
        .await
}
