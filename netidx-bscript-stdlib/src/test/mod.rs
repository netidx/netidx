use anyhow::Result;
use enumflags2::BitFlags;
use futures::channel::mpsc;
use netidx::{
    pool::Pooled, publisher::PublisherBuilder, resolver_server,
    subscriber::SubscriberBuilder,
};
use netidx_bscript::{
    rt::{BSConfig, BSCtx, BSHandle, RtEvent},
    ExecCtx,
};

mod langtest;
mod libtest;

pub struct TestCtx {
    pub _resolver: resolver_server::Server,
    pub rt: BSHandle,
}

pub async fn init(sub: mpsc::Sender<Pooled<Vec<RtEvent>>>) -> Result<TestCtx> {
    let _ = env_logger::try_init();
    let resolver = {
        use resolver_server::config::{self, file};
        let cfg = file::ConfigBuilder::default()
            .member_servers(vec![file::MemberServerBuilder::default()
                .auth(file::Auth::Anonymous)
                .addr("127.0.0.1:0".parse()?)
                .bind_addr("127.0.0.1".parse()?)
                .build()?])
            .build()?;
        let cfg = config::Config::from_file(cfg)?;
        resolver_server::Server::new(cfg, false, 0).await?
    };
    let addr = *resolver.local_addr();
    let cfg = {
        use netidx::config::{self, file, DefaultAuthMech};
        let cfg = file::ConfigBuilder::default()
            .addrs(vec![(addr, file::Auth::Anonymous)])
            .default_auth(DefaultAuthMech::Anonymous)
            .default_bind_config("local")
            .build()?;
        config::Config::from_file(cfg)?
    };
    let publisher = PublisherBuilder::new(cfg.clone()).build().await?;
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let mut ctx = ExecCtx::new(BSCtx::new(publisher, subscriber));
    let (root, mods) = crate::register(&mut ctx, BitFlags::all())?;
    Ok(TestCtx {
        _resolver: resolver,
        rt: BSConfig::builder(ctx, sub)
            .root(root)
            .resolvers(vec![mods])
            .build()?
            .start()
            .await?,
    })
}

#[macro_export]
macro_rules! run {
    ($name:ident, $code:expr, $pred:expr) => {
        #[tokio::test(flavor = "current_thread")]
        async fn $name() -> ::anyhow::Result<()> {
            let (tx, mut rx) = futures::channel::mpsc::channel(10);
            let ctx = $crate::test::init(tx).await?;
            let bs = ctx.rt;
            match bs.compile(arcstr::ArcStr::from($code)).await {
                Err(e) => assert!($pred(dbg!(Err(e)))),
                Ok(e) => {
                    dbg!("compilation succeeded");
                    let eid = e.exprs[0].id;
                    loop {
                        match futures::StreamExt::next(&mut rx).await {
                            None => bail!("runtime died"),
                            Some(mut batch) => {
                                for e in batch.drain(..) {
                                    match e {
                                        netidx_bscript::rt::RtEvent::Env(_) => (),
                                        netidx_bscript::rt::RtEvent::Updated(id, v) => {
                                            assert_eq!(id, eid);
                                            eprintln!("{v}");
                                            assert!($pred(Ok(&v)));
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    };
}
