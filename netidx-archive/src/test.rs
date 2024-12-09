use crate::{
    config::{ConfigBuilder, PublishConfigBuilder, RecordConfigBuilder},
    recorder::Recorder,
    recorder_client::Client,
};
use anyhow::{Context, Result};
use futures::{channel::oneshot, TryFuture};
use netidx::{
    config::{
        file::{self as client, Auth as CAuth},
        Config as CConfig, DefaultAuthMech,
    },
    path::Path,
    publisher::{Publisher, PublisherBuilder, Value},
    resolver_server::{
        config::{
            file::{self as server, Auth as SAuth},
            Config as SConfig,
        },
        Server,
    },
    subscriber::{Subscriber, SubscriberBuilder},
};
use std::time::Duration;
use tokio::{task, time};

struct Ctx {
    publisher: Publisher,
    subscriber: Subscriber,
    server: Server,
}

impl Ctx {
    async fn new() -> Result<Self> {
        let server_cfg = server::ConfigBuilder::default()
            .member_servers(vec![server::MemberServerBuilder::default()
                .addr("127.0.0.1:0".parse().context("parsing addr")?)
                .bind_addr("127.0.0.1".parse().context("parsing ip addr")?)
                .auth(SAuth::Anonymous)
                .build()
                .context("member server config")?])
            .build()
            .context("server config")?;
        let server_cfg =
            SConfig::from_file(server_cfg).context("validating server config")?;
        let server =
            Server::new(server_cfg, false, 0).await.context("starting server")?;
        let client_cfg = client::ConfigBuilder::default()
            .addrs(vec![(*server.local_addr(), CAuth::Anonymous)])
            .default_auth(DefaultAuthMech::Anonymous)
            .default_bind_config("local")
            .build()
            .context("client config")?;
        let client_cfg =
            CConfig::from_file(client_cfg).context("validating client config")?;
        let publisher = PublisherBuilder::new(client_cfg.clone())
            .build()
            .await
            .context("building publisher")?;
        let subscriber = SubscriberBuilder::new()
            .config(client_cfg.clone())
            .build()
            .context("building subscriber")?;
        Ok(Self { server, publisher, subscriber })
    }
}

const PATH: &str = "test-collection";
const SHARD: &str = "0";
const BASE: &str = "/recorder";

#[tokio::test(flavor = "multi_thread")]
async fn recorder() -> Result<()> {
    let ctx = Ctx::new().await.context("build creating context")?;
    let record = RecordConfigBuilder::default().build().context("build record config")?;
    let publish = PublishConfigBuilder::default()
        .bind("local".parse()?)
        .base(BASE)
        .build()
        .context("publish config")?;
    let cfg = ConfigBuilder::default()
        .record([(SHARD.into(), record)])
        .publish(publish)
        .archive_directory(PATH)
        .build()
        .context("build config")?;
    let (pub_tx, pub_rx) = oneshot::channel();
    let publish = || {
        let publisher = ctx.publisher.clone();
        let ids = [
            publisher.publish(Path::from("/test/d0"), Value::Null)?,
            publisher.publish(Path::from("/test/d1"), Value::Null)?,
        ];
        Ok::<_, anyhow::Error>(async move {
            publisher.flushed().await;
            let _ = pub_tx.send(());
            let mut i: u64 = 0;
            loop {
                let mut b = publisher.start_batch();
                for id in &ids {
                    id.update(&mut b, i)
                }
                b.commit(None).await;
                time::sleep(Duration::from_millis(250)).await;
                i += 1
            }
        })
    };
    let jh = task::spawn(publish()?);
    pub_rx.await.map_err(|_| anyhow!("publish task died"))?;
    let recorder = Recorder::start_with(
        cfg,
        Some(ctx.publisher.clone()),
        Some(ctx.subscriber.clone()),
    )
    .await
    .context("creating recorder")?;
    let client = Client::new(&ctx.subscriber, &Path::from(BASE))
        .context("create record client")?;
    
    unimplemented!()
}
