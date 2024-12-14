use crate::{
    config::{ConfigBuilder, PublishConfigBuilder, RecordConfigBuilder},
    logfile::{BatchItem, Seek, BATCH_POOL},
    recorder::{Recorder, State},
    recorder_client::{Client, Speed},
};
use anyhow::{Context, Result};
use chrono::prelude::*;
use futures::{
    channel::{mpsc, oneshot},
    future,
    prelude::*,
};
use netidx::{
    chars::Chars,
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
    subscriber::{Event, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use netidx_netproto::glob::GlobSet;
use std::{fs, time::Duration};
use tokio::{task, time};

struct Ctx {
    publisher: Publisher,
    subscriber: Subscriber,
    _server: Server,
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
        Ok(Self { _server: server, publisher, subscriber })
    }
}

const PATH0: &str = "test-recorder";
const PATH1: &str = "test-embedded-recorder";
const SHARD: &str = "0";
const BASE0: &str = "/recorder";
const BASE1: &str = "/embedded-recorder";
const D0: &str = "/test/d0";
const D1: &str = "/test/d1";
const N: u64 = 1000;

#[tokio::test(flavor = "multi_thread")]
async fn recorder() -> Result<()> {
    let _ = env_logger::try_init();
    let _ = fs::remove_dir_all(PATH0);
    eprintln!("creating context");
    let ctx = Ctx::new().await.context("build creating context")?;
    eprintln!("context created");
    let record = RecordConfigBuilder::default()
        .try_spec(vec![Chars::from("/test/**")])
        .context("compiling spec")?
        .build()
        .context("build record config")?;
    let publish = PublishConfigBuilder::default()
        .bind("local".parse()?)
        .base(BASE0)
        .build()
        .context("publish config")?;
    let cfg = ConfigBuilder::default()
        .record([(SHARD.into(), record)])
        .publish(publish)
        .archive_directory(PATH0)
        .build()
        .context("build config")?;
    let (pub_tx, pub_rx) = oneshot::channel();
    let (fin_tx, fin_rx) = oneshot::channel();
    let publish = || {
        let publisher = ctx.publisher.clone();
        let ids = [
            publisher.publish(Path::from(D0), Value::Null)?,
            publisher.publish(Path::from(D1), Value::Null)?,
        ];
        Ok::<_, anyhow::Error>(async move {
            eprintln!("starting publisher task");
            publisher.flushed().await;
            eprintln!("paths published");
            let _ = pub_tx.send(());
            eprintln!("publisher waiting for clients");
            future::join_all(ids.iter().map(|pb| publisher.wait_client(pb.id()))).await;
            eprintln!("publisher got clients");
            for i in 0..N {
                let mut b = publisher.start_batch();
                for id in &ids {
                    id.update(&mut b, i)
                }
                b.commit(None).await;
                time::sleep(Duration::from_millis(1)).await;
            }
            let _ = fin_tx.send(());
        })
    };
    task::spawn(publish()?);
    eprintln!("waiting for publisher to start");
    pub_rx.await.map_err(|_| anyhow!("publish task died"))?;
    eprintln!("publisher started, starting recorder");
    let recorder = Recorder::start_with(
        cfg,
        Some(ctx.publisher.clone()),
        Some(ctx.subscriber.clone()),
    )
    .await
    .context("creating recorder")?;
    eprintln!("recorder started");
    let client = Client::new(&ctx.subscriber, &Path::from(BASE0))
        .context("create record client")?;
    fin_rx.await.map_err(|_| anyhow!("publish task died"))?;
    let session = client
        .session()
        .pos(Seek::Beginning)
        .context("set pos")?
        .speed(Speed::Unlimited)
        .build()
        .await
        .context("build session")?;
    eprintln!("recorder client session started");
    let (tx_up, mut rx_up) = mpsc::channel(10);
    let tx_up = [(UpdatesFlags::empty(), tx_up)];
    let (d0, d1) = (
        ctx.subscriber.subscribe_updates(session.data_path().append(D0), tx_up.clone()),
        ctx.subscriber.subscribe_updates(session.data_path().append(D1), tx_up),
    );
    eprintln!("waiting for initial subscription");
    future::join_all([d0.wait_subscribed(), d1.wait_subscribed()]).await;
    eprintln!("initial subscription complete, pressing play");
    session.set_state(State::Play).await.context("pressing play")?;
    let mut i = 0;
    while let Some(up) = rx_up.next().await {
        if i < 2 {
            assert_eq!(up.len(), 1);
            assert_eq!(up[0].1, Event::Update(Value::Null));
        } else if i == 2 {
            assert_eq!(up.len(), 4);
            assert_eq!(up[2].0, d0.id());
            assert_eq!(up[3].0, d1.id());
            assert_eq!(up[0].1, Event::Update(Value::Null));
            assert_eq!(up[1].1, Event::Update(Value::Null));
            assert_eq!(up[2].1, Event::Update(Value::U64(0)));
            assert_eq!(up[3].1, Event::Update(Value::U64(0)));
        } else if i < 1002 {
            assert_eq!(up.len(), 2);
            assert_eq!(up[0].0, d0.id());
            assert_eq!(up[1].0, d1.id());
            assert_eq!(up[0].1, Event::Update(Value::U64(i - 2)));
            assert_eq!(up[1].1, Event::Update(Value::U64(i - 2)));
        } else if i == 1002 {
            assert_eq!(up.len(), 2);
            assert_eq!(up[0].1, Event::Update(Value::Null));
            assert_eq!(up[1].1, Event::Update(Value::Null));
            break;
        } else {
            panic!("unexpected number of batches")
        }
        i += 1;
    }
    drop(recorder);
    fs::remove_dir_all(PATH0)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_recorder() -> Result<()> {
    let _ = env_logger::try_init();
    let _ = fs::remove_dir_all(PATH1);
    eprintln!("creating context");
    let ctx = Ctx::new().await.context("build creating context")?;
    eprintln!("context created");
    let record = RecordConfigBuilder::default()
        .spec(GlobSet::new(true, [])?)
        .build()
        .context("build record config")?;
    let publish = PublishConfigBuilder::default()
        .bind("local".parse()?)
        .base(BASE1)
        .build()
        .context("publish config")?;
    let cfg = ConfigBuilder::default()
        .record([(SHARD.into(), record)])
        .publish(publish)
        .archive_directory(PATH1)
        .build()
        .context("build config")?;
    eprintln!("starting recorder");
    let recorder = Recorder::start_with(
        cfg,
        Some(ctx.publisher.clone()),
        Some(ctx.subscriber.clone()),
    )
    .await
    .context("start recorder")?;
    eprintln!("recorder started");
    let mut archive = recorder
        .shards
        .writers
        .lock()
        .remove(&recorder.shards.by_name[SHARD])
        .ok_or_else(|| anyhow!("missing writer for shard {SHARD}"))?;
    eprintln!("got record shard");
    let (fin_tx, fin_rx) = oneshot::channel();
    task::spawn(async move {
        eprintln!("record task started");
        let paths = [Path::from(D0), Path::from(D1)];
        archive.add_paths(paths.iter()).unwrap();
        let ids = [
            archive.id_for_path(&paths[0]).unwrap(),
            archive.id_for_path(&paths[1]).unwrap(),
        ];
        let mut batch = BATCH_POOL.take();
        for i in 0..N {
            batch.clear();
            for id in &ids {
                batch.push(BatchItem(*id, Event::Update(Value::U64(i))));
            }
            archive.add_batch(false, Utc::now(), &batch).unwrap();
            task::block_in_place(|| archive.flush_all().unwrap());
        }
        eprintln!("record task finished");
        let _ = fin_tx.send(());
    });
    eprintln!("connecting to the recorder");
    let client = Client::new(&ctx.subscriber, &Path::from(BASE1))
        .context("create record client")?;
    eprintln!("connected; waiting for write task to finish");
    fin_rx.await.map_err(|_| anyhow!("publish task died"))?;
    eprintln!("starting session");
    let session = client
        .session()
        .pos(Seek::Beginning)
        .context("set pos")?
        .speed(Speed::Unlimited)
        .build()
        .await
        .context("build session")?;
    eprintln!("recorder client session started");
    let (tx_up, mut rx_up) = mpsc::channel(10);
    let tx_up = [(UpdatesFlags::empty(), tx_up)];
    let (d0, d1) = (
        ctx.subscriber.subscribe_updates(session.data_path().append(D0), tx_up.clone()),
        ctx.subscriber.subscribe_updates(session.data_path().append(D1), tx_up),
    );
    eprintln!("waiting for initial subscription");
    future::join_all([d0.wait_subscribed(), d1.wait_subscribed()]).await;
    eprintln!("initial subscription complete, pressing play");
    session.set_state(State::Play).await.context("pressing play")?;
    let mut i = 0;
    while let Some(up) = rx_up.next().await {
        eprintln!("batch {i}: {up:?}");
        if i < 2 {
            assert_eq!(up.len(), 1);
            assert_eq!(up[0].1, Event::Update(Value::Null));
        } else if i < 1002 {
            assert_eq!(up.len(), 2);
            assert_eq!(up[0].0, d0.id());
            assert_eq!(up[1].0, d1.id());
            assert_eq!(up[0].1, Event::Update(Value::U64(i - 2)));
            assert_eq!(up[1].1, Event::Update(Value::U64(i - 2)));
        }
        if i == 1001 {
            break;
        }
        i += 1;
    }
    drop(recorder);
    fs::remove_dir_all(PATH1)?;
    Ok(())
}
