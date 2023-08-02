use super::*;
use anyhow::Result;
use futures::future;
use netidx::{
    config::Config as ClientConfig,
    path::Path,
    publisher::Publisher,
    resolver_client::DesiredAuth,
    resolver_server::{config::Config as ServerConfig, Server},
    subscriber::{Subscriber, Value},
};
use std::{sync::Arc, time::Duration};
use tokio::{select, sync::oneshot, task, time};

pub(crate) struct Ctx {
    pub(crate) _server: Server,
    pub(crate) publisher: Publisher,
    pub(crate) subscriber: Subscriber,
    pub(crate) base: Path,
}

impl Ctx {
    pub(crate) async fn new() -> Self {
        let _ = env_logger::try_init();
        let cfg = ServerConfig::load("../cfg/simple-server.json")
            .expect("load simple server config");
        let _server =
            Server::new(cfg.clone(), false, 0).await.expect("start resolver server");
        let mut cfg = ClientConfig::load("../cfg/simple-client.json")
            .expect("load simple client config");
        cfg.addrs[0].0 = *_server.local_addr();
        let publisher = Publisher::new(
            cfg.clone(),
            DesiredAuth::Anonymous,
            "127.0.0.1/32".parse().unwrap(),
            768,
            3,
        )
        .await
        .unwrap();
        let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous).unwrap();
        let base = Path::from("/channel");
        Self { _server, publisher, subscriber, base }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ping_pong() {
    let ctx = Ctx::new().await;
    let mut listener =
        server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    task::spawn(async move {
        let con = client::Connection::connect(&ctx.subscriber, ctx.base).await.unwrap();
        for i in 0..100 {
            con.send(Value::U64(i as u64)).unwrap();
            match con.recv_one().await.unwrap() {
                Value::U64(j) => assert_eq!(j, i),
                _ => panic!("expected u64"),
            }
        }
    });
    let con = listener.accept().await.unwrap().wait_connected().await.unwrap();
    for _ in 0..100 {
        match con.recv_one().await.unwrap() {
            Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
            _ => panic!("expected u64"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn singleton() {
    let ctx = Ctx::new().await;
    let path = ctx.base.clone();
    let mut acceptor = server::singleton(&ctx.publisher, None, ctx.base).await.unwrap();
    task::spawn(async move {
        let con = client::Connection::connect(&ctx.subscriber, path).await.unwrap();
        for i in 0..100 {
            con.send(Value::U64(i as u64)).unwrap();
            match con.recv_one().await.unwrap() {
                Value::U64(j) => assert_eq!(j, i),
                _ => panic!("expected u64"),
            }
        }
    });
    let con = acceptor.wait_connected().await.unwrap();
    for _ in 0..100 {
        match con.recv_one().await.unwrap() {
            Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
            _ => panic!("expected u64"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn accept_cancel_safe() {
    let ctx = Ctx::new().await;
    let mut listener =
        server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    task::spawn(async move {
        for i in 0..100 {
            let con = client::Connection::connect(&ctx.subscriber, ctx.base.clone())
                .await
                .unwrap();
            con.send(Value::U64(i as u64)).unwrap();
            match con.recv_one().await.unwrap() {
                Value::U64(j) => assert_eq!(j, i),
                _ => panic!("expected u64"),
            }
        }
    });
    let mut pending: Option<server::Singleton> = None;
    async fn wait_pending(
        pending: &mut Option<server::Singleton>,
    ) -> Result<server::Connection> {
        match pending {
            Some(pending) => pending.wait_connected().await,
            None => future::pending().await,
        }
    }
    async fn wait_accept(
        pending: bool,
        listener: &mut server::Listener,
    ) -> Result<server::Singleton> {
        if pending {
            future::pending().await
        } else {
            listener.accept().await
        }
    }
    for _ in 0..100 {
        let is_pending = pending.is_some();
	#[rustfmt::skip]
        select! {
            () = futures::future::ready(()) => println!("cancel!"), // ensure a lot of cancels happen
            r = wait_accept(is_pending, &mut listener) => {
		pending = Some(r.unwrap());
            },
            r = wait_pending(&mut pending) => {
		pending = None;
		let con = r.unwrap();
		match con.recv_one().await.unwrap() {
                    Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
                    _ => panic!("expected u64"),
		}
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hang() {
    const SZ: usize = 10000;
    let ctx = Arc::new(Ctx::new().await);
    let mut listener =
        server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    let ctx_ = ctx.clone();
    task::spawn(async move {
        let con = client::Connection::connect(&ctx_.subscriber, ctx_.base.clone())
            .await
            .unwrap();
        for i in 0..SZ {
            con.send(Value::U64(i as u64)).unwrap();
        }
        con.flush().await.unwrap();
        for i in 0..SZ {
            match con.recv_one().await.unwrap() {
                Value::U64(j) => assert_eq!(j as usize, i),
                _ => panic!("expected u64"),
            }
        }
    });
    let ctx_ = ctx.clone();
    task::spawn(async move {
        let con = client::Connection::connect(&ctx_.subscriber, ctx_.base.clone())
            .await
            .unwrap();
        for i in 0..SZ {
            con.send(Value::U64(i as u64)).unwrap();
        }
        con.flush().await.unwrap();
        for i in 0..SZ {
            match con.recv_one().await.unwrap() {
                Value::U64(j) => assert_eq!(j as usize, i),
                _ => panic!("expected u64"),
            }
        }
    });
    let con = listener.accept().await.unwrap().wait_connected().await.unwrap();
    let (tx, rx) = oneshot::channel();
    task::spawn(async move {
        for _ in 0..SZ {
            match con.recv_one().await.unwrap() {
                Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
                _ => panic!("expected u64"),
            }
        }
        let _ = tx.send(());
    });
    // hang the second connection
    let _con = listener.accept().await.unwrap().wait_connected().await.unwrap();
    time::timeout(Duration::from_secs(10), rx).await.unwrap().unwrap()
}
