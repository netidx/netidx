use super::*;
use netidx::{
    config::Config as ClientConfig,
    path::Path,
    protocol::resolver::PublisherPriority,
    publisher::Publisher,
    resolver_client::DesiredAuth,
    resolver_server::{Server, config::Config as ServerConfig},
    subscriber::{Subscriber, Val, Value},
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
        cfg.detach();
        let publisher = Publisher::new(
            cfg.clone(),
            DesiredAuth::Anonymous,
            "127.0.0.1/32".parse().unwrap(),
            PublisherPriority::Normal,
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
    let con = listener.accept().await.unwrap();
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
    for _ in 0..100 {
        #[rustfmt::skip]
        select! {
            () = futures::future::ready(()) => println!("cancel!"), // ensure a lot of cancels happen
            r = listener.accept() => {
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
    let con = listener.accept().await.unwrap();
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
    let _con = listener.accept().await.unwrap();
    time::timeout(Duration::from_secs(10), rx).await.unwrap().unwrap()
}

const TO: Duration = Duration::from_secs(10);

/// A server connection and the client connected to it.
async fn pair(ctx: &Ctx) -> (server::Connection, client::Connection) {
    let mut acceptor =
        server::singleton(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    let (subscriber, path) = (ctx.subscriber.clone(), ctx.base.clone());
    let client =
        task::spawn(async move { client::Connection::connect(&subscriber, path).await });
    let server = acceptor.wait_connected().await.unwrap();
    (server, client.await.unwrap().unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_reads_what_arrived_before_the_connection_died() {
    let ctx = Ctx::new().await;
    let (server, client) = pair(&ctx).await;
    for i in 0..3u64 {
        server.send_one(Value::U64(i)).await.unwrap();
    }
    ctx.publisher.clone().shutdown().await;
    // the connection is gone, so a write to it fails and marks it dead
    time::timeout(TO, async {
        while client.send(Value::Null).is_ok() && client.flush().await.is_ok() {
            time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    assert!(client.is_dead());
    for i in 0..3u64 {
        assert_eq!(
            time::timeout(TO, client.recv_one()).await.unwrap().unwrap(),
            Value::U64(i)
        );
    }
    assert!(time::timeout(TO, client.recv_one()).await.unwrap().is_err());
    drop(server)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_reads_what_the_client_sent_before_it_left() {
    let ctx = Ctx::new().await;
    let (server, client) = pair(&ctx).await;
    for i in 0..3u64 {
        client.send(Value::U64(i)).unwrap();
    }
    client.flush().await.unwrap();
    drop(client);
    time::timeout(TO, async {
        while !server.is_dead() {
            time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    for i in 0..3u64 {
        assert_eq!(
            time::timeout(TO, server.recv_one()).await.unwrap().unwrap(),
            Value::U64(i)
        );
    }
    assert!(time::timeout(TO, server.recv_one()).await.unwrap().is_err());
}

/// The client's last messages and its going away come in one batch.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_reads_what_the_client_sent_as_it_left() {
    let ctx = Ctx::new().await;
    let (server, client) = pair(&ctx).await;
    for i in 0..3u64 {
        client.send(Value::U64(i)).unwrap();
    }
    drop(client);
    for i in 0..3u64 {
        assert_eq!(
            time::timeout(TO, server.recv_one()).await.unwrap().unwrap(),
            Value::U64(i)
        );
    }
    assert!(time::timeout(TO, server.recv_one()).await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_waiting_to_read_learns_the_client_left() {
    let ctx = Ctx::new().await;
    let (server, client) = pair(&ctx).await;
    let waiting = task::spawn(async move { server.recv_one().await.is_err() });
    time::sleep(Duration::from_millis(100)).await;
    drop(client);
    assert!(time::timeout(TO, waiting).await.expect("recv_one never returned").unwrap());
}

/// Ask the listener at `base` for a session, as a client would, and return
/// its path with the subscription the request went through.
async fn ask_for_session(ctx: &Ctx) -> (Val, Path) {
    let listener = ctx
        .subscriber
        .subscribe_nondurable_one(ctx.base.clone(), Some(TO))
        .await
        .unwrap();
    let session = listener.write_with_recipt(Value::from("connect"));
    let session = time::timeout(TO, session).await.unwrap().unwrap();
    (listener, session.cast_to::<Path>().unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_never_comes_to_its_session_holds_up_no_other() {
    let ctx = Ctx::new().await;
    let mut listener =
        server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    let _asked = ask_for_session(&ctx).await;
    let (subscriber, base) = (ctx.subscriber.clone(), ctx.base.clone());
    let client =
        task::spawn(async move { client::Connection::connect(&subscriber, base).await });
    let server = time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("the listener is held up")
        .unwrap();
    let client = client.await.unwrap().unwrap();
    client.send(Value::U64(42)).unwrap();
    assert_eq!(
        time::timeout(TO, server.recv_one()).await.unwrap().unwrap(),
        Value::U64(42)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_handshake_is_not_an_accept_error() {
    let ctx = Ctx::new().await;
    let mut listener =
        server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    let (_asked, session) = ask_for_session(&ctx).await;
    let bad = ctx.subscriber.subscribe_nondurable_one(session, Some(TO)).await.unwrap();
    bad.write(Value::from("not ready"));
    let (subscriber, base) = (ctx.subscriber.clone(), ctx.base.clone());
    let client =
        task::spawn(async move { client::Connection::connect(&subscriber, base).await });
    let server = time::timeout(TO, listener.accept()).await.unwrap().unwrap();
    let client = client.await.unwrap().unwrap();
    client.send(Value::U64(42)).unwrap();
    assert_eq!(
        time::timeout(TO, server.recv_one()).await.unwrap().unwrap(),
        Value::U64(42)
    );
    drop(bad)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_waiting_to_read_learns_its_publisher_shut_down() {
    let ctx = Ctx::new().await;
    let (server, _client) = pair(&ctx).await;
    let waiting = task::spawn(async move { server.recv_one().await.is_err() });
    time::sleep(Duration::from_millis(100)).await;
    ctx.publisher.clone().shutdown().await;
    assert!(time::timeout(TO, waiting).await.expect("recv_one never returned").unwrap());
}
