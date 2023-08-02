use super::*;
use crate::channel::test::Ctx;
use anyhow::Result;
use futures::future;
use netidx::subscriber::Value;
use tokio::{task, select};

#[tokio::test(flavor = "multi_thread")]
async fn pack_ping_pong() {
    let ctx = Ctx::new().await;
    let mut listener =
        server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    task::spawn(async move {
        let con = client::Connection::connect(&ctx.subscriber, ctx.base).await.unwrap();
        for i in 0..100u64 {
            con.send_one(&i).unwrap();
            let j: u64 = con.recv_one().await.unwrap();
            assert_eq!(j, i)
        }
    });
    let con = listener.accept().await.unwrap().wait_connected().await.unwrap();
    for _ in 0..100 {
        let i: u64 = con.recv_one().await.unwrap();
        con.send_one(&i).await.unwrap();
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
            con.send_one(&(i as u64)).unwrap();
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
		let i: u64 = con.recv_one().await.unwrap();
                con.send_one(&i).await.unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pack_batch_ping_pong() {
    let ctx = Ctx::new().await;
    let mut listener =
        server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
    task::spawn(async move {
        let con = client::Connection::connect(&ctx.subscriber, ctx.base).await.unwrap();
        for _ in 0..100 {
            let mut b = con.start_batch();
            for i in 0..100u64 {
                b.queue(&i).unwrap()
            }
            con.send(b).unwrap();
            let mut v: Vec<u64> = Vec::new();
            con.recv(|i| {
                v.push(i);
                true
            })
            .await
            .unwrap();
            let mut i = 0;
            for j in v {
                assert_eq!(j, i);
                i += 1;
            }
            assert_eq!(i, 100)
        }
    });
    let con = listener.accept().await.unwrap().wait_connected().await.unwrap();
    for _ in 0..100 {
        let mut v: Vec<u64> = Vec::new();
        con.recv(|i| {
            v.push(i);
            true
        })
        .await
        .unwrap();
        let mut b = con.start_batch();
        for i in v {
            b.queue(&i).unwrap();
        }
        con.send(b).await.unwrap();
    }
}
