use super::*;
use crate::channel::test::Ctx;
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
    let con = listener.accept().await.unwrap();
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
    for _ in 0..100 {
        #[rustfmt::skip]
        select! {
            () = futures::future::ready(()) => println!("cancel!"), // ensure a lot of cancels happen
            r = listener.accept() => {
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
    let con = listener.accept().await.unwrap();
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
