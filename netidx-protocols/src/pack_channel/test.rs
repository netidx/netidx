use super::*;
use crate::channel::test::Ctx;
use tokio::{runtime::Runtime, task};

#[test]
fn pack_ping_pong() {
    Runtime::new().unwrap().block_on(async move {
        let ctx = Ctx::new().await;
        let mut listener =
            server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
        task::spawn(async move {
            let con =
                client::Connection::connect(&ctx.subscriber, ctx.base).await.unwrap();
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
    })
}

#[test]
fn pack_batch_ping_pong() {
    Runtime::new().unwrap().block_on(async move {
        let ctx = Ctx::new().await;
        let mut listener =
            server::Listener::new(&ctx.publisher, None, ctx.base.clone()).await.unwrap();
        task::spawn(async move {
            let con =
                client::Connection::connect(&ctx.subscriber, ctx.base).await.unwrap();
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
    })
}
