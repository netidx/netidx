use crate::{
    channel::Channel,
    protocol::{
        publisher::{From, To, Id},
        value::Value,
    },
    test::netproto::publisher::{to, from},
};
use bytes::Bytes;
use cross_krb5::ServerCtx;
use parking_lot::Mutex;
use proptest::prelude::*;
use rand::{thread_rng, Rng};
use tokio::{net::UnixStream, runtime::Runtime};

fn generate_from_msgs() -> Vec<From> {
    let msgs = Mutex::new(Vec::new());
    proptest!(|(m in from())| {
        msgs.lock().push(m);
    });
    msgs.into_inner()
}

fn generate_to_msgs() -> Vec<To> {
    let msgs = Mutex::new(Vec::new());
    proptest!(|(m in to())| {
        msgs.lock().push(m);
    });
    msgs.into_inner()
}

async fn run_channel_packing_from() {
    let (s0, s1) = UnixStream::pair().unwrap();
    let mut in_chan = Channel::new::<ServerCtx, UnixStream>(None, s0);
    let mut out_chan = Channel::new::<ServerCtx, UnixStream>(None, s1);
    let mut in_batch = Vec::new();
    let mut out_batch = Vec::new();
    for m in generate_from_msgs() {
        in_chan.queue_send(&m).unwrap();
        in_batch.push(m);
        let zc = thread_rng().gen_range(0..100);
        if zc == 0 {
            let buf = Bytes::from("jkflkadjflkahjaheiufhaejahfkjahdkjadh");
            let id = unsafe { Id::mk(42) };
            let m = From::Update(id, Value::Bytes(buf.clone()));
            in_chan.queue_send_zero_copy_update(id, buf).unwrap();
            in_batch.push(m);
        } else if zc == 1 {
            let buf = Bytes::from("jkflkadjflkahjaheiufhaejahfkjahdkjadh");
            let id = unsafe { Id::mk(42) };
            let m = From::WriteResult(id, Value::Bytes(buf.clone()));
            in_chan.queue_send_zero_copy_write_result(id, buf).unwrap();
            in_batch.push(m);
        }
        if thread_rng().gen_range(0..1000) == 0 {
            in_chan.flush().await.unwrap();
            out_chan.receive_batch(&mut out_batch).await.unwrap();
            assert_eq!(in_batch.len(), out_batch.len());
            for (m0, m1) in in_batch.iter().zip(out_batch.iter()) {
                assert_eq!(m0, m1);
            }
            in_batch.clear();
            out_batch.clear();
        }
    }
}

async fn run_channel_packing_to() {
    let (s0, s1) = UnixStream::pair().unwrap();
    let mut in_chan = Channel::new::<ServerCtx, UnixStream>(None, s0);
    let mut out_chan = Channel::new::<ServerCtx, UnixStream>(None, s1);
    let mut in_batch = Vec::new();
    let mut out_batch = Vec::new();
    for m in generate_to_msgs() {
        in_chan.queue_send(&m).unwrap();
        in_batch.push(m);
        if thread_rng().gen_range(0..50) == 0 {
            let buf = Bytes::from("jkflkadjflkahjaheiufhaejahfkjahdkjadh");
            let id = unsafe { Id::mk(42) };
            let m = To::Write(id, false, Value::Bytes(buf.clone()));
            in_chan.queue_send_zero_copy_write(id, false, buf).unwrap();
            in_batch.push(m);
        }
        if thread_rng().gen_range(0..1000) == 0 {
            in_chan.flush().await.unwrap();
            out_chan.receive_batch(&mut out_batch).await.unwrap();
            assert_eq!(in_batch.len(), out_batch.len());
            for (m0, m1) in in_batch.iter().zip(out_batch.iter()) {
                assert_eq!(m0, m1);
            }
            in_batch.clear();
            out_batch.clear();
        }
    }
}

#[test]
fn channel_packing() {
    Runtime::new().unwrap().block_on(async {
        run_channel_packing_to().await;
        run_channel_packing_from().await;
    })
}
