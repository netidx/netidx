use crate::{channel::Channel, protocol::publisher::To, test::netproto::publisher::to};
use cross_krb5::ServerCtx;
use parking_lot::Mutex;
use proptest::prelude::*;
use rand::{thread_rng, Rng};
use tokio::{net::UnixStream, runtime::Runtime};

fn generate_msgs() -> Vec<To> {
    let msgs = Mutex::new(Vec::new());
    proptest!(|(m in to())| {
        msgs.lock().push(m);
    });
    msgs.into_inner()
}

async fn run_channel_packing() {
    let (s0, s1) = UnixStream::pair().unwrap();
    let mut in_chan = Channel::new::<ServerCtx, UnixStream>(None, s0);
    let mut out_chan = Channel::new::<ServerCtx, UnixStream>(None, s1);
    let mut in_batch = Vec::new();
    let mut out_batch = Vec::new();
    for m in generate_msgs() {
        in_chan.queue_send(&m).unwrap();
        in_batch.push(m);
        if thread_rng().gen_range(0..100) == 0 {
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
    Runtime::new().unwrap().block_on(run_channel_packing())
}
