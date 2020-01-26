use json_pubsub::{
    path::Path,
    publisher::{BindCfg, Publisher},
};
use futures::{
    select,
    prelude::*
};
use tokio::{
    task, time,
    runtime::Runtime,
    sync::mpsc,
};
use std::time::Duration;
use super::ResolverConfig;

async fn run_group(
    publisher: Publisher,
    i: usize,
    n: usize,
    vsize: usize,
    tick: mpsc::UnboundedSender<()>,
) {
    let mut vals = (0..vsize).into_iter().collect::<Vec<_>>();
    let published = (i..n).into_iter().map(|i| {
        let path = Path::from(format!("/bench/{}", i));
        publisher.publish(path, &vals).expect("encode")
    }).collect::<Vec<_>>();
    publisher.flush(None).await.expect("publish");
    loop {
        publisher.wait_client(published[0].id()).await;
        for i in 0..vals.len() {
            vals[i] += 1;
        }
        for p in published.iter() {
            p.update(&vals).expect("encode");
        }
        publisher.flush(None).await.expect("flush");
        match tick.send(()) {
            Ok(()) => (),
            Err(_) => break,
        }
    }
}

pub(crate) fn run(config: ResolverConfig, nvals: usize, vsize: usize, ntasks: usize) {
    assert!(nvals > ntasks);
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let publisher = Publisher::new(config.bind, BindCfg::Any).await
            .expect("failed to create publisher");
        let (tx, rx) = mpsc::unbounded_channel();
        let per_tsk = nvals / ntasks;
        let mut i = 0;
        while i < nvals {
            task::spawn(run_group(publisher.clone(), i, i + per_tsk, vsize, tx.clone()));
            i += per_tsk;
        }
        let mut stat = time::interval(Duration::from_secs(1)).fuse();
        let mut tick = rx.fuse();
        let mut sent = 0;
        loop {
            select! {
                _ = tick.next() => { sent += per_tsk; }
                _ = stat.next() => {
                    println!("tx: {}", sent);
                    sent = 0;
                }
            }
        }
    });
}
