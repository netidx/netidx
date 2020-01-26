use json_pubsub::{
    path::Path,
    publisher::{BindCfg, Publisher},
};
use futures::prelude::*;
use tokio::{
    task,
    runtime::Runtime,
};
use super::ResolverConfig;

async fn run_group(publisher: Publisher, i: usize, n: usize, vsize: usize) {
    let mut vals = (0..vsize).into_iter().collect::<Vec<_>>();
    let published = (i..n).into_iter().map(|i| {
        let path = Path::from(format!("/bench/{}", i));
        publisher.publish(path, &vals).expect("encode")
    }).collect::<Vec<_>>();
    publisher.flush(None).await.expect("publish");
    loop {
        for i in 0..vals.len() {
            vals[i] += 1;
        }
        for p in published.iter() {
            p.update(&vals).expect("encode");
        }
        publisher.flush(None).await.expect("flush");
    }
}

pub(crate) fn run(config: ResolverConfig, nvals: usize, vsize: usize, ntasks: usize) {
    assert!(nvals > ntasks);
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let publisher = Publisher::new(config.bind, BindCfg::Any).await
            .expect("failed to create publisher");
        let per_tsk = nvals / ntasks;
        let mut i = 0;
        while i < nvals {
            task::spawn(run_group(publisher.clone(), i, i + per_tsk, vsize));
            i += per_tsk;
        }
        future::pending::<()>().await;
        drop(publisher);
    });
}
