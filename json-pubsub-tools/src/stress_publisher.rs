use json_pubsub::{
    path::Path,
    publisher::{BindCfg, Publisher},
    resolver::Auth,
    config::resolver::Config
};
use tokio::{
    runtime::Runtime,
    signal, time,
};
use futures::{prelude::*, select};
use std::{
    time::{Duration, Instant},
    ops::{Deref, DerefMut},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Data(pub(crate) Vec<usize>);

impl Deref for Data {
    type Target = Vec<usize>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Data {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

async fn run_publisher(config: Config, nvals: usize, vsize: usize, auth: Auth) {
    let publisher = Publisher::new(config, auth, BindCfg::Any).await
        .expect("failed to create publisher");
    let mut sent: usize = 0;
    let mut vals = Data((0..vsize).into_iter().collect::<Vec<_>>());
    let published = (0..nvals).into_iter().map(|i| {
        let path = Path::from(format!("/bench/{}", i));
        publisher.publish_val(path, &vals).expect("encode")
    }).collect::<Vec<_>>();
    publisher.flush(None).await.expect("publish");
    let mut last_stat = Instant::now();
    let one_second = Duration::from_secs(1);
    loop {
        select! {
            _ = publisher.wait_any_client().fuse() => (),
            _ = signal::ctrl_c().fuse() => break,
        }
        for i in 0..vals.len() {
            vals[i] += 1;
        }
        for p in published.iter() {
            p.update(&vals).expect("encode");
            sent += 1;
        }
        publisher.flush(None).await.expect("flush");
        let now = Instant::now();
        let elapsed = now - last_stat;
        if elapsed > one_second {
            last_stat = now;
            println!("tx: {:.0}", sent as f64 / elapsed.as_secs_f64());
            sent = 0;
        }
    }
}

pub(crate) fn run(config: Config, nvals: usize, vsize: usize, auth: Auth) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        run_publisher(config, nvals, vsize, auth).await;
        // Allow the publisher time to send the clear message
        time::delay_for(Duration::from_secs(1)).await;
    });
}
