use std::path::PathBuf;
use futures::{select_biased, prelude::*};
use netidx_archive::recorder::{Config, Recorder};
use tokio::{runtime::Runtime, signal::ctrl_c};

pub fn run(config: PathBuf, example: bool) {
    if example {
        println!("{}", Config::example());
    } else {
        Runtime::new().expect("failed to create runtime").block_on(async move {
            let config = Config::load(&config).await.expect("failed to read config file");
            let recorder = Recorder::start(config);
            loop {
                select_biased! {
                    _ = recorder.wait_shutdown().fuse() => break,
                    r = ctrl_c().fuse() => if let Ok(()) = r {
                        recorder.shutdown()
                    },
                }
            }
        })
    }
}
