use netidx_archive::recorder::{Config, Recorder};
use std::path::PathBuf;
use tokio::runtime::Runtime;

pub fn run(config: PathBuf, example: bool) {
    if example {
        println!("{}", Config::example());
    } else {
        Runtime::new().expect("failed to create runtime").block_on(async move {
            let config = Config::load(&config).await.expect("failed to read config file");
            Recorder::start(config).await.expect("recorder failed");
        })
    }
}
