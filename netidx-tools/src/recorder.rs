use futures::{prelude::*, select_biased};
use netidx_archive::recorder::{Config, Recorder};
use std::{
    fs,
    path::{self, PathBuf},
};
use tokio::{runtime::Runtime, signal::ctrl_c};

pub fn run(config: PathBuf, example: bool) {
    if example {
        println!("{}", Config::example());
    } else {
        Runtime::new().expect("failed to create runtime").block_on(async move {
            let config = Config::load(&config).await.expect("failed to read config file");
            if !path::Path::exists(&config.archive_directory) {
                fs::create_dir_all(&config.archive_directory)
                    .expect("creating archive directory");
            } else if !path::Path::is_dir(&config.archive_directory) {
                panic!("archive_directory must be a directory")
            }
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
