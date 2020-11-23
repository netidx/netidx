use futures::prelude::*;
use netidx::{
    config::Config,
    publisher::{BindCfg, Publisher, Val, Value},
    resolver::Auth,
    path::Path,
};
use netidx_protocols::archive::{Archive, ReadOnly, ReadWrite};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Copy)]
enum Level {
    OneValue,
    OneLevel,
    SubTree
}

#[derive(Debug, Clone)]
struct Spec {
    base: Path,
    level: Level,
}

pub(crate) fn run(
    config: Config,
    foreground: bool,
    bind: Option<BindCfg>,
    publish_base: Option<Path>,
    auth: Auth,
    archive: String,
    spec: Vec<String>,
) {
    let publish_args = match (bind, publish_base) {
        (None, None) => None,
        (Some(bind), Some(publish_base)) => Some((bind, publish_base)),
        (Some(_), None) | (None, Some(_)) => {
            panic!("you must specify bind and publish_base to publish an archive")
        }
    };
    
    let rt = Runtime::new().expect("failed to init tokio runtime");
}
