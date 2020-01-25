use json_pubsub::{
    path::Path,
    resolver::{Resolver, ReadOnly, WriteOnly},
};
use tokio::runtime::Runtime;
use super::{ResolverConfig, ResolverCmd};

pub(crate) fn run(config: ResolverConfig, cmd: ResolverCmd) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        match cmd {
            ResolverCmd::Resolve { path } => {
                let mut resolver = Resolver::<ReadOnly>::new_r(config.bind).unwrap();
                for addrs in resolver.resolve(vec![path]).await.unwrap() {
                    for addr in addrs {
                        println!("{}", addr);
                    }
                }
            }
            ResolverCmd::List { path } => {
                let mut resolver = Resolver::<ReadOnly>::new_r(config.bind).unwrap();
                let path = path.unwrap_or_else(|| Path::from("/"));
                for p in resolver.list(path).await.unwrap() {
                    println!("{}", p);
                }
            },
            ResolverCmd::Add {path, socketaddr} => {
                let mut resolver =
                    Resolver::<WriteOnly>::new_w(config.bind, socketaddr).unwrap();
                resolver.publish(vec![path]).await.unwrap();
            },
            ResolverCmd::Remove {path, socketaddr} => {
                let mut resolver =
                    Resolver::<WriteOnly>::new_w(config.bind, socketaddr).unwrap();
                resolver.unpublish(vec![path]).await.unwrap();
            },
        }
    });
}
