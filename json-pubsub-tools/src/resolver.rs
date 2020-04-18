use json_pubsub::{
    path::Path,
    resolver::{ResolverRead, ResolverWrite, Auth},
    config::resolver::Config,
};
use tokio::runtime::Runtime;
use super::ResolverCmd;

pub(crate) fn run(config: Config, cmd: ResolverCmd, auth: Auth) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        match cmd {
            ResolverCmd::Resolve { path } => {
                let resolver = ResolverRead::new(config, auth).unwrap();
                let resolved = resolver.resolve(vec![path]).await.unwrap();
                println!("resolver: {:?}", resolved.resolver);
                for (addr, principal) in resolved.krb5_spns.iter() {
                    println!("{}: {}", addr, principal);
                }
                for addrs in &resolved.addrs {
                    for (addr, _) in addrs {
                        println!("{}", addr);
                    }
                }
            }
            ResolverCmd::List { path } => {
                let resolver = ResolverRead::new(config, auth).unwrap();
                let path = path.unwrap_or_else(|| Path::from("/"));
                for p in resolver.list(path).await.unwrap() {
                    println!("{}", p);
                }
            },
            ResolverCmd::Add {path, socketaddr} => {
                let resolver =
                    ResolverWrite::new(config, auth, socketaddr).unwrap();
                resolver.publish(vec![path]).await.unwrap();
            },
            ResolverCmd::Remove {path, socketaddr} => {
                let resolver =
                    ResolverWrite::new(config, auth, socketaddr).unwrap();
                resolver.unpublish(vec![path]).await.unwrap();
            },
        }
    });
}
