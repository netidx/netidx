use super::ResolverCmd;
use netidx::{
    config::Config,
    path::Path,
    resolver::{Auth, ResolverRead, ResolverWrite},
};
use tokio::runtime::Runtime;

pub(crate) fn run(config: Config, cmd: ResolverCmd, auth: Auth) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        match cmd {
            ResolverCmd::Resolve { path } => {
                let resolver = ResolverRead::new(config, auth);
                let resolved = resolver.resolve(vec![path]).await.unwrap();
                println!("resolver: {:?}", resolved[0].resolver);
                for (addr, principal) in resolved[0].krb5_spns.iter() {
                    println!("{}: {}", addr, principal);
                }
                for (addr, _) in resolved[0].addrs.iter() {
                    println!("{}", addr);
                }
            }
            ResolverCmd::List { path } => {
                let resolver = ResolverRead::new(config, auth);
                let path = path.unwrap_or_else(|| Path::from("/"));
                for p in resolver.list(path).await.unwrap().iter() {
                    println!("{}", p);
                }
            }
            ResolverCmd::Table { path } => {
                let resolver = ResolverRead::new(config, auth);
                let path = path.unwrap_or_else(|| Path::from("/"));
                let desc = resolver.table(path).await.unwrap();
                println!("columns:");
                for (name, count) in desc.cols.iter() {
                    println!("{}: {}", name, count.0)
                }
                println!("rows:");
                for row in desc.rows.iter() {
                    println!("{}", row);
                }
            }
            ResolverCmd::Add { path, socketaddr } => {
                let resolver = ResolverWrite::new(config, auth, socketaddr);
                resolver.publish(vec![path]).await.unwrap();
            }
            ResolverCmd::Remove { path, socketaddr } => {
                let resolver = ResolverWrite::new(config, auth, socketaddr);
                resolver.unpublish(vec![path]).await.unwrap();
            }
        }
    });
}
