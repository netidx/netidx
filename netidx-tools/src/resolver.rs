use super::ResolverCmd;
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    protocol::glob::{Glob, GlobSet},
    resolver::{Auth, ResolverRead, ResolverWrite},
};
use std::{collections::HashSet, iter, sync::Arc};
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
                let path = path.map(|p| Path::from(Arc::from(p))).unwrap_or(Path::root());
                if !Glob::is_glob(&*path) {
                    for path in resolver.list(path).await.unwrap().drain(..) {
                        println!("{}", path)
                    }
                } else {
                    let glob = Glob::new(Chars::from(String::from(&*path))).unwrap();
                    let globs = GlobSet::new(false, iter::once(glob)).unwrap();
                    let mut saw = HashSet::new();
                    for b in resolver.list_matching(&globs).await.unwrap().iter() {
                        for p in b.iter() {
                            if !saw.contains(p) {
                                saw.insert(p);
                                println!("{}", p);
                            }
                        }
                    }
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
