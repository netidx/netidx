use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    protocol::glob::{Glob, GlobSet},
    resolver_client::{DesiredAuth, ChangeTracker, ResolverRead, ResolverWrite},
};
use std::{collections::HashSet, iter, time::Duration, net::SocketAddr};
use tokio::{runtime::Runtime, time};
use arcstr::ArcStr;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub(super) enum ResolverCmd {
    #[structopt(name = "resolve", about = "resolve an in the resolver server")]
    Resolve { path: Vec<Path> },
    #[structopt(name = "list", about = "list entries in the resolver server")]
    List {
        #[structopt(
            long = "no-structure",
            short = "n",
            help = "don't list structural items, only published paths"
        )]
        no_structure: bool,
        #[structopt(
            long = "watch",
            short = "w",
            help = "poll the resolver for new paths matching the specified pattern"
        )]
        watch: bool,
        #[structopt(name = "pattern")]
        path: Option<String>,
    },
    #[structopt(name = "table", about = "table descriptor for path")]
    Table {
        #[structopt(name = "path")]
        path: Option<Path>,
    },
    #[structopt(name = "add", about = "add a new entry")]
    Add {
        #[structopt(name = "path")]
        path: Path,
        #[structopt(name = "socketaddr")]
        socketaddr: SocketAddr,
    },
    #[structopt(name = "remove", about = "remove an entry")]
    Remove {
        #[structopt(name = "path")]
        path: Path,
        #[structopt(name = "socketaddr")]
        socketaddr: SocketAddr,
    },
}

pub(super) fn run(config: Config, auth: DesiredAuth, cmd: ResolverCmd) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        match cmd {
            ResolverCmd::Resolve { path } => {
                let resolver = ResolverRead::new(config, auth);
                let (publishers, resolved) = resolver.resolve(path).await.unwrap();
                if publishers.len() > 0 {
                    for pb in publishers.values() {
                        println!("publisher: {:?}", pb);
                    }
                    for pref in resolved[0].publishers.iter() {
                        println!("{:?}", pref.id);
                    }
                }
            }
            ResolverCmd::List { watch, no_structure, path } => {
                let resolver = ResolverRead::new(config, auth);
                let pat = {
                    let path =
                        path.map(|p| Path::from(ArcStr::from(p))).unwrap_or(Path::root());
                    if !Glob::is_glob(&*path) {
                        path.append("*")
                    } else {
                        path
                    }
                };
                let glob = Glob::new(Chars::from(String::from(&*pat))).unwrap();
                let mut ct = ChangeTracker::new(Path::from(ArcStr::from(glob.base())));
                let globs = GlobSet::new(no_structure, iter::once(glob)).unwrap();
                let mut paths = HashSet::new();
                loop {
                    if resolver.check_changed(&mut ct).await.unwrap() {
                        for b in resolver.list_matching(&globs).await.unwrap().iter() {
                            for p in b.iter() {
                                if !paths.contains(p) {
                                    paths.insert(p.clone());
                                    println!("{}", p);
                                }
                            }
                        }
                    }
                    if watch {
                        time::sleep(Duration::from_secs(5)).await
                    } else {
                        break;
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
