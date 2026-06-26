use anyhow::{Context, Result};
use arcstr::ArcStr;
use clap::Subcommand;
use netidx::{
    config::Config,
    path::Path,
    protocol::{
        glob::{Glob, GlobSet},
        resolver::PublisherPriority,
    },
    resolver_client::{ChangeTracker, DesiredAuth, ResolverRead, ResolverWrite},
};
use std::{collections::HashSet, iter, net::SocketAddr, time::Duration};
use tokio::time;

#[derive(Subcommand, Debug)]
pub(super) enum ResolverCmd {
    /// resolve an in the resolver server
    Resolve { path: Vec<Path> },
    /// list entries in the resolver server
    List {
        /// don't list structural items, only published paths
        #[arg(short, long)]
        no_structure: bool,
        /// poll the resolver for new paths matching the specified pattern
        #[arg(short, long)]
        watch: bool,
        #[arg(value_name = "pattern")]
        path: Option<String>,
    },
    /// table descriptor for path
    Table { path: Option<Path> },
    /// add a new entry
    Add { path: Path, socketaddr: SocketAddr },
    /// remove an entry
    Remove { path: Path, socketaddr: SocketAddr },
}

#[tokio::main]
pub(super) async fn run(
    config: Config,
    auth: DesiredAuth,
    cmd: ResolverCmd,
) -> Result<()> {
    env_logger::init();
    match cmd {
        ResolverCmd::Resolve { path } => {
            let resolver = ResolverRead::new(config, auth);
            let (publishers, resolved) =
                resolver.resolve(path).await.context("resolve")?;
            if publishers.len() > 0 {
                for pb in publishers.values() {
                    println!("publisher: {:?}", pb);
                }
                for res in resolved.iter() {
                    for i in 0..res.publishers.len() {
                        if i < res.publishers.len() - 1 {
                            print!("{:?}, ", res.publishers[i].id);
                        } else {
                            print!("{:?}", res.publishers[i].id);
                        }
                    }
                    println!("");
                }
            }
        }
        ResolverCmd::List { watch, no_structure, path } => {
            let resolver = ResolverRead::new(config, auth);
            let pat = {
                let path =
                    path.map(|p| Path::from(ArcStr::from(p))).unwrap_or(Path::root());
                if !Glob::is_glob(&*path) { path.append("*") } else { path }
            };
            let glob = Glob::new(pat.into()).unwrap();
            let mut ct = ChangeTracker::new(Path::from(ArcStr::from(glob.base())));
            let globs = GlobSet::new(no_structure, iter::once(glob)).unwrap();
            let mut paths = HashSet::new();
            loop {
                if resolver.check_changed(&mut ct).await.context("check changed")? {
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
            let desc = resolver.table(path).await.context("resove table")?;
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
            let resolver =
                ResolverWrite::new(config, auth, socketaddr, PublisherPriority::Normal)
                    .context("create resolver write")?;
            resolver.publish(vec![path]).await.context("add publisher")?;
        }
        ResolverCmd::Remove { path, socketaddr } => {
            let resolver =
                ResolverWrite::new(config, auth, socketaddr, PublisherPriority::Normal)
                    .context("create resolver write")?;
            resolver.unpublish(vec![path]).await.context("remove publisher")?;
        }
    }
    Ok(())
}
