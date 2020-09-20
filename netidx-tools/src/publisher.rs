use anyhow::anyhow;
use futures::prelude::*;
use netidx::{
    config::Config,
    path::Path,
    publisher::{BindCfg, Typ, Publisher, Val},
    resolver::Auth,
    utils::{self, BatchItem, Batched},
};
use std::{
    collections::HashMap,
    convert::From,
    time::Duration,
};
use tokio::{
    io::{stdin, AsyncBufReadExt, BufReader},
    runtime::Runtime,
};

pub(crate) fn run(config: Config, bcfg: BindCfg, timeout: Option<u64>, auth: Auth) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let timeout = timeout.map(Duration::from_secs);
        let mut published: HashMap<Path, Val> = HashMap::new();
        let publisher =
            Publisher::new(config, auth, bcfg).await.expect("creating publisher");
        let mut lines = Batched::new(BufReader::new(stdin()).lines(), 1000);
        let mut batch = Vec::new();
        while let Some(l) = lines.next().await {
            match l {
                BatchItem::InBatch(l) => {
                    batch.push(match l {
                        Err(_) => break,
                        Ok(l) => l,
                    });
                }
                BatchItem::EndBatch => {
                    for line in batch.drain(..) {
                        let mut m = utils::splitn_escaped(&*line, 3, '\\', '|');
                        let path = try_cf!(
                            "missing path",
                            continue,
                            m.next().ok_or_else(|| anyhow!("missing path"))
                        );
                        let typ = try_cf!(
                            "missing type",
                            continue,
                            m.next()
                                .ok_or_else(|| anyhow!("missing type"))
                                .and_then(|s| Ok(s.parse::<Typ>()?))
                        );
                        let val = {
                            let v = try_cf!(
                                "missing value",
                                continue,
                                m.next().ok_or_else(|| anyhow!("malformed data"))
                            );
                            try_cf!("parse val", continue, typ.parse(v))
                        };
                        match published.get(path) {
                            Some(p) => {
                                p.update(val);
                            }
                            None => {
                                let path = Path::from(String::from(path));
                                let publ = try_cf!(
                                    "failed to publish",
                                    continue,
                                    publisher.publish(path.clone(), val)
                                );
                                published.insert(path, publ);
                            }
                        }
                    }
                    try_cf!("flush failed", continue, publisher.flush(timeout).await);
                }
            }
        }
        // run until we are killed even if stdin closes or ends
        future::pending::<()>().await;
        drop(publisher);
    });
}
