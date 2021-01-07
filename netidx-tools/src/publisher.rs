use anyhow::anyhow;
use futures::prelude::*;
use netidx::{
    config::Config,
    path::Path,
    publisher::{BindCfg, Publisher, Typ, Val, Value},
    resolver::Auth,
    utils,
};
use std::{collections::HashMap, convert::From, time::Duration};
use tokio::{
    io::{stdin, AsyncBufReadExt, BufReader},
    runtime::Runtime,
};

pub(crate) fn run(config: Config, bcfg: BindCfg, timeout: Option<u64>, auth: Auth) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let timeout = timeout.map(Duration::from_secs);
        let mut published: HashMap<Path, Val> = HashMap::new();
        let publisher =
            Publisher::new(config, auth, bcfg).await.expect("creating publisher");
        let mut buf = String::new();
        let mut stdin = BufReader::new(stdin());
        let res = loop {
            buf.clear();
            match stdin.read_line(&mut buf).await {
                Err(e) => break Err(anyhow::Error::from(e)),
                Ok(len) if len == 0 => break Err::<(), anyhow::Error>(anyhow!("EOF")),
                Ok(_) => (),
            }
            let mut m = utils::splitn_escaped(buf.as_str().trim(), 3, '\\', '|');
            let path = try_cf!(
                "missing path",
                continue,
                m.next().ok_or_else(|| anyhow!("missing path"))
            );
            let typ_or_null = try_cf!(
                "missing type",
                continue,
                m.next().ok_or_else(|| anyhow!("missing type"))
            );
            let val = if typ_or_null == "null" {
                Value::Null
            } else {
                let typ = try_cf!("invalid type", continue, typ_or_null.parse::<Typ>());
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
            publisher.flush(timeout).await
        };
        eprintln!("read loop exited {:?}", res);
        // run until we are killed even if stdin closes or ends
        future::pending::<()>().await;
        drop(publisher);
    });
}
