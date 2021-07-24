use anyhow::{anyhow, Result};
use futures::{
    channel::mpsc::{self, Receiver},
    prelude::*,
};
use fxhash::FxBuildHasher;
use log::{error, warn};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    publisher::{BindCfg, Id, Publisher, Typ, Val, Value, WriteRequest},
    resolver::Auth,
    utils,
};
use parking_lot::Mutex;
use std::{collections::HashMap, convert::From, sync::Arc, time::Duration};
use tokio::{
    io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader},
    runtime::Runtime,
    task,
};

macro_rules! tryc {
    ($msg:expr, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                error!("{}: {}", $msg, e);
                continue;
            }
        }
    };
}

type ById = Arc<Mutex<HashMap<Id, Val, FxBuildHasher>>>;

async fn handle_writes_loop(
    by_id: ById,
    mut rx: Receiver<Pooled<Vec<WriteRequest>>>,
) -> Result<()> {
    let mut stdout = stdout();
    let mut buf = Vec::new();
    while let Some(mut batch) = rx.next().await {
        buf.clear();
        {
            let by_id = by_id.lock();
            for req in batch.drain(..) {
                if let Some(val) = by_id.get(&req.id) {
                    use std::io::Write;
                    let typ = match Typ::get(&req.value) {
                        None => "none",
                        Some(t) => t.name(),
                    };
                    write!(buf, "{}|{}|{}\n", val.path(), typ, &req.value)?;
                }
            }
        }
        stdout.write_all(&buf).await?;
        stdout.flush().await?
    }
    Ok(())
}

pub(crate) fn run(config: Config, bcfg: BindCfg, timeout: Option<u64>, auth: Auth) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let timeout = timeout.map(Duration::from_secs);
        let mut by_path: HashMap<Path, Val> = HashMap::new();
        let by_id: ById =
            Arc::new(Mutex::new(HashMap::with_hasher(FxBuildHasher::default())));
        let publisher =
            Publisher::new(config, auth, bcfg).await.expect("creating publisher");
        let (writes_tx, writes_rx) = mpsc::channel(100);
        let mut buf = String::new();
        let mut stdin = BufReader::new(stdin());
        fn publish(
            by_path: &mut HashMap<Path, Val>,
            by_id: &ById,
            publisher: &Publisher,
            path: &str,
            value: Value,
        ) -> Result<Val> {
            let path = Path::from(String::from(path));
            let val = publisher.publish(path.clone(), value)?;
            by_path.insert(path, val.clone());
            let id = val.id();
            by_id.lock().insert(id, val.clone());
            Ok(val)
        }
        task::spawn({
            let by_id = by_id.clone();
            async move {
                let r = handle_writes_loop(by_id, writes_rx).await;
                error!("writes loop terminated {:?}", r);
            }
        });
        let res = loop {
            let mut batch = publisher.start_batch();
            buf.clear();
            match stdin.read_line(&mut buf).await {
                Err(e) => break Err(anyhow::Error::from(e)),
                Ok(len) if len == 0 => break Err::<(), anyhow::Error>(anyhow!("EOF")),
                Ok(_) => (),
            }
            if buf.starts_with("DROP|") {
                let path = buf.trim_start_matches("DROP|").trim();
                if let Some(val) = by_path.remove(path) {
                    by_id.lock().remove(&val.id());
                }
            } else if buf.starts_with("WRITE|") {
                let path = buf.trim_start_matches("WRITE|").trim();
                match by_path.get(path) {
                    Some(val) => {
                        val.writes(writes_tx.clone());
                    }
                    None => {
                        let val = tryc!(
                            "failed to publish",
                            publish(&mut by_path, &by_id, &publisher, path, Value::Null)
                        );
                        val.writes(writes_tx.clone());
                    }
                }
            } else {
                let mut m = utils::splitn_escaped(buf.as_str().trim(), 3, '\\', '|');
                let path = tryc!(
                    "missing path",
                    m.next().ok_or_else(|| anyhow!("missing path"))
                );
                let typ_or_null = tryc!(
                    "missing type",
                    m.next().ok_or_else(|| anyhow!("missing type"))
                );
                let val = if typ_or_null == "null" {
                    Value::Null
                } else {
                    let typ = tryc!("invalid type", typ_or_null.parse::<Typ>());
                    let v = tryc!(
                        "missing value",
                        m.next().ok_or_else(|| anyhow!("malformed data"))
                    );
                    tryc!("parse val", typ.parse(v))
                };
                match by_path.get(path) {
                    Some(p) => {
                        p.update(&mut batch, val);
                    }
                    None => {
                        tryc!(
                            "failed to publish",
                            publish(&mut by_path, &by_id, &publisher, path, val)
                        );
                    }
                }
            }
            batch.commit(timeout).await
        };
        warn!("read loop exited {:?}, running until killed", res);
        // run until we are killed even if stdin closes or ends
        future::pending::<()>().await;
        drop(publisher);
    });
}
