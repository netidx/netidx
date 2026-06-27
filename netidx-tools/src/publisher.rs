use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use clap::Args;
use futures::{
    channel::mpsc::{self, Receiver},
    prelude::*,
};
use log::{error, warn};
use netidx::{
    config::Config,
    path::Path,
    publisher::{
        BindCfg, DesiredAuth, Id, Publisher, PublisherBuilder, Typ, Val, Value,
        WriteRequest,
    },
};
use nohash::IntMap;
use parking_lot::Mutex;
use poolshark::global::GPooled;
use std::{convert::From, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin, stdout},
    task,
};

#[derive(Args, Debug)]
pub(crate) struct Params {
    /// configure the bind address e.g. local, 192.168.0.0/16
    #[arg(short, long)]
    pub(crate) bind: Option<BindCfg>,
    /// require subscribers to consume values before timeout (seconds)
    #[arg(long)]
    pub(crate) timeout: Option<u64>,
}

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

type ById = Arc<Mutex<IntMap<Id, Arc<Val>>>>;

async fn handle_writes_loop(
    by_id: ById,
    publisher: Publisher,
    mut rx: Receiver<GPooled<Vec<WriteRequest>>>,
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
                    if let Some(path) = publisher.path(val.id()) {
                        write!(buf, "{}|{}\n", path, &req.value)?;
                    }
                }
            }
        }
        stdout.write_all(&buf).await?;
        stdout.flush().await?
    }
    Ok(())
}

#[tokio::main]
pub(super) async fn run(config: Config, auth: DesiredAuth, params: Params) -> Result<()> {
    env_logger::init();
    let timeout = params.timeout.map(Duration::from_secs);
    let mut by_path: AHashMap<Path, Arc<Val>> = AHashMap::new();
    let by_id: ById = Arc::new(Mutex::new(IntMap::default()));
    let publisher = PublisherBuilder::new(config)
        .desired_auth(auth)
        .bind_cfg(params.bind)
        .build()
        .await
        .context("creating publisher")?;
    let (writes_tx, writes_rx) = mpsc::channel(100);
    let mut buf = String::new();
    let mut stdin = BufReader::new(stdin());
    fn publish(
        by_path: &mut AHashMap<Path, Arc<Val>>,
        by_id: &ById,
        publisher: &Publisher,
        path: &str,
        value: Value,
    ) -> Result<Arc<Val>> {
        let path = Path::from(String::from(path));
        let val = Arc::new(publisher.publish(path.clone(), value)?);
        by_path.insert(path, val.clone());
        let id = val.id();
        by_id.lock().insert(id, val.clone());
        Ok(val)
    }
    let _publisher = publisher.clone();
    task::spawn({
        let by_id = by_id.clone();
        async move {
            let r = handle_writes_loop(by_id, _publisher, writes_rx).await;
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
                    publisher.writes(val.id(), writes_tx.clone());
                }
                None => {
                    let val = tryc!(
                        "failed to publish",
                        publish(&mut by_path, &by_id, &publisher, path, Value::Null)
                    );
                    publisher.writes(val.id(), writes_tx.clone());
                }
            }
        } else {
            let mut m = escaping::splitn(buf.as_str().trim(), '\\', 3, '|');
            let path =
                tryc!("missing path", m.next().ok_or_else(|| anyhow!("missing path")));
            let typ = {
                let v = tryc!(
                    "missing type",
                    m.next().ok_or_else(|| anyhow!("malformed line"))
                );
                tryc!("parse type", v.parse::<Typ>())
            };
            let val = {
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
    // run until we are asked to stop even if stdin closes or ends
    netidx_activation::shutdown::wait().await;
    publisher.shutdown().await;
    Ok(())
}
