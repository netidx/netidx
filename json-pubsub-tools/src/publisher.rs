use bytes::Bytes;
use json_pubsub::{
    utils::{self, Batched, BatchItem},
    path::Path,
    publisher::{BindCfg, Publisher, PublishedRaw},
};
use futures::future;
use async_std::{
    prelude::*,
    task,
    io::{stdin, BufReader},
};
use super::ResolverConfig;
use std::{collections::HashMap, time::Duration};
use failure::Error;

fn jsonv_to_rmpv(v: serde_json::Value) -> rmpv::Value {
    use serde_json::Value as Jv;
    use rmpv::{Value as Rv, Integer, Utf8String};
    match v {
        Jv::Null => Rv::Nil,
        Jv::Bool(b) => Rv::Boolean(b),
        Jv::String(s) => Rv::String(Utf8String::from(s)),
        Jv::Array(a) => Rv::Array(a.into_iter().map(jsonv_to_rmpv).collect()),
        Jv::Object(m) => Rv::Map(m.into_iter().map(|(k, v)| {
            (Rv::String(Utf8String::from(k)), jsonv_to_rmpv(v))
        }).collect()),
        Jv::Number(n) => {
            if let Some(i) = n.as_i64() {
                Rv::Integer(Integer::from(i))
            } else if let Some(u) = n.as_u64() {
                Rv::Integer(Integer::from(u))
            } else if let Some(f) = n.as_f64() {
                Rv::F64(f)
            } else {
                unreachable!("invalid number")
            }
        }
    }
}

fn from_json(s: &str) -> Result<Bytes, Error> {
    utils::rmpv_encode(&jsonv_to_rmpv(serde_json::from_str(s)?))
}

fn e() -> Error { format_err!("") }

pub(crate) fn run(config: ResolverConfig, json: bool, timeout: Option<u64>) {
    task::block_on(async {
        let timeout = timeout.map(Duration::from_secs);
        let mut published: HashMap<Path, PublishedRaw> = HashMap::new();
        let publisher = Publisher::new(config.bind, BindCfg::Any).await
            .expect("creating publisher");
        let mut lines = Batched::new(BufReader::new(stdin()).lines(), 1000);
        let mut batch = Vec::new();
        while let Some(l) = lines.next().await {
            match l {
                BatchItem::InBatch(l) => { batch.push(try_brk!("reading line", l)); },
                BatchItem::EndBatch => {
                    for line in batch.drain(..) {
                        let mut m = utils::splitn_escaped(&*line, 2, '\\', '|');
                        let path = try_cont!("missing path", m.next().ok_or_else(e));
                        let val = {
                            let v = try_cont!("missing value", m.next().ok_or_else(e));
                            if json {
                                try_cont!("invalid json", from_json(v))
                            } else {
                                utils::str_encode(v)
                            }
                        };
                        match published.get(path) {
                            Some(p) => { p.update(val); },
                            None => {
                                let path = Path::from(path);
                                let publ = try_cont!(
                                    "failed to publish",
                                    publisher.publish_raw(path.clone(), val)
                                );
                                published.insert(path, publ);
                            }
                        }
                    }
                    try_cont!("flush failed", publisher.flush(timeout).await);
                }
            }
        }
        // run until we are killed even if stdin closes or ends
        future::pending::<()>().await;
        drop(publisher);
    });
}
