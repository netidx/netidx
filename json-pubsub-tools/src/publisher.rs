use json_pubsub::{
    utils::{self, Batched, BatchItem},
    path::Path,
    publisher::{BindCfg, Publisher},
};
use futures::future;
use async_std::{
    prelude::*,
    task,
    io::{stdin, BufReader},
}
use super::ResolverConfig;
use std::collections::HashMap;
use failure::Error;

fn jsonv_to_rmpv(v: serde_json::Value) -> rmpv::Value {
    use serde_json::Value as Jv;
    use rmpv::{Value as Rv, Integer, Utf8String};
    match v {
        Jv::Null => Rv::Nil,
        Jv::Bool(b) => Rv::Bool(b),
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
            } if let Some(f) = n.as_f64() {
                Rv::F64(f)
            } else {
                unreachable!("invalid number");
            }
        }
    }
}

fn from_json(s: &str) -> Result<Bytes, Error> {
    utils::rmpv_encode(&jsonv_to_rmpv(serde_json::from_str(s)?))
}

pub(crate) fn run(config: ResolverConfig, json: bool, path: Path) {
    task::block_on(async {
        let mut published = HashMap::new();
        let publisher = Publisher::new(config.bind, BindCfg::Any).await
            .expect("creating publisher");
        let mut lines = Batched::new(BufReader::new(stdin()).lines(), 1000);
        let mut batch = Vec::new();
        while let Some(l) = lines.next().await {
            match l {
                BatchItem::InBatch(l) => { batch.push(l); },
                BatchItem::EndBatch => {
                    for line in batch.drain(..) {
                        let mut m = utils::splitn_escaped(&*line, 2, '\\', '|');
                        let path = try_cont!("missing path", m.next().ok_or(Err(())));
                        let val = {
                            let v = try_cont!("missing value", m.next().ok_or(Err(())));
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
                                    publisher.publish_raw(p.key().clone(), val)
                                );
                                published.insert(path, publ);
                            }
                        }
                    }
                    try_cont!("flush failed", publisher.flush(None).await);
                }
            }
        }
        // run until we are killed even if stdin closes or ends
        future::pending::<()>().await;
        drop(publisher);
    });
}
