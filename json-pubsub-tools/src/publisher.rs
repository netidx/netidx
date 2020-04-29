use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::prelude::*;
use json_pubsub::{
    chars::Chars,
    config::resolver::Config,
    path::Path,
    publisher::{BindCfg, Publisher, Val, Value},
    resolver::Auth,
    utils::{self, BatchItem, Batched},
};
use std::{collections::HashMap, str::FromStr, time::Duration};
use tokio::{
    io::{stdin, AsyncBufReadExt, BufReader},
    runtime::Runtime,
};

#[derive(Debug, Clone, Copy)]
pub enum Typ {
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    String,
    Json,
}

impl FromStr for Typ {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "u32" => Ok(Typ::U32),
            "i32" => Ok(Typ::I32),
            "u64" => Ok(Typ::U64),
            "i64" => Ok(Typ::U64),
            "f32" => Ok(Typ::F32),
            "f64" => Ok(Typ::F64),
            "string" => Ok(Typ::String),
            "json" => Ok(Typ::Json),
            s => Err(anyhow!(
                "invalid type, {}, valid types: u32, i32, u64, i64, f32, f64, string, json", s))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum SValue {
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl SValue {
    pub fn to_value(self) -> Value {
        match self {
            SValue::U32(n) => Value::U32(n),
            SValue::I32(n) => Value::I32(n),
            SValue::U64(n) => Value::U64(n),
            SValue::I64(n) => Value::I64(n),
            SValue::F32(n) => Value::F32(n),
            SValue::F64(n) => Value::F64(n),
            SValue::String(s) => Value::String(Chars::from(s)),
            SValue::Bytes(v) => Value::Bytes(Bytes::from(v)),
        }
    }

    pub fn from_value(v: Value) -> Self {
        match v {
            Value::U32(n) => SValue::U32(n),
            Value::I32(n) => SValue::I32(n),
            Value::U64(n) => SValue::U64(n),
            Value::I64(n) => SValue::I64(n),
            Value::F32(n) => SValue::F32(n),
            Value::F64(n) => SValue::F64(n),
            Value::String(c) => SValue::String(String::from(c.as_ref())),
            Value::Bytes(b) => SValue::Bytes(Vec::from(&*b)),
        }
    }
}

fn parse_val(typ: Typ, s: &str) -> Result<SValue> {
    Ok(match typ {
        Typ::U32 => SValue::U32(s.parse::<u32>()?),
        Typ::I32 => SValue::I32(s.parse::<i32>()?),
        Typ::U64 => SValue::U64(s.parse::<u64>()?),
        Typ::I64 => SValue::I64(s.parse::<i64>()?),
        Typ::F32 => SValue::F32(s.parse::<f32>()?),
        Typ::F64 => SValue::F64(s.parse::<f64>()?),
        Typ::String => SValue::String(String::from(s)),
        Typ::Json => serde_json::from_str(s)?,
    })
}

pub(crate) fn run(config: Config, typ: Typ, timeout: Option<u64>, auth: Auth) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let timeout = timeout.map(Duration::from_secs);
        let mut published: HashMap<Path, Val> = HashMap::new();
        let publisher =
            Publisher::new(config, auth, BindCfg::Any).await.expect("creating publisher");
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
                        let mut m = utils::splitn_escaped(&*line, 2, '\\', '|');
                        let path = try_cf!(
                            "missing path",
                            continue,
                            m.next().ok_or_else(|| anyhow!("missing path"))
                        );
                        let val = {
                            let v = try_cf!(
                                "missing value",
                                continue,
                                m.next().ok_or_else(|| anyhow!("malformed data"))
                            );
                            try_cf!("parse val", continue, parse_val(typ, v))
                        };
                        match published.get(path) {
                            Some(p) => {
                                p.update(val.to_value());
                            }
                            None => {
                                let path = Path::from(path);
                                let publ = try_cf!(
                                    "failed to publish",
                                    continue,
                                    publisher.publish(path.clone(), val.to_value())
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
