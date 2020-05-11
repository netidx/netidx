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
use std::{collections::HashMap, str::FromStr, time::Duration, convert::{From, Into}};
use tokio::{
    io::{stdin, AsyncBufReadExt, BufReader},
    runtime::Runtime,
};

#[derive(Debug, Clone, Copy)]
pub enum Typ {
    U32,
    V32,
    I32,
    Z32,
    U64,
    V64,
    I64,
    Z64,
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
            "v32" => Ok(Typ::V32),
            "i32" => Ok(Typ::I32),
            "z32" => Ok(Typ::Z32),
            "u64" => Ok(Typ::U64),
            "v64" => Ok(Typ::V64),
            "i64" => Ok(Typ::I64),
            "z64" => Ok(Typ::Z64),
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
    V32(u32),
    I32(i32),
    Z32(i32),
    U64(u64),
    V64(u64),
    I64(i64),
    Z64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl From<Value> for SValue {
    fn from(v: Value) -> Self {
        match v {
            Value::U32(n) => SValue::U32(n),
            Value::V32(n) => SValue::V32(n),
            Value::I32(n) => SValue::I32(n),
            Value::Z32(n) => SValue::Z32(n),
            Value::U64(n) => SValue::U64(n),
            Value::V64(n) => SValue::V64(n),
            Value::I64(n) => SValue::I64(n),
            Value::Z64(n) => SValue::Z64(n),
            Value::F32(n) => SValue::F32(n),
            Value::F64(n) => SValue::F64(n),
            Value::String(c) => SValue::String(String::from(c.as_ref())),
            Value::Bytes(b) => SValue::Bytes(Vec::from(&*b)),
        }
    }
}

impl Into<Value> for SValue {
    fn into(self) -> Value {
        match self {
            SValue::U32(n) => Value::U32(n),
            SValue::V32(n) => Value::V32(n),
            SValue::I32(n) => Value::I32(n),
            SValue::Z32(n) => Value::Z32(n),
            SValue::U64(n) => Value::U64(n),
            SValue::V64(n) => Value::V64(n),
            SValue::I64(n) => Value::I64(n),
            SValue::Z64(n) => Value::Z64(n),
            SValue::F32(n) => Value::F32(n),
            SValue::F64(n) => Value::F64(n),
            SValue::String(s) => Value::String(Chars::from(s)),
            SValue::Bytes(v) => Value::Bytes(Bytes::from(v)),
        }
    }
}

fn parse_val(typ: Typ, s: &str) -> Result<SValue> {
    Ok(match typ {
        Typ::U32 => SValue::U32(s.parse::<u32>()?),
        Typ::V32 => SValue::V32(s.parse::<u32>()?),
        Typ::I32 => SValue::I32(s.parse::<i32>()?),
        Typ::Z32 => SValue::Z32(s.parse::<i32>()?),
        Typ::U64 => SValue::U64(s.parse::<u64>()?),
        Typ::V64 => SValue::V64(s.parse::<u64>()?),
        Typ::I64 => SValue::I64(s.parse::<i64>()?),
        Typ::Z64 => SValue::Z64(s.parse::<i64>()?),
        Typ::F32 => SValue::F32(s.parse::<f32>()?),
        Typ::F64 => SValue::F64(s.parse::<f64>()?),
        Typ::String => SValue::String(String::from(s)),
        Typ::Json => serde_json::from_str(s)?,
    })
}

pub(crate) fn run(
    config: Config,
    bcfg: BindCfg,
    typ: Typ,
    timeout: Option<u64>,
    auth: Auth,
) {
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
                                p.update(val.into());
                            }
                            None => {
                                let path = Path::from(path);
                                let publ = try_cf!(
                                    "failed to publish",
                                    continue,
                                    publisher.publish(path.clone(), val.into())
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
