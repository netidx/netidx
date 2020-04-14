use json_pubsub::{
    path::Path,
    subscriber::{Subscriber, DUVal},
    resolver::Auth,
    utils::{Batched, BatchItem, BytesWriter},
    config::resolver::Config,
};
use futures::{
    prelude::*,
    stream::{self, SelectAll, FusedStream},
    select,
};
use tokio::{
    runtime::Runtime,
    io::{self, BufReader, AsyncWriteExt, AsyncBufReadExt},
};
use std::{
    collections::{HashMap, HashSet},
    str::{FromStr, from_utf8},
    result::Result,
};
use bytes::{BytesMut, Bytes};
use serde_json::Value;

fn mpv_to_json(v: rmpv::Value) -> Value {
    use rmpv::{Value as Rv, Utf8String};
    use serde_json::{Value as Jv, Number, Map};
    use std::num::FpCategory;
    let cvt_str = |s: Utf8String| -> String {
        String::from_utf8_lossy(s.as_bytes()).into()
    };
    let cvt_float = |f: f64| -> Value {
        match Number::from_f64(f) {
            Some(n) => Jv::Number(n),
            None => match f64::classify(f) {
                FpCategory::Nan => Jv::String("NaN".into()),
                FpCategory::Infinite => Jv::String("Infinite".into()),
                FpCategory::Zero | FpCategory::Subnormal | FpCategory::Normal =>
                    unreachable!("float should convert"),
            }
        }
    };
    match v {
        Rv::Nil => Jv::Null,
        Rv::Boolean(b) => Jv::Bool(b),
        Rv::F32(f) => cvt_float(f as f64),
        Rv::F64(f) => cvt_float(f),
        Rv::String(s) => Jv::String(cvt_str(s)),
        Rv::Binary(b) => Jv::String(format!("{:?}", b)),
        Rv::Array(a) => Jv::Array(a.into_iter().map(mpv_to_json).collect()),
        Rv::Ext(u, b) => Jv::String(format!("extension: {} val: {:?}", u, b)),
        Rv::Integer(i) => {
            if let Some(i) = i.as_i64() {
                Jv::Number(Number::from(i))
            } else if let Some(u) = i.as_u64() {
                Jv::Number(Number::from(u))
            } else if let Some(f) = i.as_f64() {
                cvt_float(f)
            } else {
                unreachable!("invalid number")
            }
        }
        Rv::Map(vals) => {
            let mut m = Map::new();
            for (k, v) in vals {
                match k {
                    Rv::String(s) => {
                        m.insert(cvt_str(s), mpv_to_json(v));
                    },
                    _ => {
                        let k = serde_json::to_string(&mpv_to_json(k)).unwrap();
                        m.insert(k, mpv_to_json(v));
                    }
                }
            }
            Jv::Object(m)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum In {
    Add(Path),
    Drop(Path),
}

impl FromStr for In {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(i) = serde_json::from_str(s) {
            Ok(i)
        } else if s.starts_with("DROP|") && s.len() > 5 {
            Ok(In::Drop(Path::from(&s[5..])))
        } else if s.starts_with("ADD|") && s.len() > 4 {
            Ok(In::Add(Path::from(&s[4..])))
        } else {
            let p = Path::from(s);
            if !p.is_absolute() {
                Err(format!("path is not absolute {}", p))
            } else {
                Ok(In::Add(p))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Out {
    path: Path,
    value: Value,
}

impl Out {
    fn write(&self, to_stdout: &mut BytesMut, to_stderr: &mut BytesMut) {
        match serde_json::to_writer(&mut BytesWriter(&mut *to_stdout), self) {
            Ok(()) => { to_stdout.extend_from_slice(b"\n") },
            Err(e) => {
                to_stderr.extend_from_slice(format!("{}|{}\n", self.path, e).as_ref());
            }
        }
    }
}

pub(crate) fn run(cfg: Config, paths: Vec<String>, auth: Auth) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let mut subscriptions: HashMap::<Path, DUVal> = HashMap::new();
        let subscriber = Subscriber::new(cfg, auth).expect("create subscriber");
        let mut requests:
        Box<dyn FusedStream<Item = BatchItem<Result<String, io::Error>>> + Unpin> = {
            let stdin = BufReader::new(io::stdin()).lines();
            Box::new(Batched::new(
                stream::iter(paths).map(|p| Ok(p)).chain(stdin), 1000
            ))
        };
        let mut updates:
        Batched<SelectAll<Box<dyn Stream<Item = (Path, Bytes)> + Unpin>>> =
            Batched::new(SelectAll::new(), 100_000);
        let mut stdout = io::stdout();
        let mut stderr = io::stderr();
        let mut to_stdout = BytesMut::new();
        let mut to_stderr = BytesMut::new();
        let mut add = HashSet::new();
        let mut drop = HashSet::new();
        // ensure that we never see None from updates.next()
        updates.inner_mut().push(Box::new(stream::pending()));
        loop {
            select! {
                u = updates.next() => match u {
                    None => unreachable!(),
                    Some(BatchItem::EndBatch) => {
                        if to_stdout.len() > 0 {
                            let to_write = to_stdout.split().freeze();
                            try_brk!("write stdout", stdout.write_all(&*to_write).await);
                        }
                        if to_stderr.len() > 0 {
                            let to_write = to_stderr.split().freeze();
                            try_brk!("write stderr", stderr.write_all(&*to_write).await);
                        }
                    }
                    Some(BatchItem::InBatch((path, v))) => {
                        let value = match rmpv::decode::value::read_value(&mut &*v) {
                            Ok(v) => mpv_to_json(v),
                            Err(_) => match from_utf8(&*v) {
                                Ok(s) => Value::String(s.into()),
                                Err(_) => Value::String(format!("{:?}", v)),
                            }
                        };
                        Out {path, value}.write(&mut to_stdout, &mut to_stderr)
                    }
                },
                r = requests.next() => match r {
                    None | Some(BatchItem::InBatch(Err(_))) => {
                        requests = Box::new(stream::pending());
                    }
                    Some(BatchItem::InBatch(Ok(l))) => match l.parse::<In>() {
                        Err(e) => eprintln!("{}", e),
                        Ok(In::Add(p)) => {
                            if !drop.remove(&p) {
                                add.insert(p);
                            }
                        }
                        Ok(In::Drop(p)) => {
                            if !add.remove(&p) {
                                drop.insert(p);
                            }
                        }
                    }
                    Some(BatchItem::EndBatch) => {
                        for p in drop.drain() { subscriptions.remove(&p); }
                        for p in add.drain() {
                            subscriptions.entry(p.clone()).or_insert_with(|| {
                                let s = subscriber.durable_subscribe_val_ut(p.clone());
                                updates.inner_mut().push(Box::new(
                                    s.updates(true).map(move |v| (p.clone(), v))
                                ));
                                s
                            });
                        }
                    }
                }
            }
        }
    });
}
