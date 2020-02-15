use json_pubsub::{
    path::Path,
    subscriber::Subscriber,
    utils::{Batched, BatchItem, BytesDeque, BytesWriter},
    config,
};
use futures::{
    prelude::*,
    select,
};
use tokio::{
    task, time, runtime::Runtime,
    io::{self, BufReader, AsyncWriteExt, AsyncBufReadExt},
    sync::{oneshot, mpsc},
};
use std::{
    mem,
    collections::{HashMap, HashSet},
    str::{FromStr, from_utf8},
    result::Result,
    cell::RefCell,
    time::Duration,
};
use bytes::{BytesMut, Bytes};

fn mpv_to_json(v: rmpv::Value) -> serde_json::Value {
    use rmpv::{Value as Rv, Utf8String};
    use serde_json::{Value as Jv, Number, Map};
    use std::num::FpCategory;
    let cvt_str = |s: Utf8String| -> String {
        String::from_utf8_lossy(s.as_bytes()).into()
    };
    let cvt_float = |f: f64| -> serde_json::Value {
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

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

fn json_encode(v: &serde_json::Value) -> Result<Bytes, serde_json::Error> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        serde_json::to_writer(&mut BytesWriter(&mut *b), &v)?;
        Ok(b.split().freeze())
    })
}

pub fn str_encode(t: &str) -> Bytes {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        b.extend_from_slice(t.as_bytes());
        b.split().freeze()
    })
}

macro_rules! ret {
    ($e:expr) => {
        match $e {
            Ok(r) => r,
            Err(_) => return,
        }
    };
}

async fn run_subscription(
    path: Path,
    subscriber: Subscriber,
    stop: oneshot::Receiver<()>,
    mut out: mpsc::Sender<(Bytes, Bytes)>,
) {
    let path_b = str_encode(path.replace('|', r"\|").as_str());
    let mut stop = stop.fuse();
    loop {
        let sub = {
            let mut tries: u64 = 0;
            loop {
                select! {
                    _ = stop => return,
                    r = subscriber.subscribe_one_ut(path.clone()).fuse() => match r {
                        Ok(sub) => break sub,
                        Err(e) => {
                            let m = str_encode(&format!("{}", e));
                            ret!(out.send((path_b.clone(), m)).await);
                            tries += 1;
                            time::delay_for(Duration::from_secs(tries)).await;
                        }
                    }
                }
            }
        };
        let mut updates = sub.updates(true).fuse();
        loop {
            select! {
                _ = stop => return,
                m = updates.next() => match m {
                    None => break,
                    Some(m) => {
                        let v = match rmpv::decode::value::read_value(&mut &*m) {
                            Ok(v) => try_brk!("json", json_encode(&mpv_to_json(v))),
                            Err(_) => match from_utf8(&*m) {
                                Ok(_) => m,
                                Err(_) => str_encode(format!("{:?}", m).as_str()),
                            }
                        };
                        ret!(out.send((path_b.clone(), v)).await);
                    }
                }
            }
        }
    }
}

async fn output_vals(msgs: mpsc::Receiver<(Bytes, Bytes)>) {
    let fsep = str_encode("|");
    let rsep = str_encode("\n");
    let mut msgs = Batched::new(msgs, 10000);
    let mut stdout = io::stdout();
    let mut buf = BytesDeque::new();
    'main: while let Some(m) = msgs.next().await {
        match m {
            BatchItem::InBatch((path, v)) => {
                buf.push_back(path);
                buf.push_back(fsep.clone());
                buf.push_back(v);
                buf.push_back(rsep.clone());
            },
            BatchItem::EndBatch => while buf.len() > 0 {
                match stdout.write_buf(&mut buf).await {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("error writing to stdout: {}", e);
                        break 'main
                    }
                }
            }
        }
    }
}

enum Req {
    Add(Path),
    Drop(Path),
}

impl FromStr for Req {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("drop: ") && s.len() > 6 {
            Ok(Req::Drop(Path::from(&s[6..])))
        } else {
            let p = Path::from(s);
            if !p.is_absolute() {
                Err(format!("path is not absolute {}", p))
            } else {
                Ok(Req::Add(p))
            }
        }
    }
}

pub(crate) fn run(cfg: config::Resolver, paths: Vec<String>) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let mut subscriptions: HashMap::<Path, oneshot::Sender<()>> = HashMap::new();
        let subscriber = Subscriber::new(cfg).expect("create subscriber");
        let (out_tx, out_rx) = mpsc::channel(100);
        task::spawn(output_vals(out_rx));
        let stdin = BufReader::new(io::stdin()).lines();
        let mut requests = Batched::new(
            stream::iter(paths).map(|p| Ok(p)).chain(stdin), 1000
        );
        let mut add = HashSet::new();
        let mut drop = HashSet::new();
        while let Some(l) = requests.next().await {
            match l {
                BatchItem::InBatch(l) => match try_brk!("reading", l).parse::<Req>() {
                    Err(e) => eprintln!("{}", e),
                    Ok(Req::Add(p)) => {
                        if !drop.remove(&p) {
                            add.insert(p);
                        }
                    }
                    Ok(Req::Drop(p)) => {
                        if !add.remove(&p) {
                            drop.insert(p);
                        }
                    }
                }
                BatchItem::EndBatch => {
                    for p in drop.drain() {
                        subscriptions.remove(&p).into_iter().for_each(|s| {
                            let _ = s.send(());
                        })
                    }
                    for p in add.drain() {
                        let (tx, rx) = oneshot::channel();
                        subscriptions.insert(p.clone(), tx);
                        let subscriber = subscriber.clone();
                        task::spawn(run_subscription(p, subscriber, rx, out_tx.clone()));
                    }
                }
            }
        }
        // run until we are killed even if stdin closes
        future::pending::<()>().await;
        mem::drop(subscriber);
    });
}
