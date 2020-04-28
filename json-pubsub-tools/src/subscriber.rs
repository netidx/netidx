use crate::publisher::SValue;
use anyhow::Error;
use bytes::BytesMut;
use futures::{
    prelude::*,
    select_biased,
    stream::{self, FusedStream, SelectAll},
};
use json_pubsub::{
    config::resolver::Config,
    path::Path,
    resolver::Auth,
    subscriber::{DVal, Subscriber},
    utils::{BatchItem, Batched, BytesWriter},
};
use std::{
    collections::{HashMap, HashSet},
    result::Result,
    str::FromStr,
};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    runtime::Runtime,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum In {
    Add(String),
    Drop(String),
}

impl FromStr for In {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(i) = serde_json::from_str(s) {
            Ok(i)
        } else if s.starts_with("DROP|") && s.len() > 5 {
            Ok(In::Drop(String::from(&s[5..])))
        } else if s.starts_with("ADD|") && s.len() > 4 {
            Ok(In::Add(String::from(&s[4..])))
        } else {
            let p = String::from(s);
            if Path::is_absolute(&p) {
                Err(format!("path is not absolute {}", p))
            } else {
                Ok(In::Add(p))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Out<'a> {
    path: &'a str,
    value: SValue,
}

impl<'a> Out<'a> {
    fn write(&self, to_stdout: &mut BytesMut, to_stderr: &mut BytesMut) {
        match serde_json::to_writer(&mut BytesWriter(&mut *to_stdout), self) {
            Ok(()) => to_stdout.extend_from_slice(b"\n"),
            Err(e) => {
                to_stderr.extend_from_slice(format!("{}|{}\n", self.path, e).as_ref());
            }
        }
    }
}

fn process_request(
    subscriptions: &mut HashMap<Path, DVal<SValue>>,
    add: &mut HashSet<String>,
    drop: &mut HashSet<String>,
    subscriber: &Subscriber,
    updates: &mut Batched<SelectAll<Box<dyn Stream<Item = (Path, SValue)> + Unpin>>>,
    requests: &mut Box<
        dyn FusedStream<Item = BatchItem<Result<String, io::Error>>> + Unpin,
    >,
    r: Option<BatchItem<Result<String, io::Error>>>,
) {
    match r {
        None | Some(BatchItem::InBatch(Err(_))) => {
            *requests = Box::new(stream::pending());
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
        },
        Some(BatchItem::EndBatch) => {
            for p in drop.drain() {
                subscriptions.remove(&*p);
            }
            for p in add.drain() {
                let p = Path::from(p);
                subscriptions.entry(p.clone()).or_insert_with(|| {
                    let s = subscriber.durable_subscribe_val(p.clone());
                    updates.inner_mut().push(Box::new(s.updates(true).map(move |v| {
                        let v = match v {
                            Err(e) => SValue::String(format!("{}", e)),
                            Ok(v) => v,
                        };
                        (p.clone(), v)
                    })));
                    s
                });
            }
        }
    }
}

async fn process_update(
    to_stdout: &mut BytesMut,
    to_stderr: &mut BytesMut,
    stdout: &mut io::Stdout,
    stderr: &mut io::Stderr,
    u: Option<BatchItem<(Path, SValue)>>,
) -> Result<(), anyhow::Error> {
    Ok(match u {
        None => unreachable!(),
        Some(BatchItem::EndBatch) => {
            if to_stdout.len() > 0 {
                let to_write = to_stdout.split().freeze();
                stdout.write_all(&*to_write).await?;
            }
            if to_stderr.len() > 0 {
                let to_write = to_stderr.split().freeze();
                stderr.write_all(&*to_write).await?;
            }
        }
        Some(BatchItem::InBatch((path, value))) => {
            Out { path: &*path, value }.write(to_stdout, to_stderr);
        }
    })
}

async fn subscribe(cfg: Config, paths: Vec<String>, auth: Auth) {
    let mut subscriptions: HashMap<Path, DVal<SValue>> = HashMap::new();
    let subscriber = Subscriber::new(cfg, auth).expect("create subscriber");
    let mut requests: Box<
        dyn FusedStream<Item = BatchItem<Result<String, io::Error>>> + Unpin,
    > = {
        let stdin = BufReader::new(io::stdin()).lines();
        Box::new(Batched::new(stream::iter(paths).map(|p| Ok(p)).chain(stdin), 1000))
    };
    let mut updates: Batched<SelectAll<Box<dyn Stream<Item = (Path, SValue)> + Unpin>>> =
        Batched::new(SelectAll::new(), 100_000);
    let mut stdout = io::stdout();
    let mut stderr = io::stderr();
    let mut to_stdout = BytesMut::new();
    let mut to_stderr = BytesMut::new();
    let mut add: HashSet<String> = HashSet::new();
    let mut drop: HashSet<String> = HashSet::new();
    // ensure that we never see None from updates.next()
    updates.inner_mut().push(Box::new(stream::pending()));
    loop {
        select_biased! {
            u = updates.next() => {
                let r = process_update(
                    &mut to_stdout, &mut to_stderr, &mut stdout, 
                    &mut stderr, u
                ).await;
                match r {
                    Ok(()) => (),
                    Err(_) => break,
                }
            },
            r = requests.next() => process_request(
                &mut subscriptions, &mut add, &mut drop,
                &subscriber, &mut updates, &mut requests, r
            )
        }
    }
}

pub(crate) fn run(cfg: Config, paths: Vec<String>, auth: Auth) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(subscribe(cfg, paths, auth));
}
