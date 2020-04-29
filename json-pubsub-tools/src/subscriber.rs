use crate::publisher::SValue;
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
    subscriber::{DVal, Subscriber, Value},
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

struct Ctx {
    subscriptions: HashMap<Path, DVal>,
    subscriber: Subscriber,
    requests: Box<dyn FusedStream<Item = BatchItem<Result<String, io::Error>>> + Unpin>,
    updates: Batched<SelectAll<Box<dyn Stream<Item = (Path, Value)> + Unpin>>>,
    stdout: io::Stdout,
    stderr: io::Stderr,
    to_stdout: BytesMut,
    to_stderr: BytesMut,
    add: HashSet<String>,
    drop: HashSet<String>,
}

impl Ctx {
    fn new(subscriber: Subscriber, paths: Vec<String>) -> Self {
        Ctx {
            subscriber,
            subscriptions: HashMap::new(),
            requests: {
                let stdin = BufReader::new(io::stdin()).lines();
                Box::new(Batched::new(
                    stream::iter(paths).map(|p| Ok(p)).chain(stdin),
                    1000,
                ))
            },
            updates: Batched::new(SelectAll::new(), 100_000),
            stdout: io::stdout(),
            stderr: io::stderr(),
            to_stdout: BytesMut::new(),
            to_stderr: BytesMut::new(),
            add: HashSet::new(),
            drop: HashSet::new(),
        }
    }

    fn process_request(&mut self, r: Option<BatchItem<Result<String, io::Error>>>) {
        match r {
            None | Some(BatchItem::InBatch(Err(_))) => {
                self.requests = Box::new(stream::pending());
            }
            Some(BatchItem::InBatch(Ok(l))) => match l.parse::<In>() {
                Err(e) => eprintln!("{}", e),
                Ok(In::Add(p)) => {
                    if !self.drop.remove(&p) {
                        self.add.insert(p);
                    }
                }
                Ok(In::Drop(p)) => {
                    if !self.add.remove(&p) {
                        self.drop.insert(p);
                    }
                }
            },
            Some(BatchItem::EndBatch) => {
                for p in self.drop.drain() {
                    self.subscriptions.remove(&*p);
                }
                for p in self.add.drain() {
                    let p = Path::from(p);
                    let updates = &mut self.updates;
                    let subscriptions = &mut self.subscriptions;
                    let subscriber = &self.subscriber;
                    subscriptions.entry(p.clone()).or_insert_with(|| {
                        let s = subscriber.durable_subscribe_val(p.clone());
                        updates
                            .inner_mut()
                            .push(Box::new(s.updates(true).map(move |v| (p.clone(), v))));
                        s
                    });
                }
            }
        }
    }

    async fn process_update(
        &mut self,
        u: Option<BatchItem<(Path, Value)>>,
    ) -> Result<(), anyhow::Error> {
        Ok(match u {
            None => unreachable!(),
            Some(BatchItem::EndBatch) => {
                if self.to_stdout.len() > 0 {
                    let to_write = self.to_stdout.split().freeze();
                    self.stdout.write_all(&*to_write).await?;
                }
                if self.to_stderr.len() > 0 {
                    let to_write = self.to_stderr.split().freeze();
                    self.stderr.write_all(&*to_write).await?;
                }
            }
            Some(BatchItem::InBatch((path, value))) => {
                let value = SValue::from_value(value);
                Out { path: &*path, value }
                    .write(&mut self.to_stdout, &mut self.to_stderr);
            }
        })
    }
}

async fn subscribe(cfg: Config, paths: Vec<String>, auth: Auth) {
    let subscriber = Subscriber::new(cfg, auth).expect("create subscriber");
    let mut ctx = Ctx::new(subscriber, paths);
    // ensure that we never see None from updates.next()
    ctx.updates.inner_mut().push(Box::new(stream::pending()));
    loop {
        select_biased! {
            u = ctx.updates.next() => {
                match ctx.process_update(u).await {
                    Ok(()) => (),
                    Err(_) => break,
                }
            },
            r = ctx.requests.next() => ctx.process_request(r)
        }
    }
}

pub(crate) fn run(cfg: Config, paths: Vec<String>, auth: Auth) {
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(subscribe(cfg, paths, auth));
}
