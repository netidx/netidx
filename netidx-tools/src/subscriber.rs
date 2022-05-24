use anyhow::{anyhow, Error, Result};
use arcstr::ArcStr;
use bytes::BytesMut;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    prelude::*,
    select_biased,
    stream::{self, FusedStream},
};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    resolver_client::DesiredAuth,
    subscriber::{Dval, Event, SubId, Subscriber, Typ, UpdatesFlags, Value},
    utils::{split_escaped, splitn_escaped, BatchItem, Batched},
};
use netidx_protocols::rpc::client::Proc;
use std::{
    collections::HashMap,
    fmt,
    io::Write,
    str::FromStr,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader},
    runtime::Runtime,
    time,
};

#[derive(StructOpt, Debug)]
pub(super) struct Params {
    #[structopt(
        short = "o",
        long = "oneshot",
        help = "unsubscribe after printing one value for each subscription"
    )]
    oneshot: bool,
    #[structopt(short = "n", long = "no-stdin", help = "don't read commands from stdin")]
    no_stdin: bool,
    #[structopt(short = "r", long = "raw", help = "don't print the path or the type")]
    raw: bool,
    #[structopt(
        short = "t",
        long = "subscribe-timeout",
        help = "cancel subscription unless it succeeds within timeout"
    )]
    subscribe_timeout: Option<u64>,
    #[structopt(name = "paths")]
    paths: Vec<String>,
}

#[derive(Debug, Clone)]
enum In {
    Add(Path),
    Drop(Path),
    Write(Path, Value),
    Call(Path, Vec<(String, Value)>),
}

impl FromStr for In {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with("DROP|") && s.len() > 5 {
            Ok(In::Drop(Path::from(ArcStr::from(&s[5..]))))
        } else if s.starts_with("ADD|") && s.len() > 4 {
            Ok(In::Add(Path::from(ArcStr::from(&s[4..]))))
        } else if s.starts_with("WRITE|") && s.len() > 6 {
            let mut parts = splitn_escaped(&s[6..], 3, '\\', '|');
            let path = parts.next().ok_or_else(|| anyhow!("expected | before path"))?;
            let path = Path::from(ArcStr::from(path));
            let typ =
                parts.next().ok_or_else(|| anyhow!("expected type"))?.parse::<Typ>()?;
            let val = parts.next().ok_or_else(|| anyhow!("expected value"))?;
            let val = typ.parse(val)?;
            Ok(In::Write(path, val))
        } else if s.starts_with("CALL|") && s.len() > 5 {
            let mut parts = splitn_escaped(&s[5..], 2, '\\', '|');
            let path = parts.next().ok_or_else(|| anyhow!("expected| before path"))?;
            let path = Path::from(ArcStr::from(path));
            let args = match parts.next() {
                None => vec![],
                Some(s) => {
                    let mut args = vec![];
                    for arg in split_escaped(s, '\\', ',') {
                        let mut arg = splitn_escaped(arg.trim(), 2, '\\', '=');
                        let key =
                            arg.next().ok_or_else(|| anyhow!("expected keyword"))?;
                        let val = arg
                            .next()
                            .ok_or_else(|| anyhow!("expected value"))?
                            .parse::<Value>()?;
                        args.push((String::from(key), val));
                    }
                    args
                }
            };
            Ok(In::Call(path, args))
        } else {
            bail!("parse error, expected ADD, DROP, WRITE, or CALL")
        }
    }
}

pub struct BytesWriter<'a>(pub &'a mut BytesMut);

impl Write for BytesWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct WVal<'a>(&'a Value);

impl<'a> fmt::Display for WVal<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt_naked(f)
    }
}

#[derive(Debug, Clone)]
struct Out<'a> {
    raw: bool,
    path: &'a str,
    value: Event,
}

impl<'a> Out<'a> {
    fn write(&self, to_stdout: &mut BytesMut) {
        match &self.value {
            Event::Unsubscribed => {
                if !self.raw {
                    to_stdout.extend_from_slice(b"Unsubscribed");
                    to_stdout.extend_from_slice(b"|");
                    to_stdout.extend_from_slice(self.path.as_bytes());
                    to_stdout.extend_from_slice(b"\n");
                }
            }
            Event::Update(v) => {
                if self.raw {
                    let w = &mut BytesWriter(to_stdout);
                    writeln!(w, "{}", WVal(v)).unwrap()
                } else {
                    to_stdout.extend_from_slice(self.path.as_bytes());
                    to_stdout.extend_from_slice(b"|");
                    let typ = Typ::get(v);
                    let w = &mut BytesWriter(to_stdout);
                    write!(w, "{}|", typ).unwrap();
                    writeln!(w, "{}", WVal(v)).unwrap()
                }
            }
        }
    }
}

struct Ctx {
    sender_updates: Sender<Pooled<Vec<(SubId, Event)>>>,
    paths: HashMap<SubId, Path>,
    subscriptions: HashMap<Path, Dval>,
    rpcs: HashMap<Path, Proc>,
    subscribe_ts: HashMap<Path, Instant>,
    subscriber: Subscriber,
    requests: Box<dyn FusedStream<Item = Result<String>> + Unpin>,
    updates: Batched<Receiver<Pooled<Vec<(SubId, Event)>>>>,
    stdout: io::Stdout,
    stderr: io::Stderr,
    to_stdout: BytesMut,
    to_stderr: BytesMut,
    oneshot: bool,
    requests_finished: bool,
    raw: bool,
    subscribe_timeout: Option<Duration>,
}

impl Ctx {
    fn new(subscriber: Subscriber, p: Params) -> Self {
        let (sender_updates, updates) = mpsc::channel(100);
        Ctx {
            sender_updates,
            paths: HashMap::new(),
            subscriber,
            subscriptions: HashMap::new(),
            rpcs: HashMap::new(),
            subscribe_ts: HashMap::new(),
            subscribe_timeout: p.subscribe_timeout.map(Duration::from_secs),
            requests: {
                let init = stream::iter(p.paths).map(|mut p| {
                    p.insert_str(0, "ADD|");
                    Ok(p)
                });
                if p.no_stdin {
                    Box::new(init.fuse())
                } else {
                    let stdin = Box::pin({
                        let mut stdin = BufReader::new(io::stdin()).lines();
                        async_stream::stream! {
                            loop {
                                match stdin.next_line().await {
                                    Ok(None) => break,
                                    Ok(Some(line)) => yield Ok(line),
                                    Err(e) => yield Err(Error::from(e)),
                                }
                            }
                        }
                    });
                    Box::new(init.chain(stdin))
                }
            },
            updates: Batched::new(updates, 100_000),
            stdout: io::stdout(),
            stderr: io::stderr(),
            to_stdout: BytesMut::new(),
            to_stderr: BytesMut::new(),
            oneshot: p.oneshot,
            requests_finished: false,
            raw: p.raw,
        }
    }

    fn remove_subscription(&mut self, path: &str) {
        if let Some(dv) = self.subscriptions.remove(path) {
            self.subscribe_ts.remove(path);
            self.paths.remove(&dv.id());
        }
    }

    fn add_subscription(&mut self, path: &Path) -> &Dval {
        let subscriptions = &mut self.subscriptions;
        let subscribe_ts = &mut self.subscribe_ts;
        let subscribe_timeout = self.subscribe_timeout.is_some();
        let paths = &mut self.paths;
        let subscriber = &self.subscriber;
        let sender_updates = self.sender_updates.clone();
        subscriptions.entry(path.clone()).or_insert_with(|| {
            let s = subscriber.durable_subscribe(path.clone());
            paths.insert(s.id(), path.clone());
            s.updates(
                UpdatesFlags::BEGIN_WITH_LAST | UpdatesFlags::STOP_COLLECTING_LAST,
                sender_updates,
            );
            if subscribe_timeout {
                subscribe_ts.insert(path.clone(), Instant::now());
            }
            s
        })
    }

    async fn process_request(&mut self, r: Option<Result<String>>) -> Result<()> {
        match r {
            None | Some(Err(_)) => {
                // This handles the case the user did something like
                // call us with stdin redirected from a file and we
                // read EOF, or a hereis doc, or input is piped into
                // us. We don't want to die in any of these cases.
                self.requests = Box::new(stream::pending());
                self.requests_finished = true;
                if self.oneshot && self.paths.len() == 0 {
                    self.flush().await?;
                    bail!("finished")
                } else {
                    Ok(())
                }
            }
            Some(Ok(l)) => {
                if !l.trim().is_empty() {
                    match l.parse::<In>() {
                        Err(e) => eprintln!("parse error: {}", e),
                        Ok(In::Add(p)) => {
                            self.add_subscription(&p);
                        }
                        Ok(In::Drop(p)) => {
                            self.remove_subscription(&p);
                            self.rpcs.remove(&p);
                        }
                        Ok(In::Write(p, v)) => {
                            let dv = self.add_subscription(&p);
                            if !dv.write(v.into()) {
                                eprintln!(
                                    "WARNING: {} queued writes to {}",
                                    dv.queued_writes(),
                                    p
                                )
                            }
                        }
                        Ok(In::Call(p, args)) => {
                            let proc = match self.rpcs.get(&p) {
                                Some(proc) => proc,
                                None => {
                                    let proc =
                                        Proc::new(&self.subscriber, p.clone()).await;
                                    let proc = match proc {
                                        Ok(proc) => proc,
                                        Err(e) => {
                                            eprintln!("CALL error: {}", e);
                                            return Ok(());
                                        }
                                    };
                                    self.rpcs.insert(p.clone(), proc);
                                    &self.rpcs[&p]
                                }
                            };
                            println!("CALLED|{}|{:?}", p, proc.call(args).await)
                        }
                    }
                }
                Ok(())
            }
        }
    }

    async fn flush(&mut self) -> Result<()> {
        if self.to_stdout.len() > 0 {
            let to_write = self.to_stdout.split().freeze();
            self.stdout.write_all(&*to_write).await?;
        }
        if self.to_stderr.len() > 0 {
            let to_write = self.to_stderr.split().freeze();
            self.stderr.write_all(&*to_write).await?;
        }
        Ok(())
    }

    async fn check_timeouts(&mut self, timeout: Duration) -> Result<()> {
        let mut failed = Vec::new();
        for (path, started) in &self.subscribe_ts {
            if started.elapsed() > timeout {
                failed.push(path.clone())
            }
        }
        for path in failed {
            eprintln!(
                "WARNING: subscription to {} did not succeed before timeout and will be canceled",
                &path
            );
            self.remove_subscription(&path)
        }
        if self.oneshot && self.paths.len() == 0 && self.requests_finished {
            self.flush().await?;
            bail!("oneshot done")
        }
        Ok(())
    }

    async fn process_update(
        &mut self,
        u: Option<BatchItem<Pooled<Vec<(SubId, Event)>>>>,
    ) -> Result<()> {
        Ok(match u {
            None => unreachable!(), // channel will never close
            Some(BatchItem::EndBatch) => self.flush().await?,
            Some(BatchItem::InBatch(mut batch)) => {
                for (id, value) in batch.drain(..) {
                    if let Some(path) = self.paths.get(&id) {
                        if self.subscribe_timeout.is_some() {
                            self.subscribe_ts.remove(path);
                        }
                        Out { raw: self.raw, path: &**path, value }.write(&mut self.to_stdout);
                        if self.oneshot {
                            if let Some(path) = self.paths.get(&id).cloned() {
                                self.remove_subscription(&path);
                            }
                            if self.paths.len() == 0 && self.requests_finished {
                                self.flush().await?;
                                bail!("oneshot finished")
                            }
                        }
                    }
                }
            }
        })
    }
}

async fn subscribe(cfg: Config, auth: DesiredAuth, p: Params) {
    let subscriber = Subscriber::new(cfg, auth).expect("create subscriber");
    let mut ctx = Ctx::new(subscriber, p);
    let mut tick = time::interval(Duration::from_secs(1));
    loop {
        select_biased! {
            _ = tick.tick().fuse() => if let Some(timeout) = ctx.subscribe_timeout {
                match ctx.check_timeouts(timeout).await {
                    Ok(()) => (),
                    Err(_) => break,
                }
            },
            u = ctx.updates.next() => match ctx.process_update(u).await {
                Ok(()) => (),
                Err(_) => break,
            },
            r = ctx.requests.next() => match ctx.process_request(r).await {
                Ok(()) => (),
                Err(_) => break,
            },
        }
    }
}

pub(super) fn run(cfg: Config, auth: DesiredAuth, params: Params) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(subscribe(cfg, auth, params));
}
