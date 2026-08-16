use anyhow::{Context, Error, Result, anyhow};
use arcstr::ArcStr;
use bytes::BytesMut;
use clap::Args;
use combine::{
    EasyParser, ParseError, Parser, RangeStream,
    parser::char::spaces,
    sep_by,
    stream::{Range, position},
    token,
};
use escaping::Escape;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    prelude::*,
    select_biased,
    stream::{self, FusedStream},
};
use netidx::{
    config::Config,
    path::Path,
    protocol::value_parser::{VAL_ESC, VAL_MUST_ESC, escaped_string, value},
    resolver_client::DesiredAuth,
    subscriber::{Dval, Event, SubId, Subscriber, Typ, UpdatesFlags, Value},
    utils::{BatchItem, Batched},
};
use netidx_protocols::rpc::client::Proc;
use poolshark::global::GPooled;
use std::{
    collections::HashMap,
    fmt,
    io::Write,
    str::FromStr,
    sync::LazyLock,
    time::{Duration, Instant},
};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    time,
};

#[derive(Args, Debug)]
pub(super) struct Params {
    /// unsubscribe after printing one value for each subscription
    #[arg(short, long)]
    oneshot: bool,
    /// don't read commands from stdin
    #[arg(short, long)]
    no_stdin: bool,
    /// don't print the path or the type
    #[arg(short, long)]
    raw: bool,
    /// cancel subscription unless it succeeds within timeout
    #[arg(short = 't', long)]
    subscribe_timeout: Option<u64>,
    paths: Vec<String>,
}

static RPC_ARG_ESC: LazyLock<Escape> =
    LazyLock::new(|| Escape::new('\\', &['\\', '='], &[], None).unwrap());

async fn write_buffer<W>(writer: &mut W, buffer: &mut BytesMut) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if !buffer.is_empty() {
        let to_write = buffer.split().freeze();
        writer.write_all(&to_write).await?;
        writer.flush().await?;
    }
    Ok(())
}

fn rpc_arg<I>() -> impl Parser<I, Output = (String, Value)>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        spaces().with(escaped_string(&['\\', '='], &RPC_ARG_ESC)),
        spaces().with(token('=')),
        spaces().with(value(&VAL_MUST_ESC, &VAL_ESC)),
    )
        .map(|(arg_name, _, arg_val)| (arg_name, arg_val))
}

fn rpc_args<I>() -> impl Parser<I, Output = Vec<(String, Value)>>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    sep_by(rpc_arg(), spaces().with(token(',')))
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
            let mut parts = escaping::splitn(&s[6..], '\\', 3, '|');
            let path = parts.next().ok_or_else(|| anyhow!("expected | before path"))?;
            let path = Path::from(ArcStr::from(path));
            let typ =
                parts.next().ok_or_else(|| anyhow!("expected type"))?.parse::<Typ>()?;
            let val = parts.next().ok_or_else(|| anyhow!("expected value"))?;
            let val = typ.parse(val)?;
            Ok(In::Write(path, val))
        } else if s.starts_with("CALL|") && s.len() > 5 {
            let mut parts = escaping::splitn(&s[5..], '\\', 2, '|');
            let path = parts.next().ok_or_else(|| anyhow!("expected| before path"))?;
            let path = Path::from(ArcStr::from(path));
            let args = match parts.next() {
                None => vec![],
                Some(s) => rpc_args()
                    .easy_parse(position::Stream::new(s))
                    .map(|(r, _)| r)
                    .map_err(|e| anyhow!(format!("{}", e)))?,
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
pub(crate) struct Out<'a> {
    pub(crate) raw: bool,
    pub(crate) path: &'a str,
    pub(crate) value: Event,
}

impl<'a> Out<'a> {
    pub(crate) fn write(&self, to_stdout: &mut BytesMut) -> Result<()> {
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
                    writeln!(w, "{}", WVal(v)).context("write raw line")?
                } else {
                    to_stdout.extend_from_slice(self.path.as_bytes());
                    to_stdout.extend_from_slice(b"|");
                    let typ = Typ::get(v);
                    let w = &mut BytesWriter(to_stdout);
                    write!(w, "{}|", typ).context("write line")?;
                    writeln!(w, "{}", WVal(v)).context("finish write line")?
                }
            }
        }
        Ok(())
    }
}

struct Ctx {
    sender_updates: Sender<GPooled<Vec<(SubId, Event)>>>,
    paths: HashMap<SubId, Path>,
    subscriptions: HashMap<Path, Dval>,
    rpcs: HashMap<Path, Proc>,
    subscribe_ts: HashMap<Path, Instant>,
    subscriber: Subscriber,
    requests: Box<dyn FusedStream<Item = Result<String>> + Unpin>,
    updates: Batched<Receiver<GPooled<Vec<(SubId, Event)>>>>,
    stdout: io::Stdout,
    stderr: io::Stderr,
    to_stdout: BytesMut,
    to_stderr: BytesMut,
    oneshot: bool,
    requests_finished: bool,
    raw: bool,
    subscribe_timeout: Option<Duration>,
}

fn completes_oneshot(event: &Event) -> bool {
    matches!(event, Event::Update(_))
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
                        let stdin = BufReader::new(io::stdin()).lines();
                        stream::unfold(stdin, |mut stdin| async move {
                            match stdin.next_line().await {
                                Ok(None) => None,
                                Ok(Some(line)) => Some((Ok(line), stdin)),
                                Err(e) => Some((Err(anyhow::Error::from(e)), stdin)),
                            }
                        })
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
            let s = subscriber.subscribe(path.clone());
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
                                    let proc = Proc::new(&self.subscriber, p.clone());
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
        write_buffer(&mut self.stdout, &mut self.to_stdout).await?;
        write_buffer(&mut self.stderr, &mut self.to_stderr).await?;
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
        u: Option<BatchItem<GPooled<Vec<(SubId, Event)>>>>,
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
                        // Resolver referral changes can transiently deliver an
                        // Unsubscribed event before the subscription reconnects
                        // at another hierarchy level. `--oneshot` promises one
                        // value, so only an actual update completes it; treating
                        // Unsubscribed as success exits silently before the
                        // redirected value arrives.
                        let received_value = completes_oneshot(&value);
                        Out { raw: self.raw, path: &**path, value }
                            .write(&mut self.to_stdout)?;
                        if self.oneshot && received_value {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    #[derive(Default)]
    struct FlushProbe {
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl AsyncWrite for FlushProbe {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.bytes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            self.flushes += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn oneshot_waits_through_referral_unsubscribe_for_a_value() {
        assert!(!completes_oneshot(&Event::Unsubscribed));
        assert!(completes_oneshot(&Event::Update(Value::Bool(true))));
    }

    #[tokio::test]
    async fn buffered_output_is_flushed_before_oneshot_exit() {
        let mut writer = FlushProbe::default();
        let mut buffer = BytesMut::from(&b"value\n"[..]);

        write_buffer(&mut writer, &mut buffer).await.unwrap();

        assert_eq!(writer.bytes, b"value\n");
        assert_eq!(writer.flushes, 1);
        assert!(buffer.is_empty());
    }
}

#[tokio::main]
pub(super) async fn run(cfg: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    env_logger::init();
    let subscriber = Subscriber::new(cfg, auth).context("create subscriber")?;
    crate::log_errors::subscriber(&subscriber);
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
    Ok(())
}
