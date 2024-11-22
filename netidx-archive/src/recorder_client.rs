use crate::{
    logfile::{BatchItem, Id, Seek},
    recorder::State,
};
use anyhow::Result;
use chrono::prelude::*;
use futures::{channel::mpsc, prelude::*};
use fxhash::FxHashMap;
use netidx::{
    chars::Chars,
    pack::Pack,
    path::Path,
    pool::{Pool, Pooled},
    publisher::FromValue,
    resolver_client::GlobSet,
    subscriber::{Dval, Event, Subscriber, UpdatesFlags, Value},
};
use netidx_derive::Pack;
use netidx_protocols::{call_rpc, rpc::client::Proc};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::{task, try_join};

lazy_static! {
    pub(crate) static ref PATHMAPS: Pool<FxHashMap<Id, Path>> = Pool::new(100, 10_000);
    pub(crate) static ref SHARDS: Pool<Vec<OneshotReplyShard>> = Pool::new(100, 128);
}

#[derive(Debug, Clone, Pack)]
pub struct OneshotReplyShard {
    pub pathmap: Pooled<FxHashMap<Id, Path>>,
    pub image: Pooled<FxHashMap<Id, Event>>,
    pub deltas: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
}

#[derive(Debug, Clone, Pack)]
pub struct OneshotReply(pub Pooled<Vec<OneshotReplyShard>>);

fn encode_bound(bound: &Option<DateTime<Utc>>) -> Value {
    match bound {
        None => Value::String(Chars::from("unbounded")),
        Some(dt) => Value::DateTime(*dt),
    }
}

fn encode_filter(filter: &GlobSet) -> Value {
    let v =
        filter.iter().map(|glob| Value::String(glob.raw().clone())).collect::<Vec<_>>();
    Value::Array(Arc::from(v))
}

#[derive(Debug, Clone, Copy)]
pub enum Speed {
    Unlimited,
    Rate(f64),
}

impl Into<Value> for Speed {
    fn into(self) -> Value {
        match self {
            Self::Rate(f) => Value::F64(f),
            Self::Unlimited => Value::from("unlimited"),
        }
    }
}

impl FromValue for Speed {
    fn from_value(v: Value) -> Result<Self> {
        match v.cast_to::<Option<f64>>()? {
            None => Ok(Self::Unlimited),
            Some(f) => Ok(Self::Rate(f)),
        }
    }
}

/// A recorder playback session. The session will not time out unless this
/// is dropped.
pub struct Session {
    base: Path,
    start: Dval,
    end: Dval,
    speed: Dval,
    state: Dval,
    pos: Dval,
}

impl Session {
    async fn new(base: Path, subscriber: Subscriber) -> Result<Self> {
        let start = subscriber.subscribe(base.append("control/start/current"));
        let end = subscriber.subscribe(base.append("control/end/current"));
        let speed = subscriber.subscribe(base.append("control/speed/current"));
        let state = subscriber.subscribe(base.append("control/state/current"));
        let pos = subscriber.subscribe(base.append("control/pos/current"));
        try_join!(
            start.wait_subscribed(),
            end.wait_subscribed(),
            speed.wait_subscribed(),
            state.wait_subscribed(),
            pos.wait_subscribed()
        )?;
        Ok(Self { base, start, end, speed, state, pos })
    }

    /// Return the base path of the session's published data. Recorded
    /// values with be published at their original observed paths
    /// appended to this path.
    pub fn data_path(&self) -> Path {
        self.base.append("data")
    }

    /// Return the current start
    pub fn start(&self) -> Result<Option<DateTime<Utc>>> {
        match self.start.last() {
            Event::Unsubscribed => bail!("not subscribed to start"),
            Event::Update(v) => Ok(v.cast_to()?),
        }
    }

    /// Change the start position of the session
    pub async fn set_start(&self, st: Option<DateTime<Utc>>) -> Result<()> {
        match self.start.write_with_recipt(st.into()).await {
            Err(_) => bail!("canceled"),
            Ok(Value::Ok) => Ok(()),
            Ok(v) => bail!("{v:?}"),
        }
    }

    /// Return the current end
    pub fn end(&self) -> Result<Option<DateTime<Utc>>> {
        match self.end.last() {
            Event::Unsubscribed => bail!("not subscribed to end"),
            Event::Update(v) => Ok(v.cast_to()?),
        }
    }

    /// Change the end position of the session
    pub async fn set_end(&self, st: Option<DateTime<Utc>>) -> Result<()> {
        match self.end.write_with_recipt(st.into()).await {
            Err(_) => bail!("canceled"),
            Ok(Value::Ok) => Ok(()),
            Ok(v) => bail!("{v:?}"),
        }
    }

    /// Return the current playback speed
    pub fn speed(&self) -> Result<Speed> {
        match self.speed.last() {
            Event::Unsubscribed => bail!("not subscribed to speed"),
            Event::Update(v) => Ok(v.cast_to()?),
        }
    }

    /// Change the playback speed of the session
    pub async fn set_speed(&self, sp: Speed) -> Result<()> {
        match self.speed.write_with_recipt(sp.into()).await {
            Err(_) => bail!("canceled"),
            Ok(Value::Ok) => Ok(()),
            Ok(v) => bail!("{v:?}"),
        }
    }

    /// Return the current state
    pub fn state(&self) -> Result<State> {
        match self.state.last() {
            Event::Unsubscribed => bail!("not subscribed to state"),
            Event::Update(v) => Ok(v.cast_to()?),
        }
    }

    /// Change the state of the session
    pub async fn set_state(&self, s: State) -> Result<()> {
        match self.state.write_with_recipt(s.into()).await {
            Err(_) => bail!("canceled"),
            Ok(Value::Ok) => Ok(()),
            Ok(v) => bail!("{v:?}"),
        }
    }

    /// Return the current position
    pub fn pos(&self) -> Result<Option<DateTime<Utc>>> {
        match self.pos.last() {
            Event::Unsubscribed => bail!("not subscribed to pos"),
            Event::Update(v) => Ok(v.cast_to()?),
        }
    }

    /// Change the position of the session
    pub async fn set_pos(&self, s: Seek) -> Result<()> {
        match self.pos.write_with_recipt(s.into()).await {
            Err(_) => bail!("canceled"),
            Ok(Value::Ok) => Ok(()),
            Ok(v) => bail!("{v:?}"),
        }
    }

    /// Update the specified channel whenever the position changes
    pub fn pos_updates(&self, mut tx: mpsc::Sender<Option<DateTime<Utc>>>) {
        let (rtx, mut rrx) = mpsc::channel(10);
        self.pos.updates(UpdatesFlags::empty(), rtx);
        task::spawn(async move {
            while let Some(mut batch) = rrx.next().await {
                for (_, e) in batch.drain(..) {
                    if let Event::Update(v) = e {
                        if let Ok(dt) = v.cast_to::<Option<DateTime<Utc>>>() {
                            match tx.send(dt).await {
                                Ok(()) => (),
                                Err(_) => break,
                            }
                        }
                    }
                }
            }
        });
    }
}

/// Builder to create a new recorder session
#[derive(Debug)]
pub struct SessionBuilder {
    base: Path,
    proc: Proc,
    subscriber: Subscriber,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    pos: Option<Seek>,
    state: Option<State>,
    speed: Option<Speed>,
    play_after: Option<Duration>,
    filter: Option<GlobSet>,
}

impl SessionBuilder {
    fn new(base: Path, proc: Proc, subscriber: Subscriber) -> SessionBuilder {
        Self {
            base,
            proc,
            subscriber,
            start: None,
            end: None,
            pos: None,
            state: None,
            speed: None,
            play_after: None,
            filter: None,
        }
    }

    fn check_bounds(&self) -> Result<()> {
        match (self.start.as_ref(), self.end.as_ref()) {
            (None, _) | (_, None) => Ok(()),
            (Some(t0), Some(t1)) => {
                if t0 > t1 {
                    bail!("the start bound may not be after the end")
                }
                Ok(())
            }
        }
    }

    /// Set the starting time of the session. The default starting
    /// time is Unbounded, meaning the very beginning of the archive.
    pub fn start(&mut self, start: DateTime<Utc>) -> Result<&mut Self> {
        let prev = self.start;
        self.start = Some(start);
        if let Err(e) = self.check_bounds() {
            self.start = prev;
            return Err(e);
        }
        Ok(self)
    }

    /// Set the end time of the session. The default end time is
    /// Unbounded, which means the session will continue to produce
    /// data as it comes in when the archive ends.
    pub fn end(&mut self, end: DateTime<Utc>) -> Result<&mut Self> {
        let prev = self.end;
        self.end = Some(end);
        if let Err(e) = self.check_bounds() {
            self.end = prev;
            return Err(e);
        }
        Ok(self)
    }

    /// Set the initial position of the session. The default is at the beginning of start.
    pub fn pos(&mut self, pos: Seek) -> Result<&mut Self> {
        self.pos = Some(pos);
        Ok(self)
    }

    /// Set the initial state of the session. The default state is Pause.
    pub fn state(&mut self, state: State) -> Result<&mut Self> {
        self.state = Some(state);
        Ok(self)
    }

    /// Set the session to start playing after the specified time out. The default is never.
    pub fn play_after(&mut self, after: Duration) -> Result<&mut Self> {
        self.play_after = Some(after);
        Ok(self)
    }

    /// Set the filter. Only paths matching the globset will be
    /// published. The default is to publish all paths.
    pub fn filter(&mut self, filter: GlobSet) -> Result<&mut Self> {
        self.filter = Some(filter);
        Ok(self)
    }

    /// Start the session. You can call this more than once with the
    /// same builder if you wish to create multiple sessions with the
    /// same parameters.
    pub async fn start_session(&self) -> Result<Session> {
        let id = call_rpc!(
            &self.proc,
            start: encode_bound(&self.start),
            end: encode_bound(&self.end),
            speed: self.speed.unwrap_or(Speed::Rate(1.)),
            pos: self.pos,
            state: self.state,
            play_after: self.play_after,
            filter: self.filter.as_ref().map(|g| g.raw()).unwrap_or_else(|| vec![Chars::from("/**")])
        ).await?
            .get_as::<Chars>()
            .ok_or_else(|| anyhow!("expected session id to be a string"))?;
        Session::new(self.base.append(&id), self.subscriber.clone()).await
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    oneshot: Proc,
    session: Proc,
    base: Path,
    subscriber: Subscriber,
}

impl Client {
    pub fn new(subscriber: &Subscriber, base: &Path) -> Result<Client> {
        let oneshot = Proc::new(subscriber, base.append("oneshot"))?;
        let session = Proc::new(subscriber, base.append("session"))?;
        Ok(Self { oneshot, session, base: base.clone(), subscriber: subscriber.clone() })
    }

    /// Get data from the recorder in one shot without setting up a
    /// new session. There are limits to how much data you can request
    /// in one shot (defined by the recorder), you will get an error
    /// if you exceed the limit.
    pub async fn oneshot(
        &self,
        start: &Option<DateTime<Utc>>,
        end: &Option<DateTime<Utc>>,
        filter: &GlobSet,
    ) -> Result<OneshotReply> {
        let res = call_rpc!(
            &self.oneshot,
            start: encode_bound(start),
            end: encode_bound(end),
            filter: encode_filter(filter)
        )
        .await?;
        match res {
            Value::Error(e) => bail!("{}", e.to_string()),
            Value::Bytes(buf) => Ok(Pack::decode(&mut &*buf)?),
            _ => bail!("unexpected response type"),
        }
    }

    pub fn session(&self) -> SessionBuilder {
        SessionBuilder::new(
            self.base.clone(),
            self.session.clone(),
            self.subscriber.clone(),
        )
    }
}
