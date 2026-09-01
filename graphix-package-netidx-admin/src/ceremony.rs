//! The `Answerer` seam reified as a Graphix event stream.
//!
//! An interactive admin flow (a "ceremony") runs as a spawned task
//! driving the real `netidx_admin` op with a [`GxAnswerer`]. Every
//! question and progress report becomes a typed `Event` value delivered
//! through `Rt::watch_var` to the ceremony's `BindId`; blocking
//! questions carry an id and park on a oneshot that `answer(#id, c, a)`
//! resolves. This is the `UiRequest`/oneshot protocol the ratatui TUI
//! proved out, with the channel ends exposed to Graphix.
//!
//! A ceremony is INERT until observed: the op starts on the first
//! `events(c)` update, so event delivery can never race the consumer's
//! wake registration, and a ceremony nobody listens to never runs.
//! Dropping the last reference to the ceremony value cancels the op.

use crate::{FingerprintV, admin_err};
use anyhow::{Result, anyhow, bail};
use arcstr::ArcStr;
use futures::{SinkExt, channel::mpsc};
use graphix_compiler::{
    Apply, BindId, BuiltIn, Event, ExecCtx, Node, Rt, Scope, TagValue, UserEvent,
    effects::EffectKind, errf, expr::ExprId, typ::FnType,
};
use graphix_package_core::{CachedArgs, CachedVals, EvalCached};
use netidx_admin::{
    answer::{
        AdminDomainChoice, AdminDomainOption, Answerer, Field, OneTimeSecret, Progress,
        Stage,
    },
    transport::CaIdentity,
};
use netidx_admin_proto::Secret;
use netidx_derive::IntoValue;
use netidx_value::{Abstract, Value, abstract_type::AbstractWrapper};
use parking_lot::Mutex;
use poolshark::global::{GPooled, Pool};
use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        Arc, LazyLock, Weak,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::Duration,
};
use tokio::sync::{mpsc as tmpsc, oneshot};

static BATCHES: LazyLock<Pool<Vec<(BindId, Value)>>> =
    LazyLock::new(|| Pool::new(32, 128));

// ── event mirrors ────────────────────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
struct FieldInfoV {
    flag: String,
    label: String,
    help: String,
}

impl From<Field> for FieldInfoV {
    fn from(f: Field) -> Self {
        let info = f.info();
        FieldInfoV {
            flag: info.flag.into(),
            label: info.label.into(),
            help: info.help.into(),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct CaIdentityV {
    fingerprint: FingerprintV,
    domain: String,
    roles: Vec<String>,
    server_id: String,
    ca: bool,
}

impl From<&CaIdentity> for CaIdentityV {
    fn from(id: &CaIdentity) -> Self {
        CaIdentityV {
            fingerprint: FingerprintV::from(&id.fingerprint),
            domain: id.domain.clone(),
            roles: id.roles.iter().map(|r| format!("{r:?}")).collect(),
            server_id: id.server_id.to_string(),
            ca: id.ca,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct AdminDomainV {
    domain: String,
    identity: CaIdentityV,
}

#[derive(Debug, Clone, IntoValue)]
enum StageV {
    Discovering,
    Enrolling,
    WaitingApproval,
    Applying,
    Done,
}

impl From<Stage> for StageV {
    fn from(s: Stage) -> Self {
        match s {
            Stage::Discovering => StageV::Discovering,
            Stage::Enrolling => StageV::Enrolling,
            Stage::WaitingApproval => StageV::WaitingApproval,
            Stage::Applying => StageV::Applying,
            Stage::Done => StageV::Done,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
enum OneTimeSecretKindV {
    CaRecovery,
    AdminPassword(String),
}

#[derive(Debug, IntoValue)]
enum EventV {
    Text { id: Value, field: FieldInfoV, default: Option<String>, required: bool },
    Secret { id: Value, field: FieldInfoV },
    Choice { id: Value, field: FieldInfoV, choices: Vec<String>, default: Option<String> },
    Confirm { id: Value, field: FieldInfoV, default: bool },
    SelectAdminDomain { id: Value, domains: Vec<AdminDomainV> },
    Announce { id: Value, title: String, body: String },
    AnnounceIdentity { id: Value, body: String, fp: FingerprintV },
    ConfirmIdentity { id: Value, identity: CaIdentityV },
    OneTimeSecret { id: Value, kind: OneTimeSecretKindV, password: String },
    VerificationCode { purpose: String, fp: FingerprintV },
    ClearVerificationCode,
    Progress { stage: StageV, message: String, duration: Option<Duration> },
    Note(String),
    Warn(String),
    Done(Value),
}

// ── answers ──────────────────────────────────────────────────────

/// What kind of reply the armed question accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnswerKind {
    Text,
    Secret,
    Choice,
    Confirm,
    Domain,
    Ack,
}

#[derive(Debug)]
enum AnswerPayload {
    Text(Option<String>),
    Secret(String),
    Choice(String),
    Confirm(bool),
    Domain(AdminDomainChoice),
    Ack,
}

/// The parsed graphix `Answer` variant.
#[derive(Debug)]
enum ParsedAnswer {
    Payload(AnswerKind, AnswerPayload),
    Cancel,
}

fn parse_answer(v: &Value) -> Result<ParsedAnswer> {
    use {AnswerKind as K, AnswerPayload as P, ParsedAnswer as A};
    fn opt_string(v: &Value) -> Result<Option<String>> {
        match v {
            Value::Null => Ok(None),
            Value::String(s) => Ok(Some(s.to_string())),
            v => bail!("expected a string or null, got {v}"),
        }
    }
    match v {
        Value::String(tag) => match &**tag {
            "Ack" => Ok(A::Payload(K::Ack, P::Ack)),
            "Cancel" => Ok(A::Cancel),
            t => bail!("not an answer: `{t}"),
        },
        Value::Array(a) if a.len() == 2 => {
            let tag = match &a[0] {
                Value::String(s) => s,
                v => bail!("not an answer: {v}"),
            };
            match &**tag {
                "Text" => Ok(A::Payload(K::Text, P::Text(opt_string(&a[1])?))),
                "Secret" => match opt_string(&a[1])? {
                    Some(s) => Ok(A::Payload(K::Secret, P::Secret(s))),
                    None => bail!("a secret must be a string"),
                },
                "Choice" => match opt_string(&a[1])? {
                    Some(s) => Ok(A::Payload(K::Choice, P::Choice(s))),
                    None => bail!("a choice must be a string"),
                },
                "Confirm" => match &a[1] {
                    Value::Bool(b) => Ok(A::Payload(K::Confirm, P::Confirm(*b))),
                    v => bail!("expected a bool, got {v}"),
                },
                "Domain" => {
                    let choice = match &a[1] {
                        Value::String(s) if &**s == "Manual" => AdminDomainChoice::Manual,
                        Value::String(s) if &**s == "PollMore" => {
                            AdminDomainChoice::PollMore
                        }
                        Value::Array(d)
                            if d.len() == 2 && d[0] == Value::from("Discovered") =>
                        {
                            match &d[1] {
                                Value::I64(i) if *i >= 0 => {
                                    AdminDomainChoice::Discovered(*i as usize)
                                }
                                v => bail!("expected a non-negative index, got {v}"),
                            }
                        }
                        v => bail!("not an admin domain choice: {v}"),
                    };
                    Ok(A::Payload(K::Domain, P::Domain(choice)))
                }
                t => bail!("not an answer: `{t}(..)"),
            }
        }
        v => bail!("not an answer: {v}"),
    }
}

// ── question ids ─────────────────────────────────────────────────

/// The opaque `QuestionId`: only the interface can mint one, so an
/// answer can never name a question that was never asked. It carries
/// its ceremony (weakly — an id must not keep a cancelled op alive),
/// which is why `answer` takes no ceremony argument: the id IS the
/// route, and two ceremonies' questions can never be confused however
/// their sequence numbers line up.
#[derive(Clone)]
pub(crate) struct QuestionIdValue {
    shared: Weak<CeremonyShared>,
    seq: u64,
}

impl fmt::Debug for QuestionIdValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuestionId").field("seq", &self.seq).finish()
    }
}

impl PartialEq for QuestionIdValue {
    fn eq(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.shared, &other.shared) && self.seq == other.seq
    }
}

impl Eq for QuestionIdValue {}

impl PartialOrd for QuestionIdValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QuestionIdValue {
    fn cmp(&self, other: &Self) -> Ordering {
        (Weak::as_ptr(&self.shared), self.seq)
            .cmp(&(Weak::as_ptr(&other.shared), other.seq))
    }
}

impl Hash for QuestionIdValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Weak::as_ptr(&self.shared).hash(state);
        self.seq.hash(state);
    }
}

graphix_package_core::impl_no_pack!(QuestionIdValue);

static QUESTION_ID_WRAPPER: LazyLock<AbstractWrapper<QuestionIdValue>> =
    LazyLock::new(|| {
        let id = uuid::Uuid::from_bytes([
            0x9c, 0x2e, 0x51, 0x77, 0xa3, 0x08, 0x4d, 0x2b, 0xb1, 0x6a, 0xf4, 0x5d, 0x08,
            0x91, 0x27, 0x3e,
        ]);
        Abstract::register::<QuestionIdValue>(id)
            .expect("failed to register QuestionIdValue")
    });

/// Resolve one answer against the question it names. The error text is
/// operator-facing (it comes back as the `answer` builtin's value).
fn deliver_answer(qid: &QuestionIdValue, parsed: ParsedAnswer) -> Result<()> {
    let shared = match qid.shared.upgrade() {
        Some(s) => s,
        None => bail!("the ceremony is gone"),
    };
    let mut pending = shared.pending.lock();
    match &*pending {
        None => bail!("no question is outstanding"),
        Some(p) if p.seq != qid.seq => {
            bail!("stale answer: question {} is outstanding", p.seq)
        }
        Some(p) => match &parsed {
            ParsedAnswer::Cancel => (),
            ParsedAnswer::Payload(kind, payload) => {
                if *kind != p.kind {
                    bail!("the outstanding question takes a {:?} answer", p.kind)
                }
                match payload {
                    AnswerPayload::Text(None) if p.required => {
                        bail!("an answer is required")
                    }
                    AnswerPayload::Choice(s) => {
                        if let Some(choices) = &p.choices {
                            if !choices.iter().any(|c| c == s) {
                                bail!("\"{s}\" is not one of the choices")
                            }
                        }
                    }
                    _ => (),
                }
            }
        },
    }
    let p = pending.take().unwrap();
    match parsed {
        // dropping the reply cancels the op: its ask errors and the
        // ceremony finishes with `Done(Err)`
        ParsedAnswer::Cancel => (),
        ParsedAnswer::Payload(_, payload) => {
            let _ = p.reply.send(payload);
        }
    }
    Ok(())
}

// ── ceremony state ───────────────────────────────────────────────

#[derive(Debug)]
struct Pending {
    seq: u64,
    kind: AnswerKind,
    /// For `Choice` questions: the legal answers, enforced at `answer`.
    choices: Option<Vec<String>>,
    /// For `Text` questions: a null answer is refused (question stays
    /// armed) so the frontend re-prompts.
    required: bool,
    reply: oneshot::Sender<AnswerPayload>,
}

#[derive(Default)]
struct CeremonyShared {
    pending: Mutex<Option<Pending>>,
    /// The parked op, spawned by the first `events` observation.
    start: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    next_id: AtomicU64,
}

/// The opaque `Ceremony<'r>` value.
#[derive(Clone)]
pub(crate) struct CeremonyValue {
    bind_id: BindId,
    shared: Arc<CeremonyShared>,
    /// Held only by graph-side values: the last drop closes the channel,
    /// which cancels a running op.
    _cancel: Arc<oneshot::Sender<()>>,
}

impl fmt::Debug for CeremonyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ceremony").field("bind_id", &self.bind_id).finish()
    }
}

impl PartialEq for CeremonyValue {
    fn eq(&self, other: &Self) -> bool {
        self.bind_id == other.bind_id
    }
}

impl Eq for CeremonyValue {}

impl PartialOrd for CeremonyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CeremonyValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bind_id.cmp(&other.bind_id)
    }
}

impl Hash for CeremonyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bind_id.hash(state)
    }
}

graphix_package_core::impl_no_pack!(CeremonyValue);

static CEREMONY_WRAPPER: LazyLock<AbstractWrapper<CeremonyValue>> = LazyLock::new(|| {
    let id = uuid::Uuid::from_bytes([
        0x1f, 0x8a, 0x33, 0x6d, 0x0b, 0xe4, 0x42, 0x7c, 0x8e, 0x9f, 0x10, 0x21, 0x32,
        0x43, 0x54, 0x66,
    ]);
    Abstract::register::<CeremonyValue>(id).expect("failed to register CeremonyValue")
});

fn get_ceremony(cached: &CachedVals, idx: usize) -> Option<CeremonyValue> {
    match cached.0.get(idx)?.as_ref()? {
        Value::Abstract(a) => a.downcast_ref::<CeremonyValue>().cloned(),
        _ => None,
    }
}

// ── the bridge answerer ──────────────────────────────────────────

pub(crate) struct GxAnswerer {
    tx: tmpsc::UnboundedSender<(BindId, Value)>,
    bind_id: BindId,
    shared: Arc<CeremonyShared>,
    /// A pre-confirmed CA fingerprint: `confirm_identity` auto-accepts
    /// a match instead of asking, so post-connect ops don't re-run the
    /// security gesture — the ratatui `TuiAnswerer::with_glyph` rule.
    accept_glyph: Option<netidx_admin_proto::fingerprint::Fingerprint>,
}

impl GxAnswerer {
    fn emit(&self, ev: EventV) {
        let _ = self.tx.send((self.bind_id, ev.into()));
    }

    async fn ask(
        &mut self,
        kind: AnswerKind,
        choices: Option<Vec<String>>,
        required: bool,
        build: impl FnOnce(Value) -> EventV,
    ) -> Result<AnswerPayload> {
        let seq = self.shared.next_id.fetch_add(1, AtomicOrdering::Relaxed);
        let (reply, rx) = oneshot::channel();
        *self.shared.pending.lock() =
            Some(Pending { seq, kind, choices, required, reply });
        let id = QUESTION_ID_WRAPPER
            .wrap(QuestionIdValue { shared: Arc::downgrade(&self.shared), seq });
        self.emit(build(id));
        rx.await.map_err(|_| anyhow!("the operator cancelled"))
    }
}

#[async_trait::async_trait]
impl Answerer for GxAnswerer {
    fn interactive(&self) -> bool {
        true
    }

    async fn text(
        &mut self,
        field: Field,
        provided: Option<String>,
        default: Option<&str>,
        required: bool,
    ) -> Result<Option<String>> {
        if provided.is_some() {
            return Ok(provided);
        }
        let default = default.map(String::from);
        let r = self
            .ask(AnswerKind::Text, None, required, |id| EventV::Text {
                id,
                field: field.into(),
                default: default.clone(),
                required,
            })
            .await?;
        match r {
            AnswerPayload::Text(Some(s)) => Ok(Some(s)),
            AnswerPayload::Text(None) => Ok(default),
            _ => unreachable!(),
        }
    }

    async fn choice(
        &mut self,
        field: Field,
        provided: Option<String>,
        choices: &[&str],
        default: Option<&str>,
    ) -> Result<String> {
        if let Some(p) = provided {
            return Ok(p);
        }
        let choices: Vec<String> = choices.iter().map(|s| s.to_string()).collect();
        let default = default.map(String::from);
        let r =
            self.ask(AnswerKind::Choice, Some(choices.clone()), true, |id| {
                EventV::Choice { id, field: field.into(), choices, default }
            })
            .await?;
        match r {
            AnswerPayload::Choice(s) => Ok(s),
            _ => unreachable!(),
        }
    }

    async fn select_admin_domain(
        &mut self,
        domains: &[AdminDomainOption],
    ) -> Result<AdminDomainChoice> {
        let domains: Vec<AdminDomainV> = domains
            .iter()
            .map(|d| AdminDomainV {
                domain: d.domain.clone(),
                identity: CaIdentityV::from(&d.identity),
            })
            .collect();
        let r = self
            .ask(AnswerKind::Domain, None, true, |id| EventV::SelectAdminDomain {
                id,
                domains,
            })
            .await?;
        match r {
            AnswerPayload::Domain(c) => Ok(c),
            _ => unreachable!(),
        }
    }

    async fn confirm(
        &mut self,
        field: Field,
        provided: Option<bool>,
        default: bool,
    ) -> Result<bool> {
        if let Some(p) = provided {
            return Ok(p);
        }
        let r = self
            .ask(AnswerKind::Confirm, None, true, |id| EventV::Confirm {
                id,
                field: field.into(),
                default,
            })
            .await?;
        match r {
            AnswerPayload::Confirm(b) => Ok(b),
            _ => unreachable!(),
        }
    }

    async fn secret(&mut self, field: Field, provided: Option<Secret>) -> Result<Secret> {
        if let Some(p) = provided {
            return Ok(p);
        }
        let r = self
            .ask(AnswerKind::Secret, None, true, |id| EventV::Secret {
                id,
                field: field.into(),
            })
            .await?;
        match r {
            AnswerPayload::Secret(s) => Ok(Secret(s)),
            _ => unreachable!(),
        }
    }

    async fn announce(&mut self, title: &str, body: &str) -> Result<()> {
        let (title, body) = (title.to_string(), body.to_string());
        self.ask(AnswerKind::Ack, None, true, |id| EventV::Announce { id, title, body })
            .await?;
        Ok(())
    }

    async fn announce_identity(
        &mut self,
        body: &str,
        code: &netidx_admin_proto::fingerprint::Fingerprint,
    ) -> Result<()> {
        let (body, fp) = (body.to_string(), FingerprintV::from(code));
        self.ask(AnswerKind::Ack, None, true, |id| EventV::AnnounceIdentity {
            id,
            body,
            fp,
        })
        .await?;
        Ok(())
    }

    async fn confirm_identity(&mut self, identity: &CaIdentity) -> Result<bool> {
        if let Some(expected) = &self.accept_glyph {
            return Ok(&identity.fingerprint == expected);
        }
        let identity = CaIdentityV::from(identity);
        let r = self
            .ask(AnswerKind::Confirm, None, true, |id| EventV::ConfirmIdentity {
                id,
                identity,
            })
            .await?;
        match r {
            AnswerPayload::Confirm(b) => Ok(b),
            _ => unreachable!(),
        }
    }

    fn show_verification_code(
        &mut self,
        purpose: &str,
        code: &netidx_admin_proto::fingerprint::Fingerprint,
    ) {
        self.emit(EventV::VerificationCode {
            purpose: purpose.to_string(),
            fp: FingerprintV::from(code),
        });
    }

    fn clear_verification_code(&mut self) {
        self.emit(EventV::ClearVerificationCode);
    }

    fn progress(&mut self, progress: Progress) {
        self.emit(EventV::Progress {
            stage: progress.stage.into(),
            message: progress.message.to_string(),
            duration: progress.duration,
        });
    }

    fn note(&mut self, message: &str) {
        self.emit(EventV::Note(message.to_string()));
    }

    fn warn(&mut self, message: &str) {
        self.emit(EventV::Warn(message.to_string()));
    }

    async fn show_one_time_secret(
        &mut self,
        secret: OneTimeSecret,
        password: &str,
    ) -> Result<()> {
        let kind = match secret {
            OneTimeSecret::CaRecovery => OneTimeSecretKindV::CaRecovery,
            OneTimeSecret::AdminPassword { admin } => {
                OneTimeSecretKindV::AdminPassword(admin)
            }
        };
        let password = password.to_string();
        self.ask(AnswerKind::Ack, None, true, |id| EventV::OneTimeSecret {
            id,
            kind,
            password,
        })
        .await?;
        Ok(())
    }
}

// ── starting a ceremony ──────────────────────────────────────────

pub(crate) type BoxOp = Box<
    dyn for<'a> FnOnce(
            &'a mut GxAnswerer,
        ) -> futures::future::BoxFuture<'a, Result<Value>>
        + Send,
>;

async fn forward(
    mut rx: tmpsc::UnboundedReceiver<(BindId, Value)>,
    mut tx: mpsc::Sender<GPooled<Vec<(BindId, Value)>>>,
) {
    while let Some(ev) = rx.recv().await {
        let mut batch = BATCHES.take();
        batch.push(ev);
        while let Ok(more) = rx.try_recv() {
            batch.push(more)
        }
        if tx.send(batch).await.is_err() {
            break;
        }
    }
}

/// Wire up a ceremony: the event channel, the parked op (spawned on
/// first observation), and the cancel guard. Returns the wrapped
/// `Ceremony` value.
pub(crate) fn start_ceremony<R: Rt, E: UserEvent>(
    ctx: &mut ExecCtx<R, E>,
    accept_glyph: Option<netidx_admin_proto::fingerprint::Fingerprint>,
    op: BoxOp,
) -> Value {
    let bind_id = BindId::new();
    let (btx, brx) = mpsc::channel(8);
    ctx.rt.watch_var(brx);
    let (utx, urx) = tmpsc::unbounded_channel();
    tokio::spawn(forward(urx, btx));
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let shared = Arc::new(CeremonyShared::default());
    let mut ans =
        GxAnswerer { tx: utx, bind_id, shared: Arc::clone(&shared), accept_glyph };
    *shared.start.lock() = Some(Box::new(move || {
        tokio::spawn(async move {
            let r = tokio::select! {
                _ = &mut cancel_rx => return,
                r = op(&mut ans) => r,
            };
            let done = match r {
                Ok(v) => v,
                Err(e) => admin_err(e),
            };
            ans.emit(EventV::Done(done));
        });
    }));
    CEREMONY_WRAPPER.wrap(CeremonyValue { bind_id, shared, _cancel: Arc::new(cancel_tx) })
}

// ── events (accessor) ────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct Events {
    top_id: ExprId,
    cached: CachedVals,
    bind_id: Option<BindId>,
    out: TagValue,
}

impl<R: Rt, E: UserEvent> BuiltIn<R, E> for Events {
    const EFFECT: EffectKind = EffectKind::Async;
    const NAME: &str = "netidx_admin_events";

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        from: &'c [Node<R, E>],
        top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(Events {
            top_id,
            cached: CachedVals::new(from),
            bind_id: None,
            out: TagValue::phantom(),
        }))
    }
}

impl<R: Rt, E: UserEvent> Apply<R, E> for Events {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> &TagValue {
        if self.cached.update(ctx, from, event) {
            if let Some(c) = get_ceremony(&self.cached, 0) {
                if self.bind_id != Some(c.bind_id) {
                    if let Some(old) = self.bind_id.take() {
                        ctx.rt.unref_var(old, self.top_id);
                    }
                    ctx.rt.ref_var(c.bind_id, self.top_id);
                    self.bind_id = Some(c.bind_id);
                }
                // the ceremony has a listener now — run the parked op
                if let Some(start) = c.shared.start.lock().take() {
                    start()
                }
            }
        }
        match self.bind_id.and_then(|id| event.variables.get(&id)) {
            Some(tv) => self.out.set(TagValue::fired(tv.value_cloned())),
            None => self.out.ride(),
        }
    }

    fn sleep(&mut self, ctx: &mut ExecCtx<R, E>) {
        if let Some(id) = self.bind_id.take() {
            ctx.rt.unref_var(id, self.top_id);
        }
        self.cached.clear();
    }

    fn reset_replay(&mut self, _ctx: &mut ExecCtx<R, E>) {
        self.cached.clear();
    }

    fn delete(&mut self, ctx: &mut ExecCtx<R, E>) {
        if let Some(id) = self.bind_id.take() {
            ctx.rt.unref_var(id, self.top_id);
        }
    }
}

// ── answer ───────────────────────────────────────────────────────

fn get_question_id(cached: &CachedVals, idx: usize) -> Option<QuestionIdValue> {
    match cached.0.get(idx)?.as_ref()? {
        Value::Abstract(a) => a.downcast_ref::<QuestionIdValue>().cloned(),
        _ => None,
    }
}

#[derive(Debug, Default)]
pub(crate) struct AnswerEv;

impl<R: Rt, E: UserEvent> EvalCached<R, E> for AnswerEv {
    const EFFECT: EffectKind = EffectKind::Sync;
    const NAME: &str = "netidx_admin_answer";

    fn eval(&mut self, _ctx: &mut ExecCtx<R, E>, cached: &CachedVals) -> Option<Value> {
        let qid = get_question_id(cached, 0)?;
        let a = cached.0.get(1)?.as_ref()?;
        if std::env::var_os("GXDBG_ANSWER").is_some() {
            eprintln!("ANSWER seq={} a={a}", qid.seq);
        }
        let parsed = match parse_answer(a) {
            Ok(p) => p,
            Err(e) => return Some(errf!("Admin", "{e:#}")),
        };
        Some(match deliver_answer(&qid, parsed) {
            Ok(()) => Value::Null,
            Err(e) => errf!("Admin", "{e:#}"),
        })
    }
}

pub(crate) type Answer = CachedArgs<AnswerEv>;

#[cfg(test)]
mod test {
    use super::*;

    fn tag1(tag: &str, payload: Value) -> Value {
        Value::Array(netidx_value::ValArray::from([
            Value::String(ArcStr::from(tag)),
            payload,
        ]))
    }

    #[test]
    fn answers_parse() {
        assert!(matches!(
            parse_answer(&Value::from("Ack")),
            Ok(ParsedAnswer::Payload(AnswerKind::Ack, AnswerPayload::Ack))
        ));
        assert!(matches!(parse_answer(&Value::from("Cancel")), Ok(ParsedAnswer::Cancel)));
        assert!(matches!(
            parse_answer(&tag1("Text", Value::Null)),
            Ok(ParsedAnswer::Payload(AnswerKind::Text, AnswerPayload::Text(None)))
        ));
        assert!(matches!(
            parse_answer(&tag1("Confirm", Value::Bool(true))),
            Ok(ParsedAnswer::Payload(AnswerKind::Confirm, AnswerPayload::Confirm(true)))
        ));
        match parse_answer(&tag1("Domain", tag1("Discovered", Value::I64(2)))) {
            Ok(ParsedAnswer::Payload(
                AnswerKind::Domain,
                AnswerPayload::Domain(AdminDomainChoice::Discovered(2)),
            )) => (),
            r => panic!("bad domain parse: {r:?}"),
        }
        match parse_answer(&tag1("Domain", Value::from("Manual"))) {
            Ok(ParsedAnswer::Payload(
                AnswerKind::Domain,
                AnswerPayload::Domain(AdminDomainChoice::Manual),
            )) => (),
            r => panic!("bad manual parse: {r:?}"),
        }
        assert!(parse_answer(&Value::from("Bogus")).is_err());
        assert!(parse_answer(&tag1("Secret", Value::Null)).is_err());
        assert!(parse_answer(&tag1("Confirm", Value::I64(1))).is_err());
    }

    fn extract_id(ev: &Value) -> QuestionIdValue {
        let payload = match ev {
            Value::Array(a) if a.len() == 2 => &a[1],
            v => panic!("not a question event: {v}"),
        };
        let pairs = match payload {
            Value::Array(a) => a,
            v => panic!("not a struct payload: {v}"),
        };
        for p in pairs.iter() {
            if let Value::Array(kv) = p
                && kv[0] == Value::from("id")
                && let Value::Abstract(a) = &kv[1]
            {
                return a.downcast_ref::<QuestionIdValue>().cloned().unwrap();
            }
        }
        panic!("no id in {payload}")
    }

    /// The full blocking round trip, no daemon required: ask parks on
    /// the oneshot, the event carries a minted opaque id, a wrong-kind
    /// answer is refused with the question still armed, the right one
    /// resolves the ask, and the id is stale afterwards.
    #[tokio::test]
    async fn ask_answer_round_trip() {
        let (utx, mut urx) = tmpsc::unbounded_channel();
        let shared = Arc::new(CeremonyShared::default());
        let mut ans = GxAnswerer {
            tx: utx,
            bind_id: BindId::new(),
            shared: Arc::clone(&shared),
            accept_glyph: None,
        };
        let asked =
            tokio::spawn(async move { ans.confirm(Field::AdminHere, None, false).await });
        let (_, ev) = urx.recv().await.unwrap();
        let qid = extract_id(&ev);
        let wrong = parse_answer(&tag1("Text", Value::Null)).unwrap();
        assert!(deliver_answer(&qid, wrong).is_err());
        let right = parse_answer(&tag1("Confirm", Value::Bool(true))).unwrap();
        deliver_answer(&qid, right).unwrap();
        assert!(asked.await.unwrap().unwrap());
        let stale = parse_answer(&tag1("Confirm", Value::Bool(false))).unwrap();
        assert!(deliver_answer(&qid, stale).is_err());
    }

    /// Cancel drops the reply: the parked ask unwinds as an error, which
    /// a real ceremony surfaces as `Done(Err)`.
    #[tokio::test]
    async fn cancel_aborts_the_ask() {
        let (utx, mut urx) = tmpsc::unbounded_channel();
        let shared = Arc::new(CeremonyShared::default());
        let mut ans = GxAnswerer {
            tx: utx,
            bind_id: BindId::new(),
            shared: Arc::clone(&shared),
            accept_glyph: None,
        };
        let asked =
            tokio::spawn(
                async move { ans.text(Field::AdminName, None, None, true).await },
            );
        let (_, ev) = urx.recv().await.unwrap();
        let qid = extract_id(&ev);
        deliver_answer(&qid, ParsedAnswer::Cancel).unwrap();
        assert!(asked.await.unwrap().is_err());
    }
}
