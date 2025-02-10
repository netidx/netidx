use crate::{
    arity2, deftype, err, errf,
    expr::{parser::parse_fn_type, Expr, ExprId, FnType},
    stdfn::CachedVals,
    vm::{node::Node, Apply, BindId, BuiltIn, Ctx, Event, ExecCtx, InitFn},
};
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::{publisher::FromValue, subscriber::Value};
use std::{
    fmt::Debug,
    ops::SubAssign,
    sync::{Arc, LazyLock},
    time::Duration,
};

struct AfterIdle {
    args: CachedVals,
    id: Option<BindId>,
    eid: ExprId,
}

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for AfterIdle {
    const NAME: &str = "after_idle";
    deftype!("fn([duration, number], any) -> any");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, from, eid| {
            Ok(Box::new(AfterIdle { args: CachedVals::new(from), id: None, eid }))
        })
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for AfterIdle {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        let up = self.args.update_diff(ctx, from, event);
        // CR estokes: THis implementation is wrong, it should reset the timer when the value ticks
        let ((timeout, val), (timeout_up, val_up)) = arity2!(self.args.0, up);
        match ((timeout, val), (timeout_up, val_up)) {
            ((Some(secs), _), (true, _)) => match secs.clone().cast_to::<Duration>() {
                Err(e) => {
                    self.id = None;
                    return errf!("after_idle(timeout, cur): expected duration {e:?}");
                }
                Ok(dur) => {
                    let id = BindId::new();
                    self.id = Some(id);
                    ctx.user.set_timer(id, dur, self.eid);
                    return None;
                }
            },
            ((Some(_), Some(val)), (false, true)) if self.id.is_none() => {
                return Some(val.clone())
            }
            ((None, _), (_, _))
            | ((_, None), (_, _))
            | ((Some(_), Some(_)), (false, _)) => (),
        };
        match event {
            Event::Init | Event::Netidx(_, _) | Event::User(_) => None,
            Event::Variable(id, _) => {
                if self.id != Some(*id) {
                    None
                } else {
                    self.id = None;
                    self.args.0.get(1).and_then(|v| v.clone())
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Repeat {
    Yes,
    No,
    N(u64),
}

impl FromValue for Repeat {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::True => Ok(Repeat::Yes),
            Value::False => Ok(Repeat::No),
            v => match v.cast_to::<u64>() {
                Ok(n) => Ok(Repeat::N(n)),
                Err(_) => bail!("could not cast to repeat"),
            },
        }
    }
}

impl SubAssign<u64> for Repeat {
    fn sub_assign(&mut self, rhs: u64) {
        match self {
            Repeat::Yes | Repeat::No => (),
            Repeat::N(n) => *n -= rhs,
        }
    }
}

impl Repeat {
    fn will_repeat(&self) -> bool {
        match self {
            Repeat::No => false,
            Repeat::Yes => true,
            Repeat::N(n) => *n > 0,
        }
    }
}

struct Timer {
    args: CachedVals,
    timeout: Option<Duration>,
    repeat: Repeat,
    id: Option<BindId>,
    eid: ExprId,
}

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Timer {
    const NAME: &str = "timer";
    deftype!("fn([duration, number], [bool, number]) -> datetime");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, from, eid| {
            Ok(Box::new(Self {
                args: CachedVals::new(from),
                timeout: None,
                repeat: Repeat::No,
                id: None,
                eid,
            }))
        })
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for Timer {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        macro_rules! error {
            () => {{
                self.id = None;
                self.timeout = None;
                self.repeat = Repeat::No;
                return err!("timer(per, rep): expected duration, bool or number >= 0");
            }};
        }
        macro_rules! schedule {
            ($dur:expr) => {{
                let id = BindId::new();
                self.id = Some(id);
                ctx.user.set_timer(id, $dur, self.eid);
            }};
        }
        let up = self.args.update_diff(ctx, from, event);
        let ((timeout, repeat), (timeout_up, repeat_up)) = arity2!(self.args.0, up);
        match ((timeout, repeat), (timeout_up, repeat_up)) {
            ((None, Some(r)), (true, true)) | ((_, Some(r)), (false, true)) => {
                match r.clone().cast_to::<Repeat>() {
                    Err(_) => error!(),
                    Ok(repeat) => {
                        self.repeat = repeat;
                        if let Some(dur) = self.timeout {
                            if self.id.is_none() && repeat.will_repeat() {
                                schedule!(dur)
                            }
                        }
                    }
                }
            }
            ((Some(s), None), (true, _)) => match s.clone().cast_to::<Duration>() {
                Err(_) => error!(),
                Ok(dur) => self.timeout = Some(dur),
            },
            ((Some(s), Some(r)), (true, _)) => {
                match (s.clone().cast_to::<Duration>(), r.clone().cast_to::<Repeat>()) {
                    (Err(_), _) | (_, Err(_)) => error!(),
                    (Ok(dur), Ok(repeat)) => {
                        self.timeout = Some(dur);
                        self.repeat = repeat;
                        schedule!(dur)
                    }
                }
            }
            ((_, _), (false, false))
            | ((None, None), (_, _))
            | ((None, _), (true, false))
            | ((_, None), (false, true)) => (),
        }
        match event {
            Event::Init | Event::Netidx(_, _) | Event::User(_) => None,
            Event::Variable(id, now) => {
                if self.id != Some(*id) {
                    None
                } else {
                    self.id = None;
                    self.repeat -= 1;
                    if let Some(dur) = self.timeout {
                        if self.repeat.will_repeat() {
                            schedule!(dur)
                        }
                    }
                    Some(now.clone())
                }
            }
        }
    }
}

const MOD: &str = r#"
pub mod time {
    pub let after_idle = |timeout, v| 'after_idle
    pub let timer = |timeout, repeat| 'timer
}
"#;

pub fn register<C: Ctx, E: Debug + Clone>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<AfterIdle>();
    ctx.register_builtin::<Timer>();
    MOD.parse().unwrap()
}
