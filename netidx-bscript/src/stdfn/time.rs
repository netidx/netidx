use crate::{
    arity2, deftype, err, errf,
    expr::{Expr, ExprId},
    node::Node,
    stdfn::CachedVals,
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::{publisher::FromValue, subscriber::Value};
use std::{ops::SubAssign, sync::Arc, time::Duration};

struct AfterIdle {
    args: CachedVals,
    scheduled: usize,
    id: BindId,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for AfterIdle {
    const NAME: &str = "after_idle";
    deftype!("fn([duration, Number], 'a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, from, top_id| {
            let id = BindId::new();
            ctx.user.ref_var(id, top_id);
            Ok(Box::new(AfterIdle {
                args: CachedVals::new(from),
                scheduled: 0,
                id,
                top_id,
            }))
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for AfterIdle {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        let up = self.args.update_diff(ctx, from, event);
        let ((timeout, val), (timeout_up, val_up)) = arity2!(self.args.0, up);
        match ((timeout, val), (timeout_up, val_up)) {
            ((Some(secs), _), (true, _)) | ((Some(secs), _), (_, true)) => match secs
                .clone()
                .cast_to::<Duration>()
            {
                Err(e) => {
                    return errf!("after_idle(timeout, cur): expected duration {e:?}");
                }
                Ok(dur) => {
                    self.scheduled += 1;
                    ctx.user.set_timer(self.id, dur);
                    return None;
                }
            },
            ((None, _), (_, _))
            | ((_, None), (_, _))
            | ((Some(_), Some(_)), (false, _)) => (),
        };
        if self.scheduled == 0 {
            None
        } else {
            event.variables.get(&self.id).and_then(|_| {
                self.scheduled -= 1;
                if self.scheduled == 0 {
                    self.args.0[1].clone()
                } else {
                    None
                }
            })
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id)
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
            Value::Bool(true) => Ok(Repeat::Yes),
            Value::Bool(false) => Ok(Repeat::No),
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
    scheduled: bool,
    id: BindId,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Timer {
    const NAME: &str = "timer";
    deftype!("fn([duration, Number], [bool, Number]) -> datetime");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, from, top_id| {
            let id = BindId::new();
            ctx.user.ref_var(id, top_id);
            Ok(Box::new(Self {
                args: CachedVals::new(from),
                timeout: None,
                repeat: Repeat::No,
                scheduled: false,
                id,
                top_id,
            }))
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Timer {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        macro_rules! error {
            () => {{
                self.timeout = None;
                self.repeat = Repeat::No;
                return err!("timer(per, rep): expected duration, bool or number >= 0");
            }};
        }
        macro_rules! schedule {
            ($dur:expr) => {{
                self.scheduled = true;
                ctx.user.set_timer(self.id, $dur)
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
                            if self.scheduled && repeat.will_repeat() {
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
        event.variables.get(&self.id).map(|now| {
            self.repeat -= 1;
            self.scheduled = false;
            if let Some(dur) = self.timeout {
                if self.repeat.will_repeat() {
                    schedule!(dur);
                }
            }
            now.clone()
        })
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id)
    }
}

const MOD: &str = r#"
pub mod time {
    pub let after_idle = |timeout, v| 'after_idle;
    pub let timer = |timeout, repeat| 'timer
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<AfterIdle>().unwrap();
    ctx.register_builtin::<Timer>().unwrap();
    MOD.parse().unwrap()
}
