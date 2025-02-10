use crate::{
    expr::{Expr, ExprId, ModPath},
    vm::{node::Node, BindId, Ctx, Event, ExecCtx},
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use fxhash::FxHashMap;
use netidx::{
    publisher::{Publisher, PublisherBuilder, Value},
    resolver_server,
    subscriber::{Subscriber, SubscriberBuilder},
};
use smallvec::SmallVec;
use std::{collections::HashMap, mem};

struct TestCtx {
    by_ref: FxHashMap<BindId, SmallVec<[ExprId; 3]>>,
    var_updates: Vec<(ExprId, BindId, Value)>,
    resolver: resolver_server::Server,
    publisher: Publisher,
    subscriber: Subscriber,
}

impl TestCtx {
    async fn new() -> Result<Self> {
        let resolver = {
            use resolver_server::config::{self, file};
            let cfg = file::ConfigBuilder::default()
                .member_servers(vec![file::MemberServerBuilder::default()
                    .auth(file::Auth::Anonymous)
                    .addr("127.0.0.1:0".parse()?)
                    .bind_addr("127.0.0.1".parse()?)
                    .build()?])
                .build()?;
            let cfg = config::Config::from_file(cfg)?;
            resolver_server::Server::new(cfg, false, 0).await?
        };
        let addr = *resolver.local_addr();
        let cfg = {
            use netidx::config::{self, file, DefaultAuthMech};
            let cfg = file::ConfigBuilder::default()
                .addrs(vec![(addr, file::Auth::Anonymous)])
                .default_auth(DefaultAuthMech::Anonymous)
                .default_bind_config("local")
                .build()?;
            config::Config::from_file(cfg)?
        };
        let publisher = PublisherBuilder::new(cfg.clone()).build().await?;
        let subscriber = SubscriberBuilder::new(cfg).build()?;
        Ok(Self {
            by_ref: HashMap::default(),
            var_updates: vec![],
            resolver,
            publisher,
            subscriber,
        })
    }
}

impl Ctx for TestCtx {
    fn call_rpc(
        &mut self,
        name: netidx::path::Path,
        args: Vec<(ArcStr, netidx::publisher::Value)>,
        ref_by: ExprId,
        id: crate::vm::BindId,
    ) {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.by_ref.clear();
        self.var_updates.clear();
    }

    fn durable_subscribe(
        &mut self,
        flags: netidx::subscriber::UpdatesFlags,
        path: netidx::path::Path,
        ref_by: ExprId,
    ) -> netidx::subscriber::Dval {
        unimplemented!()
    }

    fn unsubscribe(
        &mut self,
        path: netidx::path::Path,
        dv: netidx::subscriber::Dval,
        ref_by: ExprId,
    ) {
        unimplemented!()
    }

    fn set_timer(
        &mut self,
        id: crate::vm::BindId,
        timeout: std::time::Duration,
        ref_by: ExprId,
    ) {
        unimplemented!()
    }

    fn ref_var(&mut self, id: BindId, ref_by: ExprId) {
        let refs = self.by_ref.entry(id).or_default();
        if !refs.contains(&ref_by) {
            refs.push(ref_by);
        }
    }

    fn unref_var(&mut self, id: BindId, ref_by: ExprId) {
        unimplemented!()
    }

    fn set_var(&mut self, id: BindId, value: Value) {
        if let Some(refs) = self.by_ref.get(&id) {
            match &**refs {
                [] => (),
                [eids @ .., eid] => {
                    for eid in eids {
                        self.var_updates.push((*eid, id, value.clone()));
                    }
                    self.var_updates.push((*eid, id, value))
                }
            }
        }
    }
}

struct TestState {
    exprs: FxHashMap<ExprId, Node<TestCtx, ()>>,
    ctx: ExecCtx<TestCtx, ()>,
}

impl TestState {
    async fn new() -> Result<Self> {
        Ok(Self { exprs: HashMap::default(), ctx: ExecCtx::new(TestCtx::new().await?) })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bind_ref_arith() -> Result<()> {
    let mut state = TestState::new().await?;
    let e = r#"
{
  let v = (((1 + 1) * 2) / 2) - 1;
  v
}
"#
    .parse::<Expr>()?;
    let eid = e.id;
    let mut n = Node::compile(&mut state.ctx, &ModPath::root(), e);
    if let Some(e) = n.extract_err() {
        bail!("compilation failed {e}")
    }
    assert_eq!(n.update(&mut state.ctx, &Event::Init), None);
    assert_eq!(state.ctx.user.var_updates.len(), 1);
    let (up_eid, _, v) = &state.ctx.user.var_updates[0];
    assert_eq!(up_eid, &eid);
    assert_eq!(v, &Value::I64(1));
    let (_, id, v) = state.ctx.user.var_updates.pop().unwrap();
    assert_eq!(n.update(&mut state.ctx, &Event::Variable(id, v)), Some(Value::I64(1)));
    assert_eq!(state.ctx.user.var_updates.len(), 0);
    Ok(())
}

macro_rules! run {
    ($name:ident, $code:expr, $pred:expr) => {
        #[tokio::test(flavor = "current_thread")]
        async fn $name() -> Result<()> {
            let mut state = TestState::new().await?;
            let mut n = Node::compile(&mut state.ctx, &ModPath::root(), $code.parse()?);
            if let Some(e) = n.extract_err() {
                if $pred(Err(dbg!(anyhow!("compilation failed {e}")))) {
                    return Ok(());
                }
            }
            assert_eq!(n.update(&mut state.ctx, &Event::Init), None);
            let mut fin = false;
            while state.ctx.user.var_updates.len() > 0 {
                for (_, id, v) in mem::take(&mut state.ctx.user.var_updates) {
                    match n.update(&mut state.ctx, &Event::Variable(id, v)) {
                        None => (),
                        Some(v) if !fin && $pred(Ok(&v)) => fin = true,
                        v => panic!("unexpected result {v:?}"),
                    }
                }
            }
            if !fin {
                bail!("did not receive expected result")
            } else {
                Ok(())
            }
        }
    };
}

const SCOPE: &str = r#"
{
  let v = (((1 + 1) * 2) / 2) - 1;
  let x = {
     let v = 42;
     v * 2
  };
  v + x
}
"#;

run!(scope, SCOPE, |v: Result<&Value>| match v {
    Ok(&Value::I64(85)) => true,
    _ => false,
});

const CORE_USE: &str = r#"
{
  let v = (((1 + 1) * 2) / 2) - 1;
  let x = {
     let v = 42;
     once(v * 2)
  };
  [v, x]
}
"#;

run!(core_use, CORE_USE, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) if &**a == &[Value::I64(1), Value::I64(84)] => true,
    _ => false,
});

const NAME_MODPATH: &str = r#"
{
  let z = "baz";
  str::join(", ", "foo", "bar", z)
}
"#;

run!(name_modpath, NAME_MODPATH, |v: Result<&Value>| match v {
    Ok(Value::String(s)) => &**s == "foo, bar, baz",
    _ => false,
});

const LAMBDA: &str = r#"
{
  let y = 10;
  let f = |x| x + y;
  f(10)
}
"#;

run!(lambda, LAMBDA, |v: Result<&Value>| match v {
    Ok(Value::I64(20)) => true,
    _ => false,
});

const STATIC_SCOPE: &str = r#"
{
  let f = |x| x + y;
  let y = 10;
  f(10)
}
"#;

run!(static_scope, STATIC_SCOPE, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const UNDEFINED: &str = r#"
{
  let y = 10;
  let z = x + y;
  let x = 10;
  z
}
"#;

run!(undefined, UNDEFINED, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const FIRST_CLASS_LAMBDAS: &str = r#"
{
  let doit = |x| x + 1;
  let g = |f, y| f(y) + 1;
  g(doit, 1)
}
"#;

run!(first_class_lambdas, FIRST_CLASS_LAMBDAS, |v: Result<&Value>| match v {
    Ok(Value::I64(3)) => true,
    _ => false,
});

const SELECT: &str = r#"
{
  let x = 1;
  let y = x + 1;
  let z = y + 1;
  let s = select any(x, y, z) {
    I64(v) if v == 1 => "first [v]",
    v if v == 2 => "second [v]",
    v => "third [v]"
  };
  group(s, |n, x| n == 3)
}
"#;

run!(select, SELECT, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &**a {
        [Value::String(a), Value::String(b), Value::String(c)]
            if &**a == "first i64:1"
                && &**b == "second i64:2"
                && &**c == "third i64:3" =>
            true,
        _ => false,
    },
    _ => false,
});

const LIB_CORE_ALL: &str = r#"
{
  let x = 1;
  let y = x;
  let z = y;
  all(x, y, z)
}
"#;

run!(lib_core_all, LIB_CORE_ALL, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});

const LIB_CORE_AND: &str = r#"
{
  let x = 1;
  let y = x + 1;
  let z = y + 1;
  and(x < y, y < z, x > 0, z < 10)
}
"#;

run!(lib_core_and, LIB_CORE_AND, |v: Result<&Value>| match v {
    Ok(Value::True) => true,
    _ => false,
});

const SIMPLE_TYPECHECK: &str = r#"
{
  "foo" + 1
}
"#;

run!(simple_typecheck, SIMPLE_TYPECHECK, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const TCFAIL: &str = r#"
{
  let x = 1;
  let y = x + 1;
  true
}
"#;

run!(tcfail, TCFAIL, |v: Result<&Value>| match v {
    Ok(Value::True) => true,
    _ => false,
});
