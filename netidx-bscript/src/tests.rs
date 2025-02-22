use crate::{
    expr::{Expr, ExprId, ModPath},
    node::Node,
    BindId, Ctx, Event, ExecCtx,
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
    _resolver: resolver_server::Server,
    _publisher: Publisher,
    _subscriber: Subscriber,
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
            _resolver: resolver,
            _publisher: publisher,
            _subscriber: subscriber,
        })
    }
}

impl Ctx for TestCtx {
    fn call_rpc(
        &mut self,
        _name: netidx::path::Path,
        _args: Vec<(ArcStr, netidx::publisher::Value)>,
        _ref_by: ExprId,
        _id: BindId,
    ) {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.by_ref.clear();
        self.var_updates.clear();
    }

    fn durable_subscribe(
        &mut self,
        _flags: netidx::subscriber::UpdatesFlags,
        _path: netidx::path::Path,
        _ref_by: ExprId,
    ) -> netidx::subscriber::Dval {
        unimplemented!()
    }

    fn unsubscribe(
        &mut self,
        _path: netidx::path::Path,
        _dv: netidx::subscriber::Dval,
        _ref_by: ExprId,
    ) {
        unimplemented!()
    }

    fn set_timer(&mut self, _id: BindId, _timeout: std::time::Duration, _ref_by: ExprId) {
        unimplemented!()
    }

    fn ref_var(&mut self, id: BindId, ref_by: ExprId) {
        let refs = self.by_ref.entry(id).or_default();
        if !refs.contains(&ref_by) {
            refs.push(ref_by);
        }
    }

    fn unref_var(&mut self, _id: BindId, _ref_by: ExprId) {
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
    ctx: ExecCtx<TestCtx, ()>,
}

impl TestState {
    async fn new() -> Result<Self> {
        Ok(Self { ctx: ExecCtx::new(TestCtx::new().await?) })
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
    i64 as v if v == 1 => "first [v]",
    _ as v if v == 2 => "second [v]",
    _ as v => "third [v]"
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
    Ok(Value::Bool(true)) => true,
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

const FUNCTION_TYPES: &str = r#"
{
  let f = |x: Number, y: Number| -> string "x is [x] and y is [y]";
  f("foo", 3)
}
"#;

run!(function_types, FUNCTION_TYPES, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const PARTIAL_FUNCTION_TYPES: &str = r#"
{
  let f = |x: Number, y| "x is [x] and y is [y]";
  f("foo", 3)
}
"#;

run!(partial_function_types, PARTIAL_FUNCTION_TYPES, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const FUNCTION_RTYPE: &str = r#"
{
  let f = |x, y| -> Number "x is [x] and y is [y]";
  f("foo", 3)
}
"#;

run!(function_rtype, FUNCTION_RTYPE, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const INFERRED_RTYPE: &str = r#"
{
  let f = |x, y| "x is [x] and y is [y]";
  let v = f("foo", 3);
  let g = |x| x + 1;
  g(v)
}
"#;

run!(inferred_rtype, INFERRED_RTYPE, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const LAMBDA_CONSTRAINT: &str = r#"
{
  let f = |f: fn(string, string) -> string, a| f("foo", a);
  f(|x, y: Number| "[x] and [y]", "foo")
}
"#;

run!(lambda_constraint, LAMBDA_CONSTRAINT, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const LOOPING_SELECT: &str = r#"
{
  let v: [Number, string, error] = "1";
  let v = select v {
    Number as i => i,
    string as s => v <- cast<i64>(s),
    error as e => never(e)
  };
  v + 1
}
"#;

run!(looping_select, LOOPING_SELECT, |v: Result<&Value>| match v {
    Ok(Value::I64(2)) => true,
    _ => false,
});

const LABELED_ARGS: &str = r#"
{
  let f = |#foo: Number, #bar: Number = 42| foo + bar;
  f(#foo: 0)
}
"#;

run!(labeled_args, LABELED_ARGS, |v: Result<&Value>| match v {
    Ok(Value::I64(42)) => true,
    _ => false,
});

const REQUIRED_ARGS: &str = r#"
{
  let f = |#foo: Number, #bar: Number = 42| foo + bar;
  f(#bar: 0)
}
"#;

run!(required_args, REQUIRED_ARGS, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const MIXED_ARGS: &str = r#"
{
  let f = |#foo: Number, #bar: Number = 42, baz| foo + bar + baz;
  f(#foo: 0, 0)
}
"#;

run!(mixed_args, MIXED_ARGS, |v: Result<&Value>| match v {
    Ok(Value::I64(42)) => true,
    _ => false,
});

const ARG_SUBTYPING: &str = r#"
{
  let f = |#foo: Number, #bar: Number = 42| foo + bar;
  let g = |f: fn(#foo: Number) -> Number| f(#foo: 3);
  g(f)
}
"#;

run!(arg_subtyping, ARG_SUBTYPING, |v: Result<&Value>| match v {
    Ok(Value::I64(45)) => true,
    _ => false,
});

const ARG_NAME_SHORT: &str = r#"
{
  let f = |#foo: Number, #bar: Number = 42| foo + bar;
  let foo = 3;
  f(#foo)
}
"#;

run!(arg_name_short, ARG_NAME_SHORT, |v: Result<&Value>| match v {
    Ok(Value::I64(45)) => true,
    _ => false,
});

const EXPLICIT_TYPE_VARS0: &str = r#"
{
  let f = 'a: Number |x: 'a, y: 'a| -> 'a x + y;
  f("foo", "bar")
}
"#;

run!(explicit_type_vars0, EXPLICIT_TYPE_VARS0, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const EXPLICIT_TYPE_VARS1: &str = r#"
{
  let f = 'a: Number |x: 'a, y: 'a| -> 'a x + y;
  f(u32:1, i64:2)
}
"#;

run!(explicit_type_vars1, EXPLICIT_TYPE_VARS1, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const EXPLICIT_TYPE_VARS2: &str = r#"
{
  let f = 'a: Number |x: 'a, y: 'a| -> 'a x + y;
  select f(1, 1) {
    i64 as t => t,
    _ as t => error("unexpected [t]")
  }
}
"#;

run!(explicit_type_vars2, EXPLICIT_TYPE_VARS2, |v: Result<&Value>| match v {
    Ok(Value::I64(2)) => true,
    _ => false,
});

const EXPLICIT_TYPE_VARS3: &str = r#"
{
  let f = 'a: Number, 'b: Number |x: 'a, y: 'b| -> ['a, 'b] x + y;
  select f(u32:1, u64:1) {
    [u32, u64] as t => t,
    _ as t => error("unexpected [t]")
  }
}
"#;

run!(explicit_type_vars3, EXPLICIT_TYPE_VARS3, |v: Result<&Value>| match v {
    Ok(Value::U32(2) | Value::U64(2)) => true,
    _ => false,
});

const TYPED_ARRAYS: &str = r#"
{
  let f = |x: Array<'a>, y: Array<'a>| -> Array<Array<'a>> [x, y];
  f([1, 2, 3], [1, 2, 3])
}
"#;

run!(typed_arrays, TYPED_ARRAYS, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &**a {
        [Value::Array(a0), Value::Array(a1)] => match (&**a0, &**a1) {
            (
                [Value::I64(1), Value::I64(2), Value::I64(3)],
                [Value::I64(1), Value::I64(2), Value::I64(3)],
            ) => true,
            _ => false,
        },
        _ => false,
    },
    _ => false,
});
