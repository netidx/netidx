use crate::rt::{BSConfigBuilder, BSHandle, RtEvent};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use futures::{channel::mpsc, StreamExt};
use netidx::{
    publisher::{PublisherBuilder, Value},
    resolver_server,
    subscriber::SubscriberBuilder,
};

pub struct TestCtx {
    pub _resolver: resolver_server::Server,
    pub rt: BSHandle,
}

pub async fn init() -> Result<TestCtx> {
    let _ = env_logger::try_init();
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
    Ok(TestCtx {
        _resolver: resolver,
        rt: BSConfigBuilder::default()
            .publisher(publisher)
            .subscriber(subscriber)
            .build()?
            .start(),
    })
}

#[tokio::test(flavor = "current_thread")]
async fn bind_ref_arith() -> Result<()> {
    let ctx = init().await?;
    let bs = ctx.rt;
    let e = r#"
{
  let v = (((1 + 1) * 2) / 2) - 1;
  v
}
"#;
    let (tx, mut rx) = mpsc::channel(10);
    bs.subscribe(tx)?;
    let eid = bs.compile(ArcStr::from(e)).await?.exprs[0].id;
    match rx.next().await {
        None => bail!("runtime died"),
        Some(RtEvent::Updated(id, v)) => {
            assert_eq!(id, eid);
            assert_eq!(v, Value::I64(1))
        }
    }
    Ok(())
}

#[macro_export]
macro_rules! run {
    ($name:ident, $code:expr, $pred:expr) => {
        #[tokio::test(flavor = "current_thread")]
        async fn $name() -> ::anyhow::Result<()> {
            let ctx = $crate::tests::init().await?;
            let bs = ctx.rt;
            let (tx, mut rx) = futures::channel::mpsc::channel(10);
            bs.subscribe(tx)?;
            match bs.compile(arcstr::ArcStr::from($code)).await.map(|r| r.exprs[0].id) {
                Err(e) => assert!($pred(dbg!(Err(e)))),
                Ok(eid) => {
                    dbg!("compilation succeeded");
                    match futures::StreamExt::next(&mut rx).await {
                        None => bail!("runtime died"),
                        Some($crate::rt::RtEvent::Updated(id, v)) => {
                            assert_eq!(id, eid);
                            assert!($pred(dbg!(Ok(&v))))
                        }
                    }
                }
            }
            Ok(())
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
  str::join(#sep:", ", "foo", "bar", z)
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
  let g = |f: fn(Number) -> Number, y| f(y) + 1;
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
  array::group(s, |n, x| n == u64:3)
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
    t => error("unexpected [t]")
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
    t => error("unexpected [t]")
  }
}
"#;

run!(explicit_type_vars3, EXPLICIT_TYPE_VARS3, |v: Result<&Value>| match v {
    Ok(Value::U32(2) | Value::U64(2)) => true,
    _ => false,
});

const TYPED_ARRAYS0: &str = r#"
{
  let f = |x: Array<'a>, y: Array<'a>| -> Array<Array<'a>> [x, y];
  f([1, 2, 3], [1, 2, 3])
}
"#;

run!(typed_arrays0, TYPED_ARRAYS0, |v: Result<&Value>| match v {
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

const TYPED_ARRAYS1: &str = r#"
{
  let f = |x: Array<'a>, y: Array<'a>| -> Array<Array<'a>> [x, y];
  f([1, 2, 3], [u32:1, 2, 3])
}
"#;

run!(typed_arrays1, TYPED_ARRAYS1, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const ARRAY_INDEXING0: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  a[0]
}
"#;

run!(array_indexing0, ARRAY_INDEXING0, |v: Result<&Value>| match v {
    Ok(Value::I64(0)) => true,
    _ => false,
});

const ARRAY_INDEXING1: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  a[0..3]
}
"#;

run!(array_indexing1, ARRAY_INDEXING1, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) if &a[..] == [Value::I64(0), Value::I64(1), Value::I64(2)] =>
        true,
    _ => false,
});

const ARRAY_INDEXING2: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  a[..2]
}
"#;

run!(array_indexing2, ARRAY_INDEXING2, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) if &a[..] == [Value::I64(0), Value::I64(1)] => true,
    _ => false,
});

const ARRAY_INDEXING3: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  a[5..]
}
"#;

run!(array_indexing3, ARRAY_INDEXING3, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) if &a[..] == [Value::I64(5), Value::I64(6)] => true,
    _ => false,
});

const ARRAY_INDEXING4: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  a[..]
}
"#;

run!(array_indexing4, ARRAY_INDEXING4, |v: Result<&Value>| match v {
    Ok(Value::Array(a))
        if &a[..]
            == [
                Value::I64(0),
                Value::I64(1),
                Value::I64(2),
                Value::I64(3),
                Value::I64(4),
                Value::I64(5),
                Value::I64(6)
            ] =>
        true,
    _ => false,
});

const ARRAY_INDEXING5: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  let out = select array::iter(a) {
    i64 as i => a[i] + 1
  };
  array::group(out, |i, x| i == u64:7)
}
"#;

run!(array_indexing5, ARRAY_INDEXING5, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const ARRAY_INDEXING6: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  let out = select array::iter(a) {
    i64 as i => a[i]? + 1
  };
  array::group(out, |i, x| i == u64:7)
}
"#;

run!(array_indexing6, ARRAY_INDEXING6, |v: Result<&Value>| match v {
    Ok(Value::Array(a))
        if &a[..]
            == [
                Value::I64(1),
                Value::I64(2),
                Value::I64(3),
                Value::I64(4),
                Value::I64(5),
                Value::I64(6),
                Value::I64(7)
            ] =>
        true,
    _ => false,
});

const ARRAY_MATCH0: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6];
  select a {
    Array<i64> as [a, b, c, d, ..] => a + b + c + d
  }
}
"#;

run!(array_match0, ARRAY_MATCH0, |v: Result<&Value>| match v {
    Ok(Value::I64(6)) => true,
    _ => false,
});

const ARRAY_MATCH1: &str = r#"
{
  let a = [0, 1, 2, 3, 4, 5, 6, 7];
  let out = select a {
    Array<i64> as [x, y, tl..] => {
      a <- tl;
      [x, y]
    }
  };
  array::group(out, |i, x| i == u64:4)
}
"#;

run!(array_match1, ARRAY_MATCH1, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => {
        a.len() == 4 && {
            a.iter().enumerate().all(|(i, a)| match a {
                Value::Array(a) => {
                    a.len() == 2
                        && match &a[0] {
                            Value::I64(x) => *x as usize == i * 2,
                            _ => false,
                        }
                        && match &a[1] {
                            Value::I64(x) => *x as usize == i * 2 + 1,
                            _ => false,
                        }
                }
                _ => false,
            })
        }
    }
    _ => false,
});

const TUPLES0: &str = r#"
{
  let t: (string, Number, Number) = ("foo", 42, 23.5);
  t
}
"#;

run!(tuples0, TUPLES0, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &a[..] {
        [Value::String(s), Value::I64(42), Value::F64(23.5)] => &*s == "foo",
        _ => false,
    },
    _ => false,
});

const TUPLES1: &str = r#"
{
  let t: (string, Number, Number) = ("foo", 42, 23.5);
  let (_, y, z) = t;
  y + z
}
"#;

run!(tuples1, TUPLES1, |v: Result<&Value>| match v {
    Ok(Value::F64(65.5)) => true,
    _ => false,
});

const TUPLES2: &str = r#"
{
  type T = (string, i64, f64);
  let t: T = ("foo", 42, 23.5);
  select t {
    T as ("foo", x, y) => x + y
  }
}
"#;

run!(tuples2, TUPLES2, |v: Result<&Value>| match v {
    Ok(Value::F64(65.5)) => true,
    _ => false,
});

const STRUCTS0: &str = r#"
{
  let x = { foo: "bar", bar: 42, baz: 84.0 };
  x
}
"#;

run!(structs0, STRUCTS0, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) if a.len() == 3 => match &a[..] {
        [Value::Array(f0), Value::Array(f1), Value::Array(f2)]
            if f0.len() == 2 && f1.len() == 2 && f2.len() == 2 =>
        {
            let f0 = match &f0[..] {
                [Value::String(n), Value::I64(42)] if n == "bar" => true,
                _ => false,
            };
            let f1 = match &f1[..] {
                [Value::String(n), Value::F64(84.0)] if n == "baz" => true,
                _ => false,
            };
            let f2 = match &f2[..] {
                [Value::String(n), Value::String(s)] if n == "foo" && s == "bar" => true,
                _ => false,
            };
            f0 && f1 && f2
        }
        _ => false,
    },
    _ => false,
});

const BINDSTRUCT: &str = r#"
{
  let x = { foo: "bar", bar: 42, baz: 84.0 };
  let { foo: _, bar, baz } = x;
  bar + baz
}
"#;

run!(bindstruct, BINDSTRUCT, |v: Result<&Value>| match v {
    Ok(Value::F64(126.0)) => true,
    _ => false,
});

const SELECTSTRUCT: &str = r#"
{
  type T = { foo: string, bar: i64, baz: f64 };
  let x = { foo: "bar", bar: 42, baz: 84.0 };
  select x {
    T as { foo: "foo", bar: 8, baz } => baz,
    T as { bar, baz, .. } => bar + baz
  }
}
"#;

run!(selectstruct, SELECTSTRUCT, |v: Result<&Value>| match v {
    Ok(Value::F64(126.0)) => true,
    _ => false,
});

const STRUCTACCESSOR: &str = r#"
{
  let x = { foo: "bar", bar: 42, baz: 84.0 };
  x.foo
}
"#;

run!(structaccessor, STRUCTACCESSOR, |v: Result<&Value>| match v {
    Ok(Value::String(s)) => s == "bar",
    _ => false,
});

const TUPLEACCESSOR: &str = r#"
{
  let x = ( "bar", 42, 84.0 );
  x.1
}
"#;

run!(tupleaccessor, TUPLEACCESSOR, |v: Result<&Value>| match v {
    Ok(Value::I64(42)) => true,
    _ => false,
});

const STRUCTWITH0: &str = r#"
{
  let x = { foo: "bar", bar: 42, baz: 84.0 };
  let x = { x with foo: 1 };
  x.foo
}
"#;

run!(structwith0, STRUCTWITH0, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

const STRUCTWITH1: &str = r#"
{
  let x = { foo: "bar", bar: 42, baz: 84.0 };
  let x = { x with bar: 1 };
  x.bar + x.baz
}
"#;

run!(structwith1, STRUCTWITH1, |v: Result<&Value>| match v {
    Ok(Value::F64(85.0)) => true,
    _ => false,
});

const NESTEDMATCH0: &str = r#"
{
  type T = { foo: (string, i64, f64), bar: i64, baz: f64 };
  let x = { foo: ("bar", 42, 5.0), bar: 42, baz: 84.0 };
  let { foo: (_, x, y), .. }: T = x;
  x + y
}
"#;

run!(nestedmatch0, NESTEDMATCH0, |v: Result<&Value>| match v {
    Ok(Value::F64(47.0)) => true,
    _ => false,
});

const NESTEDMATCH1: &str = r#"
{
  type T = { foo: {x: string, y: i64, z: f64}, bar: i64, baz: f64 };
  let x = { foo: { x: "bar", y: 42, z: 5.0 }, bar: 42, baz: 84.0 };
  select x {
    T as { foo: { y, z, .. }, .. } => y + z
  }
}
"#;

run!(nestedmatch1, NESTEDMATCH1, |v: Result<&Value>| match v {
    Ok(Value::F64(47.0)) => true,
    _ => false,
});

const NESTEDMATCH2: &str = r#"
{
  type T = { foo: Array<f64>, bar: i64, baz: f64 };
  let x = { foo: [ 1.0, 2.0, 4.3, 55.23 ], bar: 42, baz: 84.0 };
  let { foo: [x, y, ..], ..}: T = x;
  x + y
}
"#;

run!(nestedmatch2, NESTEDMATCH2, |v: Result<&Value>| match v {
    Err(e) => {
        dbg!(e);
        true
    }
    _ => false,
});

const NESTEDMATCH3: &str = r#"
{
  type T = { foo: Array<f64>, bar: i64, baz: f64 };
  let x = { foo: [ 1.0, 2.0, 4.3, 55.23 ], bar: 42, baz: 84.0 };
  select x {
    T as { foo: [x, y, ..], bar: _, baz: _ } => x + y
  }
}
"#;

run!(nestedmatch3, NESTEDMATCH3, |v: Result<&Value>| match v {
    Ok(Value::F64(3.0)) => true,
    _ => false,
});

const LAMBDAMATCH0: &str = r#"
{
  type T = { foo: Array<f64>, bar: i64, baz: f64 };
  let x = { foo: [ 1.0, 2.0, 4.3, 55.23 ], bar: 42, baz: 84.0 };
  let f = |{foo, ..}: T| foo[0]? + foo[1]?;
  f(x)
}
"#;

run!(lambdamatch0, LAMBDAMATCH0, |v: Result<&Value>| match v {
    Ok(Value::F64(3.0)) => true,
    _ => false,
});

#[cfg(test)]
const ANY0: &str = r#"
{
  let x = 1;
  let y = x + 1;
  let z = y + 1;
  array::group(any(x, y, z), |n, _| n == u64:3)
}
"#;

#[cfg(test)]
run!(any0, ANY0, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &a[..] {
        [Value::I64(1), Value::I64(2), Value::I64(3)] => true,
        _ => false,
    },
    _ => false,
});

#[cfg(test)]
const ANY1: &str = r#"
{
  let x = 1;
  let y = "[x] + 1";
  let z = [y, y];
  let r: [i64, string, Array<string>] = any(x, y, z);
  array::group(r, |n, _| n == u64:3)
}
"#;

#[cfg(test)]
run!(any1, ANY1, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &a[..] {
        [Value::I64(1), Value::String(s), Value::Array(a)] => {
            &**s == "i64:1 + 1"
                && match &a[..] {
                    [Value::String(s0), Value::String(s1)] => {
                        (&**s0 == &**s1) && &**s0 == "i64:1 + 1"
                    }
                    _ => false,
                }
        }
        _ => false,
    },
    _ => false,
});

#[cfg(test)]
const VARIANTS0: &str = r#"
{
  let a = select array::iter([`Foo, `Bar("hello world")]) {
    `Foo => 0,
    `Bar(s) if s == "hello world" => 1,
     _ => 2
  };
  array::group(a, |n, _| n == u64:2)
}
"#;

#[cfg(test)]
run!(variants0, VARIANTS0, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &a[..] {
        [Value::I64(0), Value::I64(1)] => true,
        _ => false,
    },
    _ => false,
});

#[cfg(test)]
const LATE_BINDING0: &str = r#"
{
  type T = { foo: string, bar: i64, f: fn(#x: i64, #y: i64) -> i64 };
  let t: T = { foo: "hello world", bar: 3, f: |#x: i64, #y: i64| x - y };
  let u: T = { foo: "hello foo", bar: 42, f: |#c: i64 = 1, #y: i64, #x: i64| x - y + c };
  let f = t.f;
  f(#y: 3, #x: 4)
}
"#;

#[cfg(test)]
run!(late_binding0, LATE_BINDING0, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});

#[cfg(test)]
const LATE_BINDING1: &str = r#"
{
  type F = fn(#x: i64, #y: i64) -> i64;
  type T = { foo: string, bar: i64, f: F };
  let t: T = { foo: "hello world", bar: 3, f: |#x: i64, #y: i64| x - y };
  let u: T = { foo: "hello foo", bar: 42, f: |#c: i64 = 1, #y: i64, #x: i64| (x - y) + c };
  let f: F = select array::iter([0, 1]) {
    0 => t.f,
    1 => u.f,
    _ => never()
  };
  array::group(f(#y: 3, #x: 4), |n, _| n == u64:2)
}
"#;

#[cfg(test)]
run!(late_binding1, LATE_BINDING1, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &a[..] {
        [Value::I64(1), Value::I64(2)] => true,
        _ => false,
    },
    _ => false,
});

#[cfg(test)]
const LATE_BINDING2: &str = r#"
{
  type T = { foo: string, bar: i64, f: fn(#x: i64, #y: i64) -> i64 };
  let t: T = { foo: "hello world", bar: 3, f: |#x: i64, #y: i64| x - y };
  (t.f)(#y: 3, #x: 4)
}
"#;

#[cfg(test)]
run!(late_binding2, LATE_BINDING2, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});

#[cfg(test)]
const RECTYPES0: &str = r#"
{
  type List = [
    `Cons(Any, List),
    `Nil
  ];
  let l: List = `Cons(42, `Cons(3, `Nil));
  l
}
"#;

#[cfg(test)]
run!(rectypes0, RECTYPES0, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &a[..] {
        [Value::String(s), Value::I64(42), Value::Array(a)] if &**s == "Cons" =>
            match &a[..] {
                [Value::String(s0), Value::I64(3), Value::String(s1)]
                    if &**s0 == "Cons" && s1 == "Nil" =>
                    true,
                _ => false,
            },
        _ => false,
    },
    _ => false,
});

#[cfg(test)]
const RECTYPES1: &str = r#"
{
  type List<'a> = [
    `Cons('a, List<'a>),
    `Nil
  ];
  let l: List<Any> = `Cons(42, `Cons(3, `Nil));
  l
}
"#;

#[cfg(test)]
run!(rectypes1, RECTYPES1, |v: Result<&Value>| match v {
    Ok(Value::Array(a)) => match &a[..] {
        [Value::String(s), Value::I64(42), Value::Array(a)] if &**s == "Cons" =>
            match &a[..] {
                [Value::String(s0), Value::I64(3), Value::String(s1)]
                    if &**s0 == "Cons" && s1 == "Nil" =>
                    true,
                _ => false,
            },
        _ => false,
    },
    _ => false,
});

#[cfg(test)]
const RECTYPES2: &str = r#"
{
  type List<'a> = [
    `Cons('a, List<'a>),
    `Nil
  ];
  let l: List<string> = `Cons(42, `Cons(3, `Nil));
  l
}
"#;

#[cfg(test)]
run!(rectypes2, RECTYPES2, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

#[cfg(test)]
const TYPEDEF_TVAR_ERR: &str = r#"
{
  type T<'a, 'b> = { foo: 'a, bar: 'b, baz: 'c };
  0
}
"#;

#[cfg(test)]
run!(typedef_tvar_err, TYPEDEF_TVAR_ERR, |v: Result<&Value>| match v {
    Err(_) => true,
    _ => false,
});

#[cfg(test)]
const TYPEDEF_TVAR_OK: &str = r#"
{
  type T<'a, 'b> = { foo: 'a, bar: 'b, f: fn('a, 'b, 'c) -> 'a };
  0
}
"#;

#[cfg(test)]
run!(typedef_tvar_ok, TYPEDEF_TVAR_OK, |v: Result<&Value>| match v {
    Ok(Value::I64(0)) => true,
    _ => false,
});

#[cfg(test)]
const BYREF_DEREF: &str = r#"
{
  let x = &42;
  *x
}
"#;

#[cfg(test)]
run!(byref_deref, BYREF_DEREF, |v: Result<&Value>| match v {
    Ok(Value::I64(42)) => true,
    _ => false,
});

#[cfg(test)]
const BYREF_TUPLE: &str = r#"
{
  let r = &(1, 2);
  let t = *r;
  t.0 + t.1
}
"#;

#[cfg(test)]
run!(byref_tuple, BYREF_TUPLE, |v: Result<&Value>| match v {
    Ok(Value::I64(3)) => true,
    _ => false,
});

#[cfg(test)]
const BYREF_PATTERN: &str = r#"
{
  let r = &42;
  select r as &i64 {
    _ => 1
  }
}
"#;

#[cfg(test)]
run!(byref_pattern, BYREF_PATTERN, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});
