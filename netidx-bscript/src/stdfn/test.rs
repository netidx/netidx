use crate::run;
use anyhow::bail;
use anyhow::Result;
use netidx::subscriber::Value;

#[cfg(test)]
const IS_ERR: &str = r#"
{
  let a = [42, 43, 44];
  let y = a[0]? + a[3]?;
  is_err(errors)
}
"#;

#[cfg(test)]
run!(is_err, IS_ERR, |v: Result<&Value>| match v {
    Ok(Value::Bool(b)) => *b,
    _ => false,
});

#[cfg(test)]
const FILTER_ERR: &str = r#"
{
  let a = [42, 43, 44, error("foo")];
  filter_err(array::iter(a))
}
"#;

#[cfg(test)]
run!(filter_err, FILTER_ERR, |v: Result<&Value>| match v {
    Ok(Value::Error(_)) => true,
    _ => false,
});

#[cfg(test)]
const ERROR: &str = r#"
{
  error("foo")
}
"#;

#[cfg(test)]
run!(error, ERROR, |v: Result<&Value>| match v {
    Ok(Value::Error(_)) => true,
    _ => false,
});

#[cfg(test)]
const ONCE: &str = r#"
{
  let x = [1, 2, 3, 4, 5, 6];
  once(array::iter(x))
}
"#;

#[cfg(test)]
run!(once, ONCE, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});

#[cfg(test)]
const ALL: &str = r#"
{
  let x = 1;
  let y = x;
  let z = y;
  all(x, y, z)
}
"#;

#[cfg(test)]
run!(all, ALL, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});

#[cfg(test)]
const SUM: &str = r#"
{
  let tweeeeenywon = [1, 2, 3, 4, 5, 6];
  sum(tweeeeenywon)
}
"#;

#[cfg(test)]
run!(sum, SUM, |v: Result<&Value>| match v {
    Ok(Value::I64(21)) => true,
    _ => false,
});

#[cfg(test)]
const PRODUCT: &str = r#"
{
  let tweeeeenywon = [5, 2, 2, 1.05];
  product(tweeeeenywon)
}
"#;

#[cfg(test)]
run!(product, PRODUCT, |v: Result<&Value>| match v {
    Ok(Value::F64(21.0)) => true,
    _ => false,
});

#[cfg(test)]
const DIVIDE: &str = r#"
{
  let tweeeeenywon = [84, 2, 2];
  divide(tweeeeenywon)
}
"#;

#[cfg(test)]
run!(divide, DIVIDE, |v: Result<&Value>| match v {
    Ok(Value::I64(21)) => true,
    _ => false,
});

#[cfg(test)]
const MIN: &str = r#"
{
   min(1, 2, 3, 4, 5, 6, 0)
}
"#;

#[cfg(test)]
run!(min, MIN, |v: Result<&Value>| match v {
    Ok(Value::I64(0)) => true,
    _ => false,
});

#[cfg(test)]
const MAX: &str = r#"
{
   max(1, 2, 3, 4, 5, 6, 0)
}
"#;

#[cfg(test)]
run!(max, MAX, |v: Result<&Value>| match v {
    Ok(Value::I64(6)) => true,
    _ => false,
});

#[cfg(test)]
const AND: &str = r#"
{
  let x = 1;
  let y = x + 1;
  let z = y + 1;
  and(x < y, y < z, x > 0, z < 10)
}
"#;

#[cfg(test)]
run!(and, AND, |v: Result<&Value>| match v {
    Ok(Value::Bool(true)) => true,
    _ => false,
});

#[cfg(test)]
const OR: &str = r#"
{
  or(false, false, true)
}
"#;

#[cfg(test)]
run!(or, OR, |v: Result<&Value>| match v {
    Ok(Value::Bool(true)) => true,
    _ => false,
});

#[cfg(test)]
const INDEX: &str = r#"
{
  let a = ["foo", "bar", 1, 2, 3];
  cast<i64>(a[2]?)? + cast<i64>(a[3]?)?
}
"#;

#[cfg(test)]
run!(index, INDEX, |v: Result<&Value>| match v {
    Ok(Value::I64(3)) => true,
    _ => false,
});

#[cfg(test)]
const SLICE: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  [sum(a[2..4]?), sum(a[6..]?), sum(a[..2]?)]
}
"#;

#[cfg(test)]
run!(slice, SLICE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(7), Value::I64(15), Value::I64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const FILTER: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  filter(array::iter(a), |x| x > 7)
}
"#;

#[cfg(test)]
run!(filter, FILTER, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(8)) => true,
        _ => false,
    }
});

#[cfg(test)]
const QUEUE: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  array::map(a, |v| net::publish("/local/[v]", v));
  let v = array::iter(a);
  let trigger = once(v);
  let q = queue(#trigger, v);
  let out = net::subscribe("/local/[q]")?;
  trigger <- out;
  array::group(out, |n, _| n == u64:8)
}
"#;

#[cfg(test)]
run!(queue, QUEUE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4), Value::I64(5), Value::I64(6), Value::I64(7), Value::I64(8)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const COUNT: &str = r#"
{
  let a = [0, 1, 2, 3];
  array::group(count(array::iter(a)), |n, _| n == u64:4)
}
"#;

#[cfg(test)]
run!(count, COUNT, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::U64(1), Value::U64(2), Value::U64(3), Value::U64(4)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const SAMPLE: &str = r#"
{
  let a = [0, 1, 2, 3];
  let x = "tweeeenywon!";
  array::group(sample(#trigger:array::iter(a), x), |n, _| n == u64:4)
}
"#;

#[cfg(test)]
run!(sample, SAMPLE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1), Value::String(s2), Value::String(s3)] => {
                s0 == s1 && s1 == s2 && s2 == s3 && &**s3 == "tweeeenywon!"
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const UNIQ: &str = r#"
{
  let a = [1, 1, 1, 1, 1, 1, 1];
  uniq(array::iter(a))
}
"#;

#[cfg(test)]
run!(uniq, UNIQ, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(1)) => true,
        _ => false,
    }
});

#[cfg(test)]
const SEQ: &str = r#"
{
  array::group(seq(u64:4), |n, _| n == u64:4)
}
"#;

#[cfg(test)]
run!(seq, SEQ, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::U64(0), Value::U64(1), Value::U64(2), Value::U64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const NEVER: &str = r#"
{
   let x = never(100);
   any(x, 0)
}
"#;

#[cfg(test)]
run!(never, NEVER, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(0)) => true,
        _ => false,
    }
});

#[cfg(test)]
const MEAN: &str = r#"
{
  let a = [0, 1, 2, 3];
  mean(a)
}
"#;

#[cfg(test)]
run!(mean, MEAN, |v: Result<&Value>| {
    match v {
        Ok(Value::F64(1.5)) => true,
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_MAP: &str = r#"
{
  let a = [1, 2, 3, 4];
  array::map(a, |x| x > 3)
}
"#;

#[cfg(test)]
run!(array_map, ARRAY_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::Bool(false), Value::Bool(false), Value::Bool(false), Value::Bool(true)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FILTER: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  array::filter(a, |x| x > 3)
}
"#;

#[cfg(test)]
run!(array_filter, ARRAY_FILTER, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(4), Value::I64(5), Value::I64(6), Value::I64(7), Value::I64(8)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FLAT_MAP: &str = r#"
{
  let a = [1, 2];
  array::flat_map(a, |x| [x, x + 1])
}
"#;

#[cfg(test)]
run!(array_flat_map, ARRAY_FLAT_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(2), Value::I64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FILTER_MAP: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  array::filter_map(a, |x: i64| -> [i64, null] select x > 5 {
    true => x + 1,
    false => sample(#trigger: x, null)
  })
}
"#;

#[cfg(test)]
run!(array_filter_map, ARRAY_FILTER_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(7), Value::I64(8), Value::I64(9)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FIND: &str = r#"
{
  type T = (string, i64);
  let a: Array<T> = [("foo", 1), ("bar", 2), ("baz", 3)];
  array::find(a, |(k, _): T| k == "bar")
}
"#;

#[cfg(test)]
run!(array_find, ARRAY_FIND, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s), Value::I64(2)] => &**s == "bar",
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FIND_MAP: &str = r#"
{
  type T = (string, i64);
  let a: Array<T> = [("foo", 1), ("bar", 2), ("baz", 3)];
  array::find_map(a, |(k, v): T| -> [i64, null] select k == "bar" {
    true => v,
    false => sample(#trigger:v, null)
  })
}
"#;

#[cfg(test)]
run!(array_find_map, ARRAY_FIND_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(2)) => true,
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_ITER: &str = r#"
{
   filter(array::iter([1, 2, 3, 4]), |x| x == 4)
}
"#;

#[cfg(test)]
run!(array_iter, ARRAY_ITER, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(4)) => true,
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_ITERQ: &str = r#"
{
   let a = [1, 2, 3, 4];
   a <- [5, 6, 7, 8];
   let trigger = once(null);
   let v = array::iterq(#trigger, a);
   trigger <- v;
   filter(v, |x| x == 8)
}
"#;

#[cfg(test)]
run!(array_iterq, ARRAY_ITERQ, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(8)) => true,
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FOLD: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  array::fold(a, 0, |acc, x| x + acc)
}
"#;

#[cfg(test)]
run!(array_fold, ARRAY_FOLD, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(55)) => true,
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_CONCAT: &str = r#"
{
  array::concat([1, 2, 3], [4, 5], [6])
}
"#;

#[cfg(test)]
run!(array_concat, ARRAY_CONCAT, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4), Value::I64(5), Value::I64(6)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_LEN: &str = r#"
{
  use core::array;
  len(concat([1, 2, 3], [4, 5], [6]))
}
"#;

#[cfg(test)]
run!(array_len, ARRAY_LEN, |v: Result<&Value>| {
    match v {
        Ok(Value::U64(6)) => true,
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FLATTEN: &str = r#"
{
  array::flatten([[1, 2, 3], [4, 5], [6]])
}
"#;

#[cfg(test)]
run!(array_flatten, ARRAY_FLATTEN, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4), Value::I64(5), Value::I64(6)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_GROUP: &str = r#"
{
   let x = 1;
   let y = x + 1;
   let z = y + 1;
   array::group(any(x, y, z), |_, v| v == 3)
}
"#;

#[cfg(test)]
run!(array_group, ARRAY_GROUP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const STR_STARTS_WITH: &str = r#"
{
  str::starts_with(#pfx:"foo", "foobarbaz")
}
"#;

#[cfg(test)]
run!(str_starts_with, STR_STARTS_WITH, |v: Result<&Value>| {
    match v {
        Ok(Value::Bool(true)) => true,
        _ => false,
    }
});

#[cfg(test)]
const STR_ENDS_WITH: &str = r#"
{
  str::ends_with(#sfx:"baz", "foobarbaz")
}
"#;

#[cfg(test)]
run!(str_ends_with, STR_ENDS_WITH, |v: Result<&Value>| {
    match v {
        Ok(Value::Bool(true)) => true,
        _ => false,
    }
});

#[cfg(test)]
const STR_CONTAINS: &str = r#"
{
  str::contains(#part:"bar", "foobarbaz")
}
"#;

#[cfg(test)]
run!(str_contains, STR_CONTAINS, |v: Result<&Value>| {
    match v {
        Ok(Value::Bool(true)) => true,
        _ => false,
    }
});

#[cfg(test)]
const STR_STRIP_PREFIX: &str = r#"
{
  str::strip_prefix(#pfx:"foo", "foobarbaz")
}
"#;

#[cfg(test)]
run!(str_strip_prefix, STR_STRIP_PREFIX, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "barbaz",
        _ => false,
    }
});

#[cfg(test)]
const STR_STRIP_SUFFIX: &str = r#"
{
  str::strip_suffix(#sfx:"baz", "foobarbaz")
}
"#;

#[cfg(test)]
run!(str_strip_suffix, STR_STRIP_SUFFIX, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "foobar",
        _ => false,
    }
});
