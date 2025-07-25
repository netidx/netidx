use crate::run;
use anyhow::bail;
use anyhow::Result;
use arcstr::ArcStr;
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
  error("foo")
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
   min(1, 2, 3, 4, 5, 6, 0)
"#;

#[cfg(test)]
run!(min, MIN, |v: Result<&Value>| match v {
    Ok(Value::I64(0)) => true,
    _ => false,
});

#[cfg(test)]
const MAX: &str = r#"
   max(1, 2, 3, 4, 5, 6, 0)
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
  or(false, false, true)
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
  let clock: Any = once(v);
  let q = queue(#clock, v);
  let out = net::subscribe("/local/[q]")?;
  clock <- out;
  array::group(out, |n, _| n == 8)
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
  array::group(count(array::iter(a)), |n, _| n == 4)
}
"#;

#[cfg(test)]
run!(count, COUNT, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4)] => true,
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
  array::group(array::iter(a) ~ x, |n, _| n == 4)
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
  array::group(seq(0, 4), |n, _| n == 4)
"#;

#[cfg(test)]
run!(seq, SEQ, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(0), Value::I64(1), Value::I64(2), Value::I64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const THROTTLE: &str = r#"
{
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let data = throttle(array::iter(data));
    array::group(data, |n, _| n == 2)
}
"#;

#[cfg(test)]
run!(throttle, THROTTLE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(10)] => true,
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
const ARRAY_MAP0: &str = r#"
{
  let a = [1, 2, 3, 4];
  array::map(a, |x| x > 3)
}
"#;

#[cfg(test)]
run!(array_map0, ARRAY_MAP0, |v: Result<&Value>| {
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
const ARRAY_MAP1: &str = r#"
{
  let a = [1, 2];
  let b = [1, 2];
  array::map(a, |x| array::map(b, |y| x + y))
}
"#;

#[cfg(test)]
run!(array_map1, ARRAY_MAP1, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<[[i64; 2]; 2]>() {
            Ok([[2, 3], [3, 4]]) => true,
            _ => false,
        },
        Err(_) => false,
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
    false => x ~ null
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
  array::find_map(a, |(k, v): T| select k == "bar" {
    true => v,
    false => v ~ null
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
   filter(array::iter([1, 2, 3, 4]), |x| x == 4)
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
   let clock: Any = once(null);
   let v = array::iterq(#clock, a);
   clock <- v;
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
  array::fold(a, 0, |acc: i64, x: i64| x + acc)
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
  array::concat([1, 2, 3], [4, 5], [6])
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
const ARRAY_PUSH: &str = r#"
  array::push([(1, 2), (3, 4)], (5, 6))
"#;

#[cfg(test)]
run!(array_push, ARRAY_PUSH, |v: Result<&Value>| {
    match v.and_then(|v| v.clone().cast_to::<[(u64, u64); 3]>()) {
        Ok([(1, 2), (3, 4), (5, 6)]) => true,
        Ok(_) | Err(_) => false,
    }
});

#[cfg(test)]
const ARRAY_PUSH_FRONT: &str = r#"
  array::push_front([(1, 2), (3, 4)], (5, 6))
"#;

#[cfg(test)]
run!(array_push_front, ARRAY_PUSH_FRONT, |v: Result<&Value>| {
    match v.and_then(|v| v.clone().cast_to::<[(u64, u64); 3]>()) {
        Ok([(5, 6), (1, 2), (3, 4)]) => true,
        Ok(_) | Err(_) => false,
    }
});

#[cfg(test)]
const ARRAY_WINDOW0: &str = r#"
  array::window(#n:1, [(1, 2), (3, 4)], (5, 6))
"#;

#[cfg(test)]
run!(array_window0, ARRAY_WINDOW0, |v: Result<&Value>| {
    match v.and_then(|v| v.clone().cast_to::<[(u64, u64); 1]>()) {
        Ok([(5, 6)]) => true,
        Ok(_) | Err(_) => false,
    }
});

#[cfg(test)]
const ARRAY_WINDOW1: &str = r#"
  array::window(#n:2, [(1, 2), (3, 4)], (5, 6))
"#;

#[cfg(test)]
run!(array_window1, ARRAY_WINDOW1, |v: Result<&Value>| {
    match v.and_then(|v| v.clone().cast_to::<[(u64, u64); 2]>()) {
        Ok([(3, 4), (5, 6)]) => true,
        Ok(_) | Err(_) => false,
    }
});

#[cfg(test)]
const ARRAY_WINDOW2: &str = r#"
  array::window(#n:3, [(1, 2), (3, 4)], (5, 6))
"#;

#[cfg(test)]
run!(array_window2, ARRAY_WINDOW2, |v: Result<&Value>| {
    match v.and_then(|v| v.clone().cast_to::<[(u64, u64); 3]>()) {
        Ok([(1, 2), (3, 4), (5, 6)]) => true,
        Ok(_) | Err(_) => false,
    }
});

#[cfg(test)]
const ARRAY_LEN: &str = r#"
{
  use array;
  len(concat([1, 2, 3], [4, 5], [6]))
}
"#;

#[cfg(test)]
run!(array_len, ARRAY_LEN, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(6)) => true,
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_FLATTEN: &str = r#"
  array::flatten([[1, 2, 3], [4, 5], [6]])
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
const ARRAY_SORT0: &str = r#"
{
   let a = [5, 4, 3, 2, 1];
   array::sort(a)
}
"#;

#[cfg(test)]
run!(array_sort0, ARRAY_SORT0, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<[i64; 5]>() {
            Ok([1, 2, 3, 4, 5]) => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_SORT1: &str = r#"
{
   let a = [5, 4, 3, 2, 1];
   array::sort(#dir:`Descending, a)
}
"#;

#[cfg(test)]
run!(array_sort1, ARRAY_SORT1, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<[i64; 5]>() {
            Ok([5, 4, 3, 2, 1]) => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_SORT2: &str = r#"
{
   let a = ["5", "6", "50", "60", "40", "4", "3", "2", "1"];
   array::sort(#numeric:true, a)
}
"#;

#[cfg(test)]
run!(array_sort2, ARRAY_SORT2, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<[ArcStr; 9]>() {
            Ok([a0, a1, a2, a3, a4, a5, a6, a7, a8]) => {
                &*a0 == "1"
                    && &*a1 == "2"
                    && &*a2 == "3"
                    && &*a3 == "4"
                    && &*a4 == "5"
                    && &*a5 == "6"
                    && &*a6 == "40"
                    && &*a7 == "50"
                    && &*a8 == "60"
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_SORT3: &str = r#"
{
   let a = ["5", "6", "50", "60", "40", "4", "3", "2", "1"];
   array::sort(#dir:`Descending, #numeric:true, a)
}
"#;

#[cfg(test)]
run!(array_sort3, ARRAY_SORT3, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<[ArcStr; 9]>() {
            Ok([a0, a1, a2, a3, a4, a5, a6, a7, a8]) => {
                &*a0 == "60"
                    && &*a1 == "50"
                    && &*a2 == "40"
                    && &*a3 == "6"
                    && &*a4 == "5"
                    && &*a5 == "4"
                    && &*a6 == "3"
                    && &*a7 == "2"
                    && &*a8 == "1"
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_ENUMERATE: &str = r#"
{
   let a = [1, 2, 3];
   array::enumerate(a)
}
"#;

#[cfg(test)]
run!(array_enumerate, ARRAY_ENUMERATE, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<[(i64, i64); 3]>() {
            Ok([(0, 1), (1, 2), (2, 3)]) => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_ZIP: &str = r#"
{
   let a0 = [1, 2, 5];
   let a1 = [1, 2, 3];
   array::zip(a0, a1)
}
"#;

#[cfg(test)]
run!(array_zip, ARRAY_ZIP, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<[(i64, i64); 3]>() {
            Ok([(1, 1), (2, 2), (5, 3)]) => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const ARRAY_UNZIP: &str = r#"
{
   let a = [(1, 1), (2, 2), (5, 3)];
   array::unzip(a)
}
"#;

#[cfg(test)]
run!(array_unzip, ARRAY_UNZIP, |v: Result<&Value>| {
    match v {
        Ok(v) => match v.clone().cast_to::<([i64; 3], [i64; 3])>() {
            Ok(([1, 2, 5], [1, 2, 3])) => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const STR_STARTS_WITH: &str = r#"
  str::starts_with(#pfx:"foo", "foobarbaz")
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
  str::ends_with(#sfx:"baz", "foobarbaz")
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
  str::contains(#part:"bar", "foobarbaz")
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
  str::strip_prefix(#pfx:"foo", "foobarbaz")
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
  str::strip_suffix(#sfx:"baz", "foobarbaz")
"#;

#[cfg(test)]
run!(str_strip_suffix, STR_STRIP_SUFFIX, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "foobar",
        _ => false,
    }
});

#[cfg(test)]
const STR_TRIM: &str = r#"
  str::trim(" foobarbaz ")
"#;

#[cfg(test)]
run!(str_trim, STR_TRIM, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "foobarbaz",
        _ => false,
    }
});

#[cfg(test)]
const STR_TRIM_START: &str = r#"
  str::trim_start(" foobarbaz ")
"#;

#[cfg(test)]
run!(str_trim_start, STR_TRIM_START, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "foobarbaz ",
        _ => false,
    }
});

#[cfg(test)]
const STR_TRIM_END: &str = r#"
  str::trim_end(" foobarbaz ")
"#;

#[cfg(test)]
run!(str_trim_end, STR_TRIM_END, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == " foobarbaz",
        _ => false,
    }
});

#[cfg(test)]
const STR_REPLACE: &str = r#"
  str::replace(#pat:"foo", #rep:"baz", "foobarbazfoo")
"#;

#[cfg(test)]
run!(str_replace, STR_REPLACE, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "bazbarbazbaz",
        _ => false,
    }
});

#[cfg(test)]
const STR_DIRNAME: &str = r#"
  str::dirname("/foo/bar/baz")
"#;

#[cfg(test)]
run!(str_dirname, STR_DIRNAME, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "/foo/bar",
        _ => false,
    }
});

#[cfg(test)]
const STR_BASENAME: &str = r#"
  str::basename("/foo/bar/baz")
"#;

#[cfg(test)]
run!(str_basename, STR_BASENAME, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "baz",
        _ => false,
    }
});

#[cfg(test)]
const STR_JOIN: &str = r#"
  str::join(#sep:"/", "/foo", "bar", ["baz", "zam"])
"#;

#[cfg(test)]
run!(str_join, STR_JOIN, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "/foo/bar/baz/zam",
        _ => false,
    }
});

#[cfg(test)]
const STR_CONCAT: &str = r#"
  str::concat("foo", "bar", ["baz", "zam"])
"#;

#[cfg(test)]
run!(str_concat, STR_CONCAT, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "foobarbazzam",
        _ => false,
    }
});

#[cfg(test)]
const STR_ESCAPE: &str = r#"
  str::escape("/foo/bar")
"#;

#[cfg(test)]
run!(str_escape, STR_ESCAPE, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "\\/foo\\/bar",
        _ => false,
    }
});

#[cfg(test)]
const STR_UNESCAPE: &str = r#"
  str::unescape("\\/foo\\/bar")
"#;

#[cfg(test)]
run!(str_unescape, STR_UNESCAPE, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "/foo/bar",
        _ => false,
    }
});

#[cfg(test)]
const STR_SPLIT: &str = r#"
{
  let a = str::split(#pat:",", "foo, bar, baz");
  array::map(a, |s| str::trim(s))
}
"#;

#[cfg(test)]
run!(str_split, STR_SPLIT, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1), Value::String(s2)] => {
                s0 == "foo" && s1 == "bar" && s2 == "baz"
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const STR_SPLIT_ONCE: &str = r#"
  str::split_once(#pat:", ", "foo, bar, baz")
"#;

#[cfg(test)]
run!(str_split_once, STR_SPLIT_ONCE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1)] => s0 == "foo" && s1 == "bar, baz",
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const STR_RSPLIT_ONCE: &str = r#"
  str::rsplit_once(#pat:", ", "foo, bar, baz")
"#;

#[cfg(test)]
run!(str_rsplit_once, STR_RSPLIT_ONCE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1)] => s0 == "foo, bar" && s1 == "baz",
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const STR_TO_LOWER: &str = r#"
  str::to_lower("FOO")
"#;

#[cfg(test)]
run!(str_to_lower, STR_TO_LOWER, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "foo",
        _ => false,
    }
});

#[cfg(test)]
const STR_TO_UPPER: &str = r#"
  str::to_upper("foo")
"#;

#[cfg(test)]
run!(str_to_upper, STR_TO_UPPER, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) => s == "FOO",
        _ => false,
    }
});

#[cfg(test)]
const STR_LEN: &str = r#"
  str::len("foo")
"#;

#[cfg(test)]
run!(str_len, STR_LEN, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(3)) => true,
        _ => false,
    }
});

#[cfg(test)]
const STR_SUB: &str = r#"
  str::sub(#start:1, #len:2, "💗💖🍇")
"#;

#[cfg(test)]
run!(str_sub, STR_SUB, |v: Result<&Value>| {
    match v {
        Ok(Value::String(s)) if &*s == "💖🍇" => true,
        _ => false,
    }
});

#[cfg(test)]
const STR_PARSE: &str = r#"
  str::parse("42")
"#;

#[cfg(test)]
run!(str_parse, STR_PARSE, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(42)) => true,
        _ => false,
    }
});

#[cfg(test)]
const RE_IS_MATCH: &str = r#"
  re::is_match(#pat:r'[\\[\\]0-9]+', r'foo[0]')
"#;

#[cfg(test)]
run!(re_is_match, RE_IS_MATCH, |v: Result<&Value>| {
    match v {
        Ok(Value::Bool(true)) => true,
        _ => false,
    }
});

#[cfg(test)]
const RE_FIND: &str = r#"
  re::find(#pat:r'foo', r'foobarfoobazfoo')
"#;

#[cfg(test)]
run!(re_find, RE_FIND, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1), Value::String(s2)] => {
                s0 == "foo" && s0 == s1 && s0 == s2
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const RE_CAPTURES: &str = r#"
  re::captures(#pat:r'(fo)ob', r'foobarfoobazfoo')
"#;

#[cfg(test)]
run!(re_captures, RE_CAPTURES, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::Array(a0), Value::Array(a1)] => match (&a0[..], &a1[..]) {
                (
                    [Value::String(c00), Value::String(c01)],
                    [Value::String(c10), Value::String(c11)],
                ) => c00 == "foob" && c01 == "fo" && c10 == "foob" && c11 == "fo",
                _ => false,
            },
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const RE_SPLIT: &str = r#"
  re::split(#pat:r',\\s*', r'foo, bar, baz')
"#;

#[cfg(test)]
run!(re_split, RE_SPLIT, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1), Value::String(s2)] => {
                s0 == "foo" && s1 == "bar" && s2 == "baz"
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const RE_SPLITN: &str = r#"
  re::splitn(#pat:r',\\s*', #limit:2, r'foo, bar, baz')
"#;

#[cfg(test)]
run!(re_splitn, RE_SPLITN, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1)] => s0 == "foo" && s1 == "bar, baz",
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const NET_PUB_SUB: &str = r#"
{
  net::publish("/local/foo", 42);
  net::subscribe("/local/foo")?
}
"#;

#[cfg(test)]
run!(net_pub_sub, NET_PUB_SUB, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(42)) => true,
        _ => false,
    }
});

#[cfg(test)]
const NET_WRITE: &str = r#"
{
  let p = "/local/foo";
  let x = 42;
  net::publish(#on_write:|v| x <- cast<i64>(v)?, p, x);
  let s = cast<i64>(net::subscribe(p)?)?;
  net::write(p, once(s + 1));
  array::group(s, |n, _| n == 2)
}
"#;

#[cfg(test)]
run!(net_write, NET_WRITE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(42), Value::I64(43)] => true,
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const NET_LIST: &str = r#"
{
  net::publish("/local/foo", 42);
  net::publish("/local/bar", 42);
  net::list("/local")
}
"#;

#[cfg(test)]
run!(net_list, NET_LIST, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1)] => {
                let mut a = [s0, s1];
                a.sort();
                a[0] == "/local/bar" && a[1] == "/local/foo"
            }
            _ => false,
        },
        _ => false,
    }
});

#[cfg(test)]
const NET_LIST_TABLE: &str = r#"
{
  net::publish("/local/t/0/foo", 42);
  net::publish("/local/t/0/bar", 42);
  net::publish("/local/t/1/foo", 42);
  net::publish("/local/t/1/bar", 42);
  let t = dbg(net::list_table("/local/t"));
  let cols = array::map(t.columns, |(n, _): (string, _)| n);
  (array::sort(cols) == ["bar", "foo"])
  && (array::sort(t.rows) == ["/local/t/0", "/local/t/1"])
}
"#;

#[cfg(test)]
run!(net_list_table, NET_LIST_TABLE, |v: Result<&Value>| {
    match v {
        Ok(Value::Bool(true)) => true,
        _ => false,
    }
});

#[cfg(test)]
const NET_RPC: &str = r#"
{
  let get_val = "/local/get_val";
  let set_val = "/local/set_val";
  let v: Any = never();
  net::rpc(
    #path:get_val,
    #doc:"get the value",
    #spec:[],
    #f:|a| a ~ v);
  net::rpc(
    #path:set_val,
    #doc:"set the value",
    #spec:[{name: "val", doc: "The value", default: null}],
    #f:|args| select cast<{val: Any}>(args) {
        error as e => e,
        { val } => {
          v <- val;
          val ~ null
        }
    });
  net::call(set_val, [("val", 42)]);
  net::call(get_val, [])
}
"#;

#[cfg(test)]
run!(net_rpc, NET_RPC, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(42)) => true,
        _ => false,
    }
});

#[cfg(test)]
const RAND: &str = r#"
  rand::rand(#clock:null)
"#;

#[cfg(test)]
run!(rand, RAND, |v: Result<&Value>| {
    match v {
        Ok(Value::F64(v)) if *v >= 0. && *v < 1.0 => true,
        _ => false,
    }
});

#[cfg(test)]
const RAND_PICK: &str = r#"
  rand::pick(["Chicken is coming", "Grape", "Pilot!"])
"#;

#[cfg(test)]
run!(rand_pick, RAND_PICK, |v: Result<&Value>| {
    match v {
        Ok(Value::String(v)) => v == "Chicken is coming" || v == "Grape" || v == "Pilot!",
        _ => false,
    }
});

#[cfg(test)]
const RAND_SHUFFLE: &str = r#"
  rand::shuffle(["Chicken is coming", "Grape", "Pilot!"])
"#;

#[cfg(test)]
run!(rand_shuffle, RAND_SHUFFLE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) if a.len() == 3 => {
            a.contains(&Value::from("Chicken is coming"))
                && a.contains(&Value::from("Grape"))
                && a.contains(&Value::from("Pilot!"))
        }
        _ => false,
    }
});
