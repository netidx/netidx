#[cfg(test)]
const ARRAY_MAP: &str = r#"
{
  let a = [1, 2, 3, 4];
  array::mapq(a, |x| x > 3)
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
  array::filter_map(a, |x| select x > 5 {
    true => x + 1,
    false => sample(x, null)
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
    false => sample(v, null)
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
const ITER: &str = r#"
{
   filter(array::iter([1, 2, 3, 4]), |x| x == 4)
}
"#;

#[cfg(test)]
run!(iter, ITER, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(4)) => true,
        _ => false,
    }
});
