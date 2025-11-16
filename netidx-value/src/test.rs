use crate::{abstract_type::Abstract, array::ValArray, Map, PBytes, Typ, Value};
use anyhow::{anyhow, Result};
use arcstr::{literal, ArcStr};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use enumflags2::BitFlags;
use rust_decimal::Decimal;
use std::{
    fmt::Debug,
    ops::Bound,
    panic::{catch_unwind, AssertUnwindSafe},
    time::Duration,
};
use triomphe::Arc;

#[test]
fn value_typ_discriminants() {
    for t in BitFlags::<Typ>::all().iter() {
        match t {
            Typ::U8 => assert_eq!(t as u64, Value::U8(42).discriminant()),
            Typ::I8 => assert_eq!(t as u64, Value::I8(42).discriminant()),
            Typ::U16 => assert_eq!(t as u64, Value::U16(42).discriminant()),
            Typ::I16 => assert_eq!(t as u64, Value::I16(42).discriminant()),
            Typ::U32 => assert_eq!(t as u64, Value::U32(42).discriminant()),
            Typ::V32 => assert_eq!(t as u64, Value::V32(42).discriminant()),
            Typ::I32 => assert_eq!(t as u64, Value::I32(42).discriminant()),
            Typ::Z32 => assert_eq!(t as u64, Value::Z32(42).discriminant()),
            Typ::U64 => assert_eq!(t as u64, Value::U64(42).discriminant()),
            Typ::V64 => assert_eq!(t as u64, Value::V64(42).discriminant()),
            Typ::I64 => assert_eq!(t as u64, Value::I64(42).discriminant()),
            Typ::Z64 => assert_eq!(t as u64, Value::Z64(42).discriminant()),
            Typ::F32 => assert_eq!(t as u64, Value::F32(42.).discriminant()),
            Typ::F64 => assert_eq!(t as u64, Value::F64(42.).discriminant()),
            Typ::Decimal => {
                assert_eq!(
                    t as u64,
                    Value::Decimal(Arc::new(Decimal::MIN)).discriminant()
                )
            }
            Typ::DateTime => {
                assert_eq!(
                    t as u64,
                    Value::DateTime(Arc::new(DateTime::<Utc>::MIN_UTC)).discriminant()
                )
            }
            Typ::Duration => assert_eq!(
                t as u64,
                Value::Duration(Arc::new(Duration::from_secs(42))).discriminant()
            ),
            Typ::Bool => assert_eq!(t as u64, Value::Bool(true).discriminant()),
            Typ::Null => assert_eq!(t as u64, Value::Null.discriminant()),
            Typ::String => {
                assert_eq!(t as u64, Value::String(literal!("42")).discriminant())
            }
            Typ::Bytes => {
                assert_eq!(t as u64, Value::Bytes(Bytes::new().into()).discriminant())
            }
            Typ::Error => {
                assert_eq!(t as u64, Value::error(literal!("42")).discriminant())
            }
            Typ::Array => assert_eq!(t as u64, Value::Array([].into()).discriminant()),
            Typ::Map => assert_eq!(t as u64, Value::Map(Map::new()).discriminant()),
            Typ::Abstract => {
                assert_eq!(t as u64, Value::Abstract(Abstract::default()).discriminant())
            }
        }
    }
    // did you add a new value type, make sure you add a corresponding
    // Typ, this is here to trip when you do
    match Value::Bool(true) {
        Value::U8(_) => (),
        Value::I8(_) => (),
        Value::U16(_) => (),
        Value::I16(_) => (),
        Value::U32(_) => (),
        Value::V32(_) => (),
        Value::I32(_) => (),
        Value::Z32(_) => (),
        Value::U64(_) => (),
        Value::V64(_) => (),
        Value::I64(_) => (),
        Value::Z64(_) => (),
        Value::F32(_) => (),
        Value::F64(_) => (),
        Value::Decimal(_) => (),
        Value::DateTime(_) => (),
        Value::Duration(_) => (),
        Value::Bool(_) => (),
        Value::Null => (),
        Value::String(_) => (),
        Value::Bytes(_) => (),
        Value::Error(_) => (),
        Value::Array(_) => (),
        Value::Map(_) => (),
        Value::Abstract(_) => (),
    }
}

fn test_array(s: &[usize], b: (Bound<usize>, Bound<usize>)) -> Result<usize> {
    catch_unwind(AssertUnwindSafe(|| s[b].len())).map_err(|e| anyhow!("{e:?}"))
}

fn test_array_model(a: &ValArray, len: usize) {
    let model = vec![0; len];
    macro_rules! check {
        ($f:expr) => {
            for i in 0..=len + 1 {
                let b = $f(i);
                let ss = a.subslice(b);
                let rs = test_array(&model, b);
                match (&ss, &rs) {
                    (Err(_), Err(_)) => (),
                    (Ok(ss), Ok(len)) => assert_eq!(ss.len(), *len),
                    (_, _) => panic!(
                        "differ at {b:?} {} vs {}",
                        match ss {
                            Err(e) => format!("Err({e:?})"),
                            Ok(_) => format!("Ok"),
                        },
                        match rs {
                            Err(e) => format!("Err({e:?})"),
                            Ok(_) => format!("Ok"),
                        }
                    ),
                }
            }
        };
    }
    check!(|_| (Bound::Unbounded, Bound::Unbounded));
    check!(|i| (Bound::Unbounded, Bound::Included(i)));
    check!(|i| (Bound::Unbounded, Bound::Excluded(i)));
    check!(|i| (Bound::Included(i), Bound::Unbounded));
    check!(|i| (Bound::Included(i / 2), Bound::Included(i)));
    check!(|i| (Bound::Included(i / 2), Bound::Excluded(i)));
    check!(|i| (Bound::Excluded(i), Bound::Unbounded));
    check!(|i| (Bound::Excluded(i / 2), Bound::Included(i)));
    check!(|i| (Bound::Excluded(i / 2), Bound::Excluded(i)));
}

#[test]
fn array_subslicing() -> Result<()> {
    let a = (0..1000).into_iter().map(|i| Value::U64(i as u64));
    let a = ValArray::from_iter_exact(a);
    assert_eq!(a.len(), 1000);
    test_array_model(&a, 1000);
    for (i, v) in a.iter().enumerate() {
        assert_eq!(v, &Value::U64(i as u64))
    }
    let a0 = a.subslice(100..200)?;
    assert_eq!(a0.len(), 100);
    test_array_model(&a0, 100);
    for (i, v) in a0.iter().enumerate() {
        assert_eq!(v, &Value::U64((100 + i) as u64));
    }
    let a1 = a0.subslice(10..20)?;
    assert_eq!(a1.len(), 10);
    test_array_model(&a1, 10);
    for (i, v) in a1.iter().enumerate() {
        assert_eq!(v, &Value::U64((110 + i) as u64));
    }
    let a2 = a1.subslice(5..)?;
    assert_eq!(a2.len(), 5);
    test_array_model(&a2, 5);
    for (i, v) in a2.iter().enumerate() {
        assert_eq!(v, &Value::U64((115 + i) as u64));
    }
    Ok(())
}

fn get_as_unchecked<T: Debug + PartialEq>(v: Value, expected: &T) {
    assert_eq!(unsafe { v.get_as_unchecked::<T>() }, expected)
}

#[test]
fn get_unchecked() {
    get_as_unchecked::<u8>(Value::U8(42), &42);
    get_as_unchecked::<i8>(Value::I8(42), &42);
    get_as_unchecked::<u16>(Value::U16(42), &42);
    get_as_unchecked::<i16>(Value::I16(42), &42);
    get_as_unchecked::<u32>(Value::U32(42), &42);
    get_as_unchecked::<i32>(Value::I32(42), &42);
    get_as_unchecked::<u64>(Value::U64(42), &42);
    get_as_unchecked::<i64>(Value::I64(42), &42);
    get_as_unchecked::<f32>(Value::F32(42.), &42.);
    get_as_unchecked::<f64>(Value::F64(42.), &42.);
    get_as_unchecked::<bool>(Value::Bool(true), &true);
    {
        let s = literal!("hello world");
        get_as_unchecked::<ArcStr>(Value::String(s.clone()), &s);
    }
    {
        let pb = PBytes::new(Bytes::from("12345"));
        get_as_unchecked::<PBytes>(Value::Bytes(pb.clone()), &pb);
    }
    {
        let v = Arc::new(Value::I64(42));
        get_as_unchecked::<Arc<Value>>(Value::Error(v.clone()), &v)
    }
    {
        let a = ValArray::from_iter_exact([Value::I16(42)].into_iter());
        get_as_unchecked::<ValArray>(Value::Array(a.clone()), &a);
    }
    {
        let m = Map::new();
        get_as_unchecked::<Map>(Value::Map(m.clone()), &m)
    }
    {
        let d = Arc::new(Decimal::from(42));
        get_as_unchecked::<Arc<Decimal>>(Value::Decimal(d.clone()), &d)
    }
    {
        let now = Arc::new(Utc::now());
        get_as_unchecked::<Arc<DateTime<Utc>>>(Value::DateTime(now.clone()), &now);
    }
    {
        let dur = Arc::new(Duration::MAX);
        get_as_unchecked::<Arc<Duration>>(Value::Duration(dur.clone()), &dur)
    }
    {
        let a = Abstract::default();
        get_as_unchecked::<Abstract>(Value::Abstract(a.clone()), &a)
    }
}
