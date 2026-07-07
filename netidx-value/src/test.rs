use crate::{Map, PBytes, Typ, Value, abstract_type::Abstract, array::ValArray};
use anyhow::{Result, anyhow};
use arcstr::{ArcStr, literal};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use enumflags2::BitFlags;
use rust_decimal::Decimal;
use std::{
    fmt::Debug,
    ops::Bound,
    panic::{AssertUnwindSafe, catch_unwind},
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

#[test]
fn valarray_get_unchecked() {
    // f64s
    let a = ValArray::from_iter_exact(
        [Value::F64(1.0), Value::F64(2.5), Value::F64(-3.25)].into_iter(),
    );
    assert_eq!(unsafe { a.get_unchecked::<f64>(0) }, 1.0);
    assert_eq!(unsafe { a.get_unchecked::<f64>(1) }, 2.5);
    assert_eq!(unsafe { a.get_unchecked::<f64>(2) }, -3.25);
    // i64s — same offset, different reinterpret
    let a = ValArray::from_iter_exact(
        [Value::I64(7), Value::I64(-9), Value::I64(i64::MAX)].into_iter(),
    );
    assert_eq!(unsafe { a.get_unchecked::<i64>(0) }, 7);
    assert_eq!(unsafe { a.get_unchecked::<i64>(1) }, -9);
    assert_eq!(unsafe { a.get_unchecked::<i64>(2) }, i64::MAX);
    // bool: stored at offset 8 as u8; reinterpreting as bool is well-defined
    // for the values 0/1 the Bool variant carries.
    let a = ValArray::from_iter_exact(
        [Value::Bool(true), Value::Bool(false), Value::Bool(true)].into_iter(),
    );
    assert!(unsafe { a.get_unchecked::<bool>(0) });
    assert!(!unsafe { a.get_unchecked::<bool>(1) });
    assert!(unsafe { a.get_unchecked::<bool>(2) });
}

#[test]
fn arith_no_panics() {
    // operator impls: wrapping for +/-/*, checked for //%
    // div by zero returns Error, not panic
    assert!(matches!(Value::I64(1) / Value::I64(0), Value::Error(_)));
    assert!(matches!(Value::U32(1) / Value::U32(0), Value::Error(_)));
    assert!(matches!(Value::I64(1) % Value::I64(0), Value::Error(_)));
    assert!(matches!(Value::U8(1) % Value::U8(0), Value::Error(_)));

    // wrapping: overflow wraps around
    assert_eq!(Value::I64(i64::MAX) + Value::I64(1), Value::I64(i64::MIN));
    assert_eq!(Value::U32(0) - Value::U32(1), Value::U32(u32::MAX));
    assert_eq!(Value::I8(i8::MAX) * Value::I8(2), Value::I8(-2));

    // normal operations
    assert_eq!(Value::I64(10) + Value::I64(20), Value::I64(30));
    assert_eq!(Value::I64(10) - Value::I64(3), Value::I64(7));
    assert_eq!(Value::I64(10) * Value::I64(3), Value::I64(30));
    assert_eq!(Value::I64(10) / Value::I64(3), Value::I64(3));
    assert_eq!(Value::I64(10) % Value::I64(3), Value::I64(1));
    assert_eq!(Value::F64(1.0) / Value::F64(0.0), Value::F64(f64::INFINITY));

    // decimal div by zero
    let d0 = Value::Decimal(Arc::new(Decimal::from(1)));
    let d1 = Value::Decimal(Arc::new(Decimal::from(0)));
    assert!(matches!(d0 / d1, Value::Error(_)));

    // string "0" as divisor (transitive via parse)
    assert!(matches!(Value::I64(1) / Value::String("0".into()), Value::Error(_)));

    // bool false as divisor (coerced to U32(0))
    assert!(matches!(Value::I64(1) / Value::Bool(false), Value::Error(_)));

    // duration edge cases
    let d = Value::Duration(Arc::new(Duration::from_secs(10)));
    assert!(matches!(d.clone() / Value::U32(0), Value::Error(_)));
    assert!(matches!(d.clone() * Value::I64(-1), Value::Error(_)));
    // Unchecked duration subtraction SATURATES to zero on underflow:
    // durations are unsigned, so a negative result isn't representable,
    // and erroring here diverged between the node-walk and cranelift
    // backends (graphix #176). The checked form (`checked_sub` / `-?`)
    // still reports the underflow — see `checked_methods`.
    assert_eq!(
        d.clone() - d.clone() - d.clone(),
        Value::Duration(Arc::new(Duration::from_secs(0)))
    );
    let big = Value::Duration(Arc::new(Duration::MAX));
    assert!(matches!(big.clone() + big.clone(), Value::Error(_)));
}

#[test]
fn checked_methods() {
    // checked: overflow returns Error
    assert!(matches!(Value::I64(i64::MAX).checked_add(Value::I64(1)), Value::Error(_)));
    assert!(matches!(Value::U32(0).checked_sub(Value::U32(1)), Value::Error(_)));
    assert!(matches!(Value::I8(i8::MAX).checked_mul(Value::I8(2)), Value::Error(_)));

    // checked div by zero
    assert!(matches!(Value::I64(1).checked_div(Value::I64(0)), Value::Error(_)));
    assert!(matches!(Value::U8(1).checked_rem(Value::U8(0)), Value::Error(_)));

    // checked: normal operations succeed
    assert_eq!(Value::I64(10).checked_add(Value::I64(20)), Value::I64(30));
    assert_eq!(Value::I64(10).checked_sub(Value::I64(3)), Value::I64(7));
    assert_eq!(Value::I64(10).checked_mul(Value::I64(3)), Value::I64(30));
    assert_eq!(Value::I64(10).checked_div(Value::I64(3)), Value::I64(3));
    assert_eq!(Value::I64(10).checked_rem(Value::I64(3)), Value::I64(1));

    // float: same behavior (no overflow concept)
    assert_eq!(Value::F64(1.0).checked_div(Value::F64(0.0)), Value::F64(f64::INFINITY));

    // decimal checked
    let d0 = Value::Decimal(Arc::new(Decimal::from(1)));
    let d1 = Value::Decimal(Arc::new(Decimal::from(0)));
    assert!(matches!(d0.checked_div(d1), Value::Error(_)));

    // checked duration subtraction REPORTS underflow as an Error —
    // unlike unchecked `-`, which saturates to zero (see
    // `arith_no_panics`). This is the distinction the `-?` operator
    // gives you.
    let dur = Value::Duration(Arc::new(Duration::from_secs(10)));
    assert!(matches!(
        dur.clone().checked_sub(dur.clone()).checked_sub(dur.clone()),
        Value::Error(_)
    ));

    // cross-type checked
    assert!(matches!(Value::I64(i64::MAX).checked_add(Value::I32(1)), Value::Error(_)));
}

#[test]
fn cast_datetime_to_float_preserves_subseconds() {
    // The old code added `timestamp_nanos_opt()? / 1_000_000_000` — integer
    // division that recovered the whole seconds and DOUBLE-COUNTED them, and
    // returned None for dates past ~2262 (nanos-since-epoch overflows i64).
    // The fix adds only the sub-second fraction via `timestamp_subsec_nanos`.
    let dt = Value::DateTime(Arc::new(
        DateTime::<Utc>::from_timestamp(1, 500_000_000).unwrap(),
    ));
    assert_eq!(dt.clone().cast(Typ::F64), Some(Value::F64(1.5)));
    assert_eq!(dt.cast(Typ::F32), Some(Value::F32(1.5)));

    // Sub-epoch: chrono stores floor-seconds with a non-negative subsec, so
    // -1.5s is (secs -2, nanos 5e8) and must still read back as -1.5.
    let neg = Value::DateTime(Arc::new(
        DateTime::<Utc>::from_timestamp(-2, 500_000_000).unwrap(),
    ));
    assert_eq!(neg.cast(Typ::F64), Some(Value::F64(-1.5)));

    // A date past 2262 (whole nanos-since-epoch overflows i64) — the old
    // `timestamp_nanos_opt()?` returned None here; now it converts.
    let far = Value::DateTime(Arc::new(
        DateTime::<Utc>::from_timestamp(10_000_000_000, 0).unwrap(),
    ));
    assert_eq!(far.cast(Typ::F64), Some(Value::F64(10_000_000_000.0)));
}

#[test]
fn cast_to_duration_never_panics() {
    // `Duration::from_secs_f64` panics on negative / non-finite / overflowing
    // seconds; casting must fail to None instead (durations are unsigned).
    // Reachable from e.g. `cast<duration>(-1)`.
    for v in [
        Value::I64(-1),
        Value::I32(-5),
        Value::Z64(-1),
        Value::F64(-1.0),
        Value::F64(f64::NAN),
        Value::F64(f64::INFINITY),
        Value::F64(f64::NEG_INFINITY),
        Value::F64(1e30),
        Value::U64(u64::MAX),
    ] {
        let got = catch_unwind(AssertUnwindSafe(|| v.clone().cast(Typ::Duration)));
        assert_eq!(got.ok(), Some(None), "{v:?} -> Duration must be None, not a panic");
    }
    // Valid non-negative finite values still convert.
    assert_eq!(
        Value::U32(5).cast(Typ::Duration),
        Some(Value::Duration(Arc::new(Duration::from_secs(5))))
    );
    assert_eq!(
        Value::F64(1.5).cast(Typ::Duration),
        Some(Value::Duration(Arc::new(Duration::from_secs_f64(1.5))))
    );
}

#[test]
fn cast_float_to_datetime_preserves_fraction_and_guards() {
    // Inverse of the DateTime -> float cast: the fraction survives, so
    // f64 -> DateTime -> f64 round-trips (old code hard-coded nanos = 0).
    let v = Value::F64(1.5).cast(Typ::DateTime).unwrap();
    assert_eq!(
        v,
        Value::DateTime(Arc::new(
            DateTime::<Utc>::from_timestamp(1, 500_000_000).unwrap()
        ))
    );
    assert_eq!(v.cast(Typ::F64), Some(Value::F64(1.5)));

    // Sub-epoch floors (not truncate-toward-zero): -1.5s == (secs -2, 5e8ns).
    let n = Value::F64(-1.5).cast(Typ::DateTime).unwrap();
    assert_eq!(
        n,
        Value::DateTime(Arc::new(
            DateTime::<Utc>::from_timestamp(-2, 500_000_000).unwrap()
        ))
    );
    assert_eq!(n.cast(Typ::F64), Some(Value::F64(-1.5)));

    // Non-finite and out-of-range map to None, not a bogus epoch / wrapped date.
    assert_eq!(Value::F64(f64::NAN).cast(Typ::DateTime), None);
    assert_eq!(Value::F64(f64::INFINITY).cast(Typ::DateTime), None);
    assert_eq!(Value::U64(u64::MAX).cast(Typ::DateTime), None);

    // Integer sources still cast to a whole-second DateTime.
    assert_eq!(
        Value::I64(5).cast(Typ::DateTime),
        Some(Value::DateTime(Arc::new(DateTime::<Utc>::from_timestamp(5, 0).unwrap())))
    );
}

#[test]
fn cast_to_bool_does_not_narrow_through_i64() {
    // Truthiness is "> 0", but the old `$v as i64 > 0` narrowed first, so
    // high-bit u64 values and sub-integer fractions wrongly read false.
    assert_eq!(Value::U64(u64::MAX).cast(Typ::Bool), Some(Value::Bool(true)));
    assert_eq!(Value::U64(1u64 << 63).cast(Typ::Bool), Some(Value::Bool(true)));
    assert_eq!(Value::F64(0.5).cast(Typ::Bool), Some(Value::Bool(true)));
    assert_eq!(Value::F32(0.5).cast(Typ::Bool), Some(Value::Bool(true)));
    // The "> 0" convention is preserved: zero and negatives are false.
    assert_eq!(Value::U64(0).cast(Typ::Bool), Some(Value::Bool(false)));
    assert_eq!(Value::I64(-1).cast(Typ::Bool), Some(Value::Bool(false)));
    assert_eq!(Value::F64(-0.5).cast(Typ::Bool), Some(Value::Bool(false)));
    assert_eq!(Value::I64(5).cast(Typ::Bool), Some(Value::Bool(true)));
}

#[test]
fn cast_duration_to_int_guards_overflow() {
    // Out-of-range durations are None (matching the DateTime arm), never a
    // silently wrapped or sign-flipped integer.
    let big = Value::Duration(Arc::new(Duration::from_secs(5_000_000_000)));
    assert_eq!(big.clone().cast(Typ::U32), None); // would wrap to 705_032_704
    assert_eq!(big.clone().cast(Typ::I32), None); // would sign-flip negative
    assert_eq!(big.cast(Typ::U64), Some(Value::U64(5_000_000_000)));
    let huge = Value::Duration(Arc::new(Duration::new(u64::MAX, 0)));
    assert_eq!(huge.clone().cast(Typ::I64), None); // would read as -1
    assert_eq!(huge.cast(Typ::U64), Some(Value::U64(u64::MAX)));
    // In-range still converts.
    let ok = Value::Duration(Arc::new(Duration::from_secs(10)));
    assert_eq!(ok.cast(Typ::U32), Some(Value::U32(10)));
}

#[test]
fn cast_error_to_error_is_identity() {
    let e = Value::error("boom");
    assert_eq!(e.clone().cast(Typ::Error), Some(e));
}

// Every recursive Value operation must handle ARBITRARY nesting depth
// without overflowing the thread stack: a cons-style recursive ADT's
// nesting depth is its LENGTH, so ~100k-deep values arise from
// perfectly reasonable programs (and `decode` receives depth from the
// WIRE — a hostile peer must not be able to crash the process). Drop
// is guarded in ValArrayBase::drop (and chunkmap's Node::drop for
// maps); hash/eq/cmp/Display/Pack are iterative (op.rs, print.rs, the
// lib.rs Pack impl). 200k levels on a 1MiB stack — the recursive
// versions overflowed well under 20k.
#[test]
fn deep_value_operations_bounded_stack() {
    fn deep_array(n: usize) -> Value {
        let mut v = Value::I64(42);
        for _ in 0..n {
            v = Value::Array(ValArray::from_iter_exact([v].into_iter()));
        }
        v
    }
    fn deep_map(n: usize) -> Value {
        let mut v = Value::I64(42);
        for _ in 0..n {
            let m = Map::new().insert(Value::I64(0), v).0;
            v = Value::Map(m);
        }
        v
    }
    fn deep_mixed(n: usize) -> Value {
        let mut v = Value::I64(42);
        for i in 0..n {
            v = if i % 2 == 0 {
                Value::Array(ValArray::from_iter_exact([v].into_iter()))
            } else {
                Value::Map(Map::new().insert(Value::I64(0), v).0)
            };
        }
        v
    }
    std::thread::Builder::new()
        .stack_size(1024 * 1024)
        .spawn(|| {
            use std::hash::Hash;
            const N: usize = 200_000;
            for v in [deep_array(N), deep_map(N), deep_mixed(N)] {
                // hash
                let mut h = std::collections::hash_map::DefaultHasher::new();
                v.hash(&mut h);
                // eq / cmp (a clone shares structure but the walk is
                // still structural — full depth)
                let w = v.clone();
                assert_eq!(v, w);
                assert_eq!(v.cmp(&w), std::cmp::Ordering::Equal);
                // Display to a sink
                use std::fmt::Write;
                struct Sink(usize);
                impl Write for Sink {
                    fn write_str(&mut self, s: &str) -> std::fmt::Result {
                        self.0 += s.len();
                        Ok(())
                    }
                }
                let mut sink = Sink(0);
                write!(&mut sink, "{}", v).unwrap();
                assert!(sink.0 > N);
                // Pack roundtrip
                use netidx_core::pack::Pack;
                let mut buf = bytes::BytesMut::with_capacity(v.encoded_len());
                v.encode(&mut buf).unwrap();
                let decoded = Value::decode(&mut buf).unwrap();
                assert_eq!(v, decoded);
                drop(decoded);
                drop(w);
                // the last reference: the full re-entrant drop
                drop(v);
            }
        })
        .expect("spawn")
        .join()
        .expect("a deep-value operation overflowed the stack");
}

// A deeply nested value dropped by a TLS destructor DURING THREAD
// TEARDOWN. The drop guard (array.rs — and chunkmap's twin, for the
// Map levels) must keep working there: DROP_DEPTH is a const-init
// no-destructor TLS (accessible while other TLS destructors run) and
// the deferred queue is a global — a TLS queue's own destructor could
// run before SLOT's, and the old fallback for that case recursed
// unbounded and overflowed the stack, aborting the process.
#[test]
fn deep_value_drop_at_thread_teardown() {
    use std::cell::RefCell;
    std::thread_local! {
        static SLOT: RefCell<Option<Value>> = RefCell::new(None);
    }
    std::thread::Builder::new()
        .stack_size(1024 * 1024)
        .spawn(|| {
            SLOT.with(|s| assert!(s.borrow().is_none()));
            let mut v = Value::I64(42);
            for i in 0..200_000 {
                v = if i % 2 == 0 {
                    Value::Array(ValArray::from_iter_exact([v].into_iter()))
                } else {
                    Value::Map(Map::new().insert(Value::I64(0), v).0)
                };
            }
            SLOT.with(|s| *s.borrow_mut() = Some(v));
            // dropped by SLOT's TLS destructor after thread exit
        })
        .expect("spawn")
        .join()
        .expect("teardown drop of a deep value overflowed the stack");
}
