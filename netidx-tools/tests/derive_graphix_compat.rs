// Integration tests verifying that IntoValue/FromValue derive macros produce
// wire-compatible Values with the graphix runtime, and round-trip correctly.

use anyhow::Result;
use arcstr::ArcStr;
use graphix_package_core::testing;
use graphix_rt::NoExt;
use netidx_derive::{FromValue, IntoValue};
use netidx_value::{FromValue, Value};

const TEST_REGISTER: &[testing::RegisterFn] =
    &[<graphix_package_core::P as graphix_package::Package<NoExt>>::register];

/// Check that a Rust value's IntoValue representation matches the graphix
/// evaluation of `graphix_expr`, and that FromValue round-trips back to the
/// original.
async fn check<T>(original: T, graphix_expr: &str) -> Result<()>
where
    T: Clone + PartialEq + std::fmt::Debug + Into<Value> + FromValue,
{
    let derive_val: Value = original.clone().into();
    let (gx_val, ctx) = testing::eval(graphix_expr, &TEST_REGISTER).await?;
    assert_eq!(derive_val, gx_val, "IntoValue must match graphix wire format");
    let roundtrip = T::from_value(gx_val)?;
    assert_eq!(roundtrip, original);
    ctx.shutdown().await;
    Ok(())
}

// --- Type definitions ---

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct NamedStruct {
    y: String,
    x: i64,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct TupleStruct(f64, f64);

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
enum UnitEnum {
    Red,
    Green,
    Blue,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
enum MixedEnum {
    Circle { radius: f64 },
    Point(f64, f64),
    Single(i64),
    None,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct WithAttrs {
    a: i64,
    #[value(rename = "bravo")]
    b: String,
    #[value(skip)]
    c: i64,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
enum RenamedVariant {
    #[value(rename = "circle")]
    Circle { radius: f64 },
    #[value(rename = "none")]
    None,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct Inner {
    x: i64,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct Outer {
    inner: Inner,
    label: String,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct WithDefault {
    a: i64,
    #[value(default)]
    b: i64,
    c: String,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
enum EnumWithSkip {
    Named {
        x: i64,
        #[value(skip)]
        y: i64,
    },
    Unit,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct Wrapper(i64);

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct Generic<T> {
    x: T,
}

#[derive(Debug, Clone, PartialEq, IntoValue, FromValue)]
struct Empty {}

// --- Tests ---

#[tokio::test(flavor = "current_thread")]
async fn named_struct() -> Result<()> {
    check(NamedStruct { y: "hi".into(), x: 42 }, "{ x: 42, y: \"hi\" }").await
}

#[tokio::test(flavor = "current_thread")]
async fn tuple_struct() -> Result<()> {
    check(TupleStruct(1.0, 2.0), "(1.0, 2.0)").await
}

#[tokio::test(flavor = "current_thread")]
async fn unit_enum() -> Result<()> {
    check(UnitEnum::Red, "`Red").await?;
    check(UnitEnum::Green, "`Green").await?;
    check(UnitEnum::Blue, "`Blue").await
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_enum() -> Result<()> {
    check(MixedEnum::Circle { radius: 3.14 }, "`Circle({radius: 3.14})").await?;
    check(MixedEnum::Point(1.0, 2.0), "`Point(1.0, 2.0)").await?;
    check(MixedEnum::Single(42), "`Single(42)").await?;
    check(MixedEnum::None, "`None").await
}

#[tokio::test(flavor = "current_thread")]
async fn rename_attr() -> Result<()> {
    let orig = WithAttrs { a: 1, b: "hello".into(), c: 99 };
    let derive_val: Value = orig.into();
    // c is skipped; b is renamed to "bravo"
    let (gx_val, ctx) =
        testing::eval("{ a: 1, bravo: \"hello\" }", &TEST_REGISTER).await?;
    assert_eq!(derive_val, gx_val, "IntoValue must match graphix wire format");
    let back = WithAttrs::from_value(gx_val)?;
    assert_eq!(back.a, 1);
    assert_eq!(back.b, "hello");
    assert_eq!(back.c, 0); // skipped field gets Default
    ctx.shutdown().await;
    Ok(())
}

#[test]
fn renamed_variant() {
    // Circle variant renamed to "circle"
    // (graphix backtick syntax requires uppercase, so no graphix compat check)
    let orig = RenamedVariant::Circle { radius: 1.5 };
    let v: Value = orig.clone().into();
    let (tag, _) = v.clone().cast_to::<(ArcStr, Value)>().unwrap();
    assert_eq!(tag.as_str(), "circle");
    let back = RenamedVariant::from_value(v).unwrap();
    assert_eq!(back, orig);

    // None variant renamed to "none"
    let orig = RenamedVariant::None;
    let v: Value = orig.clone().into();
    let back = RenamedVariant::from_value(v).unwrap();
    assert_eq!(back, orig);
}

#[tokio::test(flavor = "current_thread")]
async fn nested_struct() -> Result<()> {
    check(
        Outer { inner: Inner { x: 42 }, label: "test".into() },
        "{ inner: { x: 42 }, label: \"test\" }",
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn value_default_attr() -> Result<()> {
    // Full roundtrip with all fields
    check(WithDefault { a: 1, b: 2, c: "hi".into() }, "{ a: 1, b: 2, c: \"hi\" }")
        .await?;

    // Wire data missing the #[value(default)] field 'b'
    let (gx_val, ctx) = testing::eval("{ a: 10, c: \"world\" }", &TEST_REGISTER).await?;
    let back = WithDefault::from_value(gx_val)?;
    assert_eq!(back.a, 10);
    assert_eq!(back.b, 0); // Default::default() for i64
    assert_eq!(back.c, "world");
    ctx.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn enum_variant_skip() -> Result<()> {
    let orig = EnumWithSkip::Named { x: 42, y: 99 };
    let derive_val: Value = orig.into();
    // y should not be in the wire data
    let (tag, data) = derive_val.clone().cast_to::<(ArcStr, Value)>()?;
    assert_eq!(tag.as_str(), "Named");
    let pairs: Vec<(ArcStr, Value)> = data.cast_to()?;
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].0.as_str(), "x");

    let (gx_val, ctx) = testing::eval("`Named({x: 42})", &TEST_REGISTER).await?;
    assert_eq!(derive_val, gx_val, "IntoValue must match graphix wire format");
    // roundtrip: y gets Default
    let back = EnumWithSkip::from_value(gx_val)?;
    assert_eq!(back, EnumWithSkip::Named { x: 42, y: 0 });
    ctx.shutdown().await;

    check(EnumWithSkip::Unit, "`Unit").await
}

#[test]
fn single_field_tuple() {
    let orig = Wrapper(42);
    let v: Value = orig.clone().into();
    let back = Wrapper::from_value(v).unwrap();
    assert_eq!(back, orig);
}

#[tokio::test(flavor = "current_thread")]
async fn generic_struct() -> Result<()> {
    check(Generic { x: 42i64 }, "{ x: 42 }").await
}

#[test]
fn empty_struct() {
    let orig = Empty {};
    let v: Value = orig.clone().into();
    let back = Empty::from_value(v).unwrap();
    assert_eq!(back, orig);
}

#[test]
fn from_value_wrong_shape() {
    // Named struct expects array of pairs, give it a plain int
    assert!(NamedStruct::from_value(Value::I64(42)).is_err());
    // Tuple struct expects tuple, give it a string
    assert!(TupleStruct::from_value(Value::from("hello")).is_err());
    // Unit enum expects string, give it an array
    assert!(UnitEnum::from_value(Value::from([Value::I64(1)])).is_err());
}
