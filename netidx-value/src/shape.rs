//! How a struct, a tuple and a variant are written as a [`Value`]. The
//! `FromValue`/`IntoValue` derives build and read their types through a
//! [`Shape`]: [`NetidxShape`] unless the type names another with
//! `#[value(shape = "path::To::Shape")]`.
use crate::{ValArray, Value};
use arcstr::ArcStr;
use poolshark::local::LPooled;

pub trait Shape {
    /// The struct of `fields`, sorted by name.
    fn make_struct(
        fields: impl IntoIterator<Item = (ArcStr, Value), IntoIter: ExactSizeIterator>,
    ) -> Value;

    /// A struct's `[name, value]` fields, in sorted order.
    fn struct_fields(v: &Value) -> Option<&[Value]>;

    /// The tuple of `elems`.
    fn make_tuple(
        elems: impl IntoIterator<Item = Value, IntoIter: ExactSizeIterator>,
    ) -> Value;

    /// A tuple's elements.
    fn tuple_elems(v: &Value) -> Option<&[Value]>;

    /// The variant `tag` with `payload`, empty for a bare tag.
    fn make_variant(
        tag: ArcStr,
        payload: impl IntoIterator<Item = Value, IntoIter: ExactSizeIterator>,
    ) -> Value;

    /// A variant's tag and payload.
    fn variant(v: &Value) -> Option<(&ArcStr, &[Value])>;
}

/// netidx's own forms: a struct is an array of `[name, value]` pairs, a
/// tuple an array, a bare variant its tag and a payload variant an array
/// of its tag and payload.
pub struct NetidxShape;

impl Shape for NetidxShape {
    fn make_struct(
        fields: impl IntoIterator<Item = (ArcStr, Value), IntoIter: ExactSizeIterator>,
    ) -> Value {
        Value::Array(ValArray::from_iter_exact(fields.into_iter().map(|(n, v)| {
            Value::Array(ValArray::from_iter_exact([Value::String(n), v].into_iter()))
        })))
    }

    fn struct_fields(v: &Value) -> Option<&[Value]> {
        match v {
            Value::Array(a) => Some(&a[..]),
            _ => None,
        }
    }

    fn make_tuple(
        elems: impl IntoIterator<Item = Value, IntoIter: ExactSizeIterator>,
    ) -> Value {
        Value::Array(ValArray::from_iter_exact(elems.into_iter()))
    }

    fn tuple_elems(v: &Value) -> Option<&[Value]> {
        match v {
            Value::Array(a) => Some(&a[..]),
            _ => None,
        }
    }

    fn make_variant(
        tag: ArcStr,
        payload: impl IntoIterator<Item = Value, IntoIter: ExactSizeIterator>,
    ) -> Value {
        let payload = payload.into_iter();
        if payload.len() == 0 {
            return Value::String(tag);
        }
        let mut a: LPooled<Vec<Value>> = LPooled::take();
        a.push(Value::String(tag));
        a.extend(payload);
        Value::Array(ValArray::from_iter_exact(a.drain(..)))
    }

    fn variant(v: &Value) -> Option<(&ArcStr, &[Value])> {
        match v {
            Value::String(tag) => Some((tag, &[])),
            Value::Array(a) => match &a[..] {
                [Value::String(tag), payload @ ..] if !payload.is_empty() => {
                    Some((tag, payload))
                }
                _ => None,
            },
            _ => None,
        }
    }
}
