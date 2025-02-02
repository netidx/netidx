use crate::value::Value;
use bytes::{Buf, BufMut};
use fxhash::FxHashMap;
use netidx_core::{
    pack::{decode_varint, encode_varint, varint_len, Pack, PackError, MAX_VEC},
    pool::{Pool, Poolable, Pooled},
};
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use std::{borrow::Borrow, cell::RefCell, collections::HashMap, ops::Deref};
use triomphe::{Arc, ThinArc};

thread_local! {
    static POOLS: RefCell<FxHashMap<usize, Pool<ValArrayInner>>> = RefCell::new(HashMap::default());
}

fn init_pool(len: usize) -> Pool<ValArrayInner> {
    Pool::new(64 * (64 - len), 64)
}

fn assign(len: usize, t: &mut Pooled<ValArrayInner>) {
    if len > 0 && len <= 64 {
        POOLS.with_borrow_mut(|pools| {
            let pool = pools.entry(len).or_insert_with(|| init_pool(len));
            t.assign(pool)
        })
    }
}

fn orphan(len: usize) -> Pooled<ValArrayInner> {
    let iter = (0..len).map(|_| Value::False);
    Pooled::orphan(ValArrayInner(ThinArc::from_header_and_iter((), iter)))
}

fn get_by_size(len: usize) -> Pooled<ValArrayInner> {
    POOLS.with_borrow_mut(|pools| {
        if len > 0 && len <= 64 {
            let pool = pools.entry(len).or_insert_with(|| init_pool(len));
            let v = pool.take();
            if v.len() == len {
                v
            } else {
                v.detach();
                let mut v = orphan(len);
                v.assign(pool);
                v
            }
        } else {
            orphan(len)
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ValArrayInner(ThinArc<(), Value>);

impl Deref for ValArrayInner {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        &self.0.slice
    }
}

impl Poolable for ValArrayInner {
    fn capacity(&self) -> usize {
        1
    }

    fn empty() -> Self {
        ValArrayInner(ThinArc::from_header_and_iter((), [].into_iter()))
    }

    fn really_dropped(&self) -> bool {
        ThinArc::strong_count(&self.0) == 1
    }

    fn reset(&mut self) {
        self.0.with_arc_mut(|t| {
            // reset can only be called if the arc is unique
            for v in Arc::get_mut(t).unwrap().slice.iter_mut() {
                // ensure we drop any allocated values
                *v = Value::False;
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValArray(Pooled<ValArrayInner>);

impl Deref for ValArray {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        &(self.0).0.slice
    }
}

impl Borrow<[Value]> for ValArray {
    fn borrow(&self) -> &[Value] {
        &(self.0).0.slice
    }
}

impl From<Vec<Value>> for ValArray {
    fn from(v: Vec<Value>) -> Self {
        Self::from_iter(v.into_iter())
    }
}

impl<const S: usize> From<SmallVec<[Value; S]>> for ValArray {
    fn from(v: SmallVec<[Value; S]>) -> Self {
        Self::from_iter(v.into_iter())
    }
}

impl<const S: usize> From<[Value; S]> for ValArray {
    fn from(v: [Value; S]) -> Self {
        Self::from_iter(v.into_iter())
    }
}

impl From<&[Value]> for ValArray {
    fn from(v: &[Value]) -> Self {
        Self::from_iter(v.into_iter().map(|v| v.clone()))
    }
}

impl FromIterator<Value> for ValArray {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        let mut tmp: SmallVec<[Value; 64]> = smallvec![];
        for v in iter {
            tmp.push(v);
        }
        Self::from(tmp)
    }
}

impl Into<Vec<Value>> for ValArray {
    fn into(self) -> Vec<Value> {
        let mut tmp = Vec::with_capacity(self.len());
        for v in self.iter() {
            tmp.push(v.clone());
        }
        tmp
    }
}

impl<const S: usize> Into<SmallVec<[Value; S]>> for ValArray {
    fn into(self) -> SmallVec<[Value; S]> {
        let mut tmp = smallvec![];
        for v in self.iter() {
            tmp.push(v.clone())
        }
        tmp
    }
}

impl ValArray {
    pub fn from_iter_exact<I: Iterator<Item = Value> + ExactSizeIterator>(iter: I) -> Self {
        let mut res = get_by_size(iter.len());
        res.0.with_arc_mut(|res| {
            let res = Arc::get_mut(res).unwrap();
            for (i, v) in iter.enumerate() {
                res.slice[i] = v;
            }
        });
        Self(res)
    }
}

impl Serialize for ValArray {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for v in &**self {
            seq.serialize_element(v)?
        }
        seq.end()
    }
}

struct ValArrayVisitor;

impl<'de> Visitor<'de> for ValArrayVisitor {
    type Value = ValArray;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "expecting a sequence")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut tmp: SmallVec<[Value; 64]> = smallvec![];
        while let Some(v) = seq.next_element()? {
            tmp.push(v);
        }
        Ok(ValArray::from(tmp))
    }
}

impl<'de> Deserialize<'de> for ValArray {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ValArrayVisitor)
    }
}

impl Pack for ValArray {
    fn encoded_len(&self) -> usize {
        self.iter()
            .fold(varint_len(self.len() as u64), |len, t| len + Pack::encoded_len(t))
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(self.len() as u64, buf);
        for t in &**self {
            Pack::encode(t, buf)?
        }
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let elts = decode_varint(buf)? as usize;
        if elts > MAX_VEC {
            return Err(PackError::TooBig);
        }
        let mut data = get_by_size(elts);
        data.0.with_arc_mut(|data| {
            let data = Arc::get_mut(data).unwrap();
            for i in 0..elts {
                data.slice[i] = Pack::decode(buf)?;
            }
            Ok(())
        })?;
        Ok(Self(data))
    }
}
