use crate::Value;
use anyhow::{bail, Result};
use bytes::{Buf, BufMut};
use netidx_core::pack::{
    decode_varint, encode_varint, varint_len, Pack, PackError, MAX_VEC,
};
use poolshark::{
    global::{arc::TArc as PArc, RawPool, WeakPool},
    Poolable, RawPoolable,
};
use seq_macro::seq;
use serde::{de::Visitor, ser::SerializeSeq, Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};
use std::{
    borrow::Borrow,
    fmt::Debug,
    hash::{Hash, Hasher},
    mem::ManuallyDrop,
    ops::{Bound, Deref, RangeBounds},
    ptr,
    slice::Iter,
    sync::LazyLock,
};
use triomphe::{Arc, ThinArc};

const MAX_LEN: usize = 128;

static POOLS: [LazyLock<RawPool<ValArrayBase>>; 129] = seq!(N in 0..=128 {
    [
        #(LazyLock::new(|| RawPool::new(32 * (MAX_LEN + 1 - N), 1)),)*
    ]
});

static SPOOL: LazyLock<RawPool<PArc<ValArraySlice>>> =
    LazyLock::new(|| RawPool::new(1024, 64));

fn get_by_size(len: usize) -> ValArrayBase {
    if len <= MAX_LEN {
        let pool = &POOLS[len];
        match pool.try_take() {
            Some(t) => t,
            None => ValArrayBase::new_with_len(pool.downgrade(), len),
        }
    } else {
        ValArrayBase::new_with_len(WeakPool::new(), len)
    }
}

#[derive(Debug, Clone)]
pub struct ValArrayBase(ManuallyDrop<ThinArc<WeakPool<Self>, Value>>);

impl Drop for ValArrayBase {
    fn drop(&mut self) {
        if ThinArc::strong_count(&self.0) > 1 {
            unsafe { ManuallyDrop::drop(&mut self.0) }
        } else {
            match self.0.header.header.upgrade() {
                Some(pool) => pool.insert(unsafe { ptr::read(self) }),
                None => unsafe { ManuallyDrop::drop(&mut self.0) },
            }
        }
    }
}

impl Deref for ValArrayBase {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        &self.0.slice
    }
}

unsafe impl RawPoolable for ValArrayBase {
    fn capacity(&self) -> usize {
        1
    }

    fn empty(pool: WeakPool<Self>) -> Self {
        let t = ThinArc::from_header_and_iter(pool, [].into_iter());
        ValArrayBase(ManuallyDrop::new(t))
    }

    fn reset(&mut self) {
        self.0.with_arc_mut(|t| {
            // reset can only be called if the arc is unique
            for v in Arc::get_mut(t).unwrap().slice.iter_mut() {
                // ensure we drop any allocated values
                *v = Value::Bool(false);
            }
        })
    }

    fn really_drop(self) {
        let mut t = ManuallyDrop::new(self);
        unsafe { ManuallyDrop::drop(&mut t.0) }
    }
}

impl ValArrayBase {
    fn new_with_len(pool: WeakPool<Self>, len: usize) -> Self {
        let iter = (0..len).map(|_| Value::Bool(false));
        let t = ThinArc::from_header_and_iter(pool, iter);
        Self(ManuallyDrop::new(t))
    }
}

impl PartialEq for ValArrayBase {
    fn eq(&self, other: &Self) -> bool {
        self.0.slice == other.0.slice
    }
}

impl Eq for ValArrayBase {}

impl PartialOrd for ValArrayBase {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.slice.partial_cmp(&other.0.slice)
    }
}

impl Ord for ValArrayBase {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.slice.cmp(&other.0.slice)
    }
}

impl Hash for ValArrayBase {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.slice.hash(state)
    }
}

#[derive(Debug, Clone)]
pub struct ValArraySlice {
    base: ValArrayBase,
    start: Bound<usize>,
    end: Bound<usize>,
}

impl Deref for ValArraySlice {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        &self.base[(self.start, self.end)]
    }
}

impl Poolable for ValArraySlice {
    fn empty() -> Self {
        Self { base: get_by_size(0), start: Bound::Unbounded, end: Bound::Unbounded }
    }

    fn capacity(&self) -> usize {
        1
    }

    fn reset(&mut self) {
        self.base = get_by_size(0);
        self.start = Bound::Unbounded;
        self.end = Bound::Unbounded
    }
}

#[derive(Debug, Clone)]
pub enum ValArray {
    Base(ValArrayBase),
    Slice(PArc<ValArraySlice>),
}

impl Deref for ValArray {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Base(a) => &*a,
            Self::Slice(s) => &**s,
        }
    }
}

impl PartialEq for ValArray {
    fn eq(&self, other: &Self) -> bool {
        &self[..] == &other[..]
    }
}

impl Eq for ValArray {}

impl PartialOrd for ValArray {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self[..].partial_cmp(&other[..])
    }
}

impl Ord for ValArray {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self[..].cmp(&other[..])
    }
}

impl Hash for ValArray {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self[..].hash(state)
    }
}

impl Borrow<[Value]> for ValArray {
    fn borrow(&self) -> &[Value] {
        &*self
    }
}

impl From<Vec<Value>> for ValArray {
    fn from(v: Vec<Value>) -> Self {
        Self::from_iter_exact(v.into_iter())
    }
}

impl<const S: usize> From<SmallVec<[Value; S]>> for ValArray {
    fn from(v: SmallVec<[Value; S]>) -> Self {
        Self::from_iter_exact(v.into_iter())
    }
}

impl<const S: usize> From<[Value; S]> for ValArray {
    fn from(v: [Value; S]) -> Self {
        Self::from_iter_exact(v.into_iter())
    }
}

impl From<&[Value]> for ValArray {
    fn from(v: &[Value]) -> Self {
        Self::from_iter_exact(v.into_iter().map(|v| v.clone()))
    }
}

impl FromIterator<Value> for ValArray {
    fn from_iter<T: IntoIterator<Item = Value>>(iter: T) -> Self {
        let mut tmp: SmallVec<[Value; 64]> = smallvec![];
        for v in iter {
            tmp.push(v);
        }
        Self::from_iter_exact(tmp.into_iter())
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

pub struct OwnedValArrayIter {
    pos: usize,
    a: ValArray,
}

impl Iterator for OwnedValArrayIter {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.a.get(self.pos).map(|v| v.clone());
        self.pos += 1;
        res
    }
}

impl IntoIterator for ValArray {
    type IntoIter = OwnedValArrayIter;
    type Item = Value;

    fn into_iter(self) -> Self::IntoIter {
        OwnedValArrayIter { pos: 0, a: self }
    }
}

impl<'a> IntoIterator for &'a ValArray {
    type IntoIter = Iter<'a, Value>;
    type Item = &'a Value;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl ValArray {
    pub fn from_iter_exact<I: Iterator<Item = Value> + ExactSizeIterator>(
        iter: I,
    ) -> Self {
        let mut res = get_by_size(iter.len());
        res.0.with_arc_mut(|res| {
            let res = Arc::get_mut(res).unwrap();
            for (i, v) in iter.enumerate() {
                res.slice[i] = v;
            }
        });
        Self::Base(res)
    }

    /// create a zero copy owned subslice of the array. Returns an
    /// error if the subslice is out of bounds.
    pub fn subslice<R: RangeBounds<usize>>(&self, r: R) -> Result<Self> {
        fn check_bounds(
            a: &ValArrayBase,
            start: Bound<usize>,
            end: Bound<usize>,
        ) -> Result<()> {
            let len = a.len();
            match start {
                Bound::Unbounded => (),
                Bound::Excluded(i) => {
                    if i > len - 1 {
                        bail!("start index {i} out of bounds {len}")
                    }
                }
                Bound::Included(i) => {
                    if i > len {
                        bail!("start index {i} out of bounds {len}")
                    }
                }
            }
            match end {
                Bound::Unbounded => (),
                Bound::Excluded(i) => {
                    if i > len {
                        bail!("end index {i} out of bounds {len}")
                    }
                }
                Bound::Included(i) => {
                    if i >= len {
                        bail!("end index {i} out of bounds {len}")
                    }
                }
            }
            match (start, end) {
                (
                    Bound::Unbounded,
                    Bound::Unbounded | Bound::Included(_) | Bound::Excluded(_),
                )
                | (Bound::Included(_) | Bound::Excluded(_), Bound::Unbounded) => (),
                (Bound::Included(i), Bound::Included(j))
                | (Bound::Excluded(i), Bound::Included(j))
                | (Bound::Included(i), Bound::Excluded(j)) => {
                    if j < i {
                        bail!("array index starts at {i} but ends at {j}")
                    }
                }
                (Bound::Excluded(i), Bound::Excluded(j)) => {
                    if j <= i {
                        bail!("array index starts at ex {i} but ends at ex {j}")
                    }
                }
            }
            Ok(())
        }
        match self {
            Self::Base(a) => {
                let (start, end) =
                    (r.start_bound().map(|i| *i), r.end_bound().map(|i| *i));
                let t = ValArraySlice { base: a.clone(), start, end };
                check_bounds(&a, start, end)?;
                Ok(Self::Slice(PArc::new(&SPOOL, t)))
            }
            Self::Slice(s) => {
                let max_i = match s.end {
                    Bound::Unbounded => s.base.len(),
                    Bound::Excluded(i) => i,
                    Bound::Included(i) => i,
                };
                let (start, end) =
                    (r.start_bound().map(|i| *i), r.end_bound().map(|i| *i));
                match (start, end) {
                    (Bound::Excluded(i), Bound::Excluded(j)) if j <= i => {
                        bail!("negative size slice ex {i}, ex {j}")
                    }
                    (Bound::Included(i), Bound::Included(j)) if j < i => {
                        bail!("negative size slice {i}, {j}")
                    }
                    (_, _) => (),
                }
                let (start_i, start_off, start) = match (s.start, start) {
                    (Bound::Unbounded, Bound::Unbounded) => (0, 0, Bound::Unbounded),
                    (Bound::Unbounded, Bound::Excluded(i)) => {
                        if i >= max_i {
                            bail!("slice start {i} is out of bounds {max_i}")
                        }
                        (i, i, Bound::Excluded(i))
                    }
                    (Bound::Unbounded, Bound::Included(i)) => {
                        if i > max_i {
                            bail!("slice start {i} is out of bounds {max_i}")
                        }
                        (i, i, Bound::Included(i))
                    }
                    (Bound::Excluded(i), Bound::Unbounded) => (i, 0, Bound::Excluded(i)),
                    (Bound::Excluded(i), Bound::Included(j)) => {
                        let si = i + j;
                        if si >= max_i {
                            bail!("slice start {si} is out of bounds {max_i}")
                        }
                        (si, j, Bound::Excluded(si))
                    }
                    (Bound::Excluded(i), Bound::Excluded(j)) => {
                        let si = i + j;
                        if si >= max_i {
                            bail!("slice start {si} is out of bounds {max_i}")
                        }
                        (si, j, Bound::Excluded(si))
                    }
                    (Bound::Included(i), Bound::Unbounded) => (i, 0, Bound::Included(i)),
                    (Bound::Included(i), Bound::Included(j)) => {
                        let si = i + j;
                        if si > max_i {
                            bail!("slice start {si} is out of bounds {max_i}")
                        }
                        (si, j, Bound::Included(si))
                    }
                    (Bound::Included(i), Bound::Excluded(j)) => {
                        let si = i + j;
                        if si >= max_i {
                            bail!("slice start {si} is out of bounds {max_i}")
                        }
                        (si, j, Bound::Excluded(si))
                    }
                };
                let end = match (s.end, end) {
                    (Bound::Unbounded, Bound::Unbounded) => Bound::Unbounded,
                    (Bound::Unbounded, Bound::Excluded(j)) => {
                        if j < start_off {
                            bail!("array index starts at {start_off} but ends at {j}")
                        }
                        let r = start_i + (j - start_off);
                        if r > max_i {
                            bail!("slice end {r} is out of bounds {max_i}")
                        }
                        Bound::Excluded(r)
                    }
                    (Bound::Unbounded, Bound::Included(j)) => {
                        if j < start_off {
                            bail!("array index starts at {start_off} but ends at {j}")
                        }
                        let r = start_i + (j - start_off);
                        if r > max_i {
                            bail!("slice end {r} is out of bounds {max_i}")
                        }
                        Bound::Included(r)
                    }
                    (Bound::Excluded(i), Bound::Unbounded) => Bound::Excluded(i),
                    (Bound::Excluded(i), Bound::Excluded(j)) => {
                        if j < start_off {
                            bail!("array index starts at {start_off} but ends at {j}")
                        }
                        let r = start_i + (j - start_off);
                        if r > i {
                            bail!("slice end {r} is out of bounds {i}")
                        }
                        Bound::Excluded(r)
                    }
                    (Bound::Excluded(i), Bound::Included(j)) => {
                        if j < start_off {
                            bail!("array index starts at {start_off} but ends at {j}")
                        }
                        let r = start_i + (j - start_off);
                        if r >= i {
                            bail!("slice end {r} is out of bounds {i}")
                        }
                        Bound::Included(r)
                    }
                    (Bound::Included(i), Bound::Unbounded) => Bound::Included(i),
                    (Bound::Included(i), Bound::Excluded(j)) => {
                        if j < start_off {
                            bail!("array index starts at {start_off} but ends at {j}")
                        }
                        let r = start_i + (j - start_off);
                        if r > i + 1 {
                            bail!("slice end {r} is out of bounds {i}")
                        }
                        Bound::Excluded(r)
                    }
                    (Bound::Included(i), Bound::Included(j)) => {
                        if j < start_off {
                            bail!("array index starts at {start_off} but ends at {j}")
                        }
                        let r = start_i + (j - start_off);
                        if r > i {
                            bail!("slice end {r} is out of bound {i}")
                        }
                        Bound::Included(r)
                    }
                };
                let t = ValArraySlice { base: s.base.clone(), start, end };
                Ok(Self::Slice(PArc::new(&SPOOL, t)))
            }
        }
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
        Ok(Self::Base(data))
    }
}
