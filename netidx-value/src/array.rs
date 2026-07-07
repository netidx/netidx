use crate::Value;
use anyhow::{Result, bail};
use bytes::{Buf, BufMut};
use netidx_core::pack::{
    MAX_VEC, Pack, PackError, decode_varint, encode_varint, varint_len,
};
use poolshark::{
    Poolable, RawPoolable,
    global::{RawPool, WeakPool, arc::TArc as PArc},
    local::LPooled,
};
use seq_macro::seq;
use serde::{Deserialize, Serialize, de::Visitor, ser::SerializeSeq};
use smallvec::{SmallVec, smallvec};
use std::{
    borrow::Borrow,
    fmt::Debug,
    hash::{Hash, Hasher},
    mem::{self, ManuallyDrop},
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

static APOOL: LazyLock<RawPool<PArc<ValArrayInner>>> =
    LazyLock::new(|| RawPool::new(1024, 64));

static EMPTY: LazyLock<ValArrayBase> =
    LazyLock::new(|| ValArrayBase::new_with_len(POOLS[0].downgrade(), 0));

fn get_by_size(len: usize) -> ValArrayBase {
    if len == 0 {
        EMPTY.clone()
    } else if len <= MAX_LEN {
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

impl Default for ValArrayBase {
    fn default() -> Self {
        EMPTY.clone()
    }
}

/// Bound on the RE-ENTRANT depth of array destruction. A value whose
/// elements themselves contain arrays (or a cons-style recursive ADT,
/// whose nesting depth is its LENGTH) drops re-entrantly — each
/// nesting level's element drop calls back into this Drop — so the
/// Rust stack consumed is proportional to the value nesting depth:
/// ~100k levels overflow a 2MiB thread stack in drop glue, killing
/// the whole runtime (SIGABRT). Past this many re-entrant frames the
/// array is moved to a thread-local deferred queue instead, and the
/// OUTERMOST drop frame destroys the queue iteratively, bounding
/// stack use for arbitrary nesting. The twin of immutable_chunkmap's
/// `Node::drop` guard (nested maps).
const MAX_DROP_DEPTH: usize = 256;

thread_local! {
    static DROP_DEPTH: std::cell::Cell<usize> = std::cell::Cell::new(0);
    static DROP_DEFERRED: std::cell::RefCell<Vec<ValArrayBase>> =
        std::cell::RefCell::new(Vec::new());
}

impl Drop for ValArrayBase {
    fn drop(&mut self) {
        // TLS destructor order during thread teardown is arbitrary
        // (the pools are themselves thread-locals and drop cached —
        // empty, recursion-free — arrays then), so every access is
        // `try_with`, degrading to the plain recursive drop when
        // unavailable.
        let depth = DROP_DEPTH.try_with(|d| d.get()).ok();
        if let Some(depth) = depth {
            if depth >= MAX_DROP_DEPTH {
                // Move this array to the deferred queue and return —
                // the field is ManuallyDrop, so no glue runs and
                // ownership transfers to the queue. Boxed so the Err
                // path can reclaim it (a closure capture would be
                // dropped un-run on Err, re-entering this Drop at the
                // same depth).
                let raw = Box::into_raw(Box::new(unsafe { ptr::read(self) }));
                let pushed = DROP_DEFERRED.try_with(|q| {
                    q.borrow_mut().push(*unsafe { Box::from_raw(raw) })
                });
                if pushed.is_err() {
                    // Queue TLS gone (thread teardown): reclaim and
                    // drop recursively — the unguarded fallback.
                    drop(unsafe { Box::from_raw(raw) });
                }
                return;
            }
            let _ = DROP_DEPTH.try_with(|d| d.set(depth + 1));
        }
        if ThinArc::strong_count(&self.0) > 1 {
            unsafe { ManuallyDrop::drop(&mut self.0) }
        } else {
            match self.0.header.header.upgrade() {
                Some(pool) => pool.insert(unsafe { ptr::read(self) }),
                None => unsafe { ManuallyDrop::drop(&mut self.0) },
            }
        }
        if let Some(depth) = depth {
            let _ = DROP_DEPTH.try_with(|d| d.set(depth));
            if depth == 0 {
                // Drain iteratively. Pop OUTSIDE the RefCell borrow —
                // each destruction re-enters this Drop (bounded by the
                // guard) and may push deeper arrays back on the queue.
                loop {
                    match DROP_DEFERRED.try_with(|q| q.borrow_mut().pop()) {
                        Ok(Some(a)) => drop(a),
                        Ok(None) | Err(_) => break,
                    }
                }
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
            for v in Arc::get_mut(t).unwrap().slice_mut().iter_mut() {
                // ensure we drop any allocated values
                *v = Value::Null;
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
        let iter = (0..len).map(|_| Value::Null);
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

#[derive(Debug, Clone)]
pub enum ValArrayInner {
    Base(ValArrayBase),
    Slice(ValArraySlice),
}

impl Poolable for ValArrayInner {
    fn capacity(&self) -> usize {
        1
    }

    fn empty() -> Self {
        Self::Base(EMPTY.clone())
    }

    fn reset(&mut self) {
        *self = Self::Base(EMPTY.clone())
    }
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ValArray(PArc<ValArrayInner>);

impl Default for ValArray {
    fn default() -> Self {
        Self(APOOL.take())
    }
}

impl Deref for ValArray {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        match &*self.0 {
            ValArrayInner::Base(a) => &*a,
            ValArrayInner::Slice(s) => &**s,
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
        let mut tmp: LPooled<Vec<Value>> = LPooled::take();
        for v in iter {
            tmp.push(v);
        }
        Self::from_iter_exact(tmp.drain(..))
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
    /// Read element `i`'s payload as `T`, skipping the bounds check
    /// and the per-element variant tag check that
    /// `Value::get_as_unchecked` already skips.
    ///
    /// Intended for fused kernels: the typechecker has already proven
    /// every element of `self` carries the matching primitive variant
    /// (e.g. all `Value::F64(_)`), so the per-element work shrinks to
    /// one indexed load at offset 8 inside the 16-byte slot. Lets a
    /// hot loop iterate `&ValArray` without ever allocating an
    /// unboxed `Vec<T>`.
    ///
    /// # Safety
    ///
    /// 1. `i < self.len()` — there is no bounds check.
    /// 2. The element at index `i` must actually carry a `T` payload.
    ///    `T` must be one of the `Copy` primitives that `Value`'s
    ///    `#[repr(u64)]` layout stores inline at offset 8 (`i8` …
    ///    `i64`, `u8` … `u64`, `f32`, `f64`, `bool`). Heap-backed
    ///    payloads (`String`, `Bytes`, `Array`, …) are not safe to
    ///    extract this way — read the slot via `&self[i]` and clone.
    ///
    /// Both invariants are statically guaranteed when this is called
    /// from a kernel built by `graphix_compiler::fusion`.
    #[inline]
    pub unsafe fn get_unchecked<T: Copy>(&self, i: usize) -> T {
        debug_assert!(i < self.len(), "ValArray::get_unchecked OOB");
        let slice: &[Value] = self;
        unsafe { *<[Value]>::get_unchecked(slice, i).get_as_unchecked::<T>() }
    }

    /// Borrow element `i`'s payload as `&T` without bounds-check or
    /// variant-tag check. Companion to [`Self::get_unchecked`] for
    /// non-`Copy` payloads like `ArcStr` (strings) where moving out
    /// would invalidate the array's slot.
    ///
    /// # Safety
    ///
    /// Same invariants as `get_unchecked`: `i < self.len()` and the
    /// slot must actually carry a `T` payload. The returned reference
    /// borrows the `ValArray` for as long as needed.
    #[inline]
    pub unsafe fn get_ref_unchecked<T>(&self, i: usize) -> &T {
        debug_assert!(i < self.len(), "ValArray::get_ref_unchecked OOB");
        let slice: &[Value] = self;
        unsafe { <[Value]>::get_unchecked(slice, i).get_as_unchecked::<T>() }
    }

    pub fn from_iter_exact<I: Iterator<Item = Value> + ExactSizeIterator>(
        iter: I,
    ) -> Self {
        let len = iter.len();
        let mut res = get_by_size(iter.len());
        if len > 0 {
            res.0.with_arc_mut(|res| {
                let res = Arc::get_mut(res).unwrap();
                for (i, v) in iter.enumerate() {
                    res.slice_mut()[i] = v;
                }
            })
        }
        Self(PArc::new(&APOOL, ValArrayInner::Base(res)))
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
        match &*self.0 {
            ValArrayInner::Base(a) => {
                let (start, end) =
                    (r.start_bound().map(|i| *i), r.end_bound().map(|i| *i));
                let t = ValArraySlice { base: a.clone(), start, end };
                check_bounds(&a, start, end)?;
                Ok(Self(PArc::new(&APOOL, ValArrayInner::Slice(t))))
            }
            ValArrayInner::Slice(s) => {
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
                Ok(Self(PArc::new(&APOOL, ValArrayInner::Slice(t))))
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
        let len = self.len();
        if len * mem::size_of::<Value>() > MAX_VEC {
            return Err(PackError::TooBig);
        }
        encode_varint(len as u64, buf);
        for t in &**self {
            Pack::encode(t, buf)?
        }
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let elts = decode_varint(buf)? as usize;
        let sz = elts.saturating_mul(mem::size_of::<Value>());
        if sz > MAX_VEC || sz > buf.remaining() << 8 {
            return Err(PackError::TooBig);
        }
        let mut data = get_by_size(elts);
        if elts > 0 {
            data.0.with_arc_mut(|data| {
                let data = Arc::get_mut(data).unwrap();
                for i in 0..elts {
                    data.slice_mut()[i] = Pack::decode(buf)?;
                }
                Ok(())
            })?
        }
        Ok(Self(PArc::new(&APOOL, ValArrayInner::Base(data))))
    }
}
