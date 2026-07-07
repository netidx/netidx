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
    sync::{
        atomic::{AtomicUsize, Ordering},
        LazyLock, Mutex, MutexGuard,
    },
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
    // const-init with no drop glue: std registers NO TLS destructor
    // for this cell, so on native-TLS platforms it stays accessible
    // even while other TLS destructors run during thread teardown —
    // the guard keeps working for values dropped by TLS destructors.
    static DROP_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The deferred-drop queue. Global rather than thread-local: pushes
/// only happen past MAX_DROP_DEPTH re-entrant frames — a degenerate
/// case, so the mutex is not on any hot path (the per-drop probe is
/// the relaxed load of DROP_DEFERRED_LEN) — and a global queue keeps
/// working during thread teardown, when a TLS queue's own destructor
/// may already have run (dropping a deep value from another TLS
/// destructor then recursed unbounded). Entries are tagged with the
/// address of the owning thread's DROP_DEPTH cell and only ever
/// popped by that thread — the same discipline as immutable_chunkmap's
/// twin (there it is load-bearing for erased lifetimes; here it keeps
/// the drain ordering local and stays correct if pooling ever goes
/// thread-local).
static DROP_DEFERRED: Mutex<Vec<(usize, ValArrayBase)>> = Mutex::new(Vec::new());

/// Cheap emptiness probe so an outermost drop with nothing deferred
/// never touches the mutex. Relaxed suffices: only the pushing thread
/// drains its own entries, and it sees its own increments in program
/// order.
static DROP_DEFERRED_LEN: AtomicUsize = AtomicUsize::new(0);

fn deferred_lock() -> MutexGuard<'static, Vec<(usize, ValArrayBase)>> {
    // a poisoned queue is structurally intact and must still drain
    match DROP_DEFERRED.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

/// Restores DROP_DEPTH and, at the outermost frame, drains the
/// deferred queue. An RAII guard rather than straight-line code in
/// the Drop below so it also runs when unwinding: leaving the depth
/// inflated would permanently disable the outermost drain on this
/// thread, leaking every array deferred afterwards.
struct DepthGuard {
    depth: usize,
    tag: usize,
}

impl Drop for DepthGuard {
    fn drop(&mut self) {
        let _ = DROP_DEPTH.try_with(|d| d.set(self.depth));
        if self.depth == 0 && DROP_DEFERRED_LEN.load(Ordering::Relaxed) > 0 {
            drain_deferred(self.tag)
        }
    }
}

/// Destroy every deferred array pushed by this thread. Runs only at
/// the outermost drop frame — including while it is unwinding.
///
/// The depth is HELD AT 1 for the duration so a drained array's own
/// drop (entering at depth 1) never sees 0 and never drains NESTED
/// inside this loop: every deferred array is destroyed by this
/// frame's loop and the stack stays flat. A nested drain grows the
/// stack by a few frames per deferred entry — linear in the total
/// nesting depth, which is exactly the overflow this machinery
/// exists to prevent.
///
/// Each entry is destroyed under `catch_unwind` so a panicking
/// destructor can neither strand later entries in the queue nor
/// double-panic the process when several entries panic. The first
/// panic resumes once the queue is empty; later ones are dropped, and
/// if a panic is already unwinding through this frame the resume is
/// suppressed entirely — resuming would double-panic and abort.
fn drain_deferred(tag: usize) {
    let _ = DROP_DEPTH.try_with(|d| d.set(1));
    let mut panic = None;
    loop {
        // pop outside the lock — destruction may re-enter and push
        let entry = {
            let mut q = deferred_lock();
            q.iter().position(|e| e.0 == tag).map(|i| {
                DROP_DEFERRED_LEN.fetch_sub(1, Ordering::Relaxed);
                q.swap_remove(i).1
            })
        };
        match entry {
            None => break,
            Some(a) => {
                let r = std::panic::catch_unwind(
                    std::panic::AssertUnwindSafe(|| drop(a)),
                );
                if let Err(e) = r {
                    if panic.is_none() {
                        panic = Some(e)
                    }
                }
            }
        }
    }
    let _ = DROP_DEPTH.try_with(|d| d.set(0));
    if let Some(e) = panic {
        if !std::thread::panicking() {
            std::panic::resume_unwind(e)
        }
    }
}

impl Drop for ValArrayBase {
    fn drop(&mut self) {
        // Re-entrancy guard: see MAX_DROP_DEPTH. Past the limit, move
        // this array to the deferred queue and return — the field is
        // ManuallyDrop, so no glue runs and ownership transfers to
        // the queue; the outermost frame below destroys it
        // iteratively (via its DepthGuard, so the drain also runs
        // when unwinding). DROP_DEPTH is const-init with no drop glue
        // and normally survives thread teardown; only if it is
        // inaccessible (platforms without native TLS) degrade to the
        // plain recursive drop.
        let cell = DROP_DEPTH
            .try_with(|d| (d.get(), d as *const std::cell::Cell<usize> as usize))
            .ok();
        let Some((depth, tag)) = cell else {
            return self.really_drop();
        };
        if depth >= MAX_DROP_DEPTH {
            deferred_lock().push((tag, unsafe { ptr::read(self) }));
            DROP_DEFERRED_LEN.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let _guard = DepthGuard { depth, tag };
        let _ = DROP_DEPTH.try_with(|d| d.set(depth + 1));
        self.really_drop()
    }
}

impl ValArrayBase {
    /// The unguarded destructor body; only called from `drop`, under
    /// the DepthGuard whenever the TLS is accessible.
    fn really_drop(&mut self) {
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
