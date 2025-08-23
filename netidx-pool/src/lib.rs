/// This is shamelessly based on the dynamic-pool crate, with
/// modifications
use crossbeam_queue::ArrayQueue;
use fxhash::FxHashMap;
use parking_lot::Mutex;
#[cfg(feature = "serde")]
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    any::{Any, TypeId},
    borrow::Borrow,
    cell::RefCell,
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    collections::HashMap,
    default::Default,
    fmt::Debug,
    hash::{Hash, Hasher},
    mem::{self, ManuallyDrop},
    ops::{Deref, DerefMut},
    ptr,
    sync::{Arc, LazyLock, Weak},
};

pub mod arc;
pub mod pooled;
#[cfg(test)]
mod test;

/// Take a poolable type T from the generic thread local pool set. It
/// is much more efficient to construct your own pools.  size and max
/// are the pool parameters used if the pool doesn't already exist.
pub fn take_t<T: Any + Poolable + Send + 'static>(size: usize, max: usize) -> Pooled<T> {
    thread_local! {
        static POOLS: RefCell<FxHashMap<TypeId, Box<dyn Any>>> =
            RefCell::new(HashMap::default());
    }
    POOLS.with(|pools| {
        let mut pools = pools.borrow_mut();
        let pool: &mut Pool<T> = pools
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(Pool::<T>::new(size, max)))
            .downcast_mut()
            .unwrap();
        pool.take()
    })
}

/// Implementing this trait allows full low level control over where
/// the pool pointer is stored. For example if you are pooling an
/// allocated data structure, you could store the pool pointer in the
/// allocation to keep the size of the handle struct to a
/// minimum. E.G. you're pooling a ThinArc. Or, if you have a static
/// global pool, then you would not need to keep a pool pointer at
/// all.
///
/// The object's drop implementation should return the object to the
/// pool instead of deallocating it
///
/// Implementing this trait correctly is extremely tricky, and
/// requires unsafe code in almost all cases, therefore it is marked
/// as unsafe
///
/// Most of the time you should use the `Pooled` wrapper as it's
/// required trait is much eaiser to implement and there is no
/// practial place to put the pool pointer besides on the stack.
pub unsafe trait RawPoolable: Send + Sized {
    /// allocate a new empty object and set it's pool pointer to `pool`
    fn empty(pool: WeakPool<Self>) -> Self;

    /// empty the collection and reset it to it's default state so it
    /// can be put back in the pool
    fn reset(&mut self);

    /// return the capacity of the collection
    fn capacity(&self) -> usize;

    /// Actually drop the inner object, don't put it back in the pool,
    /// make sure you do not call both this method and the drop
    /// implementation that puts the object back in the pool!
    fn really_drop(self);
}

/// Trait for poolable objects
pub trait Poolable {
    /// allocate a new empty collection
    fn empty() -> Self;

    /// empty the collection and reset it to it's default state so it
    /// can be put back in the pool. This will be called when the
    /// Pooled wrapper has been dropped and the object is being put
    /// back in the pool.
    fn reset(&mut self);

    /// return the capacity of the collection
    fn capacity(&self) -> usize;

    /// return true if the object has really been dropped, e.g. if
    /// you're pooling an Arc then Arc::get_mut().is_some() == true.
    fn really_dropped(&self) -> bool {
        true
    }
}

/// A generic wrapper for pooled objects. This handles keeping track
/// of the pool pointer for you and allows you to wrap almost any
/// container type easily.
///
/// Most of the time, this is what you want to use.
#[derive(Debug, Clone)]
pub struct Pooled<T: Poolable + Send + 'static> {
    pool: ManuallyDrop<WeakPool<Self>>,
    object: ManuallyDrop<T>,
}

unsafe impl<T: Poolable + Send + 'static> RawPoolable for Pooled<T> {
    fn empty(pool: WeakPool<Self>) -> Self {
        Pooled {
            pool: ManuallyDrop::new(pool),
            object: ManuallyDrop::new(Poolable::empty()),
        }
    }

    fn reset(&mut self) {
        Poolable::reset(&mut *self.object)
    }

    fn capacity(&self) -> usize {
        Poolable::capacity(&*self.object)
    }

    fn really_drop(self) {
        drop(self.detach())
    }
}

impl<T: Poolable + Send + 'static> Borrow<T> for Pooled<T> {
    fn borrow(&self) -> &T {
        &self.object
    }
}

impl Borrow<str> for Pooled<String> {
    fn borrow(&self) -> &str {
        &self.object
    }
}

impl<T: Poolable + Send + 'static + PartialEq> PartialEq for Pooled<T> {
    fn eq(&self, other: &Pooled<T>) -> bool {
        self.object.eq(&other.object)
    }
}

impl<T: Poolable + Send + 'static + Eq> Eq for Pooled<T> {}

impl<T: Poolable + Send + 'static + PartialOrd> PartialOrd for Pooled<T> {
    fn partial_cmp(&self, other: &Pooled<T>) -> Option<Ordering> {
        self.object.partial_cmp(&other.object)
    }
}

impl<T: Poolable + Send + 'static + Ord> Ord for Pooled<T> {
    fn cmp(&self, other: &Pooled<T>) -> Ordering {
        self.object.cmp(&other.object)
    }
}

impl<T: Poolable + Send + 'static + Hash> Hash for Pooled<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        Hash::hash(&self.object, state)
    }
}

impl<T: Poolable + Send + 'static> Pooled<T> {
    /// Creates a `Pooled` that isn't connected to any pool. E.G. for
    /// branches where you know a given `Pooled` will always be empty.
    pub fn orphan(t: T) -> Self {
        Pooled { pool: ManuallyDrop::new(WeakPool::new()), object: ManuallyDrop::new(t) }
    }

    /// assign the `Pooled` to the specified pool. When it is dropped
    /// it will be placed in `pool` instead of the pool it was
    /// originally allocated from. If an orphan is assigned a pool it
    /// will no longer be orphaned.
    pub fn assign(&mut self, pool: &Pool<T>) {
        let old = mem::replace(&mut self.pool, ManuallyDrop::new(pool.downgrade()));
        drop(ManuallyDrop::into_inner(old))
    }

    /// detach the object from the pool, returning it.
    pub fn detach(self) -> T {
        let mut t = ManuallyDrop::new(self);
        unsafe {
            ManuallyDrop::drop(&mut t.pool);
            ManuallyDrop::take(&mut t.object)
        }
    }
}

impl<T: Poolable + Send + 'static> AsRef<T> for Pooled<T> {
    fn as_ref(&self) -> &T {
        &self.object
    }
}

impl<T: Poolable + Send + 'static> Deref for Pooled<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.object
    }
}

impl<T: Poolable + Send + 'static> DerefMut for Pooled<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.object
    }
}

impl<T: Poolable + Send + 'static> Drop for Pooled<T> {
    fn drop(&mut self) {
        if self.really_dropped() {
            match self.pool.upgrade() {
                Some(pool) => pool.insert(unsafe { ptr::read(self) }),
                None => unsafe {
                    ManuallyDrop::drop(&mut self.pool);
                    ManuallyDrop::drop(&mut self.object);
                },
            }
        }
    }
}

#[cfg(feature = "serde")]
impl<T: Poolable + Send + 'static + Serialize> Serialize for Pooled<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.object.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de, T: Poolable + Send + 'static + DeserializeOwned> Deserialize<'de>
    for Pooled<T>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut t = take_t::<T>(1000, 1000);
        Self::deserialize_in_place(deserializer, &mut t)?;
        Ok(t)
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <T as Deserialize>::deserialize_in_place(deserializer, &mut place.object)
    }
}

trait Prune {
    fn prune(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Pid(u32);

impl Pid {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static PID: AtomicU32 = AtomicU32::new(0);
        Self(PID.fetch_add(1, Ordering::Relaxed))
    }
}

struct GlobalState {
    pools: FxHashMap<Pid, Box<dyn Prune + Send + 'static>>,
    pool_shark_running: bool,
}

static POOLS: LazyLock<Mutex<GlobalState>> = LazyLock::new(|| {
    Mutex::new(GlobalState { pools: HashMap::default(), pool_shark_running: false })
});

#[derive(Debug)]
struct PoolInner<T: RawPoolable> {
    pool: ArrayQueue<T>,
    max_elt_capacity: usize,
    id: Pid,
}

impl<T: RawPoolable> Drop for PoolInner<T> {
    fn drop(&mut self) {
        while let Some(t) = self.pool.pop() {
            RawPoolable::really_drop(t)
        }
    }
}

fn pool_shark() {
    use std::{
        thread::{sleep, spawn},
        time::Duration,
    };
    spawn(|| loop {
        sleep(Duration::from_secs(300));
        {
            for p in POOLS.lock().pools.values() {
                p.prune()
            }
        }
    });
}

#[derive(Clone, Debug)]
pub struct WeakPool<T: RawPoolable>(Weak<PoolInner<T>>);

impl<T: RawPoolable + Send + 'static> WeakPool<T> {
    pub fn new() -> Self {
        WeakPool(Weak::new())
    }

    pub fn upgrade(&self) -> Option<RawPool<T>> {
        self.0.upgrade().map(RawPool)
    }
}

pub type Pool<T> = RawPool<Pooled<T>>;

/// a lock-free, thread-safe, dynamically-sized object pool.
///
/// this pool begins with an initial capacity and will continue
/// creating new objects on request when none are available. Pooled
/// objects are returned to the pool on destruction.
///
/// if, during an attempted return, a pool already has
/// `maximum_capacity` objects in the pool, the pool will throw away
/// that object.
#[derive(Clone, Debug)]
pub struct RawPool<T: RawPoolable + Send + 'static>(Arc<PoolInner<T>>);

impl<T: RawPoolable + Send + 'static> Drop for RawPool<T> {
    fn drop(&mut self) {
        // one held by us, and one held by the pool shark. If a weak
        // ref gets upgraded before we finish the drop the worst that
        // will happen is that pool won't be pool sharked
        if Arc::strong_count(&self.0) <= 2 {
            let res = POOLS.lock().pools.remove(&self.0.id);
            drop(res)
        }
    }
}

impl<T: RawPoolable + Send + 'static> Prune for RawPool<T> {
    fn prune(&self) {
        let len = self.0.pool.len();
        let ten_percent = std::cmp::max(1, self.0.pool.capacity() / 10);
        let one_percent = std::cmp::max(1, ten_percent / 10);
        if len > ten_percent {
            for _ in 0..ten_percent {
                if let Some(v) = self.0.pool.pop() {
                    RawPoolable::really_drop(v)
                }
            }
        } else if len > one_percent {
            for _ in 0..one_percent {
                if let Some(v) = self.0.pool.pop() {
                    RawPoolable::really_drop(v)
                }
            }
        } else if len > 0 {
            if let Some(v) = self.0.pool.pop() {
                RawPoolable::really_drop(v)
            }
        }
    }
}

impl<T: RawPoolable + Send + 'static> RawPool<T> {
    pub fn downgrade(&self) -> WeakPool<T> {
        WeakPool(Arc::downgrade(&self.0))
    }

    /// creates a new `Pool<T>`. this pool will retain up to
    /// `max_capacity` objects of size less than or equal to
    /// max_elt_capacity. Objects larger than max_elt_capacity will be
    /// deallocated immediatly.
    pub fn new(max_capacity: usize, max_elt_capacity: usize) -> RawPool<T> {
        let id = Pid::new();
        let t = RawPool(Arc::new(PoolInner {
            pool: ArrayQueue::new(max_capacity),
            max_elt_capacity,
            id,
        }));
        let mut gs = POOLS.lock();
        gs.pools.insert(id, Box::new(RawPool(Arc::clone(&t.0))));
        if !gs.pool_shark_running {
            gs.pool_shark_running = true;
            pool_shark()
        }
        t
    }

    /// try to take an element from the pool, return None if it is empty
    pub fn try_take(&self) -> Option<T> {
        self.0.pool.pop()
    }

    /// takes an item from the pool, creating one if none are available.
    pub fn take(&self) -> T {
        self.0.pool.pop().unwrap_or_else(|| RawPoolable::empty(self.downgrade()))
    }

    /// Insert an object into the pool. The object may be dropped if
    /// the pool is at capacity, or the object has too much capacity.
    pub fn insert(&self, mut t: T) {
        let cap = t.capacity();
        if cap > 0 && cap <= self.0.max_elt_capacity {
            t.reset();
            if let Err(t) = self.0.pool.push(t) {
                RawPoolable::really_drop(t)
            }
        } else {
            RawPoolable::really_drop(t)
        }
    }
}
