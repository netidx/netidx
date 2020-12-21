/// This is shamelessly based on the dynamic-pool crate, with
/// modifications
use crossbeam::queue::ArrayQueue;
use std::{
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    collections::{HashMap, HashSet, VecDeque},
    default::Default,
    fmt::Debug,
    hash::{BuildHasher, Hash},
    hash::{Hash, Hasher},
    mem,
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

pub trait Poolable {
    fn empty() -> Self;
    fn reset(&mut self);
    fn capacity(&self) -> usize;
}

impl<K, V, R> Poolable for HashMap<K, V, R>
where
    K: Hash + Eq,
    R: Default + BuildHasher,
{
    fn empty() -> Self {
        HashMap::default()
    }

    fn reset(&mut self) {
        self.clear()
    }

    fn capacity(&self) -> usize {
        HashMap::capacity(self)
    }
}

impl<K, R> Poolable for HashSet<K, R>
where
    K: Hash + Eq,
    R: Default + BuildHasher,
{
    fn empty() -> Self {
        HashSet::default()
    }

    fn reset(&mut self) {
        self.clear()
    }

    fn capacity(&self) -> usize {
        HashSet::capacity(self)
    }
}

impl<T> Poolable for Vec<T> {
    fn empty() -> Self {
        Vec::new()
    }

    fn reset(&mut self) {
        self.clear()
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }
}

impl<T> Poolable for VecDeque<T> {
    fn empty() -> Self {
        VecDeque::new()
    }

    fn reset(&mut self) {
        self.clear()
    }

    fn capacity(&self) -> usize {
        VecDeque::capacity(self)
    }
}

#[derive(Debug)]
struct PoolInner<T: Poolable + Send + Sync + 'static> {
    pool: ArrayQueue<T>,
    max_elt_capacity: usize,
}

/// a lock-free, thread-safe, dynamically-sized object pool.
///
/// this pool begins with an initial capacity and will continue
/// creating new objects on request when none are available.  pooled
/// objects are returned to the pool on destruction (with an extra
/// provision to optionally "reset" the state of an object for
/// re-use).
///
/// if, during an attempted return, a pool already has
/// `maximum_capacity` objects in the pool, the pool will throw away
/// that object.
#[derive(Clone, Debug)]
pub struct Pool<T: Poolable + Send + Sync + 'static>(Arc<PoolInner<T>>);

impl<T: Poolable + Sync + Send + 'static> Pool<T> {
    /// creates a new `Pool<T>`. this pool will retain up to
    /// `max_capacity` objects of size less than or equal to
    /// max_elt_capacity. Objects larger than max_elt_capacity will be
    /// deallocated immediatly.
    pub fn new(max_capacity: usize, max_elt_capacity: usize) -> Pool<T> {
        Pool(Arc::new(PoolInner {
            pool: ArrayQueue::new(max_capacity),
            max_elt_capacity,
        }))
    }

    /// takes an item from the pool, creating one if none are available.
    pub fn take(&self) -> Pooled<T> {
        let object = self.0.pool.pop().unwrap_or_else(Poolable::empty);
        Pooled { pool: Arc::downgrade(&self.0), object }
    }
}

/// an object, checked out from a pool.
#[derive(Debug, Clone)]
pub struct Pooled<T: Poolable + Sync + Send + 'static> {
    pool: Weak<PoolInner<T>>,
    object: T,
}

impl<T: Poolable + Sync + Send + 'static + PartialEq> PartialEq for Pooled<T> {
    fn eq(&self, other: &Pooled<T>) -> bool {
        self.object.eq(&other.object)
    }
}

impl<T: Poolable + Sync + Send + 'static + Eq> Eq for Pooled<T> {}

impl<T: Poolable + Sync + Send + 'static + PartialOrd> PartialOrd for Pooled<T> {
    fn partial_cmp(&self, other: &Pooled<T>) -> Option<Ordering> {
        self.object.partial_cmp(&other.object)
    }
}

impl<T: Poolable + Sync + Send + 'static + Ord> Ord for Pooled<T> {
    fn cmp(&self, other: &Pooled<T>) -> Ordering {
        self.object.cmp(&other.object)
    }
}

impl<T: Poolable + Sync + Send + 'static + Hash> Hash for Pooled<T> {
    fn hash<H>(&self, state: &mut H) where H: Hasher {
        Hash::hash(&self.object, state)
    }
}

impl<T: Poolable + Sync + Send + 'static> Pooled<T> {
    /// Creates a `Pooled` that isn't connected to any pool. E.G. for
    /// branches where you know a given `Pooled` will always be empty.
    pub fn orphan(t: T) -> Self {
        Pooled { pool: Weak::new(), object: t }
    }

    pub fn detach(mut self) -> T {
        mem::replace(&mut self.object, Poolable::empty())
    }
}

impl<T: Poolable + Sync + Send + 'static> AsRef<T> for Pooled<T> {
    fn as_ref(&self) -> &T {
        &self.object
    }
}

impl<T: Poolable + Sync + Send + 'static> Deref for Pooled<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.object
    }
}

impl<T: Poolable + Sync + Send + 'static> DerefMut for Pooled<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.object
    }
}

impl<T: Poolable + Sync + Send + 'static> Drop for Pooled<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.pool.upgrade() {
            if self.object.capacity() <= inner.max_elt_capacity {
                let mut object = mem::replace(&mut self.object, Poolable::empty());
                object.reset();
                inner.pool.push(object).ok();
            }
        }
    }
}
