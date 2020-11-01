/// This is shamelessly based on the dynamic-pool crate, with
/// modifications
use crossbeam::queue::ArrayQueue;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    default::Default,
    fmt::Debug,
    hash::{BuildHasher, Hash},
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
    cmp::{PartialEq, PartialOrd, Eq, Ord, Ordering},
    mem,
};

pub trait Poolable {
    fn alloc() -> Self;
    fn reset(&mut self);
}

impl<K, V, R> Poolable for HashMap<K, V, R>
where
    K: Hash + Eq,
    V: Hash + Eq,
    R: Default + BuildHasher,
{
    fn alloc() -> Self {
        HashMap::default()
    }

    fn reset(&mut self) {
        self.clear()
    }
}

impl<K, R> Poolable for HashSet<K, R>
where
    K: Hash + Eq,
    R: Default + BuildHasher,
{
    fn alloc() -> Self {
        HashSet::default()
    }

    fn reset(&mut self) {
        self.clear()
    }
}

impl<T> Poolable for Vec<T> {
    fn alloc() -> Self {
        Vec::new()
    }

    fn reset(&mut self) {
        self.clear()
    }
}

impl<T> Poolable for VecDeque<T> {
    fn alloc() -> Self {
        VecDeque::new()
    }

    fn reset(&mut self) {
        self.clear()
    }
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
pub struct Pool<T: Poolable>(Arc<ArrayQueue<T>>);

impl<T: Poolable + Sync + Send + 'static> Pool<T> {
    /// creates a new `Pool<T>`. this pool will retain up to
    /// `maximum_capacity` objects.
    pub fn new(maximum_capacity: usize) -> Pool<T> {
        Pool(Arc::new(ArrayQueue::new(maximum_capacity)))
    }

    /// takes an item from the pool, creating one if none are available.
    pub fn take(&self) -> Pooled<T> {
        let object = self.0.pop().unwrap_or_else(|| <T as Poolable>::alloc());
        Pooled { pool: Arc::downgrade(&self.0), object: Some(object) }
    }
}

/// an object, checked out from a pool.
#[derive(Debug, Clone)]
pub struct Pooled<T: Poolable> {
    pool: Weak<ArrayQueue<T>>,
    object: Option<T>,
}

impl<T: Poolable + PartialEq> PartialEq for Pooled<T> {
    fn eq(&self, other: &Pooled<T>) -> bool {
        self.object.eq(&other.object)
    }
}

impl<T: Poolable + Eq> Eq for Pooled<T> {}

impl<T: Poolable + PartialOrd> PartialOrd for Pooled<T> {
    fn partial_cmp(&self, other: &Pooled<T>) -> Option<Ordering> {
        self.object.partial_cmp(&other.object)
    }
}

impl<T: Poolable + Ord> Ord for Pooled<T> {
    fn cmp(&self, other: &Pooled<T>) -> Ordering {
        self.object.cmp(&other.object)
    }
}

impl<T: Poolable> Pooled<T> {
    /// Creates a `Pooled` that isn't connected to any pool. E.G. for
    /// branches where you know a given `Pooled` will always be empty.
    pub fn orphan(t: T) -> Self {
        Pooled {
            pool: Weak::new(),
            object: Some(t)
        }
    }

    pub fn detach(mut self) -> T {
        mem::replace(&mut self.object, None).unwrap()
    }
}

impl<T: Poolable> AsRef<T> for Pooled<T> {
    fn as_ref(&self) -> &T {
        self.object.as_ref().expect("invariant: object is always `some`.")
    }
}

impl<T: Poolable> Deref for Pooled<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.object.as_ref().expect("invariant: object is always `some`.")
    }
}

impl<T: Poolable> DerefMut for Pooled<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.object.as_mut().expect("invariant: object is always `some`.")
    }
}

impl<T: Poolable> Drop for Pooled<T> {
    fn drop(&mut self) {
        if let Some(mut object) = self.object.take() {
            object.reset();
            if let Some(q) = self.pool.upgrade() {
                q.push(object).ok();
            }
        }
    }
}
