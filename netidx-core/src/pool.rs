/// This is shamelessly based on the dynamic-pool crate, with
/// modifications
use crate::utils::take_t;
use crossbeam::queue::ArrayQueue;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    borrow::Borrow,
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    collections::{HashMap, HashSet, VecDeque},
    default::Default,
    fmt::Debug,
    hash::{BuildHasher, Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};
use triomphe::Arc as TArc;

pub trait Poolable {
    fn empty() -> Self;
    fn reset(&mut self);
    fn capacity(&self) -> usize;
    /// in case you are pooling something ref counted e.g. arc
    fn really_dropped(&self) -> bool {
        true
    }
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

impl Poolable for String {
    fn empty() -> Self {
        String::new()
    }

    fn reset(&mut self) {
        self.clear()
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }
}

impl<T: Poolable> Poolable for TArc<T> {
    fn empty() -> Self {
        TArc::new(T::empty())
    }

    fn reset(&mut self) {
        if let Some(inner) = TArc::get_mut(self) {
            inner.reset()
        }
    }

    fn capacity(&self) -> usize {
        1
    }

    fn really_dropped(&self) -> bool {
        TArc::is_unique(&self)
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
        Pooled { pool: Arc::downgrade(&self.0), object: Some(object) }
    }
}

/// an object, checked out from a pool.
#[derive(Debug, Clone)]
pub struct Pooled<T: Poolable + Sync + Send + 'static> {
    pool: Weak<PoolInner<T>>,
    // Safety invariant. This will always be Some unless the
    // pooled has been dropped
    object: Option<T>,
}

impl<T: Poolable + Sync + Send + 'static> Pooled<T> {
    #[inline(always)]
    fn get(&self) -> &T {
        match &self.object {
            Some(ref t) => t,
            None => unreachable!()
        }
    }

    #[inline(always)]
    fn get_mut(&mut self) -> &mut T {
        match &mut self.object {
            Some(ref mut t) => t,
            None => unreachable!(),
        }
    }
}

impl<T: Poolable + Sync + Send + 'static> Borrow<T> for Pooled<T> {
    fn borrow(&self) -> &T {
        self.get()
    }
}

impl Borrow<str> for Pooled<String> {
    fn borrow(&self) -> &str {
        self.get().borrow()
    }
}

impl<T: Poolable + Sync + Send + 'static + PartialEq> PartialEq for Pooled<T> {
    fn eq(&self, other: &Pooled<T>) -> bool {
        self.get().eq(other.get())
    }
}

impl<T: Poolable + Sync + Send + 'static + Eq> Eq for Pooled<T> {}

impl<T: Poolable + Sync + Send + 'static + PartialOrd> PartialOrd for Pooled<T> {
    fn partial_cmp(&self, other: &Pooled<T>) -> Option<Ordering> {
        self.get().partial_cmp(other.get())
    }
}

impl<T: Poolable + Sync + Send + 'static + Ord> Ord for Pooled<T> {
    fn cmp(&self, other: &Pooled<T>) -> Ordering {
        self.get().cmp(other.get())
    }
}

impl<T: Poolable + Sync + Send + 'static + Hash> Hash for Pooled<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        Hash::hash(self.get(), state)
    }
}

impl<T: Poolable + Sync + Send + 'static> Pooled<T> {
    /// Creates a `Pooled` that isn't connected to any pool. E.G. for
    /// branches where you know a given `Pooled` will always be empty.
    pub fn orphan(t: T) -> Self {
        Pooled { pool: Weak::new(), object: Some(t) }
    }

    pub fn detach(mut self) -> T {
        self.object.take().unwrap()
    }
}

impl<T: Poolable + Sync + Send + 'static> AsRef<T> for Pooled<T> {
    fn as_ref(&self) -> &T {
        self.get()
    }
}

impl<T: Poolable + Sync + Send + 'static> Deref for Pooled<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.get()
    }
}

impl<T: Poolable + Sync + Send + 'static> DerefMut for Pooled<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.get_mut()
    }
}

impl<T: Poolable + Sync + Send + 'static> Drop for Pooled<T> {
    fn drop(&mut self) {
        if self.get().really_dropped() {
            if let Some(inner) = self.pool.upgrade() {
                let cap = self.get().capacity();
                if cap > 0 && cap <= inner.max_elt_capacity {
                    let mut object = self.object.take().unwrap();
                    object.reset();
                    inner.pool.push(object).ok();
                }
            }
        }
    }
}

impl<T: Poolable + Sync + Send + 'static + Serialize> Serialize for Pooled<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.get().serialize(serializer)
    }
}

impl<'de, T: Poolable + Sync + Send + 'static + DeserializeOwned> Deserialize<'de>
    for Pooled<T>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut t = take_t::<T>(10000, 10000);
        Self::deserialize_in_place(deserializer, &mut t)?;
        Ok(t)
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <T as Deserialize>::deserialize_in_place(deserializer, place.get_mut())
    }
}
