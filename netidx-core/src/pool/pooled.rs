use super::{Poolable, RawPool, RawPoolable, WeakPool};
use core::fmt;
use indexmap::{IndexMap, IndexSet};
use std::{
    cmp::Eq,
    collections::{HashMap, HashSet, VecDeque},
    default::Default,
    fmt::Debug,
    hash::{BuildHasher, Hash},
    mem::ManuallyDrop,
    ops::Deref,
    ptr,
};
use triomphe::Arc;

macro_rules! impl_hashmap {
    ($ty:ident) => {
        impl<K, V, R> Poolable for $ty<K, V, R>
        where
            K: Hash + Eq,
            R: Default + BuildHasher,
        {
            fn empty() -> Self {
                $ty::default()
            }

            fn reset(&mut self) {
                self.clear()
            }

            fn capacity(&self) -> usize {
                $ty::capacity(self)
            }
        }
    };
}

impl_hashmap!(HashMap);
impl_hashmap!(IndexMap);

macro_rules! impl_hashset {
    ($ty:ident) => {
        impl<K, R> Poolable for $ty<K, R>
        where
            K: Hash + Eq,
            R: Default + BuildHasher,
        {
            fn empty() -> Self {
                $ty::default()
            }

            fn reset(&mut self) {
                self.clear()
            }

            fn capacity(&self) -> usize {
                $ty::capacity(self)
            }
        }
    };
}

impl_hashset!(HashSet);
impl_hashset!(IndexSet);

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

/// This provides a 1 word pooled Arc that does not support weak references
#[derive(Clone)]
pub struct PArc<T: Default + Send + Sync + 'static> {
    inner: ManuallyDrop<Arc<(WeakPool<Self>, T)>>,
}

unsafe impl<T: Default + Send + Sync + 'static> RawPoolable for PArc<T> {
    fn empty(pool: super::WeakPool<Self>) -> Self {
        Self { inner: ManuallyDrop::new(Arc::new((pool, T::default()))) }
    }

    fn capacity(&self) -> usize {
        1
    }

    fn reset(&mut self) {
        Arc::get_mut(&mut self.inner).unwrap().1 = T::default()
    }

    fn really_drop(self) {
        let mut t = ManuallyDrop::new(self);
        unsafe { ManuallyDrop::drop(&mut t.inner) }
    }
}

impl<T: Default + Send + Sync + 'static> Drop for PArc<T> {
    fn drop(&mut self) {
        if !Arc::is_unique(&self.inner) {
            unsafe { ManuallyDrop::drop(&mut self.inner) }
        } else {
            match self.inner.0.upgrade() {
                None => unsafe { ManuallyDrop::drop(&mut self.inner) },
                Some(pool) => pool.insert(unsafe { ptr::read(self) }),
            }
        }
    }
}

impl<T: Default + Send + Sync + 'static> Deref for PArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner.1
    }
}

impl<T: Debug + Default + Send + Sync + 'static> fmt::Debug for PArc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.1.fmt(f)
    }
}

impl<T: PartialEq + Default + Send + Sync + 'static> PartialEq for PArc<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.1 == other.inner.1
    }
}

impl<T: Eq + Default + Send + Sync + 'static> Eq for PArc<T> {}

impl<T: PartialOrd + Default + Send + Sync + 'static> PartialOrd for PArc<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.inner.1.partial_cmp(&other.inner.1)
    }
}

impl<T: Ord + Default + Send + Sync + 'static> Ord for PArc<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.1.cmp(&other.inner.1)
    }
}

impl<T: Hash + Default + Send + Sync + 'static> Hash for PArc<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.1.hash(state)
    }
}

impl<T: Default + Send + Sync + 'static> PArc<T> {
    /// allocate a new arc from the specified pool and return it containing v
    pub fn new(pool: &RawPool<Self>, v: T) -> Self {
        let mut t = pool.take();
        // values in the pool are guaranteed to be unique
        *Self::get_mut(&mut t).unwrap() = v;
        t
    }

    /// if the Arc is unique, get a mutable pointer to the inner T,
    /// otherwise return None
    pub fn get_mut(&mut self) -> Option<&mut T> {
        match Arc::get_mut(&mut *self.inner) {
            Some((_, t)) => Some(t),
            None => None,
        }
    }

    pub fn is_unique(&self) -> bool {
        Arc::is_unique(&*self.inner)
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&*self.inner)
    }
}
