use super::{RawPool, RawPoolable, WeakPool};
use core::fmt;
use std::{
    cmp::Eq, default::Default, fmt::Debug, hash::Hash, mem::ManuallyDrop, ops::Deref, ptr,
};

macro_rules! impl_arc {
    ($name:ident, $inner:ident, $uniq:expr) => {
        #[derive(Clone)]
        pub struct $name<T: Default + Send + Sync + 'static> {
            inner: ManuallyDrop<$inner<(WeakPool<Self>, T)>>,
        }

        unsafe impl<T: Default + Send + Sync + 'static> RawPoolable for $name<T> {
            fn empty(pool: super::WeakPool<Self>) -> Self {
                Self { inner: ManuallyDrop::new($inner::new((pool, T::default()))) }
            }

            fn capacity(&self) -> usize {
                1
            }

            fn reset(&mut self) {
                $inner::get_mut(&mut self.inner).unwrap().1 = T::default()
            }

            fn really_drop(self) {
                let mut t = ManuallyDrop::new(self);
                unsafe { ManuallyDrop::drop(&mut t.inner) }
            }
        }

        impl<T: Default + Send + Sync + 'static> Drop for $name<T> {
            fn drop(&mut self) {
                if !$uniq(&mut self.inner) {
                    unsafe { ManuallyDrop::drop(&mut self.inner) }
                } else {
                    match self.inner.0.upgrade() {
                        None => unsafe { ManuallyDrop::drop(&mut self.inner) },
                        Some(pool) => pool.insert(unsafe { ptr::read(self) }),
                    }
                }
            }
        }

        impl<T: Default + Send + Sync + 'static> Deref for $name<T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.inner.1
            }
        }

        impl<T: Debug + Default + Send + Sync + 'static> fmt::Debug for $name<T> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.inner.1.fmt(f)
            }
        }

        impl<T: PartialEq + Default + Send + Sync + 'static> PartialEq for $name<T> {
            fn eq(&self, other: &Self) -> bool {
                self.inner.1 == other.inner.1
            }
        }

        impl<T: Eq + Default + Send + Sync + 'static> Eq for $name<T> {}

        impl<T: PartialOrd + Default + Send + Sync + 'static> PartialOrd for $name<T> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.inner.1.partial_cmp(&other.inner.1)
            }
        }

        impl<T: Ord + Default + Send + Sync + 'static> Ord for $name<T> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.inner.1.cmp(&other.inner.1)
            }
        }

        impl<T: Hash + Default + Send + Sync + 'static> Hash for $name<T> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.inner.1.hash(state)
            }
        }

        impl<T: Default + Send + Sync + 'static> $name<T> {
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
                match $inner::get_mut(&mut *self.inner) {
                    Some((_, t)) => Some(t),
                    None => None,
                }
            }

            pub fn strong_count(&self) -> usize {
                $inner::strong_count(&*self.inner)
            }

            pub fn as_ptr(&self) -> *const (WeakPool<Self>, T) {
                $inner::as_ptr(&*self.inner)
            }
        }
    };
}

#[cfg(feature = "triomphe")]
use triomphe::Arc as TArcInner;

#[cfg(feature = "triomphe")]
impl_arc!(TArc, TArcInner, TArcInner::is_unique);

#[cfg(feature = "triomphe")]
impl<T: Default + Send + Sync + 'static> TArc<T> {
    pub fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }
}

use std::sync::Arc as ArcInner;
impl_arc!(Arc, ArcInner, |a| ArcInner::get_mut(a).is_some());
