use crate::value::Value;
use netidx_core::pool::{Pool, Poolable, Pooled};
use std::{borrow::Borrow, ops::Deref, sync::LazyLock};
use triomphe::Arc;

static POOL: LazyLock<Pool<ArcVal>> = LazyLock::new(|| Pool::new(8124, 64));

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ArcVal(Arc<Value>);

impl Poolable for ArcVal {
    fn capacity(&self) -> usize {
        1
    }

    fn empty() -> Self {
        Self(Arc::new(Value::False))
    }

    fn really_dropped(&self) -> bool {
        Arc::is_unique(&self.0)
    }

    fn reset(&mut self) {
        *Arc::make_mut(&mut self.0) = Value::False;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PVal(Pooled<ArcVal>);

impl Deref for PVal {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &*(self.0).0
    }
}

impl Borrow<Value> for PVal {
    fn borrow(&self) -> &Value {
        &*self
    }
}

impl AsRef<Value> for PVal {
    fn as_ref(&self) -> &Value {
        &**self
    }
}
