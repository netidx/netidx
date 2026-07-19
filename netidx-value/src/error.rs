use crate::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    mem::ManuallyDrop,
    ops::Deref,
};
use triomphe::Arc;

/// Shared inner value of [`Value::Error`] with bounded-stack destruction.
#[repr(transparent)]
pub struct ValError(ManuallyDrop<Arc<Value>>);

impl ValError {
    pub fn new(value: Value) -> Self {
        Self::from(Arc::new(value))
    }

    pub fn as_arc(&self) -> &Arc<Value> {
        &self.0
    }

    pub fn into_arc(self) -> Arc<Value> {
        let mut this = ManuallyDrop::new(self);
        unsafe { ManuallyDrop::take(&mut this.0) }
    }
}

impl Clone for ValError {
    fn clone(&self) -> Self {
        Self::from(Arc::clone(&self.0))
    }
}

impl Drop for ValError {
    fn drop(&mut self) {
        let mut current = unsafe { ManuallyDrop::take(&mut self.0) };
        loop {
            match Arc::try_unwrap(current) {
                Ok(Value::Error(next)) => current = next.into_arc(),
                Ok(value) => return drop(value),
                Err(shared) => return drop(shared),
            }
        }
    }
}

impl Deref for ValError {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Value> for ValError {
    fn as_ref(&self) -> &Value {
        self
    }
}

impl Borrow<Value> for ValError {
    fn borrow(&self) -> &Value {
        self
    }
}

impl From<Value> for ValError {
    fn from(value: Value) -> Self {
        Self::new(value)
    }
}

impl From<Arc<Value>> for ValError {
    fn from(value: Arc<Value>) -> Self {
        Self(ManuallyDrop::new(value))
    }
}

impl Debug for ValError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&**self, f)
    }
}

impl Display for ValError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&**self, f)
    }
}

impl Hash for ValError {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&**self, state)
    }
}

impl PartialEq for ValError {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for ValError {}

impl PartialOrd for ValError {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (**self).partial_cmp(&**other)
    }
}

impl Ord for ValError {
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl Serialize for ValError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Serialize::serialize(&**self, serializer)
    }
}

impl<'de> Deserialize<'de> for ValError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Value::deserialize(deserializer).map(Self::new)
    }
}
