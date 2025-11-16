use anyhow::bail;
use bytes::{Buf, BufMut, Bytes};
use fxhash::FxHashMap;
use netidx_core::{
    pack::{len_wrapped_decode, len_wrapped_encode, len_wrapped_len, Pack, PackError},
    utils,
};
use parking_lot::RwLock;
use serde::{de, ser, Deserialize, Serialize};
use std::{
    any::{Any, TypeId},
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap},
    fmt::{self, Debug, Formatter},
    hash::{Hash, Hasher},
    marker::PhantomData,
    result,
    sync::{Arc, LazyLock},
};
use uuid::Uuid;

type EncodedLenFn = Box<dyn Fn(&Abstract) -> usize + Send + Sync + 'static>;
type EncodeFn = Box<
    dyn Fn(&Abstract, &mut dyn BufMut) -> result::Result<(), PackError>
        + Send
        + Sync
        + 'static,
>;
type DecodeFn = Box<
    dyn Fn(&mut dyn Buf) -> result::Result<Box<dyn Any + Send + Sync>, PackError>
        + Send
        + Sync
        + 'static,
>;

type HashFn = Box<dyn Fn(&Abstract, &mut dyn Hasher) + Send + Sync + 'static>;

type EqFn = Box<dyn Fn(&Abstract, &Abstract) -> bool + Send + Sync + 'static>;

type OrdFn = Box<dyn Fn(&Abstract, &Abstract) -> Ordering + Send + Sync + 'static>;

type DebugFn =
    Box<dyn Fn(&Abstract, &mut Formatter) -> fmt::Result + Send + Sync + 'static>;

// this is necessary because Pack is not object safe
struct AbstractVtable {
    tid: TypeId,
    encoded_len: EncodedLenFn,
    encode: EncodeFn,
    decode: DecodeFn,
    debug: DebugFn,
    hash: HashFn,
    eq: EqFn,
    ord: OrdFn,
}

impl AbstractVtable {
    fn new<T: Any + Debug + Pack + Hash + Eq + Ord + Send + Sync>() -> Self {
        AbstractVtable {
            tid: TypeId::of::<T>(),
            encoded_len: Box::new(|t| {
                let t = t.downcast_ref::<T>().unwrap();
                Pack::encoded_len(t)
            }),
            encode: Box::new(|t, mut buf| {
                let t = t.downcast_ref::<T>().unwrap();
                Pack::encode(t, &mut buf)
            }),
            decode: Box::new(|mut buf| {
                let t = T::decode(&mut buf)?;
                Ok(Box::new(t))
            }),
            debug: Box::new(|t, f| {
                let t = t.downcast_ref::<T>().unwrap();
                t.fmt(f)
            }),
            hash: Box::new(|t, mut hasher| {
                let t = t.downcast_ref::<T>().unwrap();
                t.hash(&mut hasher)
            }),
            eq: Box::new(|t0, t1| {
                let t0 = t0.downcast_ref::<T>();
                let t1 = t1.downcast_ref::<T>();
                t0 == t1
            }),
            ord: Box::new(|t0, t1| match t0.type_id().cmp(&t1.type_id()) {
                Ordering::Equal => {
                    let t0 = t0.downcast_ref::<T>().unwrap();
                    let t1 = t1.downcast_ref::<T>().unwrap();
                    t0.cmp(t1)
                }
                o => o,
            }),
        }
    }
}

/// This is the type that will be decoded if we unpack an abstract type that
/// hasn't been registered.
#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnknownAbstractType;

impl Pack for UnknownAbstractType {
    fn encoded_len(&self) -> usize {
        0
    }

    fn encode(&self, _buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(())
    }

    fn decode(_buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(UnknownAbstractType)
    }
}

struct Registry {
    by_uuid: FxHashMap<Uuid, Arc<AbstractVtable>>,
    by_tid: FxHashMap<TypeId, Uuid>,
}

impl Registry {
    fn insert<T: Any + Debug + Pack + Hash + Eq + Ord + Send + Sync>(
        &mut self,
        id: Uuid,
    ) -> anyhow::Result<Arc<AbstractVtable>> {
        match self.by_uuid.entry(id) {
            Entry::Occupied(e) => {
                if e.get().tid != TypeId::of::<T>() {
                    bail!("attempt to register {id:?} with different types")
                }
                Ok(e.get().clone())
            }
            Entry::Vacant(e) => {
                match self.by_tid.entry(TypeId::of::<T>()) {
                    Entry::Vacant(e) => e.insert(id),
                    Entry::Occupied(_) => {
                        bail!("T registered multiple times with different ids")
                    }
                };
                let vt = Arc::new(AbstractVtable::new::<T>());
                e.insert(vt.clone());
                Ok(vt)
            }
        }
    }
}

/// This is the ID of UnknownAbstractType
pub static UNKNOWN_ID: Uuid = Uuid::from_bytes([
    195, 155, 41, 43, 251, 148, 70, 166, 129, 118, 150, 177, 94, 123, 235, 23,
]);

static REGISTRY: LazyLock<RwLock<Registry>> = LazyLock::new(|| {
    let mut reg = Registry { by_uuid: HashMap::default(), by_tid: HashMap::default() };
    reg.insert::<UnknownAbstractType>(UNKNOWN_ID).unwrap();
    RwLock::new(reg)
});

/// Wrap Ts as Abstracts
pub struct AbstractWrapper<T: Any + Debug + Pack + Hash + Eq + Ord + Send + Sync> {
    id: Uuid,
    vtable: Arc<AbstractVtable>,
    t: PhantomData<T>,
}

impl<T: Any + Debug + Pack + Hash + Eq + Ord + Send + Sync> AbstractWrapper<T> {
    /// Return the UUID that T is registered as
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Wrap T as an Abstract
    pub fn wrap(&self, t: T) -> Abstract {
        Abstract(Arc::new(AbstractInner {
            id: self.id,
            vtable: self.vtable.clone(),
            t: Box::new(t),
        }))
    }
}

struct AbstractInner {
    id: Uuid,
    vtable: Arc<AbstractVtable>,
    t: Box<dyn Any + Send + Sync>,
}

/// The abstract netidx value type
///
/// Any type implementing Any + Pack + Send + Sync can be wrapped in an Abstract
/// and published to netidx.
///
/// When a client reads an abstract value from netidx, it will look
/// to see if it has the same type registered (by UUID). If it does,
/// it will be able to decode the abstract type and use it. If it
/// does not, then a special type called UnknownAbstractType will
/// be substituted and the decode will not fail.
///
/// You may recover the original type by downcasting, which will fail
/// if the type isn't the one you expected.
///
/// Through abstract types the Value type can be extended in whatever way is
/// suited by the application/user site without every netidx user needing to
/// agree on a new protocol extension. As long as all the applications using an
/// abstract type register it with the same UUID, and the Pack implementation
/// remains compatible, then it can be seamlessly used without interfering with
/// non participarting applications.
#[derive(Clone)]
pub struct Abstract(Arc<AbstractInner>);

impl Abstract {
    /// Look up the UUID of the concrete type of this Abstract
    pub fn id(&self) -> Uuid {
        self.0.id
    }

    /// Downcast &self to &T. If self isn't a T then return None.
    pub fn downcast_ref<T: Any + Send + Sync>(&self) -> Option<&T> {
        (&*self.0.t).downcast_ref::<T>()
    }

    /// Register a new abstract type, return an object that will wrap instances
    /// of T as an Abstract.
    ///
    /// Register is idempotent.
    ///
    /// - it is an error to register T with multiple different ids
    /// - it is an error to register the same id with multiple Ts
    pub fn register<T: Any + Debug + Pack + Hash + Eq + Ord + Send + Sync>(
        id: Uuid,
    ) -> anyhow::Result<AbstractWrapper<T>> {
        let vtable = REGISTRY.write().insert::<T>(id)?;
        Ok(AbstractWrapper { id, vtable, t: PhantomData })
    }
}

impl Default for Abstract {
    fn default() -> Self {
        let reg = REGISTRY.read();
        Abstract(Arc::new(AbstractInner {
            id: UNKNOWN_ID,
            vtable: reg.by_uuid[&UNKNOWN_ID].clone(),
            t: Box::new(UnknownAbstractType),
        }))
    }
}

impl fmt::Debug for Abstract {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Abstract(")?;
        (self.0.vtable.debug)(self, f)?;
        write!(f, ")")
    }
}

impl Pack for Abstract {
    fn encoded_len(&self) -> usize {
        let id_len = Pack::encoded_len(&self.0.id);
        let t_len = (self.0.vtable.encoded_len)(self);
        len_wrapped_len(id_len + t_len)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        len_wrapped_encode(buf, self, |buf| {
            Pack::encode(&self.0.id, buf)?;
            (self.0.vtable.encode)(self, buf)
        })
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        len_wrapped_decode(buf, |buf| {
            let id: Uuid = Pack::decode(buf)?;
            let reg = REGISTRY.read();
            match reg.by_uuid.get(&id) {
                Some(vtable) => {
                    let t = (vtable.decode)(buf)?;
                    Ok(Abstract(Arc::new(AbstractInner {
                        id,
                        vtable: vtable.clone(),
                        t,
                    })))
                }
                None => Ok(Abstract(Arc::new(AbstractInner {
                    id: UNKNOWN_ID,
                    vtable: reg.by_uuid[&UNKNOWN_ID].clone(),
                    t: Box::new(UnknownAbstractType),
                }))),
            }
        })
    }
}

impl Serialize for Abstract {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let buf = utils::pack(self).map_err(|e| ser::Error::custom(e))?;
        buf.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Abstract {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut buf = Bytes::deserialize(deserializer)?;
        Pack::decode(&mut buf).map_err(|e| de::Error::custom(e))
    }
}

impl Hash for Abstract {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.0.vtable.hash)(self, state)
    }
}

impl PartialEq for Abstract {
    fn eq(&self, other: &Self) -> bool {
        (self.0.vtable.eq)(self, other)
    }
}

impl Eq for Abstract {}

impl PartialOrd for Abstract {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Abstract {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.0.vtable.ord)(self, other)
    }
}
