use anyhow::bail;
use bytes::{Buf, BufMut, Bytes};
use fxhash::FxHashMap;
use netidx_core::{
    pack::{encode_varint, len_wrapped_decode, len_wrapped_len, Pack, PackError},
    utils,
};
use parking_lot::RwLock;
use serde::{de, ser, Deserialize, Serialize};
use std::{
    any::{Any, TypeId},
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
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
    dyn Fn(&mut dyn Buf) -> result::Result<Abstract, PackError> + Send + Sync + 'static,
>;

struct AbstractPack {
    tid: TypeId,
    encoded_len: EncodedLenFn,
    encode: EncodeFn,
    decode: DecodeFn,
}

impl AbstractPack {
    fn new<T: Any + Pack + Send + Sync>() -> Self {
        AbstractPack {
            tid: TypeId::of::<T>(),
            encoded_len: Box::new(|t| {
                let t = (&*t.0).downcast_ref::<T>().unwrap();
                Pack::encoded_len(t)
            }),
            encode: Box::new(|t, mut buf| {
                let t = (&*t.0).downcast_ref::<T>().unwrap();
                Pack::encode(t, &mut buf)
            }),
            decode: Box::new(|mut buf| {
                let t = T::decode(&mut buf)?;
                Ok(Abstract(Arc::new(t)))
            }),
        }
    }
}

/// This is the type that will be decoded if we unpack an abstract type that
/// hasn't been registered.
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
    by_uuid: FxHashMap<Uuid, AbstractPack>,
    by_tid: FxHashMap<TypeId, Uuid>,
}

impl Registry {
    fn insert<T: Any + Pack + Send + Sync>(&mut self, id: Uuid) -> anyhow::Result<()> {
        match self.by_uuid.entry(id) {
            Entry::Occupied(e) => {
                if e.get().tid != TypeId::of::<T>() {
                    bail!("attempt to register {id:?} with different types")
                }
                Ok(())
            }
            Entry::Vacant(e) => {
                match self.by_tid.entry(TypeId::of::<T>()) {
                    Entry::Vacant(e) => e.insert(id),
                    Entry::Occupied(_) => {
                        bail!("T registered multiple times with different ids")
                    }
                };
                e.insert(AbstractPack::new::<T>());
                Ok(())
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
pub struct AbstractWrapper<T: Any + Pack + Send + Sync> {
    id: Uuid,
    t: PhantomData<T>,
}

impl<T: Any + Pack + Send + Sync> AbstractWrapper<T> {
    /// Return the UUID that T is registered as
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Wrap T as an Abstract
    pub fn wrap(&self, t: T) -> Abstract {
        Abstract(Arc::new(t))
    }
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
#[derive(Debug, Clone)]
pub struct Abstract(Arc<dyn Any + Send + Sync>);

impl Abstract {
    /// Look up the UUID of the concrete type of this Abstract
    pub fn id(&self) -> Uuid {
        REGISTRY.read().by_tid.get(&self.0.type_id()).map(|id| *id).unwrap()
    }

    /// Downcast self to T. If self isn't a T then return self.
    pub fn downcast<T: Any + Send + Sync>(self) -> result::Result<Arc<T>, Abstract> {
        match Arc::downcast::<T>(self.0.clone()) {
            Ok(t) => Ok(t),
            Err(t) => Err(Self(t)),
        }
    }

    /// Downcast &self to &T. If self isn't a T then return None.
    pub fn downcast_ref<T: Any + Send + Sync>(&self) -> Option<&T> {
        (&*self.0).downcast_ref::<T>()
    }

    /// Register a new abstract type, return an object that will wrap instances
    /// of T as an Abstract.
    ///
    /// Register is idempotent.
    ///
    /// - it is an error to register T with multiple different ids
    /// - it is an error to register the same id with multiple Ts
    pub fn register<T: Any + Pack + Send + Sync>(
        id: Uuid,
    ) -> anyhow::Result<AbstractWrapper<T>> {
        REGISTRY.write().insert::<T>(id)?;
        Ok(AbstractWrapper { id, t: PhantomData })
    }
}

impl Pack for Abstract {
    fn encoded_len(&self) -> usize {
        let reg = REGISTRY.read();
        let id = reg.by_tid[&self.0.type_id()];
        let pack = &reg.by_uuid[&id];
        let id_len = Pack::encoded_len(&id);
        let t_len = (pack.encoded_len)(self);
        len_wrapped_len(id_len + t_len)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        let reg = REGISTRY.read();
        let id = reg.by_tid[&self.0.type_id()];
        let pack = &reg.by_uuid[&id];
        // we don't use len_wrapped_encode because it would take the
        // lock a second time. Instead we do what it would do manually.
        let len = {
            let id_len = Pack::encoded_len(&id);
            let t_len = (pack.encoded_len)(self);
            len_wrapped_len(id_len + t_len) as u64
        };
        encode_varint(len, buf);
        Pack::encode(&id, buf)?;
        (pack.encode)(self, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        len_wrapped_decode(buf, |buf| {
            let id: Uuid = Pack::decode(buf)?;
            let reg = REGISTRY.read();
            match reg.by_uuid.get(&id) {
                Some(pack) => (pack.decode)(buf),
                None => Ok(Abstract(Arc::new(UnknownAbstractType))),
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
