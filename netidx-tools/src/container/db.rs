use super::ContainerConfig;
use anyhow::Result;
use bytes::{Buf, BufMut};
use netidx::{
    pack::{Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    subscriber::Value,
};
use sled;
use std::{mem, str, sync::Arc};

lazy_static! {
    static ref BUF: Pool<Vec<u8>> = Pool::new(8, 16384);
    static ref PDPAIR: Pool<Vec<(Path, UpdateKind)>> = Pool::new(16, 8124);
    static ref PATHS: Pool<Vec<Path>> = Pool::new(16, 8124);
}

pub(super) enum UpdateKind {
    Deleted,
    Inserted(Value),
    Updated(Value),
}

pub(super) struct Update {
    pub(super) data: Pooled<Vec<(Path, UpdateKind)>>,
    pub(super) formula: Pooled<Vec<(Path, UpdateKind)>>,
    pub(super) on_write: Pooled<Vec<(Path, UpdateKind)>>,
    pub(super) locked: Pooled<Vec<Path>>,
    pub(super) unlocked: Pooled<Vec<Path>>,
}

impl Update {
    fn new() -> Update {
        Update {
            data: PDPAIR.take(),
            formula: PDPAIR.take(),
            on_write: PDPAIR.take(),
            locked: PATHS.take(),
            unlocked: PATHS.take(),
        }
    }
}

pub(super) enum DatumKind {
    Data,
    Formula,
    Invalid,
}

impl DatumKind {
    fn decode(buf: &mut impl Buf) -> DatumKind {
        match buf.get_u8() {
            0 => DatumKind::Data,
            1 => DatumKind::Formula,
            _ => DatumKind::Invalid,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum Datum {
    Data(Value),
    Formula(Value, Value),
}

impl Pack for Datum {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Datum::Data(v) => v.encoded_len(),
            Datum::Formula(f, w) => f.encoded_len() + w.encoded_len(),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        match self {
            Datum::Data(v) => {
                buf.put_u8(0);
                Pack::encode(v, buf)
            }
            Datum::Formula(f, w) => {
                buf.put_u8(1);
                Pack::encode(f, buf)?;
                Pack::encode(w, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() == 0 {
            Err(PackError::InvalidFormat)
        } else {
            match buf.get_u8() {
                0 => Ok(Datum::Data(Value::decode(buf)?)),
                1 => {
                    let f = Value::decode(buf)?;
                    let w = Value::decode(buf)?;
                    Ok(Datum::Formula(f, w))
                }
                _ => Err(PackError::UnknownTag),
            }
        }
    }
}

fn lookup_value<P: AsRef<[u8]>>(tree: &sled::Tree, path: P) -> Result<Option<Datum>> {
    match tree.get(path.as_ref())? {
        None => Ok(None),
        Some(v) => Ok(Some(Datum::decode(&mut &*v)?)),
    }
}

fn iter_paths(tree: &sled::Tree) -> impl Iterator<Item = Result<Path>> + 'static {
    tree.iter().keys().map(|res| Ok(Path::from(Arc::from(str::from_utf8(&res?)?))))
}

pub(super) struct Db {
    db: sled::Db,
    data: sled::Tree,
    locked: sled::Tree,
    pending: Update,
}

impl Db {
    pub(super) fn new(cfg: &ContainerConfig) -> Result<Self> {
        let db = sled::Config::default()
            .use_compression(cfg.compress)
            .compression_factor(cfg.compress_level.unwrap_or(5) as i32)
            .cache_capacity(cfg.cache_size.unwrap_or(16 * 1024 * 1024))
            .path(&cfg.db)
            .open()?;
        let data = db.open_tree("data")?;
        let locked = db.open_tree("locked")?;
        let pending = Update::new();
        Ok(Db { db, data, locked, pending })
    }

    pub(super) fn remove(&mut self, path: Path) -> Result<()> {
        let key = path.as_bytes();
        if let Some(data) = self.data.remove(key)? {
            match DatumKind::decode(&mut &*data) {
                DatumKind::Data => self.pending.data.push((path, UpdateKind::Deleted)),
                DatumKind::Formula => {
                    self.pending.formula.push((path.clone(), UpdateKind::Deleted));
                    self.pending.on_write.push((path, UpdateKind::Deleted));
                }
                DatumKind::Invalid => (),
            }
        }
        Ok(())
    }

    pub(super) async fn flush_async(&self) -> Result<()> {
        self.db.flush_async().await?;
        Ok(())
    }

    pub(super) fn remove_subtree(&mut self, path: Path) -> Result<()> {
        for res in self.data.scan_prefix(path.as_ref()).keys() {
            let path = Path::from(Arc::from(str::from_utf8(&res?)?));
            self.remove(path)?;
        }
        Ok(())
    }

    pub(super) fn set_data(
        &mut self,
        update: bool,
        path: Path,
        value: Value,
    ) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        let datum = Datum::Data(value.clone());
        datum.encode(&mut *val)?;
        let up = match self.data.insert(key, &**val)? {
            None => UpdateKind::Inserted(value),
            Some(data) => match DatumKind::decode(&mut &*data) {
                DatumKind::Data => UpdateKind::Updated(value),
                DatumKind::Formula | DatumKind::Invalid => UpdateKind::Inserted(value),
            },
        };
        if update {
            self.pending.data.push((path, up));
        }
        Ok(())
    }

    pub(super) fn set_formula(&mut self, path: Path, value: Value) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        let up = match self.data.get(key)? {
            None => {
                Datum::Formula(value.clone(), Value::Null).encode(&mut *val)?;
                UpdateKind::Inserted(value.clone())
            }
            Some(data) => match DatumKind::decode(&mut &*data) {
                DatumKind::Data | DatumKind::Invalid => {
                    Datum::Formula(value.clone(), Value::Null).encode(&mut *val)?;
                    UpdateKind::Inserted(value.clone())
                }
                DatumKind::Formula => {
                    match Datum::decode(&mut &*data)? {
                        Datum::Data(_) => unreachable!(),
                        Datum::Formula(_, w) => {
                            Datum::Formula(value.clone(), w).encode(&mut *val)?
                        }
                    }
                    UpdateKind::Updated(value)
                }
            },
        };
        self.data.insert(key, &**val)?;
        self.pending.formula.push((path, up));
        Ok(())
    }

    pub(super) fn set_on_write(&mut self, path: Path, value: Value) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        let up = match self.data.get(key)? {
            None => {
                Datum::Formula(Value::Null, value.clone()).encode(&mut *val)?;
                UpdateKind::Inserted(value)
            }
            Some(data) => match DatumKind::decode(&mut &*data) {
                DatumKind::Data | DatumKind::Invalid => {
                    Datum::Formula(Value::Null, value.clone()).encode(&mut *val)?;
                    UpdateKind::Inserted(value)
                }
                DatumKind::Formula => {
                    match Datum::decode(&mut &*data)? {
                        Datum::Data(_) => unreachable!(),
                        Datum::Formula(f, _) => {
                            Datum::Formula(f, value.clone()).encode(&mut *val)?
                        }
                    }
                    UpdateKind::Updated(value)
                }
            },
        };
        self.data.insert(key, &**val)?;
        self.pending.on_write.push((path, up));
        Ok(())
    }

    pub(super) fn set_locked(&mut self, path: Path) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        Value::Null.encode(&mut *val)?;
        self.locked.insert(key, &**val)?;
        self.pending.locked.push(path);
        Ok(())
    }

    pub(super) fn set_unlocked(&mut self, path: Path) -> Result<()> {
        let key = path.as_bytes();
        for res in self.locked.scan_prefix(key).keys() {
            let key = res?;
            self.locked.remove(&key)?;
            let path = Path::from(Arc::from(str::from_utf8(&key)?));
            self.pending.unlocked.push(path);
        }
        Ok(())
    }

    pub(super) fn finish(&mut self) -> Update {
        mem::replace(&mut self.pending, Update::new())
    }

    pub(super) fn lookup<P: AsRef<[u8]>>(&self, path: P) -> Result<Option<Datum>> {
        lookup_value(&self.data, path)
    }

    pub(super) fn iter(
        &self,
    ) -> impl Iterator<Item = Result<(Path, DatumKind, sled::IVec)>> + 'static {
        self.data.iter().map(|res| {
            let (key, val) = res?;
            let path = Path::from(Arc::from(str::from_utf8(&key)?));
            let value = DatumKind::decode(&mut &*val);
            Ok((path, value, val))
        })
    }

    pub(super) fn locked(&self) -> impl Iterator<Item = Result<Path>> + 'static {
        iter_paths(&self.locked)
    }
}
