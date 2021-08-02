use super::ContainerConfig;
use anyhow::Result;
use netidx::{
    pack::Pack,
    path::Path,
    pool::{Pool, Pooled},
    subscriber::Value,
};
use sled;
use std::{mem, str, sync::Arc};

lazy_static! {
    static ref BUF: Pool<Vec<u8>> = Pool::new(8, 16384);
    static ref PDPAIR: Pool<Vec<(Path, Option<Value>)>> = Pool::new(16, 8124);
    static ref PATHS: Pool<Vec<Path>> = Pool::new(16, 8124);
}

pub(super) struct Update {
    pub(super) data: Pooled<Vec<(Path, Option<Value>)>>,
    pub(super) formula: Pooled<Vec<(Path, Option<Value>)>>,
    pub(super) on_write: Pooled<Vec<(Path, Option<Value>)>>,
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

fn lookup_value<P: AsRef<[u8]>>(tree: &sled::Tree, path: &P) -> Result<Option<Value>> {
    match tree.get(path.as_ref())? {
        None => Ok(None),
        Some(v) => Ok(Some(Value::decode(&mut &*v)?)),
    }
}

fn iter_vals(tree: &sled::Tree) -> impl Iterator<Item = Result<(Path, Value)>> + 'static {
    tree.iter().map(|res| {
        let (key, val) = res?;
        let path = Path::from(Arc::from(str::from_utf8(&key)?));
        let value = Value::decode(&mut &*val)?;
        Ok((path, value))
    })
}

pub(super) struct Db {
    db: sled::Db,
    data: sled::Tree,
    formulas: sled::Tree,
    on_writes: sled::Tree,
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
        let formulas = db.open_tree("formulas")?;
        let on_writes = db.open_tree("on_writes")?;
        let locked = db.open_tree("locked")?;
        let pending = Update::new();
        Ok(Db { db, data, formulas, on_writes, locked, pending })
    }

    pub(super) fn remove(&mut self, path: Path) -> Result<()> {
        let key = path.as_bytes();
        if let Some(_) = self.data.remove(key)? {
            self.pending.data.push((path.clone(), None));
        }
        if let Some(_) = self.formulas.remove(key)? {
            self.pending.formula.push((path.clone(), None));
        }
        if let Some(_) = self.on_writes.remove(key)? {
            self.pending.on_write.push((path, None));
        }
        Ok(())
    }

    pub(super) fn set_data(&mut self, path: Path, value: Value) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        value.encode(&mut *val)?;
        if self.formulas.contains_key(key)? {
            self.pending.formula.push((path.clone(), None));
            self.formulas.remove(key)?;
        }
        if self.on_writes.contains_key(key)? {
            self.pending.on_write.push((path.clone(), None));
            self.on_writes.remove(key)?;
        }
        self.pending.data.push((path, Some(value)));
        self.data.insert(key, &**val)?;
        Ok(())
    }

    pub(super) fn set_formula(&mut self, path: Path, value: Value) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        value.encode(&mut *val)?;
        self.data.remove(key)?;
        self.formulas.insert(key, &**val)?;
        self.pending.formula.push((path, Some(value)));
        Ok(())
    }

    pub(super) fn set_on_write(&mut self, path: Path, value: Value) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        value.encode(&mut *val)?;
        self.data.remove(key)?;
        self.on_writes.insert(key, &**val)?;
        self.pending.on_write.push((path, Some(value)));
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

    pub(super) fn lookup_data<P: AsRef<[u8]>>(&self, path: &P) -> Result<Option<Value>> {
        lookup_value(&self.data, path)
    }

    pub(super) fn lookup_formula<P: AsRef<[u8]>>(
        &self,
        path: &P,
    ) -> Result<Option<Value>> {
        lookup_value(&self.formulas, path)
    }

    pub(super) fn lookup_on_write<P: AsRef<[u8]>>(
        &self,
        path: &P,
    ) -> Result<Option<Value>> {
        lookup_value(&self.on_writes, path)
    }

    pub(super) fn data_paths(&self) -> impl Iterator<Item = Result<Path>> + 'static {
        self.data
            .iter()
            .keys()
            .map(|res| Ok(Path::from(Arc::from(str::from_utf8(&res?)?))))
    }

    pub(super) fn formulas(
        &self,
    ) -> impl Iterator<Item = Result<(Path, Value)>> + 'static {
        iter_vals(&self.formulas)
    }

    pub(super) fn on_writes(
        &self,
    ) -> impl Iterator<Item = Result<(Path, Value)>> + 'static {
        iter_vals(&self.on_writes)
    }
}