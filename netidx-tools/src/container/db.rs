use super::ContainerConfig;
use anyhow::Result;
use arcstr::ArcStr;
use bytes::{Buf, BufMut};
use netidx::{
    chars::Chars,
    pack::{Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    subscriber::Value,
};
use sled::{
    self,
    transaction::{ConflictableTransactionError, TransactionError, TransactionalTree},
    Transactional,
};
use std::{fmt::Write, mem, str};

lazy_static! {
    static ref BUF: Pool<Vec<u8>> = Pool::new(8, 16384);
    static ref PDPAIR: Pool<Vec<(Path, UpdateKind)>> = Pool::new(16, 8124);
    static ref PATHS: Pool<Vec<Path>> = Pool::new(16, 8124);
    static ref TXNS: Pool<Vec<TxnOp>> = Pool::new(16, 65534);
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

    fn dirty(&self) -> bool {
        self.data.len() > 0
            || self.formula.len() > 0
            || self.on_write.len() > 0
            || self.locked.len() > 0
            || self.unlocked.len() > 0
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
    tree.iter().keys().map(|res| Ok(Path::from(ArcStr::from(str::from_utf8(&res?)?))))
}

struct TxDb<'a> {
    data: &'a TransactionalTree,
    locked: &'a TransactionalTree,
    pending: Update,
}

impl<'a> TxDb<'a> {
    pub(super) fn remove(&mut self, path: &Path) -> Result<()> {
        let key = path.as_bytes();
        if let Some(data) = self.data.remove(key)? {
            match DatumKind::decode(&mut &*data) {
                DatumKind::Data => {
                    self.pending.data.push((path.clone(), UpdateKind::Deleted))
                }
                DatumKind::Formula => {
                    self.pending.formula.push((path.clone(), UpdateKind::Deleted));
                    self.pending.on_write.push((path.clone(), UpdateKind::Deleted));
                }
                DatumKind::Invalid => (),
            }
        }
        Ok(())
    }

    pub(super) fn set_data(
        &mut self,
        update: bool,
        path: &Path,
        value: &Value,
    ) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        let datum = Datum::Data(value.clone());
        datum.encode(&mut *val)?;
        let up = match self.data.insert(key, &**val)? {
            None => UpdateKind::Inserted(value.clone()),
            Some(data) => match DatumKind::decode(&mut &*data) {
                DatumKind::Data => UpdateKind::Updated(value.clone()),
                DatumKind::Formula | DatumKind::Invalid => {
                    UpdateKind::Inserted(value.clone())
                }
            },
        };
        if update {
            self.pending.data.push((path.clone(), up));
        }
        Ok(())
    }

    pub(super) fn set_formula(&mut self, path: &Path, value: &Value) -> Result<()> {
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
                    UpdateKind::Updated(value.clone())
                }
            },
        };
        self.data.insert(key, &**val)?;
        self.pending.formula.push((path.clone(), up));
        Ok(())
    }

    pub(super) fn set_on_write(&mut self, path: &Path, value: &Value) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        let up = match self.data.get(key)? {
            None => {
                Datum::Formula(Value::Null, value.clone()).encode(&mut *val)?;
                UpdateKind::Inserted(value.clone())
            }
            Some(data) => match DatumKind::decode(&mut &*data) {
                DatumKind::Data | DatumKind::Invalid => {
                    Datum::Formula(Value::Null, value.clone()).encode(&mut *val)?;
                    UpdateKind::Inserted(value.clone())
                }
                DatumKind::Formula => {
                    match Datum::decode(&mut &*data)? {
                        Datum::Data(_) => unreachable!(),
                        Datum::Formula(f, _) => {
                            Datum::Formula(f, value.clone()).encode(&mut *val)?
                        }
                    }
                    UpdateKind::Updated(value.clone())
                }
            },
        };
        self.data.insert(key, &**val)?;
        self.pending.on_write.push((path.clone(), up));
        Ok(())
    }

    pub(super) fn create_sheet(
        &mut self,
        base: &Path,
        rows: usize,
        cols: usize,
        lock: bool,
    ) -> Result<()> {
        let mut buf = String::with_capacity(128);
        let rd = 1 + (rows as f32).log10() as usize;
        let cd = 1 + (cols as f32).log10() as usize;
        for i in 0..rows {
            for j in 0..cols {
                buf.clear();
                write!(buf, "{:0rwidth$}/{:0cwidth$}", i, j, rwidth = rd, cwidth = cd)?;
                let path = base.append(buf.as_str());
                if self.data.get(path.as_bytes())?.is_none() {
                    self.set_data(true, &path, &Value::Null)?;
                }
            }
        }
        if lock {
            self.set_locked(base)?
        }
        Ok(())
    }

    pub(super) fn create_table(
        &mut self,
        base: &Path,
        rows: &Vec<Chars>,
        cols: &Vec<Chars>,
        lock: bool,
    ) -> Result<()> {
        let mut buf = String::new();
        let cols: Vec<String> =
            cols.into_iter().map(|c| Path::escape(&c).into_owned()).collect();
        for row in rows.iter() {
            for col in cols.iter() {
                buf.clear();
                write!(buf, "{}/{}", Path::escape(row), col)?;
                let path = base.append(buf.as_str());
                if self.data.get(path.as_bytes())?.is_none() {
                    self.set_data(true, &path, &Value::Null)?;
                }
            }
        }
        if lock {
            self.set_locked(base)?
        }
        Ok(())
    }

    pub(super) fn set_locked(&mut self, path: &Path) -> Result<()> {
        let key = path.as_bytes();
        let mut val = BUF.take();
        Value::Null.encode(&mut *val)?;
        self.locked.insert(key, &**val)?;
        self.pending.locked.push(path.clone());
        Ok(())
    }

    pub(super) fn set_unlocked(&mut self, path: &Path) -> Result<()> {
        self.locked.remove(path.as_bytes())?;
        self.pending.unlocked.push(path.clone());
        Ok(())
    }
}

enum TxnOp {
    Remove(Path),
    SetData(bool, Path, Value),
    SetFormula(Path, Value),
    SetOnWrite(Path, Value),
    CreateSheet { base: Path, rows: usize, cols: usize, lock: bool },
    CreateTable { base: Path, rows: Vec<Chars>, cols: Vec<Chars>, lock: bool },
    SetLocked(Path),
    SetUnlocked(Path),
    RemoveSubtree(Path),
}

pub(super) struct Txn(Vec<TxnOp>);

impl Txn {
    pub(super) fn new() -> Self {
        Self(Vec::new())
    }

    pub(super) fn dirty(&self) -> bool {
        self.0.len() > 0
    }

    pub(super) fn remove(&mut self, path: Path) {
        self.0.push(TxnOp::Remove(path))
    }

    pub(super) fn set_data(&mut self, update: bool, path: Path, value: Value) {
        self.0.push(TxnOp::SetData(update, path, value))
    }

    pub(super) fn set_formula(&mut self, path: Path, value: Value) {
        self.0.push(TxnOp::SetFormula(path, value))
    }

    pub(super) fn set_on_write(&mut self, path: Path, value: Value) {
        self.0.push(TxnOp::SetOnWrite(path, value))
    }

    pub(super) fn create_sheet(
        &mut self,
        base: Path,
        rows: usize,
        cols: usize,
        lock: bool,
    ) {
        self.0.push(TxnOp::CreateSheet { base, rows, cols, lock })
    }

    pub(super) fn create_table(
        &mut self,
        base: Path,
        rows: Vec<Chars>,
        cols: Vec<Chars>,
        lock: bool,
    ) {
        self.0.push(TxnOp::CreateTable { base, rows, cols, lock })
    }

    pub(super) fn set_locked(&mut self, path: Path) {
        self.0.push(TxnOp::SetLocked(path))
    }

    pub(super) fn set_unlocked(&mut self, path: Path) {
        self.0.push(TxnOp::SetUnlocked(path))
    }

    pub(super) fn remove_subtree(&mut self, path: Path) {
        self.0.push(TxnOp::RemoveSubtree(path))
    }
}

pub(super) struct Db {
    db: sled::Db,
    data: sled::Tree,
    locked: sled::Tree,
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
        Ok(Db { db, data, locked })
    }

    pub(super) fn commit(&self, txn: &mut Txn) -> Result<Option<Update>> {
        if txn.0.is_empty() {
            Ok(None)
        } else {
            let pending = (&self.data, &self.locked)
                .transaction(move |(data, locked)| {
                    let mut tx = TxDb { data, locked, pending: Update::new() };
                    for op in &txn.0 {
                        match op {
                            TxnOp::CreateSheet { base, rows, cols, lock } => {
                                tx.create_sheet(base, *rows, *cols, *lock)
                            }
                            TxnOp::CreateTable { base, rows, cols, lock } => {
                                tx.create_table(base, rows, cols, *lock)
                            }
                            TxnOp::Remove(path) => tx.remove(path),
                            TxnOp::RemoveSubtree(path) => {
                                self.remove_subtree(&mut tx, path)
                            }
                            TxnOp::SetData(update, path, value) => {
                                tx.set_data(*update, path, value)
                            }
                            TxnOp::SetFormula(path, value) => tx.set_formula(path, value),
                            TxnOp::SetOnWrite(path, value) => {
                                tx.set_on_write(path, value)
                            }
                            TxnOp::SetLocked(path) => tx.set_locked(path),
                            TxnOp::SetUnlocked(path) => {
                                self.unlock_subtree(&mut tx, path)
                            }
                        }
                        .map_err(ConflictableTransactionError::Abort)?;
                    }
                    Ok(tx.pending)
                })
                .map_err(|e| match e {
                    TransactionError::Abort(e) => e,
                    TransactionError::Storage(_) => unreachable!(),
                });
            txn.0.clear();
            pending.map(|p| Some(p))
        }
    }

    pub(super) async fn flush_async(&self) -> Result<()> {
        self.db.flush_async().await?;
        Ok(())
    }

    fn remove_subtree(&self, txn: &mut TxDb, path: &Path) -> Result<()> {
        let mut paths = Vec::new();
        for res in self.data.scan_prefix(path.as_ref()).keys() {
            let path = Path::from(ArcStr::from(str::from_utf8(&res?)?));
            paths.push(path);
        }
        for path in paths {
            txn.remove(&path)?;
        }
        Ok(())
    }

    pub(super) fn relative_column(
        &self,
        base: &Path,
        offset: i32,
    ) -> Result<Option<Path>> {
        use std::ops::Bound::{self, *};
        let rowbase = Path::dirname(base).ok_or_else(|| anyhow!("no row"))?;
        let mut i = 0;
        if offset == 0 {
            Ok(Some(base.clone()))
        } else if offset < 0 {
            let mut iter = self
                .data
                .range::<&str, (Bound<&str>, Bound<&str>)>((Unbounded, Excluded(&**base)))
                .keys();
            while let Some(r) = iter.next_back() {
                let r = r?;
                let path = str::from_utf8(&r)?;
                if Path::dirname(path) == Some(rowbase) {
                    i -= 1;
                    if i == offset {
                        return Ok(Some(Path::from(ArcStr::from(path))));
                    }
                } else {
                    return Ok(None);
                }
            }
            Ok(None)
        } else {
            let iter = self
                .data
                .range::<&str, (Bound<&str>, Bound<&str>)>((Excluded(&**base), Unbounded))
                .keys();
            for r in iter {
                let r = r?;
                let path = str::from_utf8(&r)?;
                if Path::dirname(path) == Some(rowbase) {
                    i += 1;
                    if i == offset {
                        return Ok(Some(Path::from(ArcStr::from(path))));
                    }
                } else {
                    return Ok(None);
                }
            }
            Ok(None)
        }
    }

    pub(super) fn relative_row(&self, base: &Path, offset: i32) -> Result<Option<Path>> {
        use std::ops::Bound::{self, *};
        macro_rules! or_none {
            ($e:expr) => {
                match $e {
                    Some(p) => p,
                    None => return Ok(None),
                }
            };
        }
        let column = or_none!(Path::basename(base));
        let mut rowbase = ArcStr::from(or_none!(Path::dirname(base)));
        let tablebase = ArcStr::from(or_none!(Path::dirname(rowbase.as_str())));
        let mut i = 0;
        if offset == 0 {
            Ok(Some(base.clone()))
        } else if offset < 0 {
            let mut iter = self
                .data
                .range::<&str, (Bound<&str>, Bound<&str>)>((Unbounded, Excluded(&**base)))
                .keys();
            while let Some(r) = iter.next_back() {
                let r = r?;
                let path = str::from_utf8(&r)?;
                let path_column = or_none!(Path::basename(path));
                let path_row = or_none!(Path::dirname(path));
                let path_table = or_none!(Path::dirname(path_row));
                if path_table == tablebase {
                    if path_row != rowbase.as_str() {
                        i -= 1;
                        rowbase = ArcStr::from(path_row);
                    }
                    if i == offset && path_column == column {
                        return Ok(Some(Path::from(ArcStr::from(path))));
                    }
                    if i < offset {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            Ok(None)
        } else {
            let iter = self
                .data
                .range::<&str, (Bound<&str>, Bound<&str>)>((Excluded(&**base), Unbounded))
                .keys();
            for r in iter {
                let r = r?;
                let path = str::from_utf8(&r)?;
                let path_column = or_none!(Path::basename(path));
                let path_row = or_none!(Path::dirname(path));
                let path_table = or_none!(Path::dirname(path_row));
                if path_table == tablebase {
                    if path_row != rowbase.as_str() {
                        i += 1;
                        rowbase = ArcStr::from(path_row);
                    }
                    if i == offset && path_column == column {
                        return Ok(Some(Path::from(ArcStr::from(path))));
                    }
                    if i > offset {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            Ok(None)
        }
    }

    fn unlock_subtree(&self, txn: &mut TxDb, path: &Path) -> Result<()> {
        let key = path.as_bytes();
        let mut paths = Vec::new();
        for res in self.locked.scan_prefix(key).keys() {
            let key = res?;
            let path = Path::from(ArcStr::from(str::from_utf8(&key)?));
            paths.push(path);
        }
        for path in paths {
            txn.set_unlocked(&path)?
        }
        Ok(())
    }

    pub(super) fn lookup<P: AsRef<[u8]>>(&self, path: P) -> Result<Option<Datum>> {
        lookup_value(&self.data, path)
    }

    pub(super) fn iter(
        &self,
    ) -> impl Iterator<Item = Result<(Path, DatumKind, sled::IVec)>> + 'static {
        self.data.iter().map(|res| {
            let (key, val) = res?;
            let path = Path::from(ArcStr::from(str::from_utf8(&key)?));
            let value = DatumKind::decode(&mut &*val);
            Ok((path, value, val))
        })
    }

    pub(super) fn locked(&self) -> impl Iterator<Item = Result<Path>> + 'static {
        iter_paths(&self.locked)
    }
}
