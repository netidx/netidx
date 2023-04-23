use super::Params;
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use bytes::{Buf, BufMut};
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    prelude::*,
    select_biased,
};
use netidx::{
    chars::Chars,
    pack::{Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    publisher::{Publisher, SendResult, UpdateBatch, Val},
    subscriber::Value,
    utils::{BatchItem, Batched},
};
use netidx_protocols::rpc::server::RpcReply;
use parking_lot::Mutex;
use sled;
use std::{
    cmp::{max, min},
    collections::{HashMap, HashSet},
    fmt::Write,
    path::PathBuf,
    str,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task;

pub enum Sendable {
    Packed(Arc<Mutex<Value>>),
    Rpc(RpcReply),
    Write(SendResult),
}

impl Sendable {
    pub fn send(self, v: Value) {
        match self {
            Sendable::Packed(res) => {
                let mut res = res.lock();
                match &*res {
                    Value::Error(_) => (),
                    _ => {
                        *res = v;
                    }
                }
            }
            Sendable::Rpc(mut reply) => {
                reply.send(v);
            }
            Sendable::Write(reply) => {
                reply.send(v);
            }
        }
    }
}

pub type Reply = Option<Sendable>;
type Txns = Vec<(TxnOp, Reply)>;

lazy_static! {
    static ref BUF: Pool<Vec<u8>> = Pool::new(8, 16384);
    static ref PDPAIR: Pool<Vec<(Path, UpdateKind)>> = Pool::new(256, 8124);
    static ref PATHS: Pool<Vec<Path>> = Pool::new(256, 65534);
    static ref TXNS: Pool<Txns> = Pool::new(16, 65534);
    static ref STXNS: Pool<Txns> = Pool::new(65534, 32);
    static ref BYPATH: Pool<HashMap<Path, Pooled<Txns>>> = Pool::new(16, 65534);
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
    pub(super) added_roots: Pooled<Vec<Path>>,
    pub(super) removed_roots: Pooled<Vec<Path>>,
}

impl Update {
    fn new() -> Update {
        Update {
            data: PDPAIR.take(),
            formula: PDPAIR.take(),
            on_write: PDPAIR.take(),
            locked: PATHS.take(),
            unlocked: PATHS.take(),
            added_roots: PATHS.take(),
            removed_roots: PATHS.take(),
        }
    }

    fn merge_from(&mut self, mut other: Update) {
        self.data.extend(other.data.drain(..));
        self.formula.extend(other.formula.drain(..));
        self.on_write.extend(other.on_write.drain(..));
        self.locked.extend(other.locked.drain(..));
        self.unlocked.extend(other.unlocked.drain(..));
        self.added_roots.extend(other.added_roots.drain(..));
        self.removed_roots.extend(other.removed_roots.drain(..));
    }

    fn merge(mut self, other: Update) -> Update {
        self.merge_from(other);
        self
    }
}

pub enum DatumKind {
    Data,
    Formula,
    Deleted,
    Invalid,
}

impl DatumKind {
    fn decode(buf: &mut impl Buf) -> DatumKind {
        match buf.get_u8() {
            0 => DatumKind::Data,
            1 => DatumKind::Formula,
            2 => DatumKind::Deleted,
            _ => DatumKind::Invalid,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Datum {
    Data(Value),
    Formula(Value, Value),
    Deleted,
}

impl Pack for Datum {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Datum::Data(v) => v.encoded_len(),
            Datum::Formula(f, w) => f.encoded_len() + w.encoded_len(),
            Datum::Deleted => 0,
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
            Datum::Deleted => Ok(buf.put_u8(2)),
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
                2 => Ok(Datum::Deleted),
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

enum TxnOp {
    Remove(Path),
    SetData(bool, Path, Value),
    SetFormula(Path, Value),
    SetOnWrite(Path, Value),
    CreateSheet {
        base: Path,
        rows: usize,
        max_rows: usize,
        max_columns: usize,
        cols: usize,
        lock: bool,
    },
    AddSheetColumns {
        base: Path,
        cols: usize,
    },
    AddSheetRows {
        base: Path,
        rows: usize,
    },
    DelSheetColumns {
        base: Path,
        cols: usize,
    },
    DelSheetRows {
        base: Path,
        rows: usize,
    },
    CreateTable {
        base: Path,
        rows: Vec<Chars>,
        cols: Vec<Chars>,
        lock: bool,
    },
    AddTableColumns {
        base: Path,
        cols: Vec<Chars>,
    },
    AddTableRows {
        base: Path,
        rows: Vec<Chars>,
    },
    DelTableColumns {
        base: Path,
        cols: Vec<Chars>,
    },
    DelTableRows {
        base: Path,
        rows: Vec<Chars>,
    },
    SetLocked(Path),
    SetUnlocked(Path),
    AddRoot(Path),
    DelRoot(Path),
    RemoveSubtree(Path),
    Flush(oneshot::Sender<()>),
}

impl TxnOp {
    fn path(&self) -> Path {
        use TxnOp::*;
        match self {
            Remove(p) => p.clone(),
            SetData(_, p, _) => p.clone(),
            SetFormula(p, _) => p.clone(),
            SetOnWrite(p, _) => p.clone(),
            CreateSheet { base, .. } => base.clone(),
            AddSheetColumns { base, .. } => base.clone(),
            AddSheetRows { base, .. } => base.clone(),
            DelSheetColumns { base, .. } => base.clone(),
            DelSheetRows { base, .. } => base.clone(),
            CreateTable { base, .. } => base.clone(),
            AddTableColumns { base, .. } => base.clone(),
            AddTableRows { base, .. } => base.clone(),
            DelTableColumns { base, .. } => base.clone(),
            DelTableRows { base, .. } => base.clone(),
            SetLocked(p) => p.clone(),
            SetUnlocked(p) => p.clone(),
            RemoveSubtree(p) => p.clone(),
            AddRoot(p) => p.clone(),
            DelRoot(p) => p.clone(),
            Flush(_) => Path::root(),
        }
    }
}

pub struct Txn(Pooled<Txns>);

impl Txn {
    pub fn new() -> Self {
        Self(TXNS.take())
    }

    pub fn dirty(&self) -> bool {
        self.0.len() > 0
    }

    pub fn remove(&mut self, path: Path, reply: Reply) {
        self.0.push((TxnOp::Remove(path), reply))
    }

    pub fn set_data(&mut self, update: bool, path: Path, value: Value, reply: Reply) {
        self.0.push((TxnOp::SetData(update, path, value), reply))
    }

    pub fn set_formula(&mut self, path: Path, value: Value, reply: Reply) {
        self.0.push((TxnOp::SetFormula(path, value), reply))
    }

    pub fn set_on_write(&mut self, path: Path, value: Value, reply: Reply) {
        self.0.push((TxnOp::SetOnWrite(path, value), reply))
    }

    pub fn create_sheet(
        &mut self,
        base: Path,
        rows: usize,
        cols: usize,
        max_rows: usize,
        max_columns: usize,
        lock: bool,
        reply: Reply,
    ) {
        self.0.push((
            TxnOp::CreateSheet { base, rows, cols, max_rows, max_columns, lock },
            reply,
        ))
    }

    pub fn add_sheet_columns(&mut self, base: Path, cols: usize, reply: Reply) {
        self.0.push((TxnOp::AddSheetColumns { base, cols }, reply))
    }

    pub fn add_sheet_rows(&mut self, base: Path, rows: usize, reply: Reply) {
        self.0.push((TxnOp::AddSheetRows { base, rows }, reply))
    }

    pub fn del_sheet_columns(&mut self, base: Path, cols: usize, reply: Reply) {
        self.0.push((TxnOp::DelSheetColumns { base, cols }, reply))
    }

    pub fn del_sheet_rows(&mut self, base: Path, rows: usize, reply: Reply) {
        self.0.push((TxnOp::DelSheetRows { base, rows }, reply))
    }

    pub fn create_table(
        &mut self,
        base: Path,
        rows: Vec<Chars>,
        cols: Vec<Chars>,
        lock: bool,
        reply: Reply,
    ) {
        self.0.push((TxnOp::CreateTable { base, rows, cols, lock }, reply))
    }

    pub fn add_table_columns(&mut self, base: Path, cols: Vec<Chars>, reply: Reply) {
        self.0.push((TxnOp::AddTableColumns { base, cols }, reply))
    }

    pub fn add_table_rows(&mut self, base: Path, rows: Vec<Chars>, reply: Reply) {
        self.0.push((TxnOp::AddTableRows { base, rows }, reply))
    }

    pub fn del_table_columns(&mut self, base: Path, cols: Vec<Chars>, reply: Reply) {
        self.0.push((TxnOp::DelTableColumns { base, cols }, reply))
    }

    pub fn del_table_rows(&mut self, base: Path, rows: Vec<Chars>, reply: Reply) {
        self.0.push((TxnOp::DelTableRows { base, rows }, reply))
    }

    pub fn set_locked(&mut self, path: Path, reply: Reply) {
        self.0.push((TxnOp::SetLocked(path), reply))
    }

    pub fn set_unlocked(&mut self, path: Path, reply: Reply) {
        self.0.push((TxnOp::SetUnlocked(path), reply))
    }

    pub fn add_root(&mut self, path: Path, reply: Reply) {
        self.0.push((TxnOp::AddRoot(path), reply));
    }

    pub fn del_root(&mut self, path: Path, reply: Reply) {
        self.0.push((TxnOp::DelRoot(path), reply));
    }

    pub fn remove_subtree(&mut self, path: Path, reply: Reply) {
        self.0.push((TxnOp::RemoveSubtree(path), reply))
    }
}

fn remove(data: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    let key = path.as_bytes();
    let mut val = BUF.take();
    Datum::Deleted.encode(&mut *val)?;
    if let Some(data) = data.insert(key, &**val)? {
        match DatumKind::decode(&mut &*data) {
            DatumKind::Data => pending.data.push((path, UpdateKind::Deleted)),
            DatumKind::Formula => {
                pending.formula.push((path.clone(), UpdateKind::Deleted));
                pending.on_write.push((path, UpdateKind::Deleted));
            }
            DatumKind::Deleted | DatumKind::Invalid => (),
        }
    }
    Ok(())
}

fn set_data(
    data: &sled::Tree,
    pending: &mut Update,
    update: bool,
    path: Path,
    value: Value,
) -> Result<()> {
    let key = path.as_bytes();
    let mut val = BUF.take();
    let datum = Datum::Data(value.clone());
    datum.encode(&mut *val)?;
    let up = match data.insert(key, &**val)? {
        None => UpdateKind::Inserted(value),
        Some(data) => match DatumKind::decode(&mut &*data) {
            DatumKind::Data => UpdateKind::Updated(value),
            DatumKind::Formula | DatumKind::Deleted | DatumKind::Invalid => {
                UpdateKind::Inserted(value)
            }
        },
    };
    if update {
        pending.data.push((path, up));
    }
    Ok(())
}

fn set_formula(
    data: &sled::Tree,
    pending: &mut Update,
    path: Path,
    value: Value,
) -> Result<()> {
    let key = path.as_bytes();
    let mut val = BUF.take();
    let up = match data.get(key)? {
        None => {
            Datum::Formula(value.clone(), Value::Null).encode(&mut *val)?;
            UpdateKind::Inserted(value)
        }
        Some(data) => match DatumKind::decode(&mut &*data) {
            DatumKind::Data | DatumKind::Deleted | DatumKind::Invalid => {
                Datum::Formula(value.clone(), Value::Null).encode(&mut *val)?;
                UpdateKind::Inserted(value)
            }
            DatumKind::Formula => {
                match Datum::decode(&mut &*data)? {
                    Datum::Deleted | Datum::Data(_) => unreachable!(),
                    Datum::Formula(_, w) => {
                        Datum::Formula(value.clone(), w).encode(&mut *val)?
                    }
                }
                UpdateKind::Updated(value)
            }
        },
    };
    data.insert(key, &**val)?;
    pending.formula.push((path, up));
    Ok(())
}

fn set_on_write(
    data: &sled::Tree,
    pending: &mut Update,
    path: Path,
    value: Value,
) -> Result<()> {
    let key = path.as_bytes();
    let mut val = BUF.take();
    let up = match data.get(key)? {
        None => {
            Datum::Formula(Value::Null, value.clone()).encode(&mut *val)?;
            UpdateKind::Inserted(value)
        }
        Some(data) => match DatumKind::decode(&mut &*data) {
            DatumKind::Data | DatumKind::Deleted | DatumKind::Invalid => {
                Datum::Formula(Value::Null, value.clone()).encode(&mut *val)?;
                UpdateKind::Inserted(value)
            }
            DatumKind::Formula => {
                match Datum::decode(&mut &*data)? {
                    Datum::Deleted | Datum::Data(_) => unreachable!(),
                    Datum::Formula(f, _) => {
                        Datum::Formula(f, value.clone()).encode(&mut *val)?
                    }
                }
                UpdateKind::Updated(value)
            }
        },
    };
    data.insert(key, &**val)?;
    pending.on_write.push((path.clone(), up));
    Ok(())
}

fn merge_err(e0: Result<()>, e1: Result<()>) -> Result<()> {
    match (e0, e1) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), Err(_)) => Err(e),
        (Err(e), Ok(())) => Err(e),
        (Ok(()), Err(e)) => Err(e),
    }
}

fn create_sheet(
    data: &sled::Tree,
    locked: &sled::Tree,
    pending: &mut Update,
    base: Path,
    rows: usize,
    cols: usize,
    max_rows: usize,
    max_columns: usize,
    lock: bool,
) -> Result<()> {
    use rayon::prelude::*;
    let rd = 1 + (max(rows, max_rows) as f32).log10() as usize;
    let cd = 1 + (max(cols, max_columns) as f32).log10() as usize;
    let (up, res) = (0..rows)
        .into_par_iter()
        .map(|row| (0..cols).into_par_iter().map(move |col| (row, col)))
        .flatten()
        .fold(
            || (Update::new(), String::new(), Ok(())),
            |(mut pending, mut buf, res), (i, j)| {
                buf.clear();
                write!(buf, "{:0rwidth$}/{:0cwidth$}", i, j, rwidth = rd, cwidth = cd)
                    .unwrap();
                let path = base.append(buf.as_str());
                let res = match data.contains_key(path.as_bytes()) {
                    Ok(false) => {
                        let r = set_data(data, &mut pending, true, path, Value::Null);
                        merge_err(r, res)
                    }
                    Ok(true) => res,
                    Err(e) => Err(e.into()),
                };
                (pending, buf, res)
            },
        )
        .map(|(u, _, r)| (u, r))
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    if lock {
        set_locked(locked, pending, base)?
    }
    res
}

struct SheetDescr {
    rows: Pooled<Vec<Path>>,
    max_col: usize,
    max_col_width: usize,
}

impl SheetDescr {
    fn new(data: &sled::Tree, base: &Path) -> Result<Self> {
        let base_levels = Path::levels(base);
        let mut rows = PATHS.take();
        let mut max_col = 0;
        let mut max_col_width = 0;
        for r in data.scan_prefix(base.as_bytes()).keys() {
            if let Ok(k) = r {
                if let Ok(path) = str::from_utf8(&*k) {
                    if Path::is_parent(base, path) {
                        let mut row = path;
                        let mut level = Path::levels(row);
                        loop {
                            if level == base_levels + 1 {
                                break;
                            } else if level == base_levels + 2 {
                                if let Some(col) = Path::basename(row) {
                                    if let Ok(c) = col.parse::<usize>() {
                                        max_col_width = max(max_col_width, col.len());
                                        max_col = max(c, max_col);
                                    }
                                }
                            }
                            row = Path::dirname(row).unwrap_or("/");
                            level -= 1;
                        }
                        match rows.last() {
                            None => rows.push(Path::from(ArcStr::from(row))),
                            Some(last) => {
                                if last.as_ref() != row {
                                    rows.push(Path::from(ArcStr::from(row)));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(Self { rows, max_col, max_col_width })
    }
}

fn add_sheet_columns(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    cols: usize,
) -> Result<()> {
    use rayon::prelude::*;
    let mut cs = SheetDescr::new(data, &base)?;
    let max_col = cs.max_col;
    let max_col_width = cs.max_col_width;
    if cs.max_col_width < 1 + ((cs.max_col + cols + 1) as f32).log10() as usize {
        bail!("columns full")
    }
    let (up, res) = cs
        .rows
        .par_drain(..)
        .fold(
            || (Update::new(), String::new(), Ok(())),
            |(mut pending, mut buf, mut res), row| {
                for i in 1..cols + 1 {
                    let col = max_col + i;
                    buf.clear();
                    write!(buf, "{}/{:0cwidth$}", row, col, cwidth = max_col_width)
                        .unwrap();
                    let path = Path::from(ArcStr::from(buf.as_str()));
                    res = match data.contains_key(path.as_bytes()) {
                        Ok(false) => {
                            let r = set_data(data, &mut pending, true, path, Value::Null);
                            merge_err(r, res)
                        }
                        Ok(true) => res,
                        Err(e) => Err(e.into()),
                    }
                }
                (pending, buf, res)
            },
        )
        .map(|(u, _, r)| (u, r))
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn del_sheet_columns(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    cols: usize,
) -> Result<()> {
    use rayon::prelude::*;
    let mut cs = SheetDescr::new(data, &base)?;
    let max_col = cs.max_col;
    let max_col_width = cs.max_col_width;
    let cols = min(cs.max_col, cols);
    let (up, res) = cs
        .rows
        .par_drain(..)
        .fold(
            || (Update::new(), String::new(), Ok(())),
            |(mut pending, mut buf, mut res), row| {
                for col in (max_col - cols..max_col + 1).rev() {
                    buf.clear();
                    write!(buf, "{}/{:0cw$}", row, col, cw = max_col_width).unwrap();
                    let path = Path::from(ArcStr::from(buf.as_str()));
                    res = merge_err(remove(data, &mut pending, path), res);
                }
                (pending, buf, res)
            },
        )
        .map(|(u, _, r)| (u, r))
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn add_sheet_rows(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    rows: usize,
) -> Result<()> {
    use rayon::prelude::*;
    let base_levels = Path::levels(&base);
    let mut max_row = 0;
    let mut max_col = 0;
    let mut max_row_width = 0;
    let mut max_col_width = 0;
    let mut cols = HashSet::new();
    for r in data.scan_prefix(base.as_bytes()).keys() {
        let k = r?;
        let path = str::from_utf8(&k)?;
        if Path::is_parent(&base, path) {
            if Path::levels(path) == base_levels + 2 {
                if let Some(row) = Path::dirname(path) {
                    if let Some(row) = Path::basename(row) {
                        if let Ok(i) = row.parse::<usize>() {
                            max_row = max(i, max_row);
                            max_row_width = max(row.len(), max_row_width);
                        }
                    }
                }
                let col = Path::basename(path)
                    .and_then(|p| p.parse::<usize>().ok().map(move |i| (p, i)))
                    .map(|(col, i)| {
                        max_col = max(i, max_col);
                        max_col_width = max(col.len(), max_col_width);
                        i
                    });
                if let Some(col) = col {
                    cols.insert(col);
                }
            }
        }
    }
    if max_row_width < 1 + ((max_row + rows + 1) as f32).log10() as usize {
        bail!("sheet is full")
    }
    let (up, res) = (max_row + 1..max_row + rows)
        .into_par_iter()
        .fold(
            || (Update::new(), String::new(), Ok(())),
            |(mut pending, mut buf, mut res), row| {
                for col in &cols {
                    buf.clear();
                    write!(
                        buf,
                        "{:0rwidth$}/{:0cwidth$}",
                        row,
                        col,
                        rwidth = max_row_width,
                        cwidth = max_col_width,
                    )
                    .unwrap();
                    let path = base.append(&buf);
                    res = match data.contains_key(path.as_bytes()) {
                        Ok(false) => {
                            let r = set_data(data, &mut pending, true, path, Value::Null);
                            merge_err(r, res)
                        }
                        Ok(true) => res,
                        Err(e) => Err(e.into()),
                    }
                }
                (pending, buf, res)
            },
        )
        .map(|(u, _, r)| (u, r))
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn del_sheet_rows(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    rows: usize,
) -> Result<()> {
    use rayon::prelude::*;
    let mut cs = SheetDescr::new(data, &base)?;
    let len = cs.rows.len();
    let rows = min(len, rows);
    let (up, res) = cs
        .rows
        .par_drain(len - rows..len + 1)
        .fold(
            || (Update::new(), Ok(())),
            |(mut pending, res), row| {
                let res = merge_err(remove_subtree(data, &mut pending, row), res);
                (pending, res)
            },
        )
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn create_table(
    data: &sled::Tree,
    locked: &sled::Tree,
    pending: &mut Update,
    base: Path,
    rows: Vec<Chars>,
    cols: Vec<Chars>,
    lock: bool,
) -> Result<()> {
    use rayon::prelude::*;
    let rows: Vec<String> =
        rows.into_iter().map(|c| Path::escape(&c).into_owned()).collect();
    let cols: Vec<String> =
        cols.into_iter().map(|c| Path::escape(&c).into_owned()).collect();
    let (up, res) = rows
        .par_iter()
        .map(|row| cols.par_iter().map(move |col| (row, col)))
        .flatten()
        .fold(
            || (Update::new(), String::new(), Ok(())),
            |(mut pending, mut buf, res), (row, col)| {
                buf.clear();
                write!(buf, "{}/{}", row, col).unwrap();
                let path = base.append(buf.as_str());
                let res = match data.contains_key(path.as_bytes()) {
                    Ok(false) => {
                        let r = set_data(data, &mut pending, true, path, Value::Null);
                        merge_err(r, res)
                    }
                    Ok(true) => res,
                    Err(e) => Err(e.into()),
                };
                (pending, buf, res)
            },
        )
        .map(|(u, _, r)| (u, r))
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    if lock {
        set_locked(locked, pending, base)?
    }
    res
}

fn table_rows(data: &sled::Tree, base: &Path) -> Result<Pooled<Vec<Path>>> {
    let base_levels = Path::levels(&base);
    let mut paths = PATHS.take();
    for r in data.scan_prefix(base.as_bytes()).keys() {
        let k = r?;
        if let Ok(path) = str::from_utf8(&*k) {
            if Path::is_parent(base, path) {
                let mut row = path;
                let mut level = Path::levels(row);
                while level > base_levels + 1 {
                    row = Path::dirname(row).unwrap_or("/");
                    level -= 1;
                }
                match paths.last() {
                    None => paths.push(Path::from(ArcStr::from(row))),
                    Some(last) => {
                        if last.as_ref() != row {
                            paths.push(Path::from(ArcStr::from(row)));
                        }
                    }
                }
            }
        }
    }
    Ok(paths)
}

fn add_table_columns(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    cols: Vec<Chars>,
) -> Result<()> {
    use rayon::prelude::*;
    let cols: Vec<String> =
        cols.into_iter().map(|c| Path::escape(&c).into_owned()).collect();
    let (up, res) = table_rows(data, &base)?
        .par_drain(..)
        .fold(
            || (Update::new(), Ok(())),
            |(mut pending, mut res), row| {
                for col in &cols {
                    let path = row.append(col);
                    res = match data.contains_key(path.as_bytes()) {
                        Ok(false) => {
                            let r = set_data(data, &mut pending, true, path, Value::Null);
                            merge_err(r, res)
                        }
                        Ok(true) => res,
                        Err(e) => Err(e.into()),
                    }
                }
                (pending, res)
            },
        )
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn del_table_columns(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    cols: Vec<Chars>,
) -> Result<()> {
    use rayon::prelude::*;
    let cols: Vec<String> =
        cols.into_iter().map(|c| Path::escape(&c).into_owned()).collect();
    let (up, res) = table_rows(data, &base)?
        .par_drain(..)
        .fold(
            || (Update::new(), Ok(())),
            |(mut pending, mut res), row| {
                for col in &cols {
                    let path = row.append(col);
                    res = merge_err(remove(data, &mut pending, path), res);
                }
                (pending, res)
            },
        )
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn add_table_rows(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    rows: Vec<Chars>,
) -> Result<()> {
    use rayon::prelude::*;
    let rows: Vec<String> =
        rows.into_iter().map(|c| Path::escape(&c).into_owned()).collect();
    let base_levels = Path::levels(&base);
    let mut cols = HashSet::new();
    for r in data.scan_prefix(base.as_bytes()).keys() {
        let k = r?;
        let path = str::from_utf8(&k)?;
        if Path::is_parent(&base, path) {
            if Path::levels(path) == base_levels + 2 {
                let col = Path::basename(path).unwrap_or("");
                cols.insert(k.subslice(k.len() - col.len(), col.len()));
            }
        }
    }
    let cols: HashSet<String> = cols
        .into_iter()
        .filter_map(|k| str::from_utf8(&k).ok().map(String::from))
        .collect();
    let (up, res) = rows
        .into_par_iter()
        .fold(
            || (Update::new(), String::new(), Ok(())),
            |(mut pending, mut buf, mut res), row| {
                for col in &cols {
                    buf.clear();
                    write!(buf, "{}/{}", row, col).unwrap();
                    let path = base.append(&buf);
                    res = match data.contains_key(path.as_bytes()) {
                        Ok(false) => {
                            let r = set_data(data, &mut pending, true, path, Value::Null);
                            merge_err(r, res)
                        }
                        Ok(true) => res,
                        Err(e) => Err(e.into()),
                    }
                }
                (pending, buf, res)
            },
        )
        .map(|(u, _, r)| (u, r))
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn del_table_rows(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    rows: Vec<Chars>,
) -> Result<()> {
    use rayon::prelude::*;
    let rows: Vec<String> =
        rows.into_iter().map(|c| Path::escape(&c).into_owned()).collect();
    let (up, res) = rows
        .into_par_iter()
        .fold(
            || (Update::new(), Ok(())),
            |(mut pending, res), row| {
                let path = base.append(&row);
                let res = merge_err(remove_subtree(data, &mut pending, path), res);
                (pending, res)
            },
        )
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn is_locked(locked: &sled::Tree, path: &Path, parent_only: bool) -> Result<bool> {
    let mut iter = if parent_only {
        locked.range(..path.as_bytes())
    } else {
        locked.range(..=path.as_bytes())
    };
    loop {
        match iter.next_back() {
            None => break Ok(false),
            Some(r) => {
                let (k, v) = r?;
                let k = str::from_utf8(&k)?;
                if Path::is_parent(k, &path) {
                    break Ok(&*v == &[1u8]);
                }
            }
        }
    }
}

fn set_locked(locked: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    if is_locked(locked, &path, true)? {
        locked.remove(path.as_bytes())?;
    } else {
        locked.insert(path.as_bytes(), &[1u8])?;
    }
    pending.locked.push(path);
    Ok(())
}

fn set_unlocked(locked: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    if !is_locked(locked, &path, true)? {
        locked.remove(path.as_bytes())?;
    } else {
        locked.insert(path.as_bytes(), &[0u8])?;
    }
    pending.unlocked.push(path);
    Ok(())
}

fn remove_subtree(data: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    use rayon::prelude::*;
    let mut paths = PATHS.take();
    for res in data.scan_prefix(path.as_ref()).keys() {
        let key = res?;
        let key = str::from_utf8(&key)?;
        if Path::is_parent(&path, &key) {
            paths.push(Path::from(ArcStr::from(key)));
        }
    }
    let (up, res) = paths
        .par_drain(..)
        .fold(
            || (Update::new(), Ok(())),
            |(mut pending, res), path| {
                let res = merge_err(remove(data, &mut pending, path), res);
                (pending, res)
            },
        )
        .reduce(
            || (Update::new(), Ok(())),
            |(u0, r0), (u1, r1)| (u0.merge(u1), merge_err(r0, r1)),
        );
    pending.merge_from(up);
    res
}

fn add_root(roots: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    let key = path.as_bytes();
    if let Some(r) = roots.range(..key).next_back() {
        let (prev, _) = r?;
        let prev = str::from_utf8(&prev)?;
        if Path::is_parent(prev, &path) {
            bail!("a parent path is already a root")
        }
    }
    if !roots.contains_key(key)? {
        roots.insert(key, &[])?;
        pending.added_roots.push(path);
    }
    Ok(())
}

fn del_root(
    data: &sled::Tree,
    roots: &sled::Tree,
    locked: &sled::Tree,
    pending: &mut Update,
    path: Path,
) -> Result<()> {
    let key = path.as_bytes();
    if roots.contains_key(key)? {
        let mut iter = roots.range(..key).keys();
        let remove = loop {
            match iter.next_back() {
                None => break false,
                Some(r) => {
                    let k = r?;
                    let k = str::from_utf8(&k)?;
                    if Path::is_parent(k, &path) {
                        break true;
                    }
                }
            }
        };
        if remove {
            remove_subtree(data, pending, path.clone())?
        }
        for r in roots.scan_prefix(key).keys() {
            let k = r?;
            let k = str::from_utf8(&k)?;
            if Path::is_parent(&path, k) {
                roots.remove(&k)?;
                pending.removed_roots.push(Path::from(ArcStr::from(k)));
            }
        }
        for r in locked.scan_prefix(key).keys() {
            let k = r?;
            let k = str::from_utf8(&k)?;
            if Path::is_parent(&path, k) {
                locked.remove(&k)?;
                pending.unlocked.push(Path::from(ArcStr::from(k)));
            }
        }
    }
    Ok(())
}

fn send_reply(reply: Reply, r: Result<()>) {
    match (r, reply) {
        (Ok(()), Some(reply)) => {
            reply.send(Value::Ok);
        }
        (Err(e), Some(reply)) => {
            let e = Value::Error(Chars::from(format!("{}", e)));
            reply.send(e);
        }
        (_, None) => (),
    }
}

fn commit_complex(
    data: &sled::Tree,
    locked: &sled::Tree,
    roots: &sled::Tree,
    mut txn: Txn,
) -> Update {
    let mut pending = Update::new();
    for (op, reply) in txn.0.drain(..) {
        let r = match op {
            TxnOp::CreateSheet { base, rows, cols, max_rows, max_columns, lock } => {
                create_sheet(
                    &data,
                    &locked,
                    &mut pending,
                    base,
                    rows,
                    cols,
                    max_rows,
                    max_columns,
                    lock,
                )
            }
            TxnOp::AddSheetColumns { base, cols } => {
                add_sheet_columns(&data, &mut pending, base, cols)
            }
            TxnOp::AddSheetRows { base, rows } => {
                add_sheet_rows(&data, &mut pending, base, rows)
            }
            TxnOp::DelSheetColumns { base, cols } => {
                del_sheet_columns(&data, &mut pending, base, cols)
            }
            TxnOp::DelSheetRows { base, rows } => {
                del_sheet_rows(&data, &mut pending, base, rows)
            }
            TxnOp::CreateTable { base, rows, cols, lock } => {
                create_table(&data, &locked, &mut pending, base, rows, cols, lock)
            }
            TxnOp::AddTableColumns { base, cols } => {
                add_table_columns(&data, &mut pending, base, cols)
            }
            TxnOp::AddTableRows { base, rows } => {
                add_table_rows(&data, &mut pending, base, rows)
            }
            TxnOp::DelTableColumns { base, cols } => {
                del_table_columns(&data, &mut pending, base, cols)
            }
            TxnOp::DelTableRows { base, rows } => {
                del_table_rows(&data, &mut pending, base, rows)
            }
            TxnOp::Remove(path) => remove(&data, &mut pending, path),
            TxnOp::RemoveSubtree(path) => remove_subtree(&data, &mut pending, path),
            TxnOp::SetData(update, path, value) => {
                set_data(&data, &mut pending, update, path, value)
            }
            TxnOp::SetFormula(path, value) => {
                set_formula(&data, &mut pending, path, value)
            }
            TxnOp::SetOnWrite(path, value) => {
                set_on_write(&data, &mut pending, path, value)
            }
            TxnOp::SetLocked(path) => set_locked(&locked, &mut pending, path),
            TxnOp::SetUnlocked(path) => set_unlocked(&locked, &mut pending, path),
            TxnOp::AddRoot(path) => add_root(&roots, &mut pending, path),
            TxnOp::DelRoot(path) => del_root(&data, &roots, &locked, &mut pending, path),
            TxnOp::Flush(finished) => {
                let _: Result<_, _> = data.flush();
                let _: Result<_, _> = locked.flush();
                let _: Result<_, _> = finished.send(());
                Ok(())
            }
        };
        send_reply(reply, r);
    }
    pending
}

fn commit_simple(data: &sled::Tree, locked: &sled::Tree, mut txn: Txn) -> Update {
    use rayon::prelude::*;
    let mut by_path = BYPATH.take();
    for (op, reply) in txn.0.drain(..) {
        by_path.entry(op.path()).or_insert_with(|| STXNS.take()).push((op, reply));
    }
    by_path
        .par_drain()
        .fold(
            || Update::new(),
            |mut pending, (_, mut ops)| {
                for (op, reply) in ops.drain(..) {
                    let r = match op {
                        TxnOp::SetData(update, path, value) => {
                            set_data(data, &mut pending, update, path, value)
                        }
                        TxnOp::Remove(path) => remove(data, &mut pending, path),
                        TxnOp::SetFormula(path, value) => {
                            set_formula(data, &mut pending, path, value)
                        }
                        TxnOp::SetOnWrite(path, value) => {
                            set_on_write(data, &mut pending, path, value)
                        }
                        TxnOp::SetLocked(path) => set_locked(locked, &mut pending, path),
                        TxnOp::SetUnlocked(path) => {
                            set_unlocked(locked, &mut pending, path)
                        }
                        TxnOp::CreateSheet { .. }
                        | TxnOp::AddSheetColumns { .. }
                        | TxnOp::AddSheetRows { .. }
                        | TxnOp::DelSheetColumns { .. }
                        | TxnOp::DelSheetRows { .. }
                        | TxnOp::CreateTable { .. }
                        | TxnOp::AddTableColumns { .. }
                        | TxnOp::AddTableRows { .. }
                        | TxnOp::DelTableColumns { .. }
                        | TxnOp::DelTableRows { .. }
                        | TxnOp::RemoveSubtree { .. }
                        | TxnOp::AddRoot(_)
                        | TxnOp::DelRoot(_)
                        | TxnOp::Flush(_) => unreachable!(),
                    };
                    send_reply(reply, r)
                }
                pending
            },
        )
        .reduce(|| Update::new(), |u0, u1| u0.merge(u1))
}

struct Stats {
    publisher: Publisher,
    busy: Val,
    queued: Val,
    queued_cnt: AtomicUsize,
    deleting: Val,
    to_commit: UnboundedSender<UpdateBatch>,
}

async fn stats_commit_task(rx: UnboundedReceiver<UpdateBatch>) {
    let mut rx = Batched::new(rx, 1_000_000);
    let mut pending: Option<UpdateBatch> = None;
    while let Some(batch) = rx.next().await {
        match batch {
            BatchItem::InBatch(mut b) => match &mut pending {
                Some(pending) => {
                    let _: Result<_> = pending.merge_from(&mut b);
                }
                None => {
                    pending = Some(b);
                }
            },
            BatchItem::EndBatch => {
                if let Some(pending) = pending.take() {
                    pending.commit(Some(Duration::from_secs(10))).await
                }
            }
        }
    }
}

impl Stats {
    fn new(publisher: Publisher, base_path: Path) -> Result<Arc<Self>> {
        let busy = publisher.publish(base_path.append("busy"), false)?;
        let queued = publisher.publish(base_path.append("queued"), 0)?;
        let deleting = publisher.publish(base_path.append("background-delete"), false)?;
        let (tx, rx) = unbounded();
        task::spawn(stats_commit_task(rx));
        Ok(Arc::new(Stats {
            publisher,
            busy,
            queued,
            queued_cnt: AtomicUsize::new(0),
            deleting,
            to_commit: tx,
        }))
    }

    fn inc_queued(&self) {
        let n = self.queued_cnt.fetch_add(1, Ordering::Relaxed) + 1;
        let mut batch = self.publisher.start_batch();
        self.queued.update(&mut batch, n);
        let _: Result<_, _> = self.to_commit.unbounded_send(batch);
    }

    fn dec_queued(&self) {
        let n = self.queued_cnt.fetch_sub(1, Ordering::Relaxed) - 1;
        let mut batch = self.publisher.start_batch();
        self.queued.update(&mut batch, n);
        self.busy.update(&mut batch, true);
        let _: Result<_, _> = self.to_commit.unbounded_send(batch);
    }

    fn set_busy(&self, busy: bool) {
        let mut batch = self.publisher.start_batch();
        self.busy.update(&mut batch, busy);
        let _: Result<_, _> = self.to_commit.unbounded_send(batch);
    }

    fn set_deleting(&self, deleting: bool) {
        let mut batch = self.publisher.start_batch();
        self.deleting.update(&mut batch, deleting);
        let _: Result<_, _> = self.to_commit.unbounded_send(batch);
    }
}

async fn background_delete_task(data: sled::Tree, stats: Option<Arc<Stats>>) {
    if let Some(stats) = &stats {
        stats.set_deleting(true);
    }
    task::block_in_place(|| {
        for r in data.iter() {
            if let Ok((k, v)) = r {
                if let DatumKind::Deleted = DatumKind::decode(&mut &*v) {
                    // CR estokes: log these errors
                    let _: Result<_, _> =
                        data.compare_and_swap(k, Some(v), None::<sled::IVec>);
                }
            }
        }
    });
    if let Some(stats) = &stats {
        stats.set_deleting(false);
    }
}

async fn commit_txns_task(
    stats: Option<Arc<Stats>>,
    data: sled::Tree,
    locked: sled::Tree,
    roots: sled::Tree,
    incoming: UnboundedReceiver<Txn>,
    outgoing: UnboundedSender<Update>,
) {
    let mut incoming = incoming.fuse();
    async fn wait_delete_task(jh: &mut Option<task::JoinHandle<()>>) {
        match jh {
            Some(jh) => {
                // CR estokes: log this
                let _: Result<_, _> = jh.await;
            }
            None => future::pending().await,
        }
    }
    let mut delete_task: Option<task::JoinHandle<()>> = None;
    let mut delete_required = false;
    loop {
        select_biased! {
            () = wait_delete_task(&mut delete_task).fuse() => {
                delete_task = None;
            }
            txn = incoming.select_next_some() => {
                if let Some(stats) = &stats {
                    stats.dec_queued();
                }
                let (simple, delete) =
                    txn.0.iter().fold((true, false), |(simple, delete), op| match &op.0 {
                        TxnOp::CreateSheet { .. }
                        | TxnOp::AddSheetColumns { .. }
                        | TxnOp::AddSheetRows { .. }
                        | TxnOp::CreateTable { .. }
                        | TxnOp::AddTableColumns { .. }
                        | TxnOp::AddTableRows { .. }
                        | TxnOp::AddRoot(_)
                        | TxnOp::Flush(_) => (false, delete),
                        TxnOp::RemoveSubtree { .. }
                        | TxnOp::DelTableColumns { .. }
                        | TxnOp::DelTableRows { .. }
                        | TxnOp::DelSheetColumns { .. }
                        | TxnOp::DelSheetRows { .. }
                        | TxnOp::DelRoot(_) => (false, true),
                        TxnOp::Remove(_) => (simple, true),
                        TxnOp::SetData(_, _, _)
                        | TxnOp::SetFormula(_, _)
                        | TxnOp::SetOnWrite(_, _)
                        | TxnOp::SetLocked(_)
                        | TxnOp::SetUnlocked(_) => (simple, delete),
                    });
                delete_required |= delete;
                let pending = if simple {
                    task::block_in_place(|| commit_simple(&data, &locked, txn))
                } else {
                    task::block_in_place(|| commit_complex(&data, &locked, &roots, txn))
                };
                if let Some(stats) = &stats {
                    stats.set_busy(false);
                }
                match outgoing.unbounded_send(pending) {
                    Ok(()) => (),
                    Err(_) => break,
                }
                if delete_required && delete_task.is_none() {
                    let job = background_delete_task(data.clone(), stats.clone());
                    delete_required = false;
                    delete_task = Some(task::spawn(job));
                }
            }
            complete => break,
        }
    }
}

#[derive(Clone)]
pub struct Db {
    db: sled::Db,
    data: sled::Tree,
    locked: sled::Tree,
    roots: sled::Tree,
    submit_txn: UnboundedSender<Txn>,
    stats: Option<Arc<Stats>>,
}

impl Db {
    pub(super) fn new(
        cfg: &Params,
        publisher: Publisher,
        base_path: Option<Path>,
    ) -> Result<(Self, UnboundedReceiver<Update>)> {
        let stats = match base_path {
            None => None,
            Some(p) => Some(Stats::new(publisher, p)?),
        };
        let path = cfg
            .db
            .as_ref()
            .map(PathBuf::from)
            .or_else(Params::default_db_path)
            .ok_or_else(|| {
                anyhow!("db dir not specified and no default could be determined")
            })?;
        let db = sled::Config::default()
            .cache_capacity(cfg.cache_size.unwrap_or(16 * 1024 * 1024))
            .path(&path)
            .open()?;
        let data = db.open_tree("data")?;
        let locked = db.open_tree("locked")?;
        let roots = db.open_tree("roots")?;
        let (tx_incoming, rx_incoming) = unbounded();
        let (tx_outgoing, rx_outgoing) = unbounded();
        task::spawn(commit_txns_task(
            stats.clone(),
            data.clone(),
            locked.clone(),
            roots.clone(),
            rx_incoming,
            tx_outgoing,
        ));
        Ok((Db { db, data, locked, roots, submit_txn: tx_incoming, stats }, rx_outgoing))
    }

    pub fn open_tree(&self, name: &str) -> Result<sled::Tree> {
        if name == "data" || name == "locked" || name == "roots" {
            bail!("tree name reserved")
        }
        Ok(self.db.open_tree(name)?)
    }

    pub(super) fn commit(&self, txn: Txn) {
        if let Some(stats) = &self.stats {
            stats.inc_queued();
        }
        let _: Result<_, _> = self.submit_txn.unbounded_send(txn);
    }

    pub(super) async fn flush_async(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let mut txn = Txn::new();
        txn.0.push((TxnOp::Flush(tx), None));
        self.commit(txn);
        let _: Result<_, _> = rx.await;
        Ok(())
    }

    pub fn relative_column(&self, base: &Path, offset: i32) -> Result<Option<Path>> {
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
                if !Path::is_parent(&base, path) {
                    return Ok(None);
                } else if Path::dirname(path) == Some(rowbase) {
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
                if !Path::is_parent(&base, path) {
                    return Ok(None);
                } else if Path::dirname(path) == Some(rowbase) {
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

    pub fn relative_row(&self, base: &Path, offset: i32) -> Result<Option<Path>> {
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
                if !Path::is_parent(&base, path) {
                    return Ok(None);
                } else if path_table == tablebase {
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
                if !Path::is_parent(&base, path) {
                    return Ok(None);
                } else if path_table == tablebase {
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

    pub fn lookup<P: AsRef<[u8]>>(&self, path: P) -> Result<Option<Datum>> {
        lookup_value(&self.data, path)
    }

    pub fn lookup_value<P: AsRef<[u8]>>(&self, path: P) -> Option<Value> {
        self.lookup(path).ok().flatten().and_then(|d| match d {
            Datum::Deleted | Datum::Formula(_, _) => None,
            Datum::Data(v) => Some(v),
        })
    }

    fn decode_iter(
        iter: sled::Iter,
    ) -> impl Iterator<Item = Result<(Path, DatumKind, sled::IVec)>> + 'static {
        iter.map(|res| {
            let (key, val) = res?;
            let path = Path::from(ArcStr::from(str::from_utf8(&key)?));
            let value = DatumKind::decode(&mut &*val);
            Ok((path, value, val))
        })
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = Result<(Path, DatumKind, sled::IVec)>> + 'static {
        Self::decode_iter(self.data.iter())
    }

    pub fn iter_prefix(
        &self,
        prefix: Path,
    ) -> impl Iterator<Item = Result<(Path, DatumKind, sled::IVec)>> + 'static {
        Self::decode_iter(self.data.scan_prefix(&*prefix))
    }

    /// This is O(N)
    pub fn prefix_len(&self, prefix: &Path) -> usize {
        self.data.scan_prefix(&**prefix).count()
    }

    pub fn locked(&self) -> impl Iterator<Item = Result<(Path, bool)>> + 'static {
        self.locked.iter().map(|r| {
            let (k, v) = r?;
            let path = Path::from(ArcStr::from(str::from_utf8(&k)?));
            let locked = &*v == &[1u8];
            Ok((path, locked))
        })
    }

    pub fn roots(&self) -> impl Iterator<Item = Result<Path>> + 'static {
        iter_paths(&self.roots)
    }

    pub fn clear(&self) -> Result<()> {
        self.db.clear()?;
        self.data.clear()?;
        self.locked.clear()?;
        Ok(self.roots.clear()?)
    }
}
