use super::ContainerConfig;
use anyhow::Result;
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
    subscriber::Value,
};
use sled;
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    fmt::Write,
    str,
};
use tokio::task;

lazy_static! {
    static ref BUF: Pool<Vec<u8>> = Pool::new(8, 16384);
    static ref PDPAIR: Pool<Vec<(Path, UpdateKind)>> = Pool::new(256, 8124);
    static ref PATHS: Pool<Vec<Path>> = Pool::new(128, 65534);
    static ref TXNS: Pool<Vec<TxnOp>> = Pool::new(16, 65534);
    static ref STXNS: Pool<Vec<TxnOp>> = Pool::new(65534, 32);
    static ref BYPATH: Pool<HashMap<Path, Pooled<Vec<TxnOp>>>> = Pool::new(16, 65534);
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

    fn merge_from(&mut self, mut other: Update) {
        self.data.extend(other.data.drain(..));
        self.formula.extend(other.formula.drain(..));
        self.on_write.extend(other.on_write.drain(..));
        self.locked.extend(other.locked.drain(..));
        self.unlocked.extend(other.unlocked.drain(..));
    }

    fn merge(mut self, other: Update) -> Update {
        self.merge_from(other);
        self
    }
}

pub(super) enum DatumKind {
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
pub(super) enum Datum {
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
    SetLocked(Path),
    SetUnlocked(Path),
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
            CreateTable { base, .. } => base.clone(),
            AddTableColumns { base, .. } => base.clone(),
            AddTableRows { base, .. } => base.clone(),
            SetLocked(p) => p.clone(),
            SetUnlocked(p) => p.clone(),
            RemoveSubtree(p) => p.clone(),
            Flush(_) => Path::root(),
        }
    }
}

pub(super) struct Txn(Pooled<Vec<TxnOp>>);

impl Txn {
    pub(super) fn new() -> Self {
        Self(TXNS.take())
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
        max_rows: usize,
        max_columns: usize,
        lock: bool,
    ) {
        self.0.push(TxnOp::CreateSheet { base, rows, cols, max_rows, max_columns, lock })
    }

    pub(super) fn add_sheet_columns(&mut self, base: Path, cols: usize) {
        self.0.push(TxnOp::AddSheetColumns { base, cols })
    }

    pub(super) fn add_sheet_rows(&mut self, base: Path, rows: usize) {
        self.0.push(TxnOp::AddSheetRows { base, rows })
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

    pub(super) fn add_table_columns(&mut self, base: Path, cols: Vec<Chars>) {
        self.0.push(TxnOp::AddTableColumns { base, cols })
    }

    pub(super) fn add_table_rows(&mut self, base: Path, rows: Vec<Chars>) {
        self.0.push(TxnOp::AddTableRows { base, rows })
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
    let up = (0..rows)
        .into_par_iter()
        .map(|row| (0..cols).into_par_iter().map(move |col| (row, col)))
        .flatten()
        .fold(
            || (Update::new(), String::new()),
            |(mut pending, mut buf), (i, j)| {
                buf.clear();
                write!(buf, "{:0rwidth$}/{:0cwidth$}", i, j, rwidth = rd, cwidth = cd)
                    .unwrap();
                let path = base.append(buf.as_str());
                if let Ok(false) = data.contains_key(path.as_bytes()) {
                    // CR estokes: log
                    let _: Result<_> =
                        set_data(data, &mut pending, true, path, Value::Null);
                }
                (pending, buf)
            },
        )
        .map(|(u, _)| u)
        .reduce(|| Update::new(), |u0, u1| u0.merge(u1));
    pending.merge_from(up);
    if lock {
        set_locked(locked, pending, base)?
    }
    Ok(())
}

fn add_sheet_columns(
    data: &sled::Tree,
    pending: &mut Update,
    base: Path,
    cols: usize,
) -> Result<()> {
    use rayon::prelude::*;
    let base_levels = Path::levels(&base);
    let mut paths = PATHS.take();
    let mut max_col = 0;
    let mut max_col_width = 0;
    for r in data.scan_prefix(base.as_bytes()).keys() {
        if let Ok(k) = r {
            if let Ok(path) = str::from_utf8(&*k) {
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
    if max_col_width < 1 + ((max_col + cols + 1) as f32).log10() as usize {
        bail!("columns full")
    }
    let up = paths
        .par_drain(..)
        .fold(
            || (Update::new(), String::new()),
            |(mut pending, mut buf), row| {
                for i in 1..cols + 1 {
                    let col = max_col + i;
                    buf.clear();
                    write!(buf, "{}/{:0cwidth$}", row, col, cwidth = max_col_width)
                        .unwrap();
                    let path = Path::from(ArcStr::from(buf.as_str()));
                    if let Ok(false) = data.contains_key(path.as_bytes()) {
                        let _: Result<_> =
                            set_data(data, &mut pending, true, path, Value::Null);
                    }
                }
                (pending, buf)
            },
        )
        .map(|(u, _)| u)
        .reduce(|| Update::new(), |u0, u1| u0.merge(u1));
    pending.merge_from(up);
    Ok(())
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
    let cols: HashSet<usize> = data
        .scan_prefix(base.as_bytes())
        .keys()
        .filter_map(|k| k.ok())
        .filter_map(|k| {
            str::from_utf8(&k).ok().and_then(|path| {
                if Path::levels(path) != base_levels + 2 {
                    None
                } else {
                    if let Some(row) = Path::dirname(path) {
                        if let Some(row) = Path::basename(row) {
                            if let Ok(i) = row.parse::<usize>() {
                                max_row = max(i, max_row);
                                max_row_width = max(row.len(), max_row_width);
                            }
                        }
                    }
                    Path::basename(path)
                        .and_then(|p| p.parse::<usize>().ok().map(move |i| (p, i)))
                        .map(|(col, i)| {
                            max_col = max(i, max_col);
                            max_col_width = max(col.len(), max_col_width);
                            i
                        })
                }
            })
        })
        .collect();
    if max_row_width < 1 + ((max_row + rows + 1) as f32).log10() as usize {
        bail!("sheet is full")
    }
    let up = (max_row + 1..max_row + rows)
        .into_par_iter()
        .fold(
            || (Update::new(), String::new()),
            |(mut pending, mut buf), row| {
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
                    if let Ok(false) = data.contains_key(path.as_bytes()) {
                        let _: Result<_> =
                            set_data(data, &mut pending, true, path, Value::Null);
                    }
                }
                (pending, buf)
            },
        )
        .map(|(u, _)| u)
        .reduce(|| Update::new(), |u0, u1| u0.merge(u1));
    pending.merge_from(up);
    Ok(())
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
    pending.merge_from(
        rows.par_iter()
            .map(|row| cols.par_iter().map(move |col| (row, col)))
            .flatten()
            .fold(
                || (Update::new(), String::new()),
                |(mut pending, mut buf), (row, col)| {
                    buf.clear();
                    write!(buf, "{}/{}", row, col).unwrap();
                    let path = base.append(buf.as_str());
                    if let Ok(false) = data.contains_key(path.as_bytes()) {
                        // CR estokes: log
                        let _: Result<_> =
                            set_data(data, &mut pending, true, path, Value::Null);
                    }
                    (pending, buf)
                },
            )
            .map(|(u, _)| u)
            .reduce(|| Update::new(), |u0, u1| u0.merge(u1)),
    );
    if lock {
        set_locked(locked, pending, base)?
    }
    Ok(())
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
    let base_levels = Path::levels(&base);
    let mut paths = PATHS.take();
    for r in data.scan_prefix(base.as_bytes()).keys() {
        if let Ok(k) = r {
            if let Ok(path) = str::from_utf8(&*k) {
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
    let up = paths
        .par_drain(..)
        .fold(
            || Update::new(),
            |mut pending, row| {
                for col in &cols {
                    let path = row.append(col);
                    if let Ok(false) = data.contains_key(path.as_bytes()) {
                        let _: Result<_> =
                            set_data(data, &mut pending, true, path, Value::Null);
                    }
                }
                pending
            },
        )
        .reduce(|| Update::new(), |u0, u1| u0.merge(u1));
    pending.merge_from(up);
    Ok(())
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
    let cols: HashSet<sled::IVec> = data
        .scan_prefix(base.as_bytes())
        .keys()
        .filter_map(|k| k.ok())
        .filter_map(|k| {
            str::from_utf8(&k).ok().and_then(|path| {
                if Path::levels(path) == base_levels + 2 {
                    let col = Path::basename(path).unwrap_or("");
                    Some(k.subslice(k.len() - col.len(), col.len()))
                } else {
                    None
                }
            })
        })
        .collect();
    let cols: HashSet<String> = cols
        .into_iter()
        .filter_map(|k| str::from_utf8(&k).ok().map(String::from))
        .collect();
    let up = rows
        .into_par_iter()
        .fold(
            || (Update::new(), String::new()),
            |(mut pending, mut buf), row| {
                for col in &cols {
                    buf.clear();
                    write!(buf, "{}/{}", row, col).unwrap();
                    let path = base.append(&buf);
                    if let Ok(false) = data.contains_key(path.as_bytes()) {
                        let _: Result<_> =
                            set_data(data, &mut pending, true, path, Value::Null);
                    }
                }
                (pending, buf)
            },
        )
        .map(|(u, _)| u)
        .reduce(|| Update::new(), |u0, u1| u0.merge(u1));
    pending.merge_from(up);
    Ok(())
}

fn set_locked(locked: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    let key = path.as_bytes();
    let mut val = BUF.take();
    Value::Null.encode(&mut *val)?;
    locked.insert(key, &**val)?;
    pending.locked.push(path);
    Ok(())
}

fn set_unlocked(locked: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    locked.remove(path.as_bytes())?;
    pending.unlocked.push(path);
    Ok(())
}

fn remove_subtree(data: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    use rayon::prelude::*;
    let mut paths = PATHS.take();
    for res in data.scan_prefix(path.as_ref()).keys() {
        let path = Path::from(ArcStr::from(str::from_utf8(&res?)?));
        paths.push(path);
    }
    pending.merge_from(
        paths
            .par_drain(..)
            .fold(
                || Update::new(),
                |mut pending, path| {
                    let _: Result<_> = remove(data, &mut pending, path);
                    pending
                },
            )
            .reduce(|| Update::new(), |u0, u1| u0.merge(u1)),
    );
    Ok(())
}

fn unlock_subtree(locked: &sled::Tree, pending: &mut Update, path: Path) -> Result<()> {
    let key = path.as_bytes();
    let mut paths = PATHS.take();
    for res in locked.scan_prefix(key).keys() {
        let key = res?;
        let path = Path::from(ArcStr::from(str::from_utf8(&key)?));
        paths.push(path);
    }
    for path in paths.drain(..) {
        set_unlocked(locked, pending, path)?
    }
    Ok(())
}

fn commit_complex(data: &sled::Tree, locked: &sled::Tree, mut txn: Txn) -> Update {
    let mut pending = Update::new();
    for op in txn.0.drain(..) {
        // CR estokes: log this
        let _: Result<_> = match op {
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
            TxnOp::CreateTable { base, rows, cols, lock } => {
                create_table(&data, &locked, &mut pending, base, rows, cols, lock)
            }
            TxnOp::AddTableColumns { base, cols } => {
                add_table_columns(&data, &mut pending, base, cols)
            }
            TxnOp::AddTableRows { base, rows } => {
                add_table_rows(&data, &mut pending, base, rows)
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
            TxnOp::SetUnlocked(path) => unlock_subtree(&locked, &mut pending, path),
            TxnOp::Flush(finished) => {
                // CR estokes: log
                let _: Result<_, _> = data.flush();
                let _: Result<_, _> = locked.flush();
                let _: Result<_, _> = finished.send(());
                Ok(())
            }
        };
    }
    pending
}

fn commit_simple(data: &sled::Tree, locked: &sled::Tree, mut txn: Txn) -> Update {
    use rayon::prelude::*;
    let mut by_path = BYPATH.take();
    for op in txn.0.drain(..) {
        by_path.entry(op.path()).or_insert_with(|| STXNS.take()).push(op);
    }
    by_path
        .par_drain()
        .fold(
            || Update::new(),
            |mut pending, (_, mut ops)| {
                for op in ops.drain(..) {
                    let _: Result<_> = match op {
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
                            unlock_subtree(locked, &mut pending, path)
                        }
                        TxnOp::CreateSheet { .. }
                        | TxnOp::AddSheetColumns { .. }
                        | TxnOp::AddSheetRows { .. }
                        | TxnOp::CreateTable { .. }
                        | TxnOp::AddTableColumns { .. }
                        | TxnOp::AddTableRows { .. }
                        | TxnOp::RemoveSubtree { .. }
                        | TxnOp::Flush(_) => unreachable!(),
                    };
                }
                pending
            },
        )
        .reduce(|| Update::new(), |u0, u1| u0.merge(u1))
}

async fn background_delete_task(data: sled::Tree) {
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
    })
}

async fn commit_txns_task(
    data: sled::Tree,
    locked: sled::Tree,
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
                let (simple, delete) =
                    txn.0.iter().fold((true, false), |(simple, delete), op| match op {
                        TxnOp::CreateSheet { .. }
                        | TxnOp::AddSheetColumns { .. }
                        | TxnOp::AddSheetRows { .. }
                        | TxnOp::CreateTable { .. }
                        | TxnOp::AddTableColumns { .. }
                        | TxnOp::AddTableRows { .. }
                        | TxnOp::Flush(_) => (false, delete),
                        TxnOp::RemoveSubtree { .. } => (false, true),
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
                    task::block_in_place(|| commit_complex(&data, &locked, txn))
                };
                match outgoing.unbounded_send(pending) {
                    Ok(()) => (),
                    Err(_) => break,
                }
                if delete_required && delete_task.is_none() {
                    delete_required = false;
                    delete_task = Some(task::spawn(background_delete_task(data.clone())));
                }
            }
            complete => break,
        }
    }
}

pub(super) struct Db {
    _db: sled::Db,
    data: sled::Tree,
    locked: sled::Tree,
    submit_txn: UnboundedSender<Txn>,
}

impl Db {
    pub(super) fn new(
        cfg: &ContainerConfig,
    ) -> Result<(Self, UnboundedReceiver<Update>)> {
        let db = sled::Config::default()
            .use_compression(cfg.compress)
            .compression_factor(cfg.compress_level.unwrap_or(5) as i32)
            .cache_capacity(cfg.cache_size.unwrap_or(16 * 1024 * 1024))
            .path(&cfg.db)
            .open()?;
        let data = db.open_tree("data")?;
        let locked = db.open_tree("locked")?;
        let (tx_incoming, rx_incoming) = unbounded();
        let (tx_outgoing, rx_outgoing) = unbounded();
        task::spawn(commit_txns_task(
            data.clone(),
            locked.clone(),
            rx_incoming,
            tx_outgoing,
        ));
        Ok((Db { _db: db, data, locked, submit_txn: tx_incoming }, rx_outgoing))
    }

    pub(super) fn commit(&self, txn: Txn) {
        let _: Result<_, _> = self.submit_txn.unbounded_send(txn);
    }

    pub(super) async fn flush_async(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let mut txn = Txn::new();
        txn.0.push(TxnOp::Flush(tx));
        self.commit(txn);
        let _: Result<_, _> = rx.await;
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
