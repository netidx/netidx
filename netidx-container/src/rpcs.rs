use anyhow::Result;
use arcstr::ArcStr;
use futures::channel::mpsc;
use netidx::{path::Path, publisher::Publisher, subscriber::Value, utils::Batched};
use netidx_protocols::rpc::server::{ArgSpec, Proc, RpcCall, RpcReply};

pub(super) enum RpcRequestKind {
    Delete(Path),
    DeleteSubtree(Path),
    LockSubtree(Path),
    UnlockSubtree(Path),
    SetData {
        path: Path,
        value: Value,
    },
    SetFormula {
        path: Path,
        formula: Option<ArcStr>,
        on_write: Option<ArcStr>,
    },
    CreateSheet {
        path: Path,
        rows: usize,
        columns: usize,
        max_rows: usize,
        max_columns: usize,
        lock: bool,
    },
    AddSheetRows(Path, usize),
    AddSheetCols(Path, usize),
    DelSheetRows(Path, usize),
    DelSheetCols(Path, usize),
    CreateTable {
        path: Path,
        rows: Vec<ArcStr>,
        columns: Vec<ArcStr>,
        lock: bool,
    },
    AddTableRows(Path, Vec<ArcStr>),
    AddTableCols(Path, Vec<ArcStr>),
    DelTableRows(Path, Vec<ArcStr>),
    DelTableCols(Path, Vec<ArcStr>),
    AddRoot(Path),
    DelRoot(Path),
    Packed(Vec<Self>),
}

pub(super) struct RpcRequest {
    pub(super) kind: RpcRequestKind,
    pub(super) reply: RpcReply,
}

pub(super) struct RpcApi {
    _delete_path_rpc: Proc,
    _delete_subtree_rpc: Proc,
    _lock_subtree_rpc: Proc,
    _unlock_subtree_rpc: Proc,
    _set_data_rpc: Proc,
    _set_formula_rpc: Proc,
    _create_sheet_rpc: Proc,
    _add_sheet_rows: Proc,
    _add_sheet_cols: Proc,
    _del_sheet_rows: Proc,
    _del_sheet_cols: Proc,
    _create_table_rpc: Proc,
    _add_table_rows: Proc,
    _add_table_cols: Proc,
    _del_table_rows: Proc,
    _del_table_cols: Proc,
    _add_root: Proc,
    _del_root: Proc,
    pub(super) rx: Batched<mpsc::Receiver<RpcRequest>>,
}

impl RpcApi {
    pub(super) fn new(publisher: &Publisher, base_path: &Path) -> Result<RpcApi> {
        let (tx, rx) = mpsc::channel(10);
        let _delete_path_rpc = start_delete_rpc(&publisher, &base_path, tx.clone())?;
        let _delete_subtree_rpc =
            start_delete_subtree_rpc(&publisher, &base_path, tx.clone())?;
        let _lock_subtree_rpc =
            start_lock_subtree_rpc(&publisher, &base_path, tx.clone())?;
        let _unlock_subtree_rpc =
            start_unlock_subtree_rpc(&publisher, &base_path, tx.clone())?;
        let _set_data_rpc = start_set_data_rpc(&publisher, &base_path, tx.clone())?;
        let _set_formula_rpc = start_set_formula_rpc(&publisher, &base_path, tx.clone())?;
        let _create_sheet_rpc =
            start_create_sheet_rpc(&publisher, &base_path, tx.clone())?;
        let _add_sheet_rows =
            start_add_sheet_rows_rpc(&publisher, &base_path, tx.clone())?;
        let _add_sheet_cols =
            start_add_sheet_cols_rpc(&publisher, &base_path, tx.clone())?;
        let _del_sheet_rows =
            start_del_sheet_rows_rpc(&publisher, &base_path, tx.clone())?;
        let _del_sheet_cols =
            start_del_sheet_cols_rpc(&publisher, &base_path, tx.clone())?;
        let _create_table_rpc =
            start_create_table_rpc(&publisher, &base_path, tx.clone())?;
        let _add_table_rows =
            start_add_table_rows_rpc(&publisher, &base_path, tx.clone())?;
        let _add_table_cols =
            start_add_table_cols_rpc(&publisher, &base_path, tx.clone())?;
        let _del_table_rows =
            start_del_table_rows_rpc(&publisher, &base_path, tx.clone())?;
        let _del_table_cols =
            start_del_table_cols_rpc(&publisher, &base_path, tx.clone())?;
        let _add_root = start_add_root_rpc(&publisher, &base_path, tx.clone())?;
        let _del_root = start_del_root_rpc(&publisher, &base_path, tx.clone())?;
        Ok(RpcApi {
            _delete_path_rpc,
            _delete_subtree_rpc,
            _lock_subtree_rpc,
            _unlock_subtree_rpc,
            _set_data_rpc,
            _set_formula_rpc,
            _create_sheet_rpc,
            _add_sheet_rows,
            _add_sheet_cols,
            _del_sheet_rows,
            _del_sheet_cols,
            _create_table_rpc,
            _add_table_rows,
            _add_table_cols,
            _del_table_rows,
            _del_table_cols,
            _add_root,
            _del_root,
            rx: Batched::new(rx, 1_000_000),
        })
    }
}

fn start_path_arg_rpc(
    publisher: &Publisher,
    base_path: &Path,
    name: &'static str,
    doc: &'static str,
    argdoc: &'static str,
    f: fn(Path) -> RpcRequestKind,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    let map = move |mut c: RpcCall, mut path: Vec<Path>| -> Option<RpcRequest> {
        if path.len() == 0 {
            rpc_err!(c.reply, "expected at least 1 path")
        } else if path.len() == 1 {
            Some(RpcRequest { kind: f(path.pop().unwrap()), reply: c.reply })
        } else {
            let reqs = path.into_iter().map(f).collect();
            Some(RpcRequest { kind: RpcRequestKind::Packed(reqs), reply: c.reply })
        }
    };
    define_rpc!(
        publisher,
        base_path.append(name),
        doc,
        map,
        Some(tx),
        path: Vec<Path> = Vec::<Path>::new(); argdoc
    )
}

pub(super) fn start_delete_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        "delete",
        "delete path(s) from the database",
        "the path(s) to delete",
        RpcRequestKind::Delete,
        tx,
    )
}

pub(super) fn start_delete_subtree_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        "delete-subtree",
        "delete subtree(s) from the database",
        "the subtree(s) to delete",
        RpcRequestKind::DeleteSubtree,
        tx,
    )
}

pub(super) fn start_lock_subtree_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        "lock-subtree",
        "lock subtree(s) so only rpc calls can create/delete values there",
        "the subtree(s) to lock",
        RpcRequestKind::LockSubtree,
        tx,
    )
}

pub(super) fn start_unlock_subtree_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        "unlock-subtree",
        "unlock subtree(s) so the default publisher can create values",
        "the subtree(s) to unlock",
        RpcRequestKind::UnlockSubtree,
        tx,
    )
}

pub(super) fn start_add_root_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        "add-root",
        "add a new root to the container",
        "the root(s) to add",
        RpcRequestKind::AddRoot,
        tx,
    )
}

pub(super) fn start_del_root_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        "remove-root",
        "remove a root and all it's children",
        "the root(s) to remove",
        RpcRequestKind::DelRoot,
        tx,
    )
}

pub(super) fn start_set_data_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(mut c: RpcCall, mut path: Vec<Path>, value: Value) -> Option<RpcRequest> {
        if path.len() == 0 {
            rpc_err!(c.reply, "expected at least 1 path")
        } else if path.len() == 1 {
            Some(RpcRequest {
                reply: c.reply,
                kind: RpcRequestKind::SetData { path: path.pop().unwrap(), value },
            })
        } else {
            let reqs = path
                .into_iter()
                .map(|path| RpcRequestKind::SetData { path, value: value.clone() })
                .collect();
            Some(RpcRequest { reply: c.reply, kind: RpcRequestKind::Packed(reqs) })
        }
    }
    define_rpc!(
        publisher,
        base_path.append("set-data"),
        "make the specified paths data and optionally set the value",
        map,
        Some(tx),
        path: Vec<Path> = Vec::<Path>::new(); "the paths to set",
        value: Value = Value::Null; "The value to set"
    )
}

pub(super) fn start_set_formula_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(
        mut c: RpcCall,
        mut path: Vec<Path>,
        formula: Option<ArcStr>,
        on_write: Option<ArcStr>,
    ) -> Option<RpcRequest> {
        if path.len() == 0 {
            rpc_err!(c.reply, "expected at least 1 path")
        } else if path.len() == 1 {
            let path = path.pop().unwrap();
            let kind = RpcRequestKind::SetFormula { path, formula, on_write };
            Some(RpcRequest { reply: c.reply, kind })
        } else {
            let reqs = path
                .into_iter()
                .map(|path| RpcRequestKind::SetFormula {
                    path,
                    formula: formula.clone(),
                    on_write: on_write.clone(),
                })
                .collect();
            Some(RpcRequest { reply: c.reply, kind: RpcRequestKind::Packed(reqs) })
        }
    }
    define_rpc!(
        publisher,
        base_path.append("set-formula"),
        "make the specified paths calculated and set their formula",
        map,
        Some(tx),
        path: Vec<Path> = Vec::<Path>::new(); "the paths to set",
        formula: Option<ArcStr> = None; "the formula",
        on_write: Option<ArcStr> = None; "the on write formula"
    )
}

pub(super) fn start_create_sheet_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(
        c: RpcCall,
        path: Path,
        rows: usize,
        columns: usize,
        max_rows: Option<usize>,
        max_columns: Option<usize>,
        lock: bool,
    ) -> Option<RpcRequest> {
        let max_rows =
            max_rows.unwrap_or_else(|| 10f32.powf(1. + (rows as f32).log10()) as usize);
        let max_columns = max_columns
            .unwrap_or_else(|| 10f32.powf(1. + (columns as f32).log10()) as usize);
        let kind = RpcRequestKind::CreateSheet {
            path,
            rows,
            columns,
            max_rows,
            max_columns,
            lock,
        };
        Some(RpcRequest { reply: c.reply, kind })
    }
    define_rpc!(
        publisher,
        base_path.append("create-sheet"),
        "create a spreadsheet like sheet",
        map,
        Some(tx),
        path: Path = Value::Null; "where to put the sheet",
        rows: usize = 1; "the number of rows",
        columns: usize = 1; "the number of columns",
        max_rows: Option<usize> = None::<u64>; "the maximum number of rows",
        max_columns: Option<usize> = None::<u64>; "the maximum number of columns",
        lock: bool = true; "lock the sheet subtree"
    )
}

pub(super) fn start_add_sheet_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, rows: usize) -> Option<RpcRequest> {
        let kind = RpcRequestKind::AddSheetRows(path, rows);
        Some(RpcRequest { reply: c.reply, kind })
    }
    define_rpc!(
        publisher,
        base_path.append("add-sheet-rows"),
        "add rows to a previously created sheet",
        map,
        Some(tx),
        path: Path = Value::Null; "the sheet to modify",
        rows: usize = Value::Null; "the number of rows to add"
    )
}

pub(super) fn start_add_sheet_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, columns: usize) -> Option<RpcRequest> {
        let kind = RpcRequestKind::AddSheetCols(path, columns);
        Some(RpcRequest { kind, reply: c.reply })
    }
    define_rpc!(
        publisher,
        base_path.append("add-sheet-columns"),
        "add columns to a previously created sheet",
        map,
        Some(tx),
        path: Path = Value::Null; "the sheet to modify",
        columns: usize = Value::Null; "the number of columns to add"
    )
}

pub(super) fn start_del_sheet_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, rows: usize) -> Option<RpcRequest> {
        let kind = RpcRequestKind::DelSheetRows(path, rows);
        Some(RpcRequest { kind, reply: c.reply })
    }
    define_rpc!(
        publisher,
        base_path.append("delete-sheet-rows"),
        "delete rows in a previously created sheet",
        map,
        Some(tx),
        path: Path = Value::Null; "the sheet to modify",
        rows: usize = Value::Null; "the number of rows to add"
    )
}

pub(super) fn start_del_sheet_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, columns: usize) -> Option<RpcRequest> {
        let kind = RpcRequestKind::DelSheetCols(path, columns);
        Some(RpcRequest { kind, reply: c.reply })
    }
    define_rpc!(
        publisher,
        base_path.append("delete-sheet-columns"),
        "delete columns in a previously created sheet",
        map,
        Some(tx),
        path: Path = Value::Null; "the sheet to modify",
        columns: usize = Value::Null; "number of columns to delete"
    )
}

pub(super) fn start_create_table_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(
        c: RpcCall,
        path: Path,
        rows: Vec<ArcStr>,
        columns: Vec<ArcStr>,
        lock: bool,
    ) -> Option<RpcRequest> {
        let kind = RpcRequestKind::CreateTable { path, rows, columns, lock };
        Some(RpcRequest { kind, reply: c.reply })
    }
    define_rpc!(
        publisher,
        base_path.append("create-table"),
        "create a database like table",
        map,
        Some(tx),
        path: Path = Value::Null; "where to put the table",
        rows: Vec<ArcStr> = Value::Null; "the row names",
        columns: Vec<ArcStr> = Value::Null; "the column names",
        lock: bool = true; "lock the table subtree"
    )
}

pub(super) fn start_add_table_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, rows: Vec<ArcStr>) -> Option<RpcRequest> {
        let kind = RpcRequestKind::AddTableRows(path, rows);
        Some(RpcRequest { kind, reply: c.reply })
    }
    define_rpc!(
        publisher,
        base_path.append("add-table-rows"),
        "add rows to a table",
        map,
        Some(tx),
        path: Path = Value::Null; "the table to modify",
        rows: Vec<ArcStr> = Value::Null; "the rows to add"
    )
}

pub(super) fn start_add_table_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, columns: Vec<ArcStr>) -> Option<RpcRequest> {
        let kind = RpcRequestKind::AddTableCols(path, columns);
        Some(RpcRequest { reply: c.reply, kind })
    }
    define_rpc!(
        publisher,
        base_path.append("add-table-columns"),
        "add columns to a table",
        map,
        Some(tx),
        path: Path = Value::Null; "the table to modify",
        columns: Vec<ArcStr> = Value::Null; "the columns to add"
    )
}

pub(super) fn start_del_table_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, rows: Vec<ArcStr>) -> Option<RpcRequest> {
        let kind = RpcRequestKind::DelTableRows(path, rows);
        Some(RpcRequest { reply: c.reply, kind })
    }
    define_rpc!(
        publisher,
        base_path.append("delete-table-rows"),
        "delete rows from a table",
        map,
        Some(tx),
        path: Path = Value::Null; "the table to modify",
        rows: Vec<ArcStr> = Value::Null; "the rows to delete"
    )
}

pub(super) fn start_del_table_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    fn map(c: RpcCall, path: Path, columns: Vec<ArcStr>) -> Option<RpcRequest> {
        let kind = RpcRequestKind::DelTableCols(path, columns);
        Some(RpcRequest { reply: c.reply, kind })
    }
    define_rpc!(
        publisher,
        base_path.append("delete-table-columns"),
        "delete columns from a table",
        map,
        Some(tx),
        path: Path = Value::Null; "the table to modify",
        columns: Vec<ArcStr> = Value::Null; "the columns to delete"
    )
}
