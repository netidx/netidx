use anyhow::{bail, Result};
use arcstr::ArcStr;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use netidx::{
    chars::Chars, path::Path, pool::Pooled, publisher::Publisher, subscriber::Value,
    utils::Batched,
};
use netidx_protocols::rpc::server::{ArgSpec, Proc, RpcCall, RpcReply};
use std::{collections::HashMap, sync::Arc};

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
        formula: Option<Chars>,
        on_write: Option<Chars>,
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
        rows: Vec<Chars>,
        columns: Vec<Chars>,
        lock: bool,
    },
    AddTableRows(Path, Vec<Chars>),
    AddTableCols(Path, Vec<Chars>),
    DelTableRows(Path, Vec<Chars>),
    DelTableCols(Path, Vec<Chars>),
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

macro_rules! get_arg_opt {
    ($reply:expr, $typ:ty, $args:expr, $arg:expr) => {
        match $args.remove($arg).map(|v| v.get_as::<$typ>()) {
            Some(Some(c)) => Some(c),
            Some(None) => {
                let msg = format!("invalid {} expected {}", $arg, stringify!($typ));
                $reply.send(Value::Error(Chars::from(msg)));
                return None;
            }
            None => None,
        }
    };
}

macro_rules! get_arg {
    ($reply:expr, $typ:ty, $args:expr, $arg:expr, $default:expr) => {
        match $args.remove($arg).map(|v| v.cast_to::<$typ>()) {
            Some(Ok(c)) => c,
            Some(Err(_)) => {
                let msg = format!("invalid {} expected {}", $arg, stringify!($typ));
                $reply.send(Value::Error(Chars::from(msg)));
                return None;
            }
            None => $default,
        }
    };
}

macro_rules! get_path {
    ($reply:expr, $path:expr) => {
        match $path {
            Value::String(path) => Path::from(ArcStr::from(&**path)),
            _ => {
                $reply.send(err("invalid argument type, expected string"));
                return None;
            }
        }
    };
}

fn err(s: &'static str) -> Value {
    Value::Error(Chars::from(s))
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
    let map = |c: RpcCall| match c.args.remove("path") {
        Some(Value::String(path)) => {
            let path = Path::from(ArcStr::from(&*path));
            Some(RpcRequest { kind: f(path), reply: c.reply })
        }
        Some(Value::Array(paths)) => {
            let mut reqs = Vec::new();
            for path in paths.iter() {
                let path = get_path!(c.reply, path);
                reqs.push(f(path));
            }
            Some(RpcRequest { kind: RpcRequestKind::Packed(reqs), reply: c.reply })
        }
        Some(_) | None => {
            c.reply.send(err("invalid argument, expected path"));
            None
        }
    };
    Ok(Proc::new(
        publisher,
        base_path.append(name),
        Value::from(doc),
        [ArgSpec {
            name: ArcStr::from("path"),
            default_value: Value::Null,
            doc: Value::from(argdoc),
        }],
        map,
        tx,
    )?)
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
    let map = |c: RpcCall| match (c.args.remove("path"), c.args.remove("value")) {
        (Some(Value::String(path)), Some(value)) => {
            let path = Path::from(ArcStr::from(&*path));
            let kind = RpcRequestKind::SetData { path, value };
            Some(RpcRequest { kind, reply: c.reply })
        }
        (Some(Value::Array(paths)), Some(value)) => {
            let mut reqs = Vec::new();
            for path in paths.iter() {
                let path = get_path!(c.reply, path);
                reqs.push(RpcRequestKind::SetData { path, value: value.clone() })
            }
            Some(RpcRequest { kind: RpcRequestKind::Packed(reqs), reply: c.reply })
        }
        (Some(_) | None, Some(_) | None) => {
            err("invalid argument, expected path");
            None
        }
    };
    Ok(Proc::new(
        publisher,
        base_path.append("set-data"),
        Value::from("make the specified path(s) as data and optionally set the value"),
        [
            ArgSpec {
                name: ArcStr::from("path"),
                default_value: Value::Null,
                doc: Value::from("the path(s) to set"),
            },
            ArgSpec {
                name: ArcStr::from("value"),
                default_value: Value::Null,
                doc: Value::from("the value(s)"),
            },
        ],
        map,
        tx,
    )?)
}

pub(super) fn start_set_formula_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    let map = |c: RpcCall| {
        let formula = get_arg_opt!(c.reply, Chars, c.args, "formula");
        let on_write = get_arg_opt!(c.reply, Chars, c.args, "on-write");
        match c.args.remove("path") {
            Some(Value::String(path)) => {
                let path = Path::from(ArcStr::from(&*path));
                Some(RpcRequest {
                    kind: RpcRequestKind::SetFormula { path, formula, on_write },
                    reply: c.reply,
                })
            }
            Some(Value::Array(paths)) => {
                let formula = formula.clone();
                let on_write = on_write.clone();
                let mut reqs = Vec::new();
                for path in paths.iter() {
                    let path = get_path!(c.reply, path);
                    reqs.push(RpcRequestKind::SetFormula { path, formula, on_write });
                }
                Some(RpcRequest { kind: RpcRequestKind::Packed(reqs), reply: c.reply })
            }
            Some(_) | None => {
                c.reply.send(err("invalid argument, expected path"));
                None
            }
        }
    };
    Ok(Proc::new(
        publisher,
        base_path.append("set-formula"),
        Value::from("make the specified path calculated and set it's formula"),
        [
            ArgSpec {
                name: ArcStr::from("path"),
                default_value: Value::Null,
                doc: Value::from("the path(s) to set"),
            },
            ArgSpec {
                name: ArcStr::from("formula"),
                default_value: Value::Null,
                doc: Value::from("the formula"),
            },
            ArgSpec {
                name: ArcStr::from("on-write"),
                default_value: Value::Null,
                doc: Value::from("the on write formula"),
            },
        ],
        map,
        tx,
    )?)
}

pub(super) fn start_create_sheet_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    let map = |c: RpcCall| {
        let rows = get_arg!(c.reply, u64, c.args, "rows", 1);
        let max_rows = 10f32.powf(1. + (rows as f32).log10()) as u64;
        let max_rows = get_arg!(c.reply, u64, c.args, "max-rows", max_rows);
        let columns = get_arg!(c.reply, u64, c.args, "columns", 1);
        let max_columns = 10f32.powf(1. + (columns as f32).log10()) as u64;
        let max_columns = get_arg!(c.reply, u64, c.args, "max-columns", max_columns);
        let lock = get_arg!(c.reply, bool, c.args, "lock", true);
        match c.args.remove("path") {
            Some(Value::String(path)) => {
                let path = Path::from(ArcStr::from(&*path));
                Some(RpcRequest {
                    reply: c.reply,
                    kind: RpcRequestKind::CreateSheet {
                        path,
                        rows: rows as usize,
                        columns: columns as usize,
                        max_rows: max_rows as usize,
                        max_columns: max_columns as usize,
                        lock,
                    },
                })
            }
            Some(_) | None => {
                c.reply.send(err("invalid argument, expected path"));
                None
            }
        }
    };
    let args = [
        ArgSpec {
            name: ArcStr::from("path"),
            default_value: Value::Null,
            doc: Value::from("where to put the sheet"),
        },
        ArgSpec {
            name: ArcStr::from("rows"),
            default_value: Value::U64(1),
            doc: Value::from("the number of rows"),
        },
        ArgSpec {
            name: ArcStr::from("columns"),
            default_value: Value::U64(1),
            doc: Value::from("the number of columns"),
        },
        ArgSpec {
            name: ArcStr::from("max-rows"),
            default_value: Value::Null,
            doc: Value::from("the max rows"),
        },
        ArgSpec {
            name: ArcStr::from("max-columns"),
            default_value: Value::Null,
            doc: Value::from("the max columns"),
        },
        ArgSpec {
            name: ArcStr::from("lock"),
            default_value: Value::True,
            doc: Value::from("lock the sheet subtree"),
        },
    ];
    Ok(Proc::new(
        publisher,
        base_path.append("create-sheet"),
        Value::from("create a spreadsheet like sheet"),
        args,
        map,
        tx,
    )?)
}

pub(super) fn start_add_sheet_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    let map = |c: RpcCall| match c.args.remove("path") {
        Some(Value::String(path)) => {
            let path = Path::from(ArcStr::from(&*path));
            let rows = get_arg!(c.reply, u64, c.args, "rows", 1);
            Some(RpcRequest {
                reply: c.reply,
                kind: RpcRequestKind::AddSheetRows(path, rows as usize),
            })
        }
        Some(_) | None => {
            c.reply.send(err("invalid argument, expected path"));
            None
        }
    };
    let args = [
        ArgSpec {
            name: ArcStr::from("path"),
            default_value: Value::Null,
            doc: Value::from("the sheets"),
        },
        ArgSpec {
            name: ArcStr::from("rows"),
            default_value: Value::U64(1),
            doc: Value::from("how many rows to add"),
        },
    ];
    Ok(Proc::new(
        publisher,
        base_path.append("add-sheet-rows"),
        Value::from("add rows to a previously created sheet"),
        args,
        map,
        tx,
    )?)
}

pub(super) fn start_add_sheet_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    let map = |c: RpcCall| match c.args.remove("path") {
        Some(Value::String(path)) => {
            let path = Path::from(ArcStr::from(&*path));
            let columns = get_arg!(c.reply, u64, c.args, "columns", 1);
            let kind = RpcRequestKind::AddSheetCols(path, columns as usize);
            Some(RpcRequest { kind, reply: c.reply })
        }
        Some(_) | None => {
            c.reply.send(err("invalid argument, expected path"));
            None
        }
    };
    let args = [
        ArgSpec {
            name: ArcStr::from("path"),
            default_value: Value::Null,
            doc: Value::from("the sheets(s)"),
        },
        ArgSpec {
            name: ArcStr::from("columns"),
            default_value: Value::U64(1),
            doc: Value::from("how many cols to add"),
        },
    ];
    Ok(Proc::new(
        publisher,
        base_path.append("add-sheet-columns"),
        Value::from("add columns to a previously created sheet"),
        args,
        map,
        tx,
    )?)
}

pub(super) fn start_del_sheet_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("delete-sheet-rows"),
        Value::from("delete rows in a previously created sheet"),
        vec![
            (Arc::from("path"), (Value::Null, Value::from("the sheets(s)"))),
            (Arc::from("rows"), (Value::U64(1), Value::from("rows to delete"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let rows = get_arg!(u64, args, "rows", 1);
                        for path in paths.drain(..) {
                            let path = get_path!(path);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::DelSheetRows(path, rows as usize);
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind, reply }).await;
                            match reply_rx.await {
                                Err(_) => return err("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?)
}

pub(super) fn start_del_sheet_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("delete-sheet-columns"),
        Value::from("delete columns in a previously created sheet"),
        vec![
            (Arc::from("path"), (Value::Null, Value::from("the sheets(s)"))),
            (Arc::from("columns"), (Value::U64(1), Value::from("cols to delete"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let columns = get_arg!(u64, args, "columns", 1);
                        for path in paths.drain(..) {
                            let path = get_path!(path);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind =
                                RpcRequestKind::DelSheetCols(path, columns as usize);
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind, reply }).await;
                            match reply_rx.await {
                                Err(_) => return err("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?)
}

fn collect_chars_vec(
    args: &mut Pooled<HashMap<Arc<str>, Pooled<Vec<Value>>>>,
    name: &str,
) -> Result<Vec<Chars>> {
    match args.remove(name) {
        None => bail!("required argument {} is missing", name),
        Some(mut rows) => {
            let mut res = Vec::new();
            for v in rows.drain(..) {
                match v.cast_to::<Chars>() {
                    Err(_) => bail!("invalid rows type, expected string"),
                    Ok(row) => res.push(row),
                }
            }
            Ok(res)
        }
    }
}

pub(super) fn start_create_table_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("create-table"),
        Value::from("create a database like table"),
        vec![
            (Arc::from("path"), (Value::Null, Value::from("the tables(s)"))),
            (Arc::from("row"), (Value::Null, Value::from("the row names"))),
            (Arc::from("column"), (Value::Null, Value::from("the column names"))),
            (Arc::from("lock"), (Value::True, Value::from("lock the table subtree"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let rows = match collect_chars_vec(&mut args, "row") {
                            Err(e) => return Value::from(format!("{}", e)),
                            Ok(rows) => rows,
                        };
                        let columns = match collect_chars_vec(&mut args, "column") {
                            Err(e) => return Value::from(format!("{}", e)),
                            Ok(columns) => columns,
                        };
                        let lock = get_arg!(bool, args, "lock", true);
                        for path in paths.drain(..) {
                            let path = get_path!(path);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::CreateTable {
                                path,
                                rows: rows.clone(),
                                columns: columns.clone(),
                                lock,
                            };
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind, reply }).await;
                            match reply_rx.await {
                                Err(_) => return err("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?)
}

pub(super) fn start_add_table_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("add-table-rows"),
        Value::from("add rows to a table"),
        vec![
            (Arc::from("path"), (Value::Null, Value::from("the tables(s)"))),
            (Arc::from("row"), (Value::Null, Value::from("the row(s)"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let rows = match collect_chars_vec(&mut args, "row") {
                            Err(e) => return Value::from(format!("{}", e)),
                            Ok(rows) => rows,
                        };
                        for path in paths.drain(..) {
                            let path = get_path!(path);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::AddTableRows(path, rows.clone());
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind, reply }).await;
                            match reply_rx.await {
                                Err(_) => return err("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?)
}

pub(super) fn start_add_table_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("add-table-columns"),
        Value::from("add columns to a table"),
        vec![
            (Arc::from("path"), (Value::Null, Value::from("the tables(s)"))),
            (Arc::from("column"), (Value::Null, Value::from("the column(s)"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let cols = match collect_chars_vec(&mut args, "columns") {
                            Err(e) => return Value::from(format!("{}", e)),
                            Ok(cols) => cols,
                        };
                        for path in paths.drain(..) {
                            let path = get_path!(path);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::AddTableCols(path, cols.clone());
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind, reply }).await;
                            match reply_rx.await {
                                Err(_) => return err("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?)
}

pub(super) fn start_del_table_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("delete-table-rows"),
        Value::from("delete rows from a table"),
        vec![
            (Arc::from("path"), (Value::Null, Value::from("the tables(s)"))),
            (Arc::from("row"), (Value::Null, Value::from("the row names"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let rows = match collect_chars_vec(&mut args, "row") {
                            Err(e) => return Value::from(format!("{}", e)),
                            Ok(rows) => rows,
                        };
                        for path in paths.drain(..) {
                            let path = get_path!(path);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::DelTableRows(path, rows.clone());
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind, reply }).await;
                            match reply_rx.await {
                                Err(_) => return err("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?)
}

pub(super) fn start_del_table_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("delete-table-columns"),
        Value::from("delete columns from a table"),
        vec![
            (Arc::from("path"), (Value::Null, Value::from("the tables(s)"))),
            (Arc::from("columns"), (Value::Null, Value::from("the column names"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let cols = match collect_chars_vec(&mut args, "columns") {
                            Err(e) => return Value::from(format!("{}", e)),
                            Ok(cols) => cols,
                        };
                        for path in paths.drain(..) {
                            let path = get_path!(path);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::DelTableCols(path, cols.clone());
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind, reply }).await;
                            match reply_rx.await {
                                Err(_) => return err("internal error"),
                                Ok(v) => match v {
                                    Value::Ok => (),
                                    v => return v,
                                },
                            }
                        }
                        Value::Ok
                    }
                }
            })
        }),
    )?)
}
