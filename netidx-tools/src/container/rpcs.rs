use anyhow::Result;
use arcstr::ArcStr;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use netidx::{
    chars::Chars, path::Path, pool::Pooled, publisher::Publisher, subscriber::Value,
    utils::Batched,
};
use netidx_protocols::rpc::server::Proc;
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
    CreateTable {
        path: Path,
        rows: Vec<Chars>,
        columns: Vec<Chars>,
        lock: bool,
    },
    AddTableRows(Path, Vec<Chars>),
    AddTableCols(Path, Vec<Chars>),
}

pub(super) struct RpcRequest {
    pub(super) kind: RpcRequestKind,
    pub(super) reply: oneshot::Sender<Value>,
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
    _create_table_rpc: Proc,
    _add_table_rows: Proc,
    _add_table_cols: Proc,
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
        let _create_table_rpc =
            start_create_table_rpc(&publisher, &base_path, tx.clone())?;
        let _add_table_rows =
            start_add_table_rows_rpc(&publisher, &base_path, tx.clone())?;
        let _add_table_cols =
            start_add_table_cols_rpc(&publisher, &base_path, tx.clone())?;
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
            _create_table_rpc,
            _add_table_rows,
            _add_table_cols,
            rx: Batched::new(rx, 1_000_000),
        })
    }
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
    Ok(Proc::new(
        publisher,
        base_path.append(name),
        Value::from(doc),
        vec![(Arc::from("path"), (Value::Null, Value::from(argdoc)))]
            .into_iter()
            .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let (reply, reply_rx) = oneshot::channel();
                            let _: Result<_, _> =
                                tx.send(RpcRequest { kind: f(path), reply }).await;
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

pub(super) fn start_set_data_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("set-data"),
        Value::from("make the specified path(s) as data and optionally set the value"),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to make calculated")),
            ),
            (Arc::from("value"), (Value::Null, Value::from("the value(s)"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let mut value = args
                            .remove("value")
                            .unwrap_or_else(|| Pooled::orphan(vec![]));
                        let mut value = value.drain(..);
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let value = value.next().unwrap_or(Value::Null);
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::SetData { path, value };
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

pub(super) fn start_set_formula_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("set-formula"),
        Value::from(
            "make the specified path calculated and set it's formula and/or its on-write",
        ),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to make calculated")),
            ),
            (Arc::from("formula"), (Value::Null, Value::from("the formula"))),
            (Arc::from("on-write"), (Value::Null, Value::from("the on write formula"))),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let formula = match args
                            .remove("formula")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.get_as::<Chars>())
                        {
                            Some(Some(c)) => Some(c),
                            Some(None) => {
                                return err("invalid formula type, expected string")
                            }
                            None => None,
                        };
                        let on_write = match args
                            .remove("on-write")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.get_as::<Chars>())
                        {
                            Some(Some(c)) => Some(c),
                            Some(None) => {
                                return err("invalid on-write type, expected string")
                            }
                            None => None,
                        };
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::SetFormula {
                                path,
                                formula: formula.clone(),
                                on_write: on_write.clone(),
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

pub(super) fn start_create_sheet_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("create-sheet"),
        Value::from(
            "create a spreadsheet like sheet with the specified number of rows and columns",
        ),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to create the sheet(s) under")),
            ),
            (Arc::from("rows"), (Value::U64(1), Value::from("the number of rows"))),
            (Arc::from("columns"), (Value::U64(1), Value::from("the number of columns"))),
            (Arc::from("max-rows"), (Value::Null, Value::from("the maximum number of rows"))),
            (Arc::from("max-columns"), (Value::Null, Value::from("the maximum number of columns"))),
            (Arc::from("lock"), (Value::True, Value::from("lock the sheet subtree (see lock)")))
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let rows = match args
                            .remove("rows")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<u64>().ok())
                        {
                            Some(Some(rows)) => rows,
                            Some(None) => return err("invalid rows type, expected number"),
                            None => 1
                        };
                        let max_rows = match args
                            .remove("max-rows")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<u64>().ok())
                        {
                            Some(Some(rows)) => rows,
                            Some(None) => return err("invalid rows type, expected number"),
                            None => 10f32.powf(1. + (rows as f32).log10()) as u64,
                        };
                        let columns = match args
                            .remove("columns")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<u64>().ok())
                        {
                            Some(Some(columns)) => columns,
                            Some(None) => {
                                return err("invalid columns type, expected number")
                            }
                            None => 1,
                        };
                        let max_columns = match args
                            .remove("max-columns")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<u64>().ok())
                        {
                            Some(Some(columns)) => columns,
                            Some(None) => {
                                return err("invalid columns type, expected number")
                            }
                            None => 10f32.powf(1. + (columns as f32).log10()) as u64,
                        };
                        let lock = match args
                            .remove("lock")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<bool>().ok())
                        {
                            Some(Some(lock)) => lock,
                            Some(None) => return err("invalid lock type, expected bool"),
                            None => true
                        };
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::CreateSheet {
                                path,
                                rows: rows as usize,
                                columns: columns as usize,
                                max_rows: max_rows as usize,
                                max_columns: max_columns as usize,
                                lock
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

pub(super) fn start_add_sheet_rows_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("add-sheet-rows"),
        Value::from("add rows to a previously created spreadsheet like sheet"),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to the sheet(s)")),
            ),
            (
                Arc::from("rows"),
                (Value::U64(1), Value::from("the number of rows to add")),
            ),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let rows = match args
                            .remove("rows")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<u64>().ok())
                        {
                            Some(Some(rows)) => rows,
                            Some(None) => {
                                return err("invalid rows type, expected number")
                            }
                            None => 1,
                        };
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::AddSheetRows(path, rows as usize);
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

pub(super) fn start_add_sheet_cols_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append("add-sheet-columns"),
        Value::from("add columns to a previously created spreadsheet like sheet"),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to the sheet(s)")),
            ),
            (
                Arc::from("columns"),
                (Value::U64(1), Value::from("the number of columns to add")),
            ),
        ]
        .into_iter()
        .collect(),
        Arc::new(move |_addr, mut args| {
            let mut tx = tx.clone();
            Box::pin(async move {
                match args.remove("path") {
                    None => err("invalid argument, expected path"),
                    Some(mut paths) => {
                        let columns = match args
                            .remove("columns")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<u64>().ok())
                        {
                            Some(Some(cols)) => cols,
                            Some(None) => {
                                return err("invalid columns type, expected number")
                            }
                            None => 1,
                        };
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let (reply, reply_rx) = oneshot::channel();
                            let kind =
                                RpcRequestKind::AddSheetCols(path, columns as usize);
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
        Value::from("create a database like table with named rows and columns"),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to create the tables(s) under")),
            ),
            (
                Arc::from("row"),
                (Value::Null, Value::from("the row names (specify multiple)")),
            ),
            (
                Arc::from("column"),
                (Value::Null, Value::from("the column names (specify multiple)")),
            ),
            (
                Arc::from("lock"),
                (Value::True, Value::from("lock the table subtree (see lock)")),
            ),
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
                        let lock = match args
                            .remove("lock")
                            .and_then(|mut v| v.pop())
                            .map(|v| v.cast_to::<bool>().ok())
                        {
                            Some(Some(lock)) => lock,
                            Some(None) => return err("invalid lock type, expected bool"),
                            None => true,
                        };
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
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
        Value::from("add rows to a database like table"),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to the tables(s)")),
            ),
            (
                Arc::from("row"),
                (Value::Null, Value::from("the row names (specify multiple)")),
            ),
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
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
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
        Value::from("add columns to a database like table"),
        vec![
            (
                Arc::from("path"),
                (Value::Null, Value::from("the path(s) to the tables(s)")),
            ),
            (
                Arc::from("columns"),
                (Value::Null, Value::from("the column names (specify multiple)")),
            ),
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
                            let path = match path {
                                Value::String(path) => Path::from(ArcStr::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
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
