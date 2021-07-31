use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use netidx::{
    chars::Chars, path::Path, pool::Pooled, publisher::Publisher, subscriber::Value,
};
use netidx_protocols::rpc::server::Proc;
use std::{collections::HashMap, sync::Arc};

pub(super) enum RpcRequestKind {
    Delete(Path),
    DeleteSubtree(Path),
    LockSubtree(Path),
    UnlockSubtree(Path),
    SetData { path: Path, value: Value },
    SetFormula { path: Path, formula: Option<Chars>, on_write: Option<Chars> },
    CreateSheet { path: Path, rows: usize, columns: usize, lock: bool },
    CreateTable { path: Path, rows: Vec<Chars>, columns: Vec<Chars>, lock: bool },
}

pub(super) struct RpcRequest {
    pub(super) kind: RpcRequestKind,
    pub(super) reply: oneshot::Sender<Value>,
}

fn err(s: &'static str) -> Value {
    Value::Error(Chars::from(s))
}

pub(super) fn start_path_arg_rpc<
    F: Fn(Path) -> RpcRequestKind + Send + Sync + 'static,
>(
    publisher: &Publisher,
    base_path: &Path,
    name: &'static str,
    doc: &'static str,
    argdoc: &'static str,
    f: F,
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
                                Value::String(path) => Path::from(Arc::from(&*path)),
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

fn start_delete_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        ".api/delete",
        "delete path(s) from the database",
        "the path(s) to delete",
        RpcRequestKind::Delete,
        tx,
    )
}

fn start_delete_subtree_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        ".api/delete-subtree",
        "delete subtree(s) from the database",
        "the subtree(s) to delete",
        RpcRequestKind::DeleteSubtree,
        tx,
    )
}

fn start_lock_subtree_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        ".api/lock-subtree",
        "lock subtree(s) so only rpc calls can create/delete values there",
        "the subtree(s) to lock",
        RpcRequestKind::LockSubtree,
        tx,
    )
}

fn start_unlock_subtree_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    start_path_arg_rpc(
        publisher,
        base_path,
        ".api/unlock-subtree",
        "unlock subtree(s) so the default publisher can create values",
        "the subtree(s) to unlock",
        RpcRequestKind::UnlockSubtree,
        tx,
    )
}

fn start_set_data_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append(".api/set-data"),
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
                        let value = value.drain(..);
                        for path in paths.drain(..) {
                            let path = match path {
                                Value::String(path) => Path::from(Arc::from(&*path)),
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

fn start_set_formula_rpc(
    publisher: &Publisher,
    base_path: &Path,
    tx: mpsc::Sender<RpcRequest>,
) -> Result<Proc> {
    Ok(Proc::new(
        publisher,
        base_path.append(".api/set-formula"),
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
                                Value::String(path) => Path::from(Arc::from(&*path)),
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
        base_path.append(".api/create-sheet"),
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
                                Value::String(path) => Path::from(Arc::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let (reply, reply_rx) = oneshot::channel();
                            let kind = RpcRequestKind::CreateSheet {
                                path,
                                rows: rows as usize,
                                columns: columns as usize,
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

fn collect_chars_vec(
    args: &mut Pooled<HashMap<Arc<str>, Pooled<Vec<Value>>>>,
    name: &str,
) -> Result<Vec<Chars>> {
    match args.remove(name) {
        None => bail!("required argument rows is missing"),
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
        base_path.append(".api/create-table"),
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
                        let columns = match collect_chars_vec(&mut args, "columns") {
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
                                Value::String(path) => Path::from(Arc::from(&*path)),
                                _ => {
                                    return err("invalid argument type, expected string")
                                }
                            };
                            let (reply, reply_rx) = oneshot::channel();
                            let kind =
                                RpcRequestKind::CreateTable { path, rows, columns, lock };
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
