use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{SubId, Dval, UpdatesFlags, Value},
};
use netidx_bscript::{
    expr::ExprId,
    vm::{Ctx, ExecCtx, Node},
};
use netidx_protocols::rpc::server::Proc;
use std::collections::HashMap;
use sled;

struct Lc {
    current: ExprId,
    var_refs: FxHashMap<Chars, FxHashSet<ExprId>>,
    sub_refs: FxHashMap<SubId, FxHashSet<ExprId>>
}

impl Ctx for Lc {
    fn clear(&mut self) {}
    fn durable_subscribe(&mut self, _flags: UpdatesFlags, _path: Path) -> Dval {
        unimplemented!()
    }

    fn set_var(
        &mut self,
        _variables: &mut HashMap<Chars, Value>,
        _name: Chars,
        _value: Value,
    ) {
        unimplemented!()
    }

    fn call_rpc(&mut self, _name: Path, _args: Vec<(Chars, Value)>) {
        unimplemented!()
    }
}

struct Container {
    root: Path,
    tree: sled::Tree,
    formulas: FxHashMap<ExprId, Node<Lc, ()>>,
}
