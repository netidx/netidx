// Widget tree data model, AST classification, source reconstruction, and sync.

use arcstr::ArcStr;
use glib::prelude::*;
use graphix_compiler::{
    env::Env,
    expr::{
        self, ApplyExpr, Expr, ExprKind, ModPath, Origin,
        Source as GxSource,
    },
    typ::Type,
};
use gtk::prelude::*;
use serde::{Serialize, Deserialize};
use std::{
    cell::{Ref, RefCell},
    rc::Rc,
};

use super::expr_util::*;

netidx_core::atomic_id!(WidgetNodeId);

pub(super) static WIDGET_KINDS: &[&str] = &[
    "label", "button", "entry", "switch", "toggle_button",
    "check_button", "combo_box", "scale", "progress_bar",
    "image", "link_button", "search_entry",
    "vbox", "hbox", "grid", "frame", "paned", "notebook",
    "table", "chart", "key_handler",
];

// ---- Tree node data ----

/// How children map back to labeled args during source reconstruction
#[derive(Clone, Debug)]
pub(super) enum ChildSlot {
    /// Children came from an Array-typed arg: #label: &[child, child, ...]
    Array(ArcStr),
    /// Child came from a single Widget-typed arg: #label: &child
    Single(ArcStr),
}

/// Data stored per TreeStore row via BoxedAnyObject.
#[derive(Clone, Debug)]
pub(super) struct TreeNodeData {
    pub(super) id: WidgetNodeId,
    pub(super) kind: ArcStr,
    /// Non-child labeled args (property args for the panel)
    pub(super) args: Vec<(ArcStr, Expr)>,
    /// Describes how tree children map to source args.
    pub(super) child_slots: Vec<ChildSlot>,
}

#[derive(Clone)]
pub(super) struct ArgInfo {
    pub(super) label: ArcStr,
    pub(super) expr: Expr,
    pub(super) line: i32,
    pub(super) col: i32,
}

// ---- AST helpers ----

fn widget_name(expr: &Expr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Ref { name } => {
            let s = name.0.as_ref();
            let rest = s.strip_prefix("/browser/")
                .or_else(|| s.strip_prefix("browser/"))?;
            if WIDGET_KINDS.contains(&rest) { Some(rest) } else { None }
        }
        _ => None,
    }
}

/// Try to interpret an expr as a widget call. Returns (kind, property_args, child_slots, child_exprs).
pub(super) fn classify_widget_expr(expr: &Expr)
    -> Option<(ArcStr, Vec<(ArcStr, Expr)>, Vec<ChildSlot>, Vec<Expr>)>
{
    let expr = match &expr.kind {
        ExprKind::ByRef(inner) => inner.as_ref(),
        _ => expr,
    };
    match &expr.kind {
        ExprKind::Apply(ApplyExpr { function, args }) => {
            let kind = widget_name(function)?;
            let mut prop_args = Vec::new();
            let mut child_slots = Vec::new();
            let mut child_exprs: Vec<Expr> = Vec::new();
            for (label, arg_expr) in args.iter() {
                if let Some(label) = label {
                    let inner = match &arg_expr.kind {
                        ExprKind::ByRef(i) => i.as_ref(),
                        _ => arg_expr,
                    };
                    match &inner.kind {
                        ExprKind::Array { args: items } => {
                            let widgets: Vec<_> = items.iter()
                                .filter_map(|e| classify_widget_expr(e).map(|_| e.clone()))
                                .collect();
                            if !items.is_empty() && widgets.len() == items.len() {
                                child_slots.push(ChildSlot::Array(label.clone()));
                                child_exprs.extend(widgets);
                            } else {
                                prop_args.push((label.clone(), arg_expr.clone()));
                            }
                        }
                        _ => {
                            if classify_widget_expr(inner).is_some() {
                                child_slots.push(ChildSlot::Single(label.clone()));
                                child_exprs.push(arg_expr.clone());
                            } else {
                                prop_args.push((label.clone(), arg_expr.clone()));
                            }
                        }
                    }
                } else {
                    match &arg_expr.kind {
                        ExprKind::Array { args: items } => {
                            let widgets: Vec<_> = items.iter()
                                .filter_map(|e| classify_widget_expr(e).map(|_| e.clone()))
                                .collect();
                            if !items.is_empty() && widgets.len() == items.len() {
                                child_slots.push(ChildSlot::Array(ArcStr::from("children")));
                                child_exprs.extend(widgets);
                            } else {
                                prop_args.push((ArcStr::from(""), arg_expr.clone()));
                            }
                        }
                        _ => {
                            if classify_widget_expr(arg_expr).is_some() {
                                child_slots.push(ChildSlot::Single(ArcStr::from("child")));
                                child_exprs.push(arg_expr.clone());
                            } else {
                                prop_args.push((ArcStr::from(""), arg_expr.clone()));
                            }
                        }
                    }
                }
            }
            Some((ArcStr::from(kind), prop_args, child_slots, child_exprs))
        }
        ExprKind::ByRef(inner) => classify_widget_expr(inner),
        _ => None,
    }
}

// ---- TreeStore helpers ----

pub(super) fn get_node_data(store: &gtk::TreeStore, iter: &gtk::TreeIter) -> glib::BoxedAnyObject {
    store.value(iter, 1).get::<glib::BoxedAnyObject>().unwrap()
}

pub(super) fn populate_from_expr(
    store: &gtk::TreeStore,
    parent: Option<&gtk::TreeIter>,
    expr: &Expr,
) {
    if let Some((kind, prop_args, child_slots, child_exprs)) = classify_widget_expr(expr) {
        let data = TreeNodeData {
            id: WidgetNodeId::new(),
            kind: kind.clone(),
            args: prop_args,
            child_slots,
        };
        let iter = store.append(parent);
        store.set_value(&iter, 0, &kind.as_str().to_value());
        store.set_value(&iter, 1, &glib::BoxedAnyObject::new(data).to_value());
        for child_expr in &child_exprs {
            populate_from_expr(store, Some(&iter), child_expr);
        }
    }
}

pub(super) fn parse_and_populate(
    store: &gtk::TreeStore,
    source: &str,
    user_code: &Rc<RefCell<Vec<Expr>>>,
) {
    store.clear();
    let ori = Origin {
        parent: None,
        source: GxSource::Unspecified,
        text: ArcStr::from(source),
    };
    if let Ok(exprs) = expr::parser::parse(ori) {
        let mut split = exprs.len();
        for i in (0..exprs.len()).rev() {
            if classify_widget_expr(&exprs[i]).is_some() {
                split = i;
            } else {
                break;
            }
        }
        *user_code.borrow_mut() = exprs[..split].to_vec();
        for expr in &exprs[split..] {
            populate_from_expr(store, None, expr);
        }
    }
}

// ---- Source reconstruction ----

pub(super) fn tree_to_source(store: &gtk::TreeStore, iter: &gtk::TreeIter) -> String {
    let boxed = get_node_data(store, iter);
    let data: Ref<TreeNodeData> = boxed.borrow();
    let mut parts: Vec<String> = Vec::new();

    let child_labels: Vec<&ArcStr> = data.child_slots.iter().map(|s| match s {
        ChildSlot::Array(l) | ChildSlot::Single(l) => l,
    }).collect();

    let node_id = data.id.inner();
    parts.push(format!("#debug_highlight: &(debug_highlighted == {})", node_id));

    for (label, expr) in &data.args {
        if !label.is_empty() && child_labels.iter().any(|cl| *cl == label) {
            continue;
        }
        if label.as_str() == "debug_highlight" { continue; }
        if label.is_empty() {
            parts.push(format!("{}", expr));
        } else {
            parts.push(format!("#{}: {}", label, expr));
        }
    }

    let child_slots = data.child_slots.clone();
    drop(data);
    let mut child_iter_opt = store.iter_children(Some(iter));
    for slot in &child_slots {
        match slot {
            ChildSlot::Array(label) => {
                let mut items = Vec::new();
                if let Some(ref ci) = child_iter_opt {
                    loop {
                        items.push(tree_to_source(store, ci));
                        if !store.iter_next(ci) { break; }
                    }
                    child_iter_opt = None;
                }
                parts.push(format!("#{}: &[{}]", label, items.join(", ")));
            }
            ChildSlot::Single(label) => {
                if let Some(ref ci) = child_iter_opt {
                    parts.push(format!("#{}: &{}", label, tree_to_source(store, ci)));
                    if !store.iter_next(ci) {
                        child_iter_opt = None;
                    }
                }
            }
        }
    }
    if let Some(ref ci) = child_iter_opt {
        let mut extras = Vec::new();
        loop {
            extras.push(tree_to_source(store, ci));
            if !store.iter_next(ci) { break; }
        }
        if !extras.is_empty() {
            parts.push(format!("#children: &[{}]", extras.join(", ")));
        }
    }

    let kind = store.value(iter, 0).get::<String>().unwrap_or_default();
    format!("browser::{}({})", kind, parts.join(", "))
}

/// Build full source from user code prefix + all tree root widgets.
pub(super) fn full_source_from_tree(
    store: &gtk::TreeStore,
    user_code: &[Expr],
) -> String {
    use graphix_compiler::expr::print::PrettyDisplay;
    let mut parts: Vec<String> = Vec::new();
    for e in user_code {
        let s = e.to_string_pretty(80).to_string();
        parts.push(s.trim_end().to_string());
    }
    if let Some(iter) = store.iter_first() {
        loop {
            parts.push(tree_to_source(store, &iter));
            if !store.iter_next(&iter) { break; }
        }
    }
    reformat_source(&parts.join(";\n"))
}

/// Full sync: rebuild source from tree, re-parse tree, re-render.
pub(super) fn sync_to_source(
    store: &gtk::TreeStore,
    tree_view: &gtk::TreeView,
    buf: &sourceview4::Buffer,
    backend: &crate::backend::Ctx,
    user_code: &Rc<RefCell<Vec<Expr>>>,
) {
    let new_src = full_source_from_tree(store, &user_code.borrow());
    buf.set_text(&new_src);
    parse_and_populate(store, &new_src, user_code);
    tree_view.expand_all();
    backend.render(ArcStr::from(new_src.as_str()));
}

/// Light sync: rebuild source from tree and re-render, but do NOT rebuild tree.
pub(super) fn sync_source_only(
    store: &gtk::TreeStore,
    buf: &sourceview4::Buffer,
    backend: &crate::backend::Ctx,
    user_code: &Rc<RefCell<Vec<Expr>>>,
) {
    let new_src = full_source_from_tree(store, &user_code.borrow());
    buf.set_text(&new_src);
    backend.render(ArcStr::from(new_src.as_str()));
}

// ---- DnD child slot adjustment ----

pub(super) fn update_child_slots_after_dnd(
    store: &gtk::TreeStore,
    env: &Rc<RefCell<Option<Env>>>,
) {
    let Some(iter) = store.iter_first() else { return };
    update_child_slots_recursive(store, &iter, env);
}

fn update_child_slots_recursive(
    store: &gtk::TreeStore,
    iter: &gtk::TreeIter,
    env: &Rc<RefCell<Option<Env>>>,
) {
    let n_children = store.iter_n_children(Some(iter));
    let boxed = get_node_data(store, iter);
    let data = boxed.borrow::<TreeNodeData>();
    let kind = data.kind.clone();
    let has_slots = !data.child_slots.is_empty();
    drop(data);
    drop(boxed);
    let type_child_slots = {
        let env_ref = env.borrow();
        let fn_name = format!("/browser/{}", kind);
        let mod_path = ModPath(netidx::path::Path::from(ArcStr::from(fn_name.as_str())));
        let scope = ModPath::root();
        env_ref.as_ref().and_then(|env| {
            env.lookup_bind(&scope, &mod_path)
                .and_then(|(_, bind)| {
                    if let Type::Fn(ft) = &bind.typ { Some(child_slots_for_fntype(ft)) } else { None }
                })
        }).unwrap_or_default()
    };
    if n_children > 0 && !has_slots {
        let slots = if type_child_slots.is_empty() {
            vec![ChildSlot::Array(ArcStr::from("children"))]
        } else {
            type_child_slots
        };
        let boxed = get_node_data(store, iter);
        let mut data: std::cell::RefMut<TreeNodeData> = boxed.borrow_mut();
        data.child_slots = slots;
    } else if n_children == 0 && has_slots && type_child_slots.is_empty() {
        let boxed = get_node_data(store, iter);
        let mut data: std::cell::RefMut<TreeNodeData> = boxed.borrow_mut();
        data.child_slots.clear();
    }
    if n_children > 0 {
        if let Some(child_iter) = store.iter_children(Some(iter)) {
            loop {
                update_child_slots_recursive(store, &child_iter, env);
                if !store.iter_next(&child_iter) { break; }
            }
        }
    }
}
