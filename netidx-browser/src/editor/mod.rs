// Two-tab view editor:
// - Designer tab (default): widget tree + type-driven property panel
// - Source tab: sourceview4 text editor for .gx source
//
// The designer parses the source using the graphix parser, walks
// the AST to find widget constructor calls, and generates property
// editors from FnType signatures. The TreeStore IS the widget tree
// data model — each row stores a BoxedAnyObject wrapping TreeNodeData
// (kind, args, child slots). Source reconstruction walks the TreeStore.

use crate::BCtx;
use arcstr::ArcStr;
use glib::{self, clone, prelude::*};
use graphix_compiler::{
    env::Env,
    expr::{
        self, ApplyExpr, Expr, ExprKind, LambdaExpr, ModPath, Origin,
        Source as GxSource,
    },
    typ::{FnType, Type},
};
use gtk::{self, prelude::*};
use netidx::{publisher::Typ, utils::Either};
use sourceview4::prelude::*;
use serde::{Serialize, Deserialize};
use std::{
    cell::{Cell, Ref, RefCell},
    rc::Rc,
};

netidx_core::atomic_id!(WidgetNodeId);

static WIDGET_KINDS: &[&str] = &[
    "label", "button", "entry", "switch", "toggle_button",
    "check_button", "combo_box", "scale", "progress_bar",
    "image", "link_button", "search_entry",
    "vbox", "hbox", "grid", "frame", "paned", "notebook",
    "table", "chart", "key_handler",
];

// ---- Tree node data ----

/// How children map back to labeled args during source reconstruction
#[derive(Clone, Debug)]
enum ChildSlot {
    /// Children came from an Array-typed arg: #label: &[child, child, ...]
    Array(ArcStr),
    /// Child came from a single Widget-typed arg: #label: &child
    Single(ArcStr),
}

/// Data stored per TreeStore row via BoxedAnyObject.
#[derive(Clone, Debug)]
struct TreeNodeData {
    id: WidgetNodeId,
    kind: ArcStr,
    /// Non-child labeled args (property args for the panel)
    args: Vec<(ArcStr, Expr)>,
    /// Describes how tree children map to source args.
    child_slots: Vec<ChildSlot>,
}

#[derive(Clone)]
struct ArgInfo {
    label: ArcStr,
    expr: Expr,
    line: i32,
    col: i32,
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
fn classify_widget_expr(expr: &Expr)
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
                    // Unwrap ByRef for child detection
                    let inner = match &arg_expr.kind {
                        ExprKind::ByRef(i) => i.as_ref(),
                        _ => arg_expr,
                    };
                    // Try to detect children
                    match &inner.kind {
                        ExprKind::Array { args: items } => {
                            // If ALL elements are widgets, this is a children array
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
                    // Unlabeled arg — try as children
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

// ---- Tree store helpers ----

fn get_node_data(store: &gtk::TreeStore, iter: &gtk::TreeIter) -> glib::BoxedAnyObject {
    store.value(iter, 1).get::<glib::BoxedAnyObject>().unwrap()
}

fn populate_from_expr(
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

fn parse_and_populate(
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
        // Scan from the end backward: consecutive widget exprs become tree nodes,
        // everything above that is user code (preserved verbatim).
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

fn tree_to_source(store: &gtk::TreeStore, iter: &gtk::TreeIter) -> String {
    let boxed = get_node_data(store, iter);
    let data: Ref<TreeNodeData> = boxed.borrow();
    let mut parts: Vec<String> = Vec::new();

    // Property args
    for (label, expr) in &data.args {
        if label.is_empty() {
            parts.push(format!("{}", expr));
        } else {
            parts.push(format!("#{}: {}", label, expr));
        }
    }

    // Children — format based on child slots
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
    // Any remaining children not covered by slots (e.g. after DnD into non-container)
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

/// Re-parse and pretty-print source text
fn reformat_source(src: &str) -> String {
    use graphix_compiler::expr::print::PrettyDisplay;
    let ori = Origin {
        parent: None,
        source: GxSource::Unspecified,
        text: ArcStr::from(src),
    };
    match expr::parser::parse(ori) {
        Ok(exprs) => {
            let mut parts = Vec::new();
            for e in &*exprs {
                parts.push(e.to_string_pretty(80).to_string());
            }
            parts.join(";\n")
        }
        Err(_) => src.to_string(),
    }
}

/// Build full source from user code prefix + all tree root widgets.
fn full_source_from_tree(
    store: &gtk::TreeStore,
    user_code: &[Expr],
) -> String {
    use graphix_compiler::expr::print::PrettyDisplay;
    let mut parts: Vec<String> = Vec::new();
    for e in user_code {
        parts.push(e.to_string_pretty(80).to_string());
    }
    if let Some(iter) = store.iter_first() {
        loop {
            parts.push(tree_to_source(store, &iter));
            if !store.iter_next(&iter) { break; }
        }
    }
    reformat_source(&parts.join(";\n"))
}

/// Common update path: reconstruct source from TreeStore, update buffer, re-render.
fn sync_to_source(
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

// ---- Type helpers ----

fn unwrap_type(typ: &Type) -> &Type {
    match typ {
        Type::ByRef(inner) => unwrap_type(inner),
        Type::Set(variants) if variants.len() == 2 => {
            variants.iter()
                .find(|t| !matches!(t, Type::Primitive(p) if p.contains(Typ::Null)))
                .map(|t| unwrap_type(t))
                .unwrap_or(typ)
        }
        _ => typ,
    }
}

fn variant_names(typ: &Type) -> Option<Vec<String>> {
    if let Type::Set(variants) = typ {
        let names: Vec<String> = variants.iter().filter_map(|t| {
            if let Type::Variant(tag, args) = t {
                if args.is_empty() { return Some(format!("`{}", tag)); }
            }
            None
        }).collect();
        if names.len() == variants.len() && !names.is_empty() { Some(names) } else { None }
    } else { None }
}

/// Check if a type (deeply) contains Widget.
fn type_contains_widget(typ: &Type) -> bool {
    match typ {
        Type::ByRef(inner) => type_contains_widget(inner),
        Type::Array(inner) => type_contains_widget(inner),
        Type::Set(variants) => {
            // Resolved Widget union: check for known variant tags
            variants.iter().any(|v| matches!(v, Type::Variant(tag, _)
                if tag.as_str() == "Label" || tag.as_str() == "VBox" || tag.as_str() == "Table"))
            // Or a Set containing a Ref to Widget
            || variants.iter().any(|v| type_contains_widget(v))
        }
        // Unresolved type reference — check if it's the Widget type
        Type::Ref { name, .. } => {
            let s = name.0.as_ref();
            s == "/Widget" || s.ends_with("/Widget")
        }
        _ => false,
    }
}

/// Determine child slots for a widget type from its FnType.
fn child_slots_for_fntype(fn_type: &FnType) -> Vec<ChildSlot> {
    let mut slots = Vec::new();
    for arg in fn_type.args.iter() {
        if let Some((name, _)) = &arg.label {
            let inner = unwrap_type(&arg.typ);
            if type_contains_widget(inner) {
                match inner {
                    Type::Array(_) => slots.push(ChildSlot::Array(name.clone())),
                    _ => slots.push(ChildSlot::Single(name.clone())),
                }
            }
        }
    }
    slots
}

/// Parse a small graphix expression string.
fn parse_expr(s: &str) -> Option<Expr> {
    let ori = Origin {
        parent: None,
        source: GxSource::Unspecified,
        text: ArcStr::from(s),
    };
    expr::parser::parse(ori).ok()?.last().cloned()
}

/// Generate a default expression for a required arg based on its type.
fn default_expr_for_type(typ: &Type) -> Option<Expr> {
    let inner = unwrap_type(typ);
    let src = match inner {
        Type::Primitive(flags) => {
            if flags.contains(Typ::String) { "&\"\"" }
            else if flags.contains(Typ::Bool) { "&false" }
            else if flags.contains(Typ::F64) || flags.contains(Typ::F32) { "&0.0" }
            else { "&0" }
        }
        Type::Array(_) => "&[]",
        _ => "&null",
    };
    parse_expr(src)
}

// ---- Lambda helpers ----

fn lambda_sig(lambda: &LambdaExpr) -> String {
    let mut s = String::from("|");
    for (i, a) in lambda.args.iter().enumerate() {
        match &a.labeled {
            None => {
                s.push_str(&format!("{}", a.pattern));
                if let Some(t) = &a.constraint {
                    s.push_str(&format!(": {}", t));
                }
            }
            Some(_) => {
                s.push_str(&format!("#{}", a.pattern));
                if let Some(t) = &a.constraint {
                    s.push_str(&format!(": {}", t));
                }
            }
        }
        if i < lambda.args.len() - 1 { s.push_str(", "); }
    }
    s.push('|');
    s
}

fn fntype_sig(ft: &FnType) -> String {
    let mut s = String::from("|");
    for (i, a) in ft.args.iter().enumerate() {
        match &a.label {
            Some((name, _)) => s.push_str(&format!("#{}: {}", name, a.typ)),
            None => s.push_str(&format!("{}", a.typ)),
        }
        if i < ft.args.len() - 1 { s.push_str(", "); }
    }
    s.push('|');
    s
}

fn lambda_body(lambda: &LambdaExpr) -> String {
    match &lambda.body {
        Either::Left(expr) => format!("{}", expr),
        Either::Right(name) => format!("'{}", name),
    }
}

// ---- Editor ----

pub(crate) struct Editor {
    root: gtk::Notebook,
}

impl Editor {
    pub(crate) fn new(ctx: &BCtx, source: ArcStr) -> Editor {
        let root = gtk::Notebook::new();
        let backend = ctx.borrow().backend.clone();
        let env: Rc<RefCell<Option<Env>>> = Rc::new(RefCell::new(
            backend.rt_handle.block_on(backend.gx.get_env()).ok()
        ));

        let user_code: Rc<RefCell<Vec<Expr>>> = Rc::new(RefCell::new(Vec::new()));

        // ---- Designer tab ----
        let designer_box = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let designer_paned = gtk::Paned::new(gtk::Orientation::Horizontal);

        let left = gtk::Box::new(gtk::Orientation::Vertical, 5);
        left.set_margin(5);
        let palette = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let kind_combo = gtk::ComboBoxText::new();
        for kind in WIDGET_KINDS { kind_combo.append(Some(kind), kind); }
        kind_combo.set_active_id(Some("label"));
        let add_btn = gtk::Button::with_label("Add");
        let remove_btn = gtk::Button::with_label("Remove");
        palette.pack_start(&kind_combo, true, true, 0);
        palette.pack_start(&add_btn, false, false, 0);
        palette.pack_start(&remove_btn, false, false, 0);
        left.pack_start(&palette, false, false, 0);

        let tree_store = gtk::TreeStore::new(&[
            glib::Type::STRING,
            glib::BoxedAnyObject::static_type(),
        ]);
        let tree_view = gtk::TreeView::with_model(&tree_store);
        let col = gtk::TreeViewColumn::new();
        col.set_title("Widget");
        let cell = gtk::CellRendererText::new();
        gtk::prelude::CellLayoutExt::pack_start(&col, &cell, true);
        gtk::prelude::CellLayoutExt::add_attribute(&col, &cell, "text", 0);
        tree_view.append_column(&col);
        tree_view.set_reorderable(true);
        let tree_scroll = gtk::ScrolledWindow::new(
            None::<&gtk::Adjustment>, None::<&gtk::Adjustment>,
        );
        tree_scroll.add(&tree_view);
        left.pack_start(&tree_scroll, true, true, 0);

        parse_and_populate(&tree_store, &source, &user_code);
        tree_view.expand_all();

        designer_paned.set_position(250);
        designer_paned.pack1(&left, false, false);

        let prop_scroll = gtk::ScrolledWindow::new(
            None::<&gtk::Adjustment>, None::<&gtk::Adjustment>,
        );
        let prop_box = gtk::Box::new(gtk::Orientation::Vertical, 5);
        prop_box.set_margin(5);
        prop_box.pack_start(
            &gtk::Label::new(Some("Select a widget to edit properties")),
            false, false, 0,
        );
        prop_scroll.add(&prop_box);
        designer_paned.pack2(&prop_scroll, true, false);
        designer_box.pack_start(&designer_paned, true, true, 0);
        root.append_page(&designer_box, Some(&gtk::Label::new(Some("Designer"))));

        // ---- Source tab ----
        let source_box = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let source_toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        source_toolbar.set_margin(5);
        let apply_btn = gtk::Button::with_label("Apply");
        source_toolbar.pack_start(&apply_btn, false, false, 0);
        source_box.pack_start(&source_toolbar, false, false, 0);
        let buf = sourceview4::Buffer::new(None::<&gtk::TextTagTable>);
        buf.set_text(&source);
        buf.set_highlight_syntax(true);
        let source_view = sourceview4::View::with_buffer(&buf);
        source_view.set_show_line_numbers(true);
        source_view.set_monospace(true);
        source_view.set_tab_width(4);
        source_view.set_auto_indent(true);
        let scroll = gtk::ScrolledWindow::new(
            None::<&gtk::Adjustment>, None::<&gtk::Adjustment>,
        );
        scroll.add(&source_view);
        source_box.pack_start(&scroll, true, true, 0);
        apply_btn.connect_clicked(clone!(
            @weak buf, @strong backend, @strong tree_store,
            @weak tree_view, @strong user_code => move |_| {
                let (start, end) = buf.bounds();
                if let Some(text) = buf.text(&start, &end, false) {
                    parse_and_populate(&tree_store, &text, &user_code);
                    tree_view.expand_all();
                    backend.render(ArcStr::from(text.as_str()));
                }
            }
        ));
        root.append_page(&source_box, Some(&gtk::Label::new(Some("Source"))));

        // Flag to prevent circular combo/selection updates
        let updating_combo: Rc<Cell<bool>> = Rc::new(Cell::new(false));

        // ---- Selection → property panel ----
        tree_view.selection().connect_changed(clone!(
            @strong tree_store, @strong prop_box, @strong env,
            @strong root, @strong buf, @strong source_view,
            @strong backend, @strong kind_combo, @strong tree_view,
            @strong updating_combo, @strong user_code => move |sel| {
                for child in prop_box.children() { prop_box.remove(&child); }
                let (_, iter) = match sel.selected() {
                    Some(x) => x,
                    None => {
                        prop_box.pack_start(
                            &gtk::Label::new(Some("Select a widget to edit properties")),
                            false, false, 0,
                        );
                        prop_box.show_all();
                        return;
                    }
                };
                let kind = tree_store.value(&iter, 0).get::<String>().unwrap_or_default();

                // Feature 1: sync combo to selection
                updating_combo.set(true);
                kind_combo.set_active_id(Some(&kind));
                updating_combo.set(false);

                // Read args from TreeStore
                let node_args: Vec<ArgInfo> = {
                    let boxed = get_node_data(&tree_store, &iter);
                    let data: Ref<TreeNodeData> = boxed.borrow();
                    data.args.iter().map(|(label, expr)| ArgInfo {
                        label: label.clone(),
                        expr: expr.clone(),
                        line: expr.pos.line as i32,
                        col: expr.pos.column as i32,
                    }).collect()
                };

                let header = gtk::Label::new(None);
                header.set_markup(&format!("<b>{}</b>", kind));
                header.set_halign(gtk::Align::Start);
                prop_box.pack_start(&header, false, false, 5);

                let env_ref = env.borrow();
                let fn_name = format!("/browser/{}", kind);
                let mod_path = ModPath(netidx::path::Path::from(ArcStr::from(fn_name.as_str())));
                let scope = ModPath::root();
                let fn_type = env_ref.as_ref().and_then(|env| {
                    env.lookup_bind(&scope, &mod_path)
                        .and_then(|(_, bind)| {
                            if let Type::Fn(ft) = &bind.typ { Some(ft.clone()) } else { None }
                        })
                });

                if let Some(fn_type) = fn_type {
                    let grid = gtk::Grid::new();
                    grid.set_column_spacing(8);
                    grid.set_row_spacing(4);
                    let mut row = 0;

                    for arg in fn_type.args.iter() {
                        let (name, _is_optional) = match &arg.label {
                            Some((n, opt)) => (n.clone(), *opt),
                            None => continue,
                        };
                        // Skip child-bearing args — they're shown in the tree
                        let inner = unwrap_type(&arg.typ);
                        if type_contains_widget(inner) { continue; }

                        let label = gtk::Label::new(Some(&format!("{}:", name)));
                        label.set_halign(gtk::Align::End);
                        grid.attach(&label, 0, row, 1, 1);

                        let arg_info = node_args.iter().find(|a| a.label == name);
                        let fn_inner = unwrap_type(inner);
                        if let Type::Fn(cb_fn) = fn_inner {
                            // Callback property — detect mode via AST
                            match arg_info {
                                Some(ai) => {
                                    match &ai.expr.kind {
                                    ExprKind::Lambda(lambda) => {
                                        // Mode A: lambda expression
                                        let sig = lambda_sig(lambda);
                                        let body = lambda_body(lambda);
                                        let hbox = gtk::Box::new(
                                            gtk::Orientation::Horizontal, 4,
                                        );
                                        let sig_label = gtk::Label::new(Some(&sig));
                                        sig_label.set_opacity(0.6);
                                        hbox.pack_start(&sig_label, false, false, 0);
                                        let entry = gtk::Entry::new();
                                        entry.set_text(&body);
                                        entry.set_hexpand(true);
                                        entry.set_placeholder_text(
                                            Some("body expression"),
                                        );
                                        let buf_c = buf.clone();
                                        let backend_c = backend.clone();
                                        let name_c = name.clone();
                                        let sig_c = sig.clone();
                                        let ts_c = tree_store.clone();
                                        let tv_c = tree_view.clone();
                                        let uc_c = user_code.clone();
                                        entry.connect_activate(move |e| {
                                            let new_body = e.text().to_string();
                                            if new_body.is_empty() { return; }
                                            let lambda = format!(
                                                "{} {}", sig_c, new_body,
                                            );
                                            let (s, e2) = buf_c.bounds();
                                            if let Some(src) = buf_c.text(&s, &e2, false) {
                                                let new_src = splice_arg(
                                                    &src, &name_c, &lambda,
                                                );
                                                buf_c.set_text(&new_src);
                                                parse_and_populate(&ts_c, &new_src, &uc_c);
                                                tv_c.expand_all();
                                                backend_c.render(ArcStr::from(
                                                    new_src.as_str(),
                                                ));
                                            }
                                        });
                                        hbox.pack_start(&entry, true, true, 0);
                                        hbox.set_hexpand(true);
                                        grid.attach(&hbox, 1, row, 1, 1);
                                    }
                                    _ => {
                                        // Mode B: named function or other expr
                                        let val_label = gtk::Label::new(Some(
                                            &format!("{}", ai.expr),
                                        ));
                                        val_label.set_halign(gtk::Align::Start);
                                        val_label.set_hexpand(true);
                                        grid.attach(&val_label, 1, row, 1, 1);
                                    }
                                }},
                                None => {
                                    // Mode C: not specified (optional, null)
                                    let sig = fntype_sig(cb_fn);
                                    let hbox = gtk::Box::new(
                                        gtk::Orientation::Horizontal, 4,
                                    );
                                    let sig_label = gtk::Label::new(Some(&sig));
                                    sig_label.set_opacity(0.6);
                                    hbox.pack_start(&sig_label, false, false, 0);
                                    let entry = gtk::Entry::new();
                                    entry.set_hexpand(true);
                                    entry.set_placeholder_text(
                                        Some("body expression"),
                                    );
                                    let buf_c = buf.clone();
                                    let backend_c = backend.clone();
                                    let name_c = name.clone();
                                    let sig_c = sig.clone();
                                    let ts_c = tree_store.clone();
                                    let tv_c = tree_view.clone();
                                    let uc_c = user_code.clone();
                                    entry.connect_activate(move |e| {
                                        let new_body = e.text().to_string();
                                        if new_body.is_empty() { return; }
                                        let lambda = format!(
                                            "{} {}", sig_c, new_body,
                                        );
                                        let (s, e2) = buf_c.bounds();
                                        if let Some(src) = buf_c.text(&s, &e2, false) {
                                            let new_src = splice_arg(
                                                &src, &name_c, &lambda,
                                            );
                                            buf_c.set_text(&new_src);
                                            parse_and_populate(&ts_c, &new_src, &uc_c);
                                            tv_c.expand_all();
                                            backend_c.render(ArcStr::from(
                                                new_src.as_str(),
                                            ));
                                        }
                                    });
                                    hbox.pack_start(&entry, true, true, 0);
                                    hbox.set_hexpand(true);
                                    grid.attach(&hbox, 1, row, 1, 1);
                                }
                            }
                        } else {
                            // Non-callback property
                            let current = arg_info
                                .map(|a| format!("{}", a.expr))
                                .unwrap_or_default();
                            let editor = make_prop_editor(
                                fn_inner, &current, name.to_string(),
                                buf.clone(), backend.clone(),
                                tree_store.clone(), tree_view.clone(),
                                user_code.clone(),
                            );
                            editor.set_hexpand(true);
                            grid.attach(&editor, 1, row, 1, 1);
                        }

                        // Edit button for every row
                        let edit_btn = gtk::Button::with_label("Edit");
                        let root_c = root.clone();
                        let buf_c = buf.clone();
                        let sv_c = source_view.clone();
                        let cb_fn_for_insert = if let Type::Fn(ft) = fn_inner {
                            Some(ft.clone())
                        } else {
                            None
                        };
                        let name_c = name.clone();
                        let ts_c = tree_store.clone();
                        let tv_c = tree_view.clone();
                        let backend_c = backend.clone();
                        let uc_c = user_code.clone();
                        let has_arg = arg_info.is_some();
                        let al = arg_info.map(|a| a.line).unwrap_or(1);
                        let ac = arg_info.map(|a| a.col).unwrap_or(1);
                        edit_btn.connect_clicked(move |_| {
                            if has_arg {
                                root_c.set_current_page(Some(1));
                                let mut iter = buf_c.iter_at_line_offset(
                                    al - 1, ac - 1,
                                );
                                let mut end_iter = iter.clone();
                                if !end_iter.ends_line() {
                                    end_iter.forward_to_line_end();
                                }
                                buf_c.select_range(&iter, &end_iter);
                                sv_c.scroll_to_iter(
                                    &mut iter, 0.0, true, 0.0, 0.5,
                                );
                            } else {
                                let value = match &cb_fn_for_insert {
                                    Some(ft) => {
                                        let sig = fntype_sig(ft);
                                        format!("{} ", sig)
                                    }
                                    None => String::new(),
                                };
                                let (s, e) = buf_c.bounds();
                                if let Some(src) = buf_c.text(&s, &e, false) {
                                    let new_src = splice_arg(
                                        &src, &name_c, &value,
                                    );
                                    buf_c.set_text(&new_src);
                                    parse_and_populate(&ts_c, &new_src, &uc_c);
                                    tv_c.expand_all();
                                    backend_c.render(ArcStr::from(
                                        new_src.as_str(),
                                    ));
                                    root_c.set_current_page(Some(1));
                                    let search = format!("#{}: ", name_c);
                                    let (s2, _) = buf_c.bounds();
                                    if let Some(src2) = buf_c.text(
                                        &s2, &buf_c.end_iter(), false,
                                    ) {
                                        if let Some(pos) = src2.find(&search) {
                                            let byte_offset = pos + search.len();
                                            let mut iter = buf_c.iter_at_offset(
                                                byte_offset as i32,
                                            );
                                            let mut end_iter = iter.clone();
                                            if !end_iter.ends_line() {
                                                end_iter.forward_to_line_end();
                                            }
                                            buf_c.select_range(
                                                &iter, &end_iter,
                                            );
                                            sv_c.scroll_to_iter(
                                                &mut iter, 0.0, true, 0.0, 0.5,
                                            );
                                        }
                                    }
                                }
                            }
                        });
                        grid.attach(&edit_btn, 2, row, 1, 1);
                        row += 1;
                    }
                    prop_box.pack_start(&grid, false, false, 0);
                }
                prop_box.show_all();
            }
        ));

        // ---- Feature 2: Combo changes widget type ----
        kind_combo.connect_changed(clone!(
            @strong tree_store, @strong tree_view, @strong buf,
            @strong backend, @strong env, @strong updating_combo,
            @strong user_code => move |combo| {
                if updating_combo.get() { return; }
                let new_kind = match combo.active_id() {
                    Some(id) => id.to_string(),
                    None => return,
                };
                let sel = tree_view.selection();
                let (_, iter) = match sel.selected() {
                    Some(x) => x,
                    None => return,
                };
                // Read old data
                let boxed = get_node_data(&tree_store, &iter);
                let old_data: Ref<TreeNodeData> = boxed.borrow();
                if old_data.kind.as_str() == new_kind { return; }
                let old_args = old_data.args.clone();
                let old_child_slots = old_data.child_slots.clone();
                drop(old_data);
                drop(boxed);

                // Look up new widget's FnType
                let env_ref = env.borrow();
                let fn_name = format!("/browser/{}", new_kind);
                let mod_path = ModPath(netidx::path::Path::from(ArcStr::from(fn_name.as_str())));
                let scope = ModPath::root();
                let fn_type = env_ref.as_ref().and_then(|env| {
                    env.lookup_bind(&scope, &mod_path)
                        .and_then(|(_, bind)| {
                            if let Type::Fn(ft) = &bind.typ { Some(ft.clone()) } else { None }
                        })
                });

                let new_child_slots = fn_type.as_ref()
                    .map(|ft| child_slots_for_fntype(ft))
                    .unwrap_or_else(|| old_child_slots.clone());

                // Build new args: preserve matching labels, add defaults for missing required
                let mut new_args: Vec<(ArcStr, Expr)> = Vec::new();
                if let Some(ref ft) = fn_type {
                    for arg in ft.args.iter() {
                        if let Some((name, is_optional)) = &arg.label {
                            let inner = unwrap_type(&arg.typ);
                            if type_contains_widget(inner) { continue; }
                            // Try to preserve from old args
                            if let Some(old) = old_args.iter().find(|(l, _)| l == name) {
                                new_args.push(old.clone());
                            } else if !is_optional {
                                // Required arg — generate default
                                if let Some(default_expr) = default_expr_for_type(&arg.typ) {
                                    new_args.push((name.clone(), default_expr));
                                }
                            }
                        }
                    }
                }

                // Update TreeStore row
                let new_data = TreeNodeData {
                    id: WidgetNodeId::new(),
                    kind: ArcStr::from(new_kind.as_str()),
                    args: new_args,
                    child_slots: new_child_slots,
                };
                tree_store.set_value(&iter, 0, &new_kind.to_value());
                tree_store.set_value(&iter, 1, &glib::BoxedAnyObject::new(new_data).to_value());
                sync_to_source(&tree_store, &tree_view, &buf, &backend, &user_code);
            }
        ));

        // ---- Feature 3: Add button adds sibling label ----
        add_btn.connect_clicked(clone!(
            @strong tree_store, @strong tree_view, @strong buf,
            @strong backend, @strong env, @strong user_code => move |_| {
                // Create new label node data
                let label_data = TreeNodeData {
                    id: WidgetNodeId::new(),
                    kind: ArcStr::from("label"),
                    args: vec![
                        (ArcStr::from("text"), parse_expr("&\"new\"").unwrap()),
                    ],
                    child_slots: vec![],
                };

                let sel = tree_view.selection();
                let new_iter = match sel.selected() {
                    Some((_, ref sel_iter)) => {
                        let parent = tree_store.iter_parent(sel_iter);
                        // Insert as sibling after selected (at same level)
                        tree_store.insert_after(parent.as_ref(), Some(sel_iter))
                    }
                    None => {
                        // Nothing selected — append as new root
                        tree_store.append(None)
                    }
                };
                tree_store.set_value(&new_iter, 0, &"label".to_value());
                tree_store.set_value(&new_iter, 1,
                    &glib::BoxedAnyObject::new(label_data).to_value());
                sync_to_source(&tree_store, &tree_view, &buf, &backend, &user_code);
            }
        ));

        // ---- Feature 5: Remove button ----
        remove_btn.connect_clicked(clone!(
            @strong tree_store, @strong tree_view, @strong buf,
            @strong backend, @strong user_code => move |_| {
                let sel = tree_view.selection();
                let (_, iter) = match sel.selected() {
                    Some(x) => x,
                    None => return,
                };
                tree_store.remove(&iter);
                sync_to_source(&tree_store, &tree_view, &buf, &backend, &user_code);
            }
        ));

        // ---- Feature 4: Drag-and-drop sync ----
        tree_view.connect_drag_end(clone!(
            @strong tree_store, @strong tree_view, @strong buf,
            @strong backend, @strong env, @strong user_code => move |_, _| {
                // After DnD, update child_slots for any parent that gained unexpected children
                update_child_slots_after_dnd(&tree_store, &env);
                sync_to_source(&tree_store, &tree_view, &buf, &backend, &user_code);
            }
        ));

        root.set_current_page(Some(0));
        Editor { root }
    }

    pub(crate) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

// ---- DnD child slot adjustment ----

fn update_child_slots_after_dnd(
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
    // Look up whether this widget type actually supports children
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
        // Node gained children but has no slots — add appropriate ones
        let slots = if type_child_slots.is_empty() {
            vec![ChildSlot::Array(ArcStr::from("children"))]
        } else {
            type_child_slots
        };
        let boxed = get_node_data(store, iter);
        let mut data: std::cell::RefMut<TreeNodeData> = boxed.borrow_mut();
        data.child_slots = slots;
    } else if n_children == 0 && has_slots && type_child_slots.is_empty() {
        // Node lost all children AND its type doesn't support children — clear slots
        let boxed = get_node_data(store, iter);
        let mut data: std::cell::RefMut<TreeNodeData> = boxed.borrow_mut();
        data.child_slots.clear();
    }
    // Recurse into children
    if n_children > 0 {
        if let Some(child_iter) = store.iter_children(Some(iter)) {
            loop {
                update_child_slots_recursive(store, &child_iter, env);
                if !store.iter_next(&child_iter) { break; }
            }
        }
    }
}

// ---- splice_arg (still used by property editors) ----

fn splice_arg(src: &str, name: &ArcStr, value: &str) -> String {
    let search = format!("#{}: ", name);
    let spliced = if let Some(pos) = src.find(&search) {
        let after_label = pos + search.len();
        let rest = &src[after_label..];
        let val_end = find_arg_end(rest) + after_label;
        let mut new = String::new();
        new.push_str(&src[..after_label]);
        new.push_str(value);
        new.push_str(&src[val_end..]);
        new
    } else {
        if let Some(close) = src.rfind(')') {
            let mut new = String::new();
            new.push_str(&src[..close]);
            let before = src[..close].trim_end();
            if !before.ends_with(',') && !before.ends_with('(') {
                new.push(',');
            }
            new.push_str(&format!("\n    #{}: {}", name, value));
            new.push_str(&src[close..]);
            new
        } else {
            src.to_string()
        }
    };
    reformat_source(&spliced)
}

fn find_arg_end(s: &str) -> usize {
    let mut depth = 0i32;
    let bytes = s.as_bytes();
    for i in 0..bytes.len() {
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => { if depth == 0 { return i; } depth -= 1; }
            b',' if depth == 0 => return i,
            _ => {}
        }
    }
    s.len()
}

fn make_prop_editor(
    typ: &Type,
    current: &str,
    name: String,
    buf: sourceview4::Buffer,
    backend: crate::backend::Ctx,
    tree_store: gtk::TreeStore,
    tree_view: gtk::TreeView,
    user_code: Rc<RefCell<Vec<Expr>>>,
) -> gtk::Widget {
    let name_arc = ArcStr::from(name.as_str());
    let on_change = {
        let name = name_arc.clone();
        let buf = buf.clone();
        let backend = backend.clone();
        let tree_store = tree_store.clone();
        let tree_view = tree_view.clone();
        let user_code = user_code.clone();
        move |val: String| {
            let (start, end) = buf.bounds();
            if let Some(src) = buf.text(&start, &end, false) {
                let new_src = splice_arg(&src, &name, &val);
                buf.set_text(&new_src);
                parse_and_populate(&tree_store, &new_src, &user_code);
                tree_view.expand_all();
                backend.render(ArcStr::from(new_src.as_str()));
            }
        }
    };

    if let Some(names) = variant_names(typ) {
        let combo = gtk::ComboBoxText::new();
        for n in &names { combo.append(Some(n), n); }
        combo.set_active_id(Some(current.trim_start_matches('&')));
        let on_change = on_change.clone();
        combo.connect_changed(move |c| {
            if let Some(id) = c.active_id() { on_change(id.to_string()); }
        });
        return combo.upcast();
    }
    if let Type::Primitive(flags) = typ {
        if flags.contains(Typ::Bool) {
            let check = gtk::CheckButton::new();
            check.set_active(current == "true" || current == "&true");
            let on_change = on_change.clone();
            check.connect_toggled(move |b| {
                on_change(if b.is_active() { "&true".into() } else { "&false".into() });
            });
            return check.upcast();
        }
    }
    let entry = gtk::Entry::new();
    entry.set_text(current);
    entry.connect_activate(move |e| { on_change(e.text().to_string()); });
    entry.upcast()
}
