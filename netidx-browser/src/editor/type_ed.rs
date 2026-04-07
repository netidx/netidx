// Type-driven property editor system.

use arcstr::ArcStr;
use glib::prelude::*;
use graphix_compiler::{
    env::Env,
    expr::{
        self, Expr, ExprKind, StructExpr,
    },
    typ::{FnType, Type},
};
use gtk::{self, prelude::*};
use netidx::publisher::{Typ, Value};
use std::{
    cell::RefCell,
    rc::Rc,
};
use triomphe::Arc;

use super::expr_util::*;
use super::tree::*;

// ---- Source navigation ----

/// Context for navigating to source from Edit buttons inside type editors.
#[derive(Clone)]
pub(super) struct SourceNav {
    pub(super) root: gtk::Notebook,
    pub(super) source_view: sourceview4::View,
    pub(super) buf: sourceview4::Buffer,
}

fn navigate_to_field(nav: &SourceNav, search: &str) {
    nav.root.set_current_page(Some(1));
    let (s, _) = nav.buf.bounds();
    if let Some(src) = nav.buf.text(&s, &nav.buf.end_iter(), false) {
        if let Some(pos) = src.find(search) {
            let byte_offset = pos + search.len();
            let mut iter = nav.buf.iter_at_offset(byte_offset as i32);
            let mut end_iter = iter.clone();
            if !end_iter.ends_line() { end_iter.forward_to_line_end(); }
            nav.buf.select_range(&iter, &end_iter);
            nav.source_view.scroll_to_iter(&mut iter, 0.0, true, 0.0, 0.5);
        }
    }
}

// ---- Leaf editors ----

fn make_entry_editor(arg_expr: Option<&Expr>, on_change: Rc<dyn Fn(Option<Expr>)>) -> gtk::Widget {
    let entry = gtk::Entry::new();
    if let Some(e) = arg_expr {
        entry.set_text(&format!("{}", e));
    }
    entry.connect_activate(move |e| {
        let text = e.text().to_string();
        if text.is_empty() { on_change(None) }
        else { on_change(parse_expr(&text)) }
    });
    entry.upcast()
}

/// Build an Rc<dyn Fn(Option<Expr>)> that updates TreeNodeData and syncs source.
pub(super) fn make_tree_on_change(
    name: ArcStr,
    tree_store: gtk::TreeStore,
    tree_view: gtk::TreeView,
    buf: sourceview4::Buffer,
    backend: crate::backend::Ctx,
    user_code: Rc<RefCell<Vec<Expr>>>,
) -> Rc<dyn Fn(Option<Expr>)> {
    Rc::new(move |val: Option<Expr>| {
        let sel = tree_view.selection();
        if let Some((_, iter)) = sel.selected() {
            let boxed = get_node_data(&tree_store, &iter);
            let mut data: std::cell::RefMut<TreeNodeData> = boxed.borrow_mut();
            match val {
                None => data.args.retain(|(l, _)| *l != name),
                Some(expr) => {
                    if let Some(arg) = data.args.iter_mut().find(|(l, _)| *l == name) {
                        arg.1 = expr;
                    } else {
                        data.args.push((name.clone(), expr));
                    }
                }
            }
            drop(data);
            drop(boxed);
            sync_source_only(&tree_store, &buf, &backend, &user_code);
        }
    })
}

// ---- Type editor dispatch ----

/// Public entry point: build a type-driven editor widget.
pub(super) fn type_editor(
    typ: &Type,
    arg_expr: Option<&Expr>,
    env: Option<&Env>,
    nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
) -> gtk::Widget {
    type_editor_inner(typ, arg_expr, env, nav, on_change, false)
}

/// Recursive type-driven editor dispatch.
fn type_editor_inner(
    typ: &Type,
    arg_expr: Option<&Expr>,
    env: Option<&Env>,
    nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
    is_optional: bool,
) -> gtk::Widget {
    match typ {
        Type::ByRef(inner) => {
            let inner_expr = arg_expr.and_then(|e| match &e.kind {
                ExprKind::ByRef(i) => Some(i.as_ref()),
                _ => Some(e),
            });
            let parent = on_change;
            let wrapped: Rc<dyn Fn(Option<Expr>)> = Rc::new(move |val: Option<Expr>| {
                parent(val.map(|e| ExprKind::ByRef(Arc::new(e)).to_expr_nopos()))
            });
            type_editor_inner(inner, inner_expr, env, nav, wrapped, is_optional)
        }
        Type::Set(variants) if is_nullable_set(variants) => {
            let inner_expr = arg_expr.and_then(|e| match &e.kind {
                ExprKind::Constant(Value::Null) => None,
                _ => Some(e),
            });
            type_editor_inner(non_null_type(variants), inner_expr, env, nav, on_change, true)
        }

        Type::Fn(ft) => callback_editor(ft, arg_expr, on_change),

        _ if arg_expr.map_or(false, |e| !is_gui_editable(e)) => {
            make_entry_editor(arg_expr, on_change)
        }

        // All-variant set with args → dropdown + sub-editors
        Type::Set(variants) if !all_argless_variants(variants) && all_variants(variants) => {
            variant_editor(variants, arg_expr, env, nav, on_change, is_optional)
        }

        Type::Set(variants) if all_argless_variants(variants) => {
            let combo = gtk::ComboBoxText::new();
            if is_optional { combo.append(Some(""), ""); }
            for v in variants.iter() {
                if let Type::Variant(tag, _) = v {
                    let s = format!("`{}", tag);
                    combo.append(Some(&s), &s);
                }
            }
            let active = arg_expr.map(|e| format!("{}", e)).unwrap_or_default();
            combo.set_active_id(Some(&active));
            let on_change = on_change.clone();
            combo.connect_changed(move |c| {
                match c.active_id() {
                    Some(id) if !id.is_empty() && id.starts_with('`') => {
                        let tag = ArcStr::from(&id[1..]);
                        on_change(Some(ExprKind::Variant {
                            tag,
                            args: Arc::from(Vec::<Expr>::new()),
                        }.to_expr_nopos()));
                    }
                    _ => on_change(None),
                }
            });
            combo.upcast()
        }

        Type::Ref { name, .. } => {
            let resolved = env.and_then(|e| typ.lookup_ref(e).ok());
            match resolved.as_ref() {
                Some(Type::Struct(fields)) => {
                    if lookup_constructor(env, name).is_some() {
                        struct_editor(name, fields, arg_expr, env, nav, on_change, is_optional)
                    } else {
                        struct_editor_inline(fields, arg_expr, env, nav, on_change, is_optional)
                    }
                }
                Some(resolved) => {
                    type_editor_inner(resolved, arg_expr, env, nav, on_change, is_optional)
                }
                None => make_entry_editor(arg_expr, on_change),
            }
        }

        Type::Primitive(flags) if flags.contains(Typ::Bool) => {
            if is_optional {
                let combo = gtk::ComboBoxText::new();
                combo.append(Some(""), "");
                combo.append(Some("true"), "true");
                combo.append(Some("false"), "false");
                let active = arg_expr.map(|e| format!("{}", e)).unwrap_or_default();
                combo.set_active_id(Some(&active));
                let on_change = on_change.clone();
                combo.connect_changed(move |c| {
                    match c.active_id() {
                        Some(id) if id == "true" => {
                            on_change(Some(ExprKind::Constant(Value::Bool(true)).to_expr_nopos()));
                        }
                        Some(id) if id == "false" => {
                            on_change(Some(ExprKind::Constant(Value::Bool(false)).to_expr_nopos()));
                        }
                        _ => on_change(None),
                    }
                });
                combo.upcast()
            } else {
                let check = gtk::CheckButton::new();
                let is_true = arg_expr.map_or(false, |e| {
                    matches!(&e.kind, ExprKind::Constant(Value::Bool(true)))
                });
                check.set_active(is_true);
                let on_change = on_change.clone();
                check.connect_toggled(move |b| {
                    on_change(Some(ExprKind::Constant(Value::Bool(b.is_active())).to_expr_nopos()));
                });
                check.upcast()
            }
        }

        Type::Struct(fields) => {
            struct_editor_inline(fields, arg_expr, env, nav, on_change, is_optional)
        }

        Type::Array(elem) => {
            array_editor(elem, arg_expr, env, nav, on_change)
        }

        Type::Map { key, value } => {
            map_editor(key, value, arg_expr, env, nav, on_change)
        }

        Type::Tuple(elems) => {
            tuple_editor(elems, arg_expr, env, nav, on_change)
        }

        _ => make_entry_editor(arg_expr, on_change),
    }
}

// ---- Specific editors ----

fn callback_editor(
    fn_type: &FnType,
    arg_expr: Option<&Expr>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
) -> gtk::Widget {
    let sig = match arg_expr {
        Some(e) => match &e.kind {
            ExprKind::Lambda(lambda) => lambda_sig(lambda),
            _ => fntype_sig(fn_type),
        },
        None => fntype_sig(fn_type),
    };
    match arg_expr {
        Some(expr) => match &expr.kind {
            ExprKind::Lambda(lambda) => {
                let body = lambda_body(lambda);
                let entry = gtk::Entry::new();
                entry.set_text(&body);
                entry.set_hexpand(true);
                entry.set_placeholder_text(Some("body expression"));
                let on_change = on_change.clone();
                entry.connect_activate(move |e| {
                    let b = e.text().to_string();
                    if !b.is_empty() {
                        on_change(parse_expr(&format!("{} {}", sig, b)));
                    }
                });
                entry.upcast()
            }
            _ => {
                let label = gtk::Label::new(Some(&format!("{}", expr)));
                label.set_halign(gtk::Align::Start);
                label.set_hexpand(true);
                label.upcast()
            }
        },
        None => {
            let entry = gtk::Entry::new();
            entry.set_hexpand(true);
            entry.set_placeholder_text(Some("body expression"));
            let on_change = on_change.clone();
            entry.connect_activate(move |e| {
                let b = e.text().to_string();
                if !b.is_empty() {
                    on_change(parse_expr(&format!("{} {}", sig, b)));
                }
            });
            entry.upcast()
        }
    }
}

fn struct_editor(
    ref_name: &expr::ModPath,
    fields: &[(ArcStr, Type)],
    arg_expr: Option<&Expr>,
    env: Option<&Env>,
    nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
    _is_optional: bool,
) -> gtk::Widget {
    let type_name = netidx::path::Path::basename(&ref_name.0)
        .unwrap_or("unknown");
    let ctor_name = ArcStr::from(type_name.to_lowercase().as_str());
    let current_sub_args = arg_expr
        .map(|a| extract_apply_args(a))
        .unwrap_or_default();
    let sub_values: Rc<RefCell<Vec<(ArcStr, Option<Expr>)>>> = Rc::new(RefCell::new(
        fields.iter().map(|(name, _)| {
            let val = current_sub_args.iter()
                .find(|(l, _)| l == name)
                .map(|(_, e)| e.clone());
            (name.clone(), val)
        }).collect()
    ));
    let expander = gtk::Expander::new(None);
    let grid = gtk::Grid::new();
    grid.set_column_spacing(8);
    grid.set_row_spacing(4);
    for (i, (name, typ)) in fields.iter().enumerate() {
        let label = gtk::Label::new(Some(&format!("{}:", name)));
        label.set_halign(gtk::Align::Start);
        label.set_tooltip_text(Some(&format!("{}", typ)));
        grid.attach(&label, 0, i as i32, 1, 1);
        let sub_expr = current_sub_args.iter()
            .find(|(l, _)| l == name)
            .map(|(_, e)| e);
        let sv = sub_values.clone();
        let ctor = ctor_name.clone();
        let parent = on_change.clone();
        let sub_on_change: Rc<dyn Fn(Option<Expr>)> = Rc::new(move |val: Option<Expr>| {
            sv.borrow_mut()[i].1 = val;
            match rebuild_constructor_expr(&ctor, &sv.borrow()) {
                Some(expr) => parent(Some(expr)),
                None => parent(None),
            }
        });
        let sub_on_change_c = sub_on_change.clone();
        let editor = type_editor_inner(
            typ, sub_expr, env, nav, sub_on_change, true,
        );
        editor.set_hexpand(true);
        editor.set_tooltip_text(Some(&format!("{}", typ)));
        grid.attach(&editor, 1, i as i32, 1, 1);
        if let Some(nav) = nav {
            let edit_btn = gtk::Button::with_label("Edit");
            let nav = nav.clone();
            let search = format!("#{}: ", name);
            let default_val = default_expr_for_type(typ, env);
            let sv_c = sub_values.clone();
            edit_btn.connect_clicked(move |_| {
                if sv_c.borrow()[i].1.is_none() {
                    sub_on_change_c(Some(default_val.clone()));
                }
                navigate_to_field(&nav, &search);
            });
            grid.attach(&edit_btn, 2, i as i32, 1, 1);
        }
    }
    expander.add(&grid);
    expander.set_hexpand(true);
    expander.upcast()
}

fn struct_editor_inline(
    fields: &[(ArcStr, Type)],
    arg_expr: Option<&Expr>,
    env: Option<&Env>,
    nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
    _is_optional: bool,
) -> gtk::Widget {
    let current_fields: Vec<(ArcStr, Expr)> = match arg_expr {
        Some(e) => match &e.kind {
            ExprKind::Struct(se) => se.args.iter()
                .map(|(n, e)| (n.clone(), e.clone()))
                .collect(),
            ExprKind::ByRef(inner) => match &inner.kind {
                ExprKind::Struct(se) => se.args.iter()
                    .map(|(n, e)| (n.clone(), e.clone()))
                    .collect(),
                _ => vec![],
            },
            _ => vec![],
        },
        None => vec![],
    };
    let sub_values: Rc<RefCell<Vec<(ArcStr, Option<Expr>)>>> = Rc::new(RefCell::new(
        fields.iter().map(|(name, _)| {
            let val = current_fields.iter()
                .find(|(l, _)| l == name)
                .map(|(_, e)| e.clone());
            (name.clone(), val)
        }).collect()
    ));
    let expander = gtk::Expander::new(None);
    let grid = gtk::Grid::new();
    grid.set_column_spacing(8);
    grid.set_row_spacing(4);
    for (i, (name, typ)) in fields.iter().enumerate() {
        let label = gtk::Label::new(Some(&format!("{}:", name)));
        label.set_halign(gtk::Align::Start);
        grid.attach(&label, 0, i as i32, 1, 1);
        let sub_expr = current_fields.iter()
            .find(|(l, _)| l == name)
            .map(|(_, e)| e);
        let sv = sub_values.clone();
        let parent = on_change.clone();
        let sub_on_change: Rc<dyn Fn(Option<Expr>)> = Rc::new(move |val: Option<Expr>| {
            sv.borrow_mut()[i].1 = val;
            let vals = sv.borrow();
            let fields: Vec<(ArcStr, Expr)> = vals.iter()
                .filter_map(|(n, e)| e.as_ref().map(|e| (n.clone(), e.clone())))
                .collect();
            if fields.is_empty() {
                parent(None);
            } else {
                parent(Some(
                    ExprKind::Struct(StructExpr { args: Arc::from(fields) }).to_expr_nopos()
                ));
            }
        });
        let sub_on_change_c = sub_on_change.clone();
        let editor = type_editor_inner(
            typ, sub_expr, env, nav, sub_on_change, true,
        );
        editor.set_hexpand(true);
        grid.attach(&editor, 1, i as i32, 1, 1);
        if let Some(nav) = nav {
            let edit_btn = gtk::Button::with_label("Edit");
            let nav = nav.clone();
            let search = format!("{}: ", name);
            let default_val = default_expr_for_type(typ, env);
            let sv_c = sub_values.clone();
            edit_btn.connect_clicked(move |_| {
                if sv_c.borrow()[i].1.is_none() {
                    sub_on_change_c(Some(default_val.clone()));
                }
                navigate_to_field(&nav, &search);
            });
            grid.attach(&edit_btn, 2, i as i32, 1, 1);
        }
    }
    expander.add(&grid);
    expander.set_hexpand(true);
    expander.upcast()
}

fn array_editor(
    elem_type: &Type,
    arg_expr: Option<&Expr>,
    env: Option<&Env>,
    nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
) -> gtk::Widget {
    let elements: Vec<Expr> = match arg_expr {
        Some(e) => match &e.kind {
            ExprKind::Array { args } => args.iter().cloned().collect(),
            _ => vec![],
        },
        None => vec![],
    };
    let values: Rc<RefCell<Vec<Option<Expr>>>> = Rc::new(RefCell::new(
        elements.into_iter().map(Some).collect()
    ));
    let container = gtk::Box::new(gtk::Orientation::Vertical, 4);
    let rebuild: Rc<dyn Fn()> = {
        let values = values.clone();
        let on_change = on_change.clone();
        Rc::new(move || {
            let vals = values.borrow();
            let exprs: Vec<Expr> = vals.iter()
                .filter_map(|e| e.clone())
                .collect();
            if exprs.is_empty() {
                on_change(None);
            } else {
                on_change(Some(
                    ExprKind::Array { args: Arc::from(exprs) }.to_expr_nopos()
                ));
            }
        })
    };
    // Owned copies for the rebuild_ui closure
    let elem_type = elem_type.clone();
    let env_owned = env.cloned();
    let nav_owned = nav.cloned();
    let add_btn = gtk::Button::with_label("+");
    // Self-referential closure: remove buttons need to call rebuild_ui
    let rebuild_ui: Rc<RefCell<Option<Rc<dyn Fn()>>>> = Rc::new(RefCell::new(None));
    let rebuild_ui_impl: Rc<dyn Fn()> = {
        let container = container.clone();
        let add_btn = add_btn.clone();
        let values = values.clone();
        let rebuild = rebuild.clone();
        let elem_type = elem_type.clone();
        let env_owned = env_owned.clone();
        let nav_owned = nav_owned.clone();
        let rebuild_ui_ref = rebuild_ui.clone();
        Rc::new(move || {
            for child in container.children() { container.remove(&child); }
            for i in 0..values.borrow().len() {
                let row = gtk::Box::new(gtk::Orientation::Horizontal, 4);
                let sub_expr = values.borrow()[i].clone();
                let v = values.clone();
                let rb = rebuild.clone();
                let sub_on_change: Rc<dyn Fn(Option<Expr>)> = Rc::new(move |val: Option<Expr>| {
                    v.borrow_mut()[i] = val;
                    rb();
                });
                let editor = type_editor_inner(
                    &elem_type, sub_expr.as_ref(), env_owned.as_ref(),
                    nav_owned.as_ref(), sub_on_change, false,
                );
                editor.set_hexpand(true);
                row.pack_start(&editor, true, true, 0);
                let remove_btn = gtk::Button::with_label("-");
                let v = values.clone();
                let rb = rebuild.clone();
                let rui = rebuild_ui_ref.clone();
                remove_btn.connect_clicked(move |_| {
                    v.borrow_mut().remove(i);
                    rb();
                    if let Some(f) = rui.borrow().as_ref() { f(); }
                });
                row.pack_start(&remove_btn, false, false, 0);
                container.pack_start(&row, false, false, 0);
            }
            container.pack_start(&add_btn, false, false, 0);
            container.show_all();
        })
    };
    *rebuild_ui.borrow_mut() = Some(rebuild_ui_impl.clone());
    // Initial build
    rebuild_ui_impl();
    // Wire up add button
    {
        let v = values.clone();
        let rb = rebuild.clone();
        let rebuild_ui_impl = rebuild_ui_impl.clone();
        let elem_default = default_expr_for_type(&elem_type, env_owned.as_ref());
        add_btn.connect_clicked(move |_| {
            v.borrow_mut().push(Some(elem_default.clone()));
            rb();
            rebuild_ui_impl();
        });
    }
    let expander = gtk::Expander::new(None);
    expander.add(&container);
    expander.set_hexpand(true);
    expander.upcast()
}

fn map_editor(
    _key_type: &Type,
    _value_type: &Type,
    arg_expr: Option<&Expr>,
    _env: Option<&Env>,
    _nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
) -> gtk::Widget {
    // TODO: proper key-value pair editor
    make_entry_editor(arg_expr, on_change)
}

fn tuple_editor(
    elem_types: &[Type],
    arg_expr: Option<&Expr>,
    env: Option<&Env>,
    nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
) -> gtk::Widget {
    let elements: Vec<Option<Expr>> = match arg_expr {
        Some(e) => match &e.kind {
            ExprKind::Tuple { args } => args.iter().map(|e| Some(e.clone())).collect(),
            _ => vec![None; elem_types.len()],
        },
        None => vec![None; elem_types.len()],
    };
    let values: Rc<RefCell<Vec<Option<Expr>>>> = Rc::new(RefCell::new(
        elements.into_iter()
            .chain(std::iter::repeat(None))
            .take(elem_types.len())
            .collect()
    ));
    let expander = gtk::Expander::new(None);
    let grid = gtk::Grid::new();
    grid.set_column_spacing(8);
    grid.set_row_spacing(4);
    for (i, typ) in elem_types.iter().enumerate() {
        let label = gtk::Label::new(Some(&format!("{}:", i)));
        label.set_halign(gtk::Align::Start);
        grid.attach(&label, 0, i as i32, 1, 1);
        let sub_expr = values.borrow()[i].clone();
        let v = values.clone();
        let parent = on_change.clone();
        let sub_on_change: Rc<dyn Fn(Option<Expr>)> = Rc::new(move |val: Option<Expr>| {
            v.borrow_mut()[i] = val;
            let vals = v.borrow();
            let exprs: Vec<Expr> = vals.iter()
                .filter_map(|e| e.clone())
                .collect();
            if exprs.is_empty() {
                parent(None);
            } else {
                parent(Some(
                    ExprKind::Tuple { args: Arc::from(exprs) }.to_expr_nopos()
                ));
            }
        });
        let editor = type_editor_inner(
            typ, sub_expr.as_ref(), env, nav, sub_on_change, false,
        );
        editor.set_hexpand(true);
        grid.attach(&editor, 1, i as i32, 1, 1);
    }
    expander.add(&grid);
    expander.set_hexpand(true);
    expander.upcast()
}

/// Variant set editor — dropdown to pick variant tag, sub-editors for args.
fn variant_editor(
    variants: &[Type],
    arg_expr: Option<&Expr>,
    env: Option<&Env>,
    nav: Option<&SourceNav>,
    on_change: Rc<dyn Fn(Option<Expr>)>,
    is_optional: bool,
) -> gtk::Widget {
    let (initial_tag, initial_args): (Option<ArcStr>, Vec<Expr>) = match arg_expr {
        Some(e) => match &e.kind {
            ExprKind::Variant { tag, args } => {
                (Some(tag.clone()), args.iter().cloned().collect())
            }
            _ => (None, vec![]),
        },
        None => (None, vec![]),
    };
    let current_tag: Rc<RefCell<Option<ArcStr>>> = Rc::new(RefCell::new(initial_tag.clone()));
    let sub_values: Rc<RefCell<Vec<Option<Expr>>>> = Rc::new(RefCell::new(
        initial_args.into_iter().map(Some).collect()
    ));
    let vbox = gtk::Box::new(gtk::Orientation::Vertical, 4);
    let combo = gtk::ComboBoxText::new();
    if is_optional { combo.append(Some(""), ""); }
    for v in variants.iter() {
        if let Type::Variant(tag, _) = v {
            let s = format!("`{}", tag);
            combo.append(Some(&s), &s);
        }
    }
    let active = initial_tag.map(|t| format!("`{}", t)).unwrap_or_default();
    combo.set_active_id(Some(&active));
    vbox.pack_start(&combo, false, false, 0);
    let sub_container = gtk::Box::new(gtk::Orientation::Vertical, 4);
    vbox.pack_start(&sub_container, false, false, 0);
    // Owned copies for closures
    let variants_owned: Vec<Type> = variants.to_vec();
    let env_owned = env.cloned();
    let nav_owned = nav.cloned();
    // Fire on_change with current tag + sub_values
    let fire_change: Rc<dyn Fn()> = {
        let ct = current_tag.clone();
        let sv = sub_values.clone();
        let on_change = on_change.clone();
        Rc::new(move || {
            match ct.borrow().as_ref() {
                Some(tag) => {
                    let args: Vec<Expr> = sv.borrow().iter()
                        .filter_map(|e| e.clone())
                        .collect();
                    on_change(Some(ExprKind::Variant {
                        tag: tag.clone(),
                        args: Arc::from(args),
                    }.to_expr_nopos()));
                }
                None => on_change(None),
            }
        })
    };
    // Rebuild sub-editors for the currently selected variant
    let rebuild_sub: Rc<dyn Fn()> = {
        let sub_container = sub_container.clone();
        let ct = current_tag.clone();
        let sv = sub_values.clone();
        let fire = fire_change.clone();
        let variants = variants_owned.clone();
        let env = env_owned.clone();
        let nav = nav_owned.clone();
        Rc::new(move || {
            for child in sub_container.children() { sub_container.remove(&child); }
            let tag = ct.borrow().clone();
            if let Some(tag) = tag {
                let variant_type = variants.iter()
                    .find(|v| matches!(v, Type::Variant(t, _) if *t == tag));
                if let Some(Type::Variant(_, arg_types)) = variant_type {
                    if arg_types.len() == 1 {
                        let sub_expr = sv.borrow().first().cloned().flatten();
                        let sv_c = sv.clone();
                        let fire_c = fire.clone();
                        let sub_on_change: Rc<dyn Fn(Option<Expr>)> = Rc::new(move |val| {
                            let mut vals = sv_c.borrow_mut();
                            if vals.is_empty() { vals.push(val); }
                            else { vals[0] = val; }
                            drop(vals);
                            fire_c();
                        });
                        let editor = type_editor_inner(
                            &arg_types[0], sub_expr.as_ref(),
                            env.as_ref(), nav.as_ref(), sub_on_change, false,
                        );
                        editor.set_hexpand(true);
                        sub_container.pack_start(&editor, false, false, 0);
                    } else if arg_types.len() > 1 {
                        let grid = gtk::Grid::new();
                        grid.set_column_spacing(8);
                        grid.set_row_spacing(4);
                        for (i, typ) in arg_types.iter().enumerate() {
                            let label = gtk::Label::new(Some(&format!("{}:", i)));
                            label.set_halign(gtk::Align::Start);
                            grid.attach(&label, 0, i as i32, 1, 1);
                            let sub_expr = sv.borrow().get(i).cloned().flatten();
                            let sv_c = sv.clone();
                            let fire_c = fire.clone();
                            let sub_on_change: Rc<dyn Fn(Option<Expr>)> = Rc::new(move |val| {
                                let mut vals = sv_c.borrow_mut();
                                while vals.len() <= i { vals.push(None); }
                                vals[i] = val;
                                drop(vals);
                                fire_c();
                            });
                            let editor = type_editor_inner(
                                typ, sub_expr.as_ref(),
                                env.as_ref(), nav.as_ref(), sub_on_change, false,
                            );
                            editor.set_hexpand(true);
                            grid.attach(&editor, 1, i as i32, 1, 1);
                        }
                        sub_container.pack_start(&grid, false, false, 0);
                    }
                }
            }
            sub_container.show_all();
        })
    };
    rebuild_sub();
    // Combo change handler
    {
        let ct = current_tag.clone();
        let sv = sub_values.clone();
        let fire = fire_change.clone();
        let rebuild = rebuild_sub.clone();
        let variants = variants_owned.clone();
        let env = env_owned.clone();
        combo.connect_changed(move |c| {
            match c.active_id() {
                Some(id) if !id.is_empty() && id.starts_with('`') => {
                    let tag = ArcStr::from(&id[1..]);
                    *ct.borrow_mut() = Some(tag.clone());
                    let variant_type = variants.iter()
                        .find(|v| matches!(v, Type::Variant(t, _) if *t == tag));
                    if let Some(Type::Variant(_, arg_types)) = variant_type {
                        *sv.borrow_mut() = arg_types.iter()
                            .map(|t| Some(default_expr_for_type(t, env.as_ref())))
                            .collect();
                    } else {
                        sv.borrow_mut().clear();
                    }
                    fire();
                    rebuild();
                }
                _ => {
                    *ct.borrow_mut() = None;
                    sv.borrow_mut().clear();
                    fire();
                    rebuild();
                }
            }
        });
    }
    vbox.upcast()
}
