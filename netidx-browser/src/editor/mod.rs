// Two-tab view editor:
// - Designer tab (default): widget tree + type-driven property panel
// - Source tab: sourceview4 text editor for .gx source
//
// The designer parses the source using the graphix parser, walks
// the AST to find widget constructor calls, and generates property
// editors from FnType signatures. The TreeStore IS the widget tree
// data model — each row stores a BoxedAnyObject wrapping TreeNodeData
// (kind, args, child slots). Source reconstruction walks the TreeStore.

mod expr_util;
mod tree;
mod type_ed;

use crate::BCtx;
use arcstr::ArcStr;
use glib::{self, clone, prelude::*};
use graphix_compiler::{
    env::Env,
    expr::{Expr, ModPath},
    typ::Type,
};
use gtk::{self, prelude::*};
use netidx::publisher::Value;
use sourceview4::prelude::*;
use std::{
    cell::{Cell, Ref, RefCell},
    rc::Rc,
};

use expr_util::*;
use tree::*;
use type_ed::*;

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
                        let _ = backend.set_var(
                            backend.debug_highlighted_bid,
                            Value::from(-1i64),
                        );
                        prop_box.pack_start(
                            &gtk::Label::new(Some("Select a widget to edit properties")),
                            false, false, 0,
                        );
                        prop_box.show_all();
                        return;
                    }
                };
                let kind = tree_store.value(&iter, 0).get::<String>().unwrap_or_default();

                updating_combo.set(true);
                kind_combo.set_active_id(Some(&kind));
                updating_combo.set(false);

                {
                    let boxed = get_node_data(&tree_store, &iter);
                    let data: Ref<TreeNodeData> = boxed.borrow();
                    let node_id = data.id.inner() as i64;
                    drop(data);
                    drop(boxed);
                    let _ = backend.set_var(
                        backend.debug_highlighted_bid,
                        Value::from(node_id),
                    );
                }

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
                        if name.as_str() == "debug_highlight" { continue; }
                        if type_contains_widget(&arg.typ) { continue; }

                        let arg_info = node_args.iter().find(|a| a.label == name);
                        let type_tooltip = format!("{}", arg.typ);
                        let label = gtk::Label::new(Some(&format!("{}:", name)));
                        label.set_halign(gtk::Align::Start);
                        label.set_tooltip_text(Some(&type_tooltip));
                        grid.attach(&label, 0, row, 1, 1);

                        let on_change = make_tree_on_change(
                            name.clone(), tree_store.clone(), tree_view.clone(),
                            buf.clone(), backend.clone(), user_code.clone(),
                        );
                        let nav = SourceNav {
                            root: root.clone(),
                            source_view: source_view.clone(),
                            buf: buf.clone(),
                        };
                        let editor = type_editor(
                            &arg.typ, arg_info.map(|a| &a.expr),
                            env_ref.as_ref(), Some(&nav), on_change,
                        );
                        editor.set_hexpand(true);
                        editor.set_tooltip_text(Some(&type_tooltip));
                        grid.attach(&editor, 1, row, 1, 1);

                        // Edit button for every row
                        let edit_btn = gtk::Button::with_label("Edit");
                        let root_c = root.clone();
                        let buf_c = buf.clone();
                        let sv_c = source_view.clone();
                        let cb_fn_for_insert = {
                            let mut t = &arg.typ;
                            loop {
                                match t {
                                    Type::ByRef(inner) => t = inner.as_ref(),
                                    Type::Set(v) if is_nullable_set(v) => t = non_null_type(v),
                                    _ => break,
                                }
                            }
                            if let Type::Fn(ft) = t { Some(ft.clone()) } else { None }
                        };
                        let default_for_insert = default_expr_for_type(
                            &arg.typ, env_ref.as_ref(),
                        );
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
                                    None => format!("{}", default_for_insert),
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

        // ---- Combo changes widget type ----
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
                let boxed = get_node_data(&tree_store, &iter);
                let old_data: Ref<TreeNodeData> = boxed.borrow();
                if old_data.kind.as_str() == new_kind { return; }
                let old_args = old_data.args.clone();
                let old_child_slots = old_data.child_slots.clone();
                drop(old_data);
                drop(boxed);

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

                let mut new_args: Vec<(ArcStr, Expr)> = Vec::new();
                if let Some(ref ft) = fn_type {
                    for arg in ft.args.iter() {
                        if let Some((name, is_optional)) = &arg.label {
                            if type_contains_widget(&arg.typ) { continue; }
                            if let Some(old) = old_args.iter().find(|(l, _)| l == name) {
                                new_args.push(old.clone());
                            } else if !is_optional {
                                new_args.push((
                                    name.clone(),
                                    default_expr_for_type(&arg.typ, env_ref.as_ref()),
                                ));
                            }
                        }
                    }
                }

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

        // ---- Add button adds sibling label ----
        add_btn.connect_clicked(clone!(
            @strong tree_store, @strong tree_view, @strong buf,
            @strong backend, @strong env, @strong user_code => move |_| {
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
                        tree_store.insert_after(parent.as_ref(), Some(sel_iter))
                    }
                    None => {
                        tree_store.append(None)
                    }
                };
                tree_store.set_value(&new_iter, 0, &"label".to_value());
                tree_store.set_value(&new_iter, 1,
                    &glib::BoxedAnyObject::new(label_data).to_value());
                sync_to_source(&tree_store, &tree_view, &buf, &backend, &user_code);
            }
        ));

        // ---- Remove button ----
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

        // ---- Drag-and-drop sync ----
        tree_view.connect_drag_end(clone!(
            @strong tree_store, @strong tree_view, @strong buf,
            @strong backend, @strong env, @strong user_code => move |_, _| {
                update_child_slots_after_dnd(&tree_store, &env);
                sync_to_source(&tree_store, &tree_view, &buf, &backend, &user_code);
            }
        ));

        root.set_current_page(Some(0));
        root.connect_destroy(clone!(@strong backend => move |_| {
            let _ = backend.set_var(
                backend.debug_highlighted_bid,
                Value::from(-1i64),
            );
        }));
        Editor { root }
    }

    pub(crate) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
