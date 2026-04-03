//! Two-tab view editor:
//! - Source tab: sourceview4 text editor for .gx source
//! - Designer tab: widget tree + type-driven property panel
//!
//! The designer generates structured .gx code from its internal
//! widget tree. Property panels are generated from FnType signatures
//! obtained via GXHandle::get_env().

use crate::BCtx;
use arcstr::ArcStr;
use glib::clone;
use graphix_compiler::{
    env::Env,
    typ::{FnType, Type},
};
use gtk::{self, prelude::*};
use netidx::publisher::Typ;
use sourceview4::prelude::*;
use std::{
    cell::RefCell,
    rc::Rc,
};

/// Known widget constructor names
static WIDGET_KINDS: &[&str] = &[
    "label", "button", "entry", "switch", "toggle_button",
    "check_button", "combo_box", "scale", "progress_bar",
    "image", "link_button", "search_entry",
    "vbox", "hbox", "grid", "frame", "paned", "notebook",
    "table", "chart", "key_handler",
];

/// A node in the designer's widget tree
#[derive(Clone)]
struct DesignerNode {
    kind: String,
    args: Vec<(String, String)>,
    children: Vec<DesignerNode>,
}

impl DesignerNode {
    fn new(kind: &str) -> Self {
        DesignerNode { kind: kind.to_string(), args: vec![], children: vec![] }
    }

    /// Generate graphix source for this widget subtree
    fn to_source(&self, indent: usize) -> String {
        let pad = "    ".repeat(indent);
        let mut s = format!("{}browser::{}", pad, self.kind);
        let named: Vec<String> = self.args.iter()
            .map(|(name, val)| format!("#{}: {}", name, val))
            .collect();
        if self.children.is_empty() {
            if named.is_empty() {
                s.push_str(r#"("")"#);
            } else {
                s.push_str("(\n");
                for (i, arg) in named.iter().enumerate() {
                    s.push_str(&format!("{}    {}", pad, arg));
                    if i < named.len() - 1 { s.push(','); }
                    s.push('\n');
                }
                s.push_str(&format!("{})", pad));
            }
        } else {
            s.push_str("(\n");
            for arg in &named {
                s.push_str(&format!("{}    {},\n", pad, arg));
            }
            s.push_str(&format!("{}    [\n", pad));
            for (i, child) in self.children.iter().enumerate() {
                s.push_str(&child.to_source(indent + 2));
                if i < self.children.len() - 1 { s.push(','); }
                s.push('\n');
            }
            s.push_str(&format!("{}    ]\n", pad));
            s.push_str(&format!("{})", pad));
        }
        s
    }
}

/// Check if a type is a set of unit variants (for combo boxes)
fn variant_names(typ: &Type) -> Option<Vec<String>> {
    match typ {
        Type::Set(variants) => {
            let names: Vec<String> = variants.iter().filter_map(|t| {
                match t {
                    Type::Variant(tag, args) if args.is_empty() => {
                        Some(format!("`{}", tag))
                    }
                    _ => None,
                }
            }).collect();
            if names.len() == variants.len() && !names.is_empty() {
                Some(names)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Unwrap &T, [T, null], etc. to get the core type
fn unwrap_type(typ: &Type) -> &Type {
    match typ {
        Type::ByRef(inner) => unwrap_type(inner),
        Type::Set(variants) if variants.len() == 2 => {
            // [T, null] → T
            variants.iter()
                .find(|t| !matches!(t, Type::Primitive(p) if p.is_empty()))
                .map(|t| unwrap_type(t))
                .unwrap_or(typ)
        }
        _ => typ,
    }
}

/// Create a property editor widget for a given type
fn create_editor_for_type(
    typ: &Type,
    current: &str,
    on_change: impl Fn(String) + Clone + 'static,
) -> gtk::Widget {
    let inner = unwrap_type(typ);
    // Function type → callback label + "Edit in Source" hint
    if matches!(inner, Type::Fn(_)) {
        let hbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let label = gtk::Label::new(Some(if current.is_empty() { "(callback)" } else { current }));
        label.set_ellipsize(pango::EllipsizeMode::End);
        label.set_max_width_chars(30);
        label.set_halign(gtk::Align::Start);
        hbox.pack_start(&label, true, true, 0);
        let hint = gtk::Label::new(Some("(edit in Source tab)"));
        hint.set_opacity(0.5);
        hbox.pack_end(&hint, false, false, 0);
        return hbox.upcast();
    }
    // Variant set → combo box
    if let Some(names) = variant_names(inner) {
        let combo = gtk::ComboBoxText::new();
        for name in &names { combo.append(Some(name), name); }
        combo.set_active_id(Some(current.trim_start_matches('&')));
        let on_change = on_change.clone();
        combo.connect_changed(move |c| {
            if let Some(id) = c.active_id() { on_change(id.to_string()); }
        });
        return combo.upcast();
    }
    // Bool → checkbox
    if let Type::Primitive(flags) = inner {
        if flags.contains(Typ::Bool) {
            let check = gtk::CheckButton::new();
            check.set_active(current == "true" || current == "&true");
            let on_change = on_change.clone();
            check.connect_toggled(move |b| {
                on_change(if b.is_active() { "true".into() } else { "false".into() });
            });
            return check.upcast();
        }
    }
    // String → text entry
    if let Type::Primitive(flags) = inner {
        if flags.contains(Typ::String) {
            let entry = gtk::Entry::new();
            entry.set_text(current.trim_matches('"'));
            let on_change = on_change.clone();
            entry.connect_activate(move |e| {
                on_change(format!("{:?}", e.text().as_str()));
            });
            return entry.upcast();
        }
    }
    // Fallback: raw text entry
    let entry = gtk::Entry::new();
    entry.set_text(current);
    let on_change = on_change.clone();
    entry.connect_activate(move |e| { on_change(e.text().to_string()); });
    entry.upcast()
}

/// Build property editors for a widget kind from FnType
fn build_props_from_fntype(
    grid: &gtk::Grid,
    fn_type: &FnType,
    node: &Rc<RefCell<DesignerNode>>,
    on_change: &Rc<dyn Fn()>,
) {
    let mut row = 0;
    for arg in fn_type.args.iter() {
        if let Some((name, _optional)) = &arg.label {
            let label = gtk::Label::new(Some(&format!("{}:", name)));
            label.set_halign(gtk::Align::End);
            grid.attach(&label, 0, row, 1, 1);
            let current = node.borrow().args.iter()
                .find(|(n, _)| n == name.as_str())
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            let inner = unwrap_type(&arg.typ);
            let name = name.clone();
            let node_c = node.clone();
            let on_change_c = on_change.clone();
            let editor = create_editor_for_type(inner, &current, move |val| {
                let mut n = node_c.borrow_mut();
                if let Some(existing) = n.args.iter_mut().find(|(k, _)| k == name.as_str()) {
                    existing.1 = val;
                } else {
                    n.args.push((name.to_string(), val));
                }
                on_change_c();
            });
            editor.set_hexpand(true);
            grid.attach(&editor, 1, row, 1, 1);
            row += 1;
        }
    }
}

pub(crate) struct Editor {
    root: gtk::Notebook,
}

impl Editor {
    pub(crate) fn new(ctx: &BCtx, source: ArcStr) -> Editor {
        let root = gtk::Notebook::new();
        let backend = ctx.borrow().backend.clone();

        // Get Env for type introspection
        let env: Rc<RefCell<Option<Env>>> = Rc::new(RefCell::new(
            backend.rt_handle.block_on(backend.gx.get_env()).ok()
        ));

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
        apply_btn.connect_clicked(clone!(@weak buf, @strong backend => move |_| {
            let (start, end) = buf.bounds();
            if let Some(text) = buf.text(&start, &end, false) {
                backend.render(ArcStr::from(text.as_str()));
            }
        }));
        root.append_page(&source_box, Some(&gtk::Label::new(Some("Source"))));

        // ---- Designer tab ----
        let designer_box = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let designer_paned = gtk::Paned::new(gtk::Orientation::Horizontal);

        // Left: widget palette + tree
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
        let tree_store = gtk::TreeStore::new(&[glib::Type::STRING]);
        let tree_view = gtk::TreeView::with_model(&tree_store);
        let col = gtk::TreeViewColumn::new();
        col.set_title("Widget");
        let cell = gtk::CellRendererText::new();
        gtk::prelude::CellLayoutExt::pack_start(&col, &cell, true);
        gtk::prelude::CellLayoutExt::add_attribute(&col, &cell, "text", 0);
        tree_view.append_column(&col);
        let tree_scroll = gtk::ScrolledWindow::new(
            None::<&gtk::Adjustment>, None::<&gtk::Adjustment>,
        );
        tree_scroll.add(&tree_view);
        left.pack_start(&tree_scroll, true, true, 0);
        designer_paned.pack1(&left, true, false);

        // Right: property panel
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

        // ---- State ----
        // Tree of DesignerNodes, indexed by TreeStore path string
        let nodes: Rc<RefCell<Vec<(String, Rc<RefCell<DesignerNode>>)>>> =
            Rc::new(RefCell::new(vec![]));

        // Regenerate source from designer nodes and apply live
        let regenerate = {
            let nodes_c = nodes.clone();
            let buf_c = buf.clone();
            let backend_c = backend.clone();
            let root_c = root.clone();
            Rc::new(move || {
                let ns = nodes_c.borrow();
                if ns.is_empty() { return; }
                let mut source = String::from("use browser;\n\n");
                if ns.len() == 1 {
                    source.push_str(&ns[0].1.borrow().to_source(0));
                } else {
                    source.push_str("browser::vbox(#spacing: 5, [\n");
                    for (i, (_, node)) in ns.iter().enumerate() {
                        source.push_str(&node.borrow().to_source(1));
                        if i < ns.len() - 1 { source.push(','); }
                        source.push('\n');
                    }
                    source.push_str("])");
                }
                source.push('\n');
                buf_c.set_text(&source);
                backend_c.render(ArcStr::from(source.as_str()));
            }) as Rc<dyn Fn()>
        };

        // Add widget
        add_btn.connect_clicked(clone!(
            @strong kind_combo, @strong tree_store, @strong nodes,
            @strong tree_view, @strong regenerate => move |_| {
                if let Some(kind) = kind_combo.active_id() {
                    let iter = match tree_view.selection().selected() {
                        Some((_, parent_iter)) => tree_store.append(Some(&parent_iter)),
                        None => tree_store.append(None),
                    };
                    tree_store.set_value(&iter, 0, &kind.to_value());
                    let node = Rc::new(RefCell::new(DesignerNode::new(&kind)));
                    let path = tree_store.path(&iter)
                        .map(|p| p.to_string())
                        .unwrap_or_default();
                    nodes.borrow_mut().push((path, node));
                    regenerate();
                }
            }
        ));

        // Remove widget
        remove_btn.connect_clicked(clone!(
            @strong tree_store, @strong tree_view, @strong nodes,
            @strong regenerate => move |_| {
                if let Some((_, iter)) = tree_view.selection().selected() {
                    let path = tree_store.path(&iter)
                        .map(|p| p.to_string())
                        .unwrap_or_default();
                    tree_store.remove(&iter);
                    nodes.borrow_mut().retain(|(p, _)| *p != path);
                    regenerate();
                }
            }
        ));

        // Selection → show properties
        tree_view.selection().connect_changed(clone!(
            @strong tree_store, @strong prop_box, @strong env,
            @strong nodes, @strong regenerate, @strong backend => move |sel| {
                for child in prop_box.children() { prop_box.remove(&child); }
                if let Some((_, iter)) = sel.selected() {
                    let kind = tree_store.value(&iter, 0)
                        .get::<String>().unwrap_or_default();
                    let path = tree_store.path(&iter)
                        .map(|p| p.to_string())
                        .unwrap_or_default();
                    let header = gtk::Label::new(None);
                    header.set_markup(&format!("<b>{}</b>", kind));
                    header.set_halign(gtk::Align::Start);
                    prop_box.pack_start(&header, false, false, 5);
                    // Find the node
                    let node = nodes.borrow().iter()
                        .find(|(p, _)| *p == path)
                        .map(|(_, n)| n.clone());
                    if let (Some(env), Some(node)) = (&*env.borrow(), node) {
                        let fn_name = format!("browser/{}", kind);
                        let mod_path = graphix_compiler::expr::ModPath(
                            netidx::path::Path::from(fn_name),
                        );
                        let scope = graphix_compiler::expr::ModPath::root();
                        if let Some((_, bind)) = env.lookup_bind(&scope, &mod_path) {
                            if let Type::Fn(fn_type) = &bind.typ {
                                let grid = gtk::Grid::new();
                                grid.set_column_spacing(8);
                                grid.set_row_spacing(4);
                                build_props_from_fntype(
                                    &grid, fn_type, &node, &regenerate,
                                );
                                prop_box.pack_start(&grid, false, false, 0);
                            }
                        }
                    }
                    prop_box.show_all();
                } else {
                    prop_box.pack_start(
                        &gtk::Label::new(Some("Select a widget to edit properties")),
                        false, false, 0,
                    );
                    prop_box.show_all();
                }
            }
        ));

        Editor { root }
    }

    pub(crate) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
