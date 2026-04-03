mod widgets;

use crate::{
    view, BCtx, WidgetPath,
};
use glib::{clone, prelude::*, ControlFlow};
use gtk::{self, prelude::*};
use netidx::path::Path;
use std::{
    boxed,
    cell::{Cell, RefCell},
    rc::Rc,
};

type OnChange = Rc<dyn Fn()>;

fn widget_kind_name(kind: &view::WidgetKind) -> &'static str {
    match kind {
        view::WidgetKind::BScript(_) => "BScript",
        view::WidgetKind::Table(_) => "Table",
        view::WidgetKind::Label(_) => "Label",
        view::WidgetKind::Button(_) => "Button",
        view::WidgetKind::LinkButton(_) => "LinkButton",
        view::WidgetKind::Switch(_) => "Switch",
        view::WidgetKind::ToggleButton(_) => "ToggleButton",
        view::WidgetKind::CheckButton(_) => "CheckButton",
        view::WidgetKind::RadioButton(_) => "RadioButton",
        view::WidgetKind::ComboBox(_) => "ComboBox",
        view::WidgetKind::Entry(_) => "Entry",
        view::WidgetKind::SearchEntry(_) => "SearchEntry",
        view::WidgetKind::ProgressBar(_) => "ProgressBar",
        view::WidgetKind::Scale(_) => "Scale",
        view::WidgetKind::Image(_) => "Image",
        view::WidgetKind::Frame(_) => "Frame",
        view::WidgetKind::Box(_) => "Box",
        view::WidgetKind::BoxChild(_) => "BoxChild",
        view::WidgetKind::Grid(_) => "Grid",
        view::WidgetKind::GridChild(_) => "GridChild",
        view::WidgetKind::GridRow(_) => "GridRow",
        view::WidgetKind::Paned(_) => "Paned",
        view::WidgetKind::Notebook(_) => "Notebook",
        view::WidgetKind::NotebookPage(_) => "NotebookPage",
        view::WidgetKind::LinePlot(_) => "LinePlot",
    }
}

fn default_widget_for_kind(name: &str) -> view::Widget {
    let kind = match name {
        "BScript" => view::WidgetKind::BScript(view::GxExpr::default()),
        "Table" => view::WidgetKind::Table(view::Table::default()),
        "Label" => view::WidgetKind::Label(view::Label::default()),
        "Button" => view::WidgetKind::Button(view::Button::default()),
        "LinkButton" => view::WidgetKind::LinkButton(view::LinkButton::default()),
        "Switch" => view::WidgetKind::Switch(view::Switch::default()),
        "ToggleButton" => {
            view::WidgetKind::ToggleButton(view::ToggleButton::default())
        }
        "CheckButton" => {
            view::WidgetKind::CheckButton(view::ToggleButton::default())
        }
        "RadioButton" => {
            view::WidgetKind::RadioButton(view::RadioButton::default())
        }
        "ComboBox" => view::WidgetKind::ComboBox(view::ComboBox::default()),
        "Entry" => view::WidgetKind::Entry(view::Entry::default()),
        "SearchEntry" => {
            view::WidgetKind::SearchEntry(view::SearchEntry::default())
        }
        "ProgressBar" => {
            view::WidgetKind::ProgressBar(view::ProgressBar::default())
        }
        "Scale" => view::WidgetKind::Scale(view::Scale::default()),
        "Image" => view::WidgetKind::Image(view::Image::default()),
        "Frame" => view::WidgetKind::Frame(view::Frame::default()),
        "Box" => view::WidgetKind::Box(view::Box::default()),
        "BoxChild" => view::WidgetKind::BoxChild(view::BoxChild::default()),
        "Grid" => view::WidgetKind::Grid(view::Grid::default()),
        "GridChild" => view::WidgetKind::GridChild(view::GridChild::default()),
        "GridRow" => view::WidgetKind::GridRow(view::GridRow::default()),
        "Paned" => view::WidgetKind::Paned(view::Paned::default()),
        "Notebook" => view::WidgetKind::Notebook(view::Notebook::default()),
        "NotebookPage" => {
            view::WidgetKind::NotebookPage(view::NotebookPage::default())
        }
        "LinePlot" => view::WidgetKind::LinePlot(view::LinePlot::default()),
        _ => view::WidgetKind::Label(view::Label::default()),
    };
    view::Widget { props: None, kind }
}

static KINDS: [&str; 25] = [
    "Box",
    "BoxChild",
    "BScript",
    "Button",
    "CheckButton",
    "ComboBox",
    "Entry",
    "Frame",
    "Grid",
    "GridChild",
    "GridRow",
    "Image",
    "Label",
    "LinePlot",
    "LinkButton",
    "Notebook",
    "NotebookPage",
    "Paned",
    "ProgressBar",
    "RadioButton",
    "Scale",
    "SearchEntry",
    "Switch",
    "Table",
    "ToggleButton",
];

/// A node stored in the tree store via glib::Boxed.
/// Contains the property editor widget and the view spec for this node.
#[derive(Clone, Debug, glib::Boxed)]
#[boxed_type(name = "NetidxEditorNode")]
struct EditorNode {
    spec: Rc<RefCell<view::Widget>>,
    prop_root: gtk::Box,
}

impl EditorNode {
    fn new(on_change: &OnChange, spec: view::Widget) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let prop_root = gtk::Box::new(gtk::Orientation::Vertical, 2);
        Self::rebuild_props(&prop_root, &spec, on_change);
        EditorNode { spec, prop_root }
    }

    fn rebuild_props(
        prop_root: &gtk::Box,
        spec: &Rc<RefCell<view::Widget>>,
        on_change: &OnChange,
    ) {
        for child in prop_root.children() {
            prop_root.remove(&child);
        }
        let s = spec.borrow();
        let kind_label = gtk::Label::new(Some(&format!(
            "<b>{}</b>",
            widget_kind_name(&s.kind),
        )));
        kind_label.set_use_markup(true);
        kind_label.set_halign(gtk::Align::Start);
        prop_root.pack_start(&kind_label, false, false, 2);
        prop_root.pack_start(
            &gtk::Separator::new(gtk::Orientation::Horizontal),
            false, false, 2,
        );
        // Common properties
        let common_exp = gtk::Expander::new(Some("Common Properties"));
        let common_grid = gtk::Grid::new();
        common_grid.set_column_spacing(4);
        common_grid.set_row_spacing(2);
        common_exp.add(&common_grid);
        widgets::build_common_props(
            &common_grid,
            &spec,
            on_change,
        );
        prop_root.pack_start(&common_exp, false, false, 2);
        // Widget-specific properties
        let specific_exp = gtk::Expander::new(Some("Widget Properties"));
        specific_exp.set_expanded(true);
        let specific_grid = gtk::Grid::new();
        specific_grid.set_column_spacing(4);
        specific_grid.set_row_spacing(2);
        specific_exp.add(&specific_grid);
        widgets::build_kind_props(
            &specific_grid,
            &spec,
            on_change,
        );
        prop_root.pack_start(&specific_exp, false, false, 2);
        prop_root.show_all();
    }

    fn widget_spec(&self) -> view::Widget {
        self.spec.borrow().clone()
    }
}

pub(crate) struct Editor {
    root: gtk::Paned,
}

impl Editor {
    pub(crate) fn new(ctx: &BCtx, _scope: Path, spec: view::Widget) -> Editor {
        let root = gtk::Paned::new(gtk::Orientation::Vertical);
        glib::idle_add_local(
            clone!(@weak root => @default-return ControlFlow::Break, move || {
                root.set_position(root.allocated_height() / 2);
                ControlFlow::Break
            }),
        );
        root.set_margin_start(5);
        root.set_margin_end(5);
        // Upper: tree view with toolbar
        let upper = gtk::Box::new(gtk::Orientation::Vertical, 5);
        // Lower: property editor in scrolled window
        let lower_win =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        lower_win.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        let lower = gtk::Box::new(gtk::Orientation::Vertical, 5);
        lower_win.add(&lower);
        root.pack1(&upper, true, false);
        root.pack2(&lower_win, true, true);
        // Toolbar
        let toolbar = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        upper.pack_start(&toolbar, false, false, 0);
        let add_icon = gtk::Image::from_icon_name(
            Some("list-add-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let add_btn = gtk::ToolButton::new(Some(&add_icon), Some("Add"));
        let add_child_icon = gtk::Image::from_icon_name(
            Some("go-down-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let add_child_btn = gtk::ToolButton::new(Some(&add_child_icon), Some("Child"));
        let del_icon = gtk::Image::from_icon_name(
            Some("list-remove-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let del_btn = gtk::ToolButton::new(Some(&del_icon), Some("Del"));
        let dup_icon = gtk::Image::from_icon_name(
            Some("edit-copy-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let dup_btn = gtk::ToolButton::new(Some(&dup_icon), Some("Dup"));
        let undo_icon = gtk::Image::from_icon_name(
            Some("edit-undo-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let undo_btn = gtk::ToolButton::new(Some(&undo_icon), Some("Undo"));
        toolbar.pack_start(&add_btn, false, false, 5);
        toolbar.pack_start(&add_child_btn, false, false, 5);
        toolbar.pack_start(&del_btn, false, false, 5);
        toolbar.pack_start(&dup_btn, false, false, 5);
        toolbar.pack_start(&undo_btn, false, false, 5);
        // Tree view
        let tree_win =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        tree_win.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        upper.pack_start(&tree_win, true, true, 5);
        let tree_view = gtk::TreeView::new();
        tree_win.add(&tree_view);
        tree_view.append_column(&{
            let col = gtk::TreeViewColumn::new();
            let cell = gtk::CellRendererText::new();
            gtk::prelude::CellLayoutExt::pack_start(&col, &cell, true);
            col.set_title("Widget");
            gtk::prelude::CellLayoutExt::add_attribute(&col, &cell, "text", 0);
            col
        });
        // TreeStore columns: 0=name(String), 1=EditorNode(Boxed)
        let store = gtk::TreeStore::new(&[
            String::static_type(),
            EditorNode::static_type(),
        ]);
        tree_view.set_model(Some(&store));
        tree_view.set_reorderable(true);
        tree_view.set_enable_tree_lines(true);
        // State
        let spec_rc = Rc::new(RefCell::new(spec.clone()));
        let undo_stack: Rc<RefCell<Vec<view::Widget>>> =
            Rc::new(RefCell::new(Vec::new()));
        let undoing = Rc::new(Cell::new(false));
        let selected: Rc<RefCell<Option<gtk::TreeIter>>> =
            Rc::new(RefCell::new(None));
        let properties = gtk::Box::new(gtk::Orientation::Vertical, 5);
        lower.pack_start(&properties, true, true, 5);
        let ctx_clone = ctx.clone();
        let on_change: OnChange = Rc::new({
            let spec_rc = spec_rc.clone();
            let store = store.clone();
            let scheduled = Rc::new(Cell::new(false));
            let undo_stack = undo_stack.clone();
            let undoing = undoing.clone();
            let ctx = ctx_clone.clone();
            move || {
                if !scheduled.get() {
                    scheduled.set(true);
                    glib::idle_add_local(clone!(
                        @strong ctx,
                        @strong spec_rc,
                        @strong store,
                        @strong scheduled,
                        @strong undo_stack,
                        @strong undoing => move || {
                            if let Some(root_iter) = store.iter_first() {
                                if undoing.get() {
                                    undoing.set(false);
                                } else {
                                    undo_stack.borrow_mut()
                                        .push(spec_rc.borrow().clone());
                                }
                                let new_spec =
                                    build_spec(&store, &root_iter);
                                *spec_rc.borrow_mut() = new_spec.clone();
                                ctx.borrow().backend.render(new_spec);
                            }
                            scheduled.set(false);
                            ControlFlow::Break
                    }));
                }
            }
        });
        // Build the tree from the initial spec
        build_tree(&on_change, &store, None, &spec);
        // Expand all nodes initially
        tree_view.expand_all();
        // Kind selector for changing widget type
        let kind_combo = gtk::ComboBoxText::new();
        for k in &KINDS {
            kind_combo.append(Some(k), k);
        }
        let inhibit_kind_change = Rc::new(Cell::new(false));
        lower.pack_start(&kind_combo, false, false, 0);
        lower.reorder_child(&kind_combo, 0);
        // Selection handling
        tree_view.selection().connect_changed(clone!(
            @strong store,
            @strong selected,
            @weak properties,
            @strong kind_combo,
            @strong inhibit_kind_change => move |sel| {
                // Remove old property editor
                for child in properties.children() {
                    properties.remove(&child);
                }
                if let Some((_, iter)) = sel.selected() {
                    *selected.borrow_mut() = Some(iter.clone());
                    let v = store.value(&iter, 1);
                    if let Ok(node) = v.get::<EditorNode>() {
                        properties.pack_start(&node.prop_root, true, true, 0);
                        inhibit_kind_change.set(true);
                        let name = widget_kind_name(
                            &node.spec.borrow().kind
                        );
                        kind_combo.set_active_id(Some(name));
                        inhibit_kind_change.set(false);
                    }
                    properties.show_all();
                } else {
                    *selected.borrow_mut() = None;
                }
            }
        ));
        // Kind change handler
        kind_combo.connect_changed(clone!(
            @strong on_change,
            @strong store,
            @strong selected,
            @weak properties,
            @strong inhibit_kind_change => move |c| {
                if inhibit_kind_change.get() {
                    return;
                }
                if let Some(iter) = selected.borrow().clone() {
                    if let Some(name) = c.active_id() {
                        let new_widget = default_widget_for_kind(&name);
                        let node = EditorNode::new(&on_change, new_widget);
                        // Remove old property editor
                        for child in properties.children() {
                            properties.remove(&child);
                        }
                        let kind_name: String = name.into();
                        store.set(
                            &iter,
                            &[(0, &kind_name), (1, &node)],
                        );
                        properties.pack_start(&node.prop_root, true, true, 0);
                        properties.show_all();
                        on_change();
                    }
                }
            }
        ));
        // Add sibling
        add_btn.connect_clicked(clone!(
            @strong on_change,
            @strong store,
            @strong selected => move |_| {
                let new = default_widget_for_kind("Label");
                let node = EditorNode::new(&on_change, new);
                let parent = selected.borrow().as_ref()
                    .and_then(|i| store.iter_parent(i));
                let iter = store.insert_with_values(
                    parent.as_ref(), None,
                    &[(0, &String::from("Label")), (1, &node)],
                );
                let _ = iter;
                on_change();
            }
        ));
        // Add child
        add_child_btn.connect_clicked(clone!(
            @strong on_change,
            @strong store,
            @strong selected => move |_| {
                let new = default_widget_for_kind("Label");
                let node = EditorNode::new(&on_change, new);
                let parent = selected.borrow().clone();
                let iter = store.insert_with_values(
                    parent.as_ref(), None,
                    &[(0, &String::from("Label")), (1, &node)],
                );
                let _ = iter;
                on_change();
            }
        ));
        // Delete
        del_btn.connect_clicked(clone!(
            @strong on_change,
            @strong store,
            @strong selected,
            @weak properties => move |_| {
                if let Some(iter) = selected.borrow_mut().take() {
                    for child in properties.children() {
                        properties.remove(&child);
                    }
                    store.remove(&iter);
                    on_change();
                }
            }
        ));
        // Duplicate
        dup_btn.connect_clicked(clone!(
            @strong on_change,
            @strong store,
            @strong selected => move |_| {
                if let Some(iter) = selected.borrow().clone() {
                    let parent = store.iter_parent(&iter);
                    let v = store.value(&iter, 1);
                    if let Ok(node) = v.get::<EditorNode>() {
                        let spec = node.widget_spec();
                        let name = widget_kind_name(&spec.kind);
                        // Deep copy including children
                        let dup_node = EditorNode::new(&on_change, spec.clone());
                        let new_iter = store.insert_with_values(
                            parent.as_ref(), None,
                            &[(0, &String::from(name)), (1, &dup_node)],
                        );
                        // Also duplicate child tree
                        copy_children(
                            &on_change, &store, &iter, &new_iter,
                        );
                        on_change();
                    }
                }
            }
        ));
        // Undo
        undo_btn.connect_clicked(clone!(
            @strong on_change,
            @strong store,
            @strong spec_rc,
            @strong undo_stack,
            @strong undoing,
            @weak properties,
            @weak tree_view => move |_| {
                if let Some(prev) = undo_stack.borrow_mut().pop() {
                    undoing.set(true);
                    for child in properties.children() {
                        properties.remove(&child);
                    }
                    store.clear();
                    build_tree(&on_change, &store, None, &prev);
                    tree_view.expand_all();
                    *spec_rc.borrow_mut() = prev.clone();
                    on_change();
                }
            }
        ));
        // Build widget path on selection for highlighting
        tree_view.selection().connect_changed(clone!(
            @strong store,
            @weak ctx_clone => move |sel| {
                if let Some((_, iter)) = sel.selected() {
                    let mut path = Vec::new();
                    build_widget_path(&store, &iter, 0, 0, &mut path);
                    let _: result::Result<_, _> =
                        ctx_clone.borrow().backend.to_gui
                            .send(crate::ToGui::Highlight(path));
                }
            }
        ));
        Editor { root }
    }

    pub(crate) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

use std::result;

fn build_tree(
    on_change: &OnChange,
    store: &gtk::TreeStore,
    parent: Option<&gtk::TreeIter>,
    spec: &view::Widget,
) {
    let name = widget_kind_name(&spec.kind);
    let node = EditorNode::new(on_change, spec.clone());
    let iter = store.insert_with_values(
        parent, None,
        &[(0, &String::from(name)), (1, &node)],
    );
    // Recurse into container children
    match &spec.kind {
        view::WidgetKind::Frame(f) => {
            if let Some(child) = &f.child {
                build_tree(on_change, store, Some(&iter), child);
            }
        }
        view::WidgetKind::Box(b) => {
            for child in &b.children {
                build_tree(on_change, store, Some(&iter), child);
            }
        }
        view::WidgetKind::BoxChild(b) => {
            build_tree(on_change, store, Some(&iter), &*b.widget);
        }
        view::WidgetKind::Grid(g) => {
            for row in &g.rows {
                build_tree(on_change, store, Some(&iter), row);
            }
        }
        view::WidgetKind::GridChild(g) => {
            build_tree(on_change, store, Some(&iter), &*g.widget);
        }
        view::WidgetKind::GridRow(g) => {
            for col in &g.columns {
                build_tree(on_change, store, Some(&iter), col);
            }
        }
        view::WidgetKind::Paned(p) => {
            if let Some(child) = &p.first_child {
                build_tree(on_change, store, Some(&iter), child);
            }
            if let Some(child) = &p.second_child {
                build_tree(on_change, store, Some(&iter), child);
            }
        }
        view::WidgetKind::Notebook(n) => {
            for child in &n.children {
                build_tree(on_change, store, Some(&iter), child);
            }
        }
        view::WidgetKind::NotebookPage(p) => {
            build_tree(on_change, store, Some(&iter), &*p.widget);
        }
        view::WidgetKind::BScript(_)
        | view::WidgetKind::Table(_)
        | view::WidgetKind::Image(_)
        | view::WidgetKind::Label(_)
        | view::WidgetKind::Button(_)
        | view::WidgetKind::LinkButton(_)
        | view::WidgetKind::ToggleButton(_)
        | view::WidgetKind::CheckButton(_)
        | view::WidgetKind::RadioButton(_)
        | view::WidgetKind::Switch(_)
        | view::WidgetKind::ComboBox(_)
        | view::WidgetKind::Scale(_)
        | view::WidgetKind::ProgressBar(_)
        | view::WidgetKind::Entry(_)
        | view::WidgetKind::SearchEntry(_)
        | view::WidgetKind::LinePlot(_) => {}
    }
}

fn build_spec(store: &gtk::TreeStore, iter: &gtk::TreeIter) -> view::Widget {
    let v = store.value(iter, 1);
    let node = match v.get::<EditorNode>() {
        Ok(n) => n,
        Err(_) => return view::Widget::default(),
    };
    let mut spec = node.widget_spec();
    match &mut spec.kind {
        view::WidgetKind::Frame(f) => {
            f.child = None;
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                f.child = Some(boxed::Box::new(build_spec(store, &child_iter)));
            }
        }
        view::WidgetKind::Paned(p) => {
            p.first_child = None;
            p.second_child = None;
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                p.first_child =
                    Some(boxed::Box::new(build_spec(store, &child_iter)));
                if store.iter_next(&child_iter) {
                    p.second_child =
                        Some(boxed::Box::new(build_spec(store, &child_iter)));
                }
            }
        }
        view::WidgetKind::Box(b) => {
            b.children.clear();
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                loop {
                    b.children.push(build_spec(store, &child_iter));
                    if !store.iter_next(&child_iter) {
                        break;
                    }
                }
            }
        }
        view::WidgetKind::BoxChild(b) => {
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                b.widget = boxed::Box::new(build_spec(store, &child_iter));
            }
        }
        view::WidgetKind::Grid(g) => {
            g.rows.clear();
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                loop {
                    g.rows.push(build_spec(store, &child_iter));
                    if !store.iter_next(&child_iter) {
                        break;
                    }
                }
            }
        }
        view::WidgetKind::GridChild(g) => {
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                g.widget = boxed::Box::new(build_spec(store, &child_iter));
            }
        }
        view::WidgetKind::GridRow(g) => {
            g.columns.clear();
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                loop {
                    g.columns.push(build_spec(store, &child_iter));
                    if !store.iter_next(&child_iter) {
                        break;
                    }
                }
            }
        }
        view::WidgetKind::Notebook(n) => {
            n.children.clear();
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                loop {
                    n.children.push(build_spec(store, &child_iter));
                    if !store.iter_next(&child_iter) {
                        break;
                    }
                }
            }
        }
        view::WidgetKind::NotebookPage(p) => {
            if let Some(child_iter) = store.iter_children(Some(iter)) {
                p.widget = boxed::Box::new(build_spec(store, &child_iter));
            }
        }
        view::WidgetKind::BScript(_)
        | view::WidgetKind::Table(_)
        | view::WidgetKind::Image(_)
        | view::WidgetKind::Label(_)
        | view::WidgetKind::Button(_)
        | view::WidgetKind::LinkButton(_)
        | view::WidgetKind::ToggleButton(_)
        | view::WidgetKind::CheckButton(_)
        | view::WidgetKind::RadioButton(_)
        | view::WidgetKind::Switch(_)
        | view::WidgetKind::ComboBox(_)
        | view::WidgetKind::Scale(_)
        | view::WidgetKind::ProgressBar(_)
        | view::WidgetKind::Entry(_)
        | view::WidgetKind::SearchEntry(_)
        | view::WidgetKind::LinePlot(_) => {}
    }
    spec
}

fn copy_children(
    on_change: &OnChange,
    store: &gtk::TreeStore,
    src: &gtk::TreeIter,
    dst: &gtk::TreeIter,
) {
    if let Some(child) = store.iter_children(Some(src)) {
        loop {
            let v = store.value(&child, 1);
            if let Ok(node) = v.get::<EditorNode>() {
                let spec = node.widget_spec();
                let name = widget_kind_name(&spec.kind);
                let new_node = EditorNode::new(on_change, spec);
                let new_child = store.insert_with_values(
                    Some(dst), None,
                    &[(0, &String::from(name)), (1, &new_node)],
                );
                copy_children(on_change, store, &child, &new_child);
            }
            if !store.iter_next(&child) {
                break;
            }
        }
    }
}

fn build_widget_path(
    store: &gtk::TreeStore,
    start: &gtk::TreeIter,
    mut nrow: usize,
    nchild: usize,
    path: &mut Vec<WidgetPath>,
) {
    let v = store.value(start, 1);
    let skip_idx = match v.get::<EditorNode>() {
        Err(_) => false,
        Ok(node) => {
            let s = node.spec.borrow();
            match &s.kind {
                view::WidgetKind::BScript(_)
                | view::WidgetKind::Table(_)
                | view::WidgetKind::Image(_)
                | view::WidgetKind::Label(_)
                | view::WidgetKind::Button(_)
                | view::WidgetKind::LinkButton(_)
                | view::WidgetKind::ToggleButton(_)
                | view::WidgetKind::CheckButton(_)
                | view::WidgetKind::RadioButton(_)
                | view::WidgetKind::Switch(_)
                | view::WidgetKind::ComboBox(_)
                | view::WidgetKind::Scale(_)
                | view::WidgetKind::ProgressBar(_)
                | view::WidgetKind::Entry(_)
                | view::WidgetKind::SearchEntry(_)
                | view::WidgetKind::LinePlot(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                view::WidgetKind::Frame(_)
                | view::WidgetKind::Box(_)
                | view::WidgetKind::Notebook(_)
                | view::WidgetKind::Paned(_) => {
                    if path.is_empty() {
                        path.insert(0, WidgetPath::Leaf);
                    } else {
                        path.insert(0, WidgetPath::Box(nchild));
                    }
                    false
                }
                view::WidgetKind::NotebookPage(_)
                | view::WidgetKind::BoxChild(_) => {
                    if path.is_empty() {
                        path.insert(0, WidgetPath::Leaf);
                    }
                    false
                }
                view::WidgetKind::Grid(_) => {
                    if path.is_empty() {
                        path.insert(0, WidgetPath::Leaf);
                    } else {
                        match path[0] {
                            WidgetPath::GridRow(_) => (),
                            WidgetPath::GridItem(_, _) => (),
                            _ => path.insert(
                                0, WidgetPath::GridItem(nrow, nchild),
                            ),
                        }
                    }
                    false
                }
                view::WidgetKind::GridChild(_) => {
                    if path.is_empty() {
                        path.insert(0, WidgetPath::Leaf);
                    }
                    false
                }
                view::WidgetKind::GridRow(_) => {
                    if let Some(tp) = store.path(start) {
                        if let Some(i) = tp.indices().last() {
                            nrow = *i as usize;
                        }
                    }
                    if path.is_empty() {
                        path.insert(0, WidgetPath::GridRow(nrow));
                    } else {
                        path.insert(
                            0, WidgetPath::GridItem(nrow, nchild),
                        );
                    }
                    true
                }
            }
        }
    };
    if let Some(parent) = store.iter_parent(start) {
        if let Some(tp) = store.path(start) {
            if let Some(i) = tp.indices().last() {
                let nchild = if skip_idx { nchild } else { *i as usize };
                build_widget_path(store, &parent, nrow, nchild, path);
            }
        }
    }
}
