use super::OnChange;
use glib::{clone, idle_add_local, prelude::*, subclass::prelude::*, GString};
use gtk::{self, prelude::*};
use log::warn;
use netidx::{chars::Chars, path::Path, subscriber::Value};
use netidx_protocols::view;
use std::{
    boxed,
    cell::{Cell, RefCell},
    fmt::Display,
    rc::Rc,
    result,
    str::FromStr,
    string::ToString,
};

struct Constant {
    root: gtk::Box,
    spec: Rc<RefCell<Value>>,
}

impl Constant {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: Value,
    ) {
        let root = gtk::Box::new(gtk::Orientation::Vertical);
        let typ = gtk::ComboBoxText::new();
        
    }
}

struct Variable;
struct Load;
struct Map;

#[derive(Clone, Debug, GBoxed)]
#[gboxed(type_name = "NetidxSourceInspectorProps")]
enum Properties {
    Constant(Constant),
    Load(Load),
    Variable(Variable),
    Map(Map),
}

impl Properties {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Source,
    ) {
        match spec {
            view::Source::Constant(v) => Constant::insert(on_change, store, iter, v),
            view::Source::Load(p) => Load::insert(on_change, store, iter, p),
            view::Source::Variable(v) => Variable::insert(on_change, store, iter, v),
            view::Source::Map { function, from: _ } => {
                Map::insert(on_change, store, iter, function)
            }
        }
    }

    fn spec(&self) -> view::Source {
        match self {
            Properties::Constant(w) => w.spec(),
            Properties::Load(w) => w.spec(),
            Properties::Variable(w) => w.spec(),
            Properties::Map(w) => w.spec(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        match self {
            Properties::Constant(w) => w.root(),
            Properties::Load(w) => w.root(),
            Properties::Variable(w) => w.root(),
            Properties::Map(w) => w.root(),
        }
    }
}

struct SourceInspector {
    root: gtk::Box,
}

static KINDS: [&'static str; 4] = ["Constant", "Load Variable", "Load Path", "Function"];

impl SourceInspector {
    fn new(on_change: impl Fn(view::Source), init: view::Source) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let text = gtk::Entry::new();
        let store = gtk::TreeStore::new(&[
            String::static_type(),
            String::static_type(),
            Properties::static_type(),
        ]);
        let view = gtk::TreeView::new();
        let reveal_properties = gtk::Revealer::new();
        let properties = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let kind = gtk::ComboBoxText::new();
        root.add(&text);
        root.add(&view);
        root.add(&reveal_properties);
        reveal_properties.add(&properties);
        properties.add(&kind);
        properties.add(&gtk::Separator::new(gtk::Orientation::Vertical));
        view.set_model(Some(&store));
        view.set_reorderable(true);
        view.set_enable_tree_lines(true);
        SourceInspector::build_tree(&on_change, &store, None, &init);
        for k in &KINDS {
            kind.append(Some(k), k);
        }
        let selected: Rc<RefCell<Option<gtk::TreeIter>>> = Rc::new(RefCell::new(None));
        let on_change: Rc<Fn()> = Rc::new({
            let store = store.clone();
            move || {
                if let Some(root) = store.get_iter_first() {
                    on_change(SourceInspector::build_source(&store, &root))
                }
            }
        });
        let inhibit: Rc<Cell<bool>> = Rc::new(Cell::new(false));
        kind.connect_changed(clone!(
        @strong on_change,
        @strong store,
        @strong selected,
        @strong inhibit,
        @weak properties => move |c| {
            if !inhibit.get() {
                if let Some(iter) = selected.borrow().clone() {
                    let pv = store.get_value(&iter, 2);
                    if let Ok(Some(p)) = pv.get::<&Properties>() {
                        properties.remove(p.root());
                    }
                    let id = c.get_active_id();
                    let src = SourceInspector::default_source(id.as_ref().map(|s| &**s));
                    Properties::insert(on_change.clone(), &store, &iter, src);
                    let pv = store.get_value(&iter, 2);
                    if let Ok(Some(p)) = pv.get::<&Properties>() {
                        properties.add(p.root());
                    }
                    on_change()
                }
            }
        }));
        let selection = view.get_selection();
        selection.set_mode(gtk::SelectionMode::Single);
        selection.connect_changed(clone!(
            @weak store,
            @strong selected,
            @weak kind,
            @strong inbibit => move |s| {
                let children = properties.get_children();
                if children.len == 3 {
                    properties.remove(&children[2]);
                }
                match s.get_selected() {
                    None => {
                        *selected.borrow_mut() = None;
                        reveal_properties.set_reveal_child(false):
                    }
                    Some((_, iter)) => {
                        *selected.borrow_mut() = Some(iter.clone());
                        let v = store.get_value(&iter, 0);
                        if let Ok(Some(id)) = v.get::<&str>() {
                            inhibit.set(true);
                            kind.set_active_id(Some(id));
                            inhibit.set(false);
                        }
                        let v = store.get_value(&iter, 2);
                        if let Ok(Some(p)) = v.get::<&Properties>() {
                            properties.add(&p.root());
                        }
                        properties.show_all();
                        reveal_properties.set_reveal_child(true):
                    }
                }
        }));
        let menu = gtk::Menu::new();
        let duplicate = gtk::MenuItem::with_label("Duplicate");
        let new_sib = gtk::MenuItem::with_label("New Sibling");
        let new_child = gtk::MenuItem::with_label("New Child");
        let delete = gtk::MenuItem::with_label("Delete");
        menu.append(&duplicate);
        menu.append(&new_sib);
        menu.append(&new_child);
        menu.append(&delete);
        duplicate.connect_activate(clone!(
        @strong on_change, @weak store, @strong selected => move |_| {
            if let Some(iter) = &*selected.borrow() {
                let src = SourceInspector::build_source(&store, iter);
                let parent = store.iter_parent(iter);
                SourceInspector::build_tree(&on_change, &store, parent.as_ref(), &src);
                on_change()
            }
        }));
        new_sib.connect_activate(clone!(
            @strong on_change, @weak store, @strong selected => move |_| {
            let iter = store.insert_after(None, selected.borrow().as_ref());
            let src = SourceInspector::default_source(Some("Constant"));
            Properties::insert(on_change.clone(), &store, &iter, src);
            on_change();
        }));
        new_child.connect_activate(clone!(
            @strong on_change, @weak store, @strong selected  => move |_| {
            let iter = store.insert_after(selected.borrow().as_ref(), None);
            let src = SourceInspector::default_source(Some("Constant"));
            Properties::insert(on_change.clone(), &store, &iter, spec);
            on_change();
        }));
        delete.connect_activate(clone!(
            @weak selection, @strong on_change, @weak store, @strong selected => move |_| {
            let iter = selected.borrow().clone();
            if let Some(iter) = iter {
                selection.unselect_iter(&iter);
                store.remove(&iter);
                on_change();
            }
        }));
        view.connect_button_press_event(move |_, b| {
            let right_click =
                gdk::EventType::ButtonPress == b.get_event_type() && b.get_button() == 3;
            if right_click {
                menu.show_all();
                menu.popup_at_pointer(Some(&*b));
                Inhibit(true)
            } else {
                Inhibit(false)
            }
        });
        store.connect_row_deleted(clone!(@strong on_change => move |_, _| {
            on_change();
        }));
        store.connect_row_inserted(clone!(@strong on_change => move |_, _, _| {
            on_change();
        }));
        SourceInspector { root }
    }

    fn default_source(id: Option<&'static str>) -> view::Source {
        match id {
            Some("Constant") => view::Source::Constant(Value::U64(42)),
            Some("Load Variable") => view::Source::Variable("foo_bar_baz"),
            Some("Load Path") => view::Source::Path(Path::from("/some/path")),
            Some("Function") => {
                let from = vec![view::Source::Constant(Value::U64(42))];
                view::Source::Map { function: "any", from }
            }
        }
    }

    fn build_tree(
        on_change: &OnChange,
        store: &gtk::TreeStore,
        parent: Option<&gtk::TreeIter>,
        s: &view::Source,
    ) {
        let iter = store.insert_before(parent, None);
        Properties::insert(on_change.clone(), store, &iter, s.clone());
        match s {
            view::Source::Constant(_)
            | view::Source::Load(_)
            | view::Source::Variable(_) => (),
            view::Source::Map { from, function: _ } => {
                for s in &from {
                    SourceInspector::build_tree(on_change, store, Some(&iter), s)
                }
            }
        }
    }

    fn build_source(store: &gtk::TreeStore, root: &gtk::TreeIter) -> view::Source {
        let v = store.get_value(root, 2);
        match v.get::<&Properties>() {
            Err(e) => {
                let v = Value::String(Chars::from(format!("tree error: {}", e)));
                view::Source::Constant(v)
            }
            Ok(None) => {
                let v = Value::String(Chars::from("tree error: missing widget"));
                view::Source::Constant(v)
            }
            Ok(Some(p)) => match p.spec() {
                v @ view::Source::Constant(_)
                | v @ view::Source::Load(_)
                | v @ view::Source::Variable(_) => v,
                view::Source::Map { mut from, function } => {
                    from.clear();
                    match store.iter_children(Some(root)) {
                        None => view::Source::Map { from, function },
                        Some(iter) => {
                            loop {
                                from.push(SourceInspector::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                            view::Source::Map { from, function }
                        }
                    }
                }
            },
        }
    }
}
