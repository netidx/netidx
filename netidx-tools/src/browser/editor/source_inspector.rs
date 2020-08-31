use super::super::formula;
use super::{util::TwoColGrid, OnChange};
use glib::{clone, prelude::*, subclass::prelude::*, idle_add_local};
use gtk::{self, prelude::*};
use netidx::{chars::Chars, path::Path, subscriber::Value};
use netidx_protocols::{
    value_type::{Typ, TYPES},
    view,
};
use std::{
    boxed,
    cell::{Cell, RefCell},
    fmt::Display,
    rc::Rc,
    result,
};

#[derive(Clone, Debug)]
struct Constant {
    root: TwoColGrid,
    spec: Rc<RefCell<Value>>,
}

impl Constant {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: Value,
    ) {
        let spec = Rc::new(RefCell::new(spec));
        let mut root = TwoColGrid::new();
        let typlbl = gtk::Label::new(Some("Type: "));
        let typsel = gtk::ComboBoxText::new();
        let vallbl = gtk::Label::new(Some("Value: "));
        let valent = gtk::Entry::new();
        root.add((typlbl.clone(), typsel.clone()));
        root.add((vallbl.clone(), valent.clone()));
        for typ in &TYPES {
            let name = typ.name();
            typsel.append(Some(name), name);
        }
        typsel.set_active_id(Typ::get(&*spec.borrow()).map(|t| t.name()));
        valent.set_text(&format!("{}", &*spec.borrow()));
        let val_change = Rc::new(clone!(
            @strong on_change,
            @strong spec,
            @strong iter,
            @strong store,
            @weak typsel,
            @weak valent,
            @weak store => move || {
            if let Some(Ok(typ)) = typsel.get_active_id().map(|s| s.parse::<Typ>()) {
                match typ.parse(&*valent.get_text()) {
                    Err(e) => store.set_value(&iter, 1, &format!("{}", e).to_value()),
                    Ok(value) => {
                        store.set_value(&iter, 1, &format!("{}", value).to_value());
                        *spec.borrow_mut() = value;
                        on_change()
                    }
                }
            }
        }));
        typsel.connect_changed(clone!(@strong val_change => move |_| val_change()));
        valent.connect_activate(clone!(@strong val_change => move |_| val_change()));
        store.set_value(iter, 0, &"Constant".to_value());
        store.set_value(iter, 1, &format!("{}", &*spec.borrow()).to_value());
        store.set_value(
            iter,
            2,
            &Properties::Constant(Constant { root, spec }).to_value(),
        );
    }

    fn spec(&self) -> view::Source {
        view::Source::Constant(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Variable {
    root: gtk::Box,
    spec: Rc<RefCell<String>>,
}

impl Variable {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: String,
    ) {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let lblvar = gtk::Label::new(Some("Name:"));
        let entvar = gtk::Entry::new();
        root.add(&lblvar);
        root.add(&entvar);
        entvar.set_text(&*spec.borrow());
        entvar.connect_activate(clone!(
        @strong on_change, @strong spec, @strong iter, @weak store => move |e|
        match format!("v:{}", &*e.get_text()).parse::<view::Source>() {
            Err(e) => store.set_value(&iter, 1, &format!("{}", e).to_value()),
            Ok(view::Source::Variable(name)) => {
                *spec.borrow_mut() = name;
                store.set_value(&iter, 1, &"".to_value());
                on_change()
            }
            Ok(_) => unreachable!()
        }));
        store.set_value(iter, 0, &"Load Variable".to_value());
        store.set_value(iter, 1, &"".to_value());
        store.set_value(
            iter,
            2,
            &Properties::Variable(Variable { root, spec }).to_value(),
        );
    }

    fn spec(&self) -> view::Source {
        view::Source::Variable(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Load {
    root: gtk::Box,
    spec: Rc<RefCell<Path>>,
}

impl Load {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: Path,
    ) {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let lblvar = gtk::Label::new(Some("Path:"));
        let entvar = gtk::Entry::new();
        root.add(&lblvar);
        root.add(&entvar);
        entvar.set_text(&**spec.borrow());
        entvar.connect_activate(clone!(
        @strong on_change, @strong spec, @strong iter, @weak store => move |e| {
            let path = Path::from(String::from(&*e.get_text()));
            if !Path::is_absolute(&*path) {
                store.set_value(&iter, 1, &"Absolute path required".to_value());
            } else {
                *spec.borrow_mut() = path;
                store.set_value(&iter, 1, &"".to_value());
                on_change()
            }
        }));
        store.set_value(iter, 0, &"Load Path".to_value());
        store.set_value(iter, 1, &"".to_value());
        store.set_value(iter, 2, &Properties::Load(Load { root, spec }).to_value());
    }

    fn spec(&self) -> view::Source {
        view::Source::Load(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Map {
    root: gtk::Box,
    spec: Rc<RefCell<String>>,
}

impl Map {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: String,
    ) {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let lblfun = gtk::Label::new(Some("Function:"));
        let cbfun = gtk::ComboBoxText::new();
        root.add(&lblfun);
        root.add(&cbfun);
        for name in &formula::FORMULAS {
            cbfun.append(Some(name), name);
        }
        cbfun.set_active_id(Some(spec.as_str()));
        let spec = Rc::new(RefCell::new(spec));
        cbfun.connect_changed(clone!(
            @strong on_change, @strong spec, @strong store, @strong iter => move |c| {
            if let Some(id) = c.get_active_id() {
                store.set_value(&iter, 0, &id.to_value());
                *spec.borrow_mut() = String::from(&*id);
                on_change()
            }
        }));
        store.set_value(iter, 0, &spec.borrow().to_value());
        store.set_value(iter, 1, &"".to_value());
        store.set_value(iter, 2, &Properties::Map(Map { root, spec }).to_value());
    }

    fn spec(&self) -> view::Source {
        view::Source::Map { function: self.spec.borrow().clone(), from: vec![] }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

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

static KINDS: [&'static str; 4] = ["Constant", "Load Variable", "Load Path", "Function"];

fn default_source(id: Option<&str>) -> view::Source {
    match id {
        None => view::Source::Constant(Value::U64(42)),
        Some("Constant") => view::Source::Constant(Value::U64(42)),
        Some("Load Variable") => view::Source::Variable("foo_bar_baz".into()),
        Some("Load Path") => view::Source::Load(Path::from("/some/path")),
        Some("Function") => {
            let from = vec![view::Source::Constant(Value::U64(42))];
            view::Source::Map { function: "any".into(), from }
        }
        _ => unreachable!(),
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
        view::Source::Constant(_) | view::Source::Load(_) | view::Source::Variable(_) => {
            ()
        }
        view::Source::Map { from, function: _ } => {
            for s in from {
                build_tree(on_change, store, Some(&iter), s)
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
                            from.push(build_source(store, &iter));
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

pub(super) fn source_inspector(
    on_change: impl Fn(view::Source) + 'static,
    init: view::Source,
) -> gtk::Box {
    let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
    let store = gtk::TreeStore::new(&[
        String::static_type(),
        String::static_type(),
        Properties::static_type(),
    ]);
    let view = gtk::TreeView::new();
    let treewin =
        gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
    treewin.set_policy(gtk::PolicyType::Never, gtk::PolicyType::Automatic);
    let reveal_properties = gtk::Revealer::new();
    let properties = gtk::Box::new(gtk::Orientation::Vertical, 5);
    let kind = gtk::ComboBoxText::new();
    treewin.add(&view);
    for (i, name) in ["kind", "current"].iter().enumerate() {
        view.append_column(&{
            let column = gtk::TreeViewColumn::new();
            let cell = gtk::CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title(name);
            column.add_attribute(&cell, "text", i as i32);
            column
        });
    }
    root.pack_start(&treewin, true, true, 5);
    root.pack_end(&reveal_properties, false, false, 5);
    reveal_properties.add(&properties);
    properties.pack_start(&kind, false, false, 5);
    properties.pack_start(
        &gtk::Separator::new(gtk::Orientation::Vertical),
        false,
        false,
        5,
    );
    view.set_model(Some(&store));
    view.set_reorderable(true);
    view.set_enable_tree_lines(true);
    for k in &KINDS {
        kind.append(Some(k), k);
    }
    let selected: Rc<RefCell<Option<gtk::TreeIter>>> = Rc::new(RefCell::new(None));
    let inhibit: Rc<Cell<bool>> = Rc::new(Cell::new(false));
    let on_change: Rc<dyn Fn()> = Rc::new({
        let store = store.clone();
        let inhibit = inhibit.clone();
        let scheduled = Rc::new(Cell::new(false));
        let on_change = Rc::new(on_change);
        move || {
            if !scheduled.get() {
                scheduled.set(true);
                idle_add_local(clone!(
                    @strong store,
                    @strong inhibit,
                    @strong scheduled,
                    @strong on_change => move || {
                        if let Some(root) = store.get_iter_first() {
                            let src = build_source(&store, &root);
                            on_change(src)
                        }
                        scheduled.set(false);
                        glib::Continue(false)
                    }
                ));
            }
        }
    });
    build_tree(&on_change, &store, None, &init);
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
                let src = default_source(id.as_ref().map(|s| &**s));
                Properties::insert(on_change.clone(), &store, &iter, src);
                let pv = store.get_value(&iter, 2);
                if let Ok(Some(p)) = pv.get::<&Properties>() {
                    properties.add(p.root());
                    properties.show_all();
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
    @strong inhibit => move |s| {
        let children = properties.get_children();
        if children.len() == 3 {
            properties.remove(&children[2]);
        }
        match s.get_selected() {
            None => {
                *selected.borrow_mut() = None;
                reveal_properties.set_reveal_child(false);
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
                    properties.add(p.root());
                }
                properties.show_all();
                reveal_properties.set_reveal_child(true);
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
            let src = build_source(&store, iter);
            let parent = store.iter_parent(iter);
            build_tree(&on_change, &store, parent.as_ref(), &src);
            on_change()
        }
    }));
    new_sib.connect_activate(clone!(
    @strong on_change, @weak store, @strong selected => move |_| {
        let iter = store.insert_after(None, selected.borrow().as_ref());
        let src = default_source(Some("Constant"));
        Properties::insert(on_change.clone(), &store, &iter, src);
        on_change();
    }));
    new_child.connect_activate(clone!(
    @strong on_change, @weak store, @strong selected  => move |_| {
        let iter = store.insert_after(selected.borrow().as_ref(), None);
        let src = default_source(Some("Constant"));
        Properties::insert(on_change.clone(), &store, &iter, src);
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
    root
}
