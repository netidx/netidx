use super::{
    util::{parse_entry, TwoColGrid},
    OnChange,
};
use glib::{clone, idle_add_local, prelude::*, subclass::prelude::*, GString};
use gtk::{self, prelude::*};
use log::warn;
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
        let root = TwoColGrid::new();
        let typlbl = gtk::Label::new(Some("Type: "));
        let typsel = gtk::ComboBoxText::new();
        let vallbl = gtk::Label::new(Some("Value: "));
        let valent = gtk::Entry::new();
        let lblerr = gtk::Label::new(None);
        root.add((&typlbl, &typ));
        root.add((&vallbl, &valent));
        root.attach(&lblerr, 0, 2, 1);
        for typ in &TYPES {
            let name = typ.name();
            typsel.add(Some(name), name);
        }
        typsel.set_active_id(Typ::get(&*spec.borrow()).map(|t| t.name()));
        let val_change = Rc::new(clone!(
            @strong on_change,
            @strong spec,
            @weak lblerr,
            @weak iter,
            @weak typsel,
            @weak store => move || {
            if let Some(Ok(typ)) = typsel.get_active_id().parse::<Typ>() {
                match typ.parse(&*valent.get_text()) {
                    Err(e) => lblerr.set_text(&format!("{}", e)),
                    Ok(value) => {
                        lblerr.set_text("");
                        *spec.borrow_mut() = value;
                        store.set_value(&iter, 1, &format!("{}", value).to_value());
                        on_change()
                    }
                }
            }
        }));
        typsel.connect_changed(clone!(@strong val_change => move |_| val_change()));
        valent.connect_activate(clone!(@strong val_change => move |_| val_change()));
        let t = Properties::Constant(Constant { root, spec });
        store.set_value(iter, 0, &"Constant".to_value());
        store.set_value(iter, 1, &format!("{}", &*spec.borrow()).to_value());
        store.set_value(iter, 2, &t.to_value());
    }

    fn spec(&self) -> view::Source {
        view::Source::Constant(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

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
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let entbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let lblvar = gtk::Label::new(Some("Name:"));
        let entvar = gtk::Entry::new();
        let lblerr = gtk::Label::new(None);
        root.add(&entbox);
        entbox.add(&lblvar);
        entbox.add(&entvar);
        root.add(&lblerr);
        entvar.set_text(&*spec.borrow());
        entvar.connect_activate(clone!(
        @strong on_change, @strong spec, @weak lblerr => move |e|
        match format!("v:{}", &*e.get_text()).parse::<view::Source>() {
            Err(e) => lblerr.set_text(&format!("{}", e)),
            Ok(view::Source::Variable(name)) => {
                *spec.borrow_mut() = name;
                lblerr.set_text("");
                on_change()
            }
            Ok(_) => unreachable!()
        }));
        let t = Properties::Variable(Variable { root, spec });
        store.set_value(iter, 0, &"Load Variable".to_value());
        store.set_value(iter, 1, &"".to_value());
        store.set_value(iter, 2, &t.to_value());
    }

    fn spec(&self) -> view::Source {
        view::Source::Variable(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

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
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let entbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let lblvar = gtk::Label::new(Some("Path:"));
        let entvar = gtk::Entry::new();
        let lblerr = gtk::Label::new(None);
        root.add(&entbox);
        entbox.add(&lblvar);
        entbox.add(&entvar);
        root.add(&lblerr);
        entvar.set_text(&**spec.borrow());
        entvar.connect_activate(clone!(
        @strong on_change, @strong spec, @weak lblerr => move |e| {
            let path = Path::from(String::from(&*e.get_text()));
            if !Path::is_absolute(&*path) {
                lblerr.set_text("Absolute path is required (must start with /)");
            } else {
                *spec.borrow_mut() = path;
                lblerr.set_text("");
                on_change()
            }
        }));
        let t = Properties::Load(Load { root, spec });
        store.set_value(iter, 0, &"Load Path".to_value());
        store.set_value(iter, 1, &"".to_value());
        store.set_value(iter, 2, &t.to_value());
    }

    fn spec(&self) -> view::Source {
        view::Source::Load(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

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
