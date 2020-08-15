use super::FromGui;
use futures::channel::mpsc;
use glib::{clone, GString};
use gtk::{self, prelude::*};
use log::warn;
use netidx::{chars::Chars, path::Path, subscriber::Value};
use netidx_protocols::view;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    result,
};

type OnChange = Rc<dyn Fn()>;

struct KindWrap {
    root: gtk::Box,
    spec: Rc<RefCell<view::Widget>>,
    store: gtk::TreeStore,
    iter: gtk::TreeIter,
}

impl KindWrap {
    fn new(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Widget,
    ) -> KindWrap {
        let kinds =
            ["Table", "Label", "Button", "Toggle", "Selector", "Entry", "Box", "Grid"];
        let kind = gtk::ComboBoxText::new();
        for k in &kinds {
            kind.append(Some(k), k);
        }
        kind.set_active_id(Some(match spec {
            view::Widget::Table(_) => "Table",
            view::Widget::Label(_) => "Label",
            view::Widget::Button(_) => "Button",
            view::Widget::Toggle(_) => "Toggle",
            view::Widget::Selector(_) => "Selector",
            view::Widget::Entry(_) => "Entry",
            view::Widget::Box(_) => "Box",
            view::Widget::Grid(_) => "Grid",
        }));
        Widget::insert(on_change.clone(), store, iter, spec.clone());
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.add(&kind);
        let wv = store.get_value(iter, 1);
        if let Ok(Some(w)) = wv.get::<&Widget>() {
            root.add(w.root());
        }
        kind.connect_changed(clone!(@weak store, @strong iter, @weak root => move |c| {
            let wv = store.get_value(&iter, 1);
            if let Ok(Some(w)) = wv.get::<&Widget>() {
                root.remove(w.root());
            }
            let spec = match c.get_active_id() {
                None => view::Widget::Table(Path::from("/")),
                Some(s) if &*s == "Table" => view::Widget::Table(Path::from("/")),
                Some(s) if &*s == "Label" => {
                    let s = Value::String(Chars::from("static label"));
                    view::Widget::Label(view::Source::Constant(s))
                }
                Some(s) if &*s == "Button" => {
                    let l = Chars::from("click me!");
                    view::Widget::Button(view::Button {
                        enabled: view::Source::Constant(Value::True),
                        label: view::Source::Constant(Value::String(l)),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    })
                }
                Some(s) if &*s == "Toggle" => {
                    view::Widget::Toggle(view::Toggle {
                        enabled: view::Source::Constant(Value::True),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    })
                },
                Some(s) if &*s == "Selector" => {
                    let choices = Chars::from(
                        r#"[[{"U64": 1}, "One"], [{"U64": 2}, "Two"]]"#
                    );
                    view::Widget::Selector(view::Selector {
                        enabled: view::Source::Constant(Value::True),
                        choices: view::Source::Constant(Value::String(choices)),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    })
                },
                Some(s) if &*s == "Entry" => {
                    view::Widget::Entry(view::Entry {
                        enabled: view::Source::Constant(Value::True),
                        visible: view::Source::Constant(Value::True),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    })
                },
                Some(s) if &*s == "Box" => {
                    view::Widget::Box(view::Box {
                        direction: view::Direction::Vertical,
                        children: Vec::new(),
                    })
                },
                Some(s) if &*s == "Grid" => todo!(),
                _ => unreachable!(),
            };
            Widget::insert(on_change.clone(), &store, &iter, spec);
            on_change();
            let wv = store.get_value(&iter, 1);
            if let Ok(Some(w)) = wv.get::<&Widget>() {
                root.add(w.root());
            }
        }));
        KindWrap { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct GridChild {
    row: usize,
    col: usize,
    spec: Rc<view::Grid>,
    parent: OnChange,
    id: usize,
    halign: gtk::ComboBoxText,
    valign: gtk::ComboBoxText,
    delete: gtk::Button,
    kind: gtk::ComboBoxText,
    child: Widget,
}

#[derive(Clone, Debug)]
struct GridRow {
    row: usize,
    spec: Rc<view::Grid>,
    parent: OnChange,
    revealer: gtk::Revealer,
    container: gtk::Box,
    delete: gtk::Button,
    add: gtk::Button,
    contents: Vec<GridChild>,
}

#[derive(Clone, Debug)]
struct Table {
    path: gtk::Entry,
    spec: Rc<RefCell<view::Path>>,
}

impl Table {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        path: Path,
    ) {
        let entry = gtk::Entry::new();
        let spec = Rc::new(RefCell::new(path));
        entry.set_text(&**spec.borrow());
        entry.connect_activate(clone!(@strong spec => move |e| {
            *spec.borrow_mut() = Path::from(String::from(&*e.get_text()));
            on_change()
        }));
        let t = Widget::Table(Table { path: entry, spec });
        let v = t.to_value();
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Table(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.path.upcast_ref()
    }
}

fn parse_entry<T: Serialize + DeserializeOwned + 'static, F: Fn(T) + 'static>(
    label: &str,
    spec: &T,
    on_change: F,
) -> (gtk::Label, gtk::Entry) {
    let label = gtk::Label::new(Some(label));
    let entry = gtk::Entry::new();
    if let Ok(s) = serde_json::to_string(spec) {
        entry.set_text(&s);
    }
    entry.connect_activate(move |e| {
        let txt = e.get_text();
        match serde_json::from_str::<T>(&*txt) {
            Err(e) => warn!("invalid value: {}, {}", &*txt, e),
            Ok(src) => on_change(src),
        }
    });
    (label, entry)
}

struct TwoColGrid {
    root: gtk::Grid,
    row: i32,
}

impl TwoColGrid {
    fn new() -> Self {
        TwoColGrid { root: gtk::Grid::new(), row: 0 }
    }

    fn add<T: IsA<gtk::Widget>, U: IsA<gtk::Widget>>(&mut self, w: (T, U)) {
        self.root.attach(&w.0, 0, self.row, 1, 1);
        self.root.attach(&w.1, 1, self.row, 1, 1);
        self.row += 1;
    }

    fn attach<T: IsA<gtk::Widget>>(&mut self, w: &T, col: i32, cols: i32, rows: i32) {
        self.root.attach(w, col, self.row, cols, rows);
        self.row += 1;
    }
}

#[derive(Clone, Debug)]
struct Label {
    root: gtk::Box,
    spec: Rc<RefCell<view::Source>>,
}

impl Label {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Source,
    ) {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let spec = Rc::new(RefCell::new(spec));
        let (l, e) = parse_entry(
            "Source:",
            &*spec.borrow(),
            clone!(@strong spec => move |s| {
                *spec.borrow_mut() = s;
                on_change()
            }),
        );
        root.add(&l);
        root.add(&e);
        let t = Widget::Label(Label { root, spec });
        let v = t.to_value();
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Label(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Button {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Button>>,
}

impl Button {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Button,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        ));
        root.add(parse_entry(
            "Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        let t = Widget::Button(Button { root, spec });
        let v = t.to_value();
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Button(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Toggle {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Toggle>>,
}

impl Toggle {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Toggle,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change();
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change();
            }),
        ));
        let t = Widget::Toggle(Toggle { root, spec });
        let v = t.to_value();
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Toggle(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Selector {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Selector>>,
}

impl Selector {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Selector,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        ));
        root.add(parse_entry(
            "Choices:",
            &spec.borrow().choices,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().choices = s;
                on_change();
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change();
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        let t = Widget::Selector(Selector { root, spec });
        let v = t.to_value();
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Selector(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Entry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Entry>>,
}

impl Entry {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Entry,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Visible:",
            &spec.borrow().visible,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().visible = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        let t = Widget::Entry(Entry { root, spec });
        let v = t.to_value();
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Entry(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct BoxChild {
    root: TwoColGrid,
    spec: Rc<RefCell<view::BoxChild>>,
}

impl BoxChild {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::Treeiter,
        spec: view::BoxChild,
    ) {
        let spec = Rc::new(RefCell::new(spec));
        let mut root = TwoColGrid::new();
        let expand = gtk::CheckButton::with_label("Expand:");
        root.attach(&expand, 0, 2, 1);
        expand.connect_toggled(clone!(@strong on_change, @strong spec => move |cb| {
            spec.borrow_mut().expand = cb.get_active();
            on_change();
        }));
        let fill = gtk::CheckButton::with_label("Fill:");
        root.attach(&fill, 0, 2, 1);
        fill.connect_toggled(clone!(@strong on_change, @strong spec => move |cb| {
            spec.borrow_mut().fill = cb.get_active();
            on_change()
        }));
        root.add(parse_entry(
            "Padding:",
            &spec.borrow().padding,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().padding = s;
                on_change()
            }),
        ));
        let aligns = ["Fill", "Start", "End", "Center", "Baseline"];
        fn align_to_str(a: view::Align) -> &'static str {
            match a {
                view::Align::Fill => "Fill",
                view::Align::Start => "Start",
                view::Align::End => "End",
                view::Align::Center => "Center",
                view::Align::Baseline => "Baseline",
            }
        }
        fn align_from_str(a: GString) -> view::Align {
            match &*a {
                "Fill" => view::Align::Fill,
                "Start" => view::Align::Start,
                "End" => view::Align::End,
                "Center" => view::Align::Center,
                "Baseline" => view::Align::Baseline,
                x => unreachable!(x),
            }
        }
        let halign_lbl = gtk::Label::new(Some("Horizontal Alignment:"));
        let halign = gtk::ComboBoxText::new();
        let valign_lbl = gtk::Label::new(Some("Vertical Alignment:"));
        let valign = gtk::ComboBoxText::new();
        root.add((halign_lbl.clone(), halign.clone()));
        root.add((valign_lbl.clone(), valign.clone()));
        for a in &aligns {
            halign.append(Some(a), a);
            valign.append(Some(a), a);
        }
        halign.set_active_id(spec.borrow().halign.map(align_to_str));
        valign.set_active_id(spec.borrow().valign.map(align_to_str));
        halign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().halign = c.get_active_id().map(align_from_str);
            on_change()
        }));
        valign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().valign = c.get_active_id().map(align_from_str);
            on_change()
        }));
        let t = Widget::BoxChild(BoxChild { root, spec });
        let v = t.to_value();
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::BoxChild(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Box {
    root: gtk::Box,
}

impl Box {
    fn new(on_change: OnChange, spec: view::Box) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let dir_add = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        root.add(&dir_add);
        let spec = Rc::new(RefCell::new(spec));
        let children = Rc::new(RefCell::new(Vec::new()));
        let dircb = gtk::ComboBoxText::new();
        dir_add.add(&dircb);
        dircb.append(Some("Horizontal"), "Horizontal");
        dircb.append(Some("Vertical"), "Vertical");
        match spec.borrow().direction {
            view::Direction::Horizontal => dircb.set_active_id(Some("Horizontal")),
            view::Direction::Vertical => dircb.set_active_id(Some("Vertical")),
        };
        dircb.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            let dir = match c.get_active_id() {
                Some(s) if &*s == "Horizontal" => view::Direction::Horizontal,
                Some(s) if &*s == "Vertical" => view::Direction::Vertical,
                _ => view::Direction::Horizontal,
            };
            spec.borrow_mut().direction = dir;
            on_change(view::Widget::Box(spec.borrow().clone()));
        }));
        let croot = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let crev = gtk::Revealer::new();
        root.add(&crev);
        crev.add(&croot);
        let reveal_children = gtk::ToggleButton::with_label(">");
        reveal_children.connect_toggled(clone!(@weak crev => move |t| {
            crev.set_reveal_child(t.get_active())
        }));
        dir_add.add(&reveal_children);
        let addbtn = gtk::Button::new();
        dir_add.add(&addbtn);
        addbtn.set_label("+");
        addbtn.connect_clicked(clone!(
            @strong on_change,
            @strong spec,
            @strong children,
            @weak croot => move |_| {
                let i = children.borrow().len();
                let s = view::BoxChild {
                    expand: false,
                    fill: false,
                    padding: 0,
                    halign: None,
                    valign: None,
                    widget: view::Widget::Label(view::Source::Constant(Value::U64(42))),
                };
                let on_change_chld = Rc::new(
                    clone!(@strong on_change, @strong spec => move |s| {
                        spec.borrow_mut().children[i] = s;
                        on_change(view::Widget::Box(spec.borrow().clone()));
                    })
                );
                let child = BoxChild::new(on_change_chld, s.clone());
                croot.add(child.root());
                croot.set_child_packing(
                    child.root(), false, false, 10, gtk::PackType::Start
                );
                croot.show_all();
                children.borrow_mut().push(child);
                spec.borrow_mut().children.push(s);
                on_change(view::Widget::Box(spec.borrow().clone()));
        }));
        for (i, c) in spec.borrow().children.iter().enumerate() {
            let on_change = Rc::new(clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().children[i] = s;
                on_change(view::Widget::Box(spec.borrow().clone()));
            }));
            let c = BoxChild::new(on_change, c.clone());
            croot.add(c.root());
            croot.set_child_packing(c.root(), false, false, 10, gtk::PackType::Start);
            children.borrow_mut().push(c);
        }
        Box { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct Grid {
    parent: OnChange,
    homogeneous_columns: gtk::CheckButton,
    homogeneous_rows: gtk::CheckButton,
    column_spacing: gtk::Entry,
    row_spacing: gtk::Entry,
    revealer: gtk::Revealer,
    container: gtk::Box,
    children: Vec<GridRow>,
}

#[derive(Clone, Debug, GBoxed)]
#[gboxed(type_name = "NetidxEditorWidget")]
enum Widget {
    Table(Table),
    Label(Label),
    Button(Button),
    Toggle(Toggle),
    Selector(Selector),
    Entry(Entry),
    Box(Box),
    BoxChild(BoxChild),
    Grid(Grid),
    GridChild(GridChild),
}

impl Widget {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Widget,
    ) {
        match spec {
            view::Widget::Table(s) => Widget::Table(Table::new(on_change, s)),
            view::Widget::Label(s) => Widget::Label(Label::new(on_change, s)),
            view::Widget::Button(s) => Widget::Button(Button::new(on_change, s)),
            view::Widget::Toggle(s) => Widget::Toggle(Toggle::new(on_change, s)),
            view::Widget::Selector(s) => Widget::Selector(Selector::new(on_change, s)),
            view::Widget::Entry(s) => Widget::Entry(Entry::new(on_change, s)),
            view::Widget::Box(s) => Widget::Box(Box::new(on_change, s)),
            view::Widget::Grid(_) => todo!(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        match self {
            Widget::Table(w) => w.root(),
            Widget::Label(w) => w.root(),
            Widget::Button(w) => w.root(),
            Widget::Toggle(w) => w.root(),
            Widget::Selector(w) => w.root(),
            Widget::Entry(w) => w.root(),
            Widget::Box(w) => w.root(),
            Widget::Grid(_) => todo!(),
        }
    }
}

pub(super) struct Editor {
    root: gtk::ScrolledWindow,
}

impl Editor {
    pub(super) fn new(
        mut from_gui: mpsc::UnboundedSender<FromGui>,
        path: Path,
        spec: view::View,
    ) -> Editor {
        let pages: Rc<RefCell<HashMap<TreePath, gtk::Widget>>> =
            Rc::new(RefCell::new(HashMap::new()));
        let store = gtk::TreeStore::new();
        let view = gtk::TreeView::new();
        let root =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        let spec = Rc::new(RefCell::new(spec));
        let on_change = Rc::new({
            let spec = Rc::clone(&spec);
            move |s: view::Widget| {
                spec.borrow_mut().root = s;
                let m = FromGui::Render(path.clone(), spec.borrow().clone());
                let _: result::Result<_, _> = from_gui.unbounded_send(m);
            }
        });
        let w = KindWrap::new(on_change, spec.borrow().root.clone());
        root.add(w.root());
        Editor { root }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
