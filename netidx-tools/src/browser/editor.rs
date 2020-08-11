use futures::channel::mpsc;
use gtk;
use netidx_protocols::view;
use std::rc::Rc;
use log::warn;

struct BoxChild {
    expand: gtk::CheckButton,
    fill: gtk::CheckButton,
    padding: gtk::Entry,
    halign: gtk::ComboBoxText,
    valign: gtk::ComboBoxText,
    kind: gtk::ComboBoxText,
    delete: gtk::Button,
    child: Widget,
}

struct GridChild {
    row: usize,
    col: usize,
    spec: Rc<view::Grid>,
    parent: Rc<Fn(view::Widget)>,
    id: usize,
    halign: gtk::ComboBoxText,
    valign: gtk::ComboBoxText,
    delete: gtk::Button,
    kind: gtk::ComboBoxText,
    child: Widget,
}

struct GridRow {
    row: usize,
    spec: Rc<view::Grid>,
    parent: Rc<Fn(view::Widget)>,
    revealer: gtk::Revealer,
    container: gtk::Box,
    delete: gtk::Button,
    add: gtk::Button,
    contents: Vec<GridChild>,
}

struct Table {
    path: gtk::Entry,
}

impl Table {
    fn new(parent: Rc<Fn(view::Widget)>, path: Path) -> Self {
        let path_entry = gtk::Entry::new();
        entry.set_text(&*path);
        entry.connect_activate(move |e| {
            let path = Path::from(String::from(&*e.get_text()));
            parent(view::Widget::Table(path))
        });
        Table { path: path_entry }
    }

    fn root(&self) -> &gtk::Widget {
        self.path.upcast_ref()
    }
}

struct Label {
    parent: Rc<Fn(view::Widget)>,
    source: gtk::Entry,
}

impl Label {
    fn new(parent: Rc<Fn(view::Widget)>, spec: view::Source) -> Self {
        let source_entry = gtk::Entry::new();
        source_entry.set_text(&serde_json::to_string(&spec).unwrap());
        source_entry.connect_activate(move |e| {
            let txt = e.get_text();
            match serde_json::from_str::<view::Source>(&*txt) {
                Err(e) => warn!("invalid source: {}, {}", txt, e),
                Ok(src) => parent(view::Widget::Label(src))
            }
        });
    }

    fn root(&self) -> &gtk::Widget {
        self.source.upcast_ref()
    }
}

struct Button {
    parent: Rc<Fn(view::Widget)>,
    enabled: gtk::Entry,
    label: gtk::Entry,
    source: gtk::Entry,
    sink: gtk::Entry,
}

struct Toggle {
    parent: Rc<Fn(view::Widget)>,
    enabled: gtk::Entry,
    source: gtk::Entry,
    sink: gtk::Entry,
}

struct Selector {
    parent: Rc<Fn(view::Widget)>,
    enabled: gtk::Entry,
    choices: gtk::Entry,
    source: gtk::Entry,
    sink: gtk::Entry,
}

struct Entry {
    parent: Rc<Fn(view::Widget)>,
    enabled: gtk::Entry,
    visible: gtk::Entry,
    source: gtk::Entry,
    sink: gtk::Entry,
}

struct Box {
    parent: Rc<Fn(view::Widget)>,
    direction: gtk::ComboBoxText,
    revealer: gtk::Revealer,
    container: gtk::Box,
    children: Vec<BoxChild>,
}

struct Grid {
    parent: Rc<Fn(view::Widget)>,
    homogeneous_columns: gtk::CheckButton,
    homogeneous_rows: gtk::CheckButton,
    column_spacing: gtk::Entry,
    row_spacing: gtk::Entry,
    revealer: gtk::Revealer,
    container: gtk::Box,
    children: Vec<GridRow>,
}

enum Widget {
    Table(Table),
    Label(Label),
    Button(Button),
    Toggle(Toggle),
    Selector(Selector),
    Entry(Entry),
    Box(Box),
    Grid(Grid),
}

impl Widget {
    fn new(parent: Rc<Fn(view::Widget)>, spec: view::Widget) -> Self {
        match spec {
            view::Widget::Table(path) => Widget::Table {},
        }
    }
}

struct Editor {
    add: gtk::Button,
    kind: gtk::ComboBoxText,
    root: Widget,
}

impl Editor {
    fn new(spec: view::View) -> Editor {}
}
