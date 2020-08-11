use super::FromGui;
use netidx::{subscriber::Value, chars::Chars};
use futures::channel::mpsc;
use glib::clone;
use gtk;
use log::warn;
use netidx_protocols::view;
use std::{cell::RefCell, rc::Rc, result};

type OnChange = Rc<Fn(view::Widget, Option<(&gtk::Widget, &gtk::Widget)>)>;

struct KindWrap {
    kind: gtk::ComboBoxText,
    widget: Rc<RefCell<Widget>>,
}

impl KindWrap {
    fn new(on_change: OnChange, spec: view::Widget) -> KindWrap {
        let kinds =
            ["Table", "Label", "Button", "Toggle", "Selector", "Entry", "Box", "Grid"];
        let combo = gtk::ComboBoxText::new();
        for k in &kinds {
            combo.append_text(k);
        }
        combo.set_active_id(Some(match spec {
            view::Widget::Table(_) => "Table",
            view::Widget::Label(_) => "Label",
            view::Widget::Button(_) => "Button",
            view::Widget::Toggle(_) => "Toggle",
            view::Widget::Selector(_) => "Selector",
            view::Widget::Entry(_) => "Entry",
            view::Widget::Box(_) => "Box",
            view::Widget::Grid(_) => "Grid",
        }));
        let widget = Rc::new(RefCell::new(Widget::new(on_change.clone(), spec)));
        combo.connect_changed({
            let widget = Rc::clone(&widget);
            move |combo| match &*combo.get_active_id() {
                "Table" => {
                    let spec = view::Widget::Table(Path::from("/"));
                    let cur = widget.borrow().root().clone();
                    *widget.borrow_mut() =
                        Widget::Table(Table::new(on_change.clone(), spec.clone()));
                    on_change(spec, Some((&cur, widget.borrow().root())));
                }
                "Label" => {
                    let s = Value::String(Chars::from("static label"));
                    let spec = view::Widget::Label(view::Source::Constant(v));
                    let cur = widget.borrow().root().clone();
                    *widget.borrow_mut() =
                        Widget::Label()
                },
                "Button" => todo!(),
                "Toggle" => todo!(),
                "Selector" => todo!(),
                "Entry" => todo!(),
                "Box" => todo!(),
                "Grid" => todo!(),
                _ => unreachable!(),
            }
        });
    }

    fn combo(&self) -> &gtk::ComboBoxText {
        &self.kind
    }

    fn root(&self) -> &gtk::Widget {
        self.widget.borrow().root()
    }
}

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
    fn new(on_change: OnChange, path: Path) -> Self {
        let path_entry = gtk::Entry::new();
        entry.set_text(&*path);
        entry.connect_activate(move |e| {
            let path = Path::from(String::from(&*e.get_text()));
            on_change(view::Widget::Table(path), None)
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
    fn new(on_change: OnChange, spec: view::Source) -> Self {
        let source_entry = gtk::Entry::new();
        source_entry.set_text(&serde_json::to_string(&spec).unwrap());
        source_entry.connect_activate(move |e| {
            let txt = e.get_text();
            match serde_json::from_str::<view::Source>(&*txt) {
                Err(e) => warn!("invalid source: {}, {}", txt, e),
                Ok(src) => on_change(view::Widget::Label(src), None),
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
    fn new(on_change: OnChange, spec: view::Widget) -> Self {
        match spec {
            view::Widget::Table(path) => Widget::Table(Table::new(on_change, path)),
            view::Widget::Label(src) => Widget::Label(Label::new(on_change, src)),
            view::Widget::Button(_) => todo!(),
            view::Widget::Toggle(_) => todo!(),
            view::Widget::Selector(_) => todo!(),
            view::Widget::Entry(_) => todo!(),
            view::Widget::Grid(_) => todo!(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        match self {
            Widget::Table(w) => w.root(),
            Widget::Label(w) => w.root(),
            Widget::Button(_) => todo!(),
            Widget::Toggle(_) => todo!(),
            Widget::Selector(_) => todo!(),
            Widget::Entry(_) => todo!(),
            Widget::Grid(_) => todo!(),
        }
    }
}

struct Editor {
    kind: gtk::ComboBoxText,
    root: Rc<RefCell<Widget>>,
}

impl Editor {
    fn new(
        root: &gtk::Box,
        mut from_gui: mpsc::UnboundedSender<FromGui>,
        path: Path,
        spec: view::View,
    ) -> Editor {
        let spec = Rc::new(RefCell::new(spec));
        let on_change = Rc::new({
            let spec = spec.clone();
            let root = root.clone();
            move |s: view::Widget, w: Option<(&gtk::Widget, &gtk::Widget)>| {
                *spec.borrow_mut().root = s;
                let m = FromGui::Render(path.clone(), spec.borrow().clone());
                let _: result::Result<_, _> = from_gui.unbounded_send(m);
                if let Some((old, new)) = w {
                    root.remove(old);
                    root.add(new);
                }
            }
        });
        let w = Rc::new(RefCell::new(Widget::new(
            on_change.clone(),
            spec.borrow().root.clone(),
        )));
        let combo = kind_combo(&spec);
        combo.connect_changed({});
        root.add(&combo);
        root.add(w.borrow().root());
        Editor { kind: combo, root: w }
    }
}
