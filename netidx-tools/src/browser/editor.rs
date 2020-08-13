use super::FromGui;
use futures::channel::mpsc;
use glib::clone;
use gtk::{self, prelude::*};
use log::warn;
use netidx::{chars::Chars, path::Path, subscriber::Value};
use netidx_protocols::view;
use serde::{de::DeserializeOwned, Serialize};
use std::{cell::RefCell, rc::Rc, result};

type OnChange = Rc<dyn Fn(view::Widget)>;

struct KindWrap {
    root: gtk::Box,
}

impl KindWrap {
    fn new(on_change: OnChange, spec: view::Widget) -> KindWrap {
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
        let widget = RefCell::new(Widget::new(on_change.clone(), spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.add(&kind);
        {
            let wr = widget.borrow();
            root.add(wr.root());
        }
        kind.connect_changed(clone!(@weak root => move |c| {
            let mut wr = widget.borrow_mut();
            match c.get_active_id() {
                Some(s) if &*s == "Table" => {
                    root.remove(wr.root());
                    *wr = Widget::Table(Table::new(on_change.clone(), Path::from("/")));
                    root.add(wr.root());
                    on_change(view::Widget::Table(Path::from("/")));
                }
                Some(s) if &*s == "Label" => {
                    let s = Value::String(Chars::from("static label"));
                    let spec = view::Source::Constant(s);
                    root.remove(wr.root());
                    *wr = Widget::Label(Label::new(on_change.clone(), spec.clone()));
                    root.add(wr.root());
                    on_change(view::Widget::Label(spec));
                }
                Some(s) if &*s == "Button" => {
                    let l = Chars::from("click me!");
                    let spec = view::Button {
                        enabled: view::Source::Constant(Value::True),
                        label: view::Source::Constant(Value::String(l)),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    };
                    root.remove(wr.root());
                    *wr = Widget::Button(Button::new(on_change.clone(), spec.clone()));
                    root.add(wr.root());
                    on_change(view::Widget::Button(spec));
                }
                Some(s) if &*s == "Toggle" => {
                    let spec = view::Toggle {
                        enabled: view::Source::Constant(Value::True),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    };
                    root.remove(wr.root());
                    *wr = Widget::Toggle(Toggle::new(on_change.clone(), spec.clone()));
                    root.add(wr.root());
                    on_change(view::Widget::Toggle(spec));
                },
                Some(s) if &*s == "Selector" => todo!(),
                Some(s) if &*s == "Entry" => todo!(),
                Some(s) if &*s == "Box" => todo!(),
                Some(s) if &*s == "Grid" => todo!(),
                None => (), // CR estokes: hmmm
                _ => unreachable!(),
            }
        }));
        KindWrap { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
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
    parent: OnChange,
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
    parent: OnChange,
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
        let entry = gtk::Entry::new();
        entry.set_text(&*path);
        entry.connect_activate(move |e| {
            let path = Path::from(String::from(&*e.get_text()));
            on_change(view::Widget::Table(path))
        });
        Table { path: entry }
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

struct Label {
    root: gtk::Box,
}

impl Label {
    fn new(on_change: OnChange, spec: view::Source) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let (l, e) =
            parse_entry("Source:", &spec, move |s| on_change(view::Widget::Label(s)));
        root.add(&l);
        root.add(&e);
        Label { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

struct Button {
    root: gtk::Grid,
}

impl Button {
    fn new(on_change: OnChange, spec: view::Button) -> Self {
        let root = gtk::Grid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (lbl, entry) = parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change(view::Widget::Button(spec.borrow().clone()))
            }),
        );
        root.attach(&lbl, 0, 0, 1, 1);
        root.attach(&entry, 1, 0, 1, 1);
        let (lbl, entry) = parse_entry(
            "Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                on_change(view::Widget::Button(spec.borrow().clone()))
            }),
        );
        root.attach(&lbl, 0, 1, 1, 1);
        root.attach(&entry, 1, 1, 1, 1);
        let (lbl, entry) = parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change(view::Widget::Button(spec.borrow().clone()))
            }),
        );
        root.attach(&lbl, 0, 2, 1, 1);
        root.attach(&entry, 1, 2, 1, 1);
        let (lbl, entry) = parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change(view::Widget::Button(spec.borrow().clone()))
            }),
        );
        root.attach(&lbl, 0, 3, 1, 1);
        root.attach(&entry, 1, 3, 1, 1);
        Button { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

struct Toggle {
    root: gtk::Grid,
}

impl Toggle {
    fn new(on_change: OnChange, spec: view::Toggle) -> Self {
        let root = gtk::Grid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (lbl, entry) = parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change(view::Widget::Toggle(spec.borrow().clone()));
            }),
        );
        root.attach(&lbl, 0, 0, 1, 1);
        root.attach(&entry, 1, 0, 1, 1);
        let (lbl, entry) = parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change(view::Widget::Toggle(spec.borrow().clone()))
            }),
        );
        root.attach(&lbl, 0, 1, 1, 1);
        root.attach(&entry, 1, 1, 1, 1);
        let (lbl, entry) = parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change(view::Widget::Toggle(spec.borrow().clone()))
            }),
        );
        root.attach(&lbl, 0, 2, 1, 1);
        root.attach(&entry, 1, 2, 1, 1);
        Toggle { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

struct Selector {
    parent: OnChange,
    enabled: gtk::Entry,
    choices: gtk::Entry,
    source: gtk::Entry,
    sink: gtk::Entry,
}

struct Entry {
    parent: OnChange,
    enabled: gtk::Entry,
    visible: gtk::Entry,
    source: gtk::Entry,
    sink: gtk::Entry,
}

struct Box {
    parent: OnChange,
    direction: gtk::ComboBoxText,
    revealer: gtk::Revealer,
    container: gtk::Box,
    children: Vec<BoxChild>,
}

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
            view::Widget::Table(s) => Widget::Table(Table::new(on_change, s)),
            view::Widget::Label(s) => Widget::Label(Label::new(on_change, s)),
            view::Widget::Button(s) => Widget::Button(Button::new(on_change, s)),
            view::Widget::Toggle(s) => Widget::Toggle(Toggle::new(on_change, s)),
            view::Widget::Selector(_) => todo!(),
            view::Widget::Entry(_) => todo!(),
            view::Widget::Box(_) => todo!(),
            view::Widget::Grid(_) => todo!(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        match self {
            Widget::Table(w) => w.root(),
            Widget::Label(w) => w.root(),
            Widget::Button(w) => w.root(),
            Widget::Toggle(w) => w.root(),
            Widget::Selector(_) => todo!(),
            Widget::Entry(_) => todo!(),
            Widget::Box(_) => todo!(),
            Widget::Grid(_) => todo!(),
        }
    }
}

pub(super) struct Editor {
    root: KindWrap,
}

impl Editor {
    pub(super) fn new(
        mut from_gui: mpsc::UnboundedSender<FromGui>,
        path: Path,
        spec: view::View,
    ) -> Editor {
        let spec = Rc::new(RefCell::new(spec));
        let on_change = Rc::new({
            let spec = Rc::clone(&spec);
            move |s: view::Widget| {
                spec.borrow_mut().root = s;
                let m = FromGui::Render(path.clone(), spec.borrow().clone());
                let _: result::Result<_, _> = from_gui.unbounded_send(m);
            }
        });
        let root = spec.borrow().root.clone();
        Editor { root: KindWrap::new(on_change, root) }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root()
    }
}
