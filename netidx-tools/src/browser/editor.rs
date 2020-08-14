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
    rc::Rc,
    result,
};

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
            root.remove(wr.root());
            match c.get_active_id() {
                Some(s) if &*s == "Table" => {
                    *wr = Widget::Table(Table::new(on_change.clone(), Path::from("/")));
                    on_change(view::Widget::Table(Path::from("/")));
                }
                Some(s) if &*s == "Label" => {
                    let s = Value::String(Chars::from("static label"));
                    let spec = view::Source::Constant(s);
                    *wr = Widget::Label(Label::new(on_change.clone(), spec.clone()));
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
                    *wr = Widget::Button(Button::new(on_change.clone(), spec.clone()));
                    on_change(view::Widget::Button(spec));
                }
                Some(s) if &*s == "Toggle" => {
                    let spec = view::Toggle {
                        enabled: view::Source::Constant(Value::True),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    };
                    *wr = Widget::Toggle(Toggle::new(on_change.clone(), spec.clone()));
                    on_change(view::Widget::Toggle(spec));
                },
                Some(s) if &*s == "Selector" => {
                    let choices = Chars::from(
                        r#"[[{"U64": 1}, "One"], [{"U64": 2}, "Two"]]"#
                    );
                    let spec = view::Selector {
                        enabled: view::Source::Constant(Value::True),
                        choices: view::Source::Constant(Value::String(choices)),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    };
                    *wr = Widget::Selector(Selector::new(on_change.clone(), spec.clone()));
                    on_change(view::Widget::Selector(spec));
                },
                Some(s) if &*s == "Entry" => {
                    let spec = view::Entry {
                        enabled: view::Source::Constant(Value::True),
                        visible: view::Source::Constant(Value::True),
                        source: view::Source::Load(Path::from("/somewhere")),
                        sink: view::Sink::Store(Path::from("/somewhere/else")),
                    };
                    *wr = Widget::Entry(Entry::new(on_change.clone(), spec.clone()));
                    on_change(view::Widget::Entry(spec));
                },
                Some(s) if &*s == "Box" => {
                    let spec = view::Box {
                        direction: view::Direction::Vertical,
                        children: Vec::new(),
                    };
                    *wr = Widget::Box(Box::new(on_change.clone(), spec.clone()));
                    on_change(view::Widget::Box(spec));
                },
                Some(s) if &*s == "Grid" => todo!(),
                None => (), // CR estokes: hmmm
                _ => unreachable!(),
            };
            root.add(wr.root());
        }));
        KindWrap { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
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
    root: TwoColGrid,
}

impl Button {
    fn new(on_change: OnChange, spec: view::Button) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let send = Rc::new(clone!(@strong on_change, @strong spec => move || {
            on_change(view::Widget::Button(spec.borrow().clone()))
        }));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                send();
            }),
        ));
        root.add(parse_entry(
            "Label:",
            &spec.borrow().label,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                send()
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                send()
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                send()
            }),
        ));
        Button { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

struct Toggle {
    root: TwoColGrid,
}

impl Toggle {
    fn new(on_change: OnChange, spec: view::Toggle) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let send = Rc::new(clone!(@strong on_change, @strong spec => move || {
            on_change(view::Widget::Toggle(spec.borrow().clone()));
        }));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                send();
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                send();
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                send();
            }),
        ));
        Toggle { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

struct Selector {
    root: TwoColGrid,
}

impl Selector {
    fn new(on_change: OnChange, spec: view::Selector) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let send = Rc::new(clone!(@strong spec => move || {
            on_change(view::Widget::Selector(spec.borrow().clone()))
        }));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                send();
            }),
        ));
        root.add(parse_entry(
            "Choices:",
            &spec.borrow().choices,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().choices = s;
                send();
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                send();
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                send()
            }),
        ));
        Selector { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

struct Entry {
    root: TwoColGrid,
}

impl Entry {
    fn new(on_change: OnChange, spec: view::Entry) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let send = Rc::new(clone!(@strong spec => move || {
            on_change(view::Widget::Entry(spec.borrow().clone()))
        }));
        root.add(parse_entry(
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                send()
            }),
        ));
        root.add(parse_entry(
            "Visible:",
            &spec.borrow().visible,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().visible = s;
                send()
            }),
        ));
        root.add(parse_entry(
            "Source:",
            &spec.borrow().source,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                send()
            }),
        ));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                send()
            }),
        ));
        Entry { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root.upcast_ref()
    }
}

struct BoxChild {
    root: gtk::Box,
}

impl BoxChild {
    fn new(on_change: Rc<dyn Fn(view::BoxChild)>, spec: view::BoxChild) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let controls = gtk::Revealer::new();
        let show_controls = gtk::ToggleButton::with_label(">");
        show_controls.connect_toggled(clone!(@weak controls => move |t| {
            controls.set_reveal_child(t.get_active());
        }));
        root.add(&show_controls);
        root.add(&controls);
        let mut cgrid = TwoColGrid::new();
        controls.add(&cgrid.root);
        let send = Rc::new(clone!(@strong spec => move || {
            on_change(spec.borrow().clone())
        }));
        let expand = gtk::CheckButton::with_label("Expand:");
        cgrid.attach(&expand, 0, 2, 1);
        expand.connect_toggled(clone!(@strong send, @strong spec => move |cb| {
            spec.borrow_mut().expand = cb.get_active();
            send();
        }));
        let fill = gtk::CheckButton::with_label("Fill:");
        cgrid.attach(&fill, 0, 2, 1);
        fill.connect_toggled(clone!(@strong send, @strong spec => move |cb| {
            spec.borrow_mut().fill = cb.get_active();
            send()
        }));
        cgrid.add(parse_entry(
            "Padding:",
            &spec.borrow().padding,
            clone!(@strong send, @strong spec => move |s| {
                spec.borrow_mut().padding = s;
                send()
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
        cgrid.add((halign_lbl.clone(), halign.clone()));
        cgrid.add((valign_lbl.clone(), valign.clone()));
        for a in &aligns {
            halign.append(Some(a), a);
            valign.append(Some(a), a);
        }
        halign.set_active_id(spec.borrow().halign.map(align_to_str));
        valign.set_active_id(spec.borrow().valign.map(align_to_str));
        halign.connect_changed(clone!(@strong send, @strong spec => move |c| {
            spec.borrow_mut().halign = c.get_active_id().map(align_from_str);
            send()
        }));
        valign.connect_changed(clone!(@strong send, @strong spec => move |c| {
            spec.borrow_mut().valign = c.get_active_id().map(align_from_str);
            send()
        }));
        let on_change = Rc::new(clone!(@strong send, @strong spec => move |w| {
            spec.borrow_mut().widget = w;
            send();
        }));
        let widget = KindWrap::new(on_change, spec.borrow().widget.clone());
        root.add(widget.root());
        BoxChild { root }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

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
