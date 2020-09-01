mod source_inspector;
mod util;
use super::{util::err_modal, FromGui, ToGui, WidgetCtx, WidgetPath};
use futures::channel::mpsc;
use glib::{
    clone, idle_add_local, prelude::*, subclass::prelude::*, value::GetError, GString,
};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{SubId, Value},
};
use netidx_protocols::view;
use source_inspector::SourceInspector;
use std::{
    boxed,
    cell::{Cell, RefCell},
    rc::Rc,
    result,
    sync::Arc,
};
use util::{parse_entry, TwoColGrid};

type OnChange = Rc<dyn Fn()>;

#[derive(Clone, Debug)]
struct Table {
    path: gtk::Entry,
    spec: Rc<RefCell<Path>>,
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
        store.set_value(iter, 0, &"Table".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Table(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.path.upcast_ref()
    }
}

type DbgSrc = Rc<RefCell<Option<(gtk::Window, SourceInspector)>>>;

fn source(
    ctx: &WidgetCtx,
    txt: &str,
    init: &view::Source,
    on_change: impl Fn(view::Source) + 'static,
) -> (gtk::Label, gtk::Box, DbgSrc) {
    let on_change = Rc::new(on_change);
    let source = Rc::new(RefCell::new(init.clone()));
    let inspector: Rc<RefCell<Option<(gtk::Window, SourceInspector)>>> =
        Rc::new(RefCell::new(None));
    let lbl = gtk::Label::new(Some(txt));
    let ibox = gtk::Box::new(gtk::Orientation::Horizontal, 0);
    let entry = gtk::Entry::new();
    let inspect = gtk::ToggleButton::new();
    let inspect_icon = gtk::Image::from_icon_name(
        Some("preferences-system"),
        gtk::IconSize::SmallToolbar,
    );
    inspect.set_image(Some(&inspect_icon));
    ibox.pack_start(&entry, true, true, 0);
    ibox.pack_end(&inspect, false, false, 0);
    entry.set_text(&source.borrow().to_string());
    entry.connect_activate(clone!(
        @strong on_change, @strong source, @weak inspect, @weak ibox => move |e| {
        match e.get_text().parse::<view::Source>() {
            Err(e) => err_modal(&ibox, &format!("parse error: {}", e)),
            Ok(s) => {
                inspect.set_active(false);
                *source.borrow_mut() = s.clone();
                on_change(s);
            }
        }
    }));
    inspect.connect_toggled(clone!(
    @strong ctx,
    @strong on_change,
    @strong inspector,
    @strong source,
    @weak entry => move |b| {
        if !b.get_active() {
            if let Some((w, _)) = inspector.borrow_mut().take() {
                w.close()
            }
        } else {
            let w = gtk::Window::new(gtk::WindowType::Toplevel);
            let on_change = {
                let on_change = on_change.clone();
                let entry = entry.clone();
                let source = source.clone();
                move |s: view::Source| {
                    entry.set_text(&s.to_string());
                    *source.borrow_mut() = s.clone();
                    on_change(s)
                }
            };
            let si = SourceInspector::new(ctx.clone(), on_change, source.borrow().clone());
            w.add(si.root());
            w.connect_delete_event(clone!(@strong inspector, @strong b => move |_, _| {
                *inspector.borrow_mut() = None;
                b.set_active(false);
                Inhibit(false)
            }));
            w.show_all();
            *inspector.borrow_mut() = Some((w, si));
        }
    }));
    (lbl, ibox, inspector)
}

#[derive(Clone, Debug)]
struct Action {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Action>>,
    source: DbgSrc,
}

impl Action {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Action,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (srclbl, srcent, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        );
        root.add((srclbl, srcent));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        let t = Widget::Action(Action { root, spec, source });
        let v = t.to_value();
        store.set_value(iter, 0, &"Action".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Action(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(changed);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update_var(name, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Label {
    root: gtk::Box,
    spec: Rc<RefCell<view::Source>>,
    source: DbgSrc,
}

impl Label {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Source,
    ) {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &*spec.borrow(),
            clone!(@strong spec => move |s| {
                *spec.borrow_mut() = s;
                on_change()
            }),
        );
        root.add(&l);
        root.add(&e);
        let t = Widget::Label(Label { root, spec, source });
        let v = t.to_value();
        store.set_value(iter, 0, &"Label".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Label(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(changed);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update_var(name, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Button {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Button>>,
    enabled_source: DbgSrc,
    label_source: DbgSrc,
    source: DbgSrc,
}

impl Button {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Button,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, label_source) = source(
            ctx,
            "Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        let t =
            Widget::Button(Button { root, spec, enabled_source, label_source, source });
        let v = t.to_value();
        store.set_value(iter, 0, &"Button".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Button(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(changed);
        }
        if let Some((_, si)) = &*self.label_source.borrow() {
            si.update(changed);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(changed);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update_var(name, value);
        }
        if let Some((_, si)) = &*self.label_source.borrow() {
            si.update_var(name, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update_var(name, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Toggle {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Toggle>>,
    enabled_source: DbgSrc,
    source: DbgSrc,
}

impl Toggle {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Toggle,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change();
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change();
            }),
        ));
        let t = Widget::Toggle(Toggle { root, spec, enabled_source, source });
        let v = t.to_value();
        store.set_value(iter, 0, &"Toggle".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Toggle(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(changed);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(changed);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update_var(name, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update_var(name, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Selector {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Selector>>,
    enabled_source: DbgSrc,
    choices_source: DbgSrc,
    source: DbgSrc,
}

impl Selector {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Selector,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, choices_source) = source(
            ctx,
            "Choices:",
            &spec.borrow().choices,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().choices = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change();
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        let t = Widget::Selector(Selector {
            root,
            spec,
            enabled_source,
            choices_source,
            source,
        });
        let v = t.to_value();
        store.set_value(iter, 0, &"Selector".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Selector(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(changed);
        }
        if let Some((_, si)) = &*self.choices_source.borrow() {
            si.update(changed);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(changed);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update_var(name, value);
        }
        if let Some((_, si)) = &*self.choices_source.borrow() {
            si.update_var(name, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update_var(name, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Entry>>,
    enabled_source: DbgSrc,
    visible_source: DbgSrc,
    source: DbgSrc,
}

impl Entry {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Entry,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, visible_source) = source(
            ctx,
            "Visible:",
            &spec.borrow().visible,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().visible = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        let t =
            Widget::Entry(Entry { root, spec, enabled_source, visible_source, source });
        let v = t.to_value();
        store.set_value(iter, 0, &"Entry".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Entry(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(changed);
        }
        if let Some((_, si)) = &*self.visible_source.borrow() {
            si.update(changed);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(changed);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update_var(name, value);
        }
        if let Some((_, si)) = &*self.visible_source.borrow() {
            si.update_var(name, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update_var(name, value);
        }
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
        iter: &gtk::TreeIter,
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
        store.set_value(iter, 0, &"BoxChild".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::BoxChild(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone, Debug)]
struct BoxContainer {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Box>>,
}

impl BoxContainer {
    fn insert(
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Box,
    ) {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let dircb = gtk::ComboBoxText::new();
        dircb.append(Some("Horizontal"), "Horizontal");
        dircb.append(Some("Vertical"), "Vertical");
        match spec.borrow().direction {
            view::Direction::Horizontal => dircb.set_active_id(Some("Horizontal")),
            view::Direction::Vertical => dircb.set_active_id(Some("Vertical")),
        };
        dircb.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().direction = match c.get_active_id() {
                Some(s) if &*s == "Horizontal" => view::Direction::Horizontal,
                Some(s) if &*s == "Vertical" => view::Direction::Vertical,
                _ => view::Direction::Horizontal,
            };
            on_change();
        }));
        let dirlbl = gtk::Label::new(Some("Direction:"));
        root.add((dirlbl, dircb));
        let homo = gtk::CheckButton::with_label("Homogeneous:");
        root.attach(&homo, 0, 2, 1);
        homo.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().homogeneous = b.get_active();
            on_change()
        }));
        root.add(parse_entry(
            "Spacing:",
            &spec.borrow().spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().spacing = s;
                on_change()
            }),
        ));
        let t = Widget::Box(BoxContainer { root, spec });
        let v = t.to_value();
        store.set_value(iter, 0, &"Box".to_value());
        store.set_value(iter, 1, &v);
    }

    fn spec(&self) -> view::Widget {
        view::Widget::Box(self.spec.borrow().clone())
    }

    fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone, Debug, GBoxed)]
#[gboxed(type_name = "NetidxEditorWidget")]
enum Widget {
    Action(Action),
    Table(Table),
    Label(Label),
    Button(Button),
    Toggle(Toggle),
    Selector(Selector),
    Entry(Entry),
    Box(BoxContainer),
    BoxChild(BoxChild),
    Grid,
    GridChild,
}

impl Widget {
    fn insert(
        ctx: &WidgetCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Widget,
    ) {
        match spec {
            view::Widget::Action(s) => Action::insert(ctx, on_change, store, iter, s),
            view::Widget::Table(s) => Table::insert(on_change, store, iter, s),
            view::Widget::Label(s) => Label::insert(ctx, on_change, store, iter, s),
            view::Widget::Button(s) => Button::insert(ctx, on_change, store, iter, s),
            view::Widget::Toggle(s) => Toggle::insert(ctx, on_change, store, iter, s),
            view::Widget::Selector(s) => Selector::insert(ctx, on_change, store, iter, s),
            view::Widget::Entry(s) => Entry::insert(ctx, on_change, store, iter, s),
            view::Widget::Box(s) => BoxContainer::insert(on_change, store, iter, s),
            view::Widget::BoxChild(s) => BoxChild::insert(on_change, store, iter, s),
            view::Widget::Grid(_) => todo!(),
            view::Widget::GridChild(_) => todo!(),
        }
    }

    fn spec(&self) -> view::Widget {
        match self {
            Widget::Action(w) => w.spec(),
            Widget::Table(w) => w.spec(),
            Widget::Label(w) => w.spec(),
            Widget::Button(w) => w.spec(),
            Widget::Toggle(w) => w.spec(),
            Widget::Selector(w) => w.spec(),
            Widget::Entry(w) => w.spec(),
            Widget::Box(w) => w.spec(),
            Widget::BoxChild(w) => w.spec(),
            Widget::Grid => todo!(),
            Widget::GridChild => todo!(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        match self {
            Widget::Action(w) => w.root(),
            Widget::Table(w) => w.root(),
            Widget::Label(w) => w.root(),
            Widget::Button(w) => w.root(),
            Widget::Toggle(w) => w.root(),
            Widget::Selector(w) => w.root(),
            Widget::Entry(w) => w.root(),
            Widget::Box(w) => w.root(),
            Widget::BoxChild(w) => w.root(),
            Widget::Grid => todo!(),
            Widget::GridChild => todo!(),
        }
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        match self {
            Widget::Action(w) => w.update(changed),
            Widget::Table(_) => (),
            Widget::Label(w) => w.update(changed),
            Widget::Button(w) => w.update(changed),
            Widget::Toggle(w) => w.update(changed),
            Widget::Selector(w) => w.update(changed),
            Widget::Entry(w) => w.update(changed),
            Widget::Box(w) => (),
            Widget::BoxChild(w) => (),
            Widget::Grid => todo!(),
            Widget::GridChild => todo!(),
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        match self {
            Widget::Action(w) => w.update_var(name, value),
            Widget::Table(_) => (),
            Widget::Label(w) => w.update_var(name, value),
            Widget::Button(w) => w.update_var(name, value),
            Widget::Toggle(w) => w.update_var(name, value),
            Widget::Selector(w) => w.update_var(name, value),
            Widget::Entry(w) => w.update_var(name, value),
            Widget::Box(w) => (),
            Widget::BoxChild(w) => (),
            Widget::Grid => todo!(),
            Widget::GridChild => todo!(),
        }
    }
}

pub(super) struct Editor {
    root: gtk::Box,
    store: gtk::TreeStore,
}

static KINDS: [&'static str; 11] = [
    "Action",
    "Table",
    "Label",
    "Button",
    "Toggle",
    "Selector",
    "Entry",
    "Box",
    "BoxChild",
    "Grid",
    "GridChild",
];

impl Editor {
    pub(super) fn new(ctx: WidgetCtx, spec: view::View) -> Editor {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let treewin =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        treewin.set_policy(gtk::PolicyType::Never, gtk::PolicyType::Automatic);
        root.pack_start(&treewin, true, true, 5);
        let view = gtk::TreeView::new();
        treewin.add(&view);
        view.append_column(&{
            let column = gtk::TreeViewColumn::new();
            let cell = gtk::CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("widget tree");
            column.add_attribute(&cell, "text", 0);
            column
        });
        let store = gtk::TreeStore::new(&[String::static_type(), Widget::static_type()]);
        view.set_model(Some(&store));
        view.set_reorderable(true);
        view.set_enable_tree_lines(true);
        let spec = Rc::new(RefCell::new(spec));
        let on_change: OnChange = Rc::new({
            let ctx = ctx.clone();
            let spec = Rc::clone(&spec);
            let store = store.clone();
            let scheduled = Rc::new(Cell::new(false));
            move || {
                if !scheduled.get() {
                    scheduled.set(true);
                    idle_add_local(clone!(
                        @strong ctx,
                        @strong spec,
                        @strong store,
                        @strong scheduled => move || {
                        if let Some(root) = store.get_iter_first() {
                            spec.borrow_mut().root = Editor::build_spec(&store, &root);
                            let m = FromGui::Render(spec.borrow().clone());
                            let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                        }
                        scheduled.set(false);
                        glib::Continue(false)
                    }));
                }
            }
        });
        Editor::build_tree(&ctx, &on_change, &store, None, &spec.borrow().root);
        let selected: Rc<RefCell<Option<gtk::TreeIter>>> = Rc::new(RefCell::new(None));
        let reveal_properties = gtk::Revealer::new();
        root.pack_end(&reveal_properties, false, false, 5);
        let properties = gtk::Box::new(gtk::Orientation::Vertical, 5);
        reveal_properties.add(&properties);
        let inhibit_change = Rc::new(Cell::new(false));
        let kind = gtk::ComboBoxText::new();
        for k in &KINDS {
            kind.append(Some(k), k);
        }
        kind.connect_changed(clone!(
            @strong on_change,
            @strong store,
            @strong selected,
            @strong ctx,
            @weak properties,
            @strong inhibit_change => move |c| {
            if let Some(iter) = selected.borrow().clone() {
                if !inhibit_change.get() {
                    let wv = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = wv.get::<&Widget>() {
                        properties.remove(w.root());
                    }
                    let id = c.get_active_id();
                    let spec = Editor::default_spec(id.as_ref().map(|s| &**s));
                    Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
                    let wv = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = wv.get::<&Widget>() {
                        properties.add(w.root());
                    }
                    on_change();
                }
            }
        }));
        properties.add(&kind);
        properties.add(&gtk::Separator::new(gtk::Orientation::Vertical));
        let selection = view.get_selection();
        selection.set_mode(gtk::SelectionMode::Single);
        selection.connect_changed(clone!(
        @strong ctx,
        @strong selected,
        @weak store,
        @weak kind,
        @weak reveal_properties,
        @weak properties,
        @strong inhibit_change => move |s| {
            let children = properties.get_children();
            if children.len() == 3 {
                properties.remove(&children[2]);
            }
            match s.get_selected() {
                None => {
                    *selected.borrow_mut() = None;
                    let _: result::Result<_, _> =
                        ctx.to_gui.send(ToGui::Highlight(vec![]));
                    reveal_properties.set_reveal_child(false);
                }
                Some((_, iter)) => {
                    *selected.borrow_mut() = Some(iter.clone());
                    let mut path = Vec::new();
                    Editor::build_widget_path(&store, &iter, 0, &mut path);
                    let _: result::Result<_,_> = ctx.to_gui.send(ToGui::Highlight(path));
                    let v = store.get_value(&iter, 0);
                    if let Ok(Some(id)) = v.get::<&str>() {
                        inhibit_change.set(true);
                        kind.set_active_id(Some(id));
                        inhibit_change.set(false);
                    }
                    let v = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = v.get::<&Widget>() {
                        properties.add(w.root());
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
        @strong on_change, @weak store, @strong selected, @strong ctx => move |_| {
            if let Some(iter) = &*selected.borrow() {
                let spec = Editor::build_spec(&store, iter);
                let parent = store.iter_parent(iter);
                Editor::build_tree(&ctx, &on_change, &store, parent.as_ref(), &spec);
                on_change()
            }
        }));
        new_sib.connect_activate(clone!(
            @strong on_change, @weak store, @strong selected, @strong ctx => move |_| {
            let iter = store.insert_after(None, selected.borrow().as_ref());
            let spec = Editor::default_spec(Some("Label"));
            Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
            on_change();
        }));
        new_child.connect_activate(clone!(
            @strong on_change, @weak store, @strong selected, @strong ctx => move |_| {
            let iter = store.insert_after(selected.borrow().as_ref(), None);
            let spec = Editor::default_spec(Some("Label"));
            Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
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
        Editor { root, store }
    }

    fn default_spec(name: Option<&str>) -> view::Widget {
        match name {
            None => view::Widget::Table(Path::from("/")),
            Some("Action") => view::Widget::Action(view::Action {
                source: view::Source::Constant(Value::U64(42)),
                sink: view::Sink::Leaf(view::SinkLeaf::Variable(String::from("foo"))),
            }),
            Some("Table") => view::Widget::Table(Path::from("/")),
            Some("Label") => {
                let s = Value::String(Chars::from("static label"));
                view::Widget::Label(view::Source::Constant(s))
            }
            Some("Button") => {
                let l = Chars::from("click me!");
                view::Widget::Button(view::Button {
                    enabled: view::Source::Constant(Value::True),
                    label: view::Source::Constant(Value::String(l)),
                    source: view::Source::Load(Path::from("/somewhere")),
                    sink: view::Sink::Leaf(view::SinkLeaf::Store(Path::from(
                        "/somewhere/else",
                    ))),
                })
            }
            Some("Toggle") => view::Widget::Toggle(view::Toggle {
                enabled: view::Source::Constant(Value::True),
                source: view::Source::Load(Path::from("/somewhere")),
                sink: view::Sink::Leaf(view::SinkLeaf::Store(Path::from(
                    "/somewhere/else",
                ))),
            }),
            Some("Selector") => {
                let choices =
                    Chars::from(r#"[[{"U64": 1}, "One"], [{"U64": 2}, "Two"]]"#);
                view::Widget::Selector(view::Selector {
                    enabled: view::Source::Constant(Value::True),
                    choices: view::Source::Constant(Value::String(choices)),
                    source: view::Source::Load(Path::from("/somewhere")),
                    sink: view::Sink::Leaf(view::SinkLeaf::Store(Path::from(
                        "/somewhere/else",
                    ))),
                })
            }
            Some("Entry") => view::Widget::Entry(view::Entry {
                enabled: view::Source::Constant(Value::True),
                visible: view::Source::Constant(Value::True),
                source: view::Source::Load(Path::from("/somewhere")),
                sink: view::Sink::Leaf(view::SinkLeaf::Store(Path::from(
                    "/somewhere/else",
                ))),
            }),
            Some("Box") => view::Widget::Box(view::Box {
                direction: view::Direction::Vertical,
                homogeneous: false,
                spacing: 0,
                children: Vec::new(),
            }),
            Some("BoxChild") => view::Widget::BoxChild(view::BoxChild {
                expand: false,
                fill: false,
                padding: 0,
                halign: None,
                valign: None,
                widget: boxed::Box::new(view::Widget::Label(view::Source::Constant(
                    Value::U64(42),
                ))),
            }),
            Some("Grid") => todo!(),
            Some("GridChild") => todo!(),
            _ => unreachable!(),
        }
    }

    fn build_tree(
        ctx: &WidgetCtx,
        on_change: &OnChange,
        store: &gtk::TreeStore,
        parent: Option<&gtk::TreeIter>,
        w: &view::Widget,
    ) {
        let iter = store.insert_before(parent, None);
        Widget::insert(ctx, on_change.clone(), store, &iter, w.clone());
        match w {
            view::Widget::Box(b) => {
                for w in &b.children {
                    Editor::build_tree(ctx, on_change, store, Some(&iter), w);
                }
            }
            view::Widget::BoxChild(b) => {
                Editor::build_tree(ctx, on_change, store, Some(&iter), &*b.widget)
            }
            view::Widget::Grid(_) => todo!(),
            view::Widget::GridChild(_) => todo!(),
            view::Widget::Action(_)
            | view::Widget::Table(_)
            | view::Widget::Label(_)
            | view::Widget::Button(_)
            | view::Widget::Toggle(_)
            | view::Widget::Selector(_)
            | view::Widget::Entry(_) => (),
        }
    }

    fn build_spec(store: &gtk::TreeStore, root: &gtk::TreeIter) -> view::Widget {
        let v = store.get_value(root, 1);
        match v.get::<&Widget>() {
            Err(e) => {
                let s = Value::String(Chars::from(format!("tree error: {}", e)));
                view::Widget::Label(view::Source::Constant(s))
            }
            Ok(None) => {
                let s = Value::String(Chars::from("tree error: missing widget"));
                view::Widget::Label(view::Source::Constant(s))
            }
            Ok(Some(w)) => match w.spec() {
                view::Widget::Box(mut b) => {
                    b.children.clear();
                    match store.iter_children(Some(root)) {
                        None => view::Widget::Box(b),
                        Some(iter) => {
                            loop {
                                b.children.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                            view::Widget::Box(b)
                        }
                    }
                }
                view::Widget::BoxChild(mut b) => match store.iter_children(Some(root)) {
                    None => view::Widget::BoxChild(b),
                    Some(iter) => {
                        b.widget = boxed::Box::new(Editor::build_spec(store, &iter));
                        view::Widget::BoxChild(b)
                    }
                },
                view::Widget::Grid(_) => todo!(),
                view::Widget::GridChild(_) => todo!(),
                w => w,
            },
        }
    }

    fn build_widget_path(
        store: &gtk::TreeStore,
        start: &gtk::TreeIter,
        nchild: usize,
        path: &mut Vec<WidgetPath>,
    ) {
        let v = store.get_value(start, 1);
        match v.get::<&Widget>() {
            Err(_) | Ok(None) => (),
            Ok(Some(w)) => match w {
                Widget::Action(_) => {
                    path.insert(0, WidgetPath::Leaf);
                }
                Widget::Table(_) => {
                    path.insert(0, WidgetPath::Leaf);
                }
                Widget::Label(_) => {
                    path.insert(0, WidgetPath::Leaf);
                }
                Widget::Button(_) => {
                    path.insert(0, WidgetPath::Leaf);
                }
                Widget::Toggle(_) => {
                    path.insert(0, WidgetPath::Leaf);
                }
                Widget::Selector(_) => {
                    path.insert(0, WidgetPath::Leaf);
                }
                Widget::Entry(_) => {
                    path.insert(0, WidgetPath::Leaf);
                }
                Widget::Box(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    } else {
                        path.insert(0, WidgetPath::Box(nchild));
                    }
                }
                Widget::BoxChild(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    }
                }
                Widget::Grid => todo!(),
                Widget::GridChild => todo!(),
            },
        }
        if let Some(parent) = store.iter_parent(start) {
            if let Some(idx) = store.get_path(start).map(|t| t.get_indices()) {
                if let Some(i) = idx.last() {
                    Editor::build_widget_path(store, &parent, *i as usize, path);
                }
            }
        }
    }

    pub(super) fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        self.store.foreach(|store, _, iter| {
            let v = store.get_value(iter, 1);
            match v.get::<&Widget>() {
                Err(_) | Ok(None) => false,
                Ok(Some(w)) => {
                    w.update(changed);
                    false
                }
            }
        })
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        self.store.foreach(|store, _, iter| {
            let v = store.get_value(iter, 1);
            match v.get::<&Widget>() {
                Err(_) | Ok(None) => false,
                Ok(Some(w)) => {
                    w.update_var(name, value);
                    false
                }
            }
        })
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
