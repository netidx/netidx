mod expr_inspector;
mod util;
mod widgets;
use super::{default_view, BSCtx, WidgetPath, DEFAULT_PROPS};
use glib::{clone, idle_add_local, prelude::*, subclass::prelude::*, GString};
use gtk::{self, prelude::*};
use netidx::{chars::Chars, path::Path, subscriber::Value};
use netidx_bscript::expr;
use netidx_protocols::view;
use std::{
    boxed,
    cell::{Cell, RefCell},
    rc::Rc,
};
use util::{parse_entry, TwoColGrid};

type OnChange = Rc<dyn Fn()>;

#[derive(Debug, Clone)]
struct WidgetProps {
    root: gtk::Expander,
    spec: Rc<RefCell<Option<view::WidgetProps>>>,
}

impl WidgetProps {
    fn new(on_change: OnChange, spec: Option<view::WidgetProps>) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Expander::new(Some("Layout Properties"));
        let on_change = Rc::new({
            let spec = spec.clone();
            move || {
                let default = spec.borrow().as_ref() == Some(&DEFAULT_PROPS);
                if default {
                    *spec.borrow_mut() = None;
                }
                on_change()
            }
        });
        util::expander_touch_enable(&root);
        let mut grid = TwoColGrid::new();
        root.add(grid.root());
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
        grid.add((halign_lbl.clone(), halign.clone()));
        grid.add((valign_lbl.clone(), valign.clone()));
        for a in &aligns {
            halign.append(Some(a), a);
            valign.append(Some(a), a);
        }
        halign.set_active_id(Some(align_to_str(
            spec.borrow().unwrap_or(DEFAULT_PROPS).halign,
        )));
        valign.set_active_id(Some(align_to_str(
            spec.borrow().unwrap_or(DEFAULT_PROPS).valign,
        )));
        halign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS);
                spec.halign =
                    c.get_active_id().map(align_from_str).unwrap_or(view::Align::Fill);
            }
            on_change()
        }));
        valign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS);
                spec.valign =
                    c.get_active_id().map(align_from_str).unwrap_or(view::Align::Fill);
            }
            on_change()
        }));
        let hexp = gtk::CheckButton::with_label("Expand Horizontally");
        grid.attach(&hexp, 0, 2, 1);
        hexp.connect_toggled(clone!(@strong spec, @strong on_change => move |b| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS);
                spec.hexpand = b.get_active();
            }
            on_change()
        }));
        let vexp = gtk::CheckButton::with_label("Expand Vertically");
        grid.attach(&vexp, 0, 2, 1);
        vexp.connect_toggled(clone!(@strong spec, @strong on_change => move |b| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS);
                spec.vexpand = b.get_active();
            }
            on_change()
        }));
        grid.add(parse_entry(
            "Top Margin:",
            &spec.borrow().unwrap_or(DEFAULT_PROPS).margin_top,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS);
                    spec.margin_top = s;
                }
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "Bottom Margin:",
            &spec.borrow().unwrap_or(DEFAULT_PROPS).margin_bottom,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS);
                    spec.margin_bottom = s;
                }
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "Start Margin:",
            &spec.borrow().unwrap_or(DEFAULT_PROPS).margin_start,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS);
                    spec.margin_start = s;
                }
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "End Margin:",
            &spec.borrow().unwrap_or(DEFAULT_PROPS).margin_end,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS);
                    spec.margin_end = s;
                }
                on_change()
            }),
        ));
        WidgetProps { root, spec }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn spec(&self) -> Option<view::WidgetProps> {
        *self.spec.borrow()
    }
}

#[derive(Clone, Debug)]
enum WidgetKind {
    Action(widgets::Action),
    Table(widgets::Table),
    Label(widgets::Label),
    Button(widgets::Button),
    LinkButton(widgets::LinkButton),
    Toggle(widgets::Toggle),
    Selector(widgets::Selector),
    Entry(widgets::Entry),
    LinePlot(widgets::LinePlot),
    Box(widgets::BoxContainer),
    BoxChild(widgets::BoxChild),
    Grid(widgets::Grid),
    GridChild(widgets::GridChild),
    GridRow,
}

impl WidgetKind {
    fn root(&self) -> Option<&gtk::Widget> {
        match self {
            WidgetKind::Action(w) => Some(w.root()),
            WidgetKind::Table(w) => Some(w.root()),
            WidgetKind::Label(w) => Some(w.root()),
            WidgetKind::Button(w) => Some(w.root()),
            WidgetKind::LinkButton(w) => Some(w.root()),
            WidgetKind::Toggle(w) => Some(w.root()),
            WidgetKind::Selector(w) => Some(w.root()),
            WidgetKind::Entry(w) => Some(w.root()),
            WidgetKind::LinePlot(w) => Some(w.root()),
            WidgetKind::Box(w) => Some(w.root()),
            WidgetKind::BoxChild(w) => Some(w.root()),
            WidgetKind::Grid(w) => Some(w.root()),
            WidgetKind::GridChild(w) => Some(w.root()),
            WidgetKind::GridRow => None,
        }
    }
}

#[derive(Clone, Debug, GBoxed)]
#[gboxed(type_name = "NetidxEditorWidget")]
struct Widget {
    root: gtk::Box,
    props: Option<WidgetProps>,
    kind: WidgetKind,
}

impl Widget {
    fn insert(
        ctx: &BSCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Widget,
    ) {
        let (name, kind, props) = match spec {
            view::Widget { props: _, kind: view::WidgetKind::Action(s) } => (
                "Action",
                WidgetKind::Action(widgets::Action::new(
                    ctx,
                    on_change.clone(),
                    store,
                    iter,
                    s,
                )),
                None,
            ),
            view::Widget { props, kind: view::WidgetKind::Table(s) } => (
                "Table",
                WidgetKind::Table(widgets::Table::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Label(s) } => (
                "Label",
                WidgetKind::Label(widgets::Label::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Button(s) } => (
                "Button",
                WidgetKind::Button(widgets::Button::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::LinkButton(s) } => (
                "LinkButton",
                WidgetKind::LinkButton(widgets::LinkButton::new(
                    ctx,
                    on_change.clone(),
                    s,
                )),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::CheckButton(_) } => {
                unimplemented!()
            }
            view::Widget { props: _, kind: view::WidgetKind::ToggleButton(_) } => {
                unimplemented!()
            }
            view::Widget { props, kind: view::WidgetKind::Toggle(s) } => (
                "Toggle",
                WidgetKind::Toggle(widgets::Toggle::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Selector(s) } => (
                "Selector",
                WidgetKind::Selector(widgets::Selector::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Entry(s) } => (
                "Entry",
                WidgetKind::Entry(widgets::Entry::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::Frame(_) } => {
                unimplemented!()
            }
            view::Widget { props, kind: view::WidgetKind::Box(s) } => (
                "Box",
                WidgetKind::Box(widgets::BoxContainer::new(on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::BoxChild(s) } => (
                "BoxChild",
                WidgetKind::BoxChild(widgets::BoxChild::new(on_change, s)),
                None,
            ),
            view::Widget { props, kind: view::WidgetKind::Grid(s) } => (
                "Grid",
                WidgetKind::Grid(widgets::Grid::new(on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::GridChild(s) } => (
                "GridChild",
                WidgetKind::GridChild(widgets::GridChild::new(on_change, s)),
                None,
            ),
            view::Widget { props: _, kind: view::WidgetKind::GridRow(_) } => {
                ("GridRow", WidgetKind::GridRow, None)
            }
            view::Widget { props: _, kind: view::WidgetKind::Pane(_) } => {
                unimplemented!()
            }
            view::Widget { props: _, kind: view::WidgetKind::Stack(_) } => {
                unimplemented!()
            }
            view::Widget { props, kind: view::WidgetKind::LinePlot(s) } => (
                "LinePlot",
                WidgetKind::LinePlot(widgets::LinePlot::new(ctx, on_change.clone(), s)),
                Some(WidgetProps::new(on_change, props)),
            ),
        };
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        if let Some(p) = props.as_ref() {
            root.pack_start(p.root(), false, false, 0);
        }
        if let Some(r) = kind.root() {
            root.pack_start(r, true, true, 0);
        }
        store.set_value(iter, 0, &name.to_value());
        root.set_sensitive(false);
        let t = Widget { root, props, kind };
        store.set_value(iter, 1, &t.to_value());
    }

    fn spec(&self) -> view::Widget {
        let props = self.props.as_ref().and_then(|p| p.spec());
        let kind = match &self.kind {
            WidgetKind::Action(w) => w.spec(),
            WidgetKind::Table(w) => w.spec(),
            WidgetKind::Label(w) => w.spec(),
            WidgetKind::Button(w) => w.spec(),
            WidgetKind::LinkButton(w) => w.spec(),
            WidgetKind::Toggle(w) => w.spec(),
            WidgetKind::Selector(w) => w.spec(),
            WidgetKind::Entry(w) => w.spec(),
            WidgetKind::LinePlot(w) => w.spec(),
            WidgetKind::Box(w) => w.spec(),
            WidgetKind::BoxChild(w) => w.spec(),
            WidgetKind::Grid(w) => w.spec(),
            WidgetKind::GridChild(w) => w.spec(),
            WidgetKind::GridRow => {
                view::WidgetKind::GridRow(view::GridRow { columns: vec![] })
            }
        };
        view::Widget { props, kind }
    }

    fn default_spec(name: Option<&str>) -> view::Widget {
        fn widget(kind: view::WidgetKind) -> view::Widget {
            view::Widget { kind, props: None }
        }
        fn table() -> view::Widget {
            default_view(Path::from("/")).root
        }
        match name {
            None => table(),
            Some("Action") => widget(view::WidgetKind::Action(
                expr::ExprKind::Constant(Value::U64(42)).to_expr(),
            )),
            Some("Table") => table(),
            Some("Label") => {
                let s = Value::String(Chars::from("static label"));
                widget(view::WidgetKind::Label(expr::ExprKind::Constant(s).to_expr()))
            }
            Some("Button") => {
                let l = Chars::from("click me!");
                widget(view::WidgetKind::Button(view::Button {
                    enabled: expr::ExprKind::Constant(Value::True).to_expr(),
                    label: expr::ExprKind::Constant(Value::String(l)).to_expr(),
                    on_click: expr::ExprKind::Apply {
                        args: vec![
                            expr::ExprKind::Constant(Value::from("/somewhere/in/netidx"))
                                .to_expr(),
                            expr::ExprKind::Apply {
                                args: vec![],
                                function: "event".into(),
                            }
                            .to_expr(),
                        ],
                        function: "store".into(),
                    }
                    .to_expr(),
                }))
            }
            Some("LinkButton") => {
                let u = Chars::from("file:///");
                let l = Chars::from("click me!");
                widget(view::WidgetKind::LinkButton(view::LinkButton {
                    enabled: expr::ExprKind::Constant(Value::True).to_expr(),
                    uri: expr::ExprKind::Constant(Value::String(u)).to_expr(),
                    label: expr::ExprKind::Constant(Value::String(l)).to_expr(),
                    on_activate_link: expr::ExprKind::Constant(Value::Null).to_expr(),
                }))
            }
            Some("Toggle") => widget(view::WidgetKind::Toggle(view::Toggle {
                enabled: expr::ExprKind::Constant(Value::True).to_expr(),
                value: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Constant(Value::from("/somewhere")).to_expr()
                    ],
                    function: "load".into(),
                }
                .to_expr(),
                on_change: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Constant(Value::from("/somewhere")).to_expr(),
                        expr::ExprKind::Apply { args: vec![], function: "event".into() }
                            .to_expr(),
                    ],
                    function: "store".into(),
                }
                .to_expr(),
            })),
            Some("Selector") => {
                let choices =
                    Chars::from(r#"[[{"U64": 1}, "One"], [{"U64": 2}, "Two"]]"#);
                widget(view::WidgetKind::Selector(view::Selector {
                    enabled: expr::ExprKind::Constant(Value::True).to_expr(),
                    choices: expr::ExprKind::Constant(Value::String(choices)).to_expr(),
                    selected: expr::ExprKind::Apply {
                        args: vec![
                            expr::ExprKind::Constant(Value::from("/somewhere")).to_expr()
                        ],
                        function: "load".into(),
                    }
                    .to_expr(),
                    on_change: expr::ExprKind::Apply {
                        args: vec![
                            expr::ExprKind::Constant(Value::from("/somewhere")).to_expr(),
                            expr::ExprKind::Apply {
                                args: vec![],
                                function: "event".into(),
                            }
                            .to_expr(),
                        ],
                        function: "store".into(),
                    }
                    .to_expr(),
                }))
            }
            Some("Entry") => widget(view::WidgetKind::Entry(view::Entry {
                enabled: expr::ExprKind::Constant(Value::True).to_expr(),
                visible: expr::ExprKind::Constant(Value::True).to_expr(),
                text: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Constant(Value::from("/somewhere")).to_expr()
                    ],
                    function: "load".into(),
                }
                .to_expr(),
                on_change: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Apply { args: vec![], function: "event".into() }
                            .to_expr(),
                        expr::ExprKind::Constant(Value::True).to_expr(),
                    ],
                    function: "sample".into(),
                }
                .to_expr(),
                on_activate: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Constant(Value::from("/somewhere")).to_expr(),
                        expr::ExprKind::Apply { args: vec![], function: "event".into() }
                            .to_expr(),
                    ],
                    function: "store".into(),
                }
                .to_expr(),
            })),
            Some("LinePlot") => widget(view::WidgetKind::LinePlot(view::LinePlot {
                title: String::from("Line Plot"),
                x_label: String::from("x axis"),
                y_label: String::from("y axis"),
                x_labels: 4,
                y_labels: 4,
                x_grid: true,
                y_grid: true,
                fill: Some(view::RGB { r: 1., g: 1., b: 1. }),
                margin: 3,
                label_area: 50,
                x_min: expr::ExprKind::Constant(Value::Null).to_expr(),
                x_max: expr::ExprKind::Constant(Value::Null).to_expr(),
                y_min: expr::ExprKind::Constant(Value::Null).to_expr(),
                y_max: expr::ExprKind::Constant(Value::Null).to_expr(),
                keep_points: expr::ExprKind::Constant(Value::U64(256)).to_expr(),
                series: Vec::new(),
            })),
            Some("Box") => widget(view::WidgetKind::Box(view::Box {
                direction: view::Direction::Vertical,
                homogeneous: false,
                spacing: 0,
                children: Vec::new(),
            })),
            Some("BoxChild") => {
                let s = Value::String(Chars::from("empty box child"));
                let w = view::Widget {
                    kind: view::WidgetKind::Label(expr::ExprKind::Constant(s).to_expr()),
                    props: None,
                };
                widget(view::WidgetKind::BoxChild(view::BoxChild {
                    pack: view::Pack::Start,
                    padding: 0,
                    widget: boxed::Box::new(w),
                }))
            }
            Some("Grid") => widget(view::WidgetKind::Grid(view::Grid {
                homogeneous_columns: false,
                homogeneous_rows: false,
                column_spacing: 0,
                row_spacing: 0,
                rows: Vec::new(),
            })),
            Some("GridChild") => {
                let s = Value::String(Chars::from("empty grid child"));
                let w = view::Widget {
                    kind: view::WidgetKind::Label(expr::ExprKind::Constant(s).to_expr()),
                    props: None,
                };
                widget(view::WidgetKind::GridChild(view::GridChild {
                    width: 1,
                    height: 1,
                    widget: boxed::Box::new(w),
                }))
            }
            Some("GridRow") => {
                widget(view::WidgetKind::GridRow(view::GridRow { columns: vec![] }))
            }
            _ => unreachable!(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn moved(&self, iter: &gtk::TreeIter) {
        match &self.kind {
            WidgetKind::Action(w) => w.moved(iter),
            WidgetKind::Table(_)
            | WidgetKind::Label(_)
            | WidgetKind::Button(_)
            | WidgetKind::LinkButton(_)
            | WidgetKind::Toggle(_)
            | WidgetKind::Selector(_)
            | WidgetKind::Entry(_)
            | WidgetKind::LinePlot(_)
            | WidgetKind::Box(_)
            | WidgetKind::BoxChild(_)
            | WidgetKind::Grid(_)
            | WidgetKind::GridChild(_)
            | WidgetKind::GridRow => (),
        }
    }
}

static KINDS: [&'static str; 14] = [
    "Action",
    "Table",
    "Label",
    "Button",
    "LinkButton",
    "Toggle",
    "Selector",
    "Entry",
    "LinePlot",
    "Box",
    "BoxChild",
    "Grid",
    "GridChild",
    "GridRow",
];

pub(super) struct Editor {
    root: gtk::Paned,
}

impl Editor {
    pub(super) fn new(ctx: BSCtx, spec: view::View) -> Editor {
        let root = gtk::Paned::new(gtk::Orientation::Vertical);
        idle_add_local(
            clone!(@weak root => @default-return glib::Continue(false), move || {
                root.set_position(root.get_allocated_height() / 2);
                glib::Continue(false)
            }),
        );
        root.set_margin_start(5);
        root.set_margin_end(5);
        let root_upper = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let win_lower =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        win_lower.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        let root_lower = gtk::Box::new(gtk::Orientation::Vertical, 5);
        win_lower.add(&root_lower);
        root.add1(&root_upper);
        root.add2(&win_lower);
        let treebtns = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        root_upper.pack_start(&treebtns, false, false, 0);
        let addbtnicon = gtk::Image::from_icon_name(
            Some("list-add-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let addbtn = gtk::ToolButton::new(Some(&addbtnicon), None);
        let addchbtnicon = gtk::Image::from_icon_name(
            Some("go-down-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let addchbtn = gtk::ToolButton::new(Some(&addchbtnicon), None);
        let delbtnicon = gtk::Image::from_icon_name(
            Some("list-remove-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let delbtn = gtk::ToolButton::new(Some(&delbtnicon), None);
        let dupbtnicon = gtk::Image::from_icon_name(
            Some("edit-copy-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let dupbtn = gtk::ToolButton::new(Some(&dupbtnicon), None);
        let undobtnicon = gtk::Image::from_icon_name(
            Some("edit-undo-symbolic"),
            gtk::IconSize::SmallToolbar,
        );
        let undobtn = gtk::ToolButton::new(Some(&undobtnicon), None);
        treebtns.pack_start(&addbtn, false, false, 5);
        treebtns.pack_start(&addchbtn, false, false, 5);
        treebtns.pack_start(&delbtn, false, false, 5);
        treebtns.pack_start(&dupbtn, false, false, 5);
        treebtns.pack_start(&undobtn, false, false, 5);
        let treewin =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        treewin.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        root_upper.pack_start(&treewin, true, true, 5);
        let view = gtk::TreeView::new();
        treewin.add(&view);
        view.append_column(&{
            let column = gtk::TreeViewColumn::new();
            let cell = gtk::CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("widget name");
            column.add_attribute(&cell, "text", 0);
            column
        });
        view.append_column(&{
            let column = gtk::TreeViewColumn::new();
            let cell = gtk::CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("desc");
            column.add_attribute(&cell, "text", 2);
            column
        });
        let store = gtk::TreeStore::new(&[
            String::static_type(),
            Widget::static_type(),
            String::static_type(),
        ]);
        view.set_model(Some(&store));
        view.set_reorderable(true);
        view.set_enable_tree_lines(true);
        let spec = Rc::new(RefCell::new(spec));
        let undo_stack: Rc<RefCell<Vec<view::View>>> = Rc::new(RefCell::new(Vec::new()));
        let undoing = Rc::new(Cell::new(false));
        let on_change: OnChange = Rc::new({
            let ctx = ctx.clone();
            let spec = Rc::clone(&spec);
            let store = store.clone();
            let scheduled = Rc::new(Cell::new(false));
            let undo_stack = undo_stack.clone();
            let undoing = undoing.clone();
            move || {
                if !scheduled.get() {
                    scheduled.set(true);
                    idle_add_local(clone!(
                    @strong ctx,
                    @strong spec,
                    @strong store,
                    @strong scheduled,
                    @strong undo_stack,
                    @strong undoing => move || {
                        if let Some(root) = store.get_iter_first() {
                            if undoing.get() {
                                undoing.set(false)
                            } else {
                                undo_stack.borrow_mut().push(spec.borrow().clone());
                            }
                            spec.borrow_mut().root = Editor::build_spec(&store, &root);
                            ctx.user.backend.render(spec.borrow().clone());
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
        root_lower.pack_start(&reveal_properties, true, true, 5);
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
                        w.root().hide();
                        w.root().set_sensitive(false);
                        properties.remove(w.root());
                    }
                    let id = c.get_active_id();
                    let spec = Widget::default_spec(id.as_ref().map(|s| &**s));
                    Widget::insert(
                        &ctx,
                        on_change.clone(),
                        &store,
                        &iter,
                        spec
                    );
                    let wv = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = wv.get::<&Widget>() {
                        properties.pack_start(w.root(), true, true, 5);
                        w.root().set_sensitive(true);
                        w.root().grab_focus();
                    }
                    properties.show_all();
                    on_change();
                }
            }
        }));
        properties.pack_start(&kind, false, false, 0);
        properties.pack_start(
            &gtk::Separator::new(gtk::Orientation::Vertical),
            false,
            false,
            0,
        );
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
            {
                let children = properties.get_children();
                if children.len() == 3 {
                    children[2].hide();
                    children[2].set_sensitive(false);
                    properties.remove(&children[2]);
                }
            }
            match s.get_selected() {
                None => {
                    *selected.borrow_mut() = None;
                    ctx.user.backend.highlight(vec![]);
                    reveal_properties.set_reveal_child(false);
                }
                Some((_, iter)) => {
                    *selected.borrow_mut() = Some(iter.clone());
                    let mut path = Vec::new();
                    Editor::build_widget_path(&store, &iter, 0, 0, &mut path);
                    ctx.user.backend.highlight(path);
                    let v = store.get_value(&iter, 0);
                    if let Ok(Some(id)) = v.get::<&str>() {
                        inhibit_change.set(true);
                        kind.set_active_id(Some(id));
                        inhibit_change.set(false);
                    }
                    let v = store.get_value(&iter, 1);
                    if let Ok(Some(w)) = v.get::<&Widget>() {
                        properties.pack_start(w.root(), true, true, 5);
                        w.root().set_sensitive(true);
                        w.root().grab_focus();
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
        let undo = gtk::MenuItem::with_label("Undo");
        menu.append(&duplicate);
        menu.append(&new_sib);
        menu.append(&new_child);
        menu.append(&delete);
        menu.append(&undo);
        let dup = Rc::new(clone!(
            @strong on_change,
            @weak store,
            @strong selected,
            @strong ctx => move || {
            if let Some(iter) = &*selected.borrow() {
                let spec = Editor::build_spec(&store, iter);
                let parent = store.iter_parent(iter);
                Editor::build_tree(
                    &ctx,
                    &on_change,
                    &store,
                    parent.as_ref(),
                    &spec
                );
                on_change()
            }
        }));
        duplicate.connect_activate(clone!(@strong dup => move |_| dup()));
        dupbtn.connect_clicked(clone!(@strong dup => move |_| dup()));
        let newsib = Rc::new(clone!(
            @strong on_change,
            @weak store,
            @strong selected,
            @strong ctx => move || {
            let iter = store.insert_after(None, selected.borrow().as_ref());
            let spec = Widget::default_spec(Some("Label"));
            Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
            on_change();
        }));
        new_sib.connect_activate(clone!(@strong newsib => move |_| newsib()));
        addbtn.connect_clicked(clone!(@strong newsib => move |_| newsib()));
        let newch = Rc::new(clone!(
            @strong on_change,
            @weak store,
            @strong selected,
            @strong ctx => move || {
            let iter = store.insert_after(selected.borrow().as_ref(), None);
            let spec = Widget::default_spec(Some("Label"));
            Widget::insert(&ctx, on_change.clone(), &store, &iter, spec);
            on_change();
        }));
        new_child.connect_activate(clone!(@strong newch => move |_| newch()));
        addchbtn.connect_clicked(clone!(@strong newch => move |_| newch()));
        let del = Rc::new(clone!(
            @weak selection, @strong on_change, @weak store, @strong selected => move || {
            let iter = selected.borrow().clone();
            if let Some(iter) = iter {
                selection.unselect_iter(&iter);
                store.remove(&iter);
                on_change();
            }
        }));
        delete.connect_activate(clone!(@strong del => move |_| del()));
        delbtn.connect_clicked(clone!(@strong del => move |_| del()));
        let und = Rc::new(clone!(
            @weak store,
            @strong undo_stack,
            @strong spec,
            @strong selected,
            @weak selection,
            @strong on_change,
            @strong undoing => move || {
                let s = undo_stack.borrow_mut().pop();
                if let Some(s) = s {
                    undoing.set(true);
                    let iter = selected.borrow().clone();
                    if let Some(iter) = iter {
                        selection.unselect_iter(&iter);
                    }
                    store.clear();
                    *spec.borrow_mut() = s.clone();
                    Editor::build_tree(
                        &ctx,
                        &on_change,
                        &store,
                        None,
                        &s.root
                    );
                    on_change();
                }
        }));
        undo.connect_activate(clone!(@strong und => move |_| und()));
        undobtn.connect_clicked(clone!(@strong und => move |_| und()));
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
        store.connect_row_inserted(clone!(
        @strong store, @strong on_change => move |_, _, iter| {
            idle_add_local(clone!(@strong store, @strong iter => move || {
                let v = store.get_value(&iter, 1);
                match v.get::<&Widget>() {
                    Err(_) | Ok(None) => (),
                    Ok(Some(w)) => w.moved(&iter),
                }
                glib::Continue(false)
            }));
            on_change();
        }));
        Editor { root }
    }

    fn build_tree(
        ctx: &BSCtx,
        on_change: &OnChange,
        store: &gtk::TreeStore,
        parent: Option<&gtk::TreeIter>,
        w: &view::Widget,
    ) {
        let iter = store.insert_before(parent, None);
        Widget::insert(ctx, on_change.clone(), store, &iter, w.clone());
        match &w.kind {
            view::WidgetKind::Box(b) => {
                for w in &b.children {
                    Editor::build_tree(ctx, on_change, store, Some(&iter), w);
                }
            }
            view::WidgetKind::BoxChild(b) => {
                Editor::build_tree(ctx, on_change, store, Some(&iter), &*b.widget)
            }
            view::WidgetKind::Grid(g) => {
                for w in &g.rows {
                    Editor::build_tree(ctx, on_change, store, Some(&iter), w);
                }
            }
            view::WidgetKind::GridChild(g) => {
                Editor::build_tree(ctx, on_change, store, Some(&iter), &*g.widget)
            }
            view::WidgetKind::GridRow(g) => {
                for w in &g.columns {
                    Editor::build_tree(ctx, on_change, store, Some(&iter), w);
                }
            }
            view::WidgetKind::Action(_)
            | view::WidgetKind::Table(_)
            | view::WidgetKind::Label(_)
            | view::WidgetKind::Button(_)
            | view::WidgetKind::LinkButton(_)
            | view::WidgetKind::CheckButton(_)
            | view::WidgetKind::ToggleButton(_)
            | view::WidgetKind::Toggle(_)
            | view::WidgetKind::Selector(_)
            | view::WidgetKind::Entry(_)
            | view::WidgetKind::Frame(_)
            | view::WidgetKind::Pane(_)
            | view::WidgetKind::Stack(_)
            | view::WidgetKind::LinePlot(_) => (),
        }
    }

    fn build_spec(store: &gtk::TreeStore, root: &gtk::TreeIter) -> view::Widget {
        let v = store.get_value(root, 1);
        match v.get::<&Widget>() {
            Err(e) => {
                let s = Value::from(format!("tree error: {}", e));
                view::Widget {
                    kind: view::WidgetKind::Label(expr::ExprKind::Constant(s).to_expr()),
                    props: None,
                }
            }
            Ok(None) => {
                let s = Value::from("tree error: missing widget");
                view::Widget {
                    kind: view::WidgetKind::Label(expr::ExprKind::Constant(s).to_expr()),
                    props: None,
                }
            }
            Ok(Some(w)) => {
                let mut spec = w.spec();
                match &mut spec.kind {
                    view::WidgetKind::Box(ref mut b) => {
                        b.children.clear();
                        if let Some(iter) = store.iter_children(Some(root)) {
                            loop {
                                b.children.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                        }
                    }
                    view::WidgetKind::BoxChild(ref mut b) => {
                        if let Some(iter) = store.iter_children(Some(root)) {
                            b.widget = boxed::Box::new(Editor::build_spec(store, &iter));
                        }
                    }
                    view::WidgetKind::GridChild(ref mut g) => {
                        if let Some(iter) = store.iter_children(Some(root)) {
                            g.widget = boxed::Box::new(Editor::build_spec(store, &iter));
                        }
                    }
                    view::WidgetKind::Grid(ref mut g) => {
                        g.rows.clear();
                        if let Some(iter) = store.iter_children(Some(root)) {
                            loop {
                                g.rows.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                        }
                    }
                    view::WidgetKind::GridRow(ref mut g) => {
                        g.columns.clear();
                        if let Some(iter) = store.iter_children(Some(root)) {
                            loop {
                                g.columns.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                        }
                    }
                    _ => (),
                };
                spec
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
        let v = store.get_value(start, 1);
        let skip_idx = match v.get::<&Widget>() {
            Err(_) | Ok(None) => false,
            Ok(Some(w)) => match &w.kind {
                WidgetKind::Action(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Table(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Label(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Button(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::LinkButton(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Toggle(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Selector(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Entry(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::LinePlot(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Box(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    } else {
                        path.insert(0, WidgetPath::Box(nchild));
                    }
                    false
                }
                WidgetKind::BoxChild(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    }
                    false
                }
                WidgetKind::Grid(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf)
                    } else {
                        match path[0] {
                            WidgetPath::GridRow(_) => (),
                            WidgetPath::GridItem(_, _) => (),
                            _ => path.insert(0, WidgetPath::GridItem(nrow, nchild)),
                        }
                    }
                    false
                }
                WidgetKind::GridChild(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    }
                    false
                }
                WidgetKind::GridRow => {
                    if let Some(idx) = store.get_path(start).map(|t| t.get_indices()) {
                        if let Some(i) = idx.last() {
                            nrow = *i as usize;
                        }
                    }
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::GridRow(nrow));
                    } else {
                        path.insert(0, WidgetPath::GridItem(nrow, nchild));
                    }
                    true
                }
            },
        };
        if let Some(parent) = store.iter_parent(start) {
            if let Some(idx) = store.get_path(start).map(|t| t.get_indices()) {
                if let Some(i) = idx.last() {
                    let nchild = if skip_idx { nchild } else { *i as usize };
                    Editor::build_widget_path(store, &parent, nrow, nchild, path);
                }
            }
        };
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
