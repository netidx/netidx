mod completion;
mod expr_inspector;
mod util;
mod widgets;
use super::{default_view, BSCtx, WidgetPath, DEFAULT_PROPS};
use glib::{clone, idle_add_local, prelude::*, GString};
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
type Scope = Rc<RefCell<Path>>;

fn ce(v: Value) -> expr::Expr {
    expr::ExprKind::Constant(v).to_expr()
}

fn label_with_txt(text: &'static str) -> view::Widget {
    view::Widget {
        kind: view::WidgetKind::Label(view::Label {
            text: ce(Value::String(Chars::from(text))),
            width: ce(Value::Null),
            ellipsize: ce(Value::Null),
            selectable: ce(Value::True),
            single_line: ce(Value::True),
        }),
        props: None,
    }
}

#[derive(Clone)]
struct WidgetProps {
    root: gtk::Expander,
    _dbg_sensitive: widgets::DbgExpr,
    _dbg_visible: widgets::DbgExpr,
    spec: Rc<RefCell<Option<view::WidgetProps>>>,
}

impl WidgetProps {
    fn new(
        ctx: &BSCtx,
        scope: Scope,
        on_change: OnChange,
        spec: Option<view::WidgetProps>,
    ) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Expander::new(Some("Common Properties"));
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
                x => unreachable!("{}", x),
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
            spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).halign,
        )));
        valign.set_active_id(Some(align_to_str(
            spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).valign,
        )));
        halign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                spec.halign =
                    c.active_id().map(align_from_str).unwrap_or(view::Align::Fill);
            }
            on_change()
        }));
        valign.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                spec.valign =
                    c.active_id().map(align_from_str).unwrap_or(view::Align::Fill);
            }
            on_change()
        }));
        let hexp = gtk::CheckButton::with_label("Expand Horizontally");
        grid.attach(&hexp, 0, 2, 1);
        hexp.connect_toggled(clone!(@strong spec, @strong on_change => move |b| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                spec.hexpand = b.is_active();
            }
            on_change()
        }));
        let vexp = gtk::CheckButton::with_label("Expand Vertically");
        grid.attach(&vexp, 0, 2, 1);
        vexp.connect_toggled(clone!(@strong spec, @strong on_change => move |b| {
            {
                let mut spec = spec.borrow_mut();
                let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                spec.vexpand = b.is_active();
            }
            on_change()
        }));
        grid.add(parse_entry(
            "Top Margin:",
            &spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).margin_top,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                    spec.margin_top = s;
                }
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "Bottom Margin:",
            &spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).margin_bottom,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                    spec.margin_bottom = s;
                }
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "Start Margin:",
            &spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).margin_start,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                    spec.margin_start = s;
                }
                on_change()
            }),
        ));
        grid.add(parse_entry(
            "End Margin:",
            &spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).margin_end,
            clone!(@strong spec, @strong on_change => move |s| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                    spec.margin_end = s;
                }
                on_change()
            }),
        ));
        let (l, e, _dbg_sensitive) = widgets::expr(
            ctx,
            "Sensitive:",
            scope.clone(),
            &spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).sensitive,
            clone!(@strong spec, @strong on_change => move |e| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                    spec.sensitive = e;
                }
                on_change()
            }),
        );
        grid.add((l, e));
        let (l, e, _dbg_visible) = widgets::expr(
            ctx,
            "Visible:",
            scope.clone(),
            &spec.borrow().as_ref().unwrap_or(&DEFAULT_PROPS).visible,
            clone!(@strong spec, @strong on_change => move |e| {
                {
                    let mut spec = spec.borrow_mut();
                    let spec = spec.get_or_insert(DEFAULT_PROPS.clone());
                    spec.visible = e;
                }
                on_change()
            }),
        );
        grid.add((l, e));
        WidgetProps { root, spec, _dbg_sensitive, _dbg_visible }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn spec(&self) -> Option<view::WidgetProps> {
        self.spec.borrow().clone()
    }
}

#[derive(Clone)]
enum WidgetKind {
    BScript(widgets::BScript),
    Table(widgets::Table),
    Image(widgets::Image),
    Label(widgets::Label),
    Button(widgets::Button),
    LinkButton(widgets::LinkButton),
    ToggleButton(widgets::ToggleButton),
    CheckButton(widgets::ToggleButton),
    Scale(widgets::Scale),
    ProgressBar(widgets::ProgressBar),
    Switch(widgets::Switch),
    ComboBox(widgets::ComboBox),
    RadioButton(widgets::RadioButton),
    Entry(widgets::Entry),
    LinePlot(widgets::LinePlot),
    Frame(widgets::Frame),
    Box(widgets::BoxContainer),
    BoxChild(widgets::BoxChild),
    Grid(widgets::Grid),
    GridChild(widgets::GridChild),
    Paned(widgets::Paned),
    Notebook(widgets::Notebook),
    NotebookPage(widgets::NotebookPage),
    GridRow,
}

impl WidgetKind {
    fn root(&self) -> Option<&gtk::Widget> {
        match self {
            WidgetKind::BScript(w) => Some(w.root()),
            WidgetKind::Table(w) => Some(w.root()),
            WidgetKind::Image(w) => Some(w.root()),
            WidgetKind::Label(w) => Some(w.root()),
            WidgetKind::Button(w) => Some(w.root()),
            WidgetKind::LinkButton(w) => Some(w.root()),
            WidgetKind::ToggleButton(w) => Some(w.root()),
            WidgetKind::CheckButton(w) => Some(w.root()),
            WidgetKind::Switch(w) => Some(w.root()),
            WidgetKind::ProgressBar(w) => Some(w.root()),
            WidgetKind::ComboBox(w) => Some(w.root()),
            WidgetKind::RadioButton(w) => Some(w.root()),
            WidgetKind::Scale(w) => Some(w.root()),
            WidgetKind::Entry(w) => Some(w.root()),
            WidgetKind::LinePlot(w) => Some(w.root()),
            WidgetKind::Frame(w) => Some(w.root()),
            WidgetKind::Box(w) => Some(w.root()),
            WidgetKind::BoxChild(w) => Some(w.root()),
            WidgetKind::Grid(w) => Some(w.root()),
            WidgetKind::GridChild(w) => Some(w.root()),
            WidgetKind::Paned(w) => Some(w.root()),
            WidgetKind::Notebook(w) => Some(w.root()),
            WidgetKind::NotebookPage(w) => Some(w.root()),
            WidgetKind::GridRow => None,
        }
    }
}

#[derive(Clone, Boxed)]
#[boxed_type(name = "NetidxEditorWidget")]
struct Widget {
    root: gtk::Box,
    props: Option<WidgetProps>,
    kind: WidgetKind,
    scope: Scope,
}

impl Widget {
    fn insert(
        ctx: &BSCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        scope: Path,
        spec: view::Widget,
    ) {
        let scope = Rc::new(RefCell::new(scope));
        let (name, kind, props) = match spec {
            view::Widget { props: _, kind: view::WidgetKind::BScript(s) } => (
                "BScript",
                WidgetKind::BScript(widgets::BScript::new(
                    ctx,
                    on_change.clone(),
                    store,
                    iter,
                    scope.clone(),
                    s,
                )),
                None,
            ),
            view::Widget { props, kind: view::WidgetKind::Table(s) } => (
                "Table",
                WidgetKind::Table(widgets::Table::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Image(s) } => (
                "Image",
                WidgetKind::Image(widgets::Image::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Label(s) } => (
                "Label",
                WidgetKind::Label(widgets::Label::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Button(s) } => (
                "Button",
                WidgetKind::Button(widgets::Button::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::LinkButton(s) } => (
                "LinkButton",
                WidgetKind::LinkButton(widgets::LinkButton::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::ToggleButton(s) } => (
                "ToggleButton",
                WidgetKind::ToggleButton(widgets::ToggleButton::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::CheckButton(s) } => (
                "CheckButton",
                WidgetKind::CheckButton(widgets::ToggleButton::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::RadioButton(s) } => (
                "RadioButton",
                WidgetKind::RadioButton(widgets::RadioButton::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Switch(s) } => (
                "Switch",
                WidgetKind::Switch(widgets::Switch::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::ComboBox(s) } => (
                "ComboBox",
                WidgetKind::ComboBox(widgets::ComboBox::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Scale(s) } => (
                "Scale",
                WidgetKind::Scale(widgets::Scale::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::ProgressBar(s) } => (
                "ProgressBar",
                WidgetKind::ProgressBar(widgets::ProgressBar::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Entry(s) } => (
                "Entry",
                WidgetKind::Entry(widgets::Entry::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Frame(s) } => (
                "Frame",
                WidgetKind::Frame(widgets::Frame::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Box(s) } => (
                "Box",
                WidgetKind::Box(widgets::BoxContainer::new(
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::BoxChild(s) } => (
                "BoxChild",
                WidgetKind::BoxChild(widgets::BoxChild::new(on_change, scope.clone(), s)),
                None,
            ),
            view::Widget { props, kind: view::WidgetKind::Grid(s) } => (
                "Grid",
                WidgetKind::Grid(widgets::Grid::new(on_change.clone(), scope.clone(), s)),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::GridChild(s) } => (
                "GridChild",
                WidgetKind::GridChild(widgets::GridChild::new(
                    on_change,
                    scope.clone(),
                    s,
                )),
                None,
            ),
            view::Widget { props: _, kind: view::WidgetKind::GridRow(_) } => {
                ("GridRow", WidgetKind::GridRow, None)
            }
            view::Widget { props, kind: view::WidgetKind::Paned(s) } => (
                "Paned",
                WidgetKind::Paned(widgets::Paned::new(
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props, kind: view::WidgetKind::Notebook(s) } => (
                "Notebook",
                WidgetKind::Notebook(widgets::Notebook::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
            view::Widget { props: _, kind: view::WidgetKind::NotebookPage(s) } => (
                "NotebookPage",
                WidgetKind::NotebookPage(widgets::NotebookPage::new(
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                None,
            ),
            view::Widget { props, kind: view::WidgetKind::LinePlot(s) } => (
                "LinePlot",
                WidgetKind::LinePlot(widgets::LinePlot::new(
                    ctx,
                    on_change.clone(),
                    scope.clone(),
                    s,
                )),
                Some(WidgetProps::new(ctx, scope.clone(), on_change, props)),
            ),
        };
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        if let Some(p) = props.as_ref() {
            root.pack_start(p.root(), false, false, 0);
            root.pack_start(
                &gtk::Separator::new(gtk::Orientation::Horizontal),
                false,
                false,
                0,
            );
        }
        if let Some(r) = kind.root() {
            root.pack_start(r, true, true, 0);
        }
        store.set_value(iter, 0, &name.to_value());
        root.set_sensitive(false);
        let t = Widget { root, props, kind, scope };
        store.set_value(iter, 1, &t.to_value());
    }

    fn spec(&self) -> view::Widget {
        let props = self.props.as_ref().and_then(|p| p.spec());
        let kind = match &self.kind {
            WidgetKind::BScript(w) => view::WidgetKind::BScript(w.spec()),
            WidgetKind::Table(w) => view::WidgetKind::Table(w.spec()),
            WidgetKind::Image(w) => view::WidgetKind::Image(w.spec()),
            WidgetKind::Label(w) => view::WidgetKind::Label(w.spec()),
            WidgetKind::Button(w) => view::WidgetKind::Button(w.spec()),
            WidgetKind::LinkButton(w) => view::WidgetKind::LinkButton(w.spec()),
            WidgetKind::ToggleButton(w) => view::WidgetKind::ToggleButton(w.spec()),
            WidgetKind::CheckButton(w) => view::WidgetKind::CheckButton(w.spec()),
            WidgetKind::RadioButton(w) => view::WidgetKind::RadioButton(w.spec()),
            WidgetKind::Switch(w) => view::WidgetKind::Switch(w.spec()),
            WidgetKind::ComboBox(w) => view::WidgetKind::ComboBox(w.spec()),
            WidgetKind::Scale(w) => view::WidgetKind::Scale(w.spec()),
            WidgetKind::ProgressBar(w) => view::WidgetKind::ProgressBar(w.spec()),
            WidgetKind::Entry(w) => view::WidgetKind::Entry(w.spec()),
            WidgetKind::LinePlot(w) => view::WidgetKind::LinePlot(w.spec()),
            WidgetKind::Frame(w) => view::WidgetKind::Frame(w.spec()),
            WidgetKind::Box(w) => view::WidgetKind::Box(w.spec()),
            WidgetKind::BoxChild(w) => view::WidgetKind::BoxChild(w.spec()),
            WidgetKind::Grid(w) => view::WidgetKind::Grid(w.spec()),
            WidgetKind::GridChild(w) => view::WidgetKind::GridChild(w.spec()),
            WidgetKind::Paned(w) => view::WidgetKind::Paned(w.spec()),
            WidgetKind::Notebook(w) => view::WidgetKind::Notebook(w.spec()),
            WidgetKind::NotebookPage(w) => view::WidgetKind::NotebookPage(w.spec()),
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
            default_view(Path::from("/"))
        }
        match name {
            None => table(),
            Some("BScript") => widget(view::WidgetKind::BScript(ce(Value::U64(42)))),
            Some("Table") => table(),
            Some("Image") => widget(view::WidgetKind::Image(view::Image {
                spec: ce(Value::from("media-floppy-symbolic")),
                on_click: ce(Value::Null),
            })),
            Some("Label") => label_with_txt("static label"),
            Some("Button") => widget(view::WidgetKind::Button(view::Button {
                label: ce(Value::String(Chars::from("click me!"))),
                image: ce(Value::Null),
                on_click: expr::ExprKind::Apply {
                    args: vec![
                        ce(Value::from("/somewhere/in/netidx")),
                        expr::ExprKind::Apply { args: vec![], function: "event".into() }
                            .to_expr(),
                    ],
                    function: "store".into(),
                }
                .to_expr(),
            })),
            Some("LinkButton") => {
                widget(view::WidgetKind::LinkButton(view::LinkButton {
                    uri: ce(Value::String(Chars::from("file:///"))),
                    label: ce(Value::String(Chars::from("click me!"))),
                    on_activate_link: ce(Value::Null),
                }))
            }
            Some("ToggleButton") | Some("CheckButton") => {
                let tb = view::ToggleButton {
                    label: ce(Value::from("click me!")),
                    image: ce(Value::Null),
                    toggle: view::Switch {
                        value: expr::ExprKind::Apply {
                            args: vec![ce(Value::from("/somewhere"))],
                            function: "load".into(),
                        }
                        .to_expr(),
                        on_change: expr::ExprKind::Apply {
                            args: vec![
                                ce(Value::from("/somewhere")),
                                expr::ExprKind::Apply {
                                    args: vec![],
                                    function: "event".into(),
                                }
                                .to_expr(),
                            ],
                            function: "store".into(),
                        }
                        .to_expr(),
                    },
                };
                if name == Some("ToggleButton") {
                    widget(view::WidgetKind::ToggleButton(tb))
                } else if name == Some("CheckButton") {
                    widget(view::WidgetKind::CheckButton(tb))
                } else {
                    unreachable!()
                }
            }
            Some("RadioButton") => {
                widget(view::WidgetKind::RadioButton(view::RadioButton {
                    label: ce(Value::from("click me!")),
                    image: ce(Value::Null),
                    group: ce(Value::from("group0")),
                    on_toggled: ce(Value::Null),
                }))
            }
            Some("Switch") => widget(view::WidgetKind::Switch(view::Switch {
                value: expr::ExprKind::Apply {
                    args: vec![ce(Value::from("/somewhere"))],
                    function: "load".into(),
                }
                .to_expr(),
                on_change: expr::ExprKind::Apply {
                    args: vec![
                        ce(Value::from("/somewhere")),
                        expr::ExprKind::Apply { args: vec![], function: "event".into() }
                            .to_expr(),
                    ],
                    function: "store".into(),
                }
                .to_expr(),
            })),
            Some("ComboBox") => {
                let choices = ce(vec![
                    vec![Value::from("1"), Value::from("One")],
                    vec![Value::from("2"), Value::from("Two")],
                ]
                .into());
                widget(view::WidgetKind::ComboBox(view::ComboBox {
                    choices,
                    selected: expr::ExprKind::Apply {
                        args: vec![ce(Value::from("/somewhere"))],
                        function: "load".into(),
                    }
                    .to_expr(),
                    on_change: expr::ExprKind::Apply {
                        args: vec![
                            ce(Value::from("/somewhere")),
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
            Some("Scale") => widget(view::WidgetKind::Scale(view::Scale {
                direction: view::Direction::Horizontal,
                draw_value: ce(Value::True),
                marks: ce(Value::Null),
                has_origin: ce(Value::True),
                value: ce((0f64).into()),
                min: ce((0f64).into()),
                max: ce((1f64).into()),
                on_change: ce(Value::Null),
            })),
            Some("ProgressBar") => {
                widget(view::WidgetKind::ProgressBar(view::ProgressBar {
                    ellipsize: ce("none".into()),
                    fraction: ce((0f64).into()),
                    pulse: ce(Value::Null),
                    text: ce(Value::Null),
                    show_text: ce(Value::False),
                }))
            }
            Some("Entry") => widget(view::WidgetKind::Entry(view::Entry {
                text: expr::ExprKind::Apply {
                    args: vec![ce(Value::from("/somewhere"))],
                    function: "load".into(),
                }
                .to_expr(),
                on_change: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Apply { args: vec![], function: "event".into() }
                            .to_expr(),
                        ce(Value::True),
                    ],
                    function: "sample".into(),
                }
                .to_expr(),
                on_activate: expr::ExprKind::Apply {
                    args: vec![
                        ce(Value::from("/somewhere")),
                        expr::ExprKind::Apply { args: vec![], function: "event".into() }
                            .to_expr(),
                    ],
                    function: "store".into(),
                }
                .to_expr(),
            })),
            Some("LinePlot") => {
                let props = Some(view::WidgetProps {
                    vexpand: true,
                    hexpand: true,
                    ..DEFAULT_PROPS.clone()
                });
                let kind = view::WidgetKind::LinePlot(view::LinePlot {
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
                    x_min: ce(Value::Null),
                    x_max: ce(Value::Null),
                    y_min: ce(Value::Null),
                    y_max: ce(Value::Null),
                    keep_points: ce(Value::U64(256)),
                    series: Vec::new(),
                });
                view::Widget { kind, props }
            }
            Some("Frame") => widget(view::WidgetKind::Frame(view::Frame {
                label: ce(Value::Null),
                label_align_horizontal: 0.,
                label_align_vertical: 0.5,
                child: None,
            })),
            Some("Box") => widget(view::WidgetKind::Box(view::Box {
                direction: view::Direction::Vertical,
                homogeneous: false,
                spacing: 0,
                children: Vec::new(),
            })),
            Some("BoxChild") => widget(view::WidgetKind::BoxChild(view::BoxChild {
                pack: view::Pack::Start,
                padding: 0,
                widget: boxed::Box::new(label_with_txt("empty box child")),
            })),
            Some("Grid") => widget(view::WidgetKind::Grid(view::Grid {
                homogeneous_columns: false,
                homogeneous_rows: false,
                column_spacing: 0,
                row_spacing: 0,
                rows: Vec::new(),
            })),
            Some("Paned") => widget(view::WidgetKind::Paned(view::Paned {
                direction: view::Direction::Vertical,
                wide_handle: false,
                first_child: None,
                second_child: None,
            })),
            Some("GridChild") => widget(view::WidgetKind::GridChild(view::GridChild {
                width: 1,
                height: 1,
                widget: boxed::Box::new(label_with_txt("empty grid child")),
            })),
            Some("GridRow") => {
                widget(view::WidgetKind::GridRow(view::GridRow { columns: vec![] }))
            }
            Some("NotebookPage") => {
                widget(view::WidgetKind::NotebookPage(view::NotebookPage {
                    label: "Some Page".into(),
                    reorderable: false,
                    widget: boxed::Box::new(label_with_txt("empty notebook page")),
                }))
            }
            Some("Notebook") => widget(view::WidgetKind::Notebook(view::Notebook {
                tabs_visible: true,
                tabs_position: view::TabPosition::Top,
                tabs_scrollable: false,
                tabs_popup: false,
                children: vec![],
                page: ce(Value::Null),
                on_switch_page: ce(Value::Null),
            })),
            _ => unreachable!(),
        }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn moved(&self, iter: &gtk::TreeIter) {
        match &self.kind {
            WidgetKind::BScript(w) => w.moved(iter),
            WidgetKind::Table(_)
            | WidgetKind::Image(_)
            | WidgetKind::Label(_)
            | WidgetKind::Button(_)
            | WidgetKind::LinkButton(_)
            | WidgetKind::ToggleButton(_)
            | WidgetKind::CheckButton(_)
            | WidgetKind::RadioButton(_)
            | WidgetKind::Switch(_)
            | WidgetKind::ComboBox(_)
            | WidgetKind::Scale(_)
            | WidgetKind::ProgressBar(_)
            | WidgetKind::Entry(_)
            | WidgetKind::LinePlot(_)
            | WidgetKind::Frame(_)
            | WidgetKind::Box(_)
            | WidgetKind::BoxChild(_)
            | WidgetKind::Grid(_)
            | WidgetKind::GridChild(_)
            | WidgetKind::Paned(_)
            | WidgetKind::Notebook(_)
            | WidgetKind::NotebookPage(_)
            | WidgetKind::GridRow => (),
        }
    }
}

static KINDS: [&'static str; 24] = [
    "Box",
    "BoxChild",
    "BScript",
    "Button",
    "CheckButton",
    "ComboBox",
    "Entry",
    "Frame",
    "Grid",
    "GridChild",
    "GridRow",
    "Image",
    "Label",
    "LinePlot",
    "LinkButton",
    "Notebook",
    "NotebookPage",
    "Paned",
    "ProgressBar",
    "RadioButton",
    "Scale",
    "Switch",
    "Table",
    "ToggleButton",
];

pub(super) struct Editor {
    root: gtk::Paned,
}

impl Editor {
    pub(super) fn new(ctx: BSCtx, scope: Path, spec: view::Widget) -> Editor {
        let root = gtk::Paned::new(gtk::Orientation::Vertical);
        idle_add_local(
            clone!(@weak root => @default-return glib::Continue(false), move || {
                root.set_position(root.allocated_height() / 2);
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
        root.pack1(&root_upper, true, false);
        root.pack2(&win_lower, true, true);
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
        let undo_stack: Rc<RefCell<Vec<view::Widget>>> =
            Rc::new(RefCell::new(Vec::new()));
        let undoing = Rc::new(Cell::new(false));
        let on_change: OnChange = Rc::new({
            let scope = scope.clone();
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
                        @strong scope,
                        @strong ctx,
                        @strong spec,
                        @strong store,
                        @strong scheduled,
                        @strong undo_stack,
                        @strong undoing => move || {
                            if let Some(root) = store.iter_first() {
                                if undoing.get() {
                                    undoing.set(false)
                                } else {
                                    undo_stack.borrow_mut().push(spec.borrow().clone());
                                }
                                Editor::update_scope(&store, scope.clone(), &root);
                                *spec.borrow_mut() =
                                    Editor::build_spec(&store, &root);
                                ctx.borrow().user.backend.render(spec.borrow().clone());
                            }
                            scheduled.set(false);
                            glib::Continue(false)
                    }));
                }
            }
        });
        Editor::build_tree(
            &ctx,
            &on_change,
            &store,
            scope.clone(),
            None,
            &*spec.borrow(),
        );
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
            @strong scope,
            @strong on_change,
            @strong store,
            @strong selected,
            @strong ctx,
            @weak properties,
            @strong inhibit_change => move |c| {
                if let Some(iter) = selected.borrow().clone() {
                    if !inhibit_change.get() {
                        let wv = store.value(&iter, 1);
                        if let Ok(w) = wv.get::<&Widget>() {
                            w.root().hide();
                            w.root().set_sensitive(false);
                            properties.remove(w.root());
                        }
                        let id = c.active_id();
                        let spec = Widget::default_spec(id.as_ref().map(|s| &**s));
                        // clear description column
                        store.set_value(&iter, 2, &"".to_value());
                        Widget::insert(
                            &ctx,
                            on_change.clone(),
                            &store,
                            &iter,
                            scope.clone(), // will be overwritten by on_change
                            spec
                        );
                        let wv = store.value(&iter, 1);
                        if let Ok(w) = wv.get::<&Widget>() {
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
        let selection = view.selection();
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
                    let children = properties.children();
                    if children.len() == 3 {
                        children[2].hide();
                        children[2].set_sensitive(false);
                        properties.remove(&children[2]);
                    }
                }
                match s.selected() {
                    None => {
                        *selected.borrow_mut() = None;
                        ctx.borrow().user.backend.highlight(vec![]);
                        reveal_properties.set_reveal_child(false);
                    }
                    Some((_, iter)) => {
                        *selected.borrow_mut() = Some(iter.clone());
                        let mut path = Vec::new();
                        Editor::build_widget_path(&store, &iter, 0, 0, &mut path);
                        ctx.borrow().user.backend.highlight(path);
                        let v = store.value(&iter, 0);
                        if let Ok(id) = v.get::<&str>() {
                            inhibit_change.set(true);
                            kind.set_active_id(Some(id));
                            inhibit_change.set(false);
                        }
                        let v = store.value(&iter, 1);
                        if let Ok(w) = v.get::<&Widget>() {
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
            @strong scope,
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
                        scope.clone(), // overwritten by on_change
                        parent.as_ref(),
                        &spec
                    );
                    on_change()
                }
        }));
        duplicate.connect_activate(clone!(@strong dup => move |_| dup()));
        dupbtn.connect_clicked(clone!(@strong dup => move |_| dup()));
        let newsib = Rc::new(clone!(
            @strong scope,
            @strong on_change,
            @weak store,
            @strong selected,
            @strong ctx => move || {
                let iter = store.insert_after(None, selected.borrow().as_ref());
                let spec = Widget::default_spec(Some("Label"));
                Widget::insert(&ctx, on_change.clone(), &store, &iter, scope.clone(), spec);
                on_change();
        }));
        new_sib.connect_activate(clone!(@strong newsib => move |_| newsib()));
        addbtn.connect_clicked(clone!(@strong newsib => move |_| newsib()));
        let newch = Rc::new(clone!(
            @strong scope,
            @strong on_change,
            @weak store,
            @strong selected,
            @strong ctx => move || {
                let iter = store.insert_after(selected.borrow().as_ref(), None);
                let spec = Widget::default_spec(Some("Label"));
                Widget::insert(
                    &ctx,
                    on_change.clone(),
                    &store,
                    &iter,
                    scope.clone(),
                    spec
                );
                on_change();
        }));
        new_child.connect_activate(clone!(@strong newch => move |_| newch()));
        addchbtn.connect_clicked(clone!(@strong newch => move |_| newch()));
        let del = Rc::new(clone!(
            @weak selection,
            @strong on_change,
            @weak store,
            @strong selected => move || {
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
                        scope.clone(),
                        None,
                        &s
                    );
                    on_change();
                }
        }));
        undo.connect_activate(clone!(@strong und => move |_| und()));
        undobtn.connect_clicked(clone!(@strong und => move |_| und()));
        view.connect_button_press_event(move |_, b| {
            let right_click =
                gdk::EventType::ButtonPress == b.event_type() && b.button() == 3;
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
                    let v = store.value(&iter, 1);
                    if let Ok(w) = v.get::<&Widget>() {
                        w.moved(&iter)
                    }
                    glib::Continue(false)
                }));
                on_change();
        }));
        Editor { root }
    }

    fn update_scope(store: &gtk::TreeStore, scope: Path, root: &gtk::TreeIter) {
        let v = store.value(root, 1);
        if let Ok(w) = v.get::<&Widget>() {
            *w.scope.borrow_mut() = scope.clone();
            let scope = match &w.kind {
                WidgetKind::Notebook(_) => scope.append("n"),
                WidgetKind::Box(_) => scope.append("b"),
                WidgetKind::Grid(_) => scope.append("g"),
                WidgetKind::Paned(_) => scope.append("p"),
                WidgetKind::Frame(_)
                | WidgetKind::GridRow
                | WidgetKind::NotebookPage(_)
                | WidgetKind::BoxChild(_)
                | WidgetKind::GridChild(_)
                | WidgetKind::BScript(_)
                | WidgetKind::Table(_)
                | WidgetKind::Image(_)
                | WidgetKind::Label(_)
                | WidgetKind::Button(_)
                | WidgetKind::LinkButton(_)
                | WidgetKind::ToggleButton(_)
                | WidgetKind::CheckButton(_)
                | WidgetKind::RadioButton(_)
                | WidgetKind::Switch(_)
                | WidgetKind::ComboBox(_)
                | WidgetKind::Scale(_)
                | WidgetKind::ProgressBar(_)
                | WidgetKind::Entry(_)
                | WidgetKind::LinePlot(_) => scope.clone(),
            };
            if let Some(iter) = store.iter_children(Some(root)) {
                loop {
                    Editor::update_scope(store, scope.clone(), &iter);
                    if !store.iter_next(&iter) {
                        break;
                    }
                }
            }
        }
    }

    fn build_tree(
        ctx: &BSCtx,
        on_change: &OnChange,
        store: &gtk::TreeStore,
        scope: Path,
        parent: Option<&gtk::TreeIter>,
        w: &view::Widget,
    ) {
        let iter = store.insert_before(parent, None);
        Widget::insert(ctx, on_change.clone(), store, &iter, scope.clone(), w.clone());
        match &w.kind {
            view::WidgetKind::Frame(f) => {
                if let Some(w) = &f.child {
                    Editor::build_tree(ctx, on_change, store, scope, Some(&iter), w);
                }
            }
            view::WidgetKind::NotebookPage(p) => {
                Editor::build_tree(ctx, on_change, store, scope, Some(&iter), &*p.widget);
            }
            view::WidgetKind::Notebook(n) => {
                let scope = scope.append("n");
                for w in n.children.iter() {
                    Editor::build_tree(
                        ctx,
                        on_change,
                        store,
                        scope.clone(),
                        Some(&iter),
                        w,
                    );
                }
            }
            view::WidgetKind::Box(b) => {
                let scope = scope.append("b");
                for w in b.children.iter() {
                    Editor::build_tree(
                        ctx,
                        on_change,
                        store,
                        scope.clone(),
                        Some(&iter),
                        w,
                    );
                }
            }
            view::WidgetKind::BoxChild(b) => {
                Editor::build_tree(ctx, on_change, store, scope, Some(&iter), &*b.widget)
            }
            view::WidgetKind::Grid(g) => {
                let scope = scope.append("g");
                for w in g.rows.iter() {
                    Editor::build_tree(
                        ctx,
                        on_change,
                        store,
                        scope.clone(),
                        Some(&iter),
                        w,
                    );
                }
            }
            view::WidgetKind::GridChild(g) => {
                Editor::build_tree(ctx, on_change, store, scope, Some(&iter), &*g.widget)
            }
            view::WidgetKind::GridRow(g) => {
                for w in g.columns.iter() {
                    Editor::build_tree(
                        ctx,
                        on_change,
                        store,
                        scope.clone(),
                        Some(&iter),
                        w,
                    );
                }
            }
            view::WidgetKind::Paned(p) => {
                let scope = scope.append("p");
                if let Some(w) = &p.first_child {
                    Editor::build_tree(
                        ctx,
                        on_change,
                        store,
                        scope.clone(),
                        Some(&iter),
                        w,
                    );
                }
                if let Some(w) = &p.second_child {
                    Editor::build_tree(ctx, on_change, store, scope, Some(&iter), w);
                }
            }
            view::WidgetKind::BScript(_)
            | view::WidgetKind::Table(_)
            | view::WidgetKind::Image(_)
            | view::WidgetKind::Label(_)
            | view::WidgetKind::Button(_)
            | view::WidgetKind::LinkButton(_)
            | view::WidgetKind::ToggleButton(_)
            | view::WidgetKind::CheckButton(_)
            | view::WidgetKind::RadioButton(_)
            | view::WidgetKind::Switch(_)
            | view::WidgetKind::ComboBox(_)
            | view::WidgetKind::Scale(_)
            | view::WidgetKind::ProgressBar(_)
            | view::WidgetKind::Entry(_)
            | view::WidgetKind::LinePlot(_) => (),
        }
    }

    fn build_spec(store: &gtk::TreeStore, root: &gtk::TreeIter) -> view::Widget {
        let v = store.value(root, 1);
        match v.get::<&Widget>() {
            Err(_) => label_with_txt("tree error"),
            Ok(w) => {
                let mut spec = w.spec();
                match &mut spec.kind {
                    view::WidgetKind::Frame(ref mut f) => {
                        f.child = None;
                        if let Some(iter) = store.iter_children(Some(root)) {
                            f.child =
                                Some(boxed::Box::new(Editor::build_spec(store, &iter)));
                        }
                    }
                    view::WidgetKind::Paned(ref mut p) => {
                        p.first_child = None;
                        p.second_child = None;
                        if let Some(iter) = store.iter_children(Some(root)) {
                            p.first_child =
                                Some(boxed::Box::new(Editor::build_spec(store, &iter)));
                            if store.iter_next(&iter) {
                                p.second_child = Some(boxed::Box::new(
                                    Editor::build_spec(store, &iter),
                                ));
                            }
                        }
                    }
                    view::WidgetKind::Notebook(ref mut n) => {
                        n.children.clear();
                        if let Some(iter) = store.iter_children(Some(root)) {
                            loop {
                                n.children.push(Editor::build_spec(store, &iter));
                                if !store.iter_next(&iter) {
                                    break;
                                }
                            }
                        }
                    }
                    view::WidgetKind::NotebookPage(ref mut p) => {
                        if let Some(iter) = store.iter_children(Some(root)) {
                            p.widget = boxed::Box::new(Editor::build_spec(store, &iter));
                        }
                    }
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
                    view::WidgetKind::BScript(_)
                    | view::WidgetKind::Table(_)
                    | view::WidgetKind::Image(_)
                    | view::WidgetKind::Label(_)
                    | view::WidgetKind::Button(_)
                    | view::WidgetKind::LinkButton(_)
                    | view::WidgetKind::ToggleButton(_)
                    | view::WidgetKind::CheckButton(_)
                    | view::WidgetKind::RadioButton(_)
                    | view::WidgetKind::Switch(_)
                    | view::WidgetKind::ComboBox(_)
                    | view::WidgetKind::Scale(_)
                    | view::WidgetKind::ProgressBar(_)
                    | view::WidgetKind::Entry(_)
                    | view::WidgetKind::LinePlot(_) => (),
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
        let v = store.value(start, 1);
        let skip_idx = match v.get::<&Widget>() {
            Err(_) => false,
            Ok(w) => match &w.kind {
                WidgetKind::BScript(_)
                | WidgetKind::Table(_)
                | WidgetKind::Image(_)
                | WidgetKind::Label(_)
                | WidgetKind::Button(_)
                | WidgetKind::LinkButton(_)
                | WidgetKind::ToggleButton(_)
                | WidgetKind::CheckButton(_)
                | WidgetKind::RadioButton(_)
                | WidgetKind::Switch(_)
                | WidgetKind::ComboBox(_)
                | WidgetKind::Scale(_)
                | WidgetKind::ProgressBar(_)
                | WidgetKind::Entry(_)
                | WidgetKind::LinePlot(_) => {
                    path.insert(0, WidgetPath::Leaf);
                    false
                }
                WidgetKind::Frame(_)
                | WidgetKind::Box(_)
                | WidgetKind::Notebook(_)
                | WidgetKind::Paned(_) => {
                    if path.len() == 0 {
                        path.insert(0, WidgetPath::Leaf);
                    } else {
                        path.insert(0, WidgetPath::Box(nchild));
                    }
                    false
                }
                WidgetKind::NotebookPage(_) | WidgetKind::BoxChild(_) => {
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
                    if let Some(idx) = store.path(start).map(|t| t.indices()) {
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
            if let Some(idx) = store.path(start).map(|t| t.indices()) {
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
