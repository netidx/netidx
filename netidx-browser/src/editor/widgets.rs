use super::super::{util::err_modal, Target, WidgetCtx};
use super::{
    source_inspector::SourceInspector,
    util::{parse_entry, TwoColGrid},
    OnChange,
};
use glib::{clone, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::{path::Path, subscriber::Value};
use netidx_protocols::view;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone, Debug)]
pub(super) struct Table {
    root: gtk::Box,
    spec: Rc<RefCell<Path>>,
}

impl Table {
    pub(super) fn new(on_change: OnChange, path: Path) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let label = gtk::Label::new(Some("Path:"));
        let entry = gtk::Entry::new();
        root.pack_start(&label, false, false, 0);
        root.pack_start(&entry, true, true, 0);
        let spec = Rc::new(RefCell::new(path));
        entry.set_text(&**spec.borrow());
        entry.connect_activate(clone!(@strong spec => move |e| {
            *spec.borrow_mut() = Path::from(String::from(&*e.get_text()));
            on_change()
        }));
        Table { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Table(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
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
            w.set_default_size(640, 480);
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
            si.root().set_property_margin(5);
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
pub(super) struct Action {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Action>>,
    source: DbgSrc,
}

impl Action {
    pub(super) fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Action) -> Self {
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
        Action { root, spec, source }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Action(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Label {
    root: gtk::Box,
    spec: Rc<RefCell<view::Source>>,
    source: DbgSrc,
}

impl Label {
    pub(super) fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Source) -> Self {
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
        root.pack_start(&l, false, false, 0);
        root.pack_start(&e, true, true, 0);
        Label { root, spec, source }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Label(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Button {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Button>>,
    enabled_source: DbgSrc,
    label_source: DbgSrc,
    source: DbgSrc,
}

impl Button {
    pub(super) fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Button) -> Self {
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
        Button { root, spec, enabled_source, label_source, source }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Button(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.label_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Toggle {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Toggle>>,
    enabled_source: DbgSrc,
    source: DbgSrc,
}

impl Toggle {
    pub(super) fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Toggle) -> Self {
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
        Toggle { root, spec, enabled_source, source }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Toggle(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Selector {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Selector>>,
    enabled_source: DbgSrc,
    choices_source: DbgSrc,
    source: DbgSrc,
}

impl Selector {
    pub(super) fn new(
        ctx: &WidgetCtx,
        on_change: OnChange,
        spec: view::Selector,
    ) -> Self {
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
        Selector { root, spec, enabled_source, choices_source, source }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Selector(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.choices_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Entry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Entry>>,
    enabled_source: DbgSrc,
    visible_source: DbgSrc,
    source: DbgSrc,
}

impl Entry {
    pub(super) fn new(ctx: &WidgetCtx, on_change: OnChange, spec: view::Entry) -> Self {
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
        Entry { root, spec, enabled_source, visible_source, source }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Entry(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.visible_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct LinePlot {
    root: gtk::Box,
    spec: Rc<RefCell<view::LinePlot>>,
    x_min: DbgSrc,
    x_max: DbgSrc,
    y_min: DbgSrc,
    y_max: DbgSrc,
    keep_points: DbgSrc,
    x: DbgSrc,
    series: Rc<RefCell<IndexMap<usize, DbgSrc>>>,
}

impl LinePlot {
    pub(super) fn new(
        ctx: &WidgetCtx,
        on_change: OnChange,
        spec: view::LinePlot,
    ) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        LinePlot::build_chart_style_editor(&root, &spec);
        LinePlot::build_axis_style_editor(&root, &spec);
        let (x_min, x_max, y_min, y_max, keep_points) =
            LinePlot::build_axis_range_editor(&root, &spec);
        let (x, series) = LinePlot::build_series_editor(&root, &spec);
        LinePlot { root, spec, x_min, x_max, y_min, y_max, keep_points, series }
    }

    fn build_axis_style_editor(root: &gtk::Box) {
        let axis_exp = gtk::Expander::new(Some("Axis Style"));
        let mut axis = TwoColGrid::new();
        root.pack_start(&axis_exp, false, false, 0);
        axis_exp.add(axis.root());
        axis.add(parse_entry(
            "Title:",
            &spec.borrow().title,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().title = s;
                on_change()
            }),
        ));
        axis.add(parse_entry(
            "X Axis Label:",
            &spec.borrow().x_label,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_label = s;
                on_change()
            }),
        ));
        axis.add(parse_entry(
            "Y Axis Label:",
            &spec.borrow().y_label,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_label = s;
                on_change()
            }),
        ));
        axis.add(parse_entry(
            "X Labels:",
            &spec.borrow().x_labels,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_labels = s;
                on_change()
            }),
        ));
        axis.add(parse_entry(
            "Y Labels:",
            &spec.borrow().y_labels,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_labels = s;
                on_change()
            }),
        ));
        let x_grid = gtk::CheckButton::with_label("X Axis Grid");
        x_grid.set_active(spec.borrow().x_grid);
        x_grid.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().x_grid = b.get_active();
            on_change()
        }));
        axis.attach(&x_grid, 0, 2, 1);
        let y_grid = gtk::CheckButton::with_label("Y Axis Grid");
        y_grid.set_active(spec.borrow().y_grid);
        y_grid.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().y_grid = b.get_active();
            on_change()
        }));
        axis.attach(&y_grid, 0, 2, 1);
    }

    fn build_axis_range_editor(
        root: &gtk::Box,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) -> (DbgSrc, DbgSrc, DbgSrc, DbgSrc) {
        let range_exp = gtk::Expander::new(Some("Axis Range"));
        let mut range = TwoColGrid::new();
        root.pack_start(&range_exp, false, false, 0);
        range_exp.add(range.root());
        let (l, e, x_min) = source(
            ctx,
            "x min:",
            &spec.borrow().x_min,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_min = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, x_max) = source(
            ctx,
            "x max:",
            &spec.borrow().x_max,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_max = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, y_min) = source(
            ctx,
            "y min:",
            &spec.borrow().y_min,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_min = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, y_max) = source(
            ctx,
            "y max:",
            &spec.borrow().y_max,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_max = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, keep_points) = source(
            ctx,
            "Keep Points:",
            &spec.borrow().keep_points,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().keep_points = s;
                on_change()
            }),
        );
        range.add((l, e));
        (x_min, x_max, y_min, y_max, keep_points)
    }

    fn build_chart_style_editor(root: &gtk::Box, spec: &Rc<RefCell<view::LinePlot>>) {
        let style_exp = gtk::Expander::new(Some("Chart Style"));
        let mut style = TwoColGrid::new();
        root.pack_start(&style_exp, false, false, 0);
        style_exp.add(style.root());
        let has_fill = gtk::CheckButton::with_label("Fill");
        let fill_reveal = gtk::Revealer::new();
        let fill_color = gtk::ColorButton::new();
        fill_reveal.add(&fill_color);
        style.add((has_fill.clone(), fill_reveal.clone()));
        if let Some(c) = spec.borrow().fill {
            has_fill.set_active(true);
            fill_reveal.set_reveal_child(true);
            fill_color.set_rgba(&gdk::RGBA {
                red: c.r,
                green: c.g,
                blue: c.b,
                alpha: 1.,
            });
        }
        has_fill.connect_toggled(clone!(
            @strong on_change,
            @strong spec,
            @weak fill_reveal,
            @weak fill_color => move |b| {
                if b.get_active() {
                    fill_reveal.set_reveal_child(true);
                    let c = fill_color.get_rgba();
                    let c = view::RGB { r: c.red, g: c.green, b: c.blue };
                    spec.borrow_mut().fill = Some(c);
                } else {
                    fill_reveal.set_reveal_child(false);
                    spec.borrow_mut().fill = None;
                }
                on_change()
        }));
        fill_color.connect_color_set(
            clone!(@strong on_change, @strong spec => move |b| {
                let c = b.get_rgba();
                let c = view::RGB { r: c.red, g: c.green, b: c.blue };
                spec.borrow_mut().fill = Some(c);
                on_change()
            }),
        );
        style.add(parse_entry(
            "Margin:",
            &spec.borrow().margin,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().margin = s;
                on_change()
            }),
        ));
        style.add(parse_entry(
            "Label Area:",
            &spec.borrow().label_area,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().label_area = s;
                on_change()
            }),
        ))
    }

    fn build_series_editor(
        root: &gtk::Box,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) -> (DbgSrc, Rc<RefCell<IndexMap<usize, DbgSrc>>>) {
        let series_exp = gtk::Expander::new(Some("Series"));
        let seriesbox = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let addbtn = gtk::Button::with_label("+");
        series_exp.add(&seriesbox);
        root.pack_start(&series_exp, false, false, 0);
        let (l, e, x) = source(
            ctx,
            "X:",
            &spec.borrow().x,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x = s;
                on_change()
            }),
        );
        let xbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        xbox.pack_start(&l, false, false, 0);
        xbox.pack_start(&e, false, false, 0);
        seriesbox.pack_start(&xbox, false, false, 0);
        seriesbox.pack_start(&addbtn, false, false, 0);
        let series_id = Rc::new(Cell::new(0));
        let series = Rc::new(RefCell::new(IndexMap::new()));
        let build_series = Rc::new(clone!(
            @weak seriesbox,
            @strong ctx,
            @strong on_change,
            @strong spec,
            @strong series => move || {
                let i = series_id.get();
                series_id.set(i + 1);
                let mut grid = TwoColGrid::new();
                seriesbox.pack_start(grid.root(), false, false, 0);
                let sep = gtk::Separator::new(gtk::Orientation::Vertical);
                grid.attach(&sep, 0, 2, 1);
                grid.add(parse_entry(
                    "Title:",
                    &spec.borrow().series[i].title,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().series[i].title = s;
                        on_change()
                    })
                ));
                let c = spec.borrow().series[i].line_color;
                let rgba = gdk::RGBA { red: c.r, green: c.g, blue: c.b, alpha: 1.};
                let line_color = gtk::ColorButton::with_rgba(&rgba);
                let lbl_line_color = gtk::Label::new(Some("Line Color:"));
                line_color.connect_color_set(clone!(
                    @strong on_change, @strong spec => move |b| {
                        let c = b.get_rgba();
                        let c = view::RGB { r: c.red, g: c.green, b: c.blue };
                        spec.borrow_mut().series[i].line_color = c;
                        on_change()
                    }));
                grid.add((lbl_line_color, line_color));
                let (l, e, x) = source(
                    &ctx,
                    "X:",
                    &spec.borrow().series[i].x,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().series[i].x = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let (l, e, y) = source(
                    &ctx,
                    "Y:",
                    &spec.borrow().series[i].y,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().series[i].y = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let remove = gtk::Button::with_label("-");
                grid.attach(&remove, 0, 2, 1);
                series.borrow_mut().insert(i, Series { x, y });
                seriesbox.show_all();
                remove.connect_clicked(clone!(
                    @strong series,
                    @weak seriesbox,
                    @strong spec,
                    @strong on_change => move |_| {
                        series.borrow_mut().remove(i);
                        spec.borrow_mut().series.remove(i);
                        seriesbox.remove(&seriesbox.get_children()[i]);
                        on_change()
                    }));
        }));
        addbtn.connect_clicked(clone!(
            @strong spec, @strong build_series => move |_| {
                spec.borrow_mut().series.push(view::Series {
                    title: String::from("Series"),
                    line_color: view::RGB { r: 0., g: 0., b: 0. },
                    x: view::Source::Load(Path::from("/somewhere/in/netidx/x")),
                    y: view::Source::Load(Path::from("/somewhere/in/netidx/y")),
                });
                build_series(spec.borrow().series.len() - 1)
            }
        ));
        for i in 0..spec.borrow().series.len() {
            build_series(i)
        }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::LinePlot(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, s)) = &*self.x_min.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.x_max.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.y_min.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.y_max.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.keep_points.borrow() {
            s.update(tgt, value);
        }
        for s in self.series.borrow().values() {
            if let Some((_, s)) = &*s.x.borrow() {
                s.update(tgt, value);
            }
            if let Some((_, s)) = &*s.y.borrow() {
                s.update(tgt, value);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct BoxChild {
    root: TwoColGrid,
    spec: Rc<RefCell<view::BoxChild>>,
}

impl BoxChild {
    pub(super) fn new(on_change: OnChange, spec: view::BoxChild) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let mut root = TwoColGrid::new();
        let packlbl = gtk::Label::new(Some("Pack:"));
        let packcb = gtk::ComboBoxText::new();
        packcb.append(Some("Start"), "Start");
        packcb.append(Some("End"), "End");
        packcb.set_active_id(Some(match spec.borrow().pack {
            view::Pack::Start => "Start",
            view::Pack::End => "End",
        }));
        packcb.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().pack = match c.get_active_id() {
                Some(s) if &*s == "Start" => view::Pack::Start,
                Some(s) if &*s == "End" => view::Pack::End,
                _ => view::Pack::Start
            };
            on_change()
        }));
        root.add((packlbl, packcb));
        root.add(parse_entry(
            "Padding:",
            &spec.borrow().padding,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().padding = s;
                on_change()
            }),
        ));
        BoxChild { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::BoxChild(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

fn dirselect(
    cur: view::Direction,
    on_change: impl Fn(view::Direction) + 'static,
) -> gtk::ComboBoxText {
    let dircb = gtk::ComboBoxText::new();
    dircb.append(Some("Horizontal"), "Horizontal");
    dircb.append(Some("Vertical"), "Vertical");
    match cur {
        view::Direction::Horizontal => dircb.set_active_id(Some("Horizontal")),
        view::Direction::Vertical => dircb.set_active_id(Some("Vertical")),
    };
    dircb.connect_changed(move |c| {
        on_change(match c.get_active_id() {
            Some(s) if &*s == "Horizontal" => view::Direction::Horizontal,
            Some(s) if &*s == "Vertical" => view::Direction::Vertical,
            _ => view::Direction::Horizontal,
        })
    });
    dircb
}

#[derive(Clone, Debug)]
pub(super) struct BoxContainer {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Box>>,
}

impl BoxContainer {
    pub(super) fn new(on_change: OnChange, spec: view::Box) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let dircb = dirselect(
            spec.borrow().direction,
            clone!(@strong on_change, @strong spec => move |d| {
                spec.borrow_mut().direction = d;
                on_change()
            }),
        );
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
        BoxContainer { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Box(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone, Debug)]
pub(super) struct GridChild {
    root: TwoColGrid,
    spec: Rc<RefCell<view::GridChild>>,
}

impl GridChild {
    pub(super) fn new(on_change: OnChange, spec: view::GridChild) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Width:",
            &spec.borrow().width,
            clone!(@strong on_change, @strong spec => move |w| {
                spec.borrow_mut().width = w;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Height:",
            &spec.borrow().height,
            clone!(@strong on_change, @strong spec => move |h| {
                spec.borrow_mut().height = h;
                on_change()
            }),
        ));
        GridChild { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::GridChild(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone, Debug)]
pub(super) struct Grid {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Grid>>,
}

impl Grid {
    pub(super) fn new(on_change: OnChange, spec: view::Grid) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Homogeneous Columns:",
            &spec.borrow().homogeneous_columns,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().homogeneous_columns = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Homogeneous Rows:",
            &spec.borrow().homogeneous_rows,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().homogeneous_rows = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Column Spacing:",
            &spec.borrow().column_spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().column_spacing = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Row Spacing:",
            &spec.borrow().row_spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().row_spacing = s;
                on_change()
            }),
        ));
        Grid { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Grid(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}
