use super::{val_to_bool, BSCtx, BSNode};
use crate::{bscript::Target, view};
use anyhow::{anyhow, Result};
use cairo;
use chrono::prelude::*;
use gdk::{self, prelude::*};
use glib::{clone, idle_add_local};
use gtk::{self, prelude::*};
use log::warn;
use netidx::{
    chars::Chars,
    subscriber::{Typ, Value},
};
use netidx_bscript::expr::Expr;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    panic::{catch_unwind, AssertUnwindSafe},
    rc::Rc,
};

pub(super) struct Button {
    enabled: Rc<BSNode>,
    label: Rc<BSNode>,
    on_click: Rc<BSNode>,
    button: gtk::Button,
}

impl Button {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Button,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::Button::new();
        let enabled = Rc::new(BSNode::compile(&ctx, spec.enabled.clone()));
        let label = Rc::new(BSNode::compile(&ctx, spec.label.clone()));
        let on_click = Rc::new(BSNode::compile(&ctx, spec.on_click.clone()));
        if let Some(v) = enabled.current() {
            button.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = label.current() {
            button.set_label(&format!("{}", v));
        }
        button.connect_clicked(clone!(@strong ctx, @strong on_click => move |_| {
            on_click.update(&ctx, &Target::Event(Value::Null));
        }));
        button.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(&format!("on_click: {}", spec.on_click));
            Inhibit(false)
        }));
        button.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(&format!("on_click: {}", spec.on_click));
                Inhibit(false)
            }),
        );
        Button { enabled, label, on_click, button }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.button.upcast_ref()
    }

    pub(super) fn update(&self, ctx: &BSCtx, event: &Target) {
        if let Some(new) = self.enabled.update(ctx, event) {
            self.button.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.label.update(ctx, event) {
            self.button.set_label(&format!("{}", new));
        }
        self.on_click.update(ctx, event);
    }
}

pub(super) struct Label {
    label: gtk::Label,
    text: Rc<BSNode>,
}

impl Label {
    pub(super) fn new(ctx: &BSCtx, spec: Expr, selected_path: gtk::Label) -> Label {
        let text = Rc::new(BSNode::compile(&ctx, spec.clone()));
        let txt = match text.current() {
            None => String::new(),
            Some(v) => format!("{}", v),
        };
        let label = gtk::Label::new(Some(txt.as_str()));
        label.set_selectable(true);
        label.set_single_line_mode(true);
        label.connect_button_press_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(&format!("{}", spec));
                Inhibit(false)
            }),
        );
        label.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(&format!("{}", spec));
            Inhibit(false)
        }));
        Label { text, label }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.label.upcast_ref()
    }

    pub(super) fn update(&self, ctx: &BSCtx, event: &Target) {
        if let Some(new) = self.text.update(ctx, event) {
            self.label.set_label(&format!("{}", new));
        }
    }
}

pub(super) struct Action {
    action: Rc<BSNode>,
}

impl Action {
    pub(super) fn new(ctx: &BSCtx, spec: Expr) -> Self {
        let action = Rc::new(BSNode::compile(&ctx, spec.clone()));
        action.update(ctx, &Target::Event(Value::Null));
        Action { action }
    }

    pub(super) fn update(&self, ctx: &BSCtx, event: &Target) {
        self.action.update(ctx, event);
    }
}

pub(super) struct Selector {
    root: gtk::EventBox,
    combo: gtk::ComboBoxText,
    enabled: Rc<BSNode>,
    choices: Rc<BSNode>,
    selected: Rc<BSNode>,
    on_change: Rc<BSNode>,
    we_set: Rc<Cell<bool>>,
}

impl Selector {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Selector,
        selected_path: gtk::Label,
    ) -> Self {
        let combo = gtk::ComboBoxText::new();
        let root = gtk::EventBox::new();
        root.add(&combo);
        combo.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("on_change: {}, choices: {}", spec.on_change, spec.choices)
            );
            Inhibit(false)
        }));
        root.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("on_change: {}, choices: {}", spec.on_change, spec.choices)
                );
                Inhibit(false)
            }),
        );
        let enabled = Rc::new(BSNode::compile(&ctx, spec.enabled.clone()));
        let choices = Rc::new(BSNode::compile(&ctx, spec.choices.clone()));
        let selected = Rc::new(BSNode::compile(&ctx, spec.selected.clone()));
        let on_change = Rc::new(BSNode::compile(&ctx, spec.on_change.clone()));
        let we_set = Rc::new(Cell::new(false));
        if let Some(v) = enabled.current() {
            combo.set_sensitive(val_to_bool(&v));
        }
        if let Some(choices) = choices.current() {
            Selector::update_choices(&combo, &choices, &selected.current());
        }
        we_set.set(true);
        Selector::update_active(&combo, &selected.current());
        we_set.set(false);
        combo.connect_changed(clone!(
            @strong we_set,
            @strong on_change,
            @strong ctx,
            @strong selected => move |combo| {
            if !we_set.get() {
                if let Some(id) = combo.get_active_id() {
                    if let Ok(idv) = serde_json::from_str::<Value>(id.as_str()) {
                        on_change.update(&ctx, &Target::Event(idv));
                    }
                }
                idle_add_local(clone!(
                    @strong selected, @strong combo, @strong we_set => move || {
                        we_set.set(true);
                        Selector::update_active(&combo, &selected.current());
                        we_set.set(false);
                        Continue(false)
                    })
                );
            }
        }));
        Selector { root, combo, enabled, choices, selected, on_change, we_set }
    }

    fn update_active(combo: &gtk::ComboBoxText, source: &Option<Value>) {
        if let Some(source) = source {
            if let Ok(current) = serde_json::to_string(source) {
                combo.set_active_id(Some(current.as_str()));
            }
        }
    }

    fn update_choices(
        combo: &gtk::ComboBoxText,
        choices: &Value,
        source: &Option<Value>,
    ) {
        let choices = match choices {
            Value::String(s) => {
                match serde_json::from_str::<Vec<(Value, String)>>(&**s) {
                    Ok(choices) => choices,
                    Err(e) => {
                        warn!(
                            "failed to parse combo choices, source {:?}, {}",
                            choices, e
                        );
                        vec![]
                    }
                }
            }
            v => {
                warn!("combo choices wrong type, expected json string not {:?}", v);
                vec![]
            }
        };
        combo.remove_all();
        for (id, val) in choices {
            if let Ok(id) = serde_json::to_string(&id) {
                combo.append(Some(id.as_str()), val.as_str());
            }
        }
        Selector::update_active(combo, source)
    }

    pub(super) fn update(&self, ctx: &BSCtx, event: &Target) {
        self.we_set.set(true);
        self.on_change.update(ctx, event);
        if let Some(new) = self.enabled.update(ctx, event) {
            self.combo.set_sensitive(val_to_bool(&new));
        }
        Selector::update_active(&self.combo, &self.selected.update(ctx, event));
        if let Some(new) = self.choices.update(ctx, event) {
            Selector::update_choices(&self.combo, &new, &self.selected.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Toggle {
    enabled: Rc<BSNode>,
    value: Rc<BSNode>,
    on_change: Rc<BSNode>,
    we_set: Rc<Cell<bool>>,
    switch: gtk::Switch,
}

impl Toggle {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Toggle,
        selected_path: gtk::Label,
    ) -> Self {
        let switch = gtk::Switch::new();
        let enabled = Rc::new(BSNode::compile(&ctx, spec.enabled.clone()));
        let value = Rc::new(BSNode::compile(&ctx, spec.value.clone()));
        let on_change = Rc::new(BSNode::compile(&ctx, spec.on_change.clone()));
        let we_set = Rc::new(Cell::new(false));
        if let Some(v) = enabled.current() {
            switch.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = value.current() {
            let v = val_to_bool(&v);
            switch.set_active(v);
            switch.set_state(v);
        }
        switch.connect_state_set(clone!(
        @strong ctx, @strong on_change, @strong we_set, @strong value =>
        move |switch, state| {
            if !we_set.get() {
                on_change.update(
                    &ctx,
                    &Target::Event(if state { Value::True } else { Value::False }),
                );
                idle_add_local(
                    clone!(@strong value, @strong switch, @strong we_set => move || {
                    we_set.set(true);
                    if let Some(v) = value.current() {
                        let v = val_to_bool(&v);
                        switch.set_active(v);
                        switch.set_state(v);
                    }
                    we_set.set(false);
                    Continue(false)
                }));
            }
            Inhibit(true)
        }));
        switch.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("value: {}, on_change: {}", spec.value, spec.on_change)
            );
            Inhibit(false)
        }));
        switch.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("value: {}, on_change: {}", spec.value, spec.on_change)
                );
                Inhibit(false)
            }),
        );
        Toggle { enabled, value, on_change, switch, we_set }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.switch.upcast_ref()
    }

    pub(super) fn update(&self, ctx: &BSCtx, event: &Target) {
        if let Some(new) = self.enabled.update(ctx, event) {
            self.switch.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.value.update(ctx, event) {
            self.we_set.set(true);
            self.switch.set_active(val_to_bool(&new));
            self.switch.set_state(val_to_bool(&new));
            self.we_set.set(false);
        }
        self.on_change.update(ctx, event);
    }
}

pub(super) struct Entry {
    entry: gtk::Entry,
    we_changed: Rc<Cell<bool>>,
    enabled: Rc<BSNode>,
    visible: Rc<BSNode>,
    text: Rc<BSNode>,
    on_change: Rc<BSNode>,
    on_activate: Rc<BSNode>,
}

impl Entry {
    pub(super) fn new(ctx: &BSCtx, spec: view::Entry, selected_path: gtk::Label) -> Self {
        let we_changed = Rc::new(Cell::new(false));
        let enabled = Rc::new(BSNode::compile(&ctx, spec.enabled.clone()));
        let visible = Rc::new(BSNode::compile(&ctx, spec.visible.clone()));
        let text = Rc::new(BSNode::compile(&ctx, spec.text.clone()));
        let on_change = Rc::new(BSNode::compile(&ctx, spec.on_change.clone()));
        let on_activate = Rc::new(BSNode::compile(&ctx, spec.on_activate.clone()));
        let entry = gtk::Entry::new();
        if let Some(v) = enabled.current() {
            entry.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = visible.current() {
            entry.set_visibility(val_to_bool(&v));
        }
        match text.current() {
            None => entry.set_text(""),
            Some(Value::String(s)) => entry.set_text(&*s),
            Some(v) => entry.set_text(&format!("{}", v)),
        }
        entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
        entry.connect_activate(clone!(
        @strong ctx,
        @strong we_changed,
        @strong text,
        @strong on_activate => move |entry| {
            entry.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
            on_activate.update(
                &ctx,
                &Target::Event(Value::String(Chars::from(String::from(entry.get_text())))),
            );
            idle_add_local(clone!(
                @strong we_changed, @strong text, @strong entry => move || {
                    we_changed.set(true);
                    match text.current() {
                        None => entry.set_text(""),
                        Some(Value::String(s)) => entry.set_text(&*s),
                        Some(v) => entry.set_text(&format!("{}", v)),
                    }
                    we_changed.set(false);
                    Continue(false)
                }));
        }));
        entry.connect_changed(clone!(
        @strong ctx,
        @strong we_changed,
        @strong on_change => move |e| {
            if !we_changed.get() {
                let v = on_change.update(
                    &ctx,
                    &Target::Event(
                        Value::String(Chars::from(String::from(e.get_text())))
                    ),
                );
                if let Some(v) = v {
                    if let Some(set) = v.cast_to::<bool>().ok() {
                        if set {
                            e.set_icon_from_icon_name(
                                gtk::EntryIconPosition::Secondary,
                                Some("media-floppy")
                            );
                        }
                    }
                }
            }
        }));
        entry.connect_icon_press(move |e, _, _| e.emit_activate());
        entry.connect_focus(clone!(@strong spec, @strong selected_path => move |_, _| {
            selected_path.set_label(
                &format!("text: {}, on_change: {}", spec.text, spec.on_change)
            );
            Inhibit(false)
        }));
        Entry { we_changed, entry, enabled, visible, text, on_change, on_activate }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.entry.upcast_ref()
    }

    pub(super) fn update(&self, ctx: &BSCtx, event: &Target) {
        if let Some(new) = self.enabled.update(ctx, event) {
            self.entry.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.visible.update(ctx, event) {
            self.entry.set_visibility(val_to_bool(&new));
        }
        if let Some(new) = self.text.update(ctx, event) {
            self.we_changed.set(true);
            match new {
                Value::String(s) => self.entry.set_text(&*s),
                v => self.entry.set_text(&format!("{}", v)),
            }
            self.we_changed.set(false);
        }
        self.on_change.update(ctx, event);
        self.on_activate.update(ctx, event);
    }
}

fn valid_typ(v: &Value) -> bool {
    v.is_number() || Typ::get(v) == Some(Typ::DateTime)
}

struct ValidTypIter<'a, I> {
    last_valid: Option<&'a Value>,
    inner: I,
}

impl<'a, I: Iterator<Item = &'a Value>> ValidTypIter<'a, I> {
    fn new(i: I) -> Self {
        ValidTypIter { last_valid: None, inner: i }
    }
}

impl<'a, I: Iterator<Item = &'a Value>> Iterator for ValidTypIter<'a, I> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                None => break None,
                Some(v) => {
                    if valid_typ(v) {
                        self.last_valid = Some(v);
                        break Some(v);
                    } else if let Some(v) = self.last_valid {
                        break Some(v);
                    }
                }
            }
        }
    }
}

struct Series {
    line_color: view::RGB,
    x: BSNode,
    y: BSNode,
    x_data: VecDeque<Value>,
    y_data: VecDeque<Value>,
}

pub(super) struct LinePlot {
    root: gtk::Box,
    x_min: Rc<BSNode>,
    x_max: Rc<BSNode>,
    y_min: Rc<BSNode>,
    y_max: Rc<BSNode>,
    keep_points: Rc<BSNode>,
    series: Rc<RefCell<Vec<Series>>>,
}

impl LinePlot {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::LinePlot,
        _selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let canvas = gtk::DrawingArea::new();
        root.pack_start(&canvas, true, true, 0);
        let x_min = Rc::new(BSNode::compile(&ctx, spec.x_min.clone()));
        let x_max = Rc::new(BSNode::compile(&ctx, spec.x_max.clone()));
        let y_min = Rc::new(BSNode::compile(&ctx, spec.y_min.clone()));
        let y_max = Rc::new(BSNode::compile(&ctx, spec.y_max.clone()));
        let keep_points = Rc::new(BSNode::compile(&ctx, spec.keep_points.clone()));
        let series = Rc::new(RefCell::new(
            spec.series
                .iter()
                .map(|series| Series {
                    line_color: series.line_color,
                    x: BSNode::compile(&ctx, series.x.clone()),
                    y: BSNode::compile(&ctx, series.y.clone()),
                    x_data: VecDeque::new(),
                    y_data: VecDeque::new(),
                })
                .collect::<Vec<_>>(),
        ));
        let allocated_width = Rc::new(Cell::new(0));
        let allocated_height = Rc::new(Cell::new(0));
        canvas.connect_draw(clone!(
            @strong allocated_width,
            @strong allocated_height,
            @strong x_min,
            @strong x_max,
            @strong y_min,
            @strong y_max,
            @strong series => move |_, context| {
                // CR estokes: there is a bug in plotters that causes
                // it to somtimes panic in draw it probably isn't
                // strictly unwind safe, but whatever happens it's a
                // lot better than crashing the entire browser.
            let res = catch_unwind(AssertUnwindSafe(|| LinePlot::draw(
                &spec,
                &allocated_width,
                &allocated_height,
                &x_min,
                &x_max,
                &y_min,
                &y_max,
                &series,
                context
            )));
            match res {
                Ok(Ok(())) => (),
                Ok(Err(e)) => warn!("failed to draw lineplot {}", e),
                Err(_) => warn!("failed to draw lineplot, draw paniced"),
            }
            gtk::Inhibit(true)
        }));
        canvas.connect_size_allocate(clone!(
        @strong allocated_width,
        @strong allocated_height => move |_, a| {
            allocated_width.set(i32::abs(a.width) as u32);
            allocated_height.set(i32::abs(a.height) as u32);
        }));
        LinePlot { root, x_min, x_max, y_min, y_max, keep_points, series }
    }

    fn draw(
        spec: &view::LinePlot,
        width: &Rc<Cell<u32>>,
        height: &Rc<Cell<u32>>,
        x_min: &Rc<BSNode>,
        x_max: &Rc<BSNode>,
        y_min: &Rc<BSNode>,
        y_max: &Rc<BSNode>,
        series: &Rc<RefCell<Vec<Series>>>,
        context: &cairo::Context,
    ) -> Result<()> {
        use chrono::Duration;
        use plotters::{coord::ranged1d::ValueFormatter, prelude::*, style::RGBColor};
        use plotters_cairo::CairoBackend;
        use std::cmp::max;
        fn get_min_max(specified: Option<Value>, computed: Value) -> Value {
            match specified {
                None => computed,
                Some(v @ Value::DateTime(_)) => v,
                Some(v @ Value::F64(_)) => v,
                Some(v) => v.cast(Typ::F64).unwrap_or(computed),
            }
        }
        fn to_style(c: view::RGB) -> RGBColor {
            let cvt = |f| (f64::min(1., f) * 255.) as u8;
            RGBColor(cvt(c.r), cvt(c.g), cvt(c.b))
        }
        fn draw_mesh<'a, DB, XT, YT, X, Y>(
            spec: &view::LinePlot,
            chart: &mut ChartContext<'a, DB, Cartesian2d<X, Y>>,
        ) -> Result<()>
        where
            DB: DrawingBackend,
            X: Ranged<ValueType = XT> + ValueFormatter<XT>,
            Y: Ranged<ValueType = YT> + ValueFormatter<YT>,
        {
            let mut mesh = chart.configure_mesh();
            mesh.x_desc(spec.x_label.as_str())
                .y_desc(spec.y_label.as_str())
                .x_labels(spec.x_labels)
                .y_labels(spec.y_labels);
            if !spec.x_grid {
                mesh.disable_x_mesh();
            }
            if !spec.y_grid {
                mesh.disable_y_mesh();
            }
            mesh.draw().map_err(|e| anyhow!("{}", e))
        }
        if width.get() > 0 && height.get() > 0 {
            let (x_min, x_max, y_min, y_max) =
                (x_min.current(), x_max.current(), y_min.current(), y_max.current());
            let mut computed_x_min = series
                .borrow()
                .last()
                .and_then(|s| s.x_data.iter().find(|v| valid_typ(v)))
                .unwrap_or(&Value::F64(0.))
                .clone();
            let mut computed_x_max = computed_x_min.clone();
            let mut computed_y_min = series
                .borrow()
                .last()
                .and_then(|s| s.y_data.iter().find(|v| valid_typ(v)))
                .unwrap_or(&Value::F64(0.))
                .clone();
            let mut computed_y_max = computed_y_min.clone();
            for s in series.borrow().iter() {
                for x in ValidTypIter::new(s.x_data.iter()) {
                    if x < &computed_x_min {
                        computed_x_min = x.clone();
                    }
                    if x > &computed_x_max {
                        computed_x_max = x.clone();
                    }
                }
                for y in ValidTypIter::new(s.y_data.iter()) {
                    if y < &computed_y_min {
                        computed_y_min = y.clone();
                    }
                    if y > &computed_y_max {
                        computed_y_max = y.clone();
                    }
                }
            }
            let x_min = get_min_max(x_min, computed_x_min);
            let x_max = get_min_max(x_max, computed_x_max);
            let y_min = get_min_max(y_min, computed_y_min);
            let y_max = get_min_max(y_max, computed_y_max);
            let back = CairoBackend::new(context, (width.get(), height.get()))?
                .into_drawing_area();
            match spec.fill {
                None => (),
                Some(c) => back.fill(&to_style(c))?,
            }
            let mut chart = ChartBuilder::on(&back);
            chart
                .caption(spec.title.as_str(), ("sans-sherif", 14))
                .margin(spec.margin)
                .set_all_label_area_size(spec.label_area);
            let xtyp = match (Typ::get(&x_min), Typ::get(&x_max)) {
                (Some(t0), Some(t1)) if t0 == t1 => Some(t0),
                (_, _) => None,
            };
            let ytyp = match (Typ::get(&y_min), Typ::get(&y_max)) {
                (Some(t0), Some(t1)) if t0 == t1 => Some(t0),
                (_, _) => None,
            };
            match (xtyp, ytyp) {
                (Some(Typ::DateTime), Some(Typ::DateTime)) => {
                    let xmin = x_min.cast_to::<DateTime<Utc>>().unwrap();
                    let xmax = max(
                        xmin + Duration::seconds(1),
                        x_max.cast_to::<DateTime<Utc>>().unwrap(),
                    );
                    let ymin = y_min.cast_to::<DateTime<Utc>>().unwrap();
                    let ymax = max(
                        ymin + Duration::seconds(1),
                        y_max.cast_to::<DateTime<Utc>>().unwrap(),
                    );
                    let mut chart = chart.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data =
                            s.x_data
                                .iter()
                                .cloned()
                                .filter_map(|v| v.cast_to::<DateTime<Utc>>().ok())
                                .zip(
                                    s.y_data.iter().cloned().filter_map(|v| {
                                        v.cast_to::<DateTime<Utc>>().ok()
                                    }),
                                );
                        let style = to_style(s.line_color);
                        chart.draw_series(LineSeries::new(data, &style))?;
                    }
                }
                (Some(Typ::DateTime), Some(_))
                    if y_min.clone().cast_to::<f64>().is_ok() =>
                {
                    let xmin = x_min.cast_to::<DateTime<Utc>>().unwrap();
                    let xmax = max(
                        xmin + Duration::seconds(1),
                        x_max.cast_to::<DateTime<Utc>>().unwrap(),
                    );
                    let ymin = y_min.cast_to::<f64>().unwrap();
                    let ymax = f64::max(ymin + 1., y_max.cast_to::<f64>().unwrap());
                    let mut chart = chart.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data = s
                            .x_data
                            .iter()
                            .cloned()
                            .filter_map(|v| v.cast_to::<DateTime<Utc>>().ok())
                            .zip(
                                s.y_data
                                    .iter()
                                    .cloned()
                                    .filter_map(|v| v.cast_to::<f64>().ok()),
                            );
                        let style = to_style(s.line_color);
                        chart.draw_series(LineSeries::new(data, &style))?;
                    }
                }
                (Some(_), Some(Typ::DateTime))
                    if x_min.clone().cast_to::<f64>().is_ok() =>
                {
                    let xmin = x_min.cast_to::<f64>().unwrap();
                    let xmax = f64::max(xmin + 1., x_max.cast_to::<f64>().unwrap());
                    let ymin = y_min.cast_to::<DateTime<Utc>>().unwrap();
                    let ymax = max(
                        ymin + Duration::seconds(1),
                        y_max.cast_to::<DateTime<Utc>>().unwrap(),
                    );
                    let mut chart = chart.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data =
                            s.x_data
                                .iter()
                                .cloned()
                                .filter_map(|v| v.cast_to::<f64>().ok())
                                .zip(
                                    s.y_data.iter().cloned().filter_map(|v| {
                                        v.cast_to::<DateTime<Utc>>().ok()
                                    }),
                                );
                        let style = to_style(s.line_color);
                        chart.draw_series(LineSeries::new(data, &style))?;
                    }
                }
                (Some(_), Some(_))
                    if x_min.clone().cast_to::<f64>().is_ok()
                        && y_min.clone().cast_to::<f64>().is_ok() =>
                {
                    let xmin = x_min.cast_to::<f64>().unwrap();
                    let xmax = f64::max(xmin + 1., x_max.cast_to::<f64>().unwrap());
                    let ymin = y_min.cast_to::<f64>().unwrap();
                    let ymax = f64::max(ymin + 1., y_max.cast_to::<f64>().unwrap());
                    let mut chart = chart.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data = s
                            .x_data
                            .iter()
                            .cloned()
                            .filter_map(|v| v.cast_to::<f64>().ok())
                            .zip(
                                s.y_data
                                    .iter()
                                    .cloned()
                                    .filter_map(|v| v.cast_to::<f64>().ok()),
                            );
                        let style = to_style(s.line_color);
                        chart.draw_series(LineSeries::new(data, &style))?;
                    }
                }
                (_, _) => (),
            }
        }
        Ok(())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    pub(super) fn update(&self, ctx: &BSCtx, event: &Target) {
        let mut queue_draw = false;
        if self.x_min.update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.x_max.update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.y_min.update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.y_max.update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.keep_points.update(ctx, event).is_some() {
            queue_draw = true;
        }
        for s in self.series.borrow_mut().iter_mut() {
            if let Some(v) = s.x.update(ctx, event) {
                s.x_data.push_back(v);
                queue_draw = true;
            }
            if let Some(v) = s.y.update(ctx, event) {
                s.y_data.push_back(v);
                queue_draw = true;
            }
            let keep = self
                .keep_points
                .current()
                .and_then(|v| v.cast_to::<u64>().ok())
                .unwrap_or(0);
            while s.x_data.len() > keep as usize {
                s.x_data.pop_front();
            }
            while s.y_data.len() > keep as usize {
                s.y_data.pop_front();
            }
        }
        if queue_draw {
            self.root.queue_draw();
        }
    }
}
