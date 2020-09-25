use super::{val_to_bool, Sink, Source, Target, WidgetCtx};
use crate::view;
use anyhow::{anyhow, Result};
use cairo;
use gdk::{self, prelude::*};
use glib::{clone, idle_add_local};
use gtk::{self, prelude::*};
use log::warn;
use netidx::{
    chars::Chars,
    subscriber::{Typ, Value},
};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    rc::Rc,
};

pub(super) struct Button {
    enabled: Source,
    label: Source,
    source: Source,
    button: gtk::Button,
}

impl Button {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Button,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::Button::new();
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let label = Source::new(&ctx, variables, spec.label.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        if let Some(v) = enabled.current() {
            button.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = label.current() {
            button.set_label(&format!("{}", v));
        }
        button.connect_clicked(clone!(@strong ctx, @strong source, @strong sink =>
        move |_| {
            if let Some(v) = source.current() {
                sink.set(&ctx, v);
            }
        }));
        button.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("source: {}, sink: {}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        button.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("source: {}, sink: {}", spec.source, spec.sink)
                );
                Inhibit(false)
            }),
        );
        Button { enabled, label, source, button }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.button.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        let _: Option<Value> = self.source.update(tgt, value);
        if let Some(new) = self.enabled.update(tgt, value) {
            self.button.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.label.update(tgt, value) {
            self.button.set_label(&format!("{}", new));
        }
    }
}

pub(super) struct Label {
    label: gtk::Label,
    source: Source,
}

impl Label {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Source,
        selected_path: gtk::Label,
    ) -> Label {
        let source = Source::new(&ctx, variables, spec.clone());
        let txt = match source.current() {
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
        Label { source, label }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.label.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some(new) = self.source.update(tgt, value) {
            self.label.set_label(&format!("{}", new));
        }
    }
}

pub(super) struct Action {
    source: Source,
    sink: Sink,
    ctx: WidgetCtx,
}

impl Action {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Action,
    ) -> Self {
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        if let Some(cur) = source.current() {
            sink.set(&ctx, cur);
        }
        Action { source, sink, ctx }
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some(new) = self.source.update(tgt, value) {
            self.sink.set(&self.ctx, new);
        }
    }
}

pub(super) struct Selector {
    root: gtk::EventBox,
    combo: gtk::ComboBoxText,
    enabled: Source,
    choices: Source,
    source: Source,
    we_set: Rc<Cell<bool>>,
}

impl Selector {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Selector,
        selected_path: gtk::Label,
    ) -> Self {
        let combo = gtk::ComboBoxText::new();
        let root = gtk::EventBox::new();
        root.add(&combo);
        combo.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!(
                    "source: {}, sink: {}, choices: {}",
                    spec.source, spec.sink, spec.choices
                )
            );
            Inhibit(false)
        }));
        root.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!(
                        "source: {}, sink: {}, choices: {}",
                        spec.source, spec.sink, spec.choices
                    )
                );
                Inhibit(false)
            }),
        );
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let choices = Source::new(&ctx, variables, spec.choices.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        let we_set = Rc::new(Cell::new(false));
        if let Some(v) = enabled.current() {
            combo.set_sensitive(val_to_bool(&v));
        }
        if let Some(choices) = choices.current() {
            Selector::update_choices(&combo, &choices, &source.current());
        }
        we_set.set(true);
        Selector::update_active(&combo, &source.current());
        we_set.set(false);
        combo.connect_changed(clone!(
            @strong we_set, @strong sink, @strong ctx, @strong source => move |combo| {
            if !we_set.get() {
                if let Some(id) = combo.get_active_id() {
                    if let Ok(idv) = serde_json::from_str::<Value>(id.as_str()) {
                        sink.set(&ctx, idv);
                    }
                }
                idle_add_local(clone!(
                    @strong source, @strong combo, @strong we_set => move || {
                        we_set.set(true);
                        Selector::update_active(&combo, &source.current());
                        we_set.set(false);
                        Continue(false)
                    })
                );
            }
        }));
        Selector { root, combo, enabled, choices, source, we_set }
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        self.we_set.set(true);
        if let Some(new) = self.enabled.update(tgt, value) {
            self.combo.set_sensitive(val_to_bool(&new));
        }
        Selector::update_active(&self.combo, &self.source.update(tgt, value));
        if let Some(new) = self.choices.update(tgt, value) {
            Selector::update_choices(&self.combo, &new, &self.source.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Toggle {
    enabled: Source,
    source: Source,
    we_set: Rc<Cell<bool>>,
    switch: gtk::Switch,
}

impl Toggle {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Toggle,
        selected_path: gtk::Label,
    ) -> Self {
        let switch = gtk::Switch::new();
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        let we_set = Rc::new(Cell::new(false));
        if let Some(v) = enabled.current() {
            switch.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = source.current() {
            let v = val_to_bool(&v);
            switch.set_active(v);
            switch.set_state(v);
        }
        switch.connect_state_set(clone!(
        @strong ctx, @strong sink, @strong we_set, @strong source =>
        move |switch, state| {
            if !we_set.get() {
                sink.set(&ctx, if state { Value::True } else { Value::False });
                idle_add_local(
                    clone!(@strong source, @strong switch, @strong we_set => move || {
                    we_set.set(true);
                    if let Some(v) = source.current() {
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
                &format!("source: {}, sink: {}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        switch.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("source: {}, sink: {}", spec.source, spec.sink)
                );
                Inhibit(false)
            }),
        );
        Toggle { enabled, source, switch, we_set }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.switch.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some(new) = self.enabled.update(tgt, value) {
            self.switch.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.source.update(tgt, value) {
            self.we_set.set(true);
            self.switch.set_active(val_to_bool(&new));
            self.switch.set_state(val_to_bool(&new));
            self.we_set.set(false);
        }
    }
}

pub(super) struct Entry {
    entry: gtk::Entry,
    enabled: Source,
    visible: Source,
    source: Source,
}

impl Entry {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Entry,
        selected_path: gtk::Label,
    ) -> Self {
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let visible = Source::new(&ctx, variables, spec.visible.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        let entry = gtk::Entry::new();
        if let Some(v) = enabled.current() {
            entry.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = visible.current() {
            entry.set_visibility(val_to_bool(&v));
        }
        match source.current() {
            None => entry.set_text(""),
            Some(Value::String(s)) => entry.set_text(&*s),
            Some(v) => entry.set_text(&format!("{}", v)),
        }
        fn submit(ctx: &WidgetCtx, source: &Source, sink: &Sink, entry: &gtk::Entry) {
            sink.set(&ctx, Value::String(Chars::from(String::from(entry.get_text()))));
            idle_add_local(clone!(@strong source, @strong entry => move || {
                match source.current() {
                    None => entry.set_text(""),
                    Some(Value::String(s)) => entry.set_text(&*s),
                    Some(v) => entry.set_text(&format!("{}", v)),
                }
                Continue(false)
            }));
        }
        entry.connect_activate(
            clone!(@strong source, @strong ctx, @strong sink => move |entry| {
                submit(&ctx, &source, &sink, entry)
            }),
        );
        entry.connect_focus_out_event(
            clone!(@strong source, @strong ctx, @strong sink => move |w, _| {
                let entry = w.downcast_ref::<gtk::Entry>().unwrap();
                submit(&ctx, &source, &sink, entry);
                Inhibit(false)
            }),
        );
        entry.connect_focus(clone!(@strong spec, @strong selected_path => move |_, _| {
            selected_path.set_label(
                &format!("source: {}, sink: {}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        // CR estokes: give the user an indication that it's out of sync
        Entry { entry, enabled, visible, source }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.entry.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some(new) = self.enabled.update(tgt, value) {
            self.entry.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.visible.update(tgt, value) {
            self.entry.set_visibility(val_to_bool(&new));
        }
        if let Some(new) = self.source.update(tgt, value) {
            match new {
                Value::String(s) => self.entry.set_text(&*s),
                v => self.entry.set_text(&format!("{}", v)),
            }
        }
    }
}

struct Series {
    line_color: view::RGB,
    x: Source,
    y: Source,
    x_data: VecDeque<Value>,
    y_data: VecDeque<Value>,
}

pub(super) struct LinePlot {
    root: gtk::DrawingArea,
    x_min: Rc<Source>,
    x_max: Rc<Source>,
    y_min: Rc<Source>,
    y_max: Rc<Source>,
    keep_points: Rc<Source>,
    series: Rc<RefCell<Vec<Series>>>,
}

impl LinePlot {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::LinePlot,
        _selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::DrawingArea::new();
        let x_min = Rc::new(Source::new(&ctx, variables, spec.x_min.clone()));
        let x_max = Rc::new(Source::new(&ctx, variables, spec.x_max.clone()));
        let y_min = Rc::new(Source::new(&ctx, variables, spec.y_min.clone()));
        let y_max = Rc::new(Source::new(&ctx, variables, spec.y_max.clone()));
        let keep_points = Rc::new(Source::new(&ctx, variables, spec.keep_points.clone()));
        let series = Rc::new(RefCell::new(
            spec.series
                .iter()
                .map(|series| Series {
                    line_color: series.line_color,
                    x: Source::new(&ctx, variables, series.x.clone()),
                    y: Source::new(&ctx, variables, series.y.clone()),
                    x_data: VecDeque::new(),
                    y_data: VecDeque::new(),
                })
                .collect::<Vec<_>>(),
        ));
        let allocated_width = Rc::new(Cell::new(0));
        let allocated_height = Rc::new(Cell::new(0));
        root.connect_draw(clone!(
            @strong allocated_width,
            @strong allocated_height,
            @strong x_min,
            @strong x_max,
            @strong y_min,
            @strong y_max,
            @strong keep_points,
            @strong series => move |_, context| {
            let res = LinePlot::draw(
                &spec,
                &allocated_width,
                &allocated_height,
                &x_min,
                &x_max,
                &y_min,
                &y_max,
                &series,
                context
            );
            match res {
                Ok(()) => (),
                Err(e) => warn!("failed to draw lineplot {}", e),
            }
            gtk::Inhibit(true)
        }));
        root.connect_size_allocate(clone!(
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
        x_min: &Rc<Source>,
        x_max: &Rc<Source>,
        y_min: &Rc<Source>,
        y_max: &Rc<Source>,
        series: &Rc<RefCell<Vec<Series>>>,
        context: &cairo::Context,
    ) -> Result<()> {
        use plotters::{coord::ranged1d::ValueFormatter, prelude::*, style::RGBColor};
        use plotters_cairo::CairoBackend;
        fn get_min_max(specified: Option<Value>, computed: Value) -> Value {
            match specified {
                None => computed,
                Some(v @ Value::DateTime(_)) => v,
                Some(v @ Value::F64(_)) => v,
                Some(v) => v.cast(Typ::F64).unwrap_or(computed),
            }
        }
        fn to_style(c: view::RGB) -> RGBColor {
            let cvt = |f| (f64::max(1., f) * 255.) as u8;
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
            mesh.x_desc(spec.x_label.as_str()).y_desc(spec.y_label.as_str());
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
                .and_then(|s| s.x_data.back())
                .unwrap_or(&Value::F64(0.))
                .clone();
            let mut computed_x_max = computed_x_min.clone();
            let mut computed_y_min = series
                .borrow()
                .last()
                .and_then(|s| s.y_data.back())
                .unwrap_or(&Value::F64(0.))
                .clone();
            let mut computed_y_max = computed_y_min.clone();
            for s in series.borrow().iter() {
                for x in s.x_data.iter() {
                    if x < &computed_x_min {
                        computed_x_min = x.clone();
                    }
                    if x > &computed_x_max {
                        computed_x_max = x.clone();
                    }
                }
                for y in s.y_data.iter() {
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
                (Some(Typ::F64), Some(Typ::F64)) => {
                    let xr = x_min.cast_f64().unwrap()..x_max.cast_f64().unwrap();
                    let yr = y_min.cast_f64().unwrap()..y_max.cast_f64().unwrap();
                    let mut chart = chart.build_cartesian_2d(xr, yr)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data =
                            s.x_data.iter().cloned().filter_map(|v| v.cast_f64()).zip(
                                s.y_data.iter().cloned().filter_map(|v| v.cast_f64()),
                            );
                        let style = to_style(s.line_color);
                        chart.draw_series(LineSeries::new(data, &style))?;
                    }
                }
                (Some(Typ::DateTime), Some(Typ::DateTime)) => {
                    let xr =
                        x_min.cast_datetime().unwrap()..x_max.cast_datetime().unwrap();
                    let yr =
                        y_min.cast_datetime().unwrap()..y_max.cast_datetime().unwrap();
                    let mut chart = chart.build_cartesian_2d(xr, yr)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data = s
                            .x_data
                            .iter()
                            .cloned()
                            .filter_map(|v| v.cast_datetime())
                            .zip(
                                s.y_data
                                    .iter()
                                    .cloned()
                                    .filter_map(|v| v.cast_datetime()),
                            );
                        let style = to_style(s.line_color);
                        chart.draw_series(LineSeries::new(data, &style))?;
                    }
                }
                (Some(Typ::DateTime), Some(Typ::F64)) => {
                    let xr =
                        x_min.cast_datetime().unwrap()..x_max.cast_datetime().unwrap();
                    let yr = y_min.cast_f64().unwrap()..y_max.cast_f64().unwrap();
                    let mut chart = chart.build_cartesian_2d(xr, yr)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data = s
                            .x_data
                            .iter()
                            .cloned()
                            .filter_map(|v| v.cast_datetime())
                            .zip(s.y_data.iter().cloned().filter_map(|v| v.cast_f64()));
                        let style = to_style(s.line_color);
                        chart.draw_series(LineSeries::new(data, &style))?;
                    }
                }
                (Some(Typ::F64), Some(Typ::DateTime)) => {
                    let xr = x_min.cast_f64().unwrap()..x_max.cast_f64().unwrap();
                    let yr =
                        y_min.cast_datetime().unwrap()..y_max.cast_datetime().unwrap();
                    let mut chart = chart.build_cartesian_2d(xr, yr)?;
                    draw_mesh(spec, &mut chart)?;
                    for s in series.borrow().iter() {
                        let data =
                            s.x_data.iter().cloned().filter_map(|v| v.cast_f64()).zip(
                                s.y_data
                                    .iter()
                                    .cloned()
                                    .filter_map(|v| v.cast_datetime()),
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        let mut queue_draw = false;
        if self.x_min.update(tgt, value).is_some() {
            queue_draw = true;
        }
        if self.x_max.update(tgt, value).is_some() {
            queue_draw = true;
        }
        if self.y_min.update(tgt, value).is_some() {
            queue_draw = true;
        }
        if self.y_max.update(tgt, value).is_some() {
            queue_draw = true;
        }
        if self.keep_points.update(tgt, value).is_some() {
            queue_draw = true;
        }
        for s in self.series.borrow_mut().iter_mut() {
            if let Some(v) = s.x.update(tgt, value) {
                s.x_data.push_back(v);
                queue_draw = true;
            }
            if let Some(v) = s.y.update(tgt, value) {
                s.y_data.push_back(v);
                queue_draw = true;
            }
            let keep = self.keep_points.current().and_then(|v| v.cast_u64()).unwrap_or(0);
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
