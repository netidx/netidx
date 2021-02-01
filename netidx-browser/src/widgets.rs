use super::{val_to_bool, Vars, WidgetCtx};
use crate::{
    formula::{Expr, Target},
    view,
};
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
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
};

pub(super) struct Button {
    enabled: Expr,
    label: Expr,
    on_click: Expr,
    button: gtk::Button,
}

impl Button {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &Vars,
        spec: view::Button,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::Button::new();
        let enabled = Expr::new(&ctx, variables.clone(), spec.enabled.clone());
        let label = Expr::new(&ctx, variables.clone(), spec.label.clone());
        let on_click = Expr::new(&ctx, variables.clone(), spec.on_click.clone());
        if let Some(v) = enabled.current() {
            button.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = label.current() {
            button.set_label(&format!("{}", v));
        }
        button.connect_clicked(clone!(@strong ctx, @strong on_click => move |_| {
            on_click.update(Target::Event, &Value::Null);
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some(new) = self.enabled.update(tgt, value) {
            self.button.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.label.update(tgt, value) {
            self.button.set_label(&format!("{}", new));
        }
        self.on_click.update(tgt, value);
    }
}

pub(super) struct Label {
    label: gtk::Label,
    text: Expr,
}

impl Label {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &Vars,
        spec: view::Expr,
        selected_path: gtk::Label,
    ) -> Label {
        let text = Expr::new(&ctx, variables.clone(), spec.clone());
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some(new) = self.text.update(tgt, value) {
            self.label.set_label(&format!("{}", new));
        }
    }
}

pub(super) struct Action {
    action: Expr,
}

impl Action {
    pub(super) fn new(ctx: WidgetCtx, variables: &Vars, spec: view::Expr) -> Self {
        let action = Expr::new(&ctx, variables.clone(), spec.clone());
        action.update(Target::Event, &Value::Null);
        Action { action }
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        self.action.update(tgt, value);
    }
}

pub(super) struct Selector {
    root: gtk::EventBox,
    combo: gtk::ComboBoxText,
    enabled: Expr,
    choices: Expr,
    selected: Expr,
    on_change: Expr,
    we_set: Rc<Cell<bool>>,
}

impl Selector {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &Vars,
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
        let enabled = Expr::new(&ctx, variables.clone(), spec.enabled.clone());
        let choices = Expr::new(&ctx, variables.clone(), spec.choices.clone());
        let selected = Expr::new(&ctx, variables.clone(), spec.selected.clone());
        let on_change = Expr::new(&ctx, variables.clone(), spec.on_change.clone());
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
                        on_change.update(Target::Event, &idv);
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        self.we_set.set(true);
        self.on_change.update(tgt, value);
        if let Some(new) = self.enabled.update(tgt, value) {
            self.combo.set_sensitive(val_to_bool(&new));
        }
        Selector::update_active(&self.combo, &self.selected.update(tgt, value));
        if let Some(new) = self.choices.update(tgt, value) {
            Selector::update_choices(&self.combo, &new, &self.selected.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Toggle {
    enabled: Expr,
    value: Expr,
    on_change: Expr,
    we_set: Rc<Cell<bool>>,
    switch: gtk::Switch,
}

impl Toggle {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &Vars,
        spec: view::Toggle,
        selected_path: gtk::Label,
    ) -> Self {
        let switch = gtk::Switch::new();
        let enabled = Expr::new(&ctx, variables.clone(), spec.enabled.clone());
        let value = Expr::new(&ctx, variables.clone(), spec.value.clone());
        let on_change = Expr::new(&ctx, variables.clone(), spec.on_change.clone());
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
                    Target::Event,
                    &if state { Value::True } else { Value::False }
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some(new) = self.enabled.update(tgt, value) {
            self.switch.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.value.update(tgt, value) {
            self.we_set.set(true);
            self.switch.set_active(val_to_bool(&new));
            self.switch.set_state(val_to_bool(&new));
            self.we_set.set(false);
        }
        self.on_change.update(tgt, value);
    }
}

pub(super) struct Entry {
    entry: gtk::Entry,
    enabled: Expr,
    visible: Expr,
    text: Expr,
    on_change: Expr,
}

impl Entry {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &Vars,
        spec: view::Entry,
        selected_path: gtk::Label,
    ) -> Self {
        let enabled = Expr::new(&ctx, variables.clone(), spec.enabled.clone());
        let visible = Expr::new(&ctx, variables.clone(), spec.visible.clone());
        let text = Expr::new(&ctx, variables.clone(), spec.text.clone());
        let on_change = Expr::new(&ctx, variables.clone(), spec.on_change.clone());
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
        fn submit(text: &Expr, on_change: &Expr, entry: &gtk::Entry) {
            on_change.update(
                Target::Event,
                &Value::String(Chars::from(String::from(entry.get_text()))),
            );
            idle_add_local(clone!(@strong text, @strong entry => move || {
                match text.current() {
                    None => entry.set_text(""),
                    Some(Value::String(s)) => entry.set_text(&*s),
                    Some(v) => entry.set_text(&format!("{}", v)),
                }
                Continue(false)
            }));
        }
        entry.connect_activate(clone!(@strong text, @strong on_change => move |entry| {
            submit(&text, &on_change, entry)
        }));
        entry.connect_focus_out_event(
            clone!(@strong text, @strong on_change => move |w, _| {
                let entry = w.downcast_ref::<gtk::Entry>().unwrap();
                submit(&text, &on_change, entry);
                Inhibit(false)
            }),
        );
        entry.connect_focus(clone!(@strong spec, @strong selected_path => move |_, _| {
            selected_path.set_label(
                &format!("text: {}, on_change: {}", spec.text, spec.on_change)
            );
            Inhibit(false)
        }));
        // CR estokes: give the user an indication that it's out of sync
        Entry { entry, enabled, visible, text, on_change }
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
        if let Some(new) = self.text.update(tgt, value) {
            match new {
                Value::String(s) => self.entry.set_text(&*s),
                v => self.entry.set_text(&format!("{}", v)),
            }
        }
        self.on_change.update(tgt, value);
    }
}

struct Series {
    line_color: view::RGB,
    x: Expr,
    y: Expr,
    x_data: VecDeque<Value>,
    y_data: VecDeque<Value>,
}

pub(super) struct LinePlot {
    root: gtk::Box,
    x_min: Rc<Expr>,
    x_max: Rc<Expr>,
    y_min: Rc<Expr>,
    y_max: Rc<Expr>,
    keep_points: Rc<Expr>,
    series: Rc<RefCell<Vec<Series>>>,
}

impl LinePlot {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &Vars,
        spec: view::LinePlot,
        _selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let canvas = gtk::DrawingArea::new();
        root.pack_start(&canvas, true, true, 0);
        let x_min = Rc::new(Expr::new(&ctx, variables.clone(), spec.x_min.clone()));
        let x_max = Rc::new(Expr::new(&ctx, variables.clone(), spec.x_max.clone()));
        let y_min = Rc::new(Expr::new(&ctx, variables.clone(), spec.y_min.clone()));
        let y_max = Rc::new(Expr::new(&ctx, variables.clone(), spec.y_max.clone()));
        let keep_points =
            Rc::new(Expr::new(&ctx, variables.clone(), spec.keep_points.clone()));
        let series = Rc::new(RefCell::new(
            spec.series
                .iter()
                .map(|series| Series {
                    line_color: series.line_color,
                    x: Expr::new(&ctx, variables.clone(), series.x.clone()),
                    y: Expr::new(&ctx, variables.clone(), series.y.clone()),
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
        x_min: &Rc<Expr>,
        x_max: &Rc<Expr>,
        y_min: &Rc<Expr>,
        y_max: &Rc<Expr>,
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
        fn valid_typ(v: &Value) -> bool {
            v.is_number() || Typ::get(v) == Some(Typ::DateTime)
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
                for x in s.x_data.iter().filter(|v| valid_typ(v)) {
                    if x < &computed_x_min {
                        computed_x_min = x.clone();
                    }
                    if x > &computed_x_max {
                        computed_x_max = x.clone();
                    }
                }
                for y in s.y_data.iter().filter(|v| valid_typ(v)) {
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
