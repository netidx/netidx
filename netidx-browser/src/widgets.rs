use super::{val_to_bool, Sink, Source, Target, WidgetCtx};
use crate::view;
use anyhow::Result;
use cairo;
use gdk::{self, prelude::*};
use glib::{clone, idle_add_local};
use gtk::{self, prelude::*};
use log::warn;
use netidx::{chars::Chars, subscriber::Value};
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
    title: Source,
    x: Source,
    y: Source,
    x_data: VecDeque<f64>,
    y_data: VecDeque<f64>,
}

pub(super) struct LinePlot {
    root: gtk::DrawingArea,
    title: Rc<Source>,
    x_label: Rc<Source>,
    y_label: Rc<Source>,
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
        let title = Rc::new(Source::new(&ctx, variables, spec.title.clone()));
        let x_label = Rc::new(Source::new(&ctx, variables, spec.x_label.clone()));
        let y_label = Rc::new(Source::new(&ctx, variables, spec.y_label.clone()));
        let timeseries = Rc::new(Source::new(&ctx, variables, spec.timeseries.clone()));
        let keep_points = Rc::new(Source::new(&ctx, variables, spec.keep_points.clone()));
        let series = Rc::new(RefCell::new(
            spec.series
                .iter()
                .map(|series| Series {
                    title: Source::new(&ctx, variables, series.title.clone()),
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
            @strong title,
            @strong x_label,
            @strong y_label,
            @strong allocated_width,
            @strong allocated_height,
            @strong timeseries,
            @strong series => move |_, context| {
            let res = LinePlot::draw(
                &title,
                &x_label,
                &y_label,
                &allocated_width,
                &allocated_height,
                &timeseries,
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
        LinePlot { root, title, x_label, y_label, keep_points, series }
    }

    fn draw(
        title: &Rc<Source>,
        x_label: &Rc<Source>,
        y_label: &Rc<Source>,
        width: &Rc<Cell<u32>>,
        height: &Rc<Cell<u32>>,
        _timeseries: &Rc<Source>,
        series: &Rc<RefCell<Vec<Series>>>,
        context: &cairo::Context,
    ) -> Result<()> {
        use plotters::prelude::*;
        use plotters_cairo::CairoBackend;
        fn get_str(v: &Option<Value>) -> &str {
            match v {
                Some(Value::String(c)) => &*c,
                Some(_) | None => "",
            }
        }
        if width.get() > 0 && height.get() > 0 {
            let title = title.current();
            let title = get_str(&title);
            let x_label = x_label.current();
            let x_label = get_str(&x_label);
            let y_label = y_label.current();
            let y_label = get_str(&y_label);
            let mut x_min = 0.;
            let mut x_max = 0.;
            let mut y_min = 0.;
            let mut y_max = 0.;
            for s in series.borrow().iter() {
                for x in s.x_data.iter() {
                    x_min = f64::min(x_min, *x);
                    x_max = f64::max(x_max, *x);
                }
                for y in s.y_data.iter() {
                    y_min = f64::min(y_min, *y);
                    y_max = f64::max(y_max, *y);
                }
            }
            let back = CairoBackend::new(context, (width.get(), height.get()))?
                .into_drawing_area();
            back.fill(&WHITE)?;
            let xr = dbg!(x_min)..dbg!(f64::max(x_min + 1., x_max));
            let yr = dbg!(y_min)..dbg!(f64::max(y_min + 1., y_max));
            let mut chart = ChartBuilder::on(&back)
                .caption(title, ("sans-sherif", 14))
                .build_cartesian_2d(xr, yr)?;
            chart.configure_mesh().x_desc(x_label).y_desc(y_label).draw()?;
            let styles = [&RED, &GREEN, &MAGENTA, &BLUE, &BLACK, &CYAN, &WHITE, &YELLOW];
            for (i, s) in series.borrow().iter().enumerate() {
                let data = s.x_data.iter().copied().zip(s.y_data.iter().copied());
                chart.draw_series(LineSeries::new(data, styles[i % 8]))?;
            }
        }
        Ok(())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        let mut queue_draw = false;
        if self.title.update(tgt, value).is_some() {
            queue_draw = true;
        }
        if self.x_label.update(tgt, value).is_some() {
            queue_draw = true;
        }
        if self.y_label.update(tgt, value).is_some() {
            queue_draw = true;
        }
        if self.keep_points.update(tgt, value).is_some() {
            queue_draw = true;
        }
        for s in self.series.borrow_mut().iter_mut() {
            if let Some(v) = s.x.update(tgt, value).and_then(|v| v.cast_f64()) {
                s.x_data.push_back(v);
                queue_draw = true;
            }
            if let Some(v) = s.y.update(tgt, value).and_then(|v| v.cast_f64()) {
                s.y_data.push_back(v);
                queue_draw = true;
            }
            if s.title.update(tgt, value).is_some() {
                queue_draw = true;
            }
            if let Some(keep) = self.keep_points.current().and_then(|v| v.cast_u64()) {
                while s.x_data.len() > keep as usize {
                    s.x_data.pop_front();
                }
                while s.y_data.len() > keep as usize {
                    s.y_data.pop_front();
                }
            }
        }
        if queue_draw {
            self.root.queue_draw();
        }
    }
}
