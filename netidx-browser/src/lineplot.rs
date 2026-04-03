use crate::{
    cairo_backend::CairoBackend, view, BCtx, BWidget, GxProp,
};
use anyhow::anyhow;
use chrono::prelude::*;
use glib::{clone, Propagation};
use graphix_compiler::expr::ExprId;
use gtk::prelude::*;
use log::warn;
use netidx::publisher::{Typ, Value};
use std::{
    cell::{Cell, RefCell},
    cmp::max,
    collections::VecDeque,
    panic::{catch_unwind, AssertUnwindSafe},
    rc::Rc,
};

fn valid_typ(v: &Value) -> bool {
    v.number() || Typ::get(v) == Typ::DateTime
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

struct SeriesData {
    x: Option<GxProp>,
    y: Option<GxProp>,
    x_data: VecDeque<Value>,
    y_data: VecDeque<Value>,
}

/// Shared state between the widget and the draw callback.
/// The draw callback captures an Rc to this so it can read the
/// accumulated data without requiring &mut self.
struct PlotState {
    x_min: Option<Value>,
    x_max: Option<Value>,
    y_min: Option<Value>,
    y_max: Option<Value>,
    series: Vec<SeriesState>,
}

struct SeriesState {
    line_color: view::RGB,
    x_data: VecDeque<Value>,
    y_data: VecDeque<Value>,
}

pub(crate) struct LinePlot {
    root: gtk::Box,
    canvas: gtk::DrawingArea,
    x_min: Option<GxProp>,
    x_max: Option<GxProp>,
    y_min: Option<GxProp>,
    y_max: Option<GxProp>,
    keep_points: Option<GxProp>,
    series: Vec<SeriesData>,
    state: Rc<RefCell<PlotState>>,
}

fn to_style(c: view::RGB) -> plotters::style::RGBColor {
    let cvt = |f: f64| (f64::min(1., f) * 255.) as u8;
    plotters::style::RGBColor(cvt(c.r), cvt(c.g), cvt(c.b))
}

fn get_min_max(specified: &Option<Value>, computed: Value) -> Value {
    match specified {
        None => computed,
        Some(v) => match Typ::get(v) {
            Typ::DateTime | Typ::F64 => v.clone(),
            _ => v.clone().cast(Typ::F64).unwrap_or(computed),
        },
    }
}

/// Configure the mesh on a chart context. This is a macro because the
/// plotters mesh API requires the concrete Ranged types to implement
/// ValueFormatter, which makes a generic helper difficult.
macro_rules! configure_mesh {
    ($spec:expr, $chart:expr) => {{
        let mut mesh = $chart.configure_mesh();
        mesh.x_desc($spec.x_label.as_str())
            .y_desc($spec.y_label.as_str())
            .x_labels($spec.x_labels)
            .y_labels($spec.y_labels);
        if !$spec.x_grid {
            mesh.disable_x_mesh();
        }
        if !$spec.y_grid {
            mesh.disable_y_mesh();
        }
        mesh.draw().map_err(|e| anyhow!("{}", e))
    }};
}

impl LinePlot {
    pub(crate) fn new(ctx: &BCtx, spec: view::LinePlot) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let canvas = gtk::DrawingArea::new();
        root.set_no_show_all(true);
        canvas.set_no_show_all(true);
        root.pack_start(&canvas, true, true, 0);
        let x_min = GxProp::compile(ctx, &spec.x_min);
        let x_max = GxProp::compile(ctx, &spec.x_max);
        let y_min = GxProp::compile(ctx, &spec.y_min);
        let y_max = GxProp::compile(ctx, &spec.y_max);
        let keep_points = GxProp::compile(ctx, &spec.keep_points);
        let series: Vec<SeriesData> = spec
            .series
            .iter()
            .map(|s| SeriesData {
                x: GxProp::compile(ctx, &s.x),
                y: GxProp::compile(ctx, &s.y),
                x_data: VecDeque::new(),
                y_data: VecDeque::new(),
            })
            .collect();
        let state = Rc::new(RefCell::new(PlotState {
            x_min: None,
            x_max: None,
            y_min: None,
            y_max: None,
            series: spec
                .series
                .iter()
                .map(|s| SeriesState {
                    line_color: s.line_color,
                    x_data: VecDeque::new(),
                    y_data: VecDeque::new(),
                })
                .collect(),
        }));
        let allocated_width = Rc::new(Cell::new(0u32));
        let allocated_height = Rc::new(Cell::new(0u32));
        canvas.connect_draw(clone!(
            @strong allocated_width,
            @strong allocated_height,
            @strong state => move |_, context| {
                let res = catch_unwind(AssertUnwindSafe(|| {
                    Self::draw_plot(
                        &spec,
                        allocated_width.get(),
                        allocated_height.get(),
                        &state.borrow(),
                        context,
                    )
                }));
                match res {
                    Ok(Ok(())) => (),
                    Ok(Err(e)) => warn!("failed to draw lineplot: {}", e),
                    Err(_) => warn!("lineplot draw panicked"),
                }
                Propagation::Stop
            }
        ));
        canvas.connect_size_allocate(clone!(
            @strong allocated_width,
            @strong allocated_height => move |_, a| {
                allocated_width.set(i32::abs(a.width()) as u32);
                allocated_height.set(i32::abs(a.height()) as u32);
            }
        ));
        LinePlot {
            root,
            canvas,
            x_min,
            x_max,
            y_min,
            y_max,
            keep_points,
            series,
            state,
        }
    }

    /// Sync the mutable series data into the shared state for the draw callback.
    fn sync_state(&self) {
        let mut st = self.state.borrow_mut();
        st.x_min = self.x_min.as_ref().and_then(|p| p.last.clone());
        st.x_max = self.x_max.as_ref().and_then(|p| p.last.clone());
        st.y_min = self.y_min.as_ref().and_then(|p| p.last.clone());
        st.y_max = self.y_max.as_ref().and_then(|p| p.last.clone());
        for (sd, ss) in self.series.iter().zip(st.series.iter_mut()) {
            ss.x_data.clone_from(&sd.x_data);
            ss.y_data.clone_from(&sd.y_data);
        }
    }

    fn draw_plot(
        spec: &view::LinePlot,
        width: u32,
        height: u32,
        state: &PlotState,
        context: &gdk::cairo::Context,
    ) -> anyhow::Result<()> {
        use plotters::prelude::*;
        if width == 0 || height == 0 {
            return Ok(());
        }
        // Compute axis bounds from data if not specified
        let mut computed_x_min = state
            .series
            .last()
            .and_then(|s| s.x_data.iter().find(|v| valid_typ(v)))
            .cloned()
            .unwrap_or(Value::F64(0.));
        let mut computed_x_max = computed_x_min.clone();
        let mut computed_y_min = state
            .series
            .last()
            .and_then(|s| s.y_data.iter().find(|v| valid_typ(v)))
            .cloned()
            .unwrap_or(Value::F64(0.));
        let mut computed_y_max = computed_y_min.clone();
        for s in state.series.iter() {
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
        let x_min = get_min_max(&state.x_min, computed_x_min);
        let x_max = get_min_max(&state.x_max, computed_x_max);
        let y_min = get_min_max(&state.y_min, computed_y_min);
        let y_max = get_min_max(&state.y_max, computed_y_max);
        let back =
            CairoBackend::new(context, (width, height))?.into_drawing_area();
        if let Some(c) = spec.fill {
            back.fill(&to_style(c))?;
        }
        let mut builder = ChartBuilder::on(&back);
        builder
            .caption(spec.title.as_str(), ("sans-serif", 14))
            .margin(spec.margin)
            .set_all_label_area_size(spec.label_area);
        let xtyp = match (Typ::get(&x_min), Typ::get(&x_max)) {
            (t0, t1) if t0 == t1 => Some(t0),
            (_, _) => None,
        };
        let ytyp = match (Typ::get(&y_min), Typ::get(&y_max)) {
            (t0, t1) if t0 == t1 => Some(t0),
            (_, _) => None,
        };
        match (xtyp, ytyp) {
            (Some(Typ::DateTime), Some(Typ::DateTime)) => {
                let xmin = x_min.cast_to::<DateTime<Utc>>().unwrap();
                let xmax = max(
                    xmin + chrono::Duration::seconds(1),
                    x_max.cast_to::<DateTime<Utc>>().unwrap(),
                );
                let ymin = y_min.cast_to::<DateTime<Utc>>().unwrap();
                let ymax = max(
                    ymin + chrono::Duration::seconds(1),
                    y_max.cast_to::<DateTime<Utc>>().unwrap(),
                );
                let mut chart =
                    builder.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                configure_mesh!(spec, chart)?;
                for s in state.series.iter() {
                    let data = s
                        .x_data
                        .iter()
                        .cloned()
                        .filter_map(|v| v.cast_to::<DateTime<Utc>>().ok())
                        .zip(
                            s.y_data
                                .iter()
                                .cloned()
                                .filter_map(|v| v.cast_to::<DateTime<Utc>>().ok()),
                        );
                    let style = to_style(s.line_color);
                    chart
                        .draw_series(LineSeries::new(data, &style))
                        .map_err(|e| anyhow!("{}", e))?;
                }
            }
            (Some(Typ::DateTime), Some(_))
                if y_min.clone().cast_to::<f64>().is_ok() =>
            {
                let xmin = x_min.cast_to::<DateTime<Utc>>().unwrap();
                let xmax = max(
                    xmin + chrono::Duration::seconds(1),
                    x_max.cast_to::<DateTime<Utc>>().unwrap(),
                );
                let ymin = y_min.cast_to::<f64>().unwrap();
                let ymax = f64::max(ymin + 1., y_max.cast_to::<f64>().unwrap());
                let mut chart =
                    builder.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                configure_mesh!(spec, chart)?;
                for s in state.series.iter() {
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
                    chart
                        .draw_series(LineSeries::new(data, &style))
                        .map_err(|e| anyhow!("{}", e))?;
                }
            }
            (Some(_), Some(Typ::DateTime))
                if x_min.clone().cast_to::<f64>().is_ok() =>
            {
                let xmin = x_min.cast_to::<f64>().unwrap();
                let xmax = f64::max(xmin + 1., x_max.cast_to::<f64>().unwrap());
                let ymin = y_min.cast_to::<DateTime<Utc>>().unwrap();
                let ymax = max(
                    ymin + chrono::Duration::seconds(1),
                    y_max.cast_to::<DateTime<Utc>>().unwrap(),
                );
                let mut chart =
                    builder.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                configure_mesh!(spec, chart)?;
                for s in state.series.iter() {
                    let data = s
                        .x_data
                        .iter()
                        .cloned()
                        .filter_map(|v| v.cast_to::<f64>().ok())
                        .zip(
                            s.y_data
                                .iter()
                                .cloned()
                                .filter_map(|v| v.cast_to::<DateTime<Utc>>().ok()),
                        );
                    let style = to_style(s.line_color);
                    chart
                        .draw_series(LineSeries::new(data, &style))
                        .map_err(|e| anyhow!("{}", e))?;
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
                let mut chart =
                    builder.build_cartesian_2d(xmin..xmax, ymin..ymax)?;
                configure_mesh!(spec, chart)?;
                for s in state.series.iter() {
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
                    chart
                        .draw_series(LineSeries::new(data, &style))
                        .map_err(|e| anyhow!("{}", e))?;
                }
            }
            (_, _) => (),
        }
        Ok(())
    }
}

impl BWidget for LinePlot {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut queue_draw = false;
        if let Some(ref mut p) = self.x_min {
            if p.update(id, value) {
                queue_draw = true;
            }
        }
        if let Some(ref mut p) = self.x_max {
            if p.update(id, value) {
                queue_draw = true;
            }
        }
        if let Some(ref mut p) = self.y_min {
            if p.update(id, value) {
                queue_draw = true;
            }
        }
        if let Some(ref mut p) = self.y_max {
            if p.update(id, value) {
                queue_draw = true;
            }
        }
        if let Some(ref mut p) = self.keep_points {
            if p.update(id, value) {
                queue_draw = true;
            }
        }
        let keep = self
            .keep_points
            .as_ref()
            .and_then(|p| p.last.clone())
            .and_then(|v| v.cast_to::<u64>().ok())
            .unwrap_or(0) as usize;
        for s in self.series.iter_mut() {
            if let Some(ref mut p) = s.x {
                if p.update(id, value) {
                    s.x_data.push_back(value.clone());
                    queue_draw = true;
                }
            }
            if let Some(ref mut p) = s.y {
                if p.update(id, value) {
                    s.y_data.push_back(value.clone());
                    queue_draw = true;
                }
            }
            while s.x_data.len() > keep {
                s.x_data.pop_front();
            }
            while s.y_data.len() > keep {
                s.y_data.pop_front();
            }
        }
        if queue_draw {
            self.sync_state();
            self.root.queue_draw();
        }
        queue_draw
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.canvas.set_visible(v);
        self.root.set_visible(v);
    }
}
