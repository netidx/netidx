use super::{BSCtx, BSCtxRef, BSNode, BWidget};
use crate::{bscript::LocalEvent, view};
use anyhow::{anyhow, Result};

use chrono::prelude::*;
use futures::channel::oneshot;
use gdk::{self, cairo, prelude::*};
use glib::clone;
use gtk::{self, prelude::*};
use log::warn;
use netidx::{
    path::Path,
    subscriber::{Typ, Value},
};
use netidx_bscript::vm;
use std::{
    cell::{Cell, RefCell},
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

struct Series {
    line_color: view::RGB,
    x: BSNode,
    y: BSNode,
    x_data: VecDeque<Value>,
    y_data: VecDeque<Value>,
}

pub(super) struct LinePlot {
    root: gtk::Box,
    canvas: gtk::DrawingArea,
    x_min: Rc<RefCell<BSNode>>,
    x_max: Rc<RefCell<BSNode>>,
    y_min: Rc<RefCell<BSNode>>,
    y_max: Rc<RefCell<BSNode>>,
    keep_points: Rc<RefCell<BSNode>>,
    series: Rc<RefCell<Vec<Series>>>,
}

impl LinePlot {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::LinePlot,
        scope: Path,
        _selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let canvas = gtk::DrawingArea::new();
        root.set_no_show_all(true);
        canvas.set_no_show_all(true);
        root.pack_start(&canvas, true, true, 0);
        let x_min = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.x_min.clone(),
        )));
        let x_max = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.x_max.clone(),
        )));
        let y_min = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.y_min.clone(),
        )));
        let y_max = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.y_max.clone(),
        )));
        let keep_points = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.keep_points.clone(),
        )));
        let series = Rc::new(RefCell::new(
            spec.series
                .iter()
                .map(|series| {
                    let x = BSNode::compile(
                        &mut ctx.borrow_mut(),
                        scope.clone(),
                        series.x.clone(),
                    );
                    let y = BSNode::compile(
                        &mut ctx.borrow_mut(),
                        scope.clone(),
                        series.y.clone(),
                    );
                    Series {
                        line_color: series.line_color,
                        x,
                        y,
                        x_data: VecDeque::new(),
                        y_data: VecDeque::new(),
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let allocated_width = Rc::new(Cell::new(0));
        let allocated_height = Rc::new(Cell::new(0));
        canvas.connect_draw(clone!(
            @strong ctx,
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
                &ctx,
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
            allocated_width.set(i32::abs(a.width()) as u32);
            allocated_height.set(i32::abs(a.height()) as u32);
        }));
        LinePlot { root, canvas, x_min, x_max, y_min, y_max, keep_points, series }
    }

    fn draw(
        ctx: &BSCtx,
        spec: &view::LinePlot,
        width: &Rc<Cell<u32>>,
        height: &Rc<Cell<u32>>,
        x_min: &Rc<RefCell<BSNode>>,
        x_max: &Rc<RefCell<BSNode>>,
        y_min: &Rc<RefCell<BSNode>>,
        y_max: &Rc<RefCell<BSNode>>,
        series: &Rc<RefCell<Vec<Series>>>,
        context: &cairo::Context,
    ) -> Result<()> {
        use super::cairo_backend::CairoBackend;
        use chrono::Duration;
        use plotters::{coord::ranged1d::ValueFormatter, prelude::*, style::RGBColor};
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
            let x_min = x_min.borrow().current(&mut ctx.borrow_mut());
            let x_max = x_max.borrow().current(&mut ctx.borrow_mut());
            let y_min = y_min.borrow().current(&mut ctx.borrow_mut());
            let y_max = y_max.borrow().current(&mut ctx.borrow_mut());
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
}

impl BWidget for LinePlot {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        let mut queue_draw = false;
        if self.x_min.borrow_mut().update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.x_max.borrow_mut().update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.y_min.borrow_mut().update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.y_max.borrow_mut().update(ctx, event).is_some() {
            queue_draw = true;
        }
        if self.keep_points.borrow_mut().update(ctx, event).is_some() {
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
                .borrow()
                .current(ctx)
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

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.canvas.set_visible(v);
        self.root.set_visible(v);
    }
}
