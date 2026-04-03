use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use crate::cairo_backend::CairoBackend;
use graphix_compiler::expr::ExprId;
use graphix_package_gui::widgets::chart::{
    ChartColor, ChartMode, DatasetEntry, XYKind,
    chart_mode, compile_datasets, compute_ranges, compute_time_ranges,
    pad_range,
    XYData,
    OptMeshStyle, OptLegendStyle, OptLegendPosition,
    OptAxisRange, OptXAxisRange, XAxisRange,
    OptColor, OptF64,
};
use graphix_rt::{GXExt, GXHandle, Ref, TRef};
use glib;
use gtk::prelude::*;
use log::error;
use netidx::publisher::Value;
use plotters::{
    chart::ChartBuilder,
    element::PathElement,
    prelude::{
        AreaSeries, Circle, Histogram, IntoDrawingArea,
        IntoSegmentedCoord, LineSeries, SeriesLabelPosition,
    },
    style::{
        Color as PlotColor, IntoFont, RGBColor, ShapeStyle, TextStyle, BLACK, WHITE,
    },
};
use poolshark::local::LPooled;
use std::cell::RefCell;
use std::rc::Rc;

const PALETTE: [RGBColor; 8] = [
    RGBColor(31, 119, 180),
    RGBColor(255, 127, 14),
    RGBColor(44, 160, 44),
    RGBColor(214, 39, 40),
    RGBColor(148, 103, 189),
    RGBColor(140, 86, 75),
    RGBColor(227, 119, 194),
    RGBColor(127, 127, 127),
];

fn to_rgb(c: ChartColor) -> RGBColor {
    c.to_plotters_rgb()
}

/// Mutable chart data shared between the widget and the draw callback.
struct ChartData<X: GXExt> {
    gx: GXHandle<X>,
    datasets_ref: Ref<X>,
    datasets: LPooled<Vec<DatasetEntry<X>>>,
    title: TRef<X, Option<String>>,
    title_color: TRef<X, OptColor>,
    x_label: TRef<X, Option<String>>,
    y_label: TRef<X, Option<String>>,
    x_range: TRef<X, OptXAxisRange>,
    y_range: TRef<X, OptAxisRange>,
    background: TRef<X, OptColor>,
    margin: TRef<X, OptF64>,
    title_size: TRef<X, OptF64>,
    legend_position: TRef<X, OptLegendPosition>,
    legend_style: TRef<X, OptLegendStyle>,
    mesh: TRef<X, OptMeshStyle>,
}

pub(crate) struct ChartW<X: GXExt> {
    drawing_area: gtk::DrawingArea,
    data: Rc<RefCell<ChartData<X>>>,
}

impl<X: GXExt> ChartW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields arrive sorted alphabetically (13 fields):
        // background, datasets, legend_position, legend_style, margin,
        // mesh, title, title_color, title_size, x_label, x_range,
        // y_label, y_range
        let [(_, background), (_, datasets), (_, legend_position), (_, legend_style), (_, margin), (_, mesh), (_, title), (_, title_color), (_, title_size), (_, x_label), (_, x_range), (_, y_label), (_, y_range)] =
            source.cast_to::<[(ArcStr, u64); 13]>().context("chart flds")?;
        let (
            background_ref,
            datasets_ref,
            legend_position_ref,
            legend_style_ref,
            margin_ref,
            mesh_ref,
            title_ref,
            title_color_ref,
            title_size_ref,
            x_label_ref,
            x_range_ref,
            y_label_ref,
            y_range_ref,
        ) = tokio::try_join! {
            ctx.gx.compile_ref(background),
            ctx.gx.compile_ref(datasets),
            ctx.gx.compile_ref(legend_position),
            ctx.gx.compile_ref(legend_style),
            ctx.gx.compile_ref(margin),
            ctx.gx.compile_ref(mesh),
            ctx.gx.compile_ref(title),
            ctx.gx.compile_ref(title_color),
            ctx.gx.compile_ref(title_size),
            ctx.gx.compile_ref(x_label),
            ctx.gx.compile_ref(x_range),
            ctx.gx.compile_ref(y_label),
            ctx.gx.compile_ref(y_range),
        }?;
        let entries = match datasets_ref.last.as_ref() {
            Some(v) => compile_datasets(&ctx.gx, v.clone()).await?,
            None => LPooled::take(),
        };
        let data = Rc::new(RefCell::new(ChartData {
            gx: ctx.gx.clone(),
            datasets_ref,
            datasets: entries,
            title: TRef::new(title_ref).context("chart tref title")?,
            title_color: TRef::new(title_color_ref).context("chart tref title_color")?,
            x_label: TRef::new(x_label_ref).context("chart tref x_label")?,
            y_label: TRef::new(y_label_ref).context("chart tref y_label")?,
            x_range: TRef::new(x_range_ref).context("chart tref x_range")?,
            y_range: TRef::new(y_range_ref).context("chart tref y_range")?,
            background: TRef::new(background_ref).context("chart tref background")?,
            margin: TRef::new(margin_ref).context("chart tref margin")?,
            title_size: TRef::new(title_size_ref).context("chart tref title_size")?,
            legend_position: TRef::new(legend_position_ref)
                .context("chart tref legend_position")?,
            legend_style: TRef::new(legend_style_ref)
                .context("chart tref legend_style")?,
            mesh: TRef::new(mesh_ref).context("chart tref mesh")?,
        }));
        let drawing_area = gtk::DrawingArea::new();
        drawing_area.set_size_request(400, 300);
        let data_clone = Rc::clone(&data);
        drawing_area.connect_draw(move |widget, cr| {
            let w = widget.allocated_width() as u32;
            let h = widget.allocated_height() as u32;
            if w > 0 && h > 0 {
                let d = data_clone.borrow();
                draw_chart(&d, cr, w, h);
            }
            glib::Propagation::Proceed
        });
        drawing_area.show();
        Ok(Box::new(ChartW { drawing_area, data }))
    }
}

impl<X: GXExt> GtkWidget<X> for ChartW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut d = self.data.borrow_mut();
        let mut changed = false;
        if id == d.datasets_ref.id {
            d.datasets_ref.last = Some(v.clone());
            d.datasets = rt
                .block_on(compile_datasets(&d.gx, v.clone()))
                .context("chart datasets recompile")?;
            changed = true;
        }
        for ds in d.datasets.iter_mut() {
            let updated = match ds {
                DatasetEntry::XY { data, .. }
                | DatasetEntry::DashedLine { data, .. } => {
                    data.update(id, v).context("chart update xy data")?.is_some()
                }
                DatasetEntry::Bar { data, .. } | DatasetEntry::Pie { data, .. } => {
                    data.update(id, v).context("chart update bar data")?.is_some()
                }
                DatasetEntry::Candlestick { data, .. } => {
                    data.update(id, v).context("chart update ohlc data")?.is_some()
                }
                DatasetEntry::ErrorBar { data, .. } => {
                    data.update(id, v).context("chart update eb data")?.is_some()
                }
                DatasetEntry::Scatter3D { data, .. }
                | DatasetEntry::Line3D { data, .. } => {
                    data.update(id, v).context("chart update 3d data")?.is_some()
                }
                DatasetEntry::Surface { data, .. } => {
                    data.update(id, v)
                        .context("chart update surface data")?
                        .is_some()
                }
            };
            if updated {
                changed = true;
            }
        }
        macro_rules! up {
            ($f:ident) => {
                if d.$f
                    .update(id, v)
                    .context(concat!("chart update ", stringify!($f)))?
                    .is_some()
                {
                    changed = true;
                }
            };
        }
        up!(title);
        up!(title_color);
        up!(x_label);
        up!(y_label);
        up!(x_range);
        up!(y_range);
        up!(background);
        up!(margin);
        up!(title_size);
        up!(legend_position);
        up!(legend_style);
        up!(mesh);
        drop(d);
        if changed {
            self.drawing_area.queue_draw();
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.drawing_area.upcast_ref()
    }
}

// ── Mesh configuration macro ──────────────────────────────────────

macro_rules! configure_mesh {
    ($chart:expr, $x_label:expr, $y_label:expr, $mesh_style:expr) => {{
        let mut mesh_cfg = $chart.configure_mesh();
        if let Some(xl) = $x_label {
            mesh_cfg.x_desc(xl);
        }
        if let Some(yl) = $y_label {
            mesh_cfg.y_desc(yl);
        }
        if let Some(ms) = $mesh_style {
            if ms.show_x_grid == Some(false) {
                mesh_cfg.disable_x_mesh();
            }
            if ms.show_y_grid == Some(false) {
                mesh_cfg.disable_y_mesh();
            }
            if let Some(c) = ms.grid_color {
                mesh_cfg.light_line_style(to_rgb(c));
            }
            if let Some(c) = ms.axis_color {
                mesh_cfg.axis_style(to_rgb(c));
            }
            if ms.label_size.is_some() || ms.label_color.is_some() {
                let s = ms.label_size.unwrap_or(12.0);
                if let Some(lc) = ms.label_color {
                    let mut style =
                        TextStyle::from(("sans-serif", s).into_font());
                    style.color = to_rgb(lc).to_backend_color();
                    mesh_cfg.label_style(style.clone());
                    mesh_cfg.axis_desc_style(style);
                } else {
                    mesh_cfg.label_style(("sans-serif", s).into_font());
                }
            }
            if let Some(n) = ms.x_labels {
                mesh_cfg.x_labels(n as usize);
            }
            if let Some(n) = ms.y_labels {
                mesh_cfg.y_labels(n as usize);
            }
        }
        if let Err(e) = mesh_cfg.draw() {
            error!("chart mesh draw: {e:?}");
        }
    }};
}

// ── XY series drawing macro ───────────────────────────────────────

macro_rules! draw_xy_series {
    ($chart:expr, $datasets:expr, $xy_variant:path, $label_sz:expr,
     $legend_pos:expr, $legend_style:expr) => {{
        for (i, ds) in $datasets.iter().enumerate() {
            if let DatasetEntry::XY { kind, data, style } = ds {
                let pts = match data.t.as_ref() {
                    Some($xy_variant(p)) => p,
                    _ => continue,
                };
                let color = style
                    .color
                    .map(to_rgb)
                    .unwrap_or(PALETTE[i % PALETTE.len()]);
                let sw = style.stroke_width.unwrap_or(2.0) as u32;
                let ps = style.point_size.unwrap_or(3.0) as u32;
                let line_style = ShapeStyle::from(color).stroke_width(sw);
                let fill_style = ShapeStyle::from(color).filled();
                let label = style.label.as_deref();
                match kind {
                    XYKind::Line => {
                        let series =
                            LineSeries::new(pts.iter().copied(), line_style);
                        match $chart.draw_series(series) {
                            Ok(ann) => {
                                if let Some(l) = label {
                                    ann.label(l).legend(move |(x, y)| {
                                        PathElement::new(
                                            [(x, y), (x + 20, y)],
                                            line_style,
                                        )
                                    });
                                }
                            }
                            Err(e) => error!("chart draw line: {e:?}"),
                        }
                    }
                    XYKind::Scatter => {
                        let series = pts
                            .iter()
                            .map(|&(x, y)| Circle::new((x, y), ps, fill_style));
                        match $chart.draw_series(series) {
                            Ok(ann) => {
                                if let Some(l) = label {
                                    ann.label(l).legend(move |(x, y)| {
                                        Circle::new((x, y), ps, fill_style)
                                    });
                                }
                            }
                            Err(e) => error!("chart draw scatter: {e:?}"),
                        }
                    }
                    XYKind::Area => {
                        let area_fill = color.mix(0.3);
                        let series = AreaSeries::new(
                            pts.iter().copied(),
                            0.0,
                            ShapeStyle::from(area_fill).filled(),
                        )
                        .border_style(line_style);
                        match $chart.draw_series(series) {
                            Ok(ann) => {
                                if let Some(l) = label {
                                    ann.label(l).legend(move |(x, y)| {
                                        PathElement::new(
                                            [(x, y), (x + 20, y)],
                                            line_style,
                                        )
                                    });
                                }
                            }
                            Err(e) => error!("chart draw area: {e:?}"),
                        }
                    }
                }
            }
        }
        draw_legend!($chart, $datasets, $legend_pos, $legend_style, $label_sz);
    }};
}

macro_rules! draw_legend {
    ($chart:expr, $datasets:expr, $legend_pos:expr, $legend_style:expr,
     $label_sz:expr) => {{
        let has_labels = $datasets.iter().any(|ds| ds.label().is_some());
        if has_labels {
            let ls = $legend_style;
            let legend_bg =
                ls.and_then(|s| s.background).map(to_rgb).unwrap_or(WHITE);
            let legend_border =
                ls.and_then(|s| s.border).map(to_rgb).unwrap_or(BLACK);
            let legend_font_sz =
                ls.and_then(|s| s.label_size).unwrap_or($label_sz);
            let mut labels = $chart.configure_series_labels();
            labels.position($legend_pos);
            labels.margin(15);
            labels.background_style(legend_bg.mix(0.8));
            labels.border_style(legend_border);
            let mut style =
                TextStyle::from(("sans-serif", legend_font_sz).into_font());
            if let Some(lc) = ls.and_then(|s| s.label_color) {
                style.color = to_rgb(lc).to_backend_color();
            }
            labels.label_font(style);
            if let Err(e) = labels.draw() {
                error!("chart series labels draw: {e:?}");
            }
        }
    }};
}

// ── Main draw function ────────────────────────────────────────────

fn draw_chart<X: GXExt>(
    d: &ChartData<X>,
    cr: &gtk::cairo::Context,
    w: u32,
    h: u32,
) {
    let mode = chart_mode(&d.datasets);
    if mode == ChartMode::Empty {
        return;
    }
    let backend = match CairoBackend::new(cr, (w, h)) {
        Ok(b) => b,
        Err(e) => {
            error!("chart cairo backend: {e:?}");
            return;
        }
    };
    let root = backend.into_drawing_area();

    let bg = d
        .background
        .t
        .as_ref()
        .and_then(|o| o.0)
        .map(to_rgb)
        .unwrap_or(WHITE);
    if let Err(e) = root.fill(&bg) {
        error!("chart fill: {e:?}");
        return;
    }

    let title = d.title.t.as_ref().and_then(|o| o.as_deref());
    let x_label = d.x_label.t.as_ref().and_then(|o| o.as_deref());
    let y_label = d.y_label.t.as_ref().and_then(|o| o.as_deref());
    let margin = d.margin.t.as_ref().and_then(|o| o.0).unwrap_or(10.0);
    let title_size =
        d.title_size.t.as_ref().and_then(|o| o.0).unwrap_or(16.0);
    let mesh_style = d.mesh.t.as_ref().and_then(|m| m.0.as_ref());

    let legend_pos = d
        .legend_position
        .t
        .as_ref()
        .and_then(|o| o.0.as_ref())
        .map(|p| p.0.clone())
        .unwrap_or(SeriesLabelPosition::UpperLeft);
    let legend_style = d.legend_style.t.as_ref().and_then(|o| o.0.as_ref());
    let label_sz = mesh_style.and_then(|ms| ms.label_size).unwrap_or(12.0);

    let mut builder = ChartBuilder::on(&root);
    builder.margin(margin as u32);
    if let Some(t) = title {
        let font = ("sans-serif", title_size).into_font();
        if let Some(tc) = d.title_color.t.as_ref().and_then(|o| o.0) {
            let mut style = TextStyle::from(font);
            style.color = to_rgb(tc).to_backend_color();
            builder.caption(t, style);
        } else {
            builder.caption(t, font);
        }
    }

    let y_range_opt = d.y_range.t.as_ref().and_then(|r| r.0.as_ref());

    let x_area = mesh_style
        .and_then(|ms| ms.x_label_area_size)
        .map(|s| s as u32)
        .unwrap_or(40);
    let y_area = mesh_style
        .and_then(|ms| ms.y_label_area_size)
        .map(|s| s as u32)
        .unwrap_or(50);
    builder.x_label_area_size(x_area);
    builder.y_label_area_size(y_area);

    match mode {
        ChartMode::Numeric => {
            let (auto_x, auto_y) = compute_ranges(&d.datasets);
            let x = match d.x_range.t.as_ref().and_then(|r| r.0.as_ref()) {
                Some(XAxisRange::Numeric { min, max }) => (*min, *max),
                _ => auto_x,
            };
            let y = match y_range_opt {
                Some(r) => (r.min, r.max),
                None => auto_y,
            };

            let mut chart =
                match builder.build_cartesian_2d(x.0..x.1, y.0..y.1) {
                    Ok(c) => c,
                    Err(_) => return,
                };
            configure_mesh!(chart, x_label, y_label, mesh_style);
            draw_xy_series!(
                chart,
                d.datasets,
                XYData::Numeric,
                label_sz,
                legend_pos,
                legend_style
            );
        }

        ChartMode::TimeSeries => {
            let (auto_x, auto_y) = compute_time_ranges(&d.datasets);
            let x_dt =
                match d.x_range.t.as_ref().and_then(|r| r.0.as_ref()) {
                    Some(XAxisRange::DateTime { min, max }) => (*min, *max),
                    _ => auto_x,
                };
            let y = match y_range_opt {
                Some(r) => (r.min, r.max),
                None => auto_y,
            };

            let mut chart =
                match builder.build_cartesian_2d(x_dt.0..x_dt.1, y.0..y.1) {
                    Ok(c) => c,
                    Err(_) => return,
                };
            configure_mesh!(chart, x_label, y_label, mesh_style);
            draw_xy_series!(
                chart,
                d.datasets,
                XYData::DateTime,
                label_sz,
                legend_pos,
                legend_style
            );
        }

        ChartMode::Bar => {
            let mut categories: Vec<String> = Vec::new();
            let mut y_min = f64::INFINITY;
            let mut y_max = f64::NEG_INFINITY;
            for ds in d.datasets.iter() {
                if let DatasetEntry::Bar { data, .. } = ds {
                    if let Some(bd) = data.t.as_ref() {
                        for (cat, val) in bd.0.iter() {
                            if !categories.iter().any(|c| c == cat) {
                                categories.push(cat.clone());
                            }
                            if *val < y_min {
                                y_min = *val;
                            }
                            if *val > y_max {
                                y_max = *val;
                            }
                        }
                    }
                }
            }
            if categories.is_empty() {
                return;
            }
            if y_min > 0.0 {
                y_min = 0.0;
            }
            if y_max < 0.0 {
                y_max = 0.0;
            }
            let y = match y_range_opt {
                Some(r) => (r.min, r.max),
                None => pad_range(y_min, y_max),
            };

            let mut chart = match builder.build_cartesian_2d(
                categories.as_slice().into_segmented(),
                y.0..y.1,
            ) {
                Ok(c) => c,
                Err(_) => return,
            };
            configure_mesh!(chart, x_label, y_label, mesh_style);

            for (i, ds) in d.datasets.iter().enumerate() {
                if let DatasetEntry::Bar { data, style } = ds {
                    if let Some(bd) = data.t.as_ref() {
                        let color = style
                            .color
                            .map(to_rgb)
                            .unwrap_or(PALETTE[i % PALETTE.len()]);
                        let fill_style = ShapeStyle::from(color).filled();
                        let margin_px = style.margin.unwrap_or(5.0) as u32;
                        let hist = Histogram::vertical(&chart)
                            .style(fill_style)
                            .margin(margin_px)
                            .data(
                                bd.0.iter().map(|(cat, val)| (cat, *val)),
                            );
                        match chart.draw_series(hist) {
                            Ok(ann) => {
                                if let Some(l) = style.label.as_deref() {
                                    ann.label(l).legend(move |(x, y)| {
                                        plotters::element::Rectangle::new(
                                            [(x, y - 5), (x + 20, y + 5)],
                                            fill_style,
                                        )
                                    });
                                }
                            }
                            Err(e) => error!("chart draw bar: {e:?}"),
                        }
                    }
                }
            }
            draw_legend!(
                chart,
                d.datasets,
                legend_pos,
                legend_style,
                label_sz
            );
        }

        // Pie and 3D modes are not supported in the GTK browser
        _ => {}
    }
}
