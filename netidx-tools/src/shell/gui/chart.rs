use super::{
    into_borrowed_line, layout::ConstraintV, AlignmentV, GuiW, GuiWidget, LineV, StyleV,
    TRef,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use futures::future::try_join_all;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::{Constraint, Rect},
    symbols,
    widgets::{Axis, Chart, Dataset, GraphType, LegendPosition},
    Frame,
};
use smallvec::SmallVec;
use tokio::try_join;

#[derive(Clone, Copy)]
struct MarkerV(symbols::Marker);

impl FromValue for MarkerV {
    fn from_value(v: Value) -> Result<Self> {
        let m = match &*v.cast_to::<ArcStr>()? {
            "Dot" => symbols::Marker::Dot,
            "Block" => symbols::Marker::Block,
            "Bar" => symbols::Marker::Bar,
            "Braille" => symbols::Marker::Braille,
            "HalfBlock" => symbols::Marker::HalfBlock,
            s => bail!("invalid marker {s}"),
        };
        Ok(Self(m))
    }
}

#[derive(Clone, Copy)]
struct GraphTypeV(GraphType);

impl FromValue for GraphTypeV {
    fn from_value(v: Value) -> Result<Self> {
        let g = match &*v.cast_to::<ArcStr>()? {
            "Scatter" => GraphType::Scatter,
            "Line" => GraphType::Line,
            "Bar" => GraphType::Bar,
            s => bail!("invalid graphtype {s}"),
        };
        Ok(Self(g))
    }
}

#[derive(Clone, Copy)]
struct LegendPositionV(LegendPosition);

impl FromValue for LegendPositionV {
    fn from_value(v: Value) -> Result<Self> {
        let p = match &*v.cast_to::<ArcStr>()? {
            "Top" => LegendPosition::Top,
            "TopRight" => LegendPosition::TopRight,
            "TopLeft" => LegendPosition::TopLeft,
            "Left" => LegendPosition::Left,
            "Right" => LegendPosition::Right,
            "Bottom" => LegendPosition::Bottom,
            "BottomRight" => LegendPosition::BottomRight,
            "BottomLeft" => LegendPosition::BottomLeft,
            s => bail!("invalid legend position {s}"),
        };
        Ok(Self(p))
    }
}

#[derive(Clone)]
struct AxisV(Axis<'static>);

impl FromValue for AxisV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, bounds), (_, labels), (_, labels_alignment), (_, style), (_, title)] =
            v.cast_to::<[(ArcStr, Value); 5]>()?;
        let [(_, min), (_, max)] = bounds.cast_to::<[(ArcStr, f64); 2]>()?;
        let mut axis = Axis::default().bounds([min, max]);
        if let Some(lbls) = labels.cast_to::<Option<Vec<LineV>>>()? {
            let lbls = lbls.into_iter().map(|l| l.0).collect::<Vec<_>>();
            axis = axis.labels(lbls);
        }
        if let Some(al) = labels_alignment.cast_to::<Option<AlignmentV>>()? {
            axis = axis.labels_alignment(al.0);
        }
        if let Some(st) = style.cast_to::<Option<StyleV>>()? {
            axis = axis.style(st.0);
        }
        if let Some(LineV(t)) = title.cast_to::<Option<LineV>>()? {
            axis = axis.title(t);
        }
        Ok(Self(axis))
    }
}

#[derive(Clone, Copy)]
struct HLConstraintsV((Constraint, Constraint));

impl FromValue for HLConstraintsV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, w), (_, h)] = v.cast_to::<[(ArcStr, Value); 2]>()?;
        let w = w.cast_to::<ConstraintV>()?.0;
        let h = h.cast_to::<ConstraintV>()?.0;
        Ok(Self((w, h)))
    }
}

struct DatasetW {
    name: TRef<Option<LineV>>,
    data_ref: Ref,
    data: Vec<(f64, f64)>,
    marker: TRef<Option<MarkerV>>,
    graph_type: TRef<Option<GraphTypeV>>,
    style: TRef<Option<StyleV>>,
}

impl DatasetW {
    async fn compile(bs: &BSHandle, v: Value) -> Result<Self> {
        let [(_, data), (_, graph_type), (_, marker), (_, name), (_, style)] =
            v.cast_to::<[(ArcStr, u64); 5]>()?;
        let (name, data_ref, marker, graph_type, style) = try_join! {
            bs.compile_ref(name),
            bs.compile_ref(data),
            bs.compile_ref(marker),
            bs.compile_ref(graph_type),
            bs.compile_ref(style)
        }?;
        let mut t = Self {
            name: TRef::new(name)?,
            data_ref,
            data: vec![],
            marker: TRef::new(marker)?,
            graph_type: TRef::new(graph_type)?,
            style: TRef::new(style)?,
        };
        if let Some(v) = t.data_ref.last.take() {
            t.set_data(&v)?;
        }
        Ok(t)
    }

    fn set_data(&mut self, v: &Value) -> Result<()> {
        self.data.clear();
        match v {
            Value::Array(a) => {
                for v in a {
                    let (x, y) = v
                        .clone()
                        .cast_to::<(f64, f64)>()
                        .context("invalid dataset pair")?;
                    self.data.push((x, y));
                }
            }
            v => bail!("invalid dataset {v}"),
        }
        Ok(())
    }

    fn update(&mut self, id: ExprId, v: &Value) -> Result<()> {
        self.name.update(id, v).context("dataset name update")?;
        if id == self.data_ref.id {
            self.set_data(v)?;
        }
        self.marker.update(id, v).context("dataset marker update")?;
        self.graph_type.update(id, v).context("dataset graph_type update")?;
        self.style.update(id, v).context("dataset style update")?;
        Ok(())
    }

    fn build<'a>(&'a self) -> Dataset<'a> {
        let Self { name, data_ref: _, data, marker, graph_type, style } = self;
        let mut ds = Dataset::default().data(data);
        if let Some(Some(LineV(l))) = &name.t {
            ds = ds.name(into_borrowed_line(l));
        }
        if let Some(Some(m)) = marker.t {
            ds = ds.marker(m.0);
        }
        if let Some(Some(g)) = graph_type.t {
            ds = ds.graph_type(g.0);
        }
        if let Some(Some(s)) = style.t {
            ds = ds.style(s.0);
        }
        ds
    }
}

pub(super) struct ChartW {
    bs: BSHandle,
    datasets_ref: Ref,
    datasets: Vec<DatasetW>,
    hidden_legend_constraints: TRef<Option<HLConstraintsV>>,
    legend_position: TRef<Option<LegendPositionV>>,
    style: TRef<Option<StyleV>>,
    x_axis: TRef<Option<AxisV>>,
    y_axis: TRef<Option<AxisV>>,
}

impl ChartW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, datasets), (_, hidden_legend_constraints), (_, legend_position), (_, style), (_, x_axis), (_, y_axis)] =
            v.cast_to::<[(ArcStr, u64); 6]>()?;
        let (
            datasets_ref,
            hidden_legend_constraints,
            legend_position,
            style,
            x_axis,
            y_axis,
        ) = try_join! {
            bs.compile_ref(datasets),
            bs.compile_ref(hidden_legend_constraints),
            bs.compile_ref(legend_position),
            bs.compile_ref(style),
            bs.compile_ref(x_axis),
            bs.compile_ref(y_axis)
        }?;
        let mut t = Self {
            bs: bs.clone(),
            datasets_ref,
            datasets: vec![],
            hidden_legend_constraints: TRef::new(hidden_legend_constraints)?,
            legend_position: TRef::new(legend_position)?,
            style: TRef::new(style)?,
            x_axis: TRef::new(x_axis)?,
            y_axis: TRef::new(y_axis)?,
        };
        if let Some(v) = t.datasets_ref.last.take() {
            t.set_datasets(v).await?;
        }
        Ok(Box::new(t))
    }

    async fn set_datasets(&mut self, v: Value) -> Result<()> {
        let ds = v
            .cast_to::<SmallVec<[Value; 8]>>()?
            .into_iter()
            .map(|d| DatasetW::compile(&self.bs, d));
        self.datasets = try_join_all(ds).await?;
        Ok(())
    }
}

#[async_trait]
impl GuiWidget for ChartW {
    async fn handle_event(&mut self, _e: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs: _,
            datasets_ref: _,
            datasets: _,
            hidden_legend_constraints,
            legend_position,
            style,
            x_axis,
            y_axis,
        } = self;
        hidden_legend_constraints.update(id, &v).context("chart hidden update")?;
        legend_position.update(id, &v).context("chart legend update")?;
        style.update(id, &v).context("chart style update")?;
        x_axis.update(id, &v).context("chart x_axis update")?;
        y_axis.update(id, &v).context("chart y_axis update")?;
        if self.datasets_ref.id == id {
            self.set_datasets(v.clone()).await?;
        }
        for d in &mut self.datasets {
            d.update(id, &v)?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            datasets_ref: _,
            datasets,
            hidden_legend_constraints,
            legend_position,
            style,
            x_axis,
            y_axis,
        } = self;
        let mut chart = Chart::new(datasets.iter().map(|d| d.build()).collect());
        if let Some(Some(h)) = &hidden_legend_constraints.t {
            chart = chart.hidden_legend_constraints(h.0);
        }
        if let Some(Some(p)) = legend_position.t {
            chart = chart.legend_position(Some(p.0));
        }
        if let Some(Some(s)) = &style.t {
            chart = chart.style(s.0);
        }
        if let Some(Some(a)) = &x_axis.t {
            chart = chart.x_axis(a.0.clone());
        }
        if let Some(Some(a)) = &y_axis.t {
            chart = chart.y_axis(a.0.clone());
        }
        frame.render_widget(chart, rect);
        Ok(())
    }
}
