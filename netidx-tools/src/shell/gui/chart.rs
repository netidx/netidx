use super::{into_borrowed_line, AlignmentV, GuiW, GuiWidget, LineV, StyleV, TRef,
    layout::ConstraintV};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use futures::future::try_join_all;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{expr::ExprId, rt::{BSHandle, Ref}};
use ratatui::{
    layout::{Constraint, Rect},
    style::Style,
    text::Line,
    widgets::{self, Axis, Chart, Dataset, GraphType, LegendPosition},
    symbols,
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
        if let Some(lbls) = labels.cast_to::<Option<SmallVec<[LineV; 8]>>>()? {
            let lines = lbls.into_iter().map(|l| into_borrowed_line(&l.0)).collect::<Vec<_>>();
            axis = axis.labels(lines);
        }
        if let Some(al) = labels_alignment.cast_to::<Option<AlignmentV>>()? {
            axis = axis.labels_alignment(al.0);
        }
        if let Some(st) = style.cast_to::<Option<StyleV>>()? {
            axis = axis.style(st.0);
        }
        if let Some(LineV(t)) = title.cast_to::<Option<LineV>>()? {
            axis = axis.title(into_borrowed_line(&t));
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
    data: TRef<SmallVec<[(f64, f64); 32]>>,
    marker: TRef<Option<MarkerV>>,
    graph_type: TRef<Option<GraphTypeV>>,
    style: TRef<Option<StyleV>>,
}

impl DatasetW {
    async fn compile(bs: &BSHandle, v: Value) -> Result<Self> {
        let [(_, name), (_, data), (_, marker), (_, graph_type), (_, style)] =
            v.cast_to::<[(ArcStr, Value); 5]>()?;
        let (name, data, marker, graph_type, style) = try_join! {
            bs.compile_ref(name),
            bs.compile_ref(data),
            bs.compile_ref(marker),
            bs.compile_ref(graph_type),
            bs.compile_ref(style)
        }?;
        Ok(Self {
            name: TRef::new(name)?,
            data: TRef::new(data)?,
            marker: TRef::new(marker)?,
            graph_type: TRef::new(graph_type)?,
            style: TRef::new(style)?,
        })
    }

    fn update(&mut self, id: ExprId, v: &Value) -> Result<()> {
        self.name.update(id, v).context("dataset name update")?;
        self.data.update(id, v).context("dataset data update")?;
        self.marker.update(id, v).context("dataset marker update")?;
        self.graph_type.update(id, v).context("dataset graph_type update")?;
        self.style.update(id, v).context("dataset style update")?;
        Ok(())
    }

    fn build<'a>(&'a self) -> Dataset<'a> {
        let mut ds = Dataset::default();
        let data_slice: &[(f64, f64)] = self.data.t.as_ref().map(|d| &d[..]).unwrap_or(&[]);
        ds = ds.data(data_slice);
        if let Some(Some(LineV(l))) = &self.name.t {
            ds = ds.name(into_borrowed_line(l));
        }
        if let Some(Some(m)) = self.marker.t {
            ds = ds.marker(m.0);
        }
        if let Some(Some(g)) = self.graph_type.t {
            ds = ds.graph_type(g.0);
        }
        if let Some(Some(s)) = &self.style.t {
            ds = ds.style(s.0);
        }
        ds
    }
}

pub(super) struct ChartW {
    bs: BSHandle,
    datasets_ref: Ref,
    datasets: Vec<DatasetW>,
    hidden: TRef<Option<HLConstraintsV>>,
    legend: TRef<Option<LegendPositionV>>,
    style: TRef<Option<StyleV>>,
    x_axis: TRef<Option<AxisV>>,
    y_axis: TRef<Option<AxisV>>,
}

impl ChartW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [
            (_, datasets),
            (_, hidden),
            (_, legend),
            (_, style),
            (_, x_axis),
            (_, y_axis),
        ] = v.cast_to::<[(ArcStr, Value); 6]>()?;
        let (
            datasets_ref,
            hidden,
            legend,
            style,
            x_axis,
            y_axis,
        ) = try_join! {
            bs.compile_ref(datasets),
            bs.compile_ref(hidden),
            bs.compile_ref(legend),
            bs.compile_ref(style),
            bs.compile_ref(x_axis),
            bs.compile_ref(y_axis)
        }?;
        let mut t = Self {
            bs: bs.clone(),
            datasets_ref,
            datasets: vec![],
            hidden: TRef::new(hidden)?,
            legend: TRef::new(legend)?,
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
        self.hidden.update(id, &v).context("chart hidden update")?;
        self.legend.update(id, &v).context("chart legend update")?;
        self.style.update(id, &v).context("chart style update")?;
        self.x_axis.update(id, &v).context("chart x_axis update")?;
        self.y_axis.update(id, &v).context("chart y_axis update")?;
        if self.datasets_ref.id == id {
            self.set_datasets(v.clone()).await?;
        }
        for d in &mut self.datasets {
            d.update(id, &v)?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let mut ds: SmallVec<[Dataset; 8]> = SmallVec::new();
        for d in &self.datasets {
            ds.push(d.build());
        }
        let mut chart = Chart::new(ds.into_vec());
        if let Some(Some(h)) = &self.hidden.t {
            chart = chart.hidden_legend_constraints(h.0);
        }
        if let Some(Some(p)) = self.legend.t {
            chart = chart.legend_position(Some(p.0));
        }
        if let Some(Some(s)) = &self.style.t {
            chart = chart.style(s.0);
        }
        if let Some(Some(a)) = &self.x_axis.t {
            chart = chart.x_axis(a.0.clone());
        }
        if let Some(Some(a)) = &self.y_axis.t {
            chart = chart.y_axis(a.0.clone());
        }
        frame.render_widget(chart, rect);
        Ok(())
    }
}

