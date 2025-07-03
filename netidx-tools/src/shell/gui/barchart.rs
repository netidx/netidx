use super::{into_borrowed_line, DirectionV, GuiW, GuiWidget, LineV, StyleV, TRef};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use futures::future::{self, try_join_all};
use log::debug;
use netidx::publisher::Value;
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::Rect,
    widgets::{Bar, BarChart, BarGroup},
    Frame,
};
use smallvec::{smallvec, SmallVec};
use tokio::try_join;

struct BarW {
    label: TRef<Option<LineV>>,
    style: TRef<Option<StyleV>>,
    text_value: TRef<Option<ArcStr>>,
    value: TRef<u64>,
    value_style: TRef<Option<StyleV>>,
}

impl BarW {
    async fn compile(bs: &BSHandle, v: Value) -> Result<Self> {
        let [(_, label), (_, style), (_, text_value), (_, value), (_, value_style)] =
            v.cast_to::<[(ArcStr, u64); 5]>()?;
        let (label, style, text_value, value, value_style) = try_join! {
            bs.compile_ref(label),
            bs.compile_ref(style),
            bs.compile_ref(text_value),
            bs.compile_ref(value),
            bs.compile_ref(value_style)
        }?;
        Ok(Self {
            label: TRef::new(label)?,
            style: TRef::new(style)?,
            text_value: TRef::new(text_value)?,
            value: TRef::new(value)?,
            value_style: TRef::new(value_style)?,
        })
    }

    fn update(&mut self, id: ExprId, v: &Value) -> Result<()> {
        self.label.update(id, v).context("bar label update")?;
        self.style.update(id, v).context("bar style update")?;
        self.text_value.update(id, v).context("bar text_value update")?;
        self.value.update(id, v).context("bar value update")?;
        self.value_style.update(id, v).context("bar value_style update")?;
        Ok(())
    }

    fn build<'a>(&'a self) -> Bar<'a> {
        let mut bar = Bar::default().value(self.value.t.unwrap_or(0));
        if let Some(Some(LineV(l))) = &self.label.t {
            bar = bar.label(into_borrowed_line(l));
        }
        if let Some(Some(s)) = &self.style.t {
            bar = bar.style(s.0);
        }
        if let Some(Some(s)) = &self.value_style.t {
            bar = bar.value_style(s.0);
        }
        if let Some(Some(tv)) = &self.text_value.t {
            bar = bar.text_value(tv.to_string());
        }
        bar
    }
}

struct BarGroupW {
    label: Option<LineV>,
    bars: Vec<BarW>,
}

impl BarGroupW {
    async fn compile(bs: &BSHandle, v: Value) -> Result<Self> {
        let [(_, bars), (_, label)] =
            v.cast_to::<[(ArcStr, Value); 2]>().context("bargroup fields")?;
        let label = label.cast_to::<Option<LineV>>()?;
        let bars = bars
            .cast_to::<SmallVec<[Value; 8]>>()?
            .into_iter()
            .map(|b| BarW::compile(bs, b));
        let bars = future::try_join_all(bars).await?;
        Ok(Self { label, bars })
    }
}

pub(super) struct BarChartW {
    bs: BSHandle,
    data_ref: Ref,
    data: Vec<BarGroupW>,
    bar_gap: TRef<Option<u16>>,
    bar_style: TRef<Option<StyleV>>,
    bar_width: TRef<Option<u16>>,
    direction: TRef<Option<DirectionV>>,
    group_gap: TRef<Option<u16>>,
    label_style: TRef<Option<StyleV>>,
    max: TRef<Option<u64>>,
    style: TRef<Option<StyleV>>,
    value_style: TRef<Option<StyleV>>,
}

impl BarChartW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let flds = v.cast_to::<[(ArcStr, u64); 10]>().context("barchart fields")?;
        debug!("compile fields {flds:?}");
        let [(_, bar_gap), (_, bar_style), (_, bar_width), (_, data), (_, direction), (_, group_gap), (_, label_style), (_, max), (_, style), (_, value_style)] =
            flds;
        let (
            bar_gap,
            bar_style,
            bar_width,
            data_ref,
            direction,
            group_gap,
            label_style,
            max,
            style,
            value_style,
        ) = try_join! {
            bs.compile_ref(bar_gap),
            bs.compile_ref(bar_style),
            bs.compile_ref(bar_width),
            bs.compile_ref(data),
            bs.compile_ref(direction),
            bs.compile_ref(group_gap),
            bs.compile_ref(label_style),
            bs.compile_ref(max),
            bs.compile_ref(style),
            bs.compile_ref(value_style)
        }?;
        let bar_gap =
            TRef::<Option<u16>>::new(bar_gap).context("barchart tref bar_gap")?;
        let bar_style =
            TRef::<Option<StyleV>>::new(bar_style).context("barchart tref bar_style")?;
        let bar_width =
            TRef::<Option<u16>>::new(bar_width).context("barchart tref bar_width")?;
        let direction = TRef::<Option<DirectionV>>::new(direction)
            .context("barchart tref direction")?;
        let group_gap =
            TRef::<Option<u16>>::new(group_gap).context("barchart tref group_gap")?;
        let label_style = TRef::<Option<StyleV>>::new(label_style)
            .context("barchart tref label_style")?;
        let max = TRef::<Option<u64>>::new(max).context("barchart tref max")?;
        let style = TRef::<Option<StyleV>>::new(style).context("barchart tref style")?;
        let value_style = TRef::<Option<StyleV>>::new(value_style)
            .context("barchart tref value_style")?;
        let mut t = Self {
            bs: bs.clone(),
            data_ref,
            data: vec![],
            bar_gap,
            bar_style,
            bar_width,
            direction,
            group_gap,
            label_style,
            max,
            style,
            value_style,
        };
        if let Some(v) = t.data_ref.last.take() {
            debug!("data init {v}");
            t.set_data(v).await?;
        }
        Ok(Box::new(t))
    }

    async fn set_data(&mut self, v: Value) -> Result<()> {
        let groups = v
            .cast_to::<SmallVec<[Value; 8]>>()?
            .into_iter()
            .map(|g| BarGroupW::compile(&self.bs, g));
        Ok(self.data = try_join_all(groups).await?)
    }
}

#[async_trait]
impl GuiWidget for BarChartW {
    async fn handle_event(&mut self, _e: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        debug!("handle update {:?}", (id, &v));
        let Self {
            bs: _,
            data_ref,
            data: _,
            bar_gap,
            bar_style,
            bar_width,
            direction,
            group_gap,
            label_style,
            max,
            style,
            value_style,
        } = self;
        bar_gap.update(id, &v).context("barchart update bar_gap")?;
        bar_style.update(id, &v).context("barchart update bar_style")?;
        bar_width.update(id, &v).context("barchart update bar_width")?;
        direction.update(id, &v).context("barchart update direction")?;
        group_gap.update(id, &v).context("barchart update group_gap")?;
        label_style.update(id, &v).context("barchart update label_style")?;
        max.update(id, &v).context("barchart update max")?;
        style.update(id, &v).context("barchart update style")?;
        value_style.update(id, &v).context("barchart update value_style")?;
        if data_ref.id == id {
            debug!("update data {}", v);
            self.set_data(v.clone()).await?;
        }
        for g in self.data.iter_mut() {
            for b in &mut g.bars {
                b.update(id, &v)?;
            }
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            data_ref: _,
            data,
            bar_gap,
            bar_style,
            bar_width,
            direction,
            group_gap,
            label_style,
            max,
            style,
            value_style,
        } = self;
        let mut chart = BarChart::default();
        if let Some(Some(g)) = bar_gap.t {
            chart = chart.bar_gap(g);
        }
        if let Some(Some(w)) = bar_width.t {
            chart = chart.bar_width(w);
        }
        if let Some(Some(s)) = &bar_style.t {
            chart = chart.bar_style(s.0);
        }
        if let Some(Some(s)) = &value_style.t {
            chart = chart.value_style(s.0);
        }
        if let Some(Some(s)) = &label_style.t {
            chart = chart.label_style(s.0);
        }
        if let Some(Some(s)) = &style.t {
            chart = chart.style(s.0);
        }
        if let Some(Some(m)) = max.t {
            chart = chart.max(m);
        }
        if let Some(Some(d)) = direction.t {
            chart = chart.direction(d.0);
        }
        if let Some(Some(gap)) = group_gap.t {
            chart = chart.group_gap(gap);
        }
        debug!("draw data length: {}", data.len());
        for group in data.iter_mut() {
            let mut bars: SmallVec<[Bar; 8]> = smallvec![];
            let mut g = BarGroup::default();
            if let Some(LineV(l)) = &group.label {
                g = g.label(into_borrowed_line(l));
            }
            for bar in &group.bars {
                bars.push(bar.build());
            }
            let g = g.bars(&bars);
            chart = chart.data(g);
        }
        frame.render_widget(chart, rect);
        Ok(())
    }
}
