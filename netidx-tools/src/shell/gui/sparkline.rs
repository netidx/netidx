use super::{GuiW, GuiWidget, StyleV, TRef};
use anyhow::{bail, Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::Rect,
    widgets::{RenderDirection, Sparkline, SparklineBar},
    Frame,
};
use tokio::try_join;

#[derive(Clone, Copy)]
struct RenderDirectionV(RenderDirection);

impl FromValue for RenderDirectionV {
    fn from_value(v: Value) -> Result<Self> {
        match &*v.cast_to::<ArcStr>()? {
            "LeftToRight" => Ok(Self(RenderDirection::LeftToRight)),
            "RightToLeft" => Ok(Self(RenderDirection::RightToLeft)),
            s => bail!("invalid render direction {s}"),
        }
    }
}

#[derive(Clone, Copy)]
struct SparklineBarV(SparklineBar);

impl FromValue for SparklineBarV {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Array(_) => {
                let [(_, style), (_, value)] = v.cast_to::<[(ArcStr, Value); 2]>()?;
                let style = style.cast_to::<Option<StyleV>>()?.map(|s| s.0);
                let value = value.cast_to::<Option<f64>>()?.map(|v| v as u64);
                Ok(Self(SparklineBar::from(value).style(style)))
            }
            v => {
                let value = v.cast_to::<Option<f64>>()?.map(|v| v as u64);
                Ok(Self(SparklineBar::from(value)))
            }
        }
    }
}

pub(super) struct SparklineW {
    absent_value_style: TRef<Option<StyleV>>,
    absent_value_symbol: TRef<Option<ArcStr>>,
    data_ref: Ref,
    data: Vec<SparklineBar>,
    direction: TRef<Option<RenderDirectionV>>,
    max: TRef<Option<u64>>,
    style: TRef<Option<StyleV>>,
}

impl SparklineW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, absent_value_style), (_, absent_value_symbol), (_, data), (_, direction), (_, max), (_, style)] =
            v.cast_to::<[(ArcStr, u64); 6]>()?;
        let (absent_value_style, absent_value_symbol, data_ref, direction, max, style) =
            try_join! {
                bs.compile_ref(absent_value_style),
                bs.compile_ref(absent_value_symbol),
                bs.compile_ref(data),
                bs.compile_ref(direction),
                bs.compile_ref(max),
                bs.compile_ref(style),
            }?;
        let mut t = Self {
            absent_value_style: TRef::new(absent_value_style)
                .context("sparkline tref absent_value_style")?,
            absent_value_symbol: TRef::new(absent_value_symbol)
                .context("sparkline tref absent_value_symbol")?,
            data_ref,
            data: vec![],
            direction: TRef::new(direction).context("sparkline tref direction")?,
            max: TRef::new(max).context("sparkline tref max")?,
            style: TRef::new(style).context("sparkline tref style")?,
        };
        if let Some(v) = t.data_ref.last.take() {
            t.set_data(&v)?;
        }
        Ok(Box::new(t))
    }

    fn set_data(&mut self, v: &Value) -> Result<()> {
        self.data.clear();
        match v {
            Value::Array(a) => {
                for v in a {
                    self.data.push(v.clone().cast_to::<SparklineBarV>()?.0);
                }
            }
            v => bail!("invalid sparkline data {v}"),
        }
        Ok(())
    }
}

#[async_trait]
impl GuiWidget for SparklineW {
    async fn handle_event(&mut self, _e: Event, _v: Value) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            absent_value_style,
            absent_value_symbol,
            data_ref,
            data: _,
            direction,
            max,
            style,
        } = self;
        absent_value_style
            .update(id, &v)
            .context("sparkline update absent_value_style")?;
        absent_value_symbol
            .update(id, &v)
            .context("sparkline update absent_value_symbol")?;
        direction.update(id, &v).context("sparkline update direction")?;
        max.update(id, &v).context("sparkline update max")?;
        style.update(id, &v).context("sparkline update style")?;
        if data_ref.id == id {
            self.set_data(&v)?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            absent_value_style,
            absent_value_symbol,
            data_ref: _,
            data,
            direction,
            max,
            style,
        } = self;
        let mut spark = Sparkline::default().data(data.iter().map(|b| b.clone()));
        if let Some(Some(s)) = &absent_value_style.t {
            spark = spark.absent_value_style(s.0);
        }
        if let Some(Some(s)) = &absent_value_symbol.t {
            spark = spark.absent_value_symbol(s.to_string());
        }
        if let Some(Some(m)) = max.t {
            spark = spark.max(m);
        }
        if let Some(Some(s)) = &style.t {
            spark = spark.style(s.0);
        }
        if let Some(Some(d)) = direction.t {
            spark = spark.direction(d.0);
        }
        frame.render_widget(spark, rect);
        Ok(())
    }
}
