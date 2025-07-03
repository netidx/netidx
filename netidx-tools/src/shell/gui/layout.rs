use super::{compile, DirectionV, GuiW, GuiWidget, TRef};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use futures::future;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::{Constraint, Flex, Layout, Rect, Spacing},
    Frame,
};
use smallvec::SmallVec;
use tokio::try_join;

#[derive(Clone, Copy)]
struct ConstraintV(Constraint);

impl FromValue for ConstraintV {
    fn from_value(v: Value) -> Result<Self> {
        let t = match &v.cast_to::<SmallVec<[Value; 3]>>()?[..] {
            [Value::String(s), Value::U32(p)] => match &**s {
                "Min" => Constraint::Min(*p as u16),
                "Max" => Constraint::Max(*p as u16),
                "Percentage" => Constraint::Percentage(*p as u16),
                "Fill" => Constraint::Fill(*p as u16),
                s => bail!("invalid constraint tag {s}"),
            },
            [Value::String(s), Value::U32(n), Value::U32(d)] if &**s == "Ratio" => {
                Constraint::Ratio(*n, *d)
            }
            v => bail!("invalid constraint {v:?}"),
        };
        Ok(Self(t))
    }
}

#[derive(Clone, Copy)]
struct FlexV(Flex);

impl FromValue for FlexV {
    fn from_value(v: Value) -> Result<Self> {
        let t = match &*v.cast_to::<ArcStr>()? {
            "Legacy" => Flex::Legacy,
            "Start" => Flex::Start,
            "End" => Flex::End,
            "Center" => Flex::Center,
            "SpaceBetween" => Flex::SpaceBetween,
            "SpaceAround" => Flex::SpaceAround,
            s => bail!("invalid flex {s}"),
        };
        Ok(Self(t))
    }
}

#[derive(Clone)]
struct SpacingV(Spacing);

impl FromValue for SpacingV {
    fn from_value(v: Value) -> Result<Self> {
        let t = match v.cast_to::<(ArcStr, u16)>()? {
            (s, p) if &*s == "Space" => Spacing::Space(p),
            (s, p) if &*s == "Overlap" => Spacing::Overlap(p),
            (s, _) => bail!("invalid spacing tag {s}"),
        };
        Ok(Self(t))
    }
}

pub(super) struct LayoutW {
    bs: BSHandle,
    children: Vec<(Constraint, GuiW)>,
    children_ref: Ref,
    direction: TRef<Option<DirectionV>>,
    flex: TRef<Option<FlexV>>,
    horizontal_margin: TRef<Option<u16>>,
    margin: TRef<Option<u16>>,
    spacing: TRef<Option<SpacingV>>,
    vertical_margin: TRef<Option<u16>>,
}

impl LayoutW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, children), (_, direction), (_, flex), (_, horizontal_margin), (_, margin), (_, spacing), (_, vertical_margin)] =
            v.cast_to::<[(ArcStr, u64); 7]>().context("layout fields")?;
        let (
            children_ref,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        ) = try_join! {
            bs.compile_ref(children),
            bs.compile_ref(direction),
            bs.compile_ref(flex),
            bs.compile_ref(horizontal_margin),
            bs.compile_ref(margin),
            bs.compile_ref(spacing),
            bs.compile_ref(vertical_margin)
        }?;
        let direction = TRef::<Option<DirectionV>>::new(direction)
            .context("layout tref direction")?;
        let flex = TRef::<Option<FlexV>>::new(flex).context("layout tref flex")?;
        let horizontal_margin = TRef::<Option<u16>>::new(horizontal_margin)
            .context("layout tref horizontal_margin")?;
        let margin = TRef::<Option<u16>>::new(margin).context("layout tref margin")?;
        let spacing =
            TRef::<Option<SpacingV>>::new(spacing).context("layout tref spacing")?;
        let vertical_margin = TRef::<Option<u16>>::new(vertical_margin)
            .context("layout tref vertical_margin")?;
        let mut t = Self {
            bs,
            children: vec![],
            children_ref,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        };
        if let Some(v) = t.children_ref.last.take() {
            t.set_children(v).await?;
        }
        Ok(Box::new(t))
    }

    async fn set_children(&mut self, v: Value) -> Result<()> {
        self.children = future::join_all(
            v.cast_to::<SmallVec<[(ConstraintV, Value); 8]>>()?.into_iter().map(
                |(c, v)| {
                    let bs = self.bs.clone();
                    async move {
                        let child = compile(bs, v).await?;
                        Ok((c.0, child))
                    }
                },
            ),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[async_trait]
impl GuiWidget for LayoutW {
    async fn handle_event(&mut self, e: Event) -> Result<()> {
        future::try_join_all(self.children.iter_mut().map(|(_, c)| {
            let e = e.clone();
            async move { c.handle_event(e).await }
        }))
        .await?;
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs: _,
            children: _,
            children_ref,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        } = self;
        direction.update(id, &v).context("layout direction update")?;
        flex.update(id, &v).context("layout flex update")?;
        horizontal_margin.update(id, &v).context("layout horizontal_margin update")?;
        margin.update(id, &v).context("layout margin update")?;
        spacing.update(id, &v).context("layout spacing update")?;
        vertical_margin.update(id, &v).context("layout vertical_margin update")?;
        if children_ref.id == id {
            self.set_children(v.clone()).await?;
        }
        for (_, c) in &mut self.children {
            c.handle_update(id, v.clone()).await?
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            children,
            children_ref: _,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        } = self;
        let mut layout = Layout::default();
        if let Some(Some(d)) = direction.t {
            layout = layout.direction(d.0);
        }
        if let Some(Some(f)) = flex.t {
            layout = layout.flex(f.0);
        }
        if let Some(Some(m)) = horizontal_margin.t {
            layout = layout.horizontal_margin(m);
        }
        if let Some(Some(m)) = margin.t {
            layout = layout.margin(m);
        }
        if let Some(Some(s)) = &spacing.t {
            layout = layout.spacing(s.0.clone());
        }
        if let Some(Some(m)) = vertical_margin.t {
            layout = layout.vertical_margin(m);
        }
        layout = layout.constraints(children.iter().map(|(c, _)| *c));
        let areas = layout.split(rect);
        for (rect, (_, child)) in areas.iter().zip(children.iter_mut()) {
            child.draw(frame, *rect)?
        }
        Ok(())
    }
}
