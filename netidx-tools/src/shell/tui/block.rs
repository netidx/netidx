use super::{
    compile, into_borrowed_line, AlignmentV, EmptyW, LineV, PositionV, StyleV, TRef,
    TuiW, TuiWidget,
};
use anyhow::{Context, Result};
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
    widgets::{Block, BorderType, Borders, Padding},
    Frame,
};
use smallvec::SmallVec;
use tokio::try_join;

#[derive(Clone, Copy)]
struct BordersV(Borders);

impl FromValue for BordersV {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => match &*s {
                "All" => Ok(Self(Borders::all())),
                "None" => Ok(Self(Borders::empty())),
                s => bail!("invalid borders {s}"),
            },
            v => {
                let mut res = Borders::empty();
                for b in v.cast_to::<SmallVec<[ArcStr; 4]>>()? {
                    match &*b {
                        "Top" => res.insert(Borders::TOP),
                        "Right" => res.insert(Borders::RIGHT),
                        "Bottom" => res.insert(Borders::BOTTOM),
                        "Left" => res.insert(Borders::LEFT),
                        s => bail!("invalid border {s}"),
                    }
                }
                Ok(Self(res))
            }
        }
    }
}

#[derive(Clone, Copy)]
struct BorderTypeV(BorderType);

impl FromValue for BorderTypeV {
    fn from_value(v: Value) -> Result<Self> {
        match &*v.cast_to::<ArcStr>()? {
            "Plain" => Ok(Self(BorderType::Plain)),
            "Rounded" => Ok(Self(BorderType::Rounded)),
            "Double" => Ok(Self(BorderType::Double)),
            "Thick" => Ok(Self(BorderType::Thick)),
            "QuadrantInside" => Ok(Self(BorderType::QuadrantInside)),
            "QuadrantOutside" => Ok(Self(BorderType::QuadrantOutside)),
            s => bail!("invalid border type {s}"),
        }
    }
}

#[derive(Clone, Copy)]
struct PaddingV(Padding);

impl FromValue for PaddingV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, bottom), (_, left), (_, right), (_, top)] =
            v.cast_to::<[(ArcStr, u16); 4]>()?;
        Ok(Self(Padding { bottom, left, right, top }))
    }
}

pub(super) struct BlockW {
    bs: BSHandle,
    border: TRef<Option<BordersV>>,
    border_style: TRef<Option<StyleV>>,
    border_type: TRef<Option<BorderTypeV>>,
    child_ref: Ref,
    child: TuiW,
    padding: TRef<Option<PaddingV>>,
    style: TRef<Option<StyleV>>,
    title: TRef<Option<LineV>>,
    title_alignment: TRef<Option<AlignmentV>>,
    title_bottom: TRef<Option<LineV>>,
    title_position: TRef<Option<PositionV>>,
    title_style: TRef<Option<StyleV>>,
    title_top: TRef<Option<LineV>>,
}

impl BlockW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<TuiW> {
        let [(_, border), (_, border_style), (_, border_type), (_, child), (_, padding), (_, style), (_, title), (_, title_alignment), (_, title_bottom), (_, title_position), (_, title_style), (_, title_top)] =
            v.cast_to::<[(ArcStr, u64); 12]>().context("block flds")?;
        let (
            border,
            border_style,
            border_type,
            mut child_ref,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
        ) = try_join! {
            bs.compile_ref(border),
            bs.compile_ref(border_style),
            bs.compile_ref(border_type),
            bs.compile_ref(child),
            bs.compile_ref(padding),
            bs.compile_ref(style),
            bs.compile_ref(title),
            bs.compile_ref(title_alignment),
            bs.compile_ref(title_bottom),
            bs.compile_ref(title_position),
            bs.compile_ref(title_style),
            bs.compile_ref(title_top),
        }?;
        let border =
            TRef::<Option<BordersV>>::new(border).context("block tref border")?;
        let border_style = TRef::<Option<StyleV>>::new(border_style)
            .context("block tref border_style")?;
        let border_type = TRef::<Option<BorderTypeV>>::new(border_type)
            .context("block tref border_type")?;
        let padding =
            TRef::<Option<PaddingV>>::new(padding).context("block tref padding")?;
        let style = TRef::<Option<StyleV>>::new(style).context("block tref style")?;
        let title = TRef::<Option<LineV>>::new(title).context("block tref title")?;
        let title_alignment = TRef::<Option<AlignmentV>>::new(title_alignment)
            .context("block tref title_alignment")?;
        let title_bottom = TRef::<Option<LineV>>::new(title_bottom)
            .context("block tref title_bottom")?;
        let title_position = TRef::<Option<PositionV>>::new(title_position)
            .context("block tref title_position")?;
        let title_style =
            TRef::<Option<StyleV>>::new(title_style).context("block tref title_style")?;
        let title_top =
            TRef::<Option<LineV>>::new(title_top).context("block tref title_top")?;
        let child = match child_ref.last.take() {
            None => Box::new(EmptyW),
            Some(v) => compile(bs.clone(), v).await.context("block compile child")?,
        };
        let t = Self {
            bs,
            border,
            border_style,
            border_type,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
            child_ref,
            child,
        };
        Ok(Box::new(t))
    }
}

#[async_trait]
impl TuiWidget for BlockW {
    async fn handle_event(&mut self, e: Event, v: Value) -> Result<()> {
        self.child.handle_event(e, v).await
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs,
            border,
            border_style,
            border_type,
            child_ref,
            child,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
        } = self;
        border.update(id, &v).context("block border update")?;
        border_style.update(id, &v).context("block border_style update")?;
        border_type.update(id, &v).context("block border_type update")?;
        padding.update(id, &v).context("block padding update")?;
        style.update(id, &v).context("block style update")?;
        title.update(id, &v).context("block title update")?;
        title_alignment.update(id, &v).context("block title_alignment update")?;
        title_bottom.update(id, &v).context("block title_bottom update")?;
        title_position.update(id, &v).context("block title_position update")?;
        title_style.update(id, &v).context("block title_style update")?;
        title_top.update(id, &v).context("block title_top update")?;
        if id == child_ref.id {
            *child =
                compile(bs.clone(), v.clone()).await.context("block child compile")?;
        }
        child.handle_update(id, v).await?;
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            border,
            border_style,
            border_type,
            child_ref: _,
            child,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
        } = self;
        let mut block = Block::new();
        if let Some(Some(b)) = border.t {
            block = block.borders(b.0);
        }
        if let Some(Some(s)) = border_style.t {
            block = block.border_style(s.0);
        }
        if let Some(Some(t)) = border_type.t {
            block = block.border_type(t.0);
        }
        if let Some(Some(p)) = padding.t {
            block = block.padding(p.0);
        }
        if let Some(Some(s)) = style.t {
            block = block.style(s.0);
        }
        if let Some(Some(LineV(l))) = &title.t {
            block = block.title(into_borrowed_line(l));
        }
        if let Some(Some(a)) = title_alignment.t {
            block = block.title_alignment(a.0);
        }
        if let Some(Some(LineV(l))) = &title_bottom.t {
            block = block.title_bottom(into_borrowed_line(l));
        }
        if let Some(Some(p)) = title_position.t {
            block = block.title_position(p.0);
        }
        if let Some(Some(s)) = title_style.t {
            block = block.title_style(s.0);
        }
        if let Some(Some(LineV(l))) = &title_top.t {
            block = block.title_top(into_borrowed_line(l));
        }
        let child_rect = block.inner(rect);
        frame.render_widget(block, rect);
        child.draw(frame, child_rect)?;
        Ok(())
    }
}
