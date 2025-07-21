use super::{
    compile, into_borrowed_line, LineV, SizeV, SpanV, StyleV, TRef, TuiW, TuiWidget,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use futures::future;
use netidx::publisher::Value;
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{layout::Rect, widgets::Tabs, Frame};
use smallvec::SmallVec;
use tokio::try_join;

pub(super) struct TabsW {
    bs: BSHandle,
    tabs: Vec<(LineV, TuiW)>,
    tabs_ref: Ref,
    size_ref: Ref,
    last_size: SizeV,
    divider: TRef<Option<SpanV>>,
    highlight_style: TRef<Option<StyleV>>,
    padding_left: TRef<Option<LineV>>,
    padding_right: TRef<Option<LineV>>,
    selected: TRef<Option<u32>>,
    style: TRef<Option<StyleV>>,
}

impl TabsW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<TuiW> {
        let [(_, divider), (_, highlight_style), (_, padding_left), (_, padding_right), (_, selected), (_, size), (_, style), (_, tabs)] =
            v.cast_to::<[(ArcStr, u64); 8]>().context("tabs fields")?;
        let (
            divider,
            highlight_style,
            padding_left,
            padding_right,
            selected,
            size_ref,
            style,
            tabs_ref,
        ) = try_join! {
            bs.compile_ref(divider),
            bs.compile_ref(highlight_style),
            bs.compile_ref(padding_left),
            bs.compile_ref(padding_right),
            bs.compile_ref(selected),
            bs.compile_ref(size),
            bs.compile_ref(style),
            bs.compile_ref(tabs)
        }?;
        let divider = TRef::<Option<SpanV>>::new(divider).context("tabs tref divider")?;
        let highlight_style = TRef::<Option<StyleV>>::new(highlight_style)
            .context("tabs tref highlight_style")?;
        let padding_left =
            TRef::<Option<LineV>>::new(padding_left).context("tabs tref padding_left")?;
        let padding_right = TRef::<Option<LineV>>::new(padding_right)
            .context("tabs tref padding_right")?;
        let selected =
            TRef::<Option<u32>>::new(selected).context("tabs tref selected")?;
        let style = TRef::<Option<StyleV>>::new(style).context("tabs tref style")?;
        let mut t = Self {
            bs: bs.clone(),
            tabs: vec![],
            tabs_ref,
            divider,
            highlight_style,
            padding_left,
            padding_right,
            selected,
            size_ref,
            last_size: SizeV::default(),
            style,
        };
        if let Some(v) = t.tabs_ref.last.take() {
            t.set_tabs(v).await?;
        }
        Ok(Box::new(t))
    }

    async fn set_tabs(&mut self, v: Value) -> Result<()> {
        let arr = v.cast_to::<SmallVec<[(LineV, Value); 8]>>()?;
        self.tabs = future::try_join_all(arr.into_iter().map(|(l, v)| {
            let bs = self.bs.clone();
            async move {
                let w = compile(bs, v).await?;
                Ok::<(LineV, TuiW), anyhow::Error>((l, w))
            }
        }))
        .await?;
        Ok(())
    }
}

#[async_trait]
impl TuiWidget for TabsW {
    async fn handle_event(&mut self, e: Event, v: Value) -> Result<()> {
        let idx = self.selected.t.and_then(|o| o.map(|s| s as usize)).unwrap_or(0);
        if let Some((_, child)) = self.tabs.get_mut(idx) {
            child.handle_event(e, v).await?;
        }
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs: _,
            tabs: _,
            tabs_ref,
            size_ref: _,
            last_size: _,
            divider,
            highlight_style,
            padding_left,
            padding_right,
            selected,
            style,
        } = self;
        divider.update(id, &v).context("tabs divider update")?;
        highlight_style.update(id, &v).context("tabs highlight_style update")?;
        padding_left.update(id, &v).context("tabs padding_left update")?;
        padding_right.update(id, &v).context("tabs padding_right update")?;
        selected.update(id, &v).context("tabs selected update")?;
        style.update(id, &v).context("tabs style update")?;
        if tabs_ref.id == id {
            self.set_tabs(v.clone()).await?;
        }
        for (_, c) in &mut self.tabs {
            c.handle_update(id, v.clone()).await?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            tabs,
            tabs_ref: _,
            size_ref,
            last_size,
            divider,
            highlight_style,
            padding_left,
            padding_right,
            selected,
            style,
        } = self;
        let titles: Vec<_> = tabs.iter().map(|(l, _)| into_borrowed_line(&l.0)).collect();
        let mut t = Tabs::new(titles);
        if let Some(Some(s)) = &style.t {
            t = t.style(s.0);
        }
        if let Some(Some(s)) = &highlight_style.t {
            t = t.highlight_style(s.0);
        }
        if let Some(Some(s)) = &divider.t {
            t = t.divider(s.0.clone());
        }
        if let Some(Some(l)) = &padding_left.t {
            t = t.padding_left(into_borrowed_line(&l.0));
        }
        if let Some(Some(r)) = &padding_right.t {
            t = t.padding_right(into_borrowed_line(&r.0));
        }
        if let Some(Some(s)) = selected.t {
            t = t.select(s as usize);
        }
        let mut bar_rect = rect;
        if bar_rect.height > 0 {
            bar_rect.height = 1;
        }
        frame.render_widget(t, bar_rect);
        let mut child_rect = rect;
        if child_rect.height > 0 {
            child_rect.y = child_rect.y.saturating_add(1);
            child_rect.height = child_rect.height.saturating_sub(1);
        }
        let idx = selected.t.and_then(|o| o.map(|s| s as usize)).unwrap_or(0);
        let size = SizeV::from(child_rect);
        if *last_size != size {
            *last_size = size;
            size_ref.set_deref(size)?
        }
        if let Some((_, child)) = tabs.get_mut(idx) {
            child.draw(frame, child_rect)?;
        }
        Ok(())
    }
}
