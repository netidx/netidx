use anyhow::{bail, Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::publisher::Value;
use netidx::subscriber::Subscriber;
use std::{future::Future, pin::Pin};

/// Context passed to widget compile functions.
/// Carries the GXHandle for expression compilation and the Subscriber
/// for widgets (like Table) that need to manage their own subscriptions.
pub(crate) struct CompileCtx<X: GXExt> {
    pub gx: GXHandle<X>,
    pub subscriber: Subscriber,
}

impl<X: GXExt> Clone for CompileCtx<X> {
    fn clone(&self) -> Self {
        CompileCtx {
            gx: self.gx.clone(),
            subscriber: self.subscriber.clone(),
        }
    }
}

/// Compile an optional callable ref during widget construction.
macro_rules! compile_callable {
    ($gx:expr, $ref:ident, $label:expr) => {
        match $ref.last.as_ref() {
            Some(v) => Some($gx.compile_callable(v.clone()).await.context($label)?),
            None => None,
        }
    };
}

/// Recompile a callable ref inside `handle_update`.
macro_rules! update_callable {
    ($self:ident, $rt:ident, $id:ident, $v:ident, $field:ident, $callable:ident, $label:expr) => {
        if $id == $self.$field.id {
            $self.$field.last = Some($v.clone());
            $self.$callable = Some(
                $rt.block_on($self.ctx.gx.compile_callable($v.clone())).context($label)?,
            );
        }
    };
}

/// Compile a child widget ref during widget construction.
macro_rules! compile_child {
    ($ctx:expr, $ref:ident, $label:expr) => {
        match $ref.last.as_ref() {
            None => Box::new(EmptyW::new()) as GtkW<X>,
            Some(v) => compile($ctx.clone(), v.clone()).await.context($label)?,
        }
    };
}

/// Recompile a child widget ref inside `handle_update`.
/// Sets `$changed = true` when the child is recompiled or updated.
macro_rules! update_child {
    ($self:ident, $rt:ident, $id:ident, $v:ident, $changed:ident, $ref:ident, $child:ident, $label:expr) => {
        if $id == $self.$ref.id {
            $self.$ref.last = Some($v.clone());
            $self.$child =
                $rt.block_on(compile($self.ctx.clone(), $v.clone())).context($label)?;
            $changed = true;
        }
        $changed |= $self.$child.handle_update($rt, $id, $v)?;
    };
}

mod button;
mod chart;
mod combo;
mod containers;
mod entry;
mod image;
mod key_handler;
mod label;
mod link_button;
mod progress;
mod radio;
mod scale;
mod search_entry;
mod switch;
mod table;
mod toggle;

/// Trait for GTK widgets managed by the graphix runtime.
///
/// Unlike iced (which is immediate-mode), GTK widgets are persistent
/// objects. There is no `view()` method — instead, `gtk_widget()`
/// returns a reference to the real GTK widget for embedding in
/// containers. Updates mutate widget properties in place.
///
/// Not `Send` because `gtk::Widget` is `!Send`. All methods are
/// called from the GTK main thread.
pub(crate) trait GtkWidget<X: GXExt>: 'static {
    /// Process a graphix update. Returns true if this widget handled it.
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool>;

    /// Get the underlying GTK widget for embedding in containers.
    fn gtk_widget(&self) -> &gtk::Widget;

    /// If this widget is a BoxChild wrapper, return (pack_end, padding).
    /// pack_end=true means pack_end, false means pack_start.
    fn box_child_info(&self) -> Option<(bool, u32)> {
        None
    }

    /// If this widget is a GridCell wrapper, return (colspan, rowspan).
    fn grid_cell_info(&self) -> Option<(i32, i32)> {
        None
    }
}

pub(crate) type GtkW<X> = Box<dyn GtkWidget<X>>;

/// Future type for widget compilation (avoids infinite-size async fn).
/// Not `Send` because the output contains GTK widgets which are `!Send`.
/// These futures are always driven via `rt.block_on()` on the GTK thread.
pub(crate) type CompileFut<X> =
    Pin<Box<dyn Future<Output = Result<GtkW<X>>> + 'static>>;

/// Empty widget placeholder — used when a child ref hasn't resolved yet.
pub(crate) struct EmptyW {
    widget: gtk::Label,
}

impl EmptyW {
    fn new() -> Self {
        EmptyW { widget: gtk::Label::new(None) }
    }
}

impl<X: GXExt> GtkWidget<X> for EmptyW {
    fn handle_update(
        &mut self,
        _rt: &tokio::runtime::Handle,
        _id: ExprId,
        _v: &Value,
    ) -> Result<bool> {
        Ok(false)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}

// ---- Box packing helper ----

fn pack_box_children<X: GXExt>(
    container: &gtk::Box,
    children: &[GtkW<X>],
) {
    for child in children {
        match child.box_child_info() {
            Some((true, padding)) => {
                container.pack_end(child.gtk_widget(), true, true, padding);
            }
            Some((false, padding)) => {
                container.pack_start(child.gtk_widget(), true, true, padding);
            }
            None => {
                container.pack_start(child.gtk_widget(), true, true, 0);
            }
        }
    }
}

// ---- Box containers (VBox / HBox) ----

struct BoxW<X: GXExt> {
    ctx: CompileCtx<X>,
    container: gtk::Box,
    spacing: TRef<X, i64>,
    homogeneous: TRef<X, bool>,
    children_ref: Ref<X>,
    children: Vec<GtkW<X>>,
}

impl<X: GXExt> BoxW<X> {
    async fn compile(
        ctx: CompileCtx<X>,
        orientation: gtk::Orientation,
        source: Value,
    ) -> Result<GtkW<X>> {
        let [(_, children), (_, homogeneous), (_, spacing)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("box flds")?;
        let (children_ref, homogeneous, spacing) = tokio::try_join! {
            ctx.gx.compile_ref(children),
            ctx.gx.compile_ref(homogeneous),
            ctx.gx.compile_ref(spacing),
        }?;
        let compiled_children = match children_ref.last.as_ref() {
            None => vec![],
            Some(v) => compile_children(ctx.clone(), v.clone())
                .await
                .context("box children")?,
        };
        let spacing_val: TRef<X, i64> =
            TRef::new(spacing).context("box tref spacing")?;
        let homogeneous_val: TRef<X, bool> =
            TRef::new(homogeneous).context("box tref homogeneous")?;
        let container = gtk::Box::new(
            orientation,
            spacing_val.t.unwrap_or(0) as i32,
        );
        container.set_homogeneous(homogeneous_val.t.unwrap_or(false));
        pack_box_children(&container, &compiled_children);
        container.show_all();
        Ok(Box::new(BoxW {
            ctx,
            container,
            spacing: spacing_val,
            homogeneous: homogeneous_val,
            children_ref,
            children: compiled_children,
        }))
    }

    fn rebuild_children(&self) {
        for child in self.container.children() {
            self.container.remove(&child);
        }
        pack_box_children(&self.container, &self.children);
        self.container.show_all();
    }
}

impl<X: GXExt> GtkWidget<X> for BoxW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(sp) = self
            .spacing
            .update(id, v)
            .context("box update spacing")?
        {
            self.container.set_spacing(*sp as i32);
            changed = true;
        }
        if let Some(h) = self
            .homogeneous
            .update(id, v)
            .context("box update homogeneous")?
        {
            self.container.set_homogeneous(*h);
            changed = true;
        }
        if id == self.children_ref.id {
            self.children_ref.last = Some(v.clone());
            self.children = rt
                .block_on(compile_children(self.ctx.clone(), v.clone()))
                .context("box children recompile")?;
            self.rebuild_children();
            changed = true;
        }
        for child in &mut self.children {
            changed |= child.handle_update(rt, id, v)?;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.container.upcast_ref()
    }
}

/// Compile a widget value into a GtkW. Returns a boxed future to
/// avoid infinite-size futures from recursive async calls.
pub(crate) fn compile<X: GXExt>(
    ctx: CompileCtx<X>,
    source: Value,
) -> CompileFut<X> {
    Box::pin(async move {
        let (s, v) = source.cast_to::<(ArcStr, Value)>()?;
        match s.as_str() {
            "Label" => label::LabelW::compile(ctx, v).await,
            "Button" => button::ButtonW::compile(ctx, v).await,
            "Entry" => entry::EntryW::compile(ctx, v).await,
            "Switch" => switch::SwitchW::compile(ctx, v).await,
            "ToggleButton" => toggle::ToggleButtonW::compile_toggle(ctx, v).await,
            "CheckButton" => toggle::ToggleButtonW::compile_check(ctx, v).await,
            "RadioButton" => radio::RadioButtonW::compile(ctx, v).await,
            "ComboBox" => combo::ComboBoxW::compile(ctx, v).await,
            "Scale" => scale::ScaleW::compile(ctx, v).await,
            "ProgressBar" => progress::ProgressBarW::compile(ctx, v).await,
            "Image" => image::ImageW::compile(ctx, v).await,
            "LinkButton" => link_button::LinkButtonW::compile(ctx, v).await,
            "SearchEntry" => search_entry::SearchEntryW::compile(ctx, v).await,
            "VBox" => BoxW::compile(ctx, gtk::Orientation::Vertical, v).await,
            "HBox" => BoxW::compile(ctx, gtk::Orientation::Horizontal, v).await,
            "BoxChild" => containers::BoxChildW::compile(ctx, v).await,
            "Grid" => containers::GridW::compile(ctx, v).await,
            "GridCell" => containers::GridCellW::compile(ctx, v).await,
            "Frame" => containers::FrameW::compile(ctx, v).await,
            "Paned" => containers::PanedW::compile(ctx, v).await,
            "Notebook" => containers::NotebookW::compile(ctx, v).await,
            "Table" => table::TableW::compile(ctx, v).await,
            "Chart" => chart::ChartW::compile(ctx, v).await,
            "KeyHandler" => key_handler::KeyHandlerW::compile(ctx, v).await,
            _ => bail!("unsupported gtk widget type `{s}`"),
        }
    })
}

/// Compile an array of widget values into a Vec of GtkW.
/// Sequential because GTK widgets are !Send and can't use try_join_all.
pub(crate) async fn compile_children<X: GXExt>(
    ctx: CompileCtx<X>,
    v: Value,
) -> Result<Vec<GtkW<X>>> {
    let items = v.cast_to::<Vec<Value>>()?;
    let mut children = Vec::with_capacity(items.len());
    for item in items {
        children.push(compile(ctx.clone(), item).await?);
    }
    Ok(children)
}
