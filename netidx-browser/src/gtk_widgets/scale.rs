use super::{CommonProps, CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct ScaleW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Scale,
    common: CommonProps<X>,
    direction: TRef<X, String>,
    draw_value: TRef<X, bool>,
    has_origin: TRef<X, bool>,
    marks: Ref<X>,
    value: TRef<X, f64>,
    min: TRef<X, f64>,
    max: TRef<X, f64>,
    on_change: Ref<X>,
    on_change_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

fn parse_orientation(s: &str) -> gtk::Orientation {
    match s {
        "Vertical" => gtk::Orientation::Vertical,
        _ => gtk::Orientation::Horizontal,
    }
}

fn parse_position(s: &str) -> gtk::PositionType {
    match s {
        "Left" => gtk::PositionType::Left,
        "Right" => gtk::PositionType::Right,
        "Top" => gtk::PositionType::Top,
        _ => gtk::PositionType::Bottom,
    }
}

fn apply_marks(widget: &gtk::Scale, marks_ref: &Ref<impl GXExt>) {
    widget.clear_marks();
    let marks_val = match marks_ref.last.as_ref() {
        Some(v) => v,
        None => return,
    };
    // marks is either null or array<ScaleMark>
    // ScaleMark = { position: f64, pos: DrawValuePosition, text: [string, null] }
    // fields sorted: pos, position, text
    let items = match marks_val.clone().cast_to::<Vec<Value>>() {
        Ok(v) => v,
        Err(_) => return,
    };
    for item in &items {
        if let Ok([(_, pos_val), (_, position_val), (_, text_val)]) =
            item.clone().cast_to::<[(ArcStr, Value); 3]>()
        {
            let position = position_val.clone().cast_to::<f64>().unwrap_or(0.0);
            let pos_str = match pos_val.clone().cast_to::<(ArcStr, Value)>() {
                Ok((tag, _)) => tag.to_string(),
                Err(_) => "Bottom".to_string(),
            };
            let pos = parse_position(&pos_str);
            let text: Option<String> = text_val.clone().cast_to::<String>().ok();
            widget.add_mark(position, pos, text.as_deref());
        }
    }
}

impl<X: GXExt> ScaleW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: common, debug_highlight, direction, draw_value, has_origin, marks, max, min, on_change, value
        let [
            (_, common),
            (_, debug_highlight),
            (_, direction),
            (_, draw_value),
            (_, has_origin),
            (_, marks),
            (_, max),
            (_, min),
            (_, on_change),
            (_, value),
        ] = source.cast_to::<[(ArcStr, u64); 10]>().context("scale flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (direction, draw_value, has_origin, marks, max, min, on_change, value) =
            tokio::try_join! {
                ctx.gx.compile_ref(direction),
                ctx.gx.compile_ref(draw_value),
                ctx.gx.compile_ref(has_origin),
                ctx.gx.compile_ref(marks),
                ctx.gx.compile_ref(max),
                ctx.gx.compile_ref(min),
                ctx.gx.compile_ref(on_change),
                ctx.gx.compile_ref(value),
            }?;
        let direction: TRef<X, String> =
            TRef::new(direction).context("scale tref direction")?;
        let draw_value: TRef<X, bool> =
            TRef::new(draw_value).context("scale tref draw_value")?;
        let has_origin: TRef<X, bool> =
            TRef::new(has_origin).context("scale tref has_origin")?;
        let value: TRef<X, f64> =
            TRef::new(value).context("scale tref value")?;
        let min: TRef<X, f64> =
            TRef::new(min).context("scale tref min")?;
        let max: TRef<X, f64> =
            TRef::new(max).context("scale tref max")?;
        let on_change_callable =
            compile_callable!(ctx.gx, on_change, "scale on_change");
        let min_val = min.t.unwrap_or(0.0);
        let max_val = max.t.unwrap_or(1.0);
        let orientation = parse_orientation(
            direction.t.as_deref().unwrap_or("Horizontal"),
        );
        let widget = gtk::Scale::with_range(
            orientation,
            min_val,
            max_val,
            (max_val - min_val) / 100.0,
        );
        widget.set_draw_value(draw_value.t.unwrap_or(true));
        widget.set_has_origin(has_origin.t.unwrap_or(true));
        apply_marks(&widget, &marks);
        if let Some(v) = value.t {
            widget.set_value(v);
        }
        let signal_id = connect_on_change(&widget, &ctx.gx, &on_change_callable);
        common_props.apply(&widget);
        widget.show();
        Ok(Box::new(ScaleW {
            ctx,
            widget,
            common: common_props,
            direction,
            draw_value,
            has_origin,
            marks,
            value,
            min,
            max,
            on_change,
            on_change_callable,
            signal_id,
        }))
    }
}

fn connect_on_change<X: GXExt>(
    widget: &gtk::Scale,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_value_changed(move |scale| {
        let args = ValArray::from_iter([Value::F64(scale.value())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("scale on_change call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for ScaleW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        changed |= self.common.handle_update(rt, id, v, &self.widget)?;
        if let Some(d) = self
            .direction
            .update(id, v)
            .context("scale update direction")?
        {
            self.widget.set_orientation(parse_orientation(d));
            changed = true;
        }
        if let Some(dv) = self
            .draw_value
            .update(id, v)
            .context("scale update draw_value")?
        {
            self.widget.set_draw_value(*dv);
            changed = true;
        }
        if let Some(ho) = self
            .has_origin
            .update(id, v)
            .context("scale update has_origin")?
        {
            self.widget.set_has_origin(*ho);
            changed = true;
        }
        if id == self.marks.id {
            self.marks.last = Some(v.clone());
            apply_marks(&self.widget, &self.marks);
            changed = true;
        }
        if let Some(val) = self.value.update(id, v).context("scale update value")? {
            if let Some(ref sig) = self.signal_id {
                self.widget.block_signal(sig);
            }
            self.widget.set_value(*val);
            if let Some(ref sig) = self.signal_id {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if let Some(mn) = self.min.update(id, v).context("scale update min")? {
            let mx = self.max.t.unwrap_or(1.0);
            self.widget.set_range(*mn, mx);
            changed = true;
        }
        if let Some(mx) = self.max.update(id, v).context("scale update max")? {
            let mn = self.min.t.unwrap_or(0.0);
            self.widget.set_range(mn, *mx);
            changed = true;
        }
        if id == self.on_change.id {
            self.on_change.last = Some(v.clone());
            self.on_change_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("scale on_change recompile")?,
            );
            if let Some(sig) = self.signal_id.take() {
                self.widget.disconnect(sig);
            }
            self.signal_id =
                connect_on_change(&self.widget, &self.ctx.gx, &self.on_change_callable);
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
