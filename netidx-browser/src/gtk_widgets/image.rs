use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct ImageW<X: GXExt> {
    ctx: CompileCtx<X>,
    event_box: gtk::EventBox,
    image: gtk::Image,
    spec: Ref<X>,
    on_click: Ref<X>,
    on_click_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> ImageW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: on_click, spec
        let [(_, on_click), (_, spec)] =
            source.cast_to::<[(ArcStr, u64); 2]>().context("image flds")?;
        let (on_click, spec) = tokio::try_join! {
            ctx.gx.compile_ref(on_click),
            ctx.gx.compile_ref(spec),
        }?;
        let on_click_callable =
            compile_callable!(ctx.gx, on_click, "image on_click");
        let image = gtk::Image::new();
        if let Some(ref v) = spec.last {
            apply_image_spec(&image, v);
        }
        let event_box = gtk::EventBox::new();
        event_box.add(&image);
        let signal_id = connect_on_click(&event_box, &ctx.gx, &on_click_callable);
        event_box.show_all();
        Ok(Box::new(ImageW {
            ctx,
            event_box,
            image,
            spec,
            on_click,
            on_click_callable,
            signal_id,
        }))
    }
}

/// Apply an image spec value to a gtk::Image.
/// The spec is a variant: `Icon({name, size}), `PixBuf({data, ...}), or a plain string (icon name).
fn apply_image_spec(image: &gtk::Image, v: &Value) {
    // Try as plain string first (icon name shorthand)
    if let Ok(name) = v.clone().cast_to::<String>() {
        image.set_from_icon_name(Some(&name), gtk::IconSize::Button);
        return;
    }
    // Try as variant
    if let Ok((tag, inner)) = v.clone().cast_to::<(ArcStr, Value)>() {
        match tag.as_str() {
            "Icon" => {
                // {name: string, size: IconSize} - fields sorted: name, size
                if let Ok([(_, name_val), (_, size_val)]) =
                    inner.cast_to::<[(ArcStr, Value); 2]>()
                {
                    let name = name_val.cast_to::<String>().unwrap_or_default();
                    let size = parse_icon_size(
                        &size_val.cast_to::<String>().unwrap_or_default(),
                    );
                    image.set_from_icon_name(Some(&name), size);
                }
            }
            "PixBuf" => {
                // {data: bytes, height: [i64,null], keep_aspect: bool, width: [i64,null]}
                // fields sorted: data, height, keep_aspect, width
                if let Ok([(_, data_val), (_, height_val), (_, keep_aspect_val), (_, width_val)]) =
                    inner.cast_to::<[(ArcStr, Value); 4]>()
                {
                    if let Ok(data) = data_val.cast_to::<bytes::Bytes>() {
                        let loader = gdk_pixbuf::PixbufLoader::new();
                        if loader.write(&data).is_ok() {
                            let _ = loader.close();
                            if let Some(pixbuf) = loader.pixbuf() {
                                let width = width_val.cast_to::<i64>().ok().map(|w| w as i32);
                                let height = height_val.cast_to::<i64>().ok().map(|h| h as i32);
                                let keep_aspect = keep_aspect_val.cast_to::<bool>().unwrap_or(true);
                                let scaled = match (width, height) {
                                    (Some(w), Some(h)) => pixbuf.scale_simple(
                                        w, h,
                                        gdk_pixbuf::InterpType::Bilinear,
                                    ),
                                    (Some(w), None) if keep_aspect => {
                                        let h = (pixbuf.height() as f64 * w as f64 / pixbuf.width() as f64) as i32;
                                        pixbuf.scale_simple(w, h, gdk_pixbuf::InterpType::Bilinear)
                                    }
                                    (None, Some(h)) if keep_aspect => {
                                        let w = (pixbuf.width() as f64 * h as f64 / pixbuf.height() as f64) as i32;
                                        pixbuf.scale_simple(w, h, gdk_pixbuf::InterpType::Bilinear)
                                    }
                                    _ => Some(pixbuf),
                                };
                                image.set_from_pixbuf(scaled.as_ref());
                            }
                        }
                    }
                }
            }
            _ => {
                // Unknown variant tag, treat tag as icon name
                image.set_from_icon_name(Some(tag.as_str()), gtk::IconSize::Button);
            }
        }
    }
}

fn parse_icon_size(s: &str) -> gtk::IconSize {
    match s {
        "Menu" => gtk::IconSize::Menu,
        "SmallToolbar" => gtk::IconSize::SmallToolbar,
        "LargeToolbar" => gtk::IconSize::LargeToolbar,
        "Dnd" => gtk::IconSize::Dnd,
        "Dialog" => gtk::IconSize::Dialog,
        _ => gtk::IconSize::Button,
    }
}

fn connect_on_click<X: GXExt>(
    event_box: &gtk::EventBox,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(event_box.connect_button_press_event(move |_, _| {
        let args = ValArray::from_iter([Value::Null]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("image on_click call failed: {}", e);
        }
        glib::Propagation::Stop
    }))
}

impl<X: GXExt> GtkWidget<X> for ImageW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if self.spec.update(id, v).is_some() {
            apply_image_spec(&self.image, v);
            changed = true;
        }
        if id == self.on_click.id {
            self.on_click.last = Some(v.clone());
            self.on_click_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("image on_click recompile")?,
            );
            if let Some(sig) = self.signal_id.take() {
                self.event_box.disconnect(sig);
            }
            self.signal_id =
                connect_on_click(&self.event_box, &self.ctx.gx, &self.on_click_callable);
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.event_box.upcast_ref()
    }
}
