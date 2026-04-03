use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct LinkButtonW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::LinkButton,
    label: TRef<X, String>,
    uri: TRef<X, String>,
    on_activate_link: Ref<X>,
    on_activate_link_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> LinkButtonW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: label, on_activate_link, uri
        let [(_, label), (_, on_activate_link), (_, uri)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("link_button flds")?;
        let (label, on_activate_link, uri) = tokio::try_join! {
            ctx.gx.compile_ref(label),
            ctx.gx.compile_ref(on_activate_link),
            ctx.gx.compile_ref(uri),
        }?;
        let label: TRef<X, String> =
            TRef::new(label).context("link_button tref label")?;
        let uri: TRef<X, String> =
            TRef::new(uri).context("link_button tref uri")?;
        let on_activate_link_callable =
            compile_callable!(ctx.gx, on_activate_link, "link_button on_activate_link");
        let uri_str = uri.t.as_deref().unwrap_or("");
        let widget = gtk::LinkButton::with_label(uri_str, label.t.as_deref().unwrap_or(uri_str));
        let signal_id =
            connect_on_activate_link(&widget, &ctx.gx, &on_activate_link_callable);
        widget.show();
        Ok(Box::new(LinkButtonW {
            ctx,
            widget,
            label,
            uri,
            on_activate_link,
            on_activate_link_callable,
            signal_id,
        }))
    }
}

fn connect_on_activate_link<X: GXExt>(
    widget: &gtk::LinkButton,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_activate_link(move |btn| {
        let uri = btn.uri().map(|s| s.to_string()).unwrap_or_default();
        let args = ValArray::from_iter([Value::String(uri.into())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("link_button on_activate_link call failed: {}", e);
        }
        glib::Propagation::Stop
    }))
}

impl<X: GXExt> GtkWidget<X> for LinkButtonW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(u) = self.uri.update(id, v).context("link_button update uri")? {
            self.widget.set_uri(u);
            changed = true;
        }
        if let Some(l) = self.label.update(id, v).context("link_button update label")? {
            self.widget.set_label(l);
            changed = true;
        }
        if id == self.on_activate_link.id {
            self.on_activate_link.last = Some(v.clone());
            self.on_activate_link_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("link_button on_activate_link recompile")?,
            );
            if let Some(sig) = self.signal_id.take() {
                self.widget.disconnect(sig);
            }
            self.signal_id = connect_on_activate_link(
                &self.widget,
                &self.ctx.gx,
                &self.on_activate_link_callable,
            );
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
