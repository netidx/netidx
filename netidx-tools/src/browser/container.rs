use super::{align_to_gtk, Widget, WidgetCtx, FromGui};
use crate::browser::view;
use futures::channel::oneshot;
use glib::clone;
use gdk::{self, prelude::*, keys};
use gtk::{self, prelude::*, Orientation};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use std::{collections::HashMap, sync::Arc, result};

pub(super) struct Container {
    root: gtk::Box,
    children: Vec<Widget>,
}

impl Container {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Container,
        selected_path: gtk::Label,
    ) -> Container {
        let dir = match spec.direction {
            view::Direction::Horizontal => Orientation::Horizontal,
            view::Direction::Vertical => Orientation::Vertical,
        };
        let root = gtk::Box::new(dir, 0);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            let w = Widget::new(
                ctx.clone(),
                variables,
                s.widget.clone(),
                selected_path.clone(),
            );
            if let Some(r) = w.root() {
                root.pack_start(r, s.expand, s.fill, s.padding as u32);
                if let Some(halign) = s.halign {
                    r.set_halign(align_to_gtk(halign));
                }
                if let Some(valign) = s.valign {
                    r.set_valign(align_to_gtk(valign));
                }
            }
            children.push(w);
        }
        root.connect_key_press_event(clone!(@strong ctx, @strong spec => move |_, k| {
            let target = {
                if k.get_keyval() == keys::constants::BackSpace {
                    &spec.drill_up_target
                } else if k.get_keyval() == keys::constants::Return {
                    &spec.drill_down_target
                } else {
                    &None
                }
            };
            match target {
                None => Inhibit(false),
                Some(target) => {
                    let m = FromGui::Navigate(target.clone());
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                    Inhibit(false)
                }
            }
        }));
        Container { root, children }
    }

    pub(super) fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        updates: &Arc<IndexMap<SubId, Value>>,
    ) {
        for c in &self.children {
            c.update(waits, updates);
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        for c in &self.children {
            c.update_var(name, value);
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
