use super::{WidgetCtx, Source};
use crate::browser::view;
use glib::clone;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use std::{collections::HashMap, sync::Arc};

pub(super) struct Label {
    label: gtk::Label,
    source: Source,
}

impl Label {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Source,
        selected_path: gtk::Label,
    ) -> Label {
        let source = Source::new(&ctx, variables, spec.clone());
        let label = gtk::Label::new(Some(&format!("{}", source.current())));
        label.set_selectable(true);
        label.set_single_line_mode(true);
        label.connect_button_press_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(&format!("{:?}", spec));
                Inhibit(false)
            }),
        );
        label.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(&format!("{:?}", spec));
            Inhibit(false)
        }));
        Label { source, label }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.label.upcast_ref()
    }

    pub(super) fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.source.update(changed) {
            self.label.set_label(&format!("{}", new));
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        if self.source.update_var(name, value) {
            self.label.set_label(&format!("{}", value));
        }
    }
}
