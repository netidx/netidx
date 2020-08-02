use super::{val_to_bool, WidgetCtx, Source, Sink};
use crate::browser::view;
use glib::clone;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use std::{collections::HashMap, sync::Arc};

pub(super) struct Button {
    enabled: Source,
    label: Source,
    source: Source,
    button: gtk::Button,
}

impl Button {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Button,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::Button::new();
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let label = Source::new(&ctx, variables, spec.label.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        button.set_sensitive(val_to_bool(&enabled.current()));
        button.set_label(&format!("{}", label.current()));
        button.connect_clicked(clone!(@strong ctx, @strong source, @strong sink =>
        move |_| {
            sink.set(&ctx, source.current());
        }));
        button.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("source: {:?}, sink: {:?}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        button.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("source: {:?}, sink: {:?}", spec.source, spec.sink)
                );
                Inhibit(false)
            }),
        );
        Button { enabled, label, source, button }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.button.upcast_ref()
    }

    pub(super) fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.enabled.update(changed) {
            self.button.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.label.update(changed) {
            self.button.set_label(&format!("{}", new));
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        if self.enabled.update_var(name, value) {
            self.button.set_sensitive(val_to_bool(value));
        }
        if self.label.update_var(name, value) {
            self.button.set_label(&format!("{}", value));
        }
        self.source.update_var(name, value);
    }
}
