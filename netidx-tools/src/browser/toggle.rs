use super::{val_to_bool, WidgetCtx, Source, Sink};
use crate::browser::view;
use glib::clone;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use std::{collections::HashMap, sync::Arc, rc::Rc, cell::Cell};

pub(super) struct Toggle {
    enabled: Source,
    source: Source,
    we_set: Rc<Cell<bool>>,
    switch: gtk::Switch,
}

impl Toggle {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Toggle,
        selected_path: gtk::Label,
    ) -> Self {
        let switch = gtk::Switch::new();
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        let we_set = Rc::new(Cell::new(false));
        switch.set_sensitive(val_to_bool(&enabled.current()));
        switch.set_active(val_to_bool(&source.current()));
        switch.set_state(val_to_bool(&source.current()));
        switch.connect_state_set(clone!(
        @strong ctx, @strong sink, @strong we_set, @strong source =>
        move |switch, state| {
            if !we_set.get() {
                sink.set(&ctx, if state { Value::True } else { Value::False });
                idle_add(clone!(@strong source, @strong switch, @strong we_set => move || {
                    we_set.set(true);
                    switch.set_active(val_to_bool(&source.current()));
                    switch.set_state(val_to_bool(&source.current()));
                    we_set.set(false);
                    Continue(false)
                }));
            }
            Inhibit(true)
        }));
        switch.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("source: {:?}, sink: {:?}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        switch.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("source: {:?}, sink: {:?}", spec.source, spec.sink)
                );
                Inhibit(false)
            }),
        );
        Toggle { enabled, source, switch, we_set }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.switch.upcast_ref()
    }

    pub(super) fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.enabled.update(changed) {
            self.switch.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.source.update(changed) {
            self.we_set.set(true);
            self.switch.set_active(val_to_bool(&new));
            self.switch.set_state(val_to_bool(&new));
            self.we_set.set(false);
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        if self.enabled.update_var(name, value) {
            self.switch.set_sensitive(val_to_bool(value));
        }
        if self.source.update_var(name, value) {
            self.we_set.set(true);
            self.switch.set_active(val_to_bool(value));
            self.switch.set_state(val_to_bool(value));
            self.we_set.set(false);
        }
    }
}
