use super::{val_to_bool, Sink, Source, WidgetCtx};
use crate::browser::view;
use gdk::{self, prelude::*};
use glib::{self, clone, signal::Inhibit};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use log::warn;
use netidx::subscriber::{SubId, Value};
use std::{cell::Cell, collections::HashMap, rc::Rc, sync::Arc};

pub(super) struct ComboBox {
    root: gtk::EventBox,
    combo: gtk::ComboBoxText,
    enabled: Source,
    choices: Source,
    source: Source,
    we_set: Rc<Cell<bool>>,
}

impl ComboBox {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::ComboBox,
        selected_path: gtk::Label,
    ) -> Self {
        let combo = if spec.has_entry {
            gtk::ComboBoxText::with_entry()
        } else {
            gtk::ComboBoxText::new()
        };
        let root = gtk::EventBox::new();
        root.add(&combo);
        combo.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!(
                    "source: {:?}, sink: {:?}, choices: {:?}",
                    spec.source, spec.sink, spec.choices
                )
            );
            Inhibit(false)
        }));
        root.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!(
                        "source: {:?}, sink: {:?}, choices: {:?}",
                        spec.source, spec.sink, spec.choices
                    )
                );
                Inhibit(false)
            }),
        );
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let choices = Source::new(&ctx, variables, spec.choices.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        let we_set = Rc::new(Cell::new(false));
        combo.set_sensitive(val_to_bool(&enabled.current()));
        ComboBox::update_choices(&combo, &choices.current(), &source.current());
        we_set.set(true);
        ComboBox::update_active(&combo, &source.current());
        we_set.set(false);
        combo.connect_changed(clone!(
            @strong we_set, @strong sink, @strong ctx, @strong source => move |combo| {
            if !we_set.get() {
                if let Some(id) = combo.get_active_id() {
                    if let Ok(idv) = serde_json::from_str::<Value>(id.as_str()) {
                        sink.set(&ctx, idv);
                    }
                }
                idle_add(clone!(
                    @strong source, @strong combo, @strong we_set => move || {
                        we_set.set(true);
                        ComboBox::update_active(&combo, &source.current());
                        we_set.set(false);
                        Continue(false)
                    })
                );
            }
        }));
        ComboBox { root, combo, enabled, choices, source, we_set }
    }

    fn update_active(combo: &gtk::ComboBoxText, source: &Value) {
        if let Ok(current) = serde_json::to_string(source) {
            combo.set_active_id(Some(current.as_str()));
        }
    }

    fn update_choices(combo: &gtk::ComboBoxText, choices: &Value, source: &Value) {
        let choices = match choices {
            Value::String(s) => {
                match serde_json::from_str::<Vec<(Value, String)>>(&**s) {
                    Ok(choices) => choices,
                    Err(e) => {
                        warn!(
                            "failed to parse combo choices, source {:?}, {}",
                            choices, e
                        );
                        vec![]
                    }
                }
            }
            v => {
                warn!("combo choices wrong type, expected json string not {:?}", v);
                vec![]
            }
        };
        combo.remove_all();
        for (id, val) in choices {
            if let Ok(id) = serde_json::to_string(&id) {
                combo.append(Some(id.as_str()), val.as_str());
            }
        }
        ComboBox::update_active(combo, source)
    }

    pub(super) fn update(&self, updates: &Arc<IndexMap<SubId, Value>>) {
        self.we_set.set(true);
        if let Some(new) = self.enabled.update(updates) {
            self.combo.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.source.update(updates) {
            ComboBox::update_active(&self.combo, &new);
        }
        if let Some(new) = self.choices.update(updates) {
            ComboBox::update_choices(&self.combo, &new, &self.source.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        self.we_set.set(true);
        if self.enabled.update_var(name, value) {
            self.combo.set_sensitive(val_to_bool(value));
        }
        if self.source.update_var(name, value) {
            ComboBox::update_active(&self.combo, value);
        }
        if self.choices.update_var(name, value) {
            ComboBox::update_choices(&self.combo, value, &self.source.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
