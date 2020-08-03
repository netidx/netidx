use super::{val_to_bool, WidgetCtx, Source, Sink};
use crate::browser::view;
use log::warn;
use glib::clone;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use std::{collections::HashMap, sync::Arc, rc::Rc, cell::Cell};

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

pub(super) struct Selector {
    root: gtk::EventBox,
    combo: gtk::ComboBoxText,
    enabled: Source,
    choices: Source,
    source: Source,
    we_set: Rc<Cell<bool>>,
}

impl Selector {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Selector,
        selected_path: gtk::Label,
    ) -> Self {
        let combo = gtk::ComboBoxText::new();
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
        Selector::update_choices(&combo, &choices.current(), &source.current());
        we_set.set(true);
        Selector::update_active(&combo, &source.current());
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
                        Selector::update_active(&combo, &source.current());
                        we_set.set(false);
                        Continue(false)
                    })
                );
            }
        }));
        Selector { root, combo, enabled, choices, source, we_set }
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
        Selector::update_active(combo, source)
    }

    pub(super) fn update(&self, updates: &Arc<IndexMap<SubId, Value>>) {
        self.we_set.set(true);
        if let Some(new) = self.enabled.update(updates) {
            self.combo.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.source.update(updates) {
            Selector::update_active(&self.combo, &new);
        }
        if let Some(new) = self.choices.update(updates) {
            Selector::update_choices(&self.combo, &new, &self.source.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        self.we_set.set(true);
        if self.enabled.update_var(name, value) {
            self.combo.set_sensitive(val_to_bool(value));
        }
        if self.source.update_var(name, value) {
            Selector::update_active(&self.combo, value);
        }
        if self.choices.update_var(name, value) {
            Selector::update_choices(&self.combo, value, &self.source.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

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
