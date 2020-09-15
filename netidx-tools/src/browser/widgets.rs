use super::{val_to_bool, Sink, Source, WidgetCtx};
use crate::browser::view;
use gdk::{self, prelude::*};
use glib::{clone, idle_add_local};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use log::warn;
use netidx::{
    chars::Chars,
    subscriber::{SubId, Value},
};
use std::{cell::Cell, collections::HashMap, rc::Rc, sync::Arc};

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
        if let Some(v) = enabled.current() {
            button.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = label.current() {
            button.set_label(&format!("{}", v));
        }
        button.connect_clicked(clone!(@strong ctx, @strong source, @strong sink =>
        move |_| {
            if let Some(v) = source.current() {
                sink.set(&ctx, v);
            }
        }));
        button.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("source: {}, sink: {}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        button.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("source: {}, sink: {}", spec.source, spec.sink)
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
        if let Some(value) = self.enabled.update_var(name, value) {
            self.button.set_sensitive(val_to_bool(&value));
        }
        if let Some(value) = self.label.update_var(name, value) {
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
        let txt = match source.current() {
            None => String::new(),
            Some(v) => format!("{}", v),
        };
        let label = gtk::Label::new(Some(txt.as_str()));
        label.set_selectable(true);
        label.set_single_line_mode(true);
        label.connect_button_press_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(&format!("{}", spec));
                Inhibit(false)
            }),
        );
        label.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(&format!("{}", spec));
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
        if let Some(value) = self.source.update_var(name, value) {
            self.label.set_label(&format!("{}", value));
        }
    }
}

pub(super) struct Action {
    source: Source,
    sink: Sink,
    ctx: WidgetCtx,
}

impl Action {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Action,
    ) -> Self {
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        if let Some(cur) = source.current() {
            sink.set(&ctx, cur);
        }
        Action { source, sink, ctx }
    }

    pub(super) fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.source.update(changed) {
            self.sink.set(&self.ctx, new);
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        if let Some(value) = self.source.update_var(name, value) {
            self.sink.set(&self.ctx, value);
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
                    "source: {}, sink: {}, choices: {}",
                    spec.source, spec.sink, spec.choices
                )
            );
            Inhibit(false)
        }));
        root.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!(
                        "source: {}, sink: {}, choices: {}",
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
        if let Some(v) = enabled.current() {
            combo.set_sensitive(val_to_bool(&v));
        }
        if let Some(choices) = choices.current() {
            Selector::update_choices(&combo, &choices, &source.current());
        }
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
                idle_add_local(clone!(
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

    fn update_active(combo: &gtk::ComboBoxText, source: &Option<Value>) {
        if let Some(source) = source {
            if let Ok(current) = serde_json::to_string(source) {
                combo.set_active_id(Some(current.as_str()));
            }
        }
    }

    fn update_choices(
        combo: &gtk::ComboBoxText,
        choices: &Value,
        source: &Option<Value>,
    ) {
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
        Selector::update_active(&self.combo, &self.source.update(updates));
        if let Some(new) = self.choices.update(updates) {
            Selector::update_choices(&self.combo, &new, &self.source.current());
        }
        self.we_set.set(false);
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        self.we_set.set(true);
        if let Some(value) = self.enabled.update_var(name, value) {
            self.combo.set_sensitive(val_to_bool(&value));
        }
        Selector::update_active(&self.combo, &self.source.update_var(name, value));
        if let Some(value) = self.choices.update_var(name, value) {
            Selector::update_choices(&self.combo, &value, &self.source.current());
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
        if let Some(v) = enabled.current() {
            switch.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = source.current() {
            let v = val_to_bool(&v);
            switch.set_active(v);
            switch.set_state(v);
        }
        switch.connect_state_set(clone!(
        @strong ctx, @strong sink, @strong we_set, @strong source =>
        move |switch, state| {
            if !we_set.get() {
                sink.set(&ctx, if state { Value::True } else { Value::False });
                idle_add_local(
                    clone!(@strong source, @strong switch, @strong we_set => move || {
                    we_set.set(true);
                    if let Some(v) = source.current() {
                        let v = val_to_bool(&v);
                        switch.set_active(v);
                        switch.set_state(v);
                    }
                    we_set.set(false);
                    Continue(false)
                }));
            }
            Inhibit(true)
        }));
        switch.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("source: {}, sink: {}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        switch.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("source: {}, sink: {}", spec.source, spec.sink)
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
        if let Some(value) = self.enabled.update_var(name, value) {
            self.switch.set_sensitive(val_to_bool(&value));
        }
        if let Some(value) = self.source.update_var(name, value) {
            self.we_set.set(true);
            self.switch.set_active(val_to_bool(&value));
            self.switch.set_state(val_to_bool(&value));
            self.we_set.set(false);
        }
    }
}

pub(super) struct Entry {
    entry: gtk::Entry,
    enabled: Source,
    visible: Source,
    source: Source,
}

impl Entry {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Entry,
        selected_path: gtk::Label,
    ) -> Self {
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let visible = Source::new(&ctx, variables, spec.visible.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        let entry = gtk::Entry::new();
        if let Some(v) = enabled.current() {
            entry.set_sensitive(val_to_bool(&v));
        }
        if let Some(v) = visible.current() {
            entry.set_visibility(val_to_bool(&v));
        }
        match source.current() {
            None => entry.set_text(""),
            Some(Value::String(s)) => entry.set_text(&*s),
            Some(v) => entry.set_text(&format!("{}", v)),
        }
        fn submit(ctx: &WidgetCtx, source: &Source, sink: &Sink, entry: &gtk::Entry) {
            sink.set(&ctx, Value::String(Chars::from(String::from(entry.get_text()))));
            idle_add_local(clone!(@strong source, @strong entry => move || {
                match source.current() {
                    None => entry.set_text(""),
                    Some(Value::String(s)) => entry.set_text(&*s),
                    Some(v) => entry.set_text(&format!("{}", v)),
                }
                Continue(false)
            }));
        }
        entry.connect_activate(
            clone!(@strong source, @strong ctx, @strong sink => move |entry| {
                submit(&ctx, &source, &sink, entry)
            }),
        );
        entry.connect_focus_out_event(
            clone!(@strong source, @strong ctx, @strong sink => move |w, _| {
                let entry = w.downcast_ref::<gtk::Entry>().unwrap();
                submit(&ctx, &source, &sink, entry);
                Inhibit(false)
            }),
        );
        entry.connect_focus(clone!(@strong spec, @strong selected_path => move |_, _| {
            selected_path.set_label(
                &format!("source: {}, sink: {}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        // CR estokes: give the user an indication that it's out of sync
        Entry { entry, enabled, visible, source }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.entry.upcast_ref()
    }

    pub(super) fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.enabled.update(changed) {
            self.entry.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.visible.update(changed) {
            self.entry.set_visibility(val_to_bool(&new));
        }
        if let Some(new) = self.source.update(changed) {
            match new {
                Value::String(s) => self.entry.set_text(&*s),
                v => self.entry.set_text(&format!("{}", v)),
            }
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        if let Some(value) = self.enabled.update_var(name, value) {
            self.entry.set_sensitive(val_to_bool(&value));
        }
        if let Some(value) = self.visible.update_var(name, value) {
            self.entry.set_visibility(val_to_bool(&value));
        }
        if let Some(value) = self.source.update_var(name, value) {
            match value {
                Value::String(s) => self.entry.set_text(&*s),
                v => self.entry.set_text(&format!("{}", v)),
            }
        }
    }
}
