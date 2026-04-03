use crate::{
    view, BCtx, BWidget, GxCallback, GxProp, ImageSpec, val_to_bool,
};
use arcstr::ArcStr;
use glib::{clone, Propagation};
use graphix_compiler::expr::ExprId;
use gtk::prelude::*;
use netidx::publisher::Value;
use netidx::protocol::valarray::ValArray;
use std::{cell::RefCell, rc::Rc};

// ---- BScript (raw expression display) ----

pub(crate) struct BScript {
    label: gtk::Label,
    prop: Option<GxProp>,
}

impl BScript {
    pub(crate) fn new(ctx: &BCtx, spec: view::GxExpr) -> Self {
        let label = gtk::Label::new(None);
        label.set_selectable(true);
        let prop = GxProp::compile(ctx, &spec);
        BScript { label, prop }
    }
}

impl BWidget for BScript {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        if let Some(ref mut p) = self.prop {
            if p.update(id, value) {
                self.label.set_text(&format!("{}", crate::WVal(value)));
                return true;
            }
        }
        false
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.label.upcast_ref())
    }
}

// ---- Label ----

pub(crate) struct Label {
    label: gtk::Label,
    text: Option<GxProp>,
    width: Option<GxProp>,
    ellipsize: Option<GxProp>,
    selectable: Option<GxProp>,
    single_line: Option<GxProp>,
}

impl Label {
    pub(crate) fn new(ctx: &BCtx, spec: view::Label) -> Self {
        let label = gtk::Label::new(None);
        Label {
            label,
            text: GxProp::compile(ctx, &spec.text),
            width: GxProp::compile(ctx, &spec.width),
            ellipsize: GxProp::compile(ctx, &spec.ellipsize),
            selectable: GxProp::compile(ctx, &spec.selectable),
            single_line: GxProp::compile(ctx, &spec.single_line),
        }
    }
}

impl BWidget for Label {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.text {
            if p.update(id, value) {
                self.label.set_text(&format!("{}", crate::WVal(value)));
                handled = true;
            }
        }
        if let Some(ref mut p) = self.width {
            if p.update(id, value) {
                if let Ok(w) = value.clone().cast_to::<i32>() {
                    self.label.set_max_width_chars(w);
                    self.label.set_width_chars(w);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.ellipsize {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    let mode = match &**s {
                        "start" => pango::EllipsizeMode::Start,
                        "middle" => pango::EllipsizeMode::Middle,
                        "end" => pango::EllipsizeMode::End,
                        _ => pango::EllipsizeMode::None,
                    };
                    self.label.set_ellipsize(mode);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.selectable {
            if p.update(id, value) {
                self.label.set_selectable(val_to_bool(value));
                handled = true;
            }
        }
        if let Some(ref mut p) = self.single_line {
            if p.update(id, value) {
                self.label.set_single_line_mode(val_to_bool(value));
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.label.upcast_ref())
    }
}

// ---- Button ----

pub(crate) struct Button {
    button: gtk::Button,
    label_prop: Option<GxProp>,
    image_prop: Option<GxProp>,
    on_click: Rc<RefCell<Option<GxCallback>>>,
}

impl Button {
    pub(crate) fn new(ctx: &BCtx, spec: view::Button) -> Self {
        let button = gtk::Button::new();
        let on_click = Rc::new(RefCell::new(GxCallback::compile(ctx, &spec.on_click)));
        button.connect_clicked(clone!(@strong on_click => move |_| {
            if let Some(ref cb) = *on_click.borrow() {
                cb.fire(ValArray::from([Value::Null]));
            }
        }));
        Button {
            button,
            label_prop: GxProp::compile(ctx, &spec.label),
            image_prop: GxProp::compile(ctx, &spec.image),
            on_click,
        }
    }
}

impl BWidget for Button {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.label_prop {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.button.set_label(&*s);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.image_prop {
            if p.update(id, value) {
                if let Ok(spec) = value.clone().cast_to::<ImageSpec>() {
                    self.button.set_image(Some(&spec.get()));
                    self.button.set_always_show_image(true);
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_click.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
    }
}

// ---- LinkButton ----

pub(crate) struct LinkButton {
    button: gtk::LinkButton,
    uri: Option<GxProp>,
    label_prop: Option<GxProp>,
    on_activate_link: Rc<RefCell<Option<GxCallback>>>,
}

impl LinkButton {
    pub(crate) fn new(ctx: &BCtx, spec: view::LinkButton) -> Self {
        let button = gtk::LinkButton::new("");
        let on_activate_link = Rc::new(RefCell::new(
            GxCallback::compile(ctx, &spec.on_activate_link),
        ));
        button.connect_activate_link(clone!(@strong on_activate_link => move |btn| {
            if let Some(ref cb) = *on_activate_link.borrow() {
                let uri = btn.uri().unwrap_or_default();
                cb.fire(ValArray::from([Value::String(ArcStr::from(uri.as_str()))]));
                return Propagation::Stop;
            }
            Propagation::Proceed
        }));
        LinkButton {
            button,
            uri: GxProp::compile(ctx, &spec.uri),
            label_prop: GxProp::compile(ctx, &spec.label),
            on_activate_link,
        }
    }
}

impl BWidget for LinkButton {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.uri {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.button.set_uri(&*s);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.label_prop {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.button.set_label(&*s);
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_activate_link.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
    }
}

// ---- Switch ----

pub(crate) struct Switch {
    switch: gtk::Switch,
    value: Option<GxProp>,
    on_change: Rc<RefCell<Option<GxCallback>>>,
}

impl Switch {
    pub(crate) fn new(ctx: &BCtx, spec: view::Switch) -> Self {
        let switch = gtk::Switch::new();
        let on_change = Rc::new(RefCell::new(GxCallback::compile(ctx, &spec.on_change)));
        switch.connect_state_set(clone!(@strong on_change => move |_, new_state| {
            if let Some(ref cb) = *on_change.borrow() {
                cb.fire(ValArray::from([Value::Bool(new_state)]));
            }
            Propagation::Proceed
        }));
        Switch {
            switch,
            value: GxProp::compile(ctx, &spec.value),
            on_change,
        }
    }
}

impl BWidget for Switch {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.value {
            if p.update(id, value) {
                let new_val = val_to_bool(value);
                if self.switch.is_active() != new_val {
                    self.switch.set_active(new_val);
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_change.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.switch.upcast_ref())
    }
}

// ---- ToggleButton ----

pub(crate) struct ToggleButton {
    button: gtk::ToggleButton,
    value: Option<GxProp>,
    label_prop: Option<GxProp>,
    image_prop: Option<GxProp>,
    on_change: Rc<RefCell<Option<GxCallback>>>,
}

impl ToggleButton {
    pub(crate) fn new<F: Fn() -> gtk::ToggleButton>(
        ctx: &BCtx,
        spec: view::ToggleButton,
        make_button: F,
    ) -> Self {
        let button = make_button();
        let on_change = Rc::new(RefCell::new(
            GxCallback::compile(ctx, &spec.toggle.on_change),
        ));
        button.connect_toggled(clone!(@strong on_change => move |btn| {
            if let Some(ref cb) = *on_change.borrow() {
                cb.fire(ValArray::from([Value::Bool(btn.is_active())]));
            }
        }));
        ToggleButton {
            button,
            value: GxProp::compile(ctx, &spec.toggle.value),
            label_prop: GxProp::compile(ctx, &spec.label),
            image_prop: GxProp::compile(ctx, &spec.image),
            on_change,
        }
    }
}

impl BWidget for ToggleButton {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.value {
            if p.update(id, value) {
                self.button.set_active(val_to_bool(value));
                handled = true;
            }
        }
        if let Some(ref mut p) = self.label_prop {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.button.set_label(&*s);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.image_prop {
            if p.update(id, value) {
                if let Ok(spec) = value.clone().cast_to::<ImageSpec>() {
                    self.button.set_image(Some(&spec.get()));
                    self.button.set_always_show_image(true);
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_change.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
    }
}

// ---- ComboBox ----

pub(crate) struct ComboBox {
    combo: gtk::ComboBoxText,
    choices: Option<GxProp>,
    selected: Option<GxProp>,
    on_change: Rc<RefCell<Option<GxCallback>>>,
}

impl ComboBox {
    pub(crate) fn new(ctx: &BCtx, spec: view::ComboBox) -> Self {
        let combo = gtk::ComboBoxText::new();
        let on_change = Rc::new(RefCell::new(GxCallback::compile(ctx, &spec.on_change)));
        combo.connect_changed(clone!(@strong on_change => move |cb| {
            if let Some(ref callback) = *on_change.borrow() {
                if let Some(id) = cb.active_id() {
                    callback.fire(ValArray::from(
                        [Value::String(ArcStr::from(id.as_str()))],
                    ));
                }
            }
        }));
        ComboBox {
            combo,
            choices: GxProp::compile(ctx, &spec.choices),
            selected: GxProp::compile(ctx, &spec.selected),
            on_change,
        }
    }
}

impl BWidget for ComboBox {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.choices {
            if p.update(id, value) {
                // TODO: parse choices and update combo
                handled = true;
            }
        }
        if let Some(ref mut p) = self.selected {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.combo.set_active_id(Some(&*s));
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_change.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.combo.upcast_ref())
    }
}

// ---- RadioButton ----

pub(crate) struct RadioButton {
    button: gtk::RadioButton,
    label_prop: Option<GxProp>,
    value: Option<GxProp>,
    on_toggled: Rc<RefCell<Option<GxCallback>>>,
}

impl RadioButton {
    pub(crate) fn new(ctx: &BCtx, spec: view::RadioButton) -> Self {
        let button = gtk::RadioButton::new();
        let on_toggled = Rc::new(RefCell::new(
            GxCallback::compile(ctx, &spec.on_toggled),
        ));
        button.connect_toggled(clone!(@strong on_toggled => move |btn| {
            if let Some(ref cb) = *on_toggled.borrow() {
                cb.fire(ValArray::from([Value::Bool(btn.is_active())]));
            }
        }));
        RadioButton {
            button,
            label_prop: GxProp::compile(ctx, &spec.label),
            value: GxProp::compile(ctx, &spec.value),
            on_toggled,
        }
    }
}

impl BWidget for RadioButton {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.label_prop {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.button.set_label(&*s);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.value {
            if p.update(id, value) {
                self.button.set_active(val_to_bool(value));
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_toggled.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
    }
}

// ---- Entry ----

pub(crate) struct Entry {
    entry: gtk::Entry,
    text: Option<GxProp>,
    on_change: Rc<RefCell<Option<GxCallback>>>,
    on_activate: Rc<RefCell<Option<GxCallback>>>,
}

impl Entry {
    pub(crate) fn new(ctx: &BCtx, spec: view::Entry) -> Self {
        let entry = gtk::Entry::new();
        let on_change = Rc::new(RefCell::new(GxCallback::compile(ctx, &spec.on_change)));
        let on_activate = Rc::new(RefCell::new(
            GxCallback::compile(ctx, &spec.on_activate),
        ));
        entry.connect_changed(clone!(@strong on_change => move |e| {
            if let Some(ref cb) = *on_change.borrow() {
                let text = e.text();
                cb.fire(ValArray::from(
                    [Value::String(ArcStr::from(text.as_str()))],
                ));
            }
        }));
        entry.connect_activate(clone!(@strong on_activate => move |e| {
            if let Some(ref cb) = *on_activate.borrow() {
                let text = e.text();
                cb.fire(ValArray::from(
                    [Value::String(ArcStr::from(text.as_str()))],
                ));
            }
        }));
        Entry {
            entry,
            text: GxProp::compile(ctx, &spec.text),
            on_change,
            on_activate,
        }
    }
}

impl BWidget for Entry {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.text {
            if p.update(id, value) {
                let new_text = format!("{}", crate::WVal(value));
                // Avoid re-setting text if GTK already shows this value
                // (prevents loop when on_change lambda sets a Ref that
                // flows back through the text expression)
                if self.entry.text().as_str() != new_text {
                    self.entry.set_text(&new_text);
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_change.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_activate.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.entry.upcast_ref())
    }
}

// ---- SearchEntry ----

pub(crate) struct SearchEntry {
    entry: gtk::SearchEntry,
    text: Option<GxProp>,
    on_search_changed: Rc<RefCell<Option<GxCallback>>>,
    on_activate: Rc<RefCell<Option<GxCallback>>>,
}

impl SearchEntry {
    pub(crate) fn new(ctx: &BCtx, spec: view::SearchEntry) -> Self {
        let entry = gtk::SearchEntry::new();
        let on_search_changed = Rc::new(RefCell::new(
            GxCallback::compile(ctx, &spec.on_search_changed),
        ));
        let on_activate = Rc::new(RefCell::new(
            GxCallback::compile(ctx, &spec.on_activate),
        ));
        entry.connect_search_changed(clone!(@strong on_search_changed => move |e| {
            if let Some(ref cb) = *on_search_changed.borrow() {
                let text = e.text();
                cb.fire(ValArray::from(
                    [Value::String(ArcStr::from(text.as_str()))],
                ));
            }
        }));
        entry.connect_activate(clone!(@strong on_activate => move |e| {
            if let Some(ref cb) = *on_activate.borrow() {
                let text = e.text();
                cb.fire(ValArray::from(
                    [Value::String(ArcStr::from(text.as_str()))],
                ));
            }
        }));
        SearchEntry {
            entry,
            text: GxProp::compile(ctx, &spec.text),
            on_search_changed,
            on_activate,
        }
    }
}

impl BWidget for SearchEntry {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.text {
            if p.update(id, value) {
                self.entry.set_text(&format!("{}", crate::WVal(value)));
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_search_changed.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_activate.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.entry.upcast_ref())
    }
}

// ---- ProgressBar ----

pub(crate) struct ProgressBar {
    bar: gtk::ProgressBar,
    fraction: Option<GxProp>,
    text: Option<GxProp>,
    pulse: Option<GxProp>,
}

impl ProgressBar {
    pub(crate) fn new(ctx: &BCtx, spec: view::ProgressBar) -> Self {
        let bar = gtk::ProgressBar::new();
        ProgressBar {
            bar,
            fraction: GxProp::compile(ctx, &spec.fraction),
            text: GxProp::compile(ctx, &spec.text),
            pulse: GxProp::compile(ctx, &spec.pulse),
        }
    }
}

impl BWidget for ProgressBar {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.fraction {
            if p.update(id, value) {
                if let Ok(f) = value.clone().cast_to::<f64>() {
                    self.bar.set_fraction(f);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.text {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.bar.set_text(Some(&*s));
                    self.bar.set_show_text(true);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.pulse {
            if p.update(id, value) {
                self.bar.pulse();
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.bar.upcast_ref())
    }
}

// ---- Scale ----

pub(crate) struct Scale {
    scale: gtk::Scale,
    value: Option<GxProp>,
    min: Option<GxProp>,
    max: Option<GxProp>,
    on_change: Rc<RefCell<Option<GxCallback>>>,
}

impl Scale {
    pub(crate) fn new(ctx: &BCtx, spec: view::Scale) -> Self {
        let orientation = match spec.direction {
            view::Direction::Horizontal => gtk::Orientation::Horizontal,
            view::Direction::Vertical => gtk::Orientation::Vertical,
        };
        let scale = gtk::Scale::with_range(orientation, 0.0, 1.0, 0.01);
        let on_change = Rc::new(RefCell::new(GxCallback::compile(ctx, &spec.on_change)));
        scale.connect_value_changed(clone!(@strong on_change => move |s| {
            if let Some(ref cb) = *on_change.borrow() {
                cb.fire(ValArray::from([Value::F64(s.value())]));
            }
        }));
        Scale {
            scale,
            value: GxProp::compile(ctx, &spec.value),
            min: GxProp::compile(ctx, &spec.min),
            max: GxProp::compile(ctx, &spec.max),
            on_change,
        }
    }
}

impl BWidget for Scale {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.value {
            if p.update(id, value) {
                if let Ok(f) = value.clone().cast_to::<f64>() {
                    // Avoid re-setting if value hasn't actually changed
                    if (self.scale.value() - f).abs() > f64::EPSILON {
                        self.scale.set_value(f);
                    }
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.min {
            if p.update(id, value) {
                if let Ok(f) = value.clone().cast_to::<f64>() {
                    let adj = self.scale.adjustment();
                    adj.set_lower(f);
                }
                handled = true;
            }
        }
        if let Some(ref mut p) = self.max {
            if p.update(id, value) {
                if let Ok(f) = value.clone().cast_to::<f64>() {
                    let adj = self.scale.adjustment();
                    adj.set_upper(f);
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_change.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.scale.upcast_ref())
    }
}

// ---- Image ----

pub(crate) struct Image {
    event_box: gtk::EventBox,
    image: gtk::Image,
    spec: Option<GxProp>,
    on_click: Rc<RefCell<Option<GxCallback>>>,
}

impl Image {
    pub(crate) fn new(ctx: &BCtx, spec: view::Image) -> Self {
        let image = gtk::Image::new();
        let event_box = gtk::EventBox::new();
        event_box.add(&image);
        let on_click = Rc::new(RefCell::new(GxCallback::compile(ctx, &spec.on_click)));
        event_box.connect_button_press_event(
            clone!(@strong on_click => move |_, _| {
                if let Some(ref cb) = *on_click.borrow() {
                    cb.fire(ValArray::from([Value::Null]));
                    return Propagation::Stop;
                }
                Propagation::Proceed
            }),
        );
        Image {
            event_box,
            image,
            spec: GxProp::compile(ctx, &spec.spec),
            on_click,
        }
    }
}

impl BWidget for Image {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.spec {
            if p.update(id, value) {
                if let Ok(spec) = value.clone().cast_to::<ImageSpec>() {
                    spec.apply(&self.image);
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_click.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.event_box.upcast_ref())
    }
}
