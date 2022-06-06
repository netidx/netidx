use super::{val_to_bool, BSCtx, BSCtxRef, BSNode, BWidget, WVal, WidgetPath, util};
use crate::{bscript::LocalEvent, containers, view};
use anyhow::{anyhow, bail, Result};
use bytes::Bytes;

use futures::channel::oneshot;
use gdk::{self, prelude::*};
use glib::{clone, idle_add_local};
use gtk::{self, prelude::*};
use log::warn;
use netidx::{chars::Chars, path::Path, protocol::value::FromValue, subscriber::Value};
use netidx_bscript::{expr::Expr, vm};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    str::FromStr,
};

#[derive(Debug)]
enum ImageSpec {
    Icon { name: Chars, size: gtk::IconSize },
    PixBuf { bytes: Bytes, width: Option<u32>, height: Option<u32>, keep_aspect: bool },
}

impl ImageSpec {
    fn get(&self) -> gtk::Image {
        let image = gtk::Image::new();
        self.apply(&image);
        image
    }

    // apply the spec to an existing image
    fn apply(&self, image: &gtk::Image) {
        match self {
            Self::Icon { name, size } => image.set_from_icon_name(Some(&**name), *size),
            Self::PixBuf { bytes, width, height, keep_aspect } => {
                let width = width.map(|i| i as i32).unwrap_or(-1);
                let height = height.map(|i| i as i32).unwrap_or(-1);
                let bytes = glib::Bytes::from_owned(bytes.clone());
                let stream = gio::MemoryInputStream::from_bytes(&bytes);
                let pixbuf = gdk_pixbuf::Pixbuf::from_stream_at_scale(
                    &stream,
                    width,
                    height,
                    *keep_aspect,
                    gio::Cancellable::NONE,
                )
                .ok();
                image.set_from_pixbuf(pixbuf.as_ref())
            }
        }
    }
}

impl FromValue for ImageSpec {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(name) => {
                Ok(Self::Icon { name, size: gtk::IconSize::SmallToolbar })
            }
            Value::Bytes(bytes) => {
                Ok(Self::PixBuf { bytes, width: None, height: None, keep_aspect: true })
            }
            Value::Array(elts) => match &*elts {
                [Value::String(name), Value::String(size)] => {
                    let size = match &**size {
                        "menu" => gtk::IconSize::Menu,
                        "small-toolbar" => gtk::IconSize::SmallToolbar,
                        "large-toolbar" => gtk::IconSize::LargeToolbar,
                        "dnd" => gtk::IconSize::Dnd,
                        "dialog" => gtk::IconSize::Dialog,
                        _ => bail!("invalid size"),
                    };
                    Ok(Self::Icon { name: name.clone(), size })
                }
                _ => {
                    let mut alist =
                        Value::Array(elts).cast_to::<HashMap<Chars, Value>>()?;
                    let bytes = alist
                        .remove("image")
                        .ok_or_else(|| anyhow!("missing bytes"))?
                        .cast_to::<Bytes>()?;
                    let width =
                        alist.remove("width").and_then(|v| v.cast_to::<u32>().ok());
                    let height =
                        alist.remove("height").and_then(|v| v.cast_to::<u32>().ok());
                    let keep_aspect = alist
                        .remove("keep-aspect")
                        .and_then(|v| v.cast_to::<bool>().ok())
                        .unwrap_or(true);
                    Ok(Self::PixBuf { bytes, width, height, keep_aspect })
                }
            },
            _ => bail!("expected bytes or array"),
        }
    }

    fn get(v: Value) -> Option<Self> {
        <Self as FromValue>::from_value(v).ok()
    }
}

fn parse_ellipsize(e: Value) -> pango::EllipsizeMode {
    match e.cast_to::<Chars>().ok() {
        Some(c) if &*c == "none" => pango::EllipsizeMode::None,
        Some(c) if &*c == "start" => pango::EllipsizeMode::Start,
        Some(c) if &*c == "middle" => pango::EllipsizeMode::Middle,
        Some(c) if &*c == "end" => pango::EllipsizeMode::End,
        None | Some(_) => pango::EllipsizeMode::None,
    }
}

fn hover_path(
    w: &impl WidgetExt,
    selected_path: &gtk::Label,
    name: &'static str,
    expr: &Expr,
) {
    w.connect_focus(clone!(@strong selected_path, @strong expr => move |_, _| {
        selected_path.set_label(&format!("{}: {}", name, expr));
        Inhibit(false)
    }));
    w.connect_enter_notify_event(
        clone!(@strong selected_path, @strong expr => move |_, _| {
            selected_path.set_label(&format!("{}: {}", name, expr));
            Inhibit(false)
        }),
    );
}

pub(super) struct Button {
    label: BSNode,
    image: BSNode,
    on_click: Rc<RefCell<BSNode>>,
    button: gtk::Button,
}

impl Button {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Button,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::Button::new();
        button.set_no_show_all(true);
        let (label, image, on_click) = {
            let mut ctx = ctx.borrow_mut();
            let ctx = &mut ctx;
            let label = BSNode::compile(ctx, scope.clone(), spec.label.clone());
            let image = BSNode::compile(ctx, scope.clone(), spec.image.clone());
            let on_click =
                Rc::new(RefCell::new(BSNode::compile(ctx, scope, spec.on_click.clone())));
            (label, image, on_click)
        };
        Self::set_label(&button, label.current());
        Self::set_image(&button, image.current());
        hover_path(&button, &selected_path, "on_click", &spec.on_click);
        button.connect_clicked(clone!(@strong ctx, @strong on_click => move |_| {
            on_click.borrow_mut().update(
                &mut *ctx.borrow_mut(),
                &vm::Event::User(LocalEvent::Event(Value::Null))
            );
        }));
        Self { label, image, on_click, button }
    }

    fn set_label(button: &gtk::Button, value: Option<Value>) {
        if let Some(v) = value {
            button.set_label(&format!("{}", WVal(&v)))
        }
    }

    fn set_image(button: &gtk::Button, value: Option<Value>) {
        if let Some(s) = value.and_then(|v| v.cast_to::<ImageSpec>().ok()) {
            match button.image() {
                Some(image) if image.is::<gtk::Image>() => {
                    s.apply(image.downcast_ref().unwrap())
                }
                Some(_) | None => {
                    button.set_image(Some(&s.get()));
                    button.set_always_show_image(true);
                }
            }
        }
    }
}

impl BWidget for Button {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::set_label(&self.button, self.label.update(ctx, event));
        Self::set_image(&self.button, self.image.update(ctx, event));
        self.on_click.borrow_mut().update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
    }
}

pub(super) struct LinkButton {
    uri: BSNode,
    label: BSNode,
    on_activate_link: Rc<RefCell<BSNode>>,
    button: gtk::LinkButton,
}

impl LinkButton {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::LinkButton,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let mut ctx_r = ctx.borrow_mut();
        let ctx_r = &mut ctx_r;
        let uri = BSNode::compile(ctx_r, scope.clone(), spec.uri.clone());
        let label = BSNode::compile(ctx_r, scope.clone(), spec.label.clone());
        let on_activate_link = Rc::new(RefCell::new(BSNode::compile(
            ctx_r,
            scope,
            spec.on_activate_link.clone(),
        )));
        let button = gtk::LinkButton::new("file:///");
        button.set_no_show_all(true);
        Self::set_uri(&button, uri.current());
        Self::set_label(&button, label.current());
        hover_path(&button, &selected_path, "on_activate_link", &spec.on_activate_link);
        button.connect_activate_link(
            clone!(@strong ctx, @strong on_activate_link => move |_| {
                let ev = vm::Event::User(LocalEvent::Event(Value::Null));
                match on_activate_link.borrow_mut().update(&mut *ctx.borrow_mut(), &ev) {
                    Some(Value::True) => Inhibit(true),
                    _ => Inhibit(false),
                }
            }),
        );
        LinkButton { uri, label, on_activate_link, button }
    }

    fn set_uri(button: &gtk::LinkButton, value: Option<Value>) {
        if let Some(new) = value.and_then(|v| v.cast_to::<Chars>().ok()) {
            button.set_uri(&new);
        }
    }

    fn set_label(button: &gtk::LinkButton, value: Option<Value>) {
        if let Some(new) = value.and_then(|v| v.cast_to::<Chars>().ok()) {
            button.set_label(&new);
        }
    }
}

impl BWidget for LinkButton {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::set_uri(&self.button, self.uri.update(ctx, event));
        Self::set_label(&self.button, self.label.update(ctx, event));
        self.on_activate_link.borrow_mut().update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
    }
}

pub(super) struct Label {
    label: gtk::Label,
    text: BSNode,
    width: BSNode,
    ellipsize: BSNode,
    single_line: BSNode,
    selectable: BSNode,
}

impl Label {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Label,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Label {
        let text =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.text.clone());
        let width =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.width.clone());
        let ellipsize = BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.ellipsize.clone(),
        );
        let single_line = BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.single_line.clone(),
        );
        let selectable =
            BSNode::compile(&mut *ctx.borrow_mut(), scope, spec.selectable.clone());
        let label = gtk::Label::new(None);
        label.set_no_show_all(true);
        Self::set_text(&label, text.current());
        Self::set_single_line(&label, single_line.current());
        Self::set_selectable(&label, selectable.current());
        Self::set_width(&label, width.current());
        Self::set_ellipsize(&label, ellipsize.current());
        hover_path(&label, &selected_path, "text", &spec.text);
        Label { text, label, width, ellipsize, single_line, selectable }
    }

    fn set_text(label: &gtk::Label, value: Option<Value>) {
        if let Some(txt) = value {
            label.set_label(&format!("{}", WVal(&txt)));
        }
    }

    fn set_width(label: &gtk::Label, value: Option<Value>) {
        if let Some(w) = value.and_then(|v| v.cast_to::<i32>().ok()) {
            label.set_width_chars(w);
        }
    }

    fn set_ellipsize(label: &gtk::Label, value: Option<Value>) {
        if let Some(mode) = value.map(parse_ellipsize) {
            label.set_ellipsize(mode);
        }
    }

    fn set_single_line(label: &gtk::Label, value: Option<Value>) {
        if let Some(mode) = value.and_then(|v| v.cast_to::<bool>().ok()) {
            label.set_single_line_mode(mode)
        }
    }

    fn set_selectable(label: &gtk::Label, value: Option<Value>) {
        if let Some(mode) = value.and_then(|v| v.cast_to::<bool>().ok()) {
            label.set_selectable(mode)
        }
    }
}

impl BWidget for Label {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::set_text(&self.label, self.text.update(ctx, event));
        Self::set_width(&self.label, self.width.update(ctx, event));
        Self::set_ellipsize(&self.label, self.ellipsize.update(ctx, event));
        Self::set_single_line(&self.label, self.single_line.update(ctx, event));
        Self::set_selectable(&self.label, self.selectable.update(ctx, event));
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.label.upcast_ref())
    }
}

pub(super) struct BScript {
    expr: BSNode,
}

impl BScript {
    pub(super) fn new(ctx: &BSCtx, scope: Path, spec: Expr) -> Self {
        let mut ctx_r = ctx.borrow_mut();
        let ctx_r = &mut ctx_r;
        let mut expr = BSNode::compile(ctx_r, scope, spec.clone());
        expr.update(ctx_r, &vm::Event::User(LocalEvent::Event(Value::Null)));
        Self { expr }
    }
}

impl BWidget for BScript {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        self.expr.update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        None
    }
}

pub(super) struct ToggleButton<T> {
    button: T,
    we_set: Rc<Cell<bool>>,
    value: Rc<RefCell<BSNode>>,
    label: BSNode,
    image: BSNode,
    on_change: Rc<RefCell<BSNode>>,
}

impl<T> ToggleButton<T>
where
    T: ToggleButtonExt
        + ButtonExt
        + WidgetExt
        + glib::ObjectType
        + IsA<gtk::Widget>
        + 'static,
{
    pub(super) fn new<F: FnOnce() -> T>(
        ctx: &BSCtx,
        spec: view::ToggleButton,
        scope: Path,
        selected_path: gtk::Label,
        new: F,
    ) -> Self {
        let button = new();
        button.set_no_show_all(true);
        let we_set = Rc::new(Cell::new(false));
        let value = Rc::new(RefCell::new(BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.toggle.value.clone(),
        )));
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.toggle.on_change.clone(),
        )));
        let label =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.label.clone());
        let image =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.image.clone());
        Self::set_label(&button, label.current());
        Self::set_image(&button, image.current());
        Self::we_set_value(&we_set, &button, value.borrow().current());
        hover_path(&button, &selected_path, "on_change", &spec.toggle.on_change);
        button.connect_toggled(clone!(
            @strong value, @strong on_change, @strong ctx, @strong we_set => move |button| {
                if !we_set.get() {
                    let e = vm::Event::User(LocalEvent::Event(button.is_active().into()));
                    on_change.borrow_mut().update(&mut *ctx.borrow_mut(), &e);
                    idle_add_local(clone!(@strong we_set, @strong value, @strong button => move || {
                        Self::we_set_value(&we_set, &button, value.borrow().current());
                        Continue(false)
                    }));
                }
            }),
        );
        Self { button, label, image, value, on_change, we_set }
    }

    fn set_label(button: &T, v: Option<Value>) {
        if let Some(v) = v {
            button.set_label(&format!("{}", WVal(&v)));
        }
    }

    fn set_image(button: &T, v: Option<Value>) {
        if let Some(s) = v.and_then(|v| v.cast_to::<ImageSpec>().ok()) {
            match button.image() {
                Some(w) if w.is::<gtk::Image>() => s.apply(w.downcast_ref().unwrap()),
                Some(_) | None => {
                    button.set_image(Some(&s.get()));
                    button.set_always_show_image(true);
                }
            }
        }
    }

    fn we_set_value(we_set: &Cell<bool>, button: &T, v: Option<Value>) {
        we_set.set(true);
        Self::set_value(button, v);
        we_set.set(false);
    }

    fn set_value(button: &T, v: Option<Value>) {
        if let Some(v) = v {
            match v.get_as::<bool>() {
                None => button.set_inconsistent(true),
                Some(b) => {
                    button.set_active(b);
                    button.set_inconsistent(false);
                }
            }
        }
    }
}

impl<T> BWidget for ToggleButton<T>
where
    T: ToggleButtonExt
        + ButtonExt
        + WidgetExt
        + glib::ObjectType
        + IsA<gtk::Widget>
        + 'static,
{
    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
    }

    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::we_set_value(
            &self.we_set,
            &self.button,
            self.value.borrow_mut().update(ctx, event),
        );
        Self::set_label(&self.button, self.label.update(ctx, event));
        Self::set_image(&self.button, self.image.update(ctx, event));
        self.on_change.borrow_mut().update(ctx, event);
    }
}

pub(super) struct ComboBox {
    root: gtk::EventBox,
    combo: gtk::ComboBoxText,
    choices: BSNode,
    selected: Rc<RefCell<BSNode>>,
    on_change: Rc<RefCell<BSNode>>,
    we_set: Rc<Cell<bool>>,
}

impl ComboBox {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::ComboBox,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let combo = gtk::ComboBoxText::new();
        combo.set_no_show_all(true);
        let root = gtk::EventBox::new();
        root.add(&combo);
        let choices =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.choices.clone());
        let selected = Rc::new(RefCell::new(BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.selected.clone(),
        )));
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope,
            spec.on_change.clone(),
        )));
        let we_set = Rc::new(Cell::new(false));
        if let Some(choices) = choices.current() {
            Self::update_choices(&combo, &choices, &selected.borrow().current());
        }
        we_set.set(true);
        Self::update_active(&combo, &selected.borrow().current());
        we_set.set(false);
        hover_path(&combo, &selected_path, "on_change", &spec.on_change);
        combo.connect_changed(clone!(
            @strong we_set,
            @strong on_change,
            @strong ctx,
            @strong selected => move |combo| {
            if !we_set.get() {
                if let Some(id) = combo.active_id() {
                    if let Ok(idv) = serde_json::from_str::<Value>(id.as_str()) {
                        on_change.borrow_mut().update(
                            &mut *ctx.borrow_mut(),
                            &vm::Event::User(LocalEvent::Event(idv))
                        );
                    }
                }
                idle_add_local(clone!(
                    @strong selected, @strong combo, @strong we_set => move || {
                        we_set.set(true);
                        Self::update_active(&combo, &selected.borrow().current());
                        we_set.set(false);
                        Continue(false)
                    })
                );
            }
        }));
        Self { root, combo, choices, selected, on_change, we_set }
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
        Self::update_active(combo, source)
    }
}

impl BWidget for ComboBox {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        self.we_set.set(true);
        self.on_change.borrow_mut().update(ctx, event);
        Self::update_active(&self.combo, &self.selected.borrow_mut().update(ctx, event));
        if let Some(new) = self.choices.update(ctx, event) {
            Self::update_choices(&self.combo, &new, &self.selected.borrow().current());
        }
        self.we_set.set(false);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.root.set_visible(v);
        self.combo.set_visible(v);
    }

    fn set_sensitive(&self, e: bool) {
        self.combo.set_sensitive(e);
    }

    fn set_highlight(&self, mut path: std::slice::Iter<WidgetPath>, h: bool) {
        if let Some(WidgetPath::Leaf) = path.next() {
            util::set_highlight(&self.combo, h);
        }
    }
}

pub(super) struct Switch {
    value: Rc<RefCell<BSNode>>,
    on_change: Rc<RefCell<BSNode>>,
    we_set: Rc<Cell<bool>>,
    switch: gtk::Switch,
}

impl Switch {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Switch,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let switch = gtk::Switch::new();
        switch.set_no_show_all(true);
        let mut ctx_r = ctx.borrow_mut();
        let ctx_r = &mut ctx_r;
        let value = Rc::new(RefCell::new(BSNode::compile(
            ctx_r,
            scope.clone(),
            spec.value.clone(),
        )));
        let on_change =
            Rc::new(RefCell::new(BSNode::compile(ctx_r, scope, spec.on_change.clone())));
        let we_set = Rc::new(Cell::new(false));
        Self::we_set_value(&we_set, &switch, value.borrow().current());
        switch.connect_state_set(clone!(
        @strong ctx, @strong on_change, @strong we_set, @strong value =>
        move |switch, state| {
            if !we_set.get() {
                on_change.borrow_mut().update(
                    &mut *ctx.borrow_mut(),
                    &vm::Event::User(
                        LocalEvent::Event(state.into())
                    ),
                );
                idle_add_local(
                    clone!(@strong value, @strong switch, @strong we_set => move || {
                        Self::we_set_value(&we_set, &switch, value.borrow().current());
                        Continue(false)
                }));
            }
            Inhibit(true)
        }));
        switch.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("value: {}, on_change: {}", spec.value, spec.on_change)
            );
            Inhibit(false)
        }));
        switch.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("value: {}, on_change: {}", spec.value, spec.on_change)
                );
                Inhibit(false)
            }),
        );
        Self { value, on_change, switch, we_set }
    }

    fn set_value(switch: &gtk::Switch, v: Option<Value>) {
        if let Some(v) = v {
            let v = val_to_bool(&v);
            switch.set_active(v);
            switch.set_state(v);
        }
    }

    fn we_set_value(we_set: &Cell<bool>, switch: &gtk::Switch, v: Option<Value>) {
        we_set.set(true);
        Self::set_value(switch, v);
        we_set.set(false);
    }
}

impl BWidget for Switch {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::we_set_value(
            &self.we_set,
            &self.switch,
            self.value.borrow_mut().update(ctx, event),
        );
        self.on_change.borrow_mut().update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.switch.upcast_ref())
    }
}

pub(super) struct Entry {
    entry: gtk::Entry,
    we_changed: Rc<Cell<bool>>,
    text: Rc<RefCell<BSNode>>,
    on_change: Rc<RefCell<BSNode>>,
    on_activate: Rc<RefCell<BSNode>>,
}

impl Entry {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Entry,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let we_changed = Rc::new(Cell::new(false));
        let mut ctx_r = ctx.borrow_mut();
        let ctx_r = &mut ctx_r;
        let text = Rc::new(RefCell::new(BSNode::compile(
            ctx_r,
            scope.clone(),
            spec.text.clone(),
        )));
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            ctx_r,
            scope.clone(),
            spec.on_change.clone(),
        )));
        let on_activate = Rc::new(RefCell::new(BSNode::compile(
            ctx_r,
            scope,
            spec.on_activate.clone(),
        )));
        let entry = gtk::Entry::new();
        entry.set_no_show_all(true);
        Self::set_text(&entry, text.borrow().current());
        entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
        entry.connect_activate(clone!(
        @strong ctx,
        @strong we_changed,
        @strong text,
        @strong on_activate => move |entry| {
            entry.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
            on_activate.borrow_mut().update(
                &mut *ctx.borrow_mut(),
                &vm::Event::User(
                    LocalEvent::Event(Value::String(Chars::from(String::from(entry.text()))))
                ),
            );
            idle_add_local(clone!(
                @strong we_changed, @strong text, @strong entry => move || {
                    Self::we_set_text(&we_changed, &entry, text.borrow().current());
                    Continue(false)
                }));
        }));
        entry.connect_changed(clone!(
        @strong ctx,
        @strong we_changed,
        @strong on_change => move |e| {
            if !we_changed.get() {
                let v = on_change.borrow_mut().update(
                    &mut *ctx.borrow_mut(),
                    &vm::Event::User(LocalEvent::Event(
                        Value::String(Chars::from(String::from(e.text())))
                    )),
                );
                if let Some(v) = v {
                    if let Some(set) = v.cast_to::<bool>().ok() {
                        if set {
                            e.set_icon_from_icon_name(
                                gtk::EntryIconPosition::Secondary,
                                Some("media-floppy")
                            );
                        }
                    }
                }
            }
        }));
        entry.connect_icon_press(move |e, _, _| e.emit_activate());
        hover_path(&entry, &selected_path, "on_change", &spec.on_change);
        Entry { we_changed, entry, text, on_change, on_activate }
    }

    fn set_text(entry: &gtk::Entry, v: Option<Value>) {
        if let Some(s) = v.and_then(|v| v.cast_to::<Chars>().ok()) {
            entry.set_text(&*s);
        }
    }

    fn we_set_text(we_set: &Cell<bool>, entry: &gtk::Entry, v: Option<Value>) {
        we_set.set(true);
        Self::set_text(entry, v);
        we_set.set(false);
    }
}

impl BWidget for Entry {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::we_set_text(
            &self.we_changed,
            &self.entry,
            self.text.borrow_mut().update(ctx, event),
        );
        self.on_change.borrow_mut().update(ctx, event);
        self.on_activate.borrow_mut().update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.entry.upcast_ref())
    }
}

pub(super) struct Image {
    image_spec: BSNode,
    on_click: Rc<RefCell<BSNode>>,
    image: gtk::Image,
    root: gtk::EventBox,
}

impl Image {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Image,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::EventBox::new();
        let image = gtk::Image::new();
        image.set_no_show_all(true);
        root.add(&image);
        let image_spec =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.spec.clone());
        let on_click =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.on_click.clone());
        let on_click = Rc::new(RefCell::new(on_click));
        let button_pressed = Rc::new(Cell::new(false));
        Self::set_spec(&image, image_spec.current());
        root.connect_button_press_event(clone!(@strong button_pressed => move |_, e| {
            if e.button() == 1 {
                button_pressed.set(true);
            }
            Inhibit(true)
        }));
        root.connect_button_release_event(clone!(
            @strong button_pressed, @strong on_click, @strong ctx => move |_, e| {
            if e.button() == 1 && button_pressed.get() {
                button_pressed.set(false);
                on_click.borrow_mut().update(
                    &mut *ctx.borrow_mut(),
                    &vm::Event::User(LocalEvent::Event(Value::Null))
                );
            }
            Inhibit(true)
        }));
        hover_path(&image, &selected_path, "on_click", &spec.spec);
        Image { image_spec, on_click, image, root }
    }

    fn set_spec(image: &gtk::Image, v: Option<Value>) {
        if let Some(spec) = v.and_then(|v| v.get_as::<ImageSpec>()) {
            spec.apply(&image)
        }
    }
}

impl BWidget for Image {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        self.on_click.borrow_mut().update(ctx, event);
        Self::set_spec(&self.image, self.image_spec.update(ctx, event));
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.root.set_visible(v);
        self.image.set_visible(v);
    }

    fn set_sensitive(&self, e: bool) {
        self.root.set_sensitive(e);
    }

    fn set_highlight(&self, mut path: std::slice::Iter<crate::WidgetPath>, h: bool) {
        if let Some(WidgetPath::Leaf) = path.next() {
            util::set_highlight(&self.image, h)
        }
    }
}

#[derive(Debug)]
struct PositionSpec(gtk::PositionType);

impl FromStr for PositionSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "left" => Ok(PositionSpec(gtk::PositionType::Left)),
            "right" => Ok(PositionSpec(gtk::PositionType::Right)),
            "top" => Ok(PositionSpec(gtk::PositionType::Top)),
            "bottom" => Ok(PositionSpec(gtk::PositionType::Bottom)),
            _ => bail!("expected left, right, top, or bottom"),
        }
    }
}

impl FromValue for PositionSpec {
    fn from_value(v: Value) -> Result<Self> {
        v.cast_to::<Chars>()?.parse()
    }

    fn get(v: Value) -> Option<Self> {
        FromValue::from_value(v).ok()
    }
}

pub(super) struct Scale {
    scale: gtk::Scale,
    draw_value: BSNode,
    marks: BSNode,
    has_origin: BSNode,
    value: BSNode,
    min: BSNode,
    max: BSNode,
    on_change: Rc<RefCell<BSNode>>,
    we_set: Rc<Cell<bool>>,
}

impl Scale {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Scale,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let scale = gtk::Scale::new(
            containers::dir_to_gtk(&spec.direction),
            gtk::Adjustment::NONE,
        );
        scale.set_no_show_all(true);
        let draw_value = BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.draw_value.clone(),
        );
        let marks =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.marks.clone());
        let has_origin = BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.has_origin.clone(),
        );
        let value =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.value.clone());
        let min =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.min.clone());
        let max =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.max.clone());
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.on_change.clone(),
        )));
        let we_set = Rc::new(Cell::new(false));
        Self::set_min(&scale, min.current());
        Self::set_max(&scale, max.current());
        Self::set_value(&scale, value.current());
        Self::set_draw_value(&scale, draw_value.current());
        Self::set_marks(&scale, marks.current());
        Self::set_has_origin(&scale, has_origin.current());
        scale.connect_value_changed(
            clone!(@strong on_change, @strong ctx, @strong we_set => move |scale| {
                if !we_set.get() {
                    on_change.borrow_mut().update(
                        &mut *ctx.borrow_mut(),
                        &vm::Event::User(LocalEvent::Event(scale.value().into()))
                    );
                }
            }),
        );
        hover_path(&scale, &selected_path, "on_change", &spec.on_change);
        Self { scale, draw_value, marks, has_origin, value, min, max, on_change, we_set }
    }

    fn set_min(scale: &gtk::Scale, v: Option<Value>) {
        if let Some(v) = v.and_then(|v| v.cast_to::<f64>().ok()) {
            scale.adjustment().set_lower(v);
        }
    }

    fn set_max(scale: &gtk::Scale, v: Option<Value>) {
        if let Some(v) = v.and_then(|v| v.cast_to::<f64>().ok()) {
            scale.adjustment().set_upper(v);
        }
    }

    fn set_value(scale: &gtk::Scale, v: Option<Value>) {
        if let Some(v) = v.and_then(|v| v.cast_to::<f64>().ok()) {
            scale.set_value(v);
        }
    }

    fn set_has_origin(scale: &gtk::Scale, v: Option<Value>) {
        if let Some(v) = v.and_then(|v| v.cast_to::<bool>().ok()) {
            scale.set_has_origin(v);
        }
    }

    fn set_draw_value(scale: &gtk::Scale, v: Option<Value>) {
        if let Some(v) = v {
            match v.clone().get_as::<bool>() {
                Some(b) => scale.set_draw_value(b),
                None => match v.cast_to::<(PositionSpec, Option<i32>)>().ok() {
                    None => scale.set_draw_value(false),
                    Some((pos, decimals)) => {
                        scale.set_draw_value(true);
                        scale.set_value_pos(pos.0);
                        match decimals {
                            Some(decimals) => scale.set_digits(decimals),
                            None => scale.set_digits(-1),
                        }
                    }
                },
            }
        }
    }

    fn set_marks(scale: &gtk::Scale, v: Option<Value>) {
        if let Some(marks) =
            v.and_then(|v| v.cast_to::<Vec<(f64, PositionSpec, Option<Chars>)>>().ok())
        {
            scale.clear_marks();
            for (pos, spec, text) in marks {
                scale.add_mark(pos, spec.0, text.as_ref().map(|c| &**c))
            }
        }
    }
}

impl BWidget for Scale {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::set_draw_value(&self.scale, self.draw_value.update(ctx, event));
        Self::set_marks(&self.scale, self.marks.update(ctx, event));
        Self::set_has_origin(&self.scale, self.has_origin.update(ctx, event));
        self.we_set.set(true);
        Self::set_value(&self.scale, self.value.update(ctx, event));
        self.we_set.set(false);
        Self::set_min(&self.scale, self.min.update(ctx, event));
        Self::set_max(&self.scale, self.max.update(ctx, event));
        self.on_change.borrow_mut().update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.scale.upcast_ref())
    }
}

pub(super) struct ProgressBar {
    progress: gtk::ProgressBar,
    ellipsize: BSNode,
    fraction: Rc<RefCell<BSNode>>,
    pulse: BSNode,
    text: BSNode,
    show_text: BSNode,
}

impl ProgressBar {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::ProgressBar,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let progress = gtk::ProgressBar::new();
        progress.set_no_show_all(true);
        hover_path(&progress, &selected_path, "fraction", &spec.fraction);
        let ellipsize =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.ellipsize);
        let fraction = Rc::new(RefCell::new(BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.fraction,
        )));
        let pulse = BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.pulse);
        let text = BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.text);
        let show_text = BSNode::compile(&mut *ctx.borrow_mut(), scope, spec.show_text);
        Self::set_ellipsize(&progress, ellipsize.current());
        Self::set_fraction(&progress, fraction.borrow().current());
        Self::set_pulse(&progress, pulse.current());
        Self::set_text(&progress, text.current());
        Self::set_show_text(&progress, show_text.current());
        progress.connect_show(clone!(@strong fraction => move |progress| {
            Self::set_fraction(&progress, fraction.borrow().current());
        }));
        Self { progress, ellipsize, fraction, pulse, text, show_text }
    }

    fn set_ellipsize(progress: &gtk::ProgressBar, v: Option<Value>) {
        if let Some(mode) = v.map(parse_ellipsize) {
            progress.set_ellipsize(mode);
        }
    }

    fn set_fraction(progress: &gtk::ProgressBar, v: Option<Value>) {
        if let Some(f) = v.and_then(|v| v.cast_to::<f64>().ok()) {
            progress.set_fraction(f);
        }
    }

    fn set_pulse(progress: &gtk::ProgressBar, v: Option<Value>) {
        if let Some(_) = v {
            progress.pulse();
        }
    }

    fn set_text(progress: &gtk::ProgressBar, v: Option<Value>) {
        if let Some(text) = v.and_then(|v| v.cast_to::<Chars>().ok()) {
            progress.set_text(Some(&*text));
        }
    }

    fn set_show_text(progress: &gtk::ProgressBar, v: Option<Value>) {
        if let Some(show) = v.and_then(|v| v.cast_to::<bool>().ok()) {
            progress.set_show_text(show);
        }
    }
}

impl BWidget for ProgressBar {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::set_ellipsize(&self.progress, self.ellipsize.update(ctx, event));
        Self::set_fraction(&self.progress, self.fraction.borrow_mut().update(ctx, event));
        Self::set_pulse(&self.progress, self.pulse.update(ctx, event));
        Self::set_text(&self.progress, self.text.update(ctx, event));
        Self::set_show_text(&self.progress, self.show_text.update(ctx, event));
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.progress.upcast_ref())
    }
}
