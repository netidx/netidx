use super::{
    util, val_to_bool, BSCtx, BSCtxRef, BSNode, BWidget, ImageSpec, WVal, WidgetPath,
};
use crate::{bscript::LocalEvent, containers, view};
use anyhow::{bail, Result};
use futures::channel::oneshot;
use gdk4::{self as gdk4, prelude::*};
use glib::{clone, idle_add_local, idle_add_local_once};
use gtk4::{self as gtk, prelude::*, Inhibit};
use indexmap::IndexSet;
use netidx::{chars::Chars, path::Path, protocol::value::FromValue, subscriber::Value};
use netidx_bscript::{expr::Expr, vm};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    str::FromStr,
};

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
    let ec = gtk::EventControllerFocus::new();
    w.add_controller(ec.clone());
    ec.connect_enter(clone!(@strong selected_path, @strong expr => move |_| {
        selected_path.set_label(&format!("{}: {}", name, expr));
    }));
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
        let (label, image, on_click) = {
            let label =
                BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.label.clone());
            let image =
                BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.image.clone());
            let on_click = Rc::new(RefCell::new(BSNode::compile(
                &mut ctx.borrow_mut(),
                scope,
                spec.on_click.clone(),
            )));
            (label, image, on_click)
        };
        Self::set_label(&button, label.current(&mut ctx.borrow_mut()));
        Self::set_image(&button, image.current(&mut ctx.borrow_mut()));
        hover_path(&button, &selected_path, "on_click", &spec.on_click);
        button.connect_clicked(clone!(@strong ctx, @strong on_click => move |_| {
            on_click.borrow_mut().update(
                &mut ctx.borrow_mut(),
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
            match button.child() {
                Some(image) if image.is::<gtk::Image>() => {
                    s.apply(image.downcast_ref().unwrap())
                }
                Some(_) | None => button.set_child(Some(&s.get())),
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
        let uri = BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.uri.clone());
        let label =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.label.clone());
        let on_activate_link = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope,
            spec.on_activate_link.clone(),
        )));
        let button = gtk::LinkButton::new("file:///");
        Self::set_uri(&button, uri.current(&mut ctx.borrow_mut()));
        Self::set_label(&button, label.current(&mut ctx.borrow_mut()));
        hover_path(&button, &selected_path, "on_activate_link", &spec.on_activate_link);
        button.connect_activate_link(
            clone!(@strong ctx, @strong on_activate_link => move |button| {
                let uri = button.uri().as_str();
                let ev = vm::Event::User(LocalEvent::Event(uri.into()));
                match on_activate_link.borrow_mut().update(&mut ctx.borrow_mut(), &ev) {
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
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.text.clone());
        let width =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.width.clone());
        let ellipsize =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.ellipsize.clone());
        let single_line = BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.single_line.clone(),
        );
        let selectable =
            BSNode::compile(&mut ctx.borrow_mut(), scope, spec.selectable.clone());
        let label = gtk::Label::new(None);
        Self::set_text(&label, text.current(&mut ctx.borrow_mut()));
        Self::set_single_line(&label, single_line.current(&mut ctx.borrow_mut()));
        Self::set_selectable(&label, selectable.current(&mut ctx.borrow_mut()));
        Self::set_width(&label, width.current(&mut ctx.borrow_mut()));
        Self::set_ellipsize(&label, ellipsize.current(&mut ctx.borrow_mut()));
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
        let mut expr = BSNode::compile(&mut ctx.borrow_mut(), scope, spec.clone());
        expr.update(
            &mut ctx.borrow_mut(),
            &vm::Event::User(LocalEvent::Event(Value::Null)),
        );
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

pub(super) struct ToggleButton {
    button: gtk::ToggleButton,
    we_set: Rc<Cell<bool>>,
    value: Rc<RefCell<BSNode>>,
    label: BSNode,
    image: BSNode,
    on_change: Rc<RefCell<BSNode>>,
}

impl ToggleButton {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::ToggleButton,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::ToggleButton::new();
        let we_set = Rc::new(Cell::new(false));
        let value = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.toggle.value.clone(),
        )));
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.toggle.on_change.clone(),
        )));
        let label =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.label.clone());
        let image =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.image.clone());
        Self::set_label(&button, label.current(&mut ctx.borrow_mut()));
        Self::set_image(&button, image.current(&mut ctx.borrow_mut()));
        Self::we_set_value(
            &we_set,
            &button,
            value.borrow().current(&mut ctx.borrow_mut()),
        );
        hover_path(&button, &selected_path, "on_change", &spec.toggle.on_change);
        button.connect_toggled(clone!(
            @strong value, @strong on_change, @strong ctx, @strong we_set => move |button| {
                if !we_set.get() {
                    let e = vm::Event::User(LocalEvent::Event(button.is_active().into()));
                    on_change.borrow_mut().update(&mut ctx.borrow_mut(), &e);
                    idle_add_local(clone!(@strong ctx, @strong we_set, @strong value, @strong button => move || {
                        Self::we_set_value(&we_set, &button, value.borrow().current(&mut ctx.borrow_mut()));
                        Continue(false)
                    }));
                }
            }),
        );
        Self { button, label, image, value, on_change, we_set }
    }

    fn set_label(button: &gtk::ToggleButton, v: Option<Value>) {
        if let Some(v) = v {
            button.set_label(&format!("{}", WVal(&v)));
        }
    }

    fn set_image(button: &gtk::ToggleButton, v: Option<Value>) {
        if let Some(s) = v.and_then(|v| v.cast_to::<ImageSpec>().ok()) {
            match button.child() {
                Some(w) if w.is::<gtk::Image>() => s.apply(w.downcast_ref().unwrap()),
                Some(_) | None => button.set_child(Some(&s.get())),
            }
        }
    }

    fn we_set_value(we_set: &Cell<bool>, button: &gtk::ToggleButton, v: Option<Value>) {
        we_set.set(true);
        Self::set_value(button, v);
        we_set.set(false);
    }

    fn set_value(button: &gtk::ToggleButton, v: Option<Value>) {
        if let Some(v) = v {
            match v.get_as::<bool>() {
                None => (), //button.set_inconsistent(true)
                Some(b) => {
                    button.set_active(b);
                    //button.set_inconsistent(false);
                }
            }
        }
    }
}

impl BWidget for ToggleButton {
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

pub(super) struct CheckButton {
    button: gtk::CheckButton,
    we_set: Rc<Cell<bool>>,
    value: Rc<RefCell<BSNode>>,
    label: BSNode,
    image: BSNode,
    on_change: Rc<RefCell<BSNode>>,
}

impl CheckButton {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::ToggleButton,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::CheckButton::new();
        let we_set = Rc::new(Cell::new(false));
        let value = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.toggle.value.clone(),
        )));
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.toggle.on_change.clone(),
        )));
        let label =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.label.clone());
        let image =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.image.clone());
        Self::set_label(&button, label.current(&mut ctx.borrow_mut()));
        Self::we_set_value(
            &we_set,
            &button,
            value.borrow().current(&mut ctx.borrow_mut()),
        );
        hover_path(&button, &selected_path, "on_change", &spec.toggle.on_change);
        button.connect_toggled(clone!(
            @strong value, @strong on_change, @strong ctx, @strong we_set => move |button| {
                if !we_set.get() {
                    let e = vm::Event::User(LocalEvent::Event(button.is_active().into()));
                    on_change.borrow_mut().update(&mut ctx.borrow_mut(), &e);
                    idle_add_local(clone!(@strong ctx, @strong we_set, @strong value, @strong button => move || {
                        Self::we_set_value(&we_set, &button, value.borrow().current(&mut ctx.borrow_mut()));
                        Continue(false)
                    }));
                }
            }),
        );
        Self { button, label, image, value, on_change, we_set }
    }

    fn set_label(button: &gtk::CheckButton, v: Option<Value>) {
        if let Some(v) = v {
            button.set_label(Some(&format!("{}", WVal(&v))));
        }
    }

    fn we_set_value(we_set: &Cell<bool>, button: &gtk::CheckButton, v: Option<Value>) {
        we_set.set(true);
        Self::set_value(button, v);
        we_set.set(false);
    }

    fn set_value(button: &gtk::CheckButton, v: Option<Value>) {
        if let Some(v) = v {
            match v.get_as::<bool>() {
                None => (), //button.set_inconsistent(true)
                Some(b) => {
                    button.set_active(b);
                    //button.set_inconsistent(false);
                }
            }
        }
    }
}

impl BWidget for CheckButton {
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
        self.on_change.borrow_mut().update(ctx, event);
    }
}

pub(super) struct ComboBox {
    root: gtk::ComboBoxText,
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
        let root = gtk::ComboBoxText::new();
        let choices =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.choices.clone());
        let selected = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.selected.clone(),
        )));
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope,
            spec.on_change.clone(),
        )));
        let we_set = Rc::new(Cell::new(false));
        Self::set_choices(&root, choices.current(&mut ctx.borrow_mut()));
        Self::we_set_selected(
            &we_set,
            &root,
            selected.borrow().current(&mut ctx.borrow_mut()),
        );
        hover_path(&root, &selected_path, "on_change", &spec.on_change);
        root.connect_changed(clone!(
            @strong we_set,
            @strong on_change,
            @strong ctx,
            @strong selected => move |combo| {
            if !we_set.get() {
                if let Some(id) = combo.active_id() {
                    let idv = Value::from(Chars::from(String::from(id)));
                    on_change.borrow_mut().update(
                        &mut ctx.borrow_mut(),
                        &vm::Event::User(LocalEvent::Event(idv))
                    );
                }
                idle_add_local(clone!(
                    @strong ctx, @strong selected, @strong combo, @strong we_set => move || {
                        Self::we_set_selected(&we_set, &combo, selected.borrow().current(&mut ctx.borrow_mut()));
                        Continue(false)
                    })
                );
            }
        }));
        Self { root, choices, selected, on_change, we_set }
    }

    fn set_selected(combo: &gtk::ComboBoxText, v: Option<Value>) {
        if let Some(id) = v.and_then(|v| v.cast_to::<Chars>().ok()) {
            combo.set_active_id(Some(&*id));
        }
    }

    fn we_set_selected(we_set: &Cell<bool>, combo: &gtk::ComboBoxText, v: Option<Value>) {
        we_set.set(true);
        Self::set_selected(&combo, v);
        we_set.set(false);
    }

    fn set_choices(combo: &gtk::ComboBoxText, v: Option<Value>) {
        if let Some(choices) = v.and_then(|v| v.cast_to::<Vec<(Chars, Chars)>>().ok()) {
            combo.remove_all();
            for (id, val) in choices {
                combo.append(Some(&id.to_string()), &*val);
            }
        }
    }
}

impl BWidget for ComboBox {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        self.on_change.borrow_mut().update(ctx, event);
        Self::set_choices(&self.root, self.choices.update(ctx, event));
        Self::we_set_selected(
            &self.we_set,
            &self.root,
            self.selected.borrow_mut().update(ctx, event),
        );
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.root.set_visible(v);
    }

    fn set_sensitive(&self, e: bool) {
        self.root.set_sensitive(e);
    }

    fn set_highlight(&self, mut path: std::slice::Iter<WidgetPath>, h: bool) {
        if let Some(WidgetPath::Leaf) = path.next() {
            util::set_highlight(&self.root, h);
        }
    }
}

pub(super) struct RadioButton {
    button: gtk::CheckButton,
    on_toggled: Rc<RefCell<BSNode>>,
    label: BSNode,
    image: BSNode,
    group: BSNode,
    value: BSNode,
    we_changed: Rc<Cell<bool>>,
    current_group: Option<String>,
    group_changing: Rc<Cell<bool>>,
}

impl RadioButton {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::RadioButton,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let we_changed = Rc::new(Cell::new(false));
        let group_changing = Rc::new(Cell::new(false));
        let on_toggled = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.on_toggled.clone(),
        )));
        let label =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.label.clone());
        let image =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.image.clone());
        let group =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.group.clone());
        let value =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.value.clone());
        let button = gtk::CheckButton::new();
        button.connect_toggled(clone!(
        @strong on_toggled,
        @strong ctx,
        @strong group_changing,
        @strong we_changed => move |button| {
            if !we_changed.get() {
                let active = button.is_active();
                let e = vm::Event::User(LocalEvent::Event(active.into()));
                if group_changing.get() {
                    idle_add_local(clone!(@strong on_toggled, @strong ctx  => move || {
                        on_toggled.borrow_mut().update(&mut ctx.borrow_mut(), &e);
                        Continue(false)
                    }));
                } else {
                    on_toggled.borrow_mut().update(&mut ctx.borrow_mut(), &e);
                }
            }
        }));
        hover_path(&button, &selected_path, "on_toggled", &spec.on_toggled);
        let mut t = Self {
            button,
            on_toggled,
            label,
            image,
            value,
            group,
            group_changing,
            we_changed,
            current_group: None,
        };
        t.set_label(t.label.current(&mut ctx.borrow_mut()));
        let group_current = t.group.current(&mut ctx.borrow_mut());
        t.set_group(&mut ctx.borrow_mut(), group_current);
        t.we_set_value(t.value.current(&mut ctx.borrow_mut()));
        t
    }

    fn set_label(&self, v: Option<Value>) {
        if let Some(text) = v.and_then(|v| v.cast_to::<Chars>().ok()) {
            self.button.set_label(Some(&*text));
        }
    }

    fn set_group(&mut self, ctx: BSCtxRef, v: Option<Value>) {
        if let Some(group) = v.and_then(|v| v.cast_to::<String>().ok()) {
            self.group_changing.set(true);
            if let Some(current) = self.current_group.take() {
                self.button.set_group(None::<&gtk::CheckButton>);
                if let Some((_, group)) = ctx.user.radio_groups.get_mut(&current) {
                    group.remove(&self.button);
                    if group.is_empty() {
                        ctx.user.radio_groups.remove(&current);
                    }
                }
            }
            self.current_group = Some(group.clone());
            let (we_changed, group) = ctx
                .user
                .radio_groups
                .entry(group)
                .or_insert_with(|| (self.we_changed.clone(), IndexSet::default()));
            self.we_changed = we_changed.clone();
            self.button.set_group(group.last());
            group.insert(self.button.clone());
            self.group_changing.set(false);
        }
    }

    fn we_set_value_(
        button: &gtk::CheckButton,
        we_changed: &Rc<Cell<bool>>,
        v: Option<Value>,
    ) {
        we_changed.set(true);
        if let Some(value) = v.and_then(|v| v.cast_to::<bool>().ok()) {
            button.set_active(value);
        }
        we_changed.set(false);
    }

    fn we_set_value(&mut self, v: Option<Value>) {
        Self::we_set_value_(&self.button, &self.we_changed, v)
    }
}

impl BWidget for RadioButton {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        let v = self.label.update(ctx, event);
        self.set_label(v);
        let v = self.image.update(ctx, event);
        // CR: estokes implement set_image on gtk4
        let v = self.group.update(ctx, event);
        self.set_group(ctx, v);
        let v = self.value.update(ctx, event);
        self.we_set_value(v);
        self.on_toggled.borrow_mut().update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.button.upcast_ref())
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
        let switch_ev = gtk::EventControllerFocus::new();
        switch.add_controller(switch_ev.clone());
        let value = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.value.clone(),
        )));
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope,
            spec.on_change.clone(),
        )));
        let we_set = Rc::new(Cell::new(false));
        Self::we_set_value(
            &we_set,
            &switch,
            value.borrow().current(&mut ctx.borrow_mut()),
        );
        switch.connect_state_set(clone!(
        @strong ctx, @strong on_change, @strong we_set, @strong value =>
        move |switch, state| {
            if !we_set.get() {
                on_change.borrow_mut().update(
                    &mut ctx.borrow_mut(),
                    &vm::Event::User(
                        LocalEvent::Event(state.into())
                    ),
                );
                idle_add_local(
                    clone!(@strong ctx, @strong value, @strong switch, @strong we_set => move || {
                        Self::we_set_value(&we_set, &switch, value.borrow().current(&mut ctx.borrow_mut()));
                        Continue(false)
                }));
            }
            Inhibit(true)
        }));
        switch_ev.connect_enter(clone!(@strong selected_path, @strong spec => move |_| {
            selected_path.set_label(
                &format!("value: {}, on_change: {}", spec.value, spec.on_change)
            );
        }));
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
        Self::set_text(&entry, text.borrow().current(&mut ctx.borrow_mut()));
        entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
        entry.connect_activate(clone!(
        @strong ctx,
        @strong we_changed,
        @strong text,
        @strong on_activate => move |entry| {
            entry.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
            on_activate.borrow_mut().update(
                &mut ctx.borrow_mut(),
                &vm::Event::User(
                    LocalEvent::Event(Value::String(Chars::from(String::from(entry.text()))))
                ),
            );
            idle_add_local(clone!(
                @strong ctx, @strong we_changed, @strong text, @strong entry => move || {
                    Self::we_set_text(&we_changed, &entry, text.borrow().current(&mut ctx.borrow_mut()));
                    Continue(false)
                }));
        }));
        entry.connect_changed(clone!(
        @strong ctx,
        @strong we_changed,
        @strong on_change => move |e| {
            if !we_changed.get() {
                let v = on_change.borrow_mut().update(
                    &mut ctx.borrow_mut(),
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
        entry.connect_icon_press(move |e, _| e.emit_activate());
        hover_path(&entry, &selected_path, "on_change", &spec.on_change);
        Entry { we_changed, entry, text, on_change, on_activate }
    }

    fn set_text(entry: &gtk::Entry, v: Option<Value>) {
        if let Some(s) = v.and_then(|v| v.cast_to::<Chars>().ok()) {
            if &*entry.text() != &*s {
                entry.set_text(&*s);
            }
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

pub(super) struct SearchEntry {
    entry: gtk::SearchEntry,
    we_changed: Rc<Cell<bool>>,
    text: BSNode,
    on_search_changed: Rc<RefCell<BSNode>>,
    on_activate: Rc<RefCell<BSNode>>,
}

impl SearchEntry {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::SearchEntry,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let we_changed = Rc::new(Cell::new(false));
        let text =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.text.clone());
        let on_search_changed = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.on_search_changed.clone(),
        )));
        let on_activate = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.on_activate.clone(),
        )));
        let entry = gtk::SearchEntry::new();
        hover_path(&entry, &selected_path, "on_search_changed", &spec.on_search_changed);
        Self::set_text(&entry, text.current(&mut ctx.borrow_mut()));
        entry.connect_search_changed(
            clone!(@strong on_search_changed, @strong ctx, @strong we_changed => move |entry| {
                if !we_changed.get() {
                    let s = Value::from(entry.text().to_string());
                    let e = vm::Event::User(LocalEvent::Event(s));
                    on_search_changed.borrow_mut().update(&mut ctx.borrow_mut(), &e);
                }
            }));
        entry.connect_activate(clone!(@strong on_activate, @strong ctx => move |e| {
            let s = Value::from(e.text().to_string());
            let e = vm::Event::User(LocalEvent::Event(s));
            on_activate.borrow_mut().update(&mut ctx.borrow_mut(), &e);
        }));
        Self { entry, we_changed, text, on_search_changed, on_activate }
    }

    fn set_text(entry: &gtk::SearchEntry, v: Option<Value>) {
        if let Some(s) = v.and_then(|v| v.cast_to::<Chars>().ok()) {
            if &*entry.text() != &*s {
                entry.set_text(&*s);
            }
        }
    }

    fn we_set_text(we_set: &Cell<bool>, entry: &gtk::SearchEntry, v: Option<Value>) {
        we_set.set(true);
        Self::set_text(entry, v);
        we_set.set(false);
    }
}

impl BWidget for SearchEntry {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        _waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        Self::we_set_text(&*self.we_changed, &self.entry, self.text.update(ctx, event));
        self.on_search_changed.borrow_mut().update(ctx, event);
        self.on_activate.borrow_mut().update(ctx, event);
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(&self.entry.upcast_ref())
    }
}

pub(super) struct Image {
    image_spec: BSNode,
    on_click: Rc<RefCell<BSNode>>,
    root: gtk::Image,
}

impl Image {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Image,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::Image::new();
        let root_ev = gtk::GestureClick::new();
        let image_spec =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.spec.clone());
        let on_click =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.on_click.clone());
        let on_click = Rc::new(RefCell::new(on_click));
        let button_pressed = Rc::new(Cell::new(false));
        Self::set_spec(&root, image_spec.current(&mut ctx.borrow_mut()));
        root_ev.connect_pressed(clone!(@strong button_pressed => move |c, _, _, _| {
            if c.current_button() == 1 {
                button_pressed.set(true);
            }
        }));
        root_ev.connect_released(clone!(
            @strong button_pressed, @strong on_click, @strong ctx => move |c, _, _, _| {
            if c.current_button() == 1 && button_pressed.get() {
                button_pressed.set(false);
                on_click.borrow_mut().update(
                    &mut ctx.borrow_mut(),
                    &vm::Event::User(LocalEvent::Event(Value::Null))
                );
            }
        }));
        hover_path(&root, &selected_path, "on_click", &spec.spec);
        Image { image_spec, on_click, root }
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
        Self::set_spec(&self.root, self.image_spec.update(ctx, event));
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.root.set_visible(v);
    }

    fn set_sensitive(&self, e: bool) {
        self.root.set_sensitive(e);
    }

    fn set_highlight(&self, mut path: std::slice::Iter<crate::WidgetPath>, h: bool) {
        if let Some(WidgetPath::Leaf) = path.next() {
            util::set_highlight(&self.root, h)
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
        let draw_value = BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.draw_value.clone(),
        );
        let marks =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.marks.clone());
        let has_origin = BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.has_origin.clone(),
        );
        let value =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.value.clone());
        let min = BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.min.clone());
        let max = BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.max.clone());
        let on_change = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.on_change.clone(),
        )));
        let we_set = Rc::new(Cell::new(false));
        Self::set_min(&scale, min.current(&mut ctx.borrow_mut()));
        Self::set_max(&scale, max.current(&mut ctx.borrow_mut()));
        Self::set_value(&scale, value.current(&mut ctx.borrow_mut()));
        Self::set_draw_value(&scale, draw_value.current(&mut ctx.borrow_mut()));
        Self::set_marks(&scale, marks.current(&mut ctx.borrow_mut()));
        Self::set_has_origin(&scale, has_origin.current(&mut ctx.borrow_mut()));
        scale.connect_value_changed(
            clone!(@strong on_change, @strong ctx, @strong we_set => move |scale| {
                if !we_set.get() {
                    on_change.borrow_mut().update(
                        &mut ctx.borrow_mut(),
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
        hover_path(&progress, &selected_path, "fraction", &spec.fraction);
        let ellipsize =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.ellipsize);
        let fraction = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.fraction,
        )));
        let pulse = BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.pulse);
        let text = BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.text);
        let show_text = BSNode::compile(&mut ctx.borrow_mut(), scope, spec.show_text);
        Self::set_ellipsize(&progress, ellipsize.current(&mut ctx.borrow_mut()));
        Self::set_fraction(&progress, fraction.borrow().current(&mut ctx.borrow_mut()));
        Self::set_pulse(&progress, pulse.current(&mut ctx.borrow_mut()));
        Self::set_text(&progress, text.current(&mut ctx.borrow_mut()));
        Self::set_show_text(&progress, show_text.current(&mut ctx.borrow_mut()));
        progress.connect_show(clone!(@strong ctx, @strong fraction => move |progress| {
            idle_add_local_once(clone!(@strong ctx, @strong fraction, @strong progress => move || {
                Self::set_fraction(&progress, fraction.borrow().current(&mut ctx.borrow_mut()));
            }));
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
