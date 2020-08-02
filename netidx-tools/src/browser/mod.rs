mod table;
mod view;
use futures::{
    channel::{mpsc, oneshot},
    future::{pending, FutureExt},
    select_biased,
    stream::StreamExt,
};
use gdk::keys;
use gio::prelude::*;
use glib::{self, clone, signal::Inhibit, source::PRIORITY_LOW};
use gtk::{self, prelude::*, Application, ApplicationWindow, Orientation};
use indexmap::IndexMap;
use log::{info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::Pooled,
    resolver::{Auth, ResolverRead},
    subscriber::{DvState, Dval, SubId, Subscriber, Value},
};
use netidx_protocols::view as protocol_view;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    mem, process,
    rc::Rc,
    result,
    sync::{mpsc as smpsc, Arc},
    thread,
    time::Duration,
};
use tokio::{runtime::Runtime, time};

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone)]
enum ToGui {
    View(Path, view::View),
    Update(Arc<IndexMap<SubId, Value>>),
    UpdateVar(String, Value),
}

#[derive(Debug, Clone)]
enum FromGui {
    Navigate(Path),
    Updated,
}

#[derive(Debug, Clone)]
struct WidgetCtx {
    subscriber: Subscriber,
    resolver: ResolverRead,
    updates: mpsc::Sender<Batch>,
    state_updates: mpsc::UnboundedSender<(SubId, DvState)>,
    to_gui: mpsc::UnboundedSender<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
}

#[derive(Debug, Clone)]
enum Source {
    Constant(Value),
    Load(Dval),
    Variable(String, Rc<RefCell<Value>>),
}

impl Source {
    fn new(
        ctx: &WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Source,
    ) -> Self {
        match spec {
            view::Source::Constant(v) => Source::Constant(v),
            view::Source::Load(path) => {
                let dv = ctx.subscriber.durable_subscribe(path);
                dv.updates(true, ctx.updates.clone());
                dv.state_updates(true, ctx.state_updates.clone());
                Source::Load(dv)
            }
            view::Source::Variable(name) => {
                let v = variables.get(&name).cloned().unwrap_or(Value::Null);
                Source::Variable(name, Rc::new(RefCell::new(v)))
            }
        }
    }

    fn current(&self) -> Value {
        match self {
            Source::Constant(v) => v.clone(),
            Source::Variable(_, v) => v.borrow().clone(),
            Source::Load(dv) => match dv.last() {
                None => Value::String(Chars::from("#SUB")),
                Some(v) => v.clone(),
            },
        }
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) -> Option<Value> {
        match self {
            Source::Constant(_) | Source::Variable(_, _) => None,
            Source::Load(dv) => changed.get(&dv.id()).cloned(),
        }
    }

    fn update_var(&self, name: &str, value: &Value) -> bool {
        match self {
            Source::Load(_) | Source::Constant(_) => false,
            Source::Variable(our_name, cur) => {
                if name == our_name {
                    *cur.borrow_mut() = value.clone();
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Clone)]
enum Sink {
    Store(Dval),
    Variable(String),
}

impl Sink {
    fn new(ctx: &WidgetCtx, spec: view::Sink) -> Self {
        match spec {
            view::Sink::Variable(name) => Sink::Variable(name),
            view::Sink::Store(path) => {
                Sink::Store(ctx.subscriber.durable_subscribe(path))
            }
        }
    }

    fn set(&self, ctx: &WidgetCtx, v: Value) {
        match self {
            Sink::Store(dv) => {
                dv.write(v);
            }
            Sink::Variable(name) => {
                let _: result::Result<_, _> =
                    ctx.to_gui.unbounded_send(ToGui::UpdateVar(name.clone(), v));
            }
        }
    }
}

fn val_to_bool(v: &Value) -> bool {
    match v {
        Value::False | Value::Null => false,
        _ => true,
    }
}

fn align_to_gtk(a: view::Align) -> gtk::Align {
    match a {
        view::Align::Fill => gtk::Align::Fill,
        view::Align::Start => gtk::Align::Start,
        view::Align::End => gtk::Align::End,
        view::Align::Center => gtk::Align::Center,
        view::Align::Baseline => gtk::Align::Baseline,
    }
}

struct Label {
    label: gtk::Label,
    source: Source,
}

impl Label {
    fn new(
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

    fn root(&self) -> &gtk::Widget {
        self.label.upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.source.update(changed) {
            self.label.set_label(&format!("{}", new));
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if self.source.update_var(name, value) {
            self.label.set_label(&format!("{}", value));
        }
    }
}

struct Button {
    enabled: Source,
    label: Source,
    source: Source,
    button: gtk::Button,
}

impl Button {
    fn new(
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

    fn root(&self) -> &gtk::Widget {
        self.button.upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.enabled.update(changed) {
            self.button.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.label.update(changed) {
            self.button.set_label(&format!("{}", new));
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if self.enabled.update_var(name, value) {
            self.button.set_sensitive(val_to_bool(value));
        }
        if self.label.update_var(name, value) {
            self.button.set_label(&format!("{}", value));
        }
        self.source.update_var(name, value);
    }
}

struct Toggle {
    enabled: Source,
    source: Source,
    we_set: Rc<Cell<bool>>,
    switch: gtk::Switch,
}

impl Toggle {
    fn new(
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

    fn root(&self) -> &gtk::Widget {
        self.switch.upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
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

    fn update_var(&self, name: &str, value: &Value) {
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

struct Action {
    source: Source,
    sink: Sink,
    ctx: WidgetCtx,
}

impl Action {
    fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Action,
    ) -> Self {
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        Action { source, sink, ctx }
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.source.update(changed) {
            self.sink.set(&self.ctx, new);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if self.source.update_var(name, value) {
            self.sink.set(&self.ctx, value.clone())
        }
    }
}

struct Container {
    root: gtk::Box,
    children: Vec<Widget>,
}

impl Container {
    fn new(
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

    fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        updates: &Arc<IndexMap<SubId, Value>>,
    ) {
        for c in &self.children {
            c.update(waits, updates);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        for c in &self.children {
            c.update_var(name, value);
        }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

struct Grid {
    root: gtk::Grid,
    children: Vec<Vec<Widget>>,
}

impl Grid {
    fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Grid,
        selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::Grid::new();
        root.set_column_homogeneous(spec.homogeneous_columns);
        root.set_row_homogeneous(spec.homogeneous_rows);
        root.set_column_spacing(spec.column_spacing);
        root.set_row_spacing(spec.row_spacing);
        let children = spec
            .children
            .into_iter()
            .enumerate()
            .map(|(j, row)| {
                row.into_iter()
                    .enumerate()
                    .map(|(i, spec)| {
                        let w = Widget::new(
                            ctx.clone(),
                            variables,
                            spec.widget.clone(),
                            selected_path.clone(),
                        );
                        if let Some(r) = w.root() {
                            root.attach(r, i as i32, j as i32, 1, 1);
                            if let Some(halign) = spec.halign {
                                r.set_halign(align_to_gtk(halign));
                            }
                            if let Some(valign) = spec.valign {
                                r.set_valign(align_to_gtk(valign));
                            }
                        }
                        w
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Grid { root, children }
    }

    fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        updates: &Arc<IndexMap<SubId, Value>>,
    ) {
        for row in &self.children {
            for child in row {
                child.update(waits, updates);
            }
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        for row in &self.children {
            for child in row {
                child.update_var(name, value);
            }
        }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

struct ComboBox {
    combo: gtk::ComboBoxText,
    enabled: Source,
    choices: Source,
    source: Source,
    we_set: Rc<Cell<bool>>,
}

impl ComboBox {
    fn new(
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
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let choices = Source::new(&ctx, variables, spec.choices.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        let we_set = Rc::new(Cell::new(false));
        combo.set_sensitive(val_to_bool(&enabled.current()));
        ComboBox::update_choices(&combo, &choices.current());
        ComboBox::update_active(&combo, &source.current());
        combo.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!(
                    "source: {:?}, sink: {:?}, choices: {:?}",
                    spec.source, spec.sink, spec.choices
                )
            );
            Inhibit(false)
        }));
        combo.connect_enter_notify_event(
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
        combo.connect_changed(
            clone!(@strong we_set, @strong sink, @strong ctx => move |combo| {
                if !we_set.get() {
                    if let Some(id) = combo.get_active_id() {
                        if let Ok(idv) = serde_json::from_str::<Value>(id.as_str()) {
                            sink.set(&ctx, idv);
                        }
                    }
                }
            }),
        );
        ComboBox { combo, enabled, choices, source, we_set }
    }

    fn update_active(combo: &gtk::ComboBoxText, source: &Value) {
        if let Ok(current) = serde_json::to_string(source) {
            combo.set_active_id(Some(current.as_str()));
        }
    }

    fn update_choices(combo: &gtk::ComboBoxText, choices: &Value) {
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
    }

    fn update(&self, updates: &Arc<IndexMap<SubId, Value>>) {
        self.we_set.set(true);
        if let Some(new) = self.enabled.update(updates) {
            self.combo.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.choices.update(updates) {
            ComboBox::update_choices(&self.combo, &new);
        }
        if let Some(new) = self.source.update(updates) {
            ComboBox::update_active(&self.combo, &new);
        }
        self.we_set.set(false);
    }

    fn update_var(&self, name: &str, value: &Value) {
        self.we_set.set(true);
        if self.enabled.update_var(name, value) {
            self.combo.set_sensitive(val_to_bool(value));
        }
        if self.choices.update_var(name, value) {
            ComboBox::update_choices(&self.combo, value);
        }
        if self.source.update_var(name, value) {
            ComboBox::update_active(&self.combo, value);
        }
        self.we_set.set(false);
    }

    fn root(&self) -> &gtk::Widget {
        self.combo.upcast_ref()
    }
}

enum Widget {
    Table(table::Table),
    Label(Label),
    Action(Action),
    Button(Button),
    Toggle(Toggle),
    ComboBox(ComboBox),
    Container(Container),
    Grid(Grid),
}

impl Widget {
    fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Widget,
        selected_path: gtk::Label,
    ) -> Widget {
        match spec {
            view::Widget::StaticTable(_) => todo!(),
            view::Widget::Table(base_path, spec) => Widget::Table(table::Table::new(
                ctx.clone(),
                base_path,
                spec,
                selected_path,
            )),
            view::Widget::Label(spec) => {
                Widget::Label(Label::new(ctx.clone(), variables, spec, selected_path))
            }
            view::Widget::Action(spec) => {
                Widget::Action(Action::new(ctx.clone(), variables, spec))
            }
            view::Widget::Button(spec) => {
                Widget::Button(Button::new(ctx.clone(), variables, spec, selected_path))
            }
            view::Widget::Toggle(spec) => {
                Widget::Toggle(Toggle::new(ctx.clone(), variables, spec, selected_path))
            }
            view::Widget::ComboBox(spec) => Widget::ComboBox(ComboBox::new(
                ctx.clone(),
                variables,
                spec,
                selected_path,
            )),
            view::Widget::Radio(_) => todo!(),
            view::Widget::Entry(_) => todo!(),
            view::Widget::Container(s) => {
                Widget::Container(Container::new(ctx, variables, s, selected_path))
            }
            view::Widget::Grid(spec) => {
                Widget::Grid(Grid::new(ctx, variables, spec, selected_path))
            }
        }
    }

    fn root(&self) -> Option<&gtk::Widget> {
        match self {
            Widget::Table(t) => Some(t.root()),
            Widget::Label(t) => Some(t.root()),
            Widget::Action(_) => None,
            Widget::Button(t) => Some(t.root()),
            Widget::Toggle(t) => Some(t.root()),
            Widget::ComboBox(t) => Some(t.root()),
            Widget::Container(t) => Some(t.root()),
            Widget::Grid(t) => Some(t.root()),
        }
    }

    fn update<'a, 'b: 'a>(
        &'a self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        changed: &'b Arc<IndexMap<SubId, Value>>,
    ) {
        match self {
            Widget::Table(t) => t.update(waits, changed),
            Widget::Label(t) => t.update(changed),
            Widget::Action(t) => t.update(changed),
            Widget::Button(t) => t.update(changed),
            Widget::Toggle(t) => t.update(changed),
            Widget::ComboBox(t) => t.update(changed),
            Widget::Container(t) => t.update(waits, changed),
            Widget::Grid(t) => t.update(waits, changed),
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        match self {
            Widget::Table(_) => (),
            Widget::Label(t) => t.update_var(name, value),
            Widget::Action(t) => t.update_var(name, value),
            Widget::Button(t) => t.update_var(name, value),
            Widget::Toggle(t) => t.update_var(name, value),
            Widget::ComboBox(t) => t.update_var(name, value),
            Widget::Container(t) => t.update_var(name, value),
            Widget::Grid(t) => t.update_var(name, value),
        }
    }
}

struct View {
    root: gtk::Box,
    widget: Widget,
}

impl View {
    fn new(ctx: WidgetCtx, spec: view::View) -> View {
        let selected_path = gtk::Label::new(None);
        selected_path.set_halign(gtk::Align::Start);
        selected_path.set_margin_start(5);
        selected_path.set_selectable(true);
        selected_path.set_single_line_mode(true);
        let widget =
            Widget::new(ctx, &spec.variables, spec.root.clone(), selected_path.clone());
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        if let Some(wroot) = widget.root() {
            root.add(wroot);
            root.set_child_packing(wroot, true, true, 1, gtk::PackType::Start);
        }
        root.add(&selected_path);
        root.set_child_packing(&selected_path, false, false, 1, gtk::PackType::End);
        View { root, widget }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        changed: &Arc<IndexMap<SubId, Value>>,
    ) {
        self.widget.update(waits, changed);
    }

    fn update_var(&self, name: &str, value: &Value) {
        self.widget.update_var(name, value)
    }
}

async fn netidx_main(
    cfg: Config,
    auth: Auth,
    mut updates: mpsc::Receiver<Batch>,
    mut state_updates: mpsc::UnboundedReceiver<(SubId, DvState)>,
    mut from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: mpsc::UnboundedSender<ToGui>,
    to_init: smpsc::Sender<(Subscriber, ResolverRead)>,
) {
    async fn read_view(rx_view: &mut Option<mpsc::Receiver<Batch>>) -> Option<Batch> {
        match rx_view {
            None => pending().await,
            Some(rx_view) => rx_view.next().await,
        }
    }
    let subscriber = Subscriber::new(cfg, auth).unwrap();
    let resolver = subscriber.resolver();
    let _: result::Result<_, _> = to_init.send((subscriber.clone(), resolver.clone()));
    let mut view_path: Option<Path> = None;
    let mut rx_view: Option<mpsc::Receiver<Batch>> = None;
    let mut _dv_view: Option<Dval> = None;
    let mut changed = IndexMap::new();
    let mut refresh = time::interval(Duration::from_secs(1)).fuse();
    let mut refreshing = false;
    loop {
        select_biased! {
            _ = refresh.next() => {
                if !refreshing && changed.len() > 0 {
                    refreshing = true;
                    let b = Arc::new(mem::replace(&mut changed, IndexMap::new()));
                    match to_gui.unbounded_send(ToGui::Update(b)) {
                        Ok(()) => (),
                        Err(e) => break
                    }
                }
            },
            b = updates.next() => if let Some(mut batch) = b {
                for (id, v) in batch.drain(..) {
                    changed.insert(id, v);
                }
            },
            s = state_updates.next() => if let Some((id, st)) = s {
                match st {
                    DvState::Subscribed => (),
                    DvState::Unsubscribed => {
                        changed.insert(id, Value::String(Chars::from("#SUB")));
                    }
                    DvState::FatalError(_) => {
                        changed.insert(id, Value::String(Chars::from("#ERR")));
                    }
                }
            },
            m = read_view(&mut rx_view).fuse() => match m {
                None => {
                    view_path = None;
                    rx_view = None;
                    _dv_view = None;
                },
                Some(mut batch) => if let Some((_, view)) = batch.pop() {
                    match view {
                        Value::String(s) => {
                            match serde_json::from_str::<protocol_view::View>(&*s) {
                                Err(e) => warn!("error parsing view definition {}", e),
                                Ok(v) => if let Some(path) = &view_path {
                                    match view::View::new(&resolver, v).await {
                                        Err(e) => warn!("failed to raeify view {}", e),
                                        Ok(v) => {
                                            let m = ToGui::View(path.clone(), v);
                                            match to_gui.unbounded_send(m) {
                                                Err(_) => break,
                                                Ok(()) => info!("updated gui view")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        v => warn!("unexpected type of view definition {}", v),
                    }
                }
            },
            m = from_gui.next() => match m {
                None => break,
                Some(FromGui::Updated) => { refreshing = false; },
                Some(FromGui::Navigate(base_path)) => {
                    match resolver.table(base_path.clone()).await {
                        Err(e) => warn!("can't fetch table spec for {}, {}", base_path, e),
                        Ok(spec) => {
                            let default = view::View {
                                variables: HashMap::new(),
                                root: view::Widget::Table(base_path.clone(), spec)
                            };
                            let m = ToGui::View(base_path.clone(), default);
                            match to_gui.unbounded_send(m) {
                                Err(_) => break,
                                Ok(()) => {
                                    let s = subscriber
                                        .durable_subscribe(base_path.append(".view"));
                                    let (tx, rx) = mpsc::channel(2);
                                    s.updates(true, tx);
                                    view_path = Some(base_path.clone());
                                    rx_view = Some(rx);
                                    _dv_view = Some(s);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn run_netidx(
    cfg: Config,
    auth: Auth,
    updates: mpsc::Receiver<Batch>,
    state_updates: mpsc::UnboundedReceiver<(SubId, DvState)>,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: mpsc::UnboundedSender<ToGui>,
) -> (Subscriber, ResolverRead) {
    let (tx, rx) = smpsc::channel();
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(netidx_main(cfg, auth, updates, state_updates, from_gui, to_gui, tx));
    });
    rx.recv().unwrap()
}

fn run_gui(
    ctx: WidgetCtx,
    app: &Application,
    mut to_gui: mpsc::UnboundedReceiver<ToGui>,
) {
    let main_context = glib::MainContext::default();
    let app = app.clone();
    let window = ApplicationWindow::new(&app);
    window.set_title("Netidx browser");
    window.set_default_size(800, 600);
    window.show_all();
    window.connect_destroy(move |_| process::exit(0));
    main_context.spawn_local_with_priority(PRIORITY_LOW, async move {
        let mut current: Option<View> = None;
        let mut waits = Vec::new();
        while let Some(m) = to_gui.next().await {
            match m {
                ToGui::Update(batch) => {
                    if let Some(root) = &current {
                        root.update(&mut waits, &batch);
                        for r in waits.drain(..) {
                            let _: result::Result<_, _> = r.await;
                        }
                        let _: result::Result<_, _> =
                            ctx.from_gui.unbounded_send(FromGui::Updated);
                    }
                }
                ToGui::View(path, view) => {
                    let cur = View::new(ctx.clone(), view);
                    if let Some(cur) = current.take() {
                        window.remove(cur.root());
                    }
                    window.set_title(&format!("Netidx Browser {}", path));
                    window.add(cur.root());
                    window.show_all();
                    current = Some(cur);
                    let m = ToGui::Update(Arc::new(IndexMap::new()));
                    let _: result::Result<_, _> = ctx.to_gui.unbounded_send(m);
                }
                ToGui::UpdateVar(name, value) => {
                    if let Some(root) = &current {
                        root.update_var(&name, &value);
                    }
                }
            }
        }
    })
}

pub(crate) fn run(cfg: Config, auth: Auth, path: Path) {
    let application = Application::new(Some("org.netidx.browser"), Default::default())
        .expect("failed to initialize GTK application");
    application.connect_activate(move |app| {
        let (tx_updates, rx_updates) = mpsc::channel(2);
        let (tx_state_updates, rx_state_updates) = mpsc::unbounded();
        let (tx_to_gui, rx_to_gui) = mpsc::unbounded();
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();
        // navigate to the initial location
        tx_from_gui.unbounded_send(FromGui::Navigate(path.clone())).unwrap();
        let (subscriber, resolver) = run_netidx(
            cfg.clone(),
            auth.clone(),
            rx_updates,
            rx_state_updates,
            rx_from_gui,
            tx_to_gui.clone(),
        );
        let ctx = WidgetCtx {
            subscriber,
            resolver,
            updates: tx_updates,
            state_updates: tx_state_updates,
            to_gui: tx_to_gui,
            from_gui: tx_from_gui,
        };
        run_gui(ctx, app, rx_to_gui)
    });
    application.run(&[]);
}
