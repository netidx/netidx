use futures::{
    channel::{mpsc, oneshot},
    future::{join_all, pending},
    prelude::*,
    select_biased,
};
use gdk::{keys, EventKey, RGBA};
use gio::prelude::*;
use glib::{
    self, clone,
    signal::Inhibit,
    source::{Continue, PRIORITY_LOW},
};
use gtk::{
    idle_add, prelude::*, Adjustment, Align, Application, ApplicationWindow,
    Box as GtkBox, CellRenderer, CellRendererText, Label, ListStore, Orientation,
    PackType, ScrolledWindow, SelectionMode, SortColumn, SortType, StateFlags,
    StyleContext, TreeIter, TreeModel, TreePath, TreeView, TreeViewColumn,
    TreeViewColumnSizing, Widget as GtkWidget,
};
use indexmap::IndexMap;
use log::{info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::Pooled,
    resolver::{self, Auth, ResolverRead},
    subscriber::{DvState, Dval, SubId, Subscriber, Value},
};
use netidx_protocols::view as protocol_view;
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{HashMap, HashSet},
    mem,
    ops::Drop,
    pin::Pin,
    process,
    rc::{Rc, Weak},
    result,
    sync::mpsc as smpsc,
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::{runtime::Runtime, time};
mod view;

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone)]
enum ToGui {
    View(Path, view::View),
    Update(Arc<IndexMap<SubId, Value>>),
}

#[derive(Debug, Clone)]
enum FromGui {
    Navigate(Path),
    Updated,
}

#[derive(Clone)]
struct WidgetCtx {
    subscriber: Subscriber,
    resolver: ResolverRead,
    updates: mpsc::Sender<Batch>,
    state_updates: mpsc::UnboundedSender<(SubId, DvState)>,
    to_gui: mpsc::UnboundedSender<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
}

struct Subscription {
    _sub: Dval,
    row: TreeIter,
    col: u32,
}

struct TableInner {
    ctx: WidgetCtx,
    root: GtkBox,
    view: TreeView,
    style: StyleContext,
    selected_path: Label,
    store: ListStore,
    by_id: RefCell<HashMap<SubId, Subscription>>,
    subscribed: RefCell<HashMap<String, HashSet<u32>>>,
    focus_column: RefCell<Option<TreeViewColumn>>,
    focus_row: RefCell<Option<String>>,
    descriptor: resolver::Table,
    vector_mode: bool,
    base_path: Path,
    sort_column: Cell<Option<u32>>,
    sort_temp_disabled: Cell<bool>,
}

impl Drop for TableInner {
    fn drop(&mut self) {
        self.view.set_model(None::<&ListStore>)
    }
}

#[derive(Clone)]
struct Table(Rc<TableInner>);

struct TableWeak(Weak<TableInner>);

impl clone::Downgrade for Table {
    type Weak = TableWeak;

    fn downgrade(&self) -> Self::Weak {
        TableWeak(Rc::downgrade(&self.0))
    }
}

impl clone::Upgrade for TableWeak {
    type Strong = Table;

    fn upgrade(&self) -> Option<Self::Strong> {
        Weak::upgrade(&self.0).map(Table)
    }
}

fn get_sort_column(store: &ListStore) -> Option<u32> {
    match store.get_sort_column_id() {
        None | Some((SortColumn::Default, _)) => None,
        Some((SortColumn::Index(c), _)) => {
            if c == 0 {
                None
            } else {
                Some(c)
            }
        }
    }
}

fn compare_row(col: i32, m: &TreeModel, r0: &TreeIter, r1: &TreeIter) -> Ordering {
    let v0_v = m.get_value(r0, col);
    let v1_v = m.get_value(r1, col);
    let v0_r = v0_v.get::<&str>();
    let v1_r = v1_v.get::<&str>();
    match (v0_r, v1_r) {
        (Err(_), Err(_)) => Ordering::Equal,
        (Err(_), _) => Ordering::Greater,
        (_, Err(_)) => Ordering::Less,
        (Ok(None), Ok(None)) => Ordering::Equal,
        (Ok(None), _) => Ordering::Less,
        (_, Ok(None)) => Ordering::Greater,
        (Ok(Some(v0)), Ok(Some(v1))) => match (v0.parse::<f64>(), v1.parse::<f64>()) {
            (Ok(v0f), Ok(v1f)) => v0f.partial_cmp(&v1f).unwrap_or(Ordering::Equal),
            (_, _) => v0.cmp(v1),
        },
    }
}

impl Table {
    fn new(ctx: WidgetCtx, base_path: Path, mut descriptor: resolver::Table) -> Table {
        let view = TreeView::new();
        let tablewin = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        let root = GtkBox::new(Orientation::Vertical, 5);
        let selected_path = Label::new(None);
        selected_path.set_halign(Align::Start);
        selected_path.set_margin_start(5);
        tablewin.add(&view);
        root.add(&tablewin);
        root.set_child_packing(&tablewin, true, true, 1, PackType::Start);
        root.set_child_packing(&selected_path, false, false, 1, PackType::End);
        root.add(&selected_path);
        selected_path.set_selectable(true);
        selected_path.set_single_line_mode(true);
        let nrows = descriptor.rows.len();
        descriptor.rows.sort();
        descriptor.cols.sort_by_key(|(p, _)| p.clone());
        descriptor.cols.retain(|(_, i)| i.0 >= (nrows / 2) as u64);
        view.get_selection().set_mode(SelectionMode::None);
        let vector_mode = descriptor.cols.len() == 0;
        let column_types = if vector_mode {
            vec![String::static_type(); 2]
        } else {
            (0..descriptor.cols.len() + 1)
                .into_iter()
                .map(|_| String::static_type())
                .collect::<Vec<_>>()
        };
        let store = ListStore::new(&column_types);
        for row in descriptor.rows.iter() {
            let row_name = Path::basename(row).unwrap_or("").to_value();
            let row = store.append();
            store.set_value(&row, 0, &row_name.to_value());
        }
        let style = view.get_style_context();
        let ncols = if vector_mode { 1 } else { descriptor.cols.len() };
        let t = Table(Rc::new(TableInner {
            ctx,
            root,
            view,
            selected_path,
            store,
            descriptor,
            vector_mode,
            base_path,
            style,
            by_id: RefCell::new(HashMap::new()),
            subscribed: RefCell::new(HashMap::new()),
            focus_column: RefCell::new(None),
            focus_row: RefCell::new(None),
            sort_column: Cell::new(None),
            sort_temp_disabled: Cell::new(false),
        }));
        t.view().append_column(&{
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("name");
            column.add_attribute(&cell, "text", 0);
            column.set_sort_column_id(0);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            column
        });
        for col in 0..ncols {
            let id = (col + 1) as i32;
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            let f = Box::new(clone!(@weak t =>
                move |c: &TreeViewColumn,
                      cr: &CellRenderer,
                      _: &TreeModel,
                      i: &TreeIter| t.render_cell(id, c, cr, i)));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
            column.set_title(&if vector_mode {
                Path::from("value")
            } else {
                t.0.descriptor.cols[col].0.clone()
            });
            t.store().set_sort_func(SortColumn::Index(id as u32), move |m, r0, r1| {
                compare_row(id, m, r0, r1)
            });
            column.set_sort_column_id(id);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            t.view().append_column(&column);
        }
        t.store().connect_sort_column_changed(clone!(@weak t => move |_| {
            if t.0.sort_temp_disabled.get() {
                return
            }
            match get_sort_column(t.store()) {
                Some(col) => { t.0.sort_column.set(Some(col)); }
                None => {
                    t.0.sort_column.set(None);
                    t.store().set_unsorted();
                }
            }
            idle_add(clone!(@weak t => @default-return Continue(false), move || {
                t.update_subscriptions();
                Continue(false)
            }));
        }));
        t.view().set_fixed_height_mode(true);
        t.view().set_model(Some(t.store()));
        t.view()
            .connect_row_activated(clone!(@weak t => move |_, p, _| t.row_activated(p)));
        t.view().connect_key_press_event(clone!(
            @weak t => @default-return Inhibit(false), move |_, k| t.handle_key(k)));
        t.view().connect_cursor_changed(clone!(@weak t => move |_| t.cursor_changed()));
        tablewin.get_vadjustment().map(|va| {
            va.connect_value_changed(clone!(@weak t => move |_| {
                idle_add(clone!(@weak t => @default-return Continue(false), move || {
                    t.update_subscriptions();
                    Continue(false)
                }));
            }));
        });
        t
    }

    fn render_cell(&self, id: i32, c: &TreeViewColumn, cr: &CellRenderer, i: &TreeIter) {
        let cr = cr.clone().downcast::<CellRendererText>().unwrap();
        let rn_v = self.store().get_value(i, 0);
        let rn = rn_v.get::<&str>();
        cr.set_property_text(match self.store().get_value(i, id).get::<&str>() {
            Ok(v) => v,
            _ => return,
        });
        match (&*self.0.focus_column.borrow(), &*self.0.focus_row.borrow(), rn) {
            (Some(fc), Some(fr), Ok(Some(rn))) if fc == c && fr.as_str() == rn => {
                let st = StateFlags::SELECTED;
                let fg = self.0.style.get_color(st);
                let bg =
                    StyleContextExt::get_property(&self.0.style, "background-color", st);
                let bg = bg.get::<RGBA>().unwrap();
                cr.set_property_cell_background_rgba(bg.as_ref());
                cr.set_property_foreground_rgba(Some(&fg));
            }
            _ => {
                cr.set_property_cell_background(None);
                cr.set_property_foreground(None);
            }
        }
    }

    fn handle_key(&self, key: &EventKey) -> Inhibit {
        if key.get_keyval() == keys::constants::BackSpace {
            let path = Path::dirname(&self.0.base_path).unwrap_or("/");
            let m = FromGui::Navigate(Path::from(String::from(path)));
            let _: result::Result<_, _> = self.0.ctx.from_gui.unbounded_send(m);
        }
        if key.get_keyval() == keys::constants::Escape {
            // unset the focus
            self.view().set_cursor::<TreeViewColumn>(&TreePath::new(), None, false);
            *self.0.focus_column.borrow_mut() = None;
            *self.0.focus_row.borrow_mut() = None;
            self.0.selected_path.set_label("");
        }
        Inhibit(false)
    }

    fn row_activated(&self, path: &TreePath) {
        if let Some(row) = self.store().get_iter(&path) {
            let row_name = self.store().get_value(&row, 0);
            if let Ok(Some(row_name)) = row_name.get::<&str>() {
                let path = self.0.base_path.append(row_name);
                let _: result::Result<_, _> =
                    self.0.ctx.from_gui.unbounded_send(FromGui::Navigate(path));
            }
        }
    }

    fn cursor_changed(&self) {
        let (p, c) = self.view().get_cursor();
        let row_name = match p {
            None => None,
            Some(p) => match self.store().get_iter(&p) {
                None => None,
                Some(i) => Some(self.store().get_value(&i, 0)),
            },
        };
        let path = match row_name {
            None => Path::from(""),
            Some(row_name) => match row_name.get::<&str>().ok().flatten() {
                None => Path::from(""),
                Some(row_name) => {
                    *self.0.focus_column.borrow_mut() = c.clone();
                    *self.0.focus_row.borrow_mut() = Some(String::from(row_name));
                    let col_name = if self.0.vector_mode {
                        None
                    } else if self.view().get_column(0) == c {
                        None
                    } else {
                        c.as_ref().and_then(|c| c.get_title())
                    };
                    match col_name {
                        None => self.0.base_path.append(row_name),
                        Some(col_name) => {
                            self.0.base_path.append(row_name).append(col_name.as_str())
                        }
                    }
                }
            },
        };
        self.0.selected_path.set_label(&*path);
        let (mut start, end) = match self.view().get_visible_range() {
            None => return,
            Some((s, e)) => (s, e),
        };
        self.view().columns_autosize();
        while start <= end {
            if let Some(i) = self.store().get_iter(&start) {
                self.store().row_changed(&start, &i);
            }
            start.next();
        }
    }

    fn update_subscriptions(&self) {
        let ncols = if self.0.vector_mode { 1 } else { self.0.descriptor.cols.len() };
        let (mut start, mut end) = match self.view().get_visible_range() {
            Some((s, e)) => (s, e),
            None => {
                let t = self;
                idle_add(clone!(@weak t => @default-return Continue(false), move || {
                    t.update_subscriptions();
                    Continue(false)
                }));
                return;
            }
        };
        for _ in 0..50 {
            start.prev();
            end.next();
        }
        // unsubscribe invisible rows
        self.0.by_id.borrow_mut().retain(|_, v| match self.store().get_path(&v.row) {
            None => false,
            Some(p) => {
                let visible =
                    (p >= start && p <= end) || (Some(v.col) == self.0.sort_column.get());
                if !visible {
                    let row_name_v = self.store().get_value(&v.row, 0);
                    if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                        let mut subscribed = self.0.subscribed.borrow_mut();
                        if let Some(set) = subscribed.get_mut(row_name) {
                            set.remove(&v.col);
                            if set.is_empty() {
                                subscribed.remove(row_name);
                            }
                        }
                    }
                }
                visible
            }
        });
        let maybe_subscribe_col = |row: &TreeIter, row_name: &str, id: u32| {
            let mut subscribed = self.0.subscribed.borrow_mut();
            if !subscribed.get(row_name).map(|s| s.contains(&id)).unwrap_or(false) {
                subscribed.entry(row_name.into()).or_insert_with(HashSet::new).insert(id);
                let p = self.0.base_path.append(row_name);
                let p = if self.0.vector_mode {
                    p
                } else {
                    p.append(&self.0.descriptor.cols[(id - 1) as usize].0)
                };
                let s = self.0.ctx.subscriber.durable_subscribe(p);
                s.updates(true, self.0.ctx.updates.clone());
                s.state_updates(true, self.0.ctx.state_updates.clone());
                self.0.by_id.borrow_mut().insert(
                    s.id(),
                    Subscription { _sub: s, row: row.clone(), col: id as u32 },
                );
            }
        };
        // subscribe to all the visible rows
        while start < end {
            if let Some(row) = self.store().get_iter(&start) {
                let row_name_v = self.store().get_value(&row, 0);
                if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                    for col in 0..ncols {
                        maybe_subscribe_col(&row, row_name, (col + 1) as u32);
                    }
                }
            }
            start.next();
        }
        // subscribe to all rows in the sort column
        if let Some(id) = self.0.sort_column.get() {
            if let Some(row) = self.store().get_iter_first() {
                loop {
                    let row_name_v = self.store().get_value(&row, 0);
                    if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                        maybe_subscribe_col(&row, row_name, id);
                    }
                    if !self.store().iter_next(&row) {
                        break;
                    }
                }
            }
        }
        self.cursor_changed();
    }

    fn disable_sort(&self) -> Option<(SortColumn, SortType)> {
        self.0.sort_temp_disabled.set(true);
        let col = self.store().get_sort_column_id();
        self.view().freeze_child_notify();
        self.store().set_unsorted();
        self.0.sort_temp_disabled.set(false);
        col
    }

    fn enable_sort(&self, col: Option<(SortColumn, SortType)>) {
        self.0.sort_temp_disabled.set(true);
        if let Some((col, dir)) = col {
            if self.0.sort_column.get().is_some() {
                self.store().set_sort_column_id(col, dir);
            }
        }
        self.0.sort_temp_disabled.set(false);
    }

    fn root(&self) -> &GtkWidget {
        self.0.root.upcast_ref()
    }

    fn view(&self) -> &TreeView {
        &self.0.view
    }

    fn store(&self) -> &ListStore {
        &self.0.store
    }

    async fn update(&self, changed: Arc<IndexMap<SubId, Value>>) {
        let (tx, rx) = oneshot::channel();
        let mut tx = Some(tx);
        let sctx = self.disable_sort();
        idle_add({
            let t = self.clone();
            let mut i = 0;
            move || {
                let mut n = 0;
                while n < 10000 && i < changed.len() {
                    let (id, v) = changed.get_index(i).unwrap();
                    if let Some(sub) = t.0.by_id.borrow().get(id) {
                        let s = &format!("{}", v).to_value();
                        t.store().set_value(&sub.row, sub.col, s);
                    };
                    i += 1;
                    n += 1;
                }
                if i < changed.len() {
                    Continue(true)
                } else {
                    if let Some(tx) = tx.take() {
                        let _: result::Result<_, _> = tx.send(());
                    }
                    Continue(false)
                }
            }
        });
        let _: result::Result<_, _> = rx.await;
        self.enable_sort(sctx);
        self.update_subscriptions();
    }
}

struct Container {
    spec: view::Container,
    root: GtkBox,
    children: Vec<Widget>,
}

impl Container {
    fn new(ctx: WidgetCtx, spec: view::Container) -> Container {
        let dir = match spec.direction {
            view::Direction::Horizontal => Orientation::Horizontal,
            view::Direction::Vertical => Orientation::Vertical,
        };
        let root = GtkBox::new(dir, 5);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            let w = Widget::new(ctx.clone(), s.clone());
            if let Some(r) = w.root() {
                root.add(r);
            }
            children.push(w);
        }
        Container { spec, root, children }
    }

    async fn update(&self, updates: Arc<IndexMap<SubId, Value>>) {
        join_all(self.children.iter().map(|c| c.update(updates.clone()))).await;
    }

    fn root(&self) -> &GtkWidget {
        self.root.upcast_ref()
    }
}

enum Widget {
    Table(Table),
    Container(Container),
}

impl Widget {
    fn new(ctx: WidgetCtx, spec: view::Widget) -> Widget {
        match spec {
            view::Widget::StaticTable(_) => todo!(),
            view::Widget::Table(base_path, spec) => {
                Widget::Table(Table::new(ctx.clone(), base_path, spec))
            }
            view::Widget::Label(_) => todo!(),
            view::Widget::Action(_) => todo!(),
            view::Widget::Button(_) => todo!(),
            view::Widget::Toggle(_) => todo!(),
            view::Widget::ComboBox(_) => todo!(),
            view::Widget::Radio(_) => todo!(),
            view::Widget::Entry(_) => todo!(),
            view::Widget::Container(s) => Widget::Container(Container::new(ctx, s)),
        }
    }

    fn root(&self) -> Option<&GtkWidget> {
        match self {
            Widget::Table(t) => Some(t.root()),
            Widget::Container(t) => Some(t.root()),
        }
    }

    fn update<'a>(
        &'a self,
        changed: Arc<IndexMap<SubId, Value>>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            match self {
                Widget::Table(t) => t.update(changed).await,
                Widget::Container(c) => c.update(changed).await,
            }
        })
    }
}

struct View {
    spec: view::View,
    root: Widget,
}

impl View {
    fn new(ctx: WidgetCtx, spec: view::View) -> View {
        let root = Widget::new(ctx, spec.root.clone());
        View { spec, root }
    }

    fn root(&self) -> Option<&GtkWidget> {
        self.root.root()
    }

    async fn update(&self, changed: Arc<IndexMap<SubId, Value>>) {
        self.root.update(changed).await;
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
                                scripts: vec![],
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
        while let Some(m) = to_gui.next().await {
            match m {
                ToGui::Update(batch) => {
                    if let Some(root) = &current {
                        root.update(batch).await;
                        let _: result::Result<_, _> =
                            ctx.from_gui.unbounded_send(FromGui::Updated);
                    }
                }
                ToGui::View(path, view) => {
                    let cur = View::new(ctx.clone(), view);
                    if let Some(root) = cur.root() {
                        if let Some(cur) = current.take() {
                            if let Some(r) = cur.root() {
                                window.remove(r);
                            }
                        }
                        window.set_title(&format!("Netidx Browser {}", path));
                        window.add(root);
                        window.show_all();
                        current = Some(cur);
                        let m = ToGui::Update(Arc::new(IndexMap::new()));
                        let _: result::Result<_, _> = ctx.to_gui.unbounded_send(m);
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
