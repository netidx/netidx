use futures::{channel::mpsc, prelude::*, select_biased};
use gdk::{keys, EventKey};
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
    TreeViewColumnSizing,
};
use log::error;
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::Pooled,
    resolver::{Auth, Table},
    subscriber::{DvState, Dval, SubId, Subscriber, Value},
};
use std::{
    cell::RefCell,
    cmp::{min, Ordering},
    collections::{HashMap, HashSet},
    mem,
    rc::{Rc, Weak},
    thread,
    time::Duration,
    process,
};
use tokio::{runtime::Runtime, time};

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone)]
enum ToGui {
    Table(Subscriber, Path, Table),
    Refresh(Option<Vec<(SubId, Value)>>),
}

#[derive(Debug, Clone)]
enum FromGui {
    Navigate(Path),
    Refreshed,
}

struct Subscription {
    _sub: Dval,
    row: TreeIter,
    col: u32,
}

struct NetidxTableInner {
    subscriber: Subscriber,
    updates: mpsc::Sender<Batch>,
    state_updates: mpsc::UnboundedSender<(SubId, DvState)>,
    to_gui: mpsc::UnboundedSender<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
    root: GtkBox,
    view: TreeView,
    style: StyleContext,
    selected_path: Label,
    store: ListStore,
    by_id: HashMap<SubId, Subscription>,
    subscribed: HashMap<String, HashSet<u32>>,
    focus_column: Option<TreeViewColumn>,
    focus_row: Option<String>,
    descriptor: Table,
    vector_mode: bool,
    base_path: Path,
    sort_column: Option<u32>,
    sort_temp_disabled: bool,
}

#[derive(Clone)]
struct NetidxTable(Rc<RefCell<NetidxTableInner>>);

struct NetidxTableWeak(Weak<RefCell<NetidxTableInner>>);

impl clone::Downgrade for NetidxTable {
    type Weak = NetidxTableWeak;

    fn downgrade(&self) -> Self::Weak {
        NetidxTableWeak(Rc::downgrade(&self.0))
    }
}

impl clone::Upgrade for NetidxTableWeak {
    type Strong = NetidxTable;

    fn upgrade(&self) -> Option<Self::Strong> {
        Weak::upgrade(&self.0).map(NetidxTable)
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

impl NetidxTable {
    fn new(
        subscriber: Subscriber,
        base_path: Path,
        mut descriptor: Table,
        updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
        state_updates: mpsc::UnboundedSender<(SubId, DvState)>,
        to_gui: mpsc::UnboundedSender<ToGui>,
        from_gui: mpsc::UnboundedSender<FromGui>,
    ) -> NetidxTable {
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
        let t = NetidxTable(Rc::new(RefCell::new(NetidxTableInner {
            subscriber,
            root,
            view,
            selected_path,
            store,
            descriptor,
            vector_mode,
            base_path,
            to_gui,
            from_gui,
            updates,
            state_updates,
            style,
            by_id: HashMap::new(),
            subscribed: HashMap::new(),
            focus_column: None,
            focus_row: None,
            sort_column: None,
            sort_temp_disabled: false,
        })));
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
                t.0.borrow().descriptor.cols[col].0.clone()
            });
            t.store().set_sort_func(SortColumn::Index(id as u32), move |m, r0, r1| {
                compare_row(id, m, r0, r1)
            });
            column.set_sort_column_id(id);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            t.view().append_column(&column);
        }
        t.store().connect_sort_column_changed(clone!(@weak t => move |_| {
            if t.0.borrow().sort_temp_disabled {
                return
            }
            match get_sort_column(&t.store()) {
                Some(col) => { t.0.borrow_mut().sort_column = Some(col); }
                None => {
                    t.0.borrow_mut().sort_column = None;
                    t.store().set_unsorted();
                }
            }
            let _ = t.0.borrow().to_gui.unbounded_send(ToGui::Refresh(None));
        }));
        t.view().set_fixed_height_mode(true);
        t.view().set_model(Some(&t.store()));
        t.view()
            .connect_row_activated(clone!(@weak t => move |_, p, _| t.row_activated(p)));
        t.view().connect_key_press_event(clone!(
            @weak t => @default-return Inhibit(false), move |_, k| t.handle_key(k)));
        t.view().connect_cursor_changed(clone!(@weak t => move |_| t.cursor_changed()));
        tablewin.get_vadjustment().map(|va| {
            va.connect_value_changed(clone!(@weak t => move |_| {
                let _ = t.0.borrow().to_gui.unbounded_send(ToGui::Refresh(None));
            }));
        });
        t
    }

    fn render_cell(&self, id: i32, c: &TreeViewColumn, cr: &CellRenderer, i: &TreeIter) {
        let inner = self.0.borrow();
        let cr = cr.clone().downcast::<CellRendererText>().unwrap();
        let rn_v = inner.store.get_value(i, 0);
        let rn = rn_v.get::<&str>();
        cr.set_property_text(match inner.store.get_value(i, id).get::<&str>() {
            Ok(it) => it,
            _ => return,
        });
        match (&inner.focus_column, &inner.focus_row, rn) {
            (Some(fc), Some(fr), Ok(Some(rn))) if fc == c && fr.as_str() == rn => {
                let st = StateFlags::SELECTED;
                let fg = inner.style.get_color(st);
                let bg = inner.style.get_background_color(st);
                cr.set_property_cell_background_rgba(Some(&bg));
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
            let base_path = self.0.borrow().base_path.clone();
            let path = Path::dirname(&base_path).unwrap_or("/");
            let m = FromGui::Navigate(Path::from(String::from(path)));
            let _ = self.0.borrow_mut().from_gui.unbounded_send(m);
        }
        if key.get_keyval() == keys::constants::Escape {
            // unset the focus
            self.view().set_cursor::<TreeViewColumn>(&TreePath::new(), None, false);
            self.0.borrow_mut().focus_column = None;
            self.0.borrow_mut().focus_row = None;
            self.0.borrow().selected_path.set_label("");
        }
        Inhibit(false)
    }

    fn row_activated(&self, path: &TreePath) {
        let mut inner = self.0.borrow_mut();
        let t = &mut *inner;
        if let Some(row) = t.store.get_iter(&path) {
            let row_name = t.store.get_value(&row, 0);
            if let Ok(Some(row_name)) = row_name.get::<&str>() {
                let path = t.base_path.append(row_name);
                let _ = t.from_gui.unbounded_send(FromGui::Navigate(path));
            }
        }
    }

    fn cursor_changed(&self) {
        let (store, view, mut start, end) = {
            let mut inner = self.0.borrow_mut();
            let t = &mut *inner;
            let (p, c) = t.view.get_cursor();
            let row_name = match p {
                None => None,
                Some(p) => match t.store.get_iter(&p) {
                    None => None,
                    Some(i) => Some(t.store.get_value(&i, 0)),
                },
            };
            let path = match row_name {
                None => Path::from(""),
                Some(row_name) => match row_name.get::<&str>().ok().flatten() {
                    None => Path::from(""),
                    Some(row_name) => {
                        t.focus_column = c.clone();
                        t.focus_row = Some(String::from(row_name));
                        let col_name = if t.vector_mode {
                            None
                        } else if t.view.get_column(0) == c {
                            None
                        } else {
                            c.as_ref().and_then(|c| c.get_title())
                        };
                        match col_name {
                            None => t.base_path.append(row_name),
                            Some(col_name) => {
                                t.base_path.append(row_name).append(col_name.as_str())
                            }
                        }
                    }
                },
            };
            t.selected_path.set_label(&*path);
            let (start, end) = match t.view.get_visible_range() {
                None => return,
                Some((s, e)) => (s, e),
            };
            (t.store.clone(), t.view.clone(), start, end)
        };
        view.columns_autosize();
        while start <= end {
            if let Some(i) = store.get_iter(&start) {
                store.row_changed(&start, &i);
            }
            start.next();
        }
    }

    fn update_subscriptions(&self) {
        {
            let mut inner = self.0.borrow_mut();
            let t = &mut *inner;
            let by_id = &mut t.by_id;
            let store = &t.store;
            let subscribed = &mut t.subscribed;
            let subscriber = &t.subscriber;
            let updates = &t.updates;
            let state_updates = &t.state_updates;
            let descriptor = &t.descriptor;
            let base_path = &t.base_path;
            let vector_mode = t.vector_mode;
            let sort_column = t.sort_column;
            let ncols = if t.vector_mode { 1 } else { t.descriptor.cols.len() };
            let (mut start, mut end) = match t.view.get_visible_range() {
                None => {
                    let _ = t.to_gui.unbounded_send(ToGui::Refresh(None));
                    return;
                }
                Some((s, e)) => (s, e),
            };
            for _ in 0..50 {
                start.prev();
                end.next();
            }
            // unsubscribe invisible rows
            by_id.retain(|_, v| match store.get_path(&v.row) {
                None => false,
                Some(p) => {
                    let visible =
                        (p >= start && p <= end) || (Some(v.col) == sort_column);
                    if !visible {
                        let row_name_v = store.get_value(&v.row, 0);
                        if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
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
            let mut maybe_subscribe_col = |row: &TreeIter, row_name: &str, id: u32| {
                if !subscribed.get(row_name).map(|s| s.contains(&id)).unwrap_or(false) {
                    subscribed
                        .entry(row_name.into())
                        .or_insert_with(HashSet::new)
                        .insert(id);
                    let p = base_path.append(row_name);
                    let p = if vector_mode {
                        p
                    } else {
                        p.append(&descriptor.cols[(id - 1) as usize].0)
                    };
                    let s = subscriber.durable_subscribe(p);
                    s.updates(true, updates.clone());
                    s.state_updates(true, state_updates.clone());
                    by_id.insert(
                        s.id(),
                        Subscription { _sub: s, row: row.clone(), col: id as u32 },
                    );
                }
            };
            // subscribe to all the visible rows
            while start < end {
                if let Some(row) = store.get_iter(&start) {
                    let row_name_v = store.get_value(&row, 0);
                    if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                        for col in 0..ncols {
                            maybe_subscribe_col(&row, row_name, (col + 1) as u32);
                        }
                    }
                }
                start.next();
            }
            // subscribe to all rows in the sort column
            if let Some(id) = sort_column {
                if let Some(row) = store.get_iter_first() {
                    loop {
                        let row_name_v = store.get_value(&row, 0);
                        if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                            maybe_subscribe_col(&row, row_name, id);
                        }
                        if !store.iter_next(&row) {
                            break;
                        }
                    }
                }
            }
        }
        self.cursor_changed();
    }

    fn disable_sort(&self) -> Option<(SortColumn, SortType)> {
        self.0.borrow_mut().sort_temp_disabled = true;
        let col = self.store().get_sort_column_id();
        self.view().freeze_child_notify();
        self.store().set_unsorted();
        self.0.borrow_mut().sort_temp_disabled = false;
        col
    }

    fn enable_sort(&self, col: Option<(SortColumn, SortType)>) {
        self.0.borrow_mut().sort_temp_disabled = true;
        if let Some((col, dir)) = col {
            if self.0.borrow().sort_column.is_some() {
                self.store().set_sort_column_id(col, dir);
            }
        }
        self.view().thaw_child_notify();
        self.0.borrow_mut().sort_temp_disabled = false;
    }

    fn root(&self) -> GtkBox {
        self.0.borrow().root.clone()
    }

    fn view(&self) -> TreeView {
        self.0.borrow().view.clone()
    }

    fn store(&self) -> ListStore {
        self.0.borrow().store.clone()
    }
}

async fn netidx_main(
    cfg: Config,
    auth: Auth,
    mut updates: mpsc::Receiver<Batch>,
    mut state_updates: mpsc::UnboundedReceiver<(SubId, DvState)>,
    to_gui: mpsc::UnboundedSender<ToGui>,
    mut from_gui: mpsc::UnboundedReceiver<FromGui>,
) {
    let subscriber = Subscriber::new(cfg, auth).expect("failed to create subscriber");
    let resolver = subscriber.resolver();
    let mut changed = HashMap::new();
    let mut refresh = time::interval(Duration::from_secs(1)).fuse();
    let mut refreshing = false;
    loop {
        select_biased! {
            _ = refresh.next() => {
                if !refreshing && changed.len() > 0 {
                    refreshing = true;
                    let b = mem::replace(&mut changed, HashMap::new());
                    let b = b.into_iter().collect::<Vec<_>>();
                    let m = ToGui::Refresh(Some(b));
                    match to_gui.unbounded_send(m) {
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
            m = from_gui.next() => match m {
                None => break,
                Some(FromGui::Refreshed) => { refreshing = false; },
                Some(FromGui::Navigate(path)) => {
                    let table = match resolver.table(path.clone()).await {
                        Ok(table) => table,
                        Err(e) => {
                            error!("can't load path {}", e);
                            continue
                        }
                    };
                    let m = ToGui::Table(subscriber.clone(), path, table);
                    match to_gui.unbounded_send(m) {
                        Err(_) => break,
                        Ok(()) => ()
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
    to_gui: mpsc::UnboundedSender<ToGui>,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
) {
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(netidx_main(cfg, auth, updates, state_updates, to_gui, from_gui));
    });
}

fn run_gui(
    app: &Application,
    updates: mpsc::Sender<Batch>,
    state_updates: mpsc::UnboundedSender<(SubId, DvState)>,
    tx_to_gui: mpsc::UnboundedSender<ToGui>,
    mut to_gui: mpsc::UnboundedReceiver<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
) {
    let main_context = glib::MainContext::default();
    let app = app.clone();
    let window = ApplicationWindow::new(&app);
    let refreshing = Rc::new(RefCell::new(None));
    window.set_title("Netidx browser");
    window.set_default_size(800, 600);
    window.show_all();
    window.connect_destroy(move |_| process::exit(0));
    main_context.spawn_local_with_priority(PRIORITY_LOW, async move {
        let mut current: Option<NetidxTable> = None;
        while let Some(m) = to_gui.next().await {
            match m {
                ToGui::Refresh(mut batch) => {
                    if let (None, Some(_)) = (&batch, &*refreshing.borrow()) {
                        continue;
                    }
                    let sav = batch
                        .as_ref()
                        .and_then(|_| current.as_ref().and_then(|t| t.disable_sort()));
                    *refreshing.borrow_mut() = Some(idle_add({
                        let current = current.clone();
                        let from_gui = from_gui.clone();
                        let refreshing = refreshing.clone();
                        move || {
                            if let Some(t) = &current {
                                if let Some(batch) = &mut batch {
                                    for _ in 0..min(batch.len(), 10000) {
                                        let (id, v) = batch.pop().unwrap();
                                        let (r, c) = match t.0.borrow().by_id.get(&id) {
                                            None => continue,
                                            Some(sub) => (sub.row.clone(), sub.col),
                                        };
                                        let s = &format!("{}", v).to_value();
                                        t.store().set_value(&r, c, s);
                                    }
                                    if batch.len() > 0 {
                                        return Continue(true);
                                    }
                                    t.enable_sort(sav);
                                    let _ = from_gui.unbounded_send(FromGui::Refreshed);
                                }
                                t.update_subscriptions();
                            }
                            *refreshing.borrow_mut() = None;
                            Continue(false)
                        }
                    }));
                }
                ToGui::Table(subscriber, path, table) => {
                    if let Some(cur) = current.take() {
                        window.remove(&cur.root());
                        cur.view().set_model(None::<&ListStore>);
                    }
                    window.set_title(&format!("Netidx Browser {}", path));
                    let cur = NetidxTable::new(
                        subscriber,
                        path,
                        table,
                        updates.clone(),
                        state_updates.clone(),
                        tx_to_gui.clone(),
                        from_gui.clone(),
                    );
                    window.add(&cur.0.borrow().root);
                    window.show_all();
                    current = Some(cur);
                    let _ = tx_to_gui.unbounded_send(ToGui::Refresh(None));
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
        run_netidx(
            cfg.clone(),
            auth.clone(),
            rx_updates,
            rx_state_updates,
            tx_to_gui.clone(),
            rx_from_gui,
        );
        run_gui(app, tx_updates, tx_state_updates, tx_to_gui, rx_to_gui, tx_from_gui)
    });
    application.run(&[]);
}
