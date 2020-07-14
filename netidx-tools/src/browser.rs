use futures::{channel::mpsc, prelude::*, select_biased};
use gdk::{keys, EventKey};
use gio::prelude::*;
use glib::{self, clone, prelude::*, signal::Inhibit, subclass::prelude::*};
use gtk::{
    prelude::*, Adjustment, Align, Application, ApplicationWindow, Box as GtkBox,
    CellLayout, CellRenderer, CellRendererText, Label, ListStore, Orientation, PackType,
    ScrolledWindow, StateFlags, TreeIter, TreeModel, TreePath, TreeStore, TreeView,
    TreeViewColumn, TreeViewColumnSizing,
};
use log::{debug, error, info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::Pooled,
    resolver::{Auth, Table},
    subscriber::{Dval, SubId, Subscriber, Value},
};
use std::{
    cell::{Cell, RefCell},
    cmp::max,
    collections::{BTreeSet, HashMap},
    iter,
    rc::Rc,
    thread,
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    time::{self, Instant},
};

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone)]
enum ToGui {
    Table(Subscriber, Path, Table),
    Batch(Batch),
    Refresh,
}

#[derive(Debug, Clone)]
enum FromGui {
    Navigate(Path),
}

struct Subscription {
    sub: Dval,
    row: TreeIter,
    col: u32,
}

struct NetidxTable {
    root: GtkBox,
    view: TreeView,
    store: ListStore,
    by_id: Rc<RefCell<HashMap<SubId, Subscription>>>,
    update_subscriptions: Rc<dyn Fn()>,
}

impl NetidxTable {
    fn new(
        subscriber: Subscriber,
        base_path: Path,
        mut descriptor: Table,
        updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
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
        root.set_child_packing(&tablewin, true, true, 2, PackType::Start);
        root.set_child_packing(&selected_path, false, false, 2, PackType::End);
        root.add(&selected_path);
        selected_path.set_selectable(true);
        selected_path.set_single_line_mode(true);
        let nrows = descriptor.rows.len();
        descriptor.rows.sort();
        descriptor.cols.sort_by_key(|(p, _)| p.clone());
        descriptor.cols.retain(|(_, i)| i.0 >= (nrows / 2) as u64);
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
        let descriptor = Rc::new(descriptor);
        let by_id: Rc<RefCell<HashMap<SubId, Subscription>>> =
            Rc::new(RefCell::new(HashMap::new()));
        let style = view.get_style_context();
        let focus_column: Rc<RefCell<Option<TreeViewColumn>>> =
            Rc::new(RefCell::new(None));
        let sort_column: Rc<RefCell<Option<i32>>> = Rc::new(RefCell::new(None));
        let cursor_changed = Rc::new(clone!(
            @weak focus_column, @weak store, @weak selected_path, @strong base_path =>
            move |v: &TreeView| {
                let (p, c) = v.get_cursor();
                let row_name = match p {
                    None => None,
                    Some(p) => match store.get_iter(&p) {
                        None => None,
                        Some(i) => Some(store.get_value(&i, 0))
                    }
                };
                let path = match row_name {
                    None => Path::from(""),
                    Some(row_name) => match row_name.get::<&str>().ok().flatten() {
                        None => Path::from(""),
                        Some(row_name) => {
                            let col_name = if vector_mode {
                                None
                            } else if v.get_column(0) == c {
                                None
                            } else {
                                c.as_ref().and_then(|c| c.get_title())
                            };
                            match col_name {
                                None => base_path.append(row_name),
                                Some(col_name) =>
                                    base_path.append(row_name).append(col_name.as_str()),
                            }
                        }
                    }
                };
                selected_path.set_label(&*path);
                *focus_column.borrow_mut() = c;
                v.columns_autosize();
                let (mut start, end) = match v.get_visible_range() {
                    None => return,
                    Some((s, e)) => (s, e)
                };
                while start <= end {
                    if let Some(i) = store.get_iter(&start) {
                        store.row_changed(&start, &i);
                    }
                    start.next();
                }
            }
        ));
        let update_subscriptions = Rc::new({
            let base_path = base_path.clone();
            let view = view.downgrade();
            let store = store.downgrade();
            let by_id = by_id.clone();
            let cursor_changed = Rc::clone(&cursor_changed);
            let descriptor = Rc::clone(&descriptor);
            let sort_column = Rc::clone(&sort_column);
            let subscribed: RefCell<BTreeSet<String>> = RefCell::new(BTreeSet::new());
            move || {
                let view = match view.upgrade() {
                    None => return,
                    Some(view) => view,
                };
                let store = match store.upgrade() {
                    None => return,
                    Some(store) => store,
                };
                let (mut start, mut end) = match view.get_visible_range() {
                    None => return,
                    Some((s, e)) => (s, e),
                };
                for _ in 0..50 {
                    start.prev();
                }
                for _ in 0..50 {
                    end.next();
                }
                let mut setval = Vec::new();
                let sort_column = *sort_column.borrow();
                // unsubscribe invisible rows
                by_id.borrow_mut().retain(|_, v| match store.get_path(&v.row) {
                    None => false,
                    Some(p) => {
                        let visible = (p >= start && p <= end)
                            || (Some(v.col as i32) == sort_column);
                        if !visible {
                            let row_name_v = store.get_value(&v.row, 0);
                            if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                                subscribed.borrow_mut().remove(row_name);
                            }
                            setval.push((v.row.clone(), None));
                        }
                        visible
                    }
                });
                // subscribe to all the visible rows
                while start < end {
                    if let Some(row) = store.get_iter(&start) {
                        let row_name_v = store.get_value(&row, 0);
                        if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                            if !subscribed.borrow().contains(row_name) {
                                setval.push((row.clone(), Some("#subscribe")));
                                subscribed.borrow_mut().insert(row_name.into());
                                let cols =
                                    if vector_mode { 1 } else { descriptor.cols.len() };
                                for col in 0..cols {
                                    let id = col + 1;
                                    let p = base_path.append(row_name);
                                    let p = if vector_mode {
                                        p
                                    } else {
                                        p.append(&descriptor.cols[col].0)
                                    };
                                    let s = subscriber.durable_subscribe(p);
                                    s.updates(true, updates.clone());
                                    by_id.borrow_mut().insert(
                                        s.id(),
                                        Subscription {
                                            sub: s,
                                            row: row.clone(),
                                            col: id as u32,
                                        },
                                    );
                                }
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
                                if !subscribed.borrow().contains(row_name) {
                                    setval.push((row.clone(), Some("#subscribe")));
                                    subscribed.borrow_mut().insert(row_name.into());
                                    let p = base_path.append(row_name).append(
                                        &descriptor.cols[(id - 1) as usize].0,
                                    );
                                    let s = subscriber.durable_subscribe(p);
                                    s.updates(true, updates.clone());
                                    by_id.borrow_mut().insert(
                                        s.id(),
                                        Subscription {
                                            sub: s,
                                            row: row.clone(),
                                            col: id as u32,
                                        },
                                    );
                                }
                            }
                            if !store.iter_next(&row) {
                                break;
                            }
                        }
                    }
                }
                for (row, val) in setval {
                    for id in 1..(if vector_mode { 2 } else { descriptor.cols.len() + 1 })
                    {
                        store.set_value(&row, id as u32, &val.to_value());
                    }
                }
                cursor_changed(&view);
            }
        });
        view.append_column(&{
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("name");
            column.add_attribute(&cell, "text", 0);
            column.set_sort_column_id(0);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            column.connect_clicked({
                let sort_column = Rc::clone(&sort_column);
                let update_subscriptions = Rc::clone(&update_subscriptions);
                move |_| {
                    *sort_column.borrow_mut() = None;
                    update_subscriptions();
                }
            });
            column
        });
        for col in 0..(if vector_mode { 1 } else { descriptor.cols.len() }) {
            let id = (col + 1) as i32;
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            TreeViewColumnExt::set_cell_data_func(
                &column,
                &cell,
                Some(Box::new({
                    let focus_column = Rc::clone(&focus_column);
                    let style = style.clone();
                    move |c: &TreeViewColumn,
                          cr: &CellRenderer,
                          s: &TreeModel,
                          i: &TreeIter| {
                        let cr = cr.clone().downcast::<CellRendererText>().unwrap();
                        if let Ok(Some(v)) = s.get_value(i, id).get::<&str>() {
                            cr.set_property_text(Some(v));
                            match &*focus_column.borrow() {
                                Some(fc) => {
                                    if fc == c {
                                        let fg = style.get_color(StateFlags::SELECTED);
                                        let bg = style
                                            .get_background_color(StateFlags::SELECTED);
                                        cr.set_property_cell_background_rgba(Some(&bg));
                                        cr.set_property_foreground_rgba(Some(&fg));
                                    } else {
                                        cr.set_property_cell_background(None);
                                        cr.set_property_foreground(None);
                                    }
                                }
                                _ => {
                                    cr.set_property_cell_background(None);
                                    cr.set_property_foreground(None);
                                }
                            }
                        }
                    }
                })),
            );
            column.set_title(if vector_mode {
                "value"
            } else {
                descriptor.cols[col].0.as_ref()
            });
            column.set_sort_column_id(id);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            view.append_column(&column);
            column.connect_clicked({
                let sort_column = Rc::clone(&sort_column);
                let update_subscriptions = Rc::clone(&update_subscriptions);
                move |_| {
                    *sort_column.borrow_mut() = Some(id);
                    update_subscriptions();
                }
            });
        }
        view.set_fixed_height_mode(true);
        view.set_model(Some(&store));
        view.connect_row_activated(clone!(
            @weak store, @strong base_path, @strong from_gui => move |_view, path, _col| {
                if let Some(row) = store.get_iter(&path) {
                    let row_name = store.get_value(&row, 0);
                    if let Ok(Some(row_name)) = row_name.get::<&str>() {
                        let path = base_path.append(row_name);
                        let _ = from_gui.unbounded_send(FromGui::Navigate(path));
                    }
                }
        }));
        view.connect_key_press_event(clone!(
            @strong base_path, @strong from_gui, @weak view, @weak focus_column,
            @weak selected_path =>
            @default-return Inhibit(false), move |_, key| {
                if key.get_keyval() == keys::constants::BackSpace {
                    let path = Path::dirname(&base_path).unwrap_or("/");
                    let m = FromGui::Navigate(Path::from(String::from(path)));
                    let _ = from_gui.unbounded_send(m);
                }
                if key.get_keyval() == keys::constants::Escape {
                    // unset the focus
                    view.set_cursor::<TreeViewColumn>(&TreePath::new(), None, false);
                    view.get_selection().unselect_all();
                    *focus_column.borrow_mut() = None;
                    selected_path.set_label("");
                }
                Inhibit(false)
        }));
        view.connect_cursor_changed({
            let cursor_changed = Rc::clone(&cursor_changed);
            move |v| cursor_changed(v)
        });
        tablewin.get_vadjustment().map(|va| {
            let f = Rc::clone(&update_subscriptions);
            va.connect_value_changed(move |_| f());
        });
        NetidxTable { root, view, store, by_id, update_subscriptions }
    }
}

async fn netidx_main(
    cfg: Config,
    auth: Auth,
    mut updates: mpsc::Receiver<Batch>,
    mut to_gui: mpsc::Sender<ToGui>,
    mut from_gui: mpsc::UnboundedReceiver<FromGui>,
) {
    let subscriber = Subscriber::new(cfg, auth).expect("failed to create subscriber");
    let resolver = subscriber.resolver();
    let mut refresh = time::interval(Duration::from_secs(1)).fuse();
    loop {
        select_biased! {
            _ = refresh.next() => match to_gui.send(ToGui::Refresh).await {
                Ok(()) => (),
                Err(e) => break
            },
            b = updates.next() => if let Some(batch) = b {
                match to_gui.send(ToGui::Batch(batch)).await {
                    Ok(()) => (),
                    Err(e) => break
                }
            },
            m = from_gui.next() => match m {
                None => break,
                Some(FromGui::Navigate(path)) => {
                    let table = match resolver.table(path.clone()).await {
                        Ok(table) => table,
                        Err(e) => {
                            error!("can't load path {}", e);
                            continue
                        }
                    };
                    let m = ToGui::Table(subscriber.clone(), path, table);
                    match to_gui.send(m).await {
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
    to_gui: mpsc::Sender<ToGui>,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
) {
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(netidx_main(cfg, auth, updates, to_gui, from_gui));
    });
}

fn run_gui(
    app: &Application,
    updates: mpsc::Sender<Batch>,
    mut to_gui: mpsc::Receiver<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
) {
    let main_context = glib::MainContext::default();
    let app = app.clone();
    let window = ApplicationWindow::new(&app);
    window.set_title("Netidx browser");
    window.set_default_size(800, 600);
    window.show_all();
    main_context.spawn_local(async move {
        let mut current: Option<NetidxTable> = None;
        let mut changed: HashMap<SubId, (TreeIter, u32, Value)> = HashMap::new();
        while let Some(m) = to_gui.next().await {
            match m {
                ToGui::Refresh => {
                    if let Some(t) = &mut current {
                        for (id, (row, col, v)) in changed.drain() {
                            t.store.set_value(&row, col, &format!("{}", v).to_value());
                        }
                        t.view.columns_autosize();
                        (t.update_subscriptions)();
                    }
                }
                ToGui::Batch(mut b) => {
                    if let Some(t) = &mut current {
                        let subs = t.by_id.borrow();
                        for (id, v) in b.drain(..) {
                            if let Some(sub) = subs.get(&id) {
                                changed.insert(id, (sub.row.clone(), sub.col, v));
                            }
                        }
                    }
                }
                ToGui::Table(subscriber, path, table) => {
                    if let Some(cur) = current.take() {
                        window.remove(&cur.root);
                        cur.view.set_model(None::<&ListStore>);
                    }
                    changed.clear();
                    window.set_title(&format!("Netidx Browser {}", path));
                    let cur = NetidxTable::new(
                        subscriber,
                        path,
                        table,
                        updates.clone(),
                        from_gui.clone(),
                    );
                    window.add(&cur.root);
                    window.show_all();
                    current = Some(cur);
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
        let (tx_to_gui, rx_to_gui) = mpsc::channel(2);
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();
        // navigate to the initial location
        tx_from_gui.unbounded_send(FromGui::Navigate(path.clone())).unwrap();
        run_netidx(cfg.clone(), auth.clone(), rx_updates, tx_to_gui, rx_from_gui);
        run_gui(app, tx_updates, rx_to_gui, tx_from_gui)
    });
    application.run(&[]);
}
