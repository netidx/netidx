use futures::{channel::mpsc, prelude::*, select_biased};
use gdk::{keys, EventKey};
use gio::prelude::*;
use glib::{self, clone, prelude::*, signal::Inhibit, subclass::prelude::*};
use gtk::{
    prelude::*, Adjustment, Application, ApplicationWindow, CellLayout, CellRenderer,
    CellRendererText, ListStore, ScrolledWindow, TreeIter, TreeModel, TreeStore,
    TreeView, TreeViewColumn, TreeViewColumnSizing,
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
    root: ScrolledWindow,
    view: TreeView,
    store: ListStore,
    by_id: Rc<RefCell<HashMap<SubId, Subscription>>>,
    update_subscriptions: Rc<Fn()>,
}

impl NetidxTable {
    fn new(
        subscriber: Subscriber,
        base_path: Path,
        mut descriptor: Table,
        updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
        from_gui: mpsc::UnboundedSender<FromGui>,
    ) -> NetidxTable {
        let nrows = descriptor.rows.len();
        descriptor.rows.sort();
        descriptor.cols.sort_by_key(|(p, _)| p.clone());
        descriptor.cols.retain(|(_, i)| i.0 >= (nrows / 2) as u64);
        let column_types = (0..descriptor.cols.len() + 1)
            .into_iter()
            .map(|_| String::static_type())
            .collect::<Vec<_>>();
        let store = ListStore::new(&column_types);
        for row in descriptor.rows.iter() {
            let row_name = Path::basename(row).unwrap_or("").to_value();
            let row = store.append();
            store.set_value(&row, 0, &row_name.to_value());
        }
        let by_id: Rc<RefCell<HashMap<SubId, Subscription>>> =
            Rc::new(RefCell::new(HashMap::new()));
        let view = TreeView::new();
        view.append_column(&{
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("name");
            column.add_attribute(&cell, "text", 0);
            column.set_sort_column_id(0);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            column
        });
        for col in 0..descriptor.cols.len() {
            let id = (col + 1) as i32;
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title(descriptor.cols[col].0.as_ref());
            column.add_attribute(&cell, "text", id);
            column.set_sort_column_id(id);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            view.append_column(&column);
        }
        view.set_fixed_height_mode(true);
        view.set_model(Some(&store));
        view.connect_row_activated(clone!(@weak store, @strong base_path,
               @strong from_gui => move |_view, path, _col| {
            if let Some(row) = store.get_iter(&path) {
                let row_name = store.get_value(&row, 0);
                if let Ok(Some(row_name)) = row_name.get::<&str>() {
                    let path = base_path.append(row_name);
                    let _ = from_gui.unbounded_send(FromGui::Navigate(path));
                }
            }
        }));
        view.connect_key_press_event(
            clone!(@strong base_path, @strong from_gui => move |_, key| {
                if key.get_keyval() == keys::constants::BackSpace {
                    let path = Path::dirname(&base_path).unwrap_or("/");
                    let m = FromGui::Navigate(Path::from(String::from(path)));
                    let _ = from_gui.unbounded_send(m);
                }
                Inhibit(false)
            }),
        );
        let root = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        root.add(&view);
        let update_subscriptions = Rc::new({
            let view = view.downgrade();
            let store = store.downgrade();
            let by_id = by_id.clone();
            let mut subscribed: RefCell<BTreeSet<TreeIter>> =
                RefCell::new(BTreeSet::new());
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
                for i in 0..50 {
                    start.prev();
                }
                for i in 0..50 {
                    end.next();
                }
                let mut setval = Vec::new();
                // unsubscribe invisible rows
                by_id.borrow_mut().retain(|_, v| {
                    let visible = match store.get_path(&v.row) {
                        None => false,
                        Some(p) => p >= start && p <= end,
                    };
                    if !visible {
                        subscribed.borrow_mut().remove(&v.row);
                        setval.push((v.row.clone(), None));
                    }
                    visible
                });
                // subscribe to all the visible rows
                while start < end {
                    if let Some(row) = store.get_iter(&start) {
                        if !subscribed.borrow().contains(&row) {
                            setval.push((row.clone(), Some("#subscribe")));
                            subscribed.borrow_mut().insert(row.clone());
                            for col in 0..descriptor.cols.len() {
                                let id = col + 1;
                                let row_name_v = store.get_value(&row, 0);
                                if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                                    let p = base_path
                                        .append(row_name)
                                        .append(&descriptor.cols[col].0);
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
                for (row, val) in setval {
                    for id in 1..descriptor.cols.len() + 1 {
                        store.set_value(&row, id as u32, &val.to_value());
                    }
                }
            }
        });
        root.get_vadjustment().map(|va| {
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
                        if t.by_id.borrow().len() == 0 {
                            (t.update_subscriptions)();
                        }
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
                    }
                    changed.clear();
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
