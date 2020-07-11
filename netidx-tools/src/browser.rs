use futures::{channel::mpsc, prelude::*, select_biased};
use gio::prelude::*;
use glib::{self, prelude::*, subclass::prelude::*};
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
}

impl NetidxTable {
    fn new(
        subscriber: Subscriber,
        base_path: Path,
        mut descriptor: Table,
        updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
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
            let row_name = Path::basename(row).unwrap_or("");
            let row = store.append();
            store.set_value(&row, 0, &row_name.to_value());
            for col in 0..descriptor.cols.len() {
                let id = (col + 1) as u32;
                store.set_value(&row, id, &None::<&str>.to_value());
            }
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
        let root = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        root.add(&view);
        let va = root.get_vadjustment().unwrap();
        {
            let view = view.clone();
            let store = store.clone();
            let by_id = by_id.clone();
            let mut subscribed: RefCell<BTreeSet<TreeIter>> =
                RefCell::new(BTreeSet::new());
            va.connect_value_changed(move |a| {
                println!(
                    "scrolled {} {} {}",
                    a.get_value(),
                    a.get_lower(),
                    a.get_upper()
                );
                let (mut start, mut end) = match view.get_visible_range() {
                    None => return,
                    Some((s, e)) => (s, e),
                };
                start.prev();
                end.next();
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
            });
        }
        NetidxTable { root, view, store, by_id }
    }
}

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone)]
enum ToGui {
    Table(Subscriber, Path, Table),
    Batch(Batch),
    Refresh,
}

async fn netidx_main(
    cfg: Config,
    auth: Auth,
    path: Path,
    mut updates: mpsc::Receiver<Batch>,
    mut to_gui: mpsc::Sender<ToGui>,
) {
    let subscriber = Subscriber::new(cfg, auth).expect("failed to create subscriber");
    let resolver = subscriber.resolver();
    let table = resolver.table(path.clone()).await.expect("can't load initial table");
    let mut refresh = time::interval(Duration::from_secs(1)).fuse();
    to_gui.send(ToGui::Table(subscriber.clone(), path, table)).await.unwrap();
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
            }
        }
    }
}

fn run_netidx(
    cfg: Config,
    auth: Auth,
    path: Path,
    updates: mpsc::Receiver<Batch>,
    to_gui: mpsc::Sender<ToGui>,
) {
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(netidx_main(cfg, auth, path, updates, to_gui));
    });
}

fn run_gui(
    app: &Application,
    updates: mpsc::Sender<Batch>,
    mut to_gui: mpsc::Receiver<ToGui>,
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
                        if t.by_id.borrow().len() == 0 {
                            // kickstart subscription
                            t.root.get_vadjustment().map(|a| a.value_changed());
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
                    let cur = NetidxTable::new(subscriber, path, table, updates.clone());
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
        run_netidx(cfg.clone(), auth.clone(), path.clone(), rx_updates, tx_to_gui);
        run_gui(app, tx_updates, rx_to_gui)
    });
    application.run(&[]);
}
