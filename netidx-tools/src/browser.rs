use futures::{channel::mpsc, prelude::*, select_biased};
use gio::prelude::*;
use glib::{self, prelude::*, subclass::prelude::*};
use gtk::{
    prelude::*, Adjustment, Application, ApplicationWindow, CellLayout, CellRenderer,
    CellRendererText, ListStore, ScrolledWindow, TreeIter, TreeModel, TreeStore,
    TreeView, TreeViewColumn,
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
    collections::{HashMap, BTreeSet},
    iter,
    rc::Rc,
    thread,
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    time::{self, Instant},
};

struct CellValueInner {
    value: String,
    id: Option<SubId>,
}

#[derive(Clone, GBoxed)]
#[gboxed(type_name = "GBoxedCellValue")]
struct CellValue(Rc<RefCell<CellValueInner>>);

struct Subscription {
    sub: Dval,
    row: TreeIter,
    col: u32,
}

struct NetidxTable {
    root: ScrolledWindow,
    view: TreeView,
    store: ListStore,
    subscribed: Rc<RefCell<HashMap<SubId, Subscription>>>,
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
            .map(|i| {
                if i == 0 {
                    glib::types::Type::String
                } else {
                    CellValue::static_type()
                }
            })
            .collect::<Vec<_>>();
        let store = ListStore::new(&column_types);
        for row in descriptor.rows.iter() {
            let row_name = Path::basename(row).unwrap_or("");
            let row = store.append();
            store.set_value(&row, 0, &row_name.to_value());
            for col in 0..descriptor.cols.len() {
                let id = (col + 1) as u32;
                let cell = CellValue(Rc::new(RefCell::new(CellValueInner {
                    value: String::from("#subscribe"),
                    id: None,
                })));
                store.set_value(&row, id, &cell.to_value());
            }
        }
        let subscribed: Rc<RefCell<HashMap<SubId, Subscription>>> =
            Rc::new(RefCell::new(HashMap::new()));
        let view = TreeView::new();
        view.append_column(&{
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("name");
            column.add_attribute(&cell, "text", 0);
            column.set_sort_column_id(0);
            column
        });
        for col in 0..descriptor.cols.len() {
            let id = (col + 1) as i32;
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title(descriptor.cols[col].0.as_ref());
            column.set_sort_column_id(id);
            let f = move |_: &TreeViewColumn,
                          cell: &CellRenderer,
                          model: &TreeModel,
                          row: &TreeIter| {
                let cell = cell.clone().downcast::<CellRendererText>().unwrap();
                let sub_row = model.get_value(row, id);
                let sub = sub_row.get::<&CellValue>().unwrap().unwrap();
                cell.set_property_text(Some(&*sub.0.borrow().value));
            };
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(Box::new(f)));
            view.append_column(&column);
        }
        view.set_model(Some(&store));
        let root = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        root.add(&view);
        let va = root.get_vadjustment().unwrap();
        {
            let view = view.clone();
            let store = store.clone();
            let subscribed = subscribed.clone();
            va.connect_value_changed(move |a| {
                println!("scrolled {} {} {}", a.get_value(), a.get_lower(), a.get_upper());
                let mut notify = BTreeSet::new();
                let (mut start, mut end) = match view.get_visible_range() {
                    None => return,
                    Some((s, e)) => (s, e),
                };
                start.prev();
                end.next();
                // unsubscribe invisible rows
                subscribed.borrow_mut().retain(|_, v| {
                    let (path, visible) = match store.get_path(&v.row) {
                        None => return false,
                        Some(p) => {
                            let visible = p >= start && p <= end;
                            (p, visible)
                        }
                    };
                    if !visible {
                        for col in 0..descriptor.cols.len() {
                            let id = col + 1;
                            let cell = store.get_value(&v.row, id as i32);
                            if let Ok(Some(cell)) = cell.get::<&CellValue>() {
                                let mut inner = cell.0.borrow_mut();
                                inner.id = None;
                                inner.value = String::from("#subscribe");
                                notify.insert((path.clone(), v.row.clone()));
                            }
                        }
                    }
                    visible
                });
                while start < end {
                    if let Some(row) = store.get_iter(&start) {
                        for col in 0..descriptor.cols.len() {
                            let id = col + 1;
                            let cell = store.get_value(&row, id as i32);
                            if let Ok(Some(cell)) = cell.get::<&CellValue>() {
                                let mut inner = cell.0.borrow_mut();
                                if inner.id.is_none() {
                                    let row_name_v = store.get_value(&row, 0);
                                    if let Ok(Some(row_name)) = row_name_v.get::<&str>()
                                    {
                                        let p = base_path
                                            .append(row_name)
                                            .append(&descriptor.cols[col].0);
                                        let s = subscriber.durable_subscribe(p);
                                        inner.id = Some(s.id());
                                        inner.value = String::from("#subscribe");
                                        s.updates(true, updates.clone());
                                        subscribed.borrow_mut().insert(
                                            s.id(),
                                            Subscription {
                                                sub: s,
                                                row: row.clone(),
                                                col: id as u32,
                                            },
                                        );
                                        notify.insert((start.clone(), row.clone()));
                                    }
                                }
                            }
                        }
                    }
                    start.next();
                }
                for (path, row) in notify {
                    store.row_changed(&path, &row);
                }
            });
        }
        NetidxTable { root, view, store, subscribed }
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
                            if let Some(path) = t.store.get_path(&row) {
                                let cell_v = t.store.get_value(&row, col as i32);
                                if let Ok(Some(cell)) = cell_v.get::<&CellValue>() {
                                    cell.0.borrow_mut().value = format!("{}", v);
                                    t.store.row_changed(&path, &row);
                                }
                            }
                        }
                        if t.subscribed.borrow().len() == 0 {
                            // kickstart subscription
                            t.root.get_vadjustment().map(|a| a.value_changed());
                        }
                    }
                }
                ToGui::Batch(mut b) => {
                    if let Some(t) = &mut current {
                        let subs = t.subscribed.borrow();
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
        let (tx_updates, rx_updates) = mpsc::channel(3);
        let (tx_to_gui, rx_to_gui) = mpsc::channel(2);
        run_netidx(cfg.clone(), auth.clone(), path.clone(), rx_updates, tx_to_gui);
        run_gui(app, tx_updates, rx_to_gui)
    });
    application.run(&[]);
}
