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
    collections::HashMap,
    iter,
    rc::Rc,
    thread,
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    time::{self, Instant},
};

#[derive(Clone, Copy)]
enum CellState {
    Invisible(Instant),
    Visible(Instant),
}

struct CellValueInner {
    value: RefCell<String>,
    id: Cell<Option<SubId>>,
    state: Cell<CellState>,
}

#[derive(Clone, GBoxed)]
#[gboxed(type_name = "GBoxedCellValue")]
struct CellValue(Rc<CellValueInner>);

impl CellValue {
    fn is_invisible_for(&self, duration: Duration) -> bool {
        match self.0.state.get() {
            CellState::Invisible(ts) => ts.elapsed() >= duration,
            CellState::Visible(_) => false,
        }
    }

    fn is_visible_for(&self, duration: Duration) -> bool {
        match self.0.state.get() {
            CellState::Visible(ts) => ts.elapsed() >= duration,
            CellState::Invisible(_) => false,
        }
    }

    fn update_visible(&self, visible: bool) {
        if visible {
            match self.0.state.get() {
                CellState::Visible(_) => (),
                CellState::Invisible(_) => {
                    self.0.state.set(CellState::Visible(Instant::now()));
                }
            }
        } else {
            match self.0.state.get() {
                CellState::Invisible(_) => (),
                CellState::Visible(_) => {
                    self.0.state.set(CellState::Invisible(Instant::now()));
                }
            }
        }
    }
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
        let descriptor = Rc::new(descriptor);
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
                let cell = CellValue(Rc::new(CellValueInner {
                    value: RefCell::new(String::from("#subscribe")),
                    id: Cell::new(None),
                    state: Cell::new(CellState::Invisible(Instant::now())),
                }));
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
            let f = {
                let descriptor = Rc::clone(&descriptor);
                let subscribed = Rc::clone(&subscribed);
                let base_path = base_path.clone();
                let subscriber = subscriber.clone();
                let updates = updates.clone();
                let view = view.clone();
                move |_: &TreeViewColumn,
                      cell: &CellRenderer,
                      model: &TreeModel,
                      row: &TreeIter| {
                    let cell = cell.clone().downcast::<CellRendererText>().unwrap();
                    let sub_row = model.get_value(row, id);
                    let sub = sub_row.get::<&CellValue>().unwrap().unwrap();
                    cell.set_property_text(Some(&*sub.0.value.borrow()));
                    let path = model.get_path(row).unwrap();
                    sub.update_visible(match view.get_visible_range() {
                        None => false,
                        Some((s, e)) => path >= s && path <= e,
                    });
                    match sub.0.id.get() {
                        Some(subid) => {
                            if sub.is_invisible_for(Duration::from_secs(1)) {
                                sub.0.id.set(None);
                                *sub.0.value.borrow_mut() = String::from("#subscribe");
                                subscribed.borrow_mut().remove(&subid);
                                model.row_changed(&path, &row);
                            }
                        }
                        None => match sub.0.state.get() {
                            CellState::Invisible(_) => (),
                            CellState::Visible(_) => {
                                let row_name_v = model.get_value(row, 0);
                                let row_name = row_name_v.get::<&str>().unwrap().unwrap();
                                let s = subscriber.durable_subscribe(dbg!(base_path
                                    .append(row_name)
                                    .append(&descriptor.cols[col].0)));
                                sub.0.id.set(Some(s.id()));
                                *sub.0.value.borrow_mut() = String::from("#subscribe");
                                s.updates(true, updates.clone());
                                subscribed.borrow_mut().insert(
                                    s.id(),
                                    Subscription {
                                        sub: s,
                                        row: row.clone(),
                                        col: id as u32,
                                    },
                                );
                                model.row_changed(&path, &row)
                            }
                        },
                    }
                }
            };
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(Box::new(f)));
            view.append_column(&column);
        }
        view.set_model(Some(&store));
        let root = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        root.add(&view);
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
                                let cell = cell_v.get::<&CellValue>().unwrap().unwrap();
                                *cell.0.value.borrow_mut() = format!("{}", v);
                                t.store.row_changed(&path, &row);
                            }
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
