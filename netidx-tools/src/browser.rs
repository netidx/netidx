use crossbeam_channel::Sender as CSender;
use cursive::{
    direction::Direction,
    event::{Event, EventResult},
    theme::Style,
    utils::span::SpannedString,
    view::{Nameable, Selector, View},
    views::{LinearLayout, NamedView, PaddedView, ScrollView, SelectView, TextView},
    Cursive, CursiveExt, Printer, Rect, XY,
};
use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    resolver::{Auth, Table},
    subscriber::{DvState, Dval, SubId, Subscriber, Value},
};
use netidx_protocols::view;
use parking_lot::Mutex;
use std::{
    cmp::{max, min},
    collections::HashMap,
    io::{self, Write},
    iter,
    rc::Rc,
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::{runtime::Runtime, task, time::Instant};

struct TableDvalInner {
    row: usize,
    col: usize,
    name: Path,
    sub: Dval,
    last: Value,
    last_rendered: Instant,
}

#[derive(Clone)]
struct TableDval(Arc<Mutex<TableDvalInner>>);

struct SubMgrInner {
    updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
    subscriber: Subscriber,
    table_by_id: HashMap<SubId, TableDval>,
    table_by_path: HashMap<Path, TableDval>,
}

impl SubMgrInner {
    fn subscribe_table_cell(&mut self, path: Path, name: Path, row: usize, col: usize) {
        let sub = self.subscriber.durable_subscribe(path.clone());
        sub.updates(true, self.updates.clone());
        let id = sub.id();
        let dv = TableDval(Arc::new(Mutex::new(TableDvalInner {
            row,
            col,
            name,
            sub,
            last: Value::Null,
            last_rendered: Instant::now(),
        })));
        self.table_by_path.insert(path, dv.clone());
        self.table_by_id.insert(id, dv);
    }
}

#[derive(Clone)]
struct SubMgr(Arc<Mutex<SubMgrInner>>);

fn pad(i: usize, len: usize, s: &str) -> String {
    if i == 0 {
        format!("{} ", s)
    } else if i == len - 1 {
        format!(" {}", s)
    } else {
        format!(" {} ", s)
    }
}

#[derive(Debug, Clone)]
struct TableCell {
    columns: Rc<Vec<Path>>,
    path: Path,
    name: Path,
    id: usize,
}

struct NetidxTable {
    root: LinearLayout,
    name: Path,
    submgr: SubMgr,
}

impl View for NetidxTable {
    fn draw(&self, printer: &Printer) {
        self.root.draw(printer);
    }

    fn layout(&mut self, v: XY<usize>) {
        self.root.layout(v)
    }

    fn needs_relayout(&self) -> bool {
        self.root.needs_relayout()
    }

    fn required_size(&mut self, constraint: XY<usize>) -> XY<usize> {
        self.root.required_size(constraint)
    }

    fn on_event(&mut self, e: Event) -> EventResult {
        self.root.on_event(e)
    }

    fn call_on_any<'a>(
        &mut self,
        s: &Selector,
        f: &'a mut (dyn FnMut(&mut (dyn View + 'static)) + 'a),
    ) {
        self.root.call_on_any(s, f)
    }

    fn focus_view(&mut self, s: &Selector) -> Result<(), ()> {
        self.root.focus_view(s)
    }

    fn take_focus(&mut self, source: Direction) -> bool {
        self.root.take_focus(source)
    }

    fn important_area(&self, view_size: XY<usize>) -> Rect {
        self.root.important_area(view_size)
    }

    fn type_name(&self) -> &'static str {
        self.root.type_name()
    }
}

fn table_on_select(c: &mut Cursive, t: &TableCell) {
    let submgr = c.user_data::<SubMgr>().unwrap().clone();
    c.call_on_name(&*t.name, |v: &mut NamedView<ScrollView<LinearLayout>>| {
        let mut v = v.get_mut();
        let nrows = v.content_viewport().height();
        let ll = v.get_inner_mut();
        let mut visible = Vec::new(); // estokes: pooled
        for col in 0..ll.len() {
            if let Some(c) = ll.get_child_mut(col) {
                if let Some(vcol) = c.downcast_mut::<SelectView<TableCell>>() {
                    vcol.set_selection(t.id);
                    let start_row = if t.id < nrows { 0 } else { t.id - nrows };
                    let end_row = min(vcol.len(), t.id + nrows);
                    for row in start_row..end_row {
                        if let Some((_, t)) = vcol.get_item(row) {
                            visible.push((t.path.clone(), row, col));
                        }
                    }
                }
            }
        }
        let mut mgr = submgr.0.lock();
        for (path, row, col) in visible {
            match mgr.table_by_path.get_mut(&path) {
                Some(tdv) => {
                    tdv.0.lock().last_rendered = Instant::now();
                }
                None => {
                    mgr.subscribe_table_cell(path, t.name.clone(), row, col);
                }
            }
        }
    });
}

impl NetidxTable {
    fn new(submgr: SubMgr, base_path: Path, table: Table) -> NetidxTable {
        let len = table.rows.len();
        let columns = Rc::new(
            iter::once(base_path.append("name"))
                .chain(table.cols.iter().filter_map(|(p, c)| {
                    if c.0 < (len / 2) as u64 {
                        None
                    } else {
                        Some(base_path.append(p))
                    }
                }))
                .collect::<Vec<_>>(),
        );
        let header =
            columns.iter().fold(
                LinearLayout::horizontal(),
                |ll, p| match Path::basename(p) {
                    None => ll,
                    Some(name) => ll.child(TextView::new(name)),
                },
            );
        let data = ScrollView::new(columns.clone().iter().enumerate().fold(
            LinearLayout::horizontal(),
            |ll, (i, cname)| match Path::basename(cname) {
                None => ll,
                Some(cname) => {
                    let mut col = SelectView::<TableCell>::new();
                    let smi = submgr.0.lock();
                    for (j, r) in table.rows.iter().enumerate() {
                        if i == 0 { // row name column
                            let path = Path::from("");
                            let name = Path::basename(r).unwrap_or("");
                            let d = TableCell {
                                columns: columns.clone(),
                                path,
                                name: base_path.clone(),
                                id: j,
                            };
                            col.add_item(name, d);
                        } else { // data column
                            let path = r.append(cname);
                            let lbl = match smi.table_by_path.get(&path) {
                                None => pad(i, len, "#u"),
                                Some(tdv) => {
                                    let tdv = tdv.0.lock();
                                    match tdv.sub.state() {
                                        DvState::Unsubscribed => pad(i, len, "#u"),
                                        DvState::FatalError(_) => pad(i, len, "#e"),
                                        DvState::Subscribed => {
                                            pad(i, len, &format!("{}", tdv.last))
                                        }
                                    }
                                }
                            };
                            let d = TableCell {
                                columns: columns.clone(),
                                path,
                                name: base_path.clone(),
                                id: j,
                            };
                            col.add_item(lbl, d)
                        }
                    }
                    col.set_on_select(table_on_select);
                    ll.child(col)
                }
            },
        ))
        .with_name(&*base_path);
        let root = LinearLayout::vertical().child(header).child(data);
        NetidxTable { root, name: base_path, submgr }
    }

    fn process_update(&mut self, mut batch: Pooled<Vec<(SubId, Value)>>) {
        let mut data = self.root.get_child_mut(1).unwrap();
        let mut data =
            data.downcast_mut::<NamedView<ScrollView<LinearLayout>>>().unwrap();
        let mut data = data.get_mut();
        let mut data = data.get_inner_mut();
        let mgr = self.submgr.0.lock();
        for (id, v) in batch.drain(..) {
            if let Some(tdv) = mgr.table_by_id.get(&id) {
                let (row, col) = {
                    let mut tdv = tdv.0.lock();
                    tdv.last = v.clone();
                    (tdv.row, tdv.col)
                };
                if let Some(column) = data.get_child_mut(col) {
                    if let Some(column) = column.downcast_mut::<SelectView<TableCell>>() {
                        if let Some((l, t)) = column.get_item_mut(row) {
                            *l = SpannedString::<Style>::plain(pad(
                                col,
                                t.columns.len(),
                                &format!("{}", v),
                            ));
                        }
                    }
                }
            }
        }
    }
}

async fn async_main(
    cfg: Config,
    auth: Auth,
    path: Path,
    gui: CSender<Box<dyn FnOnce(&mut Cursive) + 'static + Send>>,
) {
    let subscriber = Subscriber::new(cfg, auth).expect("failed to create subscriber");
    let (tx_updates, mut rx_updates) = mpsc::channel(3);
    let resolver = subscriber.resolver();
    let submgr = SubMgr(Arc::new(Mutex::new(SubMgrInner {
        updates: tx_updates,
        subscriber,
        table_by_id: HashMap::new(),
        table_by_path: HashMap::new(),
    })));
    let table = resolver.table(path.clone()).await.expect("can't load initial table");
    gui.send(Box::new(move |c: &mut Cursive| {
        c.set_user_data(submgr.clone());
        c.add_fullscreen_layer(NetidxTable::new(submgr, path, table).with_name("root"));
    }));
    while let Some(batch) = rx_updates.next().await {
        gui.send(Box::new(move |c: &mut Cursive| {
            c.call_on_name("root", |v: &mut NetidxTable| v.process_update(batch));
        }));
    }
}

fn run_async(
    cfg: Config,
    auth: Auth,
    path: Path,
    gui: CSender<Box<dyn FnOnce(&mut Cursive) + 'static + Send>>,
) {
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(async_main(cfg, auth, path, gui));
    });
}

pub(crate) fn run(cfg: Config, auth: Auth, path: Path) {
    let mut c = Cursive::crossterm().unwrap();
    c.set_fps(1);
    run_async(cfg, auth, path, c.cb_sink().clone());
    c.run()
}
