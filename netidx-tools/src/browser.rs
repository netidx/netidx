use cursive::{
    theme::Style,
    utils::span::SpannedString,
    view::{Nameable, View},
    views::{LinearLayout, NamedView, PaddedView, ScrollView, SelectView, TextView},
    Cursive, CursiveExt, Printer, XY,
};
use futures::channel::mpsc;
use netidx::{
    path::Path,
    pool::Pooled,
    protocol::resolver::Table,
    subscriber::{DVal, DvState, SubId, Subscriber, Value},
};
use netidx_protocols::view::{Direction, Keybind, Sink, Source, View, Widget};
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    cmp::{max, min},
    collections::HashMap,
    io::{self, Write},
    iter,
    rc::Rc,
    time::Duration,
};
use tokio::time::Instant;

struct TableDval {
    row: usize,
    col: usize,
    name: Path,
    sub: Dval,
    last: Option<Value>,
    last_rendered: Instant,
}

struct SubMgrInner {
    updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
    subscriber: Subscriber,
    by_id: HashMap<SubId, Path>,
    tables: HashMap<Path, TableDval>,
}

impl SubMgrInner {
    fn subscribe_table_cell(&mut self, path: Path, name: Path, row: usize, col: usize) {
        let sub = self.subscriber.durable_subscribe(path.clone());
        self.by_id.insert(sub.id(), path.clone());
        sub.updates(true, self.updates.clone());
        self.tables.insert(
            path,
            TableDval { row, col, name, sub, last: None, last_rendered: Instant::now() },
        );
    }
}

#[derive(Clone)]
struct SubMgr(Rc<RefCell<SubMgrInner>>);

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
        let start_row = printer.offset.y;
        let nrows = printer.output_size.y;
        let mut visible = Vec::new();
        if let Some(c) = root.get_child(1) {
            if let Some(c) = c.downcast::<NamedView<LinearLayout>>() {}
        }
    }
}

impl NetidxTable {
    fn update_subscriptions(&self, nrows: usize) -> Option<()> {
        let sv =
            self.root.get_child(1)?.downcast::<ScrollView<NamedView<LinearLayout>>>()?;
        let named = sv.get_inner();
        let data = named.get_inner();
        let mut visible = Vec::new();
        // CR estokes: Think about the degenerate case where there are millions of columns?
        for col in 0..data.len() {
            let vcol = data.get_child(col)?.downcast::<SelectView<TableCell>>()?;
            let selected = vcol.selected_id()?;
            let start_row = max(0, selected - nrows);
            let end_row = min(vcol.len(), selected + nrows);
            for row in start_row..end_row {
                if let Some((_, t)) = vcol.get_item(row) {
                    visible.push((t.path.clone(), row, col));
                }
            }
        }
        let mut mgr = self.submgr.borrow_mut();
        for (path, row, col) in visible {
            match mgr.tables.get_mut(&path) {
                Some(tdv) => {
                    tdv.last_rendered = Instant::now();
                }
                None => {
                    mgr.subscribe_table_cell(path, self.name.clone, row, col);
                }
            }
        }
        Some(())
    }

    fn new(submgr: SubMgr, base_path: Path, table: Table) -> NetidxTable {
        fn on_select(c: &mut Cursive, t: &TableCell) {
            c.call_on_name(&*t.name, |v: &mut NamedView<LinearLayout>| {
                for i in 0..v.len() {
                    if let Some(c) = v.get_child_mut(i) {
                        if let Some(c) = child.downcast_mut::<SelectView<TableCell>>() {
                            c.set_selection(t.id);
                        }
                    }
                }
            });
        }
        let len = table.rows.len();
        let columns = Rc::new(
            iter::once(base_path.append(Path::from("name")))
                .chain(table.cols.iter().filter_map(|(p, c)| {
                    if c.0 < len / 2 {
                        None
                    } else {
                        Some(base_path.append(p))
                    }
                }))
                .collect::<Vec<_>>(),
        );
        let header = cols.iter().fold(LinearLayout::horizontal(), |ll, p| {
            match Path::basename(hd) {
                None => ll,
                Some(name) => ll.child(TextView::new(name)),
            }
        });
        let data = ScrollView::new(
            columns
                .clone()
                .iter()
                .enumerate()
                .fold(LinearLayout::horizontal(), |ll, (i, cname)| {
                    match Path::basename(cname) {
                        None => ll,
                        Some(cname) => {
                            let mut col = SelectView::<TableCell>::new();
                            let smi = submgr.borrow();
                            for (j, r) in table.rows.iter().enumerate() {
                                let path = r.append(cname);
                                let lbl = match smi.tables.get(&path) {
                                    None => pad(i, len, "#u"),
                                    Some(tdv) => match tdv.sub.state {
                                        DvState::Unsubscribed => pad(i, len, "#u"),
                                        DvState::FatalError => pad(i, len, "#e"),
                                        DvState::Subscribed => match tdv.last {
                                            None => pad(i, len, "#n"),
                                            Some(v) => pad(i, len, &format!("{}", v)),
                                        },
                                    },
                                };
                                let d = TableCell {
                                    columns: columns.clone(),
                                    path,
                                    name: base_path.clone(),
                                    id: j,
                                };
                                col.add_item(lbl, d)
                            }
                            col.set_on_select(on_select);
                            ll.child(col)
                        }
                    }
                })
                .with_name(&*base_path),
        );
        let root = LinearLayout.vertical().child(header).child(data);
        NetidxTable { root, name: base_path, submgr }
    }
}

pub(crate) fn run() {
    let mut term = Cursive::crossterm().unwrap();
    let mut c0 = SelectView::new().with_name("c0");
    let mut c1 = SelectView::new().with_name("c1");
    c0.get_mut().set_on_select(|c, i| {
        c.call_on_name("c1", |v: &mut NamedView<SelectView<usize>>| {
            v.get_mut().set_selection(*i)
        });
    });
    c1.get_mut().set_on_select(|c, i| {
        c.call_on_name("c0", |v: &mut NamedView<SelectView<usize>>| {
            v.get_mut().set_selection(*i)
        });
    });
    for i in 0..100 {
        c0.get_mut().add_item(format!("{},0", i), i);
    }
    for i in 0..100 {
        c1.get_mut().add_item(format!("{},1", i), i);
    }
    c0.get_mut().add_item(format!("very very very long item"), 100);
    c1.get_mut().add_item(format!("100,1"), 100);
    *c0.get_mut().get_item_mut(0).unwrap().0 = SpannedString::<Style>::plain("edited");
    term.add_fullscreen_layer(ScrollView::new(
        LinearLayout::horizontal()
            .child(PaddedView::lrtb(0, 1, 0, 0, c0))
            .child(PaddedView::lrtb(0, 0, 0, 0, c1)),
    ));
    term.run()
}
