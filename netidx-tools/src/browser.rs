use cursive::{
    theme::Style,
    utils::span::SpannedString,
    view::{Nameable, View},
    views::{LinearLayout, NamedView, PaddedView, ScrollView, SelectView, TextView},
    Cursive, CursiveExt,
};
use netidx::{
    path::Path,
    protocol::resolver::Table,
    subscriber::{DVal, SubId, Subscriber, Value},
};
use netidx_protocols::view::{Direction, Keybind, Sink, Source, View, Widget};
use std::{
    cell::RefCell,
    cmp::{max, min},
    collections::HashMap,
    io::{self, Write},
    iter,
    rc::Rc,
    time::{Duration, Instant},
};

struct TableDval {
    cols: usize,
    row: usize,
    col: usize,
    table_name: Path,
    sub: Dval,
    last_rendered: Instant,
}

struct SubMgrInner {
    subscriber: Subscriber,
    by_id: HashMap<SubId, Path>,
    tables: HashMap<Path, TableDval>,
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
}

struct NetidxTable {
    root: LinearLayout,
    submgr: SubMgr,
}

impl NetidxTable {
    fn new(submgr: SubMgr, base_path: Path, table: Table) -> NetidxTable {
        fn on_select(c: &mut Cursive, t: &TableCell) {}
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
                            for r in &table.rows {
                                let path = r.append(cname);
                                let lbl = match smi.tables.get(&path) {
                                    None => {
                                let d = TableCell {
                                    columns: columns.clone(),
                                    path,
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
        NetidxTable { root, base_path, subscriber, subscribed: HashMap::new() }
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
