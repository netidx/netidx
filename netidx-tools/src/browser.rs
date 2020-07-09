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

fn pad(i: usize, len: usize, mut s: String) -> String {
    if i == 0 {
        s.push(' ');
        s
    } else if i == len - 1 {
        s.insert(0, ' ');
        s
    } else {
        s.insert(0, ' ');
        s.push(' ');
        s
    }
}

fn fit(i: usize, mut s: String) -> String {
    if s.len() >= i {
        while s.len() > i {
            s.pop();
        }
        s
    } else {
        let pad = i - s.len();
        let hpad = pad / 2;
        for _ in 0..hpad {
            s.insert(0, ' ');
        }
        let hpad = if pad % 2 == 0 { hpad } else { hpad + 1 };
        for _ in 0..hpad {
            s.push(' ');
        }
        s
    }
}

fn expand(i: usize, s: String) -> String {
    if s.len() < i {
        fit(i, s)
    } else {
        s
    }
}

struct Cell {
    sub: Dval,
    row: usize,
    col: usize,
}

struct NetidxTable {
    root: LinearLayout,
    base_path: Path,
    table: Table,
    columns: Rc<Vec<Path>>,
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

impl NetidxTable {
    fn new(sub: Subscriber, base_path: Path, mut table: Table) -> NetidxTable {
        let len = table.rows.len();
        table.cols.retain(|(_, i)| i >= threshold);
        let table = Rc::new(table);
        let columns = Rc::new(
            iter::once(Path::from("name"))
                .chain(table.cols.iter().map(|(p, _)| p.clone()))
                .collect::<Vec<_>>(),
        );
        let head = columns.iter().fold(LinearLayout::horizontal(), |ll, col| {
            ll.child(TextView::new(col.as_ref()))
        });
        let on_select = {
            let columns = Rc::clone(&columns);
            let table = table.clone();
            let base_path = base_path.clone();
            let sub = sub.clone();
            Rc::new(move |c: &mut Cursive, t: &Cell| {
                c.call_on_name(
                    &*base_path,
                    |v: &mut NamedView<ScrollView<LinearLayout>>| {
                        let mut data = v.get_mut();
                        let mut column = data.get_child_mut(t.col).unwrap();
                        let mut column =
                            column.downcast_mut::<SelectView<Cell>>().unwrap();
                        if let Some(row) = column.selected_id() {
                            let len = column.len();
                            let must_add = (row == 0 && t.row > 0)
                                || row == len - 1 && t.row < table.rows.len() - 1;
                            // sync the row selection
                            for col in 1..data.len() {
                                let mut column = data.get_child_mut(col).unwrap();
                                let mut column =
                                    column.downcast_mut::<SelectView<Cell>>().unwrap();
                                column.set_selection(row); // ignore the callback
                                                           // add the new data rows
                                if must_add {
                                    let r = if row == 0 { t.row - 1 } else { t.row + 1 };
                                    let path = table.rows[r].append(&columns[col]);
                                    let sub = sub.durable_subscribe(path);
                                    let c = Cell { sub, row: r, col };
                                    if row == 0 {
                                        column.insert_item(0, "#u", c);
                                    } else {
                                        column.add_item("#u", c)
                                    }
                                }
                            }
                            // add the new row name row
                            if must_add {
                                let mut column = data.get_child_mut(0).unwrap();
                                let mut column =
                                    column.downcast_mut::<LinearLayout>().unwrap();
                                let r = if row == 0 { t.row - 1 } else { t.row + 1 };
                                let lbl = TextView::new(pad(
                                    0,
                                    columns.len(),
                                    expand(
                                        columns[0].len(),
                                        Path::basename(&table.rows[r])
                                            .unwrap_or("")
                                            .into(),
                                    ),
                                ));
                                if row == 0 {
                                    column.insert_child(0, lbl);
                                } else {
                                    column.add_child(lbl);
                                }
                            }
                        }
                    },
                )
            })
        };
        let data = ScrollView::new(columns.iter().enumerate().fold(
            LinearLayout::horizontal(),
            |ll, (i, cname)| {
                if i == 0 {
                    // row name column
                    let mut col = LinearLayout::new();
                    for (j, r) in table.rows.iter().enumerate().take(100) {
                        let lbl = TextView::new(&pad(
                            i,
                            len,
                            expand(cname.len(), Path::basename(&r).unwrap_or("").into()),
                        ));
                        col.add_child(lbl);
                    }
                    ll.child(col)
                } else {
                    // data column
                    let mut col = SelectView::<Cell>::new();
                    for (j, r) in table.rows.iter().enumerate().take(100) {
                        let path = r.append(cname);
                        let sub = sub.durable_subscribe(path);
                        col.add_item("#u", Cell { sub, row: j, col: i });
                        col.set_on_select(on_select.clone());
                    }
                    ll.child(col)
                }
            },
        ))
        .with_name(&*base_path);
        let root = LinearLayout::vertical().child(head).child(data);
        NetidxTable { root, base_path, table, columns }
    }

    fn update(&mut self) {
        let mut data = self.root.get_child_mut(1).unwrap();
        let mut data =
            data.downcast_mut::<NamedView<ScrollView<LinearLayout>>>().unwrap();
        let mut data = data.get_mut();
        let mut data = data.get_inner_mut();
        let mut max_widths: Vec<_> = self.columns.iter().map(|c| c.len()).collect();
        let mut name_col = data.get_child(0).unwrap();
        let mut name_col = name_col.downcast_ref::<LinearLayout>().unwrap();
        for row in 0..name_col.len() {
            let tv = name_col.get_child(row).unwrap();
            max_widths[0] = max(max_widths[0], tv.content().len());
        }
        for col in 1..data.len() {
            let mut column = data.get_child_mut(col).unwrap();
            let mut column = column.downcast_mut::<SelectView<TableCell>>();
            for row in 0..column.len() {
                if let Some((l, t)) = column.get_item(row) {
                    *l = SpannedString::<Style>::plain(pad(
                        col,
                        t.columns.len(),
                        expand(max_widths[col], format!("{}", t.sub.last())),
                    ));
                    max_widths[col] = max(max_widths[col], l.len());
                }
            }
        }
        let mut head = self.root.get_child_mut(0).unwrap();
        let mut head = head.downcast_mut::<LinearLayout>().unwrap();
        for i in 0..max_widths.len() {
            if let Some(l) = head.get_child_mut(i) {
                if let Some(l) = l.downcast_mut::<TextView>() {
                    let c = l.get_shared_content();
                    if c.get_content().width() != max_widths[i] {
                        let hd = Path::basename(&self.columns[i]).unwrap_or("");
                        c.set_content(fit(max_widths[i], hd.into()));
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
    let table = resolver.table(path.clone()).await.expect("can't load initial table");
    gui.send(Box::new(move |c: &mut Cursive| {
        c.add_fullscreen_layer(
            NetidxTable::new(subscriber, path, table).with_name("root"),
        );
    }));
    while let Some(batch) = rx_updates.next().await {
        gui.send(Box::new(move |c: &mut Cursive| {
            c.call_on_name("root", |v: &mut NetidxTable| v.update());
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
