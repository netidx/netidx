use crossbeam_channel::Sender as CSender;
use cursive::{
    direction::Direction,
    event::{Event, EventResult},
    theme::Style,
    utils::span::SpannedString,
    view::{Nameable, Selector, View},
    views::{LinearLayout, NamedView, ScrollView, SelectView, TextView},
    Cursive, CursiveExt, Printer, Rect, XY,
};
use futures::prelude::*;
use netidx::{
    config::Config,
    path::Path,
    resolver::{Auth, Table},
    subscriber::{Dval, Subscriber},
};
use std::{cmp::max, iter, rc::Rc, thread, time::Duration};
use tokio::{runtime::Runtime, time};

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
    fn new(
        sub: Subscriber,
        base_path: Path,
        mut table: Table,
        rows: usize,
    ) -> NetidxTable {
        let len = table.rows.len();
        table.cols.retain(|(_, i)| i.0 >= (len as u64 / 2));
        table.cols.sort_by_key(|v| v.0.clone());
        table.rows.sort();
        let table = Rc::new(table);
        let columns = Rc::new(
            iter::once(Path::from("name"))
                .chain(table.cols.iter().map(|(p, _)| p.clone()))
                .collect::<Vec<_>>(),
        );
        let head = columns.iter().fold(LinearLayout::horizontal(), |ll, col| {
            ll.child(TextView::new(col.as_ref()))
        });
        let on_select = || {
            let columns = Rc::clone(&columns);
            let table = table.clone();
            let base_path = base_path.clone();
            let sub = sub.clone();
            move |c: &mut Cursive, t: &Cell| {
                c.call_on_name(
                    &*base_path,
                    |v: &mut NamedView<ScrollView<LinearLayout>>| {
                        let mut data = v.get_mut();
                        let data = data.get_inner_mut();
                        let column = data.get_child_mut(t.col).unwrap();
                        let column = column.downcast_mut::<SelectView<Cell>>().unwrap();
                        if let Some(row) = column.selected_id() {
                            let len = column.len();
                            let must_add = (row == 0 && t.row > 0)
                                || row == len - 1 && t.row < table.rows.len() - 1;
                            for col in 1..data.len() {
                                let column = data.get_child_mut(col).unwrap();
                                let column =
                                    column.downcast_mut::<SelectView<Cell>>().unwrap();
                                column.set_selection(row);
                                if must_add {
                                    let r = if row == 0 { t.row - 1 } else { t.row + 1 };
                                    let path = table.rows[r].append(&columns[col]);
                                    let sub = sub.durable_subscribe(path);
                                    let c = Cell { sub, row: r, col };
                                    if row == 0 {
                                        column.insert_item(0, "#u", c);
                                        column.remove_item(column.len() - 1);
                                        column.set_selection(1);
                                    } else {
                                        column.add_item("#u", c);
                                        column.remove_item(0);
                                    }
                                }
                            }
                            if must_add {
                                let column = data.get_child_mut(0).unwrap();
                                let column =
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
                                    column.remove_child(column.len() - 1);
                                } else {
                                    column.add_child(lbl);
                                    column.remove_child(0);
                                }
                            }
                        }
                    },
                );
            }
        };
        let data = ScrollView::new(columns.iter().enumerate().fold(
            LinearLayout::horizontal(),
            |ll, (i, cname)| {
                if i == 0 {
                    // row name column
                    let mut col = LinearLayout::vertical();
                    for r in table.rows.iter().take(rows) {
                        let lbl = TextView::new(&pad(
                            i,
                            columns.len(),
                            expand(cname.len(), Path::basename(&r).unwrap_or("").into()),
                        ));
                        col.add_child(lbl);
                    }
                    ll.child(col)
                } else {
                    // data column
                    let mut col = SelectView::<Cell>::new();
                    col.set_on_select(on_select());
                    for (j, r) in table.rows.iter().enumerate().take(rows) {
                        let sub = sub.durable_subscribe(r.append(cname));
                        let lbl = pad(i, columns.len(), "#u".into());
                        col.add_item(lbl, Cell { sub, row: j, col: i });
                    }
                    ll.child(col)
                }
            },
        ))
        .with_name(&*base_path);
        let root = LinearLayout::vertical().child(head).child(data);
        NetidxTable { root, columns }
    }

    fn update(&mut self) {
        let data = self.root.get_child_mut(1).unwrap();
        let data = data.downcast_mut::<NamedView<ScrollView<LinearLayout>>>().unwrap();
        let mut data = data.get_mut();
        let data = data.get_inner_mut();
        let mut max_widths: Vec<_> = self.columns.iter().map(|c| c.len()).collect();
        for col in 1..data.len() {
            let column = data.get_child_mut(col).unwrap();
            let column = column.downcast_mut::<SelectView<Cell>>().unwrap();
            for row in 0..column.len() {
                if let Some((l, t)) = column.get_item_mut(row) {
                    let v = pad(
                        col,
                        self.columns.len(),
                        match t.sub.last() {
                            None => "#n".into(),
                            Some(v) => format!("{}", v),
                        },
                    );
                    max_widths[col] = max(max_widths[col], v.len());
                    *l = SpannedString::<Style>::plain(expand(max_widths[col], v));
                }
            }
        }
        let head = self.root.get_child_mut(0).unwrap();
        let head = head.downcast_mut::<LinearLayout>().unwrap();
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
    let resolver = subscriber.resolver();
    let table = resolver.table(path.clone()).await.expect("can't load initial table");
    gui.send(Box::new(move |c: &mut Cursive| {
        c.add_fullscreen_layer(
            NetidxTable::new(subscriber, path, table, dbg!(c.screen_size().y))
                .with_name("root"),
        );
    }))
    .unwrap();
    let mut interval = time::interval(Duration::from_secs(1));
    while let Some(_) = interval.next().await {
        gui.send(Box::new(move |c: &mut Cursive| {
            c.call_on_name("root", |v: &mut NetidxTable| v.update());
        }))
        .unwrap();
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
    //c.set_fps(1);
    run_async(cfg, auth, path, c.cb_sink().clone());
    c.run()
}
