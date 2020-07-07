use fltk::{
    app::{self, App},
    draw,
    enums::{Color, Font, FrameType},
    prelude::*,
    table::{Table, TableContext},
    window::Window,
};
use futures::{channel::mpsc, prelude::*, select_biased};
use log::{error, info};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    resolver,
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
use tokio::{runtime::Runtime, task, time::{self, Instant}};

struct CellInner {
    sub: Dval,
    last: Value,
    last_used: Instant,
}

#[derive(Clone)]
struct Cell(Arc<Mutex<CellInner>>);

struct NetidxTableInner {
    base: Path,
    table: Table,
    by_loc: HashMap<(i32, i32), Cell>,
    by_id: HashMap<SubId, Cell>,
    descriptor: resolver::Table,
    subscriber: Subscriber,
    updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
}

#[derive(Clone)]
struct NetidxTable(Arc<Mutex<NetidxTableInner>>);

impl NetidxTable {
    fn new<W: WidgetExt + GroupExt>(
        root: &mut W,
        subscriber: Subscriber,
        updates: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
        base: Path,
        descriptor: resolver::Table,
    ) -> Self {
        let mut table = Table::default().with_pos(0, 0).size_of(root);
        root.insert(&table, 0);
        table.set_label(base.as_ref());
        table.set_rows(descriptor.rows.len() as u32);
        table.set_row_header(true);
        table.set_row_header_width(
            descriptor.rows.iter().map(|p| p.len()).max().unwrap_or(0) as i32 * 10,
        );
        table.set_cols(descriptor.cols.len() as u32);
        table.set_col_header(true);
        table.set_col_resize(true);
        table.end();
        let t = NetidxTable(Arc::new(Mutex::new(NetidxTableInner {
            base,
            table: table.clone(),
            descriptor,
            by_loc: HashMap::new(),
            by_id: HashMap::new(),
            subscriber,
            updates,
        })));
        fn draw_header(s: &str, x: i32, y: i32, w: i32, h: i32) {
            draw::push_clip(x, y, w, h);
            draw::draw_box(FrameType::ThinUpBox, x, y, w, h, Color::FrameDefault);
            draw::set_draw_color(Color::Black);
            draw::draw_text2(s, x, y, w, h, Align::Center);
            draw::pop_clip();
        }
        fn draw_data(s: &str, x: i32, y: i32, w: i32, h: i32, selected: bool) {
            draw::push_clip(x, y, w, h);
            if selected {
                draw::set_draw_color(Color::from_u32(0xD3D3D3));
            } else {
                draw::set_draw_color(Color::White);
            }
            draw::draw_rectf(x, y, w, h);
            draw::set_draw_color(Color::Gray0);
            draw::draw_text2(s, x, y, w, h, Align::Center);
            draw::draw_rect(x, y, w, h);
            draw::pop_clip();
        }
        {
            let t = t.clone();
            let table_c = table.clone();
            table.draw_cell(Box::new(move |ctx, row, col, x, y, w, h| {
                let mut inner = t.0.lock();
                match ctx {
                    TableContext::StartPage => draw::set_font(Font::Courier, 12),
                    TableContext::ColHeader => {
                        let s = inner.descriptor.cols[col as usize].0.as_ref();
                        draw_header(s, x, y, w, h)
                    }
                    TableContext::RowHeader => {
                        let s = inner.descriptor.rows[row as usize].as_ref();
                        draw_header(s, x, y, w, h)
                    }
                    TableContext::Cell => match inner.by_loc.get(&(row, col)) {
                        Some(v) => {
                            let mut v = v.0.lock();
                            v.last_used = Instant::now();
                            let s = format!("{}", v.last);
                            let selected = table_c.is_selected(row, col);
                            draw_data(&s, x, y, w, h, selected)
                        }
                        None => {
                            let path = inner.descriptor.rows[row as usize]
                                .append(inner.descriptor.cols[col as usize].0.as_ref());
                            let sub = inner.subscriber.durable_subscribe(path);
                            sub.updates(true, inner.updates.clone());
                            let id = sub.id();
                            let cell = Cell(Arc::new(Mutex::new(CellInner {
                                sub,
                                last: Value::Null,
                                last_used: Instant::now(),
                            })));
                            inner.by_loc.insert((row, col), cell.clone());
                            inner.by_id.insert(id, cell);
                            let selected = table_c.is_selected(row, col);
                            draw_data("#u", x, y, w, h, selected);
                        }
                    },
                    _ => (),
                }
            }));
        }
        t
    }
}

enum FromAsync {
    Table(Subscriber, Path, resolver::Table),
    Batch(Pooled<Vec<(SubId, Value)>>),
    Redraw,
}

enum ToAsync {
    Navigate(Path),
}

async fn async_main(
    cfg: Config,
    auth: resolver::Auth,
    incoming: mpsc::UnboundedReceiver<ToAsync>,
    updates: mpsc::Receiver<Pooled<Vec<(SubId, Value)>>>,
    mut outgoing: mpsc::UnboundedSender<FromAsync>,
    mut trigger: app::Sender<()>,
) {
    let subscriber = Subscriber::new(cfg, auth).expect("failed to create subscriber");
    let mut updates = updates.fuse();
    let mut incoming = incoming.fuse();
    let mut redraw = time::interval(Duration::from_secs(1)).fuse();
    let resolver = subscriber.resolver();
    loop {
        select_biased! {
            _ = redraw.next() => {
                outgoing.unbounded_send(FromAsync::Redraw);
                trigger.send(());
            },
            b = updates.next() => match b {
                None => break,
                Some(b) => {
                    outgoing.unbounded_send(FromAsync::Batch(b));
                    trigger.send(());
                }
            },
            i = incoming.next() => match i {
                None => break,
                Some(ToAsync::Navigate(p)) => {
                    match resolver.table(p.clone()).await {
                        Err(e) => error!("failed to get table descriptor {}", e),
                        Ok(t) => {
                            outgoing.unbounded_send(
                                FromAsync::Table(subscriber.clone(), p, t)
                            );
                            trigger.send(());
                        }
                    }
                }
            },
        }
    }
}

fn run_async(
    cfg: Config,
    auth: resolver::Auth,
    incoming: mpsc::UnboundedReceiver<ToAsync>,
    updates: mpsc::Receiver<Pooled<Vec<(SubId, Value)>>>,
    outgoing: mpsc::UnboundedSender<FromAsync>,
    trigger: app::Sender<()>,
) {
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(async_main(cfg, auth, incoming, updates, outgoing, trigger));
    });
}

pub(crate) fn run(cfg: Config, auth: resolver::Auth, path: Path) {
    let (tx_updates, rx_updates) = mpsc::channel(3);
    let (tx_gui_trigger, rx_gui_trigger) = app::channel();
    let (tx_gui_msg, mut rx_gui_msg) = mpsc::unbounded();
    let (tx_async, rx_async) = mpsc::unbounded();
    let app = App::default();
    let mut wind = Window::new(100, 100, 800, 600, "Hello from rust");
    wind.end();
    wind.show();
    run_async(cfg, auth, rx_async, rx_updates, tx_gui_msg, tx_gui_trigger);
    tx_async.unbounded_send(ToAsync::Navigate(path));
    let mut root = None;
    let mut redraw = false;
    while app.wait().unwrap() {
        if let Some(()) = rx_gui_trigger.recv() {
            match rx_gui_msg.try_next() {
                Err(_) => break,
                Ok(None) => (),
                Ok(Some(FromAsync::Table(sub, path, t))) => {
                    info!("building table {}", path);
                    let tbl =
                        NetidxTable::new(&mut wind, sub, tx_updates.clone(), path, t);
                    root = Some(tbl);
                }
                Ok(Some(FromAsync::Batch(mut b))) => match &mut root {
                    None => (),
                    Some(tbl) => {
                        info!("processing update");
                        let inner = tbl.0.lock();
                        for (id, v) in b.drain(..) {
                            if let Some(cell) = inner.by_id.get(&id) {
                                cell.0.lock().last = v;
                            }
                        }
                        redraw = true;
                    }
                },
                Ok(Some(FromAsync::Redraw)) => if redraw {
                    redraw = false;
                    app.redraw();
                }
            }
        }
    }
}
