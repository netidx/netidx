use super::super::{bscript::LocalEvent, util::ask_modal, BSCtx};
use super::{completion::BScriptCompletionProvider, Scope};
use chrono::prelude::*;
use fxhash::FxHashMap;
use gdk::keys;
use glib::idle_add_local_once;
use glib::{clone, prelude::*, subclass::prelude::*, thread_guard::ThreadGuard};
use gtk::{self, prelude::*};
use netidx::subscriber::Value;
use netidx_bscript::{expr, vm};
use parking_lot::Mutex;
use sourceview4::{self as sv, prelude::*, traits::ViewExt};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
    sync::Arc,
};

#[derive(Clone, Boxed)]
#[boxed_type(name = "NetidxExprInspectorWrap")]
struct ExprWrap(Arc<dyn Fn(&DateTime<Local>, &Option<vm::Event<LocalEvent>>, &Value)>);

fn log_expr_val(
    log: &gtk::ListStore,
    expr: &expr::Expr,
    ts: &DateTime<Local>,
    e: &Option<vm::Event<LocalEvent>>,
    v: &Value,
) {
    const MAX: usize = 1000;
    let i = log.append();
    log.set_value(&i, 0, &format!("{}", ts).to_value());
    log.set_value(&i, 1, &format!("{}", expr).to_value());
    match e {
        Some(e) => log.set_value(&i, 2, &format!("{:?}", e).to_value()),
        None => log.set_value(&i, 2, &"initialization".to_value()),
    }
    log.set_value(&i, 3, &format!("{}", v).to_value());
    if log.iter_n_children(None) as usize > MAX {
        if let Some(iter) = log.iter_first() {
            log.remove(&iter);
        }
    }
}

fn add_watch(
    ctx: &BSCtx,
    store: &gtk::TreeStore,
    iter: &gtk::TreeIter,
    log: &gtk::ListStore,
    expr: expr::Expr,
) {
    let id = expr.id;
    let watch: Arc<
        dyn Fn(&DateTime<Local>, &Option<vm::Event<LocalEvent>>, &Value) + Send + Sync,
    > = {
        struct CtxInner {
            store: gtk::TreeStore,
            iter: gtk::TreeIter,
            log: gtk::ListStore,
        }
        struct Ctx(Mutex<ThreadGuard<CtxInner>>);
        let ctx = Ctx(Mutex::new(ThreadGuard::new(CtxInner {
            store: store.clone(),
            iter: iter.clone(),
            log: log.clone(),
        })));
        Arc::new(
            move |ts: &DateTime<Local>, e: &Option<vm::Event<LocalEvent>>, v: &Value| {
                let inner = ctx.0.lock();
                let inner = inner.get_ref();
                inner.store.set_value(&inner.iter, 1, &format!("{}", v).to_value());
                log_expr_val(&inner.log, &expr, ts, e, v)
            },
        )
    };
    ctx.borrow_mut().dbg_ctx.add_watch(id, &watch);
    store.set_value(&iter, 2, &ExprWrap(watch).to_value());
}

struct DataFlow {
    call_root: gtk::ScrolledWindow,
    call_store: gtk::TreeStore,
    event_root: gtk::ScrolledWindow,
    event_store: gtk::ListStore,
    ctx: BSCtx,
}

impl DataFlow {
    fn new(ctx: BSCtx) -> Self {
        let call_root =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        call_root.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        call_root.set_expand(false);
        let event_root =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        event_root.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        event_root.set_expand(false);
        let call_store = gtk::TreeStore::new(&[
            String::static_type(),
            String::static_type(),
            ExprWrap::static_type(),
        ]);
        let event_store = gtk::ListStore::new(&[
            String::static_type(),
            String::static_type(),
            String::static_type(),
            String::static_type(),
        ]);
        let call_view = gtk::TreeView::new();
        let event_view = gtk::TreeView::new();
        call_root.add(&call_view);
        event_root.add(&event_view);
        for (i, name) in ["kind", "current"].iter().enumerate() {
            call_view.append_column(&{
                let column = gtk::TreeViewColumn::new();
                let cell = gtk::CellRendererText::new();
                CellLayoutExt::pack_start(&column, &cell, true);
                column.set_resizable(true);
                column.set_title(name);
                CellLayoutExt::add_attribute(&column, &cell, "text", i as i32);
                column
            });
        }
        for (i, name) in ["timestamp", "expr", "event", "result"].iter().enumerate() {
            event_view.append_column(&{
                let column = gtk::TreeViewColumn::new();
                let cell = gtk::CellRendererText::new();
                CellLayoutExt::pack_start(&column, &cell, true);
                column.set_resizable(true);
                column.set_title(name);
                CellLayoutExt::add_attribute(&column, &cell, "text", i as i32);
                column
            });
        }
        call_view.set_model(Some(&call_store));
        call_view.set_reorderable(false);
        call_view.set_enable_tree_lines(true);
        event_view.set_model(Some(&event_store));
        event_view.set_reorderable(false);
        DataFlow { call_root, call_store, event_root, event_store, ctx }
    }

    fn clear(&self) {
        self.call_store.clear();
        self.event_store.clear();
    }

    fn display_expr(
        &self,
        exprs: &mut FxHashMap<expr::ExprId, expr::Expr>,
        parent: Option<&gtk::TreeIter>,
        s: &expr::Expr,
    ) {
        let iter = self.call_store.insert_before(parent, None);
        match s {
            expr::Expr { kind: expr::ExprKind::Constant(v), id } => {
                exprs.insert(*id, s.clone());
                self.call_store.set_value(&iter, 0, &"constant".to_value());
                self.call_store.set_value(&iter, 1, &format!("{}", v).to_value());
            }
            expr::Expr { kind: expr::ExprKind::Apply { args, function }, id } => {
                exprs.insert(*id, s.clone());
                self.call_store.set_value(&iter, 0, &function.to_value());
                if let Some((_, v)) = self.ctx.borrow().dbg_ctx.get_current(&id) {
                    self.call_store.set_value(&iter, 1, &format!("{}", v).to_value());
                }
                add_watch(
                    &self.ctx,
                    &self.call_store,
                    &iter,
                    &self.event_store,
                    s.clone(),
                );
                for s in args {
                    self.display_expr(exprs, Some(&iter), s)
                }
            }
        }
    }

    fn populate_log(&self, exprs: FxHashMap<expr::ExprId, expr::Expr>) {
        for (id, (ts, ev, v)) in self.ctx.borrow().dbg_ctx.iter_events() {
            if let Some(expr) = exprs.get(id) {
                log_expr_val(&self.event_store, expr, ts, ev, v);
            }
        }
    }

    fn display(&self, e: &expr::Expr) {
        self.clear();
        let mut exprs = HashMap::default();
        self.display_expr(&mut exprs, None, e);
        self.populate_log(exprs);
    }
}

struct ErrorDisplay {
    root: gtk::ScrolledWindow,
    error_body: gtk::Label,
    error_lbl: gtk::Label,
}

impl ErrorDisplay {
    fn new() -> Self {
        let root =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        root.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        let error_body = gtk::Label::new(None);
        let error_lbl = gtk::Label::new(None);
        error_lbl.set_markup("<span>Errors</span>");
        root.add(&error_body);
        ErrorDisplay { root, error_body, error_lbl }
    }

    fn clear(&self) {
        self.error_lbl.set_markup("<span>Errors</span>");
        self.error_body.set_markup("<span></span>");
    }

    fn display(&self, msg: &str) {
        self.error_lbl.set_markup("<span foreground='red'>Errors</span>");
        let m = glib::markup_escape_text(msg);
        self.error_body.set_markup(&format!("<span foreground='red'>{}</span>", m));
    }
}

struct Tools {
    root: gtk::Notebook,
    data_flow: DataFlow,
    error: ErrorDisplay,
}

impl Tools {
    fn new(ctx: BSCtx) -> Self {
        let root = gtk::Notebook::new();
        let data_flow = DataFlow::new(ctx.clone());
        let call_tree_lbl = gtk::Label::new(Some("Call Tree"));
        let event_log_lbl = gtk::Label::new(Some("Event Log"));
        root.append_page(&data_flow.call_root, Some(&call_tree_lbl));
        root.append_page(&data_flow.event_root, Some(&event_log_lbl));
        let error = ErrorDisplay::new();
        root.append_page(&error.root, Some(&error.error_lbl));
        Tools { root, data_flow, error }
    }

    fn display(&self, e: &expr::Expr) {
        self.data_flow.display(e);
        self.error.clear()
    }

    fn set_error(&self, msg: &str) {
        self.error.display(msg)
    }

    fn clear_error(&self) {
        self.error.clear()
    }
}

struct ExprEditor {
    root: gtk::ScrolledWindow,
}

impl ExprEditor {
    fn new(
        tools: Rc<Tools>,
        save_button: gtk::ToolButton,
        unsaved: Rc<Cell<bool>>,
        ctx: BSCtx,
        scope: Scope,
        expr: Rc<RefCell<expr::Expr>>,
    ) -> Self {
        let root =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        root.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        root.set_expand(true);
        let view = sv::View::builder()
            .insert_spaces_instead_of_tabs(true)
            .show_line_numbers(true)
            .auto_indent(true)
            .build();
        view.set_expand(true);
        if let Some(completion) = view.completion() {
            let provider = BScriptCompletionProvider::new();
            provider.imp().init(ctx, scope);
            completion.add_provider(&provider).expect("bscript completion");
            completion
                .add_provider(&sv::CompletionWords::default())
                .expect("words completion");
            view.connect_key_press_event(|view, key| {
                let kv = key.keyval();
                if (kv == keys::constants::space
                    && key.state().contains(gdk::ModifierType::CONTROL_MASK))
                    || kv == keys::constants::Tab
                {
                    view.emit_show_completion();
                    Inhibit(true)
                } else {
                    Inhibit(false)
                }
            });
        }
        root.add(&view);
        if let Some(buf) = view.buffer() {
            buf.set_text(&expr.borrow().to_string_pretty(80));
            buf.connect_changed(clone!(@strong tools => move |buf: &gtk::TextBuffer| {
                unsaved.set(true);
                if let Some(text) = buf.slice(&buf.start_iter(), &buf.end_iter(), false) {
                    match text.parse::<expr::Expr>() {
                        Err(e) => {
                            tools.set_error(&format!("{}", e));
                            save_button.set_sensitive(false);
                        },
                        Ok(e) => {
                            *expr.borrow_mut() = e;
                            save_button.set_sensitive(true);
                            tools.clear_error();
                        },
                    }
                }
            }));
        }
        ExprEditor { root }
    }
}

pub(super) struct ExprInspector {
    root: gtk::Paned,
    _tools: Rc<Tools>,
    _editor: ExprEditor,
}

impl ExprInspector {
    pub(super) fn new(
        ctx: BSCtx,
        window: &gtk::Window,
        on_change: impl Fn(expr::Expr) + 'static,
        scope: Scope,
        init: expr::Expr,
    ) -> Self {
        let unsaved = Rc::new(Cell::new(false));
        let expr = Rc::new(RefCell::new(init));
        let headerbar = gtk::HeaderBar::new();
        let save_img =
            gtk::Image::from_icon_name(Some("media-floppy"), gtk::IconSize::SmallToolbar);
        let save_button = gtk::ToolButton::new(Some(&save_img), None);
        save_button.set_sensitive(false);
        headerbar.pack_start(&save_button);
        headerbar.set_show_close_button(true);
        window.set_titlebar(Some(&headerbar));
        let root = gtk::Paned::new(gtk::Orientation::Vertical);
        let tools = Rc::new(Tools::new(ctx.clone()));
        let editor = ExprEditor::new(
            tools.clone(),
            save_button.clone(),
            unsaved.clone(),
            ctx.clone(),
            scope,
            expr.clone(),
        );
        save_button.connect_clicked(
            clone!(@strong unsaved, @strong expr, @strong tools => move |b| {
                let expr = &*expr.borrow();
                tools.display(expr);
                b.set_sensitive(false);
                unsaved.set(false);
                on_change(expr.clone());
            }),
        );
        window.connect_delete_event(clone!(@strong unsaved => move |w, _| {
            if !unsaved.get() || ask_modal(w, "Unsaved changes will be lost") {
                Inhibit(false)
            } else {
                Inhibit(true)
            }
        }));
        root.pack1(&editor.root, true, false);
        root.pack2(&tools.root, true, true);
        tools.display(&*expr.borrow());
        idle_add_local_once(clone!(@weak root => move || {
            root.set_position_set(true);
        }));
        ExprInspector { root, _tools: tools, _editor: editor }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
