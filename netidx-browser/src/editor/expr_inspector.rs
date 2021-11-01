use super::super::{bscript, BSCtx};
use super::{util::TwoColGrid, OnChange};
use glib::{clone, idle_add_local, prelude::*, subclass::prelude::*};
use gtk::{self, prelude::*};
use netidx::{chars::Chars, subscriber::Value};
use netidx_bscript::expr;
use sourceview4 as gtksv;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};

#[derive(Clone, GBoxed)]
#[gboxed(type_name = "NetidxExprInspectorWrap")]
struct ExprWrap(Arc<dyn Fn(&Value)>);

fn add_watch(
    ctx: &BSCtx,
    store: &gtk::TreeStore,
    iter: &gtk::TreeIter,
    id: expr::ExprId,
) {
    let watch: Arc<dyn Fn(&Value)> = {
        let store = store.clone();
        let iter = iter.clone();
        Arc::new(move |v: &Value| store.set_value(&iter, 1, &format!("{}", v).to_value()))
    };
    ctx.borrow_mut().dbg_ctx.add_watch(id, &watch);
    store.set_value(&iter, 2, &ExprWrap(watch).to_value());
}

struct CallTree {
    root: gtk::ScrolledWindow,
    store: gtk::TreeStore,
    ctx: BSCtx,
}

impl CallTree {
    fn new(ctx: BSCtx) -> Self {
        let root =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        root.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        root.set_property_expand(false);
        let store = gtk::TreeStore::new(&[
            String::static_type(),
            String::static_type(),
            ExprWrap::static_type(),
        ]);
        let view = gtk::TreeView::new();
        root.add(&view);
        for (i, name) in ["kind", "current"].iter().enumerate() {
            view.append_column(&{
                let column = gtk::TreeViewColumn::new();
                let cell = gtk::CellRendererText::new();
                column.pack_start(&cell, true);
                column.set_title(name);
                column.add_attribute(&cell, "text", i as i32);
                column
            });
        }
        view.set_model(Some(&store));
        view.set_reorderable(false);
        view.set_enable_tree_lines(true);
        CallTree { root, store, ctx }
    }

    fn clear(&self) {
        self.store.clear()
    }

    fn display_expr(&self, parent: Option<&gtk::TreeIter>, s: &expr::Expr) {
        let iter = self.store.insert_before(parent, None);
        match s {
            expr::Expr { kind: expr::ExprKind::Constant(v), id } => {
                self.store.set_value(&iter, 0, &"constant".to_value());
                add_watch(&self.ctx, &self.store, &iter, *id);
            }
            expr::Expr { kind: expr::ExprKind::Apply { args, function }, id } => {
                self.store.set_value(&iter, 0, &function.to_value());
                add_watch(&self.ctx, &self.store, &iter, *id);
                for s in args {
                    self.display_expr(Some(&iter), s)
                }
            }
        }
    }

    fn display(&self, e: &expr::Expr) {
        self.clear();
        self.display_expr(None, e)
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
    call_tree: CallTree,
    error: ErrorDisplay,
}

impl Tools {
    fn new(ctx: BSCtx) -> Self {
        let root = gtk::Notebook::new();
        let call_tree = CallTree::new(ctx.clone());
        let call_tree_lbl = gtk::Label::new(Some("Call Tree"));
        root.append_page(&call_tree.root, Some(&call_tree_lbl));
        let error = ErrorDisplay::new();
        root.append_page(&error.root, Some(&error.error_lbl));
        Tools { root, call_tree, error }
    }

    fn display(&self, e: &expr::Expr) {
        self.call_tree.display(e);
        self.error.clear()
    }

    fn set_error(&self, msg: &str) {
        self.error.display(msg)
    }
}

struct ExprEditor {
    root: gtk::ScrolledWindow,
}

impl ExprEditor {
    fn new(
        on_change: impl Fn(expr::Expr) + 'static,
        tools: Rc<Tools>,
        init: &expr::Expr,
    ) -> Self {
        let root =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        root.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        root.set_property_expand(true);
        let view = gtksv::ViewBuilder::new()
            .insert_spaces_instead_of_tabs(true)
            .show_line_numbers(true)
            .build();
        view.set_property_expand(true);
        root.add(&view);
        if let Some(buf) = view.get_buffer() {
            buf.set_text(&init.to_string_pretty(80));
            buf.connect_changed(clone!(@strong tools => move |buf: &gtk::TextBuffer| {
                if let Some(text) = buf.get_slice(&buf.get_start_iter(), &buf.get_end_iter(), false) {
                    match text.parse::<expr::Expr>() {
                        Err(e) => tools.set_error(&format!("{}", e)),
                        Ok(expr) => {
                            tools.display(&expr);
                            on_change(expr)
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
        on_change: impl Fn(expr::Expr) + 'static,
        init: expr::Expr,
    ) -> Self {
        let root = gtk::Paned::new(gtk::Orientation::Vertical);
        let tools = Rc::new(Tools::new(ctx.clone()));
        let editor = ExprEditor::new(on_change, tools.clone(), &init);
        root.pack1(&editor.root, true, false);
        root.pack2(&tools.root, true, true);
        tools.display(&init);
        ExprInspector { root, _tools: tools, _editor: editor }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
