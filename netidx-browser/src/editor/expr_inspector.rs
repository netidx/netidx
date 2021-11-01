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

struct ExprEditor {
    ctx: BSCtx,
    view: gtksv::View,
    error: gtk::Label,
    root: gtk::Box,
}

impl ExprEditor {
    fn new(ctx: BSCtx, on_change: Rc<dyn Fn(expr::Expr)>, init: &expr::Expr) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let error = gtk::Label::new(None);
        root.pack_start(&error, false, false, 0);
        let editorwin =
            gtk::ScrolledWindow::new(None::<&gtk::Adjustment>, None::<&gtk::Adjustment>);
        editorwin.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Automatic);
        editorwin.set_property_expand(true);
        root.pack_start(&editorwin, true, true, 0);
        let view = gtksv::ViewBuilder::new()
            .insert_spaces_instead_of_tabs(true)
            .show_line_numbers(true)
            .build();
        view.set_property_expand(true);
        editorwin.add(&view);
        if let Some(buf) = view.get_buffer() {
            buf.set_text(&init.to_string_pretty(80));
            buf.connect_changed(clone!(@strong on_change, @weak error => move |buf: &gtk::TextBuffer| {
                if let Some(text) = buf.get_slice(&buf.get_start_iter(), &buf.get_end_iter(), false) {
                    match text.parse::<expr::Expr>() {
                        Err(e) => {
                            let m = glib::markup_escape_text(&format!("{}", e));
                            error.set_markup(&format!("<span foreground='red'>{}</span>", m));
                        }
                        Ok(expr) => {
                            error.set_markup("<span></span>");
                            on_change(expr);
                        }
                    }
                }
            }));
        }
        ExprEditor { ctx: Rc::clone(&ctx), view, error, root }
    }
}

pub(super) struct ExprInspector {
    root: gtk::Box,
    _call_tree: Rc<CallTree>,
    _editor: ExprEditor,
}

impl ExprInspector {
    pub(super) fn new(
        ctx: BSCtx,
        on_change: impl Fn(expr::Expr) + 'static,
        init: expr::Expr,
    ) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let edit_dbg_pane = gtk::Paned::new(gtk::Orientation::Vertical);
        let call_tree = Rc::new(CallTree::new(Rc::clone(&ctx)));
        let on_change: Rc<dyn Fn(expr::Expr)> = Rc::new({
            let call_tree = call_tree.clone();
            move |e: expr::Expr| {
                call_tree.display(&e);
                on_change(e);
            }
        });
        let editor = ExprEditor::new(Rc::clone(&ctx), on_change, &init);
        edit_dbg_pane.pack1(&editor.root, true, false);
        edit_dbg_pane.pack2(&call_tree.root, true, true);
        root.pack_start(&edit_dbg_pane, true, true, 5);
        call_tree.display(&init);
        ExprInspector { root, _call_tree: call_tree, _editor: editor }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
