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
        view.set_reorderable(true);
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
}

struct ExprEditor {
    ctx: BSCtx,
    view: gtksv::View,
    root: gtk::ScrolledWindow,
}

impl ExprEditor {
    fn new(
        ctx: BSCtx,
        on_change: impl Fn(expr::Expr) + 'static,
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
        root.add(&view);
        if let Some(buf) = view.get_buffer() {
            buf.insert_at_cursor(&init.to_string_pretty(80));
        }
        ExprEditor { ctx: ctx.clone(), view, root }
    }
}

pub(super) struct ExprInspector {
    root: gtk::Box,
    call_tree: CallTree,
    editor: ExprEditor,
}

impl ExprInspector {
    pub(super) fn new(
        ctx: BSCtx,
        on_change: impl Fn(expr::Expr) + 'static,
        init: expr::Expr,
    ) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let edit_dbg_pane = gtk::Paned::new(gtk::Orientation::Vertical);
        let call_tree = CallTree::new(ctx.clone());
        let editor = ExprEditor::new(ctx.clone(), on_change, &init);
        edit_dbg_pane.pack1(&editor.root, true, false);
        edit_dbg_pane.pack2(&call_tree.root, true, true);
        root.pack_start(&edit_dbg_pane, true, true, 5);
        /*
        let on_change: Rc<dyn Fn()> = Rc::new({
            let ctx = ctx.clone();
            let call_tree = call_tree.clone();
            let scheduled = Rc::new(Cell::new(false));
            let on_change = Rc::new(on_change);
            move || {
                if !scheduled.get() {
                    scheduled.set(true);
                    idle_add_local(clone!(
                        @strong ctx,
                        @strong call_tree,
                        @strong scheduled,
                        @strong on_change => move || {
                            if let Some(root) = store.get_iter_first() {
                                let expr = build_expr(&ctx, &store, &root);
                                on_change(expr)
                            }
                            scheduled.set(false);
                            glib::Continue(false)
                        }
                    ));
                }
            }
        });
        build_tree(&ctx, &store, None, &init);
         */
        call_tree.display_expr(None, &init);
        ExprInspector { root, call_tree, editor }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
