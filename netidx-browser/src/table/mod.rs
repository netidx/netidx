mod raeified;
mod shared;

use super::{BSCtx, BSCtxRef, BSNode, BWidget};
use crate::bscript::LocalEvent;
use futures::channel::oneshot;
use gio::prelude::*;
use glib::{self, clone, idle_add_local, ControlFlow};
use gtk::{prelude::*, Adjustment, Label, ScrolledWindow};
use netidx::{path::Path, subscriber::Value};
use netidx_bscript::vm;
use netidx_protocols::view;
use raeified::RaeifiedTable;
use shared::SharedState;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

enum TableState {
    Resolving(Path),
    Raeified(RaeifiedTable),
    Refresh(Path),
}

pub(super) struct Table {
    column_editable: BSNode,
    column_filter: BSNode,
    columns_resizable: BSNode,
    column_types: BSNode,
    column_widths: BSNode,
    selection_mode: BSNode,
    selection: BSNode,
    path: BSNode,
    refresh: BSNode,
    row_filter: BSNode,
    show_row_name: BSNode,
    sort_mode: BSNode,
    shared: Rc<SharedState>,
    state: Rc<RefCell<TableState>>,
    visible: Cell<bool>,
}

impl Table {
    pub(super) fn new(
        ctx: BSCtx,
        spec: view::Table,
        scope: Path,
        selected_path: Label,
    ) -> Table {
        let path = BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.path);
        let sort_mode =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.sort_mode);
        let column_filter =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_filter);
        let row_filter =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.row_filter);
        let column_editable =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_editable);
        let selection_mode =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.selection_mode);
        let selection =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.selection);
        let show_row_name =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.show_row_name);
        let columns_resizable = BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.columns_resizable,
        );
        let column_types =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_types);
        let column_widths =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_widths);
        let refresh =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.refresh);
        let on_select =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.on_select);
        let on_activate =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.on_activate);
        let on_edit =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.on_edit);
        let on_header_click =
            BSNode::compile(&mut *ctx.borrow_mut(), scope, spec.on_header_click);
        let root = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        let shared = Rc::new(SharedState::new(
            ctx.clone(),
            selected_path,
            root,
            on_activate,
            on_edit,
            on_header_click,
            on_select,
        ));
        shared.set_path(path.current(&mut ctx.borrow_mut()));
        shared.set_sort_mode(sort_mode.current(&mut ctx.borrow_mut()));
        shared.set_column_filter(column_filter.current(&mut ctx.borrow_mut()));
        shared.set_row_filter(row_filter.current(&mut ctx.borrow_mut()));
        shared.set_column_editable(column_editable.current(&mut ctx.borrow_mut()));
        shared.set_selection_mode(selection_mode.current(&mut ctx.borrow_mut()));
        shared.set_selection(selection.current(&mut ctx.borrow_mut()));
        shared.set_show_row_name(show_row_name.current(&mut ctx.borrow_mut()));
        shared.set_columns_resizable(columns_resizable.current(&mut ctx.borrow_mut()));
        shared.set_column_types(column_types.current(&mut ctx.borrow_mut()));
        shared.set_column_widths(column_widths.current(&mut ctx.borrow_mut()));
        let state = Rc::new(RefCell::new({
            let path = &*shared.path.borrow();
            shared.ctx.borrow().user.backend.resolve_table(path.clone());
            TableState::Resolving(path.clone())
        }));
        shared.root.vadjustment().connect_value_changed(clone!(@weak state => move |_| {
            idle_add_local(clone!(@weak state => @default-return ControlFlow::Break, move || {
                match &*state.borrow() {
                    TableState::Raeified(t) => t.update_subscriptions(),
                    TableState::Refresh {..} | TableState::Resolving(_) => ()
                }
                ControlFlow::Break
            }));
        }));
        Table {
            shared,
            column_editable,
            column_filter,
            columns_resizable,
            column_types,
            column_widths,
            selection_mode,
            selection,
            path,
            refresh,
            row_filter,
            show_row_name,
            sort_mode,
            state,
            visible: Cell::new(true),
        }
    }

    fn refresh(&self, ctx: BSCtxRef, force: bool) {
        let state = &mut *self.state.borrow_mut();
        let path = &*self.shared.path.borrow();
        match state {
            TableState::Refresh(rpath) if &*path == rpath => (),
            TableState::Resolving(rpath) if &*path == rpath => (),
            TableState::Raeified(table) if !force && &table.path == &*path => {
                *state = TableState::Refresh(path.clone());
            }
            TableState::Refresh(_)
            | TableState::Resolving(_)
            | TableState::Raeified(_) => {
                ctx.user.backend.resolve_table(path.clone());
                *state = TableState::Resolving(path.clone());
            }
        }
    }

    fn raeify(&self) {
        let state = &self.state;
        let visible = &self.visible;
        let shared = &self.shared;
        idle_add_local(
            clone!(@strong state, @strong visible, @strong shared => move || {
                if let Some(c) = shared.root.child() {
                    shared.root.remove(&c);
                }
                let table = RaeifiedTable::new(shared.clone());
                if visible.get() {
                    table.view().show();
                }
                idle_add_local(clone!(@strong table => move || {
                    table.update_subscriptions();
                    ControlFlow::Break
                }));
                *state.borrow_mut() = TableState::Raeified(table);
                ControlFlow::Break
            }),
        );
    }
}

impl BWidget for Table {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        let mut re = false;
        re |= self.shared.set_path(self.path.update(ctx, event));
        re |= self.shared.set_sort_mode(self.sort_mode.update(ctx, event));
        re |= self.shared.set_column_filter(self.column_filter.update(ctx, event));
        re |= self.shared.set_row_filter(self.row_filter.update(ctx, event));
        re |= self.shared.set_column_editable(self.column_editable.update(ctx, event));
        re |= self.shared.set_selection_mode(self.selection_mode.update(ctx, event));
        re |= self.shared.set_selection(self.selection.update(ctx, event));
        re |= self.shared.set_show_row_name(self.show_row_name.update(ctx, event));
        re |=
            self.shared.set_columns_resizable(self.columns_resizable.update(ctx, event));
        re |= self.shared.set_column_types(self.column_types.update(ctx, event));
        re |= self.shared.set_column_widths(self.column_widths.update(ctx, event));
        let force_refresh = self.refresh.update(ctx, event).is_some();
        self.shared.on_activate.borrow_mut().update(ctx, event);
        self.shared.on_select.borrow_mut().update(ctx, event);
        self.shared.on_edit.borrow_mut().update(ctx, event);
        self.shared.on_header_click.borrow_mut().update(ctx, event);
        if re || force_refresh {
            self.refresh(ctx, force_refresh);
        }
        match &*self.state.borrow() {
            TableState::Raeified(table) => table.update(ctx, waits, event),
            TableState::Refresh(_) => self.raeify(),
            TableState::Resolving(rpath) => match event {
                vm::Event::Netidx(_, _)
                | vm::Event::Rpc(_, _)
                | vm::Event::Timer(_)
                | vm::Event::Variable(_, _, _)
                | vm::Event::User(LocalEvent::Event(_))
                | vm::Event::User(LocalEvent::Poll(_)) => (),
                vm::Event::User(LocalEvent::TableResolved(path, descriptor)) => {
                    if path == rpath {
                        match self.selection.current(ctx) {
                            None | Some(Value::Null) => {
                                self.shared.selected.borrow_mut().clear();
                            }
                            v @ Some(_) => {
                                self.shared.set_selection(v);
                            }
                        }
                        *self.shared.original_descriptor.borrow_mut() =
                            Rc::clone(descriptor);
                        self.raeify()
                    }
                }
            },
        }
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.shared.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.visible.set(v);
        self.shared.root.set_visible(v);
        match &mut *self.state.borrow_mut() {
            TableState::Raeified(t) => {
                if v {
                    t.view().show();
                    idle_add_local(clone!(@strong t => move || {
                        t.update_subscriptions();
                        ControlFlow::Continue
                    }));
                } else {
                    t.view().hide()
                }
            }
            TableState::Refresh(_) | TableState::Resolving(_) => (),
        }
    }
}
