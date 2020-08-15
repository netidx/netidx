use super::{align_to_gtk, Widget, WidgetCtx};
use crate::browser::view;
use futures::channel::oneshot;
use gdk::{self, prelude::*};
use gluon::RootedThread;
use gtk::{self, prelude::*, Orientation};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use once_cell::unsync::Lazy;
use std::{collections::HashMap, rc::Rc, sync::Arc};

pub(super) struct Box {
    root: gtk::Box,
    children: Vec<Widget>,
}

impl Box {
    pub(super) fn new(
        ctx: WidgetCtx,
        vm: &Rc<Lazy<RootedThread>>,
        variables: &HashMap<String, Value>,
        spec: view::Box,
        selected_path: gtk::Label,
    ) -> Self {
        let dir = match spec.direction {
            view::Direction::Horizontal => Orientation::Horizontal,
            view::Direction::Vertical => Orientation::Vertical,
        };
        let root = gtk::Box::new(dir, 0);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            match s {
                view::Widget::BoxChild(view::BoxChild {
                    expand,
                    fill,
                    padding,
                    halign,
                    valign,
                    widget,
                }) => {
                    let w = Widget::new(
                        ctx.clone(),
                        vm,
                        variables,
                        (&**widget).clone(),
                        selected_path.clone(),
                    );
                    if let Some(r) = w.root() {
                        root.pack_start(r, *expand, *fill, *padding as u32);
                        if let Some(halign) = halign {
                            r.set_halign(align_to_gtk(*halign));
                        }
                        if let Some(valign) = valign {
                            r.set_valign(align_to_gtk(*valign));
                        }
                    }
                    children.push(w);
                }
                s => {
                    let w =
                        Widget::new(
                            ctx.clone(),
                            vm,
                            variables,
                            s.clone(),
                            selected_path.clone(),
                        );
                    if let Some(r) = w.root() {
                        root.add(r);
                    }
                    children.push(w);
                }
            }
        }
        Box { root, children }
    }

    pub(super) fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        updates: &Arc<IndexMap<SubId, Value>>,
    ) {
        for c in &self.children {
            c.update(waits, updates);
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        for c in &self.children {
            c.update_var(name, value);
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Grid {
    root: gtk::Grid,
    children: Vec<Vec<Widget>>,
}

impl Grid {
    pub(super) fn new(
        ctx: WidgetCtx,
        vm: &Rc<Lazy<RootedThread>>,
        variables: &HashMap<String, Value>,
        spec: view::Grid,
        selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::Grid::new();
        root.set_column_homogeneous(spec.homogeneous_columns);
        root.set_row_homogeneous(spec.homogeneous_rows);
        root.set_column_spacing(spec.column_spacing);
        root.set_row_spacing(spec.row_spacing);
        let children = spec
            .children
            .into_iter()
            .enumerate()
            .map(|(j, row)| {
                row.into_iter()
                    .enumerate()
                    .map(|(i, spec)| match spec {
                        view::Widget::GridChild(view::GridChild {
                            halign,
                            valign,
                            widget,
                        }) => {
                            let w = Widget::new(
                                ctx.clone(),
                                vm,
                                variables,
                                (&*widget).clone(),
                                selected_path.clone(),
                            );
                            if let Some(r) = w.root() {
                                root.attach(r, i as i32, j as i32, 1, 1);
                                if let Some(halign) = halign {
                                    r.set_halign(align_to_gtk(halign));
                                }
                                if let Some(valign) = valign {
                                    r.set_valign(align_to_gtk(valign));
                                }
                            }
                            w
                        }
                        widget => {
                            let w = Widget::new(
                                ctx.clone(),
                                vm,
                                variables,
                                widget.clone(),
                                selected_path.clone(),
                            );
                            if let Some(r) = w.root() {
                                root.attach(r, i as i32, j as i32, 1, 1);
                            }
                            w
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Grid { root, children }
    }

    pub(super) fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        updates: &Arc<IndexMap<SubId, Value>>,
    ) {
        for row in &self.children {
            for child in row {
                child.update(waits, updates);
            }
        }
    }

    pub(super) fn update_var(&self, name: &str, value: &Value) {
        for row in &self.children {
            for child in row {
                child.update_var(name, value);
            }
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
