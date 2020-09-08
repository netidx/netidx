use super::{set_common_props, Widget, WidgetCtx};
use crate::browser::view;
use futures::channel::oneshot;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*, Orientation};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use std::{collections::HashMap, sync::Arc, cmp::max};

pub(super) struct Box {
    root: gtk::Box,
    pub(super) children: Vec<Widget>,
}

impl Box {
    pub(super) fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Box,
        selected_path: gtk::Label,
    ) -> Self {
        let dir = match spec.direction {
            view::Direction::Horizontal => Orientation::Horizontal,
            view::Direction::Vertical => Orientation::Vertical,
        };
        let root = gtk::Box::new(dir, 0);
        root.set_homogeneous(spec.homogeneous);
        root.set_spacing(spec.spacing as i32);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            match s.kind {
                view::Widget::BoxChild(view::BoxChild {
                    pack,
                    padding,
                    widget,
                }) => {
                    let w = Widget::new(
                        ctx.clone(),
                        variables,
                        (&**widget).clone(),
                        selected_path.clone(),
                    );
                    if let Some(r) = w.root() {
                        match pack {
                            view::Pack::Start => {
                                root.pack_start(r, false, false, *padding as u32)
                            }
                            view::Pack::End => {
                                root.pack_end(r, false, false, *padding as u32)
                            }
                        }
                        set_common_props(s.props.clone(), r);
                    }
                    children.push(w);
                }
                k => {
                    let w = Widget::new(
                        ctx.clone(),
                        variables,
                        k.clone(),
                        selected_path.clone(),
                    );
                    if let Some(r) = w.root() {
                        root.add(r);
                        set_common_props(s.props.clone(), r);
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
        variables: &HashMap<String, Value>,
        spec: view::Grid,
        selected_path: gtk::Label,
    ) -> Self {
        let root = gtk::Grid::new();
        root.set_column_homogeneous(spec.homogeneous_columns);
        root.set_row_homogeneous(spec.homogeneous_rows);
        root.set_column_spacing(spec.column_spacing);
        root.set_row_spacing(spec.row_spacing);
        let mut i = 0;
        let mut j = 0;
        let children = spec
            .children
            .into_iter()
            .map(|row| {
                let mut max_height = 1;
                let row = row.into_iter()
                    .map(|spec| match spec.kind {
                        view::Widget::GridChild(view::GridChild {
                            width,
                            height,
                            widget,
                        }) => {
                            let w = Widget::new(
                                ctx.clone(),
                                variables,
                                (&*widget).clone(),
                                selected_path.clone(),
                            );
                            if let Some(r) = w.root() {
                                root.attach(r, i as i32, j as i32, width, height);
                                set_common_props(spec.props.clone(), r);
                            }
                            i += width;
                            max_height = max(max_height, height);
                            w
                        }
                        widget => {
                            let w = Widget::new(
                                ctx.clone(),
                                variables,
                                widget.clone(),
                                selected_path.clone(),
                            );
                            if let Some(r) = w.root() {
                                root.attach(r, i as i32, j as i32, 1, 1);
                                set_common_props(spec.props.clone(), r);
                            }
                            i += 1;
                            w
                        }
                    })
                    .collect::<Vec<_>>();
                j += max_height;
                row
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
