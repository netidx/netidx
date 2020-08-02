use super::{align_to_gtk, Widget, WidgetCtx};
use crate::browser::view;
use futures::channel::oneshot;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::subscriber::{SubId, Value};
use std::{collections::HashMap, sync::Arc};

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
        let children = spec
            .children
            .into_iter()
            .enumerate()
            .map(|(j, row)| {
                row.into_iter()
                    .enumerate()
                    .map(|(i, spec)| {
                        let w = Widget::new(
                            ctx.clone(),
                            variables,
                            spec.widget.clone(),
                            selected_path.clone(),
                        );
                        if let Some(r) = w.root() {
                            root.attach(r, i as i32, j as i32, 1, 1);
                            if let Some(halign) = spec.halign {
                                r.set_halign(align_to_gtk(halign));
                            }
                            if let Some(valign) = spec.valign {
                                r.set_valign(align_to_gtk(valign));
                            }
                        }
                        w
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
