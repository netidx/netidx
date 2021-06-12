use super::{set_common_props, BSCtx, Target, Widget, DEFAULT_PROPS};
use crate::view;
use futures::channel::oneshot;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*, Orientation};
use std::cmp::max;

pub(super) struct Box {
    root: gtk::Box,
    pub(super) children: Vec<Widget>,
}

impl Box {
    pub(super) fn new(ctx: &BSCtx, spec: view::Box, selected_path: gtk::Label) -> Self {
        fn is_fill(a: view::Align) -> bool {
            match a {
                view::Align::Fill | view::Align::Baseline => true,
                view::Align::Start | view::Align::Center | view::Align::End => false,
            }
        }
        let dir = match spec.direction {
            view::Direction::Horizontal => Orientation::Horizontal,
            view::Direction::Vertical => Orientation::Vertical,
        };
        let root = gtk::Box::new(dir, 0);
        root.set_homogeneous(spec.homogeneous);
        root.set_spacing(spec.spacing as i32);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            match &s.kind {
                view::WidgetKind::BoxChild(view::BoxChild { pack, padding, widget }) => {
                    let w = Widget::new(ctx, (&**widget).clone(), selected_path.clone());
                    if let Some(r) = w.root() {
                        let props = s.props.unwrap_or(DEFAULT_PROPS);
                        let (expand, fill) = match spec.direction {
                            view::Direction::Horizontal => {
                                (props.hexpand, is_fill(props.halign))
                            }
                            view::Direction::Vertical => {
                                (props.vexpand, is_fill(props.valign))
                            }
                        };
                        match pack {
                            view::Pack::Start => {
                                root.pack_start(r, expand, fill, *padding as u32)
                            }
                            view::Pack::End => {
                                root.pack_end(r, expand, fill, *padding as u32)
                            }
                        }
                        set_common_props(props, r);
                    }
                    children.push(w);
                }
                _ => {
                    let w = Widget::new(ctx, s.clone(), selected_path.clone());
                    if let Some(r) = w.root() {
                        root.add(r);
                        set_common_props(s.props.unwrap_or(DEFAULT_PROPS), r);
                    }
                    children.push(w);
                }
            }
        }
        Box { root, children }
    }

    pub(super) fn update(
        &self,
        ctx: &BSCtx,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &Target,
    ) {
        for c in &self.children {
            c.update(ctx, waits, event);
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Grid {
    root: gtk::Grid,
    pub(super) children: Vec<Vec<Widget>>,
}

impl Grid {
    pub(super) fn new(ctx: &BSCtx, spec: view::Grid, selected_path: gtk::Label) -> Self {
        let root = gtk::Grid::new();
        let attach_child = |props: view::WidgetProps,
                            spec: view::GridChild,
                            max_height: &mut i32,
                            i: &mut i32,
                            j: i32|
         -> Widget {
            let height = spec.height as i32;
            let width = spec.width as i32;
            let w = Widget::new(ctx, (&*spec.widget).clone(), selected_path.clone());
            if let Some(r) = w.root() {
                root.attach(r, *i, j, width, height);
                set_common_props(props, r);
            }
            *i += width;
            *max_height = max(*max_height, height);
            w
        };
        let attach_normal = |spec: view::Widget, i: &mut i32, j: i32| -> Widget {
            let w = Widget::new(ctx, spec.clone(), selected_path.clone());
            if let Some(r) = w.root() {
                root.attach(r, *i, j, 1, 1);
                set_common_props(spec.props.unwrap_or(DEFAULT_PROPS), r);
            }
            *i += 1;
            w
        };
        root.set_column_homogeneous(spec.homogeneous_columns);
        root.set_row_homogeneous(spec.homogeneous_rows);
        root.set_column_spacing(spec.column_spacing);
        root.set_row_spacing(spec.row_spacing);
        let mut i = 0i32;
        let mut j = 0i32;
        let children = spec
            .rows
            .into_iter()
            .map(|spec| {
                let mut max_height = 1;
                let row = match spec.kind {
                    view::WidgetKind::GridChild(c) => {
                        vec![attach_child(
                            spec.props.unwrap_or(DEFAULT_PROPS),
                            c,
                            &mut max_height,
                            &mut i,
                            j,
                        )]
                    }
                    view::WidgetKind::GridRow(view::GridRow { columns }) => columns
                        .into_iter()
                        .map(|spec| match spec.kind {
                            view::WidgetKind::GridChild(c) => attach_child(
                                spec.props.unwrap_or(DEFAULT_PROPS),
                                c,
                                &mut max_height,
                                &mut i,
                                j,
                            ),
                            _ => attach_normal(spec, &mut i, j),
                        })
                        .collect(),
                    _ => vec![attach_normal(spec, &mut i, j)],
                };
                j += max_height;
                i = 0;
                row
            })
            .collect::<Vec<_>>();
        Grid { root, children }
    }

    pub(super) fn update(
        &self,
        ctx: &BSCtx,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &Target,
    ) {
        for row in &self.children {
            for child in row {
                child.update(ctx, waits, event);
            }
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
