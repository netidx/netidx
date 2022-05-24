use super::{set_common_props, BSCtx, BSCtxRef, BSNode, Widget, DEFAULT_PROPS};
use crate::{bscript::LocalEvent, view};
use futures::channel::oneshot;
use gdk::{self, prelude::*};
use gtk::{self, prelude::*, Orientation};
use netidx::{chars::Chars, path::Path};
use netidx_bscript::vm;
use std::{boxed, cell::RefCell, cmp::max, rc::Rc};

fn dir_to_gtk(d: &view::Direction) -> gtk::Orientation {
    match d {
        view::Direction::Horizontal => Orientation::Horizontal,
        view::Direction::Vertical => Orientation::Vertical,
    }
}

pub(super) struct Paned {
    root: gtk::Paned,
    pub(super) first_child: Option<boxed::Box<Widget>>,
    pub(super) second_child: Option<boxed::Box<Widget>>,
}

impl Paned {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Paned,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let scope = scope.append("p");
        let root = gtk::Paned::new(dir_to_gtk(&spec.direction));
        root.set_wide_handle(spec.wide_handle);
        let first_child = spec.first_child.map(|child| {
            let w =
                Widget::new(ctx, (*child).clone(), scope.clone(), selected_path.clone());
            if let Some(w) = w.root() {
                root.pack1(w, true, true);
            }
            boxed::Box::new(w)
        });
        let second_child = spec.second_child.map(|child| {
            let w = Widget::new(ctx, (*child).clone(), scope, selected_path.clone());
            if let Some(w) = w.root() {
                root.pack2(w, true, true);
            }
            boxed::Box::new(w)
        });
        Paned { root, first_child, second_child }
    }

    pub(super) fn update(
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        if let Some(c) = &mut self.first_child {
            c.update(ctx, waits, event);
        }
        if let Some(c) = &mut self.second_child {
            c.update(ctx, waits, event);
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Frame {
    root: gtk::Frame,
    label: BSNode,
    pub(super) child: Option<boxed::Box<Widget>>,
}

impl Frame {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Frame,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let label = BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.label);
        let label_val = label.current().and_then(|v| v.get_as::<Chars>());
        let label_val = label_val.as_ref().map(|s| s.as_ref());
        let root = gtk::Frame::new(label_val);
        root.set_label_align(spec.label_align_horizontal, spec.label_align_vertical);
        let child = spec.child.map(|child| {
            let w =
                Widget::new(ctx, (*child).clone(), scope.clone(), selected_path.clone());
            if let Some(w) = w.root() {
                root.add(w);
            }
            boxed::Box::new(w)
        });
        Frame { root, label, child }
    }

    pub(super) fn update(
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        if let Some(new_lbl) = self.label.update(ctx, event) {
            self.root.set_label(new_lbl.get_as::<Chars>().as_ref().map(|c| c.as_ref()));
        }
        if let Some(c) = &mut self.child {
            c.update(ctx, waits, event);
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Notebook {
    root: gtk::Notebook,
    page: BSNode,
    on_switch_page: Rc<RefCell<BSNode>>,
    pub(super) children: Vec<Widget>,
}

impl Notebook {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Notebook,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let scope = scope.append("n");
        let root = gtk::Notebook::new();
        let page = BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.page);
        let on_switch_page = Rc::new(RefCell::new(BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.on_switch_page,
        )));
        root.set_show_tabs(spec.tabs_visible);
        root.set_tab_pos(match spec.tabs_position {
            view::TabPosition::Left => gtk::PositionType::Left,
            view::TabPosition::Right => gtk::PositionType::Right,
            view::TabPosition::Top => gtk::PositionType::Top,
            view::TabPosition::Bottom => gtk::PositionType::Bottom,
        });
        root.set_enable_popup(spec.tabs_popup);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            match &s.kind {
                view::WidgetKind::NotebookPage(view::NotebookPage {
                    label,
                    reorderable,
                    widget,
                }) => {
                    let w = Widget::new(
                        ctx,
                        (&**widget).clone(),
                        scope.clone(),
                        selected_path.clone(),
                    );
                    if let Some(r) = w.root() {
                        let lbl = gtk::Label::new(Some(label.as_str()));
                        root.append_page(r, Some(&lbl));
                        root.set_tab_reorderable(r, *reorderable);
                        set_common_props(s.props.unwrap_or(DEFAULT_PROPS), r);
                    }
                    children.push(w);
                }
                _ => {
                    let w = Widget::new(ctx, s.clone(), scope.clone(), selected_path.clone());
                    if let Some(r) = w.root() {
                        root.append_page(r, None::<&gtk::Label>);
                        set_common_props(s.props.unwrap_or(DEFAULT_PROPS), r);
                    }
                    children.push(w);
                }
            }
        }
        root.set_current_page(page.current().and_then(|v| v.get_as::<u32>()));
        root.connect_switch_page(clone!(
        @strong ctx, @strong on_switch_page => move |_, _, page| {
            let ev = vm::Event::User(LocalEvent::Event(page.into()));
            on_switch_page.borrow_mut().update(&mut ctx.borrow_mut(), &ev);
        }));
        Notebook { root, page, on_switch_page, children }
    }

    pub(super) fn update(
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        if let Some(page) = self.page.update(ctx, event) {
            if let Some(page) = page.get_as::<u32>() {
                self.root.set_current_page(Some(page));
            }
        }
        self.on_switch_page.borrow_mut().update(ctx, event);
        for c in &mut self.children {
            c.update(ctx, waits, event);
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

pub(super) struct Box {
    root: gtk::Box,
    pub(super) children: Vec<Widget>,
}

impl Box {
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Box,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        fn is_fill(a: view::Align) -> bool {
            match a {
                view::Align::Fill | view::Align::Baseline => true,
                view::Align::Start | view::Align::Center | view::Align::End => false,
            }
        }
        let scope = scope.append("b");
        let root = gtk::Box::new(dir_to_gtk(&spec.direction), 0);
        root.set_homogeneous(spec.homogeneous);
        root.set_spacing(spec.spacing as i32);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            match &s.kind {
                view::WidgetKind::BoxChild(view::BoxChild { pack, padding, widget }) => {
                    let w = Widget::new(
                        ctx,
                        (&**widget).clone(),
                        scope.clone(),
                        selected_path.clone(),
                    );
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
                    let w = Widget::new(ctx, s.clone(), scope.clone(), selected_path.clone());
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
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        for c in &mut self.children {
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
    pub(super) fn new(
        ctx: &BSCtx,
        spec: view::Grid,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Self {
        let scope = scope.append("g");
        let root = gtk::Grid::new();
        let attach_child = |props: view::WidgetProps,
                            spec: view::GridChild,
                            max_height: &mut i32,
                            i: &mut i32,
                            j: i32|
         -> Widget {
            let height = spec.height as i32;
            let width = spec.width as i32;
            let w = Widget::new(
                ctx,
                (&*spec.widget).clone(),
                scope.clone(),
                selected_path.clone(),
            );
            if let Some(r) = w.root() {
                root.attach(r, *i, j, width, height);
                set_common_props(props, r);
            }
            *i += width;
            *max_height = max(*max_height, height);
            w
        };
        let attach_normal = |spec: view::Widget, i: &mut i32, j: i32| -> Widget {
            let w = Widget::new(ctx, spec.clone(), scope.clone(), selected_path.clone());
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
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        for row in &mut self.children {
            for child in row {
                child.update(ctx, waits, event);
            }
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
