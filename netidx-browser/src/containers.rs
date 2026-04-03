use crate::{view, BCtx, BWidget, GxCallback, GxProp, Widget, WidgetPath};
use glib::clone;
use graphix_compiler::expr::ExprId;
use gtk::prelude::*;
use netidx::path::Path;
use netidx::protocol::valarray::ValArray;
use netidx::publisher::Value;
use netidx_netproto::resolver;
use std::{cell::RefCell, rc::Rc};

// ---- Frame ----

pub(crate) struct Frame {
    frame: gtk::Frame,
    label_prop: Option<GxProp>,
    child: Option<Widget>,
}

impl Frame {
    pub(crate) fn new(
        ctx: &BCtx,
        spec: view::Frame,
        selected_path: gtk::Label,
    ) -> Self {
        let frame = gtk::Frame::new(None);
        frame.set_label_align(spec.label_align_horizontal, spec.label_align_vertical);
        let child = spec.child.map(|c| {
            let w = Widget::new(ctx, (*c).clone(), selected_path);
            if let Some(r) = w.root() {
                frame.add(r);
            }
            w
        });
        Frame {
            frame,
            label_prop: GxProp::compile(ctx, &spec.label),
            child,
        }
    }
}

impl BWidget for Frame {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.label_prop {
            if p.update(id, value) {
                if let Value::String(s) = value {
                    self.frame.set_label(Some(&*s));
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = self.child {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.frame.upcast_ref())
    }

    fn table_resolved(&mut self, path: &Path, table: &resolver::Table) -> bool {
        if let Some(ref mut c) = self.child {
            c.table_resolved(path, table)
        } else {
            false
        }
    }
}

// ---- Box ----

pub(crate) struct Box {
    container: gtk::Box,
    children: Vec<Widget>,
}

impl Box {
    pub(crate) fn new(
        ctx: &BCtx,
        spec: view::Box,
        selected_path: gtk::Label,
    ) -> Self {
        let orientation = match spec.direction {
            view::Direction::Horizontal => gtk::Orientation::Horizontal,
            view::Direction::Vertical => gtk::Orientation::Vertical,
        };
        let container = gtk::Box::new(orientation, spec.spacing as i32);
        container.set_homogeneous(spec.homogeneous);
        let children: Vec<Widget> = spec
            .children
            .into_iter()
            .map(|child_spec| {
                let (pack, padding) = match &child_spec.kind {
                    view::WidgetKind::BoxChild(bc) => (bc.pack, bc.padding),
                    _ => (view::Pack::Start, 0),
                };
                let w = Widget::new(ctx, child_spec, selected_path.clone());
                if let Some(r) = w.root() {
                    match pack {
                        view::Pack::Start => {
                            container.pack_start(r, true, true, padding as u32)
                        }
                        view::Pack::End => {
                            container.pack_end(r, true, true, padding as u32)
                        }
                    }
                }
                w
            })
            .collect();
        Box { container, children }
    }
}

impl BWidget for Box {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        for child in &mut self.children {
            if child.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.container.upcast_ref())
    }

    fn table_resolved(&mut self, path: &Path, table: &resolver::Table) -> bool {
        let mut handled = false;
        for child in &mut self.children {
            if child.table_resolved(path, table) {
                handled = true;
            }
        }
        handled
    }

    fn set_highlight(&self, mut path: std::slice::Iter<WidgetPath>, h: bool) {
        match path.next() {
            Some(WidgetPath::Box(i)) => {
                if let Some(c) = self.children.get(*i) {
                    c.set_highlight(path, h);
                }
            }
            Some(WidgetPath::Leaf) => {
                if let Some(r) = self.root() {
                    crate::util::set_highlight(r, h);
                }
            }
            _ => {}
        }
    }
}

// ---- Grid ----

pub(crate) struct Grid {
    grid: gtk::Grid,
    children: Vec<Vec<Widget>>,
}

impl Grid {
    pub(crate) fn new(
        ctx: &BCtx,
        spec: view::Grid,
        selected_path: gtk::Label,
    ) -> Self {
        let grid = gtk::Grid::new();
        grid.set_column_homogeneous(spec.homogeneous_columns);
        grid.set_row_homogeneous(spec.homogeneous_rows);
        grid.set_column_spacing(spec.column_spacing);
        grid.set_row_spacing(spec.row_spacing);
        let mut children = Vec::new();
        for (row_idx, row_spec) in spec.rows.into_iter().enumerate() {
            let cols = match row_spec.kind {
                view::WidgetKind::GridRow(gr) => gr.columns,
                _ => vec![row_spec],
            };
            let mut row = Vec::new();
            for (col_idx, col_spec) in cols.into_iter().enumerate() {
                let (width, height) = match &col_spec.kind {
                    view::WidgetKind::GridChild(gc) => (gc.width.max(1), gc.height.max(1)),
                    _ => (1, 1),
                };
                let w = Widget::new(ctx, col_spec, selected_path.clone());
                if let Some(r) = w.root() {
                    grid.attach(r, col_idx as i32, row_idx as i32, width as i32, height as i32);
                }
                row.push(w);
            }
            children.push(row);
        }
        Grid { grid, children }
    }
}

impl BWidget for Grid {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        for row in &mut self.children {
            for child in row {
                if child.update(id, value) {
                    handled = true;
                }
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.grid.upcast_ref())
    }

    fn table_resolved(&mut self, path: &Path, table: &resolver::Table) -> bool {
        let mut handled = false;
        for row in &mut self.children {
            for child in row {
                if child.table_resolved(path, table) {
                    handled = true;
                }
            }
        }
        handled
    }

    fn set_highlight(&self, mut path: std::slice::Iter<WidgetPath>, h: bool) {
        match path.next() {
            Some(WidgetPath::GridItem(r, c)) => {
                if let Some(row) = self.children.get(*r) {
                    if let Some(child) = row.get(*c) {
                        child.set_highlight(path, h);
                    }
                }
            }
            Some(WidgetPath::GridRow(r)) => {
                if let Some(row) = self.children.get(*r) {
                    for child in row {
                        child.set_highlight(
                            [WidgetPath::Leaf].iter(),
                            h,
                        );
                    }
                }
            }
            Some(WidgetPath::Leaf) => {
                if let Some(r) = self.root() {
                    crate::util::set_highlight(r, h);
                }
            }
            _ => {}
        }
    }
}

// ---- Paned ----

pub(crate) struct Paned {
    paned: gtk::Paned,
    first: Option<Widget>,
    second: Option<Widget>,
}

impl Paned {
    pub(crate) fn new(
        ctx: &BCtx,
        spec: view::Paned,
        selected_path: gtk::Label,
    ) -> Self {
        let orientation = match spec.direction {
            view::Direction::Horizontal => gtk::Orientation::Horizontal,
            view::Direction::Vertical => gtk::Orientation::Vertical,
        };
        let paned = gtk::Paned::new(orientation);
        paned.set_wide_handle(spec.wide_handle);
        let first = spec.first_child.map(|c| {
            let w = Widget::new(ctx, (*c).clone(), selected_path.clone());
            if let Some(r) = w.root() {
                paned.pack1(r, true, true);
            }
            w
        });
        let second = spec.second_child.map(|c| {
            let w = Widget::new(ctx, (*c).clone(), selected_path);
            if let Some(r) = w.root() {
                paned.pack2(r, true, true);
            }
            w
        });
        Paned { paned, first, second }
    }
}

impl BWidget for Paned {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut c) = self.first {
            if c.update(id, value) {
                handled = true;
            }
        }
        if let Some(ref mut c) = self.second {
            if c.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.paned.upcast_ref())
    }

    fn table_resolved(&mut self, path: &Path, table: &resolver::Table) -> bool {
        let mut handled = false;
        if let Some(ref mut c) = self.first {
            if c.table_resolved(path, table) {
                handled = true;
            }
        }
        if let Some(ref mut c) = self.second {
            if c.table_resolved(path, table) {
                handled = true;
            }
        }
        handled
    }
}

// ---- Notebook ----

pub(crate) struct Notebook {
    notebook: gtk::Notebook,
    children: Vec<Widget>,
    page: Option<GxProp>,
    on_switch_page: Rc<RefCell<Option<GxCallback>>>,
}

impl Notebook {
    pub(crate) fn new(
        ctx: &BCtx,
        spec: view::Notebook,
        selected_path: gtk::Label,
    ) -> Self {
        let notebook = gtk::Notebook::new();
        notebook.set_tab_pos(match spec.tabs_position {
            view::TabPosition::Left => gtk::PositionType::Left,
            view::TabPosition::Right => gtk::PositionType::Right,
            view::TabPosition::Top => gtk::PositionType::Top,
            view::TabPosition::Bottom => gtk::PositionType::Bottom,
        });
        notebook.set_show_tabs(spec.tabs_visible);
        notebook.set_scrollable(spec.tabs_scrollable);
        let on_switch_page = Rc::new(RefCell::new(
            GxCallback::compile(ctx, &spec.on_switch_page),
        ));
        notebook.connect_switch_page(clone!(@strong on_switch_page => move |_, _, page| {
            if let Some(ref cb) = *on_switch_page.borrow() {
                cb.fire(ValArray::from([Value::I64(page as i64)]));
            }
        }));
        let children: Vec<Widget> = spec
            .children
            .into_iter()
            .map(|child_spec| {
                let (label_text, reorderable) = match &child_spec.kind {
                    view::WidgetKind::NotebookPage(np) => {
                        (np.label.clone(), np.reorderable)
                    }
                    _ => (String::new(), false),
                };
                let w = Widget::new(ctx, child_spec, selected_path.clone());
                if let Some(r) = w.root() {
                    let tab_label = gtk::Label::new(Some(&label_text));
                    notebook.append_page(r, Some(&tab_label));
                    notebook.set_tab_reorderable(r, reorderable);
                }
                w
            })
            .collect();
        Notebook {
            notebook,
            children,
            page: GxProp::compile(ctx, &spec.page),
            on_switch_page,
        }
    }
}

impl BWidget for Notebook {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.page {
            if p.update(id, value) {
                if let Ok(n) = value.clone().cast_to::<i32>() {
                    self.notebook.set_current_page(Some(n as u32));
                }
                handled = true;
            }
        }
        if let Some(ref mut c) = *self.on_switch_page.borrow_mut() {
            if c.update(id, value) {
                handled = true;
            }
        }
        for child in &mut self.children {
            if child.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.notebook.upcast_ref())
    }

    fn table_resolved(&mut self, path: &Path, table: &resolver::Table) -> bool {
        let mut handled = false;
        for child in &mut self.children {
            if child.table_resolved(path, table) {
                handled = true;
            }
        }
        handled
    }
}
