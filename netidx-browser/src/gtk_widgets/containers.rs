use super::{compile, CompileCtx, EmptyW, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

// ---- BoxChild ----

pub(crate) struct BoxChildW<X: GXExt> {
    ctx: CompileCtx<X>,
    child_ref: Ref<X>,
    child: GtkW<X>,
    pack: TRef<X, String>,
    padding: TRef<X, i64>,
}

impl<X: GXExt> BoxChildW<X> {
    pub(crate) async fn compile(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields alphabetical: child, pack, padding
        let [(_, child), (_, pack), (_, padding)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("box_child flds")?;
        let (child_ref, pack, padding) = tokio::try_join! {
            ctx.gx.compile_ref(child),
            ctx.gx.compile_ref(pack),
            ctx.gx.compile_ref(padding),
        }?;
        let pack: TRef<X, String> =
            TRef::new(pack).context("box_child tref pack")?;
        let padding: TRef<X, i64> =
            TRef::new(padding).context("box_child tref padding")?;
        let compiled_child = compile_child!(ctx, child_ref, "box_child child");
        compiled_child.gtk_widget().show();
        Ok(Box::new(BoxChildW {
            ctx,
            child_ref,
            child: compiled_child,
            pack,
            padding,
        }))
    }
}

impl<X: GXExt> GtkWidget<X> for BoxChildW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        update_child!(self, rt, id, v, changed, child_ref, child, "box_child child");
        if self.pack.update(id, v).context("box_child update pack")?.is_some() {
            changed = true;
        }
        if self.padding.update(id, v).context("box_child update padding")?.is_some() {
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.child.gtk_widget()
    }

    fn box_child_info(&self) -> Option<(bool, u32)> {
        let pack_end = self.pack.t.as_deref() == Some("End");
        let padding = self.padding.t.unwrap_or(0).max(0) as u32;
        Some((pack_end, padding))
    }
}

// ---- Grid ----

pub(crate) struct GridW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Grid,
    column_spacing: TRef<X, i64>,
    homogeneous_columns: TRef<X, bool>,
    homogeneous_rows: TRef<X, bool>,
    row_spacing: TRef<X, i64>,
    rows_ref: Ref<X>,
    rows: Vec<Vec<GtkW<X>>>,
}

impl<X: GXExt> GridW<X> {
    pub(crate) async fn compile(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields alphabetical: column_spacing, homogeneous_columns,
        //   homogeneous_rows, row_spacing, rows
        let [
            (_, column_spacing),
            (_, homogeneous_columns),
            (_, homogeneous_rows),
            (_, row_spacing),
            (_, rows),
        ] = source
            .cast_to::<[(ArcStr, u64); 5]>()
            .context("grid flds")?;
        let (column_spacing, homogeneous_columns, homogeneous_rows, row_spacing, rows_ref) =
            tokio::try_join! {
                ctx.gx.compile_ref(column_spacing),
                ctx.gx.compile_ref(homogeneous_columns),
                ctx.gx.compile_ref(homogeneous_rows),
                ctx.gx.compile_ref(row_spacing),
                ctx.gx.compile_ref(rows),
            }?;
        let column_spacing: TRef<X, i64> =
            TRef::new(column_spacing).context("grid tref column_spacing")?;
        let homogeneous_columns: TRef<X, bool> =
            TRef::new(homogeneous_columns).context("grid tref homogeneous_columns")?;
        let homogeneous_rows: TRef<X, bool> =
            TRef::new(homogeneous_rows).context("grid tref homogeneous_rows")?;
        let row_spacing: TRef<X, i64> =
            TRef::new(row_spacing).context("grid tref row_spacing")?;
        let compiled_rows = match rows_ref.last.as_ref() {
            None => vec![],
            Some(v) => compile_grid_rows(ctx.clone(), v.clone())
                .await
                .context("grid rows")?,
        };
        let widget = gtk::Grid::new();
        widget.set_column_spacing(column_spacing.t.unwrap_or(0) as u32);
        widget.set_row_spacing(row_spacing.t.unwrap_or(0) as u32);
        widget.set_column_homogeneous(homogeneous_columns.t.unwrap_or(false));
        widget.set_row_homogeneous(homogeneous_rows.t.unwrap_or(false));
        attach_grid_children(&widget, &compiled_rows);
        widget.show_all();
        Ok(Box::new(GridW {
            ctx,
            widget,
            column_spacing,
            homogeneous_columns,
            homogeneous_rows,
            row_spacing,
            rows_ref,
            rows: compiled_rows,
        }))
    }

    fn rebuild(&self) {
        for child in self.widget.children() {
            self.widget.remove(&child);
        }
        attach_grid_children(&self.widget, &self.rows);
        self.widget.show_all();
    }
}

fn attach_grid_children<X: GXExt>(
    grid: &gtk::Grid,
    rows: &[Vec<GtkW<X>>],
) {
    for (row_idx, row) in rows.iter().enumerate() {
        let mut col_idx: i32 = 0;
        for child in row.iter() {
            let (colspan, rowspan) = child.grid_cell_info()
                .unwrap_or((1, 1));
            grid.attach(
                child.gtk_widget(),
                col_idx,
                row_idx as i32,
                colspan,
                rowspan,
            );
            col_idx += colspan;
        }
    }
}

async fn compile_grid_rows<X: GXExt>(
    ctx: CompileCtx<X>,
    v: Value,
) -> Result<Vec<Vec<GtkW<X>>>> {
    let row_values = v.cast_to::<Vec<Value>>()?;
    let mut rows = Vec::with_capacity(row_values.len());
    for row_val in row_values {
        let items = row_val.cast_to::<Vec<Value>>()?;
        let mut row = Vec::with_capacity(items.len());
        for item in items {
            row.push(compile(ctx.clone(), item).await?);
        }
        rows.push(row);
    }
    Ok(rows)
}

impl<X: GXExt> GtkWidget<X> for GridW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(cs) = self
            .column_spacing
            .update(id, v)
            .context("grid update column_spacing")?
        {
            self.widget.set_column_spacing(*cs as u32);
            changed = true;
        }
        if let Some(hc) = self
            .homogeneous_columns
            .update(id, v)
            .context("grid update homogeneous_columns")?
        {
            self.widget.set_column_homogeneous(*hc);
            changed = true;
        }
        if let Some(hr) = self
            .homogeneous_rows
            .update(id, v)
            .context("grid update homogeneous_rows")?
        {
            self.widget.set_row_homogeneous(*hr);
            changed = true;
        }
        if let Some(rs) = self
            .row_spacing
            .update(id, v)
            .context("grid update row_spacing")?
        {
            self.widget.set_row_spacing(*rs as u32);
            changed = true;
        }
        if id == self.rows_ref.id {
            self.rows_ref.last = Some(v.clone());
            self.rows = rt
                .block_on(compile_grid_rows(self.ctx.clone(), v.clone()))
                .context("grid rows recompile")?;
            self.rebuild();
            changed = true;
        }
        for row in &mut self.rows {
            for child in row.iter_mut() {
                changed |= child.handle_update(rt, id, v)?;
            }
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}

// ---- GridCell ----

pub(crate) struct GridCellW<X: GXExt> {
    ctx: CompileCtx<X>,
    child_ref: Ref<X>,
    child: GtkW<X>,
    colspan: TRef<X, i64>,
    rowspan: TRef<X, i64>,
}

impl<X: GXExt> GridCellW<X> {
    pub(crate) async fn compile(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields alphabetical: child, colspan, rowspan
        let [(_, child), (_, colspan), (_, rowspan)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("grid_cell flds")?;
        let (child_ref, colspan, rowspan) = tokio::try_join! {
            ctx.gx.compile_ref(child),
            ctx.gx.compile_ref(colspan),
            ctx.gx.compile_ref(rowspan),
        }?;
        let colspan: TRef<X, i64> =
            TRef::new(colspan).context("grid_cell tref colspan")?;
        let rowspan: TRef<X, i64> =
            TRef::new(rowspan).context("grid_cell tref rowspan")?;
        let compiled_child = compile_child!(ctx, child_ref, "grid_cell child");
        compiled_child.gtk_widget().show();
        Ok(Box::new(GridCellW {
            ctx,
            child_ref,
            child: compiled_child,
            colspan,
            rowspan,
        }))
    }
}

impl<X: GXExt> GtkWidget<X> for GridCellW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        update_child!(self, rt, id, v, changed, child_ref, child, "grid_cell child");
        if self.colspan.update(id, v).context("grid_cell update colspan")?.is_some() {
            changed = true;
        }
        if self.rowspan.update(id, v).context("grid_cell update rowspan")?.is_some() {
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.child.gtk_widget()
    }

    fn grid_cell_info(&self) -> Option<(i32, i32)> {
        let colspan = self.colspan.t.unwrap_or(1).max(1) as i32;
        let rowspan = self.rowspan.t.unwrap_or(1).max(1) as i32;
        Some((colspan, rowspan))
    }
}

// ---- Frame ----

pub(crate) struct FrameW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Frame,
    label: TRef<X, String>,
    child_ref: Ref<X>,
    child: GtkW<X>,
}

impl<X: GXExt> FrameW<X> {
    pub(crate) async fn compile(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields alphabetical: child, label
        let [(_, child), (_, label)] =
            source.cast_to::<[(ArcStr, u64); 2]>().context("frame flds")?;
        let (child_ref, label) = tokio::try_join! {
            ctx.gx.compile_ref(child),
            ctx.gx.compile_ref(label),
        }?;
        let compiled_child = compile_child!(ctx, child_ref, "frame child");
        let label: TRef<X, String> =
            TRef::new(label).context("frame tref label")?;
        let widget = gtk::Frame::new(label.t.as_deref());
        widget.add(compiled_child.gtk_widget());
        widget.show_all();
        Ok(Box::new(FrameW {
            ctx,
            widget,
            label,
            child_ref,
            child: compiled_child,
        }))
    }
}

impl<X: GXExt> GtkWidget<X> for FrameW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(l) = self
            .label
            .update(id, v)
            .context("frame update label")?
        {
            self.widget.set_label(Some(l.as_str()));
            changed = true;
        }
        if id == self.child_ref.id {
            self.child_ref.last = Some(v.clone());
            if let Some(old) = self.widget.child() {
                self.widget.remove(&old);
            }
            self.child = rt
                .block_on(compile(self.ctx.clone(), v.clone()))
                .context("frame child recompile")?;
            self.widget.add(self.child.gtk_widget());
            self.widget.show_all();
            changed = true;
        }
        changed |= self.child.handle_update(rt, id, v)?;
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}

// ---- Paned ----

pub(crate) struct PanedW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Paned,
    direction: TRef<X, String>,
    first_ref: Ref<X>,
    first: GtkW<X>,
    second_ref: Ref<X>,
    second: GtkW<X>,
    wide_handle: TRef<X, bool>,
}

impl<X: GXExt> PanedW<X> {
    pub(crate) async fn compile(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields alphabetical: direction, first, second, wide_handle
        let [(_, direction), (_, first), (_, second), (_, wide_handle)] =
            source.cast_to::<[(ArcStr, u64); 4]>().context("paned flds")?;
        let (direction, first_ref, second_ref, wide_handle) = tokio::try_join! {
            ctx.gx.compile_ref(direction),
            ctx.gx.compile_ref(first),
            ctx.gx.compile_ref(second),
            ctx.gx.compile_ref(wide_handle),
        }?;
        let direction: TRef<X, String> =
            TRef::new(direction).context("paned tref direction")?;
        let wide_handle: TRef<X, bool> =
            TRef::new(wide_handle).context("paned tref wide_handle")?;
        let first_child = compile_child!(ctx, first_ref, "paned first");
        let second_child = compile_child!(ctx, second_ref, "paned second");
        let orientation = parse_orientation(
            direction.t.as_deref().unwrap_or("Horizontal"),
        );
        let widget = gtk::Paned::new(orientation);
        widget.set_wide_handle(wide_handle.t.unwrap_or(false));
        widget.pack1(first_child.gtk_widget(), true, false);
        widget.pack2(second_child.gtk_widget(), true, false);
        widget.show_all();
        Ok(Box::new(PanedW {
            ctx,
            widget,
            direction,
            first_ref,
            first: first_child,
            second_ref,
            second: second_child,
            wide_handle,
        }))
    }
}

fn parse_orientation(s: &str) -> gtk::Orientation {
    match s {
        "Vertical" => gtk::Orientation::Vertical,
        _ => gtk::Orientation::Horizontal,
    }
}

impl<X: GXExt> GtkWidget<X> for PanedW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(d) = self
            .direction
            .update(id, v)
            .context("paned update direction")?
        {
            self.widget.set_orientation(parse_orientation(d));
            changed = true;
        }
        if let Some(wh) = self
            .wide_handle
            .update(id, v)
            .context("paned update wide_handle")?
        {
            self.widget.set_wide_handle(*wh);
            changed = true;
        }
        // Update first child
        if id == self.first_ref.id {
            self.first_ref.last = Some(v.clone());
            if let Some(c) = self.widget.child1() {
                self.widget.remove(&c);
            }
            self.first = rt
                .block_on(compile(self.ctx.clone(), v.clone()))
                .context("paned first recompile")?;
            self.widget.pack1(self.first.gtk_widget(), true, false);
            self.widget.show_all();
            changed = true;
        }
        changed |= self.first.handle_update(rt, id, v)?;
        // Update second child
        if id == self.second_ref.id {
            self.second_ref.last = Some(v.clone());
            if let Some(c) = self.widget.child2() {
                self.widget.remove(&c);
            }
            self.second = rt
                .block_on(compile(self.ctx.clone(), v.clone()))
                .context("paned second recompile")?;
            self.widget.pack2(self.second.gtk_widget(), true, false);
            self.widget.show_all();
            changed = true;
        }
        changed |= self.second.handle_update(rt, id, v)?;
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}

// ---- Notebook ----

struct NotebookChild<X: GXExt> {
    label_text: String,
    reorderable: bool,
    // Held to keep the Ref alive (its Drop unregisters from the runtime).
    #[allow(dead_code)]
    child_ref: Ref<X>,
    child: GtkW<X>,
}

pub(crate) struct NotebookW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Notebook,
    children_ref: Ref<X>,
    children: Vec<NotebookChild<X>>,
    page: TRef<X, i64>,
    tabs_popup: TRef<X, bool>,
    on_switch_page: Ref<X>,
    on_switch_page_callable: Option<Callable<X>>,
    switch_signal: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> NotebookW<X> {
    pub(crate) async fn compile(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields alphabetical: children, on_switch_page, page, tabs_popup
        let [(_, children), (_, on_switch_page), (_, page), (_, tabs_popup)] =
            source.cast_to::<[(ArcStr, u64); 4]>().context("notebook flds")?;
        let (children_ref, on_switch_page, page, tabs_popup) = tokio::try_join! {
            ctx.gx.compile_ref(children),
            ctx.gx.compile_ref(on_switch_page),
            ctx.gx.compile_ref(page),
            ctx.gx.compile_ref(tabs_popup),
        }?;
        let page: TRef<X, i64> =
            TRef::new(page).context("notebook tref page")?;
        let tabs_popup: TRef<X, bool> =
            TRef::new(tabs_popup).context("notebook tref tabs_popup")?;
        let on_switch_page_callable =
            compile_callable!(ctx.gx, on_switch_page, "notebook on_switch_page");
        let compiled_children = match children_ref.last.as_ref() {
            None => vec![],
            Some(v) => compile_notebook_pages(ctx.clone(), v.clone())
                .await
                .context("notebook children")?,
        };
        let widget = gtk::Notebook::new();
        add_notebook_pages(&widget, &compiled_children);
        if let Some(p) = page.t {
            widget.set_current_page(Some(p as u32));
        }
        if tabs_popup.t.unwrap_or(true) {
            widget.popup_enable();
        } else {
            widget.popup_disable();
        }
        let switch_signal =
            connect_switch_page(&widget, &ctx.gx, &on_switch_page_callable);
        widget.show_all();
        Ok(Box::new(NotebookW {
            ctx,
            widget,
            children_ref,
            children: compiled_children,
            page,
            tabs_popup,
            on_switch_page,
            on_switch_page_callable,
            switch_signal,
        }))
    }

    fn rebuild_pages(&mut self) {
        if let Some(sig) = self.switch_signal.take() {
            self.widget.disconnect(sig);
        }
        while self.widget.n_pages() > 0 {
            self.widget.remove_page(Some(0));
        }
        add_notebook_pages(&self.widget, &self.children);
        self.switch_signal = connect_switch_page(
            &self.widget,
            &self.ctx.gx,
            &self.on_switch_page_callable,
        );
        self.widget.show_all();
    }
}

fn add_notebook_pages<X: GXExt>(
    notebook: &gtk::Notebook,
    children: &[NotebookChild<X>],
) {
    for child in children {
        let tab_label = gtk::Label::new(Some(&child.label_text));
        notebook.append_page(child.child.gtk_widget(), Some(&tab_label));
        notebook.set_tab_reorderable(child.child.gtk_widget(), child.reorderable);
    }
}

async fn compile_notebook_pages<X: GXExt>(
    ctx: CompileCtx<X>,
    v: Value,
) -> Result<Vec<NotebookChild<X>>> {
    let items = v.cast_to::<Vec<Value>>()?;
    let mut children = Vec::with_capacity(items.len());
    for item in items {
        // NotebookPage struct fields alphabetical: child, label, reorderable
        let [(_, child_val), (_, label_val), (_, reorderable_val)] =
            item.cast_to::<[(ArcStr, Value); 3]>().context("notebook page flds")?;
        let child_id = child_val.cast_to::<u64>().context("notebook page child id")?;
        let label_text =
            label_val.cast_to::<String>().context("notebook page label")?;
        let reorderable =
            reorderable_val.cast_to::<bool>().context("notebook page reorderable")?;
        let child_ref = ctx.gx.compile_ref(child_id).await?;
        let compiled_child = compile_child!(ctx, child_ref, "notebook page child");
        children.push(NotebookChild {
            label_text,
            reorderable,
            child_ref,
            child: compiled_child,
        });
    }
    Ok(children)
}

fn connect_switch_page<X: GXExt>(
    widget: &gtk::Notebook,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_switch_page(move |_, _, page_num| {
        let args = ValArray::from_iter([Value::I64(page_num as i64)]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("notebook on_switch_page call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for NotebookW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(p) = self
            .page
            .update(id, v)
            .context("notebook update page")?
        {
            // Block switch signal while setting page programmatically
            if let Some(ref sig) = self.switch_signal {
                self.widget.block_signal(sig);
            }
            self.widget.set_current_page(Some(*p as u32));
            if let Some(ref sig) = self.switch_signal {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if let Some(tp) = self
            .tabs_popup
            .update(id, v)
            .context("notebook update tabs_popup")?
        {
            if *tp {
                self.widget.popup_enable();
            } else {
                self.widget.popup_disable();
            }
            changed = true;
        }
        if id == self.children_ref.id {
            self.children_ref.last = Some(v.clone());
            self.children = rt
                .block_on(compile_notebook_pages(self.ctx.clone(), v.clone()))
                .context("notebook children recompile")?;
            self.rebuild_pages();
            changed = true;
        }
        if id == self.on_switch_page.id {
            self.on_switch_page.last = Some(v.clone());
            self.on_switch_page_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("notebook on_switch_page recompile")?,
            );
            if let Some(sig) = self.switch_signal.take() {
                self.widget.disconnect(sig);
            }
            self.switch_signal = connect_switch_page(
                &self.widget,
                &self.ctx.gx,
                &self.on_switch_page_callable,
            );
            changed = true;
        }
        for child in &mut self.children {
            changed |= child.child.handle_update(rt, id, v)?;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
