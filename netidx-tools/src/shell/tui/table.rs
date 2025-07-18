use super::{
    into_borrowed_line, layout::ConstraintV, FlexV, HighlightSpacingV, LineV, StyleV,
    TRef, TuiW, TuiWidget,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use futures::future::try_join_all;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::Rect,
    widgets::{Cell, Row, Table, TableState},
    Frame,
};
use tokio::try_join;

#[derive(Debug, Clone, Copy)]
struct SelectedV((usize, usize));

impl FromValue for SelectedV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, x), (_, y)] = v.cast_to::<[(ArcStr, usize); 2]>()?;
        Ok(Self((y, x)))
    }
}

struct CellV {
    content: LineV,
    style: Option<StyleV>,
}

impl FromValue for CellV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, content), (_, style)] = v.cast_to::<[(ArcStr, Value); 2]>()?;
        let content = content.cast_to::<LineV>()?;
        let style = style.cast_to::<Option<StyleV>>()?;
        Ok(Self { content, style })
    }
}

struct RowW {
    cells_ref: Ref,
    cells: Vec<CellV>,
    height: TRef<Option<u16>>,
    style: TRef<Option<StyleV>>,
    top_margin: TRef<Option<u16>>,
    bottom_margin: TRef<Option<u16>>,
}

impl RowW {
    async fn compile(bs: &BSHandle, v: Value) -> Result<Self> {
        let [(_, bottom_margin), (_, cells), (_, height), (_, style), (_, top_margin)] =
            v.cast_to::<[(ArcStr, u64); 5]>().context("row fields")?;
        let (bottom_margin, cells_ref, height, style, top_margin) = try_join! {
            bs.compile_ref(bottom_margin),
            bs.compile_ref(cells),
            bs.compile_ref(height),
            bs.compile_ref(style),
            bs.compile_ref(top_margin)
        }?;
        let mut t = Self {
            cells_ref,
            cells: vec![],
            height: TRef::new(height).context("row tref height")?,
            style: TRef::new(style).context("row tref style")?,
            top_margin: TRef::new(top_margin).context("row tref top_margin")?,
            bottom_margin: TRef::new(bottom_margin).context("row tref bottom_margin")?,
        };
        if let Some(v) = t.cells_ref.last.take() {
            t.set_cells(&v)?;
        }
        Ok(t)
    }

    fn set_cells(&mut self, v: &Value) -> Result<()> {
        self.cells = v.clone().cast_to::<Vec<CellV>>().context("cells")?;
        Ok(())
    }

    fn update(&mut self, id: ExprId, v: &Value) -> Result<()> {
        self.height.update(id, v).context("row height update")?;
        self.style.update(id, v).context("row style update")?;
        self.top_margin.update(id, v).context("row top_margin update")?;
        self.bottom_margin.update(id, v).context("row bottom_margin update")?;
        if self.cells_ref.id == id {
            self.set_cells(v)?;
        }
        Ok(())
    }

    fn build<'a>(&'a self) -> Row<'a> {
        let Self { cells_ref: _, cells, height, style, top_margin, bottom_margin } = self;
        let cells: Vec<Cell> = cells
            .iter()
            .map(|c| {
                let mut cell = Cell::from(into_borrowed_line(&c.content.0));
                if let Some(s) = &c.style {
                    cell = cell.style(s.0);
                }
                cell
            })
            .collect();
        let mut row = Row::new(cells);
        if let Some(Some(h)) = height.t {
            row = row.height(h);
        }
        if let Some(Some(s)) = &style.t {
            row = row.style(s.0);
        }
        if let Some(Some(m)) = top_margin.t {
            row = row.top_margin(m);
        }
        if let Some(Some(m)) = bottom_margin.t {
            row = row.bottom_margin(m);
        }
        row
    }
}

pub(super) struct TableW {
    bs: BSHandle,
    cell_highlight_style: TRef<Option<StyleV>>,
    column_highlight_style: TRef<Option<StyleV>>,
    column_spacing: TRef<Option<u16>>,
    flex: TRef<Option<FlexV>>,
    footer: Option<RowW>,
    footer_ref: Ref,
    header: Option<RowW>,
    header_ref: Ref,
    highlight_spacing: TRef<Option<HighlightSpacingV>>,
    highlight_symbol: TRef<Option<ArcStr>>,
    row_highlight_style: TRef<Option<StyleV>>,
    rows: Vec<RowW>,
    rows_ref: Ref,
    selected: TRef<Option<usize>>,
    selected_cell: TRef<Option<SelectedV>>,
    selected_column: TRef<Option<usize>>,
    state: TableState,
    style: TRef<Option<StyleV>>,
    widths: TRef<Option<Vec<ConstraintV>>>,
}

impl TableW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<TuiW> {
        let [(_, cell_highlight_style), (_, column_highlight_style), (_, column_spacing), (_, flex), (_, footer), (_, header), (_, highlight_spacing), (_, highlight_symbol), (_, row_highlight_style), (_, rows), (_, selected), (_, selected_cell), (_, selected_column), (_, style), (_, widths)] =
            v.cast_to::<[(ArcStr, u64); 15]>().context("table fields")?;
        let (
            cell_highlight_style,
            column_highlight_style,
            column_spacing,
            flex,
            footer_ref,
            header_ref,
            highlight_spacing,
            highlight_symbol,
            row_highlight_style,
            rows_ref,
            selected,
            selected_cell,
            selected_column,
            style,
            widths,
        ) = try_join! {
            bs.compile_ref(cell_highlight_style),
            bs.compile_ref(column_highlight_style),
            bs.compile_ref(column_spacing),
            bs.compile_ref(flex),
            bs.compile_ref(footer),
            bs.compile_ref(header),
            bs.compile_ref(highlight_spacing),
            bs.compile_ref(highlight_symbol),
            bs.compile_ref(row_highlight_style),
            bs.compile_ref(rows),
            bs.compile_ref(selected),
            bs.compile_ref(selected_cell),
            bs.compile_ref(selected_column),
            bs.compile_ref(style),
            bs.compile_ref(widths)
        }?;
        let mut t = Self {
            bs: bs.clone(),
            cell_highlight_style: TRef::new(cell_highlight_style)
                .context("table tref cell highlight style")?,
            column_highlight_style: TRef::new(column_highlight_style)
                .context("table tref column highlight style")?,
            column_spacing: TRef::new(column_spacing)
                .context("table tref column_spacing")?,
            flex: TRef::new(flex).context("table tref flex")?,
            footer_ref,
            footer: None,
            header_ref,
            header: None,
            highlight_spacing: TRef::new(highlight_spacing)
                .context("table tref highlight_spacing")?,
            row_highlight_style: TRef::new(row_highlight_style)
                .context("table tref highlight_style")?,
            highlight_symbol: TRef::new(highlight_symbol)
                .context("table tref highlight_symbol")?,
            rows_ref,
            rows: vec![],
            selected_cell: TRef::new(selected_cell)
                .context("table tref selected_cell")?,
            selected_column: TRef::new(selected_column)
                .context("table tref selected column")?,
            selected: TRef::new(selected).context("table tref selected row")?,
            style: TRef::new(style).context("table tref style")?,
            widths: TRef::new(widths).context("table tref widths")?,
            state: TableState::default(),
        };
        if let Some(v) = t.footer_ref.last.take()
            && let Some(v) = v.cast_to::<Option<Value>>()?
        {
            t.footer = Some(RowW::compile(&bs, v).await?);
        }
        if let Some(v) = t.header_ref.last.take()
            && let Some(v) = v.cast_to::<Option<Value>>()?
        {
            t.header = Some(RowW::compile(&bs, v).await?);
        }
        if let Some(v) = t.rows_ref.last.take() {
            t.set_rows(v).await?;
        }
        Ok(Box::new(t))
    }

    async fn set_rows(&mut self, v: Value) -> Result<()> {
        let rows = v.cast_to::<Vec<Value>>().context("rows")?;
        self.rows = try_join_all(rows.into_iter().map(|v| {
            let bs = self.bs.clone();
            async move { RowW::compile(&bs, v).await }
        }))
        .await?;
        Ok(())
    }
}

#[async_trait]
impl TuiWidget for TableW {
    async fn handle_event(&mut self, _e: Event, _v: Value) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs,
            cell_highlight_style,
            column_highlight_style,
            flex,
            column_spacing,
            header_ref,
            header,
            footer_ref,
            footer,
            highlight_spacing,
            row_highlight_style,
            highlight_symbol,
            rows_ref: _,
            rows: _,
            selected,
            selected_column,
            selected_cell,
            style,
            widths,
            state: _,
        } = self;
        cell_highlight_style
            .update(id, &v)
            .context("table update cell highlight style")?;
        column_highlight_style
            .update(id, &v)
            .context("table update column highlight style")?;
        flex.update(id, &v).context("table update flex")?;
        column_spacing.update(id, &v).context("table update column_spacing")?;
        highlight_spacing.update(id, &v).context("table update highlight_spacing")?;
        row_highlight_style.update(id, &v).context("table update highlight_style")?;
        highlight_symbol.update(id, &v).context("table update highlight_symbol")?;
        selected.update(id, &v).context("table update selected")?;
        selected_column.update(id, &v).context("table update selected_column")?;
        selected_cell.update(id, &v).context("table update selected_cell")?;
        style.update(id, &v).context("table update style")?;
        widths.update(id, &v).context("table update widths")?;
        if footer_ref.id == id {
            match v.clone().cast_to::<Option<Value>>()? {
                None => *footer = None,
                Some(v) => *footer = Some(RowW::compile(bs, v).await?),
            }
        }
        if let Some(r) = footer {
            r.update(id, &v)?;
        }
        if header_ref.id == id {
            match v.clone().cast_to::<Option<Value>>()? {
                None => *header = None,
                Some(v) => *header = Some(RowW::compile(bs, v).await?),
            }
        }
        if let Some(r) = header {
            r.update(id, &v)?;
        }
        if self.rows_ref.id == id {
            self.set_rows(v.clone()).await?;
        }
        for r in self.rows.iter_mut() {
            r.update(id, &v)?;
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            cell_highlight_style,
            column_highlight_style,
            flex,
            column_spacing,
            header_ref: _,
            header,
            footer_ref: _,
            footer,
            highlight_spacing,
            row_highlight_style,
            highlight_symbol,
            rows_ref: _,
            rows,
            selected,
            selected_column,
            selected_cell,
            style,
            widths,
            state,
        } = self;
        let mut table = Table::default().rows(rows.iter().map(|r| r.build()));
        if let Some(Some(s)) = cell_highlight_style.t {
            table = table.cell_highlight_style(s.0);
        }
        if let Some(Some(s)) = column_highlight_style.t {
            table = table.column_highlight_style(s.0);
        }
        if let Some(Some(f)) = flex.t {
            table = table.flex(f.0);
        }
        if let Some(Some(widths)) = &widths.t {
            table = table.widths(widths.iter().map(|c| c.0));
        }
        if let Some(Some(s)) = column_spacing.t {
            table = table.column_spacing(s);
        }
        if let Some(h) = header {
            table = table.header(h.build());
        }
        if let Some(h) = footer {
            table = table.footer(h.build());
        }
        if let Some(Some(hs)) = &highlight_spacing.t {
            table = table.highlight_spacing(hs.0.clone());
        }
        if let Some(Some(s)) = &row_highlight_style.t {
            table = table.row_highlight_style(s.0);
        }
        if let Some(Some(sym)) = &highlight_symbol.t {
            table = table.highlight_symbol(sym.as_str());
        }
        if let Some(Some(s)) = &style.t {
            table = table.style(s.0);
        }
        if let Some(Some(s)) = selected_cell.t {
            *state = state.clone().with_selected_cell(s.0);
        }
        if let Some(Some(s)) = selected_column.t {
            *state = state.clone().with_selected_column(s)
        }
        if let Some(Some(s)) = selected.t {
            *state = state.clone().with_selected(s)
        }
        frame.render_stateful_widget(table, rect, state);
        Ok(())
    }
}
