use anyhow::Result;
use futures::{future::join_all, prelude::Future};
use netidx_bscript::expr;
use netidx::{
    resolver::{self, ResolverRead},
    subscriber::Value,
};
pub(super) use netidx_protocols::view::{self, *};
use std::{boxed, collections::HashMap, pin::Pin};

#[derive(Debug, Clone)]
pub(super) struct BoxChild {
    pub pack: view::Pack,
    pub padding: u64,
    pub widget: boxed::Box<Widget>,
}

impl BoxChild {
    async fn new(resolver: &ResolverRead, c: view::BoxChild) -> Result<Self> {
        Ok(BoxChild {
            pack: c.pack,
            padding: c.padding,
            widget: boxed::Box::new(Widget::new(resolver, (&*c.widget).clone()).await?),
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct GridChild {
    pub width: u32,
    pub height: u32,
    pub widget: boxed::Box<Widget>,
}

impl GridChild {
    async fn new(resolver: &ResolverRead, c: view::GridChild) -> Result<Self> {
        Ok(GridChild {
            width: c.width,
            height: c.height,
            widget: boxed::Box::new(Widget::new(resolver, (&*c.widget).clone()).await?),
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct Box {
    pub direction: view::Direction,
    pub homogeneous: bool,
    pub spacing: u32,
    pub children: Vec<Widget>,
}

#[derive(Debug, Clone)]
pub(super) struct GridRow {
    pub columns: Vec<Widget>,
}

#[derive(Debug, Clone)]
pub(super) struct Grid {
    pub homogeneous_columns: bool,
    pub homogeneous_rows: bool,
    pub column_spacing: u32,
    pub row_spacing: u32,
    pub rows: Vec<Widget>,
}

#[derive(Debug, Clone)]
pub(super) enum WidgetKind {
    Action(expr::Expr),
    Table(view::Table, resolver::Table),
    Label(expr::Expr),
    Button(view::Button),
    Toggle(view::Toggle),
    Selector(view::Selector),
    Entry(view::Entry),
    Box(Box),
    BoxChild(BoxChild),
    Grid(Grid),
    GridChild(GridChild),
    GridRow(GridRow),
    LinePlot(view::LinePlot),
}

#[derive(Debug, Clone)]
pub(super) struct Widget {
    pub(super) props: Option<view::WidgetProps>,
    pub(super) kind: WidgetKind,
}

impl Widget {
    fn new<'a>(
        resolver: &'a ResolverRead,
        widget: view::Widget,
    ) -> Pin<boxed::Box<dyn Future<Output = Result<Self>> + 'a + Send>> {
        boxed::Box::pin(async move {
            let kind = match widget.kind {
                view::WidgetKind::Action(a) => WidgetKind::Action(a),
                view::WidgetKind::Table(spec) => {
                    let desc = resolver.table(spec.path.clone()).await?;
                    WidgetKind::Table(spec, desc)
                }
                view::WidgetKind::Label(s) => WidgetKind::Label(s),
                view::WidgetKind::Button(b) => WidgetKind::Button(b),
                view::WidgetKind::Toggle(t) => WidgetKind::Toggle(t),
                view::WidgetKind::Selector(c) => WidgetKind::Selector(c),
                view::WidgetKind::Entry(e) => WidgetKind::Entry(e),
                view::WidgetKind::LinePlot(p) => WidgetKind::LinePlot(p),
                view::WidgetKind::Box(c) => {
                    let children = join_all(
                        c.children.into_iter().map(|c| Widget::new(resolver, c)),
                    )
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                    WidgetKind::Box(Box {
                        direction: c.direction,
                        homogeneous: c.homogeneous,
                        spacing: c.spacing,
                        children,
                    })
                }
                view::WidgetKind::BoxChild(c) => {
                    WidgetKind::BoxChild(BoxChild::new(resolver, c).await?)
                }
                view::WidgetKind::Grid(c) => {
                    let rows =
                        join_all(c.rows.into_iter().map(|c| Widget::new(resolver, c)))
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()?;
                    WidgetKind::Grid(Grid {
                        homogeneous_columns: c.homogeneous_columns,
                        homogeneous_rows: c.homogeneous_rows,
                        column_spacing: c.column_spacing,
                        row_spacing: c.row_spacing,
                        rows,
                    })
                }
                view::WidgetKind::GridChild(c) => {
                    WidgetKind::GridChild(GridChild::new(resolver, c).await?)
                }
                view::WidgetKind::GridRow(c) => {
                    let columns =
                        join_all(c.columns.into_iter().map(|c| Widget::new(resolver, c)))
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()?;
                    WidgetKind::GridRow(GridRow { columns })
                }
            };
            Ok(Widget { kind, props: widget.props })
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct View {
    pub(super) variables: HashMap<String, Value>,
    pub(super) root: Widget,
}

impl View {
    pub(super) async fn new(resolver: &ResolverRead, view: view::View) -> Result<Self> {
        Ok(View {
            variables: view.variables,
            root: Widget::new(resolver, view.root).await?,
        })
    }
}
