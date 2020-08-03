use anyhow::Result;
use futures::{future::join_all, prelude::Future};
use netidx::{
    path::Path,
    resolver::{self, ResolverRead},
    subscriber::Value,
};
pub(super) use netidx_protocols::view::{self, *};
use std::{boxed::Box as MemBox, collections::HashMap, pin::Pin};

#[derive(Debug, Clone)]
pub(super) struct BoxChild {
    pub expand: bool,
    pub fill: bool,
    pub padding: u64,
    pub halign: Option<view::Align>,
    pub valign: Option<view::Align>,
    pub widget: Widget,
}

impl BoxChild {
    async fn new(resolver: &ResolverRead, c: view::BoxChild) -> Result<Self> {
        Ok(BoxChild {
            expand: c.expand,
            fill: c.fill,
            padding: c.padding,
            halign: c.halign,
            valign: c.valign,
            widget: Widget::new(resolver, c.widget).await?,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct GridChild {
    pub halign: Option<Align>,
    pub valign: Option<Align>,
    pub widget: Widget,
}

impl GridChild {
    async fn new(resolver: &ResolverRead, c: view::GridChild) -> Result<Self> {
        Ok(GridChild {
            halign: c.halign,
            valign: c.valign,
            widget: Widget::new(resolver, c.widget).await?,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct Box {
    pub direction: view::Direction,
    pub children: Vec<BoxChild>,
}

#[derive(Debug, Clone)]
pub(super) struct Grid {
    pub homogeneous_columns: bool,
    pub homogeneous_rows: bool,
    pub column_spacing: u32,
    pub row_spacing: u32,
    pub children: Vec<Vec<GridChild>>,
}

#[derive(Debug, Clone)]
pub(super) enum Widget {
    Table(Path, resolver::Table),
    StaticTable(view::Source),
    Label(view::Source),
    Action(view::Action),
    Button(view::Button),
    Toggle(view::Toggle),
    Selector(view::Selector),
    Entry(view::Entry),
    Box(Box),
    Grid(Grid),
}

impl Widget {
    fn new<'a>(
        resolver: &'a ResolverRead,
        widget: view::Widget,
    ) -> Pin<MemBox<dyn Future<Output = Result<Self>> + 'a>> {
        MemBox::pin(async move {
            match widget {
                view::Widget::Table(s @ view::Source::Constant(_)) => {
                    Ok(Widget::StaticTable(s))
                }
                view::Widget::Table(s @ view::Source::Variable(_)) => {
                    Ok(Widget::StaticTable(s))
                }
                view::Widget::Table(view::Source::Load(path)) => {
                    let spec = resolver.table(path.clone()).await?;
                    Ok(Widget::Table(path, spec))
                }
                view::Widget::Label(s) => Ok(Widget::Label(s)),
                view::Widget::Action(a) => Ok(Widget::Action(a)),
                view::Widget::Button(b) => Ok(Widget::Button(b)),
                view::Widget::Toggle(t) => Ok(Widget::Toggle(t)),
                view::Widget::Selector(c) => Ok(Widget::Selector(c)),
                view::Widget::Entry(e) => Ok(Widget::Entry(e)),
                view::Widget::Box(c) => {
                    let children = join_all(
                        c.children.into_iter().map(|c| BoxChild::new(resolver, c)),
                    )
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                    Ok(Widget::Box(Box { direction: c.direction, children }))
                }
                view::Widget::Grid(c) => {
                    let children = join_all(c.children.into_iter().map(|c| async {
                        join_all(c.into_iter().map(|c| GridChild::new(resolver, c)))
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>>>()
                    }))
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                    Ok(Widget::Grid(Grid {
                        homogeneous_columns: c.homogeneous_columns,
                        homogeneous_rows: c.homogeneous_rows,
                        column_spacing: c.column_spacing,
                        row_spacing: c.row_spacing,
                        children,
                    }))
                }
            }
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
