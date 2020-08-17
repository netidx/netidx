use anyhow::Result;
use futures::{future::join_all, prelude::Future};
use netidx::{
    path::Path,
    resolver::{self, ResolverRead},
    subscriber::Value,
};
pub(super) use netidx_protocols::view::{self, *};
use std::{boxed, collections::HashMap, pin::Pin};

#[derive(Debug, Clone)]
pub(super) struct BoxChild {
    pub expand: bool,
    pub fill: bool,
    pub padding: u64,
    pub halign: Option<view::Align>,
    pub valign: Option<view::Align>,
    pub widget: boxed::Box<Widget>,
}

impl BoxChild {
    async fn new(resolver: &ResolverRead, c: view::BoxChild) -> Result<Self> {
        Ok(BoxChild {
            expand: c.expand,
            fill: c.fill,
            padding: c.padding,
            halign: c.halign,
            valign: c.valign,
            widget: boxed::Box::new(Widget::new(resolver, (&*c.widget).clone()).await?),
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct GridChild {
    pub halign: Option<Align>,
    pub valign: Option<Align>,
    pub widget: boxed::Box<Widget>,
}

impl GridChild {
    async fn new(resolver: &ResolverRead, c: view::GridChild) -> Result<Self> {
        Ok(GridChild {
            halign: c.halign,
            valign: c.valign,
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
pub(super) struct Grid {
    pub homogeneous_columns: bool,
    pub homogeneous_rows: bool,
    pub column_spacing: u32,
    pub row_spacing: u32,
    pub children: Vec<Vec<Widget>>,
}

#[derive(Debug, Clone)]
pub(super) enum Widget {
    Table(Path, resolver::Table),
    Label(view::Source),
    Button(view::Button),
    Toggle(view::Toggle),
    Selector(view::Selector),
    Entry(view::Entry),
    Box(Box),
    BoxChild(BoxChild),
    Grid(Grid),
    GridChild(GridChild),
}

impl Widget {
    fn new<'a>(
        resolver: &'a ResolverRead,
        widget: view::Widget,
    ) -> Pin<boxed::Box<dyn Future<Output = Result<Self>> + 'a + Send>> {
        boxed::Box::pin(async move {
            match widget {
                view::Widget::Table(path) => {
                    let spec = resolver.table(path.clone()).await?;
                    Ok(Widget::Table(path, spec))
                }
                view::Widget::Label(s) => Ok(Widget::Label(s)),
                view::Widget::Button(b) => Ok(Widget::Button(b)),
                view::Widget::Toggle(t) => Ok(Widget::Toggle(t)),
                view::Widget::Selector(c) => Ok(Widget::Selector(c)),
                view::Widget::Entry(e) => Ok(Widget::Entry(e)),
                view::Widget::Box(c) => {
                    let children = join_all(
                        c.children.into_iter().map(|c| Widget::new(resolver, c)),
                    )
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                    Ok(Widget::Box(Box {
                        direction: c.direction,
                        homogeneous: c.homogeneous,
                        spacing: c.spacing,
                        children,
                    }))
                }
                view::Widget::BoxChild(c) => {
                    Ok(Widget::BoxChild(BoxChild::new(resolver, c).await?))
                }
                view::Widget::Grid(c) => {
                    let children = join_all(c.children.into_iter().map(|c| async {
                        join_all(c.into_iter().map(|c| Widget::new(resolver, c)))
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
                view::Widget::GridChild(c) => {
                    Ok(Widget::GridChild(GridChild::new(resolver, c).await?))
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct View {
    pub(super) variables: HashMap<String, Value>,
    pub(super) root: Widget,
    pub(super) scripts: Vec<(String, String)>,
}

impl View {
    pub(super) async fn new(resolver: &ResolverRead, view: view::View) -> Result<Self> {
        Ok(View {
            variables: view.variables,
            scripts: view.scripts,
            root: Widget::new(resolver, view.root).await?,
        })
    }
}
