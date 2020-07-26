use anyhow::Result;
use futures::{future::join_all, prelude::*};
use netidx::{
    path::Path,
    resolver::{self, ResolverRead},
    subscriber::Value,
};
pub(super) use netidx_protocols::view::{self, *};
use std::{boxed::Box, pin::Pin, collections::HashMap};

#[derive(Debug, Clone)]
pub(super) struct Container {
    pub direction: view::Direction,
    pub hpct: f32,
    pub vpct: f32,
    pub keybinds: Vec<view::Keybind>,
    pub variables: HashMap<String, Value>,
    pub children: Vec<Widget>,
}

#[derive(Debug, Clone)]
pub(super) enum Widget {
    Table(Path, resolver::Table),
    StaticTable(view::Source),
    Label(view::Source),
    Action(view::Action),
    Button(view::Button),
    Toggle(view::Toggle),
    ComboBox(view::ComboBox),
    Radio(view::Radio),
    Entry(view::Entry),
    Container(Container),
}

impl Widget {
    fn new<'a>(
        resolver: &'a ResolverRead,
        widget: view::Widget,
    ) -> Pin<Box<dyn Future<Output = Result<Self>> + 'a>> {
        Box::pin(async move {
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
                view::Widget::ComboBox(c) => Ok(Widget::ComboBox(c)),
                view::Widget::Radio(r) => Ok(Widget::Radio(r)),
                view::Widget::Entry(e) => Ok(Widget::Entry(e)),
                view::Widget::Container(c) => {
                    let children = join_all(
                        c.children.into_iter().map(|c| Widget::new(resolver, c)),
                    )
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                    Ok(Widget::Container(Container {
                        direction: c.direction,
                        hpct: c.hpct,
                        vpct: c.vpct,
                        keybinds: c.keybinds,
                        variables: c.variables,
                        children
                    }))
                }
            }
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct View {
    pub(super) scripts: Vec<Source>,
    pub(super) root: Widget,
}

impl View {
    pub(super) async fn new(resolver: &ResolverRead, view: view::View) -> Result<Self> {
        Ok(View { scripts: view.scripts, root: Widget::new(resolver, view.root).await? })
    }
}
