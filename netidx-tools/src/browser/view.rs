use anyhow::Result;
use futures::{future::join_all, prelude::*};
use netidx::{
    path::Path,
    resolver::{self, ResolverRead},
    subscriber::Value,
};
pub(super) use netidx_protocols::view::{self, *};
use std::{boxed::Box, collections::HashMap, pin::Pin};

#[derive(Debug, Clone)]
pub(super) struct Child {
    pub expand: bool,
    pub fill: bool,
    pub padding: u64,
    pub halign: Option<view::Align>,
    pub valign: Option<view::Align>,
    pub widget: Widget,
}

#[derive(Debug, Clone)]
pub(super) struct Container {
    pub direction: view::Direction,
    pub keybinds: Vec<view::Keybind>,
    pub drill_down_target: Option<Path>,
    pub drill_up_target: Option<Path>,
    pub children: Vec<Child>,
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
                    let children = join_all(c.children.into_iter().map(|c| {
                        let expand = c.expand;
                        let padding = c.padding;
                        let fill = c.fill;
                        let halign = c.halign;
                        let valign = c.valign;
                        Widget::new(resolver, c.widget).map(move |w| {
                            Ok(Child {
                                expand,
                                fill,
                                padding,
                                halign,
                                valign,
                                widget: w?,
                            })
                        })
                    }))
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                    Ok(Widget::Container(Container {
                        direction: c.direction,
                        keybinds: c.keybinds,
                        drill_down_target: c.drill_down_target,
                        drill_up_target: c.drill_up_target,
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
