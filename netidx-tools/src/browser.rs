use cursive::{
    view::Nameable,
    views::{LinearLayout, NamedView, PaddedView, ScrollView, SelectView, TextView},
    utils::span::SpannedString,
    theme::Style,
    Cursive, CursiveExt,
};
use netidx::{
    path::Path,
    subscriber::{Subscriber, Value},
};
use netidx_protocols::view::{Direction, Keybind, Sink, Source, View, Widget};
use std::{
    cmp::{max, min},
    io::{self, Write},
};

pub(crate) fn run() {
    let mut term = Cursive::crossterm().unwrap();
    let mut c0 = SelectView::new().with_name("c0");
    let mut c1 = SelectView::new().with_name("c1");
    c0.get_mut().set_on_select(|c, i| {
        c.call_on_name("c1", |v: &mut NamedView<SelectView<usize>>| {
            v.get_mut().set_selection(*i)
        });
    });
    c1.get_mut().set_on_select(|c, i| {
        c.call_on_name("c0", |v: &mut NamedView<SelectView<usize>>| {
            v.get_mut().set_selection(*i)
        });
    });
    for i in 0..100 {
        c0.get_mut().add_item(format!("{},0", i), i);
    }
    for i in 0..100 {
        c1.get_mut().add_item(format!("{},1", i), i);
    }
    c0.get_mut().add_item(format!("very very very long item"), 100);
    c1.get_mut().add_item(format!("100,1"), 100);
    *c0.get_mut().get_item_mut(0).unwrap().0 = SpannedString::<Style>::plain("edited");
    term.add_fullscreen_layer(ScrollView::new(
        LinearLayout::horizontal()
            .child(PaddedView::lrtb(0, 1, 0, 0, c0))
            .child(PaddedView::lrtb(0, 0, 0, 0, c1)),
    ));
    term.run()
}
