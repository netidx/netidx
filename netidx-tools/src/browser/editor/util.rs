use gtk::{self, prelude::*};
use log::warn;
use std::{fmt::Display, str::FromStr, string::ToString};

pub(crate) fn parse_entry<T, F>(
    label: &str,
    spec: &T,
    on_change: F,
) -> (gtk::Label, gtk::Entry)
where
    T: FromStr + ToString + 'static,
    T::Err: Display,
    F: Fn(T) + 'static,
{
    let label = gtk::Label::new(Some(label));
    let entry = gtk::Entry::new();
    entry.set_text(&spec.to_string());
    entry.connect_activate(move |e| {
        let txt = e.get_text();
        match txt.parse::<T>() {
            Err(e) => warn!("invalid value: {}, {}", &*txt, e),
            Ok(src) => on_change(src),
        }
    });
    (label, entry)
}

#[derive(Clone, Debug)]
pub(crate) struct TwoColGrid {
    root: gtk::Grid,
    row: i32,
}

impl TwoColGrid {
    pub(crate) fn new() -> Self {
        TwoColGrid { root: gtk::Grid::new(), row: 0 }
    }

    pub(crate) fn add<T: IsA<gtk::Widget>, U: IsA<gtk::Widget>>(&mut self, w: (T, U)) {
        self.root.attach(&w.0, 0, self.row, 1, 1);
        self.root.attach(&w.1, 1, self.row, 1, 1);
        self.row += 1;
    }

    pub(crate) fn attach<T: IsA<gtk::Widget>>(
        &mut self,
        w: &T,
        col: i32,
        cols: i32,
        rows: i32,
    ) {
        self.root.attach(w, col, self.row, cols, rows);
        self.row += 1;
    }

    pub(crate) fn root(&self) -> &gtk::Grid {
        &self.root
    }
}
