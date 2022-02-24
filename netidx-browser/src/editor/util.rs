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
    entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
    entry.connect_changed(move |e| {
        e.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, Some("media-floppy"))
    });
    entry.connect_icon_press(move |e, _, _| e.emit_activate());
    entry.connect_activate(move |e| {
        let txt = e.text();
        match txt.parse::<T>() {
            Err(e) => warn!("invalid value: {}, {}", &*txt, e),
            Ok(src) => {
                e.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
                on_change(src);
            }
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
        let root = gtk::Grid::new();
        root.set_halign(gtk::Align::Fill);
        root.set_hexpand(true);
        TwoColGrid { root, row: 0 }
    }

    pub(crate) fn add<T: IsA<gtk::Widget>, U: IsA<gtk::Widget>>(&mut self, w: (T, U)) {
        self.root.attach(&w.0, 0, self.row, 1, 1);
        self.root.attach(&w.1, 1, self.row, 1, 1);
        w.1.set_halign(gtk::Align::Fill);
        w.1.set_hexpand(true);
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

/// 2020-09-29: for some reason expanders are very hard to click
/// properly on a touch screen. However just adding an event that
/// causes them to toggle when clicked with button 1 is all that's
/// needed to fix it. I suspect that by default the expander widget
/// uses hover logic (the arrow lights up when you hover), and of
/// course that doesn't work on a touch screen. This fix should be
/// removed when that behavior is fixed.
pub(super) fn expander_touch_enable(root: &gtk::Expander) {
    root.connect_button_press_event(
        clone!(@weak root => @default-return gtk::Inhibit(false), move |_, ev| {
            let left_click =
                gdk::EventType::ButtonPress == ev.event_type()
                && ev.button() == 1;
            if left_click {
                root.set_expanded(!root.is_expanded());
                gtk::Inhibit(true)
            } else {
                gtk::Inhibit(false)
            }
        }),
    );
}
