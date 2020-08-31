use gtk::{self, prelude::*};

pub(super) fn toplevel<W: WidgetExt>(w: &W) -> gtk::Window {
    w.get_toplevel()
        .expect("modal dialog must have a toplevel window")
        .downcast::<gtk::Window>()
        .expect("not a window")
}

pub(super) fn ask_modal<W: WidgetExt>(w: &W, msg: &str) -> bool {
    let d = gtk::MessageDialog::new(
        Some(&toplevel(w)),
        gtk::DialogFlags::MODAL,
        gtk::MessageType::Question,
        gtk::ButtonsType::YesNo,
        msg,
    );
    let resp = d.run();
    d.close();
    resp == gtk::ResponseType::Yes
}

pub(super) fn err_modal<W: WidgetExt>(w: &W, msg: &str) {
    let d = gtk::MessageDialog::new(
        Some(&toplevel(w)),
        gtk::DialogFlags::MODAL,
        gtk::MessageType::Error,
        gtk::ButtonsType::Ok,
        msg,
    );
    d.run();
    d.close();
}
