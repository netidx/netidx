use gtk::{self, prelude::*};

pub(super) fn set_highlight<T: WidgetExt>(w: &T, h: bool) {
    let s = w.style_context();
    if h {
        s.add_class("highlighted");
    } else {
        s.remove_class("highlighted");
    }
}

pub(super) fn toplevel<W: WidgetExt>(w: &W) -> gtk::Window {
    w.toplevel()
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
    unsafe { d.destroy() };
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
    unsafe { d.destroy() };
}

use std::sync::Arc;
use parking_lot::{Mutex, Condvar};

#[derive(Clone)]
pub(crate) struct OneShot<T>(Arc<(Mutex<Option<T>>, Condvar)>);

impl<T> OneShot<T> {
    pub(crate) fn new() -> Self {
        OneShot(Arc::new((Mutex::new(None), Condvar::new())))
    }

    pub(crate) fn wait(&self) -> T {
        let mut inner = self.0.0.lock();
        if inner.is_none() {
            self.0.1.wait(&mut inner);
        }
        inner.take().unwrap()
    }

    pub(crate) fn send(&self, t: T) {
        let mut inner = self.0.0.lock();
        *inner = Some(t);
        self.0.1.notify_one();
    }
}
