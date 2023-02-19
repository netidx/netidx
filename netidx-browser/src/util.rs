use gtk4::{self as gtk, prelude::*};
use std::{rc::Rc, cell::RefCell};

pub(super) fn set_highlight<T: WidgetExt>(w: &T, h: bool) {
    let s = w.style_context();
    if h {
        s.add_class("highlighted");
    } else {
        s.remove_class("highlighted");
    }
}

pub(super) fn toplevel<W: WidgetExt>(w: &W) -> gtk::Window {
    w.parent()
        .expect("modal dialog must have a toplevel window")
        .downcast::<gtk::Window>()
        .expect("not a window")
}

pub(super) fn ask_modal<W: WidgetExt, F: FnMut(bool) + 'static>(w: &W, msg: &str, f: F) {
    let f = Rc::new(RefCell::new(f));
    let d = gtk::MessageDialog::new(
        Some(&toplevel(w)),
        gtk::DialogFlags::MODAL,
        gtk::MessageType::Question,
        gtk::ButtonsType::YesNo,
        msg,
    );
    d.connect_response(clone!(@strong f => move |_, resp| {
        (f.borrow_mut())(resp == gtk::ResponseType::Yes)
    }));
}

pub(super) fn err_modal<W: WidgetExt>(w: &W, msg: &str) {
    gtk::MessageDialog::new(
        Some(&toplevel(w)),
        gtk::DialogFlags::MODAL,
        gtk::MessageType::Error,
        gtk::ButtonsType::Ok,
        msg,
    );
}

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct OneShot<T>(Arc<(Mutex<Option<T>>, Condvar)>);

impl<T> OneShot<T> {
    pub(crate) fn new() -> Self {
        OneShot(Arc::new((Mutex::new(None), Condvar::new())))
    }

    pub(crate) fn wait(&self) -> T {
        let mut inner = self.0 .0.lock();
        if inner.is_none() {
            self.0 .1.wait(&mut inner);
        }
        inner.take().unwrap()
    }

    pub(crate) fn send(&self, t: T) {
        let mut inner = self.0 .0.lock();
        *inner = Some(t);
        self.0 .1.notify_one();
    }
}
