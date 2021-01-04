use crate::pool::{Pool, Pooled};
use parking_lot::Mutex;
use std::sync::{Arc, Weak};
use std::{mem, ops::Drop, clone::Clone, result};
use futures::channel::oneshot;

#[derive(Debug)]
struct BatchChannelInner<T: Send + Sync + 'static> {
    send_closed: bool,
    recv_closed: bool,
    notify: Option<oneshot::Sender<()>>,
    queue: Pooled<Vec<T>>,
    pool: Pool<Vec<T>>,
}

#[derive(Debug)]
struct Sentinal<T: Send + Sync + 'static>(Weak<Mutex<BatchChannelInner<T>>>);

impl<T: Send + Sync + 'static> Drop for Sentinal<T> {
    fn drop(&mut self) {
        if let Some(sender) = Weak::upgrade(&self.0) {
            sender.lock().send_closed = true;
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchSender<T: Send + Sync + 'static>(
    Arc<Mutex<BatchChannelInner<T>>>,
    Arc<Sentinal<T>>,
);

impl<T: Send + Sync + 'static> Clone for BatchSender<T> {
    fn clone(&self) -> Self {
        BatchSender(Arc::clone(&self.0), Arc::clone(&self.1))
    }
}

impl<T: Send + Sync + 'static> BatchSender<T> {
    pub(crate) fn send(&self, m: T) -> bool {
        let mut inner = self.0.lock();
        if inner.recv_closed {
            false
        } else {
            inner.queue.push(m);
            if let Some(sender) = inner.notify.take() {
                let _: result::Result<_, _> = sender.send(());
            }
            true
        }
    }
}

#[derive(Debug)]
pub(crate) struct BatchReceiver<T: Send + Sync + 'static>(
    Arc<Mutex<BatchChannelInner<T>>>,
);

impl<T: Send + Sync + 'static> Drop for BatchReceiver<T> {
    fn drop(&mut self) {
        self.close()
    }
}

impl<T: Send + Sync + 'static> BatchReceiver<T> {
    pub(crate) fn close(&self) {
        self.0.lock().recv_closed = true
    }

    pub(crate) fn len(&self) -> usize {
        self.0.lock().queue.len()
    }

    pub(crate) async fn recv(&self) -> Option<Pooled<Vec<T>>> {
        loop {
            let receiver = {
                let mut inner = self.0.lock();
                if inner.queue.len() > 0 {
                    let v = inner.pool.take();
                    return Some(mem::replace(&mut inner.queue, v));
                } else if inner.send_closed {
                    return None;
                } else {
                    let (tx, rx) = oneshot::channel();
                    inner.notify = Some(tx);
                    rx
                }
            };
            let _: result::Result<_, _> = receiver.await;
        }
    }
}

pub(crate) fn channel<T: Send + Sync + 'static>(
) -> (BatchSender<T>, BatchReceiver<T>) {
    let pool = Pool::new(1, 1000000);
    let inner = Arc::new(Mutex::new(BatchChannelInner {
        send_closed: false,
        recv_closed: false,
        notify: None,
        queue: pool.take(),
        pool,
    }));
    let sentinal = Arc::new(Sentinal(Arc::downgrade(&inner)));
    let sender = BatchSender(inner.clone(), sentinal);
    let receiver = BatchReceiver(inner);
    (sender, receiver)
}
