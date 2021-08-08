use crate::pool::{Pool, Pooled};
use futures::channel::oneshot;
use parking_lot::Mutex;
use std::sync::Arc;
use std::{clone::Clone, mem, ops::Drop, result};

#[derive(Debug)]
struct BatchChannelInner<T: Send + Sync + 'static> {
    send_closed: bool,
    recv_closed: bool,
    notify: Option<oneshot::Sender<()>>,
    queue: Pooled<Vec<T>>,
    pool: Pool<Vec<T>>,
}

#[derive(Debug)]
struct BatchSenderInner<T: Send + Sync + 'static>(Arc<Mutex<BatchChannelInner<T>>>);

impl<T: Send + Sync + 'static> Drop for BatchSenderInner<T> {
    fn drop(&mut self) {
        self.0.lock().send_closed = true;
    }
}

#[derive(Debug)]
pub(crate) struct BatchSender<T: Send + Sync + 'static>(Arc<BatchSenderInner<T>>);

impl<T: Send + Sync + 'static> Clone for BatchSender<T> {
    fn clone(&self) -> Self {
        BatchSender(Arc::clone(&self.0))
    }
}

impl<T: Send + Sync + 'static> BatchSender<T> {
    pub(crate) fn send(&self, m: T) -> bool {
        let mut inner = self.0 .0.lock();
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
        let mut inner = self.0.lock();
        inner.recv_closed = true;
        inner.queue.clear();
        inner.notify = None;
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

pub(crate) fn channel<T: Send + Sync + 'static>() -> (BatchSender<T>, BatchReceiver<T>) {
    let pool = Pool::new(1, 1000000);
    let inner = Arc::new(Mutex::new(BatchChannelInner {
        send_closed: false,
        recv_closed: false,
        notify: None,
        queue: pool.take(),
        pool,
    }));
    let sender = BatchSender(Arc::new(BatchSenderInner(inner.clone())));
    let receiver = BatchReceiver(inner);
    (sender, receiver)
}
