use anyhow::Result;
use futures::channel::mpsc;
use netidx::{
    path::Path,
    pool::Pooled,
    publisher::{Publisher, Val, Value, WriteRequest},
};

pub struct Listener {
    publisher: Publisher,
    listener: Val,
    waiting: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
}

impl Listener {
    pub fn new(
        publisher: &Publisher,
        queue_depth: usize,
        path: Path,
    ) -> Result<Listener> {
        let publisher = publisher.clone();
        let listener = publisher.publish(path, Value::from("channel"))?;
        let (tx_waiting, rx_waiting) = mpsc::channel(queue_depth);
        publisher.writes(listener.id(), tx_waiting);
        Ok(Self { publisher, listener, waiting: rx_waiting })
    }
}
