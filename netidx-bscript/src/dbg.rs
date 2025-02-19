use crate::{expr::ExprId, Event};
use chrono::prelude::*;
use fxhash::FxHashMap;
use netidx::subscriber::Value;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::{self, Weak},
};

pub struct DbgCtx<E: Debug> {
    pub trace: bool,
    events: VecDeque<(ExprId, (DateTime<Local>, Option<Event<E>>, Value))>,
    watch: FxHashMap<
        ExprId,
        Vec<Weak<dyn Fn(&DateTime<Local>, &Option<Event<E>>, &Value) + Send + Sync>>,
    >,
    current: FxHashMap<ExprId, (Option<Event<E>>, Value)>,
}

impl<E: Debug + Clone> DbgCtx<E> {
    pub(super) fn new() -> Self {
        DbgCtx {
            trace: false,
            events: VecDeque::new(),
            watch: HashMap::default(),
            current: HashMap::default(),
        }
    }

    pub fn iter_events(
        &self,
    ) -> impl Iterator<Item = &(ExprId, (DateTime<Local>, Option<Event<E>>, Value))> {
        self.events.iter()
    }

    pub fn get_current(&self, id: &ExprId) -> Option<&(Option<Event<E>>, Value)> {
        self.current.get(id)
    }

    pub fn add_watch(
        &mut self,
        id: ExprId,
        watch: &sync::Arc<
            dyn Fn(&DateTime<Local>, &Option<Event<E>>, &Value) + Send + Sync,
        >,
    ) {
        let watches = self.watch.entry(id).or_insert_with(Vec::new);
        watches.push(sync::Arc::downgrade(watch));
    }

    pub fn add_event(&mut self, id: ExprId, event: Option<Event<E>>, value: Value) {
        const MAX: usize = 1000;
        let now = Local::now();
        if let Some(watch) = self.watch.get_mut(&id) {
            let mut i = 0;
            while i < watch.len() {
                match Weak::upgrade(&watch[i]) {
                    None => {
                        watch.remove(i);
                    }
                    Some(f) => {
                        f(&now, &event, &value);
                        i += 1;
                    }
                }
            }
        }
        self.events.push_back((id, (now, event.clone(), value.clone())));
        self.current.insert(id, (event, value));
        if self.events.len() > MAX {
            self.events.pop_front();
            if self.watch.len() > MAX {
                self.watch.retain(|_, vs| {
                    vs.retain(|v| Weak::upgrade(v).is_some());
                    !vs.is_empty()
                });
            }
        }
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.current.clear();
        self.watch.retain(|_, v| {
            v.retain(|w| Weak::strong_count(w) > 0);
            v.len() > 0
        });
    }
}
